"""FIR crossings: ``xing-fir``.

Which flight information region a sample is in is a lookup, not a
classification: step 00c fills every Network Manager FIR volume with compact
H3 cells (``opdi_h3_airspace_ref``, layer ``FIR``), and a sample's
resolution-7 cell, or one of its ancestors, is in exactly one of them unless
two volumes overlap. The ancestors are computed here with plain bit
operations on the 64-bit H3 index (the resolution field is bits 52 to 55 and
the per-resolution digits are 3 bits each from bit 44 down; unused digits are
set to 7), so no library call runs on the executors, and the reference is
small enough to broadcast once per resolution.

A crossing is a change of volume between consecutive samples of one piece,
after short runs (shorter than ``fir_min_dwell_seconds``) have been absorbed
into the run before them, so a track skimming a boundary does not emit a
crossing per cell flip. The event sits on the first sample inside the new
volume and names both volumes, the flight level, the time between the two
samples, and the AIRAC cycle the definition came from.
"""

from typing import Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit

from opdi.config import EventConfig
from opdi.pipeline.holding import FT_PER_M
from opdi.pipeline.holes import PIECE
from opdi.utils.geometry import haversine_nm

# FIR_VERSION is stamped on the event; its two standard measurements carry the
# measurement versions every event's do.
from opdi.versions import FIR_VERSION, LEGACY_FIR_VERSION

#: The settings that change the FIR crossings: with all of them at their
#: values of release v0.0.2 the events carry :data:`LEGACY_FIR_VERSION`.
FIR_SETTINGS = (
    "fir_needs_position",
    "fir_flag_vertical",
    "crossing_skips_position_jumps",
    "crossing_max_implied_speed_kt",
    "altitude_from_cleaning",
    "altitude_plausible_min_ft",
    "altitude_plausible_max_ft",
)


def fir_version(config: EventConfig) -> str:
    """The ``version`` of the FIR crossings under ``config``."""
    if config.differs_from_release(FIR_SETTINGS):
        return FIR_VERSION
    return LEGACY_FIR_VERSION


AIRSPACE_TABLE = "opdi_h3_airspace_ref"

_RES_SHIFT = 52
_RES_MASK = 0xF << _RES_SHIFT


def h3_parent_expr(cell_col, res: int):
    """The H3 index of ``cell_col``'s parent cell at resolution ``res`` (0 to 7).

    Lower-case hexadecimal, as a native column expression (no UDF).
    """
    idx = F.conv(cell_col, 16, 10).cast("long")
    cleared = idx.bitwiseAND(lit(~_RES_MASK))
    with_res = cleared.bitwiseOR(lit(res << _RES_SHIFT))
    unused = (1 << (3 * (15 - res))) - 1
    parent = with_res.bitwiseOR(lit(unused))
    return F.lower(F.conv(parent.cast("string"), 10, 16))


def load_fir_reference(storage) -> Optional[DataFrame]:
    """The FIR layer of ``opdi_h3_airspace_ref``, or None when it cannot be used.

    None when the table is absent, or has an older layout (``h3_res_7``
    columns instead of ``h3_cell``/``h3_res``) that holds only one polygon;
    step 00c must then be run again.
    """
    if not storage.table_exists(AIRSPACE_TABLE):
        print(f"{AIRSPACE_TABLE} not found; xing-fir will not be computed.")
        return None
    ref = storage.read_table(AIRSPACE_TABLE)
    if "h3_cell" not in ref.columns or "h3_res" not in ref.columns:
        print(
            f"{AIRSPACE_TABLE} has the pre-2026-11 schema (no h3_cell/h3_res); "
            "re-run step 00c. xing-fir will not be computed."
        )
        return None
    return ref.filter(col("airspace_type") == "FIR")


def attach_fir(df: DataFrame, fir_ref: DataFrame, config: EventConfig) -> DataFrame:
    """Add the FIR of each report and its definition (NULL outside every FIR).

    The columns are ``fir_code``, ``fir_band_width``, ``fir_area_km2``,
    ``airac_cfmu``, ``airac_validity_start`` and ``airac_validity_end``.

    One broadcast left join per resolution from 7 down to 0, each against the
    reference rows stored at that resolution and gated on the FL band; the
    candidates are ranked by narrowest band, smallest area, lowest code. A
    sample matched twice at one resolution (two volumes overlapping there) is
    kept once, by a lag test inside the ``(track_id, piece)`` window the
    run-length step needs anyway.
    """
    if "_ts" not in df.columns:
        df = df.withColumn("_ts", col("event_time").cast("double"))
    if "_fl" not in df.columns:
        df = df.withColumn("_fl", col("baro_altitude_c") * FT_PER_M / 100.0)
    candidates = []
    max_res = int(fir_ref.agg(F.max("h3_res")).first()[0] or 7)
    # Down to resolution 0: a compacted ocean FIR stores whole base cells.
    for r in range(min(max_res, 7), -1, -1):
        ref_r = fir_ref.filter(col("h3_res") == r).select(
            col("h3_cell").alias(f"_c{r}"),
            col("code").alias(f"_code{r}"),
            col("min_fl").alias(f"_lo{r}"),
            col("max_fl").alias(f"_hi{r}"),
            col("area_km2").alias(f"_area{r}"),
            col("airac_cfmu").alias(f"_airac{r}"),
            F.date_format(col("validity_start"), "yyyy-MM-dd").alias(f"_vs{r}"),
            F.date_format(col("validity_end"), "yyyy-MM-dd").alias(f"_ve{r}"),
        )
        parent = h3_parent_expr(col("h3_res_7"), r) if r < 7 else col("h3_res_7")
        cond = (
            (parent == col(f"_c{r}"))
            & (col("_fl") >= col(f"_lo{r}"))
            & (col("_fl") < col(f"_hi{r}"))
        )
        df = df.join(F.broadcast(ref_r), cond, "left").drop(f"_c{r}")
        candidates.append(
            F.struct(
                (col(f"_hi{r}") - col(f"_lo{r}")).alias("band_width"),
                col(f"_area{r}").alias("area_km2"),
                col(f"_code{r}").alias("code"),
                col(f"_airac{r}").alias("airac_cfmu"),
                col(f"_vs{r}").alias("validity_start"),
                col(f"_ve{r}").alias("validity_end"),
            )
        )
    matched = F.filter(F.array(*candidates), lambda c: c["code"].isNotNull())
    best = F.try_element_at(F.array_sort(matched), lit(1))
    df = df.withColumn("_best", best)
    df = df.select(
        *[
            c
            for c in df.columns
            if not (c.startswith(("_code", "_lo", "_hi", "_area", "_airac", "_vs", "_ve")))
        ],
    )
    df = (
        df.withColumn("fir_code", col("_best.code"))
        .withColumn("fir_band_width", col("_best.band_width"))
        .withColumn("fir_area_km2", col("_best.area_km2"))
        .withColumn("airac_cfmu", col("_best.airac_cfmu"))
        .withColumn("airac_validity_start", col("_best.validity_start"))
        .withColumn("airac_validity_end", col("_best.validity_end"))
        .drop("_best")
    )
    # Duplicates from a double match at one resolution: keep the best-ranked row.
    w = Window.partitionBy("track_id", PIECE).orderBy(
        col("_ts").asc(),
        col("fir_band_width").asc_nulls_last(),
        col("fir_area_km2").asc_nulls_last(),
        col("fir_code").asc_nulls_last(),
    )
    dup = F.lag("_ts").over(w) == col("_ts")
    return df.withColumn("_dup", F.coalesce(dup, lit(False))).filter(~col("_dup")).drop("_dup")


def _debounced_runs(df: DataFrame, config: EventConfig) -> DataFrame:
    """``fir_run`` = the volume code after short runs are absorbed into the run before."""
    w = Window.partitionBy("track_id", PIECE).orderBy("_ts")
    upto = w.rowsBetween(Window.unboundedPreceding, 0)
    # The rows after this one, as the rows before it in the reversed order:
    # a frame ending at the last row is rescanned for every row (quadratic in
    # the track's length), a frame starting at the first row is not. Times
    # are unique within a track, so both frames hold the same rows, and a
    # minimum does not depend on their order.
    after = (
        Window.partitionBy("track_id", PIECE)
        .orderBy(col("_ts").desc())
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    whole = w.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    start = (
        ~(col("fir_code").eqNullSafe(F.lag("fir_code").over(w)))
        | F.lag("fir_code").over(w).isNull() & F.lag("_ts").over(w).isNull()
    )
    df = df.withColumn("_start", F.coalesce(start, lit(True)))
    df = df.withColumn("_run_start_ts", F.max(F.when(col("_start"), col("_ts"))).over(upto))
    prev_ts = F.lag("_ts").over(w)
    df = df.withColumn("_prev_ts", prev_ts)
    df = df.withColumn(
        "_run_last_ts",
        F.coalesce(
            F.min(F.when(col("_start"), col("_prev_ts"))).over(after), F.max("_ts").over(whole)
        ),
    )
    long = (col("_run_last_ts") - col("_run_start_ts")) >= config.fir_min_dwell_seconds
    df = df.withColumn("_long_code", F.when(long, col("fir_code")))
    df = df.withColumn("_long_seen", F.when(long, lit(1)))
    # A short run takes the code of the last long *known* run before it (a
    # long stretch with no volume is not a code to inherit); a short run with
    # none stays NULL (not a crossing).
    carried = F.last(
        F.when(long & col("fir_code").isNotNull(), F.struct(col("fir_code").alias("c"))),
        ignorenulls=True,
    ).over(upto)
    df = df.withColumn("fir_run", F.when(long, col("fir_code")).otherwise(carried["c"]))
    return df.drop(
        "_start", "_run_start_ts", "_prev_ts", "_run_last_ts", "_long_code", "_long_seen"
    )


def _with_vertical(crossings: DataFrame, fir_ref: DataFrame) -> DataFrame:
    """Add ``_vertical``: the regions before and after lie one above the other.

    Each region's flight-level band is the lowest and highest level of its
    volumes in the reference. Two regions lie one above the other when their
    bands do not overlap (a lower FIR up to FL195 and an upper UIR from
    FL195, say). False when they overlap, and when a band is unknown.
    Joined on the crossings only, which are few, with the band table
    broadcast.
    """
    bands = fir_ref.groupBy("code").agg(F.min("min_fl").alias("lo"), F.max("max_fl").alias("hi"))
    before = F.broadcast(
        bands.select(
            col("code").alias("_from_code"),
            col("lo").alias("_from_lo"),
            col("hi").alias("_from_hi"),
        )
    )
    after = F.broadcast(
        bands.select(
            col("code").alias("_to_code"), col("lo").alias("_to_lo"), col("hi").alias("_to_hi")
        )
    )
    out = crossings.join(before, col("_prev_run") == col("_from_code"), "left").join(
        after, col("fir_run") == col("_to_code"), "left"
    )
    apart = (col("_from_hi") <= col("_to_lo")) | (col("_to_hi") <= col("_from_lo"))
    return out.withColumn("_vertical", F.coalesce(apart, lit(False))).drop(
        "_from_code", "_from_lo", "_from_hi", "_to_code", "_to_lo", "_to_hi"
    )


def calculate_fir_events(
    sdf: DataFrame, fir_ref: Optional[DataFrame], config: EventConfig
) -> Optional[DataFrame]:
    """``xing-fir`` rows for every change of volume inside a piece.

    ``sdf`` is the step 04 input (needs ``h3_res_7`` and ``baro_altitude_c``,
    plus ``piece`` and the cumulative columns); ``fir_ref`` the frame from
    ``load_fir_reference``. None when the family is off or the reference is
    absent.

    Three settings of :class:`~opdi.config.EventConfig` apply:

    * :attr:`~opdi.config.EventConfig.fir_needs_position`: only reports with
      a latitude and a longitude are looked up, so a crossing always has a
      position, and never comes from the map cell of a position cleaning
      removed;
    * :attr:`~opdi.config.EventConfig.crossing_max_implied_speed_kt`: a
      crossing whose report and the last report in the region before it imply
      a higher ground speed is not given (a position jump);
    * :attr:`~opdi.config.EventConfig.fir_flag_vertical`: ``info.vertical``
      says whether the two regions lie one above the other.
    """
    if not config.emit_fir_crossings or fir_ref is None:
        return None
    if "h3_res_7" not in sdf.columns:
        print("h3_res_7 is not in the input; xing-fir will not be computed.")
        return None
    df = sdf.filter(col("event_time").isNotNull() & col("h3_res_7").isNotNull())
    if config.fir_needs_position:
        df = df.filter(col("lat").isNotNull() & col("lon").isNotNull())
    if PIECE not in df.columns:
        df = df.withColumn(PIECE, lit(0))
    df = attach_fir(df, fir_ref, config)
    df = _debounced_runs(df, config)
    # A crossing is a change between consecutive *known* runs: a stretch with
    # no volume in between (altitude masked, a gap in the reference) is not a
    # crossing out and back in, and the time bracket runs from the last sample
    # that had a volume.
    w = Window.partitionBy("track_id", PIECE).orderBy("_ts")
    before = w.rowsBetween(Window.unboundedPreceding, -1)
    known_ts = F.when(col("fir_run").isNotNull(), col("_ts"))
    prev_run = F.last("fir_run", ignorenulls=True).over(before)
    prev_ts = F.last(known_ts, ignorenulls=True).over(before)
    crossing = col("fir_run").isNotNull() & prev_run.isNotNull() & (col("fir_run") != prev_run)
    max_kt = config.crossing_max_implied_speed_kt
    if max_kt is not None:
        # The position of the last report in the region before, from the
        # same report as its time.
        known_pos = F.when(
            col("fir_run").isNotNull() & col("lat").isNotNull() & col("lon").isNotNull(),
            F.struct(col("_ts").alias("t"), col("lat").alias("lat"), col("lon").alias("lon")),
        )
        df = df.withColumn("_prev_pos", F.last(known_pos, ignorenulls=True).over(before))
    df = (
        df.withColumn("_prev_run", prev_run)
        .withColumn("_bracket_s", col("_ts") - prev_ts)
        .withColumn("_crossing", crossing)
        .filter(col("_crossing"))
    )
    if max_kt is not None:
        prev = col("_prev_pos")
        dist_nm = haversine_nm(prev["lat"], prev["lon"], col("lat"), col("lon"))
        dt_s = col("_ts") - prev["t"]
        reach_nm = lit(float(max_kt)) * F.greatest(dt_s, lit(0.0)) / lit(3600.0)
        too_fast = dist_nm > reach_nm + lit(float(config.position_jump_allowance_nm))
        # Unknown (no position on one side) keeps the crossing. It still
        # counts in ``crossing_seq``, as a crossing that cannot be placed does
        # in the other crossing families.
        df = df.withColumn("_too_fast", F.coalesce(too_fast, lit(False)))
    seq = Window.partitionBy("track_id").orderBy("_ts")
    df = df.withColumn("crossing_seq", F.row_number().over(seq))
    if max_kt is not None:
        df = df.filter(~col("_too_fast"))
    fields = [
        col("crossing_seq"),
        col("_prev_run").alias("fir_from"),
        col("fir_run").alias("fir_to"),
        F.round(col("_fl"), 0).alias("fl"),
        col("_bracket_s").alias("bracket_s"),
        col("airac_cfmu"),
        col("airac_validity_start"),
        col("airac_validity_end"),
    ]
    if config.fir_flag_vertical:
        df = _with_vertical(df, fir_ref)
        fields.append(col("_vertical").alias("vertical"))
    info = F.to_json(F.struct(*fields))
    return df.select(
        col("track_id"),
        lit("xing-fir").alias("type"),
        col("event_time"),
        col("lon"),
        col("lat"),
        (col("baro_altitude_c") * FT_PER_M).alias("altitude_ft"),
        col("cumulative_distance_nm"),
        col("cumulative_time_s").cast("long"),
        info.alias("info"),
        lit(fir_version(config)).alias("version"),
    )
