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

#: Version stamped on the event. Its two standard measurements carry the
#: measurement versions every event's do.
FIR_VERSION = "fir_v0.1.0"

AIRSPACE_TABLE = "opdi_h3_airspace_ref"

_RES_SHIFT = 52
_RES_MASK = 0xF << _RES_SHIFT


def h3_parent_expr(cell_col, res: int):
    """The lower-case hex index of ``cell_col``'s ancestor at ``res`` (0..7),
    as a native column expression (no UDF).
    """
    idx = F.conv(cell_col, 16, 10).cast("long")
    cleared = idx.bitwiseAND(lit(~_RES_MASK))
    with_res = cleared.bitwiseOR(lit(res << _RES_SHIFT))
    unused = (1 << (3 * (15 - res))) - 1
    parent = with_res.bitwiseOR(lit(unused))
    return F.lower(F.conv(parent.cast("string"), 10, 16))


def load_fir_reference(storage) -> Optional[DataFrame]:
    """The FIR layer of ``opdi_h3_airspace_ref``, or None when the table is
    absent or still has the earlier port's per-cell schema (``h3_res_7``
    columns, no ``h3_cell``/``h3_res``): that table was overwritten once per
    polygon and holds one country, and step 00c must be re-run first.
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
    """Add ``fir_code, fir_band_width, fir_area_km2, airac_cfmu, airac_validity_start,
    airac_validity_end`` per sample (NULL when no volume matches).

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
    after = w.rowsBetween(1, Window.unboundedFollowing)
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


def calculate_fir_events(
    sdf: DataFrame, fir_ref: Optional[DataFrame], config: EventConfig
) -> Optional[DataFrame]:
    """``xing-fir`` rows for every change of volume inside a piece.

    ``sdf`` is the step 04 input (needs ``h3_res_7`` and ``baro_altitude_c``,
    plus ``piece`` and the cumulative columns); ``fir_ref`` the frame from
    ``load_fir_reference``. None when the family is off or the reference is
    absent.
    """
    if not config.emit_fir_crossings or fir_ref is None:
        return None
    if "h3_res_7" not in sdf.columns:
        print("h3_res_7 is not in the input; xing-fir will not be computed.")
        return None
    df = sdf.filter(col("event_time").isNotNull() & col("h3_res_7").isNotNull())
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
    df = (
        df.withColumn("_prev_run", prev_run)
        .withColumn("_bracket_s", col("_ts") - prev_ts)
        .withColumn("_crossing", crossing)
        .filter(col("_crossing"))
    )
    seq = Window.partitionBy("track_id").orderBy("_ts")
    df = df.withColumn("crossing_seq", F.row_number().over(seq))
    info = F.to_json(
        F.struct(
            col("crossing_seq"),
            col("_prev_run").alias("fir_from"),
            col("fir_run").alias("fir_to"),
            F.round(col("_fl"), 0).alias("fl"),
            col("_bracket_s").alias("bracket_s"),
            col("airac_cfmu"),
            col("airac_validity_start"),
            col("airac_validity_end"),
        )
    )
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
        lit(FIR_VERSION).alias("version"),
    )
