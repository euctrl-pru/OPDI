"""Ground movement milestones: off-block (T04) and on-block (T21).

A native port of the signal in ``traffic``'s ``StartMoving``
(``traffic/src/traffic/algorithms/ground/movement.py:13-63``): the aircraft is
moving once its groundspeed stays above a threshold for a sustained period, and
the sustained part is what separates a push from a jitter in the speed field.

``traffic``'s parking-position path is **not** ported. It resolves stands
through ``airport.parking_position``, which issues a live OpenStreetMap
Overpass query whose ``lru_cache`` is stripped on pickling
(``traffic/core/structure.py:104-137``) -- so it cannot run on an executor with
no network, which is the case OPDI has to assume. OPDI's own
``hexaero_airport_layouts`` already carries the same OSM ``parking_position``
geometry as a committed H3 res-12 table, and step 04 already emits
``entry-parking_position``/``exit-parking_position`` from it. This anchors on
those events rather than re-deriving the geometry.

**Coverage, not accuracy, is the expected limitation.** ADS-B ground reception
is sparse -- an aircraft on a stand is often not received at all -- so these
milestones will be missing for a large share of flights. That number is a
finding to report, not something to tune away.
"""

from typing import TYPE_CHECKING, Optional, Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from opdi.utils.geometry import haversine_nm

if TYPE_CHECKING:  # pragma: no cover - avoids an import cycle
    from opdi.config import EventConfig

KT_PER_MPS = 1.94384


def _derived_groundspeed_kt(ordered: Window, time_col: str) -> Column:
    """Position-derived groundspeed in knots, 3-sample median filtered.

    Ported from traffic's ``StartMoving``, which does not trust the broadcast
    ``velocity`` field at all: ``resampled.cumulative_distance().filter(
    compute_gs=3).query("compute_gs > threshold")``
    (``traffic/src/traffic/algorithms/ground/movement.py:37-46``). Native
    Spark equivalent -- no UDF: haversine between consecutive samples over the
    time delta, then a centred 3-sample rolling median (``filter(compute_gs=3)``
    is itself a rolling median filter), because position-derived speed is
    spiky and traffic filters it for exactly that reason before thresholding.
    """
    prev_lat = F.lag(F.col("lat")).over(ordered)
    prev_lon = F.lag(F.col("lon")).over(ordered)
    prev_t = F.lag(F.col(time_col)).over(ordered)

    dt_s = F.col(time_col).cast("double") - prev_t.cast("double")
    dist_nm = haversine_nm(prev_lat, prev_lon, F.col("lat"), F.col("lon"))
    raw_kt = F.when(dt_s > 0, dist_nm / (dt_s / 3600.0))

    # `F.median` cannot be windowed with an explicit frame in this Spark
    # version ([INVALID_WINDOW_SPEC_FOR_AGGREGATION_FUNC]); `percentile_approx`
    # at 0.5 is Spark's windowable median and, over a 3-row frame, exact for
    # any practical purpose (default accuracy 10000 >> 3 samples).
    return F.percentile_approx(raw_kt, 0.5).over(ordered.rowsBetween(-1, 1))


def _movement_runs(
    sdf: DataFrame,
    config: "EventConfig",
    *,
    partition_cols: Sequence[str] = ("track_id",),
    time_col: str = "event_time",
    counts: Optional[Column] = None,
    break_after_seconds: Optional[float] = None,
) -> DataFrame:
    """Each run of sustained movement: ``_from`` and ``_to`` per run.

    ``counts`` says which reports may count as moving (all when None).
    ``break_after_seconds`` starts a new run after a silence longer than that,
    so two movements with nothing received between them are not one run.
    """
    ordered = Window.partitionBy(*partition_cols).orderBy(time_col)

    gs_kt = F.col("velocity") * KT_PER_MPS
    if config.ground_speed_derive_from_position and "lat" in sdf.columns and "lon" in sdf.columns:
        # Fill the gaps, do not replace: the reported speed is used wherever
        # `velocity` exists, the derived one only where it is missing (see
        # EventConfig.ground_speed_derive_from_position). Applied only when the
        # lat/lon columns are present: a caller without them (some tests, any
        # minimal projection) gets the reported speed alone rather than an
        # unresolved-column analysis error.
        gs_kt = F.coalesce(gs_kt, _derived_groundspeed_kt(ordered, time_col))
    moving = gs_kt > F.lit(config.ground_speed_threshold_kt)
    if counts is not None:
        moving = moving & counts
    work = sdf.withColumn("_moving", moving)

    # Sessionise runs of movement, then keep runs long enough to be a push
    # rather than a wobble in the speed field.
    work = work.withColumn("_prev", F.lag("_moving").over(ordered))
    starts = F.col("_moving") & ~F.coalesce(F.col("_prev"), F.lit(False))
    if break_after_seconds is not None:
        t = F.col(time_col).cast("double")
        silence = t - F.lag(t).over(ordered) > F.lit(float(break_after_seconds))
        starts = starts | (F.col("_moving") & F.coalesce(silence, F.lit(False)))
    work = work.withColumn("_new_run", F.when(starts, 1).otherwise(0))
    work = work.withColumn(
        "_run",
        F.sum("_new_run").over(ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
    )

    runs = (
        work.filter(F.col("_moving"))
        .groupBy(*partition_cols, "_run")
        .agg(F.min(time_col).alias("_from"), F.max(time_col).alias("_to"))
    )
    return runs.withColumn(
        "_seconds", F.col("_to").cast("double") - F.col("_from").cast("double")
    ).filter(F.col("_seconds") >= F.lit(config.ground_move_min_seconds))


def movement_window(
    sdf: DataFrame,
    config: "EventConfig",
    *,
    partition_cols: Sequence[str] = ("track_id",),
    time_col: str = "event_time",
) -> DataFrame:
    """First and last instant of sustained ground movement, per track.

    Returns one row per partition with ``moving_start`` and ``moving_stop``,
    or no row where nothing sustained was seen. Every report counts, also in
    flight: this is the rule of ``block_times_on_ground_only = False``.
    """
    runs = _movement_runs(sdf, config, partition_cols=partition_cols, time_col=time_col)
    return runs.groupBy(*partition_cols).agg(
        F.min("_from").alias("moving_start"), F.max("_to").alias("moving_stop")
    )


def block_times(
    movements: DataFrame,
    airport_events: DataFrame,
) -> Optional[DataFrame]:
    """Off-block and on-block, anchored on the parking-position events.

    Off-block is the start of sustained movement, but only for a track that was
    seen leaving a stand -- without that anchor the first movement of a track
    picked up mid-taxi would be reported as its off-block, which is not the
    same event and would look like a very short taxi rather than a miss.
    """
    exits = (
        airport_events.filter(F.col("type") == "exit-parking_position")
        .groupBy("track_id")
        .agg(F.min("event_time").alias("stand_exit"))
    )
    entries = (
        airport_events.filter(F.col("type") == "entry-parking_position")
        .groupBy("track_id")
        .agg(F.max("event_time").alias("stand_entry"))
    )

    out = movements.join(exits, "track_id", "left").join(entries, "track_id", "left")
    return out.select(
        "track_id",
        F.when(F.col("stand_exit").isNotNull(), F.col("moving_start")).alias("aobt"),
        F.when(F.col("stand_entry").isNotNull(), F.col("moving_stop")).alias("aibt"),
        F.col("stand_exit"),
        F.col("stand_entry"),
    ).filter(F.col("aobt").isNotNull() | F.col("aibt").isNotNull())


#: Take-off and landing times that bound the ground parts of a track, under
#: either naming (with the merge, the runway milestones are ``ATOT``/``ALDT``).
TAKE_OFF_TYPES = ("ATOT", "airborne")
LANDING_TYPES = ("ALDT", "touchdown")
_STAND_EXIT, _STAND_ENTRY = "exit-parking_position", "entry-parking_position"


def on_or_near_ground(sdf: DataFrame, config: "EventConfig") -> Column:
    """Whether a report may be on the ground: the test of the airport layout events.

    True when the report is flagged on the ground, when its altitude is
    unknown (a surface report carries none), or when it is at most
    ``airport_max_height_agl_ft`` (2,000 ft) above the lower of the flight's
    two airports. Elevations that are not on the frame count as 0 ft.
    """
    flagged = (
        F.coalesce(F.col("on_ground"), F.lit(False)) if "on_ground" in sdf.columns else F.lit(False)
    )
    if "baro_altitude_c" not in sdf.columns:
        return F.lit(True)
    alt_ft = F.col("baro_altitude_c") * F.lit(3.28084)
    elevations = [
        F.coalesce(F.col(c), F.lit(0.0)) if c in sdf.columns else F.lit(0.0)
        for c in ("elev_adep_ft", "elev_ades_ft")
    ]
    height_ft = F.least(*[alt_ft - e for e in elevations])
    low = height_ft <= F.lit(float(config.airport_max_height_agl_ft))
    return flagged | F.col("baro_altitude_c").isNull() | low


def block_events(
    sdf: DataFrame,
    airport_events: DataFrame,
    runway_events: Optional[DataFrame],
    config: "EventConfig",
) -> DataFrame:
    """Off-block and in-block times, each from the ground part of its own leg.

    The rule of ``block_times_on_ground_only``. A track can hold a departure,
    an arrival, both, or an arrival followed by a departure. Its take-off
    times (:data:`TAKE_OFF_TYPES`) and landing times (:data:`LANDING_TYPES`)
    cut it into parts:

    * from the start of the track to a take-off: an off-block time is the
      start of the first sustained movement in that part;
    * from a landing to the end of the track: an in-block time is the end of
      the last sustained movement in that part;
    * from a landing to a take-off (the aircraft landed, stood, and left
      again): the longest stop between two movements in that part is the
      stand stop; the in-block time is the end of the movement before it, the
      off-block time the start of the movement after it.

    A track with no take-off time gets its off-block time from the ground
    part at its start (up to its first report in flight); one with no
    landing time gets its in-block time from the ground part at its end.

    Movement is measured on reports that may be on the ground
    (:func:`on_or_near_ground`), so flight is never movement on the ground,
    and a silence longer than ``airport_trace_gap_seconds`` ends a movement.
    Each time also needs a stand exit (off-block) or a stand entry (in-block)
    in the same part; the event names that stand and its airport.

    Returns:
        ``track_id``, ``block`` (``"off"`` or ``"on"``), ``event_time``,
        ``stand_time`` (time of the stand exit or entry), ``stand`` (its
        reference in the airport layout) and ``stand_airport``.
    """
    # The four inputs below are small (a few rows per track) and each is read
    # several times. They are computed once and kept (``localCheckpoint``), so
    # that the plan of the reports and of the other event families is not
    # repeated in every part of this one; repeated, it is large enough to
    # exhaust the memory of the Spark driver before the job starts.
    ground = on_or_near_ground(sdf, config)
    reports = sdf.withColumn("_ground", ground)
    runs = _movement_runs(
        reports,
        config,
        counts=F.col("_ground"),
        break_after_seconds=float(config.airport_trace_gap_seconds),
    ).select("track_id", "_from", "_to")
    runs = runs.localCheckpoint()

    span = reports.groupBy("track_id").agg(
        F.min("event_time").alias("_first"),
        F.max("event_time").alias("_last"),
        F.min(F.when(~F.col("_ground"), F.col("event_time"))).alias("_first_air"),
        F.max(F.when(~F.col("_ground"), F.col("event_time"))).alias("_last_air"),
    )
    span = span.localCheckpoint()

    # Take-off ("dep") and landing ("arr") times, then the parts between them.
    if runway_events is None:
        bounds = span.select("track_id").limit(0)
        bounds = bounds.select(
            "track_id",
            F.lit(None).cast("timestamp").alias("_t"),
            F.lit(None).cast("string").alias("_kind"),
        )
    else:
        bounds = (
            runway_events.filter(
                F.col("type").isin(*TAKE_OFF_TYPES, *LANDING_TYPES)
                & F.col("event_time").isNotNull()
            )
            .select(
                "track_id",
                F.col("event_time").alias("_t"),
                F.when(F.col("type").isin(*TAKE_OFF_TYPES), F.lit("dep"))
                .otherwise(F.lit("arr"))
                .alias("_kind"),
            )
            .distinct()
        )
    bounds = bounds.localCheckpoint()
    kinds = bounds.groupBy("track_id").agg(
        F.max(F.col("_kind") == "dep").alias("_has_dep"),
        F.max(F.col("_kind") == "arr").alias("_has_arr"),
        F.min(F.when(F.col("_kind") == "dep", F.col("_t"))).alias("_first_dep"),
        F.min(F.when(F.col("_kind") == "arr", F.col("_t"))).alias("_first_arr"),
        F.max(F.when(F.col("_kind") == "dep", F.col("_t"))).alias("_last_dep"),
        F.max(F.when(F.col("_kind") == "arr", F.col("_t"))).alias("_last_arr"),
    )
    span = span.join(kinds, "track_id", "left")

    # The parts between consecutive boundaries, the start and end of the track
    # included. At equal times the start sorts first and the end last.
    marks = (
        bounds.join(span.select("track_id"), "track_id", "inner")
        .withColumn("_order", F.lit(1))
        .unionByName(
            span.select(
                "track_id",
                F.col("_first").alias("_t"),
                F.lit("start").alias("_kind"),
                F.lit(0).alias("_order"),
            )
        )
        .unionByName(
            span.select(
                "track_id",
                F.col("_last").alias("_t"),
                F.lit("end").alias("_kind"),
                F.lit(2).alias("_order"),
            )
        )
    )
    w = Window.partitionBy("track_id").orderBy("_t", "_order")
    parts = (
        marks.withColumn("_hi", F.lead("_t").over(w))
        .withColumn("_hi_kind", F.lead("_kind").over(w))
        .select(
            "track_id",
            F.col("_t").alias("_lo"),
            F.col("_kind").alias("_lo_kind"),
            "_hi",
            "_hi_kind",
        )
        .filter(F.col("_hi").isNotNull())
    )
    single_off = parts.filter((F.col("_lo_kind") == "start") & (F.col("_hi_kind") == "dep"))
    single_on = parts.filter((F.col("_lo_kind") == "arr") & (F.col("_hi_kind") == "end"))
    split = parts.filter((F.col("_lo_kind") == "arr") & (F.col("_hi_kind") == "dep"))

    # Without a take-off (landing) time: the ground part at the start (end).
    no_dep = span.filter(~F.coalesce(F.col("_has_dep"), F.lit(False))).select(
        "track_id",
        F.col("_first").alias("_lo"),
        F.least(
            F.coalesce(F.col("_first_air"), F.col("_last")),
            F.coalesce(F.col("_first_arr"), F.col("_last")),
        ).alias("_hi"),
    )
    no_arr = span.filter(~F.coalesce(F.col("_has_arr"), F.lit(False))).select(
        "track_id",
        F.greatest(
            F.coalesce(F.col("_last_air"), F.col("_first")),
            F.coalesce(F.col("_last_dep"), F.col("_first")),
        ).alias("_lo"),
        F.col("_last").alias("_hi"),
    )
    off_parts = single_off.select("track_id", "_lo", "_hi").unionByName(no_dep)
    on_parts = single_on.select("track_id", "_lo", "_hi").unionByName(no_arr)

    off = (
        off_parts.join(runs, "track_id", "inner")
        .filter((F.col("_from") >= F.col("_lo")) & (F.col("_from") <= F.col("_hi")))
        .groupBy("track_id", "_lo", "_hi")
        .agg(F.min("_from").alias("event_time"))
        .select(
            "track_id",
            F.lit("off").alias("block"),
            "event_time",
            F.col("_lo").alias("_s_lo"),
            F.col("_hi").alias("_s_hi"),
        )
    )
    on = (
        on_parts.join(runs, "track_id", "inner")
        .filter((F.col("_to") >= F.col("_lo")) & (F.col("_to") <= F.col("_hi")))
        .groupBy("track_id", "_lo", "_hi")
        .agg(F.max("_to").alias("event_time"))
        .select(
            "track_id",
            F.lit("on").alias("block"),
            "event_time",
            F.col("_lo").alias("_s_lo"),
            F.col("_hi").alias("_s_hi"),
        )
    )

    # A landing followed by a take-off: split at the longest stop.
    in_split = split.join(runs, "track_id", "inner").filter(
        (F.col("_to") >= F.col("_lo")) & (F.col("_from") <= F.col("_hi"))
    )
    by_start = Window.partitionBy("track_id", "_lo").orderBy("_from")
    in_split = in_split.withColumn("_next_from", F.lead("_from").over(by_start)).filter(
        F.col("_next_from").isNotNull()
    )
    in_split = in_split.withColumn(
        "_stop_s", F.col("_next_from").cast("double") - F.col("_to").cast("double")
    )
    longest = Window.partitionBy("track_id", "_lo").orderBy(F.col("_stop_s").desc(), "_from")
    stop = in_split.withColumn("_rank", F.row_number().over(longest)).filter(F.col("_rank") == 1)
    split_on = stop.select(
        "track_id",
        F.lit("on").alias("block"),
        F.col("_to").alias("event_time"),
        F.col("_lo").alias("_s_lo"),
        F.col("_next_from").alias("_s_hi"),
    )
    split_off = stop.select(
        "track_id",
        F.lit("off").alias("block"),
        F.col("_next_from").alias("event_time"),
        F.col("_to").alias("_s_lo"),
        F.col("_hi").alias("_s_hi"),
    )
    candidates = off.unionByName(on).unionByName(split_on).unionByName(split_off)

    # The stand of each time, from the same part of the track: for an
    # off-block time the first stand exit at or after it (else the last one
    # before it), for an in-block time the last stand entry at or before it
    # (else the first one after it).
    info = F.col("info") if "info" in airport_events.columns else F.lit(None).cast("string")
    stands = airport_events.filter(F.col("type").isin(_STAND_EXIT, _STAND_ENTRY)).select(
        F.col("track_id").alias("_s_track"),
        F.when(F.col("type") == _STAND_EXIT, F.lit("off")).otherwise(F.lit("on")).alias("_s_block"),
        F.col("event_time").alias("stand_time"),
        F.get_json_object(info, "$.osm_ref").alias("stand"),
        F.get_json_object(info, "$.osm_airport").alias("stand_airport"),
    )
    stands = stands.localCheckpoint()
    matched = candidates.join(
        stands,
        (F.col("track_id") == F.col("_s_track"))
        & (F.col("block") == F.col("_s_block"))
        & (F.col("stand_time") >= F.col("_s_lo"))
        & (F.col("stand_time") <= F.col("_s_hi")),
        "inner",
    )
    st = F.col("stand_time").cast("double")
    ev = F.col("event_time").cast("double")
    off_side = F.col("block") == "off"
    preferred = F.when(off_side, st >= ev).otherwise(st <= ev)
    pick = Window.partitionBy("track_id", "block", "event_time").orderBy(
        preferred.desc(), F.abs(st - ev).asc(), "stand_time"
    )
    return (
        matched.withColumn("_rank", F.row_number().over(pick))
        .filter(F.col("_rank") == 1)
        .select("track_id", "block", "event_time", "stand_time", "stand", "stand_airport")
    )
