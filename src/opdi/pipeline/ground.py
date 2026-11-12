"""Ground movement milestones: the off-block time (``AOBT``) and the in-block time (``AIBT``).

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

from typing import TYPE_CHECKING, List, Optional, Sequence

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
    break_after_seconds: Optional[float] = None,
) -> DataFrame:
    """Each run of sustained movement: ``_from`` and ``_to`` per run.

    Also, for the report where the run starts and the one where it ends:
    the lower of its reported and position-derived speed in knots
    (``_from_kt``, ``_to_kt``) and whether it is flagged on the ground
    (``_from_flagged``, ``_to_flagged``).

    ``break_after_seconds`` starts a new run after a silence longer than that,
    so two movements with nothing received between them are not one run.
    """
    ordered = Window.partitionBy(*partition_cols).orderBy(time_col)

    reported_kt = F.col("velocity") * KT_PER_MPS
    gs_kt = reported_kt
    has_position = "lat" in sdf.columns and "lon" in sdf.columns
    derived_kt = _derived_groundspeed_kt(ordered, time_col) if has_position else None
    if config.ground_speed_derive_from_position and has_position:
        # Fill the gaps, do not replace: the reported speed is used wherever
        # `velocity` exists, the derived one only where it is missing (see
        # EventConfig.ground_speed_derive_from_position). Applied only when the
        # lat/lon columns are present: a caller without them (some tests, any
        # minimal projection) gets the reported speed alone rather than an
        # unresolved-column analysis error.
        gs_kt = F.coalesce(gs_kt, derived_kt)
    moving = gs_kt > F.lit(config.ground_speed_threshold_kt)
    work = sdf.withColumn("_moving", moving)
    # The speed that says whether the aircraft stood or rolled slowly where a
    # movement starts or ends: the lower of the reported and the derived
    # speed (``least`` skips an unknown one), because the reported value is
    # sometimes stale on the ground.
    work = work.withColumn(
        "_slow_kt", F.least(reported_kt, derived_kt) if derived_kt is not None else reported_kt
    )
    flagged = (
        F.coalesce(F.col("on_ground"), F.lit(False)) if "on_ground" in sdf.columns else F.lit(False)
    )
    work = work.withColumn("_flagged", flagged)

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

    t_run = F.col(time_col)
    runs = (
        work.filter(F.col("_moving"))
        .groupBy(*partition_cols, "_run")
        .agg(
            F.min(time_col).alias("_from"),
            F.max(time_col).alias("_to"),
            # The report where the run starts and the one where it ends.
            F.min_by(F.col("_slow_kt"), t_run).alias("_from_kt"),
            F.max_by(F.col("_slow_kt"), t_run).alias("_to_kt"),
            F.min_by(F.col("_flagged"), t_run).alias("_from_flagged"),
            F.max_by(F.col("_flagged"), t_run).alias("_to_flagged"),
        )
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


def _report_on_ground(sdf: DataFrame, config: "EventConfig") -> Column:
    """True, False or NULL: whether a report may be on the ground.

    True when the report is flagged on the ground, has no altitude (a
    surface report carries none), or is at most
    ``airport_max_height_agl_ft`` (2,000 ft) above the airport of its leg;
    False when it has a position and an altitude higher than that; NULL
    (unknown) for a report without a position that has an altitude and is
    not flagged on the ground. Some receivers repeat a stale altitude in
    such reports, at cruise level after the aircraft parked, or near the
    ground after it took off, so these reports count neither as ground nor
    as flight.

    The airport of the leg is the one of the flight's two airports nearer to
    the report (``dist_adep_nm``, ``dist_ades_nm``): for a report on or near
    the ground, the departure airport on the departure side and the
    destination on the arrival side. When neither distance is known, the
    lower of the two airports is used. Elevations that are not on the frame
    count as 0 ft.
    """
    flagged = (
        F.coalesce(F.col("on_ground"), F.lit(False)) if "on_ground" in sdf.columns else F.lit(False)
    )
    if "baro_altitude_c" not in sdf.columns:
        return F.lit(True)
    alt_ft = F.col("baro_altitude_c") * F.lit(3.28084)

    def column_or_null(c):
        return F.col(c) if c in sdf.columns else F.lit(None).cast("double")

    elev_dep = F.coalesce(column_or_null("elev_adep_ft"), F.lit(0.0))
    elev_arr = F.coalesce(column_or_null("elev_ades_ft"), F.lit(0.0))
    d_dep, d_arr = column_or_null("dist_adep_nm"), column_or_null("dist_ades_nm")
    elevation = (
        F.when(
            d_dep.isNotNull() & d_arr.isNotNull(),
            F.when(d_dep <= d_arr, elev_dep).otherwise(elev_arr),
        )
        .when(d_dep.isNotNull(), elev_dep)
        .when(d_arr.isNotNull(), elev_arr)
        .otherwise(F.least(elev_dep, elev_arr))
    )
    low = (alt_ft - elevation) <= F.lit(float(config.airport_max_height_agl_ft))
    has_position = F.col("lat").isNotNull() if "lat" in sdf.columns else F.lit(True)
    return (
        F.when(flagged | F.col("baro_altitude_c").isNull(), F.lit(True))
        .when(has_position, low)
        .otherwise(F.lit(None).cast("boolean"))
    )


def on_or_near_ground(sdf: DataFrame, config: "EventConfig") -> Column:
    """Whether a report may be on the ground; never NULL.

    The True of :func:`_report_on_ground`; an unknown report (no position, an
    altitude, not flagged) gives False.
    """
    return F.coalesce(_report_on_ground(sdf, config), F.lit(False))


def _kept(df: DataFrame, checkpoints: Optional[List[DataFrame]]) -> DataFrame:
    """``df.localCheckpoint()``, recorded in ``checkpoints`` when given."""
    kept = df.localCheckpoint()
    if checkpoints is not None:
        checkpoints.append(kept)
    return kept


def block_events(
    sdf: DataFrame,
    airport_events: DataFrame,
    runway_events: Optional[DataFrame],
    config: "EventConfig",
    checkpoints: Optional[List[DataFrame]] = None,
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
      again): when a stand passage (stand entry or exit) lies in that part,
      the longest stop between two movements in it is the stand stop; the
      in-block time is the end of the movement before it, the off-block time
      the start of the movement after it. Without a stand passage in the
      part, the stop is a wait (at a holding point, between two circuits)
      and gives no time; a part with a single movement has no stop and gives
      no time either.

    Where the track starts with a landing, or has no take-off time, the
    off-block time comes from the ground part at its start (up to its first
    report in flight); where it ends with a take-off, or has no landing time,
    the in-block time comes from the ground part at its end (after its last
    report in flight). A take-off or landing that was not found then does not
    lose the block time next to it.

    **A movement from or to a standstill.** An off-block time is the start
    of a movement from a standstill: the report where the movement starts is
    at most ``block_time_max_start_speed_kt`` (30 kt) fast, or is flagged on
    the ground. In the same way the report where the last movement ends is
    slow or flagged on the ground for an in-block time. The speed is the
    lower of the reported and the position-derived ground speed. A movement
    that starts at the track's first report while faster than that (or ends
    at its last report) gives no time, flagged or not: the aircraft was
    already moving when first received (on the take-off roll, in the climb,
    on final) or still moving when last received, so the moment it left or
    reached the stand was not seen. Only the first movement of a part is
    read for an off-block time and the last for an in-block time; when that
    movement does not start (end) at a standstill, the part gives no time.

    Movement is measured on reports that may be on the ground
    (:func:`_report_on_ground`: flagged on the ground, without altitude, or
    at most 2,000 ft above the airport of the leg), so flight is never
    movement on the ground, and a silence longer than
    ``airport_trace_gap_seconds`` ends a movement. Reports in flight are
    left out of the movement rather than counted as standing still, and the
    track counts as in flight only where reports in flight last at least
    ``ground_move_min_seconds``, so a garbled altitude does not end a ground
    part. A report without a position that carries an altitude and is not
    flagged on the ground counts neither as ground nor as flight.

    Each time from the start or the end of a track needs a stand passage
    somewhere in the track: an exit (off-block) or an entry (in-block). It
    need not lie in the same part, because the stand at the airport of a
    single-leg track is often not received. When the stand passage lies in
    the same part, the event names the stand and its airport; otherwise it
    names none.

    Args:
        sdf: the position reports, with the airport elevations and distances
            of :func:`_report_on_ground` when known.
        airport_events: the airport layout events (stand entries and exits).
        runway_events: the take-off and landing times (``track_id``,
            ``type``, ``event_time``, optionally ``info``), or None.
        config: :class:`~opdi.config.EventConfig`.
        checkpoints: when given, the frames this function keeps with
            ``localCheckpoint`` are added to it, so that the caller can free
            them (:func:`opdi.utils.spark_helpers.release_local_checkpoint`)
            once the result has been written.

    Returns:
        ``track_id``, ``block`` (``"off"`` or ``"on"``), ``event_time``,
        ``stand_time`` (time of the stand exit or entry), ``stand`` (its
        reference in the airport layout), ``stand_airport``, and
        ``bound_time`` and ``bound_method``: the take-off after an off-block
        time, or the landing before an in-block time, of the same leg, with
        the detector that found it (``info.method`` of the runway event).
        Both are NULL where the part has no such time (the ground part at
        the start or end of the track). The plausibility checks compare the
        block time with this time.
    """
    # The four inputs below are small (a few rows per track) and each is read
    # several times. They are computed once and kept (``localCheckpoint``), so
    # that the plan of the reports and of the other event families is not
    # repeated in every part of this one; repeated, it is large enough to
    # exhaust the memory of the Spark driver before the job starts. The kept
    # data live on the executors that computed them: if one is lost, the step
    # fails and is run again (it never gives wrong data).
    reports = sdf.withColumn("_ground", _report_on_ground(sdf, config))
    # Reports in flight are left out of the movement rather than counted as
    # standing still, so a garbled altitude on the ground (a report or two at
    # 16,000 ft between two surface reports) does not cut a push-back in two.
    # A real flight leaves a silence longer than airport_trace_gap_seconds,
    # which ends the movement.
    runs = _movement_runs(
        reports.filter(F.coalesce(F.col("_ground"), F.lit(False))),
        config,
        break_after_seconds=float(config.airport_trace_gap_seconds),
    ).select("track_id", "_from", "_to", "_from_kt", "_to_kt", "_from_flagged", "_to_flagged")
    runs = _kept(runs, checkpoints)

    # The track is in flight where reports in flight follow each other for at
    # least ground_move_min_seconds (30 s); a shorter run of them is a
    # garbled altitude, not a flight.
    # A report whose state is unknown takes no part: it neither starts nor
    # ends a stretch in flight.
    ordered = Window.partitionBy("track_id").orderBy("event_time")
    air = reports.filter(F.col("_ground").isNotNull()).withColumn("_air", ~F.col("_ground"))
    air = air.withColumn(
        "_change",
        F.when(
            F.col("_air") != F.coalesce(F.lag("_air").over(ordered), ~F.col("_air")), 1
        ).otherwise(0),
    ).withColumn(
        "_stretch",
        F.sum("_change").over(ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
    )
    stretches = (
        air.filter(F.col("_air"))
        .groupBy("track_id", "_stretch")
        .agg(F.min("event_time").alias("_a_from"), F.max("event_time").alias("_a_to"))
        .filter(
            F.col("_a_to").cast("double") - F.col("_a_from").cast("double")
            >= F.lit(float(config.ground_move_min_seconds))
        )
        .groupBy(F.col("track_id").alias("_a_track"))
        .agg(F.min("_a_from").alias("_first_air"), F.max("_a_to").alias("_last_air"))
    )
    span = reports.groupBy("track_id").agg(
        F.min("event_time").alias("_first"),
        F.max("event_time").alias("_last"),
    )
    span = span.join(stretches, span["track_id"] == stretches["_a_track"], "left").drop("_a_track")
    span = _kept(span, checkpoints)

    # Take-off ("dep") and landing ("arr") times, then the parts between them.
    if runway_events is None:
        bounds = span.select("track_id").limit(0)
        bounds = bounds.select(
            "track_id",
            F.lit(None).cast("timestamp").alias("_t"),
            F.lit(None).cast("string").alias("_kind"),
            F.lit(None).cast("string").alias("_method"),
        )
    else:
        # The detector of each time (``info.method``: ``acdm`` for the runway
        # milestones, ``legacy`` for the runway window). Where both found the
        # same instant, the alphabetically first is kept.
        method = (
            F.get_json_object(F.col("info"), "$.method")
            if "info" in runway_events.columns
            else F.lit(None).cast("string")
        )
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
                method.alias("_method"),
            )
            .groupBy("track_id", "_t", "_kind")
            .agg(F.min("_method").alias("_method"))
        )
    bounds = _kept(bounds, checkpoints)
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
    # included. At equal times the start sorts first and the end last, and a
    # landing before a take-off (``arr`` before ``dep``), so the order does not
    # depend on how Spark split the data.
    marks = (
        bounds.join(span.select("track_id"), "track_id", "inner")
        .withColumn("_order", F.lit(1))
        .unionByName(
            span.select(
                "track_id",
                F.col("_first").alias("_t"),
                F.lit("start").alias("_kind"),
                F.lit(None).cast("string").alias("_method"),
                F.lit(0).alias("_order"),
            )
        )
        .unionByName(
            span.select(
                "track_id",
                F.col("_last").alias("_t"),
                F.lit("end").alias("_kind"),
                F.lit(None).cast("string").alias("_method"),
                F.lit(2).alias("_order"),
            )
        )
    )
    w = Window.partitionBy("track_id").orderBy("_t", "_order", "_kind")
    parts = (
        marks.withColumn("_hi", F.lead("_t").over(w))
        .withColumn("_hi_kind", F.lead("_kind").over(w))
        .withColumn("_hi_method", F.lead("_method").over(w))
        .select(
            "track_id",
            F.col("_t").alias("_lo"),
            F.col("_kind").alias("_lo_kind"),
            F.col("_method").alias("_lo_method"),
            "_hi",
            "_hi_kind",
            "_hi_method",
        )
        .filter(F.col("_hi").isNotNull())
    )
    single_off = parts.filter((F.col("_lo_kind") == "start") & (F.col("_hi_kind") == "dep"))
    single_on = parts.filter((F.col("_lo_kind") == "arr") & (F.col("_hi_kind") == "end"))
    split = parts.filter((F.col("_lo_kind") == "arr") & (F.col("_hi_kind") == "dep"))

    # Before the first take-off when a landing (or nothing) comes first, and
    # after the last landing when a take-off (or nothing) comes last: the
    # ground part at the start (end) of the track, up to its first (from its
    # last) report in flight. This is where a take-off or landing time that
    # was not found would have been.
    starts_without_take_off = F.col("_first_dep").isNull() | (
        F.col("_first_arr") < F.col("_first_dep")
    )
    ends_without_landing = F.col("_last_arr").isNull() | (F.col("_last_dep") > F.col("_last_arr"))
    no_bound = [
        F.lit(None).cast("timestamp").alias("_bound"),
        F.lit(None).cast("string").alias("_bound_method"),
    ]
    no_dep = span.filter(F.coalesce(starts_without_take_off, F.lit(True))).select(
        "track_id",
        F.col("_first").alias("_lo"),
        F.least(
            F.coalesce(F.col("_first_air"), F.col("_last")),
            F.coalesce(F.col("_first_arr"), F.col("_last")),
        ).alias("_hi"),
        *no_bound,
    )
    no_arr = span.filter(F.coalesce(ends_without_landing, F.lit(True))).select(
        "track_id",
        F.greatest(
            F.coalesce(F.col("_last_air"), F.col("_first")),
            F.coalesce(F.col("_last_dep"), F.col("_first")),
        ).alias("_lo"),
        F.col("_last").alias("_hi"),
        *no_bound,
    )
    # The take-off that ends a part, or the landing that starts it, bounds
    # the block time found in it: the off-block time lies before that
    # take-off, the in-block time after that landing.
    off_parts = single_off.select(
        "track_id",
        "_lo",
        "_hi",
        F.col("_hi").alias("_bound"),
        F.col("_hi_method").alias("_bound_method"),
    ).unionByName(no_dep)
    on_parts = single_on.select(
        "track_id",
        "_lo",
        "_hi",
        F.col("_lo").alias("_bound"),
        F.col("_lo_method").alias("_bound_method"),
    ).unionByName(no_arr)

    # A movement starts from a standstill when the report where it starts is
    # slow, or is flagged on the ground and is not the track's first report;
    # it ends in one when the report where it ends is slow, or is flagged on
    # the ground and is not the track's last report. A movement that starts
    # at the first report while fast (ends at the last while fast) was
    # already under way when the track was first received (still under way
    # when it was last received).
    slow = F.lit(float(config.block_time_max_start_speed_kt))
    first_last = span.select(
        F.col("track_id").alias("_fl_track"),
        F.col("_first").alias("_t_first"),
        F.col("_last").alias("_t_last"),
    )
    runs = runs.join(first_last, runs["track_id"] == first_last["_fl_track"], "inner").drop(
        "_fl_track"
    )
    runs = runs.withColumn(
        "_starts_still",
        F.coalesce(
            (F.col("_from_kt") <= slow)
            | (F.col("_from_flagged") & (F.col("_from") > F.col("_t_first"))),
            F.lit(False),
        ),
    ).withColumn(
        "_ends_still",
        F.coalesce(
            (F.col("_to_kt") <= slow) | (F.col("_to_flagged") & (F.col("_to") < F.col("_t_last"))),
            F.lit(False),
        ),
    )

    # The first movement of each part gives its off-block time and the last
    # its in-block time, each only when it starts (ends) at a standstill; a
    # later (earlier) movement is not taken instead, as it would time a wait
    # on the way rather than the departure from the stand.
    off = (
        off_parts.join(runs, "track_id", "inner")
        .filter((F.col("_from") >= F.col("_lo")) & (F.col("_from") <= F.col("_hi")))
        .groupBy("track_id", "_lo", "_hi", "_bound", "_bound_method")
        .agg(F.min_by(F.struct("_from", "_starts_still"), F.col("_from")).alias("_first_run"))
        .filter(F.col("_first_run._starts_still"))
        .withColumn("event_time", F.col("_first_run._from"))
        .select(
            "track_id",
            F.lit("off").alias("block"),
            "event_time",
            F.col("_lo").alias("_s_lo"),
            F.col("_hi").alias("_s_hi"),
            "_bound",
            "_bound_method",
        )
    )
    on = (
        on_parts.join(runs, "track_id", "inner")
        .filter((F.col("_to") >= F.col("_lo")) & (F.col("_to") <= F.col("_hi")))
        .groupBy("track_id", "_lo", "_hi", "_bound", "_bound_method")
        .agg(F.max_by(F.struct("_to", "_ends_still"), F.col("_to")).alias("_last_run"))
        .filter(F.col("_last_run._ends_still"))
        .withColumn("event_time", F.col("_last_run._to"))
        .select(
            "track_id",
            F.lit("on").alias("block"),
            "event_time",
            F.col("_lo").alias("_s_lo"),
            F.col("_hi").alias("_s_hi"),
            "_bound",
            "_bound_method",
        )
    )

    # A landing followed by a take-off: split at the longest stop, but only
    # when a stand passage lies between the landing and the take-off;
    # otherwise the stop is a wait (at a holding point, between two
    # circuits), not a stand stop. A part with a single movement has no stop
    # and gives no time.
    passages = airport_events.filter(F.col("type").isin(_STAND_EXIT, _STAND_ENTRY)).select(
        F.col("track_id").alias("_p_track"), F.col("event_time").alias("_p_time")
    )
    split = split.join(
        passages,
        (F.col("track_id") == F.col("_p_track"))
        & (F.col("_p_time") >= F.col("_lo"))
        & (F.col("_p_time") <= F.col("_hi")),
        "left_semi",
    )
    in_split = split.join(runs, "track_id", "inner").filter(
        (F.col("_to") >= F.col("_lo")) & (F.col("_from") <= F.col("_hi"))
    )
    by_start = Window.partitionBy("track_id", "_lo").orderBy("_from")
    in_split = (
        in_split.withColumn("_next_from", F.lead("_from").over(by_start))
        .withColumn("_next_starts_still", F.lead("_starts_still").over(by_start))
        .filter(F.col("_next_from").isNotNull())
    )
    in_split = in_split.withColumn(
        "_stop_s", F.col("_next_from").cast("double") - F.col("_to").cast("double")
    )
    longest = Window.partitionBy("track_id", "_lo").orderBy(F.col("_stop_s").desc(), "_from")
    stop = in_split.withColumn("_rank", F.row_number().over(longest)).filter(F.col("_rank") == 1)
    split_on = stop.filter(F.col("_ends_still")).select(
        "track_id",
        F.lit("on").alias("block"),
        F.col("_to").alias("event_time"),
        F.col("_lo").alias("_s_lo"),
        F.col("_next_from").alias("_s_hi"),
        F.col("_lo").alias("_bound"),
        F.col("_lo_method").alias("_bound_method"),
    )
    split_off = stop.filter(F.coalesce(F.col("_next_starts_still"), F.lit(False))).select(
        "track_id",
        F.lit("off").alias("block"),
        F.col("_next_from").alias("event_time"),
        F.col("_to").alias("_s_lo"),
        F.col("_hi").alias("_s_hi"),
        F.col("_hi").alias("_bound"),
        F.col("_hi_method").alias("_bound_method"),
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
    stands = _kept(stands, checkpoints)
    matched = candidates.join(
        stands,
        (F.col("track_id") == F.col("_s_track"))
        & (F.col("block") == F.col("_s_block"))
        & (F.col("stand_time") >= F.col("_s_lo"))
        & (F.col("stand_time") <= F.col("_s_hi")),
        "left",
    )
    # A part with no stand passage of its own still gives its time when the
    # track passed a stand elsewhere (the stand at the start of a departure
    # is often not received); the time then names no stand.
    anywhere = stands.select(
        F.col("_s_track").alias("_a_track"), F.col("_s_block").alias("_a_block")
    ).distinct()
    matched = matched.join(
        anywhere,
        (F.col("track_id") == F.col("_a_track")) & (F.col("block") == F.col("_a_block")),
        "inner",
    ).drop("_a_track", "_a_block")
    st = F.col("stand_time").cast("double")
    ev = F.col("event_time").cast("double")
    off_side = F.col("block") == "off"
    preferred = F.when(off_side, st >= ev).otherwise(st <= ev)
    pick = Window.partitionBy("track_id", "block", "event_time").orderBy(
        preferred.desc_nulls_last(), F.abs(st - ev).asc_nulls_last(), "stand_time"
    )
    return (
        matched.withColumn("_rank", F.row_number().over(pick))
        .filter(F.col("_rank") == 1)
        .select(
            "track_id",
            "block",
            "event_time",
            "stand_time",
            "stand",
            "stand_airport",
            F.col("_bound").alias("bound_time"),
            F.col("_bound_method").alias("bound_method"),
        )
    )
