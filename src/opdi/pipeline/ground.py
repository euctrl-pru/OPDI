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
#: Feet in a nautical mile (1,852 m at 3.28084 ft per metre).
FT_PER_NM = 1852.0 * 3.28084


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


def _derived_groundspeed_between_positions_kt(
    ordered: Window, time_col: str, max_gap_seconds: float
) -> Column:
    """Ground speed in knots derived between reports that have a position.

    The rule of ``ground_movement_through_reports_without_speed``. For a
    report with a position: the distance from the last earlier report with a
    position over the time between the two, when that time is at most
    ``max_gap_seconds``; then the median of that value and of the values of
    the reports just before and after (a report without one is skipped). A
    report without a position gets no derived speed. On the airport surface
    reports without position are common, and a speed taken only from the
    report just before would be unknown after each of them.
    """
    has_position = F.col("lat").isNotNull() & F.col("lon").isNotNull()
    t = F.col(time_col).cast("double")
    before = ordered.rowsBetween(Window.unboundedPreceding, -1)
    prev_lat = F.last(F.when(has_position, F.col("lat")), ignorenulls=True).over(before)
    prev_lon = F.last(F.when(has_position, F.col("lon")), ignorenulls=True).over(before)
    prev_t = F.last(F.when(has_position, t), ignorenulls=True).over(before)
    dt_s = t - prev_t
    dist_nm = haversine_nm(prev_lat, prev_lon, F.col("lat"), F.col("lon"))
    raw_kt = F.when(
        has_position & (dt_s > 0) & (dt_s <= F.lit(float(max_gap_seconds))),
        dist_nm / (dt_s / 3600.0),
    )
    median_kt = F.percentile_approx(raw_kt, 0.5).over(ordered.rowsBetween(-1, 1))
    return F.when(has_position, median_kt)


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
    bridge = config.ground_movement_through_reports_without_speed
    if not has_position:
        derived_kt = None
    elif bridge:
        derived_kt = _derived_groundspeed_between_positions_kt(
            ordered, time_col, config.ground_speed_max_position_gap_seconds
        )
    else:
        derived_kt = _derived_groundspeed_kt(ordered, time_col)
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
    if bridge:
        # A report whose speed is not known (``_moving`` NULL) neither starts
        # nor ends a run: the state before it is kept. A run is broken by a
        # report showing the aircraft standing, or by more than
        # ``airport_trace_gap_seconds`` (or ``break_after_seconds``) without
        # any known speed.
        before = ordered.rowsBetween(Window.unboundedPreceding, -1)
        t = F.col(time_col).cast("double")
        known_t = F.when(F.col("_moving").isNotNull(), t)
        work = work.withColumn(
            "_prev", F.last(F.col("_moving"), ignorenulls=True).over(before)
        ).withColumn("_prev_known_t", F.last(known_t, ignorenulls=True).over(before))
        gap = float(
            break_after_seconds
            if break_after_seconds is not None
            else config.airport_trace_gap_seconds
        )
        silence = t - F.col("_prev_known_t") > F.lit(gap)
        starts = F.col("_moving") & (
            ~F.coalesce(F.col("_prev"), F.lit(False)) | F.coalesce(silence, F.lit(False))
        )
        work = work.withColumn("_new_run", F.when(starts, 1).otherwise(0))
        work = work.withColumn(
            "_run",
            F.sum("_new_run").over(
                ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
        return _sustained_runs_through_unknown(work, config, partition_cols, time_col)
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

    return _sustained_runs(work, config, partition_cols, time_col)


def _sustained_runs(
    work: DataFrame,
    config: "EventConfig",
    partition_cols: Sequence[str],
    time_col: str,
) -> DataFrame:
    """The runs of moving reports (``_run``), each with its start and end.

    A run is kept when it lasts at least ``ground_move_min_seconds``.
    """
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


def _sustained_runs_through_unknown(
    work: DataFrame,
    config: "EventConfig",
    partition_cols: Sequence[str],
    time_col: str,
) -> DataFrame:
    """The runs of ``ground_movement_through_reports_without_speed``.

    A run starts and ends at a report that shows movement (``_from``,
    ``_to``). How long it lasted is measured up to the first report after it
    that shows the aircraft standing, or up to its last report whose speed
    is not known, but at most ``ground_speed_max_position_gap_seconds``
    after ``_to``: the aircraft moved until a report showed it standing, and
    a push-back seen on reports 25 s apart with nothing known after them
    still lasted longer than that. A run is kept when that time is at least
    ``ground_move_min_seconds`` and it holds at least two reports that show
    movement, so a single wrong position followed by reports without one is
    not a movement.
    """
    t = F.col(time_col)
    moving = F.coalesce(F.col("_moving"), F.lit(False))
    standing = F.col("_moving") == F.lit(False)
    at_moving = F.when(moving, F.struct(t.alias("t"), "_slow_kt", "_flagged"))
    runs = (
        work.filter(F.col("_run") > 0)
        .groupBy(*partition_cols, "_run")
        .agg(
            F.min(at_moving).alias("_first"),
            F.max(at_moving).alias("_last"),
            F.sum(F.when(moving, 1).otherwise(0)).alias("_n_moving"),
            F.min(F.when(standing, t)).alias("_first_standing"),
            F.max(t).alias("_last_report"),
        )
        .filter(F.col("_n_moving") >= 1)
    )
    to_s = F.col("_last.t").cast("double")
    span_end = F.least(
        F.coalesce(F.col("_first_standing"), F.col("_last_report")).cast("double"),
        to_s + F.lit(float(config.ground_speed_max_position_gap_seconds)),
    )
    return (
        runs.select(
            *partition_cols,
            "_run",
            F.col("_first.t").alias("_from"),
            F.col("_last.t").alias("_to"),
            F.col("_first._slow_kt").alias("_from_kt"),
            F.col("_last._slow_kt").alias("_to_kt"),
            F.col("_first._flagged").alias("_from_flagged"),
            F.col("_last._flagged").alias("_to_flagged"),
            (span_end - F.col("_first.t").cast("double")).alias("_seconds"),
            "_n_moving",
        )
        .filter(
            (F.col("_seconds") >= F.lit(config.ground_move_min_seconds)) & (F.col("_n_moving") >= 2)
        )
        .drop("_n_moving")
    )


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
#: Seconds within which a stand passage counts as starting at the track's
#: first report (ending at its last), for ``block_time_from_last_stand_stop``.
_EDGE_SECONDS = 5.0
#: Seconds within which a short stand passage counts as reached or left at
#: the block time, for naming the stand (``block_time_stand_at_same_airport``).
_NEAR_SECONDS = 60.0


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
    """``df`` kept (:func:`opdi.utils.spark_helpers.keep_frame`), recorded in ``checkpoints``."""
    from opdi.utils.spark_helpers import keep_frame

    kept = keep_frame(df)
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

    The settings of work package 3 of the 2026-06-14 corrections change these
    rules where they are on (the default):

    * ``block_time_stand_at_same_airport``: the stand named is a passage
      in the same part of at least ``block_time_min_stand_dwell_seconds``
      (30 s); without one, the time is still given but names no stand.
    * ``block_time_from_last_stand_stop``: an off-block time leaves the last
      stand stop of at least 10 minutes before the take-off (else the first
      movement of the part is kept); an in-block time reaches the first
      such stop after the landing, or the stand where the track ends (else
      the last movement of the part is kept; see :func:`_from_stand_stop`).
    * ``block_time_stand_longest_dwell``: elsewhere, the stand named is the
      longest passage of the part.
    * ``block_time_off_the_runway``: a standstill is judged on speed, the
      ground flag only where no speed is known; no time within
      ``block_time_min_gap_to_runway_time_seconds`` (60 s) of its take-off or
      landing, after the line-up or before the runway is vacated.
    * ``block_time_one_part_per_leg``: a track with only a take-off (only a
      landing) has one ground part before (after) it, so each time carries
      the take-off or landing of its leg.
    * ``block_time_needs_flight``: no time on a track that never leaves the
      ground (no take-off or landing time and no stretch in flight).
    * ``layout_ref_missing_as_null``: a stand named "None", "nan" or ""
      names no stand.

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
    # data live on the executors, twice: when one executor is lost, the other
    # copy is read.
    reports = sdf.withColumn("_ground", _report_on_ground(sdf, config))
    # Reports in flight are left out of the movement rather than counted as
    # standing still, so a garbled altitude on the ground (a report or two at
    # 16,000 ft between two surface reports) does not cut a push-back in two.
    # A real flight leaves a silence longer than airport_trace_gap_seconds,
    # which ends the movement.
    on_ground = reports.filter(F.coalesce(F.col("_ground"), F.lit(False)))
    push_back = (
        config.off_block_at_start_of_push_back and "lat" in sdf.columns and "lon" in sdf.columns
    )
    if push_back:
        # The reports on the ground are read twice, for the movements by
        # speed here and for the slow movements before each off-block time:
        # they are taken from the reports once and kept, with the columns
        # the two need.
        columns = ("track_id", "event_time", "lat", "lon", "velocity", "on_ground")
        on_ground = _kept(on_ground.select(*[c for c in columns if c in sdf.columns]), checkpoints)
    runs = _movement_runs(
        on_ground,
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
    if config.block_time_one_part_per_leg:
        # Never NULL: a track with a take-off and no landing has no ground
        # part without take-off at its start, and one with a landing and no
        # take-off none without landing at its end.
        starts_without_take_off = F.col("_first_dep").isNull() | (
            F.col("_first_arr").isNotNull() & (F.col("_first_arr") < F.col("_first_dep"))
        )
        ends_without_landing = F.col("_last_arr").isNull() | (
            F.col("_last_dep").isNotNull() & (F.col("_last_dep") > F.col("_last_arr"))
        )
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
    if config.block_time_off_the_runway:
        # On speed where the speed is known; the flag "on the ground" only
        # where it is not (the take-off and landing rolls are flagged too).
        runs = runs.withColumn(
            "_starts_still",
            F.coalesce(
                F.when(F.col("_from_kt").isNotNull(), F.col("_from_kt") <= slow).otherwise(
                    F.col("_from_flagged") & (F.col("_from") > F.col("_t_first"))
                ),
                F.lit(False),
            ),
        ).withColumn(
            "_ends_still",
            F.coalesce(
                F.when(F.col("_to_kt").isNotNull(), F.col("_to_kt") <= slow).otherwise(
                    F.col("_to_flagged") & (F.col("_to") < F.col("_t_last"))
                ),
                F.lit(False),
            ),
        )
    else:
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
                (F.col("_to_kt") <= slow)
                | (F.col("_to_flagged") & (F.col("_to") < F.col("_t_last"))),
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
            F.col("_lo").alias("_part_lo"),
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
            F.col("_lo").alias("_part_lo"),
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
        F.col("_lo").alias("_part_lo"),
    )
    split_off = stop.filter(F.coalesce(F.col("_next_starts_still"), F.lit(False))).select(
        "track_id",
        F.lit("off").alias("block"),
        F.col("_next_from").alias("event_time"),
        F.col("_to").alias("_s_lo"),
        F.col("_hi").alias("_s_hi"),
        F.col("_hi").alias("_bound"),
        F.col("_hi_method").alias("_bound_method"),
        F.col("_lo").alias("_part_lo"),
    )
    candidates = off.unionByName(on).unionByName(split_on).unionByName(split_off)

    # The stand of each time, from the same part of the track: for an
    # off-block time the first stand exit at or after it (else the last one
    # before it), for an in-block time the last stand entry at or before it
    # (else the first one after it).
    info = F.col("info") if "info" in airport_events.columns else F.lit(None).cast("string")
    stand_ref = F.get_json_object(info, "$.osm_ref")
    if config.layout_ref_missing_as_null:
        stand_ref = missing_ref_as_null(stand_ref)
    stands = airport_events.filter(F.col("type").isin(_STAND_EXIT, _STAND_ENTRY)).select(
        F.col("track_id").alias("_s_track"),
        F.when(F.col("type") == _STAND_EXIT, F.lit("off")).otherwise(F.lit("on")).alias("_s_block"),
        F.col("event_time").alias("stand_time"),
        stand_ref.alias("stand"),
        F.get_json_object(info, "$.osm_airport").alias("stand_airport"),
        F.get_json_object(info, "$.opdi_time_in_use_s").cast("double").alias("stand_dwell"),
    )
    stands = _kept(stands, checkpoints)
    out_cols = [
        "track_id",
        "block",
        "event_time",
        "stand_time",
        "stand",
        "stand_airport",
        "_s_lo",
        "_s_hi",
        "_bound",
        "_bound_method",
    ]

    anchored = None
    if config.block_time_from_last_stand_stop:
        # Each part with a stand stop of its own gives its time from that
        # stop; the other parts keep the first (last) movement.
        anchored_parts = [
            (off_parts, "off", True),
            (on_parts, "on", True),
            (
                split.select(
                    "track_id",
                    "_lo",
                    "_hi",
                    F.col("_hi").alias("_bound"),
                    F.col("_hi_method").alias("_bound_method"),
                ),
                "off",
                False,
            ),
            (
                split.select(
                    "track_id",
                    "_lo",
                    "_hi",
                    F.col("_lo").alias("_bound"),
                    F.col("_lo_method").alias("_bound_method"),
                ),
                "on",
                False,
            ),
        ]
        found = [
            _from_stand_stop(part, runs, stands, side, at_edge, config)
            for part, side, at_edge in anchored_parts
        ]
        anchored, had_stop = found[0]
        for times, stops in found[1:]:
            anchored = anchored.unionByName(times)
            had_stop = had_stop.unionByName(stops)
        # The parts that have a stand stop: their first (last) movement is
        # not read, whether or not the stand stop gave a time.
        candidates = candidates.join(
            had_stop,
            (F.col("track_id") == F.col("_h_track"))
            & (F.col("block") == F.col("_h_block"))
            & (F.col("_part_lo") == F.col("_h_lo")),
            "left_anti",
        )

    stand_window = (
        (F.col("track_id") == F.col("_s_track"))
        & (F.col("block") == F.col("_s_block"))
        & (F.col("stand_time") >= F.col("_s_lo"))
        & (F.col("stand_time") <= F.col("_s_hi"))
    )
    if config.block_time_stand_at_same_airport:
        # Only a stand passage in the same part is named, and only one of at
        # least block_time_min_stand_dwell_seconds or one that reaches to
        # within a minute of the block time: an aircraft that parks is often
        # in the stand's cells for a few seconds only, because its later
        # reports carry no position. A time without one names no stand.
        p_dwell = F.coalesce(F.col("stand_dwell"), F.lit(0.0))
        p_time = F.col("stand_time").cast("double")
        b_time = F.col("event_time").cast("double")
        named = (p_dwell >= F.lit(float(config.block_time_min_stand_dwell_seconds))) | (
            (b_time >= p_time - p_dwell - F.lit(_NEAR_SECONDS))
            & (b_time <= p_time + p_dwell + F.lit(_NEAR_SECONDS))
        )
        matched = candidates.join(stands, stand_window & named, "left")
    else:
        matched = candidates.join(stands, stand_window, "left")
    # A part with no stand passage of its own still gives its time when
    # the track passed a stand elsewhere (the stand at the start of a
    # departure is often not received); the time then names no stand.
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
    order = [preferred.desc_nulls_last(), F.abs(st - ev).asc_nulls_last(), "stand_time"]
    if config.block_time_stand_longest_dwell:
        # The passage the block time lies in (the aircraft left or reached
        # that stand), then the longest.
        within = (ev >= st - F.coalesce(F.col("stand_dwell"), F.lit(0.0))) & (
            ev <= st + F.coalesce(F.col("stand_dwell"), F.lit(0.0))
        )
        order = [within.desc_nulls_last(), F.col("stand_dwell").desc_nulls_last()] + order
    pick = Window.partitionBy("track_id", "block", "event_time").orderBy(*order)
    picked = (
        matched.withColumn("_rank", F.row_number().over(pick))
        .filter(F.col("_rank") == 1)
        .select(*out_cols)
    )
    if config.off_block_at_start_of_push_back:
        # A slow movement is read from the start of the part, or from the
        # end of the movement before the stop of a landing followed by a
        # take-off (``_s_lo``); for a time read from a stand stop, from the
        # moment the aircraft reached that stand (``_move_lo``).
        picked = picked.withColumn("_move_lo", F.col("_s_lo").cast("double"))
    if anchored is not None:
        picked = picked.unionByName(anchored)
    if push_back:
        # Kept: the off-block and the in-block times are each read from it.
        picked = _kept(_at_push_back_start(picked, on_ground, config, first_last), checkpoints)
    elif config.off_block_at_start_of_push_back:
        # Without positions there is no displacement to read.
        picked = picked.drop("_move_lo")

    if config.block_time_off_the_runway:
        picked = _off_the_runway(picked, runway_events, config)
    if config.block_time_needs_flight:
        # A track that never leaves the ground: no take-off or landing time
        # and no stretch in flight.
        flying = span.filter(
            F.col("_first_air").isNotNull()
            | F.coalesce(F.col("_has_dep"), F.lit(False))
            | F.coalesce(F.col("_has_arr"), F.lit(False))
        ).select(F.col("track_id").alias("_f_track"))
        picked = picked.join(flying, F.col("track_id") == F.col("_f_track"), "left_semi")

    first_seen = []
    if config.off_block_names_where_first_seen:
        picked = _with_first_seen_on(picked, airport_events, first_last)
        first_seen = ["first_seen_on"]
    return picked.select(
        "track_id",
        "block",
        "event_time",
        "stand_time",
        "stand",
        "stand_airport",
        F.col("_bound").alias("bound_time"),
        F.col("_bound_method").alias("bound_method"),
        *first_seen,
    )


def _with_first_seen_on(
    blocks: DataFrame, airport_events: DataFrame, first_last: DataFrame
) -> DataFrame:
    """``blocks`` with ``first_seen_on``: where the aircraft was first received on the ground.

    The rule of ``off_block_names_where_first_seen``. The layout elements
    the track enters within :data:`_EDGE_SECONDS` of its first entry of any
    element give the value: ``taxiway`` when one of them is a taxiway or a
    runway, else ``stand`` when one is a stand, else ``apron``, else
    ``other``. Only an off-block time whose ground part starts at the
    track's first report (``_s_lo``) gets a value; an in-block time, an
    off-block time after a landing in the same track, and a track that
    enters no layout element get NULL.
    """
    t = F.col("event_time")
    entries = (
        airport_events.filter(F.col("type").startswith("entry-"))
        .groupBy(F.col("track_id").alias("_e_track"))
        .agg(
            F.min(t).alias("_e_first"),
            F.min(F.when(F.col("type").isin("entry-taxiway", "entry-runway"), t)).alias("_e_taxi"),
            F.min(F.when(F.col("type") == _STAND_ENTRY, t)).alias("_e_stand"),
            F.min(F.when(F.col("type") == "entry-apron", t)).alias("_e_apron"),
        )
    )
    edge = F.col("_e_first").cast("double") + F.lit(_EDGE_SECONDS)

    def at_first(c):
        return F.coalesce(F.col(c).cast("double") <= edge, F.lit(False))

    where = (
        F.when(at_first("_e_taxi"), F.lit("taxiway"))
        .when(at_first("_e_stand"), F.lit("stand"))
        .when(at_first("_e_apron"), F.lit("apron"))
        .when(F.col("_e_first").isNotNull(), F.lit("other"))
    )
    out = blocks.join(entries, F.col("track_id") == F.col("_e_track"), "left").join(
        first_last, F.col("track_id") == F.col("_fl_track"), "left"
    )
    from_start = (F.col("block") == "off") & (F.col("_s_lo") == F.col("_t_first"))
    return out.withColumn("first_seen_on", F.when(from_start, where)).drop(
        "_e_track",
        "_e_first",
        "_e_taxi",
        "_e_stand",
        "_e_apron",
        "_fl_track",
        "_t_first",
        "_t_last",
    )


def missing_ref_as_null(ref: Column) -> Column:
    """The name of a layout element, with a missing name as NULL.

    OpenStreetMap elements without a ``ref`` tag reach the layout table as
    the text ``"None"`` or ``"nan"`` (how Python writes a missing value as
    text), or as an empty text. Each of these becomes NULL here.
    """
    return F.when(F.lower(F.trim(ref)).isin("none", "nan", ""), F.lit(None)).otherwise(ref)


def _from_stand_stop(
    parts: DataFrame,
    runs: DataFrame,
    stands: DataFrame,
    side: str,
    at_edge: bool,
    config: "EventConfig",
) -> DataFrame:
    """Block times of ``side`` (``"off"`` or ``"on"``) read from the stand stop of each part.

    The rule of ``block_time_from_last_stand_stop``. For ``"off"``, the
    stand stop of a part is its last stand exit whose passage lasts at least
    ``block_time_long_stand_stop_seconds``; the off-block time is the start
    of the first movement that starts at or after the entry of that passage
    and still goes on ``block_time_min_stand_dwell_seconds`` after it, from a
    standstill. For ``"on"``, it is the first stand entry of at least that
    long, or (with ``at_edge``) one that lasts to the last report of the
    part; the stops of the part are the
    times between two movements and, with ``at_edge``, between its last
    movement and its end; the in-block time is the end of the movement
    before the longest stop that overlaps the passage, to a standstill.

    Returns the block times, and the parts that have a stand stop
    (``_h_track``, ``_h_block``, ``_h_lo``: the track, the side and the
    start of the part).
    """
    off = side == "off"
    dwell = F.col("stand_dwell")
    st = F.col("stand_time").cast("double")
    # A block time is read from a long stand stop only (a repositioned or
    # towed aircraft): a shorter passage is often the place where the
    # engines are started after the push-back, or where the aircraft waited
    # before reaching its stand, and the first (last) movement of the part
    # is kept for it.
    passages = stands.filter(
        (F.col("_s_block") == side)
        & (dwell >= F.lit(float(config.block_time_min_stand_dwell_seconds)))
    )
    a = parts.join(
        passages,
        (F.col("track_id") == F.col("_s_track"))
        & (F.col("stand_time") >= F.col("_lo"))
        & (F.col("stand_time") <= F.col("_hi")),
        "inner",
    )
    is_long = dwell >= F.lit(float(config.block_time_long_stand_stop_seconds))
    if at_edge and not off:
        # A passage cut by the end of reception: the aircraft stood there
        # after it was last received, so the passage counts as a long stop
        # whatever its length.
        is_long = is_long | (
            F.abs(st + dwell - F.col("_hi").cast("double")) <= F.lit(_EDGE_SECONDS)
        )
    a = a.filter(is_long)
    long_time = F.when(is_long, st)
    choice = Window.partitionBy("track_id", "_lo").orderBy(
        is_long.desc(),
        long_time.desc_nulls_last() if off else long_time.asc_nulls_last(),
        dwell.desc(),
        "stand_time",
    )
    a = (
        a.withColumn("_rank", F.row_number().over(choice))
        .filter(F.col("_rank") == 1)
        .withColumn("_p_in", st - dwell if off else st)
        .withColumn("_p_out", st if off else st + dwell)
        .drop("_rank", "_s_track", "_s_block")
    )
    had_stop = a.select(
        F.col("track_id").alias("_h_track"),
        F.lit(side).alias("_h_block"),
        F.col("_lo").alias("_h_lo"),
    )

    r = a.join(runs, "track_id", "inner").filter(
        (F.col("_to") >= F.col("_lo")) & (F.col("_from") <= F.col("_hi"))
    )
    if off:
        # The first movement that starts at or after the aircraft was first
        # seen in the stand's cells, and still goes on 30 s later (the
        # movement into the stand ends there). Not the movement after the
        # longest stop: after the push-back the aircraft often stands longer
        # for the engine start than it was received before the push-back.
        r = r.filter(
            (F.col("_from") >= F.col("_lo"))
            & (F.col("_from") <= F.col("_hi"))
            & (F.col("_from").cast("double") >= F.col("_p_in"))
            & (
                F.col("_to").cast("double")
                >= F.col("_p_in") + F.lit(float(config.block_time_min_stand_dwell_seconds))
            )
        )
        longest = Window.partitionBy("track_id", "_lo").orderBy("_from")
        time_col, still = "_from", "_starts_still"
    else:
        by_start = Window.partitionBy("track_id", "_lo").orderBy("_from")
        stop_to = F.lead("_from").over(by_start)
        if at_edge:
            stop_to = F.coalesce(stop_to, F.col("_hi"))
        r = r.withColumn("_stop_from", F.col("_to").cast("double")).withColumn(
            "_stop_to", stop_to.cast("double")
        )
        r = r.filter(
            (F.col("_to") >= F.col("_lo"))
            & (F.col("_to") <= F.col("_hi"))
            & F.col("_stop_to").isNotNull()
            # The aircraft often stops just beyond the stand's cells, or its
            # last positions in them are a few seconds before it stops.
            & (F.col("_stop_from") <= F.col("_p_out") + F.lit(_NEAR_SECONDS))
            & (F.col("_stop_to") >= F.col("_p_in") - F.lit(_NEAR_SECONDS))
        )
        longest = Window.partitionBy("track_id", "_lo").orderBy(
            (F.col("_stop_to") - F.col("_stop_from")).desc(), "_from"
        )
        time_col, still = "_to", "_ends_still"
    times = (
        r.withColumn("_rank", F.row_number().over(longest))
        .filter((F.col("_rank") == 1) & F.col(still))
        .select(
            "track_id",
            F.lit(side).alias("block"),
            F.col(time_col).alias("event_time"),
            "stand_time",
            "stand",
            "stand_airport",
            F.col("_lo").alias("_s_lo"),
            F.col("_hi").alias("_s_hi"),
            "_bound",
            "_bound_method",
            *([F.col("_p_in").alias("_move_lo")] if config.off_block_at_start_of_push_back else []),
        )
    )
    return times, had_stop


def _feet(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Distance in feet between two positions."""
    return haversine_nm(lat1, lon1, lat2, lon2) * F.lit(FT_PER_NM)


def _at_push_back_start(
    blocks: DataFrame, reports: DataFrame, config: "EventConfig", first_last: DataFrame
) -> DataFrame:
    """``blocks`` with each off-block time moved back to the start of a slow push-back.

    The rule of ``off_block_at_start_of_push_back``. For each off-block time,
    the reports on the ground with a position are read from
    ``push_back_max_stop_seconds`` before it (not from before ``_move_lo``)
    to ``push_back_displacement_baseline_seconds`` after it. A report is
    displaced when it lies at least ``push_back_min_displacement_ft`` from the
    earliest of these reports in the ``push_back_displacement_baseline_seconds``
    before it. Displaced reports that follow each other, no more than
    ``ground_speed_max_position_gap_seconds`` apart, are one slow movement;
    it counts when it holds two reports or more (one wrong position is not
    a movement) and its last position lies at least
    ``push_back_min_distance_ft`` from the position its first report was
    compared with. The off-block time becomes the first report of the
    earliest such movement that starts before it. In-block times, and
    off-block times without such a movement, are returned as they are.

    A track that lands and takes off again holds two flights. Its reports
    are then read from ``_move_lo`` onwards, where the aircraft had already
    been received: a slow movement whose first displaced report lies within
    ``push_back_displacement_baseline_seconds`` of ``_move_lo`` is the end
    of the taxi-in of the arriving flight (the last metres into the stand),
    and is not taken as the push-back of the next flight.

    ``blocks`` carries ``_move_lo`` (seconds since 1970), which is dropped.
    ``reports`` holds the reports that may be on the ground
    (:func:`_report_on_ground`). ``first_last`` gives the time of the first
    report of each track (``_fl_track``, ``_t_first``).
    """
    baseline = float(config.push_back_displacement_baseline_seconds)
    times = F.broadcast(
        blocks.filter(F.col("block") == "off")
        .join(first_last, F.col("track_id") == F.col("_fl_track"), "left")
        .select(
            F.col("track_id").alias("_b_track"),
            F.col("event_time").alias("_b_time"),
            F.col("event_time").cast("double").alias("_b_s"),
            F.col("_move_lo").alias("_b_lo"),
            # The first moment a slow movement may start: later than
            # ``_move_lo`` by the baseline when the aircraft was received
            # before ``_move_lo`` (a later flight of the track).
            F.when(
                F.col("_move_lo") > F.col("_t_first").cast("double"),
                F.col("_move_lo") + F.lit(baseline),
            ).alias("_b_after"),
        )
    )
    t = F.col("event_time").cast("double")
    near = (
        reports.filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
        .select(F.col("track_id").alias("_r_track"), "event_time", t.alias("_t"), "lat", "lon")
        .join(
            times,
            (F.col("_r_track") == F.col("_b_track"))
            & (F.col("_t") <= F.col("_b_s") + F.lit(baseline))
            & (
                F.col("_t")
                >= F.greatest(
                    F.coalesce(F.col("_b_lo"), F.col("_b_s") - F.lit(1e12)),
                    F.col("_b_s") - F.lit(float(config.push_back_max_stop_seconds)),
                )
            ),
            "inner",
        )
    )
    ordered = Window.partitionBy("_b_track", "_b_time").orderBy("_t")
    seconds = int(round(baseline))
    here = F.struct(F.col("_t").alias("t"), "lat", "lon")
    # Where the aircraft was up to ``baseline`` seconds before this report
    # (the earliest report of that time).
    near = near.withColumn("_base", F.first(here).over(ordered.rangeBetween(-seconds, 0)))
    near = near.withColumn(
        "_displaced",
        (F.col("_base.t") < F.col("_t"))
        & (
            _feet(F.col("_base.lat"), F.col("_base.lon"), F.col("lat"), F.col("lon"))
            >= F.lit(float(config.push_back_min_displacement_ft))
        ),
    )
    gap = F.lit(float(config.ground_speed_max_position_gap_seconds))
    starts = F.col("_displaced") & (
        ~F.coalesce(F.lag("_displaced").over(ordered), F.lit(False))
        | (F.col("_t") - F.lag("_t").over(ordered) > gap)
    )
    near = near.withColumn("_new", F.when(starts, 1).otherwise(0)).withColumn(
        "_movement",
        F.sum("_new").over(ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
    )
    movements = (
        near.filter(F.col("_displaced"))
        .groupBy("_b_track", "_b_time", "_b_s", "_b_after", "_movement")
        .agg(
            F.min(
                F.struct(F.col("_t").alias("t"), F.col("event_time").alias("time"), "_base")
            ).alias("_first"),
            F.max(here).alias("_last"),
            F.count("*").alias("_n"),
        )
        .filter(
            (F.col("_n") >= 2)
            & (F.col("_first.t") < F.col("_b_s"))
            & F.coalesce(F.col("_first.t") > F.col("_b_after"), F.lit(True))
            & (
                _feet(
                    F.col("_first._base.lat"),
                    F.col("_first._base.lon"),
                    F.col("_last.lat"),
                    F.col("_last.lon"),
                )
                >= F.lit(float(config.push_back_min_distance_ft))
            )
        )
        .groupBy("_b_track", "_b_time")
        .agg(F.min("_first.time").alias("_push_back"))
    )
    moved = blocks.join(
        movements,
        (F.col("block") == "off")
        & (F.col("track_id") == F.col("_b_track"))
        & (F.col("event_time") == F.col("_b_time")),
        "left",
    )
    return moved.withColumn(
        "event_time", F.coalesce(F.col("_push_back"), F.col("event_time"))
    ).drop("_b_track", "_b_time", "_push_back", "_move_lo")


def _off_the_runway(
    blocks: DataFrame, runway_events: Optional[DataFrame], config: "EventConfig"
) -> DataFrame:
    """``blocks`` without the times on the runway or next to the take-off or landing.

    The rule of ``block_time_off_the_runway``. An off-block time is dropped
    when it lies less than ``block_time_min_gap_to_runway_time_seconds`` before
    the take-off of its leg (``_bound``), or after the last ``line-up`` before
    that take-off; an in-block time when it lies less than that after the
    landing of its leg, or before the first ``runway-vacated`` after it.
    """
    gap = F.lit(float(config.block_time_min_gap_to_runway_time_seconds))
    ev = F.col("event_time").cast("double")
    bound = F.col("_bound").cast("double")
    near = F.when(F.col("block") == "off", bound - ev).otherwise(ev - bound) < gap
    blocks = blocks.filter(~F.coalesce(near, F.lit(False)))
    if runway_events is None:
        return blocks
    marks = runway_events.filter(
        F.col("type").isin("line-up", "runway-vacated") & F.col("event_time").isNotNull()
    ).select(
        F.col("track_id").alias("_r_track"),
        F.when(F.col("type") == "line-up", F.lit("off")).otherwise(F.lit("on")).alias("_r_block"),
        F.col("event_time").alias("_r_time"),
    )
    # The line-up of the take-off is the last one before it within the part,
    # the runway-vacated of the landing the first one after it.
    keys = ["track_id", "block", "event_time", "_bound"]
    on_runway = (
        blocks.filter(F.col("_bound").isNotNull())
        .join(
            marks,
            (F.col("track_id") == F.col("_r_track"))
            & (F.col("block") == F.col("_r_block"))
            & F.when(
                F.col("block") == "off",
                (F.col("_r_time") <= F.col("_bound")) & (F.col("_r_time") >= F.col("_s_lo")),
            ).otherwise(
                (F.col("_r_time") >= F.col("_bound")) & (F.col("_r_time") <= F.col("_s_hi"))
            ),
            "inner",
        )
        .groupBy(*keys)
        .agg(F.max("_r_time").alias("_lineup"), F.min("_r_time").alias("_vacated"))
        .filter(
            F.when(F.col("block") == "off", F.col("event_time") >= F.col("_lineup")).otherwise(
                F.col("event_time") <= F.col("_vacated")
            )
        )
        .select(*keys)
    )
    return blocks.join(on_runway, keys, "left_anti")
