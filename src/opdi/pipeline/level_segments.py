"""Level-segment detection, for KPI17 and KPI19.

Two arms, registered in :data:`LEVEL_ARMS` and selected by
``EventConfig.level_method``. :func:`level_segments` is ICAO's anchored band,
shipped in ``events_v0.1.0`` and unchanged; :func:`level_segments_pru` is PRU's
rolling window, which defines level flight as a geometric property of the
altitudes rather than as a test on the reported vertical rate. Both are
published behaviour and neither can be shown better than the other, because no
source OPDI can reach holds level-segment truth. The rest of this note
describes the ICAO arm.

KPI17 (level-off during climb) and KPI19 (level-off during descent) are the two
vertical-efficiency indicators ADS-B can reach, and ICAO specifies them with a
complete algorithm and named parameters rather than leaving the detection to
the implementer::

    a data point starts a level segment when the altitude difference with the
    next data point is <= the level band limit and the vertical speed towards
    it is <= the vertical speed limit. The segment ends when the altitude
    differs from the segment's starting altitude by more than the level band
    limit, or the vertical speed between two consecutive points exceeds the
    limit.

**This is not what OPDI's ``level-start``/``level-end`` events compute.** Those
come from the fuzzy phase classifier, which asks whether a sample looks like
level flight; ICAO asks a geometric question about a specific altitude band
anchored at the segment's own start. The two are not interchangeable and both
are published, under different type names.

Why a separate detector is worth the code: it makes the claim checkable. No
data source holds level-segment truth -- APDF has none, and neither does
anything else OPDI can reach -- so "accurate" is not a claim this family can
support. "Conformant to ICAO's published algorithm" is, and a specification can
be tested against on synthetic trajectories whose geometry is known.

The anchoring is the part a window function cannot express directly: a
segment's end is defined against *its own start*, which is a running value that
resets. That is a sessionisation, done here in two passes -- mark where a
segment cannot continue, then forward-fill the anchor within the resulting
groups -- rather than with a UDF.
"""

from typing import TYPE_CHECKING, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

if TYPE_CHECKING:  # pragma: no cover - import cycle guard
    from opdi.config import EventConfig

FT_PER_M = 3.28084
FTMIN_PER_MPS = 196.850394

#: Per-sample distances to the flight's own aerodromes, attached upstream by
#: :func:`opdi.pipeline.vertical_pru.attach_aerodrome_geometry`. Carried through
#: the segment aggregation of *both* arms so that
#: :func:`classify_level_offs` can apply ``level_analysis_radius_nm``, which has
#: been declared since v0.1.0 and read by nothing.
DISTANCE_COLS = ("dist_adep_nm", "dist_ades_nm")

#: Ceiling on the number of interpolation grid points :func:`level_segments_pru`
#: builds for one partition.
#:
#: The grid is an ``explode`` over a single array, so its whole length is
#: materialised in one row before it is expanded: the cost is not per grid point
#: but per *partition*, and one pathological track pays it alone. At the default
#: 10 s window this cap is 11.6 days of trajectory, which no real flight
#: approaches -- but a track whose segmentation failed can span a month, and
#: 260,000 points in one array is an executor OOM rather than a slow job.
#:
#: A partition longer than the cap is **truncated at the tail**: the grid stops
#: at the cap and level segments in the remainder are not reported. The error is
#: bounded -- reported segments stay correct, only coverage is lost -- and
#: losing the tail of a track that is already a segmentation failure is the
#: cheap direction to be wrong in. The real fix is to partition on ``segment_id``
#: as well as ``track_id``, which ``partition_cols`` already accepts and which
#: nothing currently passes; that is deferred.
MAX_GRID_POINTS_PER_PARTITION = 100_000


def _geometry_aggregations(sdf: DataFrame, time_col: str):
    """Aggregations carrying per-sample geometry out to the segment.

    Only the columns the caller actually attached, so both arms stay pure
    column-in/column-out functions that a test can drive without storage.

    Each distance is taken at the end of the segment nearer its own
    aerodrome: ``dist_adep_nm`` at the segment's **start**, because a climb
    level-off is placed by where it began relative to the departure aerodrome,
    and ``dist_ades_nm`` at its **end**, because a descent one is placed by
    where it finished relative to the arrival aerodrome. Taking either at the
    other end would push a segment straddling the ring outside it on the
    strength of the part of itself that is still on its way in.

    Elevations are carried the same way, unchanged per track, so
    :func:`classify_level_offs` can measure its floors above the field.
    """
    out = []
    if "dist_adep_nm" in sdf.columns:
        out.append(F.min_by("dist_adep_nm", time_col).alias("dist_adep_nm"))
    if "dist_ades_nm" in sdf.columns:
        out.append(F.max_by("dist_ades_nm", time_col).alias("dist_ades_nm"))
    for elev in ("elev_adep_ft", "elev_ades_ft"):
        if elev in sdf.columns:
            out.append(F.first(elev).alias(elev))
    return out


def level_segments(
    sdf: DataFrame,
    config: "EventConfig",
    *,
    partition_cols: Sequence[str] = ("track_id",),
    altitude_col: str = "baro_altitude_c",
    vert_rate_col: str = "vert_rate",
    time_col: str = "event_time",
) -> DataFrame:
    """Detect ICAO level segments.

    ``altitude_col`` is metres and ``vert_rate_col`` is m/s -- the storage layer
    is SI -- and both are scaled into aviation units to meet the thresholds,
    which are expressed as ICAO expresses them.

    Returns one row per segment: the partition columns plus ``start_time``,
    ``end_time``, ``duration_seconds``, ``level_ft`` (the anchor altitude) and
    ``distance_nm`` when a cumulative distance is available.
    """
    ordered = Window.partitionBy(*partition_cols).orderBy(time_col)
    running = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    alt_ft = F.col(altitude_col) * FT_PER_M
    roc_ftmin = F.abs(F.col(vert_rate_col) * FTMIN_PER_MPS)

    work = sdf.withColumn("_alt_ft", alt_ft).withColumn("_roc", roc_ftmin)
    work = work.withColumn("_alt_next", F.lead("_alt_ft").over(ordered))

    # A sample is "flat" when the step to the next one stays inside the band and
    # the vertical speed towards it is within the limit -- ICAO's start
    # condition, evaluated at every sample.
    flat = (
        (F.abs(F.col("_alt_next") - F.col("_alt_ft")) <= F.lit(config.level_band_limit_ft))
        & (F.col("_roc") <= F.lit(config.level_vertical_speed_limit_ftmin))
    )
    work = work.withColumn("_flat", F.coalesce(flat, F.lit(False)))

    # Sessionise: a new group starts wherever flatness begins. Everything not
    # flat is discarded afterwards, so non-flat runs simply form groups of
    # their own.
    work = work.withColumn("_prev_flat", F.lag("_flat").over(ordered))
    work = work.withColumn(
        "_new_group",
        F.when(F.col("_flat") & ~F.coalesce(F.col("_prev_flat"), F.lit(False)), 1).otherwise(0),
    )
    work = work.withColumn("_group", F.sum("_new_group").over(running))

    # The anchor: the altitude at which this run of flat samples began. ICAO
    # ends a segment when the altitude leaves the band *around the anchor*, not
    # around the previous sample -- which is what stops a slow drift being read
    # as one long level segment.
    group_window = Window.partitionBy(*partition_cols, "_group").orderBy(time_col)
    work = work.withColumn("_anchor", F.first("_alt_ft").over(group_window))

    # Membership is evaluated at the sample itself, not on its step to the
    # next one. The forward step only ever *starts* a segment; ICAO ends one
    # at the point where the condition breaks, so the last level sample --
    # whose next step is the climb away -- still belongs to the segment. Using
    # the forward step for membership drops it and reports every level-off one
    # sample interval short, always in the same direction.
    work = work.withColumn(
        "_member",
        (F.abs(F.col("_alt_ft") - F.col("_anchor")) <= F.lit(config.level_band_limit_ft))
        & (F.col("_roc") <= F.lit(config.level_vertical_speed_limit_ftmin)),
    )

    # Truncate each group at its first failure rather than filtering failures
    # out, so a trajectory that leaves the band and drifts back does not get
    # stitched into one segment across the excursion.
    group_running = group_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    work = work.withColumn(
        "_failures",
        F.sum(F.when(F.col("_member"), 0).otherwise(1)).over(group_running),
    )

    seg = work.filter(F.col("_member") & (F.col("_failures") == 0))

    agg = [
        F.min(time_col).alias("start_time"),
        F.max(time_col).alias("end_time"),
        F.first("_anchor").alias("level_ft"),
    ]
    if "cumulative_distance_nm" in sdf.columns:
        agg += [
            F.min("cumulative_distance_nm").alias("_d0"),
            F.max("cumulative_distance_nm").alias("_d1"),
        ]
    agg += _geometry_aggregations(sdf, time_col)

    out = seg.groupBy(*partition_cols, "_group").agg(*agg)
    out = out.withColumn(
        "duration_seconds",
        F.col("end_time").cast("double") - F.col("start_time").cast("double"),
    )
    if "cumulative_distance_nm" in sdf.columns:
        out = out.withColumn("distance_nm", F.col("_d1") - F.col("_d0")).drop("_d0", "_d1")
    else:
        out = out.withColumn("distance_nm", F.lit(None).cast("double"))

    return out.filter(
        F.col("duration_seconds") >= F.lit(config.level_min_duration_seconds)
    ).drop("_group")


def level_segments_pru(
    sdf: DataFrame,
    config: "EventConfig",
    *,
    partition_cols: Sequence[str] = ("track_id",),
    altitude_col: str = "baro_altitude_c",
    vert_rate_col: str = "vert_rate",
    time_col: str = "event_time",
) -> DataFrame:
    """PRU's rolling-window level detection.

    PRU defines level flight geometrically rather than by vertical rate: a
    trajectory part is level when it stays inside a window of `X` seconds and
    `Y` feet with `Y/X = 300 ft/min`. That is not the same as testing the
    reported vertical rate -- `vert_rate` is a broadcast field that can be
    stale or absent, while the window is a property of the altitudes actually
    received, so the window fires on trajectories the rate test misses and
    abstains on ones it wrongly accepts.

    The altitude series is linearly interpolated onto a regular
    `level_window_seconds` grid first, as PRU specifies, because a discrete
    track has no sample at every instant the window needs.

    ``vert_rate_col`` is accepted and ignored, so that the two arms are
    interchangeable through :data:`LEVEL_ARMS` without the caller knowing which
    one it selected. Ignoring it is the whole point of this arm.

    Returns the same schema as :func:`level_segments`: the partition columns
    plus ``start_time``, ``end_time``, ``duration_seconds``, ``level_ft`` and
    ``distance_nm``, with the geometry columns when the caller attached them.
    """
    part = list(partition_cols)
    window_s = float(config.level_window_seconds)
    carried = [
        c for c in (*DISTANCE_COLS, "cumulative_distance_nm") if c in sdf.columns
    ]
    elevations = [c for c in ("elev_adep_ft", "elev_ades_ft") if c in sdf.columns]

    real = sdf.select(
        *part,
        F.col(time_col).cast("double").alias("_ts"),
        (F.col(altitude_col) * F.lit(FT_PER_M)).alias("_alt_ft"),
        *[F.col(c).cast("double").alias(c) for c in carried],
        *[F.col(c).cast("double").alias(c) for c in elevations],
    )

    # The grid is built from an index rather than from ``sequence`` over the
    # timestamps: ``sequence`` needs an integral step, and a window length is a
    # float in config. ``_t0 + i * window`` also keeps every grid instant an
    # exact multiple of the window away from the track's first sample, which is
    # what makes the geometry in the tests arithmetic.
    bounds = real.groupBy(*part).agg(
        F.min("_ts").alias("_t0"), F.max("_ts").alias("_t1")
    )
    grid = (
        bounds.withColumn(
            "_i",
            F.explode(
                F.sequence(
                    F.lit(0).cast("long"),
                    F.least(
                        F.floor(
                            (F.col("_t1") - F.col("_t0")) / F.lit(window_s)
                        ).cast("long"),
                        F.lit(MAX_GRID_POINTS_PER_PARTITION - 1).cast("long"),
                    ),
                )
            ),
        )
        .withColumn("_ts", F.col("_t0") + F.col("_i") * F.lit(window_s))
        .select(*part, "_ts")
        .withColumn("_is_grid", F.lit(True))
    )
    value_cols = ["_alt_ft"] + carried + elevations
    for c in value_cols:
        grid = grid.withColumn(c, F.lit(None).cast("double"))

    combined = grid.unionByName(real.withColumn("_is_grid", F.lit(False)))

    # Real samples sort before the grid instant they coincide with, so a grid
    # point that lands on a sample interpolates from zero distance and takes
    # that sample's value exactly rather than a rounding of it.
    order = Window.partitionBy(*part).orderBy("_ts", F.col("_is_grid").cast("int"))
    back = order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    fwd = order.rowsBetween(Window.currentRow, Window.unboundedFollowing)

    for c in value_cols:
        known = F.when(F.col(c).isNotNull(), F.col("_ts"))
        combined = (
            combined.withColumn(f"{c}__pt", F.last(known, True).over(back))
            .withColumn(f"{c}__pv", F.last(F.col(c), True).over(back))
            .withColumn(f"{c}__nt", F.first(known, True).over(fwd))
            .withColumn(f"{c}__nv", F.first(F.col(c), True).over(fwd))
        )
    for c in value_cols:
        pt, pv = F.col(f"{c}__pt"), F.col(f"{c}__pv")
        nt, nv = F.col(f"{c}__nt"), F.col(f"{c}__nv")
        combined = combined.withColumn(
            c,
            F.coalesce(
                F.col(c),
                # Linear between the samples either side. Both ends must exist
                # and be distinct; outside the sampled span, and where two
                # samples share a timestamp, the nearer value stands rather
                # than a division by zero.
                F.when(pt.isNotNull() & nt.isNotNull() & (nt != pt),
                       pv + (nv - pv) * (F.col("_ts") - pt) / (nt - pt)),
                pv,
                nv,
            ),
        )
    combined = combined.drop(
        *[f"{c}__{s}" for c in value_cols for s in ("pt", "pv", "nt", "nv")]
    )

    # ``rangeBetween`` takes integral bounds, so the ordering key is whole
    # milliseconds rather than seconds. Sub-millisecond resolution is well
    # under any ADS-B timestamp's, and a float bound raises rather than
    # silently rounding.
    span_window = (
        Window.partitionBy(*part)
        .orderBy(F.round(F.col("_ts") * 1000.0).cast("long"))
        .rangeBetween(0, int(round(window_s * 1000.0)))
    )
    whole = Window.partitionBy(*part)
    combined = combined.withColumn(
        "_span", F.max("_alt_ft").over(span_window) - F.min("_alt_ft").over(span_window)
    ).withColumn(
        "_t_last", F.max(F.when(~F.col("_is_grid"), F.col("_ts"))).over(whole)
    )

    # A grid point whose window runs past the last sample is not level; it is
    # unmeasured. Without this every track would end in a spurious segment,
    # because a truncated window spans nothing and so always looks flat.
    level = (
        (F.col("_span") <= F.lit(config.level_window_height_ft()))
        & (F.col("_ts") + F.lit(window_s) <= F.col("_t_last") + F.lit(1e-6))
    )
    points = combined.filter(F.col("_is_grid")).withColumn(
        "_level", F.coalesce(level, F.lit(False))
    )

    ordered = Window.partitionBy(*part).orderBy("_ts")
    running = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    points = points.withColumn("_prev", F.lag("_level").over(ordered))
    points = points.withColumn(
        "_new_group",
        F.when(F.col("_level") & ~F.coalesce(F.col("_prev"), F.lit(False)), 1).otherwise(0),
    )
    points = points.withColumn("_group", F.sum("_new_group").over(running))

    seg = points.filter(F.col("_level"))
    agg = [
        F.min("_ts").alias("_s0"),
        F.max("_ts").alias("_s1"),
        # No anchor to report: the PRU window is a band the trajectory stays
        # inside, not an offset from a starting sample, so the segment's
        # altitude is the mean of the interpolated altitudes within it.
        F.avg("_alt_ft").alias("level_ft"),
    ]
    if "cumulative_distance_nm" in carried:
        agg += [
            F.min("cumulative_distance_nm").alias("_d0"),
            F.max("cumulative_distance_nm").alias("_d1"),
        ]
    agg += _geometry_aggregations(seg, "_ts")

    out = seg.groupBy(*part, "_group").agg(*agg)
    # Each qualifying grid point asserts that the window *starting* at it is
    # level, so the segment runs to one window past the last of them. Ending it
    # at the last grid point instead would report every level-off exactly one
    # window short, always in the same direction.
    out = out.withColumn("_end", F.col("_s1") + F.lit(window_s))
    out = (
        out.withColumn("start_time", F.timestamp_seconds(F.col("_s0")))
        .withColumn("end_time", F.timestamp_seconds(F.col("_end")))
        .withColumn("duration_seconds", F.col("_end") - F.col("_s0"))
    )
    if "cumulative_distance_nm" in carried:
        out = out.withColumn("distance_nm", F.col("_d1") - F.col("_d0")).drop("_d0", "_d1")
    else:
        out = out.withColumn("distance_nm", F.lit(None).cast("double"))

    return out.filter(
        F.col("duration_seconds") >= F.lit(config.level_min_duration_seconds)
    ).drop("_group", "_s0", "_s1", "_end")


def classify_level_offs(
    segments: DataFrame,
    config: "EventConfig",
    *,
    toc_time,
    tod_time,
    toc_altitude_ft,
    tod_altitude_ft,
) -> DataFrame:
    """Split segments into KPI17 (climb) and KPI19 (descent) level-offs.

    Applies ICAO's two exclusions:

    * the **minimum altitude**, below which the trajectory is not analysed --
      3,000 ft in climb, 1,800 ft in descent, the difference being that an
      aircraft on final is legitimately close to level;
    * the **exclusion box**, which removes a segment sitting above
      ``level_exclusion_box_pct`` of the top-of-climb (or top-of-descent)
      altitude and lasting longer than ``level_exclusion_box_seconds``. That is
      cruise, not a level-off, and without the box every cruise would be
      counted as the largest level-off in the flight.
    """
    before_toc = F.col("end_time") <= toc_time
    after_tod = F.col("start_time") >= tod_time

    # PRU measures both floors above the field. The shipped v0.1.0 code
    # compared them against uncorrected pressure altitude, so the descent floor
    # sat 1,416 ft too low at Zurich and was effectively right at Ibiza -- a
    # per-aerodrome bias with nothing in the output to show it. The climb floor
    # takes the departure field and the descent floor the arrival one: a climb
    # level-off is near the departure aerodrome and a descent one near the
    # arrival, so each floor has an unambiguous field.
    climb_floor_ref = _floor_reference(segments, config, "elev_adep_ft")
    descent_floor_ref = _floor_reference(segments, config, "elev_ades_ft")
    climb_floor = climb_floor_ref >= F.lit(config.level_min_altitude_climb_ft)
    descent_floor = descent_floor_ref >= F.lit(config.level_min_altitude_descent_ft)

    # ``level_analysis_radius_nm`` has been declared since v0.1.0 and read by
    # nothing. PRU measures the climb within 200 NM of departure and the
    # descent within 200 NM of arrival, so each side tests its own distance.
    within_climb_radius = _within_radius(segments, config, "dist_adep_nm")
    within_descent_radius = _within_radius(segments, config, "dist_ades_nm")

    pct = F.lit(config.level_exclusion_box_pct / 100.0)
    long_enough_to_be_cruise = F.col("duration_seconds") > F.lit(
        config.level_exclusion_box_seconds
    )
    in_climb_box = (F.col("level_ft") >= pct * toc_altitude_ft) & long_enough_to_be_cruise
    in_descent_box = (F.col("level_ft") >= pct * tod_altitude_ft) & long_enough_to_be_cruise

    return segments.withColumn(
        "kpi",
        F.when(
            before_toc & climb_floor & within_climb_radius & ~in_climb_box,
            F.lit("KPI17"),
        ).when(
            after_tod & descent_floor & within_descent_radius & ~in_descent_box,
            F.lit("KPI19"),
        ),
    ).filter(F.col("kpi").isNotNull())


def _floor_reference(segments: DataFrame, config: "EventConfig", elevation_col: str):
    """The altitude the minimum-altitude floors are compared against.

    Above the field when ``level_floors_above_field``, and above the 1013.25
    datum otherwise -- which is v0.1.0's behaviour, kept reachable so released
    data stays reproducible. A missing elevation coalesces to zero, i.e. back to
    that same behaviour, rather than removing the flight's level-offs.
    """
    if not config.level_floors_above_field or elevation_col not in segments.columns:
        return F.col("level_ft")
    return F.col("level_ft") - F.coalesce(F.col(elevation_col), F.lit(0.0))


def _within_radius(segments: DataFrame, config: "EventConfig", distance_col: str):
    """Whether a segment lies inside PRU's analysis radius.

    A segment whose distance is NULL -- no aerodrome named for that end, or no
    coordinates for it -- fails the test and is excluded. The PRU definition
    needs the aerodrome, and a segment that cannot be placed relative to one is
    outside the methodology rather than inside it by default.

    A caller that attached no distance at all is a different case: the column
    is absent, the filter does not apply, and the caller gets the unrestricted
    classification it asked for. That is the same contract
    ``calculate_horizontal_segment_events`` gives the field elevations, and it
    is what keeps this function drivable from a test with no storage.
    """
    if distance_col not in segments.columns:
        return F.lit(True)
    return F.col(distance_col) <= F.lit(config.level_analysis_radius_nm)


#: The two level-segment arms, selected by ``EventConfig.level_method``.
#:
#: Both are published behaviour: ``icao`` is the anchored-band algorithm
#: shipped in ``events_v0.1.0`` and ``pru`` the rolling window. A dict rather
#: than a branch at the call site, so adding a third definition of level flight
#: is a registration rather than an edit to every caller -- and so that
#: ``EventConfig.__post_init__`` can reject an unknown name instead of letting
#: it fall through to no level segments at all.
LEVEL_ARMS = {"icao": level_segments, "pru": level_segments_pru}
