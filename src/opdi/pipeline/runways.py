"""Runway identification and touchdown/lift-off times (T08 ATOT, T17 ALDT).

A native port of ``traffic``'s ``TrackBasedRunwayDetection``
(``traffic/src/traffic/algorithms/navigation/takeoff.py:148-341``), which is a
filter, a median and a broadcast join -- no geometry library, no OpenAP, and no
runtime download. Its polygon-based sibling is deliberately *not* ported: that
one needs OpenAP for its phase call and shapely for a trapeze, and recurses into
a second alignment pass.

Runway geometry comes from ``oa_runways``, which step 00d has been generating
and nothing has ever read. It is the same OurAirports table ``traffic``
downloads at runtime, so this adds no dependency and nothing to warm on an
offline executor.

Bearings are computed from the two threshold positions rather than taken from
``le_heading_degT``/``he_heading_degT``. Those columns are frequently null in
OurAirports and, where present, are sometimes magnetic rather than true; the
positions are the thing OurAirports is reliable about, and ``traffic`` derives
its own bearings the same way for the same reason.
"""

from typing import TYPE_CHECKING, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

if TYPE_CHECKING:  # pragma: no cover - avoids an import cycle
    from opdi.config import EventConfig

from opdi.utils.geometry import angle_between, bearing_deg, haversine_nm

FT_PER_M = 3.28084
FTMIN_PER_MPS = 196.850394
KT_PER_MPS = 1.94384
EARTH_RADIUS_NM = 3440.065

#: Feet per nautical mile, used to turn ``oa_runways`` extents into the units
#: the geometric on-runway test works in.
FT_PER_NM = 6076.12
#: Metres per nautical mile.
M_PER_NM = 1852.0

#: Runway extent fallbacks for the geometric occupancy test, in the rare rows
#: where OurAirports leaves ``length_ft``/``width_ft`` null. A generic
#: long-haul length (8,000 ft) and a generous half-width (30 m): both are only
#: bounds on the on-runway membership test, so erring wide keeps a genuine
#: on-runway sample rather than dropping it, and the grid prune has already
#: bounded the sample to the neighbourhood of *this* strip.
DEFAULT_RWY_LENGTH_FT = 8000.0
DEFAULT_RWY_HALF_WIDTH_M = 30.0


def cross_track_nm(lat, lon, thr_lat, thr_lon, rwy_bearing):
    """Perpendicular distance from a point to a runway's extended centreline.

    The standard cross-track formula::

        d_xt = asin( sin(d_13 / R) * sin(theta_13 - theta_12) ) * R

    where 1 is the threshold, 2 the runway direction and 3 the aircraft. The
    absolute value is returned because which side of the centreline the
    aircraft sits on does not decide which runway it used.

    This replaces a tie-break on the distance from each threshold to the
    aerodrome reference point -- a constant of the airport, identical for every
    flight, which therefore picked the same runway of a parallel pair every
    time. That is where "a third of named landing runways are wrong" came from.
    """
    d13 = haversine_nm(thr_lat, thr_lon, lat, lon) / F.lit(EARTH_RADIUS_NM)
    theta13 = F.radians(bearing_deg(thr_lat, thr_lon, lat, lon))
    theta12 = F.radians(rwy_bearing)
    return F.abs(F.asin(F.sin(d13) * F.sin(theta13 - theta12)) * F.lit(EARTH_RADIUS_NM))


def runway_thresholds(storage) -> Optional[DataFrame]:
    """One row per runway *threshold*: the end an aircraft rolls from.

    A physical runway appears twice, once per direction, because that is the
    unit a movement is reported against -- APDF's ``AP_C_RWY`` names a
    direction, not a strip.

    Each row also carries the strip's extent -- ``rwy_length_nm`` and
    ``rwy_half_width_nm`` -- so the traversal detector can decide whether a
    grid-pruned sample actually sits *on* the runway rather than merely near
    it. Both directions of one strip share the same extent, since it is a
    property of the physical runway, not of the direction. ``thr_elev_ft`` is
    the threshold's elevation in feet (NULL where OurAirports gives none).
    ``length_ft`` null
    falls back to a generic 8,000 ft and ``width_ft`` null to a 30 m
    half-width; both are deliberately generous bounds -- see
    :data:`DEFAULT_RWY_LENGTH_FT`.
    """
    if not storage.table_exists("oa_runways"):
        return None

    rwy = storage.read_table("oa_runways").filter(
        (F.col("closed").isNull() | (F.col("closed") == False))  # noqa: E712
        & F.col("le_latitude_deg").isNotNull()
        & F.col("he_latitude_deg").isNotNull()
    )

    def _extent(name):
        # OurAirports always carries these; a hand-built ``oa_runways`` stub
        # (some tests) may not. A missing column is a typed null, which the
        # coalesce below then turns into the documented default -- so the
        # extent is optional input, never a hard schema requirement.
        return F.col(name).cast("double") if name in rwy.columns else F.lit(None).cast("double")

    length_nm = (
        F.coalesce(_extent("length_ft"), F.lit(DEFAULT_RWY_LENGTH_FT)) / F.lit(FT_PER_NM)
    ).alias("rwy_length_nm")
    half_width_nm = F.coalesce(
        _extent("width_ft") / F.lit(2.0) / F.lit(FT_PER_NM),
        F.lit(DEFAULT_RWY_HALF_WIDTH_M / M_PER_NM),
    ).alias("rwy_half_width_nm")

    le = rwy.select(
        F.col("airport_ident").alias("apt_ident"),
        F.col("le_ident").alias("rwy_ident"),
        F.col("le_latitude_deg").cast("double").alias("thr_lat"),
        F.col("le_longitude_deg").cast("double").alias("thr_lon"),
        bearing_deg(
            F.col("le_latitude_deg"),
            F.col("le_longitude_deg"),
            F.col("he_latitude_deg"),
            F.col("he_longitude_deg"),
        ).alias("rwy_bearing"),
        length_nm,
        half_width_nm,
        _extent("le_elevation_ft").alias("thr_elev_ft"),
    )
    he = rwy.select(
        F.col("airport_ident").alias("apt_ident"),
        F.col("he_ident").alias("rwy_ident"),
        F.col("he_latitude_deg").cast("double").alias("thr_lat"),
        F.col("he_longitude_deg").cast("double").alias("thr_lon"),
        bearing_deg(
            F.col("he_latitude_deg"),
            F.col("he_longitude_deg"),
            F.col("le_latitude_deg"),
            F.col("le_longitude_deg"),
        ).alias("rwy_bearing"),
        length_nm,
        half_width_nm,
        _extent("he_elevation_ft").alias("thr_elev_ft"),
    )
    return le.unionByName(he).filter(F.col("rwy_ident").isNotNull())


def detect_runway_movements(
    sdf: DataFrame,
    ends: DataFrame,
    thresholds: DataFrame,
    config: "EventConfig",
) -> DataFrame:
    """Name the runway and time the movement, per (track, role).

    ``ends`` is one row per (track_id, apt_ident, role) with the aerodrome
    position and elevation; ``role`` is ``"departure"`` or ``"arrival"``.

    The limits are those of the traffic library: within ``runway_max_dist_nm`` of the aerodrome,
    below ``runway_max_height_ft`` above the field, groundspeed above
    ``runway_min_groundspeed_kt``, and a vertical rate beyond
    ``runway_min_vert_rate_ftmin`` in the direction the role implies. What
    survives is the initial climb or the final descent; its median track is
    then matched against each runway's bearing.

    Timing follows from the same set rather than a second pass: the earliest
    surviving sample of a departure is the lift-off, the latest of an arrival
    is the touchdown. Both are proxies, and the benchmark's job is to report
    their bias against APDF rather than to assume it is zero.
    """
    work = sdf.join(F.broadcast(ends), on="track_id", how="inner")

    height_ft = F.col("baro_altitude_c") * FT_PER_M - F.coalesce(
        F.col("apt_elevation_ft"), F.lit(0.0)
    )
    roc = F.col("vert_rate") * FTMIN_PER_MPS
    gs_kt = F.col("velocity") * KT_PER_MPS
    dist_nm = haversine_nm(F.col("lat"), F.col("lon"), F.col("apt_lat"), F.col("apt_lon"))

    climbing = roc > F.lit(config.runway_min_vert_rate_ftmin)
    descending = roc < -F.lit(config.runway_min_vert_rate_ftmin)

    qualifies = (
        (dist_nm < F.lit(config.runway_max_dist_nm))
        & (height_ft < F.lit(config.runway_max_height_ft))
        & (gs_kt > F.lit(config.runway_min_groundspeed_kt))
        & F.when(F.col("role") == "departure", climbing).otherwise(descending)
    )
    if config.runway_window_times_checked:
        return _checked_movements(work, qualifies, thresholds, gs_kt, dist_nm, config)
    work = work.filter(qualifies)

    # traffic takes the median track of the surviving samples. The median, not
    # the mean: a single spurious heading during rotation would drag a mean off
    # the centreline, and the runway is chosen by nearest bearing.
    # The aerodrome position is carried through the grouping because the
    # parallel-runway tie-break needs it downstream; it is constant per
    # apt_ident, so grouping on it adds no rows.
    agg = work.groupBy("track_id", "apt_ident", "role", "apt_lat", "apt_lon").agg(
        F.expr("percentile_approx(heading, 0.5)").alias("median_track"),
        F.expr("percentile_approx(lat, 0.5)").alias("median_lat"),
        F.expr("percentile_approx(lon, 0.5)").alias("median_lon"),
        F.min("event_time").alias("first_time"),
        F.max("event_time").alias("last_time"),
        F.count(F.lit(1)).alias("n_samples"),
    )
    # Parenthesised deliberately: `&` binds tighter than `>=` in Python, so
    # without them this reads as `n_samples >= (4 & isNotNull)` -- which is a
    # type error here, but would silently be a different filter if the operands
    # happened to be compatible.
    agg = agg.filter((F.col("n_samples") >= F.lit(4)) & F.col("median_track").isNotNull())

    cand = agg.join(F.broadcast(thresholds), on="apt_ident", how="inner")
    cand = cand.withColumn(
        "bearing_error", angle_between(F.col("median_track"), F.col("rwy_bearing"))
    ).filter(F.col("bearing_error") <= F.lit(config.runway_max_bearing_deg))

    # Cross-track distance decides; the bearing error only breaks its ties.
    #
    # The candidates have already been filtered to within
    # ``runway_max_bearing_deg`` of the aircraft's own track, so every survivor
    # is plausibly aligned and the question left is *which strip*. Parallel
    # runways share a bearing to within a degree and sit hundreds of metres
    # apart, so ordering on bearing first would let a fraction of a degree of
    # heading noise pick the wrong one; the aircraft's offset from each
    # centreline cannot be confused that way. Bearing error then separates the
    # two *directions* of the chosen strip, which share a centreline exactly
    # and so tie on cross-track. The same discriminator traffic uses shapely
    # for, in closed form.
    cand = cand.withColumn(
        "cross_track_nm",
        cross_track_nm(
            F.col("median_lat"),
            F.col("median_lon"),
            F.col("thr_lat"),
            F.col("thr_lon"),
            F.col("rwy_bearing"),
        ),
    )
    from pyspark.sql.window import Window

    best = Window.partitionBy("track_id", "role").orderBy(
        F.col("cross_track_nm").asc(),
        F.col("bearing_error").asc(),
        F.col("rwy_ident").asc(),
    )
    return cand.withColumn("_r", F.row_number().over(best)).filter(F.col("_r") == 1).drop("_r")


#: Speed, in knots, below which a report near the airport shows the aircraft
#: on the ground: for the ground altitude (with an altitude, below
#: :data:`WINDOW_GROUND_ALTITUDE_MAX_KT`) and for the ground after a landing.
WINDOW_GROUND_MAX_KT = 50.0
WINDOW_GROUND_ALTITUDE_MAX_KT = 30.0
#: Seconds after a landing time within which a track may end without a
#: report on the ground: reception that ends on final.
WINDOW_TRACK_END_SECONDS = 120.0
#: Largest difference, in feet, between the track's ground altitude and the
#: airport's elevation that is read as the pressure of the day.
WINDOW_GROUND_MAX_OFFSET_FT = 1000.0


def _checked_movements(work, qualifies, thresholds, gs_kt, dist_nm, config) -> DataFrame:
    """:func:`detect_runway_movements` with the times checked.

    See :attr:`~opdi.config.EventConfig.runway_window_times_checked`.

    One pass over the reports within ``runway_max_dist_nm`` of the airport
    gives both the runway (from the reports that qualify, as without the
    check) and the track's ground: per visit of the airport (the reports
    near it split where more than ``airport_trace_gap_seconds`` pass between
    two), the median altitude of its reports that are flagged on the ground
    or slower than :data:`WINDOW_GROUND_ALTITUDE_MAX_KT`, and the time of its last report on
    the ground (flagged, or slower than :data:`WINDOW_GROUND_MAX_KT`). The
    qualifying reports are kept in a list, so they are not read twice.

    Of the qualifying reports, only those at most
    ``runway_window_max_height_ft`` above the ground (else the elevation) and
    at most ``runway_window_max_cross_track_nm`` from the chosen runway's
    extended centre line give the time (the first for a take-off, the last
    for a landing). A landing time needs a report on the ground after it,
    unless the track ends within :data:`WINDOW_TRACK_END_SECONDS`
    (``last_seen`` of the flight list).

    Adds, for the report the time comes from: ``at_lat``, ``at_lon``,
    ``at_altitude_ft``, ``at_cum_nm``, ``at_cum_s``, and ``at_track_edge``
    (it is the track's first report for a take-off, its last for a landing).
    """
    for name, dtype in (
        ("on_ground", "boolean"),
        ("cumulative_distance_nm", "double"),
        ("cumulative_time_s", "double"),
        ("first_seen", "timestamp"),
        ("last_seen", "timestamp"),
    ):
        if name not in work.columns:
            work = work.withColumn(name, F.lit(None).cast(dtype))
    flagged = F.coalesce(F.col("on_ground"), F.lit(False))
    on_ground = flagged | F.coalesce(gs_kt < F.lit(WINDOW_GROUND_MAX_KT), F.lit(False))
    alt_ground = flagged | F.coalesce(gs_kt < F.lit(WINDOW_GROUND_ALTITUDE_MAX_KT), F.lit(False))
    alt_ft = F.col("baro_altitude_c") * F.lit(FT_PER_M)
    near = work.filter(dist_nm < F.lit(config.runway_max_dist_nm)).withColumn(
        "_q", F.coalesce(qualifies, F.lit(False))
    )
    # The ground altitude of each visit of the airport: the reports near it
    # split where more than ``airport_trace_gap_seconds`` pass between two of
    # them (a landing and the take-off after a turnaround read the ground
    # apart, and the air pressure changes in between).
    from pyspark.sql.window import Window

    t = F.col("event_time").cast("double")
    visits = Window.partitionBy("track_id", "apt_ident", "role").orderBy("event_time")
    near = near.withColumn(
        "_visit",
        F.sum(
            F.when(
                t - F.lag(t).over(visits) > F.lit(float(config.airport_trace_gap_seconds)), 1
            ).otherwise(0)
        ).over(visits.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
    )
    near = near.withColumn(
        "_g_visit_ft",
        F.percentile_approx(F.when(alt_ground, alt_ft), 0.5).over(
            Window.partitionBy("track_id", "apt_ident", "role", "_visit")
        ),
    )
    q = F.col("_q")
    agg = near.groupBy("track_id", "apt_ident", "role", "apt_lat", "apt_lon").agg(
        F.percentile_approx(F.when(q, F.col("heading")), 0.5).alias("median_track"),
        F.percentile_approx(F.when(q, F.col("lat")), 0.5).alias("median_lat"),
        F.percentile_approx(F.when(q, F.col("lon")), 0.5).alias("median_lon"),
        F.min(F.when(q, F.col("event_time"))).alias("first_time"),
        F.max(F.when(q, F.col("event_time"))).alias("last_time"),
        F.count(F.when(q, F.lit(1))).alias("n_samples"),
        F.max(F.when(on_ground, F.col("event_time"))).alias("_g_last"),
        F.first("apt_elevation_ft", ignorenulls=True).alias("_elev_ft"),
        F.first("first_seen", ignorenulls=True).alias("_first_seen"),
        F.first("last_seen", ignorenulls=True).alias("_last_seen"),
        F.collect_list(
            F.when(
                q,
                F.struct(
                    F.col("event_time").alias("t"),
                    F.col("lat").alias("lat"),
                    F.col("lon").alias("lon"),
                    alt_ft.alias("alt_ft"),
                    F.col("cumulative_distance_nm").cast("double").alias("cum_nm"),
                    F.col("cumulative_time_s").cast("double").alias("cum_s"),
                    F.col("_g_visit_ft").alias("g_alt_ft"),
                ),
            )
        ).alias("_reports"),
    )
    agg = agg.filter((F.col("n_samples") >= F.lit(4)) & F.col("median_track").isNotNull())
    cand = agg.join(F.broadcast(thresholds), on="apt_ident", how="inner")
    cand = cand.withColumn(
        "bearing_error", angle_between(F.col("median_track"), F.col("rwy_bearing"))
    ).filter(F.col("bearing_error") <= F.lit(config.runway_max_bearing_deg))
    cand = cand.withColumn(
        "cross_track_nm",
        cross_track_nm(
            F.col("median_lat"),
            F.col("median_lon"),
            F.col("thr_lat"),
            F.col("thr_lon"),
            F.col("rwy_bearing"),
        ),
    )
    from pyspark.sql.window import Window

    best = Window.partitionBy("track_id", "role").orderBy(
        F.col("cross_track_nm").asc(),
        F.col("bearing_error").asc(),
        F.col("rwy_ident").asc(),
    )
    chosen = cand.withColumn("_r", F.row_number().over(best)).filter(F.col("_r") == 1).drop("_r")
    return _checked_times(chosen, config)


def _checked_times(chosen, config) -> DataFrame:
    """The time of each chosen runway movement from its checked reports."""
    keys = ["track_id", "role"]
    elevation = F.coalesce(F.col("_elev_ft"), F.lit(0.0))
    ground = F.col("r.g_alt_ft")
    ref = F.coalesce(
        F.when(F.abs(ground - elevation) <= F.lit(WINDOW_GROUND_MAX_OFFSET_FT), ground),
        elevation,
    )
    rep = chosen.select(
        *keys,
        "_elev_ft",
        "thr_lat",
        "thr_lon",
        "rwy_bearing",
        F.explode("_reports").alias("r"),
    ).withColumn("_ref", ref)
    rep = rep.filter(
        (F.col("r.alt_ft") - F.col("_ref") <= F.lit(config.runway_window_max_height_ft))
        & (
            cross_track_nm(
                F.col("r.lat"),
                F.col("r.lon"),
                F.col("thr_lat"),
                F.col("thr_lon"),
                F.col("rwy_bearing"),
            )
            <= F.lit(config.runway_window_max_cross_track_nm)
        )
    )
    dep = F.col("role") == F.lit("departure")
    # The first report for a take-off, the last for a landing.
    order = F.when(dep, F.col("r.t").cast("double")).otherwise(-F.col("r.t").cast("double"))
    picked = rep.groupBy(*keys).agg(
        F.min_by("r", order).alias("_p"),
    )
    out = chosen.drop("_reports").join(picked, keys, "inner")
    t = F.col("_p.t").cast("double")
    ends_soon = F.coalesce(
        F.col("_last_seen").cast("double") - t <= F.lit(WINDOW_TRACK_END_SECONDS), F.lit(False)
    )
    ground_after = F.coalesce(F.col("_g_last").cast("double") > t, F.lit(False))
    out = out.filter(dep | ground_after | ends_soon | F.col("_last_seen").isNull())
    edge = F.when(
        dep,
        F.coalesce(F.col("_p.cum_s") <= F.lit(1.0), F.lit(False))
        | F.coalesce(F.abs(t - F.col("_first_seen").cast("double")) <= F.lit(1.0), F.lit(False)),
    ).otherwise(
        F.coalesce(F.abs(F.col("_last_seen").cast("double") - t) <= F.lit(1.0), F.lit(False))
    )
    return (
        out.withColumn("first_time", F.when(dep, F.col("_p.t")).otherwise(F.col("first_time")))
        .withColumn("last_time", F.when(dep, F.col("last_time")).otherwise(F.col("_p.t")))
        .withColumn("at_lat", F.col("_p.lat"))
        .withColumn("at_lon", F.col("_p.lon"))
        .withColumn("at_altitude_ft", F.col("_p.alt_ft"))
        .withColumn("at_cum_nm", F.col("_p.cum_nm"))
        .withColumn("at_cum_s", F.col("_p.cum_s"))
        .withColumn("at_track_edge", edge)
        .drop("_p", "_g_last", "_elev_ft", "_first_seen", "_last_seen")
    )
