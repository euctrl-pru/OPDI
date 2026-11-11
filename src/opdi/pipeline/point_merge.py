"""Point merge: ``pms-leg-entry`` / ``pms-leg-exit`` / ``pms-merge-point``.

A Point Merge System (PMS) sequences arrivals with one *merge point* and
*sequencing legs* that are arcs at constant distance from it: the controller
keeps an aircraft on the leg to stretch its path, then clears it "direct" to
the merge point. In a track that reads as a stretch at nearly constant
distance from the merge point, inside the published distance band, moving
across the bearing rather than along it, followed by a straight run whose
track points at the merge point and ends near it. Every published detector
needs the merge point's position, so the test runs against the curated table
in ``reference/data/point_merge_systems.csv`` for arrivals at the airports it
lists. Thresholds are on ``EventConfig.pms_*``.

All of it is column and window expressions partitioned by
``(track_id, piece, merge point)``.
"""

from typing import Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit

from opdi.config import EventConfig
from opdi.pipeline.holding import FT_PER_M, _haversine_nm, _wrap_deg
from opdi.pipeline.holes import PIECE

#: Version stamped on the three events and on their measurements.
PMS_VERSION = "pms_v0.1.0"

#: The measurements read off ``pms-leg-exit``'s info (see ``events.EXTRA_MEASURES``).
PMS_MEASURES = [
    ("pms-leg-exit", "duration_s", "Point merge leg time (s)", PMS_VERSION),
    ("pms-leg-exit", "distance_nm", "Point merge leg distance (NM)", PMS_VERSION),
    ("pms-leg-exit", "direct_to_distance_nm", "Point merge direct-to distance (NM)", PMS_VERSION),
]

_KEY = ["track_id", PIECE, "pms_id", "merge_point"]


def _bearing_deg(lat1, lon1, lat2, lon2):
    """Initial bearing from point 1 to point 2, degrees in [0, 360)."""
    p1, p2 = F.radians(lat1), F.radians(lat2)
    dl = F.radians(lon2 - lon1)
    y = F.sin(dl) * F.cos(p2)
    x = F.cos(p1) * F.sin(p2) - F.sin(p1) * F.cos(p2) * F.cos(dl)
    return (F.degrees(F.atan2(y, x)) + 360.0) % 360.0


def _candidates(geo: DataFrame, pms_ref: DataFrame, config: EventConfig) -> DataFrame:
    ref = pms_ref.select(
        col("apt_icao").alias("_apt"),
        "pms_id",
        "merge_point",
        col("lat").alias("_mp_lat"),
        col("lon").alias("_mp_lon"),
        "legs_mp_min_nm",
        "legs_mp_max_nm",
    )
    df = geo.filter(col("ades").isNotNull() & col("lat").isNotNull() & col("lon").isNotNull())
    df = df.join(F.broadcast(ref), df["ades"] == ref["_apt"], "inner")
    df = (
        df.withColumn("_ts", col("event_time").cast("double"))
        .withColumn("_alt_ft", col("baro_altitude_c") * FT_PER_M)
        .withColumn("d_nm", _haversine_nm(col("lat"), col("lon"), col("_mp_lat"), col("_mp_lon")))
        .withColumn("brg_deg", _bearing_deg(col("lat"), col("lon"), col("_mp_lat"), col("_mp_lon")))
    )
    # Cleaning blanks heading, speed and vertical rate together when none of
    # them changed between broadcasts: exactly what happens on a straight
    # direct. Read the last heading the track had, within the piece.
    if PIECE not in df.columns:
        df = df.withColumn(PIECE, lit(0))
    known = (
        Window.partitionBy("track_id", PIECE)
        .orderBy("_ts")
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    df = df.withColumn("heading", F.last("heading", ignorenulls=True).over(known))
    return df.filter(
        (col("d_nm") <= config.pms_candidate_radius_nm)
        & (col("_alt_ft") <= config.pms_candidate_max_altitude_ft)
    ).withColumn("apt_icao", col("_apt"))


def _leg_and_direct_flags(df: DataFrame, config: EventConfig) -> DataFrame:
    half = int(config.pms_leg_window_seconds)
    base = Window.partitionBy(*_KEY).orderBy("_ts")
    w = base.rangeBetween(-half, half)
    # Bearing *from* the merge point to the aircraft: it sweeps along a leg.
    from_mp = (col("brg_deg") + 180.0) % 360.0
    off = F.abs(_wrap_deg(col("heading") - col("brg_deg")))
    tangential = F.when(col("heading").isNotNull(), (off >= 60.0) & (off <= 120.0))
    d_range = F.max("d_nm").over(w) - F.min("d_nm").over(w)
    d_mean = F.avg("d_nm").over(w)
    sweep = _wrap_deg(F.last(from_mp).over(w) - F.first(from_mp).over(w))
    tang_pct = F.avg(tangential.cast("int")).over(w) * 100.0
    on_leg = (
        (d_range <= config.pms_leg_distance_band_nm)
        & (d_mean >= col("legs_mp_min_nm") - config.pms_leg_distance_margin_nm)
        & (d_mean <= col("legs_mp_max_nm") + config.pms_leg_distance_margin_nm)
        & (F.abs(sweep) >= config.pms_leg_min_bearing_sweep_deg)
        & (tang_pct >= config.pms_leg_tangential_min_pct)
    )
    df = df.withColumn("_on_leg", F.coalesce(on_leg, lit(False))).withColumn("_sweep", sweep)

    # The direct: track within pms_direct_align_deg of the bearing to the merge
    # point and the distance falling. A row is "long aligned" once its run has
    # lasted pms_direct_min_duration_seconds.
    aligned = (off < config.pms_direct_align_deg) & (col("d_nm") < F.lag("d_nm").over(base))
    df = df.withColumn("_aligned", F.coalesce(aligned, lit(False)))
    a_start = col("_aligned") & ~F.coalesce(F.lag("_aligned").over(base), lit(False))
    run_start = F.max(F.when(a_start, col("_ts"))).over(
        base.rowsBetween(Window.unboundedPreceding, 0)
    )
    long_aligned = col("_aligned") & (
        (col("_ts") - run_start) >= config.pms_direct_min_duration_seconds
    )
    df = df.withColumn("_long_aligned", F.coalesce(long_aligned, lit(False)))
    ahead = base.rangeBetween(0, int(config.pms_direct_lookahead_seconds))
    df = df.withColumn("_direct_ok", F.max(col("_long_aligned").cast("int")).over(ahead) >= 1)
    df = df.withColumn("_min_d_ahead", F.min("d_nm").over(ahead))
    return df


def _legs(df: DataFrame, config: EventConfig) -> DataFrame:
    base = Window.partitionBy(*_KEY).orderBy("_ts")
    starts = col("_on_leg") & ~F.coalesce(F.lag("_on_leg").over(base), lit(False))
    df = df.withColumn(
        "_run", F.sum(starts.cast("int")).over(base.rowsBetween(Window.unboundedPreceding, 0))
    )
    legs = (
        df.filter(col("_on_leg"))
        .groupBy(*_KEY, "_run", "apt_icao")
        .agg(
            F.min("_ts").alias("start_ts"),
            F.max("_ts").alias("end_ts"),
            F.min_by("event_time", "_ts").alias("start_time"),
            F.max_by("event_time", "_ts").alias("end_time"),
            F.min_by("lat", "_ts").alias("start_lat"),
            F.min_by("lon", "_ts").alias("start_lon"),
            F.max_by("lat", "_ts").alias("end_lat"),
            F.max_by("lon", "_ts").alias("end_lon"),
            F.min_by("_alt_ft", "_ts").alias("start_alt_ft"),
            F.max_by("_alt_ft", "_ts").alias("end_alt_ft"),
            F.min_by("cumulative_distance_nm", "_ts").alias("start_dist_nm"),
            F.max_by("cumulative_distance_nm", "_ts").alias("end_dist_nm"),
            F.min_by("cumulative_time_s", "_ts").alias("start_cum_s"),
            F.max_by("cumulative_time_s", "_ts").alias("end_cum_s"),
            F.avg("d_nm").alias("leg_distance_nm"),
            F.percentile_approx("_alt_ft", 0.5).alias("leg_level_ft"),
            F.sum("_sweep").alias("sweep_deg"),
            F.max_by("d_nm", "_ts").alias("direct_to_distance_nm"),
            F.max_by("_direct_ok", "_ts").alias("direct_ok"),
            F.max_by("_min_d_ahead", "_ts").alias("min_d_ahead"),
        )
    )
    legs = legs.withColumn("duration_s", col("end_ts") - col("start_ts"))
    # A leg counts when the track then went to the merge point: within the
    # lookahead it came inside pms_merge_point_radius_nm, from a leg flown at
    # least twice that far out. Whether it flew a straight, aligned direct is
    # kept as information (``direct_aligned``): on the runway 10 system at
    # Dublin the turn-in is a curve to the fix, not a straight line, and a
    # strict alignment gate lost every one of those legs.
    legs = legs.filter(
        (col("duration_s") >= config.pms_leg_min_duration_seconds)
        & (col("min_d_ahead") <= config.pms_merge_point_radius_nm)
        & (col("direct_to_distance_nm") >= 2 * config.pms_merge_point_radius_nm)
    )
    # Two merge points of one system can sit within a mile of each other
    # (Dublin's 28L and 28R points): a leg that passes for both is kept once,
    # for the point the track came closest to. Legs of one track are grouped
    # by time overlap (a new group starts when a leg begins after every
    # earlier leg has ended), not by start minute, so two runs five seconds
    # apart across a minute boundary are still one leg.
    by_start = Window.partitionBy("track_id", PIECE).orderBy("start_ts", "merge_point")
    prev_max_end = F.max("end_ts").over(by_start.rowsBetween(Window.unboundedPreceding, -1))
    legs = legs.withColumn("_new_group", F.coalesce(col("start_ts") > prev_max_end, lit(True)))
    legs = legs.withColumn(
        "_group",
        F.sum(col("_new_group").cast("int")).over(
            by_start.rowsBetween(Window.unboundedPreceding, 0)
        ),
    )
    one = Window.partitionBy("track_id", PIECE, "_group").orderBy(
        col("min_d_ahead").asc(), col("merge_point").asc()
    )
    legs = (
        legs.withColumn("_rank", F.row_number().over(one))
        .filter(col("_rank") == 1)
        .drop("_rank", "_new_group", "_group")
    )
    seq = Window.partitionBy("track_id").orderBy("start_ts")
    return legs.withColumn("leg_seq", F.row_number().over(seq))


def _event(df: DataFrame, kind: str, type_name: str) -> DataFrame:
    return df.select(
        col("track_id"),
        lit(type_name).alias("type"),
        col(f"{kind}_time").alias("event_time"),
        col(f"{kind}_lon").alias("lon"),
        col(f"{kind}_lat").alias("lat"),
        col(f"{kind}_alt_ft").alias("altitude_ft"),
        col(f"{kind}_dist_nm").alias("cumulative_distance_nm"),
        col(f"{kind}_cum_s").cast("long").alias("cumulative_time_s"),
        col("info"),
        lit(PMS_VERSION).alias("version"),
    )


def calculate_point_merge_events(
    geo: DataFrame, pms_ref: Optional[DataFrame], config: EventConfig
) -> Optional[DataFrame]:
    """Leg pairs and merge-point overflights for the arrivals in ``geo``.

    ``geo`` is the step 04 frame with ``ades`` attached (the aerodrome
    geometry join); ``pms_ref`` is the table from
    ``reference.point_merge.point_merge_systems_for_day``. None when the family
    is off, the table is absent, or the frame has no ``ades``.
    """
    if not config.emit_point_merge or pms_ref is None or "ades" not in geo.columns:
        return None
    df = _candidates(geo, pms_ref, config)
    if PIECE not in df.columns:
        df = df.withColumn(PIECE, lit(0))
    df = _leg_and_direct_flags(df, config)
    legs = _legs(df, config)

    leg_info = F.to_json(
        F.struct(
            col("apt_icao"),
            col("pms_id"),
            col("merge_point"),
            col("leg_seq"),
            F.when(col("sweep_deg") > 0, lit("cw")).otherwise(lit("ccw")).alias("direction"),
            F.round(col("leg_distance_nm"), 2).alias("leg_distance_nm"),
            F.round(col("leg_level_ft"), 0).alias("leg_level_ft"),
            col("duration_s"),
            F.round(col("end_dist_nm") - col("start_dist_nm"), 2).alias("distance_nm"),
            F.round(col("direct_to_distance_nm"), 2).alias("direct_to_distance_nm"),
            col("direct_ok").alias("direct_aligned"),
        )
    )
    legs = legs.withColumn("info", leg_info)
    out = _event(legs, "start", "pms-leg-entry").unionByName(_event(legs, "end", "pms-leg-exit"))

    # Closest approach to a system's merge points, when inside the radius:
    # one event per (track, piece, pms_id), for the point come closest to,
    # since the points of one system can be a mile apart and the same sample
    # would otherwise give two events with one id.
    closest = (
        df.groupBy(*_KEY, "apt_icao")
        .agg(
            F.min("d_nm").alias("closest_nm"),
            F.min_by("_ts", "d_nm").alias("mp_ts"),
            F.min_by("event_time", "d_nm").alias("mp_time"),
            F.min_by("lat", "d_nm").alias("mp_lat"),
            F.min_by("lon", "d_nm").alias("mp_lon"),
            F.min_by("_alt_ft", "d_nm").alias("mp_alt_ft"),
            F.min_by("cumulative_distance_nm", "d_nm").alias("mp_dist_nm"),
            F.min_by("cumulative_time_s", "d_nm").alias("mp_cum_s"),
        )
        .filter(col("closest_nm") <= config.pms_merge_point_radius_nm)
    )
    one_mp = Window.partitionBy("track_id", PIECE, "pms_id").orderBy(
        col("closest_nm").asc(), col("merge_point").asc()
    )
    closest = (
        closest.withColumn("_rank", F.row_number().over(one_mp))
        .filter(col("_rank") == 1)
        .drop("_rank")
    )
    via = legs.select(*[col(k).alias(f"_l_{k}") for k in _KEY], col("end_ts").alias("_l_end"))
    cond = (
        (col("track_id") == col("_l_track_id"))
        & (col(PIECE) == col(f"_l_{PIECE}"))
        & (col("pms_id") == col("_l_pms_id"))
        & (col("mp_ts") >= col("_l_end"))
        & (col("mp_ts") - col("_l_end") <= config.pms_direct_lookahead_seconds)
    )
    closest = closest.join(via, cond, "left").withColumn("via_leg", col("_l_end").isNotNull())
    closest = closest.dropDuplicates(_KEY)
    mp_info = F.to_json(
        F.struct(
            col("apt_icao"),
            col("pms_id"),
            col("merge_point"),
            F.round(col("closest_nm"), 2).alias("closest_nm"),
            col("via_leg"),
        )
    )
    closest = closest.withColumn("info", mp_info)
    out = out.unionByName(_event(closest, "mp", "pms-merge-point"))
    return out
