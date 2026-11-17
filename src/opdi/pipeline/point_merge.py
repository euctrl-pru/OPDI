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
from opdi.pipeline.runways import FTMIN_PER_MPS, KT_PER_MPS
from opdi.utils.geometry import bearing_deg as _bearing_deg
from opdi.versions import (  # stamped on the events and measurements
    LEGACY_PMS_VERSION,
    PMS_VERSION,
)

#: The settings that change the point merge events: with all of them at
#: their values of release v0.0.2 the events carry :data:`LEGACY_PMS_VERSION`.
PMS_SETTINGS = (
    "pms_extend_leg_exit",
    "pms_join_split_legs",
    "pms_leg_must_turn",
    "pms_merge_point_conditions",
    "altitude_from_cleaning",
    "altitude_plausible_min_ft",
    "altitude_plausible_max_ft",
)


def pms_version(config: EventConfig) -> str:
    """The ``version`` of the point merge events and measurements under ``config``."""
    if config.differs_from_release(PMS_SETTINGS):
        return PMS_VERSION
    return LEGACY_PMS_VERSION


#: The measurements read off ``pms-leg-exit``'s info (see ``events.EXTRA_MEASURES``).
PMS_MEASURES = [
    ("pms-leg-exit", "duration_s", "Point merge leg time (s)", PMS_VERSION),
    ("pms-leg-exit", "distance_nm", "Point merge leg distance (NM)", PMS_VERSION),
    ("pms-leg-exit", "direct_to_distance_nm", "Point merge direct-to distance (NM)", PMS_VERSION),
]

_KEY = ["track_id", PIECE, "pms_id", "merge_point"]


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
    if config.pms_merge_point_conditions:
        # The speed and vertical rate at a merge-point passage, read the same
        # way: the last value the track had.
        for c in ("velocity", "vert_rate"):
            if c in df.columns:
                df = df.withColumn(f"_{c}_known", F.last(c, ignorenulls=True).over(known))
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
    if config.pms_extend_leg_exit:
        df = df.withColumn("_tangential", F.coalesce(tangential, lit(False)))
    if config.pms_leg_must_turn:
        df = df.withColumn("_from_mp", from_mp).withColumn(
            "_d_hdg", _wrap_deg(col("heading") - F.lag("heading").over(base))
        )

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


def _in_leg(df: DataFrame, config: EventConfig) -> DataFrame:
    """``_in_leg``: the reports of a leg, with its exit moved to the end of the arc.

    See :attr:`~opdi.config.EventConfig.pms_extend_leg_exit`. A report after
    a run of accepted reports continues the run while it moves along the arc
    and stays within ``pms_leg_exit_tolerance_nm`` of the distance of the
    last accepted report; the first report that does not ends it. All of it
    is running windows over the order the leg test already uses.
    """
    base = Window.partitionBy(*_KEY).orderBy("_ts")
    running = base.rowsBetween(Window.unboundedPreceding, 0)
    if not config.pms_extend_leg_exit:
        return df.withColumn("_in_leg", col("_on_leg"))
    last_leg_d = F.last(F.when(col("_on_leg"), col("d_nm")), ignorenulls=True).over(running)
    continues = col("_tangential") & (
        F.abs(col("d_nm") - last_leg_d) <= config.pms_leg_exit_tolerance_nm
    )
    df = df.withColumn("_ok", col("_on_leg") | F.coalesce(continues, lit(False)))
    last_on = F.max(F.when(col("_on_leg"), col("_ts"))).over(running)
    last_break = F.max(F.when(~col("_ok"), col("_ts"))).over(running)
    chained = col("_ok") & last_on.isNotNull() & (last_break.isNull() | (last_on > last_break))
    return df.withColumn("_in_leg", col("_on_leg") | F.coalesce(chained, lit(False))).drop("_ok")


def _legs(df: DataFrame, config: EventConfig) -> DataFrame:
    base = Window.partitionBy(*_KEY).orderBy("_ts")
    df = _in_leg(df, config)
    prev_in = F.coalesce(F.lag("_in_leg").over(base), lit(False))
    starts = col("_in_leg") & ~prev_in
    if config.pms_join_split_legs:
        # A new stretch continues the leg before it when it starts soon after
        # and turns the same way about the merge point.
        before = base.rowsBetween(Window.unboundedPreceding, -1)
        last_in_ts = F.max(F.when(col("_in_leg"), col("_ts"))).over(before)
        last_sense = F.last(F.when(col("_in_leg"), F.signum(col("_sweep"))), ignorenulls=True).over(
            before
        )
        joined = ((col("_ts") - last_in_ts) <= config.pms_join_max_gap_seconds) & (
            F.signum(col("_sweep")) == last_sense
        )
        starts = starts & ~F.coalesce(joined, lit(False))
    df = df.withColumn(
        "_run", F.sum(starts.cast("int")).over(base.rowsBetween(Window.unboundedPreceding, 0))
    )
    turn_aggs = []
    if config.pms_leg_must_turn:
        # The heading change inside the leg: the step from a report of the
        # leg to the next one (not the step into the leg).
        df = df.withColumn("_prev_in", prev_in)
        turn_aggs = [
            F.sum(F.when(col("_prev_in"), col("_d_hdg"))).alias("_turn_deg"),
            F.min_by("_from_mp", "_ts").alias("_from_mp_start"),
            F.max_by("_from_mp", "_ts").alias("_from_mp_end"),
        ]
    legs = (
        df.filter(col("_in_leg"))
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
            *turn_aggs,
        )
    )
    legs = legs.withColumn("duration_s", col("end_ts") - col("start_ts"))
    if config.pms_leg_must_turn:
        # On an arc the heading turns with the bearing from the merge point;
        # a straight segment that grazes the circle does not. A leg that
        # climbs is a missed approach or a climbing turn near the airport.
        leg_sweep = F.abs(_wrap_deg(col("_from_mp_end") - col("_from_mp_start")))
        turns = (col("duration_s") >= config.pms_leg_turn_test_max_seconds) | (
            F.abs(F.coalesce(col("_turn_deg"), lit(0.0)))
            >= config.pms_leg_min_turn_share_pct / 100.0 * leg_sweep
        )
        climbs = (col("end_alt_ft") - col("start_alt_ft")) > config.pms_leg_max_climb_ft
        legs = legs.filter(F.coalesce(turns, lit(True)) & ~F.coalesce(climbs, lit(False))).drop(
            "_turn_deg", "_from_mp_start", "_from_mp_end"
        )
    # A leg counts when the track then went to the merge point: within the
    # lookahead it came inside pms_merge_point_radius_nm, from a leg flown at
    # least twice that far out. Whether it flew a straight, aligned direct is
    # kept as information (``direct_aligned``): on the runway 10 system at
    # Dublin the turn-in is a curve to the fix, not a straight line, and a
    # strict alignment test would lose every one of those legs.
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


def _event(df: DataFrame, kind: str, type_name: str, version: str = PMS_VERSION) -> DataFrame:
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
        lit(version).alias("version"),
    )


#: Event types that mark a landing or a take-off, from either detector.
_LANDING_TYPES = ("ALDT", "touchdown")
_TAKE_OFF_TYPES = ("ATOT", "airborne")


def _merge_point_conditions(
    closest: DataFrame, runway_events: Optional[DataFrame], config: EventConfig
) -> DataFrame:
    """The merge-point passages that can be arrivals through the system.

    See :attr:`~opdi.config.EventConfig.pms_merge_point_conditions`. The
    passages are few (thousands a day); they are joined with the take-off
    and landing times of their tracks when those are known.
    """
    kt = col("_mp_velocity") * KT_PER_MPS
    ftmin = col("_mp_vert_rate") * FTMIN_PER_MPS
    closest = closest.filter(
        (kt.isNull() | (kt >= config.pms_merge_point_min_speed_kt))
        & (ftmin.isNull() | (ftmin <= config.pms_merge_point_max_climb_ftmin))
    )
    approach = lit(None).cast("double")
    if runway_events is not None:
        rwy = runway_events.filter(col("type").isin(*_LANDING_TYPES, *_TAKE_OFF_TYPES)).select(
            col("track_id").alias("_r_track"),
            col("type").isin(*_LANDING_TYPES).alias("_r_landing"),
            col("event_time").cast("double").alias("_r_ts"),
            F.get_json_object(col("info"), "$.apt_icao").alias("_r_apt"),
            F.get_json_object(col("info"), "$.runway").alias("_r_rwy"),
        )
        landing_here = (
            col("_r_landing") & (col("_r_ts") > col("mp_ts")) & (col("_r_apt") == col("apt_icao"))
        )
        keys = closest.columns
        closest = (
            closest.join(rwy, closest.track_id == rwy._r_track, "left")
            .groupBy(*keys)
            .agg(
                F.min(
                    F.when(~col("_r_landing") & (col("_r_ts") > col("mp_ts")), col("_r_ts"))
                ).alias("_next_take_off"),
                F.min(F.when(landing_here, col("_r_ts"))).alias("_next_landing"),
                F.min_by(col("_r_rwy"), F.when(landing_here, col("_r_ts"))).alias("_land_rwy"),
            )
        )
        # The runway direction a system serves, from its name (``RJTT-34L``
        # serves 340 degrees; ``LIME-ALL`` names none), against the runway
        # landed. A system serves the parallel and near-parallel runways of
        # the same flow (LTFM-16NW serves 16L, 17R and 18).
        served = F.regexp_extract(col("pms_id"), r"-(\d{2})", 1)
        landed = F.regexp_extract(F.coalesce(col("_land_rwy"), lit("")), r"^(\d{2})", 1)
        apart_deg = F.abs(
            F.pmod(served.cast("int") * 10 - landed.cast("int") * 10 + 180, lit(360)) - 180
        )
        same_flow = apart_deg <= config.pms_runway_direction_tolerance_deg
        # No take-off between the passage and the landing that follows it
        # (with no landing after it, no take-off after it): on a track that
        # holds two flights, the first one's passages stay.
        no_take_off_between = col("_next_take_off").isNull() | (
            col("_next_landing").isNotNull() & (col("_next_take_off") > col("_next_landing"))
        )
        closest = closest.filter(
            no_take_off_between
            & ((served == "") | (landed == "") | F.coalesce(same_flow, lit(True)))
        )
        approach = col("_next_landing")
    one = Window.partitionBy("track_id", F.coalesce(approach, lit(-1.0))).orderBy(
        col("via_leg").desc(), col("closest_nm").asc(), col("merge_point").asc()
    )
    return (
        closest.withColumn("_rank", F.row_number().over(one))
        .filter(col("_rank") == 1)
        .drop("_rank")
    )


def calculate_point_merge_events(
    geo: DataFrame,
    pms_ref: Optional[DataFrame],
    config: EventConfig,
    runway_events: Optional[DataFrame] = None,
) -> Optional[DataFrame]:
    """Leg pairs and merge-point overflights for the arrivals in ``geo``.

    ``geo`` is the step 04 frame with ``ades`` attached (the aerodrome
    geometry join); ``pms_ref`` is the table from
    ``reference.point_merge.point_merge_systems_for_day``. None when the family
    is off, the table is absent, or the frame has no ``ades``.
    ``runway_events`` (``track_id, type, event_time, info``), the take-off
    and landing times of the step, are read by
    :attr:`~opdi.config.EventConfig.pms_merge_point_conditions`; without them
    the conditions on take-off, runway and approach are not applied.
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
    version = pms_version(config)
    out = _event(legs, "start", "pms-leg-entry", version).unionByName(
        _event(legs, "end", "pms-leg-exit", version)
    )

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
            *[
                F.min_by(f"_{c}_known", "d_nm").alias(f"_mp_{c}")
                for c in ("velocity", "vert_rate")
                if config.pms_merge_point_conditions and f"_{c}_known" in df.columns
            ],
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
    if config.pms_merge_point_conditions:
        for c in ("velocity", "vert_rate"):
            if f"_mp_{c}" not in closest.columns:
                closest = closest.withColumn(f"_mp_{c}", lit(None).cast("double"))
        closest = _merge_point_conditions(closest, runway_events, config)
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
    out = out.unionByName(_event(closest, "mp", "pms-merge-point", version))
    return out
