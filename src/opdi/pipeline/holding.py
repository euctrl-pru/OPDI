"""Holding patterns: ``holding-start`` / ``holding-end``.

What a hold looks like in a track, and what every published detector agrees
on: the aircraft turns through at least one near-complete circle **in one
rotational sense** within a few minutes, stays inside a small area, and keeps
roughly one level. Vectoring turns alternate sense, a go-around climbs away,
an aerodrome circuit is low and slow. So the rule sums the *signed* heading
change over a sliding window and asks four things of it: enough turn, nearly
all of it one way, little displacement for the distance flown, and a flat
altitude band. Samples flagged by the trailing, the leading or the centred
window form an episode; an episode is kept when it lasts a circuit and turns a circuit.

The thresholds and the numbers behind them are documented on the
``EventConfig.holding_*`` fields. Everything here is column and window
expressions partitioned by ``(track_id, piece)``: nothing is computed across
a reception hole.
"""

from typing import Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit

from opdi.config import EventConfig
from opdi.pipeline.holes import PIECE

#: Version stamped on the two events and on their measurements.
HOLDING_VERSION = "holding_v0.1.0"

#: The measurements read off ``holding-end``'s info (see ``events.EXTRA_MEASURES``).
HOLDING_MEASURES = [
    ("holding-end", "duration_s", "Holding duration (s)", HOLDING_VERSION),
    ("holding-end", "distance_nm", "Holding distance (NM)", HOLDING_VERSION),
    ("holding-end", "circuits", "Holding circuits", HOLDING_VERSION),
]

FT_PER_M = 3.28084
KT_PER_MS = 1.94384
EARTH_RADIUS_NM = 3440.065


def _haversine_nm(lat1, lon1, lat2, lon2):
    """Great-circle distance in NM between two (lat, lon) column pairs in degrees."""
    p1, p2 = F.radians(lat1), F.radians(lat2)
    dphi = F.radians(lat2 - lat1)
    dlmb = F.radians(lon2 - lon1)
    a = F.sin(dphi / 2) ** 2 + F.cos(p1) * F.cos(p2) * F.sin(dlmb / 2) ** 2
    return lit(2 * EARTH_RADIUS_NM) * F.asin(F.sqrt(a))


def _wrap_deg(x):
    """Wrap a heading difference to (-180, 180]."""
    return ((x + 540.0) % 360.0) - 180.0


def _turn_series(df: DataFrame, config: EventConfig) -> DataFrame:
    """Per-sample signed heading change ``d_hdg`` (degrees) and ``turning``.

    The difference is taken against the last sample that *had* a heading, and
    over the time since that sample, so a stretch of masked headings does not
    read as a turn. A difference across a silence longer than
    ``holding_max_sample_gap_seconds``, or faster than
    ``holding_turn_rate_spike_deg`` per second, is dropped (NULL).
    """
    w = Window.partitionBy("track_id", PIECE).orderBy("_ts")
    before = w.rowsBetween(Window.unboundedPreceding, -1)
    df = df.withColumn("_hdg_ts", F.when(col("heading").isNotNull(), col("_ts")))
    df = df.withColumn("_prev_hdg", F.last("heading", ignorenulls=True).over(before))
    df = df.withColumn("_prev_hdg_ts", F.last("_hdg_ts", ignorenulls=True).over(before))
    dt = col("_ts") - col("_prev_hdg_ts")
    raw = _wrap_deg(col("heading") - col("_prev_hdg"))
    rate = F.abs(raw) / dt
    d_hdg = F.when(
        col("heading").isNotNull() & col("_prev_hdg").isNotNull()
        & (dt > 0) & (dt <= config.holding_max_sample_gap_seconds)
        & (rate <= config.holding_turn_rate_spike_deg),
        raw,
    )
    df = df.withColumn("d_hdg", d_hdg)
    df = df.withColumn(
        "turning",
        F.coalesce(F.abs(col("d_hdg")) / dt >= config.holding_turn_rate_min_deg, lit(False)),
    )
    return df.drop("_hdg_ts", "_prev_hdg", "_prev_hdg_ts")


def _window_flag(df: DataFrame, config: EventConfig, placement: str, name: str) -> DataFrame:
    """``name`` = the window test over the ``trailing``, ``leading`` or
    ``centred`` window of ``holding_window_seconds``.

    Three placements, not two: a single orbit shorter than the window is
    seen whole by the trailing window only near its end and by the leading
    one only near its start; at mid-orbit each holds under 300 degrees and
    the sample would go unflagged. The centred window holds the whole orbit
    there, and the union of the three flags every sample of any circuit of
    at least 330 degrees that fits in the window."""
    span = int(config.holding_window_seconds)
    base = Window.partitionBy("track_id", PIECE).orderBy("_ts")
    frames = {
        "trailing": (-span, 0),
        "leading": (0, span),
        "centred": (-(span // 2), span - span // 2),
    }
    w = base.rangeBetween(*frames[placement])
    net = F.sum("d_hdg").over(w)
    gross = F.sum(F.abs(col("d_hdg"))).over(w)
    # Displacement between the first and the last *turning* sample in the
    # window: the pattern's own extent, not the straight run into it.
    t_lat = F.when(col("turning"), col("lat"))
    t_lon = F.when(col("turning"), col("lon"))
    disp = _haversine_nm(
        F.first(t_lat, ignorenulls=True).over(w), F.first(t_lon, ignorenulls=True).over(w),
        F.last(t_lat, ignorenulls=True).over(w), F.last(t_lon, ignorenulls=True).over(w),
    )
    path = F.max("cumulative_distance_nm").over(w) - F.min("cumulative_distance_nm").over(w)
    alt_range = (F.max("_alt_ft").over(w) - F.min("_alt_ft").over(w))
    flag = (
        (F.abs(net) >= config.holding_net_turn_min_deg)
        & (gross > 0)
        & (F.abs(net) / gross >= config.holding_same_sense_min_pct / 100.0)
        & (disp <= config.holding_max_displacement_nm)
        & (path >= config.holding_min_path_nm)
        & (alt_range <= config.holding_max_altitude_range_ft)
    )
    return df.withColumn(name, F.coalesce(flag, lit(False)))


def _episodes(df: DataFrame, config: EventConfig) -> DataFrame:
    """One row per episode, trimmed to its first and last turning sample."""
    w = Window.partitionBy("track_id", PIECE).orderBy("_ts")
    before = w.rowsBetween(Window.unboundedPreceding, -1)
    after = w.rowsBetween(1, Window.unboundedFollowing)
    hold_ts = F.when(col("_in_hold"), col("_ts"))
    # Bridge a short unflagged stretch between two flagged ones.
    prev_hold = F.last(hold_ts, ignorenulls=True).over(before)
    next_hold = F.first(hold_ts, ignorenulls=True).over(after)
    bridged = col("_in_hold") | (
        ((col("_ts") - prev_hold) <= config.holding_bridge_gap_seconds)
        & ((next_hold - col("_ts")) <= config.holding_bridge_gap_seconds)
    )
    df = df.withColumn("_hold", F.coalesce(bridged, lit(False)))
    starts = col("_hold") & ~F.coalesce(F.lag("_hold").over(w), lit(False))
    df = df.withColumn("_ep", F.sum(starts.cast("int")).over(w.rowsBetween(Window.unboundedPreceding, 0)))
    flagged = df.filter(col("_hold"))
    we = Window.partitionBy("track_id", PIECE, "_ep")
    core_start = F.min(F.when(col("turning"), col("_ts"))).over(we)
    core_end = F.max(F.when(col("turning"), col("_ts"))).over(we)
    core = flagged.withColumn("_core_start", core_start).withColumn("_core_end", core_end).filter(
        col("_core_start").isNotNull() & col("_ts").between(col("_core_start"), col("_core_end"))
    )
    agg = core.groupBy("track_id", PIECE, "_ep").agg(
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
        F.sum("d_hdg").alias("turn_deg"),
        F.sum(col("turning").cast("int")).alias("n_turning"),
        F.count(lit(1)).alias("n_rows"),
    )
    agg = agg.withColumn("duration_s", col("end_ts") - col("start_ts"))
    return agg.filter(
        (col("duration_s") >= config.holding_min_duration_seconds)
        & (F.abs(col("turn_deg")) >= config.holding_min_episode_turn_deg)
    )


def calculate_holding_events(
    geo: DataFrame, config: EventConfig, go_arounds: Optional[DataFrame] = None
) -> Optional[DataFrame]:
    """``holding-start``/``holding-end`` pairs for every track in ``geo``.

    ``geo`` is the step 04 frame after the cumulative measures, ``add_piece``
    and the aerodrome geometry; the elevation columns are optional. Returns
    the nine milestone columns plus ``version``, or None when the family is
    off. ``go_arounds`` (``track_id, type, event_time``) drops episodes that
    start or end within ``holding_goaround_exclusion_seconds`` of a
    ``go-around`` on the same track.
    """
    if not config.emit_holding:
        return None
    elev_cols = [c for c in ("elev_ades_ft", "elev_adep_ft") if c in geo.columns]
    elev = F.coalesce(*[col(c) for c in elev_cols], lit(0.0)) if elev_cols else lit(0.0)
    df = (
        geo.filter(col("event_time").isNotNull())
        .withColumn("_ts", col("event_time").cast("double"))
        .withColumn("_alt_ft", col("baro_altitude_c") * FT_PER_M)
        .withColumn("_elev_ft", elev)
    )
    if PIECE not in df.columns:
        df = df.withColumn(PIECE, lit(0))
    df = _turn_series(df, config)
    df = _window_flag(df, config, "trailing", "_hold_bwd")
    df = _window_flag(df, config, "leading", "_hold_fwd")
    df = _window_flag(df, config, "centred", "_hold_mid")
    gates = (
        ((col("_alt_ft") - col("_elev_ft")) >= config.holding_min_height_ft)
        & ((col("velocity") * KT_PER_MS) >= config.holding_min_speed_kt)
    )
    df = df.withColumn(
        "_in_hold",
        (col("_hold_bwd") | col("_hold_fwd") | col("_hold_mid")) & F.coalesce(gates, lit(False)),
    )
    episodes = _episodes(df, config)

    if go_arounds is not None:
        ga = (
            go_arounds.filter(col("type") == "go-around")
            .select(col("track_id").alias("_ga_track"), col("event_time").cast("double").alias("_ga_ts"))
        )
        near = (col("track_id") == col("_ga_track")) & (
            (F.abs(col("start_ts") - col("_ga_ts")) <= config.holding_goaround_exclusion_seconds)
            | (F.abs(col("end_ts") - col("_ga_ts")) <= config.holding_goaround_exclusion_seconds)
        )
        episodes = episodes.join(ga, near, "left_anti")

    seq = Window.partitionBy("track_id").orderBy("start_ts")
    episodes = episodes.withColumn("holding_seq", F.row_number().over(seq))
    straight_pct = F.round(100.0 * (1.0 - col("n_turning") / col("n_rows")), 1)
    pattern = F.when(
        (straight_pct < config.holding_orbit_max_straight_pct)
        & (F.abs(col("turn_deg")) < config.holding_orbit_max_turn_deg),
        lit("orbit"),
    ).otherwise(lit("racetrack"))
    info = F.to_json(F.struct(
        col("holding_seq"),
        pattern.alias("pattern"),
        F.when(col("turn_deg") > 0, lit("R")).otherwise(lit("L")).alias("direction"),
        F.round(F.abs(col("turn_deg")) / 360.0, 1).alias("circuits"),
        col("duration_s"),
        F.round(col("end_dist_nm") - col("start_dist_nm"), 2).alias("distance_nm"),
        F.round(col("start_alt_ft"), 0).alias("altitude_start_ft"),
        F.round(col("end_alt_ft"), 0).alias("altitude_end_ft"),
        straight_pct.alias("straight_pct"),
    ))
    episodes = episodes.withColumn("info", info)

    def _row(kind: str) -> DataFrame:
        return episodes.select(
            col("track_id"),
            lit(f"holding-{kind}").alias("type"),
            col(f"{kind}_time").alias("event_time"),
            col(f"{kind}_lon").alias("lon"),
            col(f"{kind}_lat").alias("lat"),
            col(f"{kind}_alt_ft").alias("altitude_ft"),
            col(f"{kind}_dist_nm").alias("cumulative_distance_nm"),
            col(f"{kind}_cum_s").cast("long").alias("cumulative_time_s"),
            col("info"),
            lit(HOLDING_VERSION).alias("version"),
        )

    return _row("start").unionByName(_row("end"))
