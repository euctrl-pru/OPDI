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
from opdi.versions import HOLDING_VERSION  # stamped on the events and measurements

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
        col("heading").isNotNull()
        & col("_prev_hdg").isNotNull()
        & (dt > 0)
        & (dt <= config.holding_max_sample_gap_seconds)
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
    """The holding test over one window, as a column named ``name``.

    The window is the ``trailing``, ``leading`` or ``centred`` window of
    ``holding_window_seconds``.

    Three placements, not two: a single orbit shorter than the window is
    seen whole by the trailing window only near its end and by the leading
    one only near its start; at mid-orbit each holds under 300 degrees and
    the sample would go unflagged. The centred window holds the whole orbit
    there, and the union of the three flags every sample of any circuit of
    at least 330 degrees that fits in the window.

    **Cost.** Spark re-aggregates a sliding frame from scratch for every row,
    so a 6 min frame of 5 s samples costs about seventy times a running sum,
    per aggregate, per placement; that made step 04 three times slower on a
    worldwide day. Every window quantity here is therefore a difference of
    running sums (or a lag/lead of a position) between the sample and the one
    ``k`` rows away, where ``k`` is the window in samples at the pipeline's
    cadence (``holding_sample_period_seconds``). The elapsed time between the
    two rows must still be close to the window, or a gap is inside it and the
    window is not judged.
    """
    k = max(1, int(round(config.holding_window_seconds / config.holding_sample_period_seconds)))
    ka, kb = {"trailing": (k, 0), "leading": (0, k), "centred": (k // 2, k - k // 2)}[placement]
    w = Window.partitionBy("track_id", PIECE).orderBy("_ts")
    run = w.rowsBetween(Window.unboundedPreceding, 0)
    if "_cum_turn" not in df.columns:
        df = (
            df.withColumn("_cum_turn", F.sum(F.coalesce(col("d_hdg"), lit(0.0))).over(run))
            .withColumn("_cum_gross", F.sum(F.coalesce(F.abs(col("d_hdg")), lit(0.0))).over(run))
            .withColumn(
                "_cum_alt",
                F.sum(F.coalesce(F.abs(col("_alt_ft") - F.lag("_alt_ft").over(w)), lit(0.0))).over(
                    run
                ),
            )
            .withColumn("_cum_turning", F.sum(col("turning").cast("int")).over(run))
            .withColumn("_row", F.row_number().over(w))
        )

    def at(c, offset):
        if offset == 0:
            return col(c)
        return F.lag(c, offset).over(w) if offset > 0 else F.lead(c, -offset).over(w)

    start, stop = (lambda c: at(c, ka)), (lambda c: at(c, -kb))
    elapsed = stop("_ts") - start("_ts")
    span = float(config.holding_window_seconds)
    net = stop("_cum_turn") - start("_cum_turn")
    gross = stop("_cum_gross") - start("_cum_gross")
    disp = _haversine_nm(start("lat"), start("lon"), stop("lat"), stop("lon"))
    path = stop("cumulative_distance_nm") - start("cumulative_distance_nm")
    alt_change = stop("_cum_alt") - start("_cum_alt")
    flag = (
        elapsed.between(0.8 * span, 1.25 * span)
        & (F.abs(net) >= config.holding_net_turn_min_deg)
        & (gross > 0)
        & (F.abs(net) / gross >= config.holding_same_sense_min_pct / 100.0)
        & (disp <= config.holding_max_displacement_nm)
        & (path >= config.holding_min_path_nm)
        & (alt_change <= config.holding_max_altitude_range_ft)
    )
    return df.withColumn(name, F.coalesce(flag, lit(False)))


def _episodes(df: DataFrame, config: EventConfig) -> DataFrame:
    """One row per episode, trimmed to its first and last turning sample."""
    w = Window.partitionBy("track_id", PIECE).orderBy("_ts")
    before = w.rowsBetween(Window.unboundedPreceding, -1)
    # The rows after this one, as the rows before it in the reversed order.
    # Spark answers a frame that ends at the last row (``rowsBetween(1,
    # unboundedFollowing)``) by scanning to the end of the track for every
    # row, which is quadratic in the track's length; a frame that starts at
    # the first row grows and is kept up to date row by row. The times are
    # unique within a track (cleaning removes repeated times), so both
    # frames hold the same rows.
    after = (
        Window.partitionBy("track_id", PIECE)
        .orderBy(col("_ts").desc())
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    hold_ts = F.when(col("_in_hold"), col("_ts"))
    # Bridge a short unflagged stretch between two flagged ones.
    prev_hold = F.last(hold_ts, ignorenulls=True).over(before)
    # The next flagged time is the smallest flagged time after this row.
    next_hold = F.min(hold_ts).over(after)
    bridged = col("_in_hold") | (
        ((col("_ts") - prev_hold) <= config.holding_bridge_gap_seconds)
        & ((next_hold - col("_ts")) <= config.holding_bridge_gap_seconds)
    )
    df = df.withColumn("_hold", F.coalesce(bridged, lit(False)))
    starts = col("_hold") & ~F.coalesce(F.lag("_hold").over(w), lit(False))
    df = df.withColumn(
        "_ep", F.sum(starts.cast("int")).over(w.rowsBetween(Window.unboundedPreceding, 0))
    )
    flagged = df.filter(col("_hold"))
    we = Window.partitionBy("track_id", PIECE, "_ep")
    core_start = F.min(F.when(col("turning"), col("_ts"))).over(we)
    core_end = F.max(F.when(col("turning"), col("_ts"))).over(we)
    core = (
        flagged.withColumn("_core_start", core_start)
        .withColumn("_core_end", core_end)
        .filter(
            col("_core_start").isNotNull()
            & col("_ts").between(col("_core_start"), col("_core_end"))
        )
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
    # Speed from the last sample that had one: cleaning blanks velocity
    # together with heading on a repeated broadcast, and a NULL speed must
    # not punch a hole in an episode.
    known = (
        Window.partitionBy("track_id", PIECE)
        .orderBy("_ts")
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    speed_kt = F.last("velocity", ignorenulls=True).over(known) * KT_PER_MS
    gates = ((col("_alt_ft") - col("_elev_ft")) >= config.holding_min_height_ft) & (
        speed_kt >= config.holding_min_speed_kt
    )
    df = df.withColumn(
        "_in_hold",
        (col("_hold_bwd") | col("_hold_fwd") | col("_hold_mid")) & F.coalesce(gates, lit(False)),
    )
    episodes = _episodes(df, config)

    if go_arounds is not None:
        ga = go_arounds.filter(col("type") == "go-around").select(
            col("track_id").alias("_ga_track"), col("event_time").cast("double").alias("_ga_ts")
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
    info = F.to_json(
        F.struct(
            col("holding_seq"),
            pattern.alias("pattern"),
            F.when(col("turn_deg") > 0, lit("R")).otherwise(lit("L")).alias("direction"),
            F.round(F.abs(col("turn_deg")) / 360.0, 1).alias("circuits"),
            col("duration_s"),
            F.round(col("end_dist_nm") - col("start_dist_nm"), 2).alias("distance_nm"),
            F.round(col("start_alt_ft"), 0).alias("altitude_start_ft"),
            F.round(col("end_alt_ft"), 0).alias("altitude_end_ft"),
            straight_pct.alias("straight_pct"),
        )
    )
    episodes = episodes.withColumn("info", info)

    def _row(kind: str):
        return F.struct(
            lit(f"holding-{kind}").alias("type"),
            col(f"{kind}_time").alias("event_time"),
            col(f"{kind}_lon").alias("lon"),
            col(f"{kind}_lat").alias("lat"),
            col(f"{kind}_alt_ft").alias("altitude_ft"),
            col(f"{kind}_dist_nm").alias("cumulative_distance_nm"),
            col(f"{kind}_cum_s").cast("long").alias("cumulative_time_s"),
        )

    # Both events of an episode come from one row, so the episodes are
    # computed once. (A union of a start frame and an end frame computed the
    # whole family twice, as Spark does not share the work of two branches.)
    pair = episodes.select(
        col("track_id"), col("info"), F.explode(F.array(_row("start"), _row("end"))).alias("_e")
    )
    return pair.select(
        col("track_id"),
        col("_e.type").alias("type"),
        col("_e.event_time").alias("event_time"),
        col("_e.lon").alias("lon"),
        col("_e.lat").alias("lat"),
        col("_e.altitude_ft").alias("altitude_ft"),
        col("_e.cumulative_distance_nm").alias("cumulative_distance_nm"),
        col("_e.cumulative_time_s").alias("cumulative_time_s"),
        col("info"),
        lit(HOLDING_VERSION).alias("version"),
    )
