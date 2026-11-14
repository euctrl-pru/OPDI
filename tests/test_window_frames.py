"""The holding and FIR families give what their earlier, slower forms gave.

Spark rescans a window frame that ends at the last row (``rowsBetween(1,
unboundedFollowing)``) for every row, so the families now ask the same
question of the reversed order. The earlier forms are kept here as the
reference, and the production functions are compared with them on tracks
with pieces and NULLs. The two forms hold the same rows only when the times
are unique within a track and piece, so the holding family makes its input
unique first; that is tested with tied times.

The holding start and end events are read from one row per episode; the
reference is the earlier union of two selections.
"""

import random

from navigation_fixtures import fly, racetrack
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit

from opdi.config import EventConfig
from opdi.pipeline import fir, holding
from opdi.pipeline.holes import PIECE

# -- the earlier forms (reference) --------------------------------------------


def _episodes_forward(df: DataFrame, config: EventConfig) -> DataFrame:
    """``holding._episodes`` with the frame from the next row to the end."""
    w = Window.partitionBy("track_id", PIECE).orderBy("_ts")
    before = w.rowsBetween(Window.unboundedPreceding, -1)
    after = w.rowsBetween(1, Window.unboundedFollowing)
    hold_ts = F.when(col("_in_hold"), col("_ts"))
    prev_hold = F.last(hold_ts, ignorenulls=True).over(before)
    next_hold = F.first(hold_ts, ignorenulls=True).over(after)
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


def _debounced_runs_forward(df: DataFrame, config: EventConfig) -> DataFrame:
    """``fir._debounced_runs`` with the frame from the next row to the end."""
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
    df = df.withColumn("_prev_ts", F.lag("_ts").over(w))
    df = df.withColumn(
        "_run_last_ts",
        F.coalesce(
            F.min(F.when(col("_start"), col("_prev_ts"))).over(after), F.max("_ts").over(whole)
        ),
    )
    long = (col("_run_last_ts") - col("_run_start_ts")) >= config.fir_min_dwell_seconds
    carried = F.last(
        F.when(long & col("fir_code").isNotNull(), F.struct(col("fir_code").alias("c"))),
        ignorenulls=True,
    ).over(upto)
    df = df.withColumn("fir_run", F.when(long, col("fir_code")).otherwise(carried["c"]))
    return df.drop("_start", "_run_start_ts", "_prev_ts", "_run_last_ts")


def _pair_union(episodes: DataFrame) -> DataFrame:
    """The holding pair as two selections of the episodes and their union."""

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
            lit(holding.HOLDING_VERSION).alias("version"),
        )

    return _row("start").unionByName(_row("end"))


# -- fixtures ------------------------------------------------------------------


def _episode_input(spark, n_tracks=40, seed=7, ties=False):
    """Rows as ``_episodes`` reads them: flags, turns and positions per sample."""
    rnd = random.Random(seed)
    rows = []
    for t in range(n_tracks):
        ts = 0.0
        for i in range(rnd.randint(1, 90)):
            if not (ties and rnd.random() < 0.15):
                ts += rnd.choice([1.0, 5.0, 5.0, 7.0, 40.0, 400.0])
            rows.append(
                (
                    f"t{t}",
                    i // 30,
                    ts,
                    rnd.random() < 0.6,
                    rnd.random() < 0.7,
                    rnd.uniform(-20.0, 20.0) if rnd.random() < 0.9 else None,
                    50.0 + rnd.random(),
                    4.0 + rnd.random(),
                    rnd.uniform(9000.0, 11000.0),
                    float(i),
                    float(ts),
                )
            )
    df = spark.createDataFrame(
        rows,
        f"track_id string, {PIECE} int, _ts double, _in_hold boolean, turning boolean, "
        "d_hdg double, lat double, lon double, _alt_ft double, "
        "cumulative_distance_nm double, cumulative_time_s double",
    )
    return df.withColumn("event_time", F.timestamp_seconds(col("_ts")))


def _fir_input(spark, n_tracks=40, seed=11):
    rnd = random.Random(seed)
    rows = []
    for t in range(n_tracks):
        ts = 0.0
        for i in range(rnd.randint(1, 90)):
            ts += rnd.choice([5.0, 5.0, 30.0, 200.0])
            code = rnd.choice(["EBBU", "EBBU", "EHAA", "LFFF", None])
            rows.append((f"t{t}", i // 30, ts, code))
    return spark.createDataFrame(rows, f"track_id string, {PIECE} int, _ts double, fir_code string")


def _sorted(df: DataFrame):
    cols = sorted(df.columns)
    return sorted((tuple(r) for r in df.select(*cols).collect()), key=repr)


def _config():
    # Short limits, so that many episodes and runs pass them on random tracks.
    return EventConfig(
        holding_min_duration_seconds=20.0,
        holding_min_episode_turn_deg=5.0,
        fir_min_dwell_seconds=60.0,
    )


# -- the reversed frames --------------------------------------------------------


def test_the_episodes_are_those_of_the_forward_frame(spark):
    df = _episode_input(spark)
    config = _config()
    new = _sorted(holding._episodes(df, config))
    old = _sorted(_episodes_forward(df, config))
    assert new == old
    assert len(new) > 10


def test_the_fir_runs_are_those_of_the_forward_frame(spark):
    df = _fir_input(spark)
    config = _config()
    new = _sorted(fir._debounced_runs(df, config))
    old = _sorted(_debounced_runs_forward(df, config))
    assert new == old
    assert len({r for r in new if r[0] is not None}) > 10


# -- tied times -----------------------------------------------------------------


def test_tied_times_are_made_unique_before_the_holding_windows(spark):
    """The reviewer's case: times tied within a piece. One row per time is kept,
    and on that the episodes equal those of the forward frame."""
    df = _episode_input(spark, ties=True, seed=3)
    assert df.groupBy("track_id", PIECE, "_ts").count().filter("count > 1").count() > 0
    unique = holding._one_row_per_time(df)
    assert unique.groupBy("track_id", PIECE, "_ts").count().filter("count > 1").count() == 0
    assert unique.select("track_id", PIECE, "_ts").distinct().count() == (
        df.select("track_id", PIECE, "_ts").distinct().count()
    )
    config = _config()
    assert _sorted(holding._episodes(unique, config)) == _sorted(_episodes_forward(unique, config))


def test_the_row_kept_at_a_tied_time_does_not_depend_on_the_input_order(spark):
    df = _episode_input(spark, ties=True, seed=5)
    a = _sorted(holding._one_row_per_time(df))
    b = _sorted(holding._one_row_per_time(df.orderBy(F.rand(1)).repartition(3)))
    assert a == b


def test_a_repeated_report_does_not_change_the_holding_events(spark):
    """Step 04 can read tracks that kept repeated times (raw tracks, or cleaning
    without its duplicate removal); the events are those of the track without
    the repeats."""
    geo = fly(spark, [(300, 0, 0)] + racetrack(2) + [(300, 0, 0)])
    repeats = geo.filter((F.unix_timestamp("event_time") % 35) == 0)
    assert repeats.count() > 5
    config = EventConfig()
    plain = _sorted(holding.calculate_holding_events(geo, config))
    doubled = _sorted(holding.calculate_holding_events(geo.unionByName(repeats), config))
    assert len(plain) == 2
    assert doubled == plain


# -- the holding pair from one row ------------------------------------------------


def test_the_holding_pair_from_one_row_equals_the_union_of_two_selections(spark):
    geo = (
        fly(spark, [(300, 0, 0)] + racetrack(2) + [(300, 0, 0)], track_id="a")
        .unionByName(fly(spark, [(300, 0, 0), (270, -1.4, 0), (300, 0, 0)], track_id="b"))
        .unionByName(
            fly(spark, racetrack(1) + [(600, 0, 0)] + racetrack(2), track_id="c", lat0=52.0)
        )
    )
    episodes = holding._holding_episodes(geo, EventConfig())
    assert episodes.count() >= 3
    assert _sorted(holding._pair_rows(episodes)) == _sorted(_pair_union(episodes))
