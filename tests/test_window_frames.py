"""The reversed frames of the holding and FIR families select the same rows as the forward ones.

Spark rescans a frame that ends at the last row (``rowsBetween(1,
unboundedFollowing)``) for every row; the families ask the same question of
the reversed order. These tests compare the two forms on tracks with unique
times, NULLs and pieces.
"""

import random

from pyspark.sql import Window
from pyspark.sql import functions as F


def _tracks(spark, n_tracks=30, seed=7):
    rnd = random.Random(seed)
    rows = []
    for t in range(n_tracks):
        ts = 0.0
        for i in range(rnd.randint(1, 80)):
            ts += rnd.choice([1.0, 5.0, 5.0, 7.0, 400.0])
            flag = rnd.random() < 0.3
            prev = None if rnd.random() < 0.4 else ts - 5.0
            rows.append((f"t{t}", i // 25, ts, flag, prev))
    return spark.createDataFrame(
        rows, "track_id string, piece int, _ts double, f boolean, p double"
    )


def test_next_flagged_time_is_the_same_from_the_reversed_frame(spark):
    df = _tracks(spark)
    hold_ts = F.when(F.col("f"), F.col("_ts"))
    forward = Window.partitionBy("track_id", "piece").orderBy("_ts")
    old = F.first(hold_ts, ignorenulls=True).over(forward.rowsBetween(1, Window.unboundedFollowing))
    reverse = Window.partitionBy("track_id", "piece").orderBy(F.col("_ts").desc())
    new = F.min(hold_ts).over(reverse.rowsBetween(Window.unboundedPreceding, -1))
    out = df.select(old.alias("old"), new.alias("new"))
    assert out.filter(~F.col("old").eqNullSafe(F.col("new"))).count() == 0
    assert out.filter(F.col("old").isNotNull()).count() > 0


def test_smallest_later_value_is_the_same_from_the_reversed_frame(spark):
    df = _tracks(spark, seed=11)
    value = F.when(F.col("f"), F.col("p"))
    forward = Window.partitionBy("track_id", "piece").orderBy("_ts")
    old = F.min(value).over(forward.rowsBetween(1, Window.unboundedFollowing))
    reverse = Window.partitionBy("track_id", "piece").orderBy(F.col("_ts").desc())
    new = F.min(value).over(reverse.rowsBetween(Window.unboundedPreceding, -1))
    out = df.select(old.alias("old"), new.alias("new"))
    assert out.filter(~F.col("old").eqNullSafe(F.col("new"))).count() == 0
    assert out.filter(F.col("old").isNotNull()).count() > 0
