"""`_next_valid` must answer the same question, without the quadratic cost.

It was `first(..., ignorenulls=True)` over `rowsBetween(1, unboundedFollowing)`
-- a frame that shrinks as the row advances, which Spark answers by scanning
forward to the end of the partition for every row. That was 94% of step 02a:
one stage, 200 tasks, 94 minutes, while a neighbouring stage moved the same
9 GB of shuffle in 14 seconds.

Asking the same question of the reversed ordering turns the shrinking frame
into a growing one, which Spark maintains incrementally.
"""
import pytest
from pyspark.sql import Window
from pyspark.sql import functions as F

from opdi.cleaning.native import _T, _next_valid, _track_window


def _frame(spark, n, every=5):
    """One track, `n` rows, a non-NULL value every `every` rows."""
    return (
        spark.range(n)
        .withColumn("track_id", F.lit("t1"))
        .withColumn(_T, F.col("id").cast("double"))
        .withColumn("v", F.when(F.col("id") % every == 0, F.col("id").cast("double")))
    )


def _old_form(df):
    """The shrinking-frame implementation this replaced."""
    return df.withColumn(
        "r",
        F.first(F.when(F.col("v").isNotNull(), F.col(_T)), ignorenulls=True).over(
            _track_window().rowsBetween(1, Window.unboundedFollowing)
        ),
    ).select("id", "r")


def _new_form(df):
    return df.withColumn("r", _next_valid(F.col("v"), F.col(_T))).select("id", "r")


def test_the_rewrite_returns_exactly_the_old_answer(spark):
    """Not 'nearly identical' -- the same rows, value for value."""
    df = _frame(spark, 400)
    old, new = _old_form(df), _new_form(df)
    assert old.subtract(new).count() == 0
    assert new.subtract(old).count() == 0


def test_it_finds_the_next_non_null_not_the_current_one(spark):
    """The frame starts at offset 1, so a row whose own value is non-NULL must
    still look forward. Getting this wrong would be invisible in aggregate."""
    df = _frame(spark, 20, every=5)
    got = {r["id"]: r["r"] for r in _new_form(df).collect()}
    assert got[5] == 10.0      # own value non-NULL, still looks ahead
    assert got[6] == 10.0
    assert got[9] == 10.0


def test_the_last_rows_have_no_successor(spark):
    """Past the final non-NULL there is nothing ahead, and NULL is the honest
    answer -- not the current row's own value."""
    df = _frame(spark, 12, every=5)
    got = {r["id"]: r["r"] for r in _new_form(df).collect()}
    assert got[10] is None
    assert got[11] is None


def test_a_track_with_no_values_at_all_yields_null(spark):
    df = (
        spark.range(5)
        .withColumn("track_id", F.lit("t1"))
        .withColumn(_T, F.col("id").cast("double"))
        .withColumn("v", F.lit(None).cast("double"))
    )
    assert all(r["r"] is None for r in _new_form(df).collect())


def test_tracks_do_not_leak_into_each_other(spark):
    """The reversal keeps the same partitioning; the last row of one track must
    not borrow the first of the next."""
    a = _frame(spark, 10).withColumn("track_id", F.lit("a"))
    b = _frame(spark, 10).withColumn("track_id", F.lit("b"))
    both = a.unionByName(b)
    rows = _new_form(both).collect()
    assert len(rows) == 20


def test_no_shrinking_frame_remains():
    """The whole point. A reintroduced unboundedFollowing frame would be
    correct and quadratic -- passing tests, failing the cluster."""
    import inspect

    from opdi.cleaning import native

    import ast

    tree = ast.parse(inspect.getsource(native._next_valid).lstrip())
    # The name appears in the docstring explaining its removal; what matters is
    # that no expression references it.
    attrs = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    assert "unboundedFollowing" not in attrs
