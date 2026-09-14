"""The grid interpolation's forward look-ahead, rewritten without the quadratic frame.

`first(..., ignorenulls=True)` over `rowsBetween(currentRow, unboundedFollowing)`
shrinks as the row advances, so Spark answers each row by scanning to the end of
the partition. It was applied twice per value column. The identical pattern in
`cleaning/native._next_valid` was 94% of step 02a.

Asking the same question of the reversed ordering makes the frame grow. Both
ordering keys are reversed, so the order is exactly inverted -- which matters,
because the ordering deliberately places a real sample before the grid instant
it coincides with.
"""
import pytest
from pyspark.sql import Window
from pyspark.sql import functions as F


def _frame(spark, n):
    """Alternating real samples and grid points, values every third row."""
    return (
        spark.range(n)
        .withColumn("track_id", F.lit("t1"))
        .withColumn("_ts", (F.col("id") / 2).cast("double"))
        .withColumn("_is_grid", (F.col("id") % 2 == 1))
        .withColumn("v", F.when(F.col("id") % 3 == 0, F.col("id").cast("double")))
    )


def _old(df):
    order = Window.partitionBy("track_id").orderBy("_ts", F.col("_is_grid").cast("int"))
    fwd = order.rowsBetween(Window.currentRow, Window.unboundedFollowing)
    known = F.when(F.col("v").isNotNull(), F.col("_ts"))
    return df.withColumn("nt", F.first(known, True).over(fwd)) \
             .withColumn("nv", F.first(F.col("v"), True).over(fwd)) \
             .select("id", "nt", "nv")


def _new(df):
    rev = Window.partitionBy("track_id").orderBy(
        F.col("_ts").desc(), F.col("_is_grid").cast("int").desc()
    )
    fwd = rev.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    known = F.when(F.col("v").isNotNull(), F.col("_ts"))
    return df.withColumn("nt", F.last(known, True).over(fwd)) \
             .withColumn("nv", F.last(F.col("v"), True).over(fwd)) \
             .select("id", "nt", "nv")


def test_the_reversed_frame_gives_the_same_answer(spark):
    df = _frame(spark, 300)
    old, new = _old(df), _new(df)
    assert old.subtract(new).count() == 0
    assert new.subtract(old).count() == 0


def test_it_includes_the_current_row(spark):
    """The frame starts at currentRow, not currentRow+1 -- a row whose own
    value is non-NULL must answer with itself."""
    df = _frame(spark, 12)
    got = {r["id"]: r["nv"] for r in _new(df).collect()}
    assert got[0] == 0.0
    assert got[3] == 3.0


def test_ties_between_a_real_sample_and_its_grid_instant_are_preserved(spark):
    """Both keys are reversed, so the inversion is exact. Reversing only `_ts`
    would flip the real-before-grid precedence the ordering exists to impose."""
    rows = [(0, "t1", 1.0, False, 5.0), (1, "t1", 1.0, True, None)]
    df = spark.createDataFrame(rows, "id long, track_id string, _ts double, _is_grid boolean, v double")
    assert _old(df).subtract(_new(df)).count() == 0


def test_no_shrinking_frame_remains_in_the_interpolation():
    import ast, inspect
    from opdi.pipeline import level_segments

    src = inspect.getsource(level_segments)
    tree = ast.parse(src)
    # `back` legitimately uses unboundedPreceding; what must not return is a
    # frame ending at unboundedFollowing.
    bad = [
        n for n in ast.walk(tree)
        if isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "rowsBetween"
        and any(getattr(a, "attr", "") == "unboundedFollowing" for a in n.args)
    ]
    assert not bad, "a forward-shrinking frame is correct and quadratic"
