"""The write mode must be stated, and must be one of the known ones.

`write_table` used to default to "append", so a caller who had not thought
about re-processing got the option that silently doubles a table: nothing
errors, rows are counted twice, and every aggregate reading them is wrong by an
amount nothing reports. Three places in this repo carry a workaround for that
default; these tests are what stop a fourth being needed.
"""

import inspect

import pytest

from opdi.utils.storage import StorageManager


def test_mode_has_no_default():
    """The point of the change: a new call site cannot forget."""
    mode = inspect.signature(StorageManager.write_table).parameters["mode"]

    assert mode.default is inspect.Parameter.empty


def test_an_unknown_mode_raises_rather_than_doing_nothing(spark):
    """Previously an unrecognised mode fell through every branch of the
    Iceberg path and wrote nothing, reporting success -- a typo'd mode string
    was a silent no-op."""
    from opdi.config import OPDIConfig

    storage = StorageManager(spark, OPDIConfig())
    df = spark.createDataFrame([(1,)], "x int")

    with pytest.raises(ValueError, match="Unknown write mode"):
        storage.write_table(df, "whatever", mode="appendd")


def test_the_known_modes_are_the_documented_three():
    assert StorageManager.WRITE_MODES == ("append", "overwrite", "insert_overwrite")


def test_an_unpartitioned_iceberg_overwrite_passes_a_condition():
    """``DataFrameWriterV2.overwrite()`` has no zero-arg form in any PySpark
    version this repo runs against -- calling it with no argument raises
    ``TypeError`` before any write happens, unconditionally. That was the
    shape of this code before the fix: ``df.writeTo(qualified).overwrite()``.

    This cannot be proven by executing a real Iceberg write on this host:
    there is no Iceberg runtime anywhere on the machine (no jar found), and a
    bare local Spark session has no Iceberg catalog to write into. What this
    test proves instead, with a stub in place of a real ``DataFrame``/
    ``DataFrameWriterV2``, is narrower and stated plainly: that the Iceberg
    branch of ``write_table`` calls ``.overwrite(<something>)`` rather than
    ``.overwrite()``. It does not execute against Spark at all, so it cannot
    confirm that Spark accepts the call, nor that ``F.lit(True)`` actually
    means "replace every row" once it reaches a live catalog -- that
    semantic is PySpark's documented contract for
    ``DataFrameWriterV2.overwrite(condition)``, not something this test
    exercises.
    """
    from pyspark.sql import Column
    from pyspark.sql import functions as F

    from opdi.config import OPDIConfig

    calls = {}

    class _FakeWriter:
        def overwrite(self, condition):
            calls["condition"] = condition

    class _FakeDF:
        columns = ["x"]

        def writeTo(self, name):
            calls["table"] = name
            return _FakeWriter()

    cfg = OPDIConfig()
    cfg.spark.enable_iceberg = True  # use_s3 = False -> the Iceberg branch

    # No real SparkSession is constructed or touched: for an unpartitioned
    # overwrite, write_table never reads self.spark before reaching
    # df.writeTo(...).overwrite(...), so `spark=None` is enough to prove the
    # call shape in isolation.
    storage = StorageManager(None, cfg)
    storage.write_table(_FakeDF(), "coverage_marker", mode="overwrite")

    assert "condition" in calls, "overwrite() was called with no argument"
    assert isinstance(calls["condition"], Column)
