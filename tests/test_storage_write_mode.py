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
