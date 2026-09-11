"""Per-day partitioned writes, and the overwrite semantics they need.

Publishing one day at a time only works if writing day D replaces day D and
nothing else. In S3 mode ``write_table`` wrote a flat parquet directory and
``mode="overwrite"`` wiped all of it, so "re-run one day" and "destroy every
other day" were the same operation. These tests pin the difference.

The day columns the callers use (``dof`` on the flight list, the stamped run
day on events and measurements) are supplied by the callers; this file is only
about the storage layer honouring them.
"""
import pytest

from opdi.config import OPDIConfig
from opdi.utils.storage import StorageManager


def _s3_like_storage(spark, tmp_path):
    """A StorageManager on the S3 code path, writing to a local directory.

    ``use_s3`` is ``not enable_iceberg``, and ``_s3_path`` just joins the
    warehouse prefix to the table name -- so a filesystem path exercises
    exactly the branch the opensky environment takes.
    """
    cfg = OPDIConfig()
    cfg.spark.enable_iceberg = False
    cfg.project.warehouse_path = str(tmp_path)
    return StorageManager(spark, cfg), cfg


def _rows(spark, tmp_path, table):
    return {
        (r["v"], str(r["dof"]))
        for r in spark.read.parquet(f"{tmp_path}/{table}").collect()
    }


def test_a_partitioned_write_creates_one_directory_per_day(spark, tmp_path):
    storage, _ = _s3_like_storage(spark, tmp_path)
    df = spark.createDataFrame(
        [(1, "2026-06-01"), (2, "2026-06-02")], "v int, dof string"
    )

    storage.write_table(df, "t", mode="overwrite", partition_by=["dof"])

    days = sorted(p.name for p in (tmp_path / "t").iterdir() if p.is_dir())
    assert days == ["dof=2026-06-01", "dof=2026-06-02"]


def test_overwriting_one_day_leaves_the_others_alone(spark, tmp_path):
    """The whole reason this exists.

    Without dynamic partition overwrite, writing day 2 deletes day 1 -- and
    nothing reports it. A week run would finish with one day of data and look
    like it had worked.
    """
    storage, _ = _s3_like_storage(spark, tmp_path)
    day1 = spark.createDataFrame([(1, "2026-06-01")], "v int, dof string")
    day2 = spark.createDataFrame([(2, "2026-06-02")], "v int, dof string")

    storage.write_table(day1, "t", mode="overwrite", partition_by=["dof"])
    storage.write_table(day2, "t", mode="overwrite", partition_by=["dof"])

    assert _rows(spark, tmp_path, "t") == {(1, "2026-06-01"), (2, "2026-06-02")}


def test_rewriting_a_day_replaces_it_rather_than_appending(spark, tmp_path):
    """Re-running a day must be idempotent, not additive.

    This is the failure `append` gives: the day silently holds both attempts
    and every count over it is wrong by an amount nothing reports.
    """
    storage, _ = _s3_like_storage(spark, tmp_path)
    first = spark.createDataFrame([(1, "2026-06-01")], "v int, dof string")
    second = spark.createDataFrame([(99, "2026-06-01")], "v int, dof string")

    storage.write_table(first, "t", mode="overwrite", partition_by=["dof"])
    storage.write_table(second, "t", mode="overwrite", partition_by=["dof"])

    assert _rows(spark, tmp_path, "t") == {(99, "2026-06-01")}


def test_an_unpartitioned_overwrite_still_replaces_everything(spark, tmp_path):
    """Unchanged behaviour when no partitioning is asked for.

    Every existing caller passes no ``partition_by``, so this is the path they
    keep taking; if it moved, tables that are meant to be replaced wholesale
    would start accumulating instead.
    """
    storage, _ = _s3_like_storage(spark, tmp_path)
    storage.write_table(
        spark.createDataFrame([(1, "2026-06-01")], "v int, dof string"),
        "t", mode="overwrite",
    )
    storage.write_table(
        spark.createDataFrame([(2, "2026-06-02")], "v int, dof string"),
        "t", mode="overwrite",
    )

    assert _rows(spark, tmp_path, "t") == {(2, "2026-06-02")}


def test_appending_to_a_partitioned_table_adds_a_day(spark, tmp_path):
    storage, _ = _s3_like_storage(spark, tmp_path)
    storage.write_table(
        spark.createDataFrame([(1, "2026-06-01")], "v int, dof string"),
        "t", mode="append", partition_by=["dof"],
    )
    storage.write_table(
        spark.createDataFrame([(2, "2026-06-02")], "v int, dof string"),
        "t", mode="append", partition_by=["dof"],
    )

    assert _rows(spark, tmp_path, "t") == {(1, "2026-06-01"), (2, "2026-06-02")}


def test_partitioning_on_a_column_the_frame_lacks_fails_loudly(spark, tmp_path):
    """A typo'd partition column must not write an unpartitioned table.

    Silently ignoring it would produce a flat directory that looks fine until
    the first attempt to replace a single day wipes the lot.
    """
    storage, _ = _s3_like_storage(spark, tmp_path)
    df = spark.createDataFrame([(1, "2026-06-01")], "v int, dof string")

    with pytest.raises(ValueError, match="not in the DataFrame"):
        storage.write_table(df, "t", mode="overwrite", partition_by=["day_of"])


def test_mode_is_still_required_after_the_signature_change():
    """The guarantee test_storage_write_mode protects must survive this."""
    import inspect

    params = inspect.signature(StorageManager.write_table).parameters
    assert params["mode"].default is inspect.Parameter.empty
    assert params["partition_by"].default is None
