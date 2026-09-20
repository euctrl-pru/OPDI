"""A warehouse remembers which coverage wrote it.

The marker is not for the pipeline -- nothing reads it at query time. It exists
so that pointing a worldwide run at the European warehouse fails on the first
step instead of on the day somebody notices a flight in Auckland inside a
dataset whose track_id contract says Europe.
"""
from pathlib import Path

import pytest

from opdi.config import OPDIConfig
from opdi.utils.storage import StorageManager


def _mgr(spark, tmp_path, worldwide=False):
    cfg = OPDIConfig.for_environment("local")
    # The suite runs against a bare local SparkSession with no Iceberg
    # runtime on the classpath (see tests/conftest.py: "No cluster, no Hive,
    # no Iceberg, no credentials"). `local`'s declared config is Iceberg-mode
    # (`enable_iceberg` defaults True), which only works against a real
    # catalog. Forcing S3/plain-parquet mode -- the same workaround
    # test_storage_partitions.py already uses -- lets this test exercise real
    # reads and writes on the local filesystem without a cluster; it does not
    # touch what is being tested here, which is the marker's own read/write/
    # compare logic, not which storage backend carries it.
    cfg.spark.enable_iceberg = False
    cfg.project.warehouse_path = str(tmp_path / "wh")
    if worldwide:
        cfg.coverage = OPDIConfig.for_environment("opensky", worldwide=True).coverage
    return StorageManager(spark, cfg)


def test_an_empty_warehouse_has_no_complaint(spark, tmp_path):
    """A first run must not be blocked by the absence of its own marker."""
    assert _mgr(spark, tmp_path).check_coverage() is None


def test_a_marker_round_trips(spark, tmp_path):
    m = _mgr(spark, tmp_path)
    m.write_coverage_marker()
    got = m.read_coverage_marker()
    assert got["coverage"] == "europe"
    assert got["bbox"] == list(m.config.coverage.bbox)


def test_matching_coverage_passes(spark, tmp_path):
    m = _mgr(spark, tmp_path)
    m.write_coverage_marker()
    assert _mgr(spark, tmp_path).check_coverage() is None


def test_worldwide_into_a_european_warehouse_is_refused(spark, tmp_path):
    """The failure this exists for, in the direction that does the damage."""
    _mgr(spark, tmp_path).write_coverage_marker()
    problem = _mgr(spark, tmp_path, worldwide=True).check_coverage()
    assert problem is not None
    assert "europe" in problem and "worldwide" in problem


def test_european_into_a_worldwide_warehouse_is_refused_too(spark, tmp_path):
    """Less damaging but equally wrong: the European rows would be a subset
    nobody can identify afterwards."""
    _mgr(spark, tmp_path, worldwide=True).write_coverage_marker()
    assert _mgr(spark, tmp_path).check_coverage() is not None


def test_the_problem_names_the_warehouse(spark, tmp_path):
    """An operator reading this message has two warehouse paths in play and
    needs to be told which one they hit."""
    _mgr(spark, tmp_path).write_coverage_marker()
    problem = _mgr(spark, tmp_path, worldwide=True).check_coverage()
    assert str(tmp_path) in problem


# --- unreadable is not the same as absent, and must not fail closed --------
#
# test_an_empty_warehouse_has_no_complaint above covers a marker that was
# never written at all. The two tests below cover a marker that *exists* but
# cannot be turned into a usable row -- zero rows, or bytes that are not
# parquet. That is the more dangerous case: read_coverage_marker's
# try/except and its `if not rows` check are what is supposed to catch it,
# and a guard that instead raised or refused here would make every warehouse
# unusable the first time a write committed a marker with nothing readable
# in it -- exactly the failure mode check_coverage exists to avoid, turned
# against itself.

def test_a_zero_row_marker_is_treated_as_unstamped(spark, tmp_path):
    """The marker table exists, reads cleanly, and holds no rows -- e.g. a
    write that committed an empty part file. This exercises
    read_coverage_marker's `if not rows` branch specifically, written
    directly via write_table so it bypasses write_coverage_marker entirely."""
    m = _mgr(spark, tmp_path)
    empty = spark.createDataFrame(
        [], "coverage string, bbox array<double>, airport_types array<string>"
    )
    m.write_table(empty, m.COVERAGE_MARKER, mode="overwrite")
    assert m.check_coverage() is None


def test_a_corrupt_marker_is_treated_as_unstamped(spark, tmp_path):
    """The marker table exists on disk but is not valid parquet, so
    read_table raises when read_coverage_marker calls it. This exercises the
    outer `except Exception: return None` -- the path that would otherwise
    make the guard fail closed on its own malfunction rather than on an
    actual coverage disagreement."""
    m = _mgr(spark, tmp_path)
    marker_path = Path(m.config.project.warehouse_path) / m.COVERAGE_MARKER
    marker_path.mkdir(parents=True)
    (marker_path / "part-00000.parquet").write_text("not parquet")
    assert m.check_coverage() is None
