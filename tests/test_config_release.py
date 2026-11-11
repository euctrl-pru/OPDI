"""Where the opensky environment writes: one warehouse setting and a release root.

The warehouse is the storage prefix that holds OPDI's data. A release is one
version of the published data (``v2.0.0``); everything a release's pipeline
writes goes under ``<warehouse>/<release>``, its release root.
"""

import pytest

from opdi.config import OPDIConfig
from opdi.output.published import PublishedLayout
from opdi.utils.storage import StorageManager


@pytest.fixture(autouse=True)
def _no_warehouse_variable(monkeypatch):
    monkeypatch.delenv("OPDI_WAREHOUSE", raising=False)


def test_the_default_opensky_release_root():
    cfg = OPDIConfig.for_environment("opensky")
    assert cfg.project.warehouse_path == "s3a://eurocontrol/opdi"
    assert cfg.project.release == "v2.0.0"
    assert cfg.project.release_root == "s3a://eurocontrol/opdi/v2.0.0"


@pytest.mark.parametrize("worldwide", [True, False])
def test_the_variable_sets_the_warehouse_for_every_coverage(monkeypatch, worldwide):
    monkeypatch.setenv("OPDI_WAREHOUSE", "s3a://x/y")
    cfg = OPDIConfig.for_environment("opensky", worldwide=worldwide)
    assert cfg.project.release_root == "s3a://x/y/v2.0.0"


@pytest.mark.parametrize("worldwide", [True, False])
def test_the_argument_wins_over_the_variable(monkeypatch, worldwide):
    monkeypatch.setenv("OPDI_WAREHOUSE", "s3a://x/y")
    cfg = OPDIConfig.for_environment("opensky", worldwide=worldwide, warehouse="s3a://a/b/")
    assert cfg.project.release_root == "s3a://a/b/v2.0.0"


def test_the_old_worldwide_variable_has_no_effect(monkeypatch):
    monkeypatch.setenv("OPDI_WAREHOUSE_WORLD", "s3a://somewhere-else")
    cfg = OPDIConfig.for_environment("opensky", worldwide=True)
    assert cfg.project.release_root == "s3a://eurocontrol/opdi/v2.0.0"


@pytest.mark.parametrize(
    "env, declared",
    [
        ("live", "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged"),
        ("dev", "abfs://storage-fs@cdpdldev0.dfs.core.windows.net/data/project/opdi.db/unmanaged"),
        ("local", "./data/warehouse"),
    ],
)
def test_other_environments_are_unchanged(env, declared):
    europe = OPDIConfig.for_environment(env, worldwide=False)
    world = OPDIConfig.for_environment(env, worldwide=True)
    assert europe.project.warehouse_path == declared
    assert world.project.warehouse_path == declared + "-world"
    for cfg in (europe, world):
        assert cfg.project.release_root == cfg.project.warehouse_path


def test_the_start_up_lines_name_warehouse_root_and_coverage(monkeypatch):
    monkeypatch.setenv("OPDI_WAREHOUSE", "s3a://x/y")
    cfg = OPDIConfig.for_environment("opensky", warehouse="s3a://a/b")
    text = cfg.describe_target()
    assert "s3a://a/b" in text
    assert "s3a://a/b/v2.0.0" in text
    assert "worldwide" in text
    # The variable a pod exports is reported when the run does not use it.
    assert "OPDI_WAREHOUSE" in text and "s3a://x/y" in text


def _opensky(tmp_path, worldwide=True):
    cfg = OPDIConfig.for_environment(
        "opensky", worldwide=worldwide, warehouse=str(tmp_path / "wh"))
    return cfg


def test_storage_and_published_folder_sit_under_the_release_root(spark, tmp_path):
    storage = StorageManager(spark, _opensky(tmp_path))
    root = f"{tmp_path}/wh/v2.0.0"
    assert storage.base_path == root
    assert storage.state_path == f"{root}/_state"
    assert PublishedLayout.for_storage(storage).root == f"{root}/published"


def test_the_coverage_marker_still_refuses_a_mismatch(spark, tmp_path):
    StorageManager(spark, _opensky(tmp_path, worldwide=True)).write_coverage_marker()
    europe = StorageManager(spark, _opensky(tmp_path, worldwide=False))
    problem = europe.check_coverage()
    assert problem is not None
    assert "worldwide" in problem and "europe" in problem


def test_a_progress_mark_is_seen_by_a_second_storage(spark, tmp_path):
    first = StorageManager(spark, _opensky(tmp_path))
    second = StorageManager(spark, _opensky(tmp_path))
    assert not second.is_done("02@2026-06-14")
    first.mark_done("02@2026-06-14", seconds=12.5)
    assert second.is_done("02@2026-06-14")
    second.clear_done("02@2026-06-14")
    assert not first.is_done("02@2026-06-14")


def test_progress_logs_travel_through_the_warehouse(spark, tmp_path):
    first = StorageManager(spark, _opensky(tmp_path))
    second = StorageManager(spark, _opensky(tmp_path))
    worker_a = tmp_path / "worker_a"
    worker_b = tmp_path / "worker_b"
    worker_a.mkdir()
    (worker_a / "03_log.parquet").write_bytes(b"done")
    first.push_logs(str(worker_a))
    assert second.pull_logs(str(worker_b)) == 1
    assert (worker_b / "03_log.parquet").read_bytes() == b"done"


def test_without_the_release_folder_tables_sit_at_the_prefix(spark, tmp_path):
    """Study scripts point at a complete prefix and clear the release name."""
    cfg = _opensky(tmp_path)
    cfg.project.warehouse_path = str(tmp_path / "research")
    cfg.project.release = ""
    assert StorageManager(spark, cfg).base_path == f"{tmp_path}/research"


def test_a_day_step_keeps_its_progress_log_in_the_warehouse(spark, tmp_path, monkeypatch):
    import os

    import opdi.pipeline.tracks as tracks
    from opdi import periodrun

    class _FakeTracks:
        def __init__(self, spark, config, log_file_path):
            self.log_file_path = log_file_path

        def process_day(self, day, skip_if_processed):
            with open(self.log_file_path, "w") as handle:
                handle.write(str(day))

    monkeypatch.setattr(tracks, "TrackProcessor", _FakeTracks)
    monkeypatch.chdir(tmp_path)
    cfg = _opensky(tmp_path)
    periodrun.run_day_step(spark, cfg, "02", __import__("datetime").date(2026, 6, 14), {})

    worker_b = tmp_path / "worker_b"
    StorageManager(spark, _opensky(tmp_path)).pull_logs(str(worker_b))
    assert (worker_b / "02_osn-tracks-etl-log.parquet").read_text() == "2026-06-14"
    assert os.path.isdir(tmp_path / "wh" / "v2.0.0" / "_state" / "logs")
