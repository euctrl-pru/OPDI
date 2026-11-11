"""The track-construction V5 paper's entrypoint: every staged file has one job."""

import json
from pathlib import Path

import regenerate_track_v4 as V4
import regenerate_track_v5 as R

SHIPPED = {
    "link_min_implied_speed_kt": 400,
    "link_min_speed_ratio": 0.0,
    "link_min_stop_minutes": 0.0,
    "link_max_gap_minutes": 600,
    "link_cruise_floor_ft": 18000,
    "link_old_edge_climb": True,
    "link_new_edge_descent": True,
}


def _job(name):
    return next(j for j in R.jobs() if j.name == name)


def _json_after(args, flag):
    return json.loads(args[args.index(flag) + 1])


def test_the_jobs_and_where_they_stage():
    assert [j.name for j in R.jobs()] == ["two_days", "pipeline_day"]
    assert R.PAPER.name == "track-construction-v5"
    for job in R.jobs():
        assert job.data == R.DATA / job.name
        assert job.outputs, f"{job.name} stages nothing"
        for p in job.code_paths:
            assert (Path(R.REPO) / p).exists(), f"{job.name}: missing {p}"


def test_the_paper_dir_is_overridable_by_its_own_variable():
    assert "OPDI_PAPER_DIR_V5" in Path(R.__file__).read_text()


def test_two_days_runs_the_shipped_setting_with_each_edge_allowance_off_and_on():
    args = _job("two_days").args
    grid = _json_after(args, "--grid-json")
    assert grid == {
        **{k: [v] for k, v in SHIPPED.items()},
        "link_old_edge_climb": [False, True],
        "link_new_edge_descent": [False, True],
    }
    assert args[args.index("--days") + 1 : args.index("--grid-json")] == [
        "2026-06-14",
        "2025-06-14",
    ]


def test_the_reasons_are_read_at_the_shipped_setting_so_always_written():
    job = _job("two_days")
    assert _json_after(job.args, "--reasons-params-json") == SHIPPED
    assert job.optional == set()
    for stem in ("link_reasons", "link_positive_reasons", "link_negative_reasons"):
        assert f"{stem}_pooled.csv" in job.outputs
    for name in (
        "link_grid_pooled.csv",
        "link_controls_pooled.csv",
        "link_recommended.csv",
        "link_grid_2026-06-14.csv",
        "link_grid_2025-06-14.csv",
    ):
        assert name in job.outputs


def test_the_shipped_setting_is_what_the_code_ships():
    from opdi.config import SegmentationConfig

    shipped = SegmentationConfig()
    for name, value in SHIPPED.items():
        assert getattr(shipped, name) == value, name


def test_pipeline_day_stages_every_table_the_scorer_writes():
    import track_link_pipeline_score as scorer

    job = _job("pipeline_day")
    assert job.args == ["--day", "2026-06-14"]
    assert set(job.outputs) == set(scorer.OUTPUTS)
    assert {"link_consistency.csv", "link_distance_no_hole.csv"} <= set(job.outputs)


def test_every_job_depends_on_the_track_and_detector_code():
    for job in R.jobs():
        assert set(V4.CORE) <= set(job.code_paths), job.name
        assert set(V4.script_code_paths(job.script)) <= set(job.code_paths), job.name
    paths = _job("pipeline_day").code_paths
    for needed in (
        "src/opdi/pipeline/holes.py",
        "src/opdi/pipeline/events.py",
        "src/opdi/pipeline/flights.py",
        "benchmarks/run_track_link_day.py",
    ):
        assert needed in paths


def test_the_day_run_is_a_prerequisite_not_a_job():
    assert "prerequisite, not a job" in R.__doc__
    assert "benchmarks/run_track_link_day.py" not in [j.script for j in R.jobs()]


def test_check_needs_no_credentials(monkeypatch, capsys):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.setattr("sys.argv", ["regenerate_track_v5.py", "--check", "--allow-stale"])
    R.main()
    out = capsys.readouterr().out
    assert "analysis at" in out
    assert "two_days" in out and "pipeline_day" in out
    assert "final_grid" not in out


def test_v4_still_lists_its_own_jobs(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["regenerate_track_v4.py", "--check", "--allow-stale"])
    V4.main()
    out = capsys.readouterr().out
    assert "final_grid" in out and "two_days" not in out


def test_a_v5_job_goes_stale_when_its_arguments_change(tmp_path, monkeypatch):
    job = _job("pipeline_day")
    monkeypatch.setattr(job, "data", tmp_path)
    for n in job.outputs:
        (tmp_path / n).write_text("a\n")
        R.provenance.record(tmp_path, n, job.script, job.args, job.code_paths)
    assert job.stale() == {}
    job.args = ["--day", "2026-06-15"]
    assert set(job.stale().values()) == {"arguments changed"}
