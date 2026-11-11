"""The track-construction V4 paper's entrypoint: every staged file has one job."""

import json
from pathlib import Path

import regenerate_track_v4 as R


def _job(name):
    return next(j for j in R.jobs() if j.name == name)


def test_every_job_declares_its_outputs_and_dependencies():
    for job in R.jobs():
        assert job.outputs, f"{job.name} stages nothing"
        for p in job.code_paths:
            assert (Path(R.REPO) / p).is_file(), f"{job.name}: missing {p}"


def test_job_names_are_unique():
    names = [j.name for j in R.jobs()]
    assert len(names) == len(set(names))


def test_no_two_jobs_stage_one_file():
    # Each job has its own subdirectory, so the file is (directory, name).
    staged = [(j.data, s) for j in R.jobs() for s in j.outputs.values()]
    assert len(staged) == len(set(staged))


def test_each_job_has_its_own_manifest_directory():
    dirs = [j.data for j in R.jobs()]
    assert len(dirs) == len(set(dirs))
    assert {d.name for d in dirs} == {"final_grid", "final_edges", "stage2"}


def test_the_day_run_is_a_prerequisite_not_a_job():
    assert "run_track_link_day.py" in R.__doc__
    assert "prerequisite, not a job" in R.__doc__
    assert "benchmarks/run_track_link_day.py" not in [j.script for j in R.jobs()]
    assert "benchmarks/run_track_link_day.py" in _job("stage2").code_paths


def test_earlier_rounds_are_documented_as_not_regenerated():
    for name in ("round1", "round2", "round3_fl", "round4_fl180"):
        assert name in R.__doc__
    assert "not\nregenerated" in R.__doc__ or "not regenerated" in R.__doc__


def test_final_grid_arguments():
    args = _job("final_grid").args
    grid = json.loads(args[args.index("--grid-json") + 1])
    assert grid["link_cruise_floor_ft"] == [18000]
    assert grid["link_min_implied_speed_kt"] == [315, 400]
    assert grid["link_min_speed_ratio"] == [0.0, 0.85]
    assert grid["link_min_stop_minutes"] == [0.0, 20.0, 35.0]
    assert grid["link_max_gap_minutes"] == [360, 480, 600]
    assert grid["link_old_edge_climb"] == [True]
    assert grid["link_new_edge_descent"] == [False]
    assert args[args.index("--days") + 1 : args.index("--grid-json")] == [
        "2026-06-14",
        "2025-06-14",
    ]


def test_final_edges_arguments():
    args = _job("final_edges").args
    grid = json.loads(args[args.index("--grid-json") + 1])
    assert grid == {
        "link_min_implied_speed_kt": [400],
        "link_min_speed_ratio": [0.0],
        "link_min_stop_minutes": [0.0],
        "link_max_gap_minutes": [600],
        "link_cruise_floor_ft": [18000],
        "link_old_edge_climb": [False, True],
        "link_new_edge_descent": [False, True],
    }


def test_final_edges_stages_into_its_own_directory_and_says_what_it_varies():
    job = _job("final_edges")
    assert job.data == R.DATA / "final_edges"
    assert "directional" not in job.notes
    assert "final_direction" not in R.__doc__


def test_stage2_arguments():
    assert _job("stage2").args == ["--day", "2026-06-14"]


def test_every_job_depends_on_the_segmentation_code():
    for job in R.jobs():
        assert set(R.CORE) <= set(job.code_paths), job.name


def test_paper_dir_is_overridable_and_is_v4():
    assert "OPDI_PAPER_DIR" in Path(R.__file__).read_text()
    assert R.PAPER.name == "track-construction-v4"
    assert R.DATA.name == "data"


def test_check_needs_no_credentials(monkeypatch, capsys):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.setattr("sys.argv", ["regenerate_track_v4.py", "--check", "--allow-stale"])
    R.main()
    assert "analysis at" in capsys.readouterr().out


def test_every_job_covers_its_scripts_own_code_paths():
    for job in R.jobs():
        assert set(R.script_code_paths(job.script)) <= set(job.code_paths), job.name


def test_changed_arguments_mark_a_job_stale(tmp_path, monkeypatch):
    job = _job("stage2")
    monkeypatch.setattr(job, "data", tmp_path)
    for n in job.outputs:
        (tmp_path / n).write_text("a\n")
        R.provenance.record(tmp_path, n, job.script, job.args, job.code_paths)
    assert job.stale() == {}
    job.args = ["--day", "2026-06-15"]
    assert set(job.stale().values()) == {"arguments changed"}


def test_missing_optional_reasons_are_not_stale_but_a_recorded_one_is(tmp_path, monkeypatch):
    job = _job("final_grid")
    assert job.optional <= set(job.outputs)
    monkeypatch.setattr(job, "data", tmp_path)
    for n in job.outputs:
        if n not in job.optional:
            (tmp_path / n).write_text("a\n")
            R.provenance.record(tmp_path, n, job.script, job.args, job.code_paths)
    assert job.stale() == {}
    gone = sorted(job.optional)[0]
    (tmp_path / gone).write_text("a\n")
    R.provenance.record(tmp_path, gone, job.script, job.args, job.code_paths)
    (tmp_path / gone).unlink()
    assert job.stale() == {gone: "output missing"}


def test_the_paper_inputs_are_declared():
    names = set(_job("final_grid").outputs)
    for stem in ("link_reasons", "link_positive_reasons", "link_negative_reasons"):
        assert f"{stem}_pooled.csv" in names
    assert set(_job("final_edges").outputs) == {"link_grid_pooled.csv", "link_controls_pooled.csv"}
