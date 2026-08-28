"""The V2 spec must declare dependencies that exist, and outputs that connect.

``provenance.fingerprint`` hashes the literal bytes ``<missing>`` for a path
that is not there. It does not warn and it does not fail -- it just folds a
constant into the hash, so a misspelled dependency stops marking anything stale
for the rest of the study's life. The brief for this task named the cleaning
modules under ``src/opdi/pipeline/cleaning/``, which is not where they live;
without this test that would have been discovered by a paper quietly rendering
stale numbers months later.

No Spark, no cluster, no credentials.
"""

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

import regenerate_track_v2 as reg  # noqa: E402
import track_continuity as tc  # noqa: E402


def test_every_declared_dependency_is_a_file_that_exists():
    missing = sorted(
        {p for j in reg.jobs() for p in j.code_paths if not (REPO / p).is_file()}
    )
    assert missing == [], f"declared but absent: {missing}"


def test_the_cleaning_modules_are_declared_at_their_real_path():
    """Specifically the path the brief got wrong, so a regression names itself."""
    paths = {p for j in reg.jobs() for p in j.code_paths}
    assert "src/opdi/cleaning/native.py" in paths
    assert "src/opdi/cleaning/cleaner.py" in paths
    assert not any(p.startswith("src/opdi/pipeline/cleaning/") for p in paths)


def test_the_pipeline_job_declares_the_steps_it_runs():
    """V2's whole difference from V1 is that the steps can move a number."""
    job = next(j for j in reg.jobs() if j.name == "pipeline_2025")
    for step in ("src/opdi/pipeline/tracks.py", "src/opdi/pipeline/flights.py",
                 "src/opdi/ingestion/osn_statevectors.py"):
        assert step in job.code_paths


def test_the_pipeline_job_runs_all_three_arms_in_ablation_order():
    """Only the ends gives a total the release note cannot decompose."""
    job = next(j for j in reg.jobs() if j.name == "pipeline_2025")
    i = job.args.index("--methods")
    assert job.args[i + 1:i + 4] == ["legacy", "airframe_only", "standard"]


def test_the_pipeline_job_declares_the_extents_the_continuity_job_reads():
    """The producer's outputs and the consumer's filenames must be one name.

    Both sides go through ``track_continuity.extents_name``; this asserts that
    they do, so a rename cannot leave the continuity job hunting for a file the
    pipeline job no longer writes.
    """
    job = next(j for j in reg.jobs() if j.name == "pipeline_2025")
    for method in reg.METHODS:
        name = tc.extents_name(method, "2025")
        assert job.outputs.get(name) == name


def test_continuity_carries_the_pipeline_dependencies_too():
    """Its input is the pipeline job's output, so what moves one moves it."""
    pipe = next(j for j in reg.jobs() if j.name == "pipeline_2024")
    cont = next(j for j in reg.jobs() if j.name == "continuity_2024")
    assert set(pipe.code_paths) - {pipe.script} <= set(cont.code_paths)


def test_one_day_per_period_and_it_is_the_same_day_both_years():
    assert reg.DAYS == {"2025": "2025-06-05", "2024": "2024-06-05"}
    for period, day in reg.DAYS.items():
        job = next(j for j in reg.jobs() if j.name == f"pipeline_{period}")
        assert job.args[job.args.index("--days") + 1] == day
