"""The runner must not stamp away what the job it ran recorded.

``Job.run`` stages an output and re-records its provenance, because it knows
the staged name and the full dependency set the spec declares. It used to
re-record without ``inputs``, replacing the entry the script had just written
with a poorer one -- so every staged output carried ``inputs: {}`` and the row
counts each script prints were nowhere in the manifest.

No Spark and no subprocess here: ``subprocess.run`` is replaced by a stand-in
that writes what a real job writes into ``--results-dir``. That is the whole
contract between runner and job, and it is what the fix turns on.
"""

import types
from pathlib import Path

import provenance
import regenerate_track_v1 as reg


def _fake_script(produced, inputs, input_tables=()):
    """A stand-in for ``subprocess.run``: writes an output and its manifest."""

    def run(cmd, cwd=None):
        results_dir = Path(cmd[cmd.index("--results-dir") + 1])
        (results_dir / produced).write_text("metric,value\nrows,1\n")
        provenance.record(
            results_dir, produced,
            script="benchmarks/fake_job.py", argv=list(cmd[3:]),
            code_paths=["benchmarks/fake_job.py"],
            inputs=inputs, input_tables=list(input_tables),
        )
        return types.SimpleNamespace(returncode=0)

    return run


def _job():
    return reg.Job(
        name="fake", script="benchmarks/fake_job.py", args=["--period", "2025"],
        outputs={"produced.csv": "staged.csv"}, code_paths=[],
        notes="a stand-in job",
    )


def test_job_run_keeps_the_input_counts_the_script_recorded(tmp_path, monkeypatch):
    data = tmp_path / "data"
    monkeypatch.setattr(provenance, "s3_identity", lambda prefix: {"objects": 3})
    monkeypatch.setattr(
        reg, "subprocess",
        types.SimpleNamespace(run=_fake_script(
            "produced.csv",
            inputs={"samples": 12345, "gt_flights": 678},
            input_tables=["s3a://eurocontrol/opdi/research/tracks_2025"],
        )),
    )
    monkeypatch.setattr(reg, "DATA", data)

    _job().run()

    entry = provenance.load_manifest(data)["staged.csv"]
    assert entry["inputs"] == {"samples": 12345, "gt_flights": 678}
    assert list(entry["input_tables"]) == [
        "s3a://eurocontrol/opdi/research/tracks_2025"
    ]
    # The runner's own knowledge still wins where it is the authority: the
    # staged name, the spec's argv and notes, and the declared dependency set.
    assert entry["script"] == "benchmarks/fake_job.py"
    assert entry["argv"] == ["--period", "2025"]
    assert entry["notes"] == "a stand-in job"
    assert entry["code_paths"] == ["benchmarks/fake_job.py"]
    assert (data / "staged.csv").is_file()


def test_job_run_records_an_output_whose_script_recorded_nothing(tmp_path, monkeypatch):
    """A job that writes no manifest of its own must still be stamped.

    ``inputs`` is then genuinely empty rather than lost, and the entry has to
    exist: an output with no manifest entry is reported in the paper as
    unverified.
    """
    data = tmp_path / "data"
    monkeypatch.setattr(reg, "DATA", data)

    def run(cmd, cwd=None):
        results_dir = Path(cmd[cmd.index("--results-dir") + 1])
        (results_dir / "produced.csv").write_text("metric,value\nrows,1\n")
        return types.SimpleNamespace(returncode=0)

    monkeypatch.setattr(reg, "subprocess", types.SimpleNamespace(run=run))

    _job().run()

    entry = provenance.load_manifest(data)["staged.csv"]
    assert entry["inputs"] == {}
    assert entry["input_tables"] == {}
