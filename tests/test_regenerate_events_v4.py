"""Guards on the V4 job registry.

Needs no cluster: these check the registry's *shape*, which is where an
analysis chain goes wrong quietly. A job whose declared dependencies are
incomplete looks current while serving numbers from code that has since
changed, and nothing in the file or its timestamp says so.

They also guard the two ways this study could report something other than what
it claims: writing into the frozen V3 paper's directory, and scoring the study
set against a ground truth built over every APDF airport.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks"))

import regenerate_events_v4 as v4


def _args(job):
    return [str(a) for a in job.args]


def test_the_paper_is_v4_and_not_v3():
    """V3 is published and frozen. A stray output path is the one way this
    study could overwrite its figures."""
    assert v4.PAPER.name == "flight-events-v4"
    assert v4.DATA.parent.name == "flight-events-v4"
    for job in v4.jobs() + v4.stages():
        assert "flight-events-v3" not in " ".join(_args(job)), job.name


def test_every_job_runs_the_v4_period_only():
    assert v4.PERIODS == ("2026",)
    for job in v4.jobs():
        assert "2026" in _args(job), job.name
        assert "2025" not in _args(job), job.name


def test_the_ladder_and_the_comparison_run_the_v4_arm():
    jobs = {j.name: j for j in v4.jobs()}

    assert "--ladder" in _args(jobs["ladder_2026"])
    assert _args(jobs["ladder_2026"])[_args(jobs["ladder_2026"]).index("--ladder") + 1] == "v4"
    compare = _args(jobs["compare_2026"])
    assert compare[compare.index("--ladder") + 1] == "v4"
    assert compare[compare.index("--rung") + 1] == v4.SHIPPED_RUNG


def test_every_job_scores_the_same_population():
    """The bridge rate is the ceiling on coverage, so it has to be computed
    over the same aerodromes coverage is. A study-set ladder against an
    all-airports truth divides every coverage figure by the wrong denominator
    and reads as a detector failure.

    This pins the *flag*. That the flag reaches the bridge report at all is
    pinned in ``test_events_gt_study_set.py`` -- it did not, at first: the
    report was computed before the filter, so the staged JSON was network-wide
    while everything it capped was not.
    """
    for job in v4.jobs():
        args = _args(job)
        assert args[args.index("--airports") + 1] == "study", job.name


def test_the_detector_dependencies_name_the_v4_modules():
    """A ladder scored against a changed detector is scoring something other
    than what it names. These four are exactly the code the V4 rungs switch
    on, so an edit to any of them must mark the ladder stale."""
    for module in ("runway_ops", "vertical_pru", "layout", "elevation"):
        assert f"src/opdi/pipeline/{module}.py" in v4.DETECT, module


def test_the_ladder_depends_on_the_flight_list_the_stage_builds():
    """The stage's product is the ladder's input. Declared, so a rebuilt flight
    list marks the ladder stale rather than passing unnoticed."""
    stage = {s.name: s for s in v4.stages()}["flight_list_2026"]
    ladder = {j.name: j for j in v4.jobs()}["ladder_2026"]

    assert stage.produces == v4.T_FLIGHT_LIST
    assert v4.T_FLIGHT_LIST in ladder.inputs
    assert v4.T_TRACKS_CLEAN in stage.inputs


def test_the_chain_declares_every_table_the_paper_reads():
    """The declared set must be everything the scripts write, not a subset.

    `regenerate` copies only *declared* outputs into the paper's data/, and
    computes staleness only over declared outputs. An output a job produces but
    does not declare is therefore written to the staging directory, dropped,
    and invisible to `--check` -- while the paper goes on reading whatever
    stale copy is already sitting in data/. That is not hypothetical: this set
    once held six names against the seven `events_compare.py` writes, and the
    paper's per-airport-by-detector, pooling-rules and ring figures were being
    read from a manual run made three commits before the on_ground fix, with
    every job reporting `ok`.

    `floor_2026.csv` is deliberately absent. The inter-source floor needs the
    _CTFM columns of the full APDF extract and apdf_full_202606 has never been
    extracted, so the floor comes back empty and `write_csv` writes no file at
    all -- declaring it fails the job outright.
    """
    staged = {name for job in v4.jobs() for name in job.outputs.values()}

    assert staged == {
        "bridge_2026.json", "ladder_2026.csv", "inventory_2026.csv",
        "per_airport_2026.csv", "per_airport_by_detector_2026.csv",
        "pooling_rules_2026.csv", "rings_2026.csv",
        "runway_2026.csv", "resolution_2026.csv",
    }
    assert "floor_2026.csv" not in staged


def test_a_missing_output_is_stale_rather_than_assumed_current(tmp_path, monkeypatch):
    """`--check` must be able to say 'stale' without credentials or a cluster."""
    monkeypatch.setattr(v4, "DATA", tmp_path)

    for job in v4.jobs():
        assert job.stale(), job.name
