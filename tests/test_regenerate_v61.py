"""Guards on the v6.1 job registry.

Needs no cluster: these check the registry's *shape*, which is where this study
has historically gone wrong. A job whose declared dependencies are incomplete
looks current while serving numbers from code that has since changed, and
nothing in the file or its timestamp says so.

They also guard the one way this study could damage a published paper --
writing into V6's or V7's directory, or over V6's vote cache -- which would not
be visible in a diff of the paper's own source.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks"))

import pytest

import regenerate_v61


def test_every_pipeline_job_fingerprints_the_pipeline():
    """A job that runs process_dai must fingerprint flights.py and config.py.
    Without them the datum change would not mark its own results stale, which
    is the whole mechanism the paper's provenance rests on.
    """
    for job in regenerate_v61.jobs():
        if "flight_list_v61" in job.script:
            assert "src/opdi/pipeline/flights.py" in job.code_paths, job.name
            assert "src/opdi/config.py" in job.code_paths, job.name


def test_arm_c_fingerprints_the_banding_it_reads_along():
    """Arm C's conclusion moves if the bands move, so a band edit must mark it
    stale. It is the study's discriminating measurement; serving it from a
    stale cache would be the most expensive possible silent failure."""
    for job in regenerate_v61.jobs():
        if job.name.startswith("elevation_bands"):
            assert "benchmarks/elevation_bands.py" in job.code_paths, job.name
            assert "benchmarks/elevation_arms.py" in job.code_paths, job.name


def test_the_paper_is_v61_and_not_v6_or_v7():
    """V6 and V7 are frozen. A stray output path is the one way this study
    could overwrite a published paper's figures."""
    assert regenerate_v61.PAPER.name == "adep-ades-detection-v6.1"
    assert regenerate_v61.DATA.parent.name == "adep-ades-detection-v6.1"


def test_the_vote_caches_are_not_v6s():
    """Building into `research/trend_votes` would leave V6 reproducible only
    against a cache built for a different study, on a different datum."""
    assert regenerate_v61.T_VOTES.endswith("trend_votes_agl")
    assert regenerate_v61.T_VOTES24.endswith("trend_votes_agl_2024")
    for stage in regenerate_v61.stages():
        assert not stage.produces.endswith("research/trend_votes"), stage.name
        assert not stage.produces.endswith("research/trend_votes_2024"), stage.name


def test_no_job_runs_v6s_scripts():
    """The fork is only worth having if it is actually used."""
    for job in regenerate_v61.jobs():
        assert "flight_list_v6.py" not in job.script, job.name
        assert job.script != "benchmarks/trend_sweep.py", job.name
        for path in job.code_paths:
            assert path != "benchmarks/flight_list_v6.py", job.name
            assert path != "benchmarks/trend_sweep.py", job.name


def test_both_periods_are_covered():
    """An arm measured on one period and reported as the study's result is the
    failure this study's own notes keep recording."""
    names = {j.name for j in regenerate_v61.jobs()}
    for arm in ("datum_swap", "elevation_bands", "height_sweep", "fl_sweep",
                "height_pipeline"):
        assert f"{arm}_2025" in names, arm
        assert f"{arm}_2024" in names, arm


def test_every_staged_output_name_is_unique():
    """Two jobs staging the same filename silently overwrite each other's
    provenance -- which is exactly how V6 lost two entries."""
    seen = {}
    for job in regenerate_v61.jobs():
        for staged in job.outputs.values():
            assert staged not in seen, f"{staged}: {seen.get(staged)} vs {job.name}"
            seen[staged] = job.name


def test_the_pipeline_gets_a_relative_track_name_not_a_uri():
    """Two conventions, and mixing them fails two hours into a run.

    `trend_sweep_agl.py` reads parquet directly and wants a full `s3a://` URI.
    `FlightListProcessor(tracks_table=...)` wants a bucket-relative name and
    prefixes it itself, so handing it the URI splices
    `s3a://eurocontrol/opdi/s3a:/eurocontrol/opdi/research/tracks` and dies on
    PATH_NOT_FOUND -- after the 2025 half of the study has already run.
    """
    for job in regenerate_v61.jobs():
        if "flight_list_v61" not in job.script:
            continue
        args = job.args
        if "--tracks" not in args:
            continue
        value = args[args.index("--tracks") + 1]
        assert not value.startswith(("s3a://", "s3://")), (
            f"{job.name} passes a URI to the pipeline: {value!r}"
        )


def test_the_sweep_gets_a_uri_not_a_relative_name():
    """The mirror image: the sweep reads parquet itself and a bare name would
    resolve against the working directory."""
    for job in regenerate_v61.jobs():
        if "trend_sweep_agl" not in job.script:
            continue
        args = job.args
        if "--cache" in args:
            assert args[args.index("--cache") + 1].startswith("s3a://"), job.name


def test_the_portal_is_found_from_inside_a_worktree():
    """`REPO.parent / "opdi-portal"` is right when opdi/ sits in the workspace
    root and wrong inside a git worktree, which lives three levels deeper.

    The failure is nasty because it is silent: every output reads "output
    missing" -- indistinguishable from "never generated", and the opposite of
    what a fingerprint result would mean. A whole `--check` run looked like
    evidence about staleness when it was a path bug.
    """
    assert regenerate_v61.PAPER.parent.name == "papers"
    assert regenerate_v61.PAPER.parent.parent.name == "opdi-portal"
    # The portal must actually exist, not merely be spelled correctly.
    assert regenerate_v61.PAPER.parent.is_dir()
    assert ".claude" not in str(regenerate_v61.PAPER)


def test_arm_c_consumes_what_arm_a_produces():
    """Arm C reads Arm A's per-airport CSV by name. If Arm A stops staging that
    file, Arm C fails on a missing path hours into a run -- or worse, reads a
    stale copy left by an earlier one."""
    jobs = {j.name: j for j in regenerate_v61.jobs()}
    for period in ("2025", "2024"):
        produced = set(jobs[f"datum_swap_{period}"].outputs.values())
        assert f"per_airport_datum_{period}.csv" in produced
        consumed = " ".join(jobs[f"elevation_bands_{period}"].args)
        assert f"per_airport_datum_{period}.csv" in consumed
