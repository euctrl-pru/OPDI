"""Guards on the v6.2 job registry.

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

import flight_list_v62
import regenerate_v62


def _args(job):
    return [str(a) for a in job.args]


def test_the_paper_is_v62_and_not_v6_v61_or_v7():
    """V6 and V7 are frozen. A stray output path is the one way this study
    could overwrite a published paper's figures."""
    assert regenerate_v62.PAPER.name == "adep-ades-detection-v6.2"
    assert regenerate_v62.DATA.parent.name == "adep-ades-detection-v6.2"


#: The only jobs with any business naming V6's directory, and both only read
#: it: one compares its committed sweep, the other its recorded input
#: identities. Any third would need justifying, so the list is explicit.
V6_READERS = {"sweep_equivalence", "input_drift"}


def test_only_the_named_checks_may_even_mention_v6s_directory():
    """V6 is frozen. Two jobs read it as evidence; no other job should name it.

    V7 and v6.1 are off limits entirely.
    """
    for job in regenerate_v62.jobs():
        joined = " ".join(_args(job))
        assert "adep-ades-detection-v7/" not in joined, job.name
        assert "adep-ades-detection-v6.1/" not in joined, job.name
        if "adep-ades-detection-v6/" in joined:
            assert job.name in V6_READERS, (
                f"{job.name} names V6's directory; only {sorted(V6_READERS)} "
                f"may, and only to read it")


def test_the_drift_check_reads_v6s_manifest():
    """The claim that v6.2 recomputes v6 with one variable changed is only
    true if nothing else moved. It did -- the candidate table was rebuilt --
    so the drift is measured on every render rather than remembered here."""
    jobs = {j.name: j for j in regenerate_v62.jobs()}
    args = " ".join(_args(jobs["input_drift"]))
    assert "_manifest.json" in args
    assert "adep-ades-detection-v6/" in args
    assert "--executors" not in args      # no cluster needed


def test_every_output_is_staged_by_name_not_by_path():
    """`Job.run` copies each output into this study's own DATA directory, so a
    bare filename cannot escape it. A path here would mean a job writing
    wherever it likes -- including over a frozen paper's figures."""
    for job in regenerate_v62.jobs():
        for produced, staged in job.outputs.items():
            assert "/" not in produced, f"{job.name}: {produced}"
            assert "/" not in staged, f"{job.name}: {staged}"


def test_the_equivalence_check_reads_both_periods_and_needs_no_cluster():
    """A one-period check would license retiring only one of the two jobs.
    And it must stay cluster-free, or the cheapest guard in the study becomes
    the one nobody can run."""
    jobs = {j.name: j for j in regenerate_v62.jobs()}
    args = " ".join(_args(jobs["sweep_equivalence"]))
    assert "trend_sweep_2025.csv" in args
    assert "trend_sweep_2024.csv" in args
    assert "fl_sweep_2025.csv" in args
    assert "fl_sweep_2024.csv" in args
    assert "--executors" not in args
    assert jobs["sweep_equivalence"].inputs == []


def test_the_vote_caches_are_the_agl_ones():
    """Building into `research/trend_votes` would leave V6 reproducible only
    against a cache built for a different study, on a different datum."""
    assert regenerate_v62.T_VOTES.endswith("trend_votes_agl")
    assert regenerate_v62.T_VOTES24.endswith("trend_votes_agl_2024")
    for stage in regenerate_v62.stages():
        assert not stage.produces.endswith("research/trend_votes"), stage.name
        assert not stage.produces.endswith("research/trend_votes_2024"), stage.name


def test_no_job_runs_v6s_or_v61s_scripts():
    """The fork is only worth having if it is actually used."""
    for job in regenerate_v62.jobs():
        assert "flight_list_v6.py" not in job.script, job.name
        assert "flight_list_v61.py" not in job.script, job.name
        assert job.script != "benchmarks/trend_sweep.py", job.name
        for path in job.code_paths:
            assert path not in ("benchmarks/flight_list_v6.py",
                                "benchmarks/flight_list_v61.py",
                                "benchmarks/trend_sweep.py"), job.name


def test_the_retired_trend_sweeps_are_gone():
    """V6's trend_sweep jobs are superseded by the paired AGL sweeps, whose
    sea-level arm reproduces them cell for cell on both periods. Keeping them
    would mean two jobs computing the same 371 cells on the same datum."""
    names = {j.name for j in regenerate_v62.jobs()}
    assert "trend_sweep_2025" not in names
    assert "trend_sweep_2024" not in names


def test_the_paired_sweeps_cover_both_periods_and_both_datums():
    """An arm measured on one period and reported as the study's result is the
    failure this study's own notes keep recording."""
    names = {j.name for j in regenerate_v62.jobs()}
    for arm in ("height_sweep", "fl_sweep"):
        assert f"{arm}_2025" in names, arm
        assert f"{arm}_2024" in names, arm


def test_the_paired_sweeps_read_the_same_cache_per_period():
    """The pairing is the point: one cache carries both datums' votes, so the
    two curves are the same measurement read two ways. Reading different
    caches would make the comparison a comparison of caches."""
    jobs = {j.name: j for j in regenerate_v62.jobs()}
    for period in ("2025", "2024"):
        h = _args(jobs[f"height_sweep_{period}"])
        f = _args(jobs[f"fl_sweep_{period}"])
        assert h[h.index("--cache") + 1] == f[f.index("--cache") + 1], period
        assert h[h.index("--datum") + 1] == "field", period
        assert f[f.index("--datum") + 1] == "msl", period


def test_every_pipeline_job_fingerprints_the_pipeline():
    """A job that runs process_dai must fingerprint flights.py and config.py.
    Without them the datum change would not mark its own results stale, which
    is the whole mechanism the paper's provenance rests on."""
    for job in regenerate_v62.jobs():
        if "flight_list_v62" not in job.script:
            continue
        assert "src/opdi/pipeline/flights.py" in job.code_paths, job.name
        assert "src/opdi/config.py" in job.code_paths, job.name
        assert "benchmarks/flight_list_v62.py" in job.code_paths, job.name


def test_arm_c_fingerprints_the_banding_it_reads_along():
    """Arm C's conclusion moves if the bands move, so a band edit must mark it
    stale. It is the study's discriminating measurement; serving it from a
    stale cache would be the most expensive possible silent failure."""
    for job in regenerate_v62.jobs():
        if job.name.startswith("elevation_bands"):
            assert "benchmarks/elevation_bands.py" in job.code_paths, job.name
            assert "benchmarks/elevation_arms.py" in job.code_paths, job.name


def test_the_pipeline_arms_are_2025_only():
    """Stated as a rule so nobody re-adds a 2024 pipeline arm that cannot run.

    `process_dai` reads `h3_res_7` straight off the track table. The 2024
    tracks pre-date H3 indexing, so the column is absent and the run dies on
    UNRESOLVED_COLUMN -- after the 2025 half has already been computed, which
    is the expensive place to discover it. V6 met the same wall and ran the
    pipeline on 2025 alone.
    """
    names = {j.name for j in regenerate_v62.jobs()}
    for arm in ("datum_swap", "elevation_bands", "modes", "trend_grid",
                "pipeline_path", "pipeline_path_ring"):
        assert f"{arm}_2025" in names, arm
        assert f"{arm}_2024" not in names, (
            f"{arm}_2024 cannot run: the 2024 tracks carry no h3_res_7")


def test_every_staged_output_name_is_unique():
    """Two jobs staging the same filename silently overwrite each other's
    provenance -- which is exactly how V6 lost two entries."""
    seen = {}
    for job in regenerate_v62.jobs():
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
    for job in regenerate_v62.jobs():
        if "flight_list_v62" not in job.script:
            continue
        args = _args(job)
        if "--tracks" not in args:
            continue
        value = args[args.index("--tracks") + 1]
        assert not value.startswith(("s3a://", "s3://")), (
            f"{job.name} passes a URI to the pipeline: {value!r}")


def test_the_sweep_gets_a_uri_not_a_relative_name():
    """The mirror image: the sweep reads parquet itself and a bare name would
    resolve against the working directory."""
    for job in regenerate_v62.jobs():
        if "trend_sweep_agl" not in job.script:
            continue
        args = _args(job)
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
    assert regenerate_v62.PAPER.parent.name == "papers"
    assert regenerate_v62.PAPER.parent.parent.name == "opdi-portal"
    assert regenerate_v62.PAPER.parent.is_dir()
    assert ".claude" not in str(regenerate_v62.PAPER)


def test_pipeline_jobs_are_tuned_against_the_datum_they_run_on():
    """Passing the sea-level sweep as `--trend-sweep` would select a ceiling in
    flight levels and then apply it as feet above field -- FL60 becoming 60 ft,
    which abstains on everything."""
    for job in regenerate_v62.jobs():
        if "flight_list_v62" not in job.script:
            continue
        args = _args(job)
        if "--trend-sweep" not in args:
            continue
        value = args[args.index("--trend-sweep") + 1]
        assert "height_sweep" in value, f"{job.name} reads {value}"


def test_the_path_walk_is_given_a_sea_level_sweep_for_its_lower_rungs():
    """Rungs 0-4 must sit on the sea-level datum, or the datum has already
    moved before rung 5 -- the rung whose entire purpose is to move it. Rung 5
    would then report zero, which reads as evidence and is an artefact."""
    for job in regenerate_v62.jobs():
        if not job.name.startswith("pipeline_path"):
            continue
        args = _args(job)
        assert "--trend-sweep-msl" in args, job.name
        value = args[args.index("--trend-sweep-msl") + 1]
        assert "fl_sweep" in value, f"{job.name} reads {value}"


def test_the_path_walk_carries_the_datum_rung():
    for job in regenerate_v62.jobs():
        if not job.name.startswith("pipeline_path"):
            continue
        args = _args(job)
        assert "path5_datum" in args, job.name
        assert "path2_ceiling" in args, job.name
        # `path2_flcap` is V6's label. On the field datum it is not a
        # flight-level cap, and a run label that says otherwise gets quoted.
        assert "path2_flcap" not in args, job.name


def test_the_grid_runs_match_the_declared_grid():
    """The run labels and the --grid-* lists are two spellings of one grid.
    If they drift, the job asks for cells it never names and scores cells it
    never ran."""
    jobs = {j.name: j for j in regenerate_v62.jobs()}
    args = _args(jobs["trend_grid_2025"])

    def values_after(flag):
        out = []
        for a in args[args.index(flag) + 1:]:
            if a.startswith("--"):   # the next flag ends this list
                break
            out.append(a)
        return out

    assert sorted(map(int, values_after("--grid-height"))) == \
        sorted(flight_list_v62.GRID_HEIGHT_CAPS)
    assert sorted(map(int, values_after("--grid-margin"))) == \
        sorted(flight_list_v62.GRID_MARGINS)
    for cap in flight_list_v62.GRID_HEIGHT_CAPS:
        assert any(a.startswith(f"grid_h{cap}_") for a in args), cap

    # Every cell named, not merely every value mentioned somewhere: the count
    # is what catches a run list that dropped a dimension.
    expected = (len(flight_list_v62.GRID_HEIGHT_CAPS) * 2
                * len(flight_list_v62.GRID_MARGINS))
    named = [a for a in args if a.startswith("grid_h")]
    assert len(named) == expected, f"{len(named)} runs named, expected {expected}"
    assert len(set(named)) == len(named), "duplicate run names"


def test_the_grid_brackets_the_shipped_ceiling():
    """The shipped default was never on the sweep grid, so the claim that the
    ceiling is tuned rested on a grid missing the tuned value."""
    from opdi.config import DetectionConfig
    assert DetectionConfig().trend_max_height_ft in flight_list_v62.GRID_HEIGHT_CAPS
    assert 6100 in flight_list_v62.GRID_HEIGHT_CAPS


def _script_source(job):
    return (Path(__file__).resolve().parent.parent / job.script).read_text()


def test_every_flag_a_job_passes_exists_in_the_script_it_runs():
    """argparse rejects unknown flags, and the run dies at second zero -- but
    only once it is reached, which for a job at the end of the chain is hours
    in, after everything before it has already been computed.

    This caught `--out-name` being passed to `flight_list_v62.py`, which is
    v6.1's interface: the flag was never ported when the job was repointed at
    the new script.
    """
    for job in regenerate_v62.jobs():
        src = _script_source(job)
        for flag in (a for a in _args(job) if a.startswith("--")):
            assert f'"{flag}"' in src, (
                f"{job.name} passes {flag}, which {job.script} does not define")


def test_every_required_flag_is_supplied():
    """The mirror image: a missing required flag is the same failure, and the
    same delay before anyone finds out."""
    import re
    for job in regenerate_v62.jobs():
        src = _script_source(job)
        required = set(re.findall(
            r'add_argument\(\s*"(--[a-z0-9-]+)"[^)]*required=True', src, re.S))
        passed = {a for a in _args(job) if a.startswith("--")}
        # `--results-dir` is supplied by the runner, not by the job.
        missing = required - passed - {"--results-dir"}
        assert not missing, f"{job.name} omits required {sorted(missing)}"


def test_every_expected_output_is_a_filename_the_script_writes():
    """A job whose `outputs` key names a file the script never produces fails
    in `Job.run` with 'did not produce X' -- after the Spark work is done and
    thrown away. This caught `datum_comparison.csv`, which is what v6.1's
    runner wrote and v6.2's does not."""
    for job in regenerate_v62.jobs():
        src = _script_source(job)
        for produced in job.outputs:
            assert produced in src, (
                f"{job.name} expects {produced}, which {job.script} never writes")


def test_arm_c_consumes_what_arm_a_produces():
    """Arm C reads Arm A's per-airport CSV by name. If Arm A stops staging that
    file, Arm C fails on a missing path hours into a run -- or worse, reads a
    stale copy left by an earlier one."""
    jobs = {j.name: j for j in regenerate_v62.jobs()}
    produced = set(jobs["datum_swap_2025"].outputs.values())
    assert "per_airport_datum_2025.csv" in produced
    consumed = " ".join(_args(jobs["elevation_bands_2025"]))
    assert "per_airport_datum_2025.csv" in consumed
