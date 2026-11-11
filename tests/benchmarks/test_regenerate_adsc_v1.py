"""The ADS-C paper's entrypoint: every staged CSV has exactly one job."""

from pathlib import Path

import regenerate_adsc_v1 as R


def test_every_job_declares_its_outputs_and_dependencies():
    for job in R.jobs():
        assert job.outputs, f"{job.name} stages nothing"
        # `code_paths` always carries the script itself (Job.__init__ prepends
        # it), so an empty declared set is legal -- `census` reads the raw
        # topic and no pipeline code can change its answer.
        for p in job.code_paths:
            assert (Path(R.REPO) / p).exists(), f"{job.name}: missing {p}"


def test_job_names_are_unique():
    names = [j.name for j in R.jobs()]
    assert len(names) == len(set(names))


def test_no_two_jobs_stage_the_same_file():
    # The property the whole provenance scheme rests on: one output, one job,
    # one command. Two jobs writing one CSV makes its manifest entry a lie
    # about which run produced it.
    staged = [s for j in R.jobs() for s in j.outputs.values()]
    assert len(staged) == len(set(staged))


def test_the_arm_build_is_declared_as_a_prerequisite_not_a_job():
    # run_adsc_day.py costs ~6 cluster-hours for three days and two arms. It is
    # deliberately not a job here -- but it must be named, or a reader
    # concludes that `--force` reproduces the paper from nothing.
    assert "run_adsc_day" in R.__doc__
    assert "run_adsc_day.py" not in [j.script for j in R.jobs()]


def test_every_pipeline_job_depends_on_the_code_that_can_change_its_answer():
    # A job whose dependency set omits the mapping, the merge or the cleaner
    # would read as current after a change that invalidates it, and the page
    # would render stale numbers with a clean provenance table.
    core = set(R.CORE)
    for job in R.jobs():
        if job.name == "census":
            continue  # reads the raw topic; no pipeline code can change it
        assert core <= set(job.code_paths), job.name


def test_the_census_job_depends_on_no_pipeline_code():
    # And says so by having none: the census reads the topic before any OPDI
    # code touches it, so declaring the mapping would mark it stale after an
    # edit that cannot have changed its answer.
    census = next(j for j in R.jobs() if j.name == "census")
    assert set(census.code_paths) == {"benchmarks/adsc_census.py"}


def test_paper_dir_is_overridable():
    # Inside a git worktree the sibling walk lands somewhere that does not
    # exist, every output reads as missing, and the staleness report is a
    # confident lie.
    assert "OPDI_PAPER_DIR" in Path(R.__file__).read_text()


def test_the_paper_dir_is_the_adsc_paper():
    assert R.PAPER.name == "adsc-fusion-v1"
    assert R.DATA.name == "data"


def test_check_needs_no_credentials(monkeypatch, capsys):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.setattr("sys.argv", ["regenerate_adsc_v1.py", "--check", "--allow-stale"])
    R.main()
    assert "analysis at" in capsys.readouterr().out


def test_the_study_days_are_the_v4_period():
    import events_gt

    assert R.DAYS == events_gt.PERIODS["2026"]["days"]


def test_the_census_covers_more_than_the_study_period():
    # "The extra columns are always empty" is a claim about the archive. A
    # census over the three study days alone could not support it.
    assert set(R.DAYS) < set(R.CENSUS_DAYS)
    assert len(R.CENSUS_DAYS) >= 10
    assert min(R.CENSUS_DAYS) < "2024"


def test_a_change_of_coverage_default_marks_pipeline_jobs_stale():
    # `for_environment` flipped to worldwide in 305fc36 and `--check` went on
    # reporting every output current, because nothing here named the code that
    # chose the coverage. The runner is named too: it builds the arms.
    for path in ("src/opdi/config", "src/opdi/coverage.py", "benchmarks/run_adsc_day.py"):
        assert path in R.CORE
