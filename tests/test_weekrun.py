"""The week runner's decisions, tested without a cluster.

Everything here is a pure function or a JSON file. The parts that need Spark
are the step bodies themselves, which the pipeline's own tests cover; what this
file pins is the reasoning *around* them -- how big a heap may be, which steps
still need running, and whether a state file describes the window being asked
for. Those are the decisions that cost a week of cluster time when wrong.
"""
import json

import pytest

from opdi.runner import STEPS
from opdi.weekrun import (
    MIN_DRIVER_HEAP_GB,
    REQUIRED_REFERENCE_TABLES,
    UNDATED_STEPS,
    WEEK_STEPS,
    RunState,
    plan_steps,
    preflight,
    safe_driver_memory,
    week_window,
)

from datetime import date

GIB = 1024 ** 3


# --- driver heap ------------------------------------------------------------

def test_an_unconstrained_container_gets_what_it_asked_for():
    """No cgroup limit means no reason to interfere."""
    assert safe_driver_memory("10G", limit_bytes=None) == "10G"


def test_a_heap_that_already_fits_is_left_alone():
    """32 GiB free, 55% of which is 17 GiB: a 10 GiB ask is not the problem."""
    assert safe_driver_memory("10G", limit_bytes=32 * GIB, used_bytes=0) == "10G"


def test_the_opensky_preset_is_cut_down_to_this_pod():
    """The real numbers: a 16 GiB cap with 10.4 GiB already resident.

    5.6 GiB free, 55% of it is 3 GiB. The preset asks for 10G and gets 3g --
    which is the whole point, because a 10 GiB heap in 5.6 GiB of headroom is
    the SIGKILL that ended a 15-hour events run with no crash dump.
    """
    got = safe_driver_memory("10G", limit_bytes=16 * GIB, used_bytes=int(10.4 * GIB))
    assert got == "3g"


def test_the_result_is_never_larger_than_the_request():
    """This exists to prevent a kill, not to hand out memory nobody asked for."""
    got = safe_driver_memory("4g", limit_bytes=64 * GIB, used_bytes=0)
    assert got == "4g"


def test_too_little_headroom_refuses_instead_of_shrinking():
    """A 1 GiB heap would fail slowly. Failing at launch is a message."""
    with pytest.raises(ValueError, match="below the"):
        safe_driver_memory("10G", limit_bytes=16 * GIB, used_bytes=int(15.5 * GIB))


def test_the_floor_is_what_the_error_reports():
    with pytest.raises(ValueError, match=f"{MIN_DRIVER_HEAP_GB} GiB floor"):
        safe_driver_memory("8g", limit_bytes=4 * GIB, used_bytes=int(3.8 * GIB))


@pytest.mark.parametrize("bad", ["10", "lots", "10kb", ""])
def test_an_unparseable_size_is_rejected(bad):
    with pytest.raises(ValueError, match="cannot parse memory size"):
        safe_driver_memory(bad, limit_bytes=None)


# --- the window -------------------------------------------------------------

def test_a_seven_day_window_ends_six_days_later():
    """The steps treat the range as inclusive, so seven days is +6, not +7.

    Off by one here is a whole extra day of ingestion that nobody asked for and
    that nothing in the output would show.
    """
    assert week_window(date(2026, 6, 1), 7) == (date(2026, 6, 1), date(2026, 6, 7))


def test_a_single_day_window_starts_and_ends_together():
    assert week_window(date(2026, 6, 1), 1) == (date(2026, 6, 1), date(2026, 6, 1))


def test_a_zero_day_window_is_rejected():
    with pytest.raises(ValueError, match="days must be >= 1"):
        week_window(date(2026, 6, 1), 0)


# --- planning ---------------------------------------------------------------

def test_every_step_this_module_names_exists_in_the_runner(tmp_path):
    """WEEK_STEPS is a hand-written list; the registry is the authority.

    A typo here would be reported as 'unknown step' only at launch, after the
    operator had already queued the run.
    """
    assert set(WEEK_STEPS) <= set(STEPS)


def test_the_undated_step_split_matches_the_runner(tmp_path):
    """`runner.run_pipeline` special-cases exactly these three.

    The split is duplicated rather than imported because runner keeps it inline
    in two branches. If runner ever changes it, this fails rather than the week
    run passing a date range to a step that takes none.
    """
    assert UNDATED_STEPS == frozenset({"00", "07", "08"})


def test_completed_steps_are_skipped(tmp_path):
    state = RunState(path=tmp_path / "s.json")
    state.completed = {"01": {}, "02": {}}
    assert plan_steps(["01", "02", "02a", "03"], state) == ["02a", "03"]


def test_force_reruns_everything(tmp_path):
    state = RunState(path=tmp_path / "s.json")
    state.completed = {"01": {}, "02": {}}
    assert plan_steps(["01", "02", "03"], state, force=True) == ["01", "02", "03"]


def test_the_planned_order_is_the_requested_order_not_sorted(tmp_path):
    """`02a` must follow `02`. Sorting strings happens to put it there today;
    a step named `02b` or `10` would break that and nothing would notice."""
    state = RunState(path=tmp_path / "s.json")
    assert plan_steps(["01", "02", "02a", "03", "04"], state) == [
        "01", "02", "02a", "03", "04"
    ]


# --- state ------------------------------------------------------------------

def test_state_round_trips(tmp_path):
    p = tmp_path / "state.json"
    s = RunState(path=p, run_id="R", env="opensky",
                 start_date="2026-06-01", end_date="2026-06-07")
    s.mark("01", 12.34)
    again = RunState.load(p)
    assert again.completed["01"]["seconds"] == 12.3
    assert again.env == "opensky"
    assert again.start_date == "2026-06-01"


def test_marking_writes_immediately(tmp_path):
    """Batching completions to the end would lose them to the crash they
    exist to survive."""
    p = tmp_path / "state.json"
    s = RunState(path=p, env="opensky",
                 start_date="2026-06-01", end_date="2026-06-07")
    s.mark("01", 1.0)
    assert "01" in json.loads(p.read_text())["completed"]


def test_a_missing_state_file_is_an_empty_state(tmp_path):
    s = RunState.load(tmp_path / "nope.json")
    assert s.completed == {}


def test_a_state_file_for_another_window_is_not_reused(tmp_path):
    """The step names are identical across windows, so nothing else would
    catch this -- the run would silently skip work for different dates."""
    s = RunState(path=tmp_path / "s.json", env="opensky",
                 start_date="2026-06-01", end_date="2026-06-07")
    s.completed = {"01": {}}
    assert s.matches("opensky", date(2026, 6, 1), date(2026, 6, 7))
    assert not s.matches("opensky", date(2026, 7, 1), date(2026, 7, 7))
    assert not s.matches("live", date(2026, 6, 1), date(2026, 6, 7))


def test_an_empty_state_matches_any_window(tmp_path):
    """Nothing has been done, so there is nothing to reuse wrongly."""
    s = RunState(path=tmp_path / "s.json")
    assert s.matches("opensky", date(2026, 6, 1), date(2026, 6, 7))


# --- preflight --------------------------------------------------------------

class _Storage:
    """Minimal stand-in: `read_table` raises for names not in `present`."""

    def __init__(self, present):
        self.present = set(present)

    def read_table(self, name):
        if name not in self.present:
            raise FileNotFoundError(name)

        class _DF:
            def limit(self, n):
                return self

            def count(self):
                return 1

        return _DF()


def test_preflight_reports_every_missing_table_at_once():
    """One-at-a-time discovery means one cluster round trip per missing table."""
    storage = _Storage(set(REQUIRED_REFERENCE_TABLES) - {"oa_runways", "h3_runway_zones"})
    assert sorted(preflight(storage)) == ["h3_runway_zones", "oa_runways"]


def test_preflight_passes_when_everything_is_present():
    assert preflight(_Storage(REQUIRED_REFERENCE_TABLES)) == []


def test_the_layout_table_is_preflighted():
    """AOBT/AIBT come from stand polygons in this table and nothing else.

    Without it step 04 does not fail -- it reports zero block events, which
    reads as a detector problem rather than a missing input.
    """
    assert "hexaero_airport_layouts" in REQUIRED_REFERENCE_TABLES


# --- fresh-build contract ---------------------------------------------------

def test_a_week_run_builds_its_own_reference_tables():
    """Step 00 is part of the run, not a prerequisite of it.

    The contract is a build that reuses nothing: every table a run reads is one
    that run produced. Dropping 00 from this tuple would silently reintroduce
    the published warehouse's reference tables as inputs.
    """
    from opdi.weekrun import WEEK_STEPS as W
    assert W[0] == "00"


def test_the_default_warehouse_is_not_the_published_one():
    """Writing reference tables into `opdi/` would overwrite published data.

    The whole point of a separate prefix is that a from-scratch rebuild is safe
    to attempt; pointing the default at the published prefix would make it the
    opposite.
    """
    from opdi.weekrun import DEFAULT_WAREHOUSE
    assert DEFAULT_WAREHOUSE.rstrip("/") != "s3a://eurocontrol/opdi"
    assert DEFAULT_WAREHOUSE.startswith("s3a://")


def test_the_reference_substeps_run_in_dependency_order_not_id_order():
    """00d creates `oa_airports`; 00a, 00b and 00f all read it.

    Iterating the registry in id order put 00a and 00b first, which on an empty
    warehouse made them fall back to downloading the public OurAirports CSV --
    unreachable from the OSN cluster, and a different snapshot from the table
    the rest of the pipeline reads. Invisible on an established warehouse,
    which is why it survived; fatal for a from-scratch build.
    """
    from opdi.runner import REFERENCE_SUBSTEPS

    order = list(REFERENCE_SUBSTEPS)
    assert order.index("00d") < order.index("00a")
    assert order.index("00d") < order.index("00b")
    assert order.index("00d") < order.index("00f")


def test_the_substep_ids_are_unchanged_by_the_reorder():
    """The ids are operator-facing (--no-airport-zones, run-log labels).
    Only the execution order moved."""
    from opdi.runner import REFERENCE_SUBSTEPS
    assert set(REFERENCE_SUBSTEPS) == {"00a", "00b", "00c", "00d", "00e", "00f"}


# --- the OSM extract --------------------------------------------------------

class _H3Cfg:
    def __init__(self, path=None):
        self.airport_layout_pbf_path = path


class _Cfg:
    def __init__(self, path=None):
        self.h3 = _H3Cfg(path)


def test_the_environment_overrides_the_configured_extract(monkeypatch):
    """Matches runner._step_00b_airport_layouts' own precedence. If these two
    disagreed, the check would pass on a file the step does not read."""
    from opdi.weekrun import resolve_pbf_path

    monkeypatch.setenv("OPDI_OSM_PBF", "/from/env.pbf")
    assert resolve_pbf_path(_Cfg("/from/config.pbf")) == "/from/env.pbf"


def test_the_configured_extract_is_used_when_the_environment_is_silent(monkeypatch):
    from opdi.weekrun import resolve_pbf_path

    monkeypatch.delenv("OPDI_OSM_PBF", raising=False)
    assert resolve_pbf_path(_Cfg("/from/config.pbf")) == "/from/config.pbf"


def test_no_extract_at_all_is_a_preflight_problem(monkeypatch):
    """Falling through to Overpass is worse than failing: it returns empty
    results without erroring, so the run completes and reports no stands."""
    from opdi.weekrun import preflight_fresh_build

    monkeypatch.delenv("OPDI_OSM_PBF", raising=False)
    problems = preflight_fresh_build(_Cfg(None))
    assert len(problems) == 1
    assert "Overpass" in problems[0]


def test_an_extract_path_that_does_not_exist_is_caught(monkeypatch, tmp_path):
    from opdi.weekrun import preflight_fresh_build

    monkeypatch.delenv("OPDI_OSM_PBF", raising=False)
    problems = preflight_fresh_build(_Cfg(str(tmp_path / "absent.pbf")))
    assert len(problems) == 1
    assert "does not exist" in problems[0]


def test_a_real_extract_passes(monkeypatch, tmp_path):
    from opdi.weekrun import preflight_fresh_build

    monkeypatch.delenv("OPDI_OSM_PBF", raising=False)
    pbf = tmp_path / "aeroway.osm.pbf"
    pbf.write_bytes(b"not really a pbf, but it exists")
    assert preflight_fresh_build(_Cfg(str(pbf))) == []


# --- the day loop -----------------------------------------------------------

def test_the_window_and_day_steps_together_are_the_week_steps():
    """No step may fall between the two lists, or it silently never runs."""
    from opdi.weekrun import DAY_STEPS, WEEK_STEPS as W, WINDOW_STEPS

    assert set(WINDOW_STEPS) | set(DAY_STEPS) == set(W)
    assert set(WINDOW_STEPS).isdisjoint(DAY_STEPS)


def test_segmentation_onwards_runs_per_day_and_ingestion_does_not():
    """Ingestion is by calendar date and has no track semantics; everything
    after it works on tracks, which belong to a day."""
    from opdi.weekrun import DAY_STEPS, WINDOW_STEPS

    assert WINDOW_STEPS == ("00", "01")
    assert DAY_STEPS == ("02", "02a", "03", "04")


def test_days_in_is_inclusive_of_both_ends():
    from opdi.weekrun import days_in

    got = days_in(date(2026, 6, 1), date(2026, 6, 7))
    assert len(got) == 7
    assert got[0] == date(2026, 6, 1)
    assert got[-1] == date(2026, 6, 7)


def test_a_day_step_records_completion_per_day():
    """A bare step name would mark all seven days done after the first, and the
    rest of the week would be skipped in silence on resume."""
    from opdi.weekrun import day_state_key

    a = day_state_key("02", date(2026, 6, 1))
    b = day_state_key("02", date(2026, 6, 2))
    assert a != b
    assert a == "02@2026-06-01"


def test_a_week_of_four_day_steps_is_thirty_units():
    """Two window steps plus four per day across seven days. Stated as a number
    so a change to either list is visible rather than inferred."""
    from opdi.weekrun import DAY_STEPS, WINDOW_STEPS, days_in

    days = days_in(date(2026, 6, 1), date(2026, 6, 7))
    assert len(WINDOW_STEPS) + len(DAY_STEPS) * len(days) == 30


def test_ingestion_reaches_past_the_window():
    """The last day is segmented over data running into the day after it, so
    ingestion must already have that tail or every late departure on the final
    day is truncated."""
    from opdi.weekrun import INGEST_LOOKAHEAD_DAYS

    assert INGEST_LOOKAHEAD_DAYS >= 1


def test_a_non_day_step_is_rejected_by_the_day_dispatcher():
    """Calling a window step per day would re-ingest the week seven times."""
    from opdi.weekrun import run_day_step

    with pytest.raises(ValueError, match="not a per-day step"):
        run_day_step(None, None, "01", date(2026, 6, 1), {})
