"""A day never runs a step whose input folders are gone.

These tests run :func:`opdi.orchestration.run_unit`, :func:`run_day` and
:func:`prune_internal` against a local warehouse: real progress marks, real
folder listings, a real coverage marker. Only the bodies of the pipeline
steps are replaced, by a stand-in that writes the day folders each step
writes.
"""

from datetime import date, timedelta
from pathlib import Path

import pytest

from opdi import orchestration as o
from opdi import periodrun

D = date(2026, 6, 14)
ONE = timedelta(days=1)


class _KeepOpen:
    """The test session, which a unit must not stop."""

    def __init__(self, spark):
        self._spark = spark

    def __getattr__(self, name):
        return getattr(self._spark, name)

    def stop(self):
        pass


def _root(warehouse):
    return Path(o.make_config(warehouse).project.release_root)


def _folder(warehouse, table, column, value):
    path = _root(warehouse) / table / f"{column}={value}"
    path.mkdir(parents=True, exist_ok=True)
    (path / "part-0.parquet").write_bytes(b"")
    return path


#: The day folders each step writes for day D (table, column, days from D).
OUTPUTS = {
    "01": [("osn_statevectors_v2", "event_time_day", k) for k in (-1, 0, 1)],
    "02": [("osn_tracks", "dof", 0)],
    "02a": [("osn_tracks_clean", "dof", 0)],
    "03": [("opdi_flight_list", "DOF", 0)],
    "04": [("opdi_flight_events", "dof", 0), ("opdi_measurements", "dof", 0)],
    "04b": [],
    "09": [],
}


@pytest.fixture
def warehouse(spark, tmp_path, monkeypatch):
    """A local warehouse; the steps record what they ran in ``ran``."""
    ran = []

    def fake_step(spark_, config, step, day, kwargs):
        ran.append((step, day))
        wh = config.project.warehouse_path
        if step == "01":
            # The real step 01 fetches only the days that are not there.
            for table, column, k in OUTPUTS["01"]:
                _folder(wh, table, column, (day + k * ONE).isoformat())
            return
        for table, column, k in OUTPUTS[step]:
            _folder(wh, table, column, (day + k * ONE).isoformat())

    monkeypatch.setattr(o, "_session", lambda *a, **k: _KeepOpen(spark))
    monkeypatch.setattr(periodrun, "run_day_step", fake_step)
    monkeypatch.setattr(periodrun, "preflight", lambda storage: [])
    wh = str(tmp_path / "wh")
    return wh, ran


def _storage(spark, warehouse):
    from opdi.utils.storage import StorageManager

    return StorageManager(spark, o.make_config(warehouse))


def _mark(spark, warehouse, *steps, day=D):
    storage = _storage(spark, warehouse)
    for step in steps:
        storage.mark_done(o.unit_key(step, day))


def test_step_inputs_follow_the_code_of_each_step():
    config = o.make_config("/tmp/unused")
    sv = [("osn_statevectors_v2", "event_time_day", (D + k * ONE).isoformat()) for k in (-1, 0, 1)]
    assert o.step_inputs("01", D, config) == []
    assert o.step_inputs("02", D, config) == sv
    assert o.step_inputs("02a", D, config) == [("osn_tracks", "dof", "2026-06-14")]
    assert o.step_inputs("03", D, config) == [("osn_tracks_clean", "dof", "2026-06-14")]
    assert o.step_inputs("04", D, config) == [("osn_tracks_clean", "dof", "2026-06-14")]
    assert o.step_inputs("04b", D, config) == [
        ("opdi_flight_list", "DOF", "2026-06-14"),
        ("opdi_flight_events", "dof", "2026-06-14"),
    ]
    assert o.step_inputs("09", D, config) == [
        ("opdi_flight_list", "DOF", "2026-06-14"),
        ("opdi_flight_events", "dof", "2026-06-14"),
        ("opdi_measurements", "dof", "2026-06-14"),
    ]
    config.cleaning.enabled = False
    assert o.step_inputs("02a", D, config) == []
    assert o.step_inputs("03", D, config) == [("osn_tracks", "dof", "2026-06-14")]
    assert o.step_inputs("04", D, config) == [("osn_tracks", "dof", "2026-06-14")]


def test_a_step_whose_input_is_gone_fails_and_names_the_command(spark, warehouse, capsys):
    wh, ran = warehouse
    _mark(spark, wh, "01", "02", "02a", "03")
    assert o.run_unit("04", D, warehouse=wh) == 1
    out = capsys.readouterr().out
    assert "osn_tracks_clean dof=2026-06-14" in out
    assert "python -m opdi.orchestration day --day 2026-06-14 --force" in out
    assert ran == []
    assert not _storage(spark, wh).is_done(o.unit_key("04", D))


def test_a_step_whose_inputs_are_there_runs_and_is_marked(spark, warehouse):
    wh, ran = warehouse
    _folder(wh, "osn_tracks_clean", "dof", D.isoformat())
    assert o.run_unit("04", D, warehouse=wh) == 0
    assert ran == [("04", D)]
    assert _storage(spark, wh).is_done(o.unit_key("04", D))
    # Marked: the same command skips it.
    assert o.run_unit("04", D, warehouse=wh) == 0
    assert ran == [("04", D)]


def test_step_01_is_never_skipped_on_its_mark_alone(spark, warehouse):
    wh, ran = warehouse
    _mark(spark, wh, "01")
    assert o.run_unit("01", D, warehouse=wh) == 0
    assert ran == [("01", D)]


def test_a_step_is_refused_in_a_warehouse_of_the_other_coverage(spark, warehouse, capsys):
    from opdi.utils.storage import StorageManager

    wh, ran = warehouse
    StorageManager(spark, o.make_config(wh, worldwide=False)).write_coverage_marker()
    _folder(wh, "osn_tracks_clean", "dof", D.isoformat())
    assert o.run_unit("04", D, warehouse=wh) == 1
    assert "coverage" in capsys.readouterr().out
    assert ran == []


def test_a_day_restarts_from_step_01_when_its_tracks_are_gone(spark, warehouse):
    """Day D failed at step 04; its reports and tracks were removed since."""
    wh, ran = warehouse
    _mark(spark, wh, "01", "02", "02a", "03")
    _folder(wh, "opdi_flight_list", "DOF", D.isoformat())
    assert o.run_day(D, warehouse=wh, min_free_gb=0, keep_internal=True) == 0
    assert [s for s, _ in ran] == list(o.DAY_STEPS)
    assert o.unfinished_steps(D, wh) == []


def test_a_day_continues_where_it_stopped_when_its_inputs_are_there(spark, warehouse):
    wh, ran = warehouse
    _mark(spark, wh, "01", "02", "02a", "03")
    _folder(wh, "osn_tracks_clean", "dof", D.isoformat())
    _folder(wh, "opdi_flight_list", "DOF", D.isoformat())
    assert o.run_day(D, warehouse=wh, min_free_gb=0, keep_internal=True) == 0
    assert [s for s, _ in ran] == ["04", "04b", "09"]


def test_a_day_restarts_from_the_step_that_wrote_the_missing_folder(spark, warehouse):
    """Reports there, cleaned tracks gone: steps 02a onwards run again."""
    wh, ran = warehouse
    _mark(spark, wh, "01", "02", "02a", "03")
    for k in (-1, 0, 1):
        _folder(wh, "osn_statevectors_v2", "event_time_day", (D + k * ONE).isoformat())
    _folder(wh, "osn_tracks", "dof", D.isoformat())
    _folder(wh, "opdi_flight_list", "DOF", D.isoformat())
    assert o.run_day(D, warehouse=wh, min_free_gb=0, keep_internal=True) == 0
    # Step 01 runs again (its listing finds the three days); step 02 keeps
    # its mark because its output is there.
    assert [s for s, _ in ran] == ["01", "02a", "03", "04", "04b", "09"]


def test_a_published_day_is_skipped_even_when_its_inputs_are_gone(spark, warehouse):
    wh, ran = warehouse
    _mark(spark, wh, *o.DAY_STEPS)
    assert o.run_day(D, warehouse=wh, min_free_gb=0, keep_internal=True) == 0
    assert ran == []


def test_days_in_progress_are_the_started_days_not_yet_published(spark, warehouse):
    wh, _ = warehouse
    _mark(spark, wh, "01", "02", day=D)
    _mark(spark, wh, *o.DAY_STEPS, day=D - ONE)
    _mark(spark, wh, "01", day=D + 3 * ONE)
    assert o.days_in_progress(_storage(spark, wh)) == {D, D + 3 * ONE}


def test_prune_plan_keeps_what_an_unfinished_day_still_needs():
    present = {
        "osn_statevectors_v2": [(D + k * ONE).isoformat() for k in range(-6, 7)],
        "osn_tracks": [(D + k * ONE).isoformat() for k in range(-6, 7)],
    }
    later = D + 5 * ONE
    plan = o.prune_plan(D, 2, present, in_progress={later})
    kept_sv = set(present["osn_statevectors_v2"]) - set(plan["osn_statevectors_v2"])
    kept_tracks = set(present["osn_tracks"]) - set(plan["osn_tracks"])
    window = {(D + k * ONE).isoformat() for k in range(-1, 3)}
    assert kept_sv == window | {(later + k * ONE).isoformat() for k in (-1, 0, 1)}
    assert kept_tracks == window | {later.isoformat()}


def test_prune_internal_on_real_folders_keeps_an_unfinished_day(spark, warehouse):
    """Day D+5 failed at step 02; publishing D must not remove what it needs."""
    wh, _ = warehouse
    for k in range(-6, 7):
        _folder(wh, "osn_statevectors_v2", "event_time_day", (D + k * ONE).isoformat())
        _folder(wh, "osn_tracks", "dof", (D + k * ONE).isoformat())
        _folder(wh, "opdi_flight_events", "dof", (D + k * ONE).isoformat())
    _mark(spark, wh, "01", day=D + 5 * ONE)
    o.prune_internal(D, 2, warehouse=wh)
    root = _root(wh)
    sv = sorted(p.name.split("=")[1] for p in (root / "osn_statevectors_v2").iterdir())
    tracks = sorted(p.name.split("=")[1] for p in (root / "osn_tracks").iterdir())
    assert sv == [(D + k * ONE).isoformat() for k in (-1, 0, 1, 2, 4, 5, 6)]
    assert tracks == [(D + k * ONE).isoformat() for k in (-1, 0, 1, 2, 5)]
    # The working events are kept by default.
    assert len(list((root / "opdi_flight_events").iterdir())) == 13
    # Nothing under published/ or _state/ was touched.
    assert (root / "_state" / "done").is_dir()


def test_step_01_keeps_the_reports_an_unfinished_day_still_needs(spark, tmp_path, monkeypatch):
    """Backfill newest first: day D failed at step 02, day D-1 starts.

    Step 01 of D-1 keeps the reports of D+1, which D still needs.
    """
    from opdi.ingestion import osn_statevectors

    wh = str(tmp_path / "wh")
    config = o.make_config(wh)
    for k in (-1, 0, 1):
        _folder(wh, "osn_statevectors_v2", "event_time_day", (D + k * ONE).isoformat())
    _mark(spark, wh, "01", day=D)

    fetched = []

    class FakeIngestion:
        def __init__(self, *a, **k):
            pass

        def ingest_from_s3(self, start_date, end_date):
            fetched.append(start_date)
            _folder(wh, "osn_statevectors_v2", "event_time_day", start_date.isoformat())

    monkeypatch.setattr(osn_statevectors, "StateVectorIngestion", FakeIngestion)
    periodrun.run_day_step(spark, config, "01", D - ONE, {"log_dir": str(tmp_path / "logs")})
    assert fetched == [D - 2 * ONE]
    days = sorted(p.name.split("=")[1] for p in (_root(wh) / "osn_statevectors_v2").iterdir())
    assert days == [(D + k * ONE).isoformat() for k in (-2, -1, 0, 1)]


def test_the_reference_step_checks_its_tables_even_when_marked(spark, warehouse, monkeypatch):
    """A reference table lost after step 00 finished is built again, alone."""
    from opdi import runner

    wh, _ = warehouse
    _storage(spark, wh).mark_done("00")
    built = []
    monkeypatch.setattr(
        periodrun,
        "reference_substeps_to_run",
        lambda storage, force=False: {k: k == "00d" for k in periodrun.REFERENCE_OUTPUTS},
    )
    monkeypatch.setitem(
        runner.STEPS, "00", lambda spark_, config, **kw: built.append(kw["run_reference"])
    )
    assert o.run_unit("00", None, warehouse=wh) == 0
    assert len(built) == 1
    assert [k for k, v in built[0].items() if v] == ["00d"]

    # Nothing missing: nothing built.
    monkeypatch.setattr(
        periodrun,
        "reference_substeps_to_run",
        lambda storage, force=False: {k: False for k in periodrun.REFERENCE_OUTPUTS},
    )
    assert o.run_unit("00", None, warehouse=wh) == 0
    assert len(built) == 1
