"""Tests for running the pipeline one step and one day at a time."""

import json
from datetime import date

import pytest

from opdi import orchestration as o

D = date(2026, 6, 14)


@pytest.fixture(autouse=True)
def _inputs_are_present(monkeypatch):
    """These tests replace the units; the folder checks are tested in
    test_orchestration_inputs.py against a real warehouse."""
    monkeypatch.setattr(
        o, "_resume_plan", lambda day, first, *a: list(o.DAY_STEPS[o.DAY_STEPS.index(first) :])
    )


def test_day_steps_end_with_publishing():
    assert o.DAY_STEPS == ("01", "02", "02a", "03", "04", "04b", "09")


def test_publishing_needs_no_reference_table():
    assert "09" not in o.STEPS_NEEDING_REFERENCE
    assert set(o.STEPS_NEEDING_REFERENCE) == set(o.DAY_STEPS) - {"09"}


def test_days_are_inclusive_and_newest_first_by_default():
    assert o.days(date(2026, 6, 1), date(2026, 6, 3)) == [
        date(2026, 6, 3),
        date(2026, 6, 2),
        date(2026, 6, 1),
    ]
    assert o.days(date(2026, 6, 1), date(2026, 6, 3), "oldest_first") == [
        date(2026, 6, 1),
        date(2026, 6, 2),
        date(2026, 6, 3),
    ]
    assert o.days(D, D) == [D]


def test_days_refuse_a_reversed_range_and_an_unknown_order():
    with pytest.raises(ValueError, match="before"):
        o.days(date(2026, 6, 3), date(2026, 6, 1))
    with pytest.raises(ValueError, match="order"):
        o.days(D, D, "random")


@pytest.mark.parametrize("step, day", [("05", D), ("99", D), ("02", None), ("00", D)])
def test_run_unit_rejects_an_unknown_step_or_a_wrong_day(step, day):
    with pytest.raises(ValueError):
        o.run_unit(step, day)


def test_the_plan_command_prints_the_days(capsys):
    assert o.main(["plan", "--start", "2025-01-01", "--end", "2025-01-03"]) == 0
    assert json.loads(capsys.readouterr().out) == ["2025-01-03", "2025-01-02", "2025-01-01"]


def test_the_step_command_passes_its_arguments(monkeypatch):
    calls = []
    monkeypatch.setattr(o, "run_unit", lambda *a: calls.append(a) or 0)
    assert (
        o.main(
            ["step", "--step", "02a", "--day", "2026-06-14", "--warehouse", "s3a://x/y", "--europe"]
        )
        == 0
    )
    assert calls == [("02a", D, "s3a://x/y", False, False)]


def test_a_step_that_raises_gives_exit_code_1(monkeypatch):
    def boom(*args):
        raise RuntimeError("storage unreachable")

    monkeypatch.setattr(o, "run_unit", boom)
    assert o.main(["step", "--step", "02", "--day", "2026-06-14"]) == 1
    assert o.main(["reference"]) == 1


def test_a_failing_step_gives_exit_code_1(monkeypatch):
    monkeypatch.setattr(o, "run_unit", lambda *a: 1)
    assert o.main(["step", "--step", "04", "--day", "2026-06-14"]) == 1


def test_a_bad_date_is_refused_by_the_parser():
    with pytest.raises(SystemExit):
        o.main(["step", "--step", "02", "--day", "14/06/2026"])


def test_a_day_runs_its_steps_in_order_and_stops_at_a_failure(monkeypatch):
    ran = []
    monkeypatch.setattr(o, "unfinished_steps", lambda *a, **k: list(o.DAY_STEPS))
    monkeypatch.setattr(o, "check_space", lambda *a, **k: None)
    monkeypatch.setattr(o, "prune_internal", lambda *a, **k: ran.append("prune"))
    monkeypatch.setattr(o, "run_unit", lambda step, *a, **k: ran.append(step) or 0)
    assert o.run_day(D) == 0
    assert ran == list(o.DAY_STEPS) + ["prune"]

    ran.clear()
    monkeypatch.setattr(
        o, "run_unit", lambda step, *a, **k: ran.append(step) or (1 if step == "03" else 0)
    )
    assert o.run_day(D) == 1
    assert ran == ["01", "02", "02a", "03"]


def test_a_day_keeps_internal_tables_when_asked(monkeypatch):
    ran = []
    monkeypatch.setattr(o, "unfinished_steps", lambda *a, **k: list(o.DAY_STEPS))
    monkeypatch.setattr(o, "check_space", lambda *a, **k: None)
    monkeypatch.setattr(o, "prune_internal", lambda *a, **k: ran.append("prune"))
    monkeypatch.setattr(o, "run_unit", lambda *a, **k: 0)
    assert o.run_day(D, keep_internal=True) == 0
    assert ran == []


def test_a_day_does_not_start_when_space_is_short(monkeypatch):
    monkeypatch.setattr(o, "unfinished_steps", lambda *a, **k: list(o.DAY_STEPS))
    monkeypatch.setattr(o, "free_space_gb", lambda *a, **k: 1.0)
    monkeypatch.setattr(o, "run_unit", lambda *a, **k: pytest.fail("must not run"))
    assert o.run_day(D, min_free_gb=5) == 1


def test_a_finished_day_is_skipped_without_checking_space(monkeypatch):
    monkeypatch.setattr(o, "unfinished_steps", lambda *a, **k: [])
    monkeypatch.setattr(o, "free_space_gb", lambda *a, **k: pytest.fail("not needed"))
    monkeypatch.setattr(o, "run_unit", lambda *a, **k: pytest.fail("must not run"))
    assert o.run_day(D) == 0


def test_a_half_finished_day_continues_where_it_stopped(monkeypatch):
    ran = []
    monkeypatch.setattr(o, "unfinished_steps", lambda *a, **k: ["04", "04b", "09"])
    monkeypatch.setattr(o, "check_space", lambda *a, **k: None)
    monkeypatch.setattr(o, "prune_internal", lambda *a, **k: None)
    monkeypatch.setattr(o, "run_unit", lambda step, *a, **k: ran.append(step) or 0)
    assert o.run_day(D) == 0
    assert ran == ["04", "04b", "09"]


class _KeepOpen:
    """The test session, which a unit must not stop."""

    def __init__(self, spark):
        self._spark = spark

    def __getattr__(self, name):
        return getattr(self._spark, name)

    def stop(self):
        pass


def test_unfinished_steps_reads_the_warehouse(spark, tmp_path, monkeypatch):
    from opdi.utils.storage import StorageManager

    monkeypatch.setattr(o, "_session", lambda *a, **k: _KeepOpen(spark))
    warehouse = str(tmp_path / "wh")
    StorageManager(spark, o.make_config(warehouse)).mark_done("01@2026-06-14")
    StorageManager(spark, o.make_config(warehouse)).mark_done("02@2026-06-14")
    assert o.unfinished_steps(D, warehouse) == ["02a", "03", "04", "04b", "09"]


def test_check_space_raises_under_the_floor(monkeypatch):
    monkeypatch.setattr(o, "free_space_gb", lambda *a, **k: 4.9)
    with pytest.raises(RuntimeError, match="4.9 GB"):
        o.check_space(5)
    monkeypatch.setattr(o, "free_space_gb", lambda *a, **k: 5.1)
    o.check_space(5)


def test_free_space_of_a_local_warehouse(tmp_path):
    assert o.free_space_gb(str(tmp_path / "wh")) > 0


def test_prune_plan_keeps_the_recent_days_and_the_published_files():
    present = {
        "osn_tracks": [
            "2026-06-11",
            "2026-06-12",
            "2026-06-13",
            "2026-06-14",
            "2026-06-15",
            "2026-06-16",
            "2026-06-17",
            "2026-06-20",
        ],
        "osn_statevectors_v2": [
            "2026-06-12",
            "2026-06-13",
            "2026-06-15",
            "__HIVE_DEFAULT_PARTITION__",
        ],
        "opdi_flight_events": ["2026-06-01"],
        "published": ["2026-06-01"],
    }
    plan = o.prune_plan(D, 2, present)
    # Kept: 2026-06-13 to 2026-06-16, the days the next day can need in either order.
    assert plan == {
        "osn_tracks": ["2026-06-11", "2026-06-12", "2026-06-17", "2026-06-20"],
        "osn_statevectors_v2": ["2026-06-12"],
    }
    with pytest.raises(ValueError):
        o.prune_plan(D, 0, present)


class _Storage:
    def __init__(self, base_path):
        self.base_path = base_path
        self.dropped = []

    def list_partitions(self, table, column):
        return ["2026-06-01", "2026-06-14"]

    def done_keys(self):
        return []

    def drop_partitions(self, table, column, values):
        self.dropped.append((f"{self.base_path}/{table}", column, list(values)))
        return len(values)


def test_prune_never_targets_a_published_path():
    storage = _Storage("s3a://eurocontrol/opdi/v2.0.0")
    assert o.prune_internal(D, 2, storage=storage) == 3
    assert {p for p, _, _ in storage.dropped} == {
        f"s3a://eurocontrol/opdi/v2.0.0/{t}" for t in o.INTERNAL_TABLES
    }
    assert not [p for p, _, _ in storage.dropped if "/published" in p]

    inside = _Storage("s3a://eurocontrol/opdi/v2.0.0/published")
    with pytest.raises(RuntimeError, match="published"):
        o.prune_internal(D, 2, storage=inside)
    assert inside.dropped == []


def test_the_module_imports_without_spark():
    import subprocess
    import sys
    from pathlib import Path

    src = Path(__file__).resolve().parent.parent / "src"
    code = (
        f"import sys; sys.path.insert(0, {str(src)!r}); sys.modules['pyspark'] = None\n"
        "from opdi import orchestration\n"
        "assert orchestration.main(['plan', '--start', '2025-01-01', '--end', "
        "'2025-01-02']) == 0\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr


# --- removing the working events and measurements of published days -----------


def test_working_prune_plan_removes_only_published_days_outside_the_window():
    present = {
        "opdi_flight_events": ["2026-06-01", "2026-06-02", "2026-06-10", "2026-06-14"],
        "opdi_measurements": ["2026-06-01", "2026-06-03", "__HIVE_DEFAULT_PARTITION__"],
        "opdi_flight_list": ["2026-06-01"],
        "osn_tracks": ["2026-06-01"],
    }
    published = {date(2026, 6, 1), date(2026, 6, 10), date(2026, 6, 14)}
    plan = o.working_prune_plan(D, 7, present, published)
    # 2026-06-02 and 2026-06-03 are not published; 2026-06-10 and 2026-06-14
    # are inside the window of 7 days. The flight list is never in the plan.
    assert plan == {"opdi_flight_events": ["2026-06-01"], "opdi_measurements": ["2026-06-01"]}
    with pytest.raises(ValueError):
        o.working_prune_plan(D, -1, present, published)


def test_the_working_flight_list_is_never_pruned():
    assert "opdi_flight_list" not in o.WORKING_TABLES
    assert "opdi_flight_list" not in o.INTERNAL_TABLES


class _WorkingStorage(_Storage):
    def list_partitions(self, table, column):
        return ["2026-05-01", "2026-05-02", "2026-06-14"]


def test_prune_keeps_the_working_tables_by_default(monkeypatch):
    monkeypatch.setattr(o, "published_days", lambda storage: pytest.fail("not needed"))
    storage = _WorkingStorage("s3a://eurocontrol/opdi/v2.0.0")
    o.prune_internal(D, 2, storage=storage)
    assert not [p for p, _, _ in storage.dropped if p.endswith(tuple(o.WORKING_TABLES))]


def test_prune_drops_working_days_only_when_published(monkeypatch):
    monkeypatch.setattr(o, "published_days", lambda storage: {date(2026, 5, 1)})
    storage = _WorkingStorage("s3a://eurocontrol/opdi/v2.0.0")
    o.prune_internal(D, 2, storage=storage, drop_published_working=True, working_keep_days=7)
    working = {(p.rsplit("/", 1)[1], tuple(v)) for p, _, v in storage.dropped}
    assert ("opdi_flight_events", ("2026-05-01",)) in working
    assert ("opdi_measurements", ("2026-05-01",)) in working
    assert not [v for p, _, v in storage.dropped if "2026-05-02" in v and "opdi_" in p]


def test_published_days_needs_both_files_and_a_catalogue_row(spark, tmp_path):
    from opdi.output.published import PublishedLayout

    class S:
        base_path = str(tmp_path)

    S.spark = spark
    layout = PublishedLayout.for_storage(S)
    assert o.published_days(S) == set()
    spark.createDataFrame([(date(2026, 6, 1),), (date(2026, 6, 2),)], "day date").coalesce(
        1
    ).write.parquet(layout.catalogue)
    for d in (date(2026, 6, 1), date(2026, 6, 2), date(2026, 6, 3)):
        spark.createDataFrame([(1,)], "x int").write.parquet(layout.flight_list(d))
    spark.createDataFrame([(1,)], "x int").write.parquet(layout.flight_events(date(2026, 6, 1)))
    # 06-02 has no flight events file; 06-03 has no catalogue row.
    assert o.published_days(S) == {date(2026, 6, 1)}


def test_the_prune_command_takes_the_opt_in(monkeypatch):
    seen = {}

    def fake(day, keep_days, warehouse, worldwide, **kw):
        seen.update(kw)
        return 0

    monkeypatch.setattr(o, "prune_internal", fake)
    assert o.main(["prune", "--day", "2026-06-14"]) == 0
    assert seen.get("drop_published_working") is False
    assert o.main(["prune", "--day", "2026-06-14", "--drop-published-working"]) == 0
    assert seen["drop_published_working"] is True
    assert seen["working_keep_days"] == o.DEFAULT_WORKING_KEEP_DAYS
