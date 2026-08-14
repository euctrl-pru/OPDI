"""The benchmark's write guard, and the ladder's plan assertions.

Both are cheap to test and expensive to get wrong: an unguarded run appends a
duplicate month to the *published* event table, and a ladder whose first rung
is not the published algorithm measures a baseline nobody asked about -- at
roughly two hours per rung to discover.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks"))

import pytest

from event_bench import LADDER, build_plan, guard_writes, redirect_event_tables, verify_plan
from opdi.config import EventConfig


def test_the_first_rung_is_exactly_the_published_algorithm():
    plan = build_plan()

    assert plan["L00_legacy"] == EventConfig.legacy()


def test_the_last_rung_is_exactly_what_ships():
    """Otherwise the ladder's verdict is about a configuration nobody runs."""
    plan = build_plan()

    assert plan["L10_shipped"] == EventConfig()


def test_no_two_adjacent_rungs_are_identical():
    """A no-op rung costs a full run and reports a difference of zero, which
    reads as 'this change is worthless' rather than 'this rung did nothing'."""
    verify_plan(build_plan())


def test_every_rung_name_is_unique():
    names = [n for n, _ in LADDER]

    assert len(names) == len(set(names))


@pytest.fixture
def pristine_storage():
    """Snapshot and restore ``StorageManager``'s patchable surface.

    ``guard_writes`` patches the *class*, so without this the guard leaks into
    every test that runs afterwards in the same session -- which is exactly
    what happened: two unrelated storage tests started failing because they
    were calling the benchmark's wrapper rather than the real method. A
    module-level monkeypatch needs a teardown even when the function under test
    is idempotent.
    """
    from opdi.utils.storage import StorageManager

    saved = {
        name: getattr(StorageManager, name, None)
        for name in (
            "write_table", "_s3_path",
            "_events_guarded", "_events_orig_write", "_events_orig_path",
        )
    }
    yield StorageManager
    for name, value in saved.items():
        if value is None:
            if hasattr(StorageManager, name):
                delattr(StorageManager, name)
        else:
            setattr(StorageManager, name, value)


def test_the_guard_refuses_a_write_outside_research(spark, pristine_storage):
    from opdi.config import OPDIConfig

    storage = pristine_storage(spark, OPDIConfig.for_environment("local"))
    guard_writes()
    df = spark.createDataFrame([(1,)], "x int")

    with pytest.raises(RuntimeError, match="writes only under"):
        storage.write_table(df, "opdi_flight_events", mode="append")


def test_a_redirected_write_is_allowed_through(spark, pristine_storage):
    """The other half: the redirect is what makes the write legitimate, and the
    guard has to recognise it by path rather than by name."""
    from opdi.config import OPDIConfig

    storage = pristine_storage(spark, OPDIConfig.for_environment("local"))
    guard_writes()
    redirect_event_tables("research/events_test")

    landing = storage._s3_path("opdi_flight_events")

    assert "/research/events_test" in landing
