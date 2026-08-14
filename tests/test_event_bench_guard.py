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


def test_the_guard_refuses_a_write_outside_research(spark, tmp_path):
    from opdi.config import OPDIConfig
    from opdi.utils.storage import StorageManager

    # Fresh class state: the guard is idempotent by design, so reset it.
    for attr in ("_events_guarded", "_events_orig_write", "_events_orig_path"):
        if hasattr(StorageManager, attr):
            delattr(StorageManager, attr)
    StorageManager.write_table = StorageManager.__dict__.get(
        "write_table", StorageManager.write_table
    )

    config = OPDIConfig.for_environment("local")
    storage = StorageManager(spark, config)
    guard_writes()
    df = spark.createDataFrame([(1,)], "x int")

    with pytest.raises(RuntimeError, match="writes only under"):
        storage.write_table(df, "opdi_flight_events", mode="append")
