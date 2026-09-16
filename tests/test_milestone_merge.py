"""One variable per purpose.

``events_v0.2.0`` published two events for one question: ``ATOT`` beside
``airborne``, ``ALDT`` beside ``touchdown``. Which to trust depended on the
aerodrome, so every consumer had to encode that judgement itself. The merge
collapses each pair at source, preferring the A-CDM arm and falling back to the
legacy one, and records the arm in ``info.method`` so the mixture stays
recoverable.
"""
import datetime as dt
import json

import pytest

from opdi.config import EventConfig
from opdi.pipeline.events import merge_milestone_duplicates

T0 = dt.datetime(2026, 6, 1, 8, 0, 0)


def _events(spark, rows):
    """``(track_id, type, seconds_after_T0, method)`` onto the event frame."""
    return spark.createDataFrame(
        [
            (t, ty, T0 + dt.timedelta(seconds=s),
             json.dumps({"method": m}) if m is not None else json.dumps({}))
            for t, ty, s, m in rows
        ],
        "track_id string, type string, event_time timestamp, info string",
    )


def _by_type(out):
    return {(r["track_id"], r["type"]): r for r in out.collect()}


def test_the_acdm_arm_wins_where_both_fired(spark):
    """A-CDM is the interpolated crossing of 15 ft above field elevation;
    legacy is the extreme *sample* of a detection window, which carries a
    measured +19 s median bias on departures."""
    df = _events(spark, [
        ("trk-1", "ATOT", 19, "legacy"),
        ("trk-1", "ATOT", 0, "acdm"),
    ])

    out = merge_milestone_duplicates(df, EventConfig())

    assert out.count() == 1
    row = out.collect()[0]
    assert json.loads(row["info"])["method"] == "acdm"
    assert row["event_time"] == T0


def test_a_legacy_only_flight_keeps_its_milestone(spark):
    """The merge is a coalesce, so coverage can only rise. A-CDM reaches
    roughly 4-7% of the network against legacy's ~90%; if the merge dropped the
    legacy rows it would delete most of the table."""
    df = _events(spark, [("trk-2", "ATOT", 19, "legacy")])

    out = merge_milestone_duplicates(df, EventConfig())

    assert out.count() == 1
    assert json.loads(out.collect()[0]["info"])["method"] == "legacy"


def test_each_flight_is_resolved_on_its_own(spark):
    """One flight having both arms must not decide the other's."""
    df = _events(spark, [
        ("trk-1", "ATOT", 19, "legacy"),
        ("trk-1", "ATOT", 0, "acdm"),
        ("trk-2", "ATOT", 19, "legacy"),
    ])

    got = _by_type(merge_milestone_duplicates(df, EventConfig()))

    assert len(got) == 2
    assert json.loads(got[("trk-1", "ATOT")]["info"])["method"] == "acdm"
    assert json.loads(got[("trk-2", "ATOT")]["info"])["method"] == "legacy"


def test_the_two_merged_types_do_not_interfere(spark):
    """``ATOT`` and ``ALDT`` are resolved independently: a flight may have an
    A-CDM landing and only a legacy take-off, which is the common shape when
    reception differs between the two ends of the flight."""
    df = _events(spark, [
        ("trk-1", "ATOT", 19, "legacy"),
        ("trk-1", "ALDT", 100, "legacy"),
        ("trk-1", "ALDT", 95, "acdm"),
    ])

    got = _by_type(merge_milestone_duplicates(df, EventConfig()))

    assert len(got) == 2
    assert json.loads(got[("trk-1", "ATOT")]["info"])["method"] == "legacy"
    assert json.loads(got[("trk-1", "ALDT")]["info"])["method"] == "acdm"


def test_every_other_type_passes_through_untouched(spark):
    """A type with one source has nothing to disambiguate, so the merge must
    not deduplicate it -- two ``level-start`` rows on one flight are two real
    level segments, not a duplicate."""
    df = _events(spark, [
        ("trk-1", "level-start", 10, None),
        ("trk-1", "level-start", 40, None),
        ("trk-1", "AOBT", 0, None),
    ])

    out = merge_milestone_duplicates(df, EventConfig())

    assert out.count() == 3


def test_an_unstamped_row_may_fill_a_gap_but_never_displace_an_arm(spark):
    """An absent method is neither arm. It sorts last so that a re-processed
    table carrying older rows cannot quietly outrank a stamped one."""
    df = _events(spark, [
        ("trk-1", "ATOT", 50, None),
        ("trk-1", "ATOT", 19, "legacy"),
        ("trk-2", "ATOT", 50, None),
    ])

    got = _by_type(merge_milestone_duplicates(df, EventConfig()))

    assert len(got) == 2
    assert json.loads(got[("trk-1", "ATOT")]["info"])["method"] == "legacy"
    assert got[("trk-2", "ATOT")]["event_time"] == T0 + dt.timedelta(seconds=50)


def test_a_tie_within_one_arm_resolves_deterministically(spark):
    """Two rows of the same method for one flight would otherwise be resolved
    by partition order. ``info`` on this table has already been bitten once by
    exactly that."""
    df = _events(spark, [
        ("trk-1", "ATOT", 30, "acdm"),
        ("trk-1", "ATOT", 10, "acdm"),
    ])

    first = merge_milestone_duplicates(df, EventConfig()).collect()[0]["event_time"]
    second = merge_milestone_duplicates(
        df.repartition(4), EventConfig()
    ).collect()[0]["event_time"]

    assert first == second == T0 + dt.timedelta(seconds=10)


def test_the_unmerged_configuration_keeps_both_rows(spark):
    """v0.2.0 published both and told them apart by type string; reconstructing
    it must still produce both."""
    df = _events(spark, [
        ("trk-1", "ATOT", 19, "legacy"),
        ("trk-1", "ATOT", 0, "acdm"),
    ])

    out = merge_milestone_duplicates(
        df, EventConfig(merge_duplicate_milestones=False)
    )

    assert out.count() == 2
