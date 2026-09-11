"""Choosing a batch: a month by time, a day by the track it belongs to."""
import datetime as dt

import pytest
from pyspark.sql import functions as F

from opdi.utils.batching import filter_batch


def _tracks(spark):
    """Two tracks: one starting 2026-06-01 and running past midnight, one
    starting 2026-06-02."""
    rows = [
        ("a", dt.datetime(2026, 6, 1, 22, 50), dt.date(2026, 6, 1)),
        ("a", dt.datetime(2026, 6, 2, 1, 30), dt.date(2026, 6, 1)),
        ("b", dt.datetime(2026, 6, 2, 9, 0), dt.date(2026, 6, 2)),
    ]
    return spark.createDataFrame(rows, "track_id string, event_time timestamp, dof date")


def test_a_day_returns_whole_tracks_including_their_after_midnight_tail(spark):
    """The reason the day path exists.

    Filtering on event_time would give track `a` only its 22:50 sample and hand
    track `b`'s day the 01:30 one -- a flight that never lands, and one that
    never departs.
    """
    out = filter_batch(_tracks(spark), day=dt.date(2026, 6, 1))
    assert out.count() == 2
    assert {r["track_id"] for r in out.collect()} == {"a"}


def test_the_next_day_gets_only_its_own_track(spark):
    out = filter_batch(_tracks(spark), day=dt.date(2026, 6, 2))
    assert {r["track_id"] for r in out.collect()} == {"b"}


def test_consecutive_days_partition_the_rows_between_them(spark):
    df = _tracks(spark)
    d1 = filter_batch(df, day=dt.date(2026, 6, 1)).count()
    d2 = filter_batch(df, day=dt.date(2026, 6, 2)).count()
    assert d1 + d2 == df.count()


def test_a_month_still_selects_by_time(spark):
    out = filter_batch(_tracks(spark), month=dt.date(2026, 6, 1))
    assert out.count() == 3


def test_asking_for_both_is_rejected(spark):
    with pytest.raises(ValueError, match="exactly one"):
        filter_batch(_tracks(spark), month=dt.date(2026, 6, 1), day=dt.date(2026, 6, 1))


def test_asking_for_neither_is_rejected(spark):
    with pytest.raises(ValueError, match="exactly one"):
        filter_batch(_tracks(spark))


def test_a_day_on_a_frame_without_dof_fails_loudly(spark):
    """Silently matching nothing would report an empty day as a result."""
    df = _tracks(spark).drop("dof")
    with pytest.raises(ValueError, match="needs a 'dof' column"):
        filter_batch(df, day=dt.date(2026, 6, 1))
