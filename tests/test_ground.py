"""Tests for the ground movement milestones (T04 off-block, T21 on-block)."""

import pytest
from pyspark.sql import functions as F

from conftest import _EPOCH, make_track

from opdi.config import EventConfig
from opdi.pipeline.ground import block_times, movement_window

KT_PER_MPS = 1.94384


def _kt(kt):
    return kt / KT_PER_MPS


def _speeds(spark, speeds_kt, step_s=10):
    return make_track(
        spark, [{"t": i * step_s, "velocity": _kt(v)} for i, v in enumerate(speeds_kt)]
    )


def _stand_events(spark, types):
    rows = [("trk-1", t, _EPOCH) for t in types]
    return spark.createDataFrame(rows, "track_id string, type string, event_time timestamp")


def test_sustained_movement_is_detected(spark):
    sdf = _speeds(spark, [0, 0, 8, 10, 12, 10, 9, 0, 0])

    got = movement_window(sdf, EventConfig()).collect()

    assert len(got) == 1
    assert (got[0].moving_start - _EPOCH).total_seconds() == 20
    assert (got[0].moving_stop - _EPOCH).total_seconds() == 60


def test_a_brief_jitter_is_not_movement(spark):
    """One sample above the threshold is a wobble in the speed field, not a
    push. At 5-10 s sampling that is not a rare occurrence."""
    sdf = _speeds(spark, [0, 0, 8, 0, 0, 0, 0])

    assert movement_window(sdf, EventConfig()).collect() == []


def test_a_stationary_aircraft_yields_nothing(spark):
    assert movement_window(_speeds(spark, [0, 0, 1, 0, 1, 0]), EventConfig()).collect() == []


def test_off_block_requires_having_left_a_stand(spark):
    """Without the anchor, a track picked up mid-taxi would report its first
    movement as an off-block -- a different event, and one that would read as a
    very short taxi rather than as a miss."""
    movements = movement_window(_speeds(spark, [0, 0, 8, 10, 12, 10, 0]), EventConfig())

    anchored = block_times(movements, _stand_events(spark, ["exit-parking_position"])).collect()
    unanchored = block_times(movements, _stand_events(spark, ["entry-runway"])).collect()

    assert anchored[0].aobt is not None
    assert unanchored == []


def test_on_block_requires_having_entered_a_stand(spark):
    movements = movement_window(_speeds(spark, [0, 0, 8, 10, 12, 10, 0]), EventConfig())

    got = block_times(movements, _stand_events(spark, ["entry-parking_position"])).collect()

    assert got[0].aibt is not None
    assert got[0].aobt is None


def test_a_turnaround_gets_both(spark):
    movements = movement_window(_speeds(spark, [0, 0, 8, 10, 12, 10, 0]), EventConfig())

    got = block_times(
        movements,
        _stand_events(spark, ["exit-parking_position", "entry-parking_position"]),
    ).collect()

    assert got[0].aobt is not None and got[0].aibt is not None
