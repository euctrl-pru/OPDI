"""Tests for ground-truth flight intervals and the interval-overlap join.

The join deliberately does NOT use callsign. Arm A4 drops callsign from track
identity; joining ground truth on it would score A4 against a key it does not
have, and would do so invisibly.
"""

import datetime as dt

from pyspark.sql import Row
from track_truth import overlap_join

_T0 = dt.datetime(2025, 6, 5, 8, 0, 0)


def _gt(spark, rows):
    return spark.createDataFrame([Row(**r) for r in rows])


def _assign(spark, rows):
    return spark.createDataFrame([Row(**r) for r in rows])


def test_overlap_join_matches_a_sample_inside_the_interval(spark):
    gt = _gt(spark, [{"flight_key": "F1", "icao24": "abc123",
                      "t_off": _T0, "t_land": _T0 + dt.timedelta(hours=2)}])
    a = _assign(spark, [{"icao24": "abc123", "event_time": _T0 + dt.timedelta(minutes=30),
                         "track_id": "T1"}])
    out = overlap_join(a, gt).collect()
    assert len(out) == 1 and out[0]["flight_key"] == "F1"


def test_overlap_join_excludes_a_sample_outside_every_interval(spark):
    gt = _gt(spark, [{"flight_key": "F1", "icao24": "abc123",
                      "t_off": _T0, "t_land": _T0 + dt.timedelta(hours=2)}])
    a = _assign(spark, [{"icao24": "abc123", "event_time": _T0 - dt.timedelta(hours=1),
                         "track_id": "T1"}])
    assert overlap_join(a, gt).count() == 0


def test_overlap_join_ignores_callsign_entirely(spark):
    """A4's whole premise. If this test ever needs callsign, the join is wrong."""
    gt = _gt(spark, [{"flight_key": "F1", "icao24": "abc123", "callsign": "BEL123",
                      "t_off": _T0, "t_land": _T0 + dt.timedelta(hours=2)}])
    a = _assign(spark, [{"icao24": "abc123", "callsign": "TOTALLY_DIFFERENT",
                         "event_time": _T0 + dt.timedelta(minutes=30), "track_id": "T1"}])
    assert overlap_join(a, gt).count() == 1


def test_overlap_join_does_not_cross_airframes(spark):
    gt = _gt(spark, [{"flight_key": "F1", "icao24": "abc123",
                      "t_off": _T0, "t_land": _T0 + dt.timedelta(hours=2)}])
    a = _assign(spark, [{"icao24": "zzz999", "event_time": _T0 + dt.timedelta(minutes=30),
                         "track_id": "T1"}])
    assert overlap_join(a, gt).count() == 0


def test_overlap_join_assigns_a_sample_to_only_one_flight_when_intervals_touch(spark):
    """Back-to-back legs must not double-count the sample at the boundary."""
    gt = _gt(spark, [
        {"flight_key": "F1", "icao24": "abc123",
         "t_off": _T0, "t_land": _T0 + dt.timedelta(hours=1)},
        {"flight_key": "F2", "icao24": "abc123",
         "t_off": _T0 + dt.timedelta(hours=1), "t_land": _T0 + dt.timedelta(hours=2)},
    ])
    a = _assign(spark, [{"icao24": "abc123", "event_time": _T0 + dt.timedelta(hours=1),
                         "track_id": "T1"}])
    assert overlap_join(a, gt).count() == 1
