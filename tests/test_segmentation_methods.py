"""One test per arm, asserting the failure mode that arm exists to fix.

A test here that merely checks "some tracks came out" is worthless. Each test
constructs a trajectory that the *previous* arm gets wrong, and asserts the new
arm gets it right.
"""

import datetime as dt

from conftest import make_track

from opdi.pipeline.segmentation import SegmentationParams, assign_track_id
from opdi.pipeline.segmentation.methods import (
    airframe_only,
    legacy,
    no_month_suffix,
    traffic_style,
)

P = SegmentationParams()
_EPOCH = dt.datetime(2024, 6, 1, 12, 0, 0)


def n_tracks(df):
    return df.select("track_id").distinct().count()


def _cruise_over_midnight(spark):
    """A flight airborne across 30 June -> 1 July. One flight, no gaps."""
    off = (dt.datetime(2024, 6, 30, 23, 30, 0) - _EPOCH).total_seconds()
    return make_track(spark, [
        {"t": off + i * 300, "baro_altitude": 10000.0} for i in range(8)
    ])


# -- A2: window truncation ---------------------------------------------------

def test_legacy_splits_a_flight_at_the_month_boundary(spark):
    """The defect A2 exists to remove. If this ever fails, A2 is pointless."""
    assert n_tracks(assign_track_id(_cruise_over_midnight(spark), legacy(), P)) == 2


def test_no_month_suffix_keeps_a_midnight_crossing_flight_whole(spark):
    assert n_tracks(assign_track_id(_cruise_over_midnight(spark), no_month_suffix(), P)) == 1


def test_no_month_suffix_still_splits_on_a_real_gap(spark):
    df = make_track(spark, [
        {"t": 0, "baro_altitude": 10000.0},
        {"t": 60, "baro_altitude": 10000.0},
        {"t": 60 + 45 * 60, "baro_altitude": 10000.0},   # 45 min gap
    ])
    assert n_tracks(assign_track_id(df, no_month_suffix(), P)) == 2


# -- A3: traffic-style -------------------------------------------------------

def test_traffic_style_splits_on_its_shorter_default_gap(spark):
    """traffic's default gap is 10 minutes, so a 12 minute gap splits.

    Low altitude deliberately: at cruise the arm's own predicate suppresses the
    split -- that is what test_traffic_style_condition_suppresses... covers -- so
    a cruise trajectory here would be testing the predicate, not the threshold.
    """
    df = make_track(spark, [
        {"t": 0, "baro_altitude": 300.0},
        {"t": 12 * 60, "baro_altitude": 300.0},
    ])
    assert n_tracks(assign_track_id(df, traffic_style(), P)) == 2
    # legacy keeps it whole: 12 min is under both its 30 min and its 15 min rule.
    assert n_tracks(assign_track_id(df, legacy(), P)) == 1


def test_traffic_style_condition_suppresses_a_split_between_two_airborne_samples(spark):
    """`Flight.split(condition=...)`: do not break when both sides are high.

    A 12 minute reception hole in the cruise is a coverage gap, not a landing.
    """
    df = make_track(spark, [
        {"t": 0, "baro_altitude": 10000.0},
        {"t": 12 * 60, "baro_altitude": 10000.0},
    ])
    p = SegmentationParams(low_alt_ft=40000.0)   # everything counts as "low"
    assert n_tracks(assign_track_id(df, traffic_style(), p)) == 2
    p_high = SegmentationParams(low_alt_ft=1000.0)  # nothing counts as "low"
    assert n_tracks(assign_track_id(df, traffic_style(), p_high)) == 1


# -- A4: callsign coupling ---------------------------------------------------

def test_legacy_splits_a_flight_when_the_callsign_changes_mid_flight(spark):
    """The defect A4 exists to remove."""
    df = make_track(spark, [
        {"t": 0, "callsign": "BEL123", "baro_altitude": 10000.0},
        {"t": 60, "callsign": "BEL123", "baro_altitude": 10000.0},
        {"t": 120, "callsign": "BEL12", "baro_altitude": 10000.0},   # truncated
        {"t": 180, "callsign": "BEL12", "baro_altitude": 10000.0},
    ])
    assert n_tracks(assign_track_id(df, legacy(), P)) == 2


def test_airframe_only_keeps_a_flight_whole_across_a_callsign_change(spark):
    df = make_track(spark, [
        {"t": 0, "callsign": "BEL123", "baro_altitude": 10000.0},
        {"t": 60, "callsign": "BEL123", "baro_altitude": 10000.0},
        {"t": 120, "callsign": "BEL12", "baro_altitude": 10000.0},
        {"t": 180, "callsign": "BEL12", "baro_altitude": 10000.0},
    ])
    assert n_tracks(assign_track_id(df, airframe_only(), P)) == 1


def test_airframe_only_separates_two_airframes(spark):
    """Grouping on icao24 alone must not merge different aircraft."""
    df = make_track(spark, [
        {"t": 0, "icao24": "aaa111", "baro_altitude": 10000.0},
        {"t": 60, "icao24": "bbb222", "baro_altitude": 10000.0},
    ])
    assert n_tracks(assign_track_id(df, airframe_only(), P)) == 2


def test_airframe_only_still_splits_a_null_callsign_airframe_on_gaps(spark):
    """A null callsign collapses a whole day under legacy. A4 must still segment it.

    Legacy hashes ``icao24 || NULL``; concat_ws skips nulls, so every null-callsign
    sample for an airframe lands in one group and only gaps separate them. A4
    behaves identically here -- the point is that it does not do *worse*.
    """
    df = make_track(spark, [
        {"t": 0, "callsign": None, "baro_altitude": 10000.0},
        {"t": 45 * 60, "callsign": None, "baro_altitude": 10000.0},
    ])
    assert n_tracks(assign_track_id(df, airframe_only(), P)) == 2
