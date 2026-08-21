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
    airport_anchored,
    ground_anchored,
    legacy,
    no_month_suffix,
    traffic_style,
    vertical_profile,
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


# -- A5: ground-anchored -----------------------------------------------------

def _continuous_turnaround(spark):
    """Two flights with an 8 minute on-ground turnaround and NO data gap.

    Legacy cannot split this: the gap never exceeds 15 minutes, so neither rule
    fires. This is the case A5 exists for.
    """
    rows = []
    for i in range(5):                                    # inbound descent
        rows.append({"t": i * 60, "baro_altitude": 3000.0 - i * 600, "on_ground": False,
                     "velocity": 100.0})
    for i in range(8):                                    # 8 min on stand
        rows.append({"t": 300 + i * 60, "baro_altitude": 0.0, "on_ground": True,
                     "velocity": 0.0})
    for i in range(5):                                    # outbound climb
        rows.append({"t": 780 + i * 60, "baro_altitude": i * 600, "on_ground": False,
                     "velocity": 100.0})
    return make_track(spark, rows)


def test_legacy_merges_a_continuous_turnaround(spark):
    """The defect A5 exists to remove."""
    assert n_tracks(assign_track_id(_continuous_turnaround(spark), legacy(), P)) == 1


def test_ground_anchored_splits_a_continuous_turnaround(spark):
    p = SegmentationParams(ground_dwell_minutes=5.0)
    assert n_tracks(assign_track_id(_continuous_turnaround(spark), ground_anchored(), p)) == 2


def test_ground_anchored_ignores_a_touch_and_go(spark):
    """A 60 second ground contact is not a turnaround, whatever else it is."""
    rows = [{"t": i * 30, "baro_altitude": 1000.0 - i * 300, "on_ground": False,
             "velocity": 100.0} for i in range(4)]
    rows += [{"t": 120 + i * 30, "baro_altitude": 0.0, "on_ground": True,
              "velocity": 80.0} for i in range(2)]
    rows += [{"t": 180 + i * 30, "baro_altitude": i * 300, "on_ground": False,
              "velocity": 100.0} for i in range(4)]
    p = SegmentationParams(ground_dwell_minutes=5.0)
    assert n_tracks(assign_track_id(make_track(spark, rows), ground_anchored(), p)) == 1


# -- A6: airport-anchored -----------------------------------------------------

def test_airport_anchored_requires_the_break_to_be_at_an_airport(spark):
    """A slow, low, long dwell away from any aerodrome is not a turnaround.

    Legacy's 15-minute low-altitude rule fires on it regardless. This is the
    high-field-elevation case inverted: the test is proximity, not altitude.
    """
    rows = [{"t": 0, "baro_altitude": 100.0, "on_ground": True, "velocity": 0.0,
             "near_airport": False, "field_elev_ft": 0.0}]
    rows += [{"t": 20 * 60, "baro_altitude": 100.0, "on_ground": True, "velocity": 0.0,
              "near_airport": False, "field_elev_ft": 0.0}]
    df = make_track(spark, rows)
    assert n_tracks(assign_track_id(df, airport_anchored(), P)) == 1
    assert n_tracks(assign_track_id(df, legacy(), P)) == 2


def test_airport_anchored_uses_height_above_field_not_barometric_altitude(spark):
    """An aircraft parked at a 6,000 ft aerodrome is on the ground, not at altitude.

    ``track_quality.py`` names this exact case: legacy's ``baro_altitude < 1524 m``
    never fires, so the turnaround is missed and two flights merge.
    """
    rows = [
        {"t": 0, "baro_altitude": 1900.0, "on_ground": True, "velocity": 0.0,
         "near_airport": True, "field_elev_ft": 6200.0},
        {"t": 20 * 60, "baro_altitude": 1900.0, "on_ground": True, "velocity": 0.0,
         "near_airport": True, "field_elev_ft": 6200.0},
    ]
    df = make_track(spark, rows)
    assert n_tracks(assign_track_id(df, legacy(), P)) == 1        # missed
    assert n_tracks(assign_track_id(df, airport_anchored(), P)) == 2   # caught


# -- A7: vertical-profile -----------------------------------------------------

def test_vertical_profile_splits_on_a_descent_climb_cycle_with_no_gap_and_no_ground(spark):
    """Ground contact is not always broadcast. The profile still shows two sorties."""
    rows = [{"t": i * 60, "baro_altitude": 9000.0 - i * 900} for i in range(10)]
    rows += [{"t": 600 + i * 60, "baro_altitude": i * 900} for i in range(10)]
    for r in rows:
        r["on_ground"] = False
    p = SegmentationParams(descent_floor_ft=1500.0)
    assert n_tracks(assign_track_id(make_track(spark, rows), vertical_profile(), p)) == 2


def test_vertical_profile_does_not_split_a_step_descent_in_the_cruise(spark):
    """FL350 -> FL310 is a level change, not a landing."""
    rows = [{"t": i * 60, "baro_altitude": 10600.0, "on_ground": False} for i in range(5)]
    rows += [{"t": 300 + i * 60, "baro_altitude": 9400.0, "on_ground": False}
             for i in range(5)]
    p = SegmentationParams(descent_floor_ft=1500.0)
    assert n_tracks(assign_track_id(make_track(spark, rows), vertical_profile(), p)) == 1
