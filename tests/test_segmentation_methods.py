"""One test per arm, asserting the failure mode that arm exists to fix.

A test here that merely checks "some tracks came out" is worthless. Each test
constructs a trajectory that the *previous* arm gets wrong, and asserts the new
arm gets it right.
"""

import datetime as dt

from conftest import make_track

from opdi.pipeline.segmentation import SegmentationParams, assign_track_id
from opdi.pipeline.segmentation.methods import (
    ARMS,
    airframe_only,
    airport_anchored,
    ground_anchored,
    legacy,
    no_month_suffix,
    recommended,
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
    behaves identically here -- the point is that it does not do *worse*, and the
    legacy assertion below is what proves that rather than asserting it in prose.
    """
    df = make_track(spark, [
        {"t": 0, "callsign": None, "baro_altitude": 10000.0},
        {"t": 45 * 60, "callsign": None, "baro_altitude": 10000.0},
    ])
    assert n_tracks(assign_track_id(df, airframe_only(), P)) == 2
    # Legacy on the same fixture: identical, because the null callsign has
    # already collapsed its group key to the airframe. A4 is no worse.
    assert n_tracks(assign_track_id(df, legacy(), P)) == 2


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

def _gapped_dwell(spark, near_airport=True, field_elev_ft=6200.0):
    """Two samples 20 minutes apart, parked at a 6,200 ft aerodrome.

    ``baro_altitude`` 1900 m is 6,234 ft, so height above a 6,200 ft field is
    34 ft. **Legacy splits neither variant** -- 20 min is under its 30 min rule,
    and 1900 m is above its 1524 m low-altitude rule -- which is what makes this
    fixture able to isolate one A6 input at a time.
    """
    return make_track(spark, [
        {"t": t, "baro_altitude": 1900.0, "on_ground": True, "velocity": 0.0,
         "near_airport": near_airport, "field_elev_ft": field_elev_ft}
        for t in (0, 20 * 60)
    ])


def test_airport_anchored_requires_the_break_to_be_at_an_airport(spark):
    """A slow, low, long dwell away from any aerodrome is not a turnaround.

    Legacy's 15-minute low-altitude rule fires on it regardless. This is the
    high-field-elevation case inverted: the test is proximity, not altitude.

    This is the arm's *suppression* property, and it is only reachable because
    A6's floor is :func:`legacy_general_gap` alone. Under a full legacy floor the
    low-altitude rule would fire through the floor and A6 would report 2 -- the
    assertion would be unsatisfiable, since an arm ORing in a rule can never
    split less than that rule does. 20 minutes is under the 30-minute general
    rule, so the floor stays silent and the arm answers for itself.
    """
    rows = [{"t": 0, "baro_altitude": 100.0, "on_ground": True, "velocity": 0.0,
             "near_airport": False, "field_elev_ft": 0.0}]
    rows += [{"t": 20 * 60, "baro_altitude": 100.0, "on_ground": True, "velocity": 0.0,
              "near_airport": False, "field_elev_ft": 0.0}]
    df = make_track(spark, rows)
    assert n_tracks(assign_track_id(df, airport_anchored(), P)) == 1
    assert n_tracks(assign_track_id(df, legacy(), P)) == 2


def test_airport_anchored_isolates_proximity_from_every_other_input(spark):
    """Only ``near_airport`` differs between the two frames, and legacy splits
    neither -- so the whole difference in the answer is attributable to
    proximity, with no help from altitude, speed or the floor."""
    at_field = _gapped_dwell(spark, near_airport=True)
    nowhere = _gapped_dwell(spark, near_airport=False)
    assert n_tracks(assign_track_id(at_field, legacy(), P)) == 1
    assert n_tracks(assign_track_id(nowhere, legacy(), P)) == 1
    assert n_tracks(assign_track_id(at_field, airport_anchored(), P)) == 2
    assert n_tracks(assign_track_id(nowhere, airport_anchored(), P)) == 1


def test_airport_anchored_uses_height_above_field_not_barometric_altitude(spark):
    """An aircraft parked at a 6,000 ft aerodrome is on the ground, not at altitude.

    ``track_quality.py`` names this exact case: legacy's ``baro_altitude < 1524 m``
    never fires, so the turnaround is missed and two flights merge. Holding
    ``baro_altitude`` and ``near_airport`` fixed and moving only
    ``field_elev_ft`` isolates the height-above-field arithmetic: over a 6,200 ft
    field the aircraft is 34 ft up and stationary; over sea level the identical
    barometric altitude is 6,234 ft up and it is not.
    """
    high_field = _gapped_dwell(spark, field_elev_ft=6200.0)
    sea_level = _gapped_dwell(spark, field_elev_ft=0.0)
    assert n_tracks(assign_track_id(high_field, legacy(), P)) == 1        # missed
    assert n_tracks(assign_track_id(high_field, airport_anchored(), P)) == 2  # caught
    assert n_tracks(assign_track_id(sea_level, airport_anchored(), P)) == 1


def test_airport_anchored_still_splits_on_a_long_gap_when_its_inputs_are_missing(spark):
    """Task 6 supplies ``near_airport``/``field_elev_ft`` by a left join.

    Every sample away from a covered aerodrome therefore arrives NULL. Without an
    explicit coalesce and without a floor, ``stationary`` would be NULL, no break
    would ever fire, and an entire month of an airframe would land in one track
    -- A6 scoring as a catastrophic merger for reasons that have nothing to do
    with its idea. This is the property the floor exists for, and the *general*
    gap rule alone is enough to guarantee it: a 45-minute gap still splits.
    """
    rows = [
        {"t": 0, "baro_altitude": 300.0, "near_airport": None, "field_elev_ft": None},
        {"t": 45 * 60, "baro_altitude": 300.0,
         "near_airport": None, "field_elev_ft": None},   # 45 min gap: over 30
        {"t": 46 * 60, "baro_altitude": 300.0, "near_airport": None, "field_elev_ft": None},
    ]
    df = make_track(spark, rows)
    assert n_tracks(assign_track_id(df, airport_anchored(), P)) == 2
    assert n_tracks(assign_track_id(df, legacy(), P)) == 2


def _continuous_parked_turnaround(spark):
    """Two flights around a 30 minute parked run at an aerodrome, and NO data gap.

    60 second cadence throughout, so no gap ever reaches ``ground_dwell_minutes``
    and neither the legacy rules nor A6's reception-gap arm can fire. The only
    thing that can split this is *accumulating* the stationary run.
    """
    rows = [
        # inbound, airborne and moving
        {"t": i * 60, "baro_altitude": 3000.0 - i * 600, "on_ground": False,
         "velocity": 100.0, "near_airport": True, "field_elev_ft": 0.0}
        for i in range(5)
    ]
    rows += [
        # 30 minutes on stand: low, stopped, at the aerodrome
        {"t": 300 + i * 60, "baro_altitude": 100.0, "on_ground": True,
         "velocity": 0.0, "near_airport": True, "field_elev_ft": 0.0}
        for i in range(30)
    ]
    rows += [
        # outbound, rolling then climbing away
        {"t": 2100 + i * 60, "baro_altitude": i * 600.0, "on_ground": False,
         "velocity": 100.0, "near_airport": True, "field_elev_ft": 0.0}
        for i in range(5)
    ]
    return make_track(spark, rows)


def test_airport_anchored_splits_a_continuous_parked_run(spark):
    """A6 must accumulate a parked run, not only see gaps in reception.

    The arm used to take its dwell from ``gap_minutes()`` -- the interval from
    the immediately preceding sample -- which for a run of parked samples is the
    *sampling period*, seconds, so it could never reach
    ``ground_dwell_minutes``. A6 therefore fired only on reception gaps, and the
    docstring's claim to cover "a gap in reception **or** a run of parked
    samples" was false. Against this fixture the old implementation returned one
    track while A5 returned two -- which meant an A5-vs-A6 comparison was not
    measuring "on-ground flag versus airport geometry" at all, but that crossed
    with "continuous coverage versus gap-only". That confound is exactly what
    this study exists to avoid.
    """
    df = _continuous_parked_turnaround(spark)
    assert n_tracks(assign_track_id(df, legacy(), P)) == 1            # cannot see it
    assert n_tracks(assign_track_id(df, ground_anchored(), P)) == 2   # A5 can
    assert n_tracks(assign_track_id(df, airport_anchored(), P)) == 2  # A6 must too


# -- A7: vertical-profile -----------------------------------------------------

#: A descent that reaches the floor *continuously*, then a climb away from it.
#: 500 m is 1,640 ft (above the 1,500 ft floor); 200 m is 656 ft (below it), so
#: the descent genuinely crosses the floor without a discontinuity. An earlier
#: fixture descended 9000 -> 900 m -- never reaching the floor -- and then jumped
#: straight to 0 m on its first climb row, so the split it observed came from
#: that climb row being below the floor, not from any descent having reached it.
#: It would have passed with the whole descent leg deleted.
_DESCENT_M = [9000, 8000, 7000, 6000, 5000, 4000, 3000, 2000, 1000, 500, 200, 0]
_CLIMB_M = [0, 200, 500, 1000, 2000, 3000, 4000]


def test_vertical_profile_splits_on_a_descent_climb_cycle_with_no_gap_and_no_ground(spark):
    """Ground contact is not always broadcast. The profile still shows two sorties."""
    rows = [{"t": i * 60, "baro_altitude": float(a), "on_ground": False}
            for i, a in enumerate(_DESCENT_M)]
    rows += [{"t": (len(_DESCENT_M) + i) * 60, "baro_altitude": float(a),
              "on_ground": False}
             for i, a in enumerate(_CLIMB_M)]
    p = SegmentationParams(descent_floor_ft=1500.0)
    out = assign_track_id(make_track(spark, rows), vertical_profile(), p)
    assert n_tracks(out) == 2
    # And it split in the right *place*: the break is the climb row that crosses
    # back above the floor (500 m = 1,640 ft), so the whole descent plus the two
    # sub-floor climb rows are the first track. Asserting the sizes is what makes
    # the descent leg load-bearing -- delete it and these numbers change.
    sizes = sorted(
        r["n"] for r in out.groupBy("track_id").count()
        .withColumnRenamed("count", "n").collect()
    )
    assert sizes == [5, 14]


def test_vertical_profile_does_not_split_a_step_descent_in_the_cruise(spark):
    """FL350 -> FL310 is a level change, not a landing."""
    rows = [{"t": i * 60, "baro_altitude": 10600.0, "on_ground": False} for i in range(5)]
    rows += [{"t": 300 + i * 60, "baro_altitude": 9400.0, "on_ground": False}
             for i in range(5)]
    p = SegmentationParams(descent_floor_ft=1500.0)
    assert n_tracks(assign_track_id(make_track(spark, rows), vertical_profile(), p)) == 1


def test_vertical_profile_does_not_split_a_departure_climb_with_no_prior_descent(spark):
    """A cold track's first departure is not a new sortie.

    The arm's thesis is "a descent reaching the floor, *then* a climb back
    through it". The implementation used to require only the second half --
    ``altitude_ft() > floor`` with the lag row ``<= floor`` -- which fires on
    **any** upward crossing of 1,500 ft, including the very first departure of
    a track that has never descended anywhere. That is not the idea A7 exists
    to test, so A7's measured result was unattributable: it could have been the
    idea failing or the code not being the idea.

    This track only ever climbs, from the ground up through the floor and on
    to the cruise. No gap, no month boundary, so the legacy floor is silent and
    the arm answers for itself. The old code returned 2.
    """
    rows = [{"t": i * 60, "baro_altitude": float(a), "on_ground": False,
             "field_elev_ft": 0.0}
            for i, a in enumerate([0, 100, 200, 500, 1000, 2000, 3000, 4000])]
    p = SegmentationParams(descent_floor_ft=1500.0)
    df = make_track(spark, rows)
    assert n_tracks(assign_track_id(df, vertical_profile(), p)) == 1


def test_vertical_profile_uses_height_above_field_not_barometric_altitude(spark):
    """At a 6,200 ft aerodrome, MSL altitude never goes near the floor.

    The arm compared barometric MSL, so at any field above ``descent_floor_ft``
    the lag row was never at or below the floor, no crossing was ever seen, and
    A7 degenerated to bare legacy -- the exact blindness ``airport_anchored``'s
    docstring argues legacy suffers from, repeated in the arm that was supposed
    to be independent of it. ``field_elev_ft`` is already on the frame.

    Here the aircraft descends to 1,900 m (6,234 ft MSL, 34 ft above a 6,200 ft
    field) and climbs away again. Height above field crosses 1,500 ft downward
    and then upward; MSL altitude never drops below 6,234 ft. Legacy splits
    nothing (no gap, and 1,900 m is above its 1,524 m low-altitude rule), so
    the split is A7's alone. The old code returned 1.
    """
    profile_m = [3000, 2400, 2100, 1900, 1900, 2100, 2400, 3000]
    rows = [{"t": i * 60, "baro_altitude": float(a), "on_ground": False,
             "field_elev_ft": 6200.0}
            for i, a in enumerate(profile_m)]
    p = SegmentationParams(descent_floor_ft=1500.0)
    df = make_track(spark, rows)
    assert n_tracks(assign_track_id(df, legacy(), p)) == 1
    assert n_tracks(assign_track_id(df, vertical_profile(), p)) == 2


# -- A8 and the registry -------------------------------------------------------

def test_every_registered_arm_runs(spark):
    """``ARMS`` is what a Task-6 runner iterates, so every entry must execute.

    ``recommended`` (A8) in particular is a labelled placeholder with no
    behavioural test of its own -- it is still going to be *run*, and a runner
    that dies part-way through the ladder wastes a cluster job. This is a smoke
    test and nothing more: it asserts each arm produces a non-empty, well-formed
    ``track_id`` over a trajectory that exercises a gap, a ground run and a
    climb, not that any arm produces a particular answer.
    """
    rows = [{"t": i * 60, "baro_altitude": 3000.0 - i * 600, "on_ground": False,
             "velocity": 100.0} for i in range(5)]
    rows += [{"t": 300 + i * 60, "baro_altitude": 100.0, "on_ground": True,
              "velocity": 0.0} for i in range(8)]
    rows += [{"t": 900 + 45 * 60 + i * 60, "baro_altitude": i * 600.0,
              "on_ground": False, "velocity": 100.0} for i in range(5)]
    df = make_track(spark, rows)
    for name, factory in ARMS.items():
        out = assign_track_id(df, factory(), P)
        assert out.count() == df.count(), name
        assert out.filter(out.track_id.isNull()).count() == 0, name
        assert n_tracks(out) >= 1, name


def _blank_callsign_gap(spark):
    """One continuous flight whose callsign drops out in the middle.

    This is the common case, not a corner: 42.4% of legacy's tracks on the 2025
    sample are blank-callsign tracks produced exactly this way.
    """
    rows = [{"t": i * 60, "callsign": "ABC123"} for i in range(5)]
    rows += [{"t": (5 + i) * 60, "callsign": ""} for i in range(5)]
    rows += [{"t": (10 + i) * 60, "callsign": "ABC123"} for i in range(5)]
    return make_track(spark, rows)


def test_recommended_does_not_split_when_the_callsign_merely_blanks_out(spark):
    """The fragmentation A8 exists to fix, and legacy's own failure mode.

    ``legacy`` groups on ``(icao24, callsign)``, so the blank run lands in a
    group of its own and the one flight becomes two tracks. A8 groups on the
    airframe and carries the last *real* callsign across the blanks, so it stays
    one. Asserting legacy's count as well makes this a comparison rather than a
    number that could drift to 1 for an unrelated reason and still pass.

    Two, not three: both ``ABC123`` runs share one group, and the 6 minutes
    between them is under ``low_alt_gap_minutes``, so nothing splits them from
    each other. Only the blank run separates. Written as three first, and the
    test corrected the expectation.
    """
    df = _blank_callsign_gap(spark)
    assert n_tracks(assign_track_id(df, legacy(), P)) == 2
    assert n_tracks(assign_track_id(df, recommended(), P)) == 1


def test_recommended_does_not_reach_back_past_a_gap_for_the_previous_callsign(spark):
    """The bug the bounded lookback exists to prevent.

    Two flights of one airframe, separated by a 40-minute silence so the gap
    rule already splits them. The second starts with blank callsigns and only
    resolves to ``XYZ789`` part way in.

    With an unbounded ``F.last``, that resolution is compared against
    ``ABC123`` from *before* the gap, and the second flight splits in its own
    middle -- three tracks where there are two flights. Measured on real data
    that cost 31 points of fragmentation. Bounding the lookback to the gap
    threshold keeps the comparison inside one track.
    """
    rows = [{"t": i * 60, "callsign": "ABC123"} for i in range(5)]
    rows += [{"t": 40 * 60 + i * 60, "callsign": ""} for i in range(5)]
    rows += [{"t": 45 * 60 + i * 60, "callsign": "XYZ789"} for i in range(5)]
    df = make_track(spark, rows)
    assert n_tracks(assign_track_id(df, recommended(), P)) == 2


def test_recommended_ignores_callsign_padding(spark):
    """ADS-B pads callsigns to eight characters; that is not a change.

    Comparing untrimmed values would split this single flight in two, and the
    failure would look exactly like a genuine callsign change.
    """
    rows = [{"t": i * 60, "callsign": "ABC123  "} for i in range(5)]
    rows += [{"t": (5 + i) * 60, "callsign": "ABC123"} for i in range(5)]
    df = make_track(spark, rows)
    assert n_tracks(assign_track_id(df, recommended(), P)) == 1


def test_recommended_splits_when_the_callsign_genuinely_changes(spark):
    """The merging A8 exists to avoid, which ``airframe_only`` cannot.

    Two flights of one airframe, continuous samples and no gap between them --
    so no gap rule can separate them and only the callsign says there are two.
    ``airframe_only`` drops callsign entirely and returns one track; A8 returns
    two. 3.3% of airframe_only's real tracks are this case.
    """
    rows = [{"t": i * 60, "callsign": "ABC123"} for i in range(5)]
    rows += [{"t": (5 + i) * 60, "callsign": "XYZ789"} for i in range(5)]
    df = make_track(spark, rows)
    assert n_tracks(assign_track_id(df, airframe_only(), P)) == 1
    assert n_tracks(assign_track_id(df, recommended(), P)) == 2
