"""Scoring the A10 link decision: real holes against truth, and artificial holes.

Fixtures are short flights on the equator: a low climb, a cruise at 480 kt heading
east, and a low descent, with one sample a minute. The climb and descent stand
still, so the distance over any cut is exactly the cruise flown beyond it.
"""
import datetime as dt
import math

import pytest
from conftest import _EPOCH, make_track
from pyspark.sql import functions as F
from track_link_controls import (
    label_real_holes,
    negative_controls,
    positive_controls,
    real_hole_candidates,
    rule_of_three_upper,
)

from opdi.pipeline.flights import EARTH_R_NM
from opdi.pipeline.segmentation import SegmentationParams, assign_track_id
from opdi.pipeline.segmentation.base import FPM_PER_MPS, FT_PER_M, KT_PER_MPS
from opdi.pipeline.segmentation.methods import (
    cruise_linked,
    link_feature_columns,
    linkable,
)

P = SegmentationParams()
FLOOR_FT = 25000.0
CRUISE_M = 11000.0
LOW_M = 5000.0 / FT_PER_M
CRUISE_KT = 480.0


def _at(seconds):
    return _EPOCH + dt.timedelta(seconds=seconds)


def _lon(nm):
    return math.degrees(nm / EARTH_R_NM)


def _flight(start_s, cruise_min, start_nm=0.0, climb_min=5, descent_min=5,
            callsign="DLH1", step_s=60, key="F1", icao24="abc123"):
    """Samples of one flight, its truth row, and when its cruise starts and ends."""
    first_cruise = start_s + climb_min * 60
    last_cruise = first_cruise + cruise_min * 60
    end = last_cruise + descent_min * 60
    samples = []
    for t in range(start_s, end + 1, step_s):
        flown_min = min(max(t - first_cruise, 0), cruise_min * 60) / 60.0
        in_cruise = first_cruise <= t <= last_cruise
        samples.append(
            {
                "t": t,
                "icao24": icao24,
                "callsign": callsign,
                "lat": 0.0,
                "lon": _lon(start_nm + flown_min * CRUISE_KT / 60.0),
                "baro_altitude": CRUISE_M if in_cruise else LOW_M,
                "velocity": CRUISE_KT / KT_PER_MPS,
                "vert_rate": 0.0,
            }
        )
    truth = {
        "flight_key": key,
        "icao24": icao24,
        "callsign": callsign,
        "t_off": _at(start_s),
        "t_land": _at(end),
    }
    return samples, truth, first_cruise, last_cruise


def _truth(spark, rows):
    return spark.createDataFrame(rows).select(
        "flight_key", "icao24", "callsign", "t_off", "t_land"
    )


def _pair(spark, base_gap_min=60, cruise_1=180, cruise_2=180, callsign_2="DLH1"):
    """Two consecutive flights whose last and first cruise samples are a set gap apart."""
    s1, t1, _, last_1 = _flight(0, cruise_1, key="F1")
    # The second flight's climb is 5 min, so its cruise opens at last_1 + base gap.
    start_2 = last_1 + base_gap_min * 60 - 5 * 60
    s2, t2, first_2, _ = _flight(
        start_2, cruise_2, start_nm=cruise_1 * CRUISE_KT / 60.0,
        callsign=callsign_2, key="F2",
    )
    return make_track(spark, s1 + s2), _truth(spark, [t1, t2]), last_1, first_2


def _rows_by_depth(controls):
    return {r["depth_min"]: r for r in controls.collect()}


def _linked(controls, p=P):
    """The verdict of the rule's own function, on one row of each control."""
    features = {name: F.col(name) for name in link_feature_columns(p)}
    return controls.withColumn("_linked", linkable(features, p))


def _holes(spark, *holes, **columns):
    return spark.createDataFrame(
        [
            {"icao24": "abc123", "t_old_edge": _at(a), "t_new_edge": _at(b), **columns}
            for a, b in holes
        ]
    )


# --- real holes -------------------------------------------------------------


def test_real_hole_candidates_finds_each_gap_over_the_gap_threshold(spark):
    samples = [{"t": t} for t in (0, 60, 60 + 30 * 60, 60 + 30 * 60 + 60)]
    last = samples[-1]["t"]
    samples += [{"t": last + 31 * 60}, {"t": last + 31 * 60 + 60}]
    rows = real_hole_candidates(make_track(spark, samples), P).collect()

    assert len(rows) == 1  # exactly 30 min is not over it; 31 is
    assert rows[0]["gap_min"] == 31.0
    assert rows[0]["t_old_edge"] == _at(last)
    assert rows[0]["t_new_edge"] == _at(last + 31 * 60)


def test_a_duplicated_edge_sample_is_still_one_hole(spark):
    samples = [{"t": 0}, {"t": 60}, {"t": 60}, {"t": 60 + 40 * 60}, {"t": 60 + 40 * 60}]
    assert real_hole_candidates(make_track(spark, samples), P).count() == 1


def test_real_hole_features_reach_the_verdict_the_rule_reaches(spark):
    """One linkable hole and one too slow: `linkable` here and A10's split must agree."""
    s1, _, _, _ = _flight(0, 60)
    # A hole of 191 min over 1,530 NM, then one of 100 min over 100 NM.
    s2, _, _, _ = _flight(s1[-1]["t"] + 191 * 60, 60, start_nm=480 + 1530)
    s3, _, _, _ = _flight(s2[-1]["t"] + 100 * 60, 60, start_nm=480 + 1530 + 480 + 100)
    for samples in (s1, s2, s3):  # keep the edges in cruise: no low samples at all
        for sample in samples:
            sample["baro_altitude"] = CRUISE_M
    df = make_track(spark, s1 + s2 + s3)

    verdicts = _linked(real_hole_candidates(df, P)).orderBy("t_old_edge").collect()

    assert [r["_linked"] for r in verdicts] == [True, False]
    assert assign_track_id(df, cruise_linked(), P).select("track_id").distinct().count() == 2


# --- labels -----------------------------------------------------------------


def test_a_hole_inside_one_flight_is_a_true_link(spark):
    truth = _truth(spark, [_flight(0, 300)[1]])
    rows = label_real_holes(_holes(spark, (3600, 9000)), truth).collect()
    assert [r["label"] for r in rows] == ["true_link"]


def test_a_hole_holding_a_landing_and_a_takeoff_is_a_false_join(spark):
    first = _flight(0, 60, key="F1")[1]
    second = _flight(4 * 3600, 60, key="F2")[1]
    rows = label_real_holes(
        _holes(spark, (1800, 4 * 3600 + 1800)), _truth(spark, [first, second])
    ).collect()
    assert [r["label"] for r in rows] == ["false_join"]


def test_a_hole_with_no_truth_is_unknown(spark):
    other = {**_flight(0, 300)[1], "icao24": "def456"}
    rows = label_real_holes(_holes(spark, (3600, 9000)), _truth(spark, [other])).collect()
    assert [r["label"] for r in rows] == ["unknown"]


def test_a_landing_with_no_later_takeoff_is_unknown(spark):
    """The flight may have landed, or truth may simply not hold the rest of it."""
    only = _flight(0, 60)[1]  # lands at 70 min
    rows = label_real_holes(_holes(spark, (1800, 3 * 3600)), _truth(spark, [only])).collect()
    assert [r["label"] for r in rows] == ["unknown"]


def test_a_takeoff_followed_by_a_landing_inside_a_hole_is_not_a_false_join(spark):
    """Spec: a landing and a *later* take-off. This order proves no stop between edges."""
    whole = _flight(3600, 60)[1]
    rows = label_real_holes(_holes(spark, (0, 3 * 3600)), _truth(spark, [whole])).collect()
    assert [r["label"] for r in rows] == ["unknown"]


def test_a_false_join_outranks_a_containing_flight(spark):
    """Corrupt, overlapping truth: a glued flight is the worse error to hide."""
    long_one = _flight(0, 600, key="F0")[1]
    first = _flight(1800, 60, key="F1")[1]    # lands at 100 min
    second = _flight(4 * 3600, 60, key="F2")[1]
    rows = label_real_holes(
        _holes(spark, (3600, 5 * 3600)), _truth(spark, [long_one, first, second])
    ).collect()
    assert [r["label"] for r in rows] == ["false_join"]


def test_labels_never_use_callsign(spark):
    truth = _truth(spark, [_flight(0, 300, callsign="NOTEVEN")[1]])
    holes = _holes(spark, (3600, 9000), cs_before="DLH1", cs_after="BAW2")
    rows = label_real_holes(holes, truth).collect()
    assert [r["label"] for r in rows] == ["true_link"]


def test_labelling_keeps_one_row_per_hole_and_its_columns(spark):
    first = _flight(0, 60, key="F1")[1]
    second = _flight(4 * 3600, 60, key="F2")[1]
    third = _flight(8 * 3600, 60, key="F3")[1]
    holes = _holes(spark, (600, 1200), (1800, 4 * 3600 + 1800), (6 * 3600, 7 * 3600), gc_nm=1.0)
    out = label_real_holes(holes, _truth(spark, [first, second, third]))

    assert sorted(r["label"] for r in out.collect()) == ["false_join", "true_link", "unknown"]
    assert "gc_nm" in out.columns


# --- negative controls ------------------------------------------------------


def test_base_cut_uses_the_last_and_first_cruise_samples(spark):
    tracks, truth, last_1, first_2 = _pair(spark)
    control = negative_controls(tracks, truth, FLOOR_FT, depths_min=(0,)).collect()

    assert len(control) == 1
    assert control[0]["t_old_edge"] == _at(last_1)
    assert control[0]["t_new_edge"] == _at(first_2)
    assert control[0]["gap_min"] == 60.0
    assert control[0]["depth_min"] == 0
    assert control[0]["expected"] == "split"
    assert control[0]["control_id"]


def test_a_deep_cut_moves_into_cruise_on_each_side(spark):
    tracks, truth, last_1, first_2 = _pair(spark)
    deep = _rows_by_depth(negative_controls(tracks, truth, FLOOR_FT, depths_min=(0, 30)))

    assert deep[30]["t_old_edge"] == _at(last_1 - 30 * 60)
    assert deep[30]["t_new_edge"] == _at(first_2 + 30 * 60)
    assert deep[30]["gap_min"] == 120.0
    assert deep[30]["gc_nm"] == pytest.approx(CRUISE_KT, rel=1e-3)
    assert deep[0]["control_id"] != deep[30]["control_id"]


def test_a_deep_cut_outside_cruise_is_dropped(spark):
    tracks, truth, _, _ = _pair(spark, cruise_1=90, cruise_2=180)
    depths = _rows_by_depth(negative_controls(tracks, truth, FLOOR_FT, depths_min=(60, 120)))
    assert sorted(depths) == [60]


def test_a_deep_cut_may_not_cross_an_earlier_dip_below_the_floor(spark):
    """Cruise on both sides of a dip is not 'stays at or above the floor'."""
    s1, t1, _, last_1 = _flight(0, 180, key="F1")
    for sample in s1:
        if last_1 - 45 * 60 <= sample["t"] <= last_1 - 40 * 60:
            sample["baro_altitude"] = LOW_M
    s2, t2, _, _ = _flight(last_1 + 55 * 60, 180, key="F2")
    tracks = make_track(spark, s1 + s2)

    depths = _rows_by_depth(
        negative_controls(tracks, _truth(spark, [t1, t2]), FLOOR_FT, depths_min=(30, 60))
    )
    assert sorted(depths) == [30]


def test_a_missing_altitude_does_not_end_a_cruise(spark):
    """linkable's altitude test ignores a NULL, so the cut does too."""
    s1, t1, _, last_1 = _flight(0, 180, key="F1")
    for sample in s1:
        if sample["t"] in (last_1 - 10 * 60, last_1):
            sample["baro_altitude"] = None
    s2, t2, _, _ = _flight(last_1 + 55 * 60, 180, key="F2")
    tracks = make_track(spark, s1 + s2)

    depths = _rows_by_depth(
        negative_controls(tracks, _truth(spark, [t1, t2]), FLOOR_FT, depths_min=(0, 30))
    )
    assert sorted(depths) == [0, 30]
    assert depths[0]["t_old_edge"] == _at(last_1 - 60)  # the last sample known at the floor


def test_controls_of_30_min_or_less_are_excluded(spark):
    tracks, truth, _, _ = _pair(spark, base_gap_min=30)
    depths = _rows_by_depth(negative_controls(tracks, truth, FLOOR_FT, depths_min=(0, 30)))
    assert sorted(depths) == [30]


def test_overlapping_truth_flights_are_not_a_pair(spark):
    s1, t1, _, _ = _flight(0, 120, key="F1")
    s2, t2, _, _ = _flight(3600, 120, key="F2")
    controls = negative_controls(make_track(spark, s1 + s2), _truth(spark, [t1, t2]), FLOOR_FT)
    assert controls.count() == 0


def test_only_consecutive_flights_pair(spark):
    a, ta, _, _ = _flight(0, 60, key="F1")
    b, tb, _, _ = _flight(3 * 3600, 60, key="F2")
    c, tc, _, _ = _flight(6 * 3600, 60, key="F3")
    controls = negative_controls(
        make_track(spark, a + b + c), _truth(spark, [ta, tb, tc]), FLOOR_FT, depths_min=(0,)
    )
    assert controls.count() == 2


def test_a_continuing_flight_control_can_pass_the_speed_test(spark):
    """A to B, then B to C, a 30 min turnaround, cut 60 min into each cruise.

    960 NM over 150 min is 384 kt with both edges in cruise. Nothing about the
    edges gives the stop away, which is what makes this control a real test.
    """
    tracks, truth, _, _ = _pair(spark, base_gap_min=30, cruise_1=120, cruise_2=120)
    controls = _linked(negative_controls(tracks, truth, FLOOR_FT, depths_min=(60,)))
    row = controls.collect()[0]

    assert row["gap_min"] == 150.0
    assert row["gc_nm"] / (row["gap_min"] / 60.0) == pytest.approx(384.0, rel=1e-2)
    assert row["same_callsign"] is True
    assert row["_linked"] is True  # a false join, and the study counts it as one


def test_gap_strata_are_the_specs(spark):
    def stratum(base_gap_min):
        tracks, truth, _, _ = _pair(spark, base_gap_min=base_gap_min)
        return negative_controls(tracks, truth, FLOOR_FT, depths_min=(0,)).collect()[0]["gap_stratum"]

    assert stratum(120) == "30-120"
    assert stratum(121) == "120-240"
    assert stratum(240) == "120-240"
    assert stratum(360) == "240-360"
    assert stratum(361) == "over-360"


def test_same_callsign_reads_the_broadcast_callsigns(spark):
    tracks, truth, _, _ = _pair(spark, callsign_2="BAW2")
    row = negative_controls(tracks, truth, FLOOR_FT, depths_min=(0,)).collect()[0]
    assert (row["cs_before"], row["cs_after"]) == ("DLH1", "BAW2")
    assert row["same_callsign"] is False


def test_a_blank_callsign_counts_as_same(spark):
    """Same-callsign is the hard stratum: the callsign test cannot help there."""
    tracks, truth, _, _ = _pair(spark, callsign_2="")
    row = negative_controls(tracks, truth, FLOOR_FT, depths_min=(0,)).collect()[0]
    assert row["cs_after"] is None
    assert row["same_callsign"] is True


def test_a_control_scores_as_the_same_hole_left_in_the_data(spark):
    """Delete the samples for real: the features must not depend on which route got there."""
    tracks, truth, last_1, first_2 = _pair(spark)
    control = negative_controls(tracks, truth, FLOOR_FT, depths_min=(30,)).collect()[0]

    kept = tracks.filter(
        (F.col("event_time") <= F.lit(_at(last_1 - 30 * 60)))
        | (F.col("event_time") >= F.lit(_at(first_2 + 30 * 60)))
    )
    hole = real_hole_candidates(kept, P).collect()[0]

    for name in link_feature_columns(P):
        assert control[name] == hole[name], name


@pytest.mark.parametrize(
    "blank_before_min, blank_after_min, cs_before, cs_after",
    [
        (30, 0, "DLH1", "DLH1"),  # older than five samples, inside the lookback
        (90, 0, None, "DLH1"),    # beyond the lookback, so it no longer counts
        (0, 8, "DLH1", "DLH1"),   # first seen inside the lookahead
        (0, 15, "DLH1", None),    # first seen beyond it
    ],
)
def test_a_control_keeps_the_callsign_evidence_the_rule_reads(
    spark, blank_before_min, blank_after_min, cs_before, cs_after
):
    s1, t1, _, last_1 = _flight(0, 180, key="F1")
    s2, t2, first_2, _ = _flight(last_1 + 55 * 60, 180, key="F2")
    for sample in s1:
        if sample["t"] > last_1 - blank_before_min * 60:
            sample["callsign"] = ""
    for sample in s2:
        if first_2 <= sample["t"] < first_2 + blank_after_min * 60:
            sample["callsign"] = ""
    tracks = make_track(spark, s1 + s2)

    control = negative_controls(
        tracks, _truth(spark, [t1, t2]), FLOOR_FT, depths_min=(0,)
    ).collect()[0]

    assert (control["cs_before"], control["cs_after"]) == (cs_before, cs_after)


def test_controls_keep_enough_samples_when_they_are_sparse(spark):
    """Five samples an edge, even if they reach beyond the callsign bounds."""
    s1, t1, _, last_1 = _flight(0, 900, step_s=20 * 60, key="F1")
    s2, t2, _, _ = _flight(last_1 + 55 * 60, 900, step_s=20 * 60, key="F2")
    for sample in s1 + s2:
        sample["baro_altitude"] = CRUISE_M
    for sample in s2[:3]:
        sample["vert_rate"] = 10.0
    tracks = make_track(spark, s1 + s2)
    row = negative_controls(
        tracks, _truth(spark, [t1, t2]), FLOOR_FT, depths_min=(0,)
    ).collect()[0]
    # Three of five samples at 10 m/s is 1,181 ft/min; with only the lookahead's
    # single sample kept it would read 1,969 ft/min.
    assert row["vr_after_fpm"] == pytest.approx(10.0 * FPM_PER_MPS * 3 / 5)


# --- positive controls ------------------------------------------------------


def test_positive_control_has_a_hole_of_the_requested_length(spark):
    samples, truth, first, last = _flight(0, 240)
    controls = positive_controls(
        make_track(spark, samples), _truth(spark, [truth]), FLOOR_FT, gaps_min=(60, 120)
    ).orderBy("gap_min").collect()

    assert [r["gap_min"] for r in controls] == [60.0, 120.0]
    assert [r["requested_gap_min"] for r in controls] == [60, 120]
    assert [r["expected"] for r in controls] == ["link", "link"]
    assert all(r["depth_min"] is None for r in controls)
    assert controls[0]["control_id"] != controls[1]["control_id"]
    # The middle of the cruise, starting at the floor.
    middle = (first + last) / 2
    assert controls[0]["t_old_edge"] == _at(middle - 30 * 60)
    assert controls[0]["t_new_edge"] == _at(middle + 30 * 60)


def test_a_positive_control_on_a_continuous_flight_links(spark):
    samples, truth, _, _ = _flight(0, 240)
    controls = _linked(
        positive_controls(make_track(spark, samples), _truth(spark, [truth]), FLOOR_FT, gaps_min=(120,))
    )
    row = controls.collect()[0]
    assert row["gc_nm"] == pytest.approx(960.0, rel=1e-3)
    assert row["same_callsign"] is True
    assert row["_linked"] is True


def test_the_longest_positive_control_stays_inside_the_longest_gap_allowed(spark):
    """At 37 s sampling no pair is exactly 360 min apart. A hole of 360.1 min would
    be rejected by `gap <= 360` and reported as a missed link it did not earn."""
    samples, truth, _, _ = _flight(0, 400, step_s=37)
    controls = _linked(
        positive_controls(make_track(spark, samples), _truth(spark, [truth]), FLOOR_FT, gaps_min=(360,))
    )
    row = controls.collect()[0]

    assert 359.0 <= row["gap_min"] <= 360.0
    assert row["gap_stratum"] == "240-360"
    assert row["_linked"] is True


def test_a_positive_control_must_start_at_the_floor(spark):
    samples, truth, _, _ = _flight(0, 90)
    controls = positive_controls(
        make_track(spark, samples), _truth(spark, [truth]), FLOOR_FT, gaps_min=(60, 120)
    )
    assert [r["requested_gap_min"] for r in controls.collect()] == [60]


def test_a_real_hole_at_the_cut_gives_no_control_of_the_wrong_length(spark):
    samples, truth, first, last = _flight(0, 240)
    middle = (first + last) / 2
    samples = [s for s in samples if not middle - 40 * 60 < s["t"] < middle + 40 * 60]
    controls = positive_controls(
        make_track(spark, samples), _truth(spark, [truth]), FLOOR_FT, gaps_min=(60,)
    )
    assert controls.count() == 0


# --- rule of three ----------------------------------------------------------


def test_rule_of_three():
    assert rule_of_three_upper(0, 3000) == pytest.approx(0.001, rel=0.01)
    assert rule_of_three_upper(0, 60) == pytest.approx(0.05, rel=0.05)
    assert rule_of_three_upper(0, 0) is None


def test_rule_of_three_above_zero_is_where_the_binomial_tail_is_five_percent():
    for events, n in ((1, 100), (10, 100), (40, 1000)):
        bound = rule_of_three_upper(events, n)
        tail = sum(
            math.comb(n, i) * bound**i * (1 - bound) ** (n - i) for i in range(events + 1)
        )
        assert tail == pytest.approx(0.05, abs=1e-9)
    assert rule_of_three_upper(5, 5) == 1.0


def test_rule_of_three_refuses_impossible_counts():
    with pytest.raises(ValueError):
        rule_of_three_upper(3, 2)
    with pytest.raises(ValueError):
        rule_of_three_upper(-1, 2)
