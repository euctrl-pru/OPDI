"""Scoring the A10 link decision: real holes against truth, and artificial holes.

Fixtures are short flights on the equator: a low climb, a cruise at 480 kt heading
east, and a low descent, with one sample a minute. The climb and descent stand
still, so the distance over any cut is exactly the cruise flown beyond it.
"""
import datetime as dt
from dataclasses import replace
import math
import re

import pytest
from conftest import _EPOCH, make_track
from pyspark.sql import Window
from pyspark.sql import functions as F
from track_link_controls import (
    DEPTH_TOLERANCE_MIN,
    _with_engine_columns,
    control_funnel,
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

#: These tests were written at the pre-study settings (the grid's base), so they pass
#: them explicitly; the shipped defaults are pinned in test_segmentation_cruise_linked.
P = SegmentationParams(
    link_cruise_floor_ft=25000.0,
    link_min_implied_speed_kt=315.0,
    link_max_gap_minutes=360.0,
)
FLOOR_FT = 25000.0
CRUISE_M = 11000.0
LOW_M = 5000.0 / FT_PER_M
CRUISE_KT = 480.0


def _at(seconds):
    return _EPOCH + dt.timedelta(seconds=seconds)


def _lon(nm):
    return math.degrees(nm / EARTH_R_NM)


def _flight(start_s, cruise_min, start_nm=0.0, climb_min=5, descent_min=5,
            callsign="DLH1", step_s=60, key="F1", icao24="abc123", low_m=LOW_M,
            low_fpm=0.0):
    """Samples of one flight, its truth row, and when its cruise starts and ends.

    ``low_fpm`` is the rate of climb and of descent, level by default, and a flight
    with no rate has no edge for the low-edge control families to cut at.
    """
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
                "baro_altitude": CRUISE_M if in_cruise else low_m,
                "velocity": CRUISE_KT / KT_PER_MPS,
                "vert_rate": (
                    0.0 if in_cruise else (low_fpm if t < first_cruise else -low_fpm) / FPM_PER_MPS
                ),
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


def test_nan_is_null_in_what_the_generators_compare(spark):
    samples = _flight(0, 30)[0]
    samples[0].update(baro_altitude=float("nan"), velocity=float("nan"), vert_rate=float("nan"))
    row = _with_engine_columns(make_track(spark, samples)).orderBy("event_time").first()
    assert row["_alt_ft"] is None
    assert row["_spd_kt"] is None
    assert row["vert_rate"] is None


def test_a_nan_altitude_does_not_open_a_cruise(spark):
    """NaN passes ``>=``, so a flight with no altitude at all would read as in cruise."""
    samples, flight, _, _ = _flight(0, 240)
    for sample in samples:
        sample["baro_altitude"] = float("nan")
    funnel = control_funnel(make_track(spark, samples), _truth(spark, [flight]), FLOOR_FT, p=P)
    assert funnel["positive_controls"] == 0
    assert funnel["positive_dropped_no_cruise_sample"] == 4


def test_a_nan_rate_is_no_climb_edge(spark):
    """NaN passes ``>``, so it would cut a climb control at a sample with no direction."""
    def climb_controls(rate_of_climb_samples):
        s1, t1, _, last_1 = _flight(0, 180, key="F1", low_fpm=1500.0)
        for sample in s1:
            if sample["baro_altitude"] == LOW_M:
                sample["vert_rate"] = rate_of_climb_samples
        s2, t2, _, _ = _flight(last_1 + 55 * 60, 180, start_nm=1440.0, key="F2", low_fpm=1500.0)
        controls = negative_controls(
            make_track(spark, s1 + s2), _truth(spark, [t1, t2]), FLOOR_FT, depths_min=(0,), p=P
        )
        return len(_family(controls, "climb_cruise"))

    assert climb_controls(1500.0 / FPM_PER_MPS) == 1
    assert climb_controls(float("nan")) == 0


def test_real_holes_and_controls_read_each_edge_once(spark):
    """Column by column it is seven, which made the study 3.2x slower on a local run."""
    tracks, truth, _, _ = _pair(spark)
    for frame in (
        real_hole_candidates(tracks, P),
        negative_controls(tracks, truth, FLOOR_FT, depths_min=(0,)),
        positive_controls(tracks, truth, FLOOR_FT, gaps_min=(60,)),
    ):
        plan = _plan(frame)
        assert len(re.findall(r"\bcollect_list\(", plan)) == 2, plan


def test_real_holes_hold_what_the_separate_columns_give(spark):
    """A blank first sample after the hole, so the values are not all read at the edge."""
    samples = _flight(0, 30)[0] + [
        {**s, "baro_altitude": None} if i < 2 else s
        for i, s in enumerate(_flight(4 * 3600, 30)[0])
    ]
    tracks = make_track(spark, samples)
    window = Window.partitionBy("icao24").orderBy("_ts")
    apart = (
        tracks.withColumn("_ts", F.to_timestamp("event_time"))
        .withColumn("_alt_ft", F.col("baro_altitude") * FT_PER_M)
        .withColumn("_spd_kt", F.col("velocity") * KT_PER_MPS)
        .select(*[c.alias(name) for name, c in link_feature_columns(P, window).items()])
        .filter(F.col("gap_min") > P.gap_minutes)
    )
    together = real_hole_candidates(tracks, P).select(*apart.columns)
    assert together.collect() == apart.collect()
    assert together.count() == 1


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


def test_a_landing_alone_is_a_false_join(spark):
    """Both edges are at cruise, so a landing between them is a stop, whatever followed."""
    only = _flight(0, 60)[1]  # lands at 70 min
    rows = label_real_holes(_holes(spark, (1800, 3 * 3600)), _truth(spark, [only])).collect()
    assert [r["label"] for r in rows] == ["false_join"]


def test_a_takeoff_alone_is_a_false_join(spark):
    later = _flight(3600, 300)[1]
    rows = label_real_holes(_holes(spark, (0, 2 * 3600)), _truth(spark, [later])).collect()
    assert [r["label"] for r in rows] == ["false_join"]


def test_a_whole_flight_inside_a_hole_is_a_false_join(spark):
    """Take-off then landing, both between the edges: a flight neither edge belongs to."""
    whole = _flight(3600, 60)[1]
    rows = label_real_holes(_holes(spark, (0, 3 * 3600)), _truth(spark, [whole])).collect()
    assert [r["label"] for r in rows] == ["false_join"]


def test_a_hole_edge_on_the_flights_own_event_lies_within_it(spark):
    flight = _flight(0, 300)[1]  # 0 to 310 min
    rows = label_real_holes(
        _holes(spark, (0, 3600), (3600, 310 * 60)), _truth(spark, [flight])
    ).collect()
    assert [r["label"] for r in rows] == ["true_link", "true_link"]


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


def test_the_deep_cut_records_the_depth_it_reached(spark):
    tracks, truth, last_1, first_2 = _pair(spark)
    # A short real hole on the old side only, within the tolerance.
    holed = tracks.filter(~F.col("event_time").between(_at(last_1 - 33 * 60 + 1), _at(last_1 - 27 * 60)))
    row = negative_controls(holed, truth, FLOOR_FT, depths_min=(30,)).collect()[0]

    assert row["depth_min"] == 30
    assert row["old_depth_min"] == 33
    assert row["new_depth_min"] == 30
    assert row["t_old_edge"] == _at(last_1 - 33 * 60)
    assert row["pair_id"] == "F1|F2"


def test_a_real_hole_under_a_deep_cut_drops_it_rather_than_moving_it(spark):
    """Three depths would all land on one sample 200 min in, one hole counted thrice."""
    tracks, truth, last_1, _ = _pair(spark, cruise_1=300, cruise_2=300)
    holed = tracks.filter(
        ~F.col("event_time").between(_at(last_1 - 200 * 60 + 1), _at(last_1 - 25 * 60))
    )
    depths = (0, 30, 60, 120)

    assert sorted(_rows_by_depth(negative_controls(holed, truth, FLOOR_FT, depths_min=depths))) == [0]
    funnel = control_funnel(holed, truth, FLOOR_FT, depths_min=depths, gaps_min=(60,))
    assert funnel["negative_cuts_dropped_for_depth"] == 3


def test_the_depth_tolerance_is_inclusive(spark):
    tracks, truth, _, first_2 = _pair(spark)
    limit = int(DEPTH_TOLERANCE_MIN)

    def kept(reach_min):
        holed = tracks.filter(
            ~F.col("event_time").between(_at(first_2 + 25 * 60), _at(first_2 + reach_min * 60 - 1))
        )
        return negative_controls(holed, truth, FLOOR_FT, depths_min=(30,)).count()

    assert kept(30 + limit) == 1
    assert kept(30 + limit + 1) == 0


def test_one_hole_reached_from_two_depths_is_one_control(spark):
    tracks, truth, last_1, first_2 = _pair(spark)
    holed = tracks.filter(
        ~F.col("event_time").between(_at(last_1 - 34 * 60 + 1), _at(last_1 - 29 * 60))
        & ~F.col("event_time").between(_at(first_2 + 29 * 60), _at(first_2 + 34 * 60 - 1))
    )
    rows = negative_controls(holed, truth, FLOOR_FT, depths_min=(30, 32)).collect()

    assert [r["depth_min"] for r in rows] == [30]  # the shallower request names it
    assert (rows[0]["old_depth_min"], rows[0]["new_depth_min"]) == (34, 34)


def test_interleaved_airframes_pair_only_with_themselves(spark):
    """Every other flight in time belongs to another airframe, and they never overlap."""
    f1, tf1, _, _ = _flight(0, 60, key="F1", icao24="abc123")
    g1, tg1, _, _ = _flight(2 * 3600, 60, key="G1", icao24="def456")
    f2, tf2, _, _ = _flight(4 * 3600, 60, key="F2", icao24="abc123")
    g2, tg2, _, _ = _flight(6 * 3600, 60, key="G2", icao24="def456")
    tracks = make_track(spark, f1 + g1 + f2 + g2)
    truth = _truth(spark, [tf1, tg1, tf2, tg2])

    rows = negative_controls(tracks, truth, FLOOR_FT, depths_min=(0,)).collect()

    assert {(r["pair_id"], r["icao24"]) for r in rows} == {
        ("F1|F2", "abc123"),
        ("G1|G2", "def456"),
    }
    assert {r["gap_min"] for r in rows} == {180.0}


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


@pytest.mark.parametrize(
    "step_s, rates_counted",
    [
        # Five samples inside the edge window, three of them at 10 m/s: 1,181 ft/min.
        (60, 5),
        # 20 min apart, only the edge sample is inside it, whatever five rows reach.
        (20 * 60, 1),
    ],
)
def test_controls_summarise_the_samples_inside_the_edge_window(spark, step_s, rates_counted):
    """At most five samples an edge, and none further than the window from it."""
    s1, t1, _, last_1 = _flight(0, 900, step_s=step_s, key="F1")
    s2, t2, _, _ = _flight(last_1 + 55 * 60, 900, step_s=step_s, key="F2")
    for sample in s1 + s2:
        sample["baro_altitude"] = CRUISE_M
    for sample in s2[:3]:
        sample["vert_rate"] = 10.0
    tracks = make_track(spark, s1 + s2)
    row = negative_controls(
        tracks, _truth(spark, [t1, t2]), FLOOR_FT, depths_min=(0,)
    ).collect()[0]
    at_10_mps = min(3, rates_counted)
    assert row["vr_after_samples"] == rates_counted
    assert row["vr_after_fpm"] == pytest.approx(10.0 * FPM_PER_MPS * at_10_mps / rates_counted)


def _blank_position(samples, first_s, last_s):
    for sample in samples:
        if first_s <= sample["t"] <= last_s:
            sample["lat"] = sample["lon"] = None


def _pair_with_blank_edges(spark, step_s=60, blank_s=60):
    """Two flights whose cruise edges have no position for *blank_s* either side of the cut."""
    s1, t1, _, last_1 = _flight(0, 180, key="F1", step_s=step_s)
    s2, t2, first_2, _ = _flight(
        last_1 + 55 * 60, 180, start_nm=180 * CRUISE_KT / 60.0, key="F2", step_s=step_s
    )
    _blank_position(s1, last_1 - blank_s, last_1)
    _blank_position(s2, first_2, first_2 + blank_s)
    return make_track(spark, s1 + s2), _truth(spark, [t1, t2]), last_1, first_2


def test_a_negative_control_with_blank_edges_reads_the_positions_beside_them(spark):
    tracks, truth, _, _ = _pair_with_blank_edges(spark)
    row = negative_controls(tracks, truth, FLOOR_FT, depths_min=(0,)).collect()[0]

    assert row["gap_min"] == 60.0
    # The edge and the sample before it are blank, so each position is 2 min from its edge.
    assert row["position_gap_min"] == 64.0
    # The second flight's cruise opens where the first's ends: only those 4 min are flown.
    assert row["gc_nm"] == pytest.approx(4 * CRUISE_KT / 60.0, rel=1e-3)


def test_a_control_with_blank_edges_scores_as_the_same_hole_left_in_the_data(spark):
    tracks, truth, last_1, first_2 = _pair_with_blank_edges(spark)
    control = negative_controls(tracks, truth, FLOOR_FT, depths_min=(0,)).collect()[0]

    kept = tracks.filter(
        (F.col("event_time") <= F.lit(_at(last_1))) | (F.col("event_time") >= F.lit(_at(first_2)))
    )
    hole = real_hole_candidates(kept, P).collect()[0]
    for name in link_feature_columns(P):
        assert control[name] == hole[name], name


def test_controls_keep_the_position_window_whatever_the_callsign_bounds(spark):
    """With 10 s samples, five rows are 50 s. Callsign bounds of 30 s reach no further,
    so only the position window can keep the samples 3 min from the cut."""
    p = replace(
        P,
        link_callsign_lookback_minutes=0.5, link_callsign_lookahead_minutes=0.5
    )
    tracks, truth, _, _ = _pair_with_blank_edges(spark, step_s=10, blank_s=170)
    row = negative_controls(tracks, truth, FLOOR_FT, p=p, depths_min=(0,)).collect()[0]
    assert row["position_gap_min"] == pytest.approx(60.0 + 2 * 3.0)
    assert row["gc_nm"] is not None


def test_a_positive_control_with_a_blank_edge_still_links(spark):
    samples, truth, first, last = _flight(0, 240)
    middle = (first + last) // 2
    _blank_position(samples, middle - 60 * 60, middle - 60 * 60)   # the old edge
    _blank_position(samples, middle + 60 * 60, middle + 60 * 60)   # the new edge
    row = _linked(
        positive_controls(make_track(spark, samples), _truth(spark, [truth]), FLOOR_FT, gaps_min=(120,))
    ).collect()[0]

    assert row["gap_min"] == 120.0
    assert row["position_gap_min"] == 122.0
    assert row["gc_nm"] == pytest.approx(976.0, rel=1e-3)
    assert row["_linked"] is True


def _plan(df):
    return df._jdf.queryExecution().executedPlan().toString()


def test_minutes_are_broadcast_so_no_cartesian_product_is_planned(spark):
    tracks, truth, _, _ = _pair(spark)
    assert "CartesianProduct" not in _plan(negative_controls(tracks, truth, FLOOR_FT))
    assert "CartesianProduct" not in _plan(positive_controls(tracks, truth, FLOOR_FT))


# --- positive controls ------------------------------------------------------


def test_positive_control_has_a_hole_of_the_requested_length(spark):
    samples, truth, first, last = _flight(0, 240)
    controls = positive_controls(
        make_track(spark, samples), _truth(spark, [truth]), FLOOR_FT, gaps_min=(60, 120)
    ).orderBy("gap_min").collect()

    assert [r["gap_min"] for r in controls] == [60.0, 120.0]
    assert [r["requested_gap_min"] for r in controls] == [60, 120]
    assert [r["expected"] for r in controls] == ["link", "link"]
    assert all(r["depth_min"] is None and r["pair_id"] is None for r in controls)
    assert all(r["old_depth_min"] is None and r["new_depth_min"] is None for r in controls)
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


# --- the funnel -------------------------------------------------------------


def test_the_funnel_accounts_for_every_negative_pair_and_attempt(spark):
    # abc123: a usable pair with one of each fate, then a pair that overlaps.
    s1, t1, _, last_1 = _flight(0, 180, key="F1")
    s2, t2, first_2, last_2 = _flight(last_1 + 25 * 60, 90, key="F2")
    s3, t3, _, _ = _flight(last_2, 60, key="F3")
    # def456: a pair whose second flight never reaches the floor.
    g1, tg1, _, _ = _flight(0, 60, key="G1", icao24="def456")
    g2, tg2, _, _ = _flight(4 * 3600, 60, key="G2", icao24="def456")
    for sample in g2:
        sample["baro_altitude"] = LOW_M
    tracks = make_track(spark, s1 + s2 + s3 + g1 + g2)
    truth = _truth(spark, [t1, t2, t3, tg1, tg2])
    holed = tracks.filter(
        # One hole reached from depths 30 and 32.
        ~F.col("event_time").between(_at(last_1 - 34 * 60 + 1), _at(last_1 - 29 * 60))
        & ~F.col("event_time").between(_at(first_2 + 29 * 60), _at(first_2 + 34 * 60 - 1))
        # A hole that pushes depth 60 ten minutes too far.
        & ~F.col("event_time").between(_at(last_1 - 70 * 60 + 1), _at(last_1 - 55 * 60))
    )
    depths = (0, 30, 32, 60, 120)  # base gap 30: too short; 120: past F2's 90 min cruise

    funnel = control_funnel(holed, truth, FLOOR_FT, depths_min=depths, gaps_min=(60,))

    assert funnel["negative_pairs_considered"] == 3
    assert funnel["negative_pairs_lost_to_overlap"] == 1
    assert funnel["negative_pairs_lost_to_no_cruise_sample"] == 1
    assert funnel["negative_pairs_usable"] == 1
    assert funnel["negative_attempts"] == 5
    assert funnel["negative_excluded_too_short"] == 1
    assert funnel["negative_cuts_dropped_outside_cruise"] == 1
    assert funnel["negative_cuts_dropped_for_depth"] == 1
    assert funnel["negative_repeated_holes_dropped"] == 1
    assert funnel["negative_controls"] == 1
    assert negative_controls(holed, truth, FLOOR_FT, depths_min=depths).count() == 1


def test_the_funnel_accounts_for_every_positive_attempt(spark):
    clean, t_clean, _, _ = _flight(0, 240, key="CLEAN", icao24="aaa001")
    holed, t_holed, first, last = _flight(0, 240, key="HOLED", icao24="aaa002")
    middle = (first + last) / 2
    holed = [s for s in holed if not middle - 40 * 60 < s["t"] < middle + 40 * 60]
    low, t_low, _, _ = _flight(0, 240, key="LOW", icao24="aaa003")
    for sample in low:
        sample["baro_altitude"] = LOW_M
    tracks = make_track(spark, clean + holed + low)
    truth = _truth(spark, [t_clean, t_holed, t_low])
    gaps = (20, 60, 400)

    funnel = control_funnel(tracks, truth, FLOOR_FT, depths_min=(0,), gaps_min=gaps)

    assert funnel["positive_attempts"] == 9
    assert funnel["positive_dropped_no_cruise_sample"] == 3
    assert funnel["positive_dropped_outside_cruise"] == 2   # 400 min, clean and holed
    assert funnel["positive_dropped_by_gap_tolerance"] == 2  # 20 and 60, holed
    assert funnel["positive_excluded_too_short"] == 1        # 20, clean
    assert funnel["positive_controls"] == 1                  # 60, clean
    assert positive_controls(tracks, truth, FLOOR_FT, gaps_min=gaps).count() == 1


# --- how near the edge a false join's event is ---------------------------------


@pytest.mark.parametrize(
    "off_s, near",
    [
        (1000 + 299, True),     # just inside the old edge
        (1000 + 300, True),     # five minutes is inclusive
        (1000 + 301, False),
        (5000 - 300, True),     # counted from the new edge as well
        (5000 - 301, False),
    ],
)
def test_a_false_joins_event_near_an_edge_is_marked(spark, off_s, near):
    truth = _truth(spark, [{
        "flight_key": "F1", "icao24": "abc123", "callsign": "DLH1",
        "t_off": _at(off_s), "t_land": _at(9000),
    }])
    row = label_real_holes(_holes(spark, (1000, 5000)), truth).first()
    assert row["label"] == "false_join"
    assert row["event_near_edge"] is near


def test_a_landing_near_an_edge_is_marked_too(spark):
    truth = _truth(spark, [{
        "flight_key": "F1", "icao24": "abc123", "callsign": "DLH1",
        "t_off": _at(0), "t_land": _at(1060),
    }])
    row = label_real_holes(_holes(spark, (1000, 5000)), truth).first()
    assert (row["label"], row["event_near_edge"]) == ("false_join", True)


def test_a_hole_with_no_event_inside_is_not_near_anything(spark):
    """Never NULL, and an event outside the hole does not count however close it is."""
    truth = _truth(spark, [{
        "flight_key": "F1", "icao24": "abc123", "callsign": "DLH1",
        "t_off": _at(990), "t_land": _at(5010),
    }])
    rows = label_real_holes(_holes(spark, (1000, 5000)), truth).collect()
    assert [(r["label"], r["event_near_edge"]) for r in rows] == [("true_link", False)]
    no_truth = truth.filter(F.lit(False))
    rows = label_real_holes(_holes(spark, (1000, 5000)), no_truth).collect()
    assert [(r["label"], r["event_near_edge"]) for r in rows] == [("unknown", False)]


# --- the low-edge families ----------------------------------------------------


DIRECTIONAL = replace(P, link_old_edge_climb=True, link_new_edge_descent=True)
#: Above the low-altitude gap's limit, so that rule stays out of these fixtures.
CLIMB_M = 9000.0 / FT_PER_M
CLIMB_FPM = 1500.0


def _low_edge_pair(spark, cruise_1=120, cruise_2=120, turnaround_min=30, **flight_2):
    """Two flights with a real climb and descent, the second carrying on from the first."""
    s1, t1, first_1, last_1 = _flight(
        0, cruise_1, key="F1", low_m=CLIMB_M, low_fpm=CLIMB_FPM
    )
    start_2 = last_1 + 5 * 60 + turnaround_min * 60
    s2, t2, first_2, last_2 = _flight(
        start_2, cruise_2, start_nm=cruise_1 * CRUISE_KT / 60.0, key="F2",
        low_m=CLIMB_M, low_fpm=CLIMB_FPM, **flight_2,
    )
    return (
        make_track(spark, s1 + s2), _truth(spark, [t1, t2]),
        {"first_1": first_1, "last_1": last_1, "first_2": first_2, "last_2": last_2,
         "land_1": last_1 + 5 * 60, "off_2": start_2},
    )


def _family(controls, family):
    return [r for r in controls.collect() if r["control_family"] == family]


def test_fixtures_with_no_climb_or_descent_rate_give_only_the_cruise_family(spark):
    tracks, truth, _, _ = _pair(spark)
    controls = negative_controls(tracks, truth, FLOOR_FT, p=DIRECTIONAL).collect()
    assert controls
    assert {r["control_family"] for r in controls} == {"cruise_cruise"}


def test_cruise_descent_cuts_at_the_old_cruise_and_the_new_descent(spark):
    tracks, truth, t = _low_edge_pair(spark)
    rows = _family(negative_controls(tracks, truth, FLOOR_FT, p=DIRECTIONAL), "cruise_descent")
    by_depth = {r["depth_min"]: r for r in rows}
    assert set(by_depth) == {0.0, 30.0, 60.0, 120.0}
    base, deep = by_depth[0.0], by_depth[60.0]
    assert base["t_old_edge"] == _at(t["last_1"])
    assert base["t_new_edge"] == _at(t["last_2"] + 60)
    assert deep["t_old_edge"] == _at(t["last_1"] - 3600)
    assert deep["old_depth_min"] == 60.0
    assert deep["new_depth_min"] is None
    assert base["alt_before_ft"] >= FLOOR_FT > base["alt_after_ft"]
    assert base["vr_after_fpm"] == pytest.approx(-CLIMB_FPM)


def test_climb_cruise_cuts_at_the_old_climb_and_the_new_cruise(spark):
    tracks, truth, t = _low_edge_pair(spark)
    rows = _family(negative_controls(tracks, truth, FLOOR_FT, p=DIRECTIONAL), "climb_cruise")
    by_depth = {r["depth_min"]: r for r in rows}
    assert set(by_depth) == {0.0, 30.0, 60.0, 120.0}
    base, deep = by_depth[0.0], by_depth[60.0]
    assert base["t_old_edge"] == _at(t["first_1"] - 60)
    assert base["t_new_edge"] == _at(t["first_2"])
    assert deep["t_new_edge"] == _at(t["first_2"] + 3600)
    assert deep["new_depth_min"] == 60.0
    assert deep["old_depth_min"] is None
    assert base["alt_after_ft"] >= FLOOR_FT > base["alt_before_ft"]
    assert base["vr_before_fpm"] == pytest.approx(CLIMB_FPM)


def test_climb_descent_is_one_control_a_pair_with_no_depth(spark):
    tracks, truth, t = _low_edge_pair(spark)
    rows = _family(negative_controls(tracks, truth, FLOOR_FT, p=DIRECTIONAL), "climb_descent")
    assert len(rows) == 1
    row = rows[0]
    assert row["t_old_edge"] == _at(t["first_1"] - 60)
    assert row["t_new_edge"] == _at(t["last_2"] + 60)
    assert row["depth_min"] is None
    assert row["old_depth_min"] is None and row["new_depth_min"] is None
    assert row["pair_id"] == "F1|F2"
    assert row["gap_stratum"] == "240-360"
    assert row["same_callsign"] is True
    assert row["expected"] == "split"


@pytest.mark.parametrize("family", ["cruise_descent", "climb_cruise", "climb_descent"])
def test_each_family_hides_a_landing_and_only_direction_can_link_it(spark, family):
    """The landing and the next take-off lie inside, and the speed test cannot see them.

    Without direction these edges fail the floor, so they test nothing. With it the
    rule links across a stop, which is the false join these families exist to count.
    """
    tracks, truth, t = _low_edge_pair(spark)
    controls = negative_controls(tracks, truth, FLOOR_FT, p=DIRECTIONAL)
    controls = controls.filter(F.col("control_family") == family)
    # The deepest cut reads the same whichever side moved, so read the base and deep.
    rows = _linked(controls, DIRECTIONAL).collect()
    assert rows
    for row in rows:
        assert row["t_old_edge"] < _at(t["land_1"]) < _at(t["off_2"]) < row["t_new_edge"]
    assert all(row["_linked"] for row in rows)
    assert not any(r["_linked"] for r in _linked(controls, P).collect())


def test_the_low_edge_is_the_last_climbing_sample_not_merely_the_last_below_the_floor(spark):
    """A level-off under the floor says nothing about direction, so the cut moves back."""
    s1, t1, first_1, _ = _flight(0, 120, key="F1", low_m=CLIMB_M, low_fpm=CLIMB_FPM)
    for sample in s1:
        if sample["t"] >= first_1 - 120 and sample["t"] < first_1:
            sample["vert_rate"] = 500.0 / FPM_PER_MPS   # the threshold is strict
    s2, t2, _, _ = _flight(s1[-1]["t"] + 1800, 120, key="F2", low_m=CLIMB_M, low_fpm=CLIMB_FPM)
    rows = _family(
        negative_controls(make_track(spark, s1 + s2), _truth(spark, [t1, t2]), FLOOR_FT, p=DIRECTIONAL),
        "climb_descent",
    )
    assert rows[0]["t_old_edge"] == _at(first_1 - 180)


def test_a_null_vertical_rate_is_no_edge(spark):
    s1, t1, _, _ = _flight(0, 120, key="F1", low_m=CLIMB_M, low_fpm=CLIMB_FPM)
    for sample in s1:
        sample["vert_rate"] = None
    s2, t2, _, _ = _flight(s1[-1]["t"] + 1800, 120, key="F2", low_m=CLIMB_M, low_fpm=CLIMB_FPM)
    tracks, truth = make_track(spark, s1 + s2), _truth(spark, [t1, t2])
    controls = negative_controls(tracks, truth, FLOOR_FT, p=DIRECTIONAL)
    assert not _family(controls, "climb_descent")
    assert not _family(controls, "climb_cruise")
    assert _family(controls, "cruise_descent")


def test_a_low_edge_control_of_30_min_or_less_is_excluded(spark):
    tracks, truth, _ = _low_edge_pair(spark, cruise_1=5, cruise_2=5, turnaround_min=2)
    controls = negative_controls(tracks, truth, FLOOR_FT, depths_min=(0,), p=DIRECTIONAL)
    assert controls.count() == 0
    funnel = control_funnel(tracks, truth, FLOOR_FT, depths_min=(0,), gaps_min=(60,), p=DIRECTIONAL)
    assert funnel["climb_descent_excluded_too_short"] == 1
    assert funnel["climb_descent_controls"] == 0


def test_the_funnel_accounts_for_every_family_and_keeps_the_cruise_counts_apart(spark):
    tracks, truth, _ = _low_edge_pair(spark)
    funnel = control_funnel(tracks, truth, FLOOR_FT, gaps_min=(60,), p=DIRECTIONAL)
    assert funnel["negative_attempts"] == 4     # cruise_cruise, one per depth
    for family, attempts in (("cruise_descent", 4), ("climb_cruise", 4), ("climb_descent", 1)):
        assert funnel[f"{family}_attempts"] == attempts
        assert funnel[f"{family}_attempts"] == sum(
            funnel[f"{family}_{fate}"]
            for fate in (
                "dropped_no_low_edge", "dropped_outside_cruise", "dropped_for_depth",
                "repeated_holes_dropped", "excluded_too_short", "controls",
            )
        )
    assert funnel["climb_descent_controls"] == 1
    controls = negative_controls(tracks, truth, FLOOR_FT, p=DIRECTIONAL)
    counts = {
        r["control_family"]: r["count"] for r in controls.groupBy("control_family").count().collect()
    }
    assert counts["cruise_descent"] == funnel["cruise_descent_controls"]
    assert counts["climb_cruise"] == funnel["climb_cruise_controls"]
    assert counts["cruise_cruise"] == funnel["negative_controls"]


def test_a_flight_with_no_low_edge_is_counted_as_such(spark):
    s1, t1, _, _ = _flight(0, 120, key="F1")     # level at every sample
    s2, t2, _, _ = _flight(s1[-1]["t"] + 1800, 120, key="F2", low_m=CLIMB_M, low_fpm=CLIMB_FPM)
    tracks, truth = make_track(spark, s1 + s2), _truth(spark, [t1, t2])
    funnel = control_funnel(tracks, truth, FLOOR_FT, depths_min=(0,), gaps_min=(60,), p=DIRECTIONAL)
    assert funnel["climb_descent_dropped_no_low_edge"] == 1
    assert funnel["climb_cruise_dropped_no_low_edge"] == 1
    assert funnel["cruise_descent_controls"] == 1


def test_a_low_edge_positive_control_deletes_the_whole_cruise_and_links_only_by_direction(spark):
    samples, truth_row, first, last = _flight(0, 180, low_m=CLIMB_M, low_fpm=CLIMB_FPM)
    tracks, truth = make_track(spark, samples), _truth(spark, [truth_row])
    controls = positive_controls(tracks, truth, FLOOR_FT, gaps_min=(60,), p=DIRECTIONAL)
    rows = _family(controls, "low_edge")
    assert len(rows) == 1
    assert rows[0]["t_old_edge"] == _at(first - 60)
    assert rows[0]["t_new_edge"] == _at(last + 60)
    assert rows[0]["requested_gap_min"] is None
    assert rows[0]["expected"] == "link"
    assert {r["control_family"] for r in controls.collect()} == {"cruise_cruise", "low_edge"}

    low_edge = controls.filter(F.col("control_family") == "low_edge")
    assert _linked(low_edge, DIRECTIONAL).first()["_linked"] is True
    assert _linked(low_edge, P).first()["_linked"] is False


def test_the_funnel_counts_the_low_edge_positive_controls(spark):
    climbing, t_climbing, _, _ = _flight(
        0, 180, key="GOOD", icao24="aaa001", low_m=CLIMB_M, low_fpm=CLIMB_FPM
    )
    level, t_level, _, _ = _flight(0, 180, key="LEVEL", icao24="aaa002")
    short, t_short, _, _ = _flight(
        0, 10, key="SHORT", icao24="aaa003", low_m=CLIMB_M, low_fpm=CLIMB_FPM
    )
    never, t_never, _, _ = _flight(0, 180, key="NEVER", icao24="aaa004")
    for sample in never:
        sample["baro_altitude"] = LOW_M
    tracks = make_track(spark, climbing + level + short + never)
    truth = _truth(spark, [t_climbing, t_level, t_short, t_never])
    funnel = control_funnel(tracks, truth, FLOOR_FT, depths_min=(0,), gaps_min=(60,), p=DIRECTIONAL)
    assert funnel["positive_low_edge_attempts"] == 4
    assert funnel["positive_low_edge_dropped_no_cruise_sample"] == 1
    assert funnel["positive_low_edge_dropped_no_low_edge"] == 1
    assert funnel["positive_low_edge_excluded_too_short"] == 1
    assert funnel["positive_low_edge_controls"] == 1


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
