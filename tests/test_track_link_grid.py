"""The Stage 1 grid: counting from link decisions, the safety limits, and the pick.

Spark tests use the fixture style of ``test_track_link_controls.py``: flights on the
equator at 480 kt, so the implied speed over any hole is known exactly. The safety
limit tests hand ``with_verdicts`` plain counts, which is all the verdicts read.
"""
import csv
import datetime as dt
import json
import math

import provenance
import pytest
from conftest import _EPOCH, make_track
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)
from track_fragmentation import measure
from track_link_controls import real_hole_candidates, rule_of_three_upper
from track_link_grid import (
    COUNT_FIELDS,
    SAME_CALLSIGN_MIN_CONTROLS,
    flight_base_tracks,
    flight_track_counts,
    grid_from_json,
    grid_points,
    holes_inside_flights,
    lowest_bound_point,
    negative_counts,
    pick_recommended,
    pool_rows,
    pool_strata,
    positive_counts,
    real_hole_counts,
    recommendation,
    run_study,
    strata_counts,
    truth_months,
    with_verdicts,
)

from opdi.pipeline.flights import EARTH_R_NM
from opdi.pipeline.segmentation import SegmentationParams, assign_track_id
from opdi.pipeline.segmentation.base import FT_PER_M, KT_PER_MPS
from opdi.pipeline.segmentation.methods import cruise_linked

P = SegmentationParams()
CRUISE_M = 11000.0
LOW_M = 5000.0 / FT_PER_M
CRUISE_KT = 480.0

_FEATURE_SCHEMA = [
    StructField("icao24", StringType(), True),
    StructField("gap_min", DoubleType(), True),
    StructField("gc_nm", DoubleType(), True),
    StructField("alt_before_ft", DoubleType(), True),
    StructField("alt_after_ft", DoubleType(), True),
    StructField("vr_before_fpm", DoubleType(), True),
    StructField("vr_after_fpm", DoubleType(), True),
    StructField("spd_before_kt", DoubleType(), True),
    StructField("cs_before", StringType(), True),
    StructField("cs_after", StringType(), True),
]


def _at(seconds):
    return _EPOCH + dt.timedelta(seconds=seconds)


def _lon(nm):
    return math.degrees(nm / EARTH_R_NM)


def _sample(t, nm, callsign="DLH1", icao24="abc123", altitude=CRUISE_M):
    return {
        "t": t,
        "icao24": icao24,
        "callsign": callsign,
        "lat": 0.0,
        "lon": _lon(nm),
        "baro_altitude": altitude,
        "velocity": CRUISE_KT / KT_PER_MPS,
        "vert_rate": 0.0,
    }


def _leg(start_s, minutes, start_nm=0.0, callsign="DLH1", icao24="abc123", altitude=CRUISE_M):
    """One sample a minute of straight flight at 480 kt."""
    return [
        _sample(start_s + m * 60, start_nm + m * CRUISE_KT / 60.0, callsign, icao24, altitude)
        for m in range(minutes + 1)
    ]


def _truth(spark, *flights):
    """``(key, icao24, t_off_s, t_land_s)`` as ``load_flight_intervals`` would have them."""
    rows = [
        {"flight_key": k, "icao24": a, "callsign": "X", "t_off": _at(t0), "t_land": _at(t1)}
        for k, a, t0, t1 in flights
    ]
    return spark.createDataFrame(rows)


def _feature_row(gap_min=120.0, speed_kt=480.0, cs_before="DLH1", cs_after="DLH1", spd_before_kt=480.0):
    """A hole's features, linkable at the defaults unless the arguments say otherwise."""
    return (
        "abc123", gap_min, speed_kt * gap_min / 60.0, 36000.0, 36000.0, 0.0, 0.0,
        spd_before_kt, cs_before, cs_after,
    )


def _negatives(spark, rows):
    """Controls from ``(pair_id, feature_row)``. ``same_callsign`` as the generator sets it."""
    schema = StructType(
        [StructField("pair_id", StringType(), True), StructField("depth_min", DoubleType(), True)]
        + _FEATURE_SCHEMA
    )
    frame = spark.createDataFrame([(pair, 0.0, *features) for pair, features in rows], schema)
    return frame.withColumn(
        "same_callsign", ~F.coalesce(F.col("cs_before") != F.col("cs_after"), F.lit(False))
    ).withColumn(
        "gap_stratum",
        F.when(F.col("gap_min") <= 120, "30-120")
        .when(F.col("gap_min") <= 240, "120-240")
        .when(F.col("gap_min") <= 360, "240-360")
        .otherwise("over-360"),
    )


def _positives(spark, rows):
    """Controls from ``(requested_gap_min, feature_row)``."""
    schema = StructType(
        [StructField("requested_gap_min", DoubleType(), True), StructField("same_callsign", BooleanType(), True)]
        + _FEATURE_SCHEMA
    )
    frame = spark.createDataFrame([(g, True, *features) for g, features in rows], schema)
    return frame.withColumn("gap_stratum", F.lit("30-120"))


def _real_holes(spark, rows):
    """Labelled holes from ``(label, feature_row)``."""
    schema = StructType([StructField("label", StringType(), True)] + _FEATURE_SCHEMA)
    return spark.createDataFrame([(label, *features) for label, features in rows], schema)


def _counts(point_id=0, **overrides):
    """A row of raw counts that passes every limit, unless overridden."""
    counts = {name: 0 for name in COUNT_FIELDS}
    counts.update(
        day="pooled",
        point_id=point_id,
        flights=100,
        tracks_on_flights=110,
        flights_in_one_track=90,
        real_holes=500,
        real_holes_linked=200,
        linked_true_link=200,
        negative_controls=1000,
        same_callsign_controls=500,
        positive_controls=100,
        positive_within_gap=100,
        positive_linked=95,
    )
    counts.update(overrides)
    if "negative_within_gap" not in overrides:
        counts["negative_within_gap"] = counts["negative_controls"]
    return with_verdicts(counts)


# --- the grid ---------------------------------------------------------------


def test_the_grid_has_54_points():
    grid = grid_points()
    assert len(grid) == 54
    assert len(set(grid)) == 54
    assert {p.link_min_implied_speed_kt for p in grid} == {250.0, 315.0, 400.0}
    assert {p.link_min_speed_ratio for p in grid} == {0.0, 0.85}
    assert {p.link_min_stop_minutes for p in grid} == {0.0, 20.0, 35.0}
    assert {p.link_max_gap_minutes for p in grid} == {120.0, 240.0, 360.0}


def test_grid_points_leave_every_other_setting_at_its_default():
    for p in grid_points():
        assert p.link_cruise_floor_ft == P.link_cruise_floor_ft
        assert p.link_max_implied_speed_kt == P.link_max_implied_speed_kt
        assert p.callsign_min_persistence_seconds == P.callsign_min_persistence_seconds


def test_grid_json_replaces_the_grid():
    grid = grid_from_json(json.dumps({
        "link_min_implied_speed_kt": [300, 350],
        "link_max_implied_speed_kt": [700, 750, 800],
    }))
    assert len(grid) == 6
    assert {p.link_max_implied_speed_kt for p in grid} == {700.0, 750.0, 800.0}
    assert all(p.link_min_stop_minutes == P.link_min_stop_minutes for p in grid)


def test_grid_json_holds_one_value_of_a_setting_the_features_depend_on():
    grid = grid_from_json('{"link_cruise_floor_ft": [30000], "link_min_speed_ratio": [0, 0.9]}')
    assert {p.link_cruise_floor_ft for p in grid} == {30000.0}


@pytest.mark.parametrize(
    "text",
    [
        '{"link_min_implid_speed_kt": [300]}',  # a typo must not silently run the defaults
        '{"gap_minutes": [20]}',  # not a link setting: moves which holes exist
        '{"link_min_speed_ratio": []}',
        '{"link_min_speed_ratio": 0.5}',
        '{"link_min_speed_ratio": ["high"]}',
        "{}",
        # Cuts and features are made once per run, so these cannot vary inside one.
        '{"link_cruise_floor_ft": [25000, 30000]}',
        '{"link_callsign_lookahead_minutes": [5, 10]}',
        '{"link_callsign_lookback_minutes": [30, 60]}',
    ],
)
def test_grid_json_refuses_what_it_cannot_honour(text):
    with pytest.raises(ValueError):
        grid_from_json(text)


def test_truth_months_reach_the_neighbouring_month_at_an_edge():
    assert truth_months("2026-06-14") == ["202606"]
    assert truth_months("2026-06-01") == ["202605", "202606"]
    assert truth_months("2026-06-30") == ["202606", "202607"]


# --- counting flights from link decisions -----------------------------------


def _study_day(spark):
    """Five flights, each testing one way a hole or a boundary can fall.

    * F1: one hole that links at the defaults, 191 min at 480 kt.
    * F2: one hole far too slow to link.
    * F3: no hole, one track.
    * F4: a genuine callsign change, which no link can remove.
    * F5: no sample at all.
    """
    day = 3 * 3600
    f1 = _leg(0, 30) + _leg(30 * 60 + 191 * 60, 30, start_nm=240 + 191 * 8)
    f2 = _leg(0, 30, icao24="bbb222") + _leg(30 * 60 + 100 * 60, 30, start_nm=240 + 50, icao24="bbb222")
    f3 = _leg(0, 30, icao24="ccc333")
    f4 = _leg(0, 30, icao24="ddd444") + _leg(31 * 60, 30, start_nm=248, callsign="BAW2", icao24="ddd444")
    truth = _truth(
        spark,
        ("F1", "abc123", -60, 30 * 60 + 191 * 60 + 30 * 60 + 60),
        ("F2", "bbb222", -60, 30 * 60 + 100 * 60 + 30 * 60 + 60),
        ("F3", "ccc333", -60, day),
        ("F4", "ddd444", -60, day),
        ("F5", "eee555", -60, day),
    )
    return make_track(spark, f1 + f2 + f3 + f4), truth


def _flight_counts(spark, tracks, truth, p=P):
    flights = flight_base_tracks(tracks, truth, p)
    holes = real_hole_candidates(tracks, p)
    return flight_track_counts(flights, holes_inside_flights(holes, truth), p)


def _flight_counts_by_re_segmenting(tracks, truth, p):
    """What the shortcut stands in for: A10 really run, per truth flight."""
    tracked = assign_track_id(tracks, cruise_linked(), p).alias("s")
    per_flight = (
        truth.alias("g")
        .join(
            tracked,
            (F.col("g.icao24") == F.col("s.icao24"))
            & (F.col("s.event_time") >= F.col("g.t_off"))
            & (F.col("s.event_time") <= F.col("g.t_land")),
            "left",
        )
        .groupBy("g.flight_key")
        .agg(F.countDistinct("s.track_id").alias("n"))
        .collect()
    )
    return {
        "flights": len(per_flight),
        "tracks_on_flights": sum(r["n"] for r in per_flight),
        "flights_in_one_track": sum(r["n"] == 1 for r in per_flight),
        "flights_with_no_track": sum(r["n"] == 0 for r in per_flight),
    }


def test_a_flight_with_no_track_stays_in_the_denominator(spark):
    counts = _flight_counts(spark, *_study_day(spark))
    assert counts["flights"] == 5
    assert counts["flights_with_no_track"] == 1
    # F1 linked (1), F2 split (2), F3 (1), F4 split (2), F5 (0).
    assert counts["tracks_on_flights"] == 6
    assert counts["flights_in_one_track"] == 2

    row = _counts(flights=5, tracks_on_flights=6, flights_in_one_track=2)
    assert row["tracks_per_flight"] == pytest.approx(6 / 5)
    assert row["share_one_track"] == pytest.approx(2 / 5)


@pytest.mark.parametrize(
    "p",
    [
        P,  # F1 links
        SegmentationParams(link_max_gap_minutes=120.0),  # F1's hole is too long
        SegmentationParams(link_min_implied_speed_kt=500.0),  # F1 is too slow
    ],
)
def test_counting_from_decisions_agrees_with_really_re_segmenting(spark, p):
    tracks, truth = _study_day(spark)
    assert _flight_counts(spark, tracks, truth, p) == _flight_counts_by_re_segmenting(tracks, truth, p)


def test_a_hole_with_an_edge_outside_the_flight_does_not_count_for_it(spark):
    """The flight's samples all sit on one side, so there is nothing for it to join."""
    tracks = make_track(spark, _leg(0, 30) + _leg(30 * 60 + 120 * 60, 30, start_nm=240 + 960))
    truth = _truth(spark, ("F1", "abc123", -60, 30 * 60 + 60))
    counts = _flight_counts(spark, tracks, truth)
    assert counts["tracks_on_flights"] == 1
    assert counts == _flight_counts_by_re_segmenting(tracks, truth, P)


# --- real holes --------------------------------------------------------------


def test_the_false_join_rate_counts_only_holes_the_point_links(spark):
    holes = _real_holes(
        spark,
        [
            ("true_link", _feature_row()),
            ("true_link", _feature_row()),
            ("false_join", _feature_row()),
            ("unknown", _feature_row()),
            # A landing inside, but this point splits it, which is correct.
            ("false_join", _feature_row(speed_kt=100.0)),
            ("false_join", _feature_row(speed_kt=100.0)),
        ],
    )
    counts = real_hole_counts(holes, P)
    assert counts == {
        "real_holes": 6,
        "real_holes_linked": 4,
        "linked_true_link": 2,
        "linked_false_join": 1,
        "linked_unknown": 1,
    }
    row = _counts(**counts)
    assert row["real_false_join_rate"] == pytest.approx(1 / 3)  # known labels only
    assert row["linked_unknown_share"] == pytest.approx(1 / 4)


# --- negative controls -------------------------------------------------------


def test_negative_counts_report_controls_pairs_and_blank_callsigns(spark):
    controls = _negatives(
        spark,
        [
            # One pair falsely joined at two depths: two controls, one pair.
            ("A", _feature_row()),
            ("A", _feature_row(gap_min=100.0)),
            # Linked with a blank on one side.
            ("B", _feature_row(cs_before=None)),
            # Rejected by the callsign test, so not in the same or blank group.
            ("C", _feature_row(cs_after="BAW2")),
            # Rejected by speed, same callsign.
            ("D", _feature_row(speed_kt=100.0)),
        ],
    )
    counts = negative_counts(controls, P)
    assert counts["negative_controls"] == 5
    assert counts["negative_pairs"] == 4
    assert counts["negative_false_joins"] == 3
    assert counts["negative_pairs_with_false_join"] == 2
    assert counts["same_callsign_controls"] == 4
    assert counts["same_callsign_false_joins"] == 3
    assert counts["same_callsign_blank"] == 1

    row = _counts(**counts)
    assert row["negative_false_join_upper_95"] == pytest.approx(rule_of_three_upper(3, 5))
    assert row["same_callsign_blank_share"] == pytest.approx(1 / 4)


def test_a_stratum_counts_only_gaps_the_point_allows(spark):
    controls = _negatives(
        spark,
        [("A", _feature_row(gap_min=100.0)), ("B", _feature_row(gap_min=200.0)), ("C", _feature_row(gap_min=300.0))],
    )
    p = SegmentationParams(link_max_gap_minutes=120.0)
    counts = negative_counts(controls, p)
    # Over the point's limit they are correct rejections: in n overall, not in the stratum.
    assert counts["negative_controls"] == 3
    assert counts["negative_within_gap"] == 1
    assert counts["negative_false_joins"] == 1
    assert counts["same_callsign_controls"] == 1
    assert counts["same_callsign_false_joins"] == 1


def test_strata_show_correct_rejections_apart(spark):
    controls = _negatives(
        spark, [("A", _feature_row(gap_min=100.0)), ("B", _feature_row(gap_min=200.0))]
    )
    p = SegmentationParams(link_max_gap_minutes=120.0)
    rows = {r["gap_stratum"]: r for r in strata_counts(controls, "negative", p, point_id=7)}
    assert rows["30-120"]["within_link_max_gap"] is True
    assert (rows["30-120"]["n"], rows["30-120"]["wrong"]) == (1, 1)
    assert rows["120-240"]["within_link_max_gap"] is False
    assert (rows["120-240"]["n"], rows["120-240"]["wrong"]) == (1, 0)
    assert rows["120-240"]["point_id"] == 7


def test_a_positive_control_that_does_not_link_is_the_wrong_decision(spark):
    controls = _positives(
        spark,
        [(60.0, _feature_row(gap_min=60.0)), (60.0, _feature_row(gap_min=60.0, speed_kt=100.0))],
    )
    rows = strata_counts(controls, "positive", P, point_id=0)
    assert [(r["requested_gap_min"], r["n"], r["wrong"]) for r in rows] == [(60.0, 2, 1)]


def test_positive_counts_separate_what_the_point_allows(spark):
    controls = _positives(
        spark,
        [
            (60.0, _feature_row(gap_min=60.0)),
            (60.0, _feature_row(gap_min=60.0, speed_kt=100.0)),
            (180.0, _feature_row(gap_min=180.0)),
        ],
    )
    counts = positive_counts(controls, SegmentationParams(link_max_gap_minutes=120.0))
    assert counts == {"positive_controls": 3, "positive_within_gap": 2, "positive_linked": 1}
    assert _counts(**counts)["positive_missed_share"] == pytest.approx(1 / 2)


# --- the safety limits -------------------------------------------------------


def test_a_passing_row_is_within_limits():
    row = _counts()
    assert row["within_limits"] is True
    assert row["provisional"] is False


def test_a_point_over_the_negative_limit_is_rejected():
    # Zero of 250 bounds the rate at 1.19%, over 1% with no false join seen at all.
    row = _counts(negative_controls=250, same_callsign_controls=100, negative_false_joins=0)
    assert row["negative_false_join_upper_95"] == pytest.approx(0.0119, abs=0.0001)
    assert row["negative_within_limit"] is False
    assert row["within_limits"] is False


def test_a_point_just_under_the_negative_limit_passes():
    row = _counts(negative_controls=300, same_callsign_controls=100)
    assert row["negative_false_join_upper_95"] < 0.01
    assert row["within_limits"] is True


def test_a_same_callsign_stratum_over_5_percent_is_rejected():
    # 1 of 60 has an upper bound near 7.7%.
    row = _counts(
        negative_controls=100000, negative_false_joins=1,
        same_callsign_controls=60, same_callsign_false_joins=1,
    )
    assert row["negative_within_limit"] is True
    assert row["same_callsign_status"] == "over_limit"
    assert row["within_limits"] is False
    assert row["provisional"] is False


def test_a_same_callsign_stratum_under_60_is_insufficient_not_failed():
    row = _counts(same_callsign_controls=SAME_CALLSIGN_MIN_CONTROLS - 1)
    # An explicit count: the exact bound at 59 is 4.95%, which alone would read as passing.
    assert rule_of_three_upper(0, 59) < 0.05
    assert row["same_callsign_status"] == "insufficient"
    assert row["within_limits"] is True
    assert row["provisional"] is True


def test_an_insufficient_stratum_does_not_fail_even_with_joins_in_it():
    row = _counts(
        negative_controls=100000, negative_false_joins=5,
        same_callsign_controls=10, same_callsign_false_joins=5,
    )
    assert row["same_callsign_status"] == "insufficient"
    assert row["within_limits"] is True


def test_sixty_controls_are_enough_to_read_the_stratum():
    row = _counts(same_callsign_controls=60)
    assert row["same_callsign_status"] == "within_limit"
    assert row["provisional"] is False


def test_real_hole_false_joins_over_1_percent_are_rejected():
    row = _counts(linked_true_link=98, linked_false_join=2, real_holes_linked=100)
    assert row["real_false_join_rate"] == pytest.approx(0.02)
    assert row["real_holes_within_limit"] is False
    assert row["within_limits"] is False


def test_real_hole_false_joins_at_exactly_1_percent_pass():
    row = _counts(linked_true_link=99, linked_false_join=1, real_holes_linked=100)
    assert row["real_holes_within_limit"] is True


def test_unknown_holes_do_not_dilute_the_real_hole_rate():
    row = _counts(linked_true_link=98, linked_false_join=2, linked_unknown=10000, real_holes_linked=10100)
    assert row["real_false_join_rate"] == pytest.approx(0.02)
    assert row["within_limits"] is False


def test_no_evidence_is_not_within_limits():
    """A point cannot be shown safe by having nothing to measure it on."""
    no_controls = _counts(negative_controls=0, same_callsign_controls=0)
    assert no_controls["negative_false_join_upper_95"] is None
    assert no_controls["within_limits"] is False

    no_known_holes = _counts(linked_true_link=0, linked_unknown=50)
    assert no_known_holes["real_false_join_rate"] is None
    assert no_known_holes["within_limits"] is False


# --- the pick ----------------------------------------------------------------


def test_the_most_flights_in_one_track_wins_among_passing_points():
    rows = [
        # The lower bound, but fewer flights: the bound is a limit, not the objective.
        _counts(point_id=0, flights_in_one_track=80),
        _counts(point_id=1, flights_in_one_track=90, negative_false_joins=1),
        # The most of all, but over the limit, so it cannot be chosen.
        _counts(point_id=2, flights_in_one_track=99, linked_true_link=90, linked_false_join=10),
    ]
    assert pick_recommended(rows)["point_id"] == 1


def test_a_tie_goes_to_the_lower_bound():
    rows = [
        _counts(point_id=0, negative_false_joins=2),
        _counts(point_id=1, negative_false_joins=1),
    ]
    assert pick_recommended(rows)["point_id"] == 1


def test_no_passing_point_returns_none_and_names_the_lowest_bound():
    over = dict(linked_true_link=90, linked_false_join=10)
    rows = [
        _counts(point_id=0, negative_false_joins=5, **over),
        _counts(point_id=1, negative_false_joins=1, **over),
        _counts(point_id=2, negative_false_joins=3, **over),
    ]
    assert pick_recommended(rows) is None
    assert lowest_bound_point(rows)["point_id"] == 1
    choice = recommendation(rows)
    assert choice["status"] == "not_recommended_lowest_bound"
    assert choice["point_id"] == 1


def test_recommendation_says_when_it_is_provisional():
    assert recommendation([_counts()])["status"] == "recommended"
    assert (
        recommendation([_counts(same_callsign_controls=10)])["status"]
        == "recommended_provisional"
    )


def test_recommendation_with_no_controls_anywhere_chooses_nothing():
    choice = recommendation([_counts(negative_controls=0, same_callsign_controls=0)])
    assert choice == {"status": "no_evidence"}


# --- pooling -----------------------------------------------------------------


def test_pooling_recomputes_the_bound_from_summed_counts():
    day_1 = [_counts(day="2026-06-14", negative_controls=200, same_callsign_controls=100)]
    day_2 = [_counts(day="2025-06-14", negative_controls=200, same_callsign_controls=100)]
    # Neither day alone can reach the limit at zero joins; together they can.
    assert day_1[0]["within_limits"] is False
    assert day_2[0]["within_limits"] is False

    pooled = pool_rows([day_1, day_2])
    assert len(pooled) == 1
    assert pooled[0]["day"] == "pooled"
    assert pooled[0]["negative_controls"] == 400
    assert pooled[0]["negative_false_join_upper_95"] == pytest.approx(rule_of_three_upper(0, 400))
    assert pooled[0]["within_limits"] is True
    assert pooled[0]["share_one_track"] == pytest.approx(180 / 200)


def test_pooling_keeps_points_apart():
    pooled = pool_rows([[_counts(point_id=0), _counts(point_id=1)], [_counts(point_id=0), _counts(point_id=1)]])
    assert [r["point_id"] for r in pooled] == [0, 1]


def test_pooling_strata_sums_within_each_stratum():
    def stratum(n, wrong, gap_stratum="30-120"):
        return {
            "day": "x", "point_id": 0, "kind": "negative", "gap_stratum": gap_stratum,
            "depth_min": 0.0, "requested_gap_min": None, "same_callsign": True,
            "within_link_max_gap": True, "n": n, "wrong": wrong,
            "wrong_share": None, "wrong_upper_95": None,
        }

    pooled = pool_strata([[stratum(100, 1), stratum(5, 0, "120-240")], [stratum(50, 2)]])
    by_stratum = {r["gap_stratum"]: r for r in pooled}
    assert (by_stratum["30-120"]["n"], by_stratum["30-120"]["wrong"]) == (150, 3)
    assert by_stratum["30-120"]["wrong_upper_95"] == pytest.approx(rule_of_three_upper(3, 150))
    assert by_stratum["120-240"]["n"] == 5


# --- track_fragmentation.measure at a given point ----------------------------


def test_measure_reads_the_parameters_it_is_given(spark):
    tracks = make_track(spark, _leg(0, 30) + _leg(30 * 60 + 120 * 60, 30, start_nm=240 + 960))
    assert measure(tracks, "cruise_linked", 30.0)["tracks"] == 1
    tighter = SegmentationParams(link_max_gap_minutes=60.0)
    assert measure(tracks, "cruise_linked", 30.0, params=tighter)["tracks"] == 2


# --- the whole study on a small day ------------------------------------------


def _read_csv(path):
    with path.open() as fh:
        return list(csv.DictReader(fh))


def test_run_study_writes_every_output_with_provenance(spark, tmp_path):
    """Two identical fixture days, a two-point grid. Nothing reaches S3."""
    tracks, truth = _study_day(spark)
    # Two flights of one airframe, so there are negative controls: the landing sits in
    # the hour between them, and at 100 kt nothing links across it.
    pair = _leg(0, 90, icao24="fff666") + _leg(150 * 60, 90, start_nm=720 + 100, icao24="fff666")
    tracks = tracks.unionByName(make_track(spark, pair))
    truth = truth.unionByName(
        _truth(spark, ("F6", "fff666", -60, 90 * 60 + 60), ("F7", "fff666", 150 * 60 - 60, 240 * 60 + 60))
    )
    grid = [SegmentationParams(link_max_gap_minutes=120.0), P]

    run_study(
        ["2026-06-14", "2025-06-14"], grid,
        load_tracks=lambda day: tracks, load_truth=lambda day: truth,
        results_dir=tmp_path, argv=["--days", "x"],
    )

    expected = [
        "link_grid_2026-06-14.csv", "link_grid_2025-06-14.csv", "link_grid_pooled.csv",
        "link_controls_2026-06-14.csv", "link_controls_2025-06-14.csv", "link_controls_pooled.csv",
        "link_funnel_2026-06-14.csv", "link_funnel_2025-06-14.csv", "link_recommended.csv",
    ]
    manifest = json.loads((tmp_path / "_manifest.json").read_text())
    for name in expected:
        assert (tmp_path / name).is_file(), name
        assert manifest[name]["script"] == "benchmarks/track_link_grid.py"

    daily = _read_csv(tmp_path / "link_grid_2026-06-14.csv")
    pooled = _read_csv(tmp_path / "link_grid_pooled.csv")
    assert [r["link_max_gap_minutes"] for r in daily] == ["120.0", "360.0"]
    # F3, F6 and F7 always; F1's 191 min hole links only at the longer limit.
    assert [int(r["flights_in_one_track"]) for r in daily] == [3, 4]
    assert [int(r["flights_in_one_track"]) for r in pooled] == [6, 8]
    assert [int(r["flights"]) for r in pooled] == [14, 14]
    assert int(pooled[0]["negative_controls"]) == 2 * int(daily[0]["negative_controls"]) > 0

    funnel = _read_csv(tmp_path / "link_funnel_2026-06-14.csv")[0]
    assert funnel["day"] == "2026-06-14"
    assert "negative_pairs_considered" in funnel
    assert int(funnel["positive_controls"]) == int(funnel["positive_controls_scored"])
    assert int(funnel["negative_controls"]) == int(funnel["negative_controls_scored"])

    # Cut 60 min deep, the hole is 180 min over 1,060 NM. That is 353 kt, so the longer
    # limit links across the landing: the very case the deep cuts exist to expose.
    assert [int(r["negative_false_joins"]) for r in pooled] == [0, 2]
    assert [int(r["negative_pairs_with_false_join"]) for r in pooled] == [0, 2]

    # Too few controls to bound anything under 1%, so nothing passes, and the point
    # with the lowest bound is named and measured.
    choice = _read_csv(tmp_path / "link_recommended.csv")[0]
    assert choice["status"] == "not_recommended_lowest_bound"
    assert choice["point_id"] == "0"
    assert choice["tracks_2plus_persisted_callsigns"] == "0"
    assert choice["debounced_tracks_2plus_persisted_callsigns"] == "0"
    assert manifest["link_recommended.csv"]["sha256_16"] == provenance.file_hash(
        tmp_path / "link_recommended.csv"
    )
