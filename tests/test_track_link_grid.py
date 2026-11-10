"""The Stage 1 grid: counting from link decisions, the safety limits, and the pick.

Spark tests use the fixture style of ``test_track_link_controls.py``: flights on the
equator at 480 kt, so the implied speed over any hole is known exactly. The safety
limit tests hand ``with_verdicts`` plain counts, which is all the verdicts read.
"""
import csv
import datetime as dt
from dataclasses import replace
from functools import reduce
import json
import math
import re

import adsc_flight_list_truth
import pandas as pd
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
from track_link_controls import (
    control_funnel,
    flight_samples,
    label_real_holes,
    negative_controls,
    positive_controls,
    real_hole_candidates,
    rule_of_three_upper,
)
from track_link_grid import (
    COUNT_FIELDS,
    ONE_VALUE_PER_RUN,
    REAL_FALSE_JOIN_LIMIT,
    SAME_CALLSIGN_MIN_CONTROLS,
    flight_base_tracks,
    flight_track_counts,
    grid_from_json,
    GRID_BASE,
    grid_points,
    holes_inside_flights,
    lowest_bound_point,
    negative_counts,
    negative_reason_counts,
    pick_recommended,
    pool_counts,
    pool_rows,
    pool_strata,
    population_track_counts,
    positive_counts,
    positive_reason_counts,
    read_tracks,
    real_hole_counts,
    reason_counts,
    reasons_params_from_json,
    recommendation,
    run_study,
    strata_counts,
    true_link_holes,
    truth_months,
    with_population,
    with_verdicts,
)

from opdi.pipeline.flights import EARTH_R_NM
from opdi.pipeline.segmentation import SegmentationParams, assign_track_id
from opdi.pipeline.segmentation.base import FPM_PER_MPS, FT_PER_M, KT_PER_MPS
from opdi.pipeline.segmentation.methods import cruise_linked, debounced

#: These tests were written at the pre-study settings (the grid's base), so they pass
#: them explicitly; the shipped defaults are pinned in test_segmentation_cruise_linked.
P = SegmentationParams(
    link_cruise_floor_ft=25000.0,
    link_min_implied_speed_kt=315.0,
    link_max_gap_minutes=360.0,
    link_old_edge_climb=False,
    link_new_edge_descent=False,
)
CRUISE_M = 11000.0
LOW_M = 5000.0 / FT_PER_M
CRUISE_KT = 480.0

_FEATURE_SCHEMA = [
    StructField("icao24", StringType(), True),
    StructField("gap_min", DoubleType(), True),
    StructField("position_gap_min", DoubleType(), True),
    StructField("gc_nm", DoubleType(), True),
    StructField("alt_before_ft", DoubleType(), True),
    StructField("alt_after_ft", DoubleType(), True),
    StructField("vr_before_fpm", DoubleType(), True),
    StructField("vr_after_fpm", DoubleType(), True),
    StructField("spd_before_kt", DoubleType(), True),
    StructField("cs_before", StringType(), True),
    StructField("cs_after", StringType(), True),
    StructField("alt_before_read_past_blank", BooleanType(), True),
    StructField("alt_after_read_past_blank", BooleanType(), True),
    StructField("vr_before_samples", DoubleType(), True),
    StructField("vr_after_samples", DoubleType(), True),
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
    """``(key, icao24, t_off_s, t_land_s[, crosses_box])``, as ``main`` loads them."""
    rows = [
        {
            "flight_key": k,
            "icao24": a,
            "callsign": "X",
            "t_off": _at(t0),
            "t_land": _at(t1),
            "crosses_box": bool(crosses and crosses[0]),
        }
        for k, a, t0, t1, *crosses in flights
    ]
    return spark.createDataFrame(rows)


def _feature_row(
    gap_min=120.0, speed_kt=480.0, cs_before="DLH1", cs_after="DLH1", spd_before_kt=480.0,
    alt_before_ft=36000.0, alt_after_ft=36000.0, vr_before_fpm=0.0, vr_after_fpm=0.0,
    position_gap_min=None, alt_before_read_past_blank=False, alt_after_read_past_blank=False,
):
    """A hole's features, linkable at the defaults unless the arguments say otherwise."""
    position_gap_min = gap_min if position_gap_min is None else position_gap_min
    return (
        "abc123", gap_min, position_gap_min, speed_kt * position_gap_min / 60.0,
        alt_before_ft, alt_after_ft, vr_before_fpm, vr_after_fpm,
        spd_before_kt, cs_before, cs_after,
        alt_before_read_past_blank, alt_after_read_past_blank, 5.0, 5.0,
    )


def _negatives(spark, rows, family="cruise_cruise"):
    """Controls from ``(pair_id, feature_row[, old_depth, new_depth])``, 60 min deep by default.

    ``same_callsign`` is as the generator sets it.
    """
    schema = StructType(
        [
            StructField("pair_id", StringType(), True),
            StructField("depth_min", DoubleType(), True),
            StructField("old_depth_min", DoubleType(), True),
            StructField("new_depth_min", DoubleType(), True),
        ]
        + _FEATURE_SCHEMA
    )
    frame = spark.createDataFrame(
        [
            (pair, 0.0, *(depths or (60.0, 60.0)), *features)
            for pair, features, *depths in rows
        ],
        schema,
    )
    return frame.withColumn("control_family", F.lit(family)).withColumn(
        "same_callsign", ~F.coalesce(F.col("cs_before") != F.col("cs_after"), F.lit(False))
    ).withColumn(
        "gap_stratum",
        F.when(F.col("gap_min") <= 120, "30-120")
        .when(F.col("gap_min") <= 240, "120-240")
        .when(F.col("gap_min") <= 360, "240-360")
        .otherwise("over-360"),
    )


def _positives(spark, rows, family="cruise_cruise"):
    """Controls from ``(requested_gap_min, feature_row)``."""
    schema = StructType(
        [StructField("requested_gap_min", DoubleType(), True), StructField("same_callsign", BooleanType(), True)]
        + _FEATURE_SCHEMA
    )
    frame = spark.createDataFrame([(g, True, *features) for g, features in rows], schema)
    return frame.withColumn("gap_stratum", F.lit("30-120")).withColumn("control_family", F.lit(family))


def _real_holes(spark, rows):
    """Labelled holes from ``(label, feature_row)``."""
    schema = StructType([StructField("label", StringType(), True)] + _FEATURE_SCHEMA)
    frame = spark.createDataFrame([(label, *features) for label, features in rows], schema)
    return frame.withColumn("event_near_edge", F.lit(False))


#: Each low-edge family clean, on enough controls to bound under 1% alone.
_FAMILIES_CLEAN = dict(
    negative_low_edge_controls=1000,
    negative_climb_cruise_controls=400,
    negative_cruise_descent_controls=300,
    negative_climb_descent_controls=300,
)


def _counts(point_id=0, **overrides):
    """A row of raw counts that passes every limit, unless overridden."""
    counts = {name: 0 for name in COUNT_FIELDS}
    counts.update(
        day="pooled",
        point_id=point_id,
        link_min_implied_speed_kt=315.0,
        link_max_gap_minutes=360.0,
        link_min_speed_ratio=0.0,
        link_min_stop_minutes=0.0,
        link_old_edge_climb=False,
        link_new_edge_descent=False,
        flights=100,
        tracks_on_flights=110,
        flights_in_one_track=90,
        real_holes=500,
        real_holes_linked=200,
        linked_true_link=200,
        negative_limit_controls=1000,
        same_callsign_controls=500,
        positive_controls=100,
        positive_within_gap=100,
        positive_linked=95,
    )
    counts.update(overrides)
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
        assert p.link_cruise_floor_ft == GRID_BASE["link_cruise_floor_ft"]
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
        '{"link_edge_position_minutes": [5, 10]}',
        # Where the low-edge controls are cut depends on what counts as climbing.
        '{"link_edge_vert_rate_fpm": [300, 500]}',
        # A number is not a switch, and a switch is not a number.
        '{"link_old_edge_climb": [1]}',
        '{"link_new_edge_descent": []}',
        '{"link_min_speed_ratio": [true]}',
    ],
)
def test_grid_json_refuses_what_it_cannot_honour(text):
    with pytest.raises(ValueError):
        grid_from_json(text)


def test_grid_json_varies_direction_as_a_true_or_false():
    grid = grid_from_json('{"link_old_edge_climb": [false, true], "link_max_gap_minutes": [120]}')
    assert [p.link_old_edge_climb for p in grid] == [False, True]
    assert all(type(p.link_old_edge_climb) is bool for p in grid)


def test_the_vertical_rate_threshold_is_one_value_per_run():
    assert "link_edge_vert_rate_fpm" in ONE_VALUE_PER_RUN
    assert "link_old_edge_climb" not in ONE_VALUE_PER_RUN
    assert "link_new_edge_descent" not in ONE_VALUE_PER_RUN


def test_reasons_params_json_reads_direction():
    point = reasons_params_from_json('{"link_old_edge_climb": true}', [P])
    assert point.link_old_edge_climb is True


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


def _a10_counts(counts):
    """The counts a real A10 run can be compared on, without the A9 baseline."""
    return {k: v for k, v in counts.items() if not k.startswith("base_")}


def _flight_counts_by_re_segmenting(tracks, truth, p, rule=None):
    """What the shortcut stands in for: A10 really run, per truth flight."""
    tracked = assign_track_id(tracks, rule or cruise_linked(), p).alias("s")
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
        replace(P, link_max_gap_minutes=120.0),  # F1's hole is too long
        replace(P, link_min_implied_speed_kt=500.0),  # F1 is too slow
    ],
)
def test_counting_from_decisions_agrees_with_really_re_segmenting(spark, p):
    tracks, truth = _study_day(spark)
    assert _a10_counts(_flight_counts(spark, tracks, truth, p)) == _flight_counts_by_re_segmenting(tracks, truth, p)


def test_two_holes_in_one_flight_are_decided_one_by_one(spark):
    """A hole that links and a hole that does not: three A9 tracks become two."""
    first = _leg(0, 30)
    linkable_hole = _leg(30 * 60 + 120 * 60, 30, start_nm=240 + 960)
    too_slow_hole = _leg(180 * 60 + 100 * 60, 30, start_nm=240 + 960 + 240 + 50)
    tracks = make_track(spark, first + linkable_hole + too_slow_hole)
    truth = _truth(spark, ("F1", "abc123", -60, 310 * 60 + 60))

    counts = _flight_counts(spark, tracks, truth)
    assert counts["base_tracks_on_flights"] == 3
    assert counts["tracks_on_flights"] == 2
    assert _a10_counts(counts) == _flight_counts_by_re_segmenting(tracks, truth, P)


def test_the_base_counts_are_what_a9_scores(spark):
    tracks, truth = _study_day(spark)
    counts = _flight_counts(spark, tracks, truth)
    a9 = _flight_counts_by_re_segmenting(tracks, truth, P, rule=debounced())
    assert counts["base_tracks_on_flights"] == a9["tracks_on_flights"]
    assert counts["base_flights_in_one_track"] == a9["flights_in_one_track"]
    # F1 and F2 split, F3 whole, F4 split, F5 unseen, and F1's link is the gain.
    assert (counts["base_tracks_on_flights"], counts["base_flights_in_one_track"]) == (7, 1)
    assert (counts["tracks_on_flights"], counts["flights_in_one_track"]) == (6, 2)

    row = _counts(flights=5, base_tracks_on_flights=7, base_flights_in_one_track=1)
    assert row["base_tracks_per_flight"] == pytest.approx(7 / 5)
    assert row["base_share_one_track"] == pytest.approx(1 / 5)


def test_a_hole_with_an_edge_outside_the_flight_does_not_count_for_it(spark):
    """The flight's samples all sit on one side, so there is nothing for it to join."""
    tracks = make_track(spark, _leg(0, 30) + _leg(30 * 60 + 120 * 60, 30, start_nm=240 + 960))
    truth = _truth(spark, ("F1", "abc123", -60, 30 * 60 + 60))
    counts = _flight_counts(spark, tracks, truth)
    assert counts["tracks_on_flights"] == 1
    assert _a10_counts(counts) == _flight_counts_by_re_segmenting(tracks, truth, P)


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
        "real_holes_edge_read_through_blank": 0,
        "real_holes_event_outside_box": 0,
        "linked_event_outside_box": 0,
        "linked_true_link": 2,
        "linked_false_join": 1,
        "linked_unknown": 1,
        "linked_edge_read_through_blank": 0,
        "false_joined_edge_read_through_blank": 0,
        "linked_by_direction": 0,
        "false_joined_by_direction": 0,
        "linked_by_direction_labelled": 0,
        "linked_by_climb": 0,
        "false_joined_by_climb": 0,
        "linked_by_climb_labelled": 0,
        "linked_by_descent": 0,
        "false_joined_by_descent": 0,
        "linked_by_descent_labelled": 0,
        "linked_altitude_read_past_blank_old": 0,
        "false_joined_altitude_read_past_blank_old": 0,
        "linked_altitude_read_past_blank_new": 0,
        "false_joined_altitude_read_past_blank_new": 0,
        "false_joined_near_edge": 0,
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
    assert row["negative_limit_pairs_with_false_join"] == 2
    assert row["same_callsign_blank_share"] == pytest.approx(1 / 4)


def test_a_stratum_counts_only_gaps_the_point_allows(spark):
    controls = _negatives(
        spark,
        [("A", _feature_row(gap_min=100.0)), ("B", _feature_row(gap_min=200.0)), ("C", _feature_row(gap_min=300.0))],
    )
    p = replace(P, link_max_gap_minutes=120.0)
    counts = negative_counts(controls, p)
    # Over the point's limit they cannot link, so they test nothing here.
    assert counts["negative_controls"] == 3
    assert counts["negative_over_gap_controls"] == 2
    assert counts["negative_limit_controls"] == 1
    assert counts["negative_false_joins"] == 1
    assert counts["same_callsign_controls"] == 1
    assert counts["same_callsign_false_joins"] == 1


def test_the_limit_counts_only_allowed_gaps_at_a_realised_depth_of_30_min(spark):
    slow = _feature_row(speed_kt=100.0)
    controls = _negatives(
        spark,
        [
            # Under the limit: deep on both sides, on one side, and exactly at 30.
            ("A", _feature_row(), 60.0, 60.0),
            ("B", slow, 40.0, 2.0),
            ("C", slow, 1.0, 30.0),
            # Too shallow, one of them falsely joined.
            ("D", _feature_row(), 29.9, 29.9),
            ("E", slow, 0.0, 0.0),
            # Deep, but over this point's gap: it cannot link, so it tests nothing.
            ("F", _feature_row(gap_min=300.0), 60.0, 60.0),
        ],
    )
    counts = negative_counts(controls, replace(P, link_max_gap_minutes=240.0))

    assert counts["negative_limit_controls"] == 3
    assert counts["negative_limit_false_joins"] == 1
    assert counts["negative_limit_pairs_with_false_join"] == 1
    assert counts["negative_shallow_controls"] == 2
    assert counts["negative_shallow_false_joins"] == 1
    assert counts["negative_over_gap_controls"] == 1
    assert counts["negative_controls"] == 6
    assert counts["negative_false_joins"] == 2
    assert counts["negative_pairs_with_false_join"] == 2

    row = _counts(**counts)
    assert row["negative_false_join_upper_95"] == pytest.approx(rule_of_three_upper(1, 3))
    assert row["negative_false_join_rate"] == pytest.approx(1 / 3)


def test_shallow_and_over_gap_controls_cannot_lift_a_point_over_or_under_the_limit():
    # 250 under the limit bounds at 1.19%; thousands of easy passes must not rescue it.
    diluted = _counts(
        negative_limit_controls=250, negative_controls=10250,
        negative_shallow_controls=5000, negative_over_gap_controls=5000,
    )
    assert diluted["negative_within_limit"] is False
    # And a shallow false join is reported, but is not in this bound.
    shallow_join = _counts(
        negative_limit_controls=1000, negative_shallow_controls=100,
        negative_shallow_false_joins=50, negative_false_joins=50,
    )
    assert shallow_join["negative_false_join_upper_95"] == pytest.approx(rule_of_three_upper(0, 1000))
    assert shallow_join["negative_within_limit"] is True


def test_the_same_callsign_group_is_the_population_of_the_1_percent_limit(spark):
    controls = _negatives(
        spark,
        [
            # Under the limit: one falsely joined, one a blank, one rejected by speed.
            ("A", _feature_row(), 60.0, 60.0),
            ("B", _feature_row(cs_before=None, speed_kt=100.0), 1.0, 30.0),
            ("C", _feature_row(speed_kt=100.0), 40.0, 40.0),
            # Under the limit, but the callsign test can reject it.
            ("D", _feature_row(cs_after="BAW2"), 60.0, 60.0),
            # Same callsign, too shallow: counted apart, one falsely joined.
            ("E", _feature_row(), 0.0, 2.0),
            ("F", _feature_row(speed_kt=100.0), 29.9, 29.9),
            # Same callsign and deep, but over this point's gap.
            ("G", _feature_row(gap_min=300.0), 60.0, 60.0),
        ],
    )
    counts = negative_counts(controls, replace(P, link_max_gap_minutes=240.0))

    assert counts["same_callsign_controls"] == 3
    assert counts["same_callsign_false_joins"] == 1
    assert counts["same_callsign_blank"] == 1
    assert counts["same_callsign_shallow_controls"] == 2
    assert counts["same_callsign_shallow_false_joins"] == 1

    row = _counts(**counts)
    assert row["same_callsign_upper_95"] == pytest.approx(rule_of_three_upper(1, 3))
    assert row["same_callsign_blank_share"] == pytest.approx(1 / 3)


def test_shallow_controls_cannot_make_the_same_callsign_group_sufficient_or_fail_it():
    # 59 under the limit stays insufficient, however many shallow ones sit beside it.
    thin = _counts(same_callsign_controls=59, same_callsign_shallow_controls=5000)
    assert thin["same_callsign_status"] == "insufficient"
    assert thin["provisional"] is True
    # A shallow false join is reported, and is not in this bound.
    joined = _counts(
        same_callsign_controls=100, same_callsign_shallow_controls=100,
        same_callsign_shallow_false_joins=50,
    )
    assert joined["same_callsign_upper_95"] == pytest.approx(rule_of_three_upper(0, 100))
    assert joined["same_callsign_status"] == "within_limit"


def test_strata_show_correct_rejections_apart(spark):
    controls = _negatives(
        spark, [("A", _feature_row(gap_min=100.0)), ("B", _feature_row(gap_min=200.0))]
    )
    p = replace(P, link_max_gap_minutes=120.0)
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
    counts = positive_counts(controls, replace(P, link_max_gap_minutes=120.0))
    assert counts == {
        "positive_controls": 3, "positive_within_gap": 2, "positive_linked": 1,
        "positive_low_edge_controls": 0, "positive_low_edge_within_gap": 0,
        "positive_low_edge_linked": 0,
    }
    assert _counts(**counts)["positive_missed_share"] == pytest.approx(1 / 2)


# --- the safety limits -------------------------------------------------------


def test_a_passing_row_is_within_limits():
    row = _counts()
    assert row["within_limits"] is True
    assert row["provisional"] is False


def test_a_point_over_the_negative_limit_is_rejected():
    # Zero of 250 bounds the rate at 1.19%, over 1% with no false join seen at all.
    row = _counts(negative_limit_controls=250, same_callsign_controls=100)
    assert row["negative_false_join_upper_95"] == pytest.approx(0.0119, abs=0.0001)
    assert row["negative_within_limit"] is False
    assert row["within_limits"] is False


def test_a_point_just_under_the_negative_limit_passes():
    row = _counts(negative_limit_controls=300, same_callsign_controls=100)
    assert row["negative_false_join_upper_95"] < 0.01
    assert row["within_limits"] is True


def test_a_same_callsign_stratum_over_5_percent_is_rejected():
    # 1 of 60 has an upper bound near 7.7%.
    row = _counts(
        negative_limit_controls=100000, negative_limit_false_joins=1,
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
        negative_limit_controls=100000, negative_limit_false_joins=5,
        same_callsign_controls=10, same_callsign_false_joins=5,
    )
    assert row["same_callsign_status"] == "insufficient"
    assert row["within_limits"] is True


def test_sixty_controls_are_enough_to_read_the_stratum():
    row = _counts(same_callsign_controls=60)
    assert row["same_callsign_status"] == "within_limit"
    assert row["provisional"] is False


def test_the_real_hole_limit_is_a_tenth_of_a_percent():
    assert REAL_FALSE_JOIN_LIMIT == 0.001


def test_real_hole_false_joins_over_the_limit_are_rejected():
    """Two in a thousand passed the old 1% and is over the limit now."""
    row = _counts(linked_true_link=998, linked_false_join=2, real_holes_linked=1000)
    assert row["real_false_join_rate"] == pytest.approx(0.002)
    assert row["real_holes_within_limit"] is False
    assert row["within_limits"] is False


def test_real_hole_false_joins_at_exactly_the_limit_pass():
    row = _counts(linked_true_link=999, linked_false_join=1, real_holes_linked=1000)
    assert row["real_holes_within_limit"] is True


def test_unknown_holes_do_not_dilute_the_real_hole_rate():
    row = _counts(linked_true_link=98, linked_false_join=2, linked_unknown=10000, real_holes_linked=10100)
    assert row["real_false_join_rate"] == pytest.approx(0.02)
    assert row["within_limits"] is False


def test_no_evidence_is_not_within_limits():
    """A point cannot be shown safe by having nothing to measure it on."""
    no_controls = _counts(negative_limit_controls=0, same_callsign_controls=0)
    assert no_controls["negative_false_join_upper_95"] is None
    assert no_controls["within_limits"] is False

    no_known_holes = _counts(linked_true_link=0, linked_unknown=50)
    assert no_known_holes["real_false_join_rate"] is None
    assert no_known_holes["within_limits"] is False


# --- the pick ----------------------------------------------------------------


def test_the_most_crossing_flights_in_one_track_wins_among_passing_points():
    rows = [
        # The most flights overall, and the lowest bound, but fewer of the flights the
        # rule is for: neither of those is the objective.
        _counts(point_id=0, crossing_flights_in_one_track=70, flights_in_one_track=99),
        _counts(
            point_id=1, crossing_flights_in_one_track=80, flights_in_one_track=90,
            negative_limit_false_joins=1,
        ),
        # The most of all, but over the limit, so it cannot be chosen.
        _counts(
            point_id=2, crossing_flights_in_one_track=90,
            linked_true_link=90, linked_false_join=10,
        ),
    ]
    assert pick_recommended(rows)["point_id"] == 1


def test_a_tie_on_crossing_flights_goes_to_the_most_flights_overall():
    rows = [
        # More conservative on both settings, which only counts after this.
        _counts(
            point_id=0, crossing_flights_in_one_track=80, flights_in_one_track=85,
            link_min_implied_speed_kt=400.0, link_max_gap_minutes=120.0,
        ),
        _counts(point_id=1, crossing_flights_in_one_track=80, flights_in_one_track=90),
    ]
    assert pick_recommended(rows)["point_id"] == 1


def test_a_tie_on_flights_goes_to_the_higher_speed_floor():
    rows = [
        # The shorter gap too, which only counts after the speed floor.
        _counts(point_id=0, link_min_implied_speed_kt=250.0, link_max_gap_minutes=120.0),
        _counts(point_id=1, link_min_implied_speed_kt=315.0, link_max_gap_minutes=360.0),
    ]
    assert pick_recommended(rows)["point_id"] == 1


def test_a_tie_on_the_speed_floor_goes_to_the_shorter_gap():
    rows = [
        _counts(point_id=0, link_max_gap_minutes=360.0),
        _counts(point_id=1, link_max_gap_minutes=240.0),
    ]
    assert pick_recommended(rows)["point_id"] == 1


def test_a_tie_on_the_gap_goes_to_the_higher_speed_ratio():
    """More checks on. The first point is off on this one and strictest on the next,
    which only counts after this."""
    rows = [
        _counts(point_id=0, link_min_speed_ratio=0.0, link_min_stop_minutes=35.0),
        _counts(point_id=1, link_min_speed_ratio=0.85, link_min_stop_minutes=0.0),
    ]
    assert pick_recommended(rows)["point_id"] == 1


def test_a_tie_on_the_ratio_goes_to_the_higher_stop_minutes():
    rows = [
        _counts(point_id=0, link_min_stop_minutes=0.0),
        _counts(point_id=1, link_min_stop_minutes=20.0),
        _counts(point_id=2, link_min_stop_minutes=35.0),
    ]
    assert pick_recommended(rows)["point_id"] == 2


def test_the_gap_outranks_the_checks_that_follow_it():
    rows = [
        _counts(
            point_id=0, link_max_gap_minutes=360.0,
            link_min_speed_ratio=0.85, link_min_stop_minutes=35.0,
        ),
        _counts(point_id=1, link_max_gap_minutes=240.0),
    ]
    assert pick_recommended(rows)["point_id"] == 1


def test_a_tie_on_every_other_setting_goes_to_direction_off():
    """The more conservative. It falls after the settings above, not before them."""
    on = dict(link_old_edge_climb=True, link_new_edge_descent=True, **_FAMILIES_CLEAN)
    rows = [
        _counts(point_id=0, **on),
        _counts(point_id=1, link_old_edge_climb=False, link_new_edge_descent=False),
    ]
    assert pick_recommended(rows)["point_id"] == 1
    rows = [
        _counts(point_id=0, link_min_implied_speed_kt=400.0, **on),
        _counts(point_id=1, link_old_edge_climb=False, link_new_edge_descent=False),
    ]
    assert pick_recommended(rows)["point_id"] == 0
    rows = [
        _counts(point_id=0, link_min_stop_minutes=35.0, **on),
        _counts(point_id=1, link_old_edge_climb=False, link_new_edge_descent=False),
    ]
    assert pick_recommended(rows)["point_id"] == 0


def test_a_tie_goes_to_fewer_flags_on():
    """The count decides, so one flag on beats both whichever flag it is."""
    def flags(point_id, climb, descent):
        return _counts(
            point_id=point_id, link_old_edge_climb=climb, link_new_edge_descent=descent,
            **_FAMILIES_CLEAN,
        )

    assert pick_recommended([flags(0, True, True), flags(1, True, False)])["point_id"] == 1
    assert pick_recommended([flags(0, True, True), flags(1, False, True)])["point_id"] == 1
    assert pick_recommended([flags(0, True, False), flags(1, False, False)])["point_id"] == 1
    # One on each way has nothing left to choose them by but the first point.
    assert pick_recommended([flags(0, True, False), flags(1, False, True)])["point_id"] == 0
    assert pick_recommended([flags(0, False, True), flags(1, True, False)])["point_id"] == 0


def test_an_exact_tie_goes_to_the_first_point():
    """The bound is no longer a tie-break: it is a limit, and the pick stays deterministic."""
    rows = [
        _counts(point_id=1, negative_limit_false_joins=1),
        _counts(point_id=0, negative_limit_false_joins=2),
    ]
    assert pick_recommended(rows)["point_id"] == 0


def test_no_passing_point_returns_none_and_names_the_lowest_bound():
    over = dict(linked_true_link=90, linked_false_join=10)
    rows = [
        _counts(point_id=0, negative_limit_false_joins=5, **over),
        _counts(point_id=1, negative_limit_false_joins=1, **over),
        _counts(point_id=2, negative_limit_false_joins=3, **over),
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
    choice = recommendation([_counts(negative_limit_controls=0, same_callsign_controls=0)])
    assert choice == {"status": "no_evidence"}


# --- pooling -----------------------------------------------------------------


def test_pooling_recomputes_the_bound_from_summed_counts():
    day_1 = [_counts(day="2026-06-14", negative_limit_controls=200, same_callsign_controls=100)]
    day_2 = [_counts(day="2025-06-14", negative_limit_controls=200, same_callsign_controls=100)]
    # Neither day alone can reach the limit at zero joins; together they can.
    assert day_1[0]["within_limits"] is False
    assert day_2[0]["within_limits"] is False

    pooled = pool_rows([day_1, day_2])
    assert len(pooled) == 1
    assert pooled[0]["day"] == "pooled"
    assert pooled[0]["negative_limit_controls"] == 400
    assert pooled[0]["negative_false_join_upper_95"] == pytest.approx(rule_of_three_upper(0, 400))
    assert pooled[0]["within_limits"] is True
    assert pooled[0]["share_one_track"] == pytest.approx(180 / 200)


def test_pooling_keeps_points_apart():
    pooled = pool_rows([[_counts(point_id=0), _counts(point_id=1)], [_counts(point_id=0), _counts(point_id=1)]])
    assert [r["point_id"] for r in pooled] == [0, 1]


def test_pooling_strata_sums_within_each_stratum():
    def stratum(n, wrong, gap_stratum="30-120"):
        return {
            "day": "x", "point_id": 0, "kind": "negative", "control_family": "cruise_cruise",
            "counts_toward_limit": True,
            "gap_stratum": gap_stratum,
            "depth_min": 0.0, "requested_gap_min": None, "same_callsign": True,
            "within_link_max_gap": True, "n": n, "wrong": wrong,
            "wrong_share": None, "wrong_upper_95": None,
        }

    pooled = pool_strata([[stratum(100, 1), stratum(5, 0, "120-240")], [stratum(50, 2)]])
    by_stratum = {r["gap_stratum"]: r for r in pooled}
    assert (by_stratum["30-120"]["n"], by_stratum["30-120"]["wrong"]) == (150, 3)
    assert by_stratum["30-120"]["wrong_upper_95"] == pytest.approx(rule_of_three_upper(3, 150))
    assert by_stratum["120-240"]["n"] == 5


# --- the job's reads ---------------------------------------------------------


def test_read_tracks_reads_one_partition_of_a_partitioned_table(spark, tmp_path):
    """A table laid out as ``osn_tracks`` is, read by the job's own function."""
    source = str(tmp_path / "osn_tracks")
    (
        make_track(spark, [_sample(0, 0.0, icao24="ABC123"), _sample(60, 8.0, icao24="ABC123")])
        .withColumn("dof", F.lit("2026-06-14").cast("date"))
        .unionByName(
            make_track(spark, [_sample(0, 0.0, icao24="other1")]).withColumn(
                "dof", F.lit("2026-06-15").cast("date")
            )
        )
        .write.partitionBy("dof")
        .parquet(source)
    )

    frame = read_tracks(spark, source, "2026-06-14")

    assert {r["icao24"] for r in frame.select("icao24").collect()} == {"abc123"}  # lower-cased
    assert frame.count() == 2
    # Pruned rather than filtered, so a day costs a day and not the whole table.
    # `inputFiles()` cannot show this: it lists the relation's files before pruning.
    assert "PartitionFilters: [isnotnull(dof" in frame._jdf.queryExecution().executedPlan().toString()
    # The prefix provenance records for this day.
    assert (tmp_path / "osn_tracks" / "dof=2026-06-14").is_dir()
    assert read_tracks(spark, source, "2026-06-16").count() == 0


def test_controls_read_cached_samples_and_not_the_tracks(spark):
    """Handed ``samples``, no generator touches ``tracks``: it is the whole saving."""
    pair = _leg(0, 90) + _leg(150 * 60, 90, start_nm=820)
    tracks = make_track(spark, pair)
    truth = _truth(spark, ("F6", "abc123", -60, 90 * 60 + 60), ("F7", "abc123", 150 * 60 - 60, 240 * 60 + 60))
    samples = flight_samples(tracks, truth)

    def rows(frame):
        return sorted(tuple(r) for r in frame.select("control_id", "gap_min", "gc_nm").collect())

    floor = P.link_cruise_floor_ft
    negative = negative_controls(None, truth, floor, samples=samples)
    assert rows(negative) and rows(negative) == rows(negative_controls(tracks, truth, floor))
    positive = positive_controls(None, truth, floor, samples=samples)
    assert rows(positive) and rows(positive) == rows(positive_controls(tracks, truth, floor))
    assert control_funnel(None, truth, floor, samples=samples) == control_funnel(tracks, truth, floor)


# --- track_fragmentation.measure at a given point ----------------------------


def test_measure_reads_the_parameters_it_is_given(spark):
    tracks = make_track(spark, _leg(0, 30) + _leg(30 * 60 + 120 * 60, 30, start_nm=240 + 960))
    assert measure(tracks, "cruise_linked", 30.0)["tracks"] == 1
    tighter = replace(P, link_max_gap_minutes=60.0)
    assert measure(tracks, "cruise_linked", 30.0, params=tighter)["tracks"] == 2


# --- the whole study on a small day ------------------------------------------


def _read_csv(path):
    with path.open() as fh:
        return list(csv.DictReader(fh))


def _study_day_with_a_pair(spark):
    """Two flights of one airframe, so there are negative controls: the landing sits in
    the hour between them, and at 100 kt nothing links across it."""
    tracks, truth = _study_day(spark)
    pair = _leg(0, 90, icao24="fff666") + _leg(150 * 60, 90, start_nm=720 + 100, icao24="fff666")
    tracks = tracks.unionByName(make_track(spark, pair))
    truth = truth.unionByName(
        _truth(spark, ("F6", "fff666", -60, 90 * 60 + 60), ("F7", "fff666", 150 * 60 - 60, 240 * 60 + 60))
    )
    return tracks, truth


def test_run_study_writes_every_output_with_provenance(spark, tmp_path, capsys):
    """Two identical fixture days, a two-point grid. Nothing reaches S3."""
    tracks, truth = _study_day_with_a_pair(spark)
    grid = [replace(P, link_max_gap_minutes=120.0), P]

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
    # What the current rule scores is in the same row: F1 and F2 split, F4 split, F5 unseen.
    assert [int(r["base_flights_in_one_track"]) for r in daily] == [3, 3]
    assert float(pooled[0]["base_share_one_track"]) == pytest.approx(6 / 14)
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
    # The 180 min control is over the shorter limit, so it is not under that point's bound.
    assert int(pooled[0]["negative_over_gap_controls"]) > 0
    assert int(pooled[0]["negative_limit_controls"]) < int(pooled[1]["negative_limit_controls"])
    assert int(pooled[0]["negative_shallow_controls"]) == int(pooled[1]["negative_shallow_controls"]) > 0

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

    # Each stage of a real day reports its time, so the first run shows where it goes.
    out = capsys.readouterr().out
    for stage in (
        "real holes", "samples inside flights", "negative controls", "positive controls",
        "control funnel", "2026-06-14: grid of 2 points", "2025-06-14: two-callsign counts",
    ):
        assert re.search(rf"^{stage}: \d+ s$", out, re.MULTILINE), stage


# --- round 4: edges below the floor -------------------------------------------


DIRECTIONAL = replace(P, link_old_edge_climb=True, link_new_edge_descent=True)
_CLIMB = dict(alt_before_ft=20000.0, vr_before_fpm=1500.0)
_DESCENT = dict(alt_after_ft=20000.0, vr_after_fpm=-1500.0)
_LOW_DEPTHS = {
    "cruise_descent": (60.0, None),
    "climb_cruise": (None, 60.0),
    "climb_descent": (None, None),
}


def _low_edge_frame(spark, family, features, n=1):
    rows = [(f"P{i}", _feature_row(**features), *_LOW_DEPTHS[family]) for i in range(n)]
    return _negatives(spark, rows, family=family)


_CLIMB_ON = replace(P, link_old_edge_climb=True)
_DESCENT_ON = replace(P, link_new_edge_descent=True)
#: What each family's edges look like, so each links wherever its edges are allowed.
_FAMILY_EDGES = {
    "cruise_descent": _DESCENT,
    "climb_cruise": _CLIMB,
    "climb_descent": {**_CLIMB, **_DESCENT},
}


def _one_of_each_low_edge_family(spark):
    return reduce(
        lambda a, b: a.unionByName(b),
        [_low_edge_frame(spark, family, edges) for family, edges in _FAMILY_EDGES.items()],
    )


def test_climb_on_counts_only_climb_cruise(spark):
    counts = negative_counts(_one_of_each_low_edge_family(spark), _CLIMB_ON)
    assert counts["negative_low_edge_controls"] == 1
    assert counts["negative_low_edge_false_joins"] == 1
    assert counts["negative_limit_controls"] == 1
    assert counts["negative_low_edge_untested_controls"] == 2


def test_descent_on_counts_only_cruise_descent(spark):
    counts = negative_counts(_one_of_each_low_edge_family(spark), _DESCENT_ON)
    assert counts["negative_low_edge_controls"] == 1
    assert counts["negative_low_edge_false_joins"] == 1
    assert counts["negative_limit_controls"] == 1
    assert counts["negative_low_edge_untested_controls"] == 2


def test_both_on_counts_all_three_families(spark):
    counts = negative_counts(_one_of_each_low_edge_family(spark), DIRECTIONAL)
    assert counts["negative_low_edge_controls"] == 3
    assert counts["negative_low_edge_false_joins"] == 3
    assert counts["negative_low_edge_untested_controls"] == 0


def test_a_family_a_setting_cannot_link_is_kept_out_of_its_strata(spark):
    """The strata and the negative reasons must say what the limit read."""
    controls = _one_of_each_low_edge_family(spark)
    strata = strata_counts(controls, "negative", _CLIMB_ON, 0)
    assert {s["control_family"] for s in strata if s["counts_toward_limit"]} == {"climb_cruise"}
    assert {s["control_family"] for s in strata if not s["counts_toward_limit"]} == {
        "cruise_descent", "climb_descent",
    }


def test_a_bad_climb_family_fails_a_climb_setting_only(spark):
    """400 clean descent controls alone bound under 1%, and five bad climbs do not."""
    bad_climb = _low_edge_frame(spark, "climb_cruise", _CLIMB, n=5)
    clean_descent = _low_edge_frame(spark, "cruise_descent", {**_DESCENT, "speed_kt": 100.0}, n=400)
    controls = bad_climb.unionByName(clean_descent)

    def row(p):
        return _counts(
            **negative_counts(controls, p),
            link_old_edge_climb=p.link_old_edge_climb,
            link_new_edge_descent=p.link_new_edge_descent,
        )

    assert row(_CLIMB_ON)["negative_low_edge_within_limit"] is False
    assert row(_CLIMB_ON)["within_limits"] is False
    assert row(_DESCENT_ON)["negative_low_edge_within_limit"] is True
    assert row(_DESCENT_ON)["within_limits"] is True
    assert row(DIRECTIONAL)["negative_low_edge_within_limit"] is False
    assert row(DIRECTIONAL)["within_limits"] is False


def test_low_edge_controls_count_at_any_depth_where_the_point_can_link_them(spark):
    """Shallow cruise_cruise is kept apart, and a low-edge family has no such filter."""
    shallow = _negatives(spark, [("S", _feature_row(), 0.0, 0.0)])
    controls = shallow.unionByName(_low_edge_frame(spark, "climb_descent", {**_CLIMB, **_DESCENT}))
    counts = negative_counts(controls, DIRECTIONAL)
    assert counts["negative_limit_controls"] == 1
    assert counts["negative_limit_false_joins"] == 1
    assert counts["negative_shallow_controls"] == 1
    assert counts["negative_low_edge_controls"] == 1
    assert counts["negative_low_edge_false_joins"] == 1
    assert counts["negative_low_edge_untested_controls"] == 0


@pytest.mark.parametrize("family", list(_LOW_DEPTHS))
def test_every_low_edge_family_reaches_the_limit_and_the_same_callsign_group(spark, family):
    features = {
        "cruise_descent": _DESCENT,
        "climb_cruise": _CLIMB,
        "climb_descent": {**_CLIMB, **_DESCENT},
    }[family]
    counts = negative_counts(_low_edge_frame(spark, family, features), DIRECTIONAL)
    assert counts["negative_limit_false_joins"] == 1
    assert counts["same_callsign_false_joins"] == 1


def test_a_low_edge_over_the_allowed_gap_does_not_count(spark):
    controls = _low_edge_frame(spark, "climb_descent", {**_CLIMB, **_DESCENT, "gap_min": 400.0})
    counts = negative_counts(controls, DIRECTIONAL)
    assert counts["negative_limit_controls"] == 0
    assert counts["negative_over_gap_controls"] == 1


def test_low_edges_cannot_dilute_the_limit_of_a_point_that_cannot_link_them(spark):
    """Direction off, these fail the floor by construction, so they measure nothing.

    Counted, 8 joins in 500 cruise controls (over 1%) would pass on 3,000 free rejections.
    """
    cruise = _negatives(spark, [("C", _feature_row())])
    low = _low_edge_frame(spark, "climb_descent", {**_CLIMB, **_DESCENT}, n=3)
    counts = negative_counts(cruise.unionByName(low), P)
    assert counts["negative_controls"] == 4
    assert counts["negative_limit_controls"] == 1
    assert counts["negative_limit_false_joins"] == 1
    assert counts["same_callsign_controls"] == 1
    assert counts["negative_low_edge_controls"] == 0
    assert counts["negative_low_edge_untested_controls"] == 3
    assert (
        counts["negative_limit_controls"] + counts["negative_shallow_controls"]
        + counts["negative_over_gap_controls"] + counts["negative_low_edge_untested_controls"]
    ) == counts["negative_controls"]


def test_a_low_edge_false_join_can_take_a_point_over_the_limit(spark):
    low = _low_edge_frame(spark, "cruise_descent", _DESCENT, n=5)
    cruise = _negatives(spark, [(f"C{i}", _feature_row(speed_kt=100.0)) for i in range(5)])
    counts = negative_counts(cruise.unionByName(low), DIRECTIONAL)
    row = _counts(**counts, link_old_edge_climb=True, link_new_edge_descent=True)
    assert row["negative_within_limit"] is False
    assert row["negative_low_edge_within_limit"] is False
    assert row["within_limits"] is False


# --- the limits a direction-on point must also meet on its own evidence --------


#: A point with direction on, every limit met on enough evidence.
_ON = dict(
    link_old_edge_climb=True, link_new_edge_descent=True,
    negative_limit_controls=11000,
    **_FAMILIES_CLEAN,
    same_callsign_controls=1500,
    same_callsign_low_edge_controls=500,
    linked_by_direction_labelled=500,
    linked_by_climb_labelled=500,
    linked_by_descent_labelled=500,
)


def _on(**overrides):
    return _counts(**{**_ON, **overrides})


def test_a_direction_on_point_with_clean_evidence_passes():
    row = _on()
    assert row["within_limits"] is True
    assert row["provisional"] is False
    assert row["same_callsign_low_edge_status"] == "within_limit"
    assert row["real_by_direction_status"] == "within_limit"


def test_a_clean_cruise_pool_cannot_hide_a_bad_low_edge_family():
    """10% of 100 low-edge controls are false joins, in a pool of 100,000 otherwise clean."""
    row = _on(
        negative_limit_controls=100000, negative_limit_false_joins=10,
        negative_low_edge_controls=100, negative_low_edge_false_joins=10,
    )
    assert row["negative_false_join_upper_95"] < 0.01
    assert row["negative_within_limit"] is True
    assert row["negative_low_edge_upper_95"] > 0.01
    assert row["negative_low_edge_within_limit"] is False
    assert row["within_limits"] is False


def test_low_edge_controls_too_few_to_bound_fail_and_none_at_all_fail():
    """A point cannot be shown safe by having nothing to measure it on."""
    assert _on(negative_low_edge_controls=100)["within_limits"] is False
    none = _on(negative_low_edge_controls=0)
    assert none["negative_low_edge_upper_95"] is None
    assert none["within_limits"] is False


def test_a_bad_low_edge_same_callsign_group_fails_alone():
    row = _on(
        same_callsign_controls=100000, same_callsign_false_joins=10,
        same_callsign_low_edge_controls=100, same_callsign_low_edge_false_joins=10,
        negative_limit_controls=100000, negative_limit_false_joins=10,
        negative_low_edge_controls=100000, negative_low_edge_false_joins=10,
    )
    assert row["same_callsign_status"] == "within_limit"
    assert row["same_callsign_low_edge_status"] == "over_limit"
    assert row["within_limits"] is False


def test_a_low_edge_same_callsign_group_under_60_is_insufficient_and_provisional():
    row = _on(same_callsign_low_edge_controls=59, same_callsign_low_edge_false_joins=59)
    assert row["same_callsign_low_edge_status"] == "insufficient"
    assert row["provisional"] is True
    assert row["within_limits"] is True
    assert _on(same_callsign_low_edge_controls=60)["same_callsign_low_edge_status"] == "within_limit"


def test_linking_more_cannot_hide_a_bad_rate_among_direction_links():
    """1% of 400 links by direction, and the rate over all linked holes is 0.04%."""
    row = _on(
        linked_true_link=10396, linked_false_join=4, real_holes_linked=10400,
        linked_by_direction_labelled=400, false_joined_by_direction=4,
    )
    assert row["real_holes_within_limit"] is True
    assert row["real_by_direction_false_join_rate"] == pytest.approx(0.01)
    assert row["real_by_direction_status"] == "over_limit"
    assert row["within_limits"] is False


def test_few_labelled_links_by_direction_are_insufficient_and_reported():
    row = _on(linked_by_direction_labelled=299, false_joined_by_direction=0)
    assert row["real_by_direction_status"] == "insufficient"
    assert row["real_by_direction_false_join_rate"] == 0.0
    assert row["provisional"] is True
    assert row["real_holes_within_limit"] is True
    assert row["within_limits"] is True
    enough = _on(linked_by_direction_labelled=300, false_joined_by_direction=1)
    assert enough["real_by_direction_status"] == "over_limit"
    at_limit = _on(linked_by_direction_labelled=1000, false_joined_by_direction=1)
    assert at_limit["real_by_direction_status"] == "within_limit"


def test_the_separate_limits_do_not_apply_with_direction_off():
    row = _counts()
    assert row["negative_low_edge_within_limit"] is None
    assert row["same_callsign_low_edge_status"] == "not_applicable"
    assert row["real_by_direction_status"] == "not_applicable"
    assert row["within_limits"] is True
    assert row["provisional"] is False


def test_pooling_recomputes_the_direction_limits_from_summed_counts():
    day = _on(
        negative_low_edge_controls=200, negative_limit_controls=10200,
        same_callsign_low_edge_controls=40, linked_by_direction_labelled=200,
    )
    assert day["same_callsign_low_edge_status"] == "insufficient"
    assert day["real_by_direction_status"] == "insufficient"
    assert day["negative_low_edge_within_limit"] is False
    [pooled] = pool_rows([[day], [day]])
    assert pooled["negative_low_edge_controls"] == 400
    assert pooled["negative_low_edge_within_limit"] is True
    assert pooled["same_callsign_low_edge_status"] == "within_limit"
    assert pooled["real_by_direction_status"] == "within_limit"


def test_real_hole_counts_read_the_labelled_links_by_direction_and_near_events(spark):
    holes = _real_holes(
        spark,
        [
            ("true_link", _feature_row(**_DESCENT)),
            ("false_join", _feature_row(**_DESCENT)),
            ("unknown", _feature_row(**_DESCENT)),
            ("true_link", _feature_row()),
        ],
    ).withColumn("event_near_edge", F.col("label") == "false_join")
    extra = _real_holes(spark, [("false_join", _feature_row(speed_kt=100.0))]).withColumn(
        "event_near_edge", F.lit(True)
    )
    counts = real_hole_counts(holes.unionByName(extra), DIRECTIONAL)
    assert counts["linked_by_direction"] == 3
    assert counts["linked_by_direction_labelled"] == 2
    assert counts["false_joined_by_direction"] == 1
    assert counts["false_joined_near_edge"] == 1       # the unlinked one is no false join
    assert counts["linked_event_outside_box"] == 0     # no flag column: nothing counted


def test_real_hole_counts_say_how_many_links_had_an_event_outside_the_box(spark):
    holes = _real_holes(
        spark,
        [("unknown", _feature_row()), ("unknown", _feature_row()), ("true_link", _feature_row()),
         ("unknown", _feature_row(speed_kt=100.0))],
    ).withColumn("event_outside_box_inside", F.col("label") == "unknown")
    counts = real_hole_counts(holes, DIRECTIONAL)
    assert counts["real_holes_event_outside_box"] == 3
    assert counts["linked_event_outside_box"] == 2     # the slow one is not linked


def test_real_hole_counts_say_which_edge_was_below_the_floor(spark):
    """A hole low on both sides counts under each, and once in the total the limit reads."""
    holes = _real_holes(
        spark,
        [
            ("true_link", _feature_row(**_CLIMB)),
            ("false_join", _feature_row(**_CLIMB)),
            ("unknown", _feature_row(**_CLIMB)),
            ("true_link", _feature_row(**_DESCENT)),
            ("false_join", _feature_row(**_CLIMB, **_DESCENT)),
            ("true_link", _feature_row()),
        ],
    )
    counts = real_hole_counts(holes, DIRECTIONAL)
    assert counts["linked_by_climb"] == 4
    assert counts["false_joined_by_climb"] == 2
    assert counts["linked_by_climb_labelled"] == 3
    assert counts["linked_by_descent"] == 2
    assert counts["false_joined_by_descent"] == 1
    assert counts["linked_by_descent_labelled"] == 2
    assert counts["linked_by_direction"] == 5
    assert counts["false_joined_by_direction"] == 2
    assert counts["linked_by_direction_labelled"] == 4


def test_real_hole_counts_only_count_what_the_setting_links(spark):
    holes = _real_holes(
        spark, [("true_link", _feature_row(**_CLIMB)), ("true_link", _feature_row(**_DESCENT))]
    )
    climb_on = real_hole_counts(holes, _CLIMB_ON)
    assert (climb_on["linked_by_climb"], climb_on["linked_by_descent"]) == (1, 0)
    descent_on = real_hole_counts(holes, _DESCENT_ON)
    assert (descent_on["linked_by_climb"], descent_on["linked_by_descent"]) == (0, 1)


def test_real_hole_counts_say_which_links_rest_on_altitude_past_a_blank(spark):
    holes = _real_holes(
        spark,
        [
            ("true_link", _feature_row(alt_before_read_past_blank=True)),
            ("false_join", _feature_row(alt_before_read_past_blank=True)),
            ("false_join", _feature_row(alt_after_read_past_blank=True)),
            ("true_link", _feature_row()),
            # Not linked, so not a link resting on anything.
            ("false_join", _feature_row(speed_kt=100.0, alt_before_read_past_blank=True)),
        ],
    )
    counts = real_hole_counts(holes, P)
    assert counts["linked_altitude_read_past_blank_old"] == 2
    assert counts["false_joined_altitude_read_past_blank_old"] == 1
    assert counts["linked_altitude_read_past_blank_new"] == 1
    assert counts["false_joined_altitude_read_past_blank_new"] == 1


_DESCENT_ONLY_ROW = dict(link_old_edge_climb=False, link_new_edge_descent=True)
_CLIMB_ONLY_ROW = dict(link_old_edge_climb=True, link_new_edge_descent=False)


@pytest.mark.parametrize(
    "false_joined, labelled, status, passes, provisional",
    [
        (2, 125, "over_limit", False, None),
        (1, 186, "over_limit", False, None),
        (0, 186, "insufficient", True, True),
        (1, 1500, "within_limit", True, False),
    ],
)
@pytest.mark.parametrize(
    "way, flags",
    [
        ("direction", dict(link_old_edge_climb=True, link_new_edge_descent=True)),
        ("climb", _CLIMB_ONLY_ROW),
        ("descent", _DESCENT_ONLY_ROW),
    ],
)
def test_a_real_hole_rate_over_the_limit_fails_at_any_sample_size(
    way, flags, false_joined, labelled, status, passes, provisional
):
    """Under 300 read ``insufficient`` whatever the rate, so 1.6% could pass unnoticed."""
    enough = dict(
        linked_by_direction_labelled=1500, linked_by_climb_labelled=1500,
        linked_by_descent_labelled=1500,
    )
    row = _on(
        **{
            **flags,
            **enough,
            f"linked_by_{way}_labelled": labelled,
            f"false_joined_by_{way}": false_joined,
        }
    )
    assert row[f"real_by_{way}_status"] == status
    assert row[f"real_by_{way}_false_join_rate"] == pytest.approx(false_joined / labelled)
    assert row["within_limits"] is passes
    if provisional is not None:
        assert row["provisional"] is provisional


def test_an_edge_the_setting_does_not_allow_has_no_real_hole_verdict():
    row = _on(**_DESCENT_ONLY_ROW, linked_by_climb_labelled=100, false_joined_by_climb=50)
    assert row["real_by_climb_status"] == "not_applicable"
    assert row["within_limits"] is True
    both_off = _counts()
    assert both_off["real_by_climb_status"] == "not_applicable"
    assert both_off["real_by_descent_status"] == "not_applicable"


def test_a_bad_family_cannot_pass_diluted_at_both_on():
    """6 of 300 alone fails. Pooled to 6 of 1,500 it is under 1%, so only its own check sees it."""
    climb_alone = _on(
        **_CLIMB_ONLY_ROW,
        negative_low_edge_controls=300, negative_low_edge_false_joins=6,
        negative_climb_cruise_controls=300, negative_climb_cruise_false_joins=6,
        negative_limit_controls=10300, negative_limit_false_joins=6,
    )
    assert climb_alone["negative_low_edge_within_limit"] is False
    assert climb_alone["within_limits"] is False

    both = _on(
        negative_low_edge_controls=1500, negative_low_edge_false_joins=6,
        negative_climb_cruise_controls=300, negative_climb_cruise_false_joins=6,
        negative_cruise_descent_controls=600,
        negative_climb_descent_controls=600,
        negative_limit_controls=11500, negative_limit_false_joins=6,
    )
    assert both["negative_low_edge_within_limit"] is True      # the pool alone would pass
    assert both["negative_climb_cruise_within_limit"] is False
    assert both["negative_cruise_descent_within_limit"] is True
    assert both["within_limits"] is False


def test_a_family_with_too_few_controls_fails_a_setting_that_allows_it():
    row = _on(
        negative_climb_descent_controls=10,
        negative_cruise_descent_controls=590,
    )
    assert row["negative_low_edge_within_limit"] is True
    assert row["negative_climb_descent_within_limit"] is False
    assert row["within_limits"] is False


def test_a_family_a_setting_does_not_allow_is_not_checked():
    row = _on(
        **_CLIMB_ONLY_ROW,
        negative_low_edge_controls=400,
        negative_climb_cruise_controls=400,
        negative_cruise_descent_controls=0,
        negative_climb_descent_controls=0,
    )
    assert row["negative_cruise_descent_within_limit"] is None
    assert row["negative_climb_descent_within_limit"] is None
    assert row["within_limits"] is True


def test_the_family_counts_come_from_the_controls_and_pool(spark):
    counts = negative_counts(_one_of_each_low_edge_family(spark), DIRECTIONAL)
    for family in _FAMILY_EDGES:
        assert counts[f"negative_{family}_controls"] == 1
        assert counts[f"negative_{family}_false_joins"] == 1
    off = negative_counts(_one_of_each_low_edge_family(spark), _CLIMB_ON)
    assert off["negative_climb_cruise_controls"] == 1
    assert off["negative_cruise_descent_controls"] == 0
    day = _on(negative_climb_cruise_controls=200)
    assert pool_rows([[day], [day]])[0]["negative_climb_cruise_controls"] == 400


def test_the_reported_counts_are_reported_and_no_limit_reads_them():
    new = [
        "linked_by_climb", "linked_by_descent",
        "linked_altitude_read_past_blank_old", "linked_altitude_read_past_blank_new",
        "false_joined_altitude_read_past_blank_old", "false_joined_altitude_read_past_blank_new",
    ]
    assert set(new) <= set(COUNT_FIELDS)
    clean = with_verdicts(_on())
    bad = with_verdicts(_on(**{name: 400 for name in new}))
    assert bad["within_limits"] is clean["within_limits"]
    assert bad["provisional"] is clean["provisional"]


def test_a_false_join_near_an_edge_is_reported_and_decides_nothing():
    base = _on(linked_true_link=999, linked_false_join=1)
    near = _on(linked_true_link=999, linked_false_join=1, false_joined_near_edge=1)
    assert near["false_joined_near_edge"] == 1
    assert near["within_limits"] is base["within_limits"]
    assert near["real_false_join_rate"] == base["real_false_join_rate"]


def test_low_edge_same_callsign_counts_exclude_the_cruise_family(spark):
    controls = _negatives(spark, [("C", _feature_row())]).unionByName(
        _low_edge_frame(spark, "climb_descent", {**_CLIMB, **_DESCENT, "cs_after": "BAW2"})
    ).unionByName(_low_edge_frame(spark, "climb_cruise", _CLIMB))
    counts = negative_counts(controls, DIRECTIONAL)
    assert counts["same_callsign_controls"] == 2
    assert counts["same_callsign_low_edge_controls"] == 1
    assert counts["same_callsign_low_edge_false_joins"] == 1


def test_the_lowest_bound_is_compared_on_the_population_all_points_share():
    """Point 1's low-edge controls pull its pooled bound down. On cruise_cruise it is worse."""
    over = dict(linked_true_link=90, linked_false_join=10)
    rows = [
        _counts(point_id=0, negative_limit_controls=1000, negative_limit_false_joins=3, **over),
        _on(
            point_id=1, negative_limit_controls=3000, negative_limit_false_joins=5,
            negative_low_edge_controls=2000, **over,
        ),
    ]
    assert rows[1]["negative_false_join_upper_95"] < rows[0]["negative_false_join_upper_95"]
    assert rows[1]["negative_cruise_cruise_upper_95"] > rows[0]["negative_cruise_cruise_upper_95"]
    assert lowest_bound_point(rows)["point_id"] == 0


def test_control_rows_say_whether_they_count_toward_the_limit(spark):
    controls = _negatives(
        spark,
        [
            ("deep", _feature_row()),
            ("shallow", _feature_row(gap_min=61.0), 0.0, 0.0),
            ("long", _feature_row(gap_min=400.0)),
        ],
    ).unionByName(_low_edge_frame(spark, "climb_descent", {**_CLIMB, **_DESCENT}))

    def toward(p):
        return {
            (r["control_family"], r["gap_stratum"], r["depth_min"], r["within_link_max_gap"]):
                r["counts_toward_limit"]
            for r in strata_counts(controls, "negative", p, 0)
        }

    # Fixture depth_min is 0.0 for all, so gap_stratum tells the rows apart.
    on = toward(DIRECTIONAL)
    assert {k[0]: v for k, v in on.items() if k[0] != "cruise_cruise"} == {"climb_descent": True}
    assert toward(P)[("climb_descent", "30-120", 0.0, True)] is False
    assert on[("cruise_cruise", "over-360", 0.0, False)] is False
    positive = strata_counts(_positives(spark, [(60.0, _feature_row())]), "positive", P, 0)
    assert [r["counts_toward_limit"] for r in positive] == [False]


def test_a_rows_count_toward_the_limit_adds_up_to_the_limit_population(spark):
    controls = _negatives(
        spark, [("deep", _feature_row()), ("shallow", _feature_row(), 0.0, 0.0)]
    ).unionByName(_low_edge_frame(spark, "climb_cruise", _CLIMB))
    rows = strata_counts(controls, "negative", DIRECTIONAL, 0)
    assert sum(r["n"] for r in rows if r["counts_toward_limit"]) == (
        negative_counts(controls, DIRECTIONAL)["negative_limit_controls"]
    ) == 2


def test_negative_reasons_name_the_first_failed_test_per_family(spark):
    controls = _negatives(
        spark,
        [
            ("linked", _feature_row()),
            ("callsign", _feature_row(cs_after="BAW2")),
            ("slow", _feature_row(speed_kt=100.0)),
        ],
    ).unionByName(_low_edge_frame(spark, "climb_cruise", _CLIMB))
    rows = negative_reason_counts(controls, P)
    by_key = {(r["control_family"], r["first_failed_test"]): r for r in rows}
    assert {k: r["controls"] for k, r in by_key.items()} == {
        ("cruise_cruise", "linked"): 1,
        ("cruise_cruise", "callsign_agrees"): 1,
        ("cruise_cruise", "implied_speed_not_too_slow"): 1,
        ("climb_cruise", "cruise_at_old_edge"): 1,
    }
    assert by_key["cruise_cruise", "callsign_agrees"]["false_joins_without_callsign"] == 1
    assert by_key["cruise_cruise", "linked"]["false_joins_without_callsign"] == 1
    assert by_key["cruise_cruise", "implied_speed_not_too_slow"]["false_joins_without_callsign"] == 0
    assert by_key["climb_cruise", "cruise_at_old_edge"]["counts_toward_limit"] is False
    assert by_key["cruise_cruise", "linked"]["counts_toward_limit"] is True
    on = {
        (r["control_family"], r["first_failed_test"]): r["controls"]
        for r in negative_reason_counts(controls, DIRECTIONAL)
    }
    assert on["climb_cruise", "linked"] == 1


def test_strata_and_pooled_strata_keep_the_families_apart(spark):
    controls = _negatives(spark, [("C", _feature_row())]).unionByName(
        _low_edge_frame(spark, "climb_descent", {**_CLIMB, **_DESCENT})
    )
    rows = strata_counts(controls, "negative", DIRECTIONAL, 0)
    assert {r["control_family"]: (r["n"], r["wrong"]) for r in rows} == {
        "cruise_cruise": (1, 1), "climb_descent": (1, 1),
    }
    pooled = pool_strata([[{"day": "a", **r} for r in rows], [{"day": "b", **r} for r in rows]])
    assert {r["control_family"]: r["n"] for r in pooled} == {"cruise_cruise": 2, "climb_descent": 2}


def test_positive_counts_keep_the_low_edge_family_apart(spark):
    """Off, every one of these is missed, which must not read as a miss of the cruise rule."""
    cruise = _positives(spark, [(120.0, _feature_row())])
    low = _positives(spark, [(None, _feature_row(**_CLIMB, **_DESCENT))], family="low_edge")
    both = cruise.unionByName(low)

    off = positive_counts(both, P)
    assert (off["positive_controls"], off["positive_within_gap"], off["positive_linked"]) == (1, 1, 1)
    assert off["positive_low_edge_controls"] == 1
    assert off["positive_low_edge_within_gap"] == 1
    assert off["positive_low_edge_linked"] == 0

    on = positive_counts(both, DIRECTIONAL)
    assert on["positive_low_edge_linked"] == 1
    row = _counts(positive_low_edge_within_gap=4, positive_low_edge_linked=1)
    assert row["positive_low_edge_missed_share"] == pytest.approx(0.75)
    assert row["positive_missed_share"] == pytest.approx(0.05)


def test_positive_reasons_say_which_family(spark):
    low = _positives(spark, [(None, _feature_row(**_CLIMB))], family="low_edge")
    rows = positive_reason_counts(low, P)
    assert [(r["control_family"], r["first_failed_test"]) for r in rows] == [
        ("low_edge", "cruise_at_old_edge")
    ]


def test_real_hole_counts_say_how_the_edge_was_read(spark):
    holes = _real_holes(
        spark,
        [
            ("true_link", _feature_row()),
            ("true_link", _feature_row(position_gap_min=123.0)),
            ("false_join", _feature_row(position_gap_min=123.0)),
            ("true_link", _feature_row(**_DESCENT)),
            ("false_join", _feature_row(**_CLIMB)),
            ("unknown", _feature_row(**_CLIMB, position_gap_min=123.0)),
            ("false_join", _feature_row(position_gap_min=123.0, speed_kt=100.0)),  # not linked
        ],
    )
    on = real_hole_counts(holes, DIRECTIONAL)
    assert on["linked_edge_read_through_blank"] == 3
    assert on["false_joined_edge_read_through_blank"] == 1
    assert on["linked_by_direction"] == 3
    assert on["false_joined_by_direction"] == 1

    off = real_hole_counts(holes, P)
    assert off["linked_by_direction"] == 0
    assert off["false_joined_by_direction"] == 0
    assert off["linked_edge_read_through_blank"] == 2


def _hole_on_approach(spark, new_edge_ft):
    """F1 leaves coverage at cruise and reappears descending through *new_edge_ft*."""
    samples = _leg(0, 30)
    down = _leg(30 * 60 + 191 * 60, 10, start_nm=240 + 191 * 8, altitude=new_edge_ft / FT_PER_M)
    for sample in down:
        sample["vert_rate"] = -1500.0 / FPM_PER_MPS
    return (
        make_track(spark, samples + down),
        _truth(spark, ("F1", "abc123", -60, 30 * 60 + 191 * 60 + 10 * 60 + 60)),
    )


@pytest.mark.parametrize("new_edge_ft, tracks_on_flight", [(9000.0, 1), (3000.0, 2)])
def test_counting_from_decisions_agrees_with_re_segmenting_on_a_low_edge(
    spark, new_edge_ft, tracks_on_flight
):
    """At 3,000 ft the low-altitude gap breaks. The shortcut must not read that as linked."""
    tracks, truth = _hole_on_approach(spark, new_edge_ft)
    counted = _a10_counts(_flight_counts(spark, tracks, truth, DIRECTIONAL))
    assert counted == _flight_counts_by_re_segmenting(tracks, truth, DIRECTIONAL)
    assert counted["tracks_on_flights"] == tracks_on_flight


def _study_day_with_real_climbs(spark):
    """The pair day, its second airframe flying a real climb and descent around each cruise."""
    def flight(start_s, nm, key):
        climb = _leg(start_s, 5, start_nm=nm, altitude=9000.0 / FT_PER_M)
        cruise = _leg(start_s + 6 * 60, 120, start_nm=nm)
        descent = _leg(start_s + 127 * 60, 5, start_nm=nm + 960, altitude=9000.0 / FT_PER_M)
        for sample in climb:
            sample["vert_rate"] = 1500.0 / FPM_PER_MPS
        for sample in descent:
            sample["vert_rate"] = -1500.0 / FPM_PER_MPS
        return climb + cruise + descent, (key, "abc123", start_s, start_s + 132 * 60)

    s1, t1 = flight(0, 0.0, "F1")
    s2, t2 = flight(162 * 60, 1000.0, "F2")
    return make_track(spark, s1 + s2), _truth(spark, t1, t2)


def test_run_study_scores_and_reports_the_families(spark, tmp_path):
    tracks, truth = _study_day_with_real_climbs(spark)
    grid = [P, DIRECTIONAL]
    run_study(
        ["2026-06-14"], grid, load_tracks=lambda day: tracks, load_truth=lambda day: truth,
        results_dir=tmp_path,
    )

    strata = _read_csv(tmp_path / "link_controls_pooled.csv")
    families = {r["control_family"] for r in strata}
    assert families == {
        "cruise_cruise", "cruise_descent", "climb_cruise", "climb_descent", "low_edge",
    }
    def wrong(point, family):
        return sum(
            int(r["wrong"]) for r in strata
            if r["point_id"] == point and r["control_family"] == family and r["kind"] == "negative"
        )
    assert wrong("0", "climb_descent") == 0
    assert wrong("1", "climb_descent") == 1

    pooled = _read_csv(tmp_path / "link_grid_pooled.csv")
    assert [r["link_old_edge_climb"] for r in pooled] == ["False", "True"]
    assert int(pooled[0]["negative_low_edge_untested_controls"]) > 0
    assert int(pooled[1]["negative_low_edge_untested_controls"]) == 0
    assert int(pooled[1]["negative_limit_controls"]) > int(pooled[0]["negative_limit_controls"])
    assert int(pooled[0]["positive_low_edge_linked"]) == 0
    assert int(pooled[1]["positive_low_edge_linked"]) == 2

    funnel = _read_csv(tmp_path / "link_funnel_2026-06-14.csv")[0]
    for family in ("cruise_descent", "climb_cruise", "climb_descent"):
        assert int(funnel[f"{family}_controls"]) == int(funnel[f"{family}_controls_scored"]) > 0
    assert int(funnel["negative_controls"]) == int(funnel["negative_controls_scored"])
    assert int(funnel["positive_low_edge_controls"]) == int(funnel["positive_low_edge_controls_scored"]) == 2

    # Too few controls to bound anything, so no point passes. The reasons are read at
    # the point with the lowest bound on the population both points share.
    reasons = _read_csv(tmp_path / "link_negative_reasons_pooled.csv")
    assert {"cruise_cruise"} <= {r["control_family"] for r in reasons}
    assert set(reasons[0]) >= {
        "control_family", "counts_toward_limit", "first_failed_test",
        "controls", "false_joins_without_callsign", "settings_from",
    }
    assert (tmp_path / "link_negative_reasons_2026-06-14.csv").is_file()
    assert {r["counts_toward_limit"] for r in strata} <= {"True", "False"}


# --- round 2: which flights count as crossing the box ------------------------


def _airports(spark, *rows):
    return spark.createDataFrame(rows, ["ident", "longitude_deg", "latitude_deg"])


def _airport_table(spark):
    """Two inside ``EUROPE_BBOX``, one twice as ``oa_airports`` could hold it, one outside."""
    return _airports(
        spark,
        ("EDDF", 8.57, 50.03),
        ("EGLL", -0.46, 51.47),
        ("EGLL", -0.46, 51.47),
        ("KJFK", -73.78, 40.64),
    )


def test_crossing_agrees_with_the_far_end_labels(spark):
    pairs = [
        ("EDDF", "EGLL"), ("KJFK", "EDDF"), ("EDDF", "KJFK"), ("KJFK", "ZZZZ"),
        ("ZZZZ", "EDDF"), ("EDDF", None), (None, None),
    ]
    truth = spark.createDataFrame(
        [(i, a, b) for i, (a, b) in enumerate(pairs)],
        "flight_key int, gt_adep string, gt_ades string",
    )
    got = {r["flight_key"]: r["crosses_box"] for r in with_population(truth, _airport_table(spark)).collect()}

    labelled = adsc_flight_list_truth.label_far_end(
        pd.DataFrame(pairs, columns=["gt_adep", "gt_ades"]), {"EDDF", "EGLL"}
    )["flow"]
    want = labelled.isin(["europe_to_outside", "outside_to_europe"]).tolist()
    assert [got[i] for i in range(len(pairs))] == want
    assert want == [False, True, True, False, True, True, False]


def test_crossing_reads_padded_idents(spark):
    # load_flight_intervals does not trim ADEP/ADES as the far-end labels' reader does.
    truth = spark.createDataFrame(
        [(1, "EDDF ", " KJFK")], "flight_key int, gt_adep string, gt_ades string"
    )
    assert with_population(truth, _airport_table(spark)).first()["crosses_box"] is True


# --- round 2: tracks per population ------------------------------------------


def _with_crossing(spark, truth, *crossing_keys):
    keys = list(crossing_keys)
    return truth.drop("crosses_box").withColumn("crosses_box", F.col("flight_key").isin(keys))


def _population_counts(tracks, truth, p=P):
    flights = flight_base_tracks(tracks, truth, p).join(
        truth.select("flight_key", "crosses_box").distinct(), on="flight_key"
    )
    holes = real_hole_candidates(tracks, p)
    return population_track_counts(flights, holes_inside_flights(holes, truth), p)


def test_tracks_are_counted_per_population_and_sum_to_the_flights_total(spark):
    tracks, truth = _study_day(spark)
    # F1 links, F2 splits, F3 is one track, F4 splits, F5 was never seen.
    truth = _with_crossing(spark, truth, "F1", "F2")

    counts = _population_counts(tracks, truth)

    assert counts == {
        "crossing_flights": 2,
        "crossing_flights_in_one_track": 1,
        "crossing_tracks": 3,
        "base_crossing_flights_in_one_track": 0,
        "base_crossing_tracks": 4,
        "other_flights": 3,
        "other_flights_in_one_track": 1,
        "other_tracks": 3,
        "base_other_flights_in_one_track": 1,
        "base_other_tracks": 3,
    }
    total = _flight_counts(spark, tracks, truth)
    for population_name, total_name in (
        ("flights", "flights"),
        ("flights_in_one_track", "flights_in_one_track"),
        ("tracks", "tracks_on_flights"),
    ):
        assert counts[f"crossing_{population_name}"] + counts[f"other_{population_name}"] == total[total_name]
    assert counts["base_crossing_tracks"] + counts["base_other_tracks"] == total["base_tracks_on_flights"]


def test_population_shares_are_derived_and_pooled_from_counts():
    day1 = _counts(
        crossing_flights=10, crossing_flights_in_one_track=1, crossing_tracks=19,
        base_crossing_flights_in_one_track=0, base_crossing_tracks=20,
        other_flights=90, other_flights_in_one_track=89, other_tracks=91,
    )
    day2 = _counts(
        crossing_flights=30, crossing_flights_in_one_track=30, crossing_tracks=30,
        base_crossing_flights_in_one_track=3, base_crossing_tracks=60,
    )
    assert day1["crossing_share_one_track"] == pytest.approx(0.1)
    assert day1["crossing_tracks_per_flight"] == pytest.approx(1.9)
    assert day1["base_crossing_share_one_track"] == 0.0
    assert day1["base_crossing_tracks_per_flight"] == pytest.approx(2.0)
    assert day1["other_share_one_track"] == pytest.approx(89 / 90)

    [pooled] = pool_rows([[day1], [day2]])
    assert pooled["crossing_flights"] == 40
    assert pooled["crossing_share_one_track"] == pytest.approx(31 / 40)
    assert pooled["base_crossing_share_one_track"] == pytest.approx(3 / 40)


# --- round 2: why a hole stays split -----------------------------------------


def _reason_holes(spark, rows):
    """Holes from ``(population, feature_row)``, as :func:`true_link_holes` leaves them."""
    schema = StructType([StructField("population", StringType(), True)] + _FEATURE_SCHEMA)
    return spark.createDataFrame([(pop, *features) for pop, features in rows], schema)


def _by_key(rows):
    return {
        (r["population"], r["gap_band"], r["first_failed_test"]): (r["holes"], r["only_failed_test_holes"])
        for r in rows
    }


def test_a_hole_is_counted_under_the_first_test_it_fails(spark):
    linked = _feature_row(gap_min=120.0)
    too_slow = _feature_row(gap_min=120.0, speed_kt=100.0)
    # Fails the callsign test and then the speed test: it belongs to the callsign, and
    # relaxing either alone would still not link it.
    two_tests = _feature_row(gap_min=120.0, speed_kt=100.0, cs_after="BAW2")
    holes = _reason_holes(
        spark,
        [("crosses_box", linked), ("crosses_box", too_slow), ("crosses_box", two_tests), ("other", too_slow)],
    )

    assert _by_key(reason_counts(holes, P)) == {
        ("crosses_box", "60-120", "linked"): (1, 0),
        ("crosses_box", "60-120", "implied_speed_not_too_slow"): (1, 1),
        ("crosses_box", "60-120", "callsign_agrees"): (1, 0),
        ("other", "60-120", "implied_speed_not_too_slow"): (1, 1),
    }


@pytest.mark.parametrize(
    "gap_min, band",
    [
        (30.5, "30-60"), (60.0, "30-60"), (60.5, "60-120"), (120.0, "60-120"), (120.5, "120-240"),
        (240.0, "120-240"), (240.5, "240-360"), (360.0, "240-360"), (360.5, "over-360"),
    ],
)
def test_a_hole_falls_in_one_length_band(spark, gap_min, band):
    holes = _reason_holes(spark, [("other", _feature_row(gap_min=gap_min))])
    [row] = reason_counts(holes, P)
    assert row["gap_band"] == band
    assert row["first_failed_test"] == ("gap_not_too_long" if gap_min > 360.0 else "linked")


def test_the_first_band_starts_where_a_hole_does(spark):
    holes = _reason_holes(spark, [("other", _feature_row(gap_min=50.0))])
    [row] = reason_counts(holes, replace(P, gap_minutes=45.0))
    assert row["gap_band"] == "45-60"


def test_the_read_through_count_covers_every_hole_and_the_linked_one_its_links(spark):
    """Linked holes and all holes are different populations, so both are stated."""
    no_position = list(_feature_row())
    no_position[2:4] = [None, None]
    holes = _real_holes(
        spark,
        [
            ("true_link", _feature_row(position_gap_min=123.0)),
            # Read past a blank, but too long to link.
            ("unknown", _feature_row(gap_min=400.0, position_gap_min=403.0)),
            ("unknown", _feature_row(gap_min=400.0)),
            ("unknown", tuple(no_position)),
        ],
    )
    counts = real_hole_counts(holes, P)
    assert counts["real_holes"] == 4
    assert counts["real_holes_edge_read_through_blank"] == 2
    assert counts["linked_edge_read_through_blank"] == 1


def test_reasons_read_the_settings_they_are_given(spark):
    holes = _reason_holes(spark, [("other", _feature_row(gap_min=200.0))])
    [row] = reason_counts(holes, replace(P, link_max_gap_minutes=120.0))
    assert (row["first_failed_test"], row["holes"], row["only_failed_test_holes"]) == ("gap_not_too_long", 1, 1)


def _labelled_holes(spark, tracks, truth, p=P):
    return label_real_holes(real_hole_candidates(tracks, p), truth)


def test_reasons_count_only_true_links_and_every_one_of_them(spark):
    tracks, truth = _study_day_with_a_pair(spark)
    truth = _with_crossing(spark, truth, "F1")
    holes = _labelled_holes(spark, tracks, truth)
    all_labels = {r["label"]: r["count"] for r in holes.groupBy("label").count().collect()}
    assert all_labels["false_join"] == 1 and all_labels["true_link"] == 2

    # F1's hole links and F2's does not: neither decision moves a hole out of the count.
    kept = true_link_holes(holes, truth)
    assert {r["population"]: r["count"] for r in kept.groupBy("population").count().collect()} == {
        "crosses_box": 1,
        "other": 1,
    }
    assert _by_key(reason_counts(kept, P)) == {
        ("crosses_box", "120-240", "linked"): (1, 0),
        ("other", "60-120", "implied_speed_not_too_slow"): (1, 1),
    }


def test_the_population_split_sums_to_the_total(spark):
    tracks, truth = _study_day(spark)
    truth = _with_crossing(spark, truth, "F2")
    holes = _labelled_holes(spark, tracks, truth)
    rows = reason_counts(true_link_holes(holes, truth), P)
    assert sum(r["holes"] for r in rows) == holes.filter(F.col("label") == "true_link").count() == 2
    assert {r["population"] for r in rows} == {"crosses_box", "other"}


def test_a_hole_inside_two_truth_flights_counts_once_and_as_crossing(spark):
    tracks = make_track(spark, _leg(0, 30) + _leg(30 * 60 + 191 * 60, 30, start_nm=240 + 191 * 8))
    # Two rows of truth over one flight, as a refiled flight plan leaves in Network Manager.
    truth = _truth(
        spark,
        ("A", "abc123", -60, 30 * 60 + 191 * 60 + 30 * 60 + 60, False),
        ("B", "abc123", -50, 30 * 60 + 191 * 60 + 30 * 60 + 50, True),
    )
    kept = true_link_holes(_labelled_holes(spark, tracks, truth), truth).collect()
    assert [r["population"] for r in kept] == ["crosses_box"]


def test_a_missed_positive_control_is_counted_under_its_first_failed_test(spark):
    controls = _positives(
        spark,
        [
            (120.0, _feature_row(gap_min=120.0)),
            (120.0, _feature_row(gap_min=120.0, speed_kt=100.0)),
            (360.0, _feature_row(gap_min=360.0, speed_kt=100.0)),
            (360.0, _feature_row(gap_min=360.0, speed_kt=100.0, cs_after="BAW2")),
        ],
    )
    rows = positive_reason_counts(controls, P)
    assert {(r["requested_gap_min"], r["first_failed_test"]): r["positive_missed_by_test"] for r in rows} == {
        (120.0, "implied_speed_not_too_slow"): 1,
        (360.0, "implied_speed_not_too_slow"): 1,
        (360.0, "callsign_agrees"): 1,
    }
    assert sum(r["positive_missed_by_test"] for r in rows) == (
        positive_counts(controls, P)["positive_controls"] - positive_counts(controls, P)["positive_linked"]
    )


def test_pooling_reasons_sums_within_each_key():
    day1 = [
        {"day": "a", "population": "other", "first_failed_test": "linked", "holes": 2, "only_failed_test_holes": 0},
        {"day": "a", "population": "other", "first_failed_test": "x", "holes": 1, "only_failed_test_holes": 1},
    ]
    day2 = [
        {"day": "b", "population": "other", "first_failed_test": "x", "holes": 4, "only_failed_test_holes": 2},
        {"day": "b", "population": "crosses_box", "first_failed_test": "x", "holes": 8, "only_failed_test_holes": 0},
    ]
    pooled = pool_counts([day1, day2], ("holes", "only_failed_test_holes"))
    assert {
        (r["day"], r["population"], r["first_failed_test"]): (r["holes"], r["only_failed_test_holes"])
        for r in pooled
    } == {
        ("pooled", "other", "linked"): (2, 0),
        ("pooled", "other", "x"): (5, 3),
        ("pooled", "crosses_box", "x"): (8, 0),
    }


# --- round 2: how much safety leans on the callsign test ---------------------


def test_false_joins_without_the_callsign_test_drop_that_test_and_no_other(spark):
    differs = _feature_row(gap_min=120.0, cs_after="BAW2")
    differs_and_too_slow = _feature_row(gap_min=120.0, speed_kt=100.0, cs_after="BAW2")
    same = _feature_row(gap_min=120.0)
    differs_over_gap = _feature_row(gap_min=400.0, cs_after="BAW2")
    differs_shallow = ("p5", _feature_row(gap_min=120.0, cs_after="BAW2"), 0.0, 0.0)
    controls = _negatives(
        spark,
        [("p1", differs), ("p2", differs_and_too_slow), ("p3", same), ("p4", differs_over_gap), differs_shallow],
    )

    counts = negative_counts(controls, P)

    # Under the limit's population: p1 joins once the test is gone, p3 joined anyway.
    assert counts["negative_limit_false_joins"] == 1
    assert counts["negative_limit_false_joins_no_callsign"] == 2


def test_false_joins_without_the_callsign_test_are_reported_and_not_limited():
    row = _counts(negative_limit_false_joins_no_callsign=500)
    assert row["negative_no_callsign_false_join_rate"] == pytest.approx(0.5)
    assert row["negative_no_callsign_upper_95"] == pytest.approx(rule_of_three_upper(500, 1000))
    assert row["negative_within_limit"] is True
    assert row["within_limits"] is True

    [pooled] = pool_rows([[_counts(negative_limit_false_joins_no_callsign=1)]] * 2)
    assert pooled["negative_limit_false_joins_no_callsign"] == 2
    assert pooled["negative_no_callsign_upper_95"] == pytest.approx(rule_of_three_upper(2, 2000))


# --- round 2: the settings the reasons are read at ---------------------------


def test_reasons_params_json_keeps_unnamed_settings_at_the_grid_base():
    p = reasons_params_from_json('{"link_max_gap_minutes": 240, "link_min_speed_ratio": 0.85}', [P])
    assert p == replace(P, link_max_gap_minutes=240.0, link_min_speed_ratio=0.85)


def test_reasons_params_json_takes_what_the_features_were_made_with_from_the_grid():
    grid = [replace(P, link_cruise_floor_ft=30000.0)]
    assert reasons_params_from_json('{"link_max_gap_minutes": 240}', grid).link_cruise_floor_ft == 30000.0
    assert reasons_params_from_json('{"link_cruise_floor_ft": 30000}', grid).link_cruise_floor_ft == 30000.0


@pytest.mark.parametrize(
    "text",
    [
        "{}",
        "[1]",
        '{"link_max_gap_minute": 240}',
        '{"link_max_gap_minutes": "240"}',
        '{"link_max_gap_minutes": [240]}',
        # The features are made once, at the grid's value, so this could not be honoured.
        '{"link_cruise_floor_ft": 30000}',
        '{"link_callsign_lookahead_minutes": 20}',
    ],
)
def test_reasons_params_json_refuses_what_it_cannot_honour(text):
    with pytest.raises(ValueError):
        reasons_params_from_json(text, [P])


# --- round 2: the whole study ------------------------------------------------


def test_run_study_writes_the_reasons_at_the_chosen_point_and_pools_them(spark, tmp_path):
    tracks, truth = _study_day_with_a_pair(spark)
    truth = _with_crossing(spark, truth, "F1")
    grid = [replace(P, link_max_gap_minutes=120.0), P]

    run_study(
        ["2026-06-14", "2025-06-14"], grid,
        load_tracks=lambda day: tracks, load_truth=lambda day: truth, results_dir=tmp_path,
    )

    manifest = json.loads((tmp_path / "_manifest.json").read_text())
    for stem in ("link_reasons", "link_positive_reasons"):
        for tail in ("2026-06-14", "2025-06-14", "pooled"):
            assert f"{stem}_{tail}.csv" in manifest

    # Point 0 is chosen, and its 120 min limit is what stops F1's 191 min hole.
    rows = _read_csv(tmp_path / "link_reasons_2026-06-14.csv")
    assert {r["settings_from"] for r in rows} == {"not_recommended_lowest_bound"}
    assert {r["link_max_gap_minutes"] for r in rows} == {"120.0"}
    assert {(r["population"], r["gap_band"], r["first_failed_test"], r["holes"], r["only_failed_test_holes"]) for r in rows} == {
        ("crosses_box", "120-240", "gap_not_too_long", "1", "1"),
        ("other", "60-120", "implied_speed_not_too_slow", "1", "1"),
    }
    pooled = _read_csv(tmp_path / "link_reasons_pooled.csv")
    assert {r["day"] for r in pooled} == {"pooled"}
    assert sorted(int(r["holes"]) for r in pooled) == [2, 2]

    grid_row = _read_csv(tmp_path / "link_grid_2026-06-14.csv")[0]
    missed = _read_csv(tmp_path / "link_positive_reasons_2026-06-14.csv")
    assert sum(int(r["positive_missed_by_test"]) for r in missed) == (
        int(grid_row["positive_controls"]) - int(grid_row["positive_linked"])
    )
    pooled_missed = _read_csv(tmp_path / "link_positive_reasons_pooled.csv")
    assert sum(int(r["positive_missed_by_test"]) for r in pooled_missed) == 2 * sum(
        int(r["positive_missed_by_test"]) for r in missed
    )

    # The populations and the no-callsign count reach the grid rows, and pool.
    pooled_grid = _read_csv(tmp_path / "link_grid_pooled.csv")[1]
    assert int(pooled_grid["crossing_flights"]) == 2
    assert int(pooled_grid["crossing_flights_in_one_track"]) == 2
    assert int(pooled_grid["crossing_flights"]) + int(pooled_grid["other_flights"]) == int(pooled_grid["flights"])
    assert "negative_limit_false_joins_no_callsign" in pooled_grid


def test_run_study_reads_the_reasons_at_settings_it_is_given(spark, tmp_path):
    tracks, truth = _study_day_with_a_pair(spark)
    grid = [replace(P, link_max_gap_minutes=120.0)]

    run_study(
        ["2026-06-14"], grid,
        load_tracks=lambda day: tracks, load_truth=lambda day: truth, results_dir=tmp_path,
        reasons_params=P,
    )

    rows = _read_csv(tmp_path / "link_reasons_2026-06-14.csv")
    assert {r["settings_from"] for r in rows} == {"given"}
    assert {r["first_failed_test"] for r in rows} == {"linked", "implied_speed_not_too_slow"}


def test_with_nothing_chosen_and_nothing_given_no_reasons_are_written(spark, tmp_path):
    tracks, truth = _study_day(spark)
    run_study(
        ["2026-06-14"], [P],
        load_tracks=lambda day: tracks, load_truth=lambda day: truth, results_dir=tmp_path,
    )
    assert _read_csv(tmp_path / "link_recommended.csv")[0]["status"] == "no_evidence"
    assert not list(tmp_path.glob("link_*reasons*"))


def test_a_day_with_no_true_link_holes_still_writes_its_reasons_header(spark, tmp_path):
    # Long enough for a positive control, which links, so neither report has a row.
    tracks = make_track(spark, _leg(0, 120, icao24="ccc333"))
    truth = _truth(spark, ("F3", "ccc333", -60, 120 * 60 + 60))
    run_study(
        ["2026-06-14"], [P],
        load_tracks=lambda day: tracks, load_truth=lambda day: truth, results_dir=tmp_path,
        reasons_params=P,
    )
    for stem in ("link_reasons", "link_positive_reasons"):
        for tail in ("2026-06-14", "pooled"):
            with (tmp_path / f"{stem}_{tail}.csv").open() as fh:
                reader = csv.DictReader(fh)
                assert list(reader) == []
                assert "first_failed_test" in reader.fieldnames


def test_the_grid_base_does_not_follow_the_shipped_defaults():
    """The study's points are fixed by GRID_BASE, whatever the defaults become."""
    assert GRID_BASE == {
        "link_cruise_floor_ft": 25000.0,
        "link_min_implied_speed_kt": 315.0,
        "link_max_gap_minutes": 360.0,
        "link_old_edge_climb": False,
        "link_new_edge_descent": False,
    }
    [only] = grid_from_json('{"link_min_speed_ratio": [0.0]}')
    assert (only.link_cruise_floor_ft, only.link_max_gap_minutes) == (25000.0, 360.0)
    [over] = grid_from_json('{"link_max_gap_minutes": [600]}')
    assert over.link_max_gap_minutes == 600.0


def test_population_says_which_end_is_inside_the_box(spark):
    """The labeller needs each end on its own: an event at an aerodrome outside the box
    is an estimated time, not evidence."""
    truth = spark.createDataFrame(
        [(1, "EDDF", "KJFK"), (2, "KJFK", "EGLL"), (3, "ZZZZ", None)],
        "flight_key int, gt_adep string, gt_ades string",
    )
    got = {r["flight_key"]: (r["adep_in_box"], r["ades_in_box"])
           for r in with_population(truth, _airport_table(spark)).collect()}
    assert got == {1: (True, False), 2: (False, True), 3: (False, False)}


def test_the_study_carries_the_in_box_flags_to_the_labeller(spark):
    """`build_features` projects the truth before labelling; the flags must survive it,
    or every event counts and the grid never sees the correction."""
    from track_link_grid import build_features

    tracks, truth = _study_day_with_a_pair(spark)
    # The turnaround between F6 and F7 happens at an aerodrome outside the box.
    flagged = truth.withColumn("adep_in_box", F.col("flight_key") != "F7").withColumn(
        "ades_in_box", F.col("flight_key") != "F6"
    )
    holes = build_features(tracks, flagged, P).holes.filter(F.col("icao24") == "fff666").collect()
    assert len(holes) == 1
    assert holes[0]["label"] == "unknown"
    assert holes[0]["event_outside_box_inside"] is True

    plain = build_features(tracks, truth, P).holes.filter(F.col("icao24") == "fff666").first()
    assert plain["label"] == "false_join"
