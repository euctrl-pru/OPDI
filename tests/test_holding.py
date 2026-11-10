"""holding-start / holding-end: a full circle in one rotational sense, in a small area."""

import datetime as dt
import json

import pytest
from conftest import _EPOCH, make_track
from navigation_fixtures import fly, legs_to_samples, measured, racetrack
from pyspark.sql import functions as F

from opdi.config import EventConfig
from opdi.pipeline.holding import HOLDING_VERSION, calculate_holding_events


def _by_type(rows):
    return {r.type: r for r in rows}


def test_two_racetrack_circuits_give_one_holding_pair(spark):
    sdf = fly(spark, [(300, 0, 0)] + racetrack(2) + [(300, 0, 0)])
    rows = calculate_holding_events(sdf, EventConfig()).collect()
    assert sorted(r.type for r in rows) == ["holding-end", "holding-start"]
    by = _by_type(rows)
    info = json.loads(by["holding-start"].info)
    assert info == json.loads(by["holding-end"].info)
    assert info["pattern"] == "racetrack"
    assert info["direction"] == "R"
    assert 1.5 <= info["circuits"] <= 2.5
    assert 600 <= info["duration_s"] <= 900
    assert info["holding_seq"] == 1
    assert all(r.version == HOLDING_VERSION == "holding_v0.1.0" for r in rows)
    span = by["holding-end"].cumulative_time_s - by["holding-start"].cumulative_time_s
    assert 600 <= span <= 900
    assert by["holding-start"].lat is not None and by["holding-end"].lon is not None
    assert by["holding-end"].cumulative_distance_nm > by["holding-start"].cumulative_distance_nm


def test_a_single_left_orbit_is_an_orbit(spark):
    sdf = fly(spark, [(300, 0, 0), (270, -1.4, 0), (300, 0, 0)])   # 378° left in 4.5 min
    rows = calculate_holding_events(sdf, EventConfig()).collect()
    assert len(rows) == 2
    info = json.loads(rows[0].info)
    assert info["pattern"] == "orbit"
    assert info["direction"] == "L"


def test_alternating_vectors_are_not_a_hold(spark):
    sdf = fly(spark, [(60, 0, 0), (80, 1.5, 0), (60, 0, 0), (80, -1.5, 0)] * 3)
    assert calculate_holding_events(sdf, EventConfig()).count() == 0


def test_a_low_slow_circuit_is_not_a_hold(spark):
    sdf = fly(spark, racetrack(2), speed_kt=100.0, alt_ft=1000.0)
    assert calculate_holding_events(sdf, EventConfig()).count() == 0


def test_a_hold_near_a_go_around_is_dropped(spark):
    sdf = fly(spark, racetrack(2))
    ga = spark.createDataFrame(
        [("trk-1", "go-around", _EPOCH + dt.timedelta(seconds=60))],
        "track_id string, type string, event_time timestamp",
    )
    assert calculate_holding_events(sdf, EventConfig(), go_arounds=ga).count() == 0
    # The same frame with the go-around on another track leaves the hold alone.
    other = ga.withColumn("track_id", F.lit("trk-9"))
    assert calculate_holding_events(sdf, EventConfig(), go_arounds=other).count() == 2


def test_a_heading_gap_does_not_create_a_turn(spark):
    # Straight east, heading NULL for 90 s in the middle, then a 200° right turn
    # and straight again: a vectoring turn, no hold. A naive difference across
    # the NULL stretch, or a wrap mistake, would add a spurious half turn.
    samples = legs_to_samples([(300, 0, 0), (134, 1.5, 0), (300, 0, 0)])
    for s in samples:
        if 120 <= s["t"] < 210:
            s["heading"] = None
    sdf = measured(make_track(spark, samples))
    assert calculate_holding_events(sdf, EventConfig()).count() == 0


def test_a_hold_split_by_a_hole_gives_two_episodes(spark):
    sdf = fly(spark, racetrack(2) + [(-2400, 0, 0)] + racetrack(2))
    rows = calculate_holding_events(sdf, EventConfig()).collect()
    starts = [r for r in rows if r.type == "holding-start"]
    assert len(starts) == 2
    assert sorted(json.loads(r.info)["holding_seq"] for r in starts) == [1, 2]


def test_off_flag_returns_none(spark):
    assert calculate_holding_events(fly(spark, racetrack(2)), EventConfig(emit_holding=False)) is None
