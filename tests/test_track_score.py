"""Metric tests against cases whose answers are computable by hand.

A perfect segmentation scores 1. A totally merged one has completeness 1 and
homogeneity 0. A totally fragmented one has the reverse. If a metric does not do
that, it is not measuring what its name says.
"""

import datetime as dt

import pytest
from pyspark.sql import Row
from track_score import match_rates, score_arm, vmeasure

_T0 = dt.datetime(2025, 6, 5, 8, 0, 0)


def _matched(spark, pairs):
    """pairs: list of (track_id, flight_key, n_samples)."""
    rows = []
    for track_id, flight_key, n in pairs:
        for i in range(n):
            rows.append(Row(
                icao24="abc123",
                event_time=_T0 + dt.timedelta(seconds=i * 10),
                track_id=track_id,
                flight_key=flight_key,
                t_off=_T0,
                t_land=_T0 + dt.timedelta(hours=1),
                t_source="nm_inferred",
            ))
    return spark.createDataFrame(rows)


def test_perfect_segmentation_scores_one(spark):
    m = _matched(spark, [("T1", "F1", 10), ("T2", "F2", 10)])
    v = vmeasure(m)
    assert v["homogeneity"] == pytest.approx(1.0)
    assert v["completeness"] == pytest.approx(1.0)
    assert v["v_measure"] == pytest.approx(1.0)


def test_total_merge_has_completeness_one_and_low_homogeneity(spark):
    """One track holding both flights: nothing is scattered, everything is impure."""
    m = _matched(spark, [("T1", "F1", 10), ("T1", "F2", 10)])
    v = vmeasure(m)
    assert v["completeness"] == pytest.approx(1.0)
    assert v["homogeneity"] == pytest.approx(0.0, abs=1e-9)


def test_total_fragmentation_has_homogeneity_one_and_low_completeness(spark):
    """One flight split across two tracks: every track is pure, nothing is whole."""
    m = _matched(spark, [("T1", "F1", 10), ("T2", "F1", 10)])
    v = vmeasure(m)
    assert v["homogeneity"] == pytest.approx(1.0)
    assert v["completeness"] == pytest.approx(0.0, abs=1e-9)


def test_clean_match_counts_only_one_to_one_flights(spark):
    m = _matched(spark, [
        ("T1", "F1", 10),                      # clean
        ("T2", "F2", 6), ("T3", "F2", 4),      # fragmented
        ("T4", "F3", 5), ("T4", "F4", 5),      # merged (two flights, one track)
    ])
    r = match_rates(m)
    assert r["n_flights"] == 4
    assert r["clean_match_pct"] == pytest.approx(25.0)
    assert r["fragmented_pct"] == pytest.approx(25.0)
    assert r["merged_pct"] == pytest.approx(50.0)


def test_a_dominant_track_with_a_stray_fragment_is_not_clean(spark):
    """The strict definition: one stray sample costs the clean match.

    T1 holds 9 of 10 samples, which any tolerant threshold would accept. It is
    still fragmented, because the stray sample is a second track and that is what
    breaks endpoint-based ADEP/ADES in production.
    """
    m = _matched(spark, [("T1", "F1", 9), ("T2", "F1", 1)])
    r = match_rates(m)
    assert r["clean_match_pct"] == pytest.approx(0.0)
    assert r["fragmented_pct"] == pytest.approx(100.0)


def test_the_three_outcomes_are_mutually_exclusive_and_sum_to_100(spark):
    """No flight may be counted twice. An earlier draft counted a 60/40 split as
    both clean and fragmented, which made the rates sum to more than 100."""
    m = _matched(spark, [
        ("T1", "F1", 10),
        ("T2", "F2", 6), ("T3", "F2", 4),
        ("T4", "F3", 5), ("T4", "F4", 5),
    ])
    r = match_rates(m)
    total = r["clean_match_pct"] + r["fragmented_pct"] + r["merged_pct"]
    assert total == pytest.approx(100.0)


def test_score_arm_returns_a_flat_row_of_scalars(spark):
    m = _matched(spark, [("T1", "F1", 10)])
    row = score_arm(m)
    assert set(row) >= {"v_measure", "homogeneity", "completeness",
                        "clean_match_pct", "fragmented_pct", "merged_pct",
                        "n_flights", "n_tracks"}
    assert all(isinstance(v, (int, float)) for v in row.values())
