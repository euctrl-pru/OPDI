"""Tests for the event benchmark scorer.

Runs locally: the scorer is the part of the benchmark that can be validated
without a cluster, and it is the part where a mistake would quietly change
every number in the paper.
"""

import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks"))

import pytest
from pyspark.sql import functions as F

from events_score import align, guard_not_all_zero, score, score_runways

T0 = dt.datetime(2024, 6, 5, 10, 0, 0)
DAY = dt.date(2024, 6, 5)


def _truth(spark, rows):
    """rows: (icao24, callsign, milestone, gt_time, gt_runway, gt_subminute)"""
    return spark.createDataFrame(
        [(i, c, DAY, m, t, r, s) for i, c, m, t, r, s in rows],
        "icao24 string, callsign string, day date, milestone string, "
        "gt_time timestamp, gt_runway string, gt_subminute boolean",
    )


def _detected(spark, rows):
    """rows: (icao24, callsign, milestone, event_time, det_runway)"""
    return spark.createDataFrame(
        [(i, c, DAY, m, t, r) for i, c, m, t, r in rows],
        "icao24 string, callsign string, day date, milestone string, "
        "event_time timestamp, det_runway string",
    )


def test_a_missing_detection_counts_against_coverage(spark):
    """Ground truth is the denominator: a flight never seen is a miss, not an
    absent row. An inner join here would drop exactly the failures measured."""
    truth = _truth(spark, [
        ("abc123", "DLH1", "ATOT", T0, "07R", True),
        ("def456", "DLH2", "ATOT", T0, "07R", True),
    ])
    detected = _detected(spark, [("abc123", "DLH1", "ATOT", T0, "07R")])

    got = score(align(truth, detected)).collect()[0]

    assert got["n_truth"] == 2
    assert got["n_detected"] == 1
    assert got["coverage_pct"] == 50.0


def test_bias_is_signed_and_median(spark):
    """A detector consistently early is a different animal from one scattered
    about zero, and the sign is what says which."""
    truth = _truth(spark, [(f"a{i}", f"C{i}", "ATOT", T0, None, True) for i in range(5)])
    detected = _detected(spark, [
        (f"a{i}", f"C{i}", "ATOT", T0 - dt.timedelta(seconds=8), None) for i in range(5)
    ])

    got = score(align(truth, detected)).collect()[0]

    assert got["bias_s"] == pytest.approx(-8, abs=0.5)
    assert got["mad_s"] == pytest.approx(8, abs=0.5)


def test_one_wild_detection_does_not_move_the_bias(spark):
    """Median, not mean: a detection that landed on the wrong flight carries an
    error of hours and would drag a mean anywhere."""
    rows = [(f"a{i}", f"C{i}", "ATOT", T0, None, True) for i in range(9)]
    truth = _truth(spark, rows)
    detected = _detected(spark, [
        (f"a{i}", f"C{i}", "ATOT", T0 + dt.timedelta(seconds=2), None) for i in range(8)
    ] + [("a8", "C8", "ATOT", T0 + dt.timedelta(hours=6), None)])

    got = score(align(truth, detected)).collect()[0]

    assert got["bias_s"] == pytest.approx(2, abs=0.5)


def test_the_nearest_detection_is_kept_when_several_match(spark):
    """A go-around gives two ALDT candidates; scoring on an arbitrary one would
    measure the ordering rather than the detector."""
    truth = _truth(spark, [("abc123", "DLH1", "ALDT", T0, None, True)])
    detected = _detected(spark, [
        ("abc123", "DLH1", "ALDT", T0 + dt.timedelta(minutes=9), None),
        ("abc123", "DLH1", "ALDT", T0 + dt.timedelta(seconds=3), None),
    ])

    got = score(align(truth, detected)).collect()[0]

    assert got["bias_s"] == pytest.approx(3, abs=0.5)
    assert got["n_truth"] == 1


def test_hit_rates_are_measured_against_truth_not_detections(spark):
    """Otherwise a detector that answers once, perfectly, scores 100%."""
    truth = _truth(spark, [(f"a{i}", f"C{i}", "ATOT", T0, None, True) for i in range(4)])
    detected = _detected(spark, [("a0", "C0", "ATOT", T0, None)])

    got = score(align(truth, detected)).collect()[0]

    assert got["within_30s"] == 1
    assert got["within_30s_pct"] == 25.0


def test_runway_match_is_exact_not_fuzzy(spark):
    """07R and 07L are different runways; a fuzzy match would hide the error
    most worth finding."""
    truth = _truth(spark, [
        ("a0", "C0", "ATOT", T0, "07R", True),
        ("a1", "C1", "ATOT", T0, "07L", True),
    ])
    detected = _detected(spark, [
        ("a0", "C0", "ATOT", T0, "07r"),   # case only -- a match
        ("a1", "C1", "ATOT", T0, "07R"),   # wrong runway -- not a match
    ])

    got = score_runways(align(truth, detected)).collect()[0]

    assert got["n_named"] == 2
    assert got["n_exact"] == 1


def test_a_table_of_zeros_is_refused(spark):
    """Version 6 shipped exactly this and exited 0. Zero coverage on every
    milestone is an identity-join failure, not a result."""
    truth = _truth(spark, [("a0", "C0", "ATOT", T0, None, True)])
    empty = _detected(spark, []).filter(F.lit(False))

    with pytest.raises(SystemExit, match="identity-join"):
        guard_not_all_zero(score(align(truth, empty)))
