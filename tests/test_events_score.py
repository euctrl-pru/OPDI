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

from events_score import (
    align,
    guard_not_all_zero,
    score,
    score_by_airport,
    score_by_truth_resolution,
    score_runways,
)

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


def _aligned_two_airports(spark):
    """An already-aligned frame spanning two aerodromes, one row each, so the
    only thing under test is that `score_by_airport` groups on `gt_airport` in
    addition to `milestone` rather than pooling every aerodrome together."""
    return spark.createDataFrame(
        [("EBBR", "ATOT", 5.0), ("LSZH", "ATOT", -3.0)],
        "gt_airport string, milestone string, error_s double",
    )


def _aligned_with_n(spark, airport, n_detected):
    """An already-aligned frame for one aerodrome with exactly `n_detected`
    rows, each carrying a non-null `error_s` -- so `n_detected` (rows with a
    detection) and `n_truth` (all rows) are both `n_detected`, i.e. 100%
    coverage at whatever sample size the test wants to probe the floor with."""
    return spark.createDataFrame(
        [(airport, "ATOT", float(i)) for i in range(n_detected)],
        "gt_airport string, milestone string, error_s double",
    )


def test_per_airport_scores_split_by_aerodrome(spark):
    out = score_by_airport(_aligned_two_airports(spark))
    assert {r["gt_airport"] for r in out.collect()} == {"EBBR", "LSZH"}


def test_a_cell_below_the_floor_is_marked_unreportable_not_dropped(spark):
    """Dropping it would make a detector that failed at an aerodrome look like
    an aerodrome that was never studied. The row stays; `reportable` is False
    and the paper renders a dash."""
    out = score_by_airport(_aligned_with_n(spark, airport="UGKO", n_detected=3))
    row = [r for r in out.collect() if r["gt_airport"] == "UGKO"][0]
    assert row["reportable"] is False
    assert row["n_detected"] == 3


def test_coverage_is_reported_even_where_percentiles_are_not(spark):
    """Coverage is a ratio of counts and is meaningful at any n; a median of
    three errors is not."""
    out = score_by_airport(_aligned_with_n(spark, airport="UGKO", n_detected=3))
    assert [r for r in out.collect() if r["gt_airport"] == "UGKO"][0]["coverage_pct"] is not None


def test_the_scorer_and_the_event_table_agree_on_identity(spark):
    """The integration gap that unit tests missed.

    The event table keys on `flight_id`; ground truth keys on
    `(icao24, callsign, day)`. The first ladder run spent 68 minutes computing
    events and then died because nothing bridged the two. These tests fed
    `align` synthetic frames that already carried `icao24`, so they proved the
    arithmetic and never the schemas.

    This pins the contract: whatever `detected_events` returns must carry the
    join keys `align` uses.
    """
    import inspect

    from event_bench import detected_events

    src = inspect.getsource(detected_events)
    for key in ("icao24", "callsign", "day"):
        assert key in src, (
            f"detected_events must resolve {key!r}; align() joins on it and a "
            f"missing key fails only after the events have been computed"
        )


def _aligned_two_airports_two_resolutions(spark):
    """An already-aligned frame where one aerodrome reports to the second and
    the other does not -- which is the real shape: `gt_subminute` is a property
    of the aerodrome's reporting system, so it is nearly constant within an
    aerodrome and varies between them."""
    return spark.createDataFrame(
        [
            ("EBBR", "ATOT", True, 5.0),
            ("EBBR", "ATOT", True, -4.0),
            ("EBBR", "ALDT", True, 2.0),
            ("LSZH", "ATOT", False, 31.0),
        ],
        "gt_airport string, milestone string, gt_subminute boolean, error_s double",
    )


def test_truth_resolution_can_be_split_per_aerodrome(spark):
    """Annex A needs to say *which* aerodromes report to the second. The
    default grouping pools them, so the network figure is an average over
    aerodromes that are individually at 0% or 100%."""
    out = score_by_truth_resolution(
        _aligned_two_airports_two_resolutions(spark),
        group_cols=("gt_airport", "milestone", "gt_subminute"),
    ).collect()

    cells = {(r["gt_airport"], r["milestone"], r["gt_subminute"]): r for r in out}
    assert set(cells) == {
        ("EBBR", "ATOT", True),
        ("EBBR", "ALDT", True),
        ("LSZH", "ATOT", False),
    }
    assert cells[("EBBR", "ATOT", True)]["n_truth"] == 2
    assert cells[("LSZH", "ATOT", False)]["n_truth"] == 1


def test_the_default_grouping_is_what_it_always_was(spark):
    """Existing callers -- V3's chain among them -- must see the network-level
    split unchanged."""
    out = score_by_truth_resolution(
        _aligned_two_airports_two_resolutions(spark)
    ).collect()

    assert {(r["milestone"], r["gt_subminute"]) for r in out} == {
        ("ATOT", True), ("ATOT", False), ("ALDT", True)
    }
    assert "gt_airport" not in out[0].asDict()


def test_a_resolution_split_without_the_resolution_is_refused(spark):
    """It would be `score()` under a name that promises something else, and
    the caller would report quantisation-free figures that are nothing of the
    kind."""
    with pytest.raises(ValueError, match="gt_subminute"):
        score_by_truth_resolution(
            _aligned_two_airports_two_resolutions(spark),
            group_cols=("gt_airport", "milestone"),
        )
