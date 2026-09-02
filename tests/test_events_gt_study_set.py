"""Tests for the ground-truth period, study-set registration, and the
study-airports filter.

The period/study-set tests are plain module constants and run without Spark.
The filter tests use the local ``spark`` fixture (session-scoped, UTC, no
cluster -- see ``tests/conftest.py``) and stub ``bridged``-shaped DataFrames
directly, since building one via ``bridge()`` needs real APDF/flights
parquet.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks"))

import datetime as dt

import pytest
from pyspark.sql import functions as F

from events_gt import PERIODS, STUDY_AIRPORTS, _filter_study_airports, milestones


def test_the_2026_period_covers_all_twenty_study_aerodromes():
    """V4 does not reuse V3's period: apdf_202506 has zero UGKO movements,
    which is why the period moved to 202606."""
    assert PERIODS["2026"]["month"] == "202606"
    assert PERIODS["2026"]["days"] == ["2026-06-05", "2026-06-06", "2026-06-07"]


def test_the_study_set_is_the_top_twenty_of_the_coverage_ranking():
    """The set is not a hand-picked list; it has a derivation, and if the
    ranking is regenerated the two must not silently diverge.

    The ranking CSV lives in a sibling checkout
    (``opensky-airport-coverage/data/ranking_tier_a_2026.csv``), not this
    repo, so its path is found by walking upward from this file rather than
    hardcoded: a plain checkout and a worktree checkout (e.g.
    ``opdi/.claude/worktrees/<name>/``) nest at different depths under the
    workspace root, and a fixed relative offset breaks in one of the two.
    """
    import csv

    ranking = None
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "opensky-airport-coverage" / "data" / "ranking_tier_a_2026.csv"
        if candidate.exists():
            ranking = candidate
            break
    if ranking is None:
        pytest.skip("coverage ranking not checked out")
    rows = list(csv.DictReader(ranking.open()))
    assert tuple(r["icao"] for r in rows[:20]) == STUDY_AIRPORTS


def test_filter_study_airports_restricts_to_the_study_set(spark):
    """The shared helper both milestones() and ring_truth() route through."""
    df = spark.createDataFrame(
        [("EBBR",), ("EHAM",)], "gt_airport string"
    )

    study = _filter_study_airports(df, "study")
    assert sorted(r.gt_airport for r in study.collect()) == ["EBBR"]

    unfiltered = _filter_study_airports(df, "all")
    assert sorted(r.gt_airport for r in unfiltered.collect()) == ["EBBR", "EHAM"]


def test_milestones_routes_through_the_study_airports_filter(spark):
    """End-to-end through milestones(), not just the helper in isolation:
    EBBR is in STUDY_AIRPORTS, EHAM is not."""
    rows = [
        (
            "abc111", "TEST01 ", None, None, None, "DEP",
            dt.datetime(2026, 6, 5, 10, 0, 0), dt.datetime(2026, 6, 5, 9, 50, 0),
            "EBBR", "LFPG", "25R",
        ),
        (
            "abc222", "TEST02 ", None, None, None, "DEP",
            dt.datetime(2026, 6, 5, 11, 0, 0), dt.datetime(2026, 6, 5, 10, 50, 0),
            "EHAM", "LFPG", "18C",
        ),
    ]
    bridged = spark.createDataFrame(
        rows,
        "icao24 string, callsign string, gt_aobt timestamp, gt_adep string, "
        "gt_ades string, SRC_PHASE string, MVT_TIME_UTC timestamp, "
        "BLOCK_TIME_UTC timestamp, ADEP_ICAO string, ADES_ICAO string, "
        "AP_C_RWY string",
    )

    study = milestones(bridged, days=None, airports="study")
    assert set(r.gt_airport for r in study.collect()) == {"EBBR"}

    everything = milestones(bridged, days=None, airports="all")
    assert set(r.gt_airport for r in everything.collect()) == {"EBBR", "EHAM"}
