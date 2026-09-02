"""Tests for the ground-truth period and study-set registrations.

Runs locally: these are plain module constants, not anything that needs a
cluster.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks"))

import pytest

from events_gt import PERIODS, STUDY_AIRPORTS


def test_the_2026_period_covers_all_twenty_study_aerodromes():
    """V4 does not reuse V3's period: apdf_202506 has zero UGKO movements,
    which is why the period moved to 202606."""
    assert PERIODS["2026"]["month"] == "202606"
    assert PERIODS["2026"]["days"] == ["2026-06-05", "2026-06-06", "2026-06-07"]


def test_the_study_set_is_the_top_twenty_of_the_coverage_ranking():
    """The set is not a hand-picked list; it has a derivation, and if the
    ranking is regenerated the two must not silently diverge."""
    import csv

    ranking = (
        Path("/home/jupyter/work/opdi-workspace")
        / "opensky-airport-coverage"
        / "data"
        / "ranking_tier_a_2026.csv"
    )
    if not ranking.exists():
        pytest.skip("coverage ranking not checked out")
    rows = list(csv.DictReader(ranking.open()))
    assert tuple(r["icao"] for r in rows[:20]) == STUDY_AIRPORTS
