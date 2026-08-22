"""Arm C's aggregation: the study's discriminating measurement.

Arm C is what separates "the datum helped" from "Madrid happened". The
feasibility census showed each treatment band rests on a single aerodrome --
LEMD is 40% of `1500-3000`, LTAC 54% of `>3000` -- so the band mean alone
cannot tell those apart, and the leave-one-out column is what makes the claim
falsifiable rather than merely favourable.

Pure pandas, so it runs without Spark: the arithmetic is where a mistake would
quietly change the paper's conclusion, and it is worth testing directly.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks"))

import pandas as pd
import pytest

from elevation_arms import band_summary, per_airport_delta


def _per_airport(rows):
    """rows: (airport, run, role, n_truth, n_correct)"""
    return pd.DataFrame(
        [
            {"airport": a, "run": r, "role": role,
             "n_truth": t, "n_correct": c,
             "n_predicted_here": c, "n_assigned": c}
            for a, r, role, t, c in rows
        ]
    )


def _elev(pairs):
    return pd.DataFrame(
        [{"airport": a, "elevation_ft": e} for a, e in pairs],
        columns=["airport", "elevation_ft"],
    )


def test_delta_is_field_minus_msl():
    """Sign convention, asserted once. A flipped sign would turn a loss into
    the study's headline result and nothing else would catch it."""
    per_apt = _per_airport([
        ("LTAC", "datum_msl", "arrivals", 100, 40),
        ("LTAC", "datum_field", "arrivals", 100, 55),
    ])
    got = per_airport_delta(per_apt, _elev([("LTAC", 3125.0)]))
    row = got[got.airport == "LTAC"].iloc[0]

    assert row["n_correct_msl"] == 40
    assert row["n_correct_field"] == 55
    assert row["delta_correct"] == 15
    assert row["band"] == ">3000"


def test_band_summary_totals_its_members():
    per_apt = _per_airport([
        ("EHAM", "datum_msl", "arrivals", 500, 480),
        ("EHAM", "datum_field", "arrivals", 500, 480),
        ("LEMD", "datum_msl", "arrivals", 300, 200),
        ("LEMD", "datum_field", "arrivals", 300, 260),
        ("LTAC", "datum_msl", "arrivals", 100, 40),
        ("LTAC", "datum_field", "arrivals", 100, 55),
    ])
    elev = _elev([("EHAM", -11.0), ("LEMD", 1998.0), ("LTAC", 3125.0)])
    got = band_summary(per_apt, elev).set_index(["band", "role"])

    assert got.loc[("<500", "arrivals"), "delta_correct"] == 0       # control
    assert got.loc[("1500-3000", "arrivals"), "delta_correct"] == 60
    assert got.loc[(">3000", "arrivals"), "delta_correct"] == 15


def test_both_controls_collapse_when_one_aerodrome_carries_the_band():
    """The check the census forced.

    Two aerodromes: the busy one carries the entire gain, the other none. The
    band total looks like an elevation effect, and both controls must show it
    is not -- here they agree because the busiest field is also the mover.
    """
    per_apt = _per_airport([
        ("LEMD", "datum_msl", "arrivals", 300, 200),
        ("LEMD", "datum_field", "arrivals", 300, 260),   # +60, the whole gain
        ("LSZS", "datum_msl", "arrivals", 20, 15),
        ("LSZS", "datum_field", "arrivals", 20, 15),     # +0
    ])
    elev = _elev([("LEMD", 1998.0), ("LSZS", 1707.0)])
    row = band_summary(per_apt, elev).set_index(["band", "role"]).loc[
        ("1500-3000", "arrivals")
    ]

    assert row["delta_correct"] == 60
    assert row["largest_mover"] == "LEMD"
    assert row["delta_correct_loo"] == 0
    assert row["busiest"] == "LEMD"
    assert row["delta_correct_ex_busiest"] == 0


def test_leave_one_out_removes_the_mover_not_the_busiest():
    """The two controls answer different questions and must not be conflated.

    Here the *busiest* aerodrome (LEMD, 300 movements) gains less than the
    quieter one (LSZS, 200). Dropping the mover removes LSZS's +40; dropping
    the busiest removes LEMD's +30. Reporting one number for both would hide
    whichever case the reader actually cares about.
    """
    per_apt = _per_airport([
        ("LEMD", "datum_msl", "arrivals", 300, 200),
        ("LEMD", "datum_field", "arrivals", 300, 230),   # +30, busiest
        ("LSZS", "datum_msl", "arrivals", 200, 100),
        ("LSZS", "datum_field", "arrivals", 200, 140),   # +40, largest mover
    ])
    elev = _elev([("LEMD", 1998.0), ("LSZS", 1707.0)])
    row = band_summary(per_apt, elev).set_index(["band", "role"]).loc[
        ("1500-3000", "arrivals")
    ]

    assert row["delta_correct"] == 70
    assert row["largest_mover"] == "LSZS"
    assert row["delta_correct_loo"] == 30            # LSZS removed
    assert row["busiest"] == "LEMD"
    assert row["delta_correct_ex_busiest"] == 40     # LEMD removed


def test_a_loss_can_carry_a_band_too():
    """The mover is chosen by absolute value. An aerodrome losing heavily can
    dominate a band as surely as one gaining, and picking the largest positive
    contributor would leave that case invisible."""
    per_apt = _per_airport([
        ("LEMD", "datum_msl", "arrivals", 300, 260),
        ("LEMD", "datum_field", "arrivals", 300, 200),   # -60
        ("LSZS", "datum_msl", "arrivals", 200, 100),
        ("LSZS", "datum_field", "arrivals", 200, 110),   # +10
    ])
    elev = _elev([("LEMD", 1998.0), ("LSZS", 1707.0)])
    row = band_summary(per_apt, elev).set_index(["band", "role"]).loc[
        ("1500-3000", "arrivals")
    ]

    assert row["delta_correct"] == -50
    assert row["largest_mover"] == "LEMD"
    assert row["delta_correct_loo"] == 10


def test_an_aerodrome_without_an_elevation_is_banded_unknown():
    """Never silently folded into the control band -- see elevation_bands."""
    per_apt = _per_airport([
        ("ZZZZ", "datum_msl", "arrivals", 10, 5),
        ("ZZZZ", "datum_field", "arrivals", 10, 7),
    ])
    got = per_airport_delta(per_apt, _elev([]))
    assert got.iloc[0]["band"] == "unknown"


def test_a_band_present_in_only_one_arm_is_an_error():
    """A run missing from one arm would otherwise read as a delta equal to the
    whole of the other arm -- a spectacular fake result. Better to refuse."""
    per_apt = _per_airport([("LEMD", "datum_field", "arrivals", 300, 260)])
    with pytest.raises(ValueError, match="datum_msl"):
        band_summary(per_apt, _elev([("LEMD", 1998.0)]))
