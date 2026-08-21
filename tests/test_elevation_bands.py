"""Boundary tests for the field-elevation banding.

The banding is the axis Arm C of the v6.1 study is read along: if a boundary is
off by one, an aerodrome lands in the wrong band and the study's central claim
is measured against the wrong population. Cheap to get right, expensive to get
wrong silently, so the boundaries are asserted rather than eyeballed.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks"))

import pytest
from pyspark.sql import functions as F

from elevation_bands import BANDS, elevation_band


def _band_of(spark, elev):
    df = spark.createDataFrame([(elev,)], "elev double")
    return df.select(elevation_band(F.col("elev")).alias("b")).first()["b"]


def test_bands_are_contiguous_and_ordered():
    """No gap and no overlap: every elevation falls in exactly one band."""
    for (_, _, hi), (_, lo_next, _) in zip(BANDS, BANDS[1:]):
        assert hi == lo_next
    assert BANDS[0][1] == float("-inf")
    assert BANDS[-1][2] == float("inf")


@pytest.mark.parametrize(
    "elev,expected",
    [
        (-11.0, "<500"),        # Schiphol, below sea level
        (0.0, "<500"),
        (499.9, "<500"),
        (500.0, "500-1500"),    # boundary belongs to the upper band
        (1499.9, "500-1500"),
        (1500.0, "1500-3000"),
        (1998.0, "1500-3000"),  # Madrid
        (2999.9, "1500-3000"),
        (3000.0, ">3000"),
        (5763.0, ">3000"),      # Erzurum
    ],
)
def test_band_boundaries(spark, elev, expected):
    assert _band_of(spark, elev) == expected


def test_a_null_elevation_bands_as_unknown(spark):
    """An aerodrome OurAirports has no elevation for must be visible as such.

    Folding it into `<500` would inflate the very band the study uses as its
    control, and hide how much of the sample the reference data cannot place.
    """
    assert _band_of(spark, None) == "unknown"
