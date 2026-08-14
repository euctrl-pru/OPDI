"""Tests for runway identification and ATOT/ALDT (T08, T17).

The runway name is the one part of this family APDF can score directly, via
``AP_C_RWY``, so these pin the naming logic; the timings are proxies whose bias
the benchmark is meant to measure rather than assume.
"""

import datetime as dt

import pytest
from pyspark.sql import functions as F

from conftest import make_track

from opdi.config import EventConfig
from opdi.pipeline.runways import detect_runway_movements, runway_thresholds

FT_PER_M = 3.28084
FTMIN_PER_MPS = 196.850394
KT_PER_MPS = 1.94384

# EBBR 07R/25L, roughly.
APT_LAT, APT_LON = 50.901, 4.484


class StubStorage:
    def __init__(self, tables):
        self._tables = tables

    def table_exists(self, name):
        return name in self._tables

    def read_table(self, name):
        return self._tables[name]


@pytest.fixture
def thresholds(spark):
    """Two runways: 07R/25L pointing 070/250, and 01/19 pointing 010/190."""
    # Offsets chosen so the true bearings really are 070 and 010: a degree of
    # longitude is only cos(50.9) = 0.63 of a degree of latitude here, and
    # ignoring that put the first fixture's "07R" at 077.
    rows = [
        (1, 1, "EBBR", False, "07R", APT_LAT - 0.003, APT_LON - 0.01305,
         "25L", APT_LAT + 0.003, APT_LON + 0.01305),
        (2, 1, "EBBR", False, "01", APT_LAT - 0.02, APT_LON - 0.0056,
         "19", APT_LAT + 0.02, APT_LON + 0.0056),
    ]
    rwy = spark.createDataFrame(
        rows,
        "id int, airport_ref int, airport_ident string, closed boolean, "
        "le_ident string, le_latitude_deg double, le_longitude_deg double, "
        "he_ident string, he_latitude_deg double, he_longitude_deg double",
    )
    return runway_thresholds(StubStorage({"oa_runways": rwy}))


def _ends(spark, role):
    return spark.createDataFrame(
        [("trk-1", "EBBR", role, APT_LAT, APT_LON, 184.0)],
        "track_id string, apt_ident string, role string, "
        "apt_lat double, apt_lon double, apt_elevation_ft double",
    )


def _movement(spark, heading, climbing=True, n=8):
    """A departure climbing away on `heading`, or an arrival descending on it."""
    rate = 2000 if climbing else -2000
    return make_track(
        spark,
        [
            {"t": i * 5, "lat": APT_LAT + i * 0.001, "lon": APT_LON + i * 0.001,
             "baro_altitude": (300 + i * 100) / FT_PER_M,
             "vert_rate": rate / FTMIN_PER_MPS,
             "velocity": 160 / KT_PER_MPS, "heading": heading}
            for i in range(n)
        ],
    ).withColumn("baro_altitude_c", F.col("baro_altitude"))


def test_thresholds_are_one_row_per_direction(thresholds):
    """A movement is reported against a direction, not a strip -- as APDF's
    AP_C_RWY is."""
    got = {r.rwy_ident for r in thresholds.collect()}

    assert got == {"07R", "25L", "01", "19"}


def test_bearings_come_from_the_threshold_positions(thresholds):
    """Not from le_heading_degT, which is often null and sometimes magnetic."""
    by_ident = {r.rwy_ident: r.rwy_bearing for r in thresholds.collect()}

    assert by_ident["07R"] == pytest.approx(70, abs=6)
    assert by_ident["25L"] == pytest.approx(250, abs=6)
    assert by_ident["01"] == pytest.approx(10, abs=6)


def test_a_departure_is_matched_to_the_runway_it_departed(spark, thresholds):
    got = detect_runway_movements(
        _movement(spark, heading=70), _ends(spark, "departure"), thresholds, EventConfig()
    ).collect()

    assert len(got) == 1
    assert got[0].rwy_ident == "07R"
    assert got[0].role == "departure"


def test_the_reciprocal_direction_is_distinguished(spark, thresholds):
    """070 and 250 share a strip; only the bearing separates them, and getting
    it backwards would name the opposite runway on every movement."""
    got = detect_runway_movements(
        _movement(spark, heading=250), _ends(spark, "departure"), thresholds, EventConfig()
    ).collect()

    assert got[0].rwy_ident == "25L"


def test_a_track_on_no_runway_bearing_is_not_matched(spark, thresholds):
    """Abstention beats a wrong runway: naming one corrupts a movement count,
    and 140 degrees is 70 off the nearest centreline."""
    got = detect_runway_movements(
        _movement(spark, heading=140), _ends(spark, "departure"), thresholds, EventConfig()
    ).collect()

    assert got == []


def test_an_arrival_needs_a_descent_not_a_climb(spark, thresholds):
    """The vertical-rate gate is signed by role, so a departure's samples
    cannot be read as an arrival's."""
    climbing = _movement(spark, heading=70, climbing=True)

    assert detect_runway_movements(
        climbing, _ends(spark, "arrival"), thresholds, EventConfig()
    ).collect() == []


def test_an_arrival_is_matched_and_timed_at_its_last_sample(spark, thresholds):
    descending = _movement(spark, heading=70, climbing=False)

    got = detect_runway_movements(
        descending, _ends(spark, "arrival"), thresholds, EventConfig()
    ).collect()

    assert got[0].rwy_ident == "07R"
    # ALDT is the latest surviving sample of the final descent.
    assert got[0].last_time > got[0].first_time


def test_taxiing_is_excluded_by_the_groundspeed_gate(spark, thresholds):
    slow = _movement(spark, heading=70).withColumn(
        "velocity", F.lit(10 / KT_PER_MPS)
    )

    assert detect_runway_movements(
        slow, _ends(spark, "departure"), thresholds, EventConfig()
    ).collect() == []


def test_too_few_samples_do_not_name_a_runway(spark, thresholds):
    """traffic requires four; one spurious sample should not name a runway."""
    short = _movement(spark, heading=70, n=3)

    assert detect_runway_movements(
        short, _ends(spark, "departure"), thresholds, EventConfig()
    ).collect() == []


def test_missing_reference_table_returns_none(spark):
    assert runway_thresholds(StubStorage({})) is None
