"""Tests for the ASMA ring crossing events (KPI05, KPI08).

The detector itself is covered in ``test_crossings.py``; these cover the frame
built around it -- that the rings are taken from the flight's own ADEP/ADES,
that the crossing is interpolated, and that the bearing is computed from the
interpolated position rather than interpolated alongside it.
"""

import datetime as dt
import json

import pytest
from pyspark.sql import functions as F

from conftest import _EPOCH, make_track

from opdi.config import EventConfig
from opdi.pipeline.events import calculate_ring_crossing_events

FT_PER_M = 3.28084
EBBR_LAT, EBBR_LON = 50.901, 4.484
MONTH = dt.date(2024, 6, 1)


class StubStorage:
    """Just enough StorageManager for the ring detector."""

    def __init__(self, tables):
        self._tables = tables

    def table_exists(self, name):
        return name in self._tables

    def read_table(self, name):
        return self._tables[name]


@pytest.fixture
def storage(spark):
    flight_list = spark.createDataFrame(
        [("trk-1", dt.datetime(2024, 6, 1, 12, 0), "EHAM", "EBBR")],
        "id string, dof timestamp, adep string, ades string",
    )
    airports = spark.createDataFrame(
        [("EBBR", EBBR_LAT, EBBR_LON), ("EHAM", 52.309, 4.764)],
        "ident string, latitude_deg double, longitude_deg double",
    )
    return StubStorage({"opdi_flight_list": flight_list, "oa_airports": airports})


def _approach(spark, distances_nm):
    """An arrival approaching EBBR from due north.

    One nautical mile is 1/60 of a degree of latitude, so a distance is exact
    by construction and the interpolated crossing has an arithmetic answer.
    """
    return (
        make_track(
            spark,
            [
                {"t": i * 60, "lat": EBBR_LAT + d / 60.0, "lon": EBBR_LON,
                 "baro_altitude": 10000 / FT_PER_M}
                for i, d in enumerate(distances_nm)
            ],
        )
        .withColumn("baro_altitude_c", F.col("baro_altitude"))
        .withColumn("cumulative_distance_nm", F.lit(0.0))
        .withColumn("cumulative_time_s", F.lit(0).cast("long"))
    )


def _at(rows, apt, type_=None):
    """Crossings of one aerodrome's rings, in time order.

    Filtering matters: a flight has two ends, and the detector builds rings
    around both -- which is what KPI05 asks for, since its reference area is a
    cylinder at origin *and* destination. An unfiltered assertion silently mixes
    the departure aerodrome's rings into the arrival's.
    """
    out = [r for r in rows if json.loads(r.info)["apt_icao"] == apt]
    if type_:
        out = [r for r in out if r.type == type_]
    return sorted(out, key=lambda r: r.event_time)


def test_an_arrival_crosses_both_rings_inbound(spark, storage):
    sdf = _approach(spark, [120, 90, 60, 45, 35, 20, 10])

    rows = calculate_ring_crossing_events(sdf, MONTH, storage, EventConfig()).collect()
    got = sorted((r.type, json.loads(r.info)["direction"]) for r in _at(rows, "EBBR"))

    assert got == [("xing-100nm", "inbound"), ("xing-40nm", "inbound")]


def test_the_departure_aerodrome_gets_its_own_rings(spark, storage):
    """Both ends, because KPI05's reference area is a cylinder at each."""
    sdf = _approach(spark, [120, 90, 60, 45, 35, 20, 10])

    rows = calculate_ring_crossing_events(sdf, MONTH, storage, EventConfig()).collect()

    assert {json.loads(r.info)["apt_icao"] for r in rows} == {"EBBR", "EHAM"}


def test_the_crossing_is_interpolated_not_snapped(spark, storage):
    sdf = _approach(spark, [120, 90, 60, 45, 35, 20, 10])

    all_rows = calculate_ring_crossing_events(sdf, MONTH, storage, EventConfig()).collect()
    rows = {r.type: r for r in _at(all_rows, "EBBR")}

    # 45 -> 35 NM straddles 40 at the midpoint of a 60 s step starting at
    # t=180, so the crossing is at t=210 -- neither bracketing sample.
    assert (rows["xing-40nm"].event_time - _EPOCH).total_seconds() == pytest.approx(210, abs=1)
    # 120 -> 90 crosses 100 two thirds of the way through the step from t=0.
    assert (rows["xing-100nm"].event_time - _EPOCH).total_seconds() == pytest.approx(40, abs=1)


def test_the_info_matches_apdfs_column_set(spark, storage):
    sdf = _approach(spark, [120, 90, 60, 45, 35, 20, 10])

    row = _at(calculate_ring_crossing_events(
        sdf, MONTH, storage, EventConfig()).collect(), "EBBR", "xing-40nm")[0]
    info = json.loads(row.info)

    # C40_CROSS_{TIME,LAT,LON,FL} + C40_BEARING, column for column.
    assert set(info) >= {"crossing_seq", "direction", "apt_icao", "bearing", "flight_level"}
    assert info["apt_icao"] == "EBBR"
    assert info["flight_level"] == pytest.approx(100.0, abs=0.5)
    # Approaching from due north, so the aircraft bears 000 from the aerodrome.
    assert info["bearing"] == pytest.approx(0.0, abs=1.0) or info["bearing"] == pytest.approx(360.0, abs=1.0)


def test_a_departure_and_return_gives_both_directions(spark, storage):
    """Every crossing, not just the first -- a go-around leaves and re-enters."""
    sdf = _approach(spark, [35, 45, 60, 45, 35, 20])

    rows = _at(calculate_ring_crossing_events(
        sdf, MONTH, storage, EventConfig()).collect(), "EBBR", "xing-40nm")
    info = [(json.loads(r.info)["crossing_seq"], json.loads(r.info)["direction"])
            for r in rows]

    assert info == [(1, "outbound"), (2, "inbound")]


def test_a_track_holding_on_the_ring_emits_nothing(spark, storage):
    """The dead band again: an aircraft orbiting at 40 NM is not crossing it."""
    sdf = _approach(spark, [40.2, 39.9, 40.3, 39.8, 40.1, 39.9])

    rows = _at(calculate_ring_crossing_events(
        sdf, MONTH, storage, EventConfig()).collect(), "EBBR", "xing-40nm")

    assert rows == []


def test_legacy_emits_no_ring_events_at_all(spark, storage):
    """They did not exist under events_v0.0.2."""
    sdf = _approach(spark, [120, 90, 60, 45, 35, 20, 10])

    assert calculate_ring_crossing_events(sdf, MONTH, storage, EventConfig.legacy()) is None


def test_missing_reference_tables_skip_rather_than_fail(spark):
    """A deployment without a flight list must not fail step 04."""
    sdf = _approach(spark, [120, 20])

    assert calculate_ring_crossing_events(sdf, MONTH, StubStorage({}), EventConfig()) is None
