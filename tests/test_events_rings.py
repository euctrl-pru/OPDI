"""Tests for the ASMA ring crossing events (KPI05, KPI08).

The detector itself is covered in ``test_crossings.py``; these cover the frame
built around it -- that the rings are taken from the flight's own ADEP/ADES,
that the crossing is interpolated, and that the bearing is computed from the
interpolated position rather than interpolated alongside it.
"""

import datetime as dt
import json

import pytest
from conftest import _EPOCH, make_track
from pyspark.sql import functions as F

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
                {
                    "t": i * 60,
                    "lat": EBBR_LAT + d / 60.0,
                    "lon": EBBR_LON,
                    "baro_altitude": 10000 / FT_PER_M,
                }
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


def test_an_arrival_crosses_every_configured_ring_inbound(spark, storage):
    """All six radii, from outside the widest.

    The approach starts at 130 NM so the 120 NM ring is genuinely crossed
    rather than straddled by the first sample, and the expectation is derived
    from ``ring_radii_nm`` rather than spelled out -- the set is a published
    contract and a test that repeats it by hand stops checking the moment the
    contract changes.
    """
    sdf = _approach(spark, [130, 115, 105, 90, 70, 55, 45, 35, 20, 10])

    rows = calculate_ring_crossing_events(sdf, MONTH, storage, EventConfig()).collect()
    got = sorted((r.type, json.loads(r.info)["direction"]) for r in _at(rows, "EBBR"))

    assert got == sorted((f"xing-{int(r)}nm", "inbound") for r in EventConfig().ring_radii_nm)


def test_the_departure_aerodrome_gets_its_own_rings(spark, storage):
    """Both ends, because KPI05's reference area is a cylinder at each."""
    sdf = _approach(spark, [120, 90, 60, 45, 35, 20, 10])

    rows = calculate_ring_crossing_events(sdf, MONTH, storage, EventConfig()).collect()

    assert {json.loads(r.info)["apt_icao"] for r in rows} == {"EBBR", "EHAM"}


def test_the_crossing_is_interpolated_not_snapped(spark, storage):
    sdf = _approach(spark, [120, 90, 60, 45, 35, 20, 10])
    # 30 NM a minute (1,800 kt) keeps the arithmetic exact; the speed limit,
    # tested below, is off here.
    config = EventConfig(crossing_max_implied_speed_kt=None)

    all_rows = calculate_ring_crossing_events(sdf, MONTH, storage, config).collect()
    rows = {r.type: r for r in _at(all_rows, "EBBR")}

    # 45 -> 35 NM straddles 40 at the midpoint of a 60 s step starting at
    # t=180, so the crossing is at t=210 -- neither bracketing sample.
    assert (rows["xing-40nm"].event_time - _EPOCH).total_seconds() == pytest.approx(210, abs=1)
    # 120 -> 90 crosses 100 two thirds of the way through the step from t=0.
    assert (rows["xing-100nm"].event_time - _EPOCH).total_seconds() == pytest.approx(40, abs=1)


def test_the_info_matches_apdfs_column_set(spark, storage):
    sdf = _approach(spark, [120, 90, 60, 45, 35, 20, 10])

    row = _at(
        calculate_ring_crossing_events(sdf, MONTH, storage, EventConfig()).collect(),
        "EBBR",
        "xing-40nm",
    )[0]
    info = json.loads(row.info)

    # C40_CROSS_{TIME,LAT,LON,FL} + C40_BEARING, column for column.
    assert set(info) >= {"crossing_seq", "direction", "apt_icao", "bearing", "flight_level"}
    assert info["apt_icao"] == "EBBR"
    assert info["flight_level"] == pytest.approx(100.0, abs=0.5)
    # Approaching from due north, so the aircraft bears 000 from the aerodrome.
    assert info["bearing"] == pytest.approx(0.0, abs=1.0) or info["bearing"] == pytest.approx(
        360.0, abs=1.0
    )


def test_a_departure_and_return_gives_both_directions(spark, storage):
    """Every crossing, not just the first -- a go-around leaves and re-enters."""
    sdf = _approach(spark, [35, 45, 60, 45, 35, 20])

    rows = _at(
        calculate_ring_crossing_events(sdf, MONTH, storage, EventConfig()).collect(),
        "EBBR",
        "xing-40nm",
    )
    info = [(json.loads(r.info)["crossing_seq"], json.loads(r.info)["direction"]) for r in rows]

    assert info == [(1, "outbound"), (2, "inbound")]


def test_a_track_holding_on_the_ring_emits_nothing(spark, storage):
    """The dead band again: an aircraft orbiting at 40 NM is not crossing it."""
    sdf = _approach(spark, [40.2, 39.9, 40.3, 39.8, 40.1, 39.9])

    rows = _at(
        calculate_ring_crossing_events(sdf, MONTH, storage, EventConfig()).collect(),
        "EBBR",
        "xing-40nm",
    )

    assert rows == []


def test_legacy_emits_no_ring_events_at_all(spark, storage):
    """They did not exist under events_v0.0.2."""
    sdf = _approach(spark, [120, 90, 60, 45, 35, 20, 10])

    assert calculate_ring_crossing_events(sdf, MONTH, storage, EventConfig.legacy()) is None


def test_missing_reference_tables_skip_rather_than_fail(spark):
    """A deployment without a flight list must not fail step 04."""
    sdf = _approach(spark, [120, 20])

    assert calculate_ring_crossing_events(sdf, MONTH, StubStorage({}), EventConfig()) is None


# ---------------------------------------------------------------------------
# Work package 5: position jumps, the altitude, the airports of the flight list
# ---------------------------------------------------------------------------

EHAM_LAT, EHAM_LON = 52.3086, 4.76389

#: Spot check xing-120nm#29 (TRA5T, LOWS to EHAM, track 60d69c0bd13c1390):
#: seconds after 08:16:54, latitude, longitude, altitude in metres. The
#: report at 70 s is a position glitch 3.5 NM back along the track; the next
#: one, 5 s later, is 5.7 NM further on (4,100 kt). The 120 NM circle around
#: EHAM was published inbound, outbound and inbound within 31 s.
TRA5T = [
    (0, 51.695376, 7.967224, 12192.0),
    (10, 51.707758, 7.944107, 12192.0),
    (20, 51.719330, 7.922405, 12192.0),
    (30, 51.729996, 7.902436, 12192.0),
    (40, 51.741531, 7.880909, 12192.0),
    (45, 51.748764, 7.867399, 12192.0),
    (50, 51.750726, 7.863693, 12192.0),
    (55, 51.761806, 7.843018, 12192.0),
    (70, 51.723038, 7.915427, 12176.8),
    (75, 51.785385, 7.798808, 12153.9),
    (80, 51.791832, 7.786789, 12123.4),
    (85, 51.797791, 7.775574, 12100.6),
    (90, 51.803696, 7.764513, 12077.7),
    (95, 51.806717, 7.758872, 12047.2),
    (100, 51.815964, 7.741501, 12024.4),
    (105, 51.821777, 7.730738, 11986.3),
]


def _rows_to_track(spark, rows):
    return (
        make_track(
            spark,
            [{"t": t, "lat": lat, "lon": lon, "baro_altitude": alt} for t, lat, lon, alt in rows],
        )
        .withColumn("baro_altitude_c", F.col("baro_altitude"))
        .withColumn("cumulative_distance_nm", F.lit(0.0))
        .withColumn("cumulative_time_s", F.lit(0).cast("long"))
    )


@pytest.fixture
def storage_eham(spark):
    flight_list = spark.createDataFrame(
        [("trk-1", dt.datetime(2024, 6, 1, 12, 0), "LOWS", "EHAM")],
        "id string, dof timestamp, adep string, ades string",
    )
    airports = spark.createDataFrame(
        [("EHAM", EHAM_LAT, EHAM_LON), ("LOWS", 47.7933, 13.0043)],
        "ident string, latitude_deg double, longitude_deg double",
    )
    return StubStorage({"opdi_flight_list": flight_list, "oa_airports": airports})


def test_a_position_glitch_does_not_give_a_third_crossing(spark, storage_eham):
    sdf = _rows_to_track(spark, TRA5T)

    rows = _at(
        calculate_ring_crossing_events(sdf, MONTH, storage_eham, EventConfig()).collect(),
        "EHAM",
        "xing-120nm",
    )

    # The jump back at 75 s (4,100 kt) gives no crossing; it still counts in
    # crossing_seq. The step back at 70 s is 3.5 NM in 15 s (850 kt), within
    # the limit, so the outbound crossing before it stays.
    got = [(json.loads(r.info)["direction"], json.loads(r.info)["crossing_seq"]) for r in rows]
    assert got == [("inbound", 1), ("outbound", 2)]


def test_without_the_speed_limit_the_glitch_gives_three_crossings(spark, storage_eham):
    sdf = _rows_to_track(spark, TRA5T)
    config = EventConfig(crossing_max_implied_speed_kt=None)

    rows = _at(
        calculate_ring_crossing_events(sdf, MONTH, storage_eham, config).collect(),
        "EHAM",
        "xing-120nm",
    )

    assert [json.loads(r.info)["direction"] for r in rows] == ["inbound", "outbound", "inbound"]


def test_a_vehicle_jumping_120_nm_fires_no_ring(spark, storage):
    """Spot check xing-40nm#42: a ground vehicle parked 0.78 NM from the
    airport; after 275 s without a position the next one is 121.5 NM away
    (1,600 kt). Every circle fired outbound within seconds."""
    parked = [(t, EBBR_LAT + 0.78 / 60, EBBR_LON, 0.0) for t in range(0, 60, 5)]
    away = [(335 + 5 * k, EBBR_LAT + (121.5 + 0.1 * k) / 60, EBBR_LON, 0.0) for k in range(4)]
    sdf = _rows_to_track(spark, parked + away)

    rows = _at(calculate_ring_crossing_events(sdf, MONTH, storage, EventConfig()).collect(), "EBBR")
    unlimited = _at(
        calculate_ring_crossing_events(
            sdf, MONTH, storage, EventConfig(crossing_max_implied_speed_kt=None)
        ).collect(),
        "EBBR",
    )

    assert rows == []
    assert len(unlimited) == 6


def test_a_ring_takes_the_altitude_of_its_report_from_its_last_known_value(spark, storage):
    """Spot check xing-40nm#05: the report after the crossing has a position
    but no altitude, so the crossing had no altitude. It now takes the
    altitude the report last had, 10 s earlier."""
    rows = [(0, EBBR_LAT + 42 / 60, EBBR_LON, 5700.0), (10, EBBR_LAT + 41 / 60, EBBR_LON, 5700.0)]
    rows += [(20, EBBR_LAT + 39 / 60, EBBR_LON, None), (30, EBBR_LAT + 38 / 60, EBBR_LON, 5600.0)]
    sdf = _rows_to_track(spark, rows)

    got = _at(
        calculate_ring_crossing_events(sdf, MONTH, storage, EventConfig()).collect(),
        "EBBR",
        "xing-40nm",
    )
    off = _at(
        calculate_ring_crossing_events(
            sdf, MONTH, storage, EventConfig(crossing_value_max_gap_seconds=None)
        ).collect(),
        "EBBR",
        "xing-40nm",
    )

    assert len(got) == 1 and got[0].altitude_ft == pytest.approx(5700.0 * FT_PER_M, abs=1)
    assert len(off) == 1 and off[0].altitude_ft is None


def test_an_altitude_older_than_the_limit_is_not_used(spark, storage):
    rows = [(0, EBBR_LAT + 42 / 60, EBBR_LON, 5700.0), (10, EBBR_LAT + 41 / 60, EBBR_LON, None)]
    rows += [(60, EBBR_LAT + 39 / 60, EBBR_LON, None), (70, EBBR_LAT + 38 / 60, EBBR_LON, 5600.0)]
    sdf = _rows_to_track(spark, rows)

    got = _at(
        calculate_ring_crossing_events(sdf, MONTH, storage, EventConfig()).collect(),
        "EBBR",
        "xing-40nm",
    )

    assert len(got) == 1 and got[0].altitude_ft is None


def test_the_airports_given_replace_those_of_the_flight_list(spark, storage):
    """Spot check xing-40nm#46: the rings must be around the airports the
    flight list publishes, which the caller passes after the runway-event
    correction."""
    sdf = _approach(spark, [120, 90, 60, 45, 35, 20, 10])
    airports = spark.createDataFrame(
        [("trk-1", "EBBR", "EBBR")], "id string, adep string, ades string"
    )

    rows = calculate_ring_crossing_events(
        sdf, MONTH, storage, EventConfig(), flight_airports=airports
    ).collect()

    assert {json.loads(r.info)["apt_icao"] for r in rows} == {"EBBR"}
