"""Tests for step 04b -- the milestone columns on the flight list.

The producers are covered elsewhere (``test_events_rings.py``,
``test_events_processor.py``); these cover the reshaping: that the right event
answers each column, that a flight with no events survives, that the original
columns come through untouched, that a duplicate event resolves the same way
every time, and that a radius the configuration does not emit yields a null
column rather than an exception.
"""

import datetime as dt
import json

import pytest

from opdi.config import EventConfig
from opdi.pipeline.flight_list_milestones import (
    ADDED_COLUMNS,
    FLIGHT_LIST_RING_RADII_NM,
    enrich_flight_list,
)

_T0 = dt.datetime(2024, 6, 1, 10, 0, 0)

FLIGHT_SCHEMA = (
    "id string, ICAO24 string, FLT_ID string, dof timestamp, "
    "adep string, ades string, version string"
)
EVENT_SCHEMA = "track_id string, type string, event_time timestamp, info string, version string"


def _t(seconds: float) -> dt.datetime:
    return _T0 + dt.timedelta(seconds=seconds)


def _flights(spark, rows=None):
    rows = rows or [
        ("trk-1", "abc123", "BEL123", _T0, "EBBR", "EHAM", "flight_list_v0.0.2"),
    ]
    return spark.createDataFrame(rows, FLIGHT_SCHEMA)


def _event(track_id, type_, seconds, **info):
    return (track_id, type_, _t(seconds), json.dumps(info), "events_v0.3.0")


def _events(spark, rows):
    return spark.createDataFrame(rows, EVENT_SCHEMA)


def _row(df, id_="trk-1"):
    return df.filter(df["id"] == id_).collect()[0]


def _full_set(track_id="trk-1", adep="EBBR", ades="EHAM"):
    """One flight with every event the flight list reads."""
    rows = [
        _event(track_id, "AOBT", 0, stand_exit=str(_t(0))),
        _event(track_id, "ATOT", 600, runway="25R", runway_bearing_deg=247.4,
               apt_icao=adep, role="departure"),
        _event(track_id, "ALDT", 3600, rwy_ident="06", runway_bearing_deg=58.1,
               apt_icao=ades, traversal_class="arrival", milestone="T17"),
        _event(track_id, "AIBT", 4200, stand_entry=str(_t(4200))),
        _event(track_id, "exit-parking_position", -60, osm_ref="A12",
               osm_aeroway="parking_position", osm_airport=adep),
        _event(track_id, "entry-parking_position", 4300, osm_ref="D45",
               osm_aeroway="parking_position", osm_airport=ades),
    ]
    for i, nm in enumerate(FLIGHT_LIST_RING_RADII_NM):
        rows.append(_event(track_id, f"xing-{nm}nm", 700 + i,
                           direction="outbound", apt_icao=adep, crossing_seq=1))
        rows.append(_event(track_id, f"xing-{nm}nm", 3000 + i,
                           direction="inbound", apt_icao=ades, crossing_seq=1))
    return rows


def test_every_column_populated_for_a_complete_flight(spark):
    out = enrich_flight_list(_flights(spark), _events(spark, _full_set()), EventConfig())
    row = _row(out)

    assert row["ATOT"] == _t(600)
    assert row["ALDT"] == _t(3600)
    assert row["AOBT"] == _t(0)
    assert row["AIBT"] == _t(4200)
    assert row["RWY_DEP"] == "25R"
    # Read through the A-CDM spelling of the same field.
    assert row["RWY_ARR"] == "06"
    assert row["RWY_DEP_BEARING_DEG"] == pytest.approx(247.4)
    assert row["RWY_ARR_BEARING_DEG"] == pytest.approx(58.1)
    assert row["STND_DEP"] == "A12"
    assert row["STND_ARR"] == "D45"
    for i, nm in enumerate(FLIGHT_LIST_RING_RADII_NM):
        assert row[f"C{nm}_DEP"] == _t(700 + i), nm
        assert row[f"C{nm}_ARR"] == _t(3000 + i), nm


def test_flight_with_no_events_survives_with_nulls(spark):
    flights = _flights(spark, [
        ("trk-1", "abc123", "BEL123", _T0, "EBBR", "EHAM", "v"),
        ("trk-2", "def456", "KLM99", _T0, "EHAM", "EBBR", "v"),
    ])
    out = enrich_flight_list(flights, _events(spark, _full_set()), EventConfig())

    assert out.count() == 2
    orphan = _row(out, "trk-2")
    assert orphan["FLT_ID"] == "KLM99"
    assert all(orphan[c] is None for c in ADDED_COLUMNS)


def test_empty_event_frame_leaves_every_flight_intact(spark):
    flights = _flights(spark)
    empty = spark.createDataFrame([], EVENT_SCHEMA)
    out = enrich_flight_list(flights, empty, EventConfig())

    assert out.count() == 1
    assert all(_row(out)[c] is None for c in ADDED_COLUMNS)


def test_original_columns_are_untouched(spark):
    flights = _flights(spark)
    out = enrich_flight_list(flights, _events(spark, _full_set()), EventConfig())

    # Same names, same order, same types, and the added ones after them.
    assert out.columns == flights.columns + list(ADDED_COLUMNS)
    original = dict(flights.dtypes)
    assert {c: t for c, t in out.dtypes if c in original} == original
    before = flights.collect()[0]
    after = _row(out)
    for name in flights.columns:
        assert after[name] == before[name], name


def test_ring_columns_are_all_nullable_timestamps(spark):
    out = enrich_flight_list(_flights(spark), _events(spark, _full_set()), EventConfig())
    types = dict(out.dtypes)
    for name in ADDED_COLUMNS:
        if name.endswith("_BEARING_DEG"):
            assert types[name] == "double", name
        elif name.startswith("RWY_") or name.startswith("STND_"):
            assert types[name] == "string", name
        else:
            assert types[name] == "timestamp", name
    assert all(f.nullable for f in out.schema.fields if f.name in ADDED_COLUMNS)


def test_duplicate_milestones_resolve_earliest_for_departure_latest_for_arrival(spark):
    rows = [
        _event("trk-1", "ATOT", 600, runway="25R", runway_bearing_deg=247.4),
        _event("trk-1", "ATOT", 9000, runway="07L", runway_bearing_deg=67.4),
        _event("trk-1", "ALDT", 3600, runway="06", runway_bearing_deg=58.1),
        _event("trk-1", "ALDT", 9600, runway="24", runway_bearing_deg=236.0),
        _event("trk-1", "AOBT", 0),
        _event("trk-1", "AOBT", 8000),
        _event("trk-1", "AIBT", 4200),
        _event("trk-1", "AIBT", 9900),
        _event("trk-1", "exit-parking_position", -60, osm_ref="A12"),
        _event("trk-1", "exit-parking_position", 7800, osm_ref="B01"),
        _event("trk-1", "entry-parking_position", 4300, osm_ref="D45"),
        _event("trk-1", "entry-parking_position", 9950, osm_ref="E07"),
    ]
    row = _row(enrich_flight_list(_flights(spark), _events(spark, rows), EventConfig()))

    assert (row["ATOT"], row["AOBT"]) == (_t(600), _t(0))
    assert (row["ALDT"], row["AIBT"]) == (_t(9600), _t(9900))
    # The runway travels with the event the time came from, not with some other
    # event of the same type.
    assert (row["RWY_DEP"], row["RWY_ARR"]) == ("25R", "24")
    assert (row["RWY_DEP_BEARING_DEG"], row["RWY_ARR_BEARING_DEG"]) == (247.4, 236.0)
    assert (row["STND_DEP"], row["STND_ARR"]) == ("A12", "E07")


def test_repeated_ring_crossings_take_the_outermost_pair(spark):
    """A hold re-crosses the arrival ring; the last inbound is the one that
    precedes the landing, as APDF's ASMA entry does."""
    rows = [
        _event("trk-1", "xing-40nm", 500, direction="outbound", apt_icao="EBBR"),
        _event("trk-1", "xing-40nm", 800, direction="outbound", apt_icao="EBBR"),
        _event("trk-1", "xing-40nm", 3000, direction="inbound", apt_icao="EHAM"),
        _event("trk-1", "xing-40nm", 3300, direction="inbound", apt_icao="EHAM"),
    ]
    row = _row(enrich_flight_list(_flights(spark), _events(spark, rows), EventConfig()))
    assert row["C40_DEP"] == _t(500)
    assert row["C40_ARR"] == _t(3300)


def test_ring_direction_and_aerodrome_both_decide_the_leg(spark):
    """An inbound crossing of the *departure* ring is neither column.

    The event names the aerodrome, not the leg -- the same aerodrome can be a
    flight's origin and its destination -- so the leg is decided against the
    flight list's own ADEP/ADES.
    """
    rows = [
        _event("trk-1", "xing-40nm", 500, direction="inbound", apt_icao="EBBR"),
        _event("trk-1", "xing-40nm", 3000, direction="outbound", apt_icao="EHAM"),
        _event("trk-1", "xing-40nm", 3300, direction="inbound", apt_icao="LFPG"),
    ]
    row = _row(enrich_flight_list(_flights(spark), _events(spark, rows), EventConfig()))
    assert row["C40_DEP"] is None
    assert row["C40_ARR"] is None


def test_a_radius_the_config_does_not_emit_is_a_null_column(spark):
    config = EventConfig(ring_radii_nm=(40.0, 100.0))
    out = enrich_flight_list(_flights(spark), _events(spark, _full_set()), config)
    row = _row(out)

    # The shape does not move with the configuration.
    assert out.columns[-len(ADDED_COLUMNS):] == list(ADDED_COLUMNS)
    assert row["C40_ARR"] is not None and row["C100_ARR"] is not None
    for nm in (50, 60, 110, 120):
        assert row[f"C{nm}_ARR"] is None, nm
        assert row[f"C{nm}_DEP"] is None, nm


def test_no_radii_at_all_still_yields_the_twelve_columns(spark):
    config = EventConfig.legacy()
    assert tuple(config.ring_radii_nm) == ()
    row = _row(enrich_flight_list(_flights(spark), _events(spark, _full_set()), config))
    for nm in FLIGHT_LIST_RING_RADII_NM:
        assert row[f"C{nm}_ARR"] is None and row[f"C{nm}_DEP"] is None


def test_events_of_other_tracks_and_types_are_ignored(spark):
    rows = _full_set() + [
        _event("trk-9", "ATOT", 10, runway="99"),
        _event("trk-1", "top-of-climb", 900),
        _event("trk-1", "entry-taxiway", 120, osm_ref="T1"),
    ]
    out = enrich_flight_list(_flights(spark), _events(spark, rows), EventConfig())
    assert out.count() == 1
    row = _row(out)
    assert row["RWY_DEP"] == "25R"
    assert row["STND_DEP"] == "A12"


def test_a_missing_info_field_is_null_not_an_error(spark):
    rows = [_event("trk-1", "ATOT", 600, apt_icao="EBBR")]
    row = _row(enrich_flight_list(_flights(spark), _events(spark, rows), EventConfig()))
    assert row["ATOT"] == _t(600)
    assert row["RWY_DEP"] is None


def test_runway_bearing_is_null_when_the_key_predates_the_column(spark):
    """An event written before ``runway_bearing_deg`` existed carries no such
    key. That is the common case for anything already published, and it must
    read as an ordinary null rather than fail the whole enrichment."""
    rows = [
        _event("trk-1", "ATOT", 600, runway="25R", apt_icao="EBBR"),
        _event("trk-1", "ALDT", 3600, runway="06", apt_icao="EHAM"),
    ]
    row = _row(enrich_flight_list(_flights(spark), _events(spark, rows), EventConfig()))
    assert (row["RWY_DEP"], row["RWY_ARR"]) == ("25R", "06")
    assert row["RWY_DEP_BEARING_DEG"] is None
    assert row["RWY_ARR_BEARING_DEG"] is None


def test_enriching_twice_is_refused(spark):
    once = enrich_flight_list(_flights(spark), _events(spark, _full_set()), EventConfig())
    with pytest.raises(ValueError, match="already"):
        enrich_flight_list(once, _events(spark, _full_set()), EventConfig())
