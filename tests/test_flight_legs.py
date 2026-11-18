"""Tests for one flight-list row per flight (``opdi.pipeline.flight_legs``, step 04b).

The shapes are those of real tracks of 14 June 2026 that hold several
flights: an out-and-back under one call sign, a rotation with two stops, a
turnaround without block times, a track that starts in the air, and the
cases that must stay one flight (a touch-and-go, a track seen only on the
ground).
"""

import datetime as dt
import json

from pyspark.sql import functions as F

from opdi.config import EventConfig
from opdi.output.published import build_flight_events, build_flight_list
from opdi.pipeline.flight_legs import event_in_leg, flight_legs, flight_stops, with_flight_rows
from opdi.pipeline.flight_list_milestones import (
    enrich_flight_list,
    enrich_flight_list_and_flights,
)
from opdi.versions import FLIGHT_LIST_LEGS_VERSION, FLIGHT_LIST_VERSION

_T0 = dt.datetime(2026, 6, 14, 6, 0, 0)

FLIGHT_SCHEMA = (
    "ID string, ICAO24 string, FLT_ID string, DOF date, ADEP string, ADES string, "
    "ADEP_P string, ADES_P string, ADEP_SOURCE string, ADES_SOURCE string, "
    "VERSION string, FIRST_SEEN timestamp, LAST_SEEN timestamp, REGISTRATION string, "
    "MODEL string, TYPECODE string, ICAO_AIRCRAFT_CLASS string, ICAO_OPERATOR string"
)
EVENT_SCHEMA = "flight_id string, type string, event_time timestamp, info string, version string"


def _t(seconds: float) -> dt.datetime:
    return _T0 + dt.timedelta(seconds=seconds)


def _flight(track, adep, ades, end_s, start_s=0.0, icao24="4b1806"):
    return (
        track,
        icao24,
        "SWR1234",
        _T0.date(),
        adep,
        ades,
        "LSZR" if adep else None,
        "LSZR" if ades else None,
        "aerodrome",
        "aerodrome",
        FLIGHT_LIST_VERSION,
        _t(start_s),
        _t(end_s),
        "HB-JBA",
        "A220-300",
        "BCS3",
        "L2J",
        "SWR",
    )


def _ev(track, type_, seconds, **info):
    return (track, type_, _t(seconds), json.dumps(info), "events_v0.4.0")


def _atot(track, s, apt):
    return _ev(track, "ATOT", s, apt_icao=apt, runway="16", method="acdm")


def _aldt(track, s, apt):
    return _ev(track, "ALDT", s, apt_icao=apt, runway="23", method="acdm")


def _ring(track, s, nm, apt, direction):
    return _ev(track, f"xing-{nm}nm", s, apt_icao=apt, direction=direction)


def _out_and_back(track="trk-ab"):
    """Zurich - Hamburg - Zurich under one call sign, 40 min at the stand in Hamburg."""
    return [
        _ev(track, "AOBT", 300, stand="E45", apt_icao="LSZH"),
        _atot(track, 900, "LSZH"),
        _ring(track, 1500, 40, "LSZH", "outbound"),
        _ring(track, 4200, 40, "EDDH", "inbound"),
        _aldt(track, 4800, "EDDH"),
        _ev(track, "AIBT", 5100, stand="C12", apt_icao="EDDH"),
        _ev(track, "AOBT", 7500, stand="C12", apt_icao="EDDH"),
        _atot(track, 8100, "EDDH"),
        _ring(track, 8700, 40, "EDDH", "outbound"),
        _ring(track, 11400, 40, "LSZH", "inbound"),
        _aldt(track, 12000, "LSZH"),
        _ev(track, "AIBT", 12300, stand="E47", apt_icao="LSZH"),
        _ev(track, "first_seen", 0),
        _ev(track, "last_seen", 12400),
    ]


def _enrich(spark, flights, events, config=None):
    return enrich_flight_list_and_flights(
        spark.createDataFrame(flights, FLIGHT_SCHEMA),
        spark.createDataFrame(events, EVENT_SCHEMA),
        config or EventConfig(),
    )


def _by_id(df):
    return {r["ID"]: r for r in df.collect()}


def test_an_out_and_back_gives_two_flights_with_their_own_airports_and_times(spark):
    tracks, flights = _enrich(spark, [_flight("trk-ab", "LSZH", "LSZH", 12400)], _out_and_back())
    rows = _by_id(flights)
    assert sorted(rows) == ["trk-ab-L1", "trk-ab-L2"]
    one, two = rows["trk-ab-L1"], rows["trk-ab-L2"]
    assert (one["ADEP"], one["ADES"], two["ADEP"], two["ADES"]) == ("LSZH", "EDDH", "EDDH", "LSZH")
    assert (one["ATOT"], one["ALDT"]) == (_t(900), _t(4800))
    assert (two["ATOT"], two["ALDT"]) == (_t(8100), _t(12000))
    assert (one["AOBT"], one["AIBT"], two["AOBT"], two["AIBT"]) == (
        _t(300),
        _t(5100),
        _t(7500),
        _t(12300),
    )
    assert (one["STND_ARR"], two["STND_DEP"]) == ("C12", "C12")
    # The circles of the stop are on the flight they belong to.
    assert (one["C40_DEP"], one["C40_ARR"]) == (_t(1500), _t(4200))
    assert (two["C40_DEP"], two["C40_ARR"]) == (_t(8700), _t(11400))
    # Divided at the in-block time in Hamburg.
    assert (one["FIRST_SEEN"], one["LAST_SEEN"]) == (_t(0), _t(5100))
    assert (two["FIRST_SEEN"], two["LAST_SEEN"]) == (_t(5100), _t(12400))
    assert (one["TRACK_ID"], one["LEG"], one["LEGS"]) == ("trk-ab", 1, 2)
    assert (two["LEG_START"], two["LEG_END"], one["LEG_START"]) == (_t(5100), None, None)
    assert (one["ADES_SOURCE"], two["ADEP_SOURCE"]) == ("runway_event", "runway_event")
    assert (one["ADEP_P"], one["ADES_P"], two["ADEP_P"], two["ADES_P"]) == (
        "LSZR",
        None,
        None,
        "LSZR",
    )
    assert one["MOVEMENT_DEP"] and one["MOVEMENT_ARR"] and two["MOVEMENT_DEP"]
    assert {r["VERSION"] for r in rows.values()} == {FLIGHT_LIST_LEGS_VERSION}
    # The track's own row stays, with the track's ends.
    track = _by_id(tracks)["trk-ab"]
    assert (track["ADEP"], track["ADES"], track["ATOT"], track["ALDT"]) == (
        "LSZH",
        "LSZH",
        _t(900),
        _t(12000),
    )


def test_a_rotation_with_two_stops_gives_three_flights(spark):
    trk = "trk-rot"
    events = [
        _atot(trk, 600, "EGLL"),
        _aldt(trk, 4200, "LFPG"),
        _atot(trk, 6600, "LFPG"),
        _aldt(trk, 9600, "EDDF"),
        _atot(trk, 12000, "EDDF"),
        _aldt(trk, 15000, "LIRF"),
    ]
    _, flights = _enrich(spark, [_flight(trk, "EGLL", "LIRF", 15300)], events)
    rows = _by_id(flights)
    got = [(rows[f"{trk}-L{k}"]["ADEP"], rows[f"{trk}-L{k}"]["ADES"]) for k in (1, 2, 3)]
    assert got == [("EGLL", "LFPG"), ("LFPG", "EDDF"), ("EDDF", "LIRF")]
    # No block times: divided at the midpoint of each stop.
    assert rows[f"{trk}-L2"]["FIRST_SEEN"] == _t(5400)
    assert rows[f"{trk}-L2"]["LAST_SEEN"] == _t(10800)
    assert {r["LEGS"] for r in rows.values()} == {3}


def test_a_touch_and_go_does_not_divide_the_track(spark):
    trk = "trk-tg"
    events = [
        _atot(trk, 0, "EDDK"),
        _aldt(trk, 3000, "EDLW"),
        _atot(trk, 3040, "EDLW"),
        _aldt(trk, 6000, "EDDL"),
    ]
    tracks, flights = _enrich(spark, [_flight(trk, "EDDK", "EDDL", 6100)], events)
    assert flights.count() == 0
    assert tracks.count() == 1


def test_a_turnaround_of_twenty_minutes_is_two_flights(spark):
    trk = "trk-20"
    events = [_atot(trk, 0, "LEPA"), _aldt(trk, 3600, "LEIB"), _atot(trk, 4800, "LEIB")]
    events.append(_aldt(trk, 7200, "LEPA"))
    _, flights = _enrich(spark, [_flight(trk, "LEPA", "LEPA", 7300)], events)
    rows = _by_id(flights)
    assert rows[f"{trk}-L1"]["LAST_SEEN"] == _t(4200)
    assert (rows[f"{trk}-L2"]["ADEP"], rows[f"{trk}-L2"]["ADES"]) == ("LEIB", "LEPA")


def test_a_stop_shorter_than_the_minimum_ground_time_is_one_flight(spark):
    trk = "trk-short"
    events = [_atot(trk, 0, "LEPA"), _aldt(trk, 3600, "LEIB"), _atot(trk, 3800, "LEIB")]
    _, flights = _enrich(spark, [_flight(trk, "LEPA", None, 7300)], events)
    assert flights.count() == 0
    longer = EventConfig(flight_stop_min_ground_s=120.0)
    _, flights = _enrich(spark, [_flight(trk, "LEPA", None, 7300)], events, longer)
    assert flights.count() == 2


def test_an_off_block_before_the_midpoint_starts_the_next_flight(spark):
    trk = "trk-ob"
    events = [
        _atot(trk, 0, "EBBR"),
        _aldt(trk, 3600, "EGLL"),
        _ev(trk, "AOBT", 3900, stand="231", apt_icao="EGLL"),
        _atot(trk, 6000, "EGLL"),
    ]
    _, flights = _enrich(spark, [_flight(trk, "EBBR", None, 9000)], events)
    rows = _by_id(flights)
    assert rows[f"{trk}-L1"]["LAST_SEEN"] == _t(3899)
    assert rows[f"{trk}-L2"]["AOBT"] == _t(3900)
    assert rows[f"{trk}-L1"]["AOBT"] is None


def test_a_track_that_starts_in_the_air_then_flies_two_flights(spark):
    trk = "trk-air"
    events = [
        _aldt(trk, 1200, "LOWW"),
        _ev(trk, "AIBT", 1500, stand="F01", apt_icao="LOWW"),
        _ev(trk, "AOBT", 4500, stand="F01", apt_icao="LOWW"),
        _atot(trk, 5000, "LOWW"),
        _aldt(trk, 8000, "LKPR"),
    ]
    _, flights = _enrich(spark, [_flight(trk, None, "LKPR", 8200)], events)
    rows = _by_id(flights)
    one, two = rows[f"{trk}-L1"], rows[f"{trk}-L2"]
    assert (one["ADEP"], one["ADES"], one["ATOT"], one["ALDT"]) == (None, "LOWW", None, _t(1200))
    assert (two["ADEP"], two["ADES"], two["ATOT"], two["ALDT"]) == (
        "LOWW",
        "LKPR",
        _t(5000),
        _t(8000),
    )
    assert one["ADEP_SOURCE"] == "aerodrome"


def test_a_track_seen_only_on_the_ground_keeps_one_row(spark):
    trk = "trk-gnd"
    events = [
        _ev(trk, "AOBT", 100, stand="A1", apt_icao="LFPG"),
        _ev(trk, "AIBT", 900, stand="A9", apt_icao="LFPG"),
    ]
    tracks, flights = _enrich(spark, [_flight(trk, "LFPG", "LFPG", 1000)], events)
    assert flights.count() == 0
    assert tracks.collect()[0]["VERSION"] == FLIGHT_LIST_LEGS_VERSION


def test_rows_of_tracks_with_one_flight_differ_only_by_the_version(spark):
    trk = "trk-one"
    events = [_atot(trk, 0, "EBBR"), _aldt(trk, 3600, "EHAM")]
    flight = [_flight(trk, "EBBR", "EHAM", 3700)]
    on, _ = _enrich(spark, flight, events)
    off = enrich_flight_list(
        spark.createDataFrame(flight, FLIGHT_SCHEMA),
        spark.createDataFrame(events, EVENT_SCHEMA),
        EventConfig(flight_list_one_row_per_flight=False),
    )
    a, b = on.collect()[0].asDict(), off.collect()[0].asDict()
    assert a.pop("VERSION") == FLIGHT_LIST_LEGS_VERSION
    assert b.pop("VERSION") == FLIGHT_LIST_VERSION
    assert a == b


def test_off_in_release_v002_and_when_switched_off(spark):
    for config in (EventConfig.legacy(), EventConfig(flight_list_one_row_per_flight=False)):
        tracks, flights = _enrich(
            spark, [_flight("trk-ab", "LSZH", "LSZH", 12400)], _out_and_back(), config
        )
        assert flights is None
        assert tracks.collect()[0]["VERSION"] == FLIGHT_LIST_VERSION


def test_enriching_the_enriched_rows_again_gives_the_same_flights(spark):
    tracks, flights = _enrich(spark, [_flight("trk-ab", "LSZH", "LSZH", 12400)], _out_and_back())
    again_tracks, again = enrich_flight_list_and_flights(
        tracks, spark.createDataFrame(_out_and_back(), EVENT_SCHEMA), EventConfig()
    )
    key = sorted(flights.columns)
    assert sorted(map(tuple, flights.select(key).collect())) == sorted(
        map(tuple, again.select(key).collect())
    )
    assert again_tracks.collect()[0]["VERSION"] == FLIGHT_LIST_LEGS_VERSION


def test_stops_and_legs_from_the_events_of_step_04(spark):
    # The detector frames of step 04 name the key track_id.
    events = spark.createDataFrame(_out_and_back(), EVENT_SCHEMA).withColumnRenamed(
        "flight_id", "track_id"
    )
    stop = flight_stops(events, EventConfig()).collect()
    assert [(s["landing_airport"], s["take_off_airport"], s["division_time"]) for s in stop] == [
        ("EDDH", "EDDH", _t(5100))
    ]
    legs = flight_legs(flight_stops(events, EventConfig())).orderBy("LEG").collect()
    assert [(r["LEG"], r["leg_adep"], r["leg_ades"]) for r in legs] == [
        (1, None, "EDDH"),
        (2, "EDDH", None),
    ]


def test_the_published_events_go_to_their_flight(spark):
    tracks, flights = _enrich(
        spark,
        [_flight("trk-ab", "LSZH", "LSZH", 12400), _flight("trk-one", "EBBR", "EHAM", 3700)],
        _out_and_back() + [_atot("trk-one", 0, "EBBR")],
    )
    rows = with_flight_rows(tracks, flights)
    published = build_flight_list(rows)
    assert sorted(r["flight_id"] for r in published.collect()) == [
        "trk-ab-L1",
        "trk-ab-L2",
        "trk-one",
    ]
    events = (
        spark.createDataFrame(_out_and_back() + [_atot("trk-one", 0, "EBBR")], EVENT_SCHEMA)
        .withColumn("id", F.concat("flight_id", F.lit(":"), "type", F.col("event_time")))
        .withColumn("longitude", F.lit(None).cast("double"))
        .withColumn("latitude", F.lit(None).cast("double"))
        .withColumn("altitude", F.lit(None).cast("double"))
        .withColumn("source", F.lit("OSN"))
        .withColumn("dof", F.lit(_T0.date()))
    )
    measurements = spark.createDataFrame(
        [], "id string, milestone_id string, type string, value double, version string"
    )
    out = build_flight_events(events, measurements, rows)
    got = {
        (r["type"], r["event_time"]): (r["flight_id"], r["adep"], r["ades"]) for r in out.collect()
    }
    assert got[("ALDT", _t(4800))] == ("trk-ab-L1", "LSZH", "EDDH")
    assert got[("AIBT", _t(5100))] == ("trk-ab-L1", "LSZH", "EDDH")
    assert got[("AOBT", _t(7500))] == ("trk-ab-L2", "EDDH", "LSZH")
    assert got[("last_seen", _t(12400))] == ("trk-ab-L2", "EDDH", "LSZH")
    assert got[("first_seen", _t(0))] == ("trk-ab-L1", "LSZH", "EDDH")
    assert got[("ATOT", _t(0))] == ("trk-one", "EBBR", "EHAM")
    assert out.count() == len(_out_and_back()) + 1


def test_a_track_ending_in_the_air_does_not_send_its_last_flight_back_to_the_stop(spark):
    # FIN8NP: lands at Helsinki, takes off again, coverage ends in the climb;
    # the reports near the end named Helsinki as the destination.
    trk = "trk-end-air"
    events = [_atot(trk, 0, "EFOU"), _aldt(trk, 3600, "EFHK"), _atot(trk, 6000, "EFHK")]
    _, flights = _enrich(spark, [_flight(trk, "EFOU", "EFHK", 6600)], events)
    rows = _by_id(flights)
    last = rows[f"{trk}-L2"]
    assert (last["ADEP"], last["ADES"], last["ADES_SOURCE"]) == ("EFHK", None, "undetermined")
    assert rows[f"{trk}-L1"]["ADES"] == "EFHK"


def test_switching_the_setting_off_gives_the_track_rows_their_own_version_again(spark):
    # Step 04b run again without the setting, on rows it wrote with it: the
    # list then holds one row per track, and the flight rows the earlier run
    # left in the working table are not used.
    tracks, flights = _enrich(spark, [_flight("trk-ab", "LSZH", "LSZH", 12400)], _out_and_back())
    stale = spark.createDataFrame(flights.collect(), flights.schema)
    off, none = enrich_flight_list_and_flights(
        spark.createDataFrame(tracks.collect(), tracks.schema),
        spark.createDataFrame(_out_and_back(), EVENT_SCHEMA),
        EventConfig(flight_list_one_row_per_flight=False),
    )
    assert none is None
    assert off.collect()[0]["VERSION"] == FLIGHT_LIST_VERSION
    assert [r["ID"] for r in with_flight_rows(off, stale).collect()] == ["trk-ab"]


def test_a_go_around_and_a_rejected_take_off_do_not_divide_the_track(spark):
    # A go-around has no landing; a rejected take-off has a take-off roll but
    # no take-off. Neither gives a landing followed by a take-off.
    trk = "trk-ga"
    events = [
        _atot(trk, 600, "EHAM"),
        _ev(trk, "go-around", 4000, apt_icao="EGLL"),
        _aldt(trk, 5200, "EGLL"),
        _ev(trk, "AIBT", 5500, stand="B3", apt_icao="EGLL"),
        _ev(trk, "AOBT", 9000, stand="B3", apt_icao="EGLL"),
        _ev(trk, "take-off-roll", 9600, apt_icao="EGLL"),
        _ev(trk, "AIBT", 10500, stand="B3", apt_icao="EGLL"),
    ]
    tracks, flights = _enrich(spark, [_flight(trk, "EHAM", "EGLL", 10600)], events)
    assert flights.count() == 0
    assert [r["ID"] for r in with_flight_rows(tracks, flights).collect()] == [trk]


def test_two_landings_at_one_instant_give_the_same_stop_in_any_row_order(spark):
    trk = "trk-tie"
    events = [_aldt(trk, 3600, "EDDM"), _aldt(trk, 3600, "EDMO"), _atot(trk, 6000, "EDMO")]
    got = set()
    for rows in (events, events[::-1]):
        frame = spark.createDataFrame(rows, EVENT_SCHEMA).repartition(1)
        got.add(tuple(s["landing_airport"] for s in flight_stops(frame, EventConfig()).collect()))
    assert got == {("EDMO",)}


def test_every_event_of_a_track_belongs_to_exactly_one_flight(spark):
    # Also an event at a division time, and one without a time.
    trk = "trk-rot"
    events = [
        _atot(trk, 600, "EGLL"),
        _aldt(trk, 4200, "LFPG"),
        _ev(trk, "AIBT", 4500, stand="K1", apt_icao="LFPG"),
        _ev(trk, "entry-parking_position", 4500, apt_icao="LFPG"),
        _ev(trk, "exit-parking_position", 4501, apt_icao="LFPG"),
        _atot(trk, 6600, "LFPG"),
        _aldt(trk, 9600, "EDDF"),
        _atot(trk, 12000, "EDDF"),
        (trk, "level-start", None, "{}", "events_v0.4.0"),
    ]
    _, flights = _enrich(spark, [_flight(trk, "EGLL", None, 12600)], events)
    limits = flights.select("ID", "TRACK_ID", "LEG_START", "LEG_END")
    ev = spark.createDataFrame(events, EVENT_SCHEMA)
    joined = ev.join(
        limits,
        (F.col("flight_id") == F.col("TRACK_ID"))
        & event_in_leg(F.col("event_time"), F.col("LEG_START"), F.col("LEG_END")),
        "left",
    )
    got = sorted((r["type"], r["event_time"], r["ID"]) for r in joined.collect())
    assert len(got) == len(events)
    by = {(t, e): i for t, e, i in got}
    assert by[("AIBT", _t(4500))] == f"{trk}-L1"
    assert by[("entry-parking_position", _t(4500))] == f"{trk}-L1"
    assert by[("exit-parking_position", _t(4501))] == f"{trk}-L2"
    assert by[("level-start", None)] == f"{trk}-L1"
    assert by[("ATOT", _t(12000))] == f"{trk}-L3"


def test_step_04b_writes_the_flight_rows_and_keeps_nothing_in_memory(spark, tmp_path):
    from opdi.config import OPDIConfig
    from opdi.pipeline.flight_legs import LEGS_TABLE
    from opdi.pipeline.flight_list_step import enrich_day
    from opdi.utils.storage import StorageManager

    config = OPDIConfig.for_environment("opensky", warehouse=str(tmp_path / "wh"))
    storage = StorageManager(spark, config)
    day = _T0.date()
    flights = spark.createDataFrame([_flight("trk-ab", "LSZH", "LSZH", 12400)], FLIGHT_SCHEMA)
    events = spark.createDataFrame(_out_and_back(), EVENT_SCHEMA).withColumn("dof", F.lit(day))
    storage.write_table(flights, "opdi_flight_list", mode="overwrite", partition_by=["DOF"])
    storage.write_table(events, "opdi_flight_events", mode="overwrite", partition_by=["dof"])

    spark.catalog.clearCache()
    assert enrich_day(storage, config.events, day, day) == 1
    assert spark._jsparkSession.sharedState().cacheManager().isEmpty()

    rows = {r["ID"]: r for r in storage.read_table(LEGS_TABLE).collect()}
    assert sorted(rows) == ["trk-ab-L1", "trk-ab-L2"]
    assert {r["DOF"] for r in rows.values()} == {day}
    track = storage.read_table("opdi_flight_list").collect()
    assert [(r["ID"], r["VERSION"]) for r in track] == [("trk-ab", FLIGHT_LIST_LEGS_VERSION)]
