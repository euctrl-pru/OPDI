"""The plausibility checks of step 04: events the aircraft's own track contradicts.

Each check gets an event it removes and its nearest neighbour that it keeps
(just inside the threshold), and the cases where removing would be wrong:
unknown facts, a high airport, a touch-and-go and a track with two legs, a
slow helicopter.
"""

import datetime as dt
import json

from opdi.config import EventConfig
from opdi.pipeline.plausibility import RULES, check_events

T0 = dt.datetime(2026, 6, 14, 10, 0, 0)
FT_PER_M = 3.28084
KT_PER_MPS = 1.94384

EVENT_DDL = "track_id string, type string, event_time timestamp, info string"
TRACK_DDL = (
    "track_id string, event_time timestamp, lat double, lon double, "
    "baro_altitude_c double, velocity double, vert_rate double, on_ground boolean"
)
AIRPORT_DDL = "ident string, latitude_deg double, longitude_deg double, elevation_ft int"
FLIGHT_DDL = "id string, adep string, ades string"

#: A low airport and one at 5,000 ft, 60 NM apart.
AIRPORTS = [("LOW", 50.0, 4.0, 100), ("HIGH", 51.0, 4.0, 5000)]


def _t(s):
    return T0 + dt.timedelta(seconds=s)


def _event(type_, at, track="t1", **info):
    return (track, type_, None if at is None else _t(at), json.dumps(info))


def _reports(around, alt_ft, kt, on_ground=False, track="t1", lat=50.0):
    """Reports every 5 s within 60 s of ``around``, all in one state."""
    alt_m = None if alt_ft is None else alt_ft / FT_PER_M
    gs = None if kt is None else kt / KT_PER_MPS
    return [(track, _t(around + d), lat, 4.0, alt_m, gs, 0.0, on_ground) for d in range(-60, 61, 5)]


def _check(spark, events, tracks, flights=(("t1", "LOW", "HIGH"),), config=None):
    kept, rejected = check_events(
        spark.createDataFrame(events, EVENT_DDL),
        spark.createDataFrame(tracks, TRACK_DDL),
        spark.createDataFrame(AIRPORTS, AIRPORT_DDL),
        config or EventConfig(),
        flights=spark.createDataFrame(list(flights), FLIGHT_DDL),
    )
    kept_rows = {(r.type, r.event_time) for r in kept.collect()}
    rejected_rows = {(r.type, r.event_time): r for r in rejected.collect()}
    return kept_rows, rejected_rows


def test_the_rules_are_applied_in_the_published_order():
    assert [r.name for r in RULES] == [
        "missing_time",
        "airborne_ground_event",
        "block_after_takeoff",
        "block_before_landing",
    ]


def test_the_settings():
    c = EventConfig()
    assert c.plausibility_checks is True
    assert c.plausibility_max_ground_event_height_ft == 1500.0
    assert c.plausibility_max_ground_event_altitude_ft == 15000.0
    assert c.plausibility_max_ground_event_speed_kt == 80.0
    assert c.plausibility_context_window_s == 60
    assert EventConfig.legacy().plausibility_checks is False


# --- missing_time -----------------------------------------------------------


def test_an_event_without_a_time_is_removed(spark):
    kept, rejected = _check(spark, [_event("xing-40nm", None), _event("xing-40nm", 0)], [])

    assert kept == {("xing-40nm", _t(0))}
    assert rejected[("xing-40nm", None)].reject_rule == "missing_time"


# --- airborne_ground_event --------------------------------------------------


def test_an_off_block_time_in_flight_is_removed(spark):
    """1,601 ft above LOW (100 ft) and 81 kt, no report on the ground."""
    kept, rejected = _check(spark, [_event("AOBT", 0)], _reports(0, 1601, 81))

    assert kept == set()
    row = rejected[("AOBT", _t(0))]
    assert row.reject_rule == "airborne_ground_event"
    detail = json.loads(row.reject_detail)
    assert detail["airport"] == "LOW"
    assert round(detail["height_ft"]) == 1501
    assert detail["on_ground_share"] == 0.0


def test_just_inside_the_height_the_event_is_kept(spark):
    kept, _ = _check(spark, [_event("AOBT", 0)], _reports(0, 1599, 81))
    assert kept == {("AOBT", _t(0))}


def test_just_inside_the_speed_the_event_is_kept(spark):
    kept, _ = _check(spark, [_event("AOBT", 0)], _reports(0, 3000, 79))
    assert kept == {("AOBT", _t(0))}


def test_one_report_on_the_ground_in_the_window_keeps_the_event(spark):
    """A garbled altitude at a stand: high and fast, but the transponder says
    ground in the same minute."""
    tracks = _reports(0, 3000, 200)
    tracks[3] = tracks[3][:-1] + (True,)
    kept, _ = _check(spark, [_event("AIBT", 0)], tracks)
    assert kept == {("AIBT", _t(0))}


def test_a_layout_event_over_the_airport_is_removed(spark):
    """An overflight crossing the cells of a taxiway at 3,000 ft and 250 kt."""
    kept, rejected = _check(
        spark, [_event("entry-taxiway", 0, osm_airport="LOW")], _reports(0, 3000, 250)
    )
    assert rejected[("entry-taxiway", _t(0))].reject_rule == "airborne_ground_event"


def test_the_runway_family_is_not_checked_against_the_track_height(spark):
    events = [_event(t, 0) for t in ("line-up", "take-off-roll", "runway-vacated", "ATOT", "ALDT")]
    events.append(_event("landing", 0))
    kept, rejected = _check(spark, events, _reports(0, 30000, 450))
    assert rejected == {}


def test_an_unknown_airport_needs_an_altitude_above_every_airport(spark):
    """No flight-list airports: the height is unknown, the altitude decides."""
    tracks = _reports(0, 15001, 300) + _reports(1000, 14999, 300)
    kept, rejected = _check(
        spark, [_event("AOBT", 0), _event("AOBT", 1000)], tracks, flights=[("t1", None, None)]
    )
    assert set(rejected) == {("AOBT", _t(0))}
    assert kept == {("AOBT", _t(1000))}


# --- the cases where removing would be wrong (Review Focus) ----------------


def test_unknown_context_keeps_the_event(spark):
    """No report near the event; altitude blanked by cleaning; speed unknown;
    on-ground flag unknown. Each keeps the event."""
    events = [_event("AOBT", t) for t in (0, 1000, 2000, 3000)]
    tracks = (
        _reports(1000, None, 300)
        + _reports(2000, 30000, None)
        + [r[:-1] + (None,) for r in _reports(3000, 30000, 300)]
    )
    kept, rejected = _check(spark, events, tracks)
    assert rejected == {}
    assert len(kept) == 4


def test_ground_events_at_an_airport_at_5000_ft_are_kept(spark):
    """At HIGH (5,000 ft) the aircraft taxis at 6,000 ft barometric altitude and
    lifts off at 140 kt: 1,000 ft above the airport at most, so it is kept."""
    events = [
        _event("AIBT", 0),
        _event("exit-taxiway", 600, osm_airport="HIGH"),
        _event("entry-runway", 650, osm_airport="HIGH"),
    ]
    tracks = _reports(0, 5500, 15, lat=51.0) + _reports(650, 6000, 140, lat=51.0)
    kept, rejected = _check(spark, events, tracks)
    assert rejected == {}


def test_a_touch_and_go_and_a_two_leg_track_keep_their_events(spark):
    """t1: a touch-and-go (landing at 100 s, take-off at 130 s) in a circuit.
    t2: lands at 1,000 s, in-block at 1,400 s, off-block at 4,000 s, take-off
    at 4,500 s. Every runway and block time is kept."""
    events = [
        _event("ALDT", 100),
        _event("ATOT", 130),
        _event("ALDT", 1000, track="t2"),
        _event("AIBT", 1400, track="t2"),
        _event("AOBT", 4000, track="t2"),
        _event("ATOT", 4500, track="t2"),
    ]
    tracks = (
        _reports(100, 150, 120, on_ground=True)
        + _reports(1400, 120, 5, on_ground=True, track="t2")
        + _reports(4000, 120, 3, on_ground=True, track="t2")
    )
    kept, rejected = _check(
        spark, events, tracks, flights=[("t1", "LOW", "LOW"), ("t2", "LOW", "LOW")]
    )
    assert rejected == {}
    assert len(kept) == 6


def test_a_40_kt_helicopter_keeps_its_ground_events(spark):
    """A helicopter leaving a raised pad climbs through 1,700 ft at 40 kt:
    slow, so its stand exit and off-block time are kept."""
    events = [_event("AOBT", 0), _event("exit-parking_position", 10, osm_airport="LOW")]
    kept, rejected = _check(spark, events, _reports(0, 1800, 40))
    assert rejected == {}


# --- block_after_takeoff / block_before_landing ----------------------------


def test_block_times_on_the_wrong_side_of_the_runway_times_are_removed(spark):
    events = [
        _event("AOBT", 600),  # at the take-off: removed
        _event("ATOT", 600),
        _event("AOBT", 599, track="t2"),  # one second before: kept
        _event("ATOT", 600, track="t2"),
        _event("ALDT", 3600),
        _event("AIBT", 3600),  # at the landing: removed
        _event("ALDT", 3600, track="t2"),
        _event("AIBT", 3601, track="t2"),  # one second after: kept
    ]
    flights = [("t1", "LOW", "HIGH"), ("t2", "LOW", "HIGH")]
    kept, rejected = _check(spark, events, [], flights=flights)

    assert {k: r.reject_rule for k, r in rejected.items()} == {
        ("AOBT", _t(600)): "block_after_takeoff",
        ("AIBT", _t(3600)): "block_before_landing",
    }
    assert ("AOBT", _t(599)) in kept and ("AIBT", _t(3601)) in kept


def test_an_off_block_before_a_later_take_off_is_kept(spark):
    """A track that departs, lands and departs again: the off-block time of
    the second departure lies after the first take-off and before the last."""
    events = [_event("ATOT", 0), _event("ALDT", 3600), _event("AOBT", 6000), _event("ATOT", 6600)]
    kept, rejected = _check(spark, events, [])
    assert rejected == {}


def test_the_checks_off_return_the_events_unchanged(spark):
    events = [_event("xing-40nm", None), _event("AOBT", 0)]
    kept, rejected = _check(
        spark, events, _reports(0, 30000, 450), config=EventConfig(plausibility_checks=False)
    )
    assert rejected == {}
    assert len(kept) == 2
