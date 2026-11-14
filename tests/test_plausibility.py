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
from opdi.pipeline.runway_ops import RUNWAY_THRESHOLD_CROSSING

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
        "runway_time_too_high",
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
    assert c.plausibility_max_runway_time_height_ft == 1500.0
    assert c.plausibility_runway_time_window_s == 10.0
    assert c.plausibility_pressure_allowance_ft == 1000.0
    assert c.plausibility_ground_reference_window_s == 1800.0
    assert c.plausibility_ground_reference_radius_nm == 5.0
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
    """Line-up, take-off roll, runway vacated and the threshold crossing are
    found on the runway's own heights; only the take-off and landing times
    have a height limit (``runway_time_too_high``)."""
    events = [_event(t, 0) for t in ("line-up", "take-off-roll", "runway-vacated")]
    events.append(_event(RUNWAY_THRESHOLD_CROSSING, 0))
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


def _iso(s):
    """A time as the block time detector writes it in ``info``."""
    return _t(s).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _block(type_, at, bound_s=None, method="acdm", track="t1"):
    """A block time with the take-off or landing that bounds it, as the
    detector writes them (``info.take_off`` or ``info.landing``)."""
    if bound_s is None:
        return _event(type_, at, track=track)
    key = "take_off" if type_ in ("AOBT", "off-block") else "landing"
    return _event(type_, at, track=track, **{key: _iso(bound_s), f"{key}_method": method})


def _merged_then_checked(spark, events, flights=(("t1", "LOW", "LOW"),)):
    """Step 04's order: one ATOT and one ALDT per track, then the checks."""
    from opdi.pipeline.events import merge_milestone_duplicates

    merged = merge_milestone_duplicates(spark.createDataFrame(events, EVENT_DDL), EventConfig())
    kept, rejected = check_events(
        merged,
        spark.createDataFrame([], TRACK_DDL),
        spark.createDataFrame(AIRPORTS, AIRPORT_DDL),
        EventConfig(),
        flights=spark.createDataFrame(list(flights), FLIGHT_DDL),
    )
    return (
        {(r.type, r.event_time) for r in kept.collect()},
        {(r.type, r.event_time): r.reject_rule for r in rejected.collect()},
    )


def test_a_second_departure_keeps_its_off_block_after_the_merge(spark):
    """Departs at 0 s, lands at 3,600 s, in-block at 3,900 s, off-block at
    6,000 s, take-off at 6,600 s, lands at 9,000 s. The merge keeps one ATOT
    (0 s) and one ALDT (3,600 s); each block time is compared with the
    take-off or landing of its own leg, which the detector wrote beside it."""
    events = [
        _event("ATOT", 0, method="acdm"),
        _event("ALDT", 3600, method="acdm"),
        _block("AIBT", 3900, bound_s=3600),
        _block("AOBT", 6000, bound_s=6600),
        _event("ATOT", 6600, method="acdm"),
        _event("ALDT", 9000, method="acdm"),
        _block("AIBT", 9300, bound_s=9000),
    ]
    kept, rejected = _merged_then_checked(spark, events)

    assert rejected == {}
    assert {("AOBT", _t(6000)), ("AIBT", _t(3900)), ("AIBT", _t(9300))} <= kept


def test_a_first_arrival_keeps_its_in_block_when_the_merge_keeps_the_other_landing(spark):
    """As above, but the first landing comes from the runway window method and
    the second from the runway milestones: the merge keeps the second
    (9,000 s), which lies after the in-block time of the first arrival."""
    events = [
        _event("ATOT", 0, method="acdm"),
        _event("ALDT", 3600, method="legacy"),
        _block("AIBT", 3900, bound_s=3600, method="legacy"),
        _block("AOBT", 6000, bound_s=6600, method="legacy"),
        _event("ATOT", 6600, method="legacy"),
        _event("ALDT", 9000, method="acdm"),
        _block("AIBT", 9300, bound_s=9000),
    ]
    kept, rejected = _merged_then_checked(spark, events)

    assert rejected == {}
    assert {("AOBT", _t(6000)), ("AIBT", _t(3900)), ("AIBT", _t(9300))} <= kept


def test_block_times_on_the_wrong_side_of_their_own_runway_time_are_removed(spark):
    """An off-block time at its take-off and an in-block time at its landing
    are removed; one second inside, each is kept."""
    events = [
        _block("AOBT", 600, bound_s=600),
        _block("AOBT", 599, bound_s=600, track="t2"),
        _block("AIBT", 3600, bound_s=3600),
        _block("AIBT", 3601, bound_s=3600, track="t2"),
    ]
    flights = [("t1", "LOW", "HIGH"), ("t2", "LOW", "HIGH")]
    kept, rejected = _check(spark, events, [], flights=flights)

    assert {k: r.reject_rule for k, r in rejected.items()} == {
        ("AOBT", _t(600)): "block_after_takeoff",
        ("AIBT", _t(3600)): "block_before_landing",
    }
    detail = json.loads(rejected[("AOBT", _t(600))].reject_detail)
    assert detail["take_off_method"] == "acdm"
    assert ("AOBT", _t(599)) in kept and ("AIBT", _t(3601)) in kept


def test_a_block_time_without_its_runway_time_is_not_judged_by_the_order(spark):
    """No take-off or landing beside it (none found in its leg, or the runway
    family was not computed in that call): the order checks keep it, even
    when another take-off of the track lies before it."""
    events = [_event("ATOT", 0), _event("ALDT", 3600), _block("AOBT", 6000), _block("AIBT", 10)]
    kept, rejected = _check(spark, events, [])
    assert rejected == {}


def test_the_checks_off_return_the_events_unchanged(spark):
    events = [_event("xing-40nm", None), _event("AOBT", 0)]
    kept, rejected = _check(
        spark, events, _reports(0, 30000, 450), config=EventConfig(plausibility_checks=False)
    )
    assert rejected == {}
    assert len(kept) == 2


# --- runway_time_too_high --------------------------------------------------


def _ground_reports(at, alt_ft, track="t1", lat=50.0):
    """Reports flagged on the ground at the airport, every 30 s for 5 minutes
    from ``at``: the track's own altitude reading on the ground."""
    return [
        (track, _t(at + d), lat, 4.0, alt_ft / FT_PER_M, 5 / KT_PER_MPS, 0.0, True)
        for d in range(0, 300, 30)
    ]


def test_a_take_off_time_too_high_above_the_track_s_own_ground_level_is_removed(spark):
    """The track's reports on the ground at LOW read 100 ft. 1,601 ft above
    that is removed, 1,499 ft is kept: a time close to the ground, though not
    on it, is kept."""
    events = [_event("ATOT", 0, apt_icao="LOW"), _event("ALDT", 1000, apt_icao="LOW")]
    tracks = (
        _ground_reports(-600, 100)
        + _reports(0, 1701, 160)
        + _reports(1000, 1599, 140)
        + _ground_reports(1100, 100)
    )
    kept, rejected = _check(spark, events, tracks)

    assert {k: r.reject_rule for k, r in rejected.items()} == {
        ("ATOT", _t(0)): "runway_time_too_high"
    }
    assert ("ALDT", _t(1000)) in kept
    detail = json.loads(rejected[("ATOT", _t(0))].reject_detail)
    assert detail["airport"] == "LOW" and round(detail["height_ft"]) == 1601
    assert detail["reference"] == "ground_reports" and detail["limit_ft"] == 1500.0


def test_without_ground_reports_the_limit_allows_for_the_pressure(spark):
    """No report of the track on the ground: the height is measured above the
    airport's elevation and the limit is 1,500 + 1,000 ft, as barometric
    altitude reads about 27 ft high per hPa below standard pressure. 2,501 ft
    is removed, 2,499 ft kept."""
    events = [_event("ATOT", 0, apt_icao="LOW"), _event("ALDT", 1000, apt_icao="LOW")]
    tracks = _reports(0, 2601, 160) + _reports(1000, 2599, 140)
    kept, rejected = _check(spark, events, tracks)

    assert set(rejected) == {("ATOT", _t(0))}
    detail = json.loads(rejected[("ATOT", _t(0))].reject_detail)
    assert detail["reference"] == "airport_elevation" and detail["limit_ft"] == 2500.0
    assert ("ALDT", _t(1000)) in kept


def test_a_correct_take_off_time_on_a_low_pressure_day_is_kept(spark):
    """At 975 hPa a true 500 ft above LOW reads about 1,530 ft above it.
    Without ground reports the allowance keeps the time; with them, the
    track's own reading on the ground (also about 1,030 ft high) cancels the
    error."""
    error = 1030
    events = [
        _event("ATOT", 0, apt_icao="LOW", method="legacy"),
        _event("ATOT", 0, track="t2", apt_icao="LOW", method="legacy"),
    ]
    tracks = (
        _reports(0, 100 + 500 + error, 150)
        + _ground_reports(-900, 100 + error, track="t2")
        + _reports(0, 100 + 500 + error, 150, track="t2")
    )
    kept, rejected = _check(
        spark, events, tracks, flights=[("t1", "LOW", "HIGH"), ("t2", "LOW", "HIGH")]
    )
    assert rejected == {}


def test_a_take_off_time_4000_ft_above_the_airport_is_removed_with_either_reference(spark):
    events = [
        _event("ATOT", 0, apt_icao="LOW"),
        _event("ATOT", 0, track="t2", apt_icao="LOW"),
    ]
    tracks = (
        _reports(0, 4100, 250)
        + _ground_reports(-900, 100, track="t2")
        + _reports(0, 4100, 250, track="t2")
    )
    kept, rejected = check_events(
        spark.createDataFrame(events, EVENT_DDL),
        spark.createDataFrame(tracks, TRACK_DDL),
        spark.createDataFrame(AIRPORTS, AIRPORT_DDL),
        EventConfig(),
        flights=spark.createDataFrame([("t1", "LOW", "HIGH"), ("t2", "LOW", "HIGH")], FLIGHT_DDL),
    )
    got = {r.track_id: json.loads(r.reject_detail)["reference"] for r in rejected.collect()}
    assert got == {"t1": "airport_elevation", "t2": "ground_reports"}
    assert kept.count() == 0


def _runway_check(spark, tracks):
    events = [_event("ATOT", 0, apt_icao="LOW")]
    return _check(spark, events, tracks)


def test_the_height_is_interpolated_between_the_reports_on_both_sides(spark):
    """No report at the time itself: 2,000 ft 5 s before, 4,000 ft 5 s after,
    so 3,000 ft at the time, 2,900 ft above LOW, removed (limit 2,500 ft)."""
    tracks = [
        ("t1", _t(-5), 50.0, 4.0, 2000 / FT_PER_M, 160 / KT_PER_MPS, 0.0, False),
        ("t1", _t(5), 50.0, 4.0, 4000 / FT_PER_M, 160 / KT_PER_MPS, 0.0, False),
    ]
    _, rejected = _runway_check(spark, tracks)
    detail = json.loads(rejected[("ATOT", _t(0))].reject_detail)
    assert round(detail["height_ft"]) == 2900


def test_a_distant_altitude_leaves_the_height_unknown(spark):
    """The nearest reports with an altitude are 50 s away, in which a jet
    climbs 2,000 to 4,000 ft, or lie on one side only: unknown, kept."""
    far = [
        ("t1", _t(-50), 50.0, 4.0, 300 / FT_PER_M, 160 / KT_PER_MPS, 0.0, False),
        ("t1", _t(50), 50.0, 4.0, 4000 / FT_PER_M, 160 / KT_PER_MPS, 0.0, False),
    ]
    one_side = [("t1", _t(5), 50.0, 4.0, 6000 / FT_PER_M, 160 / KT_PER_MPS, 0.0, False)]
    for tracks in (far, one_side):
        kept, rejected = _runway_check(spark, tracks)
        assert rejected == {} and kept == {("ATOT", _t(0))}


def test_a_stale_altitude_in_a_report_without_position_is_not_used(spark):
    """Reports without a position around the time repeat 8,000 ft: unknown,
    kept."""
    tracks = [
        ("t1", _t(d), None, None, 8000 / FT_PER_M, 160 / KT_PER_MPS, 0.0, False) for d in (-5, 0, 5)
    ]
    kept, rejected = _runway_check(spark, tracks)
    assert rejected == {}


def test_the_event_s_own_altitude_comes_before_the_track(spark):
    """The runway milestones carry the altitude at the instant they were
    interpolated to (15 ft above the field); that one decides, not a report
    50 s away."""
    events = spark.createDataFrame(
        [("t1", "ATOT", _t(0), json.dumps({"apt_icao": "LOW"}), 115.0)],
        EVENT_DDL + ", altitude_ft double",
    )
    kept, rejected = check_events(
        events,
        spark.createDataFrame(_reports(0, 3000, 200), TRACK_DDL),
        spark.createDataFrame(AIRPORTS, AIRPORT_DDL),
        EventConfig(),
    )
    assert rejected.count() == 0 and kept.count() == 1


def test_an_unknown_height_keeps_a_runway_time(spark):
    """No report near it, or no altitude in the reports, or an airport that is
    not in the airport table: kept."""
    events = [
        _event("ATOT", 0, apt_icao="LOW"),
        _event("ALDT", 1000, apt_icao="LOW"),
        _event("ATOT", 2000, apt_icao="NOWHERE"),
    ]
    tracks = _reports(1000, None, 140) + _reports(2000, 30000, 300)
    kept, rejected = _check(spark, events, tracks, flights=[("t1", None, None)])
    assert rejected == {}
    assert len(kept) == 3


def test_a_runway_time_at_an_airport_at_5000_ft_is_kept(spark):
    """At HIGH (5,000 ft) a lift-off at 5,040 ft barometric altitude is 40 ft
    above the airport."""
    events = [_event("ATOT", 0, apt_icao="HIGH"), _event("touchdown", 1000, apt_icao="HIGH")]
    tracks = _reports(0, 5040, 150, lat=51.0) + _reports(1000, 5020, 130, lat=51.0)
    kept, rejected = _check(spark, events, tracks)
    assert rejected == {}


# --- every event is kept or removed, exactly once ---------------------------


def test_an_event_without_a_type_is_kept(spark):
    kept, rejected = _check(spark, [(("t1", None, _t(0), "{}")), _event("AOBT", 10)], [])
    assert kept == {(None, _t(0)), ("AOBT", _t(10))}
    assert rejected == {}


def test_every_event_is_kept_or_removed_exactly_once_whatever_is_unknown(spark):
    """NULL in every column of the events, the reports, the flight list
    (which also names one flight twice) and the airport table (which names
    one airport twice): each event appears once, in the kept or the removed
    frame."""
    events = spark.createDataFrame(
        [
            (1, None, None, None, None, None),
            (2, "t1", None, _t(0), None, None),
            (3, None, "AOBT", _t(0), "{}", None),
            (4, "t1", "AOBT", _t(0), None, None),
            (5, "t1", "AIBT", _t(0), '{"landing": null}', None),
            (6, "t1", "ATOT", _t(0), '{"apt_icao": "LOW"}', None),
            (7, "t1", "ALDT", _t(0), '{"apt_icao": "LOW"}', 30000.0),
            (8, "t1", "AOBT", _t(0), json.dumps({"take_off": _t(0).isoformat()}), None),
            (9, "t2", "entry-taxiway", None, None, None),
            (10, "t1", "AOBT", _t(0), "not json", None),
            (11, "t1", "ATOT", _t(5), None, 1700.0),
        ],
        "eid int, track_id string, type string, event_time timestamp, info string, "
        "altitude_ft double",
    )
    reports = _reports(0, 30000, 450) + [
        ("t1", None, None, None, None, None, None, None),
        (None, _t(0), 50.0, 4.0, 100.0, 1.0, 0.0, True),
        ("t1", _t(1), None, 4.0, None, None, None, None),
        ("t1", _t(2), 50.0, None, 3000.0, None, None, True),
    ]
    flights = [("t1", "LOW", "HIGH"), ("t1", "LOW", None), (None, None, None), ("t2", None, "LOW")]
    airports = AIRPORTS + [
        ("LOW", 50.0, 4.0, 200),
        (None, None, None, None),
        ("X", None, None, None),
    ]
    kept, rejected = check_events(
        events,
        spark.createDataFrame(reports, TRACK_DDL),
        spark.createDataFrame(airports, AIRPORT_DDL),
        EventConfig(),
        flights=spark.createDataFrame(flights, FLIGHT_DDL),
    )
    got = sorted([r.eid for r in kept.collect()] + [r.eid for r in rejected.collect()])
    assert got == list(range(1, 12))
    assert set(kept.columns) == set(events.columns)


def test_a_flight_named_twice_in_the_flight_list_does_not_double_its_events(spark):
    kept, rejected = check_events(
        spark.createDataFrame([_event("AOBT", 10), _event("xing-fl100", 50)], EVENT_DDL),
        spark.createDataFrame([], TRACK_DDL),
        spark.createDataFrame(AIRPORTS, AIRPORT_DDL),
        EventConfig(),
        flights=spark.createDataFrame([("t1", "LOW", "HIGH"), ("t1", "LOW", "HIGH")], FLIGHT_DDL),
    )
    assert kept.count() == 2 and rejected.count() == 0


def test_the_step_reads_each_flight_of_the_flight_list_once(spark, tmp_path):
    from opdi.config import OPDIConfig
    from opdi.pipeline.events import FlightEventProcessor

    class _Storage:
        def table_exists(self, name):
            return name == "opdi_flight_list"

        def read_table(self, name):
            return spark.createDataFrame(
                [("t1", T0, "LOW", "HIGH"), ("t1", T0, "LOW", "HIGH")],
                "id string, dof timestamp, adep string, ades string",
            )

    proc = FlightEventProcessor(spark, OPDIConfig(), log_dir=str(tmp_path))
    proc.storage = _Storage()
    assert proc._flight_airports(dt.date(2026, 6, 1)).count() == 1
