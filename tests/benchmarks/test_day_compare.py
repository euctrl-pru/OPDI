"""The day comparison matches events by flight, type and second, and counts what differs."""

from datetime import datetime

import day_compare as c

EV = (
    "event_id string, flight_id string, type string, event_time timestamp, "
    "longitude double, latitude double, altitude_ft double, info string, "
    "distance_flown_nm double, time_passed_s double"
)


def _ev(spark, rows):
    return spark.createDataFrame(rows, EV)


def test_share_is_matched_over_the_larger_count():
    assert c.share(999, 1000, 990) == 0.999
    assert c.share(0, 0, 0) == 1.0


def test_events_match_to_the_second_and_moved_values_are_counted(spark):
    t0 = datetime(2026, 6, 14, 10, 0, 0)
    before = _ev(
        spark,
        [
            ("a", "f1", "airborne", t0, 1.0, 2.0, 100.0, "{}", 10.0, 60.0),
            ("b", "f1", "touchdown", t0.replace(minute=50), 1.0, 2.0, 0.0, "{}", 20.0, 3000.0),
            ("c", "f2", "airborne", t0, 1.0, 2.0, 100.0, "{}", 5.0, 1.0),
        ],
    )
    after = _ev(
        spark,
        [
            # Same second, a few milliseconds later: matched; the distance moved.
            (
                "a2",
                "f1",
                "airborne",
                t0.replace(microsecond=400_000),
                1.0,
                2.0,
                100.0,
                "{}",
                10.5,
                60.0,
            ),
            ("b", "f1", "touchdown", t0.replace(minute=50), 1.0, 2.0, 0.0, "{}", 20.0, 3000.0),
            ("d", "f3", "airborne", t0, 1.0, 2.0, 100.0, "{}", 5.0, 1.0),
        ],
    )
    rows = {(r["part"], r["item"]): r for r in c.compare_events(before, after)}
    air = rows[("events", "airborne")]
    assert (air["before"], air["after"], air["matched"]) == (2, 2, 1)
    assert (air["only_before"], air["only_after"], air["matched_share"]) == (1, 1, 0.5)
    assert "distance_flown_nm differs on 1" in air["note"]
    assert rows[("events", "touchdown")]["matched_share"] == 1.0
    assert rows[("measurement", "distance_flown_nm")]["note"].startswith("1 matched")
    assert rows[("measurement", "time_passed_s")]["note"].startswith("0 matched")


def test_a_repeated_key_matches_as_often_as_it_occurs_in_both(spark):
    t0 = datetime(2026, 6, 14, 10, 0, 0)
    row = ("x", "f1", "xing-fir", t0, 1.0, 2.0, 3.0, "{}", 1.0, 1.0)
    before = _ev(spark, [row, row])
    after = _ev(spark, [row])
    (ev,) = [r for r in c.compare_events(before, after) if r["part"] == "events"]
    assert (ev["before"], ev["after"], ev["matched"], ev["only_before"]) == (2, 1, 1, 1)


def test_flights_match_by_address_and_first_seen_and_airports_are_compared(spark):
    ddl = "icao24 string, first_seen timestamp, adep string, ades string"
    t0 = datetime(2026, 6, 14, 10, 0, 0)
    before = spark.createDataFrame(
        [("a", t0, "EBBR", "LFPG"), ("b", t0, "EDDF", None), ("c", t0, "EGLL", "KJFK")], ddl
    )
    after = spark.createDataFrame(
        [("a", t0, "EBBR", "LFPO"), ("b", t0, "EDDF", None), ("d", t0, "EGLL", "KJFK")], ddl
    )
    (r,) = c.compare_flights(before, after)
    assert (r["before"], r["after"], r["matched"], r["only_before"], r["only_after"]) == (
        3,
        3,
        2,
        1,
        1,
    )
    assert r["note"] == "departure airport differs on 0; destination airport differs on 1"
