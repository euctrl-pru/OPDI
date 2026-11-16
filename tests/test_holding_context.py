"""Holding: the context of a hold, its label, where it starts and ends.

The cases are those of the spot checks of 2026-06-14 (finding e1): arrival
holds before landing, a 34-minute hold (DAL1115 at KDAL), a single 360-degree
turn en route at FL410 far from the destination (EJA542, 328 NM from KHOU),
survey and surveillance flying of several hours (ASR262, 6 h at FL270), a
training hold on a local flight, a hold cut in two by a dip of the ground
speed, a lead-in turn the other way before the hold (ARTAC610), and a first
report without altitude (BAW433 at EGLL).
"""

import datetime as dt
import json

from conftest import _EPOCH, make_track
from navigation_fixtures import fly, legs_to_samples, measured, racetrack
from pyspark.sql import functions as F

from opdi.config import EventConfig
from opdi.pipeline.holding import calculate_holding_events

#: Off: the behaviour before these corrections.
BEFORE = EventConfig(
    holding_pattern_by_straight_share=False,
    holding_trim_to_main_direction=False,
    holding_altitude_from_known_reports=False,
    holding_context=False,
)


def _airports(sdf, adep="LFPG", ades="EGLL", ades_lat=51.0, ades_lon=0.6):
    return (
        sdf.withColumn("adep", F.lit(adep))
        .withColumn("ades", F.lit(ades))
        .withColumn("ades_lat", F.lit(ades_lat))
        .withColumn("ades_lon", F.lit(ades_lon))
    )


def _runway(spark, rows):
    """``(type, seconds after the epoch, airport)`` rows of track trk-1."""
    return spark.createDataFrame(
        [
            ("trk-1", t, _EPOCH + dt.timedelta(seconds=s), json.dumps({"apt_icao": apt}))
            for t, s, apt in rows
        ],
        "track_id string, type string, event_time timestamp, info string",
    )


def _holds(sdf, runway=None, config=None):
    rows = calculate_holding_events(sdf, config or EventConfig(), runway_events=runway).collect()
    starts = sorted((r for r in rows if r.type == "holding-start"), key=lambda r: r.event_time)
    ends = sorted((r for r in rows if r.type == "holding-end"), key=lambda r: r.event_time)
    return [(s, e, json.loads(e.info)) for s, e in zip(starts, ends)]


def _two_circuit_arrival(spark):
    legs = [(300, 0, 0)] + racetrack(2) + [(300, 0, 0)]
    return _airports(fly(spark, legs))


def test_a_real_arrival_hold_of_two_racetrack_circuits(spark):
    sdf = _two_circuit_arrival(spark)
    runway = _runway(spark, [("ATOT", -3600, "LFPG"), ("ALDT", 1800, "EGLL")])
    [(start, end, info)] = _holds(sdf, runway)
    assert info["context"] == "arrival"
    assert info["pattern"] == "racetrack"
    # The measurements of a clean hold are those of the method before.
    [(s0, e0, before)] = _holds(sdf, runway, BEFORE)
    for k in ("duration_s", "distance_nm", "circuits", "straight_pct"):
        assert info[k] == before[k], k
    assert (start.event_time, end.event_time) == (s0.event_time, e0.event_time)
    assert "context" not in before


def test_a_34_minute_hold_is_kept_whole(spark):
    legs = [(300, 0, 0)] + racetrack(6) + [(300, 0, 0)]
    sdf = _airports(fly(spark, legs))
    runway = _runway(spark, [("ALDT", 3600, "EGLL")])
    [(_, _, info)] = _holds(sdf, runway)
    assert info["duration_s"] >= 2000
    assert info["context"] == "arrival"


def test_a_single_360_en_route_far_from_the_destination(spark):
    sdf = _airports(
        fly(spark, [(300, 0, 0), (270, 1.4, 0), (300, 0, 0)], alt_ft=41000.0),
        ades="KHOU",
        ades_lat=46.0,
        ades_lon=0.0,
    )
    runway = _runway(spark, [("ALDT", 3 * 3600, "KHOU")])
    [(_, _, info)] = _holds(sdf, runway)
    assert info["context"] == "en-route"
    assert info["pattern"] == "orbit"


def test_survey_flying_of_several_hours_is_other(spark):
    """ASR262: one episode of hours, most of the time in the air."""
    legs = [(600, 0, 0)] + racetrack(30) + [(600, 0, 0)]
    sdf = _airports(fly(spark, legs, alt_ft=27000.0), adep="EKRK", ades="EKCH")
    runway = _runway(spark, [("ATOT", -60, "EKRK"), ("ALDT", 13000, "EKCH")])
    holds = _holds(sdf, runway)
    assert holds and all(h[2]["context"] == "other" for h in holds)
    assert sum(h[2]["duration_s"] for h in holds) > 2.5 * 3600


def test_a_training_hold_on_a_local_flight_is_other(spark):
    sdf = _airports(fly(spark, [(300, 0, 0)] + racetrack(2) + [(300, 0, 0)]), adep="EGLL")
    runway = _runway(spark, [("ATOT", -600, "EGLL"), ("ALDT", 1800, "EGLL")])
    [(_, _, info)] = _holds(sdf, runway)
    assert info["context"] == "other"


def test_a_hold_with_no_landing_after_it_is_other(spark):
    sdf = _two_circuit_arrival(spark)
    runway = _runway(spark, [("ATOT", -3600, "LFPG")])
    [(_, _, info)] = _holds(sdf, runway)
    assert info["context"] == "other"


def test_three_holds_on_one_track_are_other(spark):
    legs = []
    for _ in range(3):
        legs += [(600, 0, 0)] + racetrack(2)
    legs += [(600, 0, 0)]
    sdf = _airports(fly(spark, legs))
    runway = _runway(spark, [("ATOT", -7200, "LFPG"), ("ALDT", 6000, "EGLL")])
    holds = _holds(sdf, runway)
    assert len(holds) == 3 and {h[2]["context"] for h in holds} == {"other"}


def test_a_hold_cut_by_a_speed_dip_keeps_its_context(spark):
    """The ground speed drops under 140 kt for 3 minutes in the wind: two
    episodes, each an arrival hold with its own measurements."""
    samples = legs_to_samples([(300, 0, 0)] + racetrack(4) + [(300, 0, 0)])
    for s in samples:
        if 1000 <= s["t"] < 1180:
            s["velocity"] = 130 / 1.94384
    sdf = _airports(measured(make_track(spark, samples)))
    runway = _runway(spark, [("ATOT", -3600, "LFPG"), ("ALDT", 3000, "EGLL")])
    holds = _holds(sdf, runway)
    assert len(holds) == 2
    assert all(h[2]["context"] == "arrival" and h[2]["duration_s"] >= 240 for h in holds)


def test_without_runway_times_no_context_is_written(spark):
    [(_, _, info)] = _holds(_two_circuit_arrival(spark), None)
    assert "context" not in info


# --- label and boundaries ----------------------------------------------------


def test_continuous_circling_is_an_orbit(spark):
    """BAW40F: 1.3 loops with no straight report was a racetrack."""
    sdf = fly(spark, [(300, 0, 0), (360, 1.4, 0), (300, 0, 0)])  # 504 degrees
    [(_, _, before)] = _holds(sdf, None, BEFORE)
    assert before["pattern"] == "racetrack" and before["straight_pct"] < 15
    [(_, _, info)] = _holds(sdf, None)
    assert info["pattern"] == "orbit"


def test_a_lead_in_turn_the_other_way_is_not_part_of_the_hold(spark):
    """ARTAC610: a 59-degree left turn just before a right-hand orbit."""
    legs = [(300, 0, 0), (40, -1.5, 0)] + racetrack(2) + [(300, 0, 0)]
    sdf = fly(spark, legs)
    [(s0, _, before)] = _holds(sdf, None, BEFORE)
    [(start, _, info)] = _holds(sdf, None)
    lead_in_end = _EPOCH + dt.timedelta(seconds=340)
    assert s0.event_time < lead_in_end
    assert start.event_time >= lead_in_end
    assert info["circuits"] >= before["circuits"]


def test_the_start_altitude_comes_from_the_first_report_that_has_one(spark):
    """BAW433: the first report of the hold has no altitude at all."""
    samples = legs_to_samples([(300, 0, 0)] + racetrack(2) + [(300, 0, 0)])
    for s in samples:
        if 330 <= s["t"] < 420:
            s["baro_altitude"] = None
    sdf = measured(make_track(spark, samples))
    [(s0, _, before)] = _holds(sdf, None, BEFORE)
    assert s0.altitude_ft is None and "altitude_start_ft" not in before
    [(start, _, info)] = _holds(sdf, None)
    assert start.event_time == s0.event_time
    assert start.altitude_ft is not None and info["altitude_start_ft"] is not None


def test_v002_settings_switch_the_holding_corrections_off():
    legacy = EventConfig.legacy()
    assert not (
        legacy.holding_pattern_by_straight_share
        or legacy.holding_trim_to_main_direction
        or legacy.holding_altitude_from_known_reports
        or legacy.holding_context
    )
