"""The corrections of the flight list after the spot checks of 2026-06-14 (work package 4).

Each test is built from the shape of a real example of that day (the example
id is in the test's text), reduced to the few reports or events that decide
the case, and checks the correction against the behaviour without it. The
cases that must keep working are tested beside them: a real local flight, a
training circuit, a stop in the middle of a track, a go-around, a take-off
misread at the destination, a stand fragment.
"""

import datetime as dt
import json
from dataclasses import replace

from opdi.config import DetectionConfig, EventConfig, OPDIConfig
from opdi.ingestion.ourairports import drop_duplicate_airports
from opdi.pipeline.events import calculate_firstseen_lastseen_events
from opdi.pipeline.flight_list_milestones import enrich_flight_list
from opdi.pipeline.flights import (
    FlightListProcessor,
    dominant_flight_id,
    resolve_flight_id,
    wide_search_roles,
    with_endpoint_candidates,
)

_T0 = dt.datetime(2026, 6, 14, 0, 0, 0)

#: Positions and elevations (ft) of the airports in these tests, from OurAirports.
AIRPORTS = {
    "KPDX": (45.5887, -122.5975, 31),
    "KCOS": (38.8058, -104.7008, 6187),
    "PHKO": (19.7388, -156.0456, 47),
    "KDEN": (39.8617, -104.6731, 5434),
    "LFPB": (48.9694, 2.4414, 218),
    "LFPG": (49.0097, 2.5479, 392),
    "EGLF": (51.2758, -0.7763, 238),
    "KPWT": (47.4904, -122.7648, 268),
    "EGXZ": (54.2056, -1.3820, 92),
    "LROD": (47.0253, 21.9025, 465),
    "LTAI": (36.8987, 30.8005, 177),
    "KLEB": (43.6261, -72.3042, 603),
    "KLAS": (36.0801, -115.1522, 2181),
    "KDFW": (32.8968, -97.0380, 607),
    "KCLT": (35.2140, -80.9431, 748),
    "KIAD": (38.9445, -77.4558, 312),
    "LLBG": (32.0114, 34.8867, 135),
    "KMSP": (44.8820, -93.2218, 841),
    "KGRR": (42.8808, -85.5228, 794),
}


def _t(seconds: float) -> dt.datetime:
    return _T0 + dt.timedelta(seconds=seconds)


def _airports(spark):
    return spark.createDataFrame(
        [(k, lat, lon, elev) for k, (lat, lon, elev) in AIRPORTS.items()],
        "ident string, latitude_deg double, longitude_deg double, elevation_ft int",
    )


# ---------------------------------------------------------------------------
# Step 04b: the airport correction, the flags, the milestones on their leg
# ---------------------------------------------------------------------------

FLIGHT_SCHEMA = (
    "id string, ICAO24 string, FLT_ID string, DOF timestamp, ADEP string, ADES string, "
    "ADEP_P string, ADES_P string, ADEP_SOURCE string, ADES_SOURCE string, VERSION string, "
    "FIRST_SEEN timestamp, LAST_SEEN timestamp"
)
EVENT_SCHEMA = (
    "flight_id string, type string, event_time timestamp, latitude double, "
    "longitude double, altitude double, info string"
)


def _flight(id_, adep, ades, icao24="abc123", first=0.0, last=36000.0):
    return (
        id_,
        icao24,
        "TEST01",
        _T0,
        adep,
        ades,
        "",
        "",
        "aerodrome" if adep else "undetermined",
        "aerodrome" if ades else "undetermined",
        "v",
        _t(first),
        _t(last),
    )


def _ev(id_, type_, seconds, lat=None, lon=None, alt=None, **info):
    return (id_, type_, _t(seconds), lat, lon, alt, json.dumps(info) if info else "")


def _at(apt):
    lat, lon, _ = AIRPORTS[apt]
    return lat, lon


def _enrich(spark, flights, events, config=None):
    fl = spark.createDataFrame(flights, FLIGHT_SCHEMA)
    ev = spark.createDataFrame(events, EVENT_SCHEMA)
    out = enrich_flight_list(fl, ev, config or EventConfig(), _airports(spark))
    return {r["id"]: r for r in out.collect()}


#: The settings without the corrections of work package 4 in step 04b.
OFF = replace(
    EventConfig(),
    aerodrome_correction_checks_track=False,
    movement_flags_need_track_evidence=False,
    milestones_on_own_leg=False,
)


def _two_leg_kcos():
    """fl_departure_airport#39: KPDX -> KCOS -> (Oklahoma) on one track.

    The landing at KCOS (fallback method) comes before the take-off from KCOS;
    the track starts at Portland and ends 391 NM from KCOS.
    """
    f = [_flight("t39", "KPDX", None)]
    e = [
        _ev("t39", "first_seen", 0, 45.5946, -122.5939, 500.0),
        _ev("t39", "ALDT", 7885, apt_icao="KCOS", method="legacy"),
        _ev("t39", "line-up", 10140, 38.8226, -104.7161, apt_icao="KCOS"),
        _ev("t39", "take-off-roll", 10170, 38.8179, -104.7159, 5825.0, apt_icao="KCOS"),
        _ev("t39", "ATOT", 10188, 38.8058, -104.7161, 6202.0, apt_icao="KCOS", method="acdm"),
        _ev("t39", "top-of-climb", 11000, 38.0, -101.0, 41000.0),
        _ev("t39", "last_seen", 13870, 35.5272, -97.6455, 1100.0),
    ]
    return f, e


def test_a_stop_in_the_middle_of_a_track_does_not_replace_both_airports(spark):
    f, e = _two_leg_kcos()
    row = _enrich(spark, f, e)["t39"]
    assert (row["ADEP"], row["ADES"]) == ("KPDX", None)
    assert row["ADEP_SOURCE"] == "aerodrome"
    # Without the check the take-off of the stop became the departure airport.
    old = _enrich(spark, f, e, OFF)["t39"]
    assert old["ADEP"] == "KCOS" and old["ADEP_SOURCE"] == "runway_event"


def test_a_go_around_read_as_a_take_off_does_not_move_the_departure(spark):
    """fl_departure_airport#38: PHKO -> KDEN; the go-around at KDEN was read as
    line-up / take-off-roll / ATOT, and the landing time is of the fallback method."""
    f = [_flight("t38", "PHKO", "KDEN")]
    e = [
        _ev("t38", "first_seen", 0, 19.7377, -156.0420),
        _ev("t38", "take-off-roll", 23425, 39.8165, -104.6609, 5375.0, apt_icao="KDEN"),
        _ev("t38", "line-up", 23425, 39.8165, -104.6609, 5375.0, apt_icao="KDEN"),
        _ev("t38", "ATOT", 23431, 39.8221, -104.6607, 5446.0, apt_icao="KDEN", method="acdm"),
        _ev("t38", "ALDT", 24424, apt_icao="KDEN", method="legacy"),
        _ev("t38", "last_seen", 24900, 39.8593, -104.6745),
    ]
    row = _enrich(spark, f, e)["t38"]
    assert (row["ADEP"], row["ADES"]) == ("PHKO", "KDEN")
    # A fallback landing time never replaces an airport: the source stays.
    assert (row["ADEP_SOURCE"], row["ADES_SOURCE"]) == ("aerodrome", "aerodrome")
    old = _enrich(spark, f, e, OFF)["t38"]
    assert old["ADEP"] == "KDEN"


def test_a_runway_event_at_the_end_of_the_track_still_corrects_the_airport(spark):
    """fl_departure_airport#24: on the ground at LFPB, but the endpoint method
    chose LFPG; the take-off from LFPB and the landing at EGLF correct both."""
    f = [_flight("t24", "LFPG", "EGLF")]
    e = [
        _ev("t24", "first_seen", 0, 48.9688, 2.4459),
        _ev("t24", "line-up", 185, 48.9682, 2.4369, 250.0, apt_icao="LFPB"),
        _ev("t24", "ATOT", 187, 48.9674, 2.4339, 407.0, apt_icao="LFPB", method="acdm"),
        _ev("t24", "ALDT", 2784, 51.2856, -0.7470, 407.0, apt_icao="EGLF", method="acdm"),
        _ev("t24", "runway-vacated", 2795, 51.2834, -0.7535, apt_icao="EGLF"),
        _ev("t24", "last_seen", 2795, 51.2834, -0.7535),
    ]
    row = _enrich(spark, f, e)["t24"]
    assert (row["ADEP"], row["ADEP_SOURCE"]) == ("LFPB", "runway_event")
    assert (row["ADES"], row["ADES_SOURCE"]) == ("EGLF", "runway_event")
    assert row["ADEP_P"] == "LFPG"


def test_a_real_local_flight_keeps_departure_equal_to_destination(spark):
    """fl_departure_airport#43: a C172 flies 22 minutes from KPWT back to KPWT."""
    f = [_flight("t43", "KPWT", None)]
    e = [
        _ev("t43", "first_seen", 0, 47.4910, -122.7650),
        _ev("t43", "take-off-roll", 100, 47.4900, -122.7650, apt_icao="KPWT"),
        _ev("t43", "ATOT", 130, 47.4950, -122.7650, 600.0, apt_icao="KPWT", method="acdm"),
        _ev("t43", "ALDT", 1300, 47.4880, -122.7650, 300.0, apt_icao="KPWT", method="acdm"),
        _ev("t43", "runway-vacated", 1330, 47.4870, -122.7640, apt_icao="KPWT"),
        _ev("t43", "last_seen", 1400, 47.4890, -122.7660),
    ]
    row = _enrich(spark, f, e)["t43"]
    assert (row["ADEP"], row["ADES"]) == ("KPWT", "KPWT")
    assert row["ADES_SOURCE"] == "runway_event"


def test_a_training_circuit_keeps_its_airport_at_both_ends(spark):
    """Touch-and-goes at EGXZ (fl_takeoff_landing#50): take-offs and landings
    alternate, and the last landing comes after the last take-off."""
    f = [_flight("tc", "EGXZ", None)]
    e = [_ev("tc", "first_seen", 0, 54.2050, -1.3830)]
    for k in range(3):
        base = 100 + 600 * k
        e.append(_ev("tc", "take-off-roll", base, 54.2050, -1.3830, apt_icao="EGXZ"))
        e.append(_ev("tc", "ALDT", base + 500, 54.2060, -1.3810, apt_icao="EGXZ", method="acdm"))
    e.append(_ev("tc", "runway-vacated", 1750, 54.2060, -1.3810, apt_icao="EGXZ"))
    e.append(_ev("tc", "last_seen", 1800, 54.2060, -1.3810))
    row = _enrich(spark, f, e)["tc"]
    assert (row["ADEP"], row["ADES"]) == ("EGXZ", "EGXZ")


def test_a_take_off_read_as_a_landing_does_not_become_the_destination(spark):
    """fl_alternative_airports#42: LROD -> LTAI; the take-off from LROD was also
    read as a landing (9 s before the take-off time); the track ends near LTAI."""
    f = [_flight("t42", "LROD", "LTAI")]
    e = [
        _ev("t42", "first_seen", 0, 47.0250, 21.9000),
        _ev("t42", "take-off-roll", 10360, 47.0240, 21.8950, apt_icao="LROD"),
        _ev("t42", "ALDT", 10385, 47.0260, 21.8900, apt_icao="LROD", method="acdm"),
        _ev("t42", "ATOT", 10394, 47.0270, 21.8880, apt_icao="LROD", method="legacy"),
        _ev("t42", "last_seen", 16000, 36.9500, 30.7500),
    ]
    row = _enrich(spark, f, e)["t42"]
    assert (row["ADEP"], row["ADES"]) == ("LROD", "LTAI")
    assert row["ADES_SOURCE"] == "aerodrome"
    old = _enrich(spark, f, e, OFF)["t42"]
    assert old["ADES"] == "LROD"


def test_without_the_airport_table_a_distance_does_not_stop_a_correction(spark):
    f, e = _two_leg_kcos()
    fl = spark.createDataFrame(f, FLIGHT_SCHEMA)
    ev = spark.createDataFrame(e, EVENT_SCHEMA)
    row = enrich_flight_list(fl, ev, EventConfig()).collect()[0]
    # The order still decides: the landing at KCOS is a fallback time and
    # comes before the take-off, so the destination is not replaced; the
    # take-off is the first departure and, with no distance, replaces ADEP.
    assert row["ADES"] is None and row["ADEP"] == "KCOS"


def _supersede_case(height_events, adep="KLEB"):
    """Two rows of one aircraft at one airport, the first without a take-off,
    and a third flight with a take-off so that the day counts as processed."""
    f = [
        _flight("a1", adep, None, icao24="a7f001", first=0, last=1800),
        _flight("a2", adep, "KCLT", icao24="a7f001", first=3600, last=9000),
        _flight("z9", "KCLT", "KLEB", icao24="ffe001", first=0, last=5000),
    ]
    e = [_ev("z9", "ATOT", 300, apt_icao="KCLT", method="acdm")] + height_events
    return f, e


def test_a_flight_in_the_air_is_not_a_superseded_fragment(spark):
    """fl_flags#14: N172E flies the whole track at 3,800-5,000 ft near KLEB
    and was marked as a superseded departure."""
    f, e = _supersede_case([_ev("a1", "top-of-climb", 600, 43.6, -72.3, 5000.0)])
    rows = _enrich(spark, f, e)
    assert rows["a1"]["SUPERSEDED_DEP"] is False and rows["a1"]["MOVEMENT_DEP"] is True
    assert _enrich(spark, f, e, OFF)["a1"]["SUPERSEDED_DEP"] is True


def test_a_stand_fragment_is_still_superseded(spark):
    """The stand fragments of the samples (ASL85L at LYBE, JBU977 at KLAX) never
    leave the ground: they keep the flag."""
    f, e = _supersede_case([])
    rows = _enrich(spark, f, e)
    assert rows["a1"]["SUPERSEDED_DEP"] is True and rows["a1"]["MOVEMENT_DEP"] is False


def test_a_row_without_an_airport_is_never_superseded(spark):
    """fl_flags#04: ADEP and ADES NULL, yet both superseded flags were set."""
    f, e = _supersede_case([], adep=None)
    rows = _enrich(spark, f, e)
    assert rows["a1"]["SUPERSEDED_DEP"] is False
    assert _enrich(spark, f, e, OFF)["a1"]["SUPERSEDED_DEP"] is True


def test_a_departure_without_take_off_or_circles_counts_when_it_climbed(spark):
    """fl_flags#26 RJA614: a departure from OJAI with no take-off time, no
    destination and no circle crossing (no positions at 40-120 NM)."""
    f = [
        _flight("r1", "KLEB", None, icao24="740001"),
        _flight("z9", "KCLT", "KLEB", icao24="ffe001"),
    ]
    e = [
        _ev("z9", "ATOT", 300, apt_icao="KCLT", method="acdm"),
        _ev("r1", "first_seen", 0, 43.62, -72.30, 2500.0),
        _ev("r1", "top-of-climb", 1500, 43.0, -71.0, 41000.0),
        _ev("r1", "last_seen", 2000, 43.0, -71.0),
    ]
    rows = _enrich(spark, f, e)
    assert rows["r1"]["MOVEMENT_DEP"] is True
    assert _enrich(spark, f, e, OFF)["r1"]["MOVEMENT_DEP"] is False


def test_a_departure_whose_track_ends_far_away_counts(spark):
    f = [
        _flight("r2", "KLEB", None, icao24="740002"),
        _flight("z9", "KCLT", "KLEB", icao24="ffe001"),
    ]
    e = [
        _ev("z9", "ATOT", 300, apt_icao="KCLT", method="acdm"),
        _ev("r2", "first_seen", 0, 43.62, -72.30),
        _ev("r2", "last_seen", 3000, 41.0, -70.0),
    ]
    assert _enrich(spark, f, e)["r2"]["MOVEMENT_DEP"] is True


def test_a_ground_only_row_is_still_not_a_departure(spark):
    """A tow at the airport: no take-off, no destination, never in the air."""
    f = [
        _flight("g1", "KLEB", None, icao24="740003"),
        _flight("z9", "KCLT", "KLEB", icao24="ffe001"),
    ]
    e = [
        _ev("z9", "ATOT", 300, apt_icao="KCLT", method="acdm"),
        _ev("g1", "first_seen", 0, 43.626, -72.304),
        _ev("g1", "last_seen", 900, 43.627, -72.305),
    ]
    assert _enrich(spark, f, e)["g1"]["MOVEMENT_DEP"] is False


def test_block_times_and_stands_come_from_the_departure_airport(spark):
    """fl_takeoff_landing#43: KDFW -> KLAS, a turnaround, then KLAS -> KCLT.

    The off-block at KDFW (stand C21) was taken for the departure from KLAS,
    and the stand from a 15-s pass during the turnaround (D10)."""
    f = [_flight("t1", "KLAS", "KCLT")]
    e = [
        _ev("t1", "AOBT", 4860, apt_icao="KDFW", stand="C21"),
        _ev("t1", "ALDT", 14100, apt_icao="KLAS", method="acdm"),
        _ev("t1", "exit-parking_position", 18000, osm_ref="D10", osm_airport="KLAS"),
        _ev("t1", "exit-parking_position", 26700, osm_ref="D11", osm_airport="KLAS"),
        _ev("t1", "AOBT", 26700, apt_icao="KLAS", stand="D11"),
        _ev("t1", "ATOT", 27420, apt_icao="KLAS", method="acdm"),
        _ev("t1", "ALDT", 40000, apt_icao="KCLT", method="acdm"),
    ]
    row = _enrich(spark, f, e)["t1"]
    assert row["AOBT"] == _t(26700) and row["STND_DEP"] == "D11"
    old = _enrich(spark, f, e, OFF)["t1"]
    assert old["AOBT"] == _t(4860) and old["STND_DEP"] == "D10"


def test_the_layout_stand_is_the_last_stand_exit_before_the_take_off(spark):
    f = [_flight("t2", "KLAS", "KCLT")]
    e = [
        _ev("t2", "exit-parking_position", 18000, osm_ref="D10", osm_airport="KLAS"),
        _ev("t2", "exit-parking_position", 26700, osm_ref="D11", osm_airport="KLAS"),
        _ev("t2", "ATOT", 27420, apt_icao="KLAS", method="acdm"),
    ]
    assert _enrich(spark, f, e)["t2"]["STND_DEP"] == "D11"


def test_in_block_is_the_first_after_the_landing_not_a_repositioning(spark):
    """fl_block_stands#46: KGRR -> KMSP, landed 00:24; a repositioning the next
    morning (G8 to G8A) was taken as the in-block time."""
    f = [_flight("t3", "KGRR", "KMSP")]
    e = [
        _ev("t3", "ATOT", -3000, apt_icao="KGRR", method="acdm"),
        _ev("t3", "ALDT", 1440, apt_icao="KMSP", method="acdm"),
        _ev("t3", "AIBT", 2100, apt_icao="KMSP", stand="G8"),
        _ev("t3", "AIBT", 52200, apt_icao="KMSP", stand="G8A"),
    ]
    row = _enrich(spark, f, e)["t3"]
    assert row["AIBT"] == _t(2100) and row["STND_ARR"] == "G8"
    assert _enrich(spark, f, e, OFF)["t3"]["AIBT"] == _t(52200)


def test_circle_crossings_lie_after_the_take_off_and_before_the_landing(spark):
    """fl_rings#40 KIAD -> LLBG: one far-off ground report gave an outbound
    40 NM crossing 4 h before the take-off; fl_rings#35: an inbound crossing at
    KMSP 10.5 h after the landing."""
    f = [_flight("t4", "KIAD", "KMSP")]
    e = [
        _ev("t4", "xing-40nm", 5040, apt_icao="KIAD", direction="outbound"),
        _ev("t4", "ATOT", 19380, apt_icao="KIAD", method="acdm"),
        _ev("t4", "xing-40nm", 19900, apt_icao="KIAD", direction="outbound"),
        _ev("t4", "xing-40nm", 30000, apt_icao="KMSP", direction="inbound"),
        _ev("t4", "ALDT", 30700, apt_icao="KMSP", method="acdm"),
        _ev("t4", "xing-40nm", 68000, apt_icao="KMSP", direction="inbound"),
    ]
    row = _enrich(spark, f, e)["t4"]
    assert (row["C40_DEP"], row["C40_ARR"]) == (_t(19900), _t(30000))
    old = _enrich(spark, f, e, OFF)["t4"]
    assert (old["C40_DEP"], old["C40_ARR"]) == (_t(5040), _t(68000))


def test_without_take_off_and_landing_the_circles_are_as_before(spark):
    f = [_flight("t5", "KIAD", "KMSP")]
    e = [
        _ev("t5", "xing-40nm", 5040, apt_icao="KIAD", direction="outbound"),
        _ev("t5", "xing-40nm", 19900, apt_icao="KIAD", direction="outbound"),
        _ev("t5", "xing-40nm", 68000, apt_icao="KMSP", direction="inbound"),
    ]
    row = _enrich(spark, f, e)["t5"]
    assert (row["C40_DEP"], row["C40_ARR"]) == (_t(5040), _t(68000))


def test_the_internal_columns_do_not_reach_the_flight_list(spark):
    f, e = _two_leg_kcos()
    fl = spark.createDataFrame(f, FLIGHT_SCHEMA)
    ev = spark.createDataFrame(e, EVENT_SCHEMA)
    out = enrich_flight_list(fl, ev, EventConfig(), _airports(spark))
    assert not [c for c in out.columns if c.startswith("_")]
    assert out.columns == enrich_flight_list(fl, ev, OFF, _airports(spark)).columns


# ---------------------------------------------------------------------------
# first_seen / last_seen events
# ---------------------------------------------------------------------------

SEEN_SCHEMA = (
    "track_id string, event_time timestamp, lat double, lon double, baro_altitude double, "
    "baro_altitude_c double, vert_rate double, velocity double, heading double, "
    "cumulative_distance_nm double, cumulative_time_s double"
)


def _seen(spark, rows, measured=True):
    df = spark.createDataFrame(rows, SEEN_SCHEMA)
    out = calculate_firstseen_lastseen_events(df, from_measured_reports=measured).collect()
    return {r["type"]: r for r in out}


def _report(t, lat=None, lon=None, alt=None, alt_c=None, gs=None):
    return ("trk", _t(t), lat, lon, alt, alt_c, None, gs, None, 0.0, float(t))


def test_last_seen_is_the_last_measured_position_not_the_repeated_state(spark):
    """fl_identity_times#01 BKP935: last position at 11:52:59; OpenSky repeated
    the last state until 11:58:00, with no position after cleaning. And #05:
    390 s of altitude-only reports before the first position."""
    rows = [
        _report(0, alt=3000.0, alt_c=3000.0),
        _report(390, 50.0, 4.0, 3100.0, 3100.0, 120.0),
        _report(1000, 50.5, 4.5, 3500.0, 3500.0, 130.0),
        _report(1300, alt_c=3500.0),
        _report(1600, alt_c=3500.0),
    ]
    seen = _seen(spark, rows)
    assert seen["first_seen"]["event_time"] == _t(390)
    assert seen["last_seen"]["event_time"] == _t(1000)
    assert seen["last_seen"]["lat"] == 50.5
    old = _seen(spark, rows, measured=False)
    assert (old["first_seen"]["event_time"], old["last_seen"]["event_time"]) == (_t(0), _t(1600))


def test_a_track_without_position_is_seen_on_its_measured_values(spark):
    """fl_identity_times#04 KBZ214: no position at all; the reports with an
    altitude define first and last seen, not the repeated tail."""
    rows = [
        _report(0, alt_c=1000.0),
        _report(60, alt=1000.0, alt_c=1000.0),
        _report(600, alt=2000.0, alt_c=2000.0),
        _report(900, alt_c=2000.0),
    ]
    seen = _seen(spark, rows)
    assert (seen["first_seen"]["event_time"], seen["last_seen"]["event_time"]) == (
        _t(60),
        _t(600),
    )


def test_a_track_with_nothing_measured_keeps_its_first_and_last_report(spark):
    rows = [_report(0, alt_c=1000.0), _report(60, alt_c=1000.0)]
    seen = _seen(spark, rows)
    assert (seen["first_seen"]["event_time"], seen["last_seen"]["event_time"]) == (
        _t(0),
        _t(60),
    )


# ---------------------------------------------------------------------------
# Step 03: overflight rows, endpoint rows, alternatives, call signs
# ---------------------------------------------------------------------------


class _Storage:
    def __init__(self, tables=None):
        self.tables = tables or {}
        self.written = {}

    def table_exists(self, name):
        return name in self.tables

    def read_table(self, name):
        return self.tables[name]

    def write_table(self, df, name, mode="overwrite", **kwargs):
        self.written[name] = df.collect()


def _overflight_processor(tables, measured=True):
    class Stub:
        _get_data_within_timeframe = FlightListProcessor._get_data_within_timeframe
        _fetch_overflights = FlightListProcessor._fetch_overflights
        _seen_from_measured_reports = FlightListProcessor._seen_from_measured_reports

        def __init__(self):
            self.tracks_table = "osn_tracks"
            self.storage = _Storage(tables)
            self.detection = DetectionConfig()
            self.config = OPDIConfig()
            self.config.events = replace(EventConfig(), seen_from_measured_reports=measured)

        def _version_for(self, path):
            from opdi.versions import FLIGHT_LIST_VERSION

            return FLIGHT_LIST_VERSION

    return Stub()


_OF_SCHEMA = (
    "track_id string, icao24 string, callsign string, event_time timestamp, "
    "lat double, lon double, baro_altitude double"
)


def test_an_overflight_is_seen_from_its_measured_reports(spark):
    """fl_identity_times#33 AFL1318: first_seen 03:11:59 but positions only from
    04:25:49 to 04:35:44; the overflight row counted the positionless rows."""
    rows = [
        ("t1", "155f00", "AFL1318 ", _t(0), None, None, 11000.0),
        ("t1", "155f00", "AFL1318 ", _t(4430), 30.0, 50.0, 11000.0),
        ("t1", "155f00", "AFL1318 ", _t(5025), 30.5, 51.0, 11000.0),
        ("t1", "155f00", "AFL1318 ", _t(7350), None, None, None),
    ]
    sv = spark.createDataFrame(rows, _OF_SCHEMA)
    fl = spark.createDataFrame([], "id string, first_seen timestamp")
    proc = _overflight_processor({"osn_tracks": sv, "opdi_flight_list": fl})
    (row,) = proc._fetch_overflights(_T0.date()).collect()
    assert (row["first_seen"], row["last_seen"]) == (_t(4430), _t(5025))
    assert row["FLT_ID"] == "AFL1318"
    off = _overflight_processor({"osn_tracks": sv, "opdi_flight_list": fl}, measured=False)
    (row,) = off._fetch_overflights(_T0.date()).collect()
    assert (row["first_seen"], row["last_seen"]) == (_t(0), _t(7350))


def test_the_call_sign_loses_its_padding_and_counts_as_one(spark):
    """Every FLT_ID of the day carried trailing spaces ('BKP935  '). Trimmed,
    'BKP935  ' and 'BKP935' are one call sign, and it outnumbers 'XYZ1'."""
    rows = [("t1", c) for c in ["BKP935  "] * 2 + ["BKP935"] * 2 + ["XYZ1    "] * 3]
    df = spark.createDataFrame(rows, "track_id string, flight_id string")
    assert dominant_flight_id(df, trim=True).collect()[0]["_dominant_flight_id"] == "BKP935"
    assert {r["flight_id"] for r in resolve_flight_id(df, trim=True).collect()} == {"BKP935"}
    assert {r["flight_id"] for r in resolve_flight_id(df).collect()} == {"XYZ1    "}


CAND = (
    "track_id string, icao24 string, flight_id string, role string, event_time timestamp, "
    "apt_ident string, dist_nm double, agl_ft double, elev_known boolean, on_ground boolean, "
    "at_border boolean, apt_scheduled string, climb_ft double, "
    "track_first_seen timestamp, track_last_seen timestamp"
)


def _cand(track, role, apt, dist, agl=0.0, ground=False, sched="yes", climb=None, t=0.0):
    return (
        track,
        "abc123",
        "TEST01",
        role,
        _t(t),
        apt,
        dist,
        agl,
        True,
        ground,
        False,
        sched,
        climb,
        _t(0),
        _t(3600),
    )


def _classify(spark, rows, **kw):
    cand = spark.createDataFrame(rows, CAND)
    out = FlightListProcessor.classify_endpoints(
        cand, abstention_radius_nm=30.0, sched_penalty_nm=10.0, ooa=False, **kw
    )
    return {(r["track_id"], r["role"]): r["apt"] for r in out.collect()}


WAIVER = (2.0, 1500.0)


def test_no_penalty_for_the_unscheduled_airport_the_aircraft_stands_at(spark):
    """fl_departure_airport#24: on the ground 0.5 NM from LFPB (no scheduled
    service); the penalty made LFPG, 4.9 NM away, the departure airport."""
    rows = [
        _cand("t", "adep", "LFPB", 0.5, ground=True, sched="no"),
        _cand("t", "adep", "LFPG", 4.9, ground=True),
    ]
    assert _classify(spark, rows, waiver=WAIVER)[("t", "adep")] == "LFPB"
    assert _classify(spark, rows)[("t", "adep")] == "LFPG"


def test_the_penalty_stays_for_an_aircraft_passing_by(spark):
    """An aircraft in the air 5 NM from a small field and 8 NM from the
    scheduled airport is not at the small field."""
    rows = [
        _cand("t", "ades", "SMAL", 5.0, agl=3000.0, sched="no"),
        _cand("t", "ades", "BIGG", 8.0, agl=3000.0),
    ]
    assert _classify(spark, rows, waiver=WAIVER)[("t", "ades")] == "BIGG"


def test_an_arrival_fragment_names_no_departure(spark):
    """fl_alternative_airports#44: BE20 first seen 29.6 NM from YPKG at 9,700 ft,
    descending at 1,055 ft/min; it lands at YPKG 10 min later."""
    rows = [_cand("t", "adep", "YPKG", 29.6, agl=9700.0, climb=-3100.0)]
    assert _classify(spark, rows, min_climb_ft=200.0)[("t", "adep")] is None
    assert _classify(spark, rows)[("t", "adep")] == "YPKG"


def test_a_departure_first_seen_climbing_is_still_named(spark):
    """Departures first seen at 3,000-10,000 ft while climbing (fl_alternative_airports
    #32, #36, #48) keep their airport, and so do a report on the ground and a
    first report whose climb is unknown."""
    rows = [
        _cand("c", "adep", "AAAA", 12.0, agl=6000.0, climb=4500.0),
        _cand("g", "adep", "AAAA", 0.3, ground=True, climb=-50.0),
        _cand("u", "adep", "AAAA", 8.0, agl=2000.0, climb=None),
        _cand("l", "ades", "AAAA", 8.0, agl=2000.0, climb=None),
    ]
    got = _classify(spark, rows, min_climb_ft=200.0)
    assert got == {
        ("c", "adep"): "AAAA",
        ("g", "adep"): "AAAA",
        ("u", "adep"): "AAAA",
        ("l", "ades"): "AAAA",
    }


def _endpoint_table(spark, rows, **kw):
    cand = spark.createDataFrame(rows, CAND)
    classified = FlightListProcessor.classify_endpoints(
        cand, abstention_radius_nm=30.0, sched_penalty_nm=10.0, ooa=False
    )
    return FlightListProcessor._flight_table_from_endpoints(classified, cand, **kw).collect()


def test_a_row_found_only_at_its_last_report_has_its_identity_and_day(spark):
    """A track whose first report had no airport within 110 NM was written with
    no ICAO24, no FLT_ID, no FIRST_SEEN and a NULL day."""
    rows = [_cand("t", "ades", "EBBR", 2.0, ground=True, t=3600)]
    (row,) = _endpoint_table(spark, rows, both_ends=True)
    assert (row["ICAO24"], row["FLT_ID"]) == ("abc123", "TEST01")
    assert (row["first_seen"], row["last_seen"], row["DOF"]) == (_t(0), _t(3600), _T0.date())
    (old,) = _endpoint_table(spark, rows)
    assert old["ICAO24"] is None and old["DOF"] is None


def test_the_alternatives_are_ranked_like_the_choice_and_cut_at_30_nm(spark):
    """fl_alternative_airports: ADEP_P listed every airport within 110 NM (up to
    43), nearest first."""
    rows = [
        _cand("t", "adep", "CHOS", 1.0, ground=True),
        _cand("t", "adep", "UNSC", 5.0, ground=True, sched="no"),
        _cand("t", "adep", "SCHD", 8.0, ground=True),
        _cand("t", "adep", "FARR", 50.0, ground=True),
    ]
    (row,) = _endpoint_table(spark, rows, alternatives_radius_nm=30.0, sched_penalty_nm=10.0)
    assert row["ADEP_P"] == "SCHD, UNSC"
    (old,) = _endpoint_table(spark, rows)
    assert old["ADEP_P"] == "UNSC, SCHD, FARR"


def test_the_wide_candidate_set_is_still_searched_for_events(spark):
    """The runway and layout search keeps every airport within 110 NM."""
    flights = spark.createDataFrame([("t", ["KPDX", "KTTD"])], "id string, apt array<string>")
    cand = spark.createDataFrame(
        [("t", "adep", "KPDX"), ("t", "adep", "KTTD"), ("t", "adep", "KSLE"), ("t", "ades", "X")],
        "track_id string, role string, apt_ident string",
    )
    storage = _Storage({"opdi_endpoint_candidates": cand})
    (row,) = with_endpoint_candidates(flights, storage, ("adep",)).collect()
    assert sorted(row["apt"]) == ["KPDX", "KSLE", "KTTD"]
    (same,) = with_endpoint_candidates(flights, storage, ()).collect()
    assert same["apt"] == ["KPDX", "KTTD"]
    assert wide_search_roles(DetectionConfig()) == ("adep",)
    assert wide_search_roles(DetectionConfig.legacy()) == ()


def test_the_latest_landing_airport_is_the_destination(spark):
    """fl_destination_airport#44: ZGGG -> PANC -> KMEM area; the trend method
    chose PANC, a landing in the middle of the track, over the end (11 NM from
    KMEM at 2,975 ft)."""
    from test_flight_detection import CLASSIFIED

    rows = []
    for i in range(6):
        t = _t(10 * i)
        rows.append(
            (
                "abc",
                "CS",
                "trk",
                "PANC",
                61.17,
                -150.0,
                t,
                61.17,
                -150.0,
                "landing",
                1,
                _T0,
                _t(30000),
                _T0.date(),
                0,
                1,
            )
        )
    for i in range(6):
        t = _t(29000 + 10 * i)
        rows.append(
            (
                "abc",
                "CS",
                "trk",
                "KMEM",
                35.04,
                -89.98,
                t,
                35.2,
                -89.98,
                "landing",
                1,
                _T0,
                _t(30000),
                _T0.date(),
                0,
                1,
            )
        )
    df = spark.createDataFrame(rows, schema=CLASSIFIED)
    new = FlightListProcessor._compute_flight_table(
        df, rank_by="haversine", last_landing_window_s=600.0
    ).collect()[0]
    assert (new["ADES"], new["ADES_P"]) == ("KMEM", "PANC")
    old = FlightListProcessor._compute_flight_table(df, rank_by="haversine").collect()[0]
    assert old["ADES"] == "PANC"


def test_neighbouring_airports_on_one_final_still_rank_by_distance(spark):
    from test_flight_list_alternatives import _trend_frame

    out = FlightListProcessor._compute_flight_table(
        _trend_frame(spark, ["CCC9", "BBB6", "AAA3"]),
        rank_by="haversine",
        last_landing_window_s=600.0,
    ).collect()[0]
    assert (out["ADES"], out["ADES_P"]) == ("AAA3", "BBB6, CCC9")


def test_duplicate_airport_records_are_dropped(spark):
    """CA-1291 "(Duplicate)YEG" sits on CYEG and won by its code."""
    oa = spark.createDataFrame(
        [("CYEG", "Edmonton International Airport"), ("CA-1291", "(Duplicate)YEG"), ("X", None)],
        "ident string, name string",
    )
    assert sorted(r["ident"] for r in drop_duplicate_airports(oa).collect()) == ["CYEG", "X"]

    proc = FlightListProcessor.__new__(FlightListProcessor)
    proc.storage = _Storage({"oa_airports": oa})
    zones = spark.createDataFrame([("CYEG", "h1"), ("CA-1291", "h1")], "apt_ident string, h string")
    assert [r["apt_ident"] for r in proc._without_duplicate_airports(zones).collect()] == ["CYEG"]


def test_the_endpoint_candidates_carry_the_climb_and_the_track_times(spark, tmp_path):
    """The candidate table gets the climb after the first report and the track's
    first and last positioned time (columns read by the two corrections)."""
    rows = [
        ("trk", "abc123", "TEST01", _t(0), -27.5, 151.0, 2956.0, False, "c1"),
        ("trk", "abc123", "TEST01", _t(60), -27.5, 151.1, 2800.0, False, "c1"),
        ("trk", "abc123", "TEST01", _t(120), -27.5, 151.2, 2010.0, False, "c1"),
        ("trk", "abc123", "TEST01", _t(600), -27.4, 151.7, 200.0, True, "c1"),
    ]
    sv = spark.createDataFrame(
        rows,
        "track_id string, icao24 string, flight_id string, event_time timestamp, lat double, "
        "lon double, baro_altitude double, on_ground boolean, h3_res_7 string",
    )
    zones = spark.createDataFrame(
        [("c1", "YPKG", -27.4, 151.7, "yes")],
        "apt_hex_id string, apt_ident string, apt_latitude_deg double, "
        "apt_longitude_deg double, apt_scheduled string",
    )
    storage = _Storage()
    proc = FlightListProcessor.__new__(FlightListProcessor)
    proc.spark = spark
    proc.config = OPDIConfig()
    proc.detection = DetectionConfig()
    proc.storage = storage
    proc._tracks_table_override = "stub"
    proc._get_data_within_timeframe = lambda table_name, month: sv
    proc._load_airports_hex = lambda *a, **kw: zones
    proc._endpoint_log = str(tmp_path / "log.parquet")
    proc.build_endpoint_candidates(_T0.date(), rebuild=True)
    got = {r["role"]: r for r in storage.written["opdi_endpoint_candidates"]}
    assert round(got["adep"]["climb_ft"]) == round((2010.0 - 2956.0) * 3.28084)
    assert got["ades"]["climb_ft"] is None
    assert got["ades"]["track_first_seen"] == _t(0) and got["adep"]["track_last_seen"] == _t(600)
