"""Point merge: where a leg ends, split legs, straight or climbing legs, merge points.

The cases are those of the spot checks of 2026-06-14 (finding e2): the exit
placed about 25 s before the aircraft leaves the arc (27 of 28 random exits),
one arc at 22.8 NM from IMREN (LTFM) split in two by a 55 s gap in the
positions, a straight descending segment grazing the 19 NM circle at LTFJ
(heading constant at 82 degrees), a missed approach climbing through the leg
band 1.8 NM from LTFJ, the climb-out of EIDW through ABIVU at 2,750 ft/min,
light aircraft at 66 to 96 kt near merge points, and one approach to RJTT
passing the merge points of three systems for other runways.
"""

import datetime as dt
import json
import math

from conftest import _EPOCH, make_track
from navigation_fixtures import fly, legs_to_samples, measured
from pyspark.sql import functions as F

from opdi.config import EventConfig
from opdi.pipeline.point_merge import calculate_point_merge_events
from opdi.reference.point_merge import PMS_SCHEMA

MP = (52.0, 0.0)
SPEED = 250.0
R_NM = 16.0

BEFORE = EventConfig(
    pms_extend_leg_exit=False,
    pms_join_split_legs=False,
    pms_leg_must_turn=False,
    pms_merge_point_conditions=False,
)


def _ref(spark, systems=(("XXXX-ALL", "MERGE", MP),), band=(15.0, 17.0)):
    rows = [
        ("XXXX", pms_id, name, pos[0], pos[1], *band, 7000.0, 9000.0, None, None, "t", "high")
        for pms_id, name, pos in systems
    ]
    return spark.createDataFrame(rows, PMS_SCHEMA)


def _arc_rate(speed_kt=SPEED, r_nm=R_NM) -> float:
    """Right turn rate (deg/s) that keeps the aircraft on a circle of r_nm."""
    return (speed_kt / 3600.0) / r_nm * 180.0 / math.pi


def _bearing(lat1, lon1, lat2, lon2):
    return math.degrees(
        math.atan2(
            math.radians(lon2 - lon1) * math.cos(math.radians(lat1)), math.radians(lat2 - lat1)
        )
    )


def _with_direct(samples, vr_ftmin=-1000.0, speed_kt=SPEED, step_s=5.0, seconds=360):
    """Append a straight run from the last sample through the merge point."""
    last = samples[-1]
    hdg = _bearing(last["lat"], last["lon"], MP[0], MP[1]) % 360
    tail = legs_to_samples(
        [(seconds, 0.0, vr_ftmin)],
        speed_kt=speed_kt,
        alt_ft=last["baro_altitude"] * 3.28084,
        lat0=last["lat"],
        lon0=last["lon"],
        hdg0=hdg,
        t0=last["t"] + step_s,
    )
    return samples + tail


def _sdf(spark, samples):
    return measured(make_track(spark, samples)).withColumn("ades", F.lit("XXXX"))


def _arc(arc_s=180.0, turn_in_s=0.0, vr_ftmin=0.0, speed_kt=SPEED, r_nm=R_NM):
    """An arc of r_nm north of the merge point flown clockwise, then a
    standard-rate right turn towards the point, then the direct."""
    legs = [(arc_s, _arc_rate(speed_kt, r_nm), vr_ftmin)]
    if turn_in_s:
        legs.append((turn_in_s, 1.5, 0.0))
    samples = legs_to_samples(
        legs, lat0=MP[0] + r_nm / 60.0, lon0=MP[1], hdg0=90.0, alt_ft=8000.0, speed_kt=speed_kt
    )
    return _with_direct(samples, speed_kt=speed_kt)


def _events(spark, sdf, config=None, ref=None, runway=None):
    rows = calculate_point_merge_events(
        sdf, ref if ref is not None else _ref(spark), config or EventConfig(), runway_events=runway
    ).collect()
    return sorted(rows, key=lambda r: (r.event_time, r.type))


def _by_type(rows, t):
    return [r for r in rows if r.type == t]


def _s(ts):
    return (ts - _EPOCH).total_seconds()


# --- the exit at the end of the arc ------------------------------------------


def test_the_leg_exit_is_where_the_aircraft_leaves_the_arc(spark):
    sdf = _sdf(spark, _arc(arc_s=200.0, turn_in_s=60.0))
    before = _events(spark, sdf, BEFORE)
    after = _events(spark, sdf)
    [x0] = _by_type(before, "pms-leg-exit")
    [x1] = _by_type(after, "pms-leg-exit")
    # The arc ends at 200 s; the turn-in keeps the track within 60-120
    # degrees of the bearing for about its first 20 s.
    assert _s(x0.event_time) < 200
    assert 200 <= _s(x1.event_time) <= 230
    # The entry does not move.
    assert _by_type(before, "pms-leg-entry")[0].event_time == (
        _by_type(after, "pms-leg-entry")[0].event_time
    )
    i0, i1 = json.loads(x0.info), json.loads(x1.info)
    assert i1["duration_s"] > i0["duration_s"]
    assert i1["distance_nm"] > i0["distance_nm"]
    assert _by_type(after, "pms-merge-point")


def test_a_gap_in_positions_does_not_split_the_leg(spark):
    """LTFM, IMREN at 22.8 NM: 55 s without positions in the middle of the
    arc. At 220 kt the bearing sweeps under 8 degrees in the 45 s the window
    holds on either side of the gap, so the reports next to it fail."""
    samples = [s for s in _arc(arc_s=330.0, speed_kt=220.0, r_nm=22.8) if not (140 <= s["t"] < 195)]
    sdf = _sdf(spark, samples)
    ref = _ref(spark, band=(22.0, 24.0))
    after = _by_type(_events(spark, sdf, ref=ref), "pms-leg-exit")
    assert len(after) == 1
    info = json.loads(after[0].info)
    assert info["leg_seq"] == 1 and info["duration_s"] >= 280
    before = _by_type(_events(spark, sdf, BEFORE, ref=ref), "pms-leg-exit")
    assert all(json.loads(r.info)["duration_s"] < 200 for r in before)


def test_two_legs_flown_in_opposite_directions_stay_apart(spark):
    """A leg, a turn back, a second leg the other way: two legs."""
    rate = _arc_rate()
    legs = [(150, rate, 0.0), (60, 3.0, 0.0), (150, -rate, 0.0)]
    samples = legs_to_samples(legs, lat0=MP[0] + R_NM / 60.0, lon0=MP[1], hdg0=90.0, alt_ft=8000.0)
    sdf = _sdf(spark, _with_direct(samples))
    exits = _by_type(_events(spark, sdf), "pms-leg-exit")
    assert all(json.loads(r.info)["duration_s"] < 250 for r in exits)


def test_a_straight_segment_that_grazes_the_circle_is_not_a_leg(spark):
    """LTFJ pms-leg-exit#35: heading constant for 90 s, at the leg distance
    only where it touches the circle."""
    lat0 = MP[0] + R_NM / 60.0
    lon_start = MP[1] - (SPEED * 75 / 3600.0) / (60.0 * math.cos(math.radians(lat0)))
    samples = legs_to_samples(
        [(150, 0.0, 0.0)], lat0=lat0, lon0=lon_start, hdg0=90.0, alt_ft=8000.0
    )
    sdf = _sdf(spark, _with_direct(samples))
    assert _by_type(_events(spark, sdf, BEFORE), "pms-leg-exit")
    assert not _by_type(_events(spark, sdf), "pms-leg-exit")


def test_a_climbing_turn_through_the_band_is_not_a_leg(spark):
    """LTFJ pms-leg-entry#31: a missed approach climbing at 2,000 ft/min."""
    sdf = _sdf(spark, _arc(arc_s=180.0, vr_ftmin=2000.0))
    assert _by_type(_events(spark, sdf, BEFORE), "pms-leg-exit")
    assert not _by_type(_events(spark, sdf), "pms-leg-exit")


def test_a_descending_leg_is_kept(spark):
    """Legs flown descending are common (LTFJ at 12,000-14,000 ft)."""
    sdf = _sdf(spark, _arc(arc_s=180.0, vr_ftmin=-1500.0))
    assert _by_type(_events(spark, sdf), "pms-leg-exit")


# --- merge-point passages ----------------------------------------------------


def _direct(spark, speed_kt=SPEED, vr_ftmin=-1000.0, alt_ft=8000.0, t0=0.0):
    """Straight at the merge point from 25 NM north."""
    return fly(
        spark,
        [(600 * SPEED / speed_kt, 0.0, vr_ftmin)],
        lat0=MP[0] + 25 / 60.0,
        lon0=MP[1],
        hdg0=180.0,
        alt_ft=alt_ft,
        speed_kt=speed_kt,
        t0=t0,
    ).withColumn("ades", F.lit("XXXX"))


def _runway(spark, rows):
    return spark.createDataFrame(
        [
            ("trk-1", t, _EPOCH + dt.timedelta(seconds=s), json.dumps({"apt_icao": a, "runway": r}))
            for t, s, a, r in rows
        ],
        "track_id string, type string, event_time timestamp, info string",
    )


def test_an_arrival_cleared_direct_keeps_its_merge_point(spark):
    rows = _events(spark, _direct(spark), runway=_runway(spark, [("ALDT", 1200, "XXXX", "34R")]))
    assert [r.type for r in rows] == ["pms-merge-point"]


def test_a_departure_climbing_through_the_merge_point_is_not_one(spark):
    """EIDW: climbing at 2,750 ft/min through ABIVU."""
    sdf = _direct(spark, vr_ftmin=2750.0, alt_ft=3000.0)
    assert _events(spark, sdf, BEFORE)
    assert not _events(spark, sdf)


def test_a_light_aircraft_near_the_merge_point_is_not_one(spark):
    """A C152 at 90 kt near a Dublin merge point."""
    sdf = _direct(spark, speed_kt=90.0, vr_ftmin=0.0)
    assert _events(spark, sdf, BEFORE)
    assert not _events(spark, sdf)


def test_a_merge_point_before_a_take_off_of_the_track_is_not_one(spark):
    """A round trip: the track passes the merge point, lands elsewhere and
    takes off again for the destination."""
    runway = _runway(spark, [("ATOT", 1000, "YYYY", "09"), ("ALDT", 5000, "XXXX", "34R")])
    assert not _events(spark, _direct(spark), runway=runway)


def test_a_merge_point_of_the_first_of_two_flights_in_one_track_is_kept(spark):
    """One track holds two flights: the first passes the merge point, lands
    at the airport (1,200 s) and the aircraft takes off again (3,000 s). No
    take-off lies between the merge point and the landing that follows it,
    so the passage is the first flight's arrival through the system."""
    runway = _runway(spark, [("ALDT", 1200, "XXXX", "34R"), ("ATOT", 3000, "XXXX", "16L")])
    assert [r.type for r in _events(spark, _direct(spark), runway=runway)] == ["pms-merge-point"]


def test_a_system_for_another_runway_is_not_one(spark):
    """RJTT: WEDGE (34L system) passed, landing on 16L."""
    ref = _ref(spark, systems=(("XXXX-34L", "MERGE", MP),))
    land_16 = _runway(spark, [("ALDT", 1200, "XXXX", "16L")])
    land_34 = _runway(spark, [("ALDT", 1200, "XXXX", "34R")])
    assert not _events(spark, _direct(spark), ref=ref, runway=land_16)
    assert len(_events(spark, _direct(spark), ref=ref, runway=land_34)) == 1
    # A system serves the near-parallel runways of its flow (LTFM-16NW: 18).
    ltfm = _ref(spark, systems=(("XXXX-16NW", "MERGE", MP),))
    land_18 = _runway(spark, [("ALDT", 1200, "XXXX", "18")])
    assert len(_events(spark, _direct(spark), ref=ltfm, runway=land_18)) == 1
    # A system named for no runway serves them all.
    assert len(_events(spark, _direct(spark), runway=land_16)) == 1


def test_one_merge_point_per_approach(spark):
    """Three systems' points along one arrival route: one event, the
    closest."""
    ref = _ref(
        spark,
        systems=(
            ("XXXX-ALL", "MERGE", MP),
            ("XXXX-34", "NORTH", (MP[0] + 6 / 60.0, MP[1] + 0.02)),
            ("XXXX-22", "FAR", (MP[0] + 12 / 60.0, MP[1] - 0.02)),
        ),
    )
    land = _runway(spark, [("ALDT", 1200, "XXXX", "34R")])
    assert len(_events(spark, _direct(spark), BEFORE, ref=ref)) == 3
    rows = _events(spark, _direct(spark), ref=ref, runway=land)
    assert len(rows) == 1 and json.loads(rows[0].info)["merge_point"] == "MERGE"


def test_v002_settings_switch_the_point_merge_corrections_off():
    legacy = EventConfig.legacy()
    assert not (
        legacy.pms_extend_leg_exit
        or legacy.pms_join_split_legs
        or legacy.pms_leg_must_turn
        or legacy.pms_merge_point_conditions
    )
