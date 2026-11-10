"""Point merge: a sequencing leg at constant distance from the merge point, then a direct."""

import json
import math

import pytest
from navigation_fixtures import fly
from pyspark.sql import functions as F

from opdi.config import EventConfig
from opdi.pipeline.point_merge import PMS_VERSION, calculate_point_merge_events
from opdi.reference.point_merge import PMS_SCHEMA

MP = (52.0, 0.0)          # the merge point
SPEED = 250.0
R_NM = 16.0               # the leg's distance from the merge point


def _ref(spark, apt="XXXX"):
    row = (apt, f"{apt}-ALL", "MERGE", MP[0], MP[1], 15.0, 17.0, 7000.0, 9000.0, None, None, "test", "high")
    return spark.createDataFrame([row], PMS_SCHEMA)


def _arc_rate(clockwise: bool) -> float:
    """Turn rate (deg/s) that keeps a 250 kt aircraft on a circle of R_NM."""
    rate = (SPEED / 3600.0) / R_NM * 180.0 / math.pi
    return -rate if clockwise else rate


def _arc_then_direct(spark, arc_s=180.0, direct=True, ades="XXXX"):
    # Start R_NM due north of the merge point heading east: clockwise as seen
    # from the point (bearing from the point increases). A right turn at the
    # arc rate keeps the distance constant.
    lat0 = MP[0] + R_NM / 60.0
    legs = [(arc_s, -_arc_rate(True), 0.0)]
    samples_sdf = fly(spark, legs, lat0=lat0, lon0=MP[1], hdg0=90.0, alt_ft=8000.0, speed_kt=SPEED)
    last = samples_sdf.orderBy(F.col("event_time").desc()).first()
    if direct:
        # Turn onto the bearing to the merge point and fly through it.
        brg = math.degrees(math.atan2(
            math.radians(MP[1] - last.lon) * math.cos(math.radians(last.lat)),
            math.radians(MP[0] - last.lat)))
        tail = fly(spark, [(300, 0.0, 0.0)], lat0=last.lat, lon0=last.lon, hdg0=brg % 360,
                   alt_ft=8000.0, speed_kt=SPEED, t0=float(last.cumulative_time_s) + 5)
    else:
        tail = fly(spark, [(300, 0.0, 0.0)], lat0=last.lat, lon0=last.lon, hdg0=float(last.heading),
                   alt_ft=8000.0, speed_kt=SPEED, t0=float(last.cumulative_time_s) + 5)
    cols = ["track_id", "icao24", "lat", "lon", "baro_altitude", "geo_altitude", "velocity",
            "heading", "vert_rate", "on_ground", "last_contact", "last_pos_update", "callsign",
            "near_airport", "field_elev_ft", "event_time"]
    from navigation_fixtures import measured
    raw = samples_sdf.select(*cols).unionByName(tail.select(*cols))
    return measured(raw).withColumn("ades", F.lit(ades))


def test_an_arc_followed_by_a_direct_gives_a_leg_pair_and_a_merge_point(spark):
    sdf = _arc_then_direct(spark)
    rows = calculate_point_merge_events(sdf, _ref(spark), EventConfig()).collect()
    types = sorted(r.type for r in rows)
    assert types == ["pms-leg-entry", "pms-leg-exit", "pms-merge-point"]
    assert all(r.version == PMS_VERSION == "pms_v0.1.0" for r in rows)
    by = {r.type: r for r in rows}
    info = json.loads(by["pms-leg-exit"].info)
    assert info == json.loads(by["pms-leg-entry"].info)
    assert info["merge_point"] == "MERGE" and info["apt_icao"] == "XXXX"
    assert 15.0 <= info["leg_distance_nm"] <= 17.0
    assert info["direction"] == "cw"
    assert 14.0 <= info["direct_to_distance_nm"] <= 18.0
    assert 120 <= info["duration_s"] <= 200
    assert abs(info["leg_level_ft"] - 8000.0) < 50
    assert by["pms-leg-exit"].event_time > by["pms-leg-entry"].event_time
    mp = json.loads(by["pms-merge-point"].info)
    assert mp["closest_nm"] < 3.0 and mp["via_leg"] is True


def test_an_arc_without_a_direct_is_not_a_leg(spark):
    sdf = _arc_then_direct(spark, direct=False)
    assert calculate_point_merge_events(sdf, _ref(spark), EventConfig()).count() == 0


def test_a_direct_without_an_arc_gives_only_the_merge_point(spark):
    # Straight at the merge point from 25 NM north.
    sdf = fly(spark, [(600, 0.0, 0.0)], lat0=MP[0] + 25 / 60.0, lon0=MP[1], hdg0=180.0,
              alt_ft=8000.0).withColumn("ades", F.lit("XXXX"))
    rows = calculate_point_merge_events(sdf, _ref(spark), EventConfig()).collect()
    assert [r.type for r in rows] == ["pms-merge-point"]
    assert json.loads(rows[0].info)["via_leg"] is False


def test_a_destination_without_a_pms_yields_nothing(spark):
    sdf = _arc_then_direct(spark, ades="YYYY")
    assert calculate_point_merge_events(sdf, _ref(spark), EventConfig()).count() == 0


def test_a_leg_is_not_paired_across_a_hole(spark):
    # 80 s of arc, 40 min of silence, 80 s of arc, then the direct: each piece
    # is shorter than the 90 s minimum, so no leg pair survives.
    lat0 = MP[0] + R_NM / 60.0
    rate = -_arc_rate(True)
    sdf = fly(spark, [(80, rate, 0.0), (-2400, 0, 0), (80, rate, 0.0)],
              lat0=lat0, lon0=MP[1], hdg0=90.0, alt_ft=8000.0).withColumn("ades", F.lit("XXXX"))
    rows = calculate_point_merge_events(sdf, _ref(spark), EventConfig()).collect()
    assert not [r for r in rows if r.type.startswith("pms-leg")]


def test_off_flag_returns_none(spark):
    sdf = _arc_then_direct(spark)
    assert calculate_point_merge_events(sdf, _ref(spark), EventConfig(emit_point_merge=False)) is None
