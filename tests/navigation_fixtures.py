"""Synthetic flights for the holding, point-merge and FIR tests.

``fly`` integrates a list of legs on a flat earth and returns the frame step 04
hands a detector: the ``make_track`` columns plus ``baro_altitude_c``, the two
cumulative measures, ``piece`` and the aerodrome elevations. The numbers are
small enough that the flat-earth step is exact to well under the thresholds
the detectors use.
"""

import math
from typing import List, Tuple

from conftest import make_track
from pyspark.sql import functions as F

from opdi.pipeline.events import add_distance_measure, add_time_measure
from opdi.pipeline.holes import add_piece

KT_TO_MS = 1 / 1.94384
FT_TO_M = 1 / 3.28084
FTMIN_TO_MS = 1 / 196.850394

Leg = Tuple[float, float, float]  # (duration_s, turn_rate_deg_s, vert_rate_ftmin)


def legs_to_samples(
    legs: List[Leg],
    *,
    speed_kt=250.0,
    alt_ft=10000.0,
    lat0=51.0,
    lon0=0.0,
    hdg0=90.0,
    t0=0.0,
    step_s=5.0,
    callsign="TEST123",
    heading_null: bool = False,
):
    """Sample dicts for ``make_track``: one every ``step_s`` seconds.

    A leg with ``duration_s <= 0`` is a silence: time advances, nothing is
    sampled, so a reception hole can be written as ``(-2400, 0, 0)``.
    """
    lat, lon, hdg, alt, t = lat0, lon0, hdg0, alt_ft, t0
    out = []
    for duration, rate, vr in legs:
        if duration <= 0:
            t += -duration
            continue
        n = int(round(duration / step_s))
        for _ in range(n):
            out.append(
                {
                    "t": t,
                    "lat": lat,
                    "lon": lon,
                    "heading": hdg,
                    "velocity": speed_kt * KT_TO_MS,
                    "baro_altitude": alt * FT_TO_M,
                    "vert_rate": vr * FTMIN_TO_MS,
                    "callsign": callsign,
                }
            )
            dist_nm = speed_kt * step_s / 3600.0
            lat += dist_nm * math.cos(math.radians(hdg)) / 60.0
            lon += dist_nm * math.sin(math.radians(hdg)) / (60.0 * math.cos(math.radians(lat)))
            hdg = (hdg + rate * step_s) % 360.0
            alt += vr * step_s / 60.0
            t += step_s
    return out


def measured(sdf, hole_seconds: float = 1800.0, elev_ft: float = 0.0):
    """Add what step 04 adds before a detector sees the frame."""
    sdf = sdf.withColumn("baro_altitude_c", F.col("baro_altitude"))
    sdf = add_distance_measure(sdf)
    sdf = add_time_measure(sdf)
    sdf = add_piece(sdf, hole_seconds)
    return sdf.withColumn("elev_ades_ft", F.lit(elev_ft)).withColumn("elev_adep_ft", F.lit(elev_ft))


def fly(
    spark,
    legs: List[Leg],
    *,
    speed_kt=250.0,
    alt_ft=10000.0,
    lat0=51.0,
    lon0=0.0,
    hdg0=90.0,
    t0=0.0,
    step_s=5.0,
    track_id="trk-1",
    callsign="TEST123",
):
    samples = legs_to_samples(
        legs,
        speed_kt=speed_kt,
        alt_ft=alt_ft,
        lat0=lat0,
        lon0=lon0,
        hdg0=hdg0,
        t0=t0,
        step_s=step_s,
        callsign=callsign,
    )
    return measured(make_track(spark, samples, track_id=track_id))


def racetrack(n_circuits: int, leg_s: float = 60.0, turn_rate: float = 1.5) -> List[Leg]:
    """Right-hand racetrack: straight leg, 180° turn, twice per circuit."""
    return [(leg_s, 0.0, 0.0), (180.0 / turn_rate, turn_rate, 0.0)] * (2 * n_circuits)
