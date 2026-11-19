"""PRU top of climb and top of descent.

Geometry is exact by construction. For a climb to 30,000 ft the exclusion box
runs from 27,000 ft (90%) to 30,000 ft, and a hold inside it counts as cruise
only if it lasts over 300 s. So:

  * 27,500 ft for 360 s -- inside the box, long enough: ToC-CCO moves to the
    start of the hold.
  * 27,500 ft for 240 s -- inside the box, too short: the top does not move.
  * 20,000 ft for 600 s -- long enough but below the box: the top does not
    move, and this is a level-off to be reported rather than cruise.

Those three cases are the whole rule.

Positions are exact too: one nautical mile is 1/60 of a degree of latitude, so
a track laid out due north of its departure aerodrome has an arithmetic
distance from it, and the 200 NM ring falls between two named samples rather
than wherever the trigonometry happens to land.
"""

import datetime as dt
import json

import pytest
from conftest import _EPOCH, make_track
from pyspark.sql import functions as F

from opdi.config import EventConfig
from opdi.pipeline.elevation import attach_field_elevation
from opdi.pipeline.vertical_pru import (
    attach_aerodrome_geometry,
    pru_top_events,
    pru_tops,
)

FT_PER_M = 3.28084

#: Departure aerodrome, and an arrival 600 NM due north of it. The arrival is
#: deliberately far: these are climb cases, and a track that never comes within
#: 200 NM of its destination has no descent to confuse them with.
ADEP_LAT, ADEP_LON = 50.0, 4.0
ADES_LAT, ADES_LON = ADEP_LAT + 600.0 / 60.0, 4.0

MONTH = dt.date(2024, 6, 1)

SEGMENT_SCHEMA = (
    "track_id string, start_time timestamp, end_time timestamp, "
    "duration_seconds double, level_ft double, distance_nm double"
)


def _outbound(spark, profile, step_s=60):
    """A track leaving ADEP due north.

    ``profile`` is a list of ``(altitude_ft, distance_from_adep_nm)``, one entry
    per ``step_s`` seconds. Latitude is ``ADEP_LAT + distance / 60``, so the
    haversine distance the detector computes is the distance the test named.
    """
    sdf = make_track(
        spark,
        [
            {
                "t": i * step_s,
                "baro_altitude": alt_ft / FT_PER_M,
                "lat": ADEP_LAT + d_nm / 60.0,
                "lon": ADEP_LON,
            }
            for i, (alt_ft, d_nm) in enumerate(profile)
        ],
    )
    return (
        sdf.withColumn("baro_altitude_c", F.col("baro_altitude"))
        .withColumn("adep_lat", F.lit(ADEP_LAT))
        .withColumn("adep_lon", F.lit(ADEP_LON))
        .withColumn("ades_lat", F.lit(ADES_LAT))
        .withColumn("ades_lon", F.lit(ADES_LON))
        .withColumn("cumulative_distance_nm", F.lit(0.0))
        .withColumn("cumulative_time_s", F.col("event_time").cast("double"))
    )


def _climb(spark):
    """A 30 min climb to 30,000 ft, ending 150 NM out -- inside the ring.

    The ring is never reached, so D200 falls back to the last sample and
    ToC-D200 is the final one, at t+1800 s. Every hold ``_hold`` builds starts
    at t+600 s and so is comfortably inside the climb.
    """
    return _outbound(spark, [(1000 * i, 5.0 * i) for i in range(31)])


def _climb_crossing_200nm(spark):
    """The same climb, then 30 min of cruise out to 300 NM.

    The 200 NM ring is crossed at t+2400 s, by which time the aircraft has been
    at 30,000 ft for ten minutes, so the highest point inside the ring is the
    top of the climb itself.
    """
    return _outbound(
        spark,
        [(1000 * i, 5.0 * i) for i in range(31)] + [(30000, 5.0 * i) for i in range(31, 61)],
    )


def _climb_then_step_climb_at_250nm(spark):
    """As above, but a step climb to 36,000 ft beginning 250 NM out.

    250 NM is outside the ring, so the 36,000 ft the aircraft reaches there is
    invisible to the analysis and the top of climb stays at 30,000 ft.
    """
    return _outbound(
        spark,
        [(1000 * i, 5.0 * i) for i in range(31)]
        + [(30000, 5.0 * i) for i in range(31, 51)]
        + [(30000 + 600 * (i - 50), 5.0 * i) for i in range(51, 61)],
    )


def _hold(spark, level_ft, seconds):
    """One level segment starting 600 s into the climb."""
    start = _EPOCH + dt.timedelta(seconds=600)
    return spark.createDataFrame(
        [
            (
                "trk-1",
                start,
                start + dt.timedelta(seconds=seconds),
                float(seconds),
                float(level_ft),
                None,
            )
        ],
        schema=SEGMENT_SCHEMA,
    )


def _no_segments(spark):
    return spark.createDataFrame([], schema=SEGMENT_SCHEMA)


def test_toc_d200_is_the_highest_point_inside_the_ring(spark):
    tops = pru_tops(_climb_crossing_200nm(spark), _no_segments(spark), EventConfig())
    assert tops.collect()[0]["toc_d200_alt_ft"] == pytest.approx(30000, abs=50)


def test_a_long_level_high_in_the_box_moves_the_top_back(spark):
    """27,500 ft is 91.7% of 30,000, so it is inside the 90% box; 360 s exceeds
    the 300 s limit. Both conditions hold, so this hold is cruise and the top
    moves back to where it started."""
    tops = pru_tops(
        _climb(spark), _hold(spark, level_ft=27500, seconds=360), EventConfig()
    ).collect()[0]
    assert tops["toc_relocated"] is True
    assert tops["toc_cco_time"] < tops["toc_d200_time"]


def test_a_short_level_in_the_box_does_not_move_the_top(spark):
    tops = pru_tops(
        _climb(spark), _hold(spark, level_ft=27500, seconds=240), EventConfig()
    ).collect()[0]
    assert tops["toc_relocated"] is False
    assert tops["toc_cco_time"] == tops["toc_d200_time"]


def test_a_long_level_below_the_box_does_not_move_the_top(spark):
    """20,000 ft is 67% of 30,000, below the 90% floor: that is a level-off,
    not cruise, and it must be reported as one rather than relocating the top."""
    tops = pru_tops(
        _climb(spark), _hold(spark, level_ft=20000, seconds=600), EventConfig()
    ).collect()[0]
    assert tops["toc_relocated"] is False


def test_the_analysis_stops_at_the_200nm_ring(spark):
    """A step climb 250 NM out is cruise-altitude optimisation, not a top of
    climb, and PRU's radius exists precisely to exclude it."""
    tops = pru_tops(
        _climb_then_step_climb_at_250nm(spark), _no_segments(spark), EventConfig()
    ).collect()[0]
    assert tops["toc_d200_alt_ft"] == pytest.approx(30000, abs=50)


def test_the_events_name_the_algorithm_that_produced_them(spark):
    """Both tops are published, so every row has to say which one it is. The
    PRU pair carries ``method: "pru"`` and the D200 position it was relocated
    from; the fuzzy pair is stamped where it is built."""
    events = pru_top_events(_climb(spark), _hold(spark, level_ft=27500, seconds=360), EventConfig())
    by_type = {r["type"]: r for r in events.collect()}

    assert set(by_type) == {"top-of-climb", "top-of-descent"}
    info = json.loads(by_type["top-of-climb"]["info"])
    assert info["method"] == "pru"
    assert info["relocated"] is True
    assert info["analysis_radius_nm"] == pytest.approx(200.0)
    assert info["d200_altitude_ft"] == pytest.approx(30000, abs=50)


def test_no_pru_events_when_the_family_is_switched_off(spark):
    """``legacy()`` published neither, so a legacy run must emit neither."""
    events = pru_top_events(_climb(spark), _no_segments(spark), EventConfig.legacy())
    assert events.count() == 0


class StubStorage:
    """Just enough StorageManager for the geometry join."""

    def __init__(self, tables):
        self._tables = tables

    def table_exists(self, name):
        return name in self._tables

    def read_table(self, name):
        return self._tables[name]


def _airport_storage(spark):
    """One flight, EHAM to LSZH, with both aerodromes in OurAirports."""
    return StubStorage(
        {
            "opdi_flight_list": spark.createDataFrame(
                [("trk-1", dt.datetime(2024, 6, 1, 12, 0), "EHAM", "LSZH")],
                "id string, dof timestamp, adep string, ades string",
            ),
            "oa_airports": spark.createDataFrame(
                [("EHAM", 52.309, 4.764, -11.0), ("LSZH", 47.458, 8.548, 1416.0)],
                "ident string, latitude_deg double, longitude_deg double, " "elevation_ft double",
            ),
        }
    )


def _bare(spark):
    """The climb with no geometry attached, as it reaches the family."""
    return _climb(spark).drop("adep_lat", "adep_lon", "ades_lat", "ades_lon")


def test_aerodrome_geometry_attaches_both_ends(spark):
    """Both ends, and coordinates as well as elevation: the PRU radius is
    measured from the departure aerodrome in climb and the arrival one in
    descent, so one end is never enough."""
    row = attach_aerodrome_geometry(_bare(spark), MONTH, _airport_storage(spark)).collect()[0]

    assert row["adep"] == "EHAM"
    assert row["adep_lat"] == pytest.approx(52.309)
    assert row["elev_adep_ft"] == pytest.approx(-11.0)
    assert row["ades"] == "LSZH"
    assert row["ades_lon"] == pytest.approx(8.548)
    assert row["elev_ades_ft"] == pytest.approx(1416.0)


def test_a_duplicated_flight_list_id_fails_loudly(spark):
    """A left join hides a duplicate; it does not reject it.

    Every state vector of the affected track would be multiplied by the number
    of duplicate rows, and every event derived from it with them -- level
    segments detected over a doubled trajectory, two of every runway milestone,
    inflated counts in exactly the ladder the study reports. Nothing in the
    output would say so, which is why this is an error and not a warning.
    """
    duplicated = StubStorage(
        {
            "opdi_flight_list": spark.createDataFrame(
                [
                    ("trk-1", dt.datetime(2024, 6, 1, 12, 0), "EHAM", "LSZH"),
                    ("trk-1", dt.datetime(2024, 6, 1, 12, 0), "EHAM", "EDDF"),
                ],
                "id string, dof timestamp, adep string, ades string",
            ),
        }
    )

    with pytest.raises(ValueError, match="distinct ids"):
        attach_aerodrome_geometry(_bare(spark), MONTH, duplicated)


def test_the_geometry_join_survives_the_elevation_join(spark):
    """``attach_field_elevation`` runs first in the pipeline and attaches four
    of the same eight columns for the phase family. Joining on top of them
    leaves two columns of each name and an ``AMBIGUOUS_REFERENCE`` at the first
    reference -- in production, where the two are wired in sequence, and in no
    test that calls either alone.
    """
    storage = _airport_storage(spark)

    once = attach_field_elevation(_bare(spark), MONTH, storage)
    twice = attach_aerodrome_geometry(once, MONTH, storage)

    for name in ("adep", "ades", "elev_adep_ft", "elev_ades_ft", "adep_lat"):
        assert twice.columns.count(name) == 1, name
    # Reading a column is the assertion: an ambiguous reference raises here.
    row = twice.select("adep", "elev_ades_ft", "ades_lon").collect()[0]
    assert row["adep"] == "EHAM"
    assert row["elev_ades_ft"] == pytest.approx(1416.0)
    assert row["ades_lon"] == pytest.approx(8.548)

    # And a third pass changes nothing: the function is idempotent, not merely
    # tolerant of the one collision it was found with.
    thrice = attach_aerodrome_geometry(twice, MONTH, storage)
    assert thrice.columns == twice.columns


# --- where a top is placed, and whether its climb or descent was received ---


def _cruise_start(spark):
    """Shape of top-of-climb#16 (ea2bdce569d29342_1781460544): the track starts
    in cruise at 32,950 ft (no departure airport known) and descends at the
    end. No climb was received."""
    return _outbound(spark, [(32950, 5.0 * i) for i in range(20)] + [(20000, 100.0)])


def _by_type(events):
    return {r["type"]: r for r in events.collect()}


def test_a_top_of_climb_at_the_start_of_reception_is_not_observed(spark):
    tops = _by_type(pru_top_events(_cruise_start(spark), _no_segments(spark), EventConfig()))
    assert json.loads(tops["top-of-climb"]["info"])["observed"] is False


def test_a_received_climb_and_descent_are_observed(spark):
    """Climb to 30,000 ft, cruise, and descend to 5,000 ft 10 NM before the
    destination (600 NM out)."""
    profile = (
        [(1000 * i, 5.0 * i) for i in range(31)]
        + [(30000, 5.0 * i) for i in range(31, 93)]
        + [(30000 - 1000 * (i - 92), 5.0 * i) for i in range(93, 118)]
    )
    tops = _by_type(pru_top_events(_outbound(spark, profile), _no_segments(spark), EventConfig()))
    assert json.loads(tops["top-of-climb"]["info"])["observed"] is True
    assert json.loads(tops["top-of-descent"]["info"])["observed"] is True


def test_a_top_of_descent_at_the_end_of_reception_is_not_observed(spark):
    """Shape of top-of-descent#07: the track ends in cruise (reception lost)."""
    sdf = _outbound(spark, [(1000 * i, 5.0 * i) for i in range(31)] + [(30000, 155.0)])
    tops = _by_type(pru_top_events(sdf, _no_segments(spark), EventConfig()))
    assert json.loads(tops["top-of-descent"]["info"])["observed"] is False


def test_the_release_settings_write_no_observed_flag(spark):
    config = EventConfig(tops_observed_min_change_ft=None)
    tops = _by_type(pru_top_events(_cruise_start(spark), _no_segments(spark), config))
    assert "observed" not in json.loads(tops["top-of-climb"]["info"])


def _top_on_a_report_without_position(spark):
    """Shape of top-of-climb#05 (827bd881efd89cb4_1781440424): the top falls on
    a report that carries only an altitude; the report 10 s before it has a
    position, and so has the one 15 s after it."""
    rows = [
        {"t": 0, "alt": 1000, "pos": True},
        {"t": 600, "alt": 9000, "pos": True},
        {"t": 610, "alt": 9900, "pos": False},
        {"t": 625, "alt": 9800, "pos": True},
        {"t": 900, "alt": 3000, "pos": True},
    ]
    sdf = make_track(
        spark,
        [
            {
                "t": r["t"],
                "baro_altitude": r["alt"] / FT_PER_M,
                "lat": ADEP_LAT + r["t"] / 6000.0 if r["pos"] else None,
                "lon": ADEP_LON if r["pos"] else None,
            }
            for r in rows
        ],
    )
    return (
        sdf.withColumn("baro_altitude_c", F.col("baro_altitude"))
        .withColumn("adep_lat", F.lit(ADEP_LAT))
        .withColumn("adep_lon", F.lit(ADEP_LON))
        .withColumn("ades_lat", F.lit(ADES_LAT))
        .withColumn("ades_lon", F.lit(ADES_LON))
        # One NM every 6 s, so the distance names the report it comes from.
        .withColumn(
            "cumulative_distance_nm",
            (F.col("event_time").cast("double") - F.lit(_EPOCH.timestamp())) / 6.0,
        )
        .withColumn(
            "cumulative_time_s",
            F.col("event_time").cast("double") - F.lit(_EPOCH.timestamp()),
        )
    )


def test_a_top_takes_the_position_of_the_nearest_report_that_has_one(spark):
    sdf = _top_on_a_report_without_position(spark)
    top = _by_type(pru_top_events(sdf, _no_segments(spark), EventConfig()))["top-of-climb"]
    assert top["event_time"] == _EPOCH + dt.timedelta(seconds=610)
    assert top["lat"] == pytest.approx(ADEP_LAT + 600 / 6000.0)
    assert top["cumulative_distance_nm"] == pytest.approx(100.0)
    assert top["cumulative_time_s"] == pytest.approx(610.0)

    old = _by_type(
        pru_top_events(
            sdf, _no_segments(spark), EventConfig(tops_position_from_nearest_report=False)
        )
    )["top-of-climb"]
    assert old["lat"] is None


def test_level_offs_get_a_position_a_distance_and_a_time(spark):
    """A climb with a 120 s level-off at 10,000 ft, 10 s reports. The
    level-off's start and end lie on the level method's time grid; each takes
    the position of the nearest report that has one."""
    from opdi.pipeline.events import calculate_level_off_events

    profile = (
        [(4000 + 500 * i, 0.5 * i) for i in range(13)]  # to 10,000 ft
        + [(10000, 6 + 0.5 * i) for i in range(1, 13)]  # 120 s level
        + [(10000 + 500 * i, 12 + 0.5 * i) for i in range(1, 41)]  # to 30,000 ft
        + [(30000, 32 + 0.5 * i) for i in range(1, 10)]
    )
    sdf = _outbound(spark, profile, step_s=10)
    sdf = sdf.withColumn(
        "cumulative_distance_nm", (F.col("lat") - F.lit(ADEP_LAT)) * F.lit(60.0)
    ).withColumn("cumulative_time_s", F.col("event_time").cast("long") - F.lit(0))
    t0 = sdf.agg(F.min(F.col("event_time").cast("double"))).first()[0]
    sdf = sdf.withColumn("cumulative_time_s", F.col("event_time").cast("double") - F.lit(t0))

    rows = {r["type"]: r for r in calculate_level_off_events(sdf, None, EventConfig()).collect()}
    start, end = rows["level-off-climb-start"], rows["level-off-climb-end"]
    assert start["event_time"] == _EPOCH + dt.timedelta(seconds=120)
    assert start["lat"] == pytest.approx(ADEP_LAT + 6.0 / 60.0)
    assert start["cumulative_distance_nm"] == pytest.approx(6.0)
    assert start["cumulative_time_s"] == pytest.approx(120.0)
    assert end["cumulative_time_s"] == pytest.approx(240.0)

    old = calculate_level_off_events(sdf, None, EventConfig(level_offs_get_position=False))
    assert all(r["lat"] is None for r in old.collect())
