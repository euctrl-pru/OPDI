import pytest
from pyspark.sql import functions as F

from conftest import make_track
from opdi.pipeline.elevation import height_above_field_ft

FT_PER_M = 3.28084


def _with_elevations(sdf, adep_ft, ades_ft):
    """Add what the shared fixture does not carry.

    ``make_track`` builds the raw ``TRACK_SCHEMA`` and **silently ignores any
    key not in it** -- there is no ``baro_altitude_c`` there, only the raw
    ``baro_altitude``. Every event detector reads ``baro_altitude_c`` (the
    step-02 repaired column), so tests alias it on, exactly as
    ``test_events_phase._measured`` does. Passing ``baro_altitude_c`` to
    ``make_track`` directly would be dropped without error and the test would
    run against the fixture's default 10,000 m.
    """
    return (
        sdf.withColumn("baro_altitude_c", F.col("baro_altitude"))
        .withColumn("elev_adep_ft", F.lit(adep_ft).cast("double"))
        .withColumn("elev_ades_ft", F.lit(ades_ft).cast("double"))
    )


def test_height_is_measured_against_the_more_permissive_field(spark):
    """A track is on the ground at only one of its two aerodromes.

    Cruise sits far above both, so taking the *smaller* of the two heights can
    only ever understate -- which is what makes it safe to use without a
    per-sample distance deciding which end applies. Here the aircraft is at
    2,000 ft pressure with a 300 ft departure field and a 1,416 ft arrival
    field: the answer must be 584 ft, not 1,700 ft.
    """
    sv = make_track(spark, [{"t": 0, "baro_altitude": 2000.0 / FT_PER_M}])
    sv = _with_elevations(sv, 300.0, 1416.0)
    got = sv.select(height_above_field_ft(sv).alias("h")).collect()[0]["h"]
    assert got == pytest.approx(584.0, abs=1.0)


def test_a_missing_elevation_falls_back_to_the_datum(spark):
    """NULL means OurAirports has no elevation for the aerodrome, or the flight
    list named none. Coalescing to zero reproduces today's behaviour exactly
    rather than dropping the sample."""
    sv = make_track(spark, [{"t": 0, "baro_altitude": 2000.0 / FT_PER_M}])
    sv = _with_elevations(sv, None, None)
    got = sv.select(height_above_field_ft(sv).alias("h")).collect()[0]["h"]
    assert got == pytest.approx(2000.0, abs=1.0)
