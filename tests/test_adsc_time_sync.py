"""The along/cross decomposition, against a track with a known injected lag."""

import datetime as dt
import math

import pytest
from pyspark.sql import Row, functions as F

from adsc_time_sync import bracket, decompose, per_airframe

T0 = dt.datetime(2026, 6, 5, 12, 0, 0)

#: Due east at a round speed. Everything below is checked against hand
#: arithmetic, so the test fails on a wrong projection rather than agreeing
#: with itself.
LAT = 50.0
V_MS = 250.0
R = 6371000.0


def _lon_at(seconds):
    """Longitude after *seconds* of due-east flight at V_MS from lon 0."""
    return math.degrees(V_MS * seconds / (R * math.cos(math.radians(LAT))))


def _adsb(spark, times, icao24="4acb41"):
    return spark.createDataFrame([
        Row(icao24=icao24, event_time=T0 + dt.timedelta(seconds=s),
            lat=LAT, lon=_lon_at(s), velocity=V_MS,
            last_pos_update=(T0 + dt.timedelta(seconds=s)).timestamp())
        for s in times])


def _adsc(spark, at_seconds, lag_seconds=0.0, cross_m=0.0):
    """An ADS-C report tagged `at_seconds` whose position is the aircraft's
    position at `at_seconds + lag_seconds`, offset `cross_m` to the north."""
    return spark.createDataFrame([
        Row(icao24="4acb41",
            event_time=T0 + dt.timedelta(seconds=at_seconds),
            lat=LAT + math.degrees(cross_m / R),
            lon=_lon_at(at_seconds + lag_seconds),
            velocity=V_MS)])


def test_a_synchronised_report_shows_no_offset(spark):
    out = decompose(bracket(_adsc(spark, 30), _adsb(spark, [0, 60]), 60))
    r = out.collect()[0]
    assert r["along_m"] == pytest.approx(0.0, abs=1.0)
    assert r["cross_m"] == pytest.approx(0.0, abs=1.0)
    assert r["implied_dt_s"] == pytest.approx(0.0, abs=0.02)


def test_a_known_lag_is_recovered(spark):
    # The whole instrument: a report whose position is 1.5 s further along
    # than its timestamp claims must read as +1.5 s, and as +375 m along track.
    out = decompose(bracket(_adsc(spark, 30, lag_seconds=1.5),
                            _adsb(spark, [0, 60]), 60))
    r = out.collect()[0]
    assert r["implied_dt_s"] == pytest.approx(1.5, abs=0.05)
    assert r["along_m"] == pytest.approx(375.0, abs=5.0)
    assert r["cross_m"] == pytest.approx(0.0, abs=2.0)


def test_a_negative_lag_is_recovered_with_its_sign(spark):
    out = decompose(bracket(_adsc(spark, 30, lag_seconds=-12.9),
                            _adsb(spark, [0, 60]), 60))
    assert out.collect()[0]["implied_dt_s"] == pytest.approx(-12.9, abs=0.1)


def test_a_cross_track_error_does_not_read_as_a_lag(spark):
    # The discriminator the conclusion rests on. A position error is isotropic;
    # a timing error is purely along track. If a 400 m northward offset leaked
    # into `implied_dt_s`, the 43:1 ratio would be an artefact of the maths.
    out = decompose(bracket(_adsc(spark, 30, cross_m=400.0),
                            _adsb(spark, [0, 60]), 60))
    r = out.collect()[0]
    assert r["cross_m"] == pytest.approx(400.0, abs=5.0)
    assert r["along_m"] == pytest.approx(0.0, abs=5.0)
    assert r["implied_dt_s"] == pytest.approx(0.0, abs=0.05)


def test_an_unbracketed_report_is_dropped(spark):
    # Only one side: extrapolating is how a measurement invents its own answer.
    assert bracket(_adsc(spark, 30), _adsb(spark, [0, 10]), 60).count() == 0
    assert bracket(_adsc(spark, 30), _adsb(spark, [40, 50]), 60).count() == 0


def test_a_loose_bracket_is_dropped(spark):
    # 300 s either side at 250 m/s is 75 km of interpolation: the instrument
    # would be measuring its own straight line, not the aircraft.
    assert bracket(_adsc(spark, 300), _adsb(spark, [0, 600]), 60).count() == 0


def test_the_nearest_sample_each_side_is_the_one_used(spark):
    out = decompose(bracket(_adsc(spark, 30),
                            _adsb(spark, [0, 25, 35, 60]), 60))
    r = out.collect()[0]
    assert r["bracket_s"] == pytest.approx(10.0, abs=0.01)


def test_another_airframe_cannot_bracket_this_one(spark):
    other = _adsb(spark, [0, 60]).withColumn("icao24", F.lit("3c6568"))
    assert bracket(_adsc(spark, 30), other, 60).count() == 0


def test_a_slow_aircraft_is_excluded(spark):
    # The implied offset divides by speed, so a taxiing aircraft turns a 50 m
    # position disagreement into minutes of apparent skew.
    slow = _adsb(spark, [0, 60]).withColumn("velocity", F.lit(5.0))
    assert decompose(bracket(_adsc(spark, 30), slow, 60)).count() == 0


def test_per_airframe_summarises_each_separately(spark):
    pairs = decompose(bracket(
        _adsc(spark, 30, lag_seconds=1.5), _adsb(spark, [0, 60]), 60))
    summary = per_airframe(pairs).collect()
    assert len(summary) == 1
    assert summary[0]["icao24"] == "4acb41"
    assert summary[0]["n"] == 1
    assert summary[0]["median_implied_dt_s"] == pytest.approx(1.5, abs=0.05)


def test_the_adsb_position_age_is_carried_through(spark):
    # The alternative explanation -- ADS-B carrying a position forward -- has
    # to be checkable from the output rather than argued away, so the age rides
    # along with every pair.
    out = decompose(bracket(_adsc(spark, 30), _adsb(spark, [0, 60]), 60))
    assert "adsb_pos_age_s" in out.columns
    assert out.collect()[0]["adsb_pos_age_s"] == pytest.approx(0.0, abs=0.01)


def test_a_zero_gap_window_is_refused(spark):
    with pytest.raises(ValueError, match="positive"):
        bracket(_adsc(spark, 30), _adsb(spark, [0, 60]), 0)
