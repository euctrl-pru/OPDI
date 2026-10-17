"""What the merge did to the trajectories."""

import datetime as dt

import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType, TimestampType,
)

from adsc_coverage_gain import arm_summary, bridged_holes, extension_by_airframe

T0 = dt.datetime(2026, 6, 5, 12, 0, 0)

_FIELDS = [
    StructField("track_id", StringType()),
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("event_time", TimestampType()),
    StructField("segment_id", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
]
SOURCED = StructType(_FIELDS + [StructField("source", StringType())])
UNSOURCED = StructType(_FIELDS)


def _r(track, seconds, source, callsign="SAS4414", segment=None):
    return Row(
        track_id=track, icao24="4acb41", callsign=callsign,
        event_time=T0 + dt.timedelta(seconds=seconds),
        segment_id=segment or f"{track}_1", lat=52.0, lon=-5.0,
        source=source,
    )


@pytest.fixture
def merged(spark):
    # One track: three ADS-B samples, a 600 s hole, one ADS-C sample in it,
    # then two more ADS-B samples.
    return spark.createDataFrame([
        _r("t1", 0, "ADS-B"), _r("t1", 5, "ADS-B"), _r("t1", 10, "ADS-B"),
        _r("t1", 310, "ADS-C", callsign=None, segment="t1_2"),
        _r("t1", 610, "ADS-B", segment="t1_3"),
        _r("t1", 615, "ADS-B", segment="t1_3"),
    ], schema=SOURCED)


@pytest.fixture
def base(spark):
    # The same track without the ADS-C sample, and with no `source` column --
    # which is what the baseline arm actually writes.
    return spark.createDataFrame([
        Row(track_id="t1", icao24="4acb41", callsign="SAS4414",
            event_time=T0 + dt.timedelta(seconds=s),
            segment_id=seg, lat=52.0, lon=-5.0)
        for s, seg in ((0, "t1_1"), (5, "t1_1"), (10, "t1_1"),
                       (610, "t1_2"), (615, "t1_2"))
    ], schema=UNSOURCED)


def test_summary_counts_the_adsc_contribution(spark, merged):
    r = arm_summary(merged).collect()[0]
    assert r["rows"] == 6
    assert r["rows_adsc"] == 1
    assert r["tracks"] == 1
    assert r["tracks_with_adsc"] == 1
    assert r["segments"] == 3


def test_summary_works_on_a_frame_with_no_source_column(spark, base):
    # The baseline arm has no `source` column at all. A summary that raised
    # there would make the comparison impossible to compute.
    r = arm_summary(base).collect()[0]
    assert r["rows"] == 5
    assert r["rows_adsc"] == 0
    assert r["tracks_with_adsc"] == 0


def test_track_hours_spans_first_to_last_sample(spark, merged):
    r = arm_summary(merged).collect()[0]
    assert r["track_hours"] == pytest.approx(615 / 3600.0, abs=1e-6)


def test_two_callsign_tracks_are_counted(spark):
    # The merge-neutrality guard, taken from the A9 study: a track holding two
    # distinct real callsigns is two flights fused into one, which is the way
    # this change could do harm. NULL is not a callsign and must not count.
    df = spark.createDataFrame([
        _r("t1", 0, "ADS-B", callsign="SAS4414"),
        _r("t1", 5, "ADS-C", callsign=None),
        _r("t1", 10, "ADS-B", callsign="SAS9001"),
        _r("t2", 0, "ADS-B", callsign="KLM1234"),
        _r("t2", 5, "ADS-C", callsign=None),
    ], schema=SOURCED)
    assert arm_summary(df).collect()[0]["tracks_with_two_callsigns"] == 1


def test_a_hole_filling_sample_is_found(spark, merged):
    holes = bridged_holes(merged, hole_seconds=300).collect()
    assert len(holes) == 1
    assert holes[0]["gap_before_s"] == pytest.approx(300.0)
    assert holes[0]["gap_after_s"] == pytest.approx(300.0)


def test_an_adsc_sample_beside_adsb_is_not_a_bridge(spark):
    # Inside a dense stretch an ADS-C sample adds a point, not coverage. The
    # shadow filter should have removed it; if one survives, it must not be
    # counted as a closed hole.
    df = spark.createDataFrame([
        _r("t1", 0, "ADS-B"), _r("t1", 70, "ADS-C", callsign=None),
        _r("t1", 140, "ADS-B"),
    ], schema=SOURCED)
    assert bridged_holes(df, hole_seconds=300).count() == 0


def test_a_sample_past_the_last_adsb_report_is_a_bridge(spark):
    # The clearest case of added coverage: the track ends in ADS-B silence and
    # ADS-C extends it. Treating the missing neighbour as "not a hole" would
    # discard exactly this.
    df = spark.createDataFrame([
        _r("t1", 0, "ADS-B"), _r("t1", 5, "ADS-B"),
        _r("t1", 900, "ADS-C", callsign=None),
    ], schema=SOURCED)
    holes = bridged_holes(df, hole_seconds=300).collect()
    assert len(holes) == 1
    assert holes[0]["gap_after_s"] >= 1.0e9


def test_consecutive_adsc_samples_are_measured_against_adsb_not_each_other(spark):
    # Two ADS-C reports ten minutes apart are both inside one hole. Measuring
    # them against each other would report the hole closed twice and 600 s
    # narrower than it is, so both must still count and both must see the
    # ADS-B gap.
    df = spark.createDataFrame([
        _r("t1", 0, "ADS-B"),
        _r("t1", 600, "ADS-C", callsign=None),
        _r("t1", 1200, "ADS-C", callsign=None),
        _r("t1", 1800, "ADS-B"),
    ], schema=SOURCED)
    holes = bridged_holes(df, hole_seconds=300).collect()
    assert len(holes) == 2
    for h in holes:
        assert h["gap_before_s"] >= 600.0 or h["gap_after_s"] >= 600.0


def test_extension_measures_both_ends(spark, base, merged):
    # Arms cannot be joined on `track_id` -- the merge changes it, which is the
    # whole point. `icao24` plus day is the key that survives.
    ext = extension_by_airframe(base, merged).collect()[0]
    assert ext["first_shift_s"] == pytest.approx(0.0)
    assert ext["last_shift_s"] == pytest.approx(0.0)


def test_extension_is_signed_so_a_regression_is_visible(spark, base):
    later = base.selectExpr(
        "track_id", "icao24", "callsign",
        "event_time + INTERVAL 100 SECONDS AS event_time", "segment_id",
        "lat", "lon")
    ext = extension_by_airframe(base, later).collect()[0]
    assert ext["first_shift_s"] == pytest.approx(100.0)
    assert ext["last_shift_s"] == pytest.approx(100.0)
