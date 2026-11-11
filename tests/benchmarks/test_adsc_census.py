"""The census aggregation, on a frame small enough to reason about."""

import adsc_census
import pytest
from pyspark.sql import Row

EUROPE_BBOX = (-25.86653, 26.74617, 49.65699, 70.25976)


@pytest.fixture
def feed(spark):
    # Two airframes. 4a… is inside the box at FL350 with 600 s between
    # samples; 3c… is outside it (mid-Atlantic) and appears once, twice over
    # -- an exact duplicate, which is the thing the duplicate counter exists
    # to find.
    return spark.createDataFrame(
        [
            Row(
                time=1780000000.0,
                hex="4acb41",
                lat=52.0,
                lon=-5.0,
                alt_baro_ft=35000,
                track=270.0,
                gs=480.0,
                baro_rate=0,
                flight="SK4414",
                squawk="2000",
                on_ground=False,
            ),
            Row(
                time=1780000600.0,
                hex="4acb41",
                lat=52.1,
                lon=-6.0,
                alt_baro_ft=35000,
                track=270.0,
                gs=None,
                baro_rate=None,
                flight="SK4414",
                squawk=None,
                on_ground=False,
            ),
            Row(
                time=1780001200.0,
                hex="4acb41",
                lat=52.2,
                lon=-7.0,
                alt_baro_ft=9000,
                track=270.0,
                gs=None,
                baro_rate=-1200,
                flight="SK4414",
                squawk=None,
                on_ground=False,
            ),
            Row(
                time=1780000000.0,
                hex="3c6568",
                lat=45.0,
                lon=-40.0,
                alt_baro_ft=38000,
                track=90.0,
                gs=500.0,
                baro_rate=0,
                flight="LH133",
                squawk="0014",
                on_ground=False,
            ),
            Row(
                time=1780000000.0,
                hex="3c6568",
                lat=45.0,
                lon=-40.0,
                alt_baro_ft=38000,
                track=90.0,
                gs=500.0,
                baro_rate=0,
                flight="LH133",
                squawk="0014",
                on_ground=False,
            ),
        ]
    )


def test_counts_rows_inside_and_outside_the_box(spark, feed):
    r = adsc_census.census(feed, EUROPE_BBOX).collect()[0]
    assert r["rows"] == 5
    assert r["rows_in_box"] == 3
    assert r["airframes"] == 2
    assert r["airframes_in_box"] == 1


def test_median_cadence_is_per_airframe_and_ignores_duplicates(spark, feed):
    # 4acb41 has two 600 s steps, so its own median is 600. 3c6568's only step
    # is the exact duplicate, a 0 s gap, which is a redelivery and not a
    # cadence -- so that airframe contributes nothing and the headline is 600.
    #
    # The median is taken over *airframe* medians, so one busy airframe cannot
    # set the number for a day with 1,855 of them. And `percentile_approx` is
    # nearest-rank rather than interpolating, which is exactly why the zero
    # gaps have to be excluded rather than averaged away: over {0, 600} it
    # returns 0, and the day would read as a 1 Hz feed.
    r = adsc_census.census(feed, EUROPE_BBOX).collect()[0]
    assert r["median_cadence_s"] == pytest.approx(600.0, abs=0.1)


def test_altitude_and_geography_bands_are_counted_inside_the_box(spark, feed):
    # In the box, not over the whole feed: the box is the population OPDI would
    # ingest, so it is the one a claim about what ADS-C can reach has to be
    # made over. 3c6568's two FL380 rows sit at 40W and must not be counted.
    r = adsc_census.census(feed, EUROPE_BBOX).collect()[0]
    assert r["in_box_above_fl300"] == 2  # the two FL350 rows, not the FL380 pair
    assert r["in_box_below_10000ft"] == 1
    assert r["in_box_west_of_10w"] == 0  # -5/-6/-7 are all east of 10W
    assert r["in_box_on_ground"] == 0


def test_exact_duplicates_are_counted(spark, feed):
    r = adsc_census.census(feed, EUROPE_BBOX).collect()[0]
    assert r["exact_duplicate_rows"] == 1


def test_fill_rate_is_reported_per_column(spark, feed):
    r = adsc_census.census(feed, EUROPE_BBOX).collect()[0]
    assert r["fill_gs"] == pytest.approx(60.0, abs=0.1)
    assert r["fill_squawk"] == pytest.approx(60.0, abs=0.1)
    assert r["fill_alt_baro_ft"] == pytest.approx(100.0, abs=0.1)


def test_a_column_absent_from_the_feed_is_not_silently_omitted(spark, feed):
    # The census exists to answer "is this field ever populated". A column that
    # vanished from the feed must read as 0.0, not disappear from the report --
    # an absent row and a zero mean different things and only one of them is a
    # measurement.
    r = adsc_census.census(feed, EUROPE_BBOX).collect()[0]
    for absent in ("ias", "tas", "oat", "nav_altitude_mcp", "registration"):
        assert r[f"fill_{absent}"] == pytest.approx(0.0)
