"""ADS-C survives step 02a; ADS-B behaviour is untouched."""

import datetime as dt

from pyspark.sql import Row
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from opdi.cleaning.native import (
    clean_tracks,
    drop_duplicate_statevectors,
    mask_isolated_points,
    mask_stale_broadcasts,
)
from opdi.config import CleaningConfig

T0 = dt.datetime(2026, 6, 5, 12, 0, 0)

_FIELDS = [
    StructField("event_time", TimestampType()),
    StructField("track_id", StringType()),
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("baro_altitude", DoubleType()),
    StructField("geo_altitude", DoubleType()),
    StructField("velocity", DoubleType()),
    StructField("heading", DoubleType()),
    StructField("vert_rate", DoubleType()),
    StructField("on_ground", BooleanType()),
    StructField("last_contact", DoubleType()),
    StructField("last_pos_update", DoubleType()),
]

#: With a `source` column -- what the merged arm writes.
SOURCED = StructType(_FIELDS + [StructField("source", StringType())])

#: Without one -- production, and every other test in this suite.
UNSOURCED = StructType(_FIELDS)


def _row(seconds, source, lat=52.0, lon=-5.0, alt=10668.0, callsign=None):
    return Row(
        event_time=T0 + dt.timedelta(seconds=seconds),
        track_id="t1",
        icao24="4acb41",
        callsign=callsign,
        lat=lat,
        lon=lon,
        baro_altitude=alt,
        geo_altitude=None,
        velocity=240.0,
        heading=270.0,
        vert_rate=0.0,
        on_ground=False,
        last_contact=1.0,
        last_pos_update=1.0,
        source=source,
    )


def _frame(spark, rows):
    return spark.createDataFrame(rows, schema=SOURCED)


def test_an_isolated_adsc_sample_survives(spark):
    # Three ADS-C samples 600 s apart: every one is >20 s from its nearest
    # neighbour on both sides, so the unexempted rule NULLs all of them.
    df = _frame(
        spark,
        [
            _row(0, "ADS-C", lon=-5.0),
            _row(600, "ADS-C", lon=-6.0),
            _row(1200, "ADS-C", lon=-7.0),
        ],
    )
    out = mask_isolated_points(df, CleaningConfig())
    assert out.where("lat IS NULL").count() == 0
    assert out.where("baro_altitude IS NULL").count() == 0


def test_an_isolated_adsb_sample_is_still_discarded(spark):
    # The exemption must be keyed on the source, not on the gap: this is the
    # Alligier rule that the cleaning study measured, and it stays.
    df = _frame(
        spark,
        [
            _row(0, "ADS-B", lon=-5.0),
            _row(600, "ADS-B", lon=-6.0),
        ],
    )
    out = mask_isolated_points(df, CleaningConfig())
    assert out.where("lat IS NULL").count() == 2


def test_a_repeated_adsc_value_is_not_read_as_stale(spark):
    # The stale rule exists because ADS-B sends position and velocity in
    # separate message types, so an unchanged value was carried forward rather
    # than re-measured. ADS-C has no second message type to carry anything
    # forward from: an unchanged speed 600 s later is a measurement.
    df = _frame(
        spark,
        [
            _row(0, "ADS-C", lon=-5.0),
            _row(600, "ADS-C", lon=-6.0),
        ],
    )
    out = mask_stale_broadcasts(df, CleaningConfig())
    assert out.where("velocity IS NULL").count() == 0


def test_a_repeated_adsb_value_is_still_read_as_stale(spark):
    df = _frame(
        spark,
        [
            _row(0, "ADS-B", lon=-5.0),
            _row(5, "ADS-B", lon=-5.0),
        ],
    )
    out = mask_stale_broadcasts(df, CleaningConfig())
    assert out.where("lat IS NULL").count() == 1


def test_adsb_wins_an_exact_timestamp_collision(spark):
    # Two rows for one instant is what the shadow filter's window is for, but
    # the window is finite and a collision at 0 s is still possible. ADS-B is
    # denser, carries on_ground, and is what every detector was tuned on.
    df = _frame(
        spark,
        [
            _row(0, "ADS-B", lon=-5.0),
            _row(0, "ADS-C", lon=-5.9),
        ],
    )
    out = drop_duplicate_statevectors(df, CleaningConfig())
    assert out.count() == 1
    assert out.collect()[0]["source"] == "ADS-B"


def test_the_exemption_is_configurable_off(spark):
    df = _frame(
        spark,
        [
            _row(0, "ADS-C", lon=-5.0),
            _row(600, "ADS-C", lon=-6.0),
        ],
    )
    cfg = CleaningConfig(sparse_sources=())
    assert mask_isolated_points(df, cfg).where("lat IS NULL").count() == 2


def test_a_frame_without_source_behaves_exactly_as_before(spark):
    # Every existing caller -- production, and every other test in this suite
    # -- passes a frame with no `source` column. The exemption must be inert
    # there: not raise, and not change a mask.
    rows = [
        Row(**{k: v for k, v in _row(s, "ADS-B", lon=lon).asDict().items() if k != "source"})
        for s, lon in ((0, -5.0), (600, -6.0))
    ]
    df = spark.createDataFrame(rows, schema=UNSOURCED)
    out = mask_isolated_points(df, CleaningConfig())
    assert out.where("lat IS NULL").count() == 2


def test_clean_tracks_end_to_end_keeps_the_adsc_rows(spark):
    df = _frame(
        spark,
        [
            _row(0, "ADS-B", lon=-5.00),
            _row(5, "ADS-B", lon=-5.01),
            _row(10, "ADS-B", lon=-5.02),
            _row(610, "ADS-C", lon=-6.00),
            _row(1210, "ADS-C", lon=-7.00),
        ],
    )
    out = clean_tracks(df, CleaningConfig())
    adsc = out.where("source = 'ADS-C'")
    assert adsc.count() == 2
    assert adsc.where("lat IS NULL OR lon IS NULL").count() == 0
    # `segment_id` still splits at the 300 s coverage hole. Note what that
    # does and does not buy: it marks the discontinuity, and no detector reads
    # it (`grep -rn segment_id src/opdi/pipeline/` finds two comments), so it
    # does not today prevent an event being interpolated across the boundary.
    # Asserted because the column is a published contract, not because it
    # protects anything.
    assert out.select("segment_id").distinct().count() == 3


def test_without_the_exemption_the_merge_would_be_a_no_op(spark):
    # The measurement this whole task exists for, stated as a test: with the
    # exemption off, step 02a NULLs the position of every ADS-C sample, so a
    # merged arm would score identically to the baseline for a reason having
    # nothing to do with the hypothesis.
    df = _frame(
        spark,
        [
            _row(0, "ADS-B", lon=-5.00),
            _row(5, "ADS-B", lon=-5.01),
            _row(10, "ADS-B", lon=-5.02),
            _row(610, "ADS-C", lon=-6.00),
            _row(1210, "ADS-C", lon=-7.00),
        ],
    )
    unexempted = clean_tracks(df, CleaningConfig(sparse_sources=()))
    adsc = unexempted.where("source = 'ADS-C'")
    assert adsc.count() == 2
    assert adsc.where("lat IS NULL").count() == 2
