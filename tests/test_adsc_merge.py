"""The ADS-C union, and the guarantee that it is invisible when off."""

import datetime as dt

import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    ArrayType, BooleanType, DateType, DoubleType, IntegerType, StringType,
    StructField, StructType, TimestampType,
)

from opdi.config import OPDIConfig
from opdi.pipeline.tracks import TrackProcessor

T0 = dt.datetime(2026, 6, 5, 12, 0, 0)

#: `osn_statevectors_v2` as `process_month` reads it back, plus the
#: `event_time_day` partition column.
#:
#: Stated rather than inferred: `serials` is NULL on every ADS-C row and on
#: plenty of ADS-B ones, and `createDataFrame` cannot infer a type for an
#: all-NULL column -- so an inferred schema fails on the shape under test.
SV_SCHEMA = StructType([
    StructField("event_time", TimestampType()),
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
    StructField("alert", BooleanType()),
    StructField("spi", BooleanType()),
    StructField("squawk", StringType()),
    StructField("last_pos_update", DoubleType()),
    StructField("last_contact", DoubleType()),
    StructField("serials", ArrayType(IntegerType())),
    StructField("event_time_day", DateType()),
])

#: What step 01b writes: the same columns minus the partition date, plus
#: `source`. The merge adds `event_time_day` itself.
ADSC_SCHEMA = StructType(
    [f for f in SV_SCHEMA.fields if f.name != "event_time_day"]
    + [StructField("source", StringType())]
)


@pytest.fixture
def sv(spark):
    """Two ADS-B samples of one airframe, 5 s apart."""
    return spark.createDataFrame([
        Row(event_time=T0, icao24="4acb41", callsign="SAS4414 ",
            lat=52.0, lon=-5.0, baro_altitude=10668.0, geo_altitude=10700.0,
            velocity=240.0, heading=270.0, vert_rate=0.0, on_ground=False,
            alert=False, spi=False, squawk="2000", last_pos_update=1.0,
            last_contact=1.0, serials=None,
            event_time_day=dt.date(2026, 6, 5)),
        Row(event_time=T0 + dt.timedelta(seconds=5), icao24="4acb41",
            callsign="SAS4414 ", lat=52.0, lon=-5.1, baro_altitude=10668.0,
            geo_altitude=10700.0, velocity=240.0, heading=270.0,
            vert_rate=0.0, on_ground=False, alert=False, spi=False,
            squawk="2000", last_pos_update=1.0, last_contact=1.0,
            serials=None, event_time_day=dt.date(2026, 6, 5)),
    ], schema=SV_SCHEMA)


@pytest.fixture
def adsc(spark):
    """One ADS-C row far from any ADS-B sample, and one that shadows."""
    def row(seconds, lon):
        return Row(event_time=T0 + dt.timedelta(seconds=seconds),
                   icao24="4acb41", callsign=None, lat=52.0, lon=lon,
                   baro_altitude=10668.0, geo_altitude=None, velocity=240.0,
                   heading=270.0, vert_rate=0.0, on_ground=False, alert=None,
                   spi=None, squawk="2000", last_pos_update=1.0,
                   last_contact=1.0, serials=None, source="ADS-C")
    return spark.createDataFrame([row(600, -6.0), row(10, -5.05)],
                                 schema=ADSC_SCHEMA)


def _processor(spark, enabled):
    cfg = OPDIConfig.for_environment("local")
    cfg.adsc.enabled = enabled
    return TrackProcessor(spark, cfg)


def test_merge_off_returns_the_frame_untouched(spark, sv):
    out = _processor(spark, False)._merge_adsc(sv, "2026-06-05", "2026-06-06")
    # Identity, not equivalence: a `source` column added "harmlessly" when the
    # flag is off would appear in new partitions of a production table and not
    # in old ones, and `spark.read.parquet` infers its schema from a sample of
    # footers -- so the failure would be an intermittent missing column
    # downstream, not an error here.
    assert out.columns == sv.columns
    assert out.count() == sv.count()


def test_merge_on_adds_source_to_the_adsb_rows(spark, sv, adsc, monkeypatch):
    p = _processor(spark, True)
    monkeypatch.setattr(p, "_read_adsc", lambda *a: adsc)
    monkeypatch.setattr(p.storage, "table_exists", lambda t: True)
    out = p._merge_adsc(sv, "2026-06-05", "2026-06-06")
    sources = {r["source"] for r in out.collect()}
    assert sources == {"ADS-B", "ADS-C"}
    assert out.where("source = 'ADS-B'").count() == 2


def test_merge_on_drops_the_shadowed_row(spark, sv, adsc, monkeypatch):
    p = _processor(spark, True)
    monkeypatch.setattr(p, "_read_adsc", lambda *a: adsc)
    monkeypatch.setattr(p.storage, "table_exists", lambda t: True)
    out = p._merge_adsc(sv, "2026-06-05", "2026-06-06")
    # The +10 s row is inside the 60 s shadow of the T0 ADS-B sample.
    assert out.where("source = 'ADS-C'").count() == 1
    assert out.where("source = 'ADS-C'").collect()[0]["lon"] == pytest.approx(-6.0)


def test_merged_rows_get_the_partition_column(spark, sv, adsc, monkeypatch):
    # `event_time_day` is a partition column of the frame being read back, and
    # `process_month` carries every column of its input through a `select`. A
    # NULL there would write ADS-C rows into a `__HIVE_DEFAULT_PARTITION__`
    # that no day's read would ever find.
    p = _processor(spark, True)
    monkeypatch.setattr(p, "_read_adsc", lambda *a: adsc)
    monkeypatch.setattr(p.storage, "table_exists", lambda t: True)
    out = p._merge_adsc(sv, "2026-06-05", "2026-06-06")
    assert out.where("event_time_day IS NULL").count() == 0


def test_merge_on_with_no_adsc_table_raises(spark, sv, monkeypatch):
    # Silently falling back to ADS-B only would publish the baseline arm under
    # the merged arm's name -- the one failure mode that cannot be detected
    # after the fact, because the two arms' tables are schema-identical.
    p = _processor(spark, True)
    monkeypatch.setattr(p.storage, "table_exists", lambda t: False)
    with pytest.raises(RuntimeError, match="01b"):
        p._merge_adsc(sv, "2026-06-05", "2026-06-06")


def test_segmentation_sees_one_track_across_the_merge(spark, sv, adsc, monkeypatch):
    # The point of merging before `_add_track_id`: an ADS-C sample 600 s after
    # the last ADS-B one is inside the 30-minute gap rule, so it extends the
    # track rather than starting a new one. A NULL callsign must not break it
    # either -- A9 bridges NULLs with `F.last(..., ignorenulls=True)`.
    p = _processor(spark, True)
    monkeypatch.setattr(p, "_read_adsc", lambda *a: adsc)
    monkeypatch.setattr(p.storage, "table_exists", lambda t: True)
    merged = p._merge_adsc(sv, "2026-06-05", "2026-06-06")
    out = p._add_track_id(merged)
    assert out.select("track_id").distinct().count() == 1


def test_the_merge_runs_before_track_id_in_process_month():
    # Read off the source rather than trusted: merging after `_add_track_id`
    # would still produce a table with both sources in it, and every coverage
    # number would look plausible while answering a different question --
    # "which ADS-B tracks can ADS-C be attached to" instead of "what tracks do
    # the two feeds make together".
    import inspect

    from opdi.pipeline.tracks import TrackProcessor

    # Matched on the call expressions, not the bare names: the comment above
    # the merge mentions `_add_track_id` by name, so a substring search on the
    # names alone finds the prose and reports the wrong order.
    src = inspect.getsource(TrackProcessor.process_month)
    assert "self._merge_adsc(" in src
    assert "self._add_track_id(" in src
    assert src.index("self._merge_adsc(") < src.index("self._add_track_id(")
