"""The raw ADS-C topic, mapped into OPDI's state-vector schema."""

import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from opdi.ingestion.osn_adsc import ADSC_SOURCE, to_opdi_schema

#: Every column `osn_statevectors_v2` carries, plus `source`.
EXPECTED = {
    "event_time", "icao24", "lat", "lon", "velocity", "heading", "vert_rate",
    "callsign", "on_ground", "alert", "spi", "squawk", "baro_altitude",
    "geo_altitude", "last_pos_update", "last_contact", "serials", "source",
}

#: The subset of `org.opensky.avro.readsb.Aircraft` this mapping reads, with
#: the Avro types the real parquet footer declares.
#:
#: Stated rather than inferred. Every one of these columns is NULL on some day
#: of the archive -- `alt_geom` on all eleven census days -- and
#: `createDataFrame` cannot infer a type for an all-NULL column, so an inferred
#: schema would make the test fail on exactly the data shape it exists to
#: cover. The real feed is typed by its footer and never has this problem.
RAW_SCHEMA = StructType([
    StructField("time", DoubleType()),
    StructField("hex", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("alt_baro_ft", IntegerType()),
    StructField("alt_geom", IntegerType()),
    StructField("track", DoubleType()),
    StructField("gs", DoubleType()),
    StructField("baro_rate", IntegerType()),
    StructField("flight", StringType()),
    StructField("squawk", StringType()),
    StructField("on_ground", BooleanType()),
    StructField("seen", DoubleType()),
    StructField("seen_pos", DoubleType()),
    StructField("alert", IntegerType()),
    StructField("spi", IntegerType()),
])


@pytest.fixture
def raw(spark):
    return spark.createDataFrame([
        Row(time=1780000000.0, hex="4ACB41", lat=52.0, lon=-5.0,
            alt_baro_ft=35000, alt_geom=None, track=270.0, gs=480.0,
            baro_rate=-1200, flight="SK4414", squawk="2000",
            on_ground=False, seen=0.0, seen_pos=0.0, alert=0, spi=0),
    ], schema=RAW_SCHEMA)


def test_column_set_matches_the_statevector_schema(spark, raw):
    # A column the ADS-B frame does not have would survive `unionByName` and
    # land in `osn_tracks` unannounced; one it does have and this lacks would
    # become NULL for every ADS-C row without anything saying so.
    assert set(to_opdi_schema(raw).columns) == EXPECTED


def test_altitude_is_converted_to_metres(spark, raw):
    r = to_opdi_schema(raw).collect()[0]
    assert r["baro_altitude"] == pytest.approx(35000 / 3.28084, abs=0.01)


def test_ground_speed_is_converted_to_metres_per_second(spark, raw):
    r = to_opdi_schema(raw).collect()[0]
    assert r["velocity"] == pytest.approx(480.0 / 1.94384, abs=0.01)


def test_vertical_rate_is_converted_to_metres_per_second(spark, raw):
    r = to_opdi_schema(raw).collect()[0]
    assert r["vert_rate"] == pytest.approx(-1200 / 196.850394, abs=0.001)


def test_event_time_stays_a_double(spark, raw):
    # The bbox and thinning filters compare against unix seconds and must run
    # before the timestamp cast; a frame arriving as TIMESTAMP would make the
    # modulo comparison a type error at best and a silent no-op at worst.
    assert dict(to_opdi_schema(raw).dtypes)["event_time"] == "double"


def test_icao24_is_lowercased(spark, raw):
    # ADS-B `icao24` is lowercase hex and it is the join key to everything --
    # the flight list, APDF's AIRCRAFT_ADDRESS, the aircraft database. A
    # mixed-case key joins to nothing and raises no error.
    assert to_opdi_schema(raw).collect()[0]["icao24"] == "4acb41"


def test_callsign_is_dropped(spark, raw):
    # `flight` is the IATA number (SK4414), not the ICAO callsign ADS-B
    # broadcasts (SAS4414). Carried through, it would read as a callsign change
    # on every ADS-C sample and the A9 debounce would split exactly the tracks
    # this fusion exists to join.
    assert to_opdi_schema(raw).collect()[0]["callsign"] is None


def test_unavailable_fields_are_null_not_zero(spark, raw):
    r = to_opdi_schema(raw).collect()[0]
    # `alert`/`spi` arrive as integer 0 for every row: a readsb default, not an
    # observation of an ATC indicator. `serials` has no meaning for a
    # satellite link. `alt_geom` measured 0.0% fill on all eleven census days.
    assert r["alert"] is None and r["spi"] is None
    assert r["serials"] is None
    assert r["geo_altitude"] is None


def test_position_and_contact_ages_are_absolute_instants(spark, raw):
    # The ADS-B columns hold unix timestamps, not ages; `seen`/`seen_pos` hold
    # ages. Subtracting is what makes the two comparable.
    #
    # Both are constant 0 in the feed, so this amounts to the feed *asserting*
    # that every report is current. The time-sync measurement shows that
    # assertion is false by a median 1.48 s -- see benchmarks/adsc_time_sync.py.
    r = to_opdi_schema(raw).collect()[0]
    assert r["last_pos_update"] == pytest.approx(1780000000.0)
    assert r["last_contact"] == pytest.approx(1780000000.0)


def test_source_is_stamped(spark, raw):
    assert to_opdi_schema(raw).collect()[0]["source"] == ADSC_SOURCE


def test_a_historical_row_without_the_newer_columns_still_maps(spark):
    # The feed's schema grew: `track`, `gs` and `baro_rate` are empty before
    # roughly 2025-09 and `flight`, `squawk`, `on_ground` before roughly
    # 2026-06. A 2024 day must map to NULL velocity rather than fail.
    old = spark.createDataFrame([
        Row(time=1709300000.0, hex="4acb41", lat=52.0, lon=-5.0,
            alt_baro_ft=38000, alt_geom=None, track=None, gs=None,
            baro_rate=None, flight=None, squawk=None, on_ground=None,
            seen=0.0, seen_pos=0.0, alert=None, spi=None),
    ], schema=RAW_SCHEMA)
    r = to_opdi_schema(old).collect()[0]
    assert r["velocity"] is None and r["vert_rate"] is None
    assert r["heading"] is None
    assert r["baro_altitude"] == pytest.approx(38000 / 3.28084, abs=0.01)
    # A NULL `on_ground` is not "airborne unknown" downstream -- the detectors
    # read it as a boolean - so it is coalesced rather than passed through.
    assert r["on_ground"] is False
