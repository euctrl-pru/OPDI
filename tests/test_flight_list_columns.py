"""Every writer of ``opdi_flight_list`` writes the same columns.

Step 03 writes the flight list in two passes into the same day partition:
first the flights with a detected departure or arrival, then the
overflights. Both passes must write the same columns in the same order. If
one pass leaves out a column, a plain-parquet reader that takes the schema
from that pass's file loses the column for the whole day.

The reader is also made tolerant of day partitions written before both
passes carried ``ADEP_SOURCE`` and ``ADES_SOURCE``: the columns then read as
NULL instead of disappearing.
"""

from datetime import date, datetime

from opdi.config import OPDIConfig
from opdi.utils.storage import StorageManager


class _FakeStorage:
    def __init__(self, tables):
        self._tables = tables

    def read_table(self, name):
        return self._tables[name]


def _processor(tables):
    from opdi.pipeline.flights import FLIGHT_LIST_VERSION, FlightListProcessor

    class Stub:
        _get_data_within_timeframe = FlightListProcessor._get_data_within_timeframe
        _fetch_overflights = FlightListProcessor._fetch_overflights
        _add_osn_aircraft_db_data = FlightListProcessor._add_osn_aircraft_db_data

        def __init__(self):
            self.tracks_table = "osn_tracks"
            self.storage = _FakeStorage(tables)

        def _version_for(self, path):
            return FLIGHT_LIST_VERSION

    return Stub()


def _tables(spark):
    sv = spark.createDataFrame(
        [("t1", "abc123", "RYR456", datetime(2025, 6, 1, 12, m)) for m in (0, 4, 8)],
        "track_id string, icao24 string, callsign string, event_time timestamp",
    )
    fl = spark.createDataFrame([], "id string, first_seen timestamp")
    adb = spark.createDataFrame(
        [("abc123", "EI-ABC", "B738", "B738", "L2J", "RYR")],
        "icao24 string, registration string, model string, typecode string, "
        "icao_aircraft_class string, icao_operator string",
    )
    return {"osn_tracks": sv, "opdi_flight_list": fl, "osn_aircraft_db": adb}


def _detected_flight(spark):
    """One row in the shape the departure-and-arrival pass produces."""
    return spark.createDataFrame(
        [
            (
                "t2",
                "EIDW",
                "EGLL",
                "",
                "",
                "aerodrome",
                "aerodrome",
                "abc123",
                "RYR1",
                datetime(2025, 6, 1, 8),
                datetime(2025, 6, 1, 9),
                date(2025, 6, 1),
                "v",
            )
        ],
        "id string, ADEP string, ADES string, ADEP_P string, ADES_P string, "
        "adep_source string, ades_source string, ICAO24 string, FLT_ID string, "
        "first_seen timestamp, last_seen timestamp, DOF date, version string",
    )


def test_an_overflight_states_how_its_airports_were_decided(spark):
    proc = _processor(_tables(spark))
    rows = proc._fetch_overflights(date(2025, 6, 1)).collect()
    assert [(r["adep_source"], r["ades_source"]) for r in rows] == [
        ("undetermined", "undetermined")
    ]


def test_both_passes_write_the_same_columns_in_the_same_order(spark):
    proc = _processor(_tables(spark))
    overflights = proc._add_osn_aircraft_db_data(proc._fetch_overflights(date(2025, 6, 1)))
    detected = proc._add_osn_aircraft_db_data(_detected_flight(spark))
    assert overflights.columns == detected.columns
    assert "ADEP_SOURCE" in overflights.columns and "ADES_SOURCE" in overflights.columns
    assert [f.dataType for f in overflights.schema] == [f.dataType for f in detected.schema]


def test_a_frame_without_source_columns_gets_them_as_null(spark):
    proc = _processor(_tables(spark))
    detected = _detected_flight(spark).drop("adep_source", "ades_source")
    out = proc._add_osn_aircraft_db_data(detected)
    row = out.collect()[0]
    assert row["ADEP_SOURCE"] is None and row["ADES_SOURCE"] is None


def test_an_older_partition_without_the_source_columns_reads_them_as_null(spark, tmp_path):
    cfg = OPDIConfig()
    cfg.spark.enable_iceberg = False
    cfg.project.warehouse_path = str(tmp_path)
    storage = StorageManager(spark, cfg)

    new = spark.createDataFrame(
        [("a", "EIDW", "aerodrome", "2025-06-02")],
        "ID string, ADEP string, " "ADEP_SOURCE string, DOF string",
    )
    old = spark.createDataFrame([("b", "EGLL", "2025-06-01")], "ID string, ADEP string, DOF string")
    # The older day is written last, so its file is the one a plain reader
    # would most likely take the schema from.
    storage.write_table(new, "opdi_flight_list", mode="append", partition_by=["DOF"])
    storage.write_table(old, "opdi_flight_list", mode="append", partition_by=["DOF"])

    for frame in (storage.read_table("opdi_flight_list"),):
        rows = {r["ID"]: r["ADEP_SOURCE"] for r in frame.collect()}
        assert rows == {"a": "aerodrome", "b": None}
    view = storage.table_ref("opdi_flight_list")
    assert "ADEP_SOURCE" in spark.table(view).columns
