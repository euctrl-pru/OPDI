"""Tests for building and publishing a day of OPDI data."""

import datetime as dt
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from opdi.output.published import (
    PublishedLayout,
    build_flight_events,
    build_flight_list,
    publish_day,
)
from opdi.output.schema import (
    CATALOGUE,
    FLIGHT_EVENTS,
    FLIGHT_INDEX,
    FLIGHT_LIST,
    MEASUREMENT_COLUMNS,
    RELEASE,
)

DAY = dt.date(2026, 6, 14)
NEXT = dt.date(2026, 6, 15)
EMPTY_DAY = dt.date(2026, 6, 20)


def _ts(day, hh, mm):
    return dt.datetime(day.year, day.month, day.day, hh, mm)


def _flight(fid, dof, first_seen, adep, version="v5.0.0"):
    """One working flight-list row, keyed by the working column names."""
    row = {c.source: None for c in FLIGHT_LIST if c.name != "release"}
    row.update(
        ID=fid, ICAO24="abc123", FLT_ID="TST" + fid[-1], DOF=dof, ADEP=adep, ADES="EHAM",
        FIRST_SEEN=first_seen, LAST_SEEN=first_seen + dt.timedelta(hours=2),
        ADEP_SOURCE="aerodrome", ADES_SOURCE="aerodrome", VERSION=version,
        TRACK_DURATION_MIN=120.0, SUPERSEDED_DEP=False, SUPERSEDED_ARR=False,
        MOVEMENT_DEP=True, MOVEMENT_ARR=True,
        ATOT=first_seen + dt.timedelta(minutes=10),
    )
    return row


def _working_tables(spark):
    flight_cols = [c for c in FLIGHT_LIST if c.name != "release"]
    ddl = ", ".join(f"`{c.source}` {c.dtype}" for c in flight_cols)
    flights = [
        _flight("trk_1", DAY, _ts(DAY, 22, 0), "LFPG"),
        _flight("trk_2", DAY, _ts(DAY, 8, 0), "EDDF"),
        _flight("trk_3", NEXT, _ts(NEXT, 9, 0), "EGLL"),
    ]
    flight_list = spark.createDataFrame(
        [tuple(r[c.source] for c in flight_cols) for r in flights], ddl
    )
    events = spark.createDataFrame(
        [
            ("e1", "trk_1", "ATOT", _ts(DAY, 22, 10), 2.5, 49.0, 0.0, DAY),
            ("e2", "trk_1", "holding-end", _ts(DAY, 23, 30), 4.0, 51.0, 9000.0, DAY),
            # After midnight, but the flight started on DAY.
            ("e3", "trk_1", "ALDT", _ts(NEXT, 0, 40), 4.7, 52.3, 0.0, DAY),
            ("e4", "trk_2", "ATOT", _ts(DAY, 8, 10), 8.5, 50.0, 0.0, DAY),
            ("e5", "trk_2", "level-start", _ts(DAY, 8, 40), 8.0, 51.0, 35000.0, DAY),
            ("e6", "trk_3", "ATOT", _ts(NEXT, 9, 10), -0.4, 51.5, 0.0, NEXT),
        ],
        "id string, flight_id string, type string, event_time timestamp, "
        "longitude double, latitude double, altitude double, dof date",
    )
    events = (
        events.selectExpr(
            "id", "flight_id", "type", "event_time", "longitude", "latitude", "altitude",
            "'OSN' as source", "'events_v0.3.0' as version", "'{}' as info", "dof",
        )
    )
    measurements = spark.createDataFrame(
        [
            ("e1_d", "e1", "Distance flown (NM)", 0.0, "distance_v0.0.3", DAY),
            ("e1_t", "e1", "Time Passed (s)", 600.0, "time_v0.0.1", DAY),
            ("e2_d", "e2", "Distance flown (NM)", 250.0, "distance_v0.0.3", DAY),
            ("e2_t", "e2", "Time Passed (s)", 5400.0, "time_v0.0.1", DAY),
            ("e2_m", "e2", "Holding duration (s)", 720.0, "holding_v0.1.0", DAY),
            ("e3_d", "e3", "Distance flown (NM)", 300.0, "distance_v0.0.3", DAY),
            ("e4_d", "e4", "Distance flown (NM)", 0.0, "distance_v0.0.3", DAY),
            ("e6_d", "e6", "Distance flown (NM)", 0.0, "distance_v0.0.3", NEXT),
        ],
        "id string, milestone_id string, type string, value double, version string, "
        "dof date",
    )
    return {
        "opdi_flight_list": flight_list,
        "opdi_flight_events": events,
        "opdi_measurements": measurements,
    }


class _Storage:
    """Stands in for the pipeline's storage: serves the working tables."""

    def __init__(self, tables):
        self.tables = tables

    def read_table(self, name):
        return self.tables[name]

    def table_exists(self, name):
        return name in self.tables


@pytest.fixture(scope="module")
def tables(spark):
    return _working_tables(spark)


def _types(df):
    return [(f.name, f.dataType.simpleString()) for f in df.schema.fields]


def _declared(columns):
    return [(c.name, c.dtype) for c in columns]


# --- building ---------------------------------------------------------------

def test_flight_list_has_the_declared_columns_in_order(tables):
    fl = build_flight_list(tables["opdi_flight_list"])
    assert _types(fl) == _declared(FLIGHT_LIST)
    assert {r["release"] for r in fl.collect()} == {RELEASE}


def test_flight_list_refuses_a_working_table_without_the_milestones(tables):
    with pytest.raises(ValueError, match="ATOT"):
        build_flight_list(tables["opdi_flight_list"].drop("ATOT"))


def _day_events(tables, day=DAY):
    return build_flight_events(
        tables["opdi_flight_events"].filter(f"dof = '{day}'"),
        tables["opdi_measurements"].filter(f"dof = '{day}'"),
        tables["opdi_flight_list"],
    )


def test_flight_events_has_the_declared_columns_in_order(tables):
    assert _types(_day_events(tables)) == _declared(FLIGHT_EVENTS)


def test_post_midnight_event_is_in_the_start_day(tables):
    rows = {r["event_id"]: r for r in _day_events(tables).collect()}
    assert set(rows) == {"e1", "e2", "e3", "e4", "e5"}
    assert rows["e3"]["event_time"].date() == NEXT
    assert rows["e3"]["dof"] == DAY
    assert rows["e3"]["adep"] == "LFPG"


def test_missing_measurement_is_null_never_zero(tables):
    rows = {r["event_id"]: r for r in _day_events(tables).collect()}
    assert rows["e1"]["holding_duration_s"] is None
    assert rows["e2"]["holding_duration_s"] == 720.0
    # A real zero stays a zero.
    assert rows["e1"]["distance_flown_nm"] == 0.0
    # No measurement row at all: every measurement column is NULL.
    for c in MEASUREMENT_COLUMNS:
        assert rows["e5"][c.name] is None
    assert rows["e3"]["time_passed_s"] is None


def test_flight_columns_are_repeated_on_every_event(tables):
    rows = {r["event_id"]: r for r in _day_events(tables).collect()}
    assert rows["e4"]["flt_id"] == "TST2"
    assert rows["e4"]["flight_version"] == "v5.0.0"
    assert rows["e4"]["version"] == "events_v0.3.0"
    assert rows["e4"]["release"] == RELEASE
    assert rows["e4"]["h3_res_5"] is not None


# --- publishing -------------------------------------------------------------

@pytest.fixture()
def layout(tmp_path):
    return PublishedLayout(str(tmp_path / "published"))


def test_layout_paths(layout):
    assert layout.flight_list(DAY).endswith("/flight_list/flight_list_20260614.parquet")
    assert layout.flight_events(DAY).endswith(
        "/flight_events/flight_events_20260614.parquet"
    )
    assert layout.flight_index(DAY).endswith("/index/flight_index_202606.parquet")
    assert layout.catalogue.endswith("/catalogue.parquet")


def _parquet_files(root):
    return sorted(p.relative_to(root).as_posix() for p in Path(root).rglob("*.parquet"))


def test_publish_day_writes_one_file_per_table_and_no_temporary_files(
    spark, tables, layout
):
    counts = publish_day(spark, _Storage(tables), DAY, layout, commit="abc")
    assert counts["flights"] == 2
    assert counts["events"] == 5
    assert _parquet_files(layout.root) == [
        "catalogue.parquet",
        "flight_events/flight_events_20260614.parquet",
        "flight_list/flight_list_20260614.parquet",
        "index/flight_index_202606.parquet",
    ]
    assert not [p for p in Path(layout.root).rglob("*.tmp*")]
    for path in (layout.flight_list(DAY), layout.flight_events(DAY)):
        assert Path(path).is_file()

    fl = pq.read_table(layout.flight_list(DAY))
    assert fl.column_names == [c.name for c in FLIGHT_LIST]
    ev = pq.read_table(layout.flight_events(DAY))
    assert ev.column_names == [c.name for c in FLIGHT_EVENTS]
    assert sorted(ev.column("event_id").to_pylist()) == ["e1", "e2", "e3", "e4", "e5"]


def test_republishing_replaces_the_day_exactly_once(spark, tables, layout):
    storage = _Storage(tables)
    publish_day(spark, storage, DAY, layout, commit="first")
    publish_day(spark, storage, NEXT, layout, commit="first")
    publish_day(spark, storage, DAY, layout, commit="second")

    cat = spark.read.parquet(layout.catalogue)
    assert [c.name for c in CATALOGUE] == cat.columns
    rows = {r["day"]: r for r in cat.collect()}
    assert sorted(rows) == [DAY, NEXT]
    assert rows[DAY]["commit"] == "second"
    assert rows[DAY]["flights"] == 2 and rows[DAY]["events"] == 5
    assert rows[DAY]["release"] == RELEASE
    assert rows[DAY]["flight_list_version"] == "v5.0.0"
    assert rows[DAY]["event_versions"] == ["events_v0.3.0"]
    assert rows[DAY]["last_event"] == _ts(NEXT, 0, 40)
    assert rows[DAY]["flight_events_bytes"] == Path(layout.flight_events(DAY)).stat().st_size

    index = spark.read.parquet(layout.flight_index(DAY))
    assert index.columns == [c.name for c in FLIGHT_INDEX]
    ids = [r["flight_id"] for r in index.collect()]
    assert sorted(ids) == ["trk_1", "trk_2", "trk_3"]
    files = {r["flight_id"]: r["file"] for r in index.collect()}
    assert files["trk_3"] == "flight_list/flight_list_20260615.parquet"


def test_a_day_without_flights_still_writes_both_files(spark, tables, layout):
    counts = publish_day(spark, _Storage(tables), EMPTY_DAY, layout, commit="abc")
    assert counts["flights"] == 0 and counts["events"] == 0
    fl = pq.read_table(layout.flight_list(EMPTY_DAY))
    ev = pq.read_table(layout.flight_events(EMPTY_DAY))
    assert fl.num_rows == 0 and fl.column_names == [c.name for c in FLIGHT_LIST]
    assert ev.num_rows == 0 and ev.column_names == [c.name for c in FLIGHT_EVENTS]
    row = spark.read.parquet(layout.catalogue).collect()[0]
    assert row["day"] == EMPTY_DAY
    assert row["flights"] == 0 and row["events"] == 0
    assert row["first_event"] is None and row["last_event"] is None


def test_events_file_is_sorted_with_row_groups_and_bloom_filters(spark, tables, layout):
    publish_day(spark, _Storage(tables), DAY, layout, commit="abc", row_group_rows=2)
    meta = pq.ParquetFile(layout.flight_events(DAY)).metadata
    assert meta.num_row_groups == 3
    names = [meta.schema.column(i).name for i in range(meta.num_columns)]
    fid = names.index("flight_id")
    bounds = []
    for g in range(meta.num_row_groups):
        stats = meta.row_group(g).column(fid).statistics
        bounds.append((stats.min, stats.max))
    flat = [v for pair in bounds for v in pair]
    assert flat == sorted(flat)
    for col in ("flight_id", "icao24", "flt_id", "adep", "ades", "type"):
        chunk = meta.row_group(0).column(names.index(col))
        assert chunk.bloom_filter_offset is not None, col
    assert meta.row_group(0).column(fid).compression == "ZSTD"

    fl_meta = pq.ParquetFile(layout.flight_list(DAY)).metadata
    fl_names = [fl_meta.schema.column(i).name for i in range(fl_meta.num_columns)]
    for col in ("flight_id", "icao24", "flt_id", "adep", "ades"):
        assert fl_meta.row_group(0).column(fl_names.index(col)).bloom_filter_offset is not None
    adeps = pq.read_table(layout.flight_list(DAY)).column("adep").to_pylist()
    assert adeps == sorted(adeps)
