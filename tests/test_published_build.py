"""Tests for building and publishing a day of OPDI data."""

import datetime as dt
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from opdi.output.published import (
    FLIGHT_EVENTS_BLOOM,
    PublishedLayout,
    build_flight_events,
    build_flight_list,
    publish_day,
    write_single_file,
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
        ID=fid,
        ICAO24="abc123",
        FLT_ID="TST" + fid[-1],
        DOF=dof,
        ADEP=adep,
        ADES="EHAM",
        FIRST_SEEN=first_seen,
        LAST_SEEN=first_seen + dt.timedelta(hours=2),
        ADEP_SOURCE="aerodrome",
        ADES_SOURCE="aerodrome",
        VERSION=version,
        TRACK_DURATION_MIN=120.0,
        SUPERSEDED_DEP=False,
        SUPERSEDED_ARR=False,
        MOVEMENT_DEP=True,
        MOVEMENT_ARR=True,
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
    events = events.selectExpr(
        "id",
        "flight_id",
        "type",
        "event_time",
        "longitude",
        "latitude",
        "altitude",
        "'OSN' as source",
        "'events_v0.3.0' as version",
        "'{}' as info",
        "dof",
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
        "id string, milestone_id string, type string, value double, version string, " "dof date",
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
    assert layout.flight_events(DAY).endswith("/flight_events/flight_events_20260614.parquet")
    assert layout.flight_index(DAY).endswith("/index/flight_index_202606.parquet")
    assert layout.catalogue.endswith("/catalogue.parquet")


def _parquet_files(root):
    return sorted(p.relative_to(root).as_posix() for p in Path(root).rglob("*.parquet"))


def test_publish_day_writes_one_file_per_table_and_no_temporary_files(spark, tables, layout):
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
    for col in ("flight_id", "icao24", "flt_id"):
        chunk = meta.row_group(0).column(names.index(col))
        assert chunk.bloom_filter_offset is not None, col
    for col in ("adep", "ades", "type"):
        chunk = meta.row_group(0).column(names.index(col))
        assert chunk.bloom_filter_offset is None, col
    assert meta.row_group(0).column(fid).compression == "ZSTD"

    fl_meta = pq.ParquetFile(layout.flight_list(DAY)).metadata
    fl_names = [fl_meta.schema.column(i).name for i in range(fl_meta.num_columns)]
    for col in ("flight_id", "icao24", "flt_id"):
        assert fl_meta.row_group(0).column(fl_names.index(col)).bloom_filter_offset is not None
    for col in ("adep", "ades"):
        assert fl_meta.row_group(0).column(fl_names.index(col)).bloom_filter_offset is None
    adeps = pq.read_table(layout.flight_list(DAY)).column("adep").to_pylist()
    assert adeps == sorted(adeps)


# --- which day's file an event goes to --------------------------------------


def _boundary_tables(spark):
    """Working tables where the date stored on some events differs from the
    date of their flight.

    * trk_a starts on DAY at 23:50; the working events table stored its events
      under NEXT. They belong to DAY's file.
    * trk_b starts on NEXT; one of its events was stored under DAY. It belongs
      to NEXT's file.
    * orphan_1 and orphan_2 have no flight-list row at all. They stay in the
      file of the date stored on them.
    """
    flight_cols = [c for c in FLIGHT_LIST if c.name != "release"]
    ddl = ", ".join(f"`{c.source}` {c.dtype}" for c in flight_cols)
    flights = [
        _flight("trk_a", DAY, _ts(DAY, 23, 50), "LFPG"),
        _flight("trk_b", NEXT, _ts(NEXT, 0, 5), "EDDF"),
    ]
    flight_list = spark.createDataFrame(
        [tuple(r[c.source] for c in flight_cols) for r in flights], ddl
    )
    events = spark.createDataFrame(
        [
            ("a1", "trk_a", "ATOT", _ts(NEXT, 0, 2), 2.5, 49.0, 0.0, NEXT),
            ("a2", "trk_a", "ALDT", _ts(NEXT, 1, 30), 4.7, 52.3, 0.0, NEXT),
            ("b1", "trk_b", "first_seen", _ts(NEXT, 0, 5), 8.5, 50.0, 0.0, DAY),
            ("b2", "trk_b", "ATOT", _ts(NEXT, 0, 15), 8.5, 50.0, 0.0, NEXT),
            ("o1", "orphan_1", "level-start", _ts(DAY, 12, 0), 5.0, 50.0, 30000.0, DAY),
            ("o2", "orphan_2", "level-start", _ts(NEXT, 12, 0), 5.0, 50.0, 30000.0, NEXT),
        ],
        "id string, flight_id string, type string, event_time timestamp, "
        "longitude double, latitude double, altitude double, dof date",
    ).selectExpr(
        "id",
        "flight_id",
        "type",
        "event_time",
        "longitude",
        "latitude",
        "altitude",
        "'OSN' as source",
        "'events_v0.3.0' as version",
        "'{}' as info",
        "dof",
    )
    measurements = spark.createDataFrame(
        [
            ("a1_d", "a1", "Distance flown (NM)", 3.0, "distance_v0.0.3", NEXT),
            ("b1_d", "b1", "Distance flown (NM)", 0.0, "distance_v0.0.3", DAY),
            ("o1_d", "o1", "Distance flown (NM)", 99.0, "distance_v0.0.3", DAY),
        ],
        "id string, milestone_id string, type string, value double, version string, " "dof date",
    )
    return {
        "opdi_flight_list": flight_list,
        "opdi_flight_events": events,
        "opdi_measurements": measurements,
    }


def test_events_follow_their_flight_into_its_day(spark, tmp_path):
    layout = PublishedLayout(str(tmp_path / "published"))
    storage = _Storage(_boundary_tables(spark))
    publish_day(spark, storage, DAY, layout, commit="abc")
    publish_day(spark, storage, NEXT, layout, commit="abc")

    day = {r["event_id"]: r for r in pq.read_table(layout.flight_events(DAY)).to_pylist()}
    nxt = {r["event_id"]: r for r in pq.read_table(layout.flight_events(NEXT)).to_pylist()}
    assert sorted(day) == ["a1", "a2", "o1"]
    assert sorted(nxt) == ["b1", "b2", "o2"]

    # The flight's own columns, and its date, are on the events stored under
    # the other day.
    assert day["a1"]["dof"] == DAY and day["a1"]["adep"] == "LFPG"
    assert day["a1"]["distance_flown_nm"] == 3.0
    assert nxt["b1"]["dof"] == NEXT and nxt["b1"]["adep"] == "EDDF"
    assert nxt["b1"]["distance_flown_nm"] == 0.0

    # An event without a flight-list row keeps its own date and has NULL
    # flight columns.
    assert day["o1"]["dof"] == DAY and day["o1"]["adep"] is None
    assert day["o1"]["distance_flown_nm"] == 99.0
    assert nxt["o2"]["dof"] == NEXT and nxt["o2"]["flight_version"] is None

    # A flight and its events are in the same day's files.
    for d in (DAY, NEXT):
        flights = set(pq.read_table(layout.flight_list(d)).column("flight_id").to_pylist())
        events = pq.read_table(layout.flight_events(d)).to_pylist()
        assert {e["flight_id"] for e in events if e["adep"] is not None} <= flights


def test_identifiers_get_bloom_filters_and_other_columns_keep_a_dictionary(spark, tmp_path):
    rows = [
        (f"trk_{i:04d}", f"{i:06x}", f"TST{i}", "EHAM" if i % 2 else "LFPG", "ATOT")
        for i in range(2000)
    ]
    df = spark.createDataFrame(
        rows, "flight_id string, icao24 string, flt_id string, " "adep string, type string"
    )
    path = str(tmp_path / "x.parquet")
    write_single_file(df, path, sort_by=("flight_id",), bloom_filters=FLIGHT_EVENTS_BLOOM)
    meta = pq.ParquetFile(path).metadata
    names = [meta.schema.column(i).name for i in range(meta.num_columns)]
    group = meta.row_group(0)
    for col in ("flight_id", "icao24", "flt_id"):
        chunk = group.column(names.index(col))
        assert chunk.bloom_filter_offset is not None, col
        assert not chunk.has_dictionary_page, col
        # Sized for the rows of a row group, not the 1 MB maximum.
        assert chunk.bloom_filter_length < 512 * 1024, col
    for col in ("adep", "type"):
        chunk = group.column(names.index(col))
        assert chunk.bloom_filter_offset is None, col
        assert chunk.has_dictionary_page, col
