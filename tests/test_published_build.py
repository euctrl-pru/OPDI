"""Tests for building and publishing a day of OPDI data."""

import datetime as dt
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from pyspark.sql import functions as F

from opdi.output.published import (
    FLIGHT_EVENTS_BLOOM,
    PublishedLayout,
    build_flight_events,
    build_flight_list,
    publish_day,
    write_single_file,
    write_temporary_file,
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
    assert set(rows) == {"trk_1-E0001", "trk_1-E0002", "trk_1-E0003", "trk_2-E0001", "trk_2-E0002"}
    assert rows["trk_1-E0003"]["event_time"].date() == NEXT
    assert rows["trk_1-E0003"]["dof"] == DAY
    assert rows["trk_1-E0003"]["adep"] == "LFPG"


def test_missing_measurement_is_null_never_zero(tables):
    rows = {r["event_id"]: r for r in _day_events(tables).collect()}
    assert rows["trk_1-E0001"]["holding_duration_s"] is None
    assert rows["trk_1-E0002"]["holding_duration_s"] == 720.0
    # A real zero stays a zero.
    assert rows["trk_1-E0001"]["distance_flown_nm"] == 0.0
    # No measurement row at all: every measurement column is NULL.
    for c in MEASUREMENT_COLUMNS:
        assert rows["trk_2-E0002"][c.name] is None
    assert rows["trk_1-E0003"]["time_passed_s"] is None


def test_flight_columns_are_repeated_on_every_event(tables):
    rows = {r["event_id"]: r for r in _day_events(tables).collect()}
    assert rows["trk_2-E0001"]["flt_id"] == "TST2"
    assert rows["trk_2-E0001"]["flight_version"] == "v5.0.0"
    assert rows["trk_2-E0001"]["version"] == "events_v0.3.0"
    assert rows["trk_2-E0001"]["release"] == RELEASE
    assert rows["trk_2-E0001"]["h3_res_5"] is not None


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
    assert sorted(ev.column("event_id").to_pylist()) == [
        "trk_1-E0001",
        "trk_1-E0002",
        "trk_1-E0003",
        "trk_2-E0001",
        "trk_2-E0002",
    ]


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
    assert sorted(day) == ["o1", "trk_a-E0001", "trk_a-E0002"]
    assert sorted(nxt) == ["o2", "trk_b-E0001", "trk_b-E0002"]

    # The flight's own columns, and its date, are on the events stored under
    # the other day.
    assert day["trk_a-E0001"]["dof"] == DAY and day["trk_a-E0001"]["adep"] == "LFPG"
    assert day["trk_a-E0001"]["distance_flown_nm"] == 3.0
    assert nxt["trk_b-E0001"]["dof"] == NEXT and nxt["trk_b-E0001"]["adep"] == "EDDF"
    assert nxt["trk_b-E0001"]["distance_flown_nm"] == 0.0

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


# --- a track that holds several flights ----------------------------------------


def test_the_flights_of_a_track_are_published_in_the_day_of_their_track(spark, tmp_path):
    """trk_a lands at 23:40 on DAY and takes off again at 00:40 on NEXT.

    Its second flight starts on NEXT, but both flights, and all events of the
    track, are in the files of DAY, the day of the track; the files of NEXT
    hold none of them.
    """
    flight_cols = [c for c in FLIGHT_LIST if c.name != "release"]
    ddl = ", ".join(f"`{c.source}` {c.dtype}" for c in flight_cols)
    leg_ddl = ddl + ", TRACK_ID string, LEG int, LEGS int, LEG_START timestamp, LEG_END timestamp"
    divided = _ts(NEXT, 0, 10)
    track = _flight("trk_a", DAY, _ts(DAY, 23, 10), "LFPG")
    other = _flight("trk_b", NEXT, _ts(NEXT, 0, 5), "EDDF")
    one = dict(track, ID="trk_a-L1", ADES="EDDF", LAST_SEEN=divided)
    two = dict(track, ID="trk_a-L2", ADEP="EDDF", FIRST_SEEN=divided)
    legs = [
        tuple(one[c.source] for c in flight_cols) + ("trk_a", 1, 2, None, divided),
        tuple(two[c.source] for c in flight_cols) + ("trk_a", 2, 2, divided, None),
    ]
    events = spark.createDataFrame(
        [
            ("a1", "trk_a", "ATOT", _ts(DAY, 23, 15), 2.5, 49.0, 0.0, DAY),
            ("a2", "trk_a", "ALDT", _ts(DAY, 23, 40), 8.5, 50.0, 0.0, DAY),
            ("a3", "trk_a", "ATOT", _ts(NEXT, 0, 40), 8.5, 50.0, 0.0, NEXT),
            ("a4", "trk_a", "ALDT", _ts(NEXT, 1, 30), 4.7, 52.3, 0.0, NEXT),
            ("b1", "trk_b", "ATOT", _ts(NEXT, 0, 15), 8.5, 50.0, 0.0, NEXT),
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
        "'events_v0.5.0' as version",
        "'{}' as info",
        "dof",
    )
    measurements = spark.createDataFrame(
        [("a3_d", "a3", "Distance flown (NM)", 250.0, "distance_v0.0.3", NEXT)],
        "id string, milestone_id string, type string, value double, version string, dof date",
    )
    storage = _Storage(
        {
            "opdi_flight_list": spark.createDataFrame(
                [tuple(r[c.source] for c in flight_cols) for r in (track, other)], ddl
            ),
            "opdi_flight_legs": spark.createDataFrame(legs, leg_ddl),
            "opdi_flight_events": events,
            "opdi_measurements": measurements,
        }
    )
    layout = PublishedLayout(str(tmp_path / "published"))
    publish_day(spark, storage, DAY, layout, commit="abc")
    publish_day(spark, storage, NEXT, layout, commit="abc")

    flights = {r["flight_id"]: r for r in pq.read_table(layout.flight_list(DAY)).to_pylist()}
    assert sorted(flights) == ["trk_a-L1", "trk_a-L2"]
    assert {r["dof"] for r in flights.values()} == {DAY}
    assert flights["trk_a-L2"]["first_seen"] == divided
    day = {r["event_id"]: r for r in pq.read_table(layout.flight_events(DAY)).to_pylist()}
    assert {k: v["flight_id"] for k, v in day.items()} == {
        "trk_a-L1-E0001": "trk_a-L1",
        "trk_a-L1-E0002": "trk_a-L1",
        "trk_a-L2-E0001": "trk_a-L2",
        "trk_a-L2-E0002": "trk_a-L2",
    }
    assert {r["dof"] for r in day.values()} == {DAY}
    assert (day["trk_a-L2-E0001"]["adep"], day["trk_a-L2-E0001"]["distance_flown_nm"]) == (
        "EDDF",
        250.0,
    )

    nxt_flights = pq.read_table(layout.flight_list(NEXT)).column("flight_id").to_pylist()
    nxt_events = pq.read_table(layout.flight_events(NEXT)).column("event_id").to_pylist()
    assert (nxt_flights, nxt_events) == (["trk_b"], ["trk_b-E0001"])

    index = {r["flight_id"]: r for r in pq.read_table(layout.flight_index(DAY)).to_pylist()}
    assert sorted(index) == ["trk_a-L1", "trk_a-L2", "trk_b"]
    assert index["trk_a-L2"]["dof"] == DAY
    assert index["trk_a-L2"]["file"] == f"flight_list/flight_list_{DAY:%Y%m%d}.parquet"


# --- event ids from the flight, rounded values ---------------------------------


def _mini(spark, events, measurements=(), flight="trk_x"):
    """One flight with the given working events and measurements.

    Args:
        events: ``(id, type, event_time, longitude, latitude, altitude, info)``.
        measurements: ``(milestone_id, type, value)``.
    """
    flight_cols = [c for c in FLIGHT_LIST if c.name != "release"]
    ddl = ", ".join(f"`{c.source}` {c.dtype}" for c in flight_cols)
    row = _flight(flight, DAY, _ts(DAY, 8, 0), "EDDF")
    row.update(RWY_DEP_BEARING_DEG=123.456789012, RWY_ARR_BEARING_DEG=None)
    fl = spark.createDataFrame([tuple(row[c.source] for c in flight_cols)], ddl)
    ev = spark.createDataFrame(
        [
            (i, flight, t, ts, lon, lat, alt, "OSN", "events_v0.3.0", info, DAY)
            for (i, t, ts, lon, lat, alt, info) in events
        ],
        "id string, flight_id string, type string, event_time timestamp, longitude double, "
        "latitude double, altitude double, source string, version string, info string, dof date",
    )
    ms = spark.createDataFrame(
        [(f"{m}_{k}", m, k, v, "v", DAY) for (m, k, v) in measurements],
        "id string, milestone_id string, type string, value double, version string, dof date",
    )
    return ev, ms, fl


def test_event_id_is_the_flight_id_and_the_place_in_time(tables):
    rows = _day_events(tables).collect()
    by_flight = {}
    for r in sorted(rows, key=lambda r: r["event_time"]):
        by_flight.setdefault(r["flight_id"], []).append(r["event_id"])
    assert by_flight == {
        "trk_1": ["trk_1-E0001", "trk_1-E0002", "trk_1-E0003"],
        "trk_2": ["trk_2-E0001", "trk_2-E0002"],
    }


def test_event_id_order_is_time_then_type_then_working_id(spark):
    t = _ts(DAY, 9, 0)
    ev, ms, fl = _mini(
        spark,
        [
            ("z9", "b-type", t, 1.0, 1.0, 0.0, "{}"),
            ("a1", "b-type", t, 1.0, 1.0, 0.0, "{}"),
            ("m5", "a-type", t, 1.0, 1.0, 0.0, "{}"),
            ("q0", "c-type", _ts(DAY, 8, 30), 1.0, 1.0, 0.0, "{}"),
        ],
    )
    rows = build_flight_events(ev, ms, fl).collect()
    order = {r["event_id"]: r["type"] for r in rows}
    assert order == {
        "trk_x-E0001": "c-type",
        "trk_x-E0002": "a-type",
        "trk_x-E0003": "b-type",
        "trk_x-E0004": "b-type",
    }
    # The two events of equal time and type: the working id decides ("a1" first).
    by_id = {r["event_id"]: r for r in rows}
    again = build_flight_events(ev.orderBy(F.col("id").desc()), ms, fl).collect()
    assert {r["event_id"]: r["type"] for r in again} == order
    assert by_id["trk_x-E0003"]["type"] == "b-type"


def test_measurements_stay_with_their_event_after_the_id_changes(tables):
    rows = {r["event_id"]: r for r in _day_events(tables).collect()}
    assert rows["trk_1-E0002"]["type"] == "holding-end"
    assert rows["trk_1-E0002"]["holding_duration_s"] == 720.0
    assert rows["trk_1-E0002"]["distance_flown_nm"] == 250.0
    assert rows["trk_1-E0001"]["holding_duration_s"] is None
    assert rows["trk_1-E0003"]["distance_flown_nm"] == 300.0
    assert rows["trk_2-E0002"]["distance_flown_nm"] is None


def test_the_flights_of_one_track_number_their_events_on_their_own(spark, tmp_path):
    flight_cols = [c for c in FLIGHT_LIST if c.name != "release"]
    ddl = ", ".join(f"`{c.source}` {c.dtype}" for c in flight_cols)
    leg_ddl = ddl + ", TRACK_ID string, LEG int, LEGS int, LEG_START timestamp, LEG_END timestamp"
    divided = _ts(NEXT, 0, 10)
    track = _flight("trk_a", DAY, _ts(DAY, 23, 10), "LFPG")
    one = dict(track, ID="trk_a-L1", ADES="EDDF", LAST_SEEN=divided)
    two = dict(track, ID="trk_a-L2", ADEP="EDDF", FIRST_SEEN=divided)
    legs = spark.createDataFrame(
        [
            tuple(one[c.source] for c in flight_cols) + ("trk_a", 1, 2, None, divided),
            tuple(two[c.source] for c in flight_cols) + ("trk_a", 2, 2, divided, None),
        ],
        leg_ddl,
    )
    events = spark.createDataFrame(
        [
            ("a1", "trk_a", "ATOT", _ts(DAY, 23, 15), 2.5, 49.0, 0.0, DAY),
            ("a2", "trk_a", "ALDT", _ts(DAY, 23, 40), 8.5, 50.0, 0.0, DAY),
            ("a3", "trk_a", "ATOT", _ts(NEXT, 0, 40), 8.5, 50.0, 0.0, NEXT),
            ("a4", "trk_a", "ALDT", _ts(NEXT, 1, 30), 4.7, 52.3, 0.0, NEXT),
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
        "'events_v0.5.0' as version",
        "'{}' as info",
        "dof",
    )
    ms = spark.createDataFrame(
        [("a3_d", "a3", "Distance flown (NM)", 250.0, "distance_v0.0.3", NEXT)],
        "id string, milestone_id string, type string, value double, version string, dof date",
    )
    rows = build_flight_events(events, ms, legs).collect()
    assert {(r["flight_id"], r["event_id"]) for r in rows} == {
        ("trk_a-L1", "trk_a-L1-E0001"),
        ("trk_a-L1", "trk_a-L1-E0002"),
        ("trk_a-L2", "trk_a-L2-E0001"),
        ("trk_a-L2", "trk_a-L2-E0002"),
    }
    by_id = {r["event_id"]: r for r in rows}
    assert by_id["trk_a-L2-E0001"]["distance_flown_nm"] == 250.0


def test_an_event_without_a_flight_in_the_list_keeps_its_working_id(spark):
    ev, ms, fl = _mini(spark, [("o1", "ATOT", _ts(DAY, 9, 0), 1.0, 1.0, 0.0, "{}")])
    ev = ev.withColumn("flight_id", F.lit("lost"))
    rows = build_flight_events(ev, ms, fl).collect()
    assert [(r["flight_id"], r["event_id"], r["adep"]) for r in rows] == [("lost", "o1", None)]


LONG = 1.123456789012345


def test_values_are_rounded_by_column_rule(spark):
    ev, ms, fl = _mini(
        spark,
        [("e1", "ATOT", _ts(DAY, 9, 0), 4.123456789, 50.987654321, 1075.0000344, "{}")],
        [
            ("e1", "Distance flown (NM)", 123.456789),
            ("e1", "Time Passed (s)", 600.123456),
            ("e1", "Holding circuits", 1.23456),
            ("e1", "Holding distance (NM)", 9.87654321),
        ],
    )
    [r] = build_flight_events(ev, ms, fl).collect()
    assert r["longitude"] == 4.12346
    assert r["latitude"] == 50.98765
    assert r["altitude_ft"] == 1075.0
    assert r["distance_flown_nm"] == 123.457
    assert r["holding_distance_nm"] == 9.877
    assert r["rwy_dep_bearing_deg"] == 123.457
    # Other columns keep every digit.
    assert r["time_passed_s"] == 600.123456
    assert r["holding_circuits"] == 1.23456
    assert r["track_duration_min"] == 120.0
    # Types stay double.
    assert dict(_types(build_flight_events(ev, ms, fl)))["altitude_ft"] == "double"


def test_the_flight_list_rounds_its_degree_columns_and_agrees_with_the_events(spark):
    ev, ms, fl = _mini(spark, [("e1", "ATOT", _ts(DAY, 9, 0), 1.0, 1.0, 0.0, "{}")])
    [f] = build_flight_list(fl).collect()
    [e] = build_flight_events(ev, ms, fl).collect()
    assert f["rwy_dep_bearing_deg"] == 123.457 == e["rwy_dep_bearing_deg"]
    [raw] = build_flight_list(fl, values_rounded=False).collect()
    assert raw["rwy_dep_bearing_deg"] == 123.456789012


def test_the_rounding_rule_is_in_one_place():
    from opdi.output.schema import published_decimals

    assert published_decimals("longitude") == 5
    assert published_decimals("latitude") == 5
    assert published_decimals("distance_flown_nm") == 3
    assert published_decimals("pms_leg_distance_nm") == 3
    assert published_decimals("rwy_arr_bearing_deg") == 3
    assert published_decimals("altitude_ft") == 0
    for name in ("time_passed_s", "pms_leg_time_s", "holding_circuits", "track_duration_min"):
        assert published_decimals(name) is None


INFO_CASES = [
    # Text values are left alone.
    ('{"take_off":"2026-06-14T18:19:56.166666Z"}', '{"take_off":"2026-06-14T18:19:56.166666Z"}'),
    ('{"x":9.225955234597905E-4}', '{"x":9.225E-4}'),
    ('{"x":-12.3456789}', '{"x":-12.345}'),
    ('{"a":[1.23456789,2.0,-3.98765]}', '{"a":[1.234,2.0,-3.987]}'),
    ('{"n":12,"s":1.5,"t":0.125}', '{"n":12,"s":1.5,"t":0.125}'),
    ('{"a":1.1234,"b":22.98765}', '{"a":1.123,"b":22.987}'),
    # A positive exponent is a large number: left as it is.
    ('{"x":1.2345678901234567E10}', '{"x":1.2345678901234567E10}'),
    ('{"x":-1.2345678E-5}', '{"x":-1.234E-5}'),
    ("{}", "{}"),
    ("", ""),
    (None, None),
]


@pytest.mark.parametrize("raw,expected", INFO_CASES)
def test_info_numbers_are_cut_to_three_decimals_unless_large(spark, raw, expected):
    ev, ms, fl = _mini(spark, [("e1", "ATOT", _ts(DAY, 9, 0), 1.0, 1.0, 0.0, raw)])
    [r] = build_flight_events(ev, ms, fl).collect()
    assert r["info"] == expected
    [off] = build_flight_events(ev, ms, fl, values_rounded=False).collect()
    assert off["info"] == raw


def test_with_both_settings_off_the_output_is_the_working_values(tables):
    off = build_flight_events(
        tables["opdi_flight_events"].filter(f"dof = '{DAY}'"),
        tables["opdi_measurements"].filter(f"dof = '{DAY}'"),
        tables["opdi_flight_list"],
        event_id_by_flight=False,
        values_rounded=False,
    )
    rows = {r["event_id"]: r for r in off.collect()}
    assert set(rows) == {"e1", "e2", "e3", "e4", "e5"}
    assert rows["e2"]["holding_duration_s"] == 720.0


def test_off_keeps_long_values(spark):
    ev, ms, fl = _mini(
        spark,
        [("e1", "ATOT", _ts(DAY, 9, 0), 4.123456789, 50.987654321, 1075.0000344, "{}")],
    )
    [r] = build_flight_events(ev, ms, fl, event_id_by_flight=False, values_rounded=False).collect()
    assert (r["event_id"], r["longitude"], r["altitude_ft"]) == ("e1", 4.123456789, 1075.0000344)


def test_the_settings_are_on_by_default_and_off_in_legacy():
    from opdi.config import EventConfig

    assert EventConfig().published_event_id_by_flight is True
    assert EventConfig().published_values_rounded is True
    assert EventConfig.legacy().published_event_id_by_flight is False
    assert EventConfig.legacy().published_values_rounded is False


def test_publish_day_follows_the_two_settings(spark, tables, tmp_path):
    on = PublishedLayout(str(tmp_path / "on"))
    off = PublishedLayout(str(tmp_path / "off"))
    publish_day(spark, _Storage(tables), DAY, on, commit="abc")
    publish_day(
        spark,
        _Storage(tables),
        DAY,
        off,
        commit="abc",
        event_id_by_flight=False,
        values_rounded=False,
    )
    assert set(pq.read_table(on.flight_events(DAY)).column("event_id").to_pylist()) == {
        "trk_1-E0001",
        "trk_1-E0002",
        "trk_1-E0003",
        "trk_2-E0001",
        "trk_2-E0002",
    }
    assert set(pq.read_table(off.flight_events(DAY)).column("event_id").to_pylist()) == {
        "e1",
        "e2",
        "e3",
        "e4",
        "e5",
    }


# --- how the files are written ------------------------------------------------


def _bloom_bytes(path):
    meta = pq.ParquetFile(path).metadata
    names = [meta.schema.column(i).name for i in range(meta.num_columns)]
    return meta.row_group(0).column(names.index("flight_id")).bloom_filter_length


def test_the_bloom_filter_is_sized_by_the_expected_distinct_values(spark, tmp_path):
    df = spark.createDataFrame([(f"trk_{i:05d}",) for i in range(4000)], "flight_id string")
    big = write_temporary_file(
        df, str(tmp_path / "big"), bloom_filters=("flight_id",), row_group_rows=4000
    )
    small = write_temporary_file(
        df,
        str(tmp_path / "small"),
        bloom_filters=("flight_id",),
        row_group_rows=4000,
        bloom_expected_ndv=400,
    )
    # Default: one distinct value per row of the group.
    assert _bloom_bytes(big) > 2 * _bloom_bytes(small) > 0


def test_the_published_files_size_their_filters_for_their_content():
    from opdi.output import published

    assert published.events_bloom_ndv(100_000) == 10_000
    assert published.events_bloom_ndv(5) == 1


def test_the_zstd_level_changes_the_file(spark, tmp_path):
    import random

    random.seed(3)
    words = [f"w{random.randrange(5000):04d}" for _ in range(60000)]
    rows = [(" ".join(words[i : i + 12]),) for i in range(0, 60000, 3)]
    df = spark.createDataFrame(rows, "t string")
    low = write_temporary_file(df, str(tmp_path / "l1"), zstd_level=1)
    high = write_temporary_file(df, str(tmp_path / "l19"), zstd_level=19)
    size = lambda p: Path(p).stat().st_size  # noqa: E731
    assert size(high) < size(low)


def test_the_default_zstd_level_is_nine():
    from opdi.output import published

    assert published.ZSTD_LEVEL == 9


def test_events_without_a_flight_do_not_repeat_an_id_across_days(spark):
    """One track with no flight-list row has events in the files of two days."""
    ev, ms, fl = _mini(
        spark,
        [
            ("o1", "ATOT", _ts(DAY, 23, 0), 1.0, 1.0, 0.0, "{}"),
            ("o2", "ALDT", _ts(NEXT, 1, 0), 1.0, 1.0, 0.0, "{}"),
        ],
    )
    ev = ev.withColumn("flight_id", F.lit("lost"))
    first = build_flight_events(ev.filter("id = 'o1'"), ms, fl).collect()
    second = build_flight_events(ev.filter("id = 'o2'"), ms, fl).collect()
    ids = [r["event_id"] for r in first + second]
    assert len(set(ids)) == 2
    assert {r["flight_id"] for r in first + second} == {"lost"}


def test_matched_and_unmatched_events_in_one_file_never_share_an_id(spark):
    ev, ms, fl = _mini(
        spark,
        [
            ("m1", "ATOT", _ts(DAY, 9, 0), 1.0, 1.0, 0.0, "{}"),
            ("m2", "ALDT", _ts(DAY, 10, 0), 1.0, 1.0, 0.0, "{}"),
            ("u1", "ATOT", _ts(DAY, 9, 0), 1.0, 1.0, 0.0, "{}"),
        ],
    )
    ev = ev.withColumn(
        "flight_id", F.when(F.col("id") == "u1", "lost").otherwise(F.col("flight_id"))
    )
    rows = build_flight_events(ev, ms, fl).collect()
    ids = {r["event_id"]: r["flight_id"] for r in rows}
    assert ids == {"trk_x-E0001": "trk_x", "trk_x-E0002": "trk_x", "u1": "lost"}


def test_ids_of_a_flight_sort_in_time_order_as_text_and_are_not_cut(spark):
    ev, ms, fl = _mini(
        spark,
        [
            (f"w{i}", "ATOT", _ts(DAY, 9, 0) + dt.timedelta(seconds=i), 1.0, 1.0, 0.0, "{}")
            for i in range(12)
        ],
    )
    rows = build_flight_events(ev, ms, fl).collect()
    by_text = [r["event_time"] for r in sorted(rows, key=lambda r: r["event_id"])]
    assert by_text == sorted(by_text)
    assert "trk_x-E0012" in {r["event_id"] for r in rows}


def test_the_catalogue_keeps_its_file_sizes(spark, tables, layout):
    publish_day(spark, _Storage(tables), DAY, layout, commit="abc")
    cat = spark.read.parquet(layout.catalogue)
    types = dict(cat.dtypes)
    assert types["flight_list_bytes"] == "bigint" and types["flight_events_bytes"] == "bigint"
    row = cat.collect()[0]
    assert row["flight_list_bytes"] > 0 and row["flight_events_bytes"] > 0
