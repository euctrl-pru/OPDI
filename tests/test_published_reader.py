"""Tests for the reader of the published files and its classic views."""

import sys

import pandas as pd
import pytest
from test_published_build import DAY, NEXT, _Storage, _working_tables

from opdi import published
from opdi.output.published import PublishedLayout, publish_day
from opdi.output.schema import (
    CLASSIC_EVENT_COLUMNS,
    CLASSIC_FLIGHT_LIST,
    FLIGHT_EVENTS,
    FLIGHT_LIST,
    MEASUREMENT_COLUMNS,
    MEASUREMENT_SUFFIX,
    MEASUREMENT_VERSION,
)


@pytest.fixture(scope="module")
def base(spark, tmp_path_factory):
    """Two published days in a temporary folder, as a file:// base URL."""
    root = tmp_path_factory.mktemp("published")
    layout = PublishedLayout(str(root))
    storage = _Storage(_working_tables(spark))
    publish_day(spark, storage, DAY, layout, commit="abc")
    publish_day(spark, storage, NEXT, layout, commit="abc")
    return f"file://{root}/"


def test_the_base_url_is_one_setting(monkeypatch):
    monkeypatch.delenv("OPDI_PUBLISHED_URL", raising=False)
    assert published.base_url() == published.PUBLISHED_BASE_URL
    monkeypatch.setenv("OPDI_PUBLISHED_URL", "https://example.org/opdi")
    assert published.base_url() == "https://example.org/opdi/"
    assert published.base_url("s3://b/p/") == "s3://b/p/"


def test_file_url():
    url = published.file_url("flight_events", DAY, base="https://x.org/v2.0.0/")
    assert url == "https://x.org/v2.0.0/flight_events/flight_events_20260614.parquet"
    with pytest.raises(ValueError, match="flight_list"):
        published.file_url("events", DAY)


def test_flight_list_round_trip(base):
    one = published.read_flight_list(DAY, base=base)
    both = published.read_flight_list(DAY, NEXT, base=base)
    assert list(one.columns) == [c.name for c in FLIGHT_LIST]
    assert len(one) == 2 and len(both) == 3
    some = published.read_flight_list(DAY, base=base, columns=["flight_id", "adep"])
    assert list(some.columns) == ["flight_id", "adep"]


def test_flight_events_round_trip(base):
    ev = published.read_flight_events(DAY, base=base)
    assert list(ev.columns) == [c.name for c in FLIGHT_EVENTS]
    assert sorted(ev["event_id"]) == ["e1", "e2", "e3", "e4", "e5"]
    atot = published.read_flight_events(DAY, NEXT, base=base, types=["ATOT"])
    assert sorted(atot["event_id"]) == ["e1", "e4", "e6"]


def test_a_missing_day_names_the_file(base):
    with pytest.raises(FileNotFoundError, match="flight_list_20260101"):
        published.read_flight_list(pd.Timestamp("2026-01-01").date(), base=base)


def test_as_events_has_the_ten_classic_columns(base):
    ev = published.as_events(published.read_flight_events(DAY, base=base))
    assert list(ev.columns) == [
        "id",
        "flight_id",
        "type",
        "event_time",
        "longitude",
        "latitude",
        "altitude",
        "source",
        "version",
        "info",
    ]
    assert list(ev.columns) == list(CLASSIC_EVENT_COLUMNS.values())
    assert len(ev) == 5


def test_as_measurements_keeps_the_distance_and_time_rows_of_every_event(base):
    """As release v0.0.2: one distance and one time row per event, also empty."""
    merged = published.read_flight_events(DAY, base=base)
    m = published.as_measurements(merged)
    assert list(m.columns) == ["id", "event_id", "type", "value", "version"]
    always = {"Distance flown (NM)", "Time Passed (s)"}
    other = [c.name for c in MEASUREMENT_COLUMNS if c.source not in always]
    assert len(m) == 2 * len(merged) + int(merged[other].notna().sum().sum()) == 11
    for row in m.itertuples():
        assert row.id == row.event_id + MEASUREMENT_SUFFIX[row.type]
        assert row.version == MEASUREMENT_VERSION[row.type]
    assert set(m.loc[m.event_id == "e2", "type"]) == always | {"Holding duration (s)"}
    # e5 has no measurement at all: two rows, both empty.
    e5 = m[m.event_id == "e5"]
    assert set(e5["type"]) == always and e5["value"].isna().all()
    assert not m.loc[~m.type.isin(always), "value"].isna().any()


def test_as_flight_list_v0_has_the_names_and_order_of_release_v002(base):
    v0 = published.as_flight_list_v0(published.read_flight_list(DAY, base=base))
    assert list(v0.columns) == [
        "id",
        "icao24",
        "flt_id",
        "dof",
        "adep",
        "ades",
        "adep_p",
        "ades_p",
        "registration",
        "model",
        "typecode",
        "icao_aircraft_class",
        "icao_operator",
        "first_seen",
        "last_seen",
        "version",
    ]
    assert list(v0.columns) == list(CLASSIC_FLIGHT_LIST.values())


def _sorted_rows(rows):
    return sorted(tuple("NULL" if v is None or v != v else v for v in r) for r in rows)


def test_duckdb_trino_and_pandas_give_the_same_layout_v002_rows(base):
    """The three readers of the v0.0.2 layout agree, row for row.

    The Trino views are translated to DuckDB with sqlglot and run over the
    same files.
    """
    duckdb = pytest.importorskip("duckdb")
    sqlglot = pytest.importorskip("sqlglot")
    from opdi.sql import render

    merged = published.read_flight_events(DAY, NEXT, base=base)
    flights = published.read_flight_list(DAY, NEXT, base=base)
    expected = {
        "measurements": _sorted_rows(
            published.as_measurements(merged).itertuples(index=False, name=None)
        ),
        "events": len(published.as_events(merged)),
        "flight_list_v0": list(published.as_flight_list_v0(flights).columns),
    }

    con = duckdb.connect()
    con.execute(published.duckdb_views(base=base, start=DAY, end=NEXT))
    got = _sorted_rows(con.execute("select * from measurements").fetchall())
    assert got == expected["measurements"]

    trino = duckdb.connect()
    trino.execute(published.duckdb_views(base=base, start=DAY, end=NEXT))
    for statement in sqlglot.transpile(render.trino_views("c", "s"), read="trino", write="duckdb"):
        trino.execute(statement.replace("c.s.", ""))
    got = _sorted_rows(trino.execute("select * from measurements").fetchall())
    assert got == expected["measurements"]
    assert trino.execute("select count(*) from events").fetchone()[0] == expected["events"]
    cols = [d[0] for d in trino.execute("select * from flight_list_v0").description]
    assert cols == expected["flight_list_v0"]


def test_duckdb_views_match_the_pandas_shapes(base):
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    con.execute(published.duckdb_views(base=base, start=DAY, end=NEXT))
    merged = published.read_flight_events(DAY, NEXT, base=base)
    n = con.execute("select count(*) from measurements").fetchone()[0]
    assert n == len(published.as_measurements(merged))
    assert con.execute("select count(*) from flight_list").fetchone()[0] == 3
    assert con.execute("select count(*) from flight_events").fetchone()[0] == 6
    cols = [d[0] for d in con.execute("select * from events").description]
    assert cols == list(CLASSIC_EVENT_COLUMNS.values())
    ids = {r[0] for r in con.execute("select id from measurements").fetchall()}
    assert ids == set(published.as_measurements(merged)["id"])
    v0 = [d[0] for d in con.execute("select * from flight_list_v0").description]
    assert v0 == list(CLASSIC_FLIGHT_LIST.values())


def test_the_duckdb_views_have_the_names_of_the_trino_views(base):
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    con.execute(published.duckdb_views(base=base, start=DAY, end=NEXT))
    names = {r[0] for r in con.execute("select view_name from duckdb_views()").fetchall()}
    expected = {"flight_list", "flight_events", "flight_index", "events", "measurements"}
    assert expected | {"flight_list_v0"} <= names
    assert not {n for n in names if n.startswith("opdi_")}


def test_the_flight_index_view_reads_the_monthly_index(base):
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    con.execute(published.duckdb_views(base=base, start=DAY, end=NEXT))
    n = con.execute("select count(*) from flight_index").fetchone()[0]
    assert n == len(published.read_flight_list(DAY, NEXT, base=base))


def test_without_an_index_folder_the_flight_index_view_is_left_out(tmp_path, base):
    duckdb = pytest.importorskip("duckdb")
    import shutil

    folder = tmp_path / "copy"
    shutil.copytree(base[len("file://") :], folder)
    shutil.rmtree(folder / "index")
    con = duckdb.connect()
    con.execute(published.duckdb_views(base=f"file://{folder}/", start=DAY, end=NEXT))
    names = {r[0] for r in con.execute("select view_name from duckdb_views()").fetchall()}
    assert "flight_list" in names and "flight_index" not in names


def test_duckdb_views_over_a_folder_use_a_wildcard(base):
    duckdb = pytest.importorskip("duckdb")
    sql = published.duckdb_views(base=base)
    assert "flight_list/*.parquet" in sql
    con = duckdb.connect()
    con.execute(sql)
    assert con.execute("select count(*) from flight_list").fetchone()[0] == 3
    assert con.execute("select count(*) from flight_index").fetchone()[0] == 3


def test_without_duckdb_the_readers_work_and_the_views_say_what_to_install(base, monkeypatch):
    monkeypatch.setitem(sys.modules, "duckdb", None)
    assert len(published.read_flight_list(DAY, base=base)) == 2
    assert len(published.as_events(published.read_flight_events(DAY, base=base))) == 5
    with pytest.raises(ImportError, match=r"opdi\[reader\]"):
        published.duckdb_views(base=base, start=DAY)


def test_duckdb_views_over_a_folder_whose_name_has_a_quote(tmp_path, base):
    duckdb = pytest.importorskip("duckdb")
    import shutil

    folder = tmp_path / "it's here"
    shutil.copytree(base[len("file://") :], folder)
    con = duckdb.connect()
    con.execute(published.duckdb_views(base=f"file://{folder}/"))
    assert con.execute("select count(*) from flight_list").fetchone()[0] == 3
    assert con.execute("select count(*) from flight_index").fetchone()[0] == 3
