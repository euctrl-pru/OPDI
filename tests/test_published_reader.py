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
        "id", "flight_id", "type", "event_time", "longitude", "latitude", "altitude",
        "source", "version", "info",
    ]
    assert list(ev.columns) == list(CLASSIC_EVENT_COLUMNS.values())
    assert len(ev) == 5


def test_as_measurements_has_one_row_per_measured_value(base):
    merged = published.read_flight_events(DAY, base=base)
    m = published.as_measurements(merged)
    assert list(m.columns) == ["id", "event_id", "type", "value", "version"]
    cells = int(merged[[c.name for c in MEASUREMENT_COLUMNS]].notna().sum().sum())
    assert len(m) == cells == 7
    for row in m.itertuples():
        assert row.id == row.event_id + MEASUREMENT_SUFFIX[row.type]
        assert row.version == MEASUREMENT_VERSION[row.type]
    assert set(m.loc[m.event_id == "e2", "type"]) == {
        "Distance flown (NM)", "Time Passed (s)", "Holding duration (s)",
    }


def test_as_flight_list_v0_has_the_classic_upper_case_names(base):
    v0 = published.as_flight_list_v0(published.read_flight_list(DAY, base=base))
    assert list(v0.columns) == list(CLASSIC_FLIGHT_LIST.values())
    assert v0.columns[0] == "ID" and "ADEP" in v0.columns


def test_duckdb_views_match_the_pandas_shapes(base):
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    con.execute(published.duckdb_views(base=base, start=DAY, end=NEXT))
    merged = published.read_flight_events(DAY, NEXT, base=base)
    n = con.execute("select count(*) from opdi_measurements").fetchone()[0]
    assert n == len(published.as_measurements(merged))
    assert con.execute("select count(*) from opdi_flight_list").fetchone()[0] == 3
    assert con.execute("select count(*) from opdi_flight_events_merged").fetchone()[0] == 6
    cols = [d[0] for d in con.execute("select * from opdi_flight_events").description]
    assert cols == list(CLASSIC_EVENT_COLUMNS.values())
    ids = {r[0] for r in con.execute("select id from opdi_measurements").fetchall()}
    assert ids == set(published.as_measurements(merged)["id"])


def test_duckdb_views_over_a_folder_use_a_wildcard(base):
    duckdb = pytest.importorskip("duckdb")
    sql = published.duckdb_views(base=base)
    assert "flight_list/*.parquet" in sql
    con = duckdb.connect()
    con.execute(sql)
    assert con.execute("select count(*) from opdi_flight_list").fetchone()[0] == 3


def test_without_duckdb_the_readers_work_and_the_views_say_what_to_install(
    base, monkeypatch
):
    monkeypatch.setitem(sys.modules, "duckdb", None)
    assert len(published.read_flight_list(DAY, base=base)) == 2
    assert len(published.as_events(published.read_flight_events(DAY, base=base))) == 5
    with pytest.raises(ImportError, match=r"opdi\[reader\]"):
        published.duckdb_views(base=base, start=DAY)
