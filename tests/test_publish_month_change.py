"""Publishing across a month change, and a flight without events."""

import datetime as dt

import pyarrow.parquet as pq
from test_published_build import _flight, _Storage

from opdi.output.published import PublishedLayout, publish_day
from opdi.output.schema import FLIGHT_LIST

JUNE = dt.date(2026, 6, 30)
JULY = dt.date(2026, 7, 1)


def _ts(day, hh):
    return dt.datetime(day.year, day.month, day.day, hh, 0)


def _tables(spark):
    cols = [c for c in FLIGHT_LIST if c.name != "release"]
    ddl = ", ".join(f"`{c.source}` {c.dtype}" for c in cols)
    flights = [
        _flight("trk_jun", JUNE, _ts(JUNE, 22), "LFPG"),
        # No event at all: kept in the flight list, absent from the events file.
        _flight("trk_quiet", JUNE, _ts(JUNE, 10), "EDDF"),
        _flight("trk_jul", JULY, _ts(JULY, 9), "EGLL"),
    ]
    flight_list = spark.createDataFrame([tuple(r[c.source] for c in cols) for r in flights], ddl)
    events = spark.createDataFrame(
        [
            ("e1", "trk_jun", "ATOT", _ts(JUNE, 22), JUNE),
            # After midnight, in July, but the flight started in June.
            ("e2", "trk_jun", "ALDT", _ts(JULY, 1), JUNE),
            ("e3", "trk_jul", "ATOT", _ts(JULY, 9), JULY),
        ],
        "id string, flight_id string, type string, event_time timestamp, dof date",
    ).selectExpr(
        "id",
        "flight_id",
        "type",
        "event_time",
        "2.0 as longitude",
        "49.0 as latitude",
        "0.0 as altitude",
        "'OSN' as source",
        "'events_v0.3.0' as version",
        "'{}' as info",
        "dof",
    )
    measurements = spark.createDataFrame(
        [("e1_d", "e1", "Distance flown (NM)", 0.0, "distance_v0.0.3", JUNE)],
        "id string, milestone_id string, type string, value double, version string, dof date",
    )
    return {
        "opdi_flight_list": flight_list,
        "opdi_flight_events": events,
        "opdi_measurements": measurements,
    }


def test_each_month_gets_its_own_index_and_a_quiet_flight_is_kept(spark, tmp_path):
    layout = PublishedLayout(str(tmp_path / "published"))
    storage = _Storage(_tables(spark))
    june = publish_day(spark, storage, JUNE, layout, commit="abc")
    july = publish_day(spark, storage, JULY, layout, commit="abc")

    assert june["flights"] == 2 and june["events"] == 2
    assert july["flights"] == 1 and july["events"] == 1
    assert layout.flight_index(JUNE).endswith("flight_index_202606.parquet")
    assert layout.flight_index(JULY).endswith("flight_index_202607.parquet")

    jun_index = pq.read_table(layout.flight_index(JUNE)).to_pylist()
    jul_index = pq.read_table(layout.flight_index(JULY)).to_pylist()
    assert {r["flight_id"]: r["file"] for r in jun_index} == {
        "trk_jun": "flight_list/flight_list_20260630.parquet",
        "trk_quiet": "flight_list/flight_list_20260630.parquet",
    }
    assert {r["flight_id"] for r in jul_index} == {"trk_jul"}

    flights = set(pq.read_table(layout.flight_list(JUNE)).column("flight_id").to_pylist())
    events = pq.read_table(layout.flight_events(JUNE)).to_pylist()
    assert flights == {"trk_jun", "trk_quiet"}
    assert {e["flight_id"] for e in events} == {"trk_jun"}
    # The event after midnight is in the June file, under the June day.
    assert {e["event_id"]: e["dof"] for e in events} == {"e1": JUNE, "e2": JUNE}

    catalogue = {r["day"]: r for r in spark.read.parquet(layout.catalogue).collect()}
    assert sorted(catalogue) == [JUNE, JULY]
    assert catalogue[JUNE]["last_event"] == _ts(JULY, 1)
