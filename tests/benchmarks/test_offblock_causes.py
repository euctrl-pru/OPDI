"""Tests of benchmarks/offblock_causes.py: records matched to flight rows."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "benchmarks"))

import offblock_causes as oc  # noqa: E402

_FLX_DDL = (
    "flight_id string, track_id string, _lo double, _hi double, icao24 string, cs string, "
    "adep string, ades string, fs double, ls double"
)
_EVENT_DDL = (
    "id string, flight_id string, type string, ts double, info string, apt string, _eid long"
)
_REC_DDL = (
    "kind string, _a string, _c string, rec_apt string, rec_s double, rwy_s double, "
    "rec_key string"
)


def _frames(spark):
    """A track that flies LSZH - EDDH - LSZH under one call sign: two flight
    rows, divided at 4,000 s. Its events are stored under the track."""
    flights = [
        ("t-L1", "t", None, 4000.0, "4b1234", "SWR12", "LSZH", "EDDH", 0.0, 4000.0),
        ("t-L2", "t", 4000.0, None, "4b1234", "SWR12", "EDDH", "LSZH", 4001.0, 9000.0),
    ]
    events = [
        ("e1", "t", "AOBT", 100.0, "{}", "LSZH", 1),
        ("e2", "t", "AIBT", 3900.0, "{}", "EDDH", 2),
        ("e3", "t", "AOBT", 4500.0, '{"first_seen_on": null}', "EDDH", 3),
        ("e4", "t", "AIBT", 8900.0, "{}", "LSZH", 4),
    ]
    records = [
        ("AOBT", "4b1234", "SWR12", "LSZH", 120.0, 700.0, "r1"),
        ("AOBT", "4b1234", "SWR12", "EDDH", 4480.0, 5000.0, "r2"),
        ("AIBT", "4b1234", "SWR12", "EDDH", 3880.0, 3500.0, "r3"),
    ]
    return (
        spark.createDataFrame(events, _EVENT_DDL),
        spark.createDataFrame(flights, _FLX_DDL),
        spark.createDataFrame(records, _REC_DDL),
    )


def test_a_record_gets_the_event_and_the_flight_row_of_its_own_flight(spark):
    E, FLX, blk = _frames(spark)
    ev = oc._matched_events(E, FLX, blk)
    got = {r["rec_key"]: r for r in oc._flights(FLX, blk, ev).collect()}

    assert {k: (r["opdi_s"], r["flight_id"]) for k, r in got.items()} == {
        "r1": (100.0, "t-L1"),
        "r2": (4500.0, "t-L2"),
        "r3": (3900.0, "t-L1"),
    }
    # The track the reports are read from, and the limits of the flight in it.
    assert {k: (r["track_id"], r["_lo"], r["_hi"]) for k, r in got.items()} == {
        "r1": ("t", None, 4000.0),
        "r2": ("t", 4000.0, None),
        "r3": ("t", None, 4000.0),
    }


def test_the_reports_of_a_record_are_those_of_its_flight(spark):
    """The first and last report of a record's flight are those between the
    division times of its flight row, not those of the whole track."""
    E, FLX, blk = _frames(spark)
    recs = oc._flights(FLX, blk, oc._matched_events(E, FLX, blk))
    reports = spark.createDataFrame(
        [("t", float(s)) for s in range(0, 9001, 500)], "track_id string, t double"
    )
    rep = oc._reports_of_flights(reports, recs)
    span = {
        r["rec_key"]: (r["lo"], r["hi"])
        for r in rep.groupBy("rec_key")
        .agg({"t": "min"})
        .withColumnRenamed("min(t)", "lo")
        .join(rep.groupBy("rec_key").agg({"t": "max"}).withColumnRenamed("max(t)", "hi"), "rec_key")
        .collect()
    }

    assert span == {"r1": (0.0, 4000.0), "r2": (4500.0, 9000.0), "r3": (0.0, 4000.0)}
