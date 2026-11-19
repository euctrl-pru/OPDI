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


# ---------------------------------------------------------------------------
# The off-block accuracy by where the aircraft was first received
# ---------------------------------------------------------------------------


def _classified():
    import pandas as pd

    rows = [
        # kind, record s, OPDI s, info of the OPDI event, first report s
        ("AOBT", 1000.0, 1020.0, '{"first_seen_on": "stand"}', 400.0),
        ("AOBT", 2000.0, 2200.0, '{"first_seen_on": "stand"}', 1500.0),
        ("AOBT", 3000.0, 3400.0, '{"first_seen_on": "taxiway"}', 3350.0),
        ("AOBT", 4000.0, 4030.0, '{"stand": "A1"}', 3000.0),
        ("AOBT", 5000.0, None, None, 5200.0),
        ("AIBT", 6000.0, 6010.0, "{}", 100.0),
    ]
    d = pd.DataFrame(rows, columns=["kind", "rec_s", "opdi_s", "opdi_info", "fs"])
    d["flight_id"] = "f"
    d["diff_s"] = d["opdi_s"] - d["rec_s"]
    d["lead_s"] = d["rec_s"] - d["fs"]
    return d


def test_the_off_block_accuracy_by_where_the_aircraft_was_first_received():
    """One row per value of ``info.first_seen_on`` of the off-block times that
    have a record (``not given`` when the event has no value), and one for all
    of them. In-block times and records without an OPDI time are not counted."""
    out = oc.first_seen_table({"2026-06-14": _classified()}).set_index("first_seen_on")

    assert out["off_block_times"].to_dict() == {"all": 4, "stand": 2, "taxiway": 1, "not given": 1}
    assert out.loc["stand", "pct_within_60s"] == 50.0
    assert out.loc["taxiway", "pct_within_60s"] == 0.0
    assert out.loc["all", "pct_within_60s"] == 50.0
    assert out.loc["all", "median_abs_diff_s"] == 115.0
    # Received at the stand: a report before the recorded off-block time.
    assert out.loc["all", "n_received_at_stand"] == 3
    assert out.loc["all", "pct_within_60s_when_received_at_stand"] == 66.7
    assert out.loc["taxiway", "n_received_at_stand"] == 0
    assert set(out["day"]) == {"2026-06-14"}


def test_the_tracks_divided_into_flights_are_counted(spark):
    _, FLX, _ = _frames(spark)
    one = spark.createDataFrame(
        [("u", "u", None, None, "4b0000", "SWR9", "LSZH", "LSGG", 0.0, 100.0)], _FLX_DDL
    )
    assert oc._flight_counts(FLX.unionByName(one)) == {
        "tracks": 2,
        "tracks_divided": 1,
        "flights_of_divided_tracks": 2,
        "flight_rows": 3,
    }
