"""Telling a new airframe from a fragment of a flight ADS-B already saw."""

from datetime import datetime as dt

import adsc_subset_breakdown as b


def _rows(spark, rows):
    return spark.createDataFrame(rows, ["track_id", "icao24", "source", "event_time"])


def test_an_adsc_only_track_of_a_known_airframe_is_a_fragment(spark):
    merged = _rows(spark, [
        ("a1", "aaa", "ADS-B", dt(2026, 6, 5, 8, 0)),
        ("a2", "aaa", "ADS-C", dt(2026, 6, 5, 10, 0)),   # fragment
        ("a2", "aaa", "ADS-C", dt(2026, 6, 5, 10, 10)),
        ("c1", "ccc", "ADS-C", dt(2026, 6, 5, 12, 0)),   # unheard
    ])
    adsb = merged.where("source = 'ADS-B'").select("icao24")
    got = {r["track_id"]: r for r in b.classify_adsc_tracks(merged, adsb).collect()}
    assert set(got) == {"a2", "c1"}
    assert got["a2"]["airframe_heard_by_adsb"] is True
    assert got["a2"]["span_s"] == 600
    assert got["c1"]["airframe_heard_by_adsb"] is False


def test_placement_separates_holes_extensions_and_adsc_only(spark):
    merged = _rows(spark, [
        ("t", "aaa", "ADS-B", dt(2026, 6, 5, 8, 0)),
        ("t", "aaa", "ADS-C", dt(2026, 6, 5, 8, 20)),    # interior, 20 min each side
        ("t", "aaa", "ADS-B", dt(2026, 6, 5, 8, 40)),
        ("t", "aaa", "ADS-C", dt(2026, 6, 5, 8, 41)),    # past the last ADS-B
        ("o", "ccc", "ADS-C", dt(2026, 6, 5, 9, 0)),     # no ADS-B on the track
    ])
    got = {(r["track_id"], str(r["event_time"])): r
           for r in b.adsc_placement(merged).collect()}
    interior = got[("t", "2026-06-05 08:20:00")]
    assert interior["kind"] == "interior_hole"
    assert interior["gap_before_s"] == 1200 and interior["gap_after_s"] == 1200
    assert interior["in_hole"] is True
    edge = got[("t", "2026-06-05 08:41:00")]
    assert edge["kind"] == "extends_track"
    assert edge["gap_before_s"] == 60
    assert edge["in_hole"] is False
    assert got[("o", "2026-06-05 09:00:00")]["kind"] == "no_adsb_on_track"
