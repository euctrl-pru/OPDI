"""The Spark reductions of the ADS-C study script, on a local Spark session.

These need the ``spark`` fixture of ``tests/conftest.py`` (a local session, no
cluster, no credentials).
"""

from datetime import datetime, timedelta

import adsc_effect as ae
import pandas as pd
import pytest

T0 = datetime(2026, 6, 14, 8, 0, 0)


def at(minutes):
    return T0 + timedelta(minutes=minutes)


TRACK_SCHEMA = (
    "icao24 string, track_id string, event_time timestamp, "
    "cumulative_distance_nm double, source string, callsign string"
)


@pytest.fixture
def merged_tracks(spark):
    rows = [
        # ADS-B, a hole of 120 min, two ADS-C reports in it, then ADS-B again.
        ("a", "t1", at(0), 0.0, "ADS-B", "DLH400"),
        ("a", "t1", at(1), 8.0, "ADS-B", "DLH400 "),
        ("a", "t1", at(41), 330.0, "ADS-C", None),
        ("a", "t1", at(81), 660.0, "ADS-C", None),
        ("a", "t1", at(121), 990.0, "ADS-B", "DLH401"),
        # An ADS-C report after the last ADS-B report of its aircraft.
        ("b", "t2", at(0), 0.0, "ADS-B", ""),
        ("b", "t2", at(30), 250.0, "ADS-C", None),
    ]
    return spark.createDataFrame(rows, TRACK_SCHEMA)


def test_track_summaries(merged_tracks):
    s = ae.track_summaries(merged_tracks).toPandas().set_index("track_id")
    assert s.loc["t1", "reports"] == 5 and s.loc["t1", "adsc_reports"] == 2
    assert s.loc["t1", "max_gap_s"] == 40 * 60
    # Three gaps of 40 min: the two ADS-C reports shorten the hole of 120 min
    # but each remaining gap is still over 30 min.
    assert s.loc["t1", "holes"] == 3
    assert s.loc["t1", "distance_nm"] == 990.0
    assert s.loc["t1", "callsigns"] == 2 and s.loc["t2", "callsigns"] == 0


def test_track_summaries_without_a_source_column(merged_tracks):
    base = merged_tracks.where("source = 'ADS-B'").drop("source")
    s = ae.track_summaries(base).toPandas().set_index("track_id")
    assert s.loc["t1", "adsc_reports"] == 0
    assert s.loc["t1", "max_gap_s"] == 120 * 60


def test_track_gaps_keep_gaps_from_five_minutes(merged_tracks):
    g = ae.track_gaps(merged_tracks).toPandas()
    assert sorted(g["gap_s"]) == [1800.0, 2400.0, 2400.0, 2400.0]


def test_adsc_positions(merged_tracks):
    p = ae.adsc_positions(merged_tracks).toPandas().sort_values(["icao24", "event_time"])
    p["position"] = ae.adsc_position(p["gap_before_s"], p["gap_after_s"])
    assert list(p["position"]) == ["in_hole", "in_hole", "after_last_adsb"]


def test_flight_measures_sum_the_pieces(spark, merged_tracks):
    truth = pd.DataFrame(
        {
            "flight_key": [0],
            "icao24": ["a"],
            "t_off": [pd.Timestamp(at(-10))],
            "t_on": [pd.Timestamp(at(200))],
        }
    )
    m = ae.flight_measures(spark, merged_tracks, truth).toPandas().iloc[0]
    assert m["reports"] == 5 and m["adsc_reports"] == 2
    assert m["distance_nm"] == 990.0
    assert m["holes"] == 3


def test_events_are_paired_by_aircraft_type_and_nearest_time(spark, merged_tracks):
    ev_schema = "id string, flight_id string, type string, event_time timestamp"
    base = spark.createDataFrame(
        [
            ("b1", "t1", "level-start", at(1)),
            ("b2", "t1", "xing-fir", at(121)),
            ("b3", "t2", "first_seen", at(0)),
        ],
        ev_schema,
    )
    merged = spark.createDataFrame(
        [
            ("m1", "t1", "level-start", at(1)),
            ("m2", "t1", "xing-fir", at(81)),
            ("m3", "t1", "xing-fir", at(41)),
        ],
        ev_schema,
    )
    icao = merged_tracks.select("track_id", "icao24").distinct()
    base_gaps = ae.track_gaps(merged_tracks.where("source = 'ADS-B'").drop("source"))
    holes = ae.base_holes_frame(base_gaps)
    m = ae.match_events(base, merged, icao, holes).toPandas()
    by = m.groupby(["type", "status"]).size().to_dict()
    assert by[("level-start", "same_time")] == 1
    # b2 (121 min) pairs with its nearest, m2 (81 min); m3 is left over.
    assert by[("xing-fir", "moved")] == 1 and by[("xing-fir", "only_merged")] == 1
    assert by[("first_seen", "only_base")] == 1
    moved = m[m["status"] == "moved"].iloc[0]
    assert moved["shift_s"] == -40 * 60
    left = m[m["status"] == "only_merged"].iloc[0]
    assert bool(left["in_base_hole"]) is True


def test_flag_holes_is_strictly_inside(spark):
    holes = spark.createDataFrame(
        [("a", 100.0, 200.0)], "icao24 string, hole_start double, hole_end double"
    )
    df = spark.createDataFrame(
        [("a", 100.0), ("a", 150.0), ("b", 150.0)], "icao24 string, t double"
    )
    f = ae.flag_holes(df, holes, "t", "h").toPandas().sort_values(["icao24", "t"])
    assert list(f["h"]) == [False, True, False]
