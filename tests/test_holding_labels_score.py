"""The holding rule scored against the Olive et al. 2019 label set."""

import datetime as dt

import pandas as pd
import pytest

import holding_labels_score as hs

T0 = pd.Timestamp("2019-10-01 08:00:00", tz="UTC")


def _traj(n=10, flight_id="BAW123_7"):
    return pd.DataFrame({
        "timestamp": [T0 + pd.Timedelta(seconds=i) for i in range(n)],
        "icao24": ["4ca123"] * n, "callsign": ["BAW123"] * n,
        "latitude": [51.0 + 0.001 * i for i in range(n)], "longitude": [0.0] * n,
        "altitude": [10000.0] * n, "groundspeed": [250.0] * n, "track": [90.0] * n,
        "vertical_rate": [-500.0] * n, "flight_id": [flight_id] * n,
    })


def test_as_track_frame_converts_units_and_thins_to_five_seconds(spark):
    sdf = hs.as_track_frame(spark, _traj(10), elevation_ft=83.0, step_s=5.0)
    rows = sdf.orderBy("event_time").collect()
    assert [r.event_time.second for r in rows] == [0, 5]
    # A 4 s cadence keeps one sample per 5 s bin, not one per 20 s.
    four = _traj(10)
    four["timestamp"] = [T0 + pd.Timedelta(seconds=4 * i) for i in range(10)]
    kept = hs.as_track_frame(spark, four, elevation_ft=83.0, step_s=5.0).orderBy("event_time").collect()
    assert [r.event_time.second for r in kept] == [0, 8, 12, 16, 20, 28, 32, 36]
    assert rows[0].track_id == "BAW123_7"
    assert rows[0].baro_altitude_c == pytest.approx(10000.0 / 3.28084)
    assert rows[0].velocity == pytest.approx(250.0 / 1.94384)
    assert rows[0].vert_rate == pytest.approx(-500.0 / 196.850394)
    assert rows[0].heading == 90.0
    assert rows[0].elev_ades_ft == 83.0
    assert "cumulative_distance_nm" in sdf.columns and "piece" in sdf.columns


def test_match_labels_joins_on_callsign_and_time_overlap():
    traj = pd.DataFrame({
        "flight_id": ["BAW123_7", "BAW123_9"], "callsign": ["BAW123", "BAW123"],
        "start": [T0, T0 + pd.Timedelta(hours=6)],
        "stop": [T0 + pd.Timedelta(minutes=40), T0 + pd.Timedelta(hours=6, minutes=40)],
    })
    labels = pd.DataFrame({
        "flight_id": ["BAW123_010", "BAW123_011"],
        "start": [T0 + pd.Timedelta(minutes=2), T0 + pd.Timedelta(hours=6, minutes=3)],
        "stop": [T0 + pd.Timedelta(minutes=8), T0 + pd.Timedelta(hours=6, minutes=9)],
        "airport": ["egll", "egll"], "hp": [True, False],
    })
    out = hs.match_labels(labels, traj)
    assert dict(zip(out["flight_id"], out["track_id"])) == {"BAW123_010": "BAW123_7", "BAW123_011": "BAW123_9"}


def test_windows_from_episodes_marks_overlapping_windows():
    labels = pd.DataFrame({
        "flight_id": ["f"] * 3, "track_id": ["t"] * 3,
        "start": [T0, T0 + pd.Timedelta(minutes=2), T0 + pd.Timedelta(minutes=4)],
        "stop": [T0 + pd.Timedelta(minutes=6), T0 + pd.Timedelta(minutes=8), T0 + pd.Timedelta(minutes=10)],
        "airport": ["egll"] * 3, "hp": [False, True, True],
    })
    episodes = pd.DataFrame({"track_id": ["t"], "start": [T0 + pd.Timedelta(minutes=7)],
                             "stop": [T0 + pd.Timedelta(minutes=12)]})
    # Window 1 (2..8 min) is covered for 1 min only, window 2 (4..10) for 3 min.
    assert list(hs.windows_from_episodes(labels, episodes)["hp_c"]) == [False, False, True]
    assert list(hs.windows_from_episodes(labels, episodes, min_overlap_s=0.0)["hp_c"]) == [False, True, True]


def test_score_is_time_weighted_per_flight():
    # One flight, a 600 s truth episode and a 480 s detected one inside it.
    labels = pd.DataFrame({
        "flight_id": ["f"] * 5, "track_id": ["t"] * 5, "airport": ["egll"] * 5,
        "start": [T0 + pd.Timedelta(minutes=2 * i) for i in range(5)],
        "stop": [T0 + pd.Timedelta(minutes=2 * i + 6) for i in range(5)],
        "hp": [True, True, True, False, False],
        "hp_c": [True, True, False, False, False],
    })
    s = hs.score(labels)
    # truth: windows 0..2 -> 0..10 min; detected: windows 0..1 -> 0..8 min
    assert s["precision"] == pytest.approx(1.0)
    assert s["recall"] == pytest.approx(0.8)
    assert s["iou"] == pytest.approx(0.8)
    assert s["n_flights"] == 1 and s["n_windows"] == 5 and s["n_positive"] == 3


def test_episodes_from_windows_reproduces_the_label_convention():
    labels = pd.DataFrame({
        "flight_id": ["f"] * 4,
        "start": [T0 + pd.Timedelta(minutes=2 * i) for i in range(4)],
        "stop": [T0 + pd.Timedelta(minutes=2 * i + 6) for i in range(4)],
        "x": [True, False, True, True],
    })
    eps = hs.episodes_from_windows(labels, "x")
    assert len(eps) == 2
    assert eps.iloc[1]["start"] == T0 + pd.Timedelta(minutes=4)
    assert eps.iloc[1]["stop"] == T0 + pd.Timedelta(minutes=12)
