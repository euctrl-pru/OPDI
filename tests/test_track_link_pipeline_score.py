"""Scoring one sampled day of both arms: the flight list, detectors, and what else moved.

Tracks here are flights on the equator at 480 kt with one sample a minute, so the
distance over a hole is known exactly from its length.
"""
import datetime as dt
import math
from dataclasses import replace

import events_gt
import pandas as pd
import pytest
from conftest import _EPOCH, make_track
from pyspark.sql import functions as F

import track_link_pipeline_score as s
from opdi.pipeline.flights import EARTH_R_NM
from opdi.pipeline.segmentation import SegmentationParams, assign_track_id
from opdi.pipeline.segmentation.base import KT_PER_MPS
from opdi.pipeline.segmentation.methods import cruise_linked, debounced

#: These tests were written at the pre-study settings (the grid's base), so they pass
#: them explicitly; the shipped defaults are pinned in test_segmentation_cruise_linked.
P = SegmentationParams(
    link_cruise_floor_ft=25000.0,
    link_min_implied_speed_kt=315.0,
    link_max_gap_minutes=360.0,
    link_old_edge_climb=False,
    link_new_edge_descent=False,
)
CRUISE_KT = 480.0
CRUISE_M = 11000.0
HOUR = 3600


def _at(seconds):
    return _EPOCH + dt.timedelta(seconds=seconds)


def _lon(nm):
    return math.degrees(nm / EARTH_R_NM)


def _cruise(spans, icao24="abc123", callsign="DLH1", track_id="T1"):
    """Samples a minute apart at cruise, over ``(start_s, end_s)`` spans, moving east."""
    return [
        {
            "t": t,
            "icao24": icao24,
            "callsign": callsign,
            "track_id": track_id,
            "lat": 0.0,
            "lon": _lon(t / 3600.0 * CRUISE_KT),
            "baro_altitude": CRUISE_M,
            "velocity": CRUISE_KT / KT_PER_MPS,
            "vert_rate": 0.0,
        }
        for start, end in spans
        for t in range(start, end + 1, 60)
    ]


def _holes(spark, *holes):
    """``(track_id, icao24, old_s, new_s)`` rows in the shape of ``linked_holes``."""
    return spark.createDataFrame(
        [
            (track, icao24, _at(old), _at(new), (new - old) / 60.0)
            for track, icao24, old, new in holes
        ],
        "track_id string, icao24 string, t_old_edge timestamp, t_new_edge timestamp, "
        "gap_min double",
    )


def _events(spark, *rows):
    """``(track_id, type, seconds)`` rows in the shape of ``opdi_flight_events``."""
    return spark.createDataFrame(
        [(f"{track}|{kind}|{t}", track, kind, _at(t)) for track, kind, t in rows],
        "id string, flight_id string, type string, event_time timestamp",
    )


def _flights(spark, *rows):
    """``(key, icao24, off_s, land_s)`` rows in the shape of ``scored_flights_frame``."""
    return spark.createDataFrame(
        [(key, icao24, _at(off), _at(land)) for key, icao24, off, land in rows],
        "flight_key long, icao24 string, t_off timestamp, t_land timestamp",
    )


def _family(rows, name):
    return next(r for r in rows if r["family"] == name)


# --- who is scored ----------------------------------------------------------


def test_flights_inside_the_day_only():
    truth = pd.DataFrame(
        {
            "flight_key": [0, 1, 2, 3],
            "t_off": pd.to_datetime(
                ["2026-06-14 01:00:00", "2026-06-14 23:00:00", "2026-06-13 23:30:00", "2026-06-14 23:59:59"]
            ),
            "t_on": pd.to_datetime(
                ["2026-06-14 09:00:00", "2026-06-15 01:00:00", "2026-06-14 02:00:00", "2026-06-14 23:59:59"]
            ),
        }
    )
    assert s.inside_the_day(truth, "2026-06-14")["flight_key"].tolist() == [0, 3]


def test_a_truth_flight_becomes_a_frame_in_the_sessions_utc(spark):
    truth = pd.DataFrame(
        {
            "flight_key": [7],
            "icao24": ["abc123"],
            "t_off": pd.to_datetime(["2026-06-14 01:02:03"]),
            "t_on": pd.to_datetime(["2026-06-14 09:00:00"]),
        }
    )
    row = (
        s.scored_flights_frame(spark, truth)
        .select(F.date_format("t_off", "yyyy-MM-dd HH:mm:ss").alias("off"), "flight_key", "icao24")
        .first()
    )
    assert (row["off"], row["flight_key"], row["icao24"]) == ("2026-06-14 01:02:03", 7, "abc123")


def test_a_linked_hole_is_a_gap_over_the_threshold_inside_one_track(spark):
    tracks = make_track(
        spark,
        [{"t": t, "track_id": "T1"} for t in (0, 30 * 60, 30 * 60 + 31 * 60)],
    ).union(make_track(spark, [{"t": 10 * HOUR, "track_id": "T2"}]))
    # 30 min exactly is not over the threshold; 31 min is. T2 starts hours later, but
    # the gap to it is between two tracks, which is no hole in either.
    rows = s.linked_holes(tracks, P).collect()
    assert [(r["track_id"], r["gap_min"]) for r in rows] == [("T1", 31.0)]
    assert rows[0]["t_old_edge"] == _at(30 * 60)
    assert rows[0]["t_new_edge"] == _at(30 * 60 + 31 * 60)


def test_only_holes_inside_a_scored_flight_are_scored(spark):
    holes = _holes(
        spark,
        ("T1", "abc123", HOUR, 3 * HOUR),  # inside the flight
        ("T2", "abc123", 4 * HOUR, 6 * HOUR),  # the edge is after it lands
        ("T3", "fff000", HOUR, 3 * HOUR),  # another airframe's flight
    )
    flights = _flights(spark, (0, "abc123", 0, 5 * HOUR))
    rows = s.scored_holes(holes, flights).collect()
    assert [r["track_id"] for r in rows] == ["T1"]


def test_a_hole_inside_two_overlapping_flights_is_one_hole(spark):
    holes = _holes(spark, ("T1", "abc123", HOUR, 3 * HOUR))
    flights = _flights(spark, (0, "abc123", 0, 4 * HOUR), (1, "abc123", 0, 5 * HOUR))
    assert s.scored_holes(holes, flights).count() == 1


# --- the flight list --------------------------------------------------------


def test_the_flight_list_is_paired_with_each_arm_named_in_plain_words():
    truth = pd.DataFrame(
        {
            "flight_key": [0],
            "icao24": ["aaa"],
            "callsign": ["DLH1"],
            "gt_adep": ["KJFK"],
            "gt_ades": ["EDDF"],
            "t_off": pd.to_datetime(["2026-06-14 01:00"]),
            "t_on": pd.to_datetime(["2026-06-14 09:00"]),
            "flow": ["outside_to_europe"],
        }
    )
    columns = ["icao24", "callsign", "adep", "ades", "first_seen", "last_seen"]
    split = pd.DataFrame(
        [
            ("aaa", "DLH1", "KJFK", None, pd.Timestamp("2026-06-14 01:10"), pd.Timestamp("2026-06-14 03:00")),
            ("aaa", "DLH1", None, "EDDF", pd.Timestamp("2026-06-14 06:00"), pd.Timestamp("2026-06-14 08:50")),
        ],
        columns=columns,
    )
    linked = pd.DataFrame(
        [("aaa", "DLH1", "KJFK", "EDDF", pd.Timestamp("2026-06-14 01:10"), pd.Timestamp("2026-06-14 08:50"))],
        columns=columns,
    )
    pairs, both = s.score_flight_list(truth, split, linked)

    by_flow = {r["flow"]: r for r in both.to_dict("records")}
    assert set(by_flow) == {"outside_to_europe", "crosses_the_box"}
    for flow in by_flow.values():
        assert flow["debounced_both_right"] == 0
        assert flow["cruise_linked_both_right"] == 1
        assert flow["debounced_rows_per_flight"] == 2.0
        assert flow["cruise_linked_rows_per_flight"] == 1.0
    assert not [c for c in both.columns if "base" in c or "merged" in c]
    assert not [c for c in pairs.columns if "base" in c or "merged" in c]
    # Each end read from its own row, so the split flight is right on both sides here.
    callsign = pairs[(pairs["view"] == "callsign") & (pairs["side"] == "ades")].iloc[0]
    assert callsign["debounced_right"] == 1
    assert callsign["cruise_linked_right"] == 1


def test_the_box_crossing_row_leaves_out_flights_that_do_not_cross():
    truth = pd.DataFrame(
        {
            "flight_key": [0, 1],
            "icao24": ["aaa", "bbb"],
            "callsign": ["A", "B"],
            "gt_adep": ["EDDF", "EDDF"],
            "gt_ades": ["KJFK", "EGLL"],
            "t_off": pd.to_datetime(["2026-06-14 01:00", "2026-06-14 01:00"]),
            "t_on": pd.to_datetime(["2026-06-14 09:00", "2026-06-14 02:00"]),
            "flow": ["europe_to_outside", "intra_europe"],
        }
    )
    empty = pd.DataFrame(
        {
            "icao24": pd.Series(dtype="object"),
            "callsign": pd.Series(dtype="object"),
            "adep": pd.Series(dtype="object"),
            "ades": pd.Series(dtype="object"),
            "first_seen": pd.Series(dtype="datetime64[ns]"),
            "last_seen": pd.Series(dtype="datetime64[ns]"),
        }
    )
    _, both = s.score_flight_list(truth, empty, empty)
    assert both.set_index("flow").loc["crosses_the_box", "n"] == 1


# --- detector damage --------------------------------------------------------


def test_events_are_compared_on_linked_airframes_inside_scored_flights(spark):
    debounced_tracks = spark.createDataFrame(
        [("D1", "abc123"), ("D2", "abc123"), ("D3", "fff000")], "track_id string, icao24 string"
    )
    linked_tracks = spark.createDataFrame(
        [("L1", "abc123"), ("L3", "fff000")], "track_id string, icao24 string"
    )
    debounced_events = _events(
        spark,
        ("D1", "level-start", HOUR),
        ("D2", "level-start", 3 * HOUR),
        ("D3", "level-start", HOUR),  # no linked track on this airframe
        ("D2", "level-start", 20 * HOUR),  # outside every scored flight
    )
    linked_events = _events(
        spark,
        ("L1", "level-start", HOUR),
        ("L1", "ATOT", HOUR),
        ("L3", "level-start", HOUR),
    )
    holes = _holes(spark, ("L1", "abc123", 2 * HOUR, 3 * HOUR))
    flights = _flights(spark, (0, "abc123", 0, 5 * HOUR), (1, "fff000", 0, 5 * HOUR))

    rows = s.compare_events(debounced_events, debounced_tracks, linked_events, linked_tracks, holes, flights)

    assert rows == [
        {"type": "ATOT", "debounced_events": 0, "cruise_linked_events": 1, "change": 1},
        {"type": "level-start", "debounced_events": 2, "cruise_linked_events": 1, "change": -1},
    ]


def test_an_event_in_two_flights_slack_counts_once(spark):
    tracks = spark.createDataFrame([("L1", "abc123")], "track_id string, icao24 string")
    events = _events(spark, ("L1", "first_seen", 2 * HOUR))
    holes = _holes(spark, ("L1", "abc123", 10, 20))
    # The event is ten minutes outside each flight, so within the slack of both.
    flights = _flights(
        spark,
        (0, "abc123", 0, 2 * HOUR - 600),
        (1, "abc123", 2 * HOUR + 600, 4 * HOUR),
    )
    rows = s.compare_events(events, tracks, events, tracks, holes, flights)
    assert rows == [{"type": "first_seen", "debounced_events": 1, "cruise_linked_events": 1, "change": 0}]


def test_a_level_segment_spanning_a_hole_is_counted(spark):
    holes = _holes(
        spark,
        ("T1", "abc123", 2 * HOUR, 4 * HOUR),
        ("T2", "abc123", 2 * HOUR, 4 * HOUR),
    )
    events = _events(
        spark,
        # T1: one segment open across the hole.
        ("T1", "level-start", HOUR),
        ("T1", "level-end", 5 * HOUR),
        # T2: a segment closes before the hole and another opens after it.
        ("T2", "level-start", HOUR),
        ("T2", "level-end", HOUR + 600),
        ("T2", "level-start", 5 * HOUR),
        ("T2", "level-end", 6 * HOUR),
    )
    row = _family(s.level_across_holes(events, holes), "level")
    assert row["segments_across_a_hole"] == 1
    assert row["holes_inside_a_segment"] == 1
    assert row["scored_holes"] == 2
    assert row["scored_tracks"] == 2
    assert row["tracks_with_a_segment_across_a_hole"] == 1
    assert row["share_of_tracks"] == 0.5


def test_a_start_before_and_an_end_after_are_not_enough(spark):
    # The naive test, any start before and any end after, would count this one.
    holes = _holes(spark, ("T1", "abc123", 2 * HOUR, 4 * HOUR))
    events = _events(
        spark,
        ("T1", "level-start", HOUR),
        ("T1", "level-end", HOUR + 600),
        ("T1", "level-start", 5 * HOUR),
        ("T1", "level-end", 6 * HOUR),
    )
    assert _family(s.level_across_holes(events, holes), "level")["segments_across_a_hole"] == 0


@pytest.mark.parametrize(
    "events,expected",
    [
        # A start on the old edge and an end on the new edge span it.
        ([("level-start", 2 * HOUR), ("level-end", 4 * HOUR)], 1),
        # A one-sample segment on the old edge closes where it opens.
        ([("level-start", 2 * HOUR), ("level-end", 2 * HOUR), ("level-end", 4 * HOUR)], 0),
        # A one-sample segment on the new edge opens where it closes.
        ([("level-start", 2 * HOUR), ("level-start", 4 * HOUR), ("level-end", 4 * HOUR)], 0),
        # Nothing before the hole, or nothing after it.
        ([("level-end", 4 * HOUR)], 0),
        ([("level-start", 2 * HOUR)], 0),
    ],
)
def test_events_on_the_edges_of_a_hole(spark, events, expected):
    holes = _holes(spark, ("T1", "abc123", 2 * HOUR, 4 * HOUR))
    frame = _events(spark, *[("T1", kind, t) for kind, t in events])
    assert _family(s.level_across_holes(frame, holes), "level")["segments_across_a_hole"] == expected


def test_one_segment_across_two_holes_is_one_segment_and_two_holes(spark):
    holes = _holes(
        spark,
        ("T1", "abc123", 2 * HOUR, 3 * HOUR),
        ("T1", "abc123", 4 * HOUR, 5 * HOUR),
    )
    events = _events(spark, ("T1", "level-start", HOUR), ("T1", "level-end", 6 * HOUR))
    row = _family(s.level_across_holes(events, holes), "level")
    assert row["segments_across_a_hole"] == 1
    assert row["holes_inside_a_segment"] == 2
    assert row["share_of_tracks"] == 1.0


def test_the_level_off_family_is_reported_on_its_own_row(spark):
    holes = _holes(spark, ("T1", "abc123", 2 * HOUR, 4 * HOUR))
    events = _events(
        spark,
        ("T1", "level-off-climb-start", HOUR),
        ("T1", "level-off-climb-end", 5 * HOUR),
        ("T1", "level-start", HOUR),
        ("T1", "level-end", HOUR + 60),
    )
    rows = s.level_across_holes(events, holes)
    assert _family(rows, "level_off")["segments_across_a_hole"] == 1
    assert _family(rows, "level")["segments_across_a_hole"] == 0


def test_the_any_row_counts_a_track_damaged_in_either_family_once(spark):
    holes = _holes(
        spark,
        ("T1", "abc123", 2 * HOUR, 4 * HOUR),
        ("T2", "abc123", 2 * HOUR, 4 * HOUR),
        ("T3", "abc123", 2 * HOUR, 4 * HOUR),
        ("T4", "abc123", 2 * HOUR, 4 * HOUR),
    )
    events = _events(
        spark,
        # T1 in one family, T2 in the other, T3 in both, T4 in neither.
        ("T1", "level-start", HOUR),
        ("T1", "level-end", 5 * HOUR),
        ("T2", "level-off-climb-start", HOUR),
        ("T2", "level-off-climb-end", 5 * HOUR),
        ("T3", "level-start", HOUR),
        ("T3", "level-end", 5 * HOUR),
        ("T3", "level-off-climb-start", HOUR),
        ("T3", "level-off-climb-end", 5 * HOUR),
    )
    rows = s.level_across_holes(events, holes)

    assert _family(rows, "level")["tracks_with_a_segment_across_a_hole"] == 2
    assert _family(rows, "level_off")["tracks_with_a_segment_across_a_hole"] == 2
    row = _family(rows, "any")
    assert row["scored_holes"] == 4
    assert row["scored_tracks"] == 4
    assert row["segments_across_a_hole"] == 4
    assert row["holes_inside_a_segment"] == 3
    assert row["tracks_with_a_segment_across_a_hole"] == 3
    assert row["share_of_tracks"] == 0.75


def test_with_no_holes_the_share_is_empty_rather_than_a_division_error(spark):
    holes = _holes(spark, ("T1", "abc123", HOUR, 2 * HOUR)).limit(0)
    events = _events(spark, ("T1", "level-start", HOUR))
    row = _family(s.level_across_holes(events, holes), "level")
    assert row["scored_holes"] == 0
    assert row["share_of_tracks"] is None


def test_distance_across_a_hole_is_compared_to_the_great_circle(spark):
    tracks = make_track(spark, _cruise([(0, 600), (2 * HOUR, 2 * HOUR + 600)]))
    holes = s.linked_holes(tracks, P)
    rows = s.distance_across_holes(tracks, holes).collect()

    assert len(rows) == 1
    row = rows[0]
    # 480 kt over the time from the old edge to the new one, on the equator.
    expected = (2 * HOUR - 600) / 3600.0 * CRUISE_KT
    assert row["great_circle_nm"] == pytest.approx(expected, rel=1e-6)
    assert row["jump_nm"] == pytest.approx(expected, rel=1e-6)
    # The two formulas round the Earth's radius differently, by about 2e-7 of it.
    assert row["difference_nm"] == pytest.approx(0.0, abs=1e-3)
    assert row["edge_masked"] is False


def test_a_hole_whose_distance_was_lost_shows_as_a_difference(spark):
    # The cleaner NULLs a position and keeps the row. The pipeline's segment distance
    # is zero wherever either end is NULL, so a masked edge loses the whole hole, and
    # that has to come out of this comparison rather than be skipped by it.
    samples = _cruise([(0, 600), (2 * HOUR, 2 * HOUR + 600)])
    for sample in samples:
        if sample["t"] == 2 * HOUR:
            sample["lat"] = None
            sample["lon"] = None
    tracks = make_track(spark, samples)
    holes = s.linked_holes(tracks, P)
    # The released distance (distance_v0.0.2), which is where the loss was.
    rows = s.distance_across_holes(tracks, holes, last_known_position=False).collect()

    assert len(rows) == 1
    row = rows[0]
    expected = (2 * HOUR + 60 - 600) / 3600.0 * CRUISE_KT
    assert row["great_circle_nm"] == pytest.approx(expected, rel=1e-6)
    assert row["jump_nm"] == pytest.approx(0.0, abs=1e-6)
    assert row["difference_nm"] == pytest.approx(-expected, rel=1e-6)
    assert row["edge_masked"] is True

    # The distance the pipeline computes now reads the last known position.
    row = s.distance_across_holes(tracks, holes).first()
    assert row["jump_nm"] == pytest.approx(expected, rel=1e-6)
    assert row["difference_nm"] == pytest.approx(0.0, abs=1e-3)
    assert row["edge_masked"] is True


def test_a_twin_sample_on_the_new_edge_still_gives_one_row_per_hole(spark):
    samples = _cruise([(0, 600), (2 * HOUR, 2 * HOUR + 600)])
    samples.append(dict(next(x for x in samples if x["t"] == 2 * HOUR)))
    tracks = make_track(spark, samples)
    assert s.distance_across_holes(tracks, s.linked_holes(tracks, P)).count() == 1


def test_the_distance_summary_reads_median_and_p95_of_the_difference():
    rows = pd.DataFrame(
        {
            "difference_nm": [float(x) for x in range(-1, 100)],
            "edge_masked": [True] + [False] * 100,
        }
    )
    got = s.summarise_distance(rows, holes_scored=102)
    assert got["holes_scored"] == 102
    assert got["holes_compared"] == 101
    assert got["holes_with_no_position_on_one_side"] == 1
    assert got["median_difference_nm"] == 49.0
    assert got["p95_difference_nm"] == 94.0
    assert got["p95_absolute_difference_nm"] == 94.0
    assert got["holes_off_by_over_1_nm"] == 98
    assert got["holes_with_a_masked_edge"] == 1


def test_the_distance_summary_of_nothing_is_empty():
    got = s.summarise_distance(
        pd.DataFrame({"difference_nm": pd.Series(dtype=float), "edge_masked": pd.Series(dtype=bool)}),
        holes_scored=0,
    )
    assert got["holes_compared"] == 0
    assert got["median_difference_nm"] is None


def test_the_stored_distance_is_checked_against_the_recomputed_one(spark):
    tracks = make_track(spark, _cruise([(0, 600), (2 * HOUR, 2 * HOUR + 600)]))
    flown = (2 * HOUR + 600) / 3600.0 * CRUISE_KT
    events = _events(spark, ("T1", "last_seen", 2 * HOUR + 600), ("T1", "first_seen", 0))
    event_id = f"T1|last_seen|{2 * HOUR + 600}"
    measurements = spark.createDataFrame(
        [
            (event_id, "Distance flown (NM)", flown),
            (event_id, "Time Passed (s)", 1.0),
            ("T1|first_seen|0", "Distance flown (NM)", 0.0),
        ],
        "milestone_id string, type string, value double",
    )
    good = s.stored_distance_check(tracks, events, measurements)
    assert good == {
        "scored_tracks": 1,
        "tracks_checked_against_stored_distance": 1,
        "tracks_off_stored_distance_by_over_1_nm": 0,
        "stored_distance_verdict": "agrees",
    }

    wrong = spark.createDataFrame(
        [(event_id, "Distance flown (NM)", flown - 500.0)],
        "milestone_id string, type string, value double",
    )
    bad = s.stored_distance_check(tracks, events, wrong)
    assert bad["tracks_off_stored_distance_by_over_1_nm"] == 1
    assert bad["stored_distance_verdict"] == "differs"


def test_no_track_checked_against_the_stored_distance_does_not_read_as_clean(spark):
    tracks = make_track(spark, _cruise([(0, 600), (2 * HOUR, 2 * HOUR + 600)]))
    events = _events(spark, ("T1", "first_seen", 0))
    nothing = spark.createDataFrame([], "milestone_id string, type string, value double")

    got = s.stored_distance_check(tracks, events, nothing)

    assert got == {
        "scored_tracks": 1,
        "tracks_checked_against_stored_distance": 0,
        "tracks_off_stored_distance_by_over_1_nm": None,
        "stored_distance_verdict": "not_checked",
    }


def test_some_tracks_unchecked_is_said_even_when_those_checked_agree(spark):
    tracks = make_track(spark, _cruise([(0, 600)])).union(
        make_track(spark, _cruise([(0, 600)], track_id="T2"))
    )
    flown = 600 / 3600.0 * CRUISE_KT
    events = _events(spark, ("T1", "last_seen", 600))
    stored = spark.createDataFrame(
        [("T1|last_seen|600", "Distance flown (NM)", flown)],
        "milestone_id string, type string, value double",
    )
    got = s.stored_distance_check(tracks, events, stored)
    assert got["scored_tracks"] == 2
    assert got["tracks_checked_against_stored_distance"] == 1
    assert got["stored_distance_verdict"] == "some_tracks_not_checked"


# --- nothing else moved -----------------------------------------------------


def _both_rules(frame):
    return assign_track_id(frame, debounced(), P), assign_track_id(frame, cruise_linked(), P)


def test_a_debounced_boundary_missing_from_a10_must_be_a_linked_hole(spark):
    samples = make_track(spark, _cruise([(0, 40 * 60), (2 * HOUR, 2 * HOUR + 40 * 60)]))
    split, linked = _both_rules(samples)

    got = s.partition_check(samples, split, linked, P)

    assert got["debounced_boundaries"] == 1
    assert got["cruise_linked_boundaries"] == 0
    assert got["removed_boundaries"] == 1
    assert got["removed_at_a_linked_hole"] == 1
    assert got["removed_elsewhere"] == 0
    assert got["added_boundaries"] == 0
    assert got["passed"] is True


def test_a_boundary_removed_where_the_rule_would_not_link_fails_the_check(spark):
    # A callsign change: A10 must keep this one, so losing it is a defect in the rule.
    first = _cruise([(0, 40 * 60)], callsign="DLH1")
    second = _cruise([(40 * 60 + 60, 80 * 60)], callsign="AFR2")
    samples = make_track(spark, first + second)
    split, _ = _both_rules(samples)
    glued = split.withColumn("track_id", F.lit("one_track"))

    got = s.partition_check(samples, split, glued, P)

    assert got["removed_boundaries"] == 1
    assert got["removed_at_a_linked_hole"] == 0
    assert got["removed_elsewhere"] == 1
    assert got["passed"] is False


def test_a_boundary_removed_at_a_hole_the_rule_rejects_fails_the_check(spark):
    # A real gap, but under the cruise floor: being a gap is not enough to excuse it.
    samples = _cruise([(0, 40 * 60), (2 * HOUR, 2 * HOUR + 40 * 60)])
    for sample in samples:
        sample["baro_altitude"] = 3000.0
    frame = make_track(spark, samples)
    split, linked = _both_rules(frame)
    assert s.partition_check(frame, split, linked, P)["passed"] is True

    got = s.partition_check(frame, split, split.withColumn("track_id", F.lit("one_track")), P)

    assert got["removed_boundaries"] == 1
    assert got["removed_elsewhere"] == 1
    assert got["passed"] is False


def test_a_boundary_that_moves_because_of_the_callsign_memory_is_named_as_such(spark):
    """Eleven hours of samples with no callsign, then a one-sample garble and the old
    callsign again. A9 remembers the old callsign and sees a flicker; A10 has forgotten
    it and breaks at the revert. That boundary is the memory's, not a defect."""
    samples = [{**s, "callsign": "DLH1"} for s in _cruise([(0, 30 * 60)])]
    samples += [{**s, "callsign": ""} for s in _cruise([(31 * 60, 11 * HOUR + 30 * 60)])]
    garble = 11 * HOUR + 31 * 60
    samples += [{**_cruise([(garble, garble)])[0], "callsign": "DLH7X"}]
    # The revert comes 5 s later, under the 30 s a callsign needs to persist.
    samples += [{**_cruise([(garble + 5, garble + 5)])[0], "callsign": "DLH1"}]
    samples += [{**s, "callsign": "DLH1"} for s in _cruise([(garble + 60, garble + 20 * 60)])]
    frame = make_track(spark, samples)
    split, linked = _both_rules(frame)

    got = s.partition_check(frame, split, linked, P)

    assert got["debounced_boundaries"] == 0
    assert got["added_boundaries"] == 1
    assert got["added_by_callsign_memory"] == 1
    assert got["added_elsewhere"] == 0
    assert got["passed"] is True


def test_a10_adding_a_boundary_fails_the_check(spark):
    samples = make_track(spark, _cruise([(0, 40 * 60)]))
    split, _ = _both_rules(samples)
    cut = F.when(F.col("event_time") >= F.lit(_at(20 * 60)), "second").otherwise("first")
    added = split.withColumn("track_id", cut)

    got = s.partition_check(samples, split, added, P)

    assert got["debounced_boundaries"] == 0
    assert got["added_boundaries"] == 1
    assert got["added_elsewhere"] == 1
    assert got["passed"] is False


def test_a_hole_that_can_be_one_flight_on_another_airframe_excuses_nothing(spark):
    # Boundaries are per airframe: the same instant on another airframe is not it.
    here = _cruise([(0, 40 * 60), (2 * HOUR, 2 * HOUR + 40 * 60)], icao24="fff000", callsign="AFR2")
    first = _cruise([(0, 2 * HOUR - 60)], callsign="DLH1")
    second = _cruise([(2 * HOUR, 2 * HOUR + 40 * 60)], callsign="BAW9")
    samples = make_track(spark, here + first + second)
    split, _ = _both_rules(samples)
    glued = split.withColumn(
        "track_id", F.when(F.col("icao24") == "abc123", "one").otherwise(F.col("track_id"))
    )

    got = s.partition_check(samples, split, glued, P)

    assert got["removed_elsewhere"] == 1
    assert got["passed"] is False


def _one_hole(gap_end=2 * HOUR):
    return _cruise([(0, 40 * 60), (gap_end, gap_end + 40 * 60)])


def test_the_arm_agrees_with_the_rule_it_ran(spark):
    samples = make_track(spark, _one_hole())
    arm = assign_track_id(samples, cruise_linked(), P)

    got = s.partition_row(samples, arm, P, "2024-06-01")

    assert got["arm_tracks"] == 1
    assert got["resegmented_tracks"] == 1
    assert got["arm_tracks_not_resegmented"] == 0
    assert got["resegmented_tracks_not_in_arm"] == 0
    assert got["passed"] is True


def test_a_flag_differing_from_the_arms_settings_fails_the_check(spark):
    # The arm linked an 80 min hole. Told the limit was 60 min, the two rules agree with
    # each other perfectly, and only the arm's own table can show which rule it ran.
    samples = make_track(spark, _one_hole())
    arm = assign_track_id(samples, cruise_linked(), P)
    flag = replace(P, link_max_gap_minutes=60.0)

    got = s.partition_row(samples, arm, flag, "2024-06-01")

    assert got["added_boundaries"] == 0
    assert got["removed_elsewhere"] == 0
    assert got["arm_tracks_not_resegmented"] == 1
    assert got["resegmented_tracks_not_in_arm"] == 2
    assert got["passed"] is False


def test_an_arm_stricter_than_the_flag_fails_the_check_too(spark):
    samples = make_track(spark, _one_hole())
    arm = assign_track_id(samples, cruise_linked(), replace(P, link_max_gap_minutes=60.0))

    got = s.partition_row(samples, arm, P, "2024-06-01")

    assert got["arm_tracks_not_resegmented"] == 2
    assert got["resegmented_tracks_not_in_arm"] == 1
    assert got["passed"] is False


def test_tracks_differing_only_after_midnight_are_seen(spark):
    # Both start on the day, so their starts agree. Only the end shows the difference.
    samples = make_track(spark, _one_hole())
    linked = assign_track_id(samples, cruise_linked(), P)
    cut_short = linked.filter(F.col("event_time") < F.lit(_at(2 * HOUR)))

    got = s.arm_agreement(cut_short, linked, "2024-06-01")

    assert got["arm_tracks_not_resegmented"] == 1
    assert got["resegmented_tracks_not_in_arm"] == 1


def test_only_tracks_starting_on_the_day_are_compared(spark):
    # Starts on 2024-06-02. The arm built 2024-06-01 and could never have claimed it.
    samples = make_track(spark, _cruise([(0, 600)]) + _cruise([(24 * HOUR, 24 * HOUR + 600)]))
    resegmented = assign_track_id(samples, debounced(), P)
    arm = resegmented.filter(F.col("event_time") < F.lit(_at(HOUR)))

    got = s.arm_agreement(arm, resegmented, "2024-06-01")

    assert got["arm_tracks"] == 1
    assert got["resegmented_tracks"] == 1
    assert got["arm_tracks_not_resegmented"] == 0
    assert got["resegmented_tracks_not_in_arm"] == 0


def test_the_input_is_cut_to_the_window_the_arm_read(spark):
    samples = make_track(spark, _cruise([(0, 600)]))
    # _EPOCH is noon on 2024-06-01. The lookback at the pre-study settings is 7 h.
    kept = s.arm_read_window(samples, "2024-06-01", P)
    assert kept.count() == samples.count()
    assert s.arm_read_window(samples, "2024-06-03", P).count() == 0
    assert s.arm_read_window(samples, "2024-05-29", P).count() == 0
    # Noon is 12 h before the next day: outside its 7 h lookback, inside a 13 h one.
    wider = replace(P, link_max_gap_minutes=720.0)
    assert s.arm_read_window(samples, "2024-06-02", P).count() == 0
    assert s.arm_read_window(samples, "2024-06-02", wider).count() == samples.count()


def test_movements_within_a_tenth_of_a_percent_pass():
    assert s.movement_row("all", 10000, 10000, 9990)["passed"] is True
    assert s.movement_row("all", 10000, 10000, 9989)["passed"] is False
    assert s.movement_row("all", 10000, 9000, 9500)["passed"] is True


def test_a_movement_row_says_how_far_the_count_fell():
    row = s.movement_row("ATOT", 2000, 1000, 998)
    assert row == {
        "movement": "ATOT",
        "apdf_movements": 2000,
        "debounced_matched": 1000,
        "cruise_linked_matched": 998,
        "fall_share": 0.002,
        "passed": False,
    }


def test_a_movement_row_with_nothing_matched_has_no_fall_to_report():
    row = s.movement_row("ATOT", 10, 0, 0)
    assert row["fall_share"] is None
    assert row["passed"] is True


def test_movements_are_counted_for_the_scored_flights_only(spark):
    day = dt.date(2026, 6, 14)
    aobt = dt.datetime(2026, 6, 14, 8, 0, 0)

    def truth_row(icao24, callsign, milestone, gt_aobt=aobt):
        return (icao24, callsign, day, gt_aobt, milestone, dt.datetime(2026, 6, 14, 9, 0, 0))

    apdf = spark.createDataFrame(
        [
            truth_row("abc123", "DLH1", "ATOT"),
            truth_row("abc123", "DLH1", "ALDT"),
            truth_row("abc123", "DLH1", "AOBT"),  # not a movement
            truth_row("fff000", "AFR2", "ATOT"),  # not a scored flight
            truth_row("abc123", "DLH1", "ATOT", dt.datetime(2026, 6, 14, 20, 0, 0)),  # another leg
        ],
        "icao24 string, callsign string, day date, gt_aobt timestamp, milestone string, "
        "gt_time timestamp",
    )
    scored = pd.DataFrame(
        {"icao24": ["abc123"], "callsign": ["DLH1"], "t_off": pd.to_datetime(["2026-06-14 08:00:00"])}
    )

    def detected(*milestones):
        return spark.createDataFrame(
            [("abc123", "DLH1", day, m, dt.datetime(2026, 6, 14, 9, 0, 5)) for m in milestones],
            "icao24 string, callsign string, day date, milestone string, event_time timestamp",
        )

    rows = s.movement_rows(
        s.scored_movements(spark, apdf, scored),
        detected("ATOT", "ALDT"),
        detected("ATOT"),
    )
    by_name = {r["movement"]: r for r in rows}
    assert set(by_name) == {"ATOT", "ALDT", "all"}
    assert by_name["all"]["apdf_movements"] == 2
    assert by_name["all"]["debounced_matched"] == 2
    assert by_name["all"]["cruise_linked_matched"] == 1
    assert by_name["all"]["passed"] is False
    assert by_name["ATOT"]["passed"] is True


def _movement_truth(spark, *rows):
    """``(milestone, off_block_s, movement_s)`` of airframe abc123, callsign DLH1."""
    return spark.createDataFrame(
        [("abc123", "DLH1", dt.date(2024, 6, 1), _at(aobt), m, _at(t)) for m, aobt, t in rows],
        "icao24 string, callsign string, day date, gt_aobt timestamp, milestone string, "
        "gt_time timestamp",
    )


def _detections(spark, *rows):
    """``(milestone, seconds)`` detections of airframe abc123, callsign DLH1."""
    return spark.createDataFrame(
        [("abc123", "DLH1", dt.date(2024, 6, 1), m, _at(t)) for m, t in rows],
        "icao24 string, callsign string, day date, milestone string, event_time timestamp",
    )


def test_a_lost_second_leg_of_one_callsign_is_seen(spark):
    # Keyed without off-block, these two legs are one truth row and either detection
    # answers for both, so the loss of a leg cannot show.
    truth = _movement_truth(spark, ("ATOT", 0, 600), ("ATOT", 6 * HOUR, 6 * HOUR + 600))
    both = _detections(spark, ("ATOT", 610), ("ATOT", 6 * HOUR + 610))
    first_only = _detections(spark, ("ATOT", 610))

    row = next(r for r in s.movement_rows(truth, both, first_only) if r["movement"] == "all")

    assert row["apdf_movements"] == 2
    assert row["debounced_matched"] == 2
    assert row["cruise_linked_matched"] == 1
    assert row["passed"] is False


def test_a_detection_hours_from_the_movement_is_not_a_match(spark):
    truth = _movement_truth(spark, ("ATOT", 0, 600), ("ALDT", 0, 2 * HOUR))
    near = _detections(spark, ("ATOT", 610), ("ALDT", 2 * HOUR + 10))
    far = _detections(spark, ("ATOT", 610), ("ALDT", 8 * HOUR))

    by_name = {r["movement"]: r for r in s.movement_rows(truth, near, far)}

    assert by_name["ALDT"]["debounced_matched"] == 1
    assert by_name["ALDT"]["cruise_linked_matched"] == 0
    assert by_name["ALDT"]["passed"] is False
    assert by_name["all"]["passed"] is False


def test_the_match_limit_is_inclusive_either_side_of_the_movement(spark):
    limit = int(s.MOVEMENT_MATCH_MINUTES * 60)
    truth = _movement_truth(spark, ("ATOT", 0, 2 * HOUR), ("ALDT", 0, 4 * HOUR))
    at_limit = _detections(spark, ("ATOT", 2 * HOUR - limit), ("ALDT", 4 * HOUR + limit))
    beyond = _detections(spark, ("ATOT", 2 * HOUR - limit - 1), ("ALDT", 4 * HOUR + limit))

    by_name = {r["movement"]: r for r in s.movement_rows(truth, at_limit, beyond)}

    assert by_name["ATOT"]["debounced_matched"] == 1
    assert by_name["ATOT"]["cruise_linked_matched"] == 0
    assert by_name["ALDT"]["cruise_linked_matched"] == 1


def test_several_detections_of_one_movement_count_once(spark):
    truth = _movement_truth(spark, ("ATOT", 0, 600))
    twice = _detections(spark, ("ATOT", 610), ("ATOT", 620))
    row = next(r for r in s.movement_rows(truth, twice, twice) if r["movement"] == "ATOT")
    assert row["apdf_movements"] == 1
    assert row["debounced_matched"] == 1


def test_provenance_names_every_truth_source_the_job_reads():
    tables = s._input_tables("202606")
    base = events_gt.REFERENCE_BASE
    assert f"{base}/flights_202606.parquet" in tables
    # Either can be read, as load_apdf falls back from one to the other.
    assert f"{base}/apdf_full_202606.parquet" in tables
    assert f"{base}/apdf_202606.parquet" in tables
    assert any(t.endswith("/oa_airports") for t in tables)
    assert any(t.endswith("/input/osn_statevectors_v2") for t in tables)


def test_a_join_that_matches_nothing_is_refused_rather_than_reported(spark):
    apdf = spark.createDataFrame(
        [("abc123", "DLH1", dt.date(2026, 6, 14), _at(0), "ATOT", _at(0))],
        "icao24 string, callsign string, day date, gt_aobt timestamp, milestone string, "
        "gt_time timestamp",
    )
    nothing = spark.createDataFrame(
        [], "icao24 string, callsign string, day date, milestone string, event_time timestamp"
    )
    with pytest.raises(SystemExit, match="identity-join"):
        s.movement_rows(apdf, nothing, nothing)


# --- the whole job over local frames ----------------------------------------


def test_the_job_writes_every_table_from_the_arms_tables(spark, tmp_path):
    day = "2024-06-01"
    samples = _cruise([(0, 40 * 60), (2 * HOUR, 2 * HOUR + 40 * 60)])
    raw = make_track(spark, samples)
    split, linked = _both_rules(raw)

    def clean(frame):
        return frame.withColumn("dof", F.to_date(F.lit(day)))

    def events_of(tracks):
        ids = [r["track_id"] for r in tracks.select("track_id").distinct().collect()]
        rows = []
        for t in ids:
            times = tracks.filter(F.col("track_id") == t).agg(
                F.min("event_time").alias("a"), F.max("event_time").alias("b")
            ).first()
            rows += [
                (f"{t}|level-start", t, "level-start", times["a"]),
                (f"{t}|level-end", t, "level-end", times["b"]),
                (f"{t}|last_seen", t, "last_seen", times["b"]),
            ]
        return spark.createDataFrame(
            rows, "id string, flight_id string, type string, event_time timestamp"
        ).withColumn("info", F.lit("{}")).withColumn("dof", F.to_date(F.lit(day)))

    def measurements_of(events):
        return events.select(
            F.col("id").alias("milestone_id"),
            F.lit("Distance flown (NM)").alias("type"),
            F.lit(0.0).alias("value"),
            "dof",
        )

    def flight_list_of(tracks, rows):
        """A row per track, in time order, as the pipeline writes one."""
        ids = [
            r["track_id"]
            for r in tracks.groupBy("track_id").agg(F.min("event_time").alias("a")).orderBy("a").collect()
        ]
        assert len(ids) == len(rows)
        return spark.createDataFrame(
            [(i, *row, "", "") for i, row in zip(ids, rows)],
            "ID string, ICAO24 string, FLT_ID string, ADEP string, ADES string, "
            "first_seen timestamp, last_seen timestamp, ADEP_P string, ADES_P string",
        ).withColumn("DOF", F.to_date(F.lit(day)))

    start, end = _at(0), _at(2 * HOUR + 40 * 60)
    tables = {
        ("debounced", "osn_statevectors_v2"): raw,
        ("debounced", "osn_tracks_clean"): clean(split),
        ("cruise_linked", "osn_tracks_clean"): clean(linked),
        ("debounced", "opdi_flight_events"): events_of(split),
        ("cruise_linked", "opdi_flight_events"): events_of(linked),
        ("cruise_linked", "opdi_measurements"): measurements_of(events_of(linked)),
        ("debounced", "opdi_flight_list"): flight_list_of(
            split,
            [
                ("abc123", "DLH1", "EDDF", None, start, _at(40 * 60)),
                ("abc123", "DLH1", None, "KJFK", _at(2 * HOUR), end),
            ],
        ),
        ("cruise_linked", "opdi_flight_list"): flight_list_of(
            linked, [("abc123", "DLH1", "EDDF", "KJFK", start, end)]
        ),
    }
    truth = pd.DataFrame(
        {
            "flight_key": [0],
            "icao24": ["abc123"],
            "callsign": ["DLH1"],
            "gt_adep": ["EDDF"],
            "gt_ades": ["KJFK"],
            "t_off": pd.to_datetime([_at(-600)]),
            "t_on": pd.to_datetime([_at(2 * HOUR + 50 * 60)]),
            "flow": ["europe_to_outside"],
        }
    )
    day_date = dt.date(2024, 6, 1)
    apdf = spark.createDataFrame(
        [("abc123", "DLH1", day_date, _at(-600), m, _at(0)) for m in ("ATOT", "ALDT")],
        "icao24 string, callsign string, day date, gt_aobt timestamp, milestone string, "
        "gt_time timestamp",
    )
    detection = spark.createDataFrame(
        [("abc123", "DLH1", day_date, m, _at(10)) for m in ("ATOT", "ALDT")],
        "icao24 string, callsign string, day date, milestone string, event_time timestamp",
    )

    nm_path = tmp_path / "flights.parquet"
    pd.DataFrame(
        {
            "AIRCRAFT_ADDRESS": ["ABC123"],
            "AOBT_3": pd.to_datetime([_at(-600)], utc=True),
            "ARVT_3": pd.to_datetime([_at(2 * HOUR + 50 * 60)], utc=True),
        }
    ).to_parquet(nm_path)

    written = {}

    def write(name, rows):
        written[name] = rows

    s.score_day(
        spark,
        day,
        read=lambda arm, table: s.one_day(tables[(arm, table)], table, day),
        truth=truth,
        p=P,
        apdf=apdf,
        detected={"debounced": detection, "cruise_linked": detection},
        write=write,
        truth_path=str(nm_path),
    )

    assert set(written) == set(s.OUTPUTS)
    assert list(pd.DataFrame(written["link_both_ends.csv"])["debounced_rows_per_flight"]) == [2.0, 2.0]
    assert list(pd.DataFrame(written["link_both_ends.csv"])["cruise_linked_both_right"]) == [1, 1]
    assert written["link_partition_check.csv"][0]["passed"] is True
    assert written["link_partition_check.csv"][0]["removed_at_a_linked_hole"] == 1
    assert written["link_partition_check.csv"][0]["arm_tracks"] == 1
    assert written["link_partition_check.csv"][0]["arm_tracks_not_resegmented"] == 0
    assert list(written)[0] == "link_partition_check.csv"
    assert _family(written["link_level_across_hole.csv"], "any")["share_of_tracks"] == 1.0
    level = _family(written["link_level_across_hole.csv"], "level")
    assert level["segments_across_a_hole"] == 1
    assert level["share_of_tracks"] == 1.0
    distance = written["link_distance_across_hole.csv"][0]
    assert distance["holes_compared"] == 1
    assert abs(distance["median_difference_nm"]) < 1e-3
    # The stored distance in this fixture is zero, and the recomputed one is not.
    assert distance["scored_tracks"] == 1
    assert distance["tracks_off_stored_distance_by_over_1_nm"] == 1
    assert distance["stored_distance_verdict"] == "differs"
    labels = {r["label"]: r for r in written["link_truth_all_flights.csv"]}
    assert labels["true_link"]["holes"] == 1
    assert {r["movement"] for r in written["link_movements.csv"]} == {"ATOT", "ALDT", "all"}
    assert {r["type"] for r in written["link_events.csv"]} == {"level-start", "level-end", "last_seen"}
    assert list(written)[1] == "link_consistency.csv"
    consistency = written["link_consistency.csv"][0]
    assert consistency["debounced_rows_without_a_track"] == 0
    assert consistency["cruise_linked_rows_without_a_track"] == 0
    assert consistency["unchanged_tracks"] == 0
    no_hole = written["link_distance_no_hole.csv"][0]
    assert no_hole["tracks"] == 2
    assert no_hole["tracks_short_by_over_1_nm"] == 0

    # A flight list left over from other tracks is refused, after saying so in the table.
    tables[("cruise_linked", "opdi_flight_list")] = tables[
        ("cruise_linked", "opdi_flight_list")
    ].withColumn("ID", F.lit("a-track-that-is-gone"))
    written.clear()
    with pytest.raises(SystemExit, match="without a track"):
        s.score_day(
            spark,
            day,
            read=lambda arm, table: s.one_day(tables[(arm, table)], table, day),
            truth=truth,
            p=P,
            apdf=apdf,
            detected={"debounced": detection, "cruise_linked": detection},
            write=write,
            truth_path=str(nm_path),
        )
    assert written["link_consistency.csv"][0]["cruise_linked_rows_without_a_track"] == 1
    assert "link_flight_list.csv" not in written


# --- the two arms describe the same tracks ----------------------------------


def _flight_list(spark, *rows):
    """``(id, adep, ades, adep_p, ades_p)`` rows in the shape the scorer reads."""
    return spark.createDataFrame(
        list(rows), "id string, adep string, ades string, adep_p string, ades_p string"
    )


def _tracks(spark, *spans):
    """``(track_id, icao24, start_s, end_s)``: a sample a minute over each span."""
    samples = [
        row
        for track, icao24, start, end in spans
        for row in _cruise([(start, end)], icao24=icao24, track_id=track)
    ]
    return make_track(spark, samples).select("track_id", "icao24", "event_time", "lat", "lon")


def test_a_flight_list_row_without_a_track_is_counted(spark):
    tracks = _tracks(spark, ("T1", "abc123", 0, 600))
    rows = _flight_list(
        spark, ("T1", "EDDF", None, "", ""), ("T9", "EGLL", None, "", "")
    )
    assert s.rows_without_a_track(rows, tracks) == 1


def test_a_named_airport_among_its_own_alternatives_is_counted(spark):
    rows = _flight_list(
        spark,
        ("T1", "EPWA", "EDDF", "EPMO, EPMM", ""),
        ("T2", "EPMO", "EDDF", "EPMO, EPMM", ""),
        ("T3", "EGLL", "EDDM", "", "EDMO, EDDM"),
        ("T4", None, None, None, None),
    )
    assert s.named_among_alternatives(rows) == 2


def test_tracks_the_same_under_both_rules_are_paired(spark):
    # Airframe abc123 is split in one arm and whole in the other; def456 is the same.
    split = _tracks(
        spark, ("S1", "abc123", 0, 600), ("S2", "abc123", 2 * HOUR, 2 * HOUR + 600),
        ("S3", "def456", 0, 600),
    )
    linked = _tracks(spark, ("L1", "abc123", 0, 600), ("L3", "def456", 0, 600))
    # L1 here ends at 600 s like S1, so it pairs too: a pair is an airframe and an extent.
    pairs = {tuple(r) for r in s.unchanged_tracks(split, linked).collect()}
    assert pairs == {("S1", "L1"), ("S3", "L3")}


def test_an_unchanged_track_with_another_airport_or_event_is_counted(spark):
    split = _tracks(spark, ("S1", "abc123", 0, 600), ("S2", "def456", 0, 600))
    linked = _tracks(spark, ("L1", "abc123", 0, 600), ("L2", "def456", 0, 600))
    row = s.unchanged_track_row(
        s.unchanged_tracks(split, linked),
        _flight_list(spark, ("S1", "EDDF", "EPMO", "", ""), ("S2", "EGLL", None, "", "")),
        _flight_list(spark, ("L1", "EDDF", "EPWA", "", ""), ("L2", "EGLL", None, "", "")),
        _events(spark, ("S1", "take-off", 0), ("S2", "take-off", 0), ("S2", "landing", 600)),
        _events(spark, ("L1", "take-off", 0), ("L2", "take-off", 0), ("L2", "landing", 540)),
    )
    assert row == {
        "unchanged_tracks": 2,
        "unchanged_tracks_with_other_airports": 1,
        "unchanged_tracks_with_other_events": 1,
    }


def test_an_unchanged_track_whose_events_differ_only_in_info_is_counted(spark):
    # The event's info carries measured figures (a level-off's distance), which must
    # agree too: the same track must give the same events in full.
    split = _tracks(spark, ("S1", "abc123", 0, 600))
    linked = _tracks(spark, ("L1", "abc123", 0, 600))
    rows = _flight_list(spark, ("S1", "EDDF", None, "", ""), ("L1", "EDDF", None, "", ""))
    a = _events(spark, ("S1", "level-off-climb-start", 0)).withColumn("info", F.lit('{"distance_nm": 10}'))
    b = _events(spark, ("L1", "level-off-climb-start", 0)).withColumn("info", F.lit('{"distance_nm": 12}'))
    row = s.unchanged_track_row(s.unchanged_tracks(split, linked), rows, rows, a, b)
    assert row["unchanged_tracks_with_other_events"] == 1


def test_unchanged_tracks_that_agree_count_nothing(spark):
    tracks = _tracks(spark, ("S1", "abc123", 0, 600))
    rows = _flight_list(spark, ("S1", "EDDF", None, "", ""))
    events = _events(spark, ("S1", "take-off", 0))
    row = s.unchanged_track_row(s.unchanged_tracks(tracks, tracks), rows, rows, events, events)
    assert row["unchanged_tracks"] == 1
    assert row["unchanged_tracks_with_other_airports"] == 0
    assert row["unchanged_tracks_with_other_events"] == 0


def test_distance_on_tracks_without_a_hole_compares_the_two_methods(spark):
    # Five samples a degree apart on the equator, the middle position blank: the
    # released method lost the two segments around it.
    samples = [
        {"t": i * 60, "lat": 0.0, "lon": float(i), "track_id": "T1"} for i in range(5)
    ]
    samples[2]["lat"] = samples[2]["lon"] = None
    whole = [{"t": i * 60, "lat": 0.0, "lon": float(i), "track_id": "T2"} for i in range(5)]
    tracks = make_track(spark, samples + whole).select("track_id", "event_time", "lat", "lon")
    row = s.distance_without_holes(tracks)
    assert row["tracks"] == 2
    assert row["released_total_nm"] == pytest.approx(360.0, rel=2e-3)
    assert row["new_total_nm"] == pytest.approx(480.0, rel=2e-3)
    assert row["share_of_distance_missing"] == pytest.approx(0.25, rel=2e-3)
    assert row["tracks_short_by_over_1_nm"] == 1
    assert row["share_of_tracks_short_by_over_1_nm"] == 0.5
    assert row["median_short_nm"] == pytest.approx(60.0, rel=2e-3)


def test_a_day_with_no_scored_flights_is_refused(spark):
    truth = pd.DataFrame(
        {
            "flight_key": pd.Series(dtype=int),
            "icao24": pd.Series(dtype=object),
            "t_off": pd.Series(dtype="datetime64[ns]"),
            "t_on": pd.Series(dtype="datetime64[ns]"),
        }
    )
    with pytest.raises(SystemExit, match="no truth flights"):
        s.score_day(spark, "2024-06-01", read=None, truth=truth, p=P, apdf=None, detected=None, write=None, truth_path=None)


# --- every Network Manager flight --------------------------------------------


def _label(holes, nm, pieces=(), events=()):
    def frame(rows, cols):
        return pd.DataFrame(
            [(a, *[pd.Timestamp(_at(x)) if isinstance(x, (int, float)) else x for x in r]) for a, *r in rows],
            columns=cols,
        )

    return s.label_holes_all_flights(
        frame(holes, ["icao24", "t_old_edge", "t_new_edge"]),
        frame(nm, ["icao24", "t_off", "t_on"]),
        frame(pieces, ["icao24", "t_start", "t_end"]),
        pd.DataFrame(events, columns=["icao24", "milestone", "event_time"]).assign(
            event_time=lambda d: [pd.Timestamp(_at(t)) for t in d["event_time"]]
        ),
    )


def test_a_hole_with_a_block_time_inside_it_is_a_false_join():
    holes = [("a", HOUR, 3 * HOUR)]
    assert _label(holes, [("a", 0, 2 * HOUR)])["label"][0] == "false_join"  # on-block inside
    assert _label(holes, [("a", 2 * HOUR, 5 * HOUR)])["label"][0] == "false_join"  # off-block


def test_a_hole_within_one_block_window_is_a_true_link():
    assert _label([("a", HOUR, 3 * HOUR)], [("a", 0, 4 * HOUR)])["label"][0] == "true_link"


def test_flights_elsewhere_make_the_hole_unknown_and_no_flights_not_in_truth():
    nm = [("a", 10 * HOUR, 12 * HOUR)]
    holes = [("a", HOUR, 3 * HOUR), ("b", HOUR, 3 * HOUR)]
    assert list(_label(holes, nm)["label"]) == ["unknown", "airframe_not_in_truth"]


def test_a_flight_on_another_day_still_counts():
    nm = [("a", 30 * HOUR, 32 * HOUR), ("a", 2 * HOUR - 60, 2 * HOUR + 60)]
    assert _label([("a", HOUR, 3 * HOUR)], nm)["label"][0] == "false_join"


def test_block_events_alone_never_make_a_hole_a_false_join():
    events = [("a", "AIBT", 0.5 * HOUR), ("a", "AOBT", 3.5 * HOUR)]
    pieces = [("a", 0, HOUR), ("a", 3 * HOUR, 4 * HOUR)]
    got = _label([("a", HOUR, 3 * HOUR)], [("a", 0, 5 * HOUR)], pieces, events)
    assert got["label"][0] == "true_link"
    assert got["landing_on_older"][0] and got["take_off_on_newer"][0]


def test_events_are_counted_on_the_piece_they_sit_on():
    pieces = [("a", 0, HOUR), ("a", 3 * HOUR, 4 * HOUR)]
    events = [("a", "ALDT", 3.5 * HOUR), ("a", "ATOT", 0.5 * HOUR)]  # wrong pieces
    got = _label([("a", HOUR, 3 * HOUR)], [("a", 0, 5 * HOUR)], pieces, events)
    assert not got["landing_on_older"][0] and not got["take_off_on_newer"][0]


def test_the_summary_has_a_row_per_label_even_when_empty():
    labelled = pd.DataFrame(
        {"label": ["true_link", "true_link"], "landing_on_older": [True, False],
         "take_off_on_newer": [True, True]}
    )
    rows = {r["label"]: r for r in s.summarise_hole_labels(labelled)}
    assert list(rows) == list(s.HOLE_LABELS)
    assert rows["true_link"] == {
        "label": "true_link", "holes": 2, "landing_on_older": 1,
        "take_off_on_newer": 2, "both": 1,
    }
    assert rows["false_join"]["holes"] == 0


def test_a_block_time_exactly_on_a_hole_edge_is_not_a_false_join():
    holes = [("a", HOUR, 3 * HOUR)]
    got = _label(holes, [("a", HOUR, 3 * HOUR)])
    assert got["label"][0] == "true_link"
    assert _label(holes, [("a", 0, HOUR), ("a", 3 * HOUR, 5 * HOUR)])["label"][0] == "unknown"


def test_load_all_flights_reads_zoned_times_as_utc(tmp_path):
    path = tmp_path / "f.parquet"
    pd.DataFrame(
        {
            "AIRCRAFT_ADDRESS": [" ABC123 ", "zzz999"],
            "AOBT_3": pd.to_datetime(["2026-06-14 12:00"]*2).tz_localize("Europe/Paris"),
            "ARVT_3": pd.to_datetime(["2026-06-14 14:00"]*2).tz_localize("Europe/Paris"),
        }
    ).to_parquet(path)
    got = s.load_all_flights(path, {"abc123"})
    assert list(got["icao24"]) == ["abc123"]
    assert got["t_off"][0] == pd.Timestamp("2026-06-14 10:00")
    assert got["t_on"][0] == pd.Timestamp("2026-06-14 12:00")
