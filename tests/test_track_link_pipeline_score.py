"""Scoring one sampled day of both arms: the flight list, detectors, and what else moved.

Tracks here are flights on the equator at 480 kt with one sample a minute, so the
distance over a hole is known exactly from its length.
"""
import datetime as dt
import math

import pandas as pd
import pytest
from conftest import _EPOCH, make_track
from pyspark.sql import functions as F

import track_link_pipeline_score as s
from opdi.pipeline.flights import EARTH_R_NM
from opdi.pipeline.segmentation import SegmentationParams, assign_track_id
from opdi.pipeline.segmentation.base import KT_PER_MPS
from opdi.pipeline.segmentation.methods import cruise_linked, debounced

P = SegmentationParams()
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
    rows = s.distance_across_holes(tracks, s.linked_holes(tracks, P)).collect()

    assert len(rows) == 1
    row = rows[0]
    expected = (2 * HOUR + 60 - 600) / 3600.0 * CRUISE_KT
    assert row["great_circle_nm"] == pytest.approx(expected, rel=1e-6)
    assert row["jump_nm"] == pytest.approx(0.0, abs=1e-6)
    assert row["difference_nm"] == pytest.approx(-expected, rel=1e-6)
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
    assert good == {"tracks_checked_against_stored_distance": 1, "tracks_off_stored_distance_by_over_1_nm": 0}

    wrong = spark.createDataFrame(
        [(event_id, "Distance flown (NM)", flown - 500.0)],
        "milestone_id string, type string, value double",
    )
    bad = s.stored_distance_check(tracks, events, wrong)
    assert bad == {"tracks_checked_against_stored_distance": 1, "tracks_off_stored_distance_by_over_1_nm": 1}


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


def test_a10_adding_a_boundary_fails_the_check(spark):
    samples = make_track(spark, _cruise([(0, 40 * 60)]))
    split, _ = _both_rules(samples)
    cut = F.when(F.col("event_time") >= F.lit(_at(20 * 60)), "second").otherwise("first")
    added = split.withColumn("track_id", cut)

    got = s.partition_check(samples, split, added, P)

    assert got["debounced_boundaries"] == 0
    assert got["added_boundaries"] == 1
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
        ).withColumn("dof", F.to_date(F.lit(day)))

    def measurements_of(events):
        return events.select(
            F.col("id").alias("milestone_id"),
            F.lit("Distance flown (NM)").alias("type"),
            F.lit(0.0).alias("value"),
            "dof",
        )

    def flight_list_of(rows):
        return spark.createDataFrame(
            rows,
            "ICAO24 string, FLT_ID string, ADEP string, ADES string, first_seen timestamp, "
            "last_seen timestamp",
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
            [
                ("abc123", "DLH1", "EDDF", None, start, _at(40 * 60)),
                ("abc123", "DLH1", None, "KJFK", _at(2 * HOUR), end),
            ]
        ),
        ("cruise_linked", "opdi_flight_list"): flight_list_of(
            [("abc123", "DLH1", "EDDF", "KJFK", start, end)]
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
    )

    assert set(written) == set(s.OUTPUTS)
    assert list(pd.DataFrame(written["link_both_ends.csv"])["debounced_rows_per_flight"]) == [2.0, 2.0]
    assert list(pd.DataFrame(written["link_both_ends.csv"])["cruise_linked_both_right"]) == [1, 1]
    assert written["link_partition_check.csv"][0]["passed"] is True
    assert written["link_partition_check.csv"][0]["removed_at_a_linked_hole"] == 1
    level = _family(written["link_level_across_hole.csv"], "level")
    assert level["segments_across_a_hole"] == 1
    assert level["share_of_tracks"] == 1.0
    distance = written["link_distance_across_hole.csv"][0]
    assert distance["holes_compared"] == 1
    assert abs(distance["median_difference_nm"]) < 1e-3
    # The stored distance in this fixture is zero, and the recomputed one is not.
    assert distance["tracks_off_stored_distance_by_over_1_nm"] == 1
    assert {r["movement"] for r in written["link_movements.csv"]} == {"ATOT", "ALDT", "all"}
    assert {r["type"] for r in written["link_events.csv"]} == {"level-start", "level-end", "last_seen"}


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
        s.score_day(spark, "2024-06-01", read=None, truth=truth, p=P, apdf=None, detected=None, write=None)
