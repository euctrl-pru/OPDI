"""Containment over the gate-to-gate interval, so taxi and stand samples count.

The airborne interval [t_off, t_land] is what every V1 metric was computed over,
which means no V1 number says anything about whether taxi-out was attached to
the right flight. These tests pin the wider interval, and pin the property that
makes the two comparable: the gate interval always *contains* the airborne one,
so gate matching is a superset and can only ever add samples.
"""
import datetime as dt

import track_truth
from pyspark.sql import functions as F
from track_truth import overlap_join


def _ts(s):
    return dt.datetime.fromisoformat(s)


def test_gate_interval_contains_the_airborne_interval(spark):
    """The guard that makes the comparison sound.

    APDF is real operational data: a bad AOBT after its own take-off exists.
    Without least()/greatest() such a row yields a gate interval *narrower* than
    the airborne one, and gate matching would drop samples airborne matching
    kept -- inverting the finding.
    """
    gt = spark.createDataFrame(
        [
            # normal: off-block 12 min before take-off, in-block 6 after landing
            ("f1", "abc123", _ts("2025-06-05T10:00:00"), _ts("2025-06-05T11:00:00"),
             _ts("2025-06-05T09:48:00"), _ts("2025-06-05T11:06:00")),
            # corrupt: AOBT after ATOT, AIBT before ALDT
            ("f2", "def456", _ts("2025-06-05T14:00:00"), _ts("2025-06-05T15:00:00"),
             _ts("2025-06-05T14:05:00"), _ts("2025-06-05T14:55:00")),
        ],
        "flight_key string, icao24 string, t_off timestamp, t_land timestamp, "
        "aobt timestamp, aibt timestamp",
    )
    out = track_truth.attach_gate_interval(gt, b_dep_s=600, b_arr_s=300).collect()
    by = {r["flight_key"]: r for r in out}

    assert by["f1"]["t_off_block"] == _ts("2025-06-05T09:48:00")
    assert by["f1"]["t_in_block"] == _ts("2025-06-05T11:06:00")
    # clamped to the airborne bound, never inside it
    assert by["f2"]["t_off_block"] == _ts("2025-06-05T14:00:00")
    assert by["f2"]["t_in_block"] == _ts("2025-06-05T15:00:00")


def test_null_block_times_fall_back_to_the_measured_buffer(spark):
    """aibt is APDF-only and NULL for about half the sample. That half still
    needs an interval, and the buffer is a measured median -- not a constant
    someone liked the look of."""
    gt = spark.createDataFrame(
        [("f3", "abc123", _ts("2025-06-05T10:00:00"), _ts("2025-06-05T11:00:00"),
          None, None)],
        "flight_key string, icao24 string, t_off timestamp, t_land timestamp, "
        "aobt timestamp, aibt timestamp",
    )
    r = track_truth.attach_gate_interval(gt, b_dep_s=600, b_arr_s=300).collect()[0]
    assert r["t_off_block"] == _ts("2025-06-05T09:50:00")
    assert r["t_in_block"] == _ts("2025-06-05T11:05:00")
    assert r["gate_dep_measured"] is False
    assert r["gate_arr_measured"] is False


def test_gate_buffers_are_the_measured_medians(spark):
    """b_dep is median(t_off - aobt) over the flights where aobt is measured.

    The median is the *exact*, interpolating one -- `F.median`, not
    `percentile_approx`. On two values `percentile_approx` returns the lower
    input rather than the midpoint, so a two-flight fixture cannot pin the
    quantity the docstring names. See `track_truth.gate_buffers`.
    """
    gt = spark.createDataFrame(
        [("f1", _ts("2025-06-05T10:00:00"), _ts("2025-06-05T11:00:00"),
          _ts("2025-06-05T09:50:00"), _ts("2025-06-05T11:05:00"), True, True),
         ("f2", _ts("2025-06-05T12:00:00"), _ts("2025-06-05T13:00:00"),
          _ts("2025-06-05T11:40:00"), _ts("2025-06-05T13:15:00"), True, True),
         ("f3", _ts("2025-06-05T14:00:00"), _ts("2025-06-05T15:00:00"),
          None, None, False, False)],
        "flight_key string, t_off timestamp, t_land timestamp, aobt timestamp, "
        "aibt timestamp, dep_measured boolean, arr_measured boolean",
    )
    b_dep, b_arr = track_truth.gate_buffers(gt)
    assert b_dep == 900.0    # median of 600 s and 1200 s
    assert b_arr == 600.0    # median of 300 s and 900 s


def test_gate_buffers_fall_back_to_zero_with_nothing_measured(spark):
    """No measured flight on a side degrades that side of the gate interval to
    the airborne bound, rather than inventing a duration."""
    gt = spark.createDataFrame(
        [("f1", _ts("2025-06-05T10:00:00"), _ts("2025-06-05T11:00:00"),
          None, None, False, False)],
        "flight_key string, t_off timestamp, t_land timestamp, aobt timestamp, "
        "aibt timestamp, dep_measured boolean, arr_measured boolean",
    )
    assert track_truth.gate_buffers(gt) == (0.0, 0.0)


def test_taxi_sample_matches_gate_but_not_airborne(spark):
    """The finding, as a test. One sample during taxi-out.

    Under the airborne interval it belongs to no flight and vanishes from every
    metric. Under the gate interval it belongs to the flight it obviously
    belongs to.
    """
    assign = spark.createDataFrame(
        [("abc123", _ts("2025-06-05T09:52:00"), "trk1")],
        "icao24 string, event_time timestamp, track_id string",
    )
    gt = spark.createDataFrame(
        [("f1", "abc123", _ts("2025-06-05T10:00:00"), _ts("2025-06-05T11:00:00"),
          _ts("2025-06-05T09:48:00"), _ts("2025-06-05T11:06:00"), "apdf",
          "EBBR", "LEMD")],
        "flight_key string, icao24 string, t_off timestamp, t_land timestamp, "
        "t_off_block timestamp, t_in_block timestamp, t_source string, "
        "gt_adep string, gt_ades string",
    )
    assert overlap_join(assign, gt).count() == 0
    gated = overlap_join(assign, gt, bounds=("t_off_block", "t_in_block"))
    assert gated.count() == 1
    assert gated.collect()[0]["flight_key"] == "f1"


def test_gated_overlap_join_still_emits_the_airborne_boundaries(spark):
    """`boundary_error` is defined against t_off/t_land whichever interval did
    the matching, so the output select must not follow `bounds`."""
    assign = spark.createDataFrame(
        [("abc123", _ts("2025-06-05T09:52:00"), "trk1")],
        "icao24 string, event_time timestamp, track_id string",
    )
    gt = spark.createDataFrame(
        [("f1", "abc123", _ts("2025-06-05T10:00:00"), _ts("2025-06-05T11:00:00"),
          _ts("2025-06-05T09:48:00"), _ts("2025-06-05T11:06:00"), "apdf",
          "EBBR", "LEMD")],
        "flight_key string, icao24 string, t_off timestamp, t_land timestamp, "
        "t_off_block timestamp, t_in_block timestamp, t_source string, "
        "gt_adep string, gt_ades string",
    )
    r = overlap_join(assign, gt, bounds=("t_off_block", "t_in_block")).collect()[0]
    assert r["t_off"] == _ts("2025-06-05T10:00:00")
    assert r["t_land"] == _ts("2025-06-05T11:00:00")


def test_score_arm_gated_adds_gate_rates_beside_the_airborne_row(spark):
    """`score_arm_gated` is composition, not a second metric definition: the
    airborne row is `score_arm`'s, verbatim, and the gate rates arrive under a
    prefix so neither can overwrite the other."""
    from track_score import match_rates, score_arm, score_arm_gated, track_extents

    assign = spark.createDataFrame(
        [("abc123", _ts("2025-06-05T09:52:00"), "trk1"),
         ("abc123", _ts("2025-06-05T10:10:00"), "trk1"),
         ("abc123", _ts("2025-06-05T10:50:00"), "trk1")],
        "icao24 string, event_time timestamp, track_id string",
    )
    gt = spark.createDataFrame(
        [("f1", "abc123", _ts("2025-06-05T10:00:00"), _ts("2025-06-05T11:00:00"),
          _ts("2025-06-05T09:48:00"), _ts("2025-06-05T11:06:00"), "apdf",
          "EBBR", "LEMD")],
        "flight_key string, icao24 string, t_off timestamp, t_land timestamp, "
        "t_off_block timestamp, t_in_block timestamp, t_source string, "
        "gt_adep string, gt_ades string",
    )
    extents = track_extents(assign)
    matched = overlap_join(assign, gt)
    matched_gate = overlap_join(assign, gt, bounds=("t_off_block", "t_in_block"))

    # The taxi-out sample is invisible to the airborne join and visible to the
    # gate one -- which is the whole point of scoring both.
    assert matched.count() == 2
    assert matched_gate.count() == 3

    out = score_arm_gated(matched, extents, matched_gate)
    plain = score_arm(matched, extents)
    assert {k: out[k] for k in plain} == plain
    for k, v in match_rates(matched_gate).items():
        assert out[f"gate_{k}"] == v
    assert set(out) - set(plain) == {
        "gate_n_flights", "gate_clean_match_pct", "gate_fragmented_pct",
        "gate_merged_pct",
    }


def test_airborne_metrics_are_untouched_by_the_gate_columns(spark):
    """score_arm over a gt frame carrying gate columns must equal score_arm
    over the same frame without them.

    If this fails, the gate work changed numbers V1 already published and Task
    13's re-run will silently rewrite them.
    """
    from track_score import score_arm, track_extents

    assign = spark.createDataFrame(
        [("abc123", _ts("2025-06-05T10:10:00"), "trk1"),
         ("abc123", _ts("2025-06-05T10:50:00"), "trk1"),
         ("def456", _ts("2025-06-05T14:30:00"), "trk2")],
        "icao24 string, event_time timestamp, track_id string",
    )
    plain_schema = (
        "flight_key string, icao24 string, t_off timestamp, t_land timestamp, "
        "t_source string, gt_adep string, gt_ades string"
    )
    plain_rows = [
        ("f1", "abc123", _ts("2025-06-05T10:00:00"),
         _ts("2025-06-05T11:00:00"), "apdf", "EBBR", "LEMD"),
        ("f2", "def456", _ts("2025-06-05T14:00:00"),
         _ts("2025-06-05T15:00:00"), "apdf", "EHAM", "LFPG"),
    ]
    plain = spark.createDataFrame(plain_rows, plain_schema)
    widened = track_truth.attach_gate_interval(
        plain.withColumn("aobt", F.lit(None).cast("timestamp"))
             .withColumn("aibt", F.lit(None).cast("timestamp")),
        b_dep_s=600, b_arr_s=300,
    )

    extents = track_extents(assign)
    a = score_arm(overlap_join(assign, plain), extents)
    b = score_arm(overlap_join(assign, widened), extents)
    assert a == b, f"airborne metrics moved: {a} vs {b}"
