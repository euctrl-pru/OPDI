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

    `f4` is the guard's own test, and it is why a NULL check would not do.
    Its block times are present and its *movement* times are not, which is the
    real shape of 37% of production rows: there `t_off` is itself derived as
    `aobt + TAXI_TIME_3`, so `t_off - aobt` is NM's taxi model rather than a
    measurement, and `aobt` is never NULL to filter on. f4's 6000 s moves both
    medians if `dep_measured`/`arr_measured` stop being consulted, so dropping
    either guard fails here instead of silently turning a measured median into
    a model fitted to itself.
    """
    gt = spark.createDataFrame(
        [("f1", _ts("2025-06-05T10:00:00"), _ts("2025-06-05T11:00:00"),
          _ts("2025-06-05T09:50:00"), _ts("2025-06-05T11:05:00"), True, True),
         ("f2", _ts("2025-06-05T12:00:00"), _ts("2025-06-05T13:00:00"),
          _ts("2025-06-05T11:40:00"), _ts("2025-06-05T13:15:00"), True, True),
         ("f3", _ts("2025-06-05T14:00:00"), _ts("2025-06-05T15:00:00"),
          None, None, False, False),
         # block times present, movement times not -- 6000 s on either side
         ("f4", _ts("2025-06-05T16:00:00"), _ts("2025-06-05T17:00:00"),
          _ts("2025-06-05T14:20:00"), _ts("2025-06-05T18:40:00"), False, False)],
        "flight_key string, t_off timestamp, t_land timestamp, aobt timestamp, "
        "aibt timestamp, dep_measured boolean, arr_measured boolean",
    )
    b_dep, b_arr = track_truth.gate_buffers(gt)
    assert b_dep == 900.0    # median of 600 s and 1200 s
    assert b_arr == 600.0    # median of 300 s and 900 s
    # and not the medians that let f4's modelled durations in
    assert (b_dep, b_arr) != (1200.0, 900.0)


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


def test_gate_in_window_flags_the_flights_that_escape_the_sample_window(spark):
    """A flight can be in-window airborne and out-of-window at the gate.

    `load_flight_intervals` bounds `[t_off, t_land]` by the sampled days and
    attaches the gate interval afterwards, so the gate interval reaches outside
    the window by construction. Those flights' taxi samples are clipped by the
    caller's own day filter, which truncates their gate interval in the
    flattering direction. The column is a flag, never a filter: every row must
    survive.
    """
    window = ("2025-06-05 00:00:00", "2025-06-06 00:00:00")
    gt = spark.createDataFrame(
        [  # wholly inside
            ("f1", _ts("2025-06-05T10:00:00"), _ts("2025-06-05T11:00:00"),
             _ts("2025-06-05T09:48:00"), _ts("2025-06-05T11:06:00")),
            # airborne inside, pushed back the previous evening
            ("f2", _ts("2025-06-05T00:05:00"), _ts("2025-06-05T01:05:00"),
             _ts("2025-06-04T23:50:00"), _ts("2025-06-05T01:11:00")),
            # airborne inside, on stand after the window closes
            ("f3", _ts("2025-06-05T22:30:00"), _ts("2025-06-05T23:50:00"),
             _ts("2025-06-05T22:15:00"), _ts("2025-06-06T00:10:00")),
        ],
        "flight_key string, t_off timestamp, t_land timestamp, "
        "aobt timestamp, aibt timestamp",
    )
    out = track_truth.attach_gate_interval(gt, 600, 300, window=window)
    assert out.count() == 3, "gate_in_window must not drop anything"
    flags = {r["flight_key"]: r["gate_in_window"] for r in out.collect()}
    assert flags == {"f1": True, "f2": False, "f3": False}

    # No window given -- nothing is excluded, mirroring load_flight_intervals'
    # own `in_window = F.lit(True)` when no days were passed.
    no_window = track_truth.attach_gate_interval(gt, 600, 300).collect()
    assert all(r["gate_in_window"] for r in no_window)


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
    prefix so neither can overwrite the other.

    The fixture is built so the two rate dicts genuinely *differ*, which a
    single clean flight cannot do. f1's taxi-out sample sits in its own track,
    so f1 is clean airborne -- the stray sample is invisible there -- and
    fragmented at the gate, which is exactly the class of defect this task
    exists to expose. f2 fragments under both. An implementation that scored
    the gate rates from `matched` would therefore report 50% clean where the
    gate truth is 0%, and this test would catch it.
    """
    from track_score import match_rates, score_arm, score_arm_gated, track_extents

    assign = spark.createDataFrame(
        [("abc123", _ts("2025-06-05T09:52:00"), "trk0"),   # taxi-out, own track
         ("abc123", _ts("2025-06-05T10:10:00"), "trk1"),
         ("abc123", _ts("2025-06-05T10:50:00"), "trk1"),
         ("def456", _ts("2025-06-05T14:10:00"), "trkA"),
         ("def456", _ts("2025-06-05T14:50:00"), "trkB")],
        "icao24 string, event_time timestamp, track_id string",
    )
    gt = spark.createDataFrame(
        [("f1", "abc123", _ts("2025-06-05T10:00:00"), _ts("2025-06-05T11:00:00"),
          _ts("2025-06-05T09:48:00"), _ts("2025-06-05T11:06:00"), "apdf",
          "EBBR", "LEMD"),
         ("f2", "def456", _ts("2025-06-05T14:00:00"), _ts("2025-06-05T15:00:00"),
          _ts("2025-06-05T13:48:00"), _ts("2025-06-05T15:06:00"), "apdf",
          "EHAM", "LFPG")],
        "flight_key string, icao24 string, t_off timestamp, t_land timestamp, "
        "t_off_block timestamp, t_in_block timestamp, t_source string, "
        "gt_adep string, gt_ades string",
    )
    extents = track_extents(assign)
    matched = overlap_join(assign, gt)
    matched_gate = overlap_join(assign, gt, bounds=("t_off_block", "t_in_block"))

    # The taxi-out sample is invisible to the airborne join and visible to the
    # gate one -- which is the whole point of scoring both.
    assert matched.count() == 4
    assert matched_gate.count() == 5

    out = score_arm_gated(matched, extents, matched_gate)
    plain = score_arm(matched, extents)
    assert {k: out[k] for k in plain} == plain
    for k, v in match_rates(matched_gate).items():
        assert out[f"gate_{k}"] == v
    assert set(out) - set(plain) == {
        "gate_n_flights", "gate_clean_match_pct", "gate_fragmented_pct",
        "gate_merged_pct",
    }
    # The two really are different measurements, so the assertions above are
    # not satisfied by any frame the implementation happens to be handed.
    assert out["clean_match_pct"] == 50.0
    assert out["gate_clean_match_pct"] == 0.0
    assert out["gate_fragmented_pct"] == 100.0
    # Same denominator here, unlike the general case the docstring warns about.
    assert out["n_flights"] == out["gate_n_flights"] == 2


def test_airborne_metrics_are_untouched_by_the_gate_columns(spark):
    """score_arm over a gt frame carrying gate columns must equal score_arm
    over the same frame without them.

    If this fails, the gate work changed numbers V1 already published and Task
    13's re-run will silently rewrite them.

    The 09:52 sample is what gives the test teeth. Every other sample sits
    inside its flight's airborne interval, so widening the interval would add
    nothing and the equality would hold however `overlap_join` were written.
    That one falls in the taxi window -- inside `widened`'s gate interval,
    outside every airborne one -- so the assertion now fails if the default
    join ever starts following the gate bounds.
    """
    from track_score import score_arm, track_extents

    assign = spark.createDataFrame(
        [("abc123", _ts("2025-06-05T09:52:00"), "trk1"),   # taxi-out
         ("abc123", _ts("2025-06-05T10:10:00"), "trk1"),
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
    m_plain = overlap_join(assign, plain)
    m_widened = overlap_join(assign, widened)
    # The taxi sample is outside every airborne interval and inside `widened`'s
    # gate interval, so it must be absent from both -- that is the property.
    assert m_plain.count() == m_widened.count() == 3

    a = score_arm(m_plain, extents)
    b = score_arm(m_widened, extents)
    assert a == b, f"airborne metrics moved: {a} vs {b}"
