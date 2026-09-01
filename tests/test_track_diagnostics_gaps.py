"""How often A3's split predicate cannot see the altitude it needs.

traffic's rule reads altitude on both sides of a candidate gap. On a raw OSN
frame that value is often NULL, and a NULL comparison is not a split decision --
it is the absence of one. This counts how often that happens, so section 6.3 can
say which part of A3's failure is the missing fill and which part is the
single-threshold design.
"""
import datetime as dt

from track_diagnostics import gap_boundary_nulls


def _ts(s):
    return dt.datetime.fromisoformat(s)


def test_counts_gaps_whose_boundary_altitude_is_null(spark):
    sv = spark.createDataFrame(
        [
            # a gap with altitude on both sides -- the predicate can decide
            ("a1", _ts("2025-06-05T10:00:00"), 30000.0, False),
            ("a1", _ts("2025-06-05T10:20:00"), 31000.0, False),
            # a gap with NULL on the far side -- it cannot
            ("a2", _ts("2025-06-05T10:00:00"), 30000.0, False),
            ("a2", _ts("2025-06-05T10:20:00"), None, False),
        ],
        "icao24 string, event_time timestamp, baro_altitude_ft double, "
        "on_ground boolean",
    )
    out = gap_boundary_nulls(sv, gap_minutes=10.0)
    assert out["n_gaps"] == 2
    assert out["n_null_either_side"] == 1
    assert out["null_pct"] == 50.0


def test_counts_turnarounds_with_no_gap_at_all(spark):
    """The failure no fill can fix: continuous broadcast through a turnaround.

    traffic has one threshold, on gap length. An aircraft on stand still
    broadcasting produces no gap, so the rule never splits and the two legs
    merge. Legacy catches this with its second rule, a shorter gap below
    5,000 ft. Counting these separates A3's two failure modes.
    """
    sv = spark.createDataFrame(
        [("b1", _ts("2025-06-05T10:00:00"), 300.0, True),
         ("b1", _ts("2025-06-05T10:02:00"), 300.0, True),
         ("b1", _ts("2025-06-05T10:04:00"), 300.0, True),
         ("b1", _ts("2025-06-05T10:06:00"), 300.0, True),
         ("b1", _ts("2025-06-05T10:08:00"), 300.0, True),
         ("b1", _ts("2025-06-05T10:11:00"), 300.0, True)],
        "icao24 string, event_time timestamp, baro_altitude_ft double, "
        "on_ground boolean",
    )
    out = gap_boundary_nulls(sv, gap_minutes=10.0)
    assert out["n_gaps"] == 0
    assert out["n_no_gap_turnarounds"] >= 1


def test_two_short_turnarounds_across_a_real_flight_do_not_merge(spark):
    """A "maximal run" is bounded by on_ground transitions, not just filtered.

    A run of on_ground samples is only a "no-gap turnaround" if it holds
    together as one continuous stretch. A wrong implementation that simply
    groups every on_ground=True row for an icao24 -- ignoring the airborne
    samples that fall between them -- would treat two short, genuine ground
    stops around a real flight as one long stretch, because the timestamps of
    the on_ground-only rows alone still span more than gap_minutes with no gap
    between *those* timestamps exceeding it either.

    Here: b2 sits on stand for 2 minutes (10:00-10:02), flies (10:05, airborne),
    then sits on stand again for 3 minutes (10:08-10:11). Each real stop is far
    too short to be a turnaround. But the on_ground-only timestamps alone --
    10:00, 10:02, 10:08, 10:11 -- span 11 minutes (> the 10-minute threshold)
    with consecutive gaps of only 2, 6 and 3 minutes (none > the threshold), so
    an implementation that does not respect the airborne interruption would
    wrongly count this as a turnaround. The correct implementation must not:
    on_ground never holds continuously for more than 3 minutes at a stretch.
    """
    sv = spark.createDataFrame(
        [("b2", _ts("2025-06-05T10:00:00"), 300.0, True),
         ("b2", _ts("2025-06-05T10:02:00"), 300.0, True),
         ("b2", _ts("2025-06-05T10:05:00"), 5000.0, False),
         ("b2", _ts("2025-06-05T10:08:00"), 300.0, True),
         ("b2", _ts("2025-06-05T10:11:00"), 300.0, True)],
        "icao24 string, event_time timestamp, baro_altitude_ft double, "
        "on_ground boolean",
    )
    out = gap_boundary_nulls(sv, gap_minutes=10.0)
    assert out["n_no_gap_turnarounds"] == 0


def test_pre_gap_stale_signature_separates_boundary_from_baseline(spark):
    """The mechanism test: repeats and NULLs concentrate at the gap boundary.

    c1 fades into its silence -- the pre-gap sample repeats its predecessor's
    altitude exactly, the stale-broadcast signature. c2's pre-gap sample is
    already NULL in the raw feed. The ordinary mid-track samples do neither.
    A wrong implementation that reads the sample AFTER the silence instead of
    before it, or that forgets the icao24 partition, moves these counts.
    """
    from track_diagnostics import pre_gap_stale_signature

    sv = spark.createDataFrame(
        [
            # c1: fresh, fresh, REPEAT (pre-gap), then silence, then fresh
            ("c1", _ts("2025-06-05T10:00:00"), 30000.0, False),
            ("c1", _ts("2025-06-05T10:01:00"), 30100.0, False),
            ("c1", _ts("2025-06-05T10:02:00"), 30100.0, False),
            ("c1", _ts("2025-06-05T10:30:00"), 31000.0, False),
            # c2: fresh, NULL (pre-gap), silence, fresh
            ("c2", _ts("2025-06-05T11:00:00"), 20000.0, False),
            ("c2", _ts("2025-06-05T11:01:00"), None, False),
            ("c2", _ts("2025-06-05T11:30:00"), 21000.0, False),
        ],
        "icao24 string, event_time timestamp, baro_altitude_ft double, "
        "on_ground boolean",
    )
    out = pre_gap_stale_signature(sv, gap_minutes=10.0)
    assert out["pregap_n"] == 2
    assert out["pregap_repeat_pct"] == 50.0   # c1's repeat
    assert out["pregap_null_pct"] == 50.0     # c2's NULL
    # baseline: 5 other samples, none a repeat, none NULL
    assert out["other_repeat_pct"] == 0.0
    assert out["other_null_pct"] == 0.0
