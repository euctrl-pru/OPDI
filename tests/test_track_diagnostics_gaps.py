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
