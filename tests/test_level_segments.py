"""Conformance tests for the ICAO level-segment detector (KPI17, KPI19).

These are the substitute for a benchmark. No source OPDI can reach holds
level-segment truth, so "accurate" is not a claim this family can support --
but ICAO publishes the algorithm and its parameters, so "conformant" is, and a
specification can be tested against with trajectories whose geometry is known
by construction.
"""

import pytest
from pyspark.sql import functions as F

from conftest import make_track

from opdi.config import EventConfig
from opdi.pipeline.level_segments import level_segments

FT_PER_M = 3.28084
FTMIN_PER_MPS = 196.850394


def _m(feet):
    return feet / FT_PER_M


def _mps(ft_min):
    return ft_min / FTMIN_PER_MPS


def _profile(spark, steps, step_s=10):
    """`steps` is a list of (altitude_ft, vertical_rate_ft_min)."""
    return make_track(
        spark,
        [
            {"t": i * step_s, "baro_altitude": _m(a), "vert_rate": _mps(r)}
            for i, (a, r) in enumerate(steps)
        ],
    ).withColumn("baro_altitude_c", F.col("baro_altitude"))


def _segments(spark, steps, config=None, step_s=10):
    return (
        level_segments(_profile(spark, steps, step_s), config or EventConfig())
        .orderBy("start_time")
        .collect()
    )


def test_a_climb_with_an_injected_level_off_yields_exactly_one_segment(spark):
    """The headline conformance case: a known level-off of known duration."""
    steps = (
        [(5000 + 1000 * i, 2000) for i in range(4)]      # climbing
        + [(9000, 0)] * 10                                # 90 s level at 9000 ft
        + [(9000 + 1000 * i, 2000) for i in range(1, 5)]  # climbing again
    )

    segs = _segments(spark, steps)

    assert len(segs) == 1
    assert segs[0].level_ft == pytest.approx(9000, abs=1)
    assert segs[0].duration_seconds == pytest.approx(90, abs=1)


def test_a_continuous_climb_yields_no_segment(spark):
    """The control arm. A clean climb has no level-off at all."""
    steps = [(5000 + 1000 * i, 2000) for i in range(20)]

    assert _segments(spark, steps) == []


def test_a_segment_shorter_than_the_minimum_is_discarded(spark):
    """ICAO's minimum level time, 20 s by default."""
    steps = (
        [(5000 + 1000 * i, 2000) for i in range(4)]
        + [(9000, 0)] * 2                                 # 10 s only
        + [(9000 + 1000 * i, 2000) for i in range(1, 5)]
    )

    assert _segments(spark, steps) == []


def test_a_slow_drift_does_not_become_one_long_segment(spark):
    """Why the band is anchored at the segment's start rather than the previous
    sample. Each step here is inside the 200 ft band, so a pairwise test would
    run them into a single segment; against the anchor the segment ends as soon
    as the drift leaves the band."""
    steps = [(9000 + 100 * i, 100) for i in range(12)]   # 100 ft per step

    segs = _segments(spark, steps)

    assert all(
        seg.level_ft is None or abs(seg.level_ft - 9000) <= 200 for seg in segs
    )
    # 1,100 ft of drift cannot be one level segment.
    assert all(seg.duration_seconds < 100 for seg in segs)


def test_a_high_vertical_rate_breaks_the_segment_even_at_constant_altitude(spark):
    """Both of ICAO's conditions have to hold, not either."""
    steps = (
        [(5000 + 1000 * i, 2000) for i in range(4)]
        + [(9000, 2000)] * 10        # altitude flat, but reporting a climb
        + [(9000 + 1000 * i, 2000) for i in range(1, 5)]
    )

    assert _segments(spark, steps) == []


def test_the_band_limit_is_respected_at_its_edge(spark):
    """A step of exactly the band limit is inside it; ICAO says "<=".

    Pinned because an off-by-one on an inclusive bound silently halves or
    doubles the population of a KPI.
    """
    lo, hi = 9000, 9000 + int(EventConfig().level_band_limit_ft)
    steps = (
        [(5000 + 1000 * i, 2000) for i in range(4)]
        + [(lo if i % 2 == 0 else hi, 100) for i in range(10)]
        + [(12000, 2000)]
    )

    segs = _segments(spark, steps)

    assert len(segs) == 1
    assert segs[0].duration_seconds >= 20


def test_distance_is_reported_when_the_track_carries_it(spark):
    """KPI17 and KPI19 are both reported in NM/flight as well as minutes."""
    steps = (
        [(5000 + 1000 * i, 2000) for i in range(4)]
        + [(9000, 0)] * 10
        + [(12000, 2000)]
    )
    sdf = _profile(spark, steps).withColumn(
        "cumulative_distance_nm", F.monotonically_increasing_id().cast("double") * 2.0
    )

    segs = level_segments(sdf, EventConfig()).collect()

    assert len(segs) == 1
    assert segs[0].distance_nm is not None
    assert segs[0].distance_nm > 0
