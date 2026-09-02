"""Conformance tests for the ICAO level-segment detector (KPI17, KPI19).

These are the substitute for a benchmark. No source OPDI can reach holds
level-segment truth, so "accurate" is not a claim this family can support --
but ICAO publishes the algorithm and its parameters, so "conformant" is, and a
specification can be tested against with trajectories whose geometry is known
by construction.
"""

import datetime as dt

import pytest
from pyspark.sql import functions as F

from conftest import _EPOCH, make_track

from opdi.config import EventConfig
from opdi.pipeline import level_segments as level_segments_module
from opdi.pipeline.level_segments import (
    classify_level_offs,
    level_segments,
    level_segments_pru,
)

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


# ---------------------------------------------------------------------------
# The PRU arm: a rolling window rather than an anchored band
# ---------------------------------------------------------------------------


def _pru_segment_count(spark, roc_ftmin, samples=12, step_s=10):
    """Number of PRU level segments in a steady climb at `roc_ftmin`.

    The track starts at 10,000 ft and is sampled every `step_s` seconds, so the
    interpolation grid lands exactly on the samples and the rolling window
    spans one step. `vert_rate` is set too, and deliberately consistent with
    the altitudes -- the PRU arm must not read it, and a test whose vertical
    rate disagreed with its altitudes could not tell the two arms apart.
    """
    steps = [(10000 + roc_ftmin * step_s / 60.0 * i, roc_ftmin) for i in range(samples)]
    return level_segments_pru(
        _profile(spark, steps, step_s), EventConfig()
    ).count()


def _segment_frame(
    spark, level_ft, field_elev_ft=0.0, distance_nm=10.0, duration_seconds=60.0
):
    """One level segment, 600 s into the flight, as `level_segments` returns it
    plus the geometry the caller attaches."""
    start = _EPOCH + dt.timedelta(seconds=600)
    return spark.createDataFrame(
        [(
            "trk-1",
            start,
            start + dt.timedelta(seconds=duration_seconds),
            float(duration_seconds),
            float(level_ft),
            None,
        )],
        schema=(
            "track_id string, start_time timestamp, end_time timestamp, "
            "duration_seconds double, level_ft double, distance_nm double"
        ),
    ).withColumn(
        "elev_adep_ft", F.lit(float(field_elev_ft))
    ).withColumn(
        "elev_ades_ft", F.lit(float(field_elev_ft))
    ).withColumn(
        "dist_adep_nm", F.lit(float(distance_nm))
    ).withColumn(
        "dist_ades_nm", F.lit(float(distance_nm))
    )


def _classify(spark, segments, config):
    """Classify `segments` against a top of climb an hour in and a top of
    descent two hours in, so the segment is unambiguously in the climb."""
    return classify_level_offs(
        segments,
        config,
        toc_time=F.lit(_EPOCH + dt.timedelta(seconds=3600)).cast("timestamp"),
        tod_time=F.lit(_EPOCH + dt.timedelta(seconds=7200)).cast("timestamp"),
        toc_altitude_ft=F.lit(30000.0),
        tod_altitude_ft=F.lit(30000.0),
    )


def _classified(spark, level_ft, field_elev_ft, config):
    return _classify(spark, _segment_frame(spark, level_ft, field_elev_ft), config)


def _classified_at_distance_nm(spark, distance_nm, config):
    return _classify(
        spark,
        _segment_frame(spark, level_ft=10000, distance_nm=distance_nm),
        config,
    )


def test_the_pru_window_height_is_50ft_for_a_10s_window(spark):
    """A climb at 280 ft/min is level by PRU (under 300); one at 320 is not.

    Both are sampled at 10 s, so the window is 50 ft: 280 ft/min climbs 46.7 ft
    in the window and 320 climbs 53.3 ft.
    """
    assert _pru_segment_count(spark, roc_ftmin=280) == 1
    assert _pru_segment_count(spark, roc_ftmin=320) == 0


def test_the_climb_floor_is_measured_above_the_field(spark):
    """The geometry has to sit in the gap between the two rules, or it pins
    nothing.

    A 3,500 ft level segment over Zurich's 1,416 ft field is 2,084 ft AGL. The
    old rule compared 3,500 against the 3,000 ft floor and counted it; the new
    one compares 2,084 and does not. 4,500 ft is 3,084 ft AGL and is a
    level-off under either rule, so it is the control that says the classifier
    still returns something.

    An altitude below 3,000 ft would prove nothing at all -- both rules reject
    it -- which is the trap this test was written into and out of.
    """
    assert _classified(
        spark, level_ft=3500, field_elev_ft=1416, config=EventConfig()
    ).count() == 0
    assert _classified(
        spark, level_ft=4500, field_elev_ft=1416, config=EventConfig()
    ).count() == 1


def test_the_old_floor_is_still_reachable_against_pressure_altitude(spark):
    """The mirror of the case above, and the reason it discriminates.

    ``level_floors_above_field=False`` is v0.1.0's comparison, kept reachable so
    released data stays reproducible. The same 3,500 ft segment over the same
    1,416 ft field *is* a climb level-off under it. If this test and the one
    above ever agree, the flag has stopped doing anything.
    """
    legacy_floors = EventConfig(level_floors_above_field=False)
    assert _classified(
        spark, level_ft=3500, field_elev_ft=1416, config=legacy_floors
    ).count() == 1


def test_segments_outside_the_200nm_radius_are_not_analysed(spark):
    segs = _classified_at_distance_nm(spark, 250.0, EventConfig())
    assert segs.count() == 0
    assert _classified_at_distance_nm(spark, 150.0, EventConfig()).count() == 1


def test_the_radius_bound_is_off_under_the_v0_1_0_configuration(spark):
    """v0.1.0 declared ``level_analysis_radius_nm`` and read it nowhere.

    Absence of the distance columns is not a way to reproduce that: the
    aerodrome join that attaches them is needed by the floors, the tops and the
    runway family regardless, so a detector applying the bound "whenever the
    distances are present" applies it always. Without the flag the V4 baseline
    rung -- which is meant to *be* v0.1.0 -- enforces a bound v0.1.0 never had,
    and the whole ladder's first column is about a configuration nobody ran.

    The same 250 NM segment the test above excludes is therefore returned here.
    If the two ever agree, the flag has stopped doing anything.
    """
    v0_1_0 = EventConfig(level_radius_enforced=False)
    assert _classified_at_distance_nm(spark, 250.0, v0_1_0).count() == 1


def _pru_level_to_the_end(spark):
    """A climb that levels off at 10,000 ft and is still level at the last
    sample.

    Twelve samples 10 s apart, t+0 to t+110. The climb steps 400 ft per sample
    -- eight times the 50 ft window -- and reaches 10,000 ft at t+50, where it
    stays. So the grid points from t+50 to t+100 each open a window that is
    entirely inside the level part, and the one at t+110 opens a window with
    nothing in it but itself.
    """
    steps = [(8000 + 400 * i, 2400) for i in range(6)] + [(10000, 0)] * 6
    return level_segments_pru(_profile(spark, steps), EventConfig()).collect()


def test_a_level_segment_cannot_outlast_the_track_that_carries_it(spark):
    """A grid point whose window runs past the last sample is unmeasured, not
    level, and that distinction is only visible here.

    A truncated window spans nothing and so always looks flat. The final grid
    point at t+110 is therefore level on the arithmetic, and a segment built
    from it runs to t+120 -- ten seconds after the aircraft was last heard
    from. Excluded, the segment ends at t+110 exactly, which is where the track
    does.

    Isolated, such a point makes a 10 s segment that the 20 s minimum discards,
    which is why a phantom-segment test would pass with the guard deleted. It
    is when the truncated point *extends a real segment* that the guard is
    load-bearing, and every level-off that runs to the end of its track is that
    case.
    """
    segs = _pru_level_to_the_end(spark)

    assert len(segs) == 1
    assert segs[0].duration_seconds == pytest.approx(60.0)
    assert segs[0].end_time == _EPOCH + dt.timedelta(seconds=110)


def test_the_interpolation_grid_is_bounded(spark, monkeypatch):
    """The grid is one ``explode`` over one array, so a track spanning weeks
    materialises the whole array in a single row before expanding it. The cap
    is on grid points per partition, and it truncates the tail.

    Driven with the cap at four points rather than a multi-day track, because
    the property under test is that the cap is applied at all -- and a test that
    needed 100,000 grid points to prove it would cost more than the bug.
    """
    monkeypatch.setattr(level_segments_module, "MAX_GRID_POINTS_PER_PARTITION", 4)

    segs = level_segments_pru(
        _profile(spark, [(10000, 0)] * 12), EventConfig()
    ).collect()

    # Four grid points, t+0 to t+30, so the analysed part ends one window later
    # at t+40 -- not at t+110, where the track does.
    assert len(segs) == 1
    assert segs[0].end_time == _EPOCH + dt.timedelta(seconds=40)
    assert segs[0].duration_seconds == pytest.approx(40.0)
