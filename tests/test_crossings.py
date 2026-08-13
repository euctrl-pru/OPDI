"""Tests for threshold crossing detection.

Each test builds a synthetic trajectory whose crossings are known by
construction and asserts that exactly those are found. The geometry is chosen
so the interpolated instant has an exact arithmetic answer, which is what makes
"interpolated to the threshold" a checkable claim rather than a hopeful one.
"""

import datetime as dt

import pytest

from conftest import _EPOCH, make_track
from pyspark.sql import functions as F

from opdi.config import EventConfig
from opdi.pipeline.crossings import flight_level_crossings, threshold_crossings

FT_PER_M = 3.28084


def _m(feet: float) -> float:
    """Feet to metres -- the storage layer is SI, the thresholds are aviation."""
    return feet / FT_PER_M


def _climb(spark, altitudes_ft, step_s=5):
    return make_track(
        spark,
        [{"t": i * step_s, "baro_altitude": _m(a)} for i, a in enumerate(altitudes_ft)],
    )


def _crossings(sdf, config=None, **kwargs):
    config = config or EventConfig()
    out = flight_level_crossings(sdf, config, altitude_col="baro_altitude", **kwargs)
    return out.orderBy("threshold", "crossing_seq").collect()


def _offset_s(ts) -> float:
    return (ts - _EPOCH).total_seconds()


def test_a_clean_climb_through_a_level_emits_one_crossing(spark):
    rows = _crossings(_climb(spark, [9000, 9400, 9800, 10200, 10600, 11000]))
    fl100 = [r for r in rows if r.threshold == 100.0]

    assert len(fl100) == 1
    assert fl100[0].direction == "up"
    assert fl100[0].crossing_seq == 1
    # 9800 -> 10200 straddles 10000 at the midpoint of a 5 s step starting at
    # t=10, so the crossing is at t=12.5 -- not at either bracketing sample.
    assert _offset_s(fl100[0].event_time) == pytest.approx(12.5, abs=0.05)
    assert fl100[0].bracket_seconds == pytest.approx(5.0)


def test_cruising_at_the_threshold_emits_nothing(spark):
    """The defect hysteresis exists to prevent.

    An aircraft levelled at FL100 oscillates across the bare boundary on
    barometric noise. Without a dead band this trajectory yields a crossing on
    every sample; with one it must yield none.
    """
    noisy = [9800, 10200, 9850, 10150, 9900, 10100, 9800, 10200] * 3
    rows = _crossings(_climb(spark, noisy))

    assert [r for r in rows if r.threshold == 100.0] == []


def test_every_crossing_is_reported_not_just_the_first_and_last(spark):
    """Climb through FL100, descend back below, climb again."""
    profile = [9000, 9500, 10500, 11000, 10500, 9500, 9000, 9500, 10500, 11000]
    fl100 = [r for r in _crossings(_climb(spark, profile)) if r.threshold == 100.0]

    assert [r.crossing_seq for r in fl100] == [1, 2, 3]
    assert [r.direction for r in fl100] == ["up", "down", "up"]
    assert [_offset_s(r.event_time) for r in fl100] == pytest.approx(
        [7.5, 22.5, 37.5], abs=0.05
    )


def test_one_sample_interval_spanning_two_levels_reports_both(spark):
    """The published detector's first-match ``when`` chain reported only one."""
    rows = _crossings(_climb(spark, [4000, 8000, 9000]))
    got = {r.threshold: r for r in rows}

    assert set(got) == {50.0, 70.0}
    # 4000 -> 8000 over 5 s: FL50 at a quarter of the way, FL70 at three
    # quarters.
    assert _offset_s(got[50.0].event_time) == pytest.approx(1.25, abs=0.05)
    assert _offset_s(got[70.0].event_time) == pytest.approx(3.75, abs=0.05)
    assert got[50.0].direction == got[70.0].direction == "up"


def test_position_is_interpolated_to_the_crossing_too(spark):
    sdf = make_track(
        spark,
        [
            {"t": 0, "baro_altitude": _m(9000), "lat": 50.0, "lon": 4.0},
            {"t": 5, "baro_altitude": _m(11000), "lat": 52.0, "lon": 8.0},
            {"t": 10, "baro_altitude": _m(11500), "lat": 53.0, "lon": 10.0},
        ],
    )
    fl100 = [r for r in _crossings(sdf) if r.threshold == 100.0][0]

    # 10000 ft is halfway between the bracketing samples, so the position is
    # halfway along the leg -- not the position of either sample.
    assert _offset_s(fl100.event_time) == pytest.approx(2.5, abs=0.05)
    assert fl100.lat == pytest.approx(51.0, abs=1e-6)
    assert fl100.lon == pytest.approx(6.0, abs=1e-6)


def test_a_descent_is_labelled_down(spark):
    rows = _crossings(_climb(spark, [11000, 10500, 9500, 9000]))
    fl100 = [r for r in rows if r.threshold == 100.0]

    assert len(fl100) == 1
    assert fl100[0].direction == "down"


def test_legacy_keeps_only_the_first_and_last_and_does_not_interpolate(spark):
    profile = [9000, 9500, 10500, 11000, 10500, 9500, 9000, 9500, 10500, 11000]
    legacy = EventConfig.legacy()
    fl100 = [
        r for r in _crossings(_climb(spark, profile), config=legacy)
        if r.threshold == 100.0
    ]

    assert [r.crossing_seq for r in fl100] == [1, 3]
    # Uninterpolated: the confirming samples at t=10 and t=40, which is exactly
    # the one-sided bias interpolation removes.
    assert [_offset_s(r.event_time) for r in fl100] == pytest.approx([10.0, 40.0])
    assert all(r.bracket_seconds is None for r in fl100)


def test_no_crossing_is_invented_at_the_start_of_a_track(spark):
    """The first resolved side has nothing to have flipped from."""
    rows = _crossings(_climb(spark, [10600, 11000, 11500]))

    assert rows == []


def test_a_crossing_inferred_across_a_gap_reports_the_gap(spark):
    """The instant is still the best estimate, but the consumer must be able to
    tell it was interpolated across four minutes rather than five seconds."""
    sdf = make_track(
        spark,
        [
            {"t": 0, "baro_altitude": _m(9000)},
            {"t": 240, "baro_altitude": _m(11000)},
            {"t": 245, "baro_altitude": _m(11200)},
        ],
    )
    fl100 = [r for r in _crossings(sdf) if r.threshold == 100.0][0]

    assert fl100.bracket_seconds == pytest.approx(240.0)


def test_segments_stop_a_crossing_being_inferred_across_a_hole(spark):
    """Partitioning on a segment column is how a caller opts out of inferring
    crossings across a coverage hole."""
    sdf = make_track(
        spark,
        [
            {"t": 0, "baro_altitude": _m(9000)},
            {"t": 5, "baro_altitude": _m(9400)},
            {"t": 600, "baro_altitude": _m(11000)},
            {"t": 605, "baro_altitude": _m(11200)},
        ],
    ).withColumn(
        "segment_id",
        F.when(F.col("event_time") < F.lit(_EPOCH + dt.timedelta(seconds=300)), "seg-a")
        .otherwise("seg-b"),
    )

    with_segments = _crossings(sdf, partition_cols=["track_id", "segment_id"])
    without = _crossings(sdf)

    assert [r.threshold for r in without] == [100.0]
    assert with_segments == []


def test_rings_invert_the_sense_of_the_direction_labels(spark):
    """Distance grows as the aircraft leaves, so +1 is outbound, not 'up'."""
    rows = [
        (_EPOCH + dt.timedelta(seconds=i * 30), "trk-1", "EBBR", d, 50.0, 4.0, 100.0)
        for i, d in enumerate([120.0, 90.0, 50.0, 20.0, 50.0, 90.0, 120.0])
    ]
    sdf = spark.createDataFrame(
        rows,
        "event_time timestamp, track_id string, apt_ident string, "
        "distance_nm double, lat double, lon double, flight_level double",
    )

    out = threshold_crossings(
        sdf,
        value_col="distance_nm",
        thresholds=[40.0, 100.0],
        hysteresis=1.0,
        partition_cols=["track_id", "apt_ident"],
        interpolate_cols=("lat", "lon", "flight_level"),
        up_label="outbound",
        down_label="inbound",
    ).orderBy("threshold", "crossing_seq").collect()

    got = [(r.threshold, r.direction, r.crossing_seq) for r in out]
    assert got == [
        (40.0, "inbound", 1),
        (40.0, "outbound", 2),
        (100.0, "inbound", 1),
        (100.0, "outbound", 2),
    ]


def test_no_thresholds_yields_an_empty_frame_not_an_error(spark):
    """``EventConfig.legacy()`` has no rings; a caller must still union it."""
    out = threshold_crossings(
        _climb(spark, [9000, 11000]),
        value_col="baro_altitude",
        thresholds=[],
        hysteresis=1.0,
        partition_cols=["track_id"],
    )

    assert out.count() == 0
    assert "crossing_seq" in out.columns
