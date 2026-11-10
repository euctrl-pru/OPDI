"""A hole inside a track, and what the event detectors do at one.

A *hole* is a silence longer than ``EventConfig.track_hole_seconds`` between two
consecutive samples of one track. Until the ``cruise_linked`` track rule, no
track contained one: every gap that long started a new track. Now a track can
span an ocean, and nothing was measured inside the hole, so no detector may
pair or interpolate samples across it.

The other half of the contract is what keeps released data reproducible: a
track without a hole gives exactly the events it gave before.
"""

import datetime as dt

import pytest
from pyspark.sql import functions as F

from conftest import make_track

from opdi.config import EventConfig, SegmentationConfig
from opdi.pipeline.events import (
    DISTANCE_VERSION,
    LEGACY_DISTANCE_VERSION,
    add_distance_measure,
    calculate_horizontal_segment_events,
    calculate_threshold_crossing_events,
    distance_version,
)
from opdi.pipeline.holes import add_piece
from opdi.pipeline.level_segments import LEVEL_ARMS

FT_PER_M = 3.28084
HOLE_S = 3 * 3600


def _cruise(t0, n=120, step=5, alt_ft=35000.0, lat=50.0, lon0=0.0, **extra):
    """``n`` level samples at cruise speed, heading east from ``lon0``."""
    return [
        {"t": t0 + i * step, "baro_altitude": alt_ft / FT_PER_M, "velocity": 240.0,
         "vert_rate": 0.0, "lat": lat, "lon": lon0 + 0.01 * i, **extra}
        for i in range(n)
    ]


def _measured(sdf):
    return (
        sdf.withColumn("baro_altitude_c", F.col("baro_altitude"))
        .withColumn("cumulative_distance_nm", F.lit(0.0))
        .withColumn("cumulative_time_s", F.lit(0).cast("long"))
    )


def _types(events):
    return sorted((r["type"], r["event_time"]) for r in events.collect())


# ---------------------------------------------------------------------------
# The piece index
# ---------------------------------------------------------------------------

def test_piece_counts_holes_per_track(spark):
    df = make_track(spark, [{"t": t} for t in (0, 600, 600 + 31 * 60, 600 + 76 * 60)])
    got = [r["piece"] for r in add_piece(df, 1800.0).orderBy("event_time").collect()]
    assert got == [0, 0, 1, 2]


def test_a_gap_of_exactly_the_threshold_is_not_a_hole(spark):
    df = make_track(spark, [{"t": 0}, {"t": 30 * 60}, {"t": 60 * 60 + 1}])
    got = [r["piece"] for r in add_piece(df, 1800.0).orderBy("event_time").collect()]
    assert got == [0, 0, 1]


def test_pieces_are_counted_per_track(spark):
    df = make_track(spark, [{"t": 0, "track_id": "a"}, {"t": 4000, "track_id": "a"},
                            {"t": 10, "track_id": "b"}, {"t": 20, "track_id": "b"}])
    got = {(r["track_id"], r["piece"]) for r in add_piece(df, 1800.0).collect()}
    assert got == {("a", 0), ("a", 1), ("b", 0)}


def test_hole_threshold_equals_the_segmentation_gap():
    # The two must move together: a detector that called a 20-minute silence a
    # hole would change events on tracks the old rules built.
    assert EventConfig().track_hole_seconds == SegmentationConfig().gap_minutes * 60 == 1800.0


# ---------------------------------------------------------------------------
# Level events from the phase classifier
# ---------------------------------------------------------------------------

def _cruise_hole_cruise(spark):
    first = _cruise(0)
    second = _cruise(first[-1]["t"] + HOLE_S, lon0=20.0)
    return _measured(make_track(spark, first + second)), first, second


def test_level_events_do_not_span_a_hole(spark):
    sdf, first, second = _cruise_hole_cruise(spark)
    events = calculate_horizontal_segment_events(add_piece(sdf, 1800.0), EventConfig())
    epoch = sdf.agg(F.min("event_time")).first()[0]
    at = lambda s: epoch + dt.timedelta(seconds=s)
    got = _types(events.filter(F.col("type").isin("level-start", "level-end")))
    assert got == sorted([
        ("level-start", at(first[0]["t"])), ("level-end", at(first[-1]["t"])),
        ("level-start", at(second[0]["t"])), ("level-end", at(second[-1]["t"])),
    ])


def test_without_the_piece_column_the_detector_is_unchanged(spark):
    # The published behaviour, kept reachable: a caller that attaches no piece
    # gets one segment across the silence, as before.
    sdf, first, second = _cruise_hole_cruise(spark)
    events = calculate_horizontal_segment_events(sdf, EventConfig())
    assert events.filter(F.col("type") == "level-start").count() == 1


def test_a_track_without_a_hole_gives_the_same_events(spark):
    profile = (
        [{"t": i * 5, "baro_altitude": (2000 + 400 * i) / FT_PER_M, "velocity": 150.0,
          "vert_rate": 10.0} for i in range(40)]
        + _cruise(200, n=60, alt_ft=18000.0)
        + [{"t": 500 + i * 5, "baro_altitude": (18000 - 400 * i) / FT_PER_M,
            "velocity": 150.0, "vert_rate": -10.0} for i in range(40)]
    )
    sdf = _measured(make_track(spark, profile))
    cols = ["track_id", "type", "event_time", "lon", "lat", "altitude_ft", "info"]
    plain = calculate_horizontal_segment_events(sdf, EventConfig()).select(*cols)
    pieced = calculate_horizontal_segment_events(
        add_piece(sdf, 1800.0), EventConfig()).select(*cols)
    assert plain.count() > 0
    assert sorted(map(tuple, plain.collect())) == sorted(map(tuple, pieced.collect()))


# ---------------------------------------------------------------------------
# Level segments (ICAO and PRU)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("arm", sorted(LEVEL_ARMS))
def test_level_segments_do_not_span_a_hole(spark, arm):
    sdf, first, second = _cruise_hole_cruise(spark)
    segs = LEVEL_ARMS[arm](
        add_piece(sdf, 1800.0), EventConfig(), partition_cols=("track_id", "piece"))
    rows = segs.collect()
    assert rows, "level flight either side of the hole should still be found"
    assert max(r["duration_seconds"] for r in rows) <= len(first) * 5 + 60
    assert len({r["piece"] for r in rows}) == 2


# ---------------------------------------------------------------------------
# Flight-level crossings
# ---------------------------------------------------------------------------

def _climb_through_fl245(spark, hole):
    before = [{"t": i * 5, "baro_altitude": 23000.0 / FT_PER_M, "velocity": 200.0,
               "vert_rate": 5.0} for i in range(30)]
    gap = HOLE_S if hole else 5
    after = _cruise(before[-1]["t"] + gap, n=30, alt_ft=35000.0, lon0=20.0)
    return _measured(make_track(spark, before + after))


def test_a_crossing_inside_a_hole_is_not_emitted(spark):
    sdf = add_piece(_climb_through_fl245(spark, hole=True), 1800.0)
    events = calculate_threshold_crossing_events(sdf, EventConfig())
    assert events.filter(F.col("type") == "xing-fl245").count() == 0


def test_a_crossing_between_ordinary_samples_is_still_emitted(spark):
    sdf = add_piece(_climb_through_fl245(spark, hole=False), 1800.0)
    events = calculate_threshold_crossing_events(sdf, EventConfig())
    assert events.filter(F.col("type") == "xing-fl245").count() == 1


# ---------------------------------------------------------------------------
# Cumulative distance
# ---------------------------------------------------------------------------

def _total(df, **kw):
    return add_distance_measure(df, **kw).agg(F.max("cumulative_distance_nm")).first()[0]


def test_distance_reads_the_last_known_position(spark):
    # Five samples one degree of longitude apart on the equator, the middle one
    # with no position: the track still covers four degrees, 240 NM.
    samples = [{"t": i * 60, "lat": 0.0, "lon": float(i)} for i in range(5)]
    samples[2]["lat"] = samples[2]["lon"] = None
    df = make_track(spark, samples)
    assert _total(df) == pytest.approx(240.0, rel=2e-3)
    # The published function lost the two segments around the blank.
    assert _total(df, last_known_position=False) == pytest.approx(120.0, rel=2e-3)


def test_a_position_with_one_coordinate_is_not_a_position(spark):
    samples = [{"t": i * 60, "lat": 0.0, "lon": float(i)} for i in range(5)]
    samples[2]["lon"] = None
    assert _total(make_track(spark, samples)) == pytest.approx(240.0, rel=2e-3)


def test_distance_adds_the_straight_line_across_a_hole(spark):
    # Blank positions at both edges of the hole, as cleaning leaves them.
    before = [{"t": i * 60, "lat": 0.0, "lon": float(i)} for i in range(3)]
    after = [{"t": HOLE_S + i * 60, "lat": 0.0, "lon": 30.0 + i} for i in range(3)]
    before[-1]["lat"] = before[-1]["lon"] = None
    after[0]["lat"] = after[0]["lon"] = None
    # Known positions run 0, 1, [hole], 31, 32: 32 degrees in all.
    assert _total(make_track(spark, before + after)) == pytest.approx(1920.0, rel=2e-3)


def test_a_blank_first_sample_adds_nothing(spark):
    samples = [{"t": i * 60, "lat": 0.0, "lon": float(i)} for i in range(3)]
    samples[0]["lat"] = samples[0]["lon"] = None
    assert _total(make_track(spark, samples)) == pytest.approx(60.0, rel=2e-3)


def test_distance_is_unchanged_where_every_position_is_known(spark):
    df = make_track(spark, [{"t": i * 60, "lat": 0.1 * i, "lon": float(i)} for i in range(6)])
    assert _total(df) == pytest.approx(_total(df, last_known_position=False), rel=1e-12)


def test_distance_version_follows_the_method():
    assert DISTANCE_VERSION == "distance_v0.0.3"
    assert distance_version(EventConfig()) == "distance_v0.0.3"
    # Reprocessing a released month reproduces its measurement, defect and all.
    assert LEGACY_DISTANCE_VERSION == "distance_v0.0.2"
    assert distance_version(EventConfig.legacy()) == "distance_v0.0.2"
    assert EventConfig.legacy().distance_from_last_known_position is False


def test_level_segment_distance_follows_the_distance_version(spark):
    """A level segment's ``distance_nm`` (and so the level-off events' ``info``) is a
    difference of ``cumulative_distance_nm``, which ``distance_v0.0.3`` corrects on
    every track with a blanked position, hole or not. The events' type and time do
    not move; the figure in ``info`` does, and the paper says so."""
    samples = _cruise(0, n=40, lat=0.0)
    for s in samples[10:30:2]:
        s["lat"] = s["lon"] = None          # every other position blanked mid-cruise
    df = make_track(spark, samples)

    def total_distance(last_known_position):
        measured = (
            add_distance_measure(df, last_known_position=last_known_position)
            .withColumn("baro_altitude_c", F.col("baro_altitude"))
            .withColumn("cumulative_time_s", F.lit(0).cast("long"))
        )
        segs = LEVEL_ARMS["pru"](measured, EventConfig()).collect()
        assert len(segs) == 1
        return segs[0]["distance_nm"]

    assert total_distance(False) < total_distance(True)
