"""Claiming tracks by the day they start.

Day-by-day processing needs each track to be owned by exactly one day, or the
same flight is processed twice (double-counted) or not at all (lost). The rule
is "the day the track's first sample falls in", and it has to survive the fact
that a batch reads well past midnight to capture flights that run over.

The window is asymmetric on purpose. The lookback only has to be longer than
the gap threshold, because breaks are decided locally: a track whose first
in-window sample lands after midnight with hours of silence in front of it
genuinely starts there. The lookahead has to cover the whole duration of a
track that starts on D, because that is the only direction in which being too
short silently truncates.
"""
import datetime as dt
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from conftest import make_track
from pyspark.sql import functions as F

from opdi.pipeline.tracks import keep_tracks_starting_in

HOUR = 3600
# conftest's arbitrary epoch, which make_track offsets from.
EPOCH = dt.datetime(2024, 6, 1, 12, 0)


def _at(seconds):
    return EPOCH + dt.timedelta(seconds=seconds)


def _claim(spark, samples, start, end):
    sv = make_track(spark, samples)
    return keep_tracks_starting_in(sv, start, end)


def test_a_track_starting_inside_the_day_is_kept(spark):
    out = _claim(spark, [{"t": 0, "track_id": "a"}, {"t": 60, "track_id": "a"}],
                 _at(-HOUR), _at(HOUR))
    assert {r["track_id"] for r in out.collect()} == {"a"}


def test_a_track_that_started_before_the_day_is_dropped_whole(spark):
    """The previous day already processed it. Keeping only its tail here would
    both duplicate those samples and invent a track that never departed."""
    samples = [{"t": -2 * HOUR, "track_id": "a"}, {"t": 60, "track_id": "a"}]
    out = _claim(spark, samples, _at(-HOUR), _at(HOUR))
    assert out.count() == 0


def test_a_track_starting_after_the_day_is_dropped(spark):
    """It belongs to tomorrow, which will read back far enough to find it."""
    samples = [{"t": 2 * HOUR, "track_id": "a"}, {"t": 2 * HOUR + 60, "track_id": "a"}]
    out = _claim(spark, samples, _at(-HOUR), _at(HOUR))
    assert out.count() == 0


def test_a_kept_track_keeps_every_sample_including_those_past_the_day(spark):
    """The whole point of the lookahead.

    A flight departing before the boundary and landing after it must be
    processed complete, by the day it started -- not truncated at midnight.
    """
    samples = [
        {"t": 0, "track_id": "a"},
        {"t": HOUR, "track_id": "a"},
        {"t": 5 * HOUR, "track_id": "a"},      # well past the claim window
    ]
    out = _claim(spark, samples, _at(-HOUR), _at(HOUR))
    assert out.count() == 3


def test_the_window_is_half_open_at_both_ends(spark):
    """Midnight belongs to the day that starts, not the one that ends.

    Two adjacent days must not both claim a track starting exactly on the
    boundary, and neither must skip it.
    """
    on_edge = [{"t": 0, "track_id": "a"}, {"t": 60, "track_id": "a"}]
    claimed_by_today = _claim(spark, on_edge, _at(0), _at(HOUR))
    claimed_by_yesterday = _claim(spark, on_edge, _at(-HOUR), _at(0))

    assert claimed_by_today.count() == 2
    assert claimed_by_yesterday.count() == 0


def test_two_adjacent_days_partition_the_tracks_between_them(spark):
    """Completeness, stated as a property: every track goes somewhere, once."""
    samples = [
        {"t": -30 * 60, "track_id": "yesterday"},
        {"t": 10, "track_id": "today_a"},
        {"t": 2 * HOUR, "track_id": "tomorrow"},
    ]
    today = _claim(spark, samples, _at(0), _at(HOUR))
    tomorrow = _claim(spark, samples, _at(HOUR), _at(2 * HOUR + 1))

    assert {r["track_id"] for r in today.collect()} == {"today_a"}
    assert {r["track_id"] for r in tomorrow.collect()} == {"tomorrow"}


# --- the lookahead, checked against reality ---------------------------------

def test_a_track_still_transmitting_at_the_window_edge_is_reported(spark):
    """The only way this scheme loses data, made visible.

    A track cut off by the read window is present, plausible, and simply
    stops -- and tomorrow will not pick it up, because it did not start
    tomorrow. Nothing downstream can tell. So the run counts them instead of
    trusting that the lookahead was long enough.
    """
    from opdi.pipeline.tracks import tracks_touching_window_end

    read_end = _at(6 * HOUR)
    samples = [
        {"t": 0, "track_id": "landed"},
        {"t": HOUR, "track_id": "landed"},
        {"t": 0, "track_id": "cut_off"},
        {"t": 6 * HOUR - 10, "track_id": "cut_off"},   # still going at the edge
    ]
    sv = make_track(spark, samples)

    flagged = {r["track_id"] for r in tracks_touching_window_end(sv, read_end).collect()}
    assert flagged == {"cut_off"}


def test_a_run_whose_lookahead_was_enough_reports_nothing(spark):
    """A healthy run must be quiet, or the warning becomes noise people learn
    to ignore."""
    from opdi.pipeline.tracks import tracks_touching_window_end

    sv = make_track(spark, [{"t": 0, "track_id": "a"}, {"t": HOUR, "track_id": "a"}])
    assert tracks_touching_window_end(sv, _at(6 * HOUR)).count() == 0


# --- the lookback, under an arm that links across holes -----------------------

def _processor_capturing_windows(monkeypatch, method, **segmentation):
    """A TrackProcessor with no IO, recording the windows `process_day` reads."""
    from opdi import pipeline
    from opdi.config import SegmentationConfig
    from opdi.pipeline.tracks import TrackProcessor

    monkeypatch.setattr(
        pipeline.tracks,
        "tracks_touching_window_end",
        lambda df, read_end: SimpleNamespace(count=lambda: 0),
    )
    processor = TrackProcessor.__new__(TrackProcessor)
    processor.config = SimpleNamespace(
        segmentation=SegmentationConfig(method=method, **segmentation)
    )
    processor.storage = MagicMock()
    processor.windows = []
    processor.process_month = lambda **kw: processor.windows.append(kw["window"])
    return processor


def test_cruise_linked_reads_back_past_its_longest_gap():
    """A linked track's old piece is at most `link_max_gap_minutes` before the new."""
    from opdi.pipeline.segmentation import SegmentationParams
    from opdi.pipeline.tracks import required_lookback_hours

    p = SegmentationParams()
    assert required_lookback_hours("cruise_linked", p) == 7
    assert required_lookback_hours("debounced", p) == 3
    assert required_lookback_hours("standard", p) == 3
    assert required_lookback_hours("legacy", p) == 3

    p120 = SegmentationParams(link_max_gap_minutes=120.0)
    assert required_lookback_hours("cruise_linked", p120) == 3
    # A part-hour rounds up.
    p241 = SegmentationParams(link_max_gap_minutes=241.0)
    assert required_lookback_hours("cruise_linked", p241) == 6


def test_process_day_reads_the_lookback_its_arm_requires(spark, monkeypatch):
    day = dt.date(2024, 6, 2)

    linked = _processor_capturing_windows(monkeypatch, "cruise_linked")
    linked.process_day(day)
    assert linked.windows[0][0] == "2024-06-01 17:00:00"

    wider = _processor_capturing_windows(monkeypatch, "cruise_linked")
    wider.process_day(day, lookback_hours=9)
    assert wider.windows[0][0] == "2024-06-01 15:00:00"

    default = _processor_capturing_windows(monkeypatch, "standard")
    default.process_day(day)
    assert default.windows[0][0] == "2024-06-01 21:00:00"


def test_too_short_an_explicit_lookback_raises(spark, monkeypatch):
    processor = _processor_capturing_windows(monkeypatch, "cruise_linked")
    with pytest.raises(ValueError, match="lookback_hours=6.*7"):
        processor.process_day(dt.date(2024, 6, 2), lookback_hours=6)
    assert processor.windows == []


def test_other_arms_keep_their_explicit_lookback(spark, monkeypatch):
    """The guard is about linking. Other arms' breaks are local, so one hour is fine."""
    processor = _processor_capturing_windows(monkeypatch, "debounced")
    processor.process_day(dt.date(2024, 6, 2), lookback_hours=1)
    assert processor.windows[0][0] == "2024-06-01 23:00:00"


def _claimed_by_day(sv, day_start, lookback_hours):
    """One day of `process_day`: read wide, segment, keep the tracks starting in it."""
    from opdi.pipeline.segmentation import SegmentationParams, assign_track_id
    from opdi.pipeline.segmentation.methods import cruise_linked

    day_end = day_start + dt.timedelta(days=1)
    read = sv.filter(
        (F.col("event_time") >= F.lit(day_start - dt.timedelta(hours=lookback_hours)))
        & (F.col("event_time") < F.lit(day_end + dt.timedelta(hours=24)))
    )
    tracks = assign_track_id(read, cruise_linked(), SegmentationParams())
    return keep_tracks_starting_in(tracks, day_start, day_end)


def test_a_piece_after_midnight_is_claimed_once(spark):
    """Old piece 20:50-20:55 on D-1, new piece 00:10-00:20 on D: one linked flight.

    Read back only 3 h, D never sees the old piece, takes the new one for a track
    starting on D, and claims it while D-1 claims the same samples inside the
    linked track: published twice under two ids.
    """
    from opdi.pipeline.segmentation import SegmentationParams
    from opdi.pipeline.tracks import required_lookback_hours

    d_minus_1 = dt.datetime(2024, 6, 1)
    d = dt.datetime(2024, 6, 2)
    at = lambda moment: (moment - EPOCH).total_seconds()   # noqa: E731
    cruise = {"lat": 0.0, "baro_altitude": 11000.0, "vert_rate": 0.0, "callsign": "DLH1"}

    old_start = at(d_minus_1 + dt.timedelta(hours=20, minutes=50))
    new_start = at(d + dt.timedelta(minutes=10))
    gap_min = (new_start - (old_start + 5 * 60)) / 60
    samples = [
        {**cruise, "t": old_start + i * 60, "lon": 0.0} for i in range(6)
    ] + [
        # 480 kt across the hole, on the equator.
        {**cruise, "t": new_start + i * 60, "lon": 480.0 * gap_min / 60 / 60.04}
        for i in range(11)
    ]
    sv = make_track(spark, samples)

    hours = required_lookback_hours("cruise_linked", SegmentationParams())
    claims = [
        r["last_contact"]
        for day in (d_minus_1, d)
        for r in _claimed_by_day(sv, day, hours).select("last_contact").collect()
    ]
    assert sorted(claims) == sorted(s["t"] for s in samples)

    # The same two days at A9's lookback: the duplicate this guard exists to stop.
    too_short = [
        r["last_contact"]
        for day in (d_minus_1, d)
        for r in _claimed_by_day(sv, day, 3).select("last_contact").collect()
    ]
    assert len(too_short) == len(samples) + 11
