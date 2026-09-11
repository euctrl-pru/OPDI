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

import pytest
from conftest import make_track

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
