"""Claiming tracks by the day they start.

Day-by-day processing needs each track to be owned by exactly one day, or the
same flight is processed twice (double-counted) or not at all (lost). The rule
is "the day the track's first sample falls in", and it has to survive the fact
that a batch reads well past midnight to capture flights that run over.

The window is asymmetric on purpose. Under most arms the lookback only has to be
longer than the gap threshold, because breaks are decided locally: a track whose
first in-window sample lands after midnight with hours of silence in front of it
genuinely starts there. ``cruise_linked`` is the exception. It can join across
hours of silence, so its lookback has to reach the old edge of the longest
linkable hole and the callsign evidence behind that edge, or two days' reads
decide one hole differently; see ``required_lookback_hours``. The lookahead has
to cover the whole duration of a track that starts on D, because that is the only
direction in which being too short silently truncates.
"""

import datetime as dt
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from conftest import make_track
from pyspark.sql import SparkSession
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
    out = _claim(
        spark, [{"t": 0, "track_id": "a"}, {"t": 60, "track_id": "a"}], _at(-HOUR), _at(HOUR)
    )
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
        {"t": 5 * HOUR, "track_id": "a"},  # well past the claim window
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
        {"t": 6 * HOUR - 10, "track_id": "cut_off"},  # still going at the edge
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


def _pre_study():
    """The settings these fixtures were sized for: a 6 h gap limit, so a 7 h read."""
    from opdi.pipeline.segmentation import SegmentationParams

    return SegmentationParams(
        link_max_gap_minutes=360.0,
        link_cruise_floor_ft=25000.0,
        link_min_implied_speed_kt=315.0,
    )


PRE_STUDY = _pre_study()


def _processor_capturing_windows(method, **segmentation):
    """A TrackProcessor with no IO, recording the windows `process_day` reads."""
    from opdi.config import SegmentationConfig
    from opdi.pipeline.tracks import TrackProcessor

    processor = TrackProcessor.__new__(TrackProcessor)
    processor.config = SimpleNamespace(
        segmentation=SegmentationConfig(method=method, **segmentation)
    )
    processor.storage = MagicMock()
    # Nothing written, so the truncation check has a real frame to report on.
    processor.storage.read_table.return_value = make_track(
        SparkSession.getActiveSession(), [{"t": 0}]
    ).withColumn("dof", F.lit(dt.date(1999, 1, 1)))
    processor.windows = []
    processor.process_month = lambda **kw: processor.windows.append(kw["window"])
    return processor


def test_cruise_linked_reads_back_past_its_longest_gap():
    """A linked track's old piece is at most `link_max_gap_minutes` before the new."""
    from opdi.pipeline.segmentation import SegmentationParams
    from opdi.pipeline.tracks import required_lookback_hours

    p = SegmentationParams()
    assert required_lookback_hours("cruise_linked", p) == 11
    assert required_lookback_hours("legacy", p) == 3
    assert required_lookback_hours("standard", p) == 11  # standard is cruise_linked
    assert required_lookback_hours("legacy", p) == 3

    p120 = SegmentationParams(link_max_gap_minutes=120.0)
    assert required_lookback_hours("cruise_linked", p120) == 3
    # A part-hour rounds up.
    p241 = SegmentationParams(link_max_gap_minutes=241.0)
    assert required_lookback_hours("cruise_linked", p241) == 6
    # The callsign lookback is added on top, also rounded up, with a floor at 3.
    p90 = SegmentationParams(link_max_gap_minutes=360.0, link_callsign_lookback_minutes=90.0)
    assert required_lookback_hours("cruise_linked", p90) == 8
    short = SegmentationParams(link_max_gap_minutes=60.0)
    assert required_lookback_hours("cruise_linked", short) == 3
    # The read behind the old edge is the longer of the callsign and position reads.
    far_position = SegmentationParams(link_max_gap_minutes=360.0, link_edge_position_minutes=90.0)
    assert required_lookback_hours("cruise_linked", far_position) == 8
    no_callsign = SegmentationParams(link_max_gap_minutes=360.0, link_callsign_lookback_minutes=0.0)
    assert required_lookback_hours("cruise_linked", no_callsign) == 7
    neither = SegmentationParams(
        link_max_gap_minutes=360.0,
        link_callsign_lookback_minutes=0.0,
        link_edge_position_minutes=0.0,
    )
    # The callsign guard's memory is the longest hole plus the 30 s hold, so even
    # with nothing else read behind the edge a part-hour is added for it.
    assert required_lookback_hours("cruise_linked", neither) == 7
    assert required_lookback_hours("legacy", far_position) == 3


def test_process_day_reads_the_lookback_its_arm_requires(spark):
    day = dt.date(2024, 6, 2)

    linked = _processor_capturing_windows("cruise_linked")
    linked.process_day(day)
    assert linked.windows[0][0] == "2024-06-01 13:00:00"

    wider = _processor_capturing_windows("cruise_linked")
    wider.process_day(day, lookback_hours=13)
    assert wider.windows[0][0] == "2024-06-01 11:00:00"

    # The default run reads 11 hours back: `standard` is `cruise_linked`.
    default = _processor_capturing_windows("standard")
    default.process_day(day)
    assert default.windows[0][0] == "2024-06-01 13:00:00"

    # A rule that does not link holes, such as `legacy`, reads three hours.
    previous = _processor_capturing_windows("legacy")
    previous.process_day(day)
    assert previous.windows[0][0] == "2024-06-01 21:00:00"


def test_too_short_an_explicit_lookback_raises(spark):
    processor = _processor_capturing_windows("cruise_linked")
    with pytest.raises(ValueError, match="lookback_hours=10.*11"):
        processor.process_day(dt.date(2024, 6, 2), lookback_hours=10)
    assert processor.windows == []


def test_other_arms_keep_their_explicit_lookback(spark):
    """The guard is about linking. Other arms' breaks are local, so one hour is fine."""
    processor = _processor_capturing_windows("legacy")
    processor.process_day(dt.date(2024, 6, 2), lookback_hours=1)
    assert processor.windows[0][0] == "2024-06-01 23:00:00"


def _claimed_by_day(sv, day_start, lookback_hours, p=None):
    """One day of `process_day`: read wide, segment, keep the tracks starting in it."""
    from opdi.pipeline.segmentation import assign_track_id
    from opdi.pipeline.segmentation.methods import cruise_linked

    day_end = day_start + dt.timedelta(days=1)
    read = sv.filter(
        (F.col("event_time") >= F.lit(day_start - dt.timedelta(hours=lookback_hours)))
        & (F.col("event_time") < F.lit(day_end + dt.timedelta(hours=24)))
    )
    tracks = assign_track_id(read, cruise_linked(), p or PRE_STUDY)
    return keep_tracks_starting_in(tracks, day_start, day_end)


def test_a_piece_after_midnight_is_claimed_once(spark):
    """Old piece 20:50-20:55 on D-1, new piece 00:10-00:20 on D: one linked flight.

    Read back only 3 h, D never sees the old piece, takes the new one for a track
    starting on D, and claims it while D-1 claims the same samples inside the
    linked track: published twice under two ids.
    """
    from opdi.pipeline.tracks import required_lookback_hours

    d_minus_1 = dt.datetime(2024, 6, 1)
    d = dt.datetime(2024, 6, 2)
    at = lambda moment: (moment - EPOCH).total_seconds()  # noqa: E731
    cruise = {"lat": 0.0, "baro_altitude": 11000.0, "vert_rate": 0.0, "callsign": "DLH1"}

    old_start = at(d_minus_1 + dt.timedelta(hours=20, minutes=50))
    new_start = at(d + dt.timedelta(minutes=10))
    gap_min = (new_start - (old_start + 5 * 60)) / 60
    samples = [{**cruise, "t": old_start + i * 60, "lon": 0.0} for i in range(6)] + [
        # 480 kt across the hole, on the equator.
        {**cruise, "t": new_start + i * 60, "lon": 480.0 * gap_min / 60 / 60.04}
        for i in range(11)
    ]
    sv = make_track(spark, samples)

    hours = required_lookback_hours("cruise_linked", PRE_STUDY)
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


D_MINUS_1 = dt.datetime(2024, 6, 1)
D = dt.datetime(2024, 6, 2)
CRUISE = {"lat": 0.0, "baro_altitude": 11000.0, "vert_rate": 0.0}


def _t(moment):
    return (moment - EPOCH).total_seconds()


def _every(start, end, minutes, **fields):
    """Cruise samples from *start* up to and including *end*."""
    out = []
    moment = start
    while moment <= end:
        out.append({**CRUISE, **fields, "t": _t(moment)})
        moment += dt.timedelta(minutes=minutes)
    return out


def _lon_after(gap_minutes):
    """Equator longitude reached in a hole of that length at 480 kt."""
    return 480.0 * gap_minutes / 60 / 60.04


def _assert_claimed_once(spark, samples, p=PRE_STUDY):
    from opdi.pipeline.tracks import required_lookback_hours

    sv = make_track(spark, samples)
    hours = required_lookback_hours("cruise_linked", p)
    claims = [
        r["last_contact"]
        for day in (D_MINUS_1, D)
        for r in _claimed_by_day(sv, day, hours, p).select("last_contact").collect()
    ]
    assert sorted(claims) == sorted(s["t"] for s in samples)


def test_a_callsign_older_than_the_read_cannot_lose_a_piece(spark):
    """DLH1 until 13:00, blank to 22:00, a 130 min hole, BAW2 from 00:10.

    If the old callsign counted without a bound, D-1's read would see DLH1 against
    BAW2 and split, while D's read, starting at 17:00, would see blank against BAW2
    and link, so the track would start on D-1 and D would not claim it. The piece
    after the hole would then be published by no day.
    """
    hours = lambda h, m=0: dt.timedelta(hours=h, minutes=m)  # noqa: E731
    samples = _every(D_MINUS_1 + hours(12), D_MINUS_1 + hours(13), 10, lon=0.0, callsign="DLH1")
    samples += _every(D_MINUS_1 + hours(13, 10), D_MINUS_1 + hours(22), 10, lon=0.0, callsign="")
    samples += _every(D + hours(0, 10), D + hours(0, 30), 2, lon=_lon_after(130), callsign="BAW2")
    _assert_claimed_once(spark, samples)


def test_a_recent_callsign_splits_in_both_reads(spark):
    """DLH1 until 21:30, so it counts in both reads: D claims the BAW2 piece."""
    hours = lambda h, m=0: dt.timedelta(hours=h, minutes=m)  # noqa: E731
    samples = _every(D_MINUS_1 + hours(20), D_MINUS_1 + hours(21, 30), 10, lon=0.0, callsign="DLH1")
    samples += _every(D_MINUS_1 + hours(21, 40), D_MINUS_1 + hours(22), 10, lon=0.0, callsign="")
    samples += _every(D + hours(0, 10), D + hours(0, 30), 2, lon=_lon_after(130), callsign="BAW2")
    _assert_claimed_once(spark, samples)


def test_the_furthest_evidence_a_hole_can_use_is_inside_the_read(spark):
    """The longest hole, ending at midnight, with the callsign at the lookback's limit.

    Old edge 18:00, DLH1 last seen 17:00, exactly where D's 7 h read begins. Both
    reads must see it, and split.
    """
    hours = lambda h, m=0: dt.timedelta(hours=h, minutes=m)  # noqa: E731
    samples = _every(D_MINUS_1 + hours(15), D_MINUS_1 + hours(17), 10, lon=0.0, callsign="DLH1")
    samples += _every(D_MINUS_1 + hours(17, 10), D_MINUS_1 + hours(18), 10, lon=0.0, callsign="")
    samples += _every(D, D + hours(0, 20), 2, lon=_lon_after(360), callsign="BAW2")
    _assert_claimed_once(spark, samples)


def test_positions_read_through_blank_edges_decide_alike_at_the_required_lookback(spark):
    """The longest hole, ending at midnight, with the old edge's position 5 min behind it.

    Old edge 18:00, its last position 17:55, new edge 00:00, its first position 00:05.
    D's 7 h read begins at 17:00, so both reads see that position, link, and D claims
    nothing.
    """
    from opdi.pipeline.tracks import required_lookback_hours

    p = PRE_STUDY
    hours = lambda h, m=0: dt.timedelta(hours=h, minutes=m)  # noqa: E731
    blank = {"lat": None, "lon": None}
    samples = _every(
        D_MINUS_1 + hours(17, 30), D_MINUS_1 + hours(17, 55), 5, lon=0.0, callsign="DLH1"
    )
    samples += _every(D_MINUS_1 + hours(17, 56), D_MINUS_1 + hours(18), 1, callsign="DLH1", **blank)
    samples += _every(D, D + hours(0, 4), 1, callsign="DLH1", **blank)
    samples += _every(D + hours(0, 5), D + hours(0, 20), 5, lon=_lon_after(370), callsign="DLH1")

    lookback = required_lookback_hours("cruise_linked", p)
    assert lookback == 7
    _assert_claimed_once(spark, samples)

    sv = make_track(spark, samples)
    assert _claimed_by_day(sv, D_MINUS_1, lookback).select("track_id").distinct().count() == 1
    assert _claimed_by_day(sv, D, lookback).count() == 0

    # An hour short, D's read starts at the old edge and misses the position, so D
    # takes the new piece for a new track while D-1 claims it too. This is what the
    # lookback prevents, so the assertions above are not passing by accident.
    assert _claimed_by_day(sv, D, lookback - 1).count() == len(
        _every(D, D + hours(0, 4), 1) + _every(D + hours(0, 5), D + hours(0, 20), 5)
    )


@pytest.mark.parametrize(
    "settings, position_min_before_edge",
    [
        # The position window reaches past the callsign lookback. It must stay under
        # the low-altitude gap, so that gap is raised with it.
        ({"link_edge_position_minutes": 90.0, "low_alt_gap_minutes": 120.0}, 90),
        # No callsign lookback at all, which must not take the position's read with it.
        ({"link_callsign_lookback_minutes": 0.0}, 5),
    ],
)
def test_the_old_edge_position_is_always_inside_the_read(spark, settings, position_min_before_edge):
    """The longest hole, ending at midnight, with the position at the window's limit.

    Both reads must see it and link. A lookback an hour shorter starts at or after the
    position, so D would take the new piece for a new track while D-1 claims it too.
    """
    from opdi.pipeline.tracks import required_lookback_hours

    p = replace(PRE_STUDY, **settings)
    hours = lambda h, m=0: dt.timedelta(hours=h, minutes=m)  # noqa: E731
    old_edge = D_MINUS_1 + hours(18)
    position_at = old_edge - dt.timedelta(minutes=position_min_before_edge)
    blank = {"lat": None, "lon": None}
    samples = _every(position_at - hours(0, 10), position_at, 5, lon=0.0, callsign="DLH1")
    samples += _every(position_at + hours(0, 1), old_edge, 1, callsign="DLH1", **blank)
    new_piece = _every(
        D, D + hours(0, 20), 5, callsign="DLH1", lon=_lon_after(360 + position_min_before_edge)
    )
    samples += new_piece
    sv = make_track(spark, samples)

    lookback = required_lookback_hours("cruise_linked", p)
    assert _claimed_by_day(sv, D_MINUS_1, lookback, p).select("track_id").distinct().count() == 1
    assert _claimed_by_day(sv, D_MINUS_1, lookback, p).count() == len(samples)
    assert _claimed_by_day(sv, D, lookback, p).count() == 0

    assert _claimed_by_day(sv, D, lookback - 1, p).count() == len(new_piece)


def test_a_low_sample_across_an_earlier_break_cannot_lose_a_piece(spark):
    """At the shipped settings: 3,000 ft at 11:00, cruise at 14:00, a 590 min hole, cruise at 00:00.

    The five rows before the long hole reach across the earlier break, into the 3,000 ft
    samples. Row frames alone, D-1's read sees those and splits, while D's read, starting
    at 13:00, sees only cruise and links. The track then starts on D-1, so D claims
    nothing, and the ten samples after the hole are published by neither day.
    """
    from opdi.pipeline.segmentation import SegmentationParams
    from opdi.pipeline.tracks import required_lookback_hours

    p = SegmentationParams()
    hours = lambda h, m=0: dt.timedelta(hours=h, minutes=m)  # noqa: E731
    samples = _every(
        D_MINUS_1 + hours(11),
        D_MINUS_1 + hours(11, 2),
        1,
        lon=0.0,
        callsign="DLH1",
        baro_altitude=914.4,
    )
    samples += _every(D_MINUS_1 + hours(14), D_MINUS_1 + hours(14, 10), 5, lon=0.0, callsign="DLH1")
    samples += _every(D, D + hours(0, 9), 1, lon=_lon_after(590), callsign="DLH1")
    assert len(samples) == 16
    assert required_lookback_hours("cruise_linked", p) == 11

    _assert_claimed_once(spark, samples, p)

    # Both reads link the long hole, so D-1 holds the low piece and the whole linked track.
    sv = make_track(spark, samples)
    claimed = _claimed_by_day(sv, D_MINUS_1, 11, p)
    assert claimed.count() == 16
    assert claimed.select("track_id").distinct().count() == 2
    assert _claimed_by_day(sv, D, 11, p).count() == 0


@pytest.mark.parametrize(
    "last_altitude_before_edge_min, tracks",
    [
        (4, 1),  # inside the window, so both reads hold it and link
        (70, 2),  # past it, and past D's read: neither may use it, so both split
    ],
)
def test_two_days_decide_alike_with_blank_altitudes(spark, last_altitude_before_edge_min, tracks):
    """Old edge 14:00 on D-1, a 600 min hole, new edge at midnight, altitude blank at both edges.

    D-1's read starts 13:00 the day before D's does, so at 70 min a read past blanks with
    no time bound holds the 12:50 altitude in D-1's read and nothing in D's. D-1 would then
    link what D splits, and the piece after the hole would be claimed by both or neither.
    """
    from opdi.pipeline.segmentation import SegmentationParams
    from opdi.pipeline.tracks import required_lookback_hours

    p = SegmentationParams()
    hours = lambda h, m=0: dt.timedelta(hours=h, minutes=m)  # noqa: E731
    old_edge = D_MINUS_1 + hours(14)
    last_altitude = old_edge - dt.timedelta(minutes=last_altitude_before_edge_min)
    assert required_lookback_hours("cruise_linked", p) == 11

    blank = {"lon": 0.0, "callsign": "DLH1", "baro_altitude": None}
    samples = _every(last_altitude - hours(0, 10), last_altitude, 1, lon=0.0, callsign="DLH1")
    samples += _every(last_altitude + hours(0, 1), old_edge, 1, **blank)
    new_piece = _every(
        D, D + hours(0, 3), 1, lon=_lon_after(600), callsign="DLH1", baro_altitude=None
    )
    new_piece += _every(D + hours(0, 4), D + hours(0, 9), 1, lon=_lon_after(604), callsign="DLH1")
    samples += new_piece

    _assert_claimed_once(spark, samples, p)

    sv = make_track(spark, samples)
    linked = tracks == 1
    assert _claimed_by_day(sv, D, 11, p).count() == (0 if linked else len(new_piece))
    assert _claimed_by_day(sv, D_MINUS_1, 11, p).count() == (
        len(samples) if linked else len(samples) - len(new_piece)
    )


def test_a_low_climbing_old_edge_before_midnight_is_claimed_once(spark):
    """Old edge 23:58 on D-1 at 8,000 ft and climbing, a 600 min hole, new edge at cruise on D.

    The rule accepts a climbing old edge at any altitude, so this links, and the piece on
    D belongs to the track D-1 owns. Both reads must hold the climb to agree on that.
    """
    from opdi.pipeline.segmentation import SegmentationParams

    p = SegmentationParams()
    old_edge = D - dt.timedelta(minutes=2)
    climb = [
        {
            **CRUISE,
            "lon": 0.0,
            "callsign": "DLH1",
            "t": _t(old_edge - dt.timedelta(minutes=m)),
            "baro_altitude": (8000.0 - 1000.0 * m) * 0.3048,
            "vert_rate": 1000.0 / 196.850394,
        }
        for m in range(5, -1, -1)
    ]
    new_edge = old_edge + dt.timedelta(minutes=600)
    new_piece = _every(
        new_edge, new_edge + dt.timedelta(minutes=10), 1, lon=_lon_after(600), callsign="DLH1"
    )
    samples = climb + new_piece

    _assert_claimed_once(spark, samples, p)

    sv = make_track(spark, samples)
    assert _claimed_by_day(sv, D, 11, p).count() == 0
    assert _claimed_by_day(sv, D_MINUS_1, 11, p).count() == len(samples)


# --- the truncation warning, under an arm that links across holes -------------


def _straddling_read_end(spark):
    """What D-1 writes when a 240 min hole straddles its read end.

    Cruise from 13:00 on D-1 to 21:00 on D, then a hole, then samples from 01:00 on
    D+1. D-1 reads to 00:00 on D+1, so the track's last read sample is 3 h short of
    the edge. The piece after the hole is lost, and no other day will claim it.
    """
    from pyspark.sql.functions import lit

    hours = lambda h: dt.timedelta(hours=h)  # noqa: E731
    samples = _every(D_MINUS_1 + hours(13), D + hours(21), 20, lon=0.0, callsign="DLH1")
    samples += _every(D + hours(25), D + hours(26), 5, lon=_lon_after(240), callsign="DLH1")
    written = _claimed_by_day(make_track(spark, samples), D_MINUS_1, 7)
    return written.withColumn("dof", lit(D_MINUS_1.date()))


def _warning_from_process_day(capsys, written, method, **segmentation):
    processor = _processor_capturing_windows(method, **segmentation)
    processor.storage.read_table.return_value = written
    processor.process_day(D_MINUS_1.date())
    return capsys.readouterr().out


def test_a_hole_straddling_the_read_end_is_reported(spark, capsys):
    out = _warning_from_process_day(capsys, _straddling_read_end(spark), "cruise_linked")
    assert "WARNING: 1 track(s)" in out
    assert "covered every track" not in out


def test_the_margin_follows_the_longest_linkable_hole(spark, capsys):
    """Allowing only 120 min, a track 3 h short of the edge cannot be waiting on a link."""
    out = _warning_from_process_day(
        capsys,
        _straddling_read_end(spark),
        "cruise_linked",
        link_max_gap_minutes=120.0,
    )
    assert "WARNING" not in out
    assert "covered every track" in out


@pytest.mark.parametrize(
    "lookahead_min, edge_min, warned",
    [
        (10.0, 5.0, True),  # 175 + 10 reaches the track, 3 h short of the read end
        (3.0, 8.0, True),  # 175 + 8: the longer of the two is the one added
        (3.0, 1.0, False),  # 175 + 3 does not
    ],
)
def test_the_margin_adds_what_the_decision_reads_past_the_new_edge(
    spark, capsys, lookahead_min, edge_min, warned
):
    """The hole's verdict also reads samples after its new edge, which must be in the read."""
    out = _warning_from_process_day(
        capsys,
        _straddling_read_end(spark),
        "cruise_linked",
        link_max_gap_minutes=175.0,
        link_callsign_lookahead_minutes=lookahead_min,
        link_edge_position_minutes=edge_min,
    )
    assert ("WARNING: 1 track(s)" in out) is warned


def test_other_arms_keep_the_60_second_margin(spark, capsys):
    out = _warning_from_process_day(capsys, _straddling_read_end(spark), "legacy")
    assert "WARNING" not in out
    assert "covered every track" in out


def test_the_wide_margin_says_the_track_may_simply_be_long(spark, capsys):
    """Ten hours of margin catches healthy long tracks too, so it must not claim a loss."""
    out = _warning_from_process_day(capsys, _straddling_read_end(spark), "cruise_linked")
    assert "Their tails may be missing, or the track is simply long" in out


def test_other_arms_still_say_the_tail_may_be_missing(spark, capsys):
    from pyspark.sql.functions import lit

    written = make_track(
        spark,
        [{"t": _t(D_MINUS_1 + dt.timedelta(hours=47, minutes=59, seconds=30)), "track_id": "a"}],
    ).withColumn("dof", lit(D_MINUS_1.date()))
    out = _warning_from_process_day(capsys, written, "legacy")
    assert "WARNING: 1 track(s)" in out
    assert "Their tails may be missing and no other day will claim them" in out
    assert "simply long" not in out


# --- the callsign guard remembers only what every day's read holds -------------------
#
# A9's flicker guard asks whether a new callsign persists, against the callsign it saw
# before -- however long ago. One day's read can still hold a callsign that the next
# day's read starts after, and the two then disagree about a break after midnight.
# Under ``cruise_linked`` a hole hides the callsign for hours, which makes that easy to
# reach. A10 therefore forgets a callsign not heard for longer than the longest hole it
# can link, which every day's read covers.


def _shipped():
    from opdi.pipeline.segmentation import SegmentationParams

    return SegmentationParams()


def _old_piece_then_a_long_hole():
    """DLH1 until 12:50 on D-1, no callsign to 14:10, then a 595 min hole to 00:05 on D.

    Day D reads from 13:00 on D-1, so it never sees DLH1.
    """
    h = lambda hours, minutes=0: dt.timedelta(hours=hours, minutes=minutes)  # noqa: E731
    old = _every(D_MINUS_1 + h(12, 30), D_MINUS_1 + h(12, 50), 5, lon=0.0, callsign="DLH1")
    old += _every(D_MINUS_1 + h(12, 55), D_MINUS_1 + h(14, 10), 5, lon=0.0, callsign="")
    return old, _lon_after(595)


def _after(seconds_from, seconds_to, step_s, lon, callsign):
    """Cruise samples from 00:05:00 on D plus *seconds_from*, every *step_s* seconds."""
    start = D + dt.timedelta(minutes=5)
    return _every(
        start + dt.timedelta(seconds=seconds_from),
        start + dt.timedelta(seconds=seconds_to),
        step_s / 60.0,
        lon=lon,
        callsign=callsign,
    )


def test_a_flicker_as_the_first_callsign_after_a_hole_is_claimed_once(spark):
    """After the hole: one garbled sample, then DLH1 again.

    D-1 still held DLH1 from before the hole and saw a flicker; D saw the garble as the
    first callsign there is, and broke at the revert. The samples after it were written
    by both days.
    """
    old, lon = _old_piece_then_a_long_hole()
    new = _after(0, 0, 5, lon, "DLH7X") + _after(5, 60, 5, lon, "DLH1")
    new += _after(120, 900, 60, lon, "DLH1")
    _assert_claimed_once(spark, old + new, _shipped())


def test_a_callsign_that_flickers_in_its_first_seconds_after_a_hole_is_claimed_once(spark):
    """After the hole: KLM2 for 20 s, one garbled sample, KLM2 again.

    D-1 broke at the second KLM2 run and kept what was before it; D saw no change and
    left the track to D-1. The samples from the second run on were written by neither.
    """
    old, lon = _old_piece_then_a_long_hole()
    new = _after(0, 15, 5, lon, "KLM2") + _after(20, 20, 5, lon, "KLM7X")
    new += _after(25, 60, 5, lon, "KLM2") + _after(120, 900, 60, lon, "KLM2")
    _assert_claimed_once(spark, old + new, _shipped())


def test_a_callsign_both_reads_hold_still_hides_a_flicker_after_a_hole(spark):
    """DLH1 until 14:10, no callsign to 15:20, a 525 min hole, a garble, DLH1 again.

    The callsign is over an hour before the hole, so the hole links on a blank; it is
    inside the memory and inside both reads, so the garble is a flicker in both.
    """
    from opdi.pipeline.segmentation import assign_track_id
    from opdi.pipeline.segmentation.methods import cruise_linked

    h = lambda hours, minutes=0: dt.timedelta(hours=hours, minutes=minutes)  # noqa: E731
    old = _every(D_MINUS_1 + h(13, 30), D_MINUS_1 + h(14, 10), 5, lon=0.0, callsign="DLH1")
    old += _every(D_MINUS_1 + h(14, 15), D_MINUS_1 + h(15, 20), 5, lon=0.0, callsign="")
    lon = _lon_after(525)
    new = _after(0, 0, 5, lon, "DLH7X") + _after(5, 60, 5, lon, "DLH1")
    samples = old + new
    _assert_claimed_once(spark, samples, _shipped())
    tracks = assign_track_id(make_track(spark, samples), cruise_linked(), _shipped())
    assert tracks.select("track_id").distinct().count() == 1


@pytest.mark.parametrize(
    "overrides",
    [
        {},
        {"link_max_gap_minutes": 120.0},
        {"link_max_gap_minutes": 241.0},
        {"link_callsign_lookback_minutes": 0.0, "link_edge_position_minutes": 0.0},
        {"link_max_gap_minutes": 900.0, "link_callsign_lookback_minutes": 5.0},
    ],
)
def test_the_callsign_memory_is_inside_every_days_read(overrides):
    """The memory, plus the 30 s a callsign needs to persist, never reaches past the read."""
    from opdi.pipeline.segmentation.methods import callsign_memory_minutes
    from opdi.pipeline.tracks import required_lookback_hours

    p = replace(_shipped(), **overrides)
    reach = callsign_memory_minutes(p) + p.callsign_min_persistence_seconds / 60.0
    assert reach <= required_lookback_hours("cruise_linked", p) * 60


def test_only_the_aircraft_seen_on_the_day_are_kept_with_all_their_reports(spark):
    from opdi.pipeline.tracks import keep_aircraft_seen_in

    day = dt.datetime(2026, 6, 14)
    rows = [
        ("a", dt.datetime(2026, 6, 13, 22)),  # seen on the day as well: kept whole
        ("a", dt.datetime(2026, 6, 14, 1)),
        ("a", dt.datetime(2026, 6, 15, 9)),
        ("b", dt.datetime(2026, 6, 13, 23)),  # never on the day: left out
        ("b", dt.datetime(2026, 6, 15, 0)),  # midnight belongs to the next day
        ("c", dt.datetime(2026, 6, 14, 0)),  # midnight belongs to the day
    ]
    df = spark.createDataFrame(rows, "icao24 string, event_time timestamp")
    kept = keep_aircraft_seen_in(df, day, day + dt.timedelta(days=1))
    assert sorted((r.icao24, r.event_time) for r in kept.collect()) == sorted(
        r for r in rows if r[0] in ("a", "c")
    )


def test_leaving_out_the_aircraft_not_seen_on_the_day_keeps_the_tracks(spark):
    """The tracks claimed by a day are the same with and without the aircraft filter."""
    from opdi.pipeline.tracks import keep_aircraft_seen_in

    day = dt.datetime(2026, 6, 14)
    end = day + dt.timedelta(days=1)
    rows = []
    for icao, starts in (("a", [-5, 2, 23.5]), ("b", [-10, 26]), ("c", [0, 30])):
        for k, h in enumerate(starts):
            for m in range(0, 60, 5):
                rows.append((icao, f"{icao}_{k}", day + dt.timedelta(hours=h, minutes=m)))
    df = spark.createDataFrame(rows, "icao24 string, track_id string, event_time timestamp")
    plain = keep_tracks_starting_in(df, day, end)
    filtered = keep_tracks_starting_in(keep_aircraft_seen_in(df, day, end), day, end)
    assert sorted(plain.collect()) == sorted(filtered.collect())
    assert filtered.count() > 0
