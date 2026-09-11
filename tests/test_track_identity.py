"""Track identity must be a property of the track, not of the batch.

The engine numbered tracks with a running break-count from the start of
whatever data it was given (``_offset``). That is fine for one monolithic run
and wrong for everything else: the first track of an airframe is ``_0`` in
*every* batch, so two batches produce the same id for different tracks, and the
same track gets a different id depending on how much history happened to be
loaded with it.

It was already live. Step 02 runs month by month, and the ``recommended`` arm
dropped the ``_{year}_{month}`` suffix that used to disambiguate -- so
``{hash}_0`` recurs every month. Day-by-day processing turns that into a
collision every *day*, which is what these tests pin shut.

The fix keys the id on the track's own first sample. Same track, same id, no
matter who computes it or what else is in the frame.
"""
import datetime as dt

import pytest
from conftest import make_track

from opdi.config import SegmentationConfig
from opdi.pipeline.segmentation import SegmentationParams, assign_track_id
from opdi.pipeline.segmentation.methods import ARMS

HOUR = 3600


def _params():
    return SegmentationParams.from_config(SegmentationConfig())


def _ids(df):
    """{first_event_time: track_id} -- keyed by something batch-independent."""
    rows = df.select("event_time", "track_id").collect()
    first = {}
    for r in rows:
        cur = first.get(r["track_id"])
        if cur is None or r["event_time"] < cur:
            first[r["track_id"]] = r["event_time"]
    return {v: k for k, v in first.items()}


def _segment(spark, samples, arm="recommended"):
    sv = make_track(spark, samples)
    return assign_track_id(sv, ARMS[arm](), _params())


def test_the_same_track_gets_the_same_id_in_a_wider_batch(spark):
    """The heart of day-by-day processing.

    Two tracks an hour apart (the gap rule breaks at 30 min). Segment the
    second one alone, then segment both together. The second track's id must
    not depend on whether the first was in the frame -- if it does, a day
    processed with a lookback window disagrees with the same day processed
    without one, and no downstream join survives that.
    """
    later = [{"t": 10 * HOUR}, {"t": 10 * HOUR + 60}]
    both = [{"t": 0}, {"t": 60}] + later

    alone = _ids(_segment(spark, later))
    together = _ids(_segment(spark, both))

    key = min(alone)
    assert alone[key] == together[key]


def test_two_batches_do_not_both_emit_the_first_id(spark):
    """Day D and day D+1 each hold one track for the same airframe.

    Under the running-count scheme both are ``{hash}_0`` -- the same id for two
    different tracks, silently, in a table that spans both days.
    """
    day_d = _ids(_segment(spark, [{"t": 0}, {"t": 60}]))
    day_d1 = _ids(_segment(spark, [{"t": 24 * HOUR}, {"t": 24 * HOUR + 60}]))

    assert set(day_d.values()).isdisjoint(day_d1.values())


def test_the_id_is_the_airframe_hash_and_the_start_instant(spark):
    """Shape is checkable: 16 hex of sha2(icao24), then the start epoch.

    Pinned because the id is parsed by consumers, and because a start time that
    silently became, say, the *last* sample would still look plausible.
    """
    out = _segment(spark, [{"t": 0}, {"t": 60}])
    row = out.select("event_time", "track_id").orderBy("event_time").first()
    grp, _, start = row["track_id"].rpartition("_")

    assert len(grp) == 16
    int(grp, 16)  # raises if not hex
    assert int(start) == int(row["event_time"].replace(tzinfo=dt.timezone.utc).timestamp())


def test_every_sample_of_a_track_carries_the_start_not_its_own_time(spark):
    """The id is per track, so all of its samples share one value."""
    out = _segment(spark, [{"t": 0}, {"t": 60}, {"t": 120}])
    assert out.select("track_id").distinct().count() == 1


def test_a_break_still_starts_a_new_id(spark):
    """The identity change must not alter the partition, only its labelling."""
    out = _segment(spark, [{"t": 0}, {"t": 60}, {"t": 10 * HOUR}])
    assert out.select("track_id").distinct().count() == 2


def test_legacy_identity_is_untouched(spark):
    """`legacy` reproduces released data byte for byte and is not ours to change.

    It keeps the running offset and the year/month suffix -- which is exactly
    the scheme the rest of this file exists to replace everywhere else.
    """
    out = _segment(spark, [{"t": 0}, {"t": 60}], arm="legacy")
    tid = out.select("track_id").first()["track_id"]
    parts = tid.split("_")
    assert len(parts) == 4, tid          # hash _ offset _ year _ month
    assert parts[1] == "0"               # the running counter, still counting
