"""``config.segmentation.method`` selects the production track algorithm.

The lock in ``test_segmentation_base.py`` proves the *legacy* branch still
reproduces the published ids. These tests cover the switch itself: that the
default has not moved, that selecting the new method actually changes the
partition, that it is the same partition the study benchmarked, and that a
typo fails loudly instead of silently falling back to legacy.

That last one matters more than it looks. A dispatch that treats an unknown
name as "use the default" would let a deployment believe it had switched
segmentation while producing legacy ids -- and every downstream number would
look plausible.
"""

import pytest
from conftest import make_track

from opdi.config import OPDIConfig
from opdi.pipeline.segmentation import SegmentationParams, assign_track_id
from opdi.pipeline.segmentation.methods import recommended
from opdi.pipeline.tracks import TrackProcessor


def _partition(df, id_col="track_id"):
    """Grouping as a set of event-time sets, so ids can be renamed freely."""
    groups = {}
    for r in df.select("event_time", id_col).collect():
        groups.setdefault(r[id_col], set()).add(r["event_time"])
    return {frozenset(v) for v in groups.values()}


def _proc(spark, tmp_path, method=None):
    cfg = OPDIConfig()
    if method is not None:
        cfg.segmentation.method = method
    return TrackProcessor(spark, cfg, log_file_path=str(tmp_path / "log.parquet"))


#: One airframe, one continuous flight, whose callsign blanks out in the middle
#: and comes back. Legacy puts the blank run in its own group and returns two
#: tracks; the standard method carries the last real callsign across the blanks
#: and returns one. Chosen because it is the common case -- 42% of legacy's
#: tracks on the 2025 sample are blank-callsign tracks made exactly this way.
BLANKING = (
    [{"t": i * 60, "callsign": "ABC123"} for i in range(5)]
    + [{"t": (5 + i) * 60, "callsign": ""} for i in range(5)]
    + [{"t": (10 + i) * 60, "callsign": "ABC123"} for i in range(5)]
)


def test_the_default_is_still_legacy(spark, tmp_path):
    """Nobody's ids change by upgrading. The switch has to be thrown on purpose."""
    assert OPDIConfig().segmentation.method == "legacy"
    df = make_track(spark, BLANKING)
    assert len(_partition(_proc(spark, tmp_path)._add_track_id(df))) == 2


def test_standard_changes_the_partition(spark, tmp_path):
    """The new method is actually wired through, not just accepted."""
    df = make_track(spark, BLANKING)
    legacy = _partition(_proc(spark, tmp_path)._add_track_id(df))
    standard = _partition(_proc(spark, tmp_path, "standard")._add_track_id(df))
    assert len(legacy) == 2
    assert len(standard) == 1
    assert legacy != standard


def test_standard_is_the_arm_the_study_benchmarked(spark, tmp_path):
    """Production and the benchmark must be one algorithm, not two.

    Compared as partitions rather than id strings: the study's harness and the
    pipeline are entitled to format ids differently, but if they ever group
    samples differently then the published numbers describe something other
    than what runs.
    """
    df = make_track(spark, BLANKING)
    through_pipeline = _proc(spark, tmp_path, "standard")._add_track_id(df)
    through_study = assign_track_id(df, recommended(), SegmentationParams())
    assert _partition(through_pipeline) == _partition(through_study)


def test_an_unknown_method_raises_rather_than_falling_back(spark, tmp_path):
    """A typo must not silently produce legacy ids under a new name."""
    df = make_track(spark, BLANKING)
    with pytest.raises(ValueError, match="unknown segmentation method"):
        _proc(spark, tmp_path, "recomended")._add_track_id(df)


def test_any_study_arm_can_be_selected(spark, tmp_path):
    """The benchmark drives the real pipeline by naming an arm here.

    Without this the v2 study would have to re-implement each arm outside the
    pipeline, which is the arrangement v1 had and the reason its numbers
    described a harness rather than production.
    """
    df = make_track(spark, BLANKING)
    out = _proc(spark, tmp_path, "airframe_only")._add_track_id(df)
    assert out.count() == df.count()
    assert out.filter(out.track_id.isNull()).count() == 0
