"""Tests for the generic segmentation engine.

The engine exists so that eight arms can differ only in a break expression. That
is only sound if the engine, given production's parameters, produces exactly the
partition ``tracks.py:_add_track_id`` produces -- otherwise arm A0 is not the
production baseline and every delta in the study is measured from a wrong zero.
``test_engine_reproduces_frozen_algorithm`` is that lock. It must never be
weakened; if it fails, the engine is wrong, not the test.
"""

import datetime as dt

from opdi.config import OPDIConfig
from opdi.pipeline.segmentation import SegmentationParams, assign_track_id
from opdi.pipeline.segmentation.methods import legacy
from opdi.pipeline.tracks import TrackProcessor

from conftest import make_track


def _partition(df, id_col):
    """The grouping as a set of frozensets of event_time, ignoring id spelling."""
    rows = df.select("event_time", id_col).collect()
    groups = {}
    for r in rows:
        groups.setdefault(r[id_col], set()).add(r["event_time"])
    return {frozenset(v) for v in groups.values()}


def _frozen_track_ids(spark, df, tmp_path):
    """Run the frozen production algorithm. Never modify tracks.py to make this pass."""
    proc = TrackProcessor(spark, OPDIConfig(), log_file_path=str(tmp_path / "log.parquet"))
    return proc._add_track_id(df)


SAMPLES = [
    # one flight
    {"t": 0, "baro_altitude": 300.0},
    {"t": 60, "baro_altitude": 5000.0},
    {"t": 120, "baro_altitude": 10000.0},
    # 40 min gap at altitude -> splits on the 30 min rule
    {"t": 120 + 40 * 60, "baro_altitude": 10000.0},
    {"t": 180 + 40 * 60, "baro_altitude": 9000.0},
    # 20 min gap below 1524 m -> splits on the low-altitude rule
    {"t": 180 + 60 * 60, "baro_altitude": 200.0},
    # 20 min gap above 1524 m -> does NOT split
    {"t": 180 + 80 * 60, "baro_altitude": 8000.0},
]


def test_engine_reproduces_frozen_algorithm(spark, tmp_path):
    df = make_track(spark, SAMPLES)
    frozen = _frozen_track_ids(spark, df, tmp_path)
    engine = assign_track_id(df, legacy(), SegmentationParams())
    assert _partition(engine, "track_id") == _partition(frozen, "track_id")


def test_engine_reproduces_frozen_algorithm_across_a_month_boundary(spark, tmp_path):
    """The frozen algorithm splits at midnight on the 1st via its id suffix.

    A2 removes that. A0 must still reproduce it, or the two arms are not
    comparable.
    """
    base = dt.datetime(2024, 6, 30, 23, 30, 0)
    epoch = dt.datetime(2024, 6, 1, 12, 0, 0)
    off = (base - epoch).total_seconds()
    df = make_track(spark, [
        {"t": off, "baro_altitude": 10000.0},
        {"t": off + 600, "baro_altitude": 10000.0},
        {"t": off + 1800, "baro_altitude": 10000.0},   # 00:00 on 1 July
        {"t": off + 2400, "baro_altitude": 10000.0},
    ])
    frozen = _frozen_track_ids(spark, df, tmp_path)
    engine = assign_track_id(df, legacy(), SegmentationParams())
    assert _partition(engine, "track_id") == _partition(frozen, "track_id")
    # And it really did split -- otherwise the test proves nothing.
    assert len(_partition(frozen, "track_id")) == 2


def test_engine_drops_its_temporary_columns(spark):
    df = make_track(spark, SAMPLES)
    out = assign_track_id(df, legacy(), SegmentationParams())
    assert set(out.columns) == set(df.columns) | {"track_id"}
