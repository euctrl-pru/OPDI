"""Tests for the generic segmentation engine.

The engine exists so that eight arms can differ only in a break expression. That
is only sound if the engine, given production's parameters, produces exactly the
partition ``tracks.py:_add_track_id`` produces -- otherwise arm A0 is not the
production baseline and every delta in the study is measured from a wrong zero.
``test_engine_reproduces_frozen_algorithm`` is that lock. It must never be
weakened; if it fails, the engine is wrong, not the test.
"""

import dataclasses
import datetime as dt

import pytest
from conftest import make_track

from opdi.config import OPDIConfig, SegmentationConfig
from opdi.pipeline.segmentation import BreakRule, SegmentationParams, assign_track_id
from opdi.pipeline.segmentation.methods import legacy
from opdi.pipeline.tracks import TrackProcessor


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


#: The lock's fixture. **It must contain at least three distinct groups, and at
#: least two of them must share an ``icao24``.**
#:
#: ``_partition`` compares groupings as sets of event-time sets, which is
#: invariant under *any* change of group key when the fixture has only one
#: group -- so a single-airframe fixture cannot see half of the property this
#: lock exists to protect. It was demonstrated that a rule with
#: ``group_cols=["icao24"]`` (arm A4's key, which is *not* production's) passed
#: the lock unchanged. ``test_the_lock_rejects_the_wrong_group_key`` is the
#: negative control that keeps this honest; weaken the fixture and that test
#: goes green and stops meaning anything.
#:
#: Three groups: (abc123, TEST123) carrying the gap cases, (abc123, TEST999)
#: sharing the airframe, and (def456, TEST123) sharing the callsign. Group 2's
#: samples are interleaved in time with group 1's and spaced 10 minutes apart,
#: which is under both legacy thresholds -- so collapsing the two onto ``icao24``
#: alone yields one unbroken track where the correct key yields four.
SAMPLES = [
    # -- group 1: abc123 / TEST123, one flight then two gap-driven splits -----
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
] + [
    # -- group 2: the SAME airframe on a second callsign ----------------------
    # 10 min spacing: neither legacy rule fires, so this is one track on its own
    # and part of one merged track if the callsign is dropped from the key.
    {"t": 30 + i * 600, "callsign": "TEST999", "baro_altitude": 10000.0}
    for i in range(9)
] + [
    # -- group 3: a second airframe on the first callsign ---------------------
    {"t": 0, "icao24": "def456", "baro_altitude": 10000.0},
    {"t": 45 * 60, "icao24": "def456", "baro_altitude": 10000.0},   # 45 min gap
]

#: What ``legacy()`` must produce on :data:`SAMPLES`: 3 + 1 + 2.
EXPECTED_LOCK_TRACKS = 6


def test_engine_reproduces_frozen_algorithm(spark, tmp_path):
    df = make_track(spark, SAMPLES)
    frozen = _frozen_track_ids(spark, df, tmp_path)
    engine = assign_track_id(df, legacy(), SegmentationParams())
    assert _partition(engine, "track_id") == _partition(frozen, "track_id")
    # And the fixture really does span several groups -- a lock over one group
    # cannot see the grouping half of the property at all.
    assert len(_partition(frozen, "track_id")) == EXPECTED_LOCK_TRACKS


def test_the_lock_rejects_the_wrong_group_key(spark, tmp_path):
    """The negative control. Without it the lock proves only half its property.

    Arm A4's key -- ``icao24`` alone -- is a real, deliberate, *different*
    grouping. Against the old single-airframe fixture it passed
    ``test_engine_reproduces_frozen_algorithm`` unchanged, because with one group
    a set-of-sets comparison cannot tell two group keys apart. If this test ever
    goes green, the fixture has lost its second airframe or its second callsign
    and the lock is no longer locking anything.
    """
    df = make_track(spark, SAMPLES)
    frozen = _frozen_track_ids(spark, df, tmp_path)
    wrong_key = BreakRule(
        name="wrong_key",
        break_expr=legacy().break_expr,
        group_cols=["icao24"],          # not production's (icao24, callsign)
        month_suffix=True,
    )
    engine = assign_track_id(df, wrong_key, SegmentationParams())
    assert _partition(engine, "track_id") != _partition(frozen, "track_id")


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


def test_break_rule_requires_a_break_expression():
    """It used to default to None and die later with an unhelpful TypeError.

    ``BreakRule(name=...)`` alone constructed happily and then failed inside
    ``assign_track_id`` with ``'NoneType' object is not callable`` -- a message
    naming neither the rule nor the missing field.
    """
    with pytest.raises(TypeError):
        BreakRule(name="no_expr")                              # type: ignore[call-arg]
    with pytest.raises(TypeError, match="break_expr must be callable"):
        BreakRule(name="not_callable", break_expr="nope")      # type: ignore[arg-type]


def test_segmentation_config_and_params_agree_field_by_field():
    """``SegmentationConfig`` is the config-file surface for ``SegmentationParams``.

    Two dataclasses, the same seven fields, the same seven defaults -- and before
    ``from_config`` existed, no link at all between them. ``SegmentationConfig``
    was read by nothing, so tuning ``OPDIConfig().segmentation.low_alt_ft``
    produced no effect and no error. This test is the link's guarantee: if either
    side gains, loses or re-defaults a field, it fails here rather than in a
    study whose numbers silently came from the other set of thresholds.
    """
    cfg_fields = {f.name: f.default for f in dataclasses.fields(SegmentationConfig)}
    par_fields = {f.name: f.default for f in dataclasses.fields(SegmentationParams)}
    assert cfg_fields == par_fields


def test_params_from_config_reads_the_config_values():
    cfg = OPDIConfig()
    assert SegmentationParams.from_config(cfg) == SegmentationParams()

    cfg.segmentation.low_alt_ft = 7000.0
    cfg.segmentation.ground_dwell_minutes = 11.0
    p = SegmentationParams.from_config(cfg)
    assert p.low_alt_ft == 7000.0
    assert p.ground_dwell_minutes == 11.0
    # A SegmentationConfig may also be handed over directly.
    assert SegmentationParams.from_config(cfg.segmentation) == p


def test_params_from_config_rejects_an_object_missing_fields():
    with pytest.raises(TypeError, match="missing segmentation fields"):
        SegmentationParams.from_config(object())
