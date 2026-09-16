"""A9 `debounced`: a callsign must persist before it breaks a track.

`recommended` breaks on the first sample whose callsign differs from the last
real one. A value that appears for one sample and reverts therefore cuts a
flight in half -- measured at 1,457 of 1,765 sub-minute splits on 2026-06-01,
all of them at cruise with a median implied speed of 421 kt across the gap.
"""
from dataclasses import fields

import pytest

from opdi.config import SegmentationConfig
from opdi.pipeline.segmentation import SegmentationParams


def test_the_default_is_debounce_at_30s():
    """The shipped default is now A9 debounce at 30 s.

    `standard` resolves to the `debounced` arm, and this field is what makes it
    debounce rather than reproduce A8 -- so the default must be the validated
    30 s, not zero. This reshapes `track_id` from this release forward, which is
    the contract-owner's deliberate choice; A8 stays reachable as
    `method="recommended"` (which ignores this field) for reproducing data
    published between 2026-08-27 and this release.
    """
    assert SegmentationParams().callsign_min_persistence_seconds == 30.0
    assert SegmentationConfig().callsign_min_persistence_seconds == 30.0


def test_the_field_carries_its_unit():
    """The convention `tests/test_detection_config.py` enforces elsewhere; a
    threshold whose unit is ambiguous is the most likely source of a silent
    bug, because a value 60x too small simply never fires."""
    names = {f.name for f in fields(SegmentationParams)}
    assert "callsign_min_persistence_seconds" in names


def test_the_config_value_reaches_the_params():
    """`SegmentationParams.from_config` is the only path from configuration to
    the engine. A field that does not travel it is a setting that does
    nothing."""
    cfg = SegmentationConfig(callsign_min_persistence_seconds=30.0)
    assert SegmentationParams.from_config(cfg).callsign_min_persistence_seconds == 30.0


import datetime as dt

from conftest import make_track
from opdi.pipeline.segmentation import assign_track_id
from opdi.pipeline.segmentation.methods import debounced, recommended

P0 = SegmentationParams(callsign_min_persistence_seconds=0.0)    # debounce off (A8)
P30 = SegmentationParams(callsign_min_persistence_seconds=30.0)  # debounce on (shipped default)


def n_tracks(df):
    return df.select("track_id").distinct().count()


def _with_callsigns(spark, callsigns, step_s=5):
    """One airframe at cruise, one callsign per sample, five seconds apart."""
    df = make_track(spark, [
        {"t": i * step_s, "baro_altitude": 10000.0} for i in range(len(callsigns))
    ])
    from pyspark.sql import functions as F, Window
    return df.withColumn(
        "callsign",
        F.element_at(
            F.array(*[F.lit(c) for c in callsigns]),
            F.row_number().over(
                Window.partitionBy("icao24").orderBy("event_time")
            ).cast("int"),
        ),
    )


def test_a_flicker_no_longer_splits_the_flight(spark):
    """One stray sample between eight good ones is noise, not a new flight."""
    df = _with_callsigns(spark, ["BEL123"] * 4 + ["XXXX"] + ["BEL123"] * 4)

    assert n_tracks(assign_track_id(df, recommended(), P0)) == 3, (
        "Three, not two: a flicker trips the rule on the X->Y excursion AND on "
        "the Y->X revert, so it cuts the flight into three pieces. Measured on "
        "2026-06-01, both legs are present -- 5,241 revert boundaries and 5,000 "
        "excursion boundaries. If this is not 3, `recommended` no longer has "
        "the defect and this whole plan is measuring something else."
    )
    assert n_tracks(assign_track_id(df, debounced(), P30)) == 1


def test_a_two_sample_flicker_is_also_suppressed(spark):
    """A multi-sample excursion is still a flicker, not two flights.

    The run-level persistence verdict is what handles this: the two-sample
    `XXXX` run lasts 5 s before the next transition back to `BEL123`, far under
    the 30 s hold, so the run is not `persisted`, plants no `stable` seed, and
    the forward-fill keeps `BEL123` in force across it. Both `XXXX` samples
    collapse into the surrounding `BEL123` track. (An earlier per-row
    nearest/farthest-horizon design was abandoned for this run-level one; see
    EVENTS_RUN_LOG decision 44.)
    """
    df = _with_callsigns(spark, ["BEL123"] * 4 + ["XXXX"] * 2 + ["BEL123"] * 4)

    assert n_tracks(assign_track_id(df, recommended(), P0)) == 3
    assert n_tracks(assign_track_id(df, debounced(), P30)) == 1


def test_a_three_sample_flicker_is_also_suppressed(spark):
    """One more sample than the two-sample case, same requirement."""
    df = _with_callsigns(spark, ["BEL123"] * 4 + ["XXXX"] * 3 + ["BEL123"] * 4)

    assert n_tracks(assign_track_id(df, debounced(), P30)) == 1


def test_a_genuine_change_still_splits(spark):
    """The new value holds for the rest of the flight, so it is real."""
    df = _with_callsigns(spark, ["BEL123"] * 4 + ["KLM99"] * 10)

    assert n_tracks(assign_track_id(df, debounced(), P30)) == 2


def test_a_change_that_holds_exactly_the_threshold_splits(spark):
    """The boundary is inclusive: 30 s of persistence at a 30 s threshold is
    persistence. Stated so the comparison cannot drift to `>` unnoticed."""
    # 5 s spacing: indices 4..10 are the new value, spanning 30 s.
    df = _with_callsigns(spark, ["BEL123"] * 4 + ["KLM99"] * 7 + ["BEL123"] * 4)

    assert n_tracks(assign_track_id(df, debounced(), P30)) == 3


def test_zero_persistence_reproduces_recommended(spark):
    """A9 with the debounce off must be A8, exactly -- that is what makes the
    arm safe to add beside a published one."""
    df = _with_callsigns(spark, ["BEL123"] * 4 + ["XXXX"] + ["BEL123"] * 4)

    assert n_tracks(assign_track_id(df, debounced(), P0)) == \
           n_tracks(assign_track_id(df, recommended(), P0))


def test_the_gap_rules_are_untouched(spark):
    """A9 inherits legacy's gap floor unchanged. A debounce that also quietly
    changed gap behaviour would be impossible to attribute."""
    df = _with_callsigns(spark, ["BEL123"] * 3)
    from pyspark.sql import functions as F
    late = df.withColumn(
        "event_time",
        F.when(F.col("event_time") == F.min("event_time").over(
            __import__("pyspark").sql.Window.partitionBy("icao24")
        ), F.col("event_time")).otherwise(
            F.col("event_time") + F.expr("INTERVAL 45 MINUTES")
        ),
    )
    assert n_tracks(assign_track_id(late, debounced(), P30)) == 2
