"""`callsign_lookback_minutes` decouples A8's lookback bound from `gap_minutes`.

The bound exists because `break_expr` is evaluated over the whole airframe
window, and an unbounded `F.last` reaches back past a gap break into the
previous flight. `gap_minutes` became the bound only because it was to hand --
the two quantities answer different questions, and this pins the difference.
"""
from dataclasses import fields

from opdi.pipeline.segmentation import SegmentationParams
from opdi.pipeline.segmentation.base import lookback_minutes


def test_default_follows_gap_minutes():
    """None means "follow gap_minutes" -- today's behaviour, exactly."""
    p = SegmentationParams(gap_minutes=42.0)
    assert p.callsign_lookback_minutes is None
    assert lookback_minutes(p) == 42.0


def test_explicit_value_overrides():
    p = SegmentationParams(gap_minutes=30.0, callsign_lookback_minutes=5.0)
    assert lookback_minutes(p) == 5.0


def test_zero_is_honoured_not_treated_as_unset():
    """0.0 is falsy, and `or` would silently read it as unset.

    A zero lookback is a meaningful grid cell: it disables the callsign-change
    break entirely, which is the sweep's `airframe_only` corner. If this fails,
    the implementation used `or` instead of an `is None` check.
    """
    p = SegmentationParams(gap_minutes=30.0, callsign_lookback_minutes=0.0)
    assert lookback_minutes(p) == 0.0


def test_config_and_params_still_agree_field_for_field():
    """`from_config` raises TypeError when SegmentationConfig lacks a field.

    tests/test_segmentation_base.py already asserts the default sets match. This
    asserts the *new* field, so an edit to one dataclass cannot quietly leave
    the other behind.
    """
    from opdi.config import SegmentationConfig

    names = {f.name for f in fields(SegmentationConfig)}
    assert "callsign_lookback_minutes" in names
    assert SegmentationConfig().callsign_lookback_minutes is None
    assert SegmentationParams.from_config(SegmentationConfig()) == SegmentationParams()


def test_cell_key_reads_a_three_axis_row_as_the_unset_cell():
    """--resume against a committed V1 sweep CSV must skip, not crash."""
    import pathlib
    import sys

    sys.path.insert(
        0, str(pathlib.Path(__file__).resolve().parent.parent / "benchmarks")
    )
    from track_sweep import cell_key

    legacy_row = {"gap_minutes": "30", "low_alt_gap_minutes": "15",
                  "low_alt_ft": "5000"}
    assert cell_key(legacy_row) == (30.0, 15.0, 5000.0, None)
    assert cell_key({**legacy_row, "callsign_lookback_minutes": ""}) == (
        30.0, 15.0, 5000.0, None)
    assert cell_key({**legacy_row, "callsign_lookback_minutes": "5"}) == (
        30.0, 15.0, 5000.0, 5.0)
