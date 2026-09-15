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


def test_the_default_is_no_debounce():
    """Zero reproduces `recommended` exactly.

    A8 is what every dataset since 2026-08-27 was published with, and
    `track_id` is a published contract. A non-zero default would change ids for
    every arm that inherits this field, silently.
    """
    assert SegmentationParams().callsign_min_persistence_seconds == 0.0
    assert SegmentationConfig().callsign_min_persistence_seconds == 0.0


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
