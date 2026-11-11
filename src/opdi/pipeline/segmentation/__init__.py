"""Track segmentation: turning state vectors into flight tracks.

:mod:`~opdi.pipeline.segmentation.base` is the engine and
:mod:`~opdi.pipeline.segmentation.methods` holds the rules: the default rule
(``cruise_linked``) and the v0.0.2 rule (``legacy``). Step 02
(``tracks.TrackProcessor``) builds ``track_id`` with the rule the configuration
names.
"""

from opdi.pipeline.segmentation.base import (
    BreakRule,
    SegmentationParams,
    altitude_ft,
    assign_track_id,
    gap_minutes,
    lookback_minutes,
    segment_window,
    speed_kt,
)

__all__ = [
    "BreakRule",
    "SegmentationParams",
    "assign_track_id",
    "segment_window",
    "gap_minutes",
    "altitude_ft",
    "speed_kt",
    "lookback_minutes",
]
