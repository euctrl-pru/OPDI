"""Track segmentation: turning state vectors into flight tracks.

``tracks.py:_add_track_id`` remains the production algorithm and remains frozen.
This package is the study of what should replace it.
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
