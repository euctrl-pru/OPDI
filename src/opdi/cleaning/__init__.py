"""Trajectory cleaning for the OPDI pipeline (step 02a).

:class:`~opdi.cleaning.cleaner.TrackCleaner` runs the step;
:mod:`opdi.cleaning.native` holds its stages, which follow the winning
solution of the 2024 PRC Data Challenge, and lists the techniques that are
deliberately left out.
"""

from opdi.cleaning.native import (
    CLEANED_COLUMNS,
    add_segment_id,
    clean_tracks,
    drop_duplicate_statevectors,
    mask_derivative_spikes,
    mask_isolated_points,
    mask_out_of_range_positions,
    mask_stale_broadcasts,
    null_rate_report,
)

__all__ = [
    "CLEANED_COLUMNS",
    "add_segment_id",
    "clean_tracks",
    "drop_duplicate_statevectors",
    "mask_derivative_spikes",
    "mask_isolated_points",
    "mask_out_of_range_positions",
    "mask_stale_broadcasts",
    "null_rate_report",
]
