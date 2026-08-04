"""Trajectory cleaning for the OPDI pipeline (step 02a).

Native Spark cleaning ported from the 2024 PRC Data Challenge winning
solution. See :mod:`opdi.cleaning.native` for the algorithm and for the list
of techniques deliberately not ported.

The optional ``applyInPandas`` escape hatch lives in
:mod:`opdi.cleaning.pandas_udf` and is off by default -- it needs a fat
executor image, so the default path stays dependency-free.
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
