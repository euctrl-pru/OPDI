"""Holes inside a track: gaps in reception kept within one track.

A **hole** is a silence longer than ``EventConfig.track_hole_seconds`` between
two consecutive reports of one track. The reports between two holes are a
**piece**. Under the v0.0.2 track rule every gap that long starts a new track,
so a track has no hole; under the default rule (``cruise_linked``) a track can
cross an ocean, with hours in the middle where nothing was received.

Nothing was measured inside a hole, so a detector that pairs or interpolates
samples must not do it across one: a level segment that "lasts" the three hours
nobody heard the aircraft, or a flight-level crossing placed at an instant
inside the silence, is an invention. Detectors therefore work per piece, by
adding ``piece`` to their partition. What belongs to the flight as a whole
stays per track: first and last seen, the tops of climb and descent, the block
and runway milestones, the flight list.

The threshold equals the track rule's gap on purpose: a track built by the
v0.0.2 rule has one piece, so its events are the same with or without pieces.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

__all__ = ["PIECE", "add_piece"]

#: The column :func:`add_piece` adds.
PIECE = "piece"


def add_piece(
    df: DataFrame,
    hole_seconds: float,
    track_col: str = "track_id",
    time_col: str = "event_time",
) -> DataFrame:
    """Number the pieces of each track: 0 up to the first hole, 1 after it, ...

    A gap of exactly ``hole_seconds`` is not a hole, matching the segmentation
    rules, which break only on a gap *over* their threshold.
    """
    ordered = Window.partitionBy(track_col).orderBy(time_col)
    seconds = F.col(time_col).cast("timestamp").cast("double")
    gap_s = seconds - F.lag(seconds).over(ordered)
    is_hole = F.coalesce(gap_s > F.lit(float(hole_seconds)), F.lit(False))
    running = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return df.withColumn(PIECE, F.sum(is_hole.cast("int")).over(running).cast("int"))
