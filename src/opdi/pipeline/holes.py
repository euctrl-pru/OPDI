"""Holes inside a track.

A **hole** is a silence longer than ``EventConfig.track_hole_seconds`` between
two consecutive samples of one track. The samples between two holes are a
**piece**. Until the ``cruise_linked`` track rule (A10) no track held a hole,
because every gap that long started a new track; under it a track can span an
ocean, with hours in the middle where nothing was received.

Nothing was measured inside a hole, so a detector that pairs or interpolates
samples must not do it across one: a level segment that "lasts" the three hours
nobody heard the aircraft, or a flight-level crossing placed at an instant
inside the silence, is an invention. Detectors therefore work per piece, by
adding ``piece`` to their partition. What belongs to the flight as a whole
stays per track: first and last seen, the tops of climb and descent, the block
and runway milestones, the flight list.

The threshold equals the segmentation gap on purpose. A track built by an
earlier rule has one piece, so its events are exactly what they were.
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
