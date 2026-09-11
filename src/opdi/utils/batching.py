"""Selecting the batch a pipeline step should process.

Steps 02a, 03 and 04 each read the tracks they are about to work on, and each
had its own copy of "filter ``event_time`` to this month". That was right while
the pipeline only ever processed whole months. It is wrong for a day.

**A day is not a short month.** Filtering a day by ``event_time`` returns the
samples that *occurred* that day: the tail of a flight that departed yesterday,
and only the head of one departing tonight. Neither is a track, and a step
handed those would report a flight that never landed and one that never took
off. What a day owns is the tracks that *started* in it, whole -- which is what
``TrackProcessor.process_day`` writes and what ``dof`` records.

So the day path filters on ``dof`` and the month path keeps filtering on time.
Having both here, rather than three times over, is what stops one step drifting
into a different definition of the same word.
"""

from datetime import date
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from opdi.utils.datetime_helpers import get_start_end_of_month


def filter_batch(
    df: DataFrame,
    month: Optional[date] = None,
    day: Optional[date] = None,
    time_col: str = "event_time",
) -> DataFrame:
    """Restrict *df* to one processing batch.

    Exactly one of ``month`` or ``day`` must be given.

    Args:
        df: A table carrying ``time_col``, and ``dof`` if ``day`` is used.
        month: Any date inside the month to select. Filters on ``time_col``.
        day: The day whose tracks to select. Filters on ``dof`` -- the day each
            track *started* -- so a track running past midnight comes back
            whole rather than clipped at the boundary.
        time_col: Timestamp column for the month path.

    Raises:
        ValueError: if neither or both are given, or if ``day`` is asked for on
            a frame with no ``dof``. The last one matters: without the column
            the filter would silently match nothing and the step would report
            an empty day as a result rather than as a mistake.
    """
    if (month is None) == (day is None):
        raise ValueError(
            "pass exactly one of month= or day=; got "
            f"month={month!r}, day={day!r}."
        )

    if day is not None:
        if "dof" not in df.columns:
            raise ValueError(
                "day-at-a-time selection needs a 'dof' column recording the "
                "day each track starts, and this frame has none: "
                f"{df.columns}. Tracks written before dof existed cannot be "
                "read a day at a time -- reprocess them with "
                "TrackProcessor.process_day."
            )
        return df.filter(F.col("dof") == F.lit(day))

    start_ts, end_ts = get_start_end_of_month(month)
    return df.filter(
        (F.col(time_col) >= F.to_timestamp(F.lit(start_ts)))
        & (F.col(time_col) < F.to_timestamp(F.lit(end_ts)))
    )
