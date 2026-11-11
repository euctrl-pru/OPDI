"""Step 04b: copy the main milestones of the events onto the flight list.

The flight events have one row per (flight, event). For the flights of one
day, this step reads their events, computes for each flight its main times,
runways, stands and circle crossings
(:func:`opdi.pipeline.flight_list_milestones.enrich_flight_list`), and writes
them into that day's folder of ``opdi_flight_list`` as additional columns.
It detects nothing new: every value already exists in the events table, and
a column is empty (NULL) exactly when the event behind it is missing.

Only the day folders of the days given are rewritten. The flight lists of
other days are never read or changed, so a day keeps its milestones after
its working events have been removed.

The enriched rows are first written to a staging table and then copied from
there into the flight list. Spark computes a write lazily: deleting the
day's folder and then writing rows computed from that same folder would
delete the rows before they are read. Reading them back from the staging
table removes that link. The staging table is replaced at each run and kept
afterwards, so a run that stops between the two writes leaves the rows where
they can be inspected.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from opdi.pipeline.flight_list_milestones import enrich_flight_list

if TYPE_CHECKING:  # pragma: no cover
    from opdi.config import EventConfig
    from opdi.utils.storage import StorageManager

#: The table the enriched rows are written to before they are copied into the
#: flight list. Replaced at each run, never removed.
STAGING_TABLE = "opdi_flight_list_milestones_staging"

FLIGHT_LIST_TABLE = "opdi_flight_list"
EVENTS_TABLE = "opdi_flight_events"


def _days_between(start_date: date, end_date: date) -> list:
    days, day = [], start_date
    while day <= end_date:
        days.append(day)
        day = date.fromordinal(day.toordinal() + 1)
    return days


def enrich_day(
    storage: StorageManager,
    config: EventConfig,
    start_date: date,
    end_date: date,
) -> int | None:
    """Add the milestone columns to the flight list of the days from ``start_date`` to ``end_date``.

    Reads the flights of those days and the events stored under the same
    days and one day on each side: the events table stores an event under
    the day of its track, which can be one day earlier or later than the day
    of its flight in the flight list. Only the flight list folders of the
    days given are replaced.

    Returns:
        The number of rows written, or ``None`` when there was nothing to do:
        the flight list or the events table does not exist yet, which means
        that step 03 or step 04 has not run.
    """
    if not storage.table_exists(FLIGHT_LIST_TABLE):
        print(f"  {FLIGHT_LIST_TABLE} does not exist: step 03 has not run. Skipping.")
        return None
    if not storage.table_exists(EVENTS_TABLE):
        print(f"  {EVENTS_TABLE} does not exist: step 04 has not run. Skipping.")
        return None

    days = _days_between(start_date, end_date)
    print(f"  enriching the flight list of {len(days)} day(s) from {start_date}")

    # The flight list names its day column DOF and the events table dof; the
    # day folders on disk are named the same way.
    flight_list = storage.read_table(FLIGHT_LIST_TABLE).filter(
        F.col("DOF").between(F.lit(start_date), F.lit(end_date))
    )
    events = storage.read_table(EVENTS_TABLE).filter(
        F.col("dof").between(
            F.lit(start_date - timedelta(days=1)), F.lit(end_date + timedelta(days=1))
        )
    )

    # ID is the flight identifier: the flight list holds one row per flight.
    # A repeated row is removed here, and the number removed is printed.
    before = flight_list.count()
    flight_list = flight_list.dropDuplicates(["ID"])
    removed = before - flight_list.count()
    if removed:
        print(f"  removed {removed:,} repeated ID(s): the flight list holds one row per flight")

    enriched = enrich_flight_list(flight_list, events, config)

    # The day folders are named, so a day without flights also replaces what
    # an earlier run left in the staging table.
    day_folders = [{"DOF": d.isoformat()} for d in days]
    storage.write_table(
        enriched.repartition("DOF"),
        STAGING_TABLE,
        mode="overwrite",
        partition_by=["DOF"],
        partition_values=day_folders,
    )
    staged = storage.read_table(STAGING_TABLE).filter(
        F.col("DOF").between(F.lit(start_date), F.lit(end_date))
    )
    written = staged.count()
    storage.write_table(
        staged.repartition("DOF"),
        FLIGHT_LIST_TABLE,
        mode="overwrite",
        partition_by=["DOF"],
        partition_values=day_folders,
    )
    return written
