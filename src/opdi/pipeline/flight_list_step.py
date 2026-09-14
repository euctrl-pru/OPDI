"""Step 04b -- fold the milestones back into the flight list.

``opdi_flight_events`` is long-form: one row per (flight, milestone). Asking
"when did this flight take off, from which runway, off which stand" therefore
means three joins and a pivot. APDF -- the reference OPDI benchmarks against --
is flight-shaped, so every comparison began by reshaping OPDI's output.

This step does that reshaping once, per day, and writes the result back into
``opdi_flight_list`` as additional columns. It reads; it does not detect. Every
value here already exists in the event table, and a column is null exactly when
the event behind it is missing.

**Why the round trip through a staging table.** The enriched frame is derived
from ``opdi_flight_list`` and written back to it. Spark evaluates a write
lazily, so deleting the target partitions and then appending would delete the
very rows the plan still has to read -- and the failure is silent, because the
job succeeds and writes whatever it managed to scan first. Materialising to a
staging table breaks the dependency: the second write reads a table that no
longer has anything to do with the one being replaced.
"""
from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, Optional

from pyspark.sql import DataFrame, functions as F

from opdi.pipeline.flight_list_milestones import enrich_flight_list

if TYPE_CHECKING:  # pragma: no cover
    from opdi.config import EventConfig
    from opdi.utils.storage import StorageManager

#: Written and dropped within the step. Named rather than anonymous so a run
#: that dies between the two writes leaves something a human can find and
#: inspect instead of a temp path nobody can name.
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
    storage: "StorageManager",
    config: "EventConfig",
    start_date: date,
    end_date: date,
) -> Optional[int]:
    """Enrich every flight list partition in ``[start_date, end_date]``.

    Returns the number of rows written, or ``None`` when there was nothing to
    do -- a missing flight list or a missing event table, both of which mean an
    earlier step has not run rather than that this one failed.
    """
    if not storage.table_exists(FLIGHT_LIST_TABLE):
        print(f"  {FLIGHT_LIST_TABLE} does not exist -- step 03 has not run. Skipping.")
        return None
    if not storage.table_exists(EVENTS_TABLE):
        print(f"  {EVENTS_TABLE} does not exist -- step 04 has not run. Skipping.")
        return None

    days = _days_between(start_date, end_date)

    # ``between`` on a cast date rather than ``isin`` on isoformat strings:
    # the flight list carries a ``DOF=__HIVE_DEFAULT_PARTITION__`` partition
    # for rows with no date, and an inclusive range says plainly that those
    # rows are out of scope rather than relying on a null comparing unequal to
    # every string in a list.
    #
    # The two tables really do spell the column differently -- ``DOF`` on the
    # flight list, ``dof`` on the events -- and the partition directories on S3
    # follow suit.
    dof = F.col("DOF").cast("date")
    flight_list = storage.read_table(FLIGHT_LIST_TABLE).filter(
        dof.between(F.lit(start_date), F.lit(end_date))
    )
    events = storage.read_table(EVENTS_TABLE).filter(
        F.col("dof").cast("date").between(F.lit(start_date), F.lit(end_date))
    )

    enriched = enrich_flight_list(flight_list, events, config)

    # Pass 1: break the read-write dependency on the target.
    storage.write_table(
        enriched.repartition("DOF"),
        STAGING_TABLE,
        mode="overwrite",
        partition_by=["DOF"],
        partition_values=[{"DOF": d} for d in days],
    )

    # Pass 2: replace the day's rows from a source that shares no lineage with
    # them.
    staged = storage.read_table(STAGING_TABLE).filter(
        F.col("DOF").cast("date").between(F.lit(start_date), F.lit(end_date))
    )
    written = staged.count()
    storage.write_table(
        staged.repartition("DOF"),
        FLIGHT_LIST_TABLE,
        mode="overwrite",
        partition_by=["DOF"],
        partition_values=[{"DOF": d} for d in days],
    )

    storage.drop_partitions(STAGING_TABLE, "DOF", [d.isoformat() for d in days])
    return written
