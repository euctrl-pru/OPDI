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
    """Enrich the flight list against the events written so far.

    ``start_date``/``end_date`` are kept for the runner's uniform step
    signature and for logging; the enrichment itself covers every partition,
    for the schema reason documented below.

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
    print(f"  enriching the flight list (asked for {len(days)} day(s) "
          f"from {start_date}; rewrites every partition)")

    # **The whole table, not the window.** Enriching only the requested days
    # would leave every other partition -- including the
    # ``DOF=__HIVE_DEFAULT_PARTITION__`` one that holds rows with no date --
    # carrying the pre-enrichment schema. Spark does not merge parquet schemas
    # by default; it takes the schema of one file it happens to sample. A table
    # whose partitions disagree therefore shows or hides the twenty-two added
    # columns depending on which file that is, and the failure is silent: the
    # reader sees a flight list that simply has no milestones on it.
    #
    # Rewriting everything is affordable precisely because this table is small
    # -- about 3 MB per day against the event table's gigabytes -- and it is
    # self-healing: a day whose events arrived after its first enrichment picks
    # them up on the next run rather than staying half-filled.
    #
    # The two tables really do spell the column differently -- ``DOF`` on the
    # flight list, ``dof`` on the events -- and the partition directories on S3
    # follow suit.
    flight_list = storage.read_table(FLIGHT_LIST_TABLE)
    events = storage.read_table(EVENTS_TABLE)

    enriched = enrich_flight_list(flight_list, events, config)

    # Pass 1: break the read-write dependency on the target.
    #
    # ``partition_values=None`` on purpose. Every other day-scoped write in the
    # pipeline knows its partitions and passes them to save a probe; here the
    # set is "whatever the table holds", which is what the probe computes. The
    # null-date partition in particular has no value that could be named.
    storage.write_table(
        enriched.repartition("DOF"), STAGING_TABLE, mode="overwrite",
        partition_by=["DOF"],
    )

    # Pass 2: replace from a source that shares no lineage with the target.
    staged = storage.read_table(STAGING_TABLE)
    written = staged.count()
    storage.write_table(
        staged.repartition("DOF"), FLIGHT_LIST_TABLE, mode="overwrite",
        partition_by=["DOF"],
    )
    return written
