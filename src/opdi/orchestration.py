"""Run the OPDI pipeline one step and one day at a time.

A scheduler such as Airflow runs the pipeline as a chain of small units: one
step (for example step 02, track building) for one day. This module runs one
such unit and is meant to be called as a command::

    python -m opdi.orchestration reference                       # step 00, once
    python -m opdi.orchestration step --step 02 --day 2026-06-14  # one step, one day
    python -m opdi.orchestration day --day 2026-06-14             # every step of a day
    python -m opdi.orchestration check-space --min-free-gb 25
    python -m opdi.orchestration prune --day 2026-06-14
    python -m opdi.orchestration prune --day 2026-06-14 --drop-published-working
    python -m opdi.orchestration clear-lock                       # after a stopped publish
    python -m opdi.orchestration plan --start 2025-01-01 --end 2026-11-09

Each command ends with exit code 0 when it succeeded and 1 when it failed.

Every unit keeps its state in the warehouse, not on the machine that runs it:
a finished unit is recorded under ``<release root>/_state/done/``, so running
the same command again skips it, on any machine. ``--force`` runs it again.

The days are processed by the date of the flight: day D holds the flights
first seen on D. Building day D reads the position reports of D-1 to D+1, so a
day can be processed only once the following day is complete.

The functions here import Spark only when they need it, so the module can be
imported to plan days without Spark installed.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
import traceback
from datetime import date, timedelta
from typing import Sequence

#: The steps of one day, in the order they run.
#:
#: * 01: fetch the position reports (state vectors) of D-1 to D+1.
#: * 02: build tracks: split the reports into one sequence per flight.
#: * 02a: clean the tracks (remove values that cannot be trusted).
#: * 03: build the flight list (departure and destination airports).
#: * 04: detect the flight events and their measurements.
#: * 04b: add the main milestones (for example take-off time) to the flight list.
#: * 09: publish the day's flight list and flight events files.
DAY_STEPS = ("01", "02", "02a", "03", "04", "04b", "09")

#: Day steps that read the reference tables. Publishing (09) reads only the
#: working tables, so it can run where no reference table exists.
STEPS_NEEDING_REFERENCE = ("01", "02", "02a", "03", "04", "04b")

#: The step that builds the reference tables (airports, runways, aircraft). It
#: runs once, not per day, and skips the tables that already exist.
REFERENCE_STEP = "00"

#: Internal tables that may be removed once a day is published, with the
#: column their files are split by (one folder per day). Nothing else is ever
#: removed.
INTERNAL_TABLES: dict[str, str] = {
    "osn_statevectors_v2": "event_time_day",
    "osn_tracks": "dof",
    "osn_tracks_clean": "dof",
}

#: Working tables of events and measurements, with the column their files are
#: split by. They are kept by default, so a published day can be published
#: again without being rebuilt. With ``--drop-published-working`` the day
#: folders of days that are already published, and lie outside a window of
#: :data:`DEFAULT_WORKING_KEEP_DAYS` days, are removed. The working flight
#: list (``opdi_flight_list``) is never removed: building a later day reads
#: it.
WORKING_TABLES: dict[str, str] = {
    "opdi_flight_events": "dof",
    "opdi_measurements": "dof",
}

#: Days of working events and measurements kept on each side of the day just
#: published when ``--drop-published-working`` is used.
DEFAULT_WORKING_KEEP_DAYS = 7

#: Days of internal tables kept around the day just published by
#: :func:`prune_internal`. With 2, the folders from one day before it to two
#: days after it stay (D-1 to D+2): they hold the position reports the next
#: day reads, in whichever order the days are processed. Everything further
#: away is removed.
DEFAULT_KEEP_DAYS = 2

#: Storage quota of the bucket, in GB (10^9 bytes), used to work out the free
#: space on S3, which has no free-space figure of its own. Replaced by the
#: ``OPDI_BUCKET_QUOTA_GB`` environment variable.
DEFAULT_QUOTA_GB = 200.0
QUOTA_VARIABLE = "OPDI_BUCKET_QUOTA_GB"

#: Free space below which a day does not start, in GB. A worldwide day
#: writes about 20 GB of internal tables (mostly tracks) before they are
#: removed, so the floor leaves room for one day and some margin.
DEFAULT_MIN_FREE_GB = 25.0

ORDERS = ("newest_first", "oldest_first")

#: The step that writes the day folders of each working table. When a step
#: of a day finds a folder it reads missing, the day is built again from the
#: step that writes it.
PRODUCERS: dict[str, str] = {
    "osn_statevectors_v2": "01",
    "osn_tracks": "02",
    "osn_tracks_clean": "02a",
    "opdi_flight_list": "03",
    "opdi_flight_events": "04",
    "opdi_measurements": "04",
}


def _tracks_read_by(step: str, config) -> str:
    """The track table step 03 or 04 reads under ``config``.

    The same rule as the steps themselves: the cleaned tracks when cleaning
    is on and the step is set to read them, else the tracks of step 02.
    """
    cleaning = config.cleaning
    if step == "03":
        clean = cleaning.enabled and cleaning.feeds_flight_list
    else:
        clean = cleaning.enabled and config.events.feeds_from_clean_tracks
    return "osn_tracks_clean" if clean else "osn_tracks"


def step_inputs(step: str, day: date, config) -> list[tuple[str, str, str]]:
    """The day folders that ``step`` reads for ``day``.

    Each is given as (table, partition column, value), for example
    ``("osn_tracks", "dof", "2026-06-14")``. Step 01 reads the OpenSky
    archive, not the warehouse, so it has none.

    * 02 reads the position reports of D-1, D and D+1.
    * 02a reads the tracks of D (none when cleaning is off).
    * 03 and 04 read the cleaned tracks of D, or the tracks of step 02 when
      the configuration says so.
    * 04b reads the flight list and the events of D.
    * 09 reads the flight list, the events and the measurements of D.
    """
    d = day.isoformat()
    if step == "01":
        return []
    if step == "02":
        return [
            ("osn_statevectors_v2", "event_time_day", (day + timedelta(days=k)).isoformat())
            for k in (-1, 0, 1)
        ]
    if step == "02a":
        return [("osn_tracks", "dof", d)] if config.cleaning.enabled else []
    if step in ("03", "04"):
        return [(_tracks_read_by(step, config), "dof", d)]
    if step == "04b":
        return [("opdi_flight_list", "DOF", d), ("opdi_flight_events", "dof", d)]
    if step == "09":
        return [
            ("opdi_flight_list", "DOF", d),
            ("opdi_flight_events", "dof", d),
            ("opdi_measurements", "dof", d),
        ]
    raise ValueError(f"unknown step {step!r}")


def _present(storage, cache: dict, table: str, column: str) -> set:
    if (table, column) not in cache:
        try:
            cache[(table, column)] = set(storage.list_partitions(table, column))
        except Exception:  # noqa: BLE001 - a missing table has no folders
            cache[(table, column)] = set()
    return cache[(table, column)]


def missing_inputs(storage, step: str, day: date, cache: dict | None = None) -> list[tuple]:
    """The day folders ``step`` reads for ``day`` that are not in the warehouse.

    Args:
        storage: A :class:`~opdi.utils.storage.StorageManager`.
        step: One of :data:`DAY_STEPS`.
        day: The day.
        cache: Folder listings already made, reused and filled in.
    """
    cache = {} if cache is None else cache
    return [
        (table, column, value)
        for table, column, value in step_inputs(step, day, storage.config)
        if value not in _present(storage, cache, table, column)
    ]


def _describe(folders) -> str:
    return ", ".join(f"{table} {column}={value}" for table, column, value in folders)


def restart_step(storage, day: date, first: str) -> tuple[str, list[tuple]]:
    """The step from which ``day`` must run when its next step is ``first``.

    Steps before ``first`` are recorded as finished and will not run. When a
    folder that ``first`` or a later step reads is missing and is written by
    one of those earlier steps, the day must start again from that earlier
    step; the check is repeated from there, so a chain of missing folders
    leads back to the step whose inputs are all present (at the latest step
    01, which reads the OpenSky archive).

    Returns:
        The step to start from and the missing folders found.
    """
    cache: dict = {}
    start = DAY_STEPS.index(first)
    found: list[tuple] = []
    while True:
        earlier = []
        for step in DAY_STEPS[start:]:
            for folder in missing_inputs(storage, step, day, cache):
                producer = DAY_STEPS.index(PRODUCERS[folder[0]])
                if producer < start:
                    earlier.append(producer)
                    if folder not in found:
                        found.append(folder)
        if not earlier:
            return DAY_STEPS[start], found
        start = min(earlier)


def days_in_progress(storage) -> set[date]:
    """The days that have started but are not published.

    A day has started when at least one of its steps is recorded as
    finished, and is published when its step 09 is. The folders such a day
    reads are never removed, whatever order the days run in.
    """
    started: set[date] = set()
    published: set[date] = set()
    for key in storage.done_keys():
        step, _, value = key.partition("@")
        try:
            day = date.fromisoformat(value)
        except ValueError:
            continue
        started.add(day)
        if step == "09":
            published.add(day)
    return started - published


def needed_folders(days: Sequence[date] | set[date], config=None) -> dict[str, set[str]]:
    """Table -> the day values of the internal tables that ``days`` read.

    The position reports of D-1 to D+1 and the tracks of D, for every day D.
    """
    needed: dict[str, set[str]] = {table: set() for table in INTERNAL_TABLES}
    for day in days:
        for k in (-1, 0, 1):
            needed["osn_statevectors_v2"].add((day + timedelta(days=k)).isoformat())
        needed["osn_tracks"].add(day.isoformat())
        needed["osn_tracks_clean"].add(day.isoformat())
    return needed


# --- planning ---------------------------------------------------------------


def days(start: date, end: date, order: str = "newest_first") -> list[date]:
    """Every day from ``start`` to ``end``, both included.

    Args:
        start: First day.
        end: Last day.
        order: ``"newest_first"`` (the default) or ``"oldest_first"``.

    Raises:
        ValueError: If ``end`` is before ``start`` or the order is unknown.
    """
    if order not in ORDERS:
        raise ValueError(f"order must be one of {ORDERS}, not {order!r}")
    if end < start:
        raise ValueError(f"end ({end}) is before start ({start})")
    result = [start + timedelta(days=n) for n in range((end - start).days + 1)]
    return result[::-1] if order == "newest_first" else result


# --- configuration and sessions ----------------------------------------------


def make_config(warehouse: str | None = None, worldwide: bool = True):
    """The ``opensky`` configuration for ``warehouse`` and the coverage."""
    from opdi.config import OPDIConfig

    return OPDIConfig.for_environment("opensky", worldwide=worldwide, warehouse=warehouse)


def _session(config, name: str, distributed: bool | None = None):
    """A Spark session for one unit, with a driver heap that fits the machine."""
    from opdi import periodrun
    from opdi.utils.spark_helpers import SparkSessionManager

    mem = periodrun._cgroup_v2_limit()
    limit_bytes, used_bytes = mem if mem else (None, 0)
    config.spark.driver_memory = periodrun.safe_driver_memory(
        config.spark.driver_memory, limit_bytes, used_bytes
    )
    if distributed is None:
        distributed = bool(config.spark.k8s_master)
    return SparkSessionManager.create_session(
        app_name=name,
        config=config,
        distributed=distributed,
        extra_configs=periodrun._session_extras(),
    )


def _step_kwargs() -> dict:
    from opdi import periodrun

    return dict(
        airports_hex_path="data/airport_hex/zones_res7_processed.parquet",
        airports_hex_raw_path="data/airport_hex/zones_res7.parquet",
        run_reference={k: True for k in periodrun.REFERENCE_OUTPUTS},
        adep_mode=None,
        ades_mode=None,
    )


# --- one unit -----------------------------------------------------------------


def unit_key(step: str, day: date | None) -> str:
    """Name under which a finished unit is recorded, for example ``02@2026-06-14``."""
    from opdi.periodrun import day_state_key

    return step if day is None else day_state_key(step, day)


def _check_unit(step: str, day: date | None) -> None:
    if step == REFERENCE_STEP:
        if day is not None:
            raise ValueError("step 00 builds the reference tables and takes no day")
        return
    if step not in DAY_STEPS:
        raise ValueError(
            f"unknown step {step!r}; expected {REFERENCE_STEP} or one of {', '.join(DAY_STEPS)}"
        )
    if day is None:
        raise ValueError(f"step {step} needs a day")


def run_unit(
    step: str,
    day: date | None,
    warehouse: str | None = None,
    worldwide: bool = True,
    force: bool = False,
    allow_empty: bool = False,
) -> int:
    """Run one step for one day, or step 00 when ``day`` is None.

    The unit is skipped when the warehouse records it as finished, unless
    ``force`` is set. It is recorded as finished only when it succeeds.

    Args:
        step: ``"00"`` or one of :data:`DAY_STEPS`.
        day: The day, or None for step 00.
        warehouse: The warehouse; see :meth:`opdi.config.OPDIConfig.for_environment`.
        worldwide: Process the whole world (the default) or only the European box.
        force: Run the unit even when it is recorded as finished.
        allow_empty: For step 09, replace published files that have rows
            with empty ones (see :func:`opdi.output.published.publish_day`).

    Returns:
        0 when the unit succeeded or was already finished, 1 when it failed.

    Raises:
        ValueError: If the step is unknown, or the day is missing or not
            expected.
    """
    _check_unit(step, day)
    from opdi import periodrun
    from opdi.utils.storage import StorageManager

    config = make_config(warehouse, worldwide)
    label = f"step {step}" + (f" day {day}" if day is not None else "")
    print(f"OPDI {label}")
    print(config.describe_target())

    spark = _session(config, f"OPDI {label}")
    started = time.time()
    try:
        storage = StorageManager(spark, config)
        problem = storage.check_coverage()
        if problem is not None:
            print(f"Refused: {problem}")
            return 1
        key = unit_key(step, day)
        # Step 01 is never skipped on its mark: it lists the report folders
        # of its three days and fetches only the missing ones.
        if not force and step != "01" and storage.is_done(key):
            print(f"  {label} is already finished; skipped (pass --force to run it again)")
            return 0
        if day is not None:
            gone = missing_inputs(storage, step, day)
            if gone:
                print(
                    f"Not started: {label} reads {_describe(gone)}, which is not in the "
                    "warehouse (removed, or never written). Build the day again from "
                    f"step 01 with: python -m opdi.orchestration day --day {day} --force"
                )
                return 1

        kwargs = _step_kwargs()
        if step == REFERENCE_STEP:
            from opdi.runner import STEPS, reference_substeps_for_coverage

            todo = periodrun.reference_substeps_to_run(storage, force=force)
            for substep, run in reference_substeps_for_coverage(config).items():
                if not run:
                    todo[substep] = False
            if not any(todo.values()):
                print("  every reference table is already built; nothing to do")
            else:
                if todo.get("00b"):
                    problems = periodrun.preflight_fresh_build(config)
                    if problems:
                        print("Not started: " + "; ".join(problems))
                        return 1
                kwargs["run_reference"] = todo
                STEPS[REFERENCE_STEP](spark, config, **kwargs)
        else:
            missing = periodrun.preflight(storage) if step in STEPS_NEEDING_REFERENCE else []
            if missing:
                print(
                    "Missing or unreadable reference tables: "
                    + ", ".join(missing)
                    + ". Run 'python -m opdi.orchestration reference' first."
                )
                return 1
            kwargs["allow_empty"] = allow_empty
            periodrun.run_day_step(spark, config, step, day, kwargs)

        elapsed = time.time() - started
        storage.mark_done(key, elapsed)
        print(f"  {label} finished in {elapsed:.0f} s")
        return 0
    except Exception as exc:  # noqa: BLE001 - reported as an exit code
        print(f"  {label} failed after {time.time() - started:.0f} s: {exc}")
        traceback.print_exc()
        return 1
    finally:
        spark.stop()


def unfinished_steps(day: date, warehouse: str | None = None, worldwide: bool = True) -> list[str]:
    """The steps of ``day`` not yet recorded as finished in the warehouse.

    Uses a small local Spark session, so that checking a finished day costs
    seconds rather than a session on the cluster.
    """
    from opdi.utils.storage import StorageManager

    config = make_config(warehouse, worldwide)
    spark = _session(config, f"OPDI check {day}", distributed=False)
    try:
        storage = StorageManager(spark, config)
        return [s for s in DAY_STEPS if not storage.is_done(unit_key(s, day))]
    finally:
        spark.stop()


def _resume_plan(day: date, first: str, warehouse: str | None, worldwide: bool) -> list[str]:
    """The steps to run for ``day``, whose first unfinished step is ``first``.

    When a folder that a remaining step reads is gone (removed by pruning,
    for example, while the day waited to be run again), the marks of the day
    are cleared from the step that writes that folder onwards, and the day
    starts again from step 01. Step 01 then lists its folders and fetches
    only the missing days, and the steps whose outputs are still there keep
    their marks and are skipped.
    """
    from opdi.utils.storage import StorageManager

    config = make_config(warehouse, worldwide)
    spark = _session(config, f"OPDI check {day}", distributed=False)
    try:
        storage = StorageManager(spark, config)
        start, gone = restart_step(storage, day, first)
        if start == first:
            return list(DAY_STEPS[DAY_STEPS.index(first) :])
        print(
            f"  day {day}: {_describe(gone)} is no longer in the warehouse; "
            f"the day runs again from step 01 (marks cleared from step {start})"
        )
        for step in DAY_STEPS[DAY_STEPS.index(start) :]:
            storage.clear_done(unit_key(step, day))
        return list(DAY_STEPS)
    finally:
        spark.stop()


def run_day(
    day: date,
    warehouse: str | None = None,
    worldwide: bool = True,
    force: bool = False,
    keep_internal: bool = False,
    min_free_gb: float = DEFAULT_MIN_FREE_GB,
    keep_days: int = DEFAULT_KEEP_DAYS,
    drop_published_working: bool = False,
    working_keep_days: int = DEFAULT_WORKING_KEEP_DAYS,
    allow_empty: bool = False,
) -> int:
    """Run every step of ``day`` in order, publishing last.

    Checks the free space first, then runs :data:`DAY_STEPS`. Finished steps
    are skipped, so running a failed day again continues where it stopped.
    After publishing, the day folders of the internal tables outside a window
    of ``keep_days`` days around ``day`` are removed (see :func:`prune_plan`),
    unless ``keep_internal`` is set. With ``drop_published_working``, the
    working events and measurements of published days are pruned as well;
    see :func:`prune_internal`.

    Returns:
        0 when every step succeeded, 1 at the first failure.
    """
    if force:
        todo = list(DAY_STEPS)
    else:
        todo = unfinished_steps(day, warehouse, worldwide)
        if not todo:
            print(f"  day {day} is already finished; skipped (pass --force to run it again)")
            return 0
        todo = _resume_plan(day, todo[0], warehouse, worldwide)
    if min_free_gb > 0:
        try:
            check_space(min_free_gb, warehouse=warehouse, worldwide=worldwide)
        except RuntimeError as exc:
            print(f"Not started: {exc}")
            return 1
    for step in todo:
        rc = run_unit(
            step,
            day,
            warehouse=warehouse,
            worldwide=worldwide,
            force=force,
            allow_empty=allow_empty,
        )
        if rc != 0:
            return rc
    if not keep_internal:
        prune_internal(
            day,
            keep_days,
            warehouse=warehouse,
            worldwide=worldwide,
            drop_published_working=drop_published_working,
            working_keep_days=working_keep_days,
        )
    return 0


# --- free space -----------------------------------------------------------------


def _bucket_root(path: str) -> str:
    scheme, rest = path.split("://", 1)
    return f"{scheme}://{rest.split('/', 1)[0]}"


def free_space_gb(
    warehouse: str | None = None,
    quota_gb: float | None = None,
    worldwide: bool = True,
) -> float:
    """Free space where the warehouse is, in GB (10^9 bytes).

    For a local folder it is the free space of its disk. For S3 it is the
    bucket quota minus the bytes stored in the whole bucket; counting them
    lists every file of the bucket, which takes a few seconds to minutes.

    Args:
        warehouse: The warehouse; see :func:`make_config`.
        quota_gb: Bucket quota in GB. When omitted, ``OPDI_BUCKET_QUOTA_GB``,
            else :data:`DEFAULT_QUOTA_GB`.
        worldwide: Coverage, used only to build the configuration.
    """
    config = make_config(warehouse, worldwide)
    root = config.project.release_root
    if root.startswith("file://") or "://" not in root:
        path = os.path.abspath(root[len("file://") :] if root.startswith("file://") else root)
        while not os.path.exists(path):
            path = os.path.dirname(path)
        return shutil.disk_usage(path).free / 1e9

    if quota_gb is None:
        quota_gb = float(os.environ.get(QUOTA_VARIABLE, DEFAULT_QUOTA_GB))
    spark = _session(config, "OPDI free space", distributed=False)
    try:
        jvm = spark._jvm
        p = jvm.org.apache.hadoop.fs.Path(_bucket_root(root) + "/")
        fs = p.getFileSystem(spark._jsc.hadoopConfiguration())
        used = int(fs.getContentSummary(p).getLength())
    finally:
        spark.stop()
    return quota_gb - used / 1e9


def check_space(
    min_free_gb: float,
    warehouse: str | None = None,
    quota_gb: float | None = None,
    worldwide: bool = True,
) -> None:
    """Fail when less than ``min_free_gb`` GB are free where the warehouse is.

    Raises:
        RuntimeError: If the free space is below ``min_free_gb``.
    """
    free = free_space_gb(warehouse, quota_gb, worldwide)
    print(f"  free space  : {free:.1f} GB (at least {min_free_gb:.1f} GB needed)")
    if free < min_free_gb:
        raise RuntimeError(
            f"only {free:.1f} GB free where the warehouse is, below the {min_free_gb:.1f} GB "
            "needed to process a day. Free space in the bucket, or lower the limit."
        )


# --- removing internal tables -------------------------------------------------


def prune_plan(
    day: date,
    keep_days: int,
    present: dict[str, Sequence[str]],
    in_progress: Sequence[date] | set[date] = (),
) -> dict[str, list[str]]:
    """Which day folders of the internal tables :func:`prune_internal` removes.

    The folders from ``keep_days - 1`` days before ``day`` to ``keep_days``
    days after it are kept; the others are removed. The window reaches both
    ways because a backfill can process the days newest first. The folders
    that a day in ``in_progress`` reads (see :func:`needed_folders`) are
    always kept, so a day that failed can be run again.

    Args:
        day: The day just published.
        keep_days: Size of the window kept; see :data:`DEFAULT_KEEP_DAYS`.
        present: Table name -> the day values of its folders.
        in_progress: Days that have started and are not published (see
            :func:`days_in_progress`).

    Returns:
        Table name -> the day values to remove, only for tables in
        :data:`INTERNAL_TABLES`.
    """
    if keep_days < 1:
        raise ValueError("keep_days must be at least 1: the next day needs the last one")
    oldest_kept = day - timedelta(days=keep_days - 1)
    newest_kept = day + timedelta(days=keep_days)
    needed = needed_folders(in_progress)
    plan: dict[str, list[str]] = {}
    for table, values in present.items():
        if table not in INTERNAL_TABLES:
            continue
        old = []
        for value in values:
            if str(value)[:10] in needed[table]:
                continue
            try:
                if not oldest_kept <= date.fromisoformat(str(value)[:10]) <= newest_kept:
                    old.append(value)
            except ValueError:
                continue
        if old:
            plan[table] = sorted(old)
    return plan


def working_prune_plan(
    day: date,
    keep_days: int,
    present: dict[str, Sequence[str]],
    published: set[date],
) -> dict[str, list[str]]:
    """Which day folders of the working events and measurements may be removed.

    A day folder is removed only when its day is published (both files and a
    catalogue row exist, see :func:`published_days`) and lies more than
    ``keep_days`` days before or after ``day``.

    Args:
        day: The day just published.
        keep_days: Days kept on each side of ``day``; 0 keeps only ``day``.
        present: Table name -> the day values of its folders.
        published: The days that are published.

    Returns:
        Table name -> the day values to remove, only for tables in
        :data:`WORKING_TABLES`.
    """
    if keep_days < 0:
        raise ValueError("keep_days must be 0 or more")
    oldest_kept = day - timedelta(days=keep_days)
    newest_kept = day + timedelta(days=keep_days)
    plan: dict[str, list[str]] = {}
    for table, values in present.items():
        if table not in WORKING_TABLES:
            continue
        old = []
        for value in values:
            try:
                value_day = date.fromisoformat(str(value)[:10])
            except ValueError:
                continue
            if value_day in published and not oldest_kept <= value_day <= newest_kept:
                old.append(value)
        if old:
            plan[table] = sorted(old)
    return plan


def published_days(storage) -> set[date]:
    """The days whose two published files and catalogue row all exist.

    Args:
        storage: A :class:`~opdi.utils.storage.StorageManager` (only its
            ``spark`` session and ``base_path`` are used).
    """
    from opdi.output.published import PublishedLayout, _exists

    spark = storage.spark
    layout = PublishedLayout.for_storage(storage)
    if not _exists(spark, layout.catalogue):
        return set()
    catalogued = {r["day"] for r in spark.read.parquet(layout.catalogue).select("day").collect()}
    return {
        d
        for d in catalogued
        if _exists(spark, layout.flight_list(d)) and _exists(spark, layout.flight_events(d))
    }


def prune_internal(
    day: date,
    keep_days: int = DEFAULT_KEEP_DAYS,
    warehouse: str | None = None,
    worldwide: bool = True,
    storage=None,
    drop_published_working: bool = False,
    working_keep_days: int = DEFAULT_WORKING_KEEP_DAYS,
) -> int:
    """Remove the day folders of the internal tables that are no longer needed.

    The tables in :data:`INTERNAL_TABLES` are pruned to a window around
    ``day``. With ``drop_published_working``, the working events and
    measurements (:data:`WORKING_TABLES`) of published days outside a wider
    window are removed too (see :func:`working_prune_plan`). The working
    flight list, the reference tables and everything under ``published/``
    are always kept.

    Args:
        day: The day just published.
        keep_days: Size of the window kept; see :func:`prune_plan`.
        warehouse: The warehouse; see :func:`make_config`.
        worldwide: Coverage, used only to build the configuration.
        storage: A :class:`~opdi.utils.storage.StorageManager` to use instead
            of opening a session.
        drop_published_working: Also remove the working events and
            measurements of days that are already published.
        working_keep_days: Days of working events and measurements kept on
            each side of ``day`` when ``drop_published_working`` is set.

    Returns:
        The number of day folders removed.

    Raises:
        RuntimeError: If a table to prune resolves to a path inside a
            ``published`` folder.
    """
    spark = None
    if storage is None:
        from opdi.utils.storage import StorageManager

        config = make_config(warehouse, worldwide)
        spark = _session(config, f"OPDI prune {day}", distributed=False)
        storage = StorageManager(spark, config)
    try:
        tables = dict(INTERNAL_TABLES)
        if drop_published_working:
            tables.update(WORKING_TABLES)
        for table in tables:
            path = f"{storage.base_path.rstrip('/')}/{table}"
            if "/published/" in path + "/":
                raise RuntimeError(f"refusing to prune {path}: it is inside a published folder")
        present = {}
        for table, column in INTERNAL_TABLES.items():
            try:
                present[table] = storage.list_partitions(table, column)
            except Exception:  # noqa: BLE001 - a missing table has nothing to prune
                present[table] = []
        plan = prune_plan(day, keep_days, present, days_in_progress(storage))
        if drop_published_working:
            working = {}
            for table, column in WORKING_TABLES.items():
                try:
                    working[table] = storage.list_partitions(table, column)
                except Exception:  # noqa: BLE001 - a missing table has nothing to prune
                    working[table] = []
            plan.update(
                working_prune_plan(day, working_keep_days, working, published_days(storage))
            )
        removed = 0
        for table, values in plan.items():
            n = storage.drop_partitions(table, tables[table], values)
            print(f"  removed {n} day folder(s) of {table}: {', '.join(values)}")
            removed += n
        return removed
    finally:
        if spark is not None:
            spark.stop()


# --- the publish lock -------------------------------------------------------------


def clear_publish_lock(warehouse: str | None = None, worldwide: bool = True) -> bool:
    """Remove the publish lock that a stopped publish left behind.

    Check first that no OPDI command still runs against the warehouse.
    Returns whether there was a lock.
    """
    from opdi.output.published import PublishedLayout, clear_lock
    from opdi.utils.storage import StorageManager

    config = make_config(warehouse, worldwide)
    spark = _session(config, "OPDI clear lock", distributed=False)
    try:
        layout = PublishedLayout.for_storage(StorageManager(spark, config))
        removed = clear_lock(spark, layout)
        print(f"  publish lock {'removed' if removed else 'not found'} in {layout.state}")
        return removed
    finally:
        spark.stop()


# --- command line --------------------------------------------------------------


def _day(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected a date as YYYY-MM-DD, got {value!r}") from None


def _working_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--drop-published-working",
        action="store_true",
        help="Also remove the working events and measurements of days that are "
        "already published (both files and a catalogue row). The working flight "
        "list is always kept.",
    )
    parser.add_argument(
        "--working-keep-days",
        type=int,
        default=DEFAULT_WORKING_KEEP_DAYS,
        help="Days of working events and measurements kept on each side of the day "
        f"(default {DEFAULT_WORKING_KEEP_DAYS}).",
    )


def _allow_empty_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help="Step 09: replace published files that have rows with empty ones. "
        "Without it, publishing an empty day over a published one is refused.",
    )


def _parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--warehouse",
        default=None,
        help="Warehouse to use (default: OPDI_WAREHOUSE if set, else " "s3a://eurocontrol/opdi).",
    )
    common.add_argument(
        "--europe",
        dest="worldwide",
        action="store_false",
        default=True,
        help="Keep only positions inside the European box.",
    )
    common.add_argument(
        "--force",
        action="store_true",
        help="Run units again even when they are recorded as finished.",
    )

    parser = argparse.ArgumentParser(
        prog="python -m opdi.orchestration",
        description="Run the OPDI pipeline one step and one day at a time.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("reference", parents=[common], help="Build the reference tables (step 00).")

    step = sub.add_parser("step", parents=[common], help="Run one step for one day.")
    step.add_argument("--step", required=True, choices=DAY_STEPS)
    step.add_argument("--day", type=_day, required=True)
    _allow_empty_argument(step)

    day = sub.add_parser("day", parents=[common], help="Run every step of one day.")
    day.add_argument("--day", type=_day, required=True)
    day.add_argument(
        "--keep-internal",
        action="store_true",
        help="Keep the internal tables of the day after publishing.",
    )
    day.add_argument(
        "--min-free-gb",
        type=float,
        default=DEFAULT_MIN_FREE_GB,
        help=f"Free space needed to start, in GB (default {DEFAULT_MIN_FREE_GB}). "
        "0 skips the check.",
    )
    day.add_argument("--keep-days", type=int, default=DEFAULT_KEEP_DAYS)
    _working_arguments(day)
    _allow_empty_argument(day)

    space = sub.add_parser("check-space", parents=[common], help="Fail when space is short.")
    space.add_argument("--min-free-gb", type=float, default=DEFAULT_MIN_FREE_GB)
    space.add_argument(
        "--quota-gb",
        type=float,
        default=None,
        help=f"Bucket quota in GB (default: {QUOTA_VARIABLE} if set, else " f"{DEFAULT_QUOTA_GB}).",
    )

    sub.add_parser(
        "clear-lock",
        parents=[common],
        help="Remove the publish lock left by a publish that stopped.",
    )

    prune = sub.add_parser("prune", parents=[common], help="Remove old internal day folders.")
    prune.add_argument("--day", type=_day, required=True)
    prune.add_argument("--keep-days", type=int, default=DEFAULT_KEEP_DAYS)
    _working_arguments(prune)

    plan = sub.add_parser("plan", help="Print the days of a range as a JSON list.")
    plan.add_argument("--start", type=_day, required=True)
    plan.add_argument("--end", type=_day, required=True)
    plan.add_argument("--order", choices=ORDERS, default="newest_first")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run one command; see the module description. Returns the exit code."""
    args = _parser().parse_args(argv)
    try:
        if args.command == "plan":
            print(json.dumps([d.isoformat() for d in days(args.start, args.end, args.order)]))
            return 0
        if args.command == "reference":
            return run_unit(REFERENCE_STEP, None, args.warehouse, args.worldwide, args.force)
        if args.command == "step":
            if args.allow_empty:
                return run_unit(
                    args.step,
                    args.day,
                    args.warehouse,
                    args.worldwide,
                    args.force,
                    allow_empty=True,
                )
            return run_unit(args.step, args.day, args.warehouse, args.worldwide, args.force)
        if args.command == "day":
            return run_day(
                args.day,
                args.warehouse,
                args.worldwide,
                args.force,
                args.keep_internal,
                args.min_free_gb,
                args.keep_days,
                args.drop_published_working,
                args.working_keep_days,
                args.allow_empty,
            )
        if args.command == "check-space":
            check_space(args.min_free_gb, args.warehouse, args.quota_gb, args.worldwide)
            return 0
        if args.command == "clear-lock":
            clear_publish_lock(args.warehouse, args.worldwide)
            return 0
        if args.command == "prune":
            prune_internal(
                args.day,
                args.keep_days,
                args.warehouse,
                args.worldwide,
                drop_published_working=args.drop_published_working,
                working_keep_days=args.working_keep_days,
            )
            return 0
    except Exception as exc:  # noqa: BLE001 - every failure is an exit code
        print(f"Failed: {exc}", file=sys.stderr)
        traceback.print_exc()
        return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
