"""Run the OPDI pipeline one step and one day at a time.

A scheduler such as Airflow runs the pipeline as a chain of small units: one
step (for example step 02, track building) for one day. This module runs one
such unit and is meant to be called as a command::

    python -m opdi.orchestration reference                       # step 00, once
    python -m opdi.orchestration step --step 02 --day 2026-06-14  # one step, one day
    python -m opdi.orchestration day --day 2026-06-14             # every step of a day
    python -m opdi.orchestration check-space --min-free-gb 5
    python -m opdi.orchestration prune --day 2026-06-14
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
from typing import Dict, List, Optional, Sequence

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

#: The step that builds the reference tables (airports, runways, aircraft). It
#: runs once, not per day, and skips the tables that already exist.
REFERENCE_STEP = "00"

#: Internal tables that may be removed once a day is published, with the
#: column their files are split by (one folder per day). Nothing else is ever
#: removed.
INTERNAL_TABLES: Dict[str, str] = {
    "osn_statevectors_v2": "event_time_day",
    "osn_tracks": "dof",
    "osn_tracks_clean": "dof",
}

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

#: Free space below which a day does not start, in GB.
DEFAULT_MIN_FREE_GB = 5.0

ORDERS = ("newest_first", "oldest_first")


# --- planning ---------------------------------------------------------------

def days(start: date, end: date, order: str = "newest_first") -> List[date]:
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

def make_config(warehouse: Optional[str] = None, worldwide: bool = True):
    """The ``opensky`` configuration for ``warehouse`` and the coverage."""
    from opdi.config import OPDIConfig

    return OPDIConfig.for_environment("opensky", worldwide=worldwide, warehouse=warehouse)


def _session(config, name: str, distributed: Optional[bool] = None):
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
        app_name=name, config=config, distributed=distributed,
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

def unit_key(step: str, day: Optional[date]) -> str:
    """Name under which a finished unit is recorded, for example ``02@2026-06-14``."""
    from opdi.periodrun import day_state_key

    return step if day is None else day_state_key(step, day)


def _check_unit(step: str, day: Optional[date]) -> None:
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
    day: Optional[date],
    warehouse: Optional[str] = None,
    worldwide: bool = True,
    force: bool = False,
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
        if not force and storage.is_done(key):
            print(f"  {label} is already finished; skipped (pass --force to run it again)")
            return 0

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
            missing = periodrun.preflight(storage)
            if missing:
                print("Missing or unreadable reference tables: " + ", ".join(missing)
                      + ". Run 'python -m opdi.orchestration reference' first.")
                return 1
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


def unfinished_steps(
    day: date, warehouse: Optional[str] = None, worldwide: bool = True
) -> List[str]:
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


def run_day(
    day: date,
    warehouse: Optional[str] = None,
    worldwide: bool = True,
    force: bool = False,
    keep_internal: bool = False,
    min_free_gb: float = DEFAULT_MIN_FREE_GB,
    keep_days: int = DEFAULT_KEEP_DAYS,
) -> int:
    """Run every step of ``day`` in order, publishing last.

    Checks the free space first, then runs :data:`DAY_STEPS`. Finished steps
    are skipped, so running a failed day again continues where it stopped.
    After publishing, the day folders of the internal tables outside a window
    of ``keep_days`` days around ``day`` are removed (see :func:`prune_plan`),
    unless ``keep_internal`` is set.

    Returns:
        0 when every step succeeded, 1 at the first failure.
    """
    todo = list(DAY_STEPS) if force else unfinished_steps(day, warehouse, worldwide)
    if not todo:
        print(f"  day {day} is already finished; skipped (pass --force to run it again)")
        return 0
    if min_free_gb > 0:
        try:
            check_space(min_free_gb, warehouse=warehouse, worldwide=worldwide)
        except RuntimeError as exc:
            print(f"Not started: {exc}")
            return 1
    for step in todo:
        rc = run_unit(step, day, warehouse=warehouse, worldwide=worldwide, force=force)
        if rc != 0:
            return rc
    if not keep_internal:
        prune_internal(day, keep_days, warehouse=warehouse, worldwide=worldwide)
    return 0


# --- free space -----------------------------------------------------------------

def _bucket_root(path: str) -> str:
    scheme, rest = path.split("://", 1)
    return f"{scheme}://{rest.split('/', 1)[0]}"


def free_space_gb(
    warehouse: Optional[str] = None,
    quota_gb: Optional[float] = None,
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
        path = os.path.abspath(root[len("file://"):] if root.startswith("file://") else root)
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
    warehouse: Optional[str] = None,
    quota_gb: Optional[float] = None,
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
    day: date, keep_days: int, present: Dict[str, Sequence[str]]
) -> Dict[str, List[str]]:
    """Which day folders of the internal tables :func:`prune_internal` removes.

    The folders from ``keep_days - 1`` days before ``day`` to ``keep_days``
    days after it are kept; the others are removed. The window reaches both
    ways because a backfill can process the days newest first.

    Args:
        day: The day just published.
        keep_days: Size of the window kept; see :data:`DEFAULT_KEEP_DAYS`.
        present: Table name -> the day values of its folders.

    Returns:
        Table name -> the day values to remove, only for tables in
        :data:`INTERNAL_TABLES`.
    """
    if keep_days < 1:
        raise ValueError("keep_days must be at least 1: the next day needs the last one")
    oldest_kept = day - timedelta(days=keep_days - 1)
    newest_kept = day + timedelta(days=keep_days)
    plan: Dict[str, List[str]] = {}
    for table, values in present.items():
        if table not in INTERNAL_TABLES:
            continue
        old = []
        for value in values:
            try:
                if not oldest_kept <= date.fromisoformat(str(value)[:10]) <= newest_kept:
                    old.append(value)
            except ValueError:
                continue
        if old:
            plan[table] = sorted(old)
    return plan


def prune_internal(
    day: date,
    keep_days: int = DEFAULT_KEEP_DAYS,
    warehouse: Optional[str] = None,
    worldwide: bool = True,
    storage=None,
) -> int:
    """Remove the day folders of the internal tables that are no longer needed.

    Only the tables in :data:`INTERNAL_TABLES` are touched. The working flight
    list, events and measurements tables and everything under ``published/``
    are kept.

    Args:
        day: The day just published.
        keep_days: Size of the window kept; see :func:`prune_plan`.
        warehouse: The warehouse; see :func:`make_config`.
        worldwide: Coverage, used only to build the configuration.
        storage: A :class:`~opdi.utils.storage.StorageManager` to use instead
            of opening a session.

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
        for table in INTERNAL_TABLES:
            path = f"{storage.base_path.rstrip('/')}/{table}"
            if "/published/" in path + "/":
                raise RuntimeError(f"refusing to prune {path}: it is inside a published folder")
        present = {}
        for table, column in INTERNAL_TABLES.items():
            try:
                present[table] = storage.list_partitions(table, column)
            except Exception:  # noqa: BLE001 - a missing table has nothing to prune
                present[table] = []
        removed = 0
        for table, values in prune_plan(day, keep_days, present).items():
            n = storage.drop_partitions(table, INTERNAL_TABLES[table], values)
            print(f"  removed {n} day folder(s) of {table}: {', '.join(values)}")
            removed += n
        return removed
    finally:
        if spark is not None:
            spark.stop()


# --- command line --------------------------------------------------------------

def _day(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected a date as YYYY-MM-DD, got {value!r}")


def _parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--warehouse", default=None,
                        help="Warehouse to use (default: OPDI_WAREHOUSE if set, else "
                             "s3a://eurocontrol/opdi).")
    common.add_argument("--europe", dest="worldwide", action="store_false", default=True,
                        help="Keep only positions inside the European box.")
    common.add_argument("--force", action="store_true",
                        help="Run units again even when they are recorded as finished.")

    parser = argparse.ArgumentParser(
        prog="python -m opdi.orchestration",
        description="Run the OPDI pipeline one step and one day at a time.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("reference", parents=[common], help="Build the reference tables (step 00).")

    step = sub.add_parser("step", parents=[common], help="Run one step for one day.")
    step.add_argument("--step", required=True, choices=DAY_STEPS)
    step.add_argument("--day", type=_day, required=True)

    day = sub.add_parser("day", parents=[common], help="Run every step of one day.")
    day.add_argument("--day", type=_day, required=True)
    day.add_argument("--keep-internal", action="store_true",
                     help="Keep the internal tables of the day after publishing.")
    day.add_argument("--min-free-gb", type=float, default=DEFAULT_MIN_FREE_GB,
                     help=f"Free space needed to start, in GB (default {DEFAULT_MIN_FREE_GB}). "
                          "0 skips the check.")
    day.add_argument("--keep-days", type=int, default=DEFAULT_KEEP_DAYS)

    space = sub.add_parser("check-space", parents=[common], help="Fail when space is short.")
    space.add_argument("--min-free-gb", type=float, default=DEFAULT_MIN_FREE_GB)
    space.add_argument("--quota-gb", type=float, default=None,
                       help=f"Bucket quota in GB (default: {QUOTA_VARIABLE} if set, else "
                            f"{DEFAULT_QUOTA_GB}).")

    prune = sub.add_parser("prune", parents=[common], help="Remove old internal day folders.")
    prune.add_argument("--day", type=_day, required=True)
    prune.add_argument("--keep-days", type=int, default=DEFAULT_KEEP_DAYS)

    plan = sub.add_parser("plan", help="Print the days of a range as a JSON list.")
    plan.add_argument("--start", type=_day, required=True)
    plan.add_argument("--end", type=_day, required=True)
    plan.add_argument("--order", choices=ORDERS, default="newest_first")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run one command; see the module description. Returns the exit code."""
    args = _parser().parse_args(argv)
    try:
        if args.command == "plan":
            print(json.dumps([d.isoformat() for d in days(args.start, args.end, args.order)]))
            return 0
        if args.command == "reference":
            return run_unit(REFERENCE_STEP, None, args.warehouse, args.worldwide, args.force)
        if args.command == "step":
            return run_unit(args.step, args.day, args.warehouse, args.worldwide, args.force)
        if args.command == "day":
            return run_day(args.day, args.warehouse, args.worldwide, args.force,
                           args.keep_internal, args.min_free_gb, args.keep_days)
        if args.command == "check-space":
            check_space(args.min_free_gb, args.warehouse, args.quota_gb, args.worldwide)
            return 0
        if args.command == "prune":
            prune_internal(args.day, args.keep_days, args.warehouse, args.worldwide)
            return 0
    except Exception as exc:  # noqa: BLE001 - every failure is an exit code
        print(f"Failed: {exc}", file=sys.stderr)
        traceback.print_exc()
        return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
