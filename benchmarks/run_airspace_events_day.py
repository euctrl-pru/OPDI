#!/usr/bin/env python
"""Run step 00c and step 04 for one day with the navigation families off and on.

    export PYSPARK_DRIVER_PYTHON=$PWD/.venv310/bin/python
    unset PYSPARK_PYTHON
    .venv310/bin/python -u benchmarks/run_airspace_events_day.py --day 2026-06-14 --steps 00c 03
    .venv310/bin/python -u benchmarks/run_airspace_events_day.py --day 2026-06-14 --arm off --steps 04 \\
        --timings-csv <paper>/data/pipeline_day/timings.csv
    .venv310/bin/python -u benchmarks/run_airspace_events_day.py --day 2026-06-14 --arm on --steps 04 \\
        --timings-csv <paper>/data/pipeline_day/timings.csv

The measurement run of the holding / point merge / FIR study
(``docs/superpowers/specs/2026-11-10-airspace-events-design.md``). The two
arms differ in the three ``emit_*`` flags and nothing else.

**Where it reads.** ``osn_tracks_clean`` for the day from the production
worldwide warehouse ``s3a://eurocontrol/opdi-world`` (read only); that day has
no flight list, so step 03 builds one under the study prefix (``shared/``).
With ``--input a10`` both come from the v5 study arm
``opdi-world/research/a10/cruise_linked`` (a one-in-ten airframe sample).
Reference tables are read from ``opdi-world`` and never written.

**Where it writes.** Only under ``s3a://eurocontrol/opdi-world/research/
airspace_events/``: the compact FIR reference under ``ref/`` (step 00c) and
each arm's ``opdi_flight_events`` and ``opdi_measurements`` under ``<arm>/``.
:func:`guard_write_path` raises on anything else. The v5 arm's step 04 output,
which the v4 and v5 papers depend on, is never touched.

**The bucket.** ``eurocontrol`` has a 200 GB quota; :func:`check_free_space`
stops the run under 3 GB free before Spark starts.
"""

import argparse
import re
import sys
import time
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

from run_track_link_day import (  # noqa: E402
    FLOOR_GB,
    QUOTA_GB,
    bucket_used_gb,
    check_free_space,
    record_timing,
)

from opdi.config import OPDIConfig  # noqa: E402
from opdi.periodrun import run_day_step  # noqa: E402
from opdi.utils.storage import StorageManager  # noqa: E402

PREFIX = "s3a://eurocontrol/opdi-world/research/airspace_events"
INPUT_WAREHOUSE = "s3a://eurocontrol/opdi-world"
A10_ARM = "s3a://eurocontrol/opdi-world/research/a10/cruise_linked"

ALLOWED_WRITE_ROOT = re.compile(r"^s3a://eurocontrol/opdi-world/research/airspace_events/")

#: Reference data, read from the production warehouse and never written.
PASSTHROUGH = (
    "h3_airport_detection_zones",
    "h3_runway_zones",
    "hexaero_airport_layouts",
    "oa_airports",
    "oa_runways",
    "osn_aircraft_db",
    "coverage_marker",
)

#: The tracks: read from the chosen input (production day or the v5 arm), never written.
INPUTS = ("osn_tracks_clean", "osn_tracks")

#: The flight list and what step 03 writes beside it. The production day has
#: no flight list, so with ``--input world`` step 03 builds one under
#: ``shared/``; with ``--input a10`` the v5 arm's is read.
FLIGHTS = ("opdi_flight_list", "opdi_endpoint_candidates", "opdi_flight_list_milestones_staging")

#: Written once by step 00c, read by both arms.
SHARED = ("opdi_h3_airspace_ref",)

#: What each arm produces.
DERIVED = ("opdi_flight_events", "opdi_measurements")

ARMS = ("off", "on")
STEPS = ("00c", "03", "04")
NAVIGATION_FLAGS = ("emit_holding", "emit_point_merge", "emit_fir_crossings")

_ORIG = {}


def arm_path(arm: str, table: str, input_source: str = "world") -> str:
    """Absolute S3 path for *table* under *arm*."""
    if arm not in ARMS:
        raise ValueError(f"unknown arm {arm!r}; expected one of {ARMS}")
    if table in INPUTS:
        return f"{A10_ARM}/{table}" if input_source == "a10" else f"{INPUT_WAREHOUSE}/{table}"
    if table in FLIGHTS:
        return f"{A10_ARM}/{table}" if input_source == "a10" else f"{PREFIX}/shared/{table}"
    if table in SHARED:
        return f"{PREFIX}/ref/{table}"
    if table in DERIVED:
        return f"{PREFIX}/{arm}/{table}"
    raise RuntimeError(
        f"unclassified table {table!r}: add it to PASSTHROUGH, INPUTS, FLIGHTS, SHARED or "
        f"DERIVED in benchmarks/run_airspace_events_day.py.")


def guard_write_path(path: str) -> None:
    """Refuse a write outside the study prefix. Raises."""
    if not ALLOWED_WRITE_ROOT.match(path):
        raise AssertionError(
            f"refusing to write {path!r}: every table this script writes must sit "
            f"under {PREFIX!r}. Inputs and reference tables are read-only.")


def build_config(arm: str, executors: int = 8) -> OPDIConfig:
    """Worldwide production config with the three navigation flags set by *arm*."""
    if arm not in ARMS:
        raise ValueError(f"unknown arm {arm!r}; expected one of {ARMS}")
    cfg = OPDIConfig.for_environment("opensky", worldwide=True)
    cfg.project.warehouse_path = INPUT_WAREHOUSE
    cfg.adsc.enabled = False
    cfg.spark.executor_instances = str(executors)
    for flag in NAVIGATION_FLAGS:
        setattr(cfg.events, flag, arm == "on")
    return cfg


def redirect(arm: str, input_source: str) -> None:
    """Resolve tables to the study prefix or the inputs; pass reference through."""
    if not _ORIG:
        _ORIG.update(path=StorageManager._s3_path, write=StorageManager.write_table,
                     drop=StorageManager.drop_partitions)
    orig_path, orig_write, orig_drop = _ORIG["path"], _ORIG["write"], _ORIG["drop"]

    def _s3_path(self, table_name):
        if table_name in PASSTHROUGH:
            return orig_path(self, table_name)
        return arm_path(arm, table_name, input_source)

    def write_table(self, df, table_name, mode="append", partition_by=None, partition_values=None):
        guard_write_path(_s3_path(self, table_name))
        return orig_write(self, df, table_name, mode, partition_by, partition_values)

    def drop_partitions(self, table_name, column, values):
        guard_write_path(_s3_path(self, table_name))
        return orig_drop(self, table_name, column, values)

    StorageManager._s3_path = _s3_path
    StorageManager.write_table = write_table
    StorageManager.drop_partitions = drop_partitions


def restore() -> None:
    if _ORIG:
        StorageManager._s3_path = _ORIG["path"]
        StorageManager.write_table = _ORIG["write"]
        StorageManager.drop_partitions = _ORIG["drop"]
        _ORIG.clear()


def study_session(app_name: str, executors: int = 4):
    """A cluster session for the scripts that read this study's tables: the
    pod only hosts the driver (16 GB cap), the executors run on K8s."""
    from opdi.utils.spark_helpers import SparkSessionManager

    return SparkSessionManager.create_session(
        app_name=app_name, config=build_config("on", executors), distributed=True)


def run(arm: str, day: date, executors: int, steps, input_source: str, timings_csv=None) -> None:
    from opdi.utils.spark_helpers import SparkSessionManager

    cfg = build_config(arm, executors)
    print("=" * 72)
    print(f"  arm        : {arm}  ({', '.join(f'{f}={getattr(cfg.events, f)}' for f in NAVIGATION_FLAGS)})")
    print(f"  day        : {day}")
    print(f"  inputs     : {arm_path(arm, 'osn_tracks_clean', input_source)}  (read-only)")
    print(f"  outputs    : {PREFIX}/{arm}  and  {PREFIX}/ref")
    print(f"  steps      : {' '.join(steps)}")
    print("=" * 72, flush=True)
    check_free_space(bucket_used_gb(), QUOTA_GB, FLOOR_GB)

    redirect(arm, input_source)
    spark = SparkSessionManager.create_session(
        app_name=f"airspace events {arm} {day}", config=cfg, distributed=True)
    try:
        kwargs = {"log_dir": str(REPO / "OPDI_live" / "logs" / f"airspace-events-{arm}")}
        for step in steps:
            print(f"\n{'-' * 72}\n>>> arm {arm}  step {step}  day {day}\n{'-' * 72}", flush=True)
            started = time.monotonic()
            if step == "00c":
                from opdi.reference.h3_airspaces import AirspaceH3Generator

                gen = AirspaceH3Generator(spark, cfg)
                gen.process_fir()
            else:
                run_day_step(spark, cfg, step, day, kwargs)
            if timings_csv:
                record_timing(timings_csv, arm, step, time.monotonic() - started)
    finally:
        spark.stop()
        restore()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--day", required=True, help="YYYY-MM-DD")
    ap.add_argument("--arm", choices=ARMS, default="on")
    ap.add_argument("--steps", nargs="+", default=["04"], choices=STEPS)
    ap.add_argument("--input", choices=("world", "a10"), default="world")
    ap.add_argument("--executors", type=int, default=8)
    ap.add_argument("--timings-csv", default=None)
    args = ap.parse_args()
    run(args.arm, date.fromisoformat(args.day), args.executors, args.steps, args.input, args.timings_csv)
    print(f"\nDone. arm {args.arm} for {args.day} under {PREFIX}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
