#!/usr/bin/env python
"""Run step 04 for one day with the code of this checkout, under the plausibility study prefix.

    export PYSPARK_DRIVER_PYTHON=$PWD/.venv310/bin/python
    unset PYSPARK_PYTHON
    .venv310/bin/python -u benchmarks/run_plausibility_day.py --day 2026-06-14

The "after" side of the milestone plausibility study
(``benchmarks/milestone_plausibility.py before-after``). The "before" side is
the events of the airspace events study (``research/airspace_events/on``),
built with the same settings by the code before the plausibility checks and
the two detector corrections. Both use the default settings with the
navigation families on (:func:`run_airspace_events_day.build_config`).

**Where it reads** (read only): the cleaned tracks of the day from
``s3a://eurocontrol/opdi-world``, the flight list and the FIR reference of
the airspace events study, and the reference tables of ``opdi-world``.

**Where it writes.** Only under
``s3a://eurocontrol/opdi-world/research/plausibility/after/``: the events,
the measurements and the removed events. :func:`guard_write_path` raises on
anything else. Delete the folder once the study tables are staged
(``milestone_plausibility.py clean-after``).
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

from run_airspace_events_day import (  # noqa: E402
    PASSTHROUGH,
    S3_PATIENCE,
    build_config,
)
from run_airspace_events_day import PREFIX as AIRSPACE_PREFIX  # noqa: E402
from run_track_link_day import QUOTA_GB, bucket_used_gb, check_free_space  # noqa: E402

from opdi.periodrun import run_day_step  # noqa: E402
from opdi.utils.storage import StorageManager  # noqa: E402

PREFIX = "s3a://eurocontrol/opdi-world/research/plausibility/after"
INPUT_WAREHOUSE = "s3a://eurocontrol/opdi-world"
ALLOWED_WRITE_ROOT = re.compile(r"^s3a://eurocontrol/opdi-world/research/plausibility/after/")

#: Free space needed before the run, in GB: the day writes about 1.3 GB.
NEEDED_GB = 3.0

INPUTS = ("osn_tracks_clean", "osn_tracks")
FLIGHTS = ("opdi_flight_list", "opdi_endpoint_candidates", "opdi_flight_list_milestones_staging")
SHARED = ("opdi_h3_airspace_ref",)
DERIVED = ("opdi_flight_events", "opdi_measurements", "opdi_flight_events_rejected")


def table_path(table: str) -> str:
    """Absolute S3 path of ``table`` in this run."""
    if table in INPUTS:
        return f"{INPUT_WAREHOUSE}/{table}"
    if table in FLIGHTS:
        return f"{AIRSPACE_PREFIX}/shared/{table}"
    if table in SHARED:
        return f"{AIRSPACE_PREFIX}/ref/{table}"
    if table in DERIVED:
        return f"{PREFIX}/{table}"
    raise RuntimeError(f"unclassified table {table!r} in benchmarks/run_plausibility_day.py")


def guard_write_path(path: str) -> None:
    """Refuse a write outside the study prefix. Raises."""
    if not ALLOWED_WRITE_ROOT.match(path):
        raise AssertionError(f"refusing to write {path!r}: this run writes under {PREFIX!r} only")


def redirect() -> None:
    """Resolve tables to the study prefix or the inputs; pass reference through."""
    orig_path = StorageManager._s3_path
    orig_write = StorageManager.write_table
    orig_drop = StorageManager.drop_partitions

    def _s3_path(self, table_name):
        if table_name in PASSTHROUGH:
            return orig_path(self, table_name)
        return table_path(table_name)

    def write_table(self, df, table_name, mode="append", partition_by=None, partition_values=None):
        guard_write_path(_s3_path(self, table_name))
        return orig_write(self, df, table_name, mode, partition_by, partition_values)

    def drop_partitions(self, table_name, column, values):
        guard_write_path(_s3_path(self, table_name))
        return orig_drop(self, table_name, column, values)

    StorageManager._s3_path = _s3_path
    StorageManager.write_table = write_table
    StorageManager.drop_partitions = drop_partitions


def main() -> int:
    from opdi.utils.spark_helpers import SparkSessionManager

    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--day", required=True, help="YYYY-MM-DD")
    ap.add_argument("--executors", type=int, default=8)
    args = ap.parse_args()
    day = date.fromisoformat(args.day)

    used = bucket_used_gb()
    print(f"bucket: {used:.1f} of {QUOTA_GB:.0f} GB used", flush=True)
    check_free_space(used, QUOTA_GB, NEEDED_GB)

    cfg = build_config("on", args.executors)
    redirect()
    spark = SparkSessionManager.create_session(
        app_name=f"plausibility after {day}",
        config=cfg,
        distributed=True,
        extra_configs=S3_PATIENCE,
    )
    started = time.monotonic()
    try:
        log_dir = str(REPO / "OPDI_live" / "logs" / "plausibility-after")
        run_day_step(spark, cfg, "04", day, {"log_dir": log_dir})
    finally:
        spark.stop()
    print(f"step 04 for {day} in {time.monotonic() - started:.0f} s under {PREFIX}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
