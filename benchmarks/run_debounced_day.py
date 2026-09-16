"""Run the ``debounced`` segmentation arm through the real per-day pipeline
for one day (2026-06-03), into a self-contained research warehouse, so
movement counts can be compared against the shipped ``recommended`` arm
without touching production.

Task 5 of ``.superpowers/sdd/2026-09-15-track-splitting-callsign-debounce/``.
Task 4 already validated ``debounced`` on fragmentation and merge-neutrality
in isolation; this is the "nothing broke" guard -- run it through steps
02 -> 04b exactly as the period runner would, and check APDF movement counts
against the shipped arm.

Approach (supersedes the earlier ``_s3_path`` redirect / table-mapping
version of this script, which failed twice): rather than patching
``StorageManager`` to send this run's own tables to a research prefix while
reading everything else from ``opdi-prod``, every read-only input the run
needs -- 11 reference tables plus state-vector days 2026-06-03 and
2026-06-04 -- has been copied ahead of time into a self-contained warehouse
at ``s3a://eurocontrol/opdi/research/a9``. So this run uses ONE warehouse
path for both reads and writes, and no monkeypatching at all:
``opdi_endpoint_candidates`` and every other table step 03 onward writes
lands in the same warehouse it is read back from, which is what a plain
``process_day`` call already does unassisted -- the earlier PATH_NOT_FOUND
on that table cannot recur because there is no second, un-redirected
warehouse for it to have been read from by mistake.

Steps are run for the single day via the exact per-day processor calls
``opdi.periodrun.run_day_step`` uses for each step, so this script cannot
drift from the real per-day entry points.

Usage::

    export PYSPARK_DRIVER_PYTHON=$PWD/.venv310/bin/python
    unset PYSPARK_PYTHON   # executors must use the opdi-spark image's own
                            # python (it has h3); pointing them at this venv
                            # -- which does not exist in the executor
                            # container -- kills step 02's h3_res_12 UDF with
                            # "Cannot run program .../.venv310/bin/python:
                            # Exec failed". SparkSessionManager.create_session
                            # (distributed=True) already configures the
                            # executor side correctly; do not also set
                            # spark.pyspark.python.
    .venv310/bin/python -u benchmarks/run_debounced_day.py \\
        --day 2026-06-03 --persistence-seconds 30 --executors 8

Downstream: ``benchmarks/movement_counts.py --days 2026-06-03
--warehouse opdi/research/a9`` against the debounced result, and
``--warehouse opdi-prod`` against the shipped baseline.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from opdi.config import OPDIConfig  # noqa: E402
from opdi.periodrun import run_day_step  # noqa: E402
from opdi.utils.spark_helpers import SparkSessionManager  # noqa: E402

RESEARCH_WAREHOUSE = "s3a://eurocontrol/opdi/research/a9"

STEPS = ("02", "02a", "03", "04", "04b")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--day", default="2026-06-03", help="YYYY-MM-DD")
    ap.add_argument("--persistence-seconds", type=float, default=30.0,
                     help="callsign_min_persistence_seconds for the debounced arm")
    ap.add_argument("--executors", type=int, default=8)
    args = ap.parse_args()
    day = date.fromisoformat(args.day)

    cfg = OPDIConfig.for_environment("opensky")
    cfg.project.warehouse_path = RESEARCH_WAREHOUSE
    cfg.segmentation.method = "debounced"
    cfg.segmentation.callsign_min_persistence_seconds = args.persistence_seconds
    cfg.spark.executor_instances = str(args.executors)

    # Safety net: a typo in RESEARCH_WAREHOUSE must not be able to touch
    # production. Checked before the session even starts.
    assert "research/" in cfg.project.warehouse_path, (
        f"refusing to run: warehouse_path {cfg.project.warehouse_path!r} does "
        f"not contain 'research/' -- this script must never point at a "
        f"production prefix."
    )

    print("=" * 72)
    print(f"  day       : {day}")
    print(f"  warehouse : {cfg.project.warehouse_path}  (reads AND writes)")
    print(f"  arm       : {cfg.segmentation.method}"
          f"  (persistence={cfg.segmentation.callsign_min_persistence_seconds}s)")
    print(f"  executors : {cfg.spark.executor_instances}")
    print("=" * 72)

    spark = SparkSessionManager.create_session(
        app_name="debounced day run (research/a9)", config=cfg, distributed=True,
    )
    try:
        kwargs = {
            "airports_hex_path": str(
                REPO / "data" / "airport_hex" / "zones_res7_processed.parquet"
            ),
        }
        for step in STEPS:
            print(f"\n{'-' * 72}\n>>> step {step}  day {day}\n{'-' * 72}")
            run_day_step(spark, cfg, step, day, kwargs)
        print("\nDone. research/a9 tables written for", day)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
