#!/usr/bin/env python
"""Run one sampled day through the real pipeline for A9 and A10, into research.

    export PYSPARK_DRIVER_PYTHON=$PWD/.venv310/bin/python
    unset PYSPARK_PYTHON    # executors use the opdi-spark image's python
    .venv310/bin/python -u benchmarks/run_track_link_day.py \\
        --day 2026-06-14 --arm debounced --steps 01 02 02a 03 04 04b
    .venv310/bin/python -u benchmarks/run_track_link_day.py \\
        --day 2026-06-14 --arm cruise_linked --steps 02 02a 03 04 04b

Stage 2 of the cruise-linked track study (``docs/superpowers/specs/
2026-11-03-cruise-linked-tracks-design.md``): the arms are ``debounced`` (A9,
the production default) and ``cruise_linked`` (A10). They differ in
``segmentation.method`` and nothing else, which :func:`build_config` makes
structurally true.

**Where it writes.** Worldwide, under ``s3a://eurocontrol/opdi-world/research/
a10/``. Both arms read one copy of ``osn_statevectors_v2`` under ``input/``;
each arm's derived tables sit under ``<arm>/``. Reference tables are read from
``opdi-world`` and never written. :func:`guard_write_path` is an allow-list
with a real raise, independent of the prefix constant it protects.

**The sample.** ``abs(xxhash64(icao24)) % 10 == 0`` keeps whole airframes, and
is applied only when ``osn_statevectors_v2`` is written. Segmentation
partitions by airframe, so a kept airframe is identical to its full-feed self
and the arms' difference is unchanged; only the totals are a tenth. Step 01
reuses inputs when partitions exist, so a second arm that omits step 01 reads
the first arm's.

**The bucket.** ``eurocontrol`` has a 200 GB quota and refuses every write at
it. :func:`check_free_space` runs before Spark starts and stops the run under
3 GB free.

``--link-params-json`` overrides ``link_*`` fields only, so the setting chosen
by the study can be run without editing code. The values used are printed.

Three disjoint table sets, and a table in none of them raises: the same rule
as ``run_adsc_day.py``, for the same reason (a defaulted classification is a
silently wrong one).
"""

import argparse
import dataclasses
import json
import re
import sys
import time
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

from opdi.config import OPDIConfig  # noqa: E402
from opdi.periodrun import run_day_step  # noqa: E402
from opdi.utils.spark_helpers import SparkSessionManager  # noqa: E402
from opdi.utils.storage import StorageManager  # noqa: E402

ARM_PREFIX = "s3a://eurocontrol/opdi-world/research/a10"
REFERENCE_WAREHOUSE = "s3a://eurocontrol/opdi-world"

#: The only place this script may write, independent of ARM_PREFIX: a guard
#: built from the constant could not notice the constant being wrong.
ALLOWED_WRITE_ROOT = re.compile(r"^s3a://eurocontrol/opdi-world/research/a10/")

#: Reference data, read and never written.
PASSTHROUGH = (
    "h3_airport_detection_zones",
    "h3_runway_zones",
    "hexaero_airport_layouts",
    "oa_airports",
    "oa_runways",
    "osn_aircraft_db",
    "opdi_h3_airspace_ref",
    "coverage_marker",
)

#: Written once, read by both arms.
SHARED_INPUTS = (
    "osn_statevectors_v2",
    "osn_adsc_statevectors",
)

#: What each arm produces. Per-arm, because these are the comparison.
DERIVED = (
    "osn_tracks",
    "osn_tracks_clean",
    "opdi_flight_list",
    "opdi_endpoint_candidates",
    "opdi_flight_list_milestones_staging",
    "opdi_flight_events",
    "opdi_measurements",
)

ALL_TABLES = tuple(sorted(PASSTHROUGH + SHARED_INPUTS + DERIVED))

ARMS = ("debounced", "cruise_linked")
STEPS = ("01", "02", "02a", "03", "04", "04b")

BUCKET = "eurocontrol"
ENDPOINT = "https://s3.opensky-network.org"
QUOTA_GB = 200.0
FLOOR_GB = 3.0

_ORIG_S3_PATH = None
_ORIG_WRITE = None
_ORIG_DROP = None


def arm_path(arm: str, table: str) -> str:
    """Absolute S3 path for *table* in *arm*. Shared inputs ignore *arm*."""
    if arm not in ARMS:
        raise ValueError(f"unknown arm {arm!r}; expected one of {ARMS}")
    if table in SHARED_INPUTS:
        return f"{ARM_PREFIX}/input/{table}"
    return f"{ARM_PREFIX}/{arm}/{table}"


def guard_write_path(path: str) -> None:
    """Refuse a write outside the a10 research prefix. Raises, not asserts-away."""
    if not (path.startswith(f"{ARM_PREFIX}/") and ALLOWED_WRITE_ROOT.match(path)):
        raise AssertionError(
            f"refusing to write {path!r}: every table this script writes must "
            f"sit under {ARM_PREFIX!r} (research/a10). A passthrough table is "
            f"read-only."
        )


def log_dir_name(arm: str) -> str:
    """Per arm and prefix: local ETL logs claim tables exist in one prefix only."""
    return f"a10-worldwide-{arm}"


def sample_airframes(df):
    """Whole airframes, one in ten, stable across runs and days."""
    from pyspark.sql import functions as F

    return df.where(F.abs(F.xxhash64("icao24")) % 10 == 0)


def check_free_space(
    used_gb: float, quota_gb: float = QUOTA_GB, floor_gb: float = FLOOR_GB
) -> None:
    """Raise if the bucket has less than *floor_gb* free."""
    free = quota_gb - used_gb
    if free < floor_gb:
        raise RuntimeError(
            f"only {free:.1f} GB free in {BUCKET} ({used_gb:.1f} of "
            f"{quota_gb:.0f} GB used); need {floor_gb:.0f} GB. Delete scratch "
            f"in the spec's order before running."
        )


def bucket_used_gb() -> float:
    """Total size of the bucket, listed with the credentials in ``.env``."""
    import boto3

    env_file = REPO / ".env"
    if not env_file.is_file():
        raise RuntimeError(f"no .env at {env_file}; S3 credentials are required")
    creds = {}
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            creds[key.strip()] = value.strip()
    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=creds["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=creds["AWS_SECRET_ACCESS_KEY"],
    )
    total = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET):
        total += sum(o["Size"] for o in page.get("Contents", []))
    return total / 1e9


def build_config(arm: str, link_params=None, executors: int = 8) -> OPDIConfig:
    """The one config both arms share, plus the one setting that differs."""
    if arm not in ARMS:
        raise ValueError(f"unknown arm {arm!r}; expected one of {ARMS}")
    cfg = OPDIConfig.for_environment("opensky", worldwide=True)
    cfg.project.warehouse_path = REFERENCE_WAREHOUSE
    # The prefix holds the tables directly, without a release folder.
    cfg.project.release = ""
    cfg.adsc.enabled = False
    cfg.spark.executor_instances = str(executors)
    cfg.segmentation.method = arm
    if link_params:
        valid = {f.name for f in dataclasses.fields(cfg.segmentation) if f.name.startswith("link_")}
        bad = sorted(set(link_params) - valid)
        if bad:
            raise ValueError(
                f"only link_* segmentation fields may be overridden; got {bad}. "
                f"Valid: {sorted(valid)}"
            )
        for k, v in link_params.items():
            setattr(cfg.segmentation, k, v)
    return cfg


def redirect_arm(arm: str) -> None:
    """Resolve derived and shared tables to research; pass reference through."""
    global _ORIG_S3_PATH, _ORIG_WRITE, _ORIG_DROP
    if _ORIG_S3_PATH is None:
        _ORIG_S3_PATH = StorageManager._s3_path
    if _ORIG_WRITE is None:
        _ORIG_WRITE = StorageManager.write_table
    if _ORIG_DROP is None:
        _ORIG_DROP = StorageManager.drop_partitions
    orig_path, orig_write, orig_drop = _ORIG_S3_PATH, _ORIG_WRITE, _ORIG_DROP

    def _s3_path(self, table_name):
        if table_name in PASSTHROUGH:
            return orig_path(self, table_name)
        if table_name in SHARED_INPUTS or table_name in DERIVED:
            return arm_path(arm, table_name)
        raise RuntimeError(
            f"unclassified table {table_name!r}: add it to PASSTHROUGH, "
            f"SHARED_INPUTS or DERIVED in benchmarks/run_track_link_day.py."
        )

    def write_table(self, df, table_name, mode="append", partition_by=None, partition_values=None):
        guard_write_path(_s3_path(self, table_name))
        if table_name == "osn_statevectors_v2":
            df = sample_airframes(df)
        return orig_write(self, df, table_name, mode, partition_by, partition_values)

    def drop_partitions(self, table_name, column, values):
        # Deleting is writing. A passthrough table resolves to the reference warehouse,
        # so without this a re-run would delete reference data, not research data.
        guard_write_path(_s3_path(self, table_name))
        return orig_drop(self, table_name, column, values)

    StorageManager._s3_path = _s3_path
    StorageManager.write_table = write_table
    StorageManager.drop_partitions = drop_partitions


def restore() -> None:
    """Undo the patch. Idempotent."""
    global _ORIG_S3_PATH, _ORIG_WRITE, _ORIG_DROP
    if _ORIG_S3_PATH is not None:
        StorageManager._s3_path = _ORIG_S3_PATH
        _ORIG_S3_PATH = None
    if _ORIG_WRITE is not None:
        StorageManager.write_table = _ORIG_WRITE
        _ORIG_WRITE = None
    if _ORIG_DROP is not None:
        StorageManager.drop_partitions = _ORIG_DROP
        _ORIG_DROP = None


def record_timing(path, arm: str, step: str, seconds: float) -> None:
    """Write how long *step* took for *arm* to the CSV at *path*.

    One row per arm and step: a re-run replaces its own row and leaves the
    other arm's alone.
    """
    import csv

    path = Path(path)
    rows = {}
    if path.is_file():
        with path.open() as fh:
            rows = {(x["arm"], x["step"]): x["seconds"] for x in csv.DictReader(fh)}
    rows[(arm, step)] = str(round(seconds))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        out = csv.writer(fh)
        out.writerow(["arm", "step", "seconds"])
        for (a, s), value in sorted(rows.items()):
            out.writerow([a, s, value])


def run_arm(
    arm: str, day: date, executors: int, steps=STEPS, link_params=None, timings_csv=None
) -> None:
    """One arm, one day, through the real per-day entry points."""
    cfg = build_config(arm, link_params, executors)
    seg = cfg.segmentation

    print("=" * 72)
    print(f"  arm        : {arm}  (segmentation.method={seg.method})")
    print(f"  day        : {day}")
    print(f"  reference  : {REFERENCE_WAREHOUSE}  (read-only)")
    print(f"  inputs     : {ARM_PREFIX}/input  (sample: 1 airframe in 10)")
    print(f"  outputs    : {ARM_PREFIX}/{arm}")
    print(f"  coverage   : {cfg.coverage.label}  (bbox={cfg.coverage.bbox})")
    print(f"  steps      : {' '.join(steps)}")
    if arm == "cruise_linked":
        for f in dataclasses.fields(seg):
            if f.name.startswith("link_"):
                print(f"  {f.name:<32}: {getattr(seg, f.name)}")
    print("=" * 72, flush=True)

    # Before Spark: a run that hits the quota mid-write leaves a half table.
    check_free_space(bucket_used_gb())

    redirect_arm(arm)
    spark = SparkSessionManager.create_session(
        app_name=f"a10 {arm} {day}", config=cfg, distributed=True
    )
    try:
        kwargs = {
            "airports_hex_path": str(
                REPO / "data" / "airport_hex" / "zones_res7_processed.parquet"
            ),
            "log_dir": str(REPO / "OPDI_live" / "logs" / log_dir_name(arm)),
        }
        storage = StorageManager(spark, cfg)
        problem = storage.check_coverage()
        if problem:
            raise RuntimeError(problem)
        for step in steps:
            print(
                f"\n{'-' * 72}\n>>> arm {arm}  step {step}  day {day}\n" f"{'-' * 72}", flush=True
            )
            started = time.monotonic()
            run_day_step(spark, cfg, step, day, kwargs)
            if timings_csv:
                record_timing(timings_csv, arm, step, time.monotonic() - started)
    finally:
        spark.stop()
        restore()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--day", required=True, help="YYYY-MM-DD")
    ap.add_argument("--arm", choices=ARMS, required=True)
    ap.add_argument(
        "--steps", nargs="+", default=list(STEPS), choices=STEPS, help="per-day steps, in order"
    )
    ap.add_argument("--executors", type=int, default=8)
    ap.add_argument(
        "--link-params-json",
        default=None,
        help="JSON object of link_* SegmentationConfig overrides",
    )
    ap.add_argument(
        "--timings-csv",
        default=None,
        help="write each step's duration here (one row per arm and step)",
    )
    args = ap.parse_args()
    link_params = json.loads(args.link_params_json) if args.link_params_json else None
    day = date.fromisoformat(args.day)
    run_arm(args.arm, day, args.executors, args.steps, link_params, timings_csv=args.timings_csv)
    print(f"\nDone. {args.arm} written for {day} under {ARM_PREFIX}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
