#!/usr/bin/env python
"""Measure how long each step of one worldwide day takes, and where inside a step the time goes.

    export PYSPARK_DRIVER_PYTHON=$PWD/.venv310/bin/python
    unset PYSPARK_PYTHON
    .venv310/bin/python -u benchmarks/day_timing.py run --day 2026-06-14 \\
        --steps 03 04 04b 09 --executors 8 --label base
    .venv310/bin/python -u benchmarks/day_timing.py run --day 2026-06-14 \\
        --steps 01 02 02a --executors 8 --label slice4 --slice-hours 8 4
    .venv310/bin/python benchmarks/day_timing.py summarise --label base
    .venv310/bin/python benchmarks/day_timing.py clean --label base

**What it measures.** Each step runs through :func:`opdi.periodrun.run_day_step`,
as in the daily run. Its time is the wall-clock time of that call. Inside the
step, Spark writes its event log (a file of what each job, stage and task did)
to a local folder; after the run :func:`stage_rows` turns it into one row per
stage: the step, the line of the pipeline that started the job, the Spark
operators in the stage, its duration, the number of tasks, the bytes read,
shuffled and written, the bytes spilled to disk, and the longest and median
task (a longest task far above the median means a few tasks hold most of the
data, called skew).

**Where it reads and writes.** The cleaned and the uncleaned tracks of the
day and the reference tables are read from the worldwide warehouse
``s3a://eurocontrol/opdi-world`` and never written; the FIR reference from the
airspace events study (``research/airspace_events/ref``). Everything a step
writes goes under ``s3a://eurocontrol/opdi-world/research/speed/<label>/``,
the published files included; :func:`check_write_path` refuses anything else.
``clean`` deletes that folder.

**A part of the day.** Steps 01, 02 and 02a write about 20 GB for a full day,
more than the bucket has free. With ``--slice-hours`` they run on the first
hours of the day only: step 01 fetches those hours from OpenSky, step 02
builds the tracks of those hours and step 02a cleans them. Each slice size is
a separate run. The full-day time is then estimated from two slice sizes with
a straight line through (rows, seconds), see :func:`extrapolate`: a fixed
cost plus a cost per row. The rows of the full day are counted from the
footers of the Parquet files (no data is read).

**Results.** ``step_timings.csv`` and ``stage_timings.csv`` in ``--out``
(default ``/home/jupyter/.claude/jobs/616d5918/tmp/speed/data``); a re-run of
a label replaces that label's rows.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
import sys
import time
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

PREFIX = "s3a://eurocontrol/opdi-world/research/speed"
INPUT_WAREHOUSE = "s3a://eurocontrol/opdi-world"
FIR_REFERENCE = "s3a://eurocontrol/opdi-world/research/airspace_events/ref/opdi_h3_airspace_ref"
ALLOWED_WRITE_ROOT = re.compile(r"^s3a://eurocontrol/opdi-world/research/speed/[A-Za-z0-9_.-]+/")
DEFAULT_OUT = Path("/home/jupyter/.claude/jobs/616d5918/tmp/speed/data")
EVENTLOG_ROOT = Path("/home/jupyter/.claude/jobs/616d5918/tmp/speed/eventlog")

#: Reference tables: read from the worldwide warehouse, never written.
PASSTHROUGH = (
    "h3_airport_detection_zones",
    "h3_runway_zones",
    "hexaero_airport_layouts",
    "oa_airports",
    "oa_runways",
    "osn_aircraft_db",
    "coverage_marker",
)
#: The day's tracks, read from the warehouse when this run does not build them.
TRACK_TABLES = ("osn_tracks", "osn_tracks_clean")

STEPS = ("01", "02", "02a", "03", "04", "04b", "09")
#: Steps that need the slice: they write the large tables.
SLICE_STEPS = ("01", "02", "02a")

#: Table and day column each step writes, to count the rows of its output.
STEP_OUTPUT = {
    "01": ("osn_statevectors_v2", "event_time_day"),
    "02": ("osn_tracks", "dof"),
    "02a": ("osn_tracks_clean", "dof"),
    "03": ("opdi_flight_list", "DOF"),
    "04": ("opdi_flight_events", "dof"),
    "04b": ("opdi_flight_list", "DOF"),
}

STEP_COLUMNS = (
    "label",
    "step",
    "seconds",
    "executors",
    "cores",
    "rows_in",
    "rows_out",
    "scope",
    "scale_factor",
    "full_day_seconds",
    "settings",
)
STAGE_COLUMNS = (
    "label",
    "step",
    "stage_id",
    "attempt",
    "call_site",
    "operators",
    "seconds",
    "tasks",
    "executor_run_s",
    "task_max_s",
    "task_median_s",
    "input_bytes",
    "shuffle_read_bytes",
    "shuffle_write_bytes",
    "output_bytes",
    "spill_memory_bytes",
    "spill_disk_bytes",
    "failure",
)

#: Job group of the jobs that count rows after a step; left out of the stage table.
COUNT_GROUP = "count"


# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------


def table_path(label: str, table: str, builds_tracks: bool) -> str:
    """Absolute path of ``table`` in the run ``label``.

    ``builds_tracks``: whether this run builds the tracks (steps 02 or 02a);
    otherwise they are read from the warehouse.
    """
    if table in TRACK_TABLES and not builds_tracks:
        return f"{INPUT_WAREHOUSE}/{table}"
    if table == "opdi_h3_airspace_ref":
        return FIR_REFERENCE
    return f"{PREFIX}/{label}/{table}"


def check_write_path(path: str) -> None:
    """Refuse a write outside ``research/speed/<label>/``. Raises."""
    if not ALLOWED_WRITE_ROOT.match(path):
        raise AssertionError(f"refusing to write {path!r}: this run writes under {PREFIX}/ only")


def redirect(label: str, builds_tracks: bool) -> None:
    """Point the storage layer and the published folder at this run's prefix."""
    from opdi.output import published
    from opdi.utils.storage import StorageManager

    orig_path = StorageManager._s3_path
    orig_write = StorageManager.write_table
    orig_drop = StorageManager.drop_partitions

    def _s3_path(self, table_name):
        if table_name in PASSTHROUGH:
            return orig_path(self, table_name)
        return table_path(label, table_name, builds_tracks)

    def write_table(self, df, table_name, mode="append", partition_by=None, partition_values=None):
        check_write_path(_s3_path(self, table_name))
        return orig_write(self, df, table_name, mode, partition_by, partition_values)

    def drop_partitions(self, table_name, column, values):
        check_write_path(_s3_path(self, table_name))
        return orig_drop(self, table_name, column, values)

    StorageManager._s3_path = _s3_path
    StorageManager.write_table = write_table
    StorageManager.drop_partitions = drop_partitions

    root = f"{PREFIX}/{label}/published"
    check_write_path(root + "/")
    published.PublishedLayout.for_storage = classmethod(lambda cls, storage: cls(root))


# --------------------------------------------------------------------------
# Call sites: the line of the pipeline that started each Spark job
# --------------------------------------------------------------------------

#: Frames of these files are skipped when naming the call site.
_SKIP_FRAMES = ("/pyspark/", "/py4j/", "day_timing.py")


def python_call_site(stack=None) -> str:
    """``file.py:line function`` of the innermost frame of the pipeline's own code."""
    stack = stack if stack is not None else traceback.extract_stack()
    for frame in reversed(stack):
        if any(s in frame.filename for s in _SKIP_FRAMES):
            continue
        return f"{Path(frame.filename).name}:{frame.lineno} {frame.name}"
    return "unknown"


def install_call_sites(spark) -> None:
    """Name each Spark job after the Python line that started it.

    Spark names a job after the Java method that started it, which from
    Python is always the same bridge method. The actions that start jobs are
    wrapped here so that the Spark property ``callSite.short`` holds the
    pipeline's own line while the action runs.
    """
    from pyspark.sql import DataFrame, DataFrameWriter

    sc = spark.sparkContext

    def wrap(cls, name):
        orig = getattr(cls, name)
        if getattr(orig, "_timed", False):
            return

        def inner(*args, **kwargs):
            site = python_call_site()
            previous = sc.getLocalProperty("callSite.short")
            sc.setLocalProperty("callSite.short", site)
            sc.setLocalProperty("callSite.long", site)
            try:
                return orig(*args, **kwargs)
            finally:
                sc.setLocalProperty("callSite.short", previous)
                sc.setLocalProperty("callSite.long", previous)

        inner._timed = True
        setattr(cls, name, inner)

    for name in ("count", "collect", "first", "take", "head", "toPandas", "localCheckpoint"):
        wrap(DataFrame, name)
    for name in ("parquet", "save"):
        wrap(DataFrameWriter, name)


# --------------------------------------------------------------------------
# Event log -> one row per stage
# --------------------------------------------------------------------------


def _accumulables(stage_info: dict) -> dict:
    return {a.get("Name"): a.get("Value") for a in stage_info.get("Accumulables", [])}


def _num(value) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


#: Spark operators worth naming in the stage table, in the order printed.
OPERATORS = (
    "Scan parquet",
    "Window",
    "Sort",
    "SortMergeJoin",
    "BroadcastHashJoin",
    "BroadcastNestedLoopJoin",
    "HashAggregate",
    "ObjectHashAggregate",
    "SortAggregate",
    "Generate",
    "Expand",
    "Union",
    "InMemoryTableScan",
    "Exchange",
    "AQEShuffleRead",
    "WriteFiles",
    "BatchEvalPython",
    "ArrowEvalPython",
)


def _operators(stage_info: dict) -> str:
    names = set()
    for rdd in stage_info.get("RDD Info", []):
        scope = rdd.get("Scope")
        if not scope:
            continue
        try:
            name = json.loads(scope).get("name", "")
        except ValueError:
            continue
        for op in OPERATORS:
            if name.startswith(op):
                names.add(op)
    return " ".join(op for op in OPERATORS if op in names)


def _short_site(site: str) -> str:
    """A call site with its file paths cut to the file name."""
    return re.sub(r"(?:/[^\s/:]+)+/([^\s/:]+\.py)", r"\1", site)


def stage_rows(lines, label: str) -> list[dict]:
    """One row per completed stage attempt of the event log ``lines`` (JSON text lines).

    The step is the job group (``step-<id>``) the stage ran under; stages of
    the row counts (:data:`COUNT_GROUP`) and stages outside any step are left
    out. The duration is from submission to completion; the task times are
    from the launch to the finish of each successful task.
    """
    groups: dict[int, str] = {}
    sites: dict[int, str] = {}
    tasks: dict[tuple, list] = {}
    done: list[dict] = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except ValueError:
            continue
        kind = event.get("Event")
        if kind == "SparkListenerStageSubmitted":
            props = event.get("Properties") or {}
            sid = event["Stage Info"]["Stage ID"]
            groups[sid] = props.get("spark.jobGroup.id", "")
            sites[sid] = props.get("callSite.short", "")
        elif kind == "SparkListenerTaskEnd":
            info = event.get("Task Info", {})
            if info.get("Failed") or info.get("Killed"):
                continue
            key = (event.get("Stage ID"), event.get("Stage Attempt ID", 0))
            tasks.setdefault(key, []).append(
                (_num(info.get("Finish Time")) - _num(info.get("Launch Time"))) / 1000.0
            )
        elif kind == "SparkListenerStageCompleted":
            done.append(event["Stage Info"])

    rows = []
    for info in done:
        sid = info["Stage ID"]
        group = groups.get(sid, "")
        if not group.startswith("step-"):
            continue
        attempt = info.get("Stage Attempt ID", 0)
        acc = _accumulables(info)
        durations = tasks.get((sid, attempt), [])
        submitted, completed = info.get("Submission Time"), info.get("Completion Time")
        rows.append(
            {
                "label": label,
                "step": group[len("step-") :],
                "stage_id": sid,
                "attempt": attempt,
                "call_site": _short_site(sites.get(sid) or info.get("Stage Name", "")),
                "operators": _operators(info),
                "seconds": (
                    round((_num(completed) - _num(submitted)) / 1000.0, 1)
                    if submitted and completed
                    else ""
                ),
                "tasks": info.get("Number of Tasks", 0),
                "executor_run_s": round(_num(acc.get("internal.metrics.executorRunTime")) / 1000.0),
                "task_max_s": round(max(durations), 1) if durations else "",
                "task_median_s": round(statistics.median(durations), 1) if durations else "",
                "input_bytes": _num(acc.get("internal.metrics.input.bytesRead")),
                "shuffle_read_bytes": _num(acc.get("internal.metrics.shuffle.read.remoteBytesRead"))
                + _num(acc.get("internal.metrics.shuffle.read.localBytesRead")),
                "shuffle_write_bytes": _num(acc.get("internal.metrics.shuffle.write.bytesWritten")),
                "output_bytes": _num(acc.get("internal.metrics.output.bytesWritten")),
                "spill_memory_bytes": _num(acc.get("internal.metrics.memoryBytesSpilled")),
                "spill_disk_bytes": _num(acc.get("internal.metrics.diskBytesSpilled")),
                "failure": (
                    (info.get("Failure Reason") or "").splitlines()[0][:200]
                    if info.get("Failure Reason")
                    else ""
                ),
            }
        )
    return rows


def event_log_lines(folder: Path):
    """The lines of every event log file under ``folder`` (Spark 4 writes a folder per run)."""
    for path in sorted(folder.rglob("events_*")):
        with path.open() as fh:
            yield from fh


# --------------------------------------------------------------------------
# Scaling a slice to the full day
# --------------------------------------------------------------------------


def extrapolate(points, rows_full: float) -> tuple[float, float, float]:
    """Full-day seconds from ``points`` = [(rows, seconds), ...] by a straight line.

    With one point the time is taken as proportional to the rows. With two
    or more, a least-squares line ``seconds = fixed + per_row * rows``; a
    negative fixed cost (noise) is set to zero and the line refitted through
    the origin. Returns ``(seconds_full, fixed, per_row)``.
    """
    pts = [(float(r), float(s)) for r, s in points if r and r > 0]
    if not pts:
        raise ValueError("no slice with rows to scale from")
    if len({r for r, _ in pts}) < 2:
        per_row = sum(s for _, s in pts) / sum(r for r, _ in pts)
        return per_row * rows_full, 0.0, per_row
    n = len(pts)
    mr = sum(r for r, _ in pts) / n
    ms = sum(s for _, s in pts) / n
    per_row = sum((r - mr) * (s - ms) for r, s in pts) / sum((r - mr) ** 2 for r, _ in pts)
    fixed = ms - per_row * mr
    if fixed < 0 or per_row < 0:
        per_row = sum(r * s for r, s in pts) / sum(r * r for r, _ in pts)
        fixed = 0.0
    return fixed + per_row * rows_full, fixed, per_row


# --------------------------------------------------------------------------
# CSV bookkeeping
# --------------------------------------------------------------------------


def replace_rows(path: Path, columns, rows: list[dict], label: str) -> None:
    """Write ``rows`` to the CSV at ``path``, replacing the rows of ``label`` already there."""
    kept = []
    if path.is_file():
        with path.open() as fh:
            kept = [r for r in csv.DictReader(fh) if r.get("label") != label]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(columns), extrasaction="ignore")
        writer.writeheader()
        for r in kept + rows:
            writer.writerow(r)


def append_rows(path: Path, columns, rows: list[dict]) -> None:
    """Add ``rows`` to the CSV at ``path`` (its header written when it is new)."""
    new = not path.is_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(columns), extrasaction="ignore")
        if new:
            writer.writeheader()
        for r in rows:
            writer.writerow(r)


def slowest(rows: list[dict], n: int = 10) -> dict[str, list[dict]]:
    """The ``n`` slowest stages of each step."""
    by_step: dict[str, list[dict]] = {}
    for r in rows:
        by_step.setdefault(r["step"], []).append(r)
    return {
        s: sorted(v, key=lambda r: float(r["seconds"] or 0), reverse=True)[:n]
        for s, v in by_step.items()
    }


# --------------------------------------------------------------------------
# The run
# --------------------------------------------------------------------------


def build_config(executors: int, cores: int | None = None, memory: str | None = None):
    """The worldwide ``opensky`` configuration, as in production, with ``executors`` executors."""
    from opdi.config import OPDIConfig

    cfg = OPDIConfig.for_environment("opensky", worldwide=True)
    cfg.project.warehouse_path = INPUT_WAREHOUSE
    cfg.project.release = ""
    cfg.adsc.enabled = False
    cfg.spark.executor_instances = str(executors)
    if cores:
        cfg.spark.executor_cores = str(cores)
        cfg.spark.k8s_executor_cores_limit = str(cores)
    if memory:
        cfg.spark.executor_memory = memory
    return cfg


def count_day_rows(spark, label: str, step: str, day: date, builds_tracks: bool):
    """Rows the step wrote for the day, from the Parquet footers."""
    from pyspark.sql import functions as F

    if step not in STEP_OUTPUT:
        return ""
    table, column = STEP_OUTPUT[step]
    path = table_path(label, table, builds_tracks)
    try:
        df = spark.read.parquet(path)
        if step == "01":
            return df.count()
        return df.filter(F.col(column) == F.lit(day)).count()
    except Exception as exc:  # noqa: BLE001 -- a count is informative only
        print(f"  could not count {table}: {exc}")
        return ""


def hour_rows(spark, start: datetime, hours: int) -> int:
    """Rows of OpenSky's hourly folders from ``start`` for ``hours`` hours (footers only)."""
    from opdi.ingestion.osn_statevectors import hour_paths

    return spark.read.parquet(*hour_paths(start, start + timedelta(hours=hours))).count()


def read_window_hours(cfg) -> int:
    """Hours of reports step 02 reads for one day: the lookback, the day and the lookahead."""
    from opdi.pipeline.segmentation import SegmentationParams
    from opdi.pipeline.tracks import TrackProcessor, required_lookback_hours

    params = SegmentationParams.from_config(cfg.segmentation)
    lookback = required_lookback_hours(cfg.segmentation.method, params)
    return lookback + 24 + TrackProcessor.DEFAULT_LOOKAHEAD_HOURS


def run(args) -> int:
    from opdi.periodrun import run_day_step
    from opdi.utils.spark_helpers import SparkSessionManager

    sys.path.insert(0, str(REPO / "benchmarks"))
    from run_airspace_events_day import S3_PATIENCE
    from run_track_link_day import QUOTA_GB, bucket_used_gb, check_free_space

    day = date.fromisoformat(args.day)
    label = args.label
    check_write_path(f"{PREFIX}/{label}/")
    builds_tracks = any(s in ("02", "02a") for s in args.steps)
    if args.slice_hours and any(s not in SLICE_STEPS for s in args.steps):
        raise SystemExit("--slice-hours runs steps 01, 02 and 02a only")
    if any(s in SLICE_STEPS for s in args.steps) and not args.slice_hours:
        raise SystemExit("steps 01, 02 and 02a need --slice-hours: a full day does not fit")

    used = bucket_used_gb()
    print(f"bucket: {used:.1f} of {QUOTA_GB:.0f} GB used", flush=True)
    check_free_space(used, QUOTA_GB, args.needed_gb)

    cfg = build_config(args.executors, args.cores, args.memory)
    extra = dict(S3_PATIENCE)
    for item in args.conf or []:
        key, _, value = item.partition("=")
        extra[key] = value
    eventlog = EVENTLOG_ROOT / label
    if eventlog.exists():
        for p in sorted(eventlog.rglob("*"), reverse=True):
            p.unlink() if p.is_file() else p.rmdir()
    eventlog.mkdir(parents=True, exist_ok=True)
    extra.update(
        {
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": f"file://{eventlog}",
            "spark.eventLog.compress": "false",
        }
    )
    redirect(label, builds_tracks)
    spark = SparkSessionManager.create_session(
        app_name=f"day timing {label} {day}",
        config=cfg,
        distributed=True,
        extra_configs=extra,
    )
    install_call_sites(spark)
    sc = spark.sparkContext
    settings = " ".join(f"{k}={v}" for k, v in sorted(extra.items()) if "eventLog" not in k)
    cores = int(cfg.spark.executor_cores)
    step_rows = []
    status = 0
    try:
        log_dir = str(REPO / "OPDI_live" / "logs" / f"speed-{label}")
        for step in args.steps:
            print(f"\n{'-' * 72}\n>>> {label}  step {step}  day {day}\n{'-' * 72}", flush=True)
            sc.setJobGroup(f"step-{step}", f"step {step}")
            started = time.monotonic()
            if step == "01":
                from opdi.ingestion.osn_statevectors import StateVectorIngestion

                start = datetime(day.year, day.month, day.day)
                StateVectorIngestion(spark, cfg).ingest_from_s3(
                    start_date=start, end_date=start + timedelta(hours=args.slice_hours)
                )
            else:
                run_day_step(spark, cfg, step, day, {"log_dir": log_dir})
            seconds = time.monotonic() - started
            sc.setJobGroup(COUNT_GROUP, "row counts")
            rows_out = count_day_rows(spark, label, step, day, builds_tracks)
            # What the step read: the archive's hours (01), or what the step before wrote.
            if step == "01":
                rows_in = hour_rows(spark, datetime(day.year, day.month, day.day), args.slice_hours)
            else:
                rows_in = step_rows[-1]["rows_out"] if step_rows else ""
            scope = f"first {args.slice_hours} h of the day" if args.slice_hours else "full"
            row = {
                "label": label,
                "step": step,
                "seconds": round(seconds),
                "executors": args.executors,
                "cores": cores,
                "rows_in": rows_in,
                "rows_out": rows_out,
                "scope": scope,
                "scale_factor": "",
                "full_day_seconds": round(seconds) if not args.slice_hours else "",
                "settings": settings,
            }
            step_rows.append(row)
            print(f"  step {step}: {seconds:.0f} s, {rows_out} rows written", flush=True)
            replace_rows(args.out / "step_timings.csv", STEP_COLUMNS, step_rows, label)
    except Exception:
        traceback.print_exc()
        status = 1
    finally:
        spark.stop()
    stages = stage_rows(event_log_lines(eventlog), label)
    replace_rows(args.out / "stage_timings.csv", STAGE_COLUMNS, stages, label)
    print(f"\n{len(stages)} stages recorded for {label}")
    print_summary(stages)
    return status


def print_summary(stages: list[dict], n: int = 10) -> None:
    for step, top in slowest(stages, n).items():
        total = sum(float(r["seconds"] or 0) for r in stages if r["step"] == step)
        print(f"\nstep {step}: {total:.0f} s in stages; slowest {len(top)}:")
        for r in top:
            print(
                f"  {float(r['seconds'] or 0):7.0f} s  stage {r['stage_id']:>5}  "
                f"{int(r['tasks']):>5} tasks  max {r['task_max_s']} s med {r['task_median_s']} s  "
                f"in {int(r['input_bytes']) / 1e9:.2f} GB  "
                f"shuf {int(r['shuffle_read_bytes']) / 1e9:.2f}/"
                f"{int(r['shuffle_write_bytes']) / 1e9:.2f} GB  "
                f"spill {int(r['spill_disk_bytes']) / 1e9:.2f} GB  "
                f"{r['call_site']}  [{r['operators']}]"
            )


def summarise(args) -> int:
    path = args.out / "stage_timings.csv"
    with path.open() as fh:
        rows = [r for r in csv.DictReader(fh) if r["label"] == args.label]
    print_summary(rows, args.n)
    return 0


def full_day_rows(spark, cfg, day: date, thinning: float) -> dict[str, float]:
    """Rows each of steps 01, 02 and 02a reads on the full day.

    Step 01: the archive's 24 hours of the day. Step 02: the archive's hours
    of its read window times ``thinning`` (rows kept by step 01 per row read,
    measured on the slices). Step 02a: the tracks of the day in the
    warehouse. Counted from the Parquet footers.
    """
    from pyspark.sql import functions as F

    start = datetime(day.year, day.month, day.day)
    window = read_window_hours(cfg)
    lookback = window - 24 - 24
    raw_window = hour_rows(spark, start - timedelta(hours=lookback), window)
    tracks = (
        spark.read.parquet(f"{INPUT_WAREHOUSE}/osn_tracks")
        .filter(F.col("dof") == F.lit(day))
        .count()
    )
    return {
        "01": float(hour_rows(spark, start, 24)),
        "02": raw_window * thinning,
        "02a": float(tracks),
    }


def scaled_rows(slices: list[dict], full_rows: dict[str, float], label: str) -> list[dict]:
    """Full-day rows for steps 01, 02 and 02a from the slice rows of ``step_timings.csv``.

    For each step, a line through the (rows read, seconds) of the slices
    (:func:`extrapolate`) is evaluated at the rows of the full day.
    """
    out = []
    for step in SLICE_STEPS:
        mine = [r for r in slices if r["step"] == step and r["rows_in"] not in ("", None)]
        if not mine:
            continue
        points = [(float(r["rows_in"]), float(r["seconds"])) for r in mine]
        seconds, fixed, per_row = extrapolate(points, full_rows[step])
        biggest = max(points)[0]
        out.append(
            {
                "label": label,
                "step": step,
                "seconds": "",
                "executors": mine[0]["executors"],
                "cores": mine[0]["cores"],
                "rows_in": round(full_rows[step]),
                "rows_out": "",
                "scope": (
                    "scaled from "
                    + ", ".join(f"{r['label']} ({r['scope']}, {r['seconds']} s)" for r in mine)
                    + f"; fixed {fixed:.0f} s + {per_row * 1e6:.3f} s per million rows"
                ),
                "scale_factor": round(full_rows[step] / biggest, 2),
                "full_day_seconds": round(seconds),
                "settings": mine[0]["settings"],
            }
        )
    return out


def scale(args) -> int:
    from opdi.utils.spark_helpers import SparkSessionManager

    with (args.out / "step_timings.csv").open() as fh:
        slices = [r for r in csv.DictReader(fh) if r["label"] in args.slices]
    s01 = [r for r in slices if r["step"] == "01" and r["rows_in"] and r["rows_out"]]
    thinning = sum(float(r["rows_out"]) for r in s01) / sum(float(r["rows_in"]) for r in s01)
    cfg = build_config(1)
    spark = SparkSessionManager.create_session(
        app_name="day timing rows", config=cfg, distributed=False
    )
    try:
        full = full_day_rows(spark, cfg, date.fromisoformat(args.day), thinning)
    finally:
        spark.stop()
    print(f"full-day rows read: {full} (step 01 keeps {thinning:.3f} of the rows it reads)")
    rows = scaled_rows(slices, full, args.label)
    replace_rows(args.out / "step_timings.csv", STEP_COLUMNS, rows, args.label)
    for r in rows:
        print(f"  step {r['step']}: {r['full_day_seconds']} s  ({r['scope']})")
    return 0


def s3_client():
    """A boto3 client with the credentials in ``.env`` (read, not sourced)."""
    import boto3

    creds = {}
    for line in (REPO / ".env").read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            creds[key.strip()] = value.strip().strip('"')
    return boto3.client(
        "s3",
        endpoint_url="https://s3.opensky-network.org",
        aws_access_key_id=creds["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=creds["AWS_SECRET_ACCESS_KEY"],
    )


def scratch_key(label: str) -> str:
    """The bucket key prefix of the run ``label``: ``opdi-world/research/speed/<label>/``."""
    prefix = f"{PREFIX}/{label}/"
    check_write_path(prefix)
    return prefix[len("s3a://eurocontrol/") :]


def clean(args) -> int:
    """Delete ``research/speed/<label>/`` and nothing else."""
    s3 = s3_client()
    key = scratch_key(args.label)
    n = size = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket="eurocontrol", Prefix=key):
        objs = page.get("Contents", [])
        if not objs:
            continue
        n += len(objs)
        size += sum(o["Size"] for o in objs)
        s3.delete_objects(
            Bucket="eurocontrol", Delete={"Objects": [{"Key": o["Key"]} for o in objs]}
        )
    print(f"deleted {n} objects, {size / 1e9:.2f} GB under {key}")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="command", required=True)
    r = sub.add_parser("run")
    r.add_argument("--day", required=True)
    r.add_argument("--steps", nargs="+", required=True, choices=STEPS)
    r.add_argument("--executors", type=int, default=8)
    r.add_argument("--cores", type=int, default=None)
    r.add_argument("--memory", default=None)
    r.add_argument("--label", required=True)
    r.add_argument("--slice-hours", type=int, default=None)
    r.add_argument("--needed-gb", type=float, default=3.0)
    r.add_argument("--conf", action="append", help="extra Spark setting key=value")
    r.add_argument("--out", type=Path, default=DEFAULT_OUT)
    s = sub.add_parser("summarise")
    s.add_argument("--label", required=True)
    s.add_argument("-n", type=int, default=10)
    s.add_argument("--out", type=Path, default=DEFAULT_OUT)
    c = sub.add_parser("clean")
    c.add_argument("--label", required=True)
    k = sub.add_parser("scale")
    k.add_argument("--day", required=True)
    k.add_argument("--slices", nargs="+", required=True, help="labels of the slice runs")
    k.add_argument("--label", required=True, help="label of the scaled rows")
    k.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args(argv)
    if args.command == "run":
        return run(args)
    if args.command == "clean":
        return clean(args)
    if args.command == "scale":
        return scale(args)
    return summarise(args)


if __name__ == "__main__":
    raise SystemExit(main())
