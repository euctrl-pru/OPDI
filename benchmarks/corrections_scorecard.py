#!/usr/bin/env python
"""The scorecard that judges the corrections decided after the spot checks of 2026-06-14.

    .venv310/bin/python -u benchmarks/corrections_scorecard.py subset
    .venv310/bin/python -u benchmarks/corrections_scorecard.py run --scope sample --label base
    .venv310/bin/python -u benchmarks/corrections_scorecard.py score base
    .venv310/bin/python -u benchmarks/corrections_scorecard.py compare base mine
    .venv310/bin/python benchmarks/corrections_scorecard.py agree base base_day

**What it is for.** Six work packages change the detectors of step 04 (and
the flight list of steps 03 and 04b). For each change we need to know, on
real data, whether the published items got better, which odd cases became
rarer, that no event confirmed by an airport record was lost, and that the
events step did not become slower. The guide for the people making the
changes is ``benchmarks/CORRECTIONS.md``.

**subset** writes, once, the verification sample of 2026-06-14 under
``s3a://eurocontrol/opdi-world/research/spot/dsub/``: the cleaned tracks, the
flight list of step 03 and the endpoint candidates of (a) every track of the
day matched to an airport record or a Network Manager flight (same aircraft
address and call sign, within 6 h of the track), (b) every track in the
readers' example files, so the odd cases they found stay reproducible, and
(c) 3% of the remaining tracks, drawn by a hash of the track id (always the
same tracks). The processed day keeps only the flight list after step 04b,
whose airports step 04b has already overruled; step 04 run on that list
would give other events than the day's own run. So ``subset`` first runs
step 03 again on the day's cleaned tracks, into ``research/spot/d0614_03/``,
and cuts the sample from that.

**run** runs steps 04 and 04b (and 03 when asked) on the sample or on the
whole day with the code of a worktree (``--code``, by default the worktree
this file is in), into ``research/spot/runs/<label>/``. ``--set
group.field=value`` changes one configuration field, so a correction can be
switched off and on. The time and the executor-seconds of each step are
kept in ``run.json`` beside the scorecard.

**score** computes the scorecard of one run: one CSV
(``scorecard_<label>.csv``, columns section, item, measure, value, count,
finding) and the same as a markdown file. Section i compares the events
with the airport records, ii the flight list with Network Manager, iii counts
every odd case the readers found (each counter is defined next to its code,
with the finding it comes from), iv counts the events and the filled
columns, v gives the time of each step. ``count`` is the number of cases a
value is computed over: for a counter of odd cases, ``value`` is the number
of odd cases and ``count`` the number of items it is counted among.

**compare** sets two scorecards side by side, lists the events the airport
records confirm (within 60 s) in the first run that are lost or moved in the
second, compares the content of every event type (as
``benchmarks/day_compare.py`` does), and ends with exit code 1 when a
confirmed event is lost (``--allow N`` agreed exceptions) or when the events
step is more than 5% slower.

**The lock.** The pod has 16 GB of memory for everything, and a process over
the limit is killed without a message. Every command here that starts Spark
takes the lock ``/home/jupyter/.claude/jobs/616d5918/tmp/spark.lock`` first
(it waits up to 6 h, and says so while waiting), and releases it when done.
"""

from __future__ import annotations

import argparse
import csv
import fcntl
import json
import math
import os
import re
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BENCH = REPO / "benchmarks"
if str(BENCH) not in sys.path:
    sys.path.insert(0, str(BENCH))

DAY = "2026-06-14"
MONTH = "202606"
WORLD = "s3a://eurocontrol/opdi-world"
PREFIX = f"{WORLD}/research/spot"
#: The processed day (spot checks): cleaned tracks, step-04b flight list, events.
DAY_RUN = "d0614"
#: Step 03 made again on the day's cleaned tracks (see the module text).
DAY_STEP03 = "d0614_03"
#: The verification sample.
SAMPLE = "dsub"
RUNS = "runs"
#: Where this script writes on the bucket, and nowhere else.
WRITE_ROOTS = (
    re.compile(rf"^{re.escape(PREFIX)}/{RUNS}/[A-Za-z0-9_.-]+/"),
    re.compile(rf"^{re.escape(PREFIX)}/{SAMPLE}/"),
    re.compile(rf"^{re.escape(PREFIX)}/{DAY_STEP03}/"),
)

JOB = Path("/home/jupyter/.claude/jobs/616d5918/tmp")
LOCK = JOB / "spark.lock"
LOCK_WAIT_S = 21600
OUT = JOB / "score"
SAMPLES = JOB / "spot" / "samples"

#: Spark settings of every run: a task that fails on an executor is retried
#: on another one (the events step failed twice when all attempts of one task
#: went to the same dying executor).
RUN_CONF = (
    "spark.eventLog.logStageExecutorMetrics=true",
    "spark.excludeOnFailure.enabled=true",
    "spark.excludeOnFailure.task.maxTaskAttemptsPerExecutor=1",
)
EXECUTORS = 13

#: Share of the tracks not matched to a record or an example that the sample keeps.
RANDOM_SHARE = 0.03
#: Fixed text mixed into the hash that draws the random tracks.
SEED = "dsub-20260614"
#: A record is matched to a track within this many seconds of the track.
RECORD_SEARCH_S = 6 * 3600

FT_PER_M = 3.28084
KT_PER_MPS = 1.94384
FTMIN_PER_MPS = 196.850394
EARTH_RADIUS_NM = 3440.065

STEP_TABLES = {"03": "opdi_flight_list", "04": "opdi_flight_events", "04b": "opdi_flight_list"}


def check_write_path(path: str) -> None:
    """Refuse a write outside ``runs/<label>/``, ``dsub/`` and ``d0614_03/``. Raises."""
    if not any(r.match(path) for r in WRITE_ROOTS):
        raise AssertionError(f"refusing to write {path!r}: not a folder of this scorecard")


def run_root(label: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", label):
        raise SystemExit(f"a label is letters, digits, '_', '.' and '-' only, not {label!r}")
    return f"{PREFIX}/{RUNS}/{label}"


def local_dir(label: str) -> Path:
    return OUT / RUNS / label


# --------------------------------------------------------------------------
# The lock
# --------------------------------------------------------------------------


def _lock_holders(path: Path) -> list[int]:
    """Process ids holding a ``flock`` on ``path`` (from ``/proc/locks``)."""
    try:
        inode = os.stat(path).st_ino
        lines = Path("/proc/locks").read_text().splitlines()
    except OSError:
        return []
    out = []
    for line in lines:
        parts = line.split()
        # "1: FLOCK  ADVISORY  WRITE 2164 00:3a0:30020458 0 EOF"
        if len(parts) >= 6 and parts[1] == "FLOCK":
            try:
                if int(parts[5].split(":")[-1]) == inode:
                    out.append(int(parts[4]))
            except ValueError:
                continue
    return out


def _ancestors() -> set[int]:
    pids, pid = set(), os.getpid()
    while pid > 1:
        pids.add(pid)
        try:
            fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
            pid = int(fields[1])
        except (OSError, IndexError, ValueError):
            break
    return pids


@contextmanager
def spark_lock(what: str, path: Path = LOCK, wait_s: float = LOCK_WAIT_S):
    """Hold the machine's Spark lock while the block runs.

    Waits up to ``wait_s`` seconds, printing a line when it has to wait.
    When the lock is already held by this process or one of its parents
    (a command started under ``flock``), the block runs without taking it
    again: taking it twice from one process tree would wait for ever.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(path, "a")
    try:
        holders = _lock_holders(path)
        if holders and set(holders) & _ancestors():
            print(f"lock {path}: already held by this command ({what})", flush=True)
            yield
            return
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(
                f"waiting for the Spark lock {path} (held by process "
                f"{', '.join(map(str, _lock_holders(path))) or '?'}); {what} starts when it is"
                " free",
                flush=True,
            )
            started = time.monotonic()
            while True:
                time.sleep(5)
                try:
                    fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() - started > wait_s:
                        raise SystemExit(f"gave up waiting for {path} after {wait_s:.0f} s")
            print(f"got the Spark lock after {time.monotonic() - started:.0f} s", flush=True)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)
    finally:
        fh.close()


# --------------------------------------------------------------------------
# Small helpers (no Spark)
# --------------------------------------------------------------------------


def git_state(code: Path) -> dict:
    """Commit, branch and whether the working tree has changes, of the worktree ``code``."""

    def git(*a):
        r = subprocess.run(["git", "-C", str(code), *a], capture_output=True, text=True)
        return r.stdout.strip()

    return {
        "code": str(code),
        "commit": git("rev-parse", "HEAD"),
        "branch": git("rev-parse", "--abbrev-ref", "HEAD"),
        "dirty": bool(git("status", "--porcelain", "--untracked-files=no", "--", "src")),
    }


def check_env_file(repo: Path) -> None:
    """Stop with the command to run when the worktree has no ``.env`` (S3 credentials)."""
    if not (repo / ".env").exists():
        raise SystemExit(
            f"{repo} has no .env (the S3 credentials). Link the main checkout's:\n"
            f"  ln -s /home/jupyter/work/opdi-workspace/opdi/.env {repo}/.env"
        )


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


def _key(path: str) -> str:
    return path[len("s3a://eurocontrol/") :].rstrip("/") + "/"


def s3_size(path: str) -> tuple[int, int]:
    """Objects and bytes under ``path``."""
    s3 = s3_client()
    n = size = 0
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket="eurocontrol", Prefix=_key(path)
    ):
        for o in page.get("Contents", []):
            n += 1
            size += o["Size"]
    return n, size


def s3_delete(path: str) -> float:
    """Delete everything under ``path`` (one of this script's folders); returns GB."""
    check_write_path(path.rstrip("/") + "/")
    s3 = s3_client()
    key = _key(path)
    keys, size = [], 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket="eurocontrol", Prefix=key):
        for o in page.get("Contents", []):
            keys.append(o["Key"])
            size += o["Size"]
    for k in keys:
        if not k.startswith(key):
            raise RuntimeError(f"refusing to delete {k!r}: outside {key!r}")
        s3.delete_object(Bucket="eurocontrol", Key=k)
    return size / 1e9


def example_track_ids(samples: Path = SAMPLES) -> list[str]:
    """Every track id in the readers' example files (``samples/*/*.parquet``)."""
    import pandas as pd

    ids: set[str] = set()
    for f in sorted(samples.glob("*/*.parquet")):
        cols = pd.read_parquet(f, columns=None).columns
        if "flight_id" in cols:
            ids |= set(pd.read_parquet(f, columns=["flight_id"])["flight_id"].dropna().astype(str))
    return sorted(ids)


def spark_session(name: str, executors: int = EXECUTORS, eventlog: Path | None = None):
    """A session on the cluster with the ``opensky`` settings and the run settings.

    With ``eventlog``, Spark writes its event log there (to time the parts).
    """
    from opdi.config import OPDIConfig
    from opdi.utils.spark_helpers import SparkSessionManager

    cfg = OPDIConfig.for_environment("opensky", worldwide=True)
    cfg.project.warehouse_path = WORLD
    cfg.spark.executor_instances = str(executors)
    extra = {k: v for k, v in (c.split("=", 1) for c in RUN_CONF)}
    extra["spark.sql.session.timeZone"] = "UTC"
    if eventlog is not None:
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
    return SparkSessionManager.create_session(
        app_name=name, config=cfg, distributed=True, extra_configs=extra
    )


# --------------------------------------------------------------------------
# subset
# --------------------------------------------------------------------------


def _point_day_timing(out: Path):
    """``day_timing`` writing under :data:`PREFIX` with this script's write check."""
    import day_timing

    day_timing.PREFIX = PREFIX
    # ``day_timing`` checks the folders it reads from with the same pattern,
    # so the processed day is named too; it writes only under the label it
    # is given, which here is always one of :data:`WRITE_ROOTS`.
    day_timing.ALLOWED_WRITE_ROOT = re.compile(
        "|".join([r.pattern for r in WRITE_ROOTS] + [rf"^{re.escape(PREFIX)}/{DAY_RUN}/"])
    )
    day_timing.EVENTLOG_ROOT = OUT / "eventlog"
    day_timing.DEFAULT_OUT = out
    return day_timing


def _day_timing_args(label: str, steps, reads: list[str], sets, out: Path) -> list[str]:
    args = ["run", "--day", DAY, "--label", label, "--steps", *steps]
    args += ["--executors", str(EXECUTORS), "--s3", "production", "--out", str(out)]
    for item in RUN_CONF:
        args += ["--conf", item]
    if reads:
        args += ["--read-tables", *reads]
    for item in sets or ():
        args += ["--set", item]
    return args


def subset(args) -> int:
    """Step 03 of the day again (once), then the verification sample (see the module text)."""
    check_env_file(REPO)
    OUT.mkdir(parents=True, exist_ok=True)
    with spark_lock("subset"):
        n, _ = s3_size(f"{PREFIX}/{DAY_STEP03}/opdi_flight_list")
        if n == 0 or args.redo_step03:
            print(
                f"step 03 of {DAY} on the cleaned tracks of {DAY_RUN}, into {DAY_STEP03}",
                flush=True,
            )
            # In a process of its own: ``day_timing`` wraps Spark's actions for
            # its own session, and the selection below starts another one.
            rc = subprocess.call([sys.executable, "-u", __file__, "_step03"], env=os.environ.copy())
            if rc:
                return rc
        else:
            print(f"step 03 of the day already in {DAY_STEP03}: kept", flush=True)
        return _write_subset(args)


def _step03(args) -> int:
    """Step 03 of the day on the processed day's cleaned tracks, into :data:`DAY_STEP03`."""
    dt = _point_day_timing(OUT / "subset")
    return dt.main(
        _day_timing_args(DAY_STEP03, ["03"], [f"osn_tracks_clean@{DAY_RUN}"], [], OUT / "subset")
    )


def _write_subset(args) -> int:
    import events_gt
    from pyspark.sql import functions as F

    spark = spark_session("corrections scorecard subset")
    try:
        events_gt.assert_utc_session(spark)
        day = F.lit(DAY).cast("date")
        tracks = spark.read.parquet(f"{PREFIX}/{DAY_RUN}/osn_tracks_clean").filter(
            F.col("dof") == day
        )
        fl = spark.read.parquet(f"{PREFIX}/{DAY_STEP03}/opdi_flight_list").filter(
            F.col("DOF") == day
        )
        cand = spark.read.parquet(f"{PREFIX}/{DAY_STEP03}/opdi_endpoint_candidates")
        ids = tracks.select("track_id").distinct()

        flk = fl.select(
            F.col("ID").alias("track_id"),
            F.lower(F.col("ICAO24")).alias("_a"),
            F.trim(F.col("FLT_ID")).alias("_c"),
            (F.col("FIRST_SEEN").cast("double") - RECORD_SEARCH_S).alias("_t0"),
            (F.col("LAST_SEEN").cast("double") + RECORD_SEARCH_S).alias("_t1"),
        )
        apdf = events_gt.bridge(
            events_gt.load_apdf(spark, MONTH, full=False), events_gt.load_flights(spark, MONTH)
        ).filter(F.col("icao24").isNotNull())
        rec = apdf.select(
            F.col("icao24").alias("_a"),
            F.col("callsign").alias("_c"),
            F.coalesce("MVT_TIME_UTC", "BLOCK_TIME_UTC").cast("double").alias("_t"),
        )
        by_record = (
            flk.join(rec, ["_a", "_c"])
            .filter(F.col("_t").between(F.col("_t0"), F.col("_t1")))
            .select("track_id")
            .distinct()
            .withColumn("_apdf", F.lit(True))
        )
        nm = events_gt.load_flights(spark, MONTH).select(
            F.lower(F.col("AIRCRAFT_ADDRESS")).alias("_a"),
            F.trim(F.col("AIRCRAFT_ID")).alias("_c"),
            F.col("AOBT_3").cast("double").alias("_n0"),
            F.coalesce("ARVT_3", "AOBT_3").cast("double").alias("_n1"),
        )
        by_nm = (
            flk.join(nm, ["_a", "_c"])
            .filter((F.col("_n1") >= F.col("_t0")) & (F.col("_n0") <= F.col("_t1")))
            .select("track_id")
            .distinct()
            .withColumn("_nm", F.lit(True))
        )
        examples = spark.createDataFrame([(i,) for i in example_track_ids()], "track_id string")
        examples = examples.withColumn("_ex", F.lit(True))
        draw = F.pmod(F.xxhash64(F.lit(SEED), F.col("track_id")), F.lit(10000)) < int(
            RANDOM_SHARE * 10000
        )
        chosen = (
            ids.join(by_record, "track_id", "left")
            .join(by_nm, "track_id", "left")
            .join(examples, "track_id", "left")
            .withColumn(
                "reason",
                F.when(F.col("_apdf") | F.col("_nm"), F.lit("record"))
                .when(F.col("_ex"), F.lit("example"))
                .when(draw, F.lit("random")),
            )
            .filter(F.col("reason").isNotNull())
            .select(
                "track_id",
                "reason",
                F.coalesce("_apdf", F.lit(False)).alias("airport_record"),
                F.coalesce("_nm", F.lit(False)).alias("nm_record"),
                F.coalesce("_ex", F.lit(False)).alias("example"),
            )
        )
        root = f"{PREFIX}/{SAMPLE}"
        check_write_path(root + "/")
        chosen.coalesce(1).write.mode("overwrite").parquet(f"{root}/_tracks")
        chosen = spark.read.parquet(f"{root}/_tracks").cache()
        reasons = {
            r["reason"]: r["n"]
            for r in chosen.groupBy("reason").agg(F.count("*").alias("n")).collect()
        }
        n_all = ids.count()
        print(f"tracks of the day {n_all:,}; kept {sum(reasons.values()):,}: {reasons}", flush=True)
        keep = F.broadcast(chosen.select("track_id"))
        tracks.join(keep, "track_id", "left_semi").repartition(32, "track_id").write.mode(
            "overwrite"
        ).partitionBy("dof").parquet(f"{root}/osn_tracks_clean")
        fl.join(keep.withColumnRenamed("track_id", "ID"), "ID", "left_semi").repartition(
            1
        ).write.mode("overwrite").partitionBy("DOF").parquet(f"{root}/opdi_flight_list")
        cand.join(keep, "track_id", "left_semi").repartition(8).write.mode("overwrite").parquet(
            f"{root}/opdi_endpoint_candidates"
        )
        counts = {
            "tracks_of_day": n_all,
            "tracks_kept": sum(reasons.values()),
            "by_reason": reasons,
        }
        for t, part in (
            ("osn_tracks_clean", "dof"),
            ("opdi_flight_list", "DOF"),
            ("opdi_endpoint_candidates", None),
        ):
            df = spark.read.parquet(f"{root}/{t}")
            full = {
                "osn_tracks_clean": tracks,
                "opdi_flight_list": fl,
                "opdi_endpoint_candidates": cand,
            }[t]
            n_obj, size = s3_size(f"{root}/{t}")
            n_day, size_day = s3_size(
                f"{PREFIX}/{DAY_RUN if t == 'osn_tracks_clean' else DAY_STEP03}/{t}"
            )
            counts[t] = {
                "rows": df.count(),
                "rows_day": full.count(),
                "bytes": size,
                "bytes_day": size_day,
                "files": n_obj,
            }
            print(f"  {t}: {counts[t]}", flush=True)
        counts["written"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        (OUT / "subset.json").write_text(json.dumps(counts, indent=2))
        print(json.dumps(counts, indent=2))
    finally:
        spark.stop()
    return 0


# --------------------------------------------------------------------------
# run
# --------------------------------------------------------------------------


def scope_sources(scope: str, steps) -> dict:
    """Where a run reads its inputs: table -> folder under :data:`PREFIX`.

    ``opdi_flight_list`` is copied into the run's own folder (step 04b
    replaces it there); the others are read in place. With step 03 in the
    run, the flight list and the endpoint candidates are its own.
    """
    if scope == "sample":
        src = {
            "osn_tracks_clean": SAMPLE,
            "opdi_endpoint_candidates": SAMPLE,
            "opdi_flight_list": SAMPLE,
        }
    elif scope == "day":
        src = {
            "osn_tracks_clean": DAY_RUN,
            "opdi_endpoint_candidates": DAY_STEP03,
            "opdi_flight_list": DAY_STEP03,
        }
    else:
        raise SystemExit(f"--scope is sample or day, not {scope!r}")
    if "03" in steps:
        src = {"osn_tracks_clean": src["osn_tracks_clean"]}
    return src


def _copy_flight_list_on_start(spark_manager, source: str, target: str) -> None:
    """Make the session factory copy the step-03 flight list into the run before any step."""
    orig = spark_manager.create_session

    def create_session(*a, **k):
        from pyspark.sql import functions as F

        spark = orig(*a, **k)
        t0 = time.monotonic()
        check_write_path(target + "/")
        (
            spark.read.parquet(source)
            .filter(F.col("DOF") == F.lit(DAY).cast("date"))
            .repartition(1)
            .write.mode("overwrite")
            .partitionBy("DOF")
            .parquet(target)
        )
        print(
            f"  flight list of step 03 copied into the run: {time.monotonic() - t0:.0f} s",
            flush=True,
        )
        return spark

    spark_manager.create_session = create_session


def step_rows(out: Path, label: str) -> dict:
    """Seconds and executor-seconds of each step of the run, from ``day_timing``'s files."""
    steps: dict = {}
    p = out / "step_timings.csv"
    if p.exists():
        with p.open() as fh:
            for r in csv.DictReader(fh):
                if r["label"] == label:
                    steps[r["step"]] = {
                        "seconds": float(r["seconds"] or 0),
                        "rows_out": r["rows_out"],
                    }
    p = out / "stage_timings.csv"
    if p.exists():
        with p.open() as fh:
            for r in csv.DictReader(fh):
                if r["label"] == label and r["step"] in steps:
                    s = steps[r["step"]]
                    s["executor_s"] = s.get("executor_s", 0.0) + float(r["executor_run_s"] or 0)
                    s["stages"] = s.get("stages", 0) + 1
    for s in steps.values():
        s["executor_s"] = round(s.get("executor_s", 0.0), 1)
    return steps


def run(args) -> int:
    """Steps 04 and 04b (or 03, 04, 04b) of one scope, with the code of ``--code``."""
    code = Path(args.code).resolve() if args.code else REPO
    if not (code / "src" / "opdi").is_dir():
        raise SystemExit(f"--code {code}: no src/opdi there")
    check_env_file(REPO)
    steps = list(args.steps)
    if steps not in (["04", "04b"], ["03", "04", "04b"], ["04"], ["03"], ["03", "04"]):
        raise SystemExit(f"--steps: 04 04b, or 03 04 04b, not {' '.join(steps)}")
    root = run_root(args.label)
    out = local_dir(args.label)
    n, _ = s3_size(root)
    if n and not args.replace:
        raise SystemExit(f"{root} exists: choose another label, or --replace to run it again")
    with spark_lock(f"run {args.label}"):
        if n:
            print(f"deleting the earlier run {args.label}: {s3_delete(root):.2f} GB", flush=True)
        out.mkdir(parents=True, exist_ok=True)
        for f in ("step_timings.csv", "stage_timings.csv"):
            (out / f).unlink(missing_ok=True)
        dt = _point_day_timing(out)
        # The pipeline of ``--code``, not of this worktree.
        sys.path.insert(0, str(code / "src"))
        import opdi
        import opdi.utils.spark_helpers as helpers

        if not Path(opdi.__file__).resolve().is_relative_to(code / "src"):
            raise SystemExit(f"opdi was imported from {opdi.__file__}, not from {code}/src")
        src = scope_sources(args.scope, steps)
        reads = [f"{t}@{f}" for t, f in src.items() if t != "opdi_flight_list"]
        if "opdi_flight_list" in src:
            _copy_flight_list_on_start(
                helpers.SparkSessionManager,
                f"{PREFIX}/{src['opdi_flight_list']}/opdi_flight_list",
                f"{root}/opdi_flight_list",
            )
        meta = {
            "label": args.label,
            "scope": args.scope,
            "steps": steps,
            "set": list(args.set or []),
            "reads": src,
            **git_state(code),
            "scorecard": git_state(REPO)["commit"],
            "started": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        print(json.dumps(meta, indent=2), flush=True)
        t0 = time.monotonic()
        rc = dt.main(_day_timing_args(f"{RUNS}/{args.label}", steps, reads, args.set, out))
        meta["seconds"] = round(time.monotonic() - t0)
        meta["exit"] = rc
        meta["steps_timed"] = step_rows(out, f"{RUNS}/{args.label}")
        meta["finished"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        (out / "run.json").write_text(json.dumps(meta, indent=2))
        print(json.dumps(meta["steps_timed"], indent=2))
        return rc


# --------------------------------------------------------------------------
# score: Spark column helpers
# --------------------------------------------------------------------------


def _hav_nm(lat1, lon1, lat2, lon2):
    """Great-circle distance in NM between two positions (Spark columns, degrees)."""
    from pyspark.sql import functions as F

    p1, p2 = F.radians(lat1), F.radians(lat2)
    dlat, dlon = p2 - p1, F.radians(lon2) - F.radians(lon1)
    a = F.pow(F.sin(dlat / 2), 2) + F.cos(p1) * F.cos(p2) * F.pow(F.sin(dlon / 2), 2)
    return 2 * EARTH_RADIUS_NM * F.asin(F.sqrt(F.least(a, F.lit(1.0))))


def _bearing(lat1, lon1, lat2, lon2):
    """Initial bearing from the first position to the second, degrees 0..360."""
    from pyspark.sql import functions as F

    p1, p2 = F.radians(lat1), F.radians(lat2)
    dlon = F.radians(lon2) - F.radians(lon1)
    y = F.sin(dlon) * F.cos(p2)
    x = F.cos(p1) * F.sin(p2) - F.sin(p1) * F.cos(p2) * F.cos(dlon)
    return F.pmod(F.degrees(F.atan2(y, x)) + 360.0, F.lit(360.0))


def _angdiff(a, b):
    from pyspark.sql import functions as F

    return F.abs(F.pmod(a - b + 540.0, F.lit(360.0)) - 180.0)


def _info(field: str, col: str = "info"):
    from pyspark.sql import functions as F

    return F.get_json_object(F.col(col), f"$.{field}")


def _rwy_norm(c):
    """A runway designator as two digits and a side letter (``9l`` -> ``09L``); NULL otherwise."""
    from pyspark.sql import functions as F

    num = F.regexp_extract(F.upper(F.trim(c)), r"^0*([0-9]{1,2})([LRCG]?)$", 1)
    side = F.regexp_extract(F.upper(F.trim(c)), r"^0*([0-9]{1,2})([LRCG]?)$", 2)
    return F.when(num != "", F.concat(F.lpad(num, 2, "0"), side))


def _rwy_recip(c):
    """The designator of the other end of the runway (``09L`` -> ``27R``).

    The Spark form of :func:`reciprocal_runway`.
    """
    from pyspark.sql import functions as F

    n = _rwy_norm(c)
    num = F.substring(n, 1, 2).cast("int")
    side = F.substring(n, 3, 1)
    other = F.when(side == "L", "R").when(side == "R", "L").otherwise(side)
    return F.when(
        n.isNotNull(),
        F.concat(F.lpad((F.pmod(num + 17, F.lit(36)) + 1).cast("string"), 2, "0"), other),
    )


def _secs(c):
    """A timestamp column (or ISO text) as seconds since 1970."""
    from pyspark.sql import functions as F

    return (
        F.col(c).cast("timestamp").cast("double")
        if isinstance(c, str)
        else c.cast("timestamp").cast("double")
    )


# --------------------------------------------------------------------------
# score: the frames
# --------------------------------------------------------------------------

#: Runway milestone types (departure and arrival side).
DEP_RUNWAY_TYPES = ("ATOT", "line-up", "take-off-roll")
ARR_RUNWAY_TYPES = ("ALDT", "runway-vacated", "xing-runway-threshold")
RUNWAY_TYPES = DEP_RUNWAY_TYPES + ARR_RUNWAY_TYPES
TOPS = ("top-of-climb", "top-of-descent")
LEVEL = ("level-start", "level-end")
LEVEL_OFF_STARTS = ("level-off-climb-start", "level-off-descent-start")
LEVEL_OFFS = LEVEL_OFF_STARTS + ("level-off-climb-end", "level-off-descent-end")
BLOCK = ("AOBT", "AIBT")
RINGS_RE = r"^xing-[0-9]+nm$"
FL_RE = r"^xing-fl[0-9]+$"
LAYOUT_RE = r"^(entry|exit)-(apron|taxiway|parking_position|hangar|deicing_pad|threshold|runway)$"
#: ``info`` fields the corrections add (DECISIONS.md); counted wherever they appear.
INFO_FLAGS = (
    ("observed", "false"),
    ("at_track_edge", "true"),
    ("at_track_start", "true"),
    ("at_track_end", "true"),
    ("ground_only", "true"),
    ("vertical", "true"),
    ("context", None),
    ("stand_seen", "false"),
)


def score_inputs(spark, label: str, scope: str) -> dict:
    """The tables a scorecard reads: the run's own, the scope's tracks, the references."""
    from pyspark.sql import functions as F

    root = run_root(label)
    day = F.lit(DAY).cast("date")
    src = {"sample": SAMPLE, "day": DAY_RUN}[scope]
    n, _ = s3_size(f"{root}/opdi_endpoint_candidates")
    cand_src = (
        f"{root}/opdi_endpoint_candidates"
        if n
        else (f"{PREFIX}/{SAMPLE if scope == 'sample' else DAY_STEP03}/opdi_endpoint_candidates")
    )
    fl_all = spark.read.parquet(f"{root}/opdi_flight_list")
    return {
        "events": spark.read.parquet(f"{root}/opdi_flight_events").filter(F.col("dof") == day),
        "measurements": spark.read.parquet(f"{root}/opdi_measurements").filter(F.col("dof") == day),
        "flight_list_all": fl_all,
        "flight_list": fl_all.filter(F.col("DOF") == day),
        "tracks": (
            spark.read.parquet(f"{PREFIX}/{src}/osn_tracks_clean").filter(F.col("dof") == day)
        ),
        "candidates": spark.read.parquet(cand_src),
        "airports": spark.read.parquet(f"{WORLD}/oa_airports"),
        "runways": spark.read.parquet(f"{WORLD}/oa_runways"),
    }


def runway_ends(rwy):
    """One row per runway end: airport, designator, threshold, bearing along the runway, size."""
    from pyspark.sql import functions as F

    base = rwy.select(
        "airport_ident",
        "le_ident",
        "he_ident",
        "le_latitude_deg",
        "le_longitude_deg",
        "he_latitude_deg",
        "he_longitude_deg",
        "length_ft",
        "width_ft",
    ).filter(F.col("le_latitude_deg").isNotNull() & F.col("he_latitude_deg").isNotNull())
    ends = []
    for a, b in (("le", "he"), ("he", "le")):
        ends.append(
            base.select(
                F.col("airport_ident").alias("apt"),
                _rwy_norm(F.col(f"{a}_ident")).alias("rwy_n"),
                F.col(f"{a}_latitude_deg").alias("thr_lat"),
                F.col(f"{a}_longitude_deg").alias("thr_lon"),
                _bearing(
                    F.col(f"{a}_latitude_deg"),
                    F.col(f"{a}_longitude_deg"),
                    F.col(f"{b}_latitude_deg"),
                    F.col(f"{b}_longitude_deg"),
                ).alias("thr_brg"),
                (F.col("length_ft") / 6076.12).alias("rwy_len_nm"),
                (F.col("width_ft") / 6076.12).alias("rwy_wid_nm"),
            )
        )
    return (
        ends[0]
        .unionByName(ends[1])
        .filter(F.col("rwy_n").isNotNull())
        .dropDuplicates(["apt", "rwy_n"])
    )


def build_frames(spark, inp: dict) -> dict:
    """The working frames of a scorecard (see the comments of each)."""
    from pyspark import StorageLevel
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    keep = StorageLevel.MEMORY_AND_DISK
    # Reports: cleaned tracks of the scope. ``h`` is a measured flight
    # altitude in ft: the cleaned altitude, in -2,000..60,000 ft, not on the ground.
    T = (
        inp["tracks"]
        .select(
            "track_id",
            F.col("event_time").cast("double").alias("ts"),
            "lat",
            "lon",
            "velocity",
            "heading",
            "vert_rate",
            "on_ground",
            "baro_altitude",
            "baro_altitude_c",
            F.lower("icao24").alias("icao24"),
        )
        .persist(keep)
    )
    pos = F.col("lat").isNotNull() & F.col("lon").isNotNull()
    h = F.col("baro_altitude") * FT_PER_M
    hval = F.when(
        F.col("baro_altitude").isNotNull()
        & h.between(-2000, 60000)
        & ~F.coalesce(F.col("on_ground"), F.lit(False)),
        h,
    )
    # Per track: first and last report, first and last positioned report.
    S = (
        T.groupBy("track_id")
        .agg(
            F.min("ts").alias("t0"),
            F.max("ts").alias("t1"),
            F.min(F.when(pos, F.col("ts"))).alias("p0_ts"),
            F.max(F.when(pos, F.col("ts"))).alias("p1_ts"),
            F.min_by(F.struct("lat", "lon"), F.when(pos, F.col("ts"))).alias("_p0"),
            F.max_by(F.struct("lat", "lon"), F.when(pos, F.col("ts"))).alias("_p1"),
            F.count(F.when(pos, 1)).alias("n_pos"),
            F.count(F.lit(1)).alias("n_rep"),
            F.max(F.when(F.col("on_ground") == F.lit(False), 1).otherwise(0)).alias("any_air"),
            F.max(hval).alias("max_h_ft"),
            F.min(F.when(F.col("baro_altitude_c").isNotNull(), F.col("ts"))).alias("a0_ts"),
            F.max(F.when(F.col("baro_altitude_c").isNotNull(), F.col("ts"))).alias("a1_ts"),
        )
        .select(
            "*",
            F.col("_p0.lat").alias("p0_lat"),
            F.col("_p0.lon").alias("p0_lon"),
            F.col("_p1.lat").alias("p1_lat"),
            F.col("_p1.lon").alias("p1_lon"),
        )
        .drop("_p0", "_p1")
        .persist(keep)
    )

    ev = inp["events"]
    E = ev.select(
        "id",
        "flight_id",
        "type",
        F.col("event_time").cast("double").alias("ts"),
        F.col("latitude").alias("lat"),
        F.col("longitude").alias("lon"),
        F.col("altitude").alias("alt"),
        "version",
        "info",
        F.coalesce(_info("apt_icao"), _info("osm_airport")).alias("apt"),
        _info("runway").alias("runway"),
        _info("runway_bearing_deg").cast("double").alias("rwy_brg"),
        _info("method").alias("method"),
        _info("direction").alias("direction"),
        _info("bracket_s").cast("double").alias("bracket_s"),
        _info("stand").alias("stand"),
        _secs(_info("stand_exit")).alias("stand_exit_ts"),
        _secs(_info("stand_entry")).alias("stand_entry_ts"),
        _secs(_info("take_off")).alias("take_off_ts"),
        _secs(_info("landing")).alias("landing_ts"),
        _info("osm_id").alias("osm_id"),
        _info("osm_ref").alias("osm_ref"),
        _info("opdi_time_in_use_s").cast("double").alias("tiu"),
        _info("excursion_seconds").cast("double").alias("excursion_s"),
        _info("pattern").alias("pattern"),
        _info("straight_pct").cast("double").alias("straight_pct"),
        _info("via_leg").alias("via_leg"),
        _info("pms_id").alias("pms_id"),
        _info("leg_seq").cast("int").alias("leg_seq"),
        _info("leg_distance_nm").cast("double").alias("leg_dist"),
        _info("duration_s").cast("double").alias("i_duration_s"),
        _info("distance_nm").cast("double").alias("i_distance_nm"),
        _info("fir_from").alias("fir_from"),
        _info("fir_to").alias("fir_to"),
        _info("fl").cast("double").alias("fir_fl"),
        _info("max_gs_kt").cast("double").alias("max_gs_kt"),
    )
    E = E.withColumn(
        "item",
        F.when(
            F.col("type").isin("ATOT", "ALDT"),
            F.concat(
                F.col("type"), F.lit(" ("), F.coalesce("method", F.lit("no method")), F.lit(")")
            ),
        ).otherwise(F.col("type")),
    )
    E = E.withColumn("id_rows", F.count(F.lit(1)).over(Window.partitionBy("id")))
    E = E.withColumn(
        "same_rwy_600",
        F.count(F.lit(1)).over(
            Window.partitionBy("flight_id", "type", "runway").orderBy("ts").rangeBetween(-600, 600)
        ),
    )
    E = E.withColumn("_eid", F.monotonically_increasing_id()).persist(keep)
    n_events = E.count()

    # Per track: the runway milestones and the seen events.
    mov = F.struct(
        F.col("ts").alias("ts"), F.col("apt").alias("apt"), F.col("runway").alias("runway")
    )
    TE = (
        E.groupBy("flight_id")
        .agg(
            F.max(F.when(F.col("type") == "ATOT", F.col("ts"))).alias("atot_ts"),
            F.max(F.when(F.col("type") == "ATOT", F.col("apt"))).alias("atot_apt"),
            F.max(F.when(F.col("type") == "ATOT", F.col("method"))).alias("atot_method"),
            F.max(F.when(F.col("type") == "ALDT", F.col("ts"))).alias("aldt_ts"),
            F.max(F.when(F.col("type") == "ALDT", F.col("apt"))).alias("aldt_apt"),
            F.max(F.when(F.col("type") == "ALDT", F.col("method"))).alias("aldt_method"),
            F.collect_list(F.when(F.col("type").isin(*RUNWAY_TYPES), F.col("ts"))).alias("rwy_ts"),
            F.collect_list(F.when(F.col("type") == "line-up", F.col("ts"))).alias("lineup_ts"),
            F.collect_list(F.when(F.col("type") == "runway-vacated", F.col("ts"))).alias(
                "vacated_ts"
            ),
            F.collect_list(F.when(F.col("type").isin(*RUNWAY_TYPES), mov)).alias("mov"),
            F.min(F.when(F.col("type") == "first_seen", F.col("ts"))).alias("fs_ev"),
            F.max(F.when(F.col("type") == "last_seen", F.col("ts"))).alias("ls_ev"),
            F.sum(F.when(F.col("type") == "holding-start", 1).otherwise(0)).alias("n_hold"),
            F.sum(F.when(F.col("type") == "pms-merge-point", 1).otherwise(0)).alias("n_pms_mp"),
            F.sum(F.when(F.col("type").isin("line-up", "take-off-roll"), 1).otherwise(0)).alias(
                "n_roll"
            ),
            F.sum(F.when(F.col("type") == "runway-vacated", 1).otherwise(0)).alias("n_vacated"),
        )
        .persist(keep)
    )

    apt = (
        inp["airports"]
        .select(
            F.col("ident"),
            F.col("name"),
            F.col("type").alias("apt_type"),
            F.col("latitude_deg").alias("a_lat"),
            F.col("longitude_deg").alias("a_lon"),
            F.col("elevation_ft").cast("double").alias("a_elev"),
            F.col("scheduled_service"),
        )
        .dropDuplicates(["ident"])
        .persist(keep)
    )

    fl = inp["flight_list"]
    FLX = fl.select(
        F.col("ID").alias("flight_id"),
        F.lower("ICAO24").alias("icao24"),
        F.col("FLT_ID").alias("flt_id"),
        F.trim("FLT_ID").alias("cs"),
        F.col("ADEP").alias("adep"),
        F.col("ADES").alias("ades"),
        F.col("ADEP_P").alias("adep_p"),
        F.col("ADES_P").alias("ades_p"),
        F.col("ADEP_SOURCE").alias("adep_source"),
        F.col("ADES_SOURCE").alias("ades_source"),
        F.col("TYPECODE").alias("typecode"),
        _secs("FIRST_SEEN").alias("fs"),
        _secs("LAST_SEEN").alias("ls"),
        _secs("ATOT").alias("fl_atot"),
        _secs("ALDT").alias("fl_aldt"),
        _secs("AOBT").alias("fl_aobt"),
        _secs("AIBT").alias("fl_aibt"),
        F.col("RWY_DEP").alias("rwy_dep"),
        F.col("RWY_DEP_BEARING_DEG").alias("rwy_dep_brg"),
        F.col("RWY_ARR").alias("rwy_arr"),
        F.col("RWY_ARR_BEARING_DEG").alias("rwy_arr_brg"),
        F.col("STND_DEP").alias("stnd_dep"),
        F.col("STND_ARR").alias("stnd_arr"),
        _secs("C40_DEP").alias("c40_dep"),
        _secs("C40_ARR").alias("c40_arr"),
        F.col("SUPERSEDED_DEP").alias("sup_dep"),
        F.col("SUPERSEDED_ARR").alias("sup_arr"),
        F.col("MOVEMENT_DEP").alias("mov_dep"),
        F.col("MOVEMENT_ARR").alias("mov_arr"),
        F.col("TRACK_DURATION_MIN").alias("dur_min"),
    )
    for side in ("adep", "ades"):
        FLX = FLX.join(
            F.broadcast(
                apt.select(
                    F.col("ident").alias(side),
                    F.col("a_elev").alias(f"{side}_elev"),
                    F.col("a_lat").alias(f"{side}_lat"),
                    F.col("a_lon").alias(f"{side}_lon"),
                )
            ),
            side,
            "left",
        )
    FLX = FLX.persist(keep)

    # As of each event: the last report at or before it (any), the positioned
    # reports either side, the last report with a detector altitude, and the
    # lowest measured altitude before and after it. One pass over the reports
    # of each track, with the events slotted in by time.
    rep = T.select(
        "track_id",
        "ts",
        F.lit(0).alias("_k"),
        F.lit(None).cast("long").alias("_eid"),
        F.struct(
            F.col("ts").alias("ts"),
            "lat",
            "lon",
            "velocity",
            "heading",
            "vert_rate",
            "on_ground",
            "baro_altitude",
            "baro_altitude_c",
        ).alias("r"),
        F.when(pos, F.struct(F.col("ts").alias("ts"), "lat", "lon")).alias("p"),
        F.when(
            F.col("baro_altitude_c").isNotNull(),
            F.struct(F.col("ts").alias("ts"), "baro_altitude_c", "baro_altitude"),
        ).alias("a"),
        hval.alias("h"),
    )
    evs = E.select(
        F.col("flight_id").alias("track_id"),
        "ts",
        F.lit(1).alias("_k"),
        "_eid",
        F.lit(None).cast(rep.schema["r"].dataType).alias("r"),
        F.lit(None).cast(rep.schema["p"].dataType).alias("p"),
        F.lit(None).cast(rep.schema["a"].dataType).alias("a"),
        F.lit(None).cast("double").alias("h"),
    ).filter(F.col("ts").isNotNull())
    order = Window.partitionBy("track_id").orderBy("ts", "_k")
    pre = order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    post = order.rowsBetween(Window.currentRow, Window.unboundedFollowing)
    ctx = (
        rep.unionByName(evs)
        .select(
            "_k",
            "_eid",
            F.last("r", ignorenulls=True).over(pre).alias("r"),
            F.last("p", ignorenulls=True).over(pre).alias("pp"),
            F.first("p", ignorenulls=True).over(post).alias("np"),
            F.last("a", ignorenulls=True).over(pre).alias("pa"),
            F.min("h").over(pre).alias("minh_pre"),
            F.min("h").over(post).alias("minh_post"),
        )
        .filter(F.col("_k") == 1)
        .select(
            "_eid",
            F.col("r.ts").alias("rep_ts"),
            F.col("r.lat").alias("rep_lat"),
            F.col("r.lon").alias("rep_lon"),
            F.col("r.velocity").alias("rep_vel"),
            F.col("r.heading").alias("rep_hdg"),
            F.col("r.vert_rate").alias("rep_vr"),
            F.col("r.on_ground").alias("rep_gnd"),
            (F.col("r.baro_altitude_c") * FT_PER_M).alias("rep_alt_c_ft"),
            (F.col("r.baro_altitude") * FT_PER_M).alias("rep_alt_ft"),
            F.col("pp.ts").alias("pp_ts"),
            F.col("pp.lat").alias("pp_lat"),
            F.col("pp.lon").alias("pp_lon"),
            F.col("np.ts").alias("np_ts"),
            F.col("np.lat").alias("np_lat"),
            F.col("np.lon").alias("np_lon"),
            F.col("pa.ts").alias("pa_ts"),
            F.col("pa.baro_altitude_c").alias("pa_alt_c"),
            F.col("pa.baro_altitude").alias("pa_alt"),
            "minh_pre",
            "minh_post",
        )
    )

    M = inp["measurements"]
    MA = M.groupBy(F.col("milestone_id").alias("id")).agg(
        F.max(F.when(F.col("type") == "Distance flown (NM)", F.col("value"))).alias("d_val"),
        F.max(F.when(F.col("type") == "Time Passed (s)", F.col("value"))).alias("t_val"),
        F.count(F.lit(1)).alias("n_meas"),
    )

    win = window_context(E, T, FLX)
    ends = runway_ends(inp["runways"])
    X = (
        E.join(ctx, "_eid", "left")
        .join(S.withColumnRenamed("track_id", "flight_id"), "flight_id", "left")
        .join(TE, "flight_id", "left")
        .join(
            FLX.select(
                "flight_id",
                "adep",
                "ades",
                "adep_source",
                "ades_source",
                "typecode",
                "adep_elev",
                "ades_elev",
                "adep_lat",
                "adep_lon",
                "ades_lat",
                "ades_lon",
            ),
            "flight_id",
            "left",
        )
        .join(MA, "id", "left")
        .join(
            F.broadcast(
                apt.select(
                    F.col("ident").alias("apt"),
                    F.col("a_elev").alias("apt_elev"),
                    F.col("a_lat").alias("apt_lat"),
                    F.col("a_lon").alias("apt_lon"),
                )
            ),
            "apt",
            "left",
        )
        .join(
            F.broadcast(ends.withColumnRenamed("apt", "_ra")),
            (F.col("_ra") == F.col("apt")) & (F.col("rwy_n") == _rwy_norm(F.col("runway"))),
            "left",
        )
        .join(win.filter(F.col("_eid").isNotNull()), "_eid", "left")
        .drop("_ra")
    ).persist(keep)
    print(f"  frames: {n_events:,} events, {X.count():,} rows with their context", flush=True)
    return {"T": T, "S": S, "E": E, "TE": TE, "FLX": FLX, "X": X, "apt": apt, "M": M, "win": win}


def window_context(E, T, FLX):
    """The reports around runway milestones, point-merge leg exits and the flight list's runways.

    ``hdg_fast``: the mean heading of the reports within 30 s that are faster
    than 50 kt (``hdg_n`` of them). ``roll_pos_kt``: the highest ground speed
    from two consecutive positioned reports (at most 15 s apart) in the 60 s
    before a take-off-roll. ``turn_dt``: seconds after a point-merge leg exit
    until the heading first differs by more than 10 degrees from the heading
    at the exit (within 120 s).
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    ev = E.filter(F.col("type").isin(*RUNWAY_TYPES, "pms-leg-exit")).select(
        F.col("_eid"), F.lit(None).cast("string").alias("_fl"), "flight_id", "ts", "type"
    )
    fl = FLX.select(
        F.lit(None).cast("long").alias("_eid"),
        F.concat(F.col("flight_id"), F.lit("|dep")).alias("_fl"),
        "flight_id",
        F.col("fl_atot").alias("ts"),
        F.lit("flight list RWY_DEP").alias("type"),
    ).unionByName(
        FLX.select(
            F.lit(None).cast("long").alias("_eid"),
            F.concat(F.col("flight_id"), F.lit("|arr")).alias("_fl"),
            "flight_id",
            F.col("fl_aldt").alias("ts"),
            F.lit("flight list RWY_ARR").alias("type"),
        )
    )
    sel = ev.unionByName(fl).filter(F.col("ts").isNotNull())
    sel = sel.withColumn("_key", F.coalesce(F.col("_eid").cast("string"), F.col("_fl")))
    sel = sel.withColumn(
        "_b",
        F.explode(F.sequence(F.floor((F.col("ts") - 120) / 60), F.floor((F.col("ts") + 120) / 60))),
    )
    rp = T.select(
        F.col("track_id").alias("flight_id"),
        F.col("ts").alias("rts"),
        "lat",
        "lon",
        "velocity",
        "heading",
        F.floor(F.col("ts") / 60).alias("_b"),
    )
    j = sel.join(rp, ["flight_id", "_b"]).withColumn("dt", F.col("rts") - F.col("ts"))
    fast = (
        (F.abs("dt") <= 30) & (F.col("velocity") * KT_PER_MPS > 50) & F.col("heading").isNotNull()
    )
    rad = F.radians("heading")
    agg = (
        j.groupBy("_key")
        .agg(
            F.first("_eid").alias("_eid"),
            F.first("_fl").alias("_fl"),
            F.count(F.when(fast, 1)).alias("hdg_n"),
            F.degrees(
                F.atan2(F.avg(F.when(fast, F.sin(rad))), F.avg(F.when(fast, F.cos(rad))))
            ).alias("_h"),
            F.min_by(
                F.col("heading"),
                F.when(F.col("heading").isNotNull() & (F.abs("dt") <= 10), F.abs("dt")),
            ).alias("_h0"),
        )
        .withColumn("hdg_fast", F.pmod(F.col("_h") + 360.0, F.lit(360.0)))
        .drop("_h")
    )
    # The take-off roll seen in positions.
    roll = j.filter(
        (F.col("type") == "take-off-roll")
        & F.col("lat").isNotNull()
        & (F.col("dt") >= -60)
        & (F.col("dt") < 0)
    )
    w = Window.partitionBy("_key").orderBy("rts")
    roll = (
        roll.withColumn("_plat", F.lag("lat").over(w))
        .withColumn("_plon", F.lag("lon").over(w))
        .withColumn("_pts", F.lag("rts").over(w))
        .withColumn("_gap", F.col("rts") - F.col("_pts"))
        .withColumn(
            "_kt",
            F.when(
                (F.col("_gap") > 0) & (F.col("_gap") <= 15),
                _hav_nm(F.col("_plat"), F.col("_plon"), F.col("lat"), F.col("lon"))
                / F.col("_gap")
                * 3600,
            ),
        )
        .groupBy("_key")
        .agg(F.max("_kt").alias("roll_pos_kt"), F.count(F.lit(1)).alias("roll_pos_n"))
    )
    turn = (
        j.filter(
            (F.col("type") == "pms-leg-exit")
            & (F.col("dt") > 0)
            & (F.col("dt") <= 120)
            & F.col("heading").isNotNull()
        )
        .join(agg.select("_key", "_h0"), "_key")
        .groupBy("_key")
        .agg(
            F.min(F.when(_angdiff(F.col("heading"), F.col("_h0")) > 10, F.col("dt"))).alias(
                "turn_dt"
            ),
            F.max("dt").alias("turn_seen_s"),
        )
    )
    return agg.join(roll, "_key", "left").join(turn, "_key", "left").drop("_h0")


# --------------------------------------------------------------------------
# score: section iii, the odd cases
# --------------------------------------------------------------------------


def event_counters():
    """The odd-case counters that read one event with its context.

    Each is ``(name, finding, odd case in words, base, odd)``: ``base`` says
    which events the case is counted among, ``odd`` which of them are odd
    (both Spark conditions on the frame of :func:`build_frames`). The count is
    made per item (the event type; ATOT and ALDT by ``info.method``).
    """
    from pyspark.sql import functions as F

    c = F.col
    t = c("type")
    isin = t.isin
    at_rep = c("rep_ts").isNotNull() & ((c("ts") - c("rep_ts")) <= 0.5)
    gnd = F.coalesce(c("rep_gnd"), F.lit(False))
    kt = c("rep_vel") * KT_PER_MPS
    fpm = c("rep_vr") * FTMIN_PER_MPS
    pos_kt = F.when(
        (c("np_ts") - c("pp_ts")).between(0.5, 60),
        _hav_nm(c("pp_lat"), c("pp_lon"), c("np_lat"), c("np_lon"))
        / (c("np_ts") - c("pp_ts"))
        * 3600,
    )
    brk_kt = F.when(
        c("np_ts") > c("pp_ts"),
        _hav_nm(c("pp_lat"), c("pp_lon"), c("np_lat"), c("np_lon"))
        / (c("np_ts") - c("pp_ts"))
        * 3600,
    )
    has_pos = c("lat").isNotNull() & c("lon").isNotNull()
    d_adep = _hav_nm(c("lat"), c("lon"), c("adep_lat"), c("adep_lon"))
    d_ades = _hav_nm(c("lat"), c("lon"), c("ades_lat"), c("ades_lon"))
    near_elev = F.when(
        has_pos & c("adep_lat").isNotNull() & c("ades_lat").isNotNull(),
        F.when(d_adep <= d_ades, c("adep_elev")).otherwise(c("ades_elev")),
    ).otherwise(F.coalesce(c("adep_elev"), c("ades_elev")))
    near_dist = F.least(d_adep, d_ades)
    field_elev = F.coalesce(c("apt_elev"), near_elev)
    d_thr = _hav_nm(c("thr_lat"), c("thr_lon"), c("lat"), c("lon"))
    b_thr = F.radians(_bearing(c("thr_lat"), c("thr_lon"), c("lat"), c("lon")) - c("thr_brg"))
    along = d_thr * F.cos(b_thr)
    cross = F.abs(d_thr * F.sin(b_thr))
    xing = t.startswith("xing-")
    rings = t.rlike(RINGS_RE)
    layout = t.rlike(LAYOUT_RE)
    entry = t.startswith("entry-")
    exit_ = t.startswith("exit-")
    no_name = F.coalesce(c("stand"), F.lit("")).isin(*NO_NAME)
    alt_ok = c("alt").isNotNull()
    top = isin(*TOPS)
    toc = t == "top-of-climb"
    tod = t == "top-of-descent"
    ls_ = isin(*LEVEL)
    lo_start = isin(*LEVEL_OFF_STARTS)
    legacy = c("method") == "legacy"
    acdm = c("method") == "acdm"
    any_event = F.lit(True)
    same_strip = lambda m: (m["apt"] == c("apt")) & (  # noqa: E731
        (_rwy_norm(m["runway"]) == _rwy_norm(c("runway")))
        | (_rwy_recip(m["runway"]) == _rwy_norm(c("runway")))
    )
    hold_end = t == "holding-end"
    after_land = c("aldt_ts").isNotNull() & (c("aldt_ts") > c("ts"))
    arrival_hold = (
        after_land
        & ((c("aldt_ts") - c("ts")) <= 3600)
        & (c("aldt_apt") == c("ades"))
        & (_hav_nm(c("lat"), c("lon"), c("ades_lat"), c("ades_lon")) <= 150)
    )
    return [
        # ---- work package 1: the altitude the detectors read
        (
            "alt_removed",
            "c1.1 c2.1 c3.A d2.C2",
            "the detector's altitude at the event's report is one the cleaning removed",
            at_rep & c("rep_alt_c_ft").isNotNull(),
            c("rep_alt_ft").isNull(),
        ),
        (
            "alt_differs",
            "c1.1",
            (
                "the detector's altitude at the event's report differs from the cleaned one by more"
                " than 100 ft"
            ),
            at_rep & c("rep_alt_c_ft").isNotNull() & c("rep_alt_ft").isNotNull(),
            F.abs(c("rep_alt_c_ft") - c("rep_alt_ft")) > 100,
        ),
        (
            "bracket_removed",
            "c3.A",
            (
                "crossing bracketed by an altitude the cleaning removed (last report with an"
                " altitude before it)"
            ),
            xing & c("pa_ts").isNotNull(),
            c("pa_alt").isNull(),
        ),
        (
            "alt_range",
            "c1.1 c2.3 WP1.2",
            "altitude outside -2,000..60,000 ft",
            alt_ok,
            (c("alt") < -2000) | (c("alt") > 60000),
        ),
        ("no_alt", "d1.6 e1.6", "event without an altitude", any_event, ~alt_ok),
        (
            "ghost_ground",
            "c3.A2 WP1.3",
            "on a report flagged on the ground with an altitude more than 3,000 ft above the field",
            at_rep & gnd & c("rep_alt_c_ft").isNotNull(),
            c("rep_alt_c_ft") - F.coalesce(field_elev, F.lit(0.0)) > 3000,
        ),
        (
            "top_outside_alt",
            "c1.3",
            "top of climb before the first altitude, top of descent after the last",
            top,
            (toc & (c("ts") < c("a0_ts"))) | (tod & (c("ts") > c("a1_ts"))),
        ),
        (
            "top_edge",
            "c1.2",
            "top of climb in the first 30 s of the track, top of descent in the last 30 s",
            top,
            (toc & (c("ts") <= c("t0") + 30)) | (tod & (c("ts") >= c("t1") - 30)),
        ),
        (
            "top_unobserved",
            "c1.2 WP1.6",
            (
                "no climb of 3,000 ft before the top of climb (no descent of 3,000 ft after the top"
                " of descent)"
            ),
            top & alt_ok,
            (toc & (c("minh_pre").isNull() | (c("alt") - c("minh_pre") < 3000)))
            | (tod & (c("minh_post").isNull() | (c("alt") - c("minh_post") < 3000))),
        ),
        ("top_no_pos", "c1.7", "top without a position", top, ~has_pos),
        (
            "top_no_pos_near",
            "c1.7",
            "top without a position although a positioned report is within 60 s",
            top & ~has_pos,
            ((c("ts") - c("pp_ts")) <= 60) | ((c("np_ts") - c("ts")) <= 60),
        ),
        (
            "lo_dist0",
            "c2.1",
            "level-off with a distance of 0 (made of repeated altitudes)",
            lo_start,
            c("i_distance_nm") < 0.01,
        ),
        ("lo_long", "c2.2", "level-off longer than 3,000 s", lo_start, c("i_duration_s") > 3000),
        (
            "lo_edge",
            "c2.2",
            "level-off touching the first or last 10 s of the track's seen events",
            lo_start,
            (c("ts") <= c("fs_ev") + 10) | (c("ts") + c("i_duration_s") >= c("ls_ev") - 10),
        ),
        (
            "lo_fast",
            "c2.6",
            "level-off whose distance over its duration implies more than 700 kt",
            lo_start & (c("i_duration_s") > 0),
            c("i_distance_nm") / c("i_duration_s") * 3600 > 700,
        ),
        ("lo_no_pos", "c2.5 WP1.7", "level-off without a position", isin(*LEVEL_OFFS), ~has_pos),
        (
            "level_field_fast",
            "c1.5",
            "level segment within 50 ft of the field (3 NM) at more than 50 kt",
            ls_ & alt_ok & has_pos,
            (c("alt") - near_elev < 50) & (near_dist <= 3) & at_rep & (kt > 50),
        ),
        (
            "level_near_runway",
            "c1.5",
            "level segment within 60 s of a runway milestone of the track",
            ls_,
            F.exists(c("rwy_ts"), lambda x: F.abs(x - c("ts")) <= 60),
        ),
        (
            "level_on_ground",
            "c1.5",
            "level segment on a report flagged on the ground",
            ls_ & at_rep,
            gnd,
        ),
        # ---- work package 2: runway events
        ("id_rows", "a2.C8 a3.X5", "event id on more than one row", any_event, c("id_rows") > 1),
        ("no_pos", "a2.C6 c3.D d2.C1 hint 5", "event without a position", any_event, ~has_pos),
        (
            "vacated_before_thr",
            "a2.C7",
            "runway-vacated before the threshold of its runway",
            (t == "runway-vacated") & c("thr_lat").isNotNull() & has_pos,
            along < 0,
        ),
        (
            "vacated_air",
            "a2.C7",
            (
                "runway-vacated on a report not flagged on the ground and descending 300 ft/min or"
                " more"
            ),
            (t == "runway-vacated") & at_rep,
            ~gnd & (fpm <= -300),
        ),
        (
            "roll_after_to",
            "a2.C5",
            "take-off-roll at or after the take-off time",
            (t == "take-off-roll")
            & c("atot_ts").isNotNull()
            & (F.abs(c("ts") - c("atot_ts")) <= 600),
            c("ts") >= c("atot_ts"),
        ),
        (
            "roll_late",
            "a2.C4",
            "take-off-roll late: positions show 50 kt or more in the 60 s before it",
            (t == "take-off-roll") & (c("roll_pos_n") >= 2),
            c("roll_pos_kt") >= 50,
        ),
        (
            "lineup_descending",
            "a2.C3",
            (
                "line-up or take-off-roll on a report not flagged on the ground, descending 300"
                " ft/min or more"
            ),
            isin("line-up", "take-off-roll") & at_rep & c("rep_vr").isNotNull(),
            ~gnd & (fpm <= -300),
        ),
        (
            "rwy_against_heading",
            "a1.1 a2.C1 f3.1",
            (
                "runway named against the aircraft's heading (more than 90 degrees, reports within"
                " 30 s over 50 kt)"
            ),
            isin(*RUNWAY_TYPES) & (c("hdg_n") > 0) & c("rwy_brg").isNotNull(),
            _angdiff(c("hdg_fast"), c("rwy_brg")) > 90,
        ),
        (
            "thr_high",
            "a1.7",
            "threshold crossing more than 300 ft above the field",
            (t == "xing-runway-threshold") & alt_ok & c("apt_elev").isNotNull(),
            c("alt") - c("apt_elev") > 300,
        ),
        (
            "thr_no_landing",
            "a1.7",
            "threshold crossing with no landing time of the track in the 120 s after",
            t == "xing-runway-threshold",
            ~(
                c("aldt_ts").isNotNull()
                & (c("aldt_ts") >= c("ts"))
                & (c("aldt_ts") - c("ts") <= 120)
            ),
        ),
        (
            "thr_repeated",
            "a1.7",
            "more than one threshold crossing of the runway within 10 min",
            t == "xing-runway-threshold",
            c("same_rwy_600") > 1,
        ),
        (
            "acdm_elev",
            "a1.3",
            (
                "acdm take-off or landing whose altitude is not the field elevation plus 15 ft"
                " (within 20 ft)"
            ),
            isin("ATOT", "ALDT") & acdm & alt_ok & c("apt_elev").isNotNull(),
            F.abs(c("alt") - 15 - c("apt_elev")) > 20,
        ),
        (
            "legacy_first",
            "a1.4 f3.7",
            "legacy take-off at the track's first positioned report",
            (t == "ATOT") & legacy,
            F.abs(c("ts") - c("p0_ts")) <= 1,
        ),
        (
            "legacy_last",
            "a1.4 f3.7",
            "legacy landing at the track's last positioned report",
            (t == "ALDT") & legacy,
            F.abs(c("ts") - c("p1_ts")) <= 1,
        ),
        (
            "legacy_far",
            "a1.5",
            "legacy take-off or landing more than 2 NM from the airport and 500 ft above it",
            isin("ATOT", "ALDT")
            & legacy
            & at_rep
            & c("rep_lat").isNotNull()
            & c("apt_lat").isNotNull(),
            (_hav_nm(c("rep_lat"), c("rep_lon"), c("apt_lat"), c("apt_lon")) > 2)
            & (c("rep_alt_c_ft") - c("apt_elev") > 500),
        ),
        (
            "ga_long",
            "a3.G1",
            "go-around with an excursion longer than 600 s",
            t == "go-around",
            c("excursion_s") > 600,
        ),
        (
            "ga_far",
            "a3.G2",
            "go-around more than 3 NM from its airport",
            (t == "go-around") & has_pos & c("apt_lat").isNotNull(),
            _hav_nm(c("lat"), c("lon"), c("apt_lat"), c("apt_lon")) > 3,
        ),
        (
            "ga_after_to",
            "a3.G4",
            "go-around in the 10 min after a take-off of the track",
            t == "go-around",
            c("atot_ts").isNotNull() & (c("ts") - c("atot_ts")).between(0, 600),
        ),
        (
            "ga_slow",
            "a3.G3",
            "go-around slower than 30 kt at its report",
            (t == "go-around") & at_rep,
            kt < 30,
        ),
        (
            "not_ades",
            "a3.G5 e2.C6",
            (
                "go-around, holding or point-merge event at another airport than the flight list's"
                " destination"
            ),
            (isin("go-around") | t.startswith("pms-")) & c("apt").isNotNull(),
            ~c("apt").eqNullSafe(c("ades")),
        ),
        (
            "crossing_off_rwy",
            "a3.X1",
            "runway crossing whose position is not on the runway it names",
            t.startswith("runway-crossing") & c("thr_lat").isNotNull() & has_pos,
            (along < 0) | (along > c("rwy_len_nm")) | (cross > c("rwy_wid_nm") / 2),
        ),
        (
            "crossing_own_movement",
            "a3.X2 WP2.8",
            "runway crossing of the runway the track takes off from or lands on (within 180 s)",
            t.startswith("runway-crossing"),
            F.exists(c("mov"), lambda m: same_strip(m) & (F.abs(m["ts"] - c("ts")) <= 180)),
        ),
        (
            "crossing_frozen",
            "a3.X3",
            "runway crossing with a top speed of 0 kt (frozen position)",
            t.startswith("runway-crossing"),
            c("max_gs_kt") == 0,
        ),
        # ---- work package 3: block times and stands
        (
            "block_air",
            "b1.2",
            "block time on a report not flagged on the ground and faster than 80 kt",
            isin(*BLOCK) & at_rep,
            ~gnd & (kt > 80),
        ),
        (
            "block_fast",
            "b1.2",
            "block time on a report faster than 40 kt",
            isin(*BLOCK) & at_rep & c("rep_vel").isNotNull(),
            kt > 40,
        ),
        (
            "block_on_runway",
            "b1.2",
            (
                "block time on the runway: off-block between line-up and take-off, in-block between"
                " landing and runway-vacated"
            ),
            isin(*BLOCK),
            (
                (t == "AOBT")
                & c("atot_ts").isNotNull()
                & (c("ts") <= c("atot_ts"))
                & F.exists(c("lineup_ts"), lambda x: (x <= c("ts")) & (x >= c("atot_ts") - 900))
            )
            | (
                (t == "AIBT")
                & c("aldt_ts").isNotNull()
                & (c("ts") >= c("aldt_ts"))
                & F.exists(c("vacated_ts"), lambda x: (x >= c("ts")) & (x <= c("aldt_ts") + 900))
            ),
        ),
        (
            "block_near_movement",
            "b1.2",
            "off-block within 60 s before the take-off, in-block within 60 s after the landing",
            isin(*BLOCK),
            ((t == "AOBT") & (c("atot_ts") - c("ts")).between(0, 60))
            | ((t == "AIBT") & (c("ts") - c("aldt_ts")).between(0, 60)),
        ),
        (
            "block_vs_stand",
            "b1.1",
            (
                "off-block more than 60 s after its stand exit, in-block more than 60 s before its"
                " stand entry"
            ),
            ((t == "AOBT") & c("stand_exit_ts").isNotNull())
            | ((t == "AIBT") & c("stand_entry_ts").isNotNull()),
            ((t == "AOBT") & (c("ts") - c("stand_exit_ts") > 60))
            | ((t == "AIBT") & (c("stand_entry_ts") - c("ts") > 60)),
        ),
        (
            "block_taxi_long",
            "b1.4 f3.3",
            "taxi time over 90 min (off-block to take-off, landing to in-block)",
            ((t == "AOBT") & (c("atot_ts") >= c("ts")))
            | ((t == "AIBT") & (c("aldt_ts") <= c("ts"))),
            ((t == "AOBT") & (c("atot_ts") - c("ts") > 5400))
            | ((t == "AIBT") & (c("ts") - c("aldt_ts") > 5400)),
        ),
        ("block_no_stand", "b1.3", "block time naming no stand", isin(*BLOCK), c("stand").isNull()),
        (
            "block_none_stand",
            "f3.4 WP3.7",
            'block time naming the stand "None" (or "nan", empty)',
            isin(*BLOCK) & c("stand").isNotNull(),
            no_name,
        ),
        (
            "block_no_airport",
            "b1.3",
            "block time naming no airport",
            isin(*BLOCK),
            c("apt").isNull(),
        ),
        (
            "parking_none_ref",
            "f3.4 WP3.7",
            'stand passage whose stand is named "None" (or "nan", empty)',
            isin("entry-parking_position", "exit-parking_position"),
            F.coalesce(c("osm_ref"), F.lit("")).isin(*NO_NAME),
        ),
        (
            "block_no_leg",
            "b1.7 WP3.6",
            "in-block without the landing in info although the track has one (off-block: take-off)",
            ((t == "AIBT") & c("aldt_ts").isNotNull()) | ((t == "AOBT") & c("atot_ts").isNotNull()),
            ((t == "AIBT") & c("landing_ts").isNull())
            | ((t == "AOBT") & c("take_off_ts").isNull()),
        ),
        (
            "block_ground_only",
            "b1.8 WP6.2",
            "block time on a track with no take-off and no landing",
            isin(*BLOCK),
            c("atot_ts").isNull() & c("aldt_ts").isNull(),
        ),
        (
            "block_other_airport",
            "f3.2",
            "block time at another airport than the flight list's ADEP (AOBT) or ADES (AIBT)",
            isin(*BLOCK) & c("apt").isNotNull(),
            ((t == "AOBT") & ~c("apt").eqNullSafe(c("adep")))
            | ((t == "AIBT") & ~c("apt").eqNullSafe(c("ades"))),
        ),
        # ---- work package 5: crossings and distance
        (
            "bracket_fast",
            "d1.2 d2.C3 c1.4 WP5.1",
            "crossing whose positioned reports either side imply more than 1,000 kt",
            xing & ~(t == "xing-runway-threshold") & (c("np_ts") > c("pp_ts")),
            brk_kt > 1000,
        ),
        (
            "bracket_long",
            "c3.C d1.4 d2.C4",
            "crossing with a bracket longer than 300 s",
            xing & c("bracket_s").isNotNull(),
            c("bracket_s") > 300,
        ),
        (
            "ring_not_flight",
            "d1.1 f3.5 WP5.2",
            "ring naming an airport that is not the flight's ADEP or ADES",
            rings & c("apt").isNotNull(),
            ~(c("apt").eqNullSafe(c("adep")) | c("apt").eqNullSafe(c("ades"))),
        ),
        (
            "ring_not_flight_rwy",
            "d1.1",
            (
                "ring naming an airport that is not the flight's, on flights whose airport came"
                " from a runway event"
            ),
            rings
            & c("apt").isNotNull()
            & ((c("adep_source") == "runway_event") | (c("ades_source") == "runway_event")),
            ~(c("apt").eqNullSafe(c("adep")) | c("apt").eqNullSafe(c("ades"))),
        ),
        (
            "ring_odd_ident",
            "d1.5",
            "ring around an airport whose ident is not four letters (duplicate or local record)",
            rings & c("apt").isNotNull(),
            ~c("apt").rlike(r"^[A-Z]{4}$"),
        ),
        (
            "fir_vertical",
            "d2.C5",
            "FIR crossing between a lower and an upper region of one area",
            t == "xing-fir",
            F.substring(c("fir_from"), 1, 4) == F.substring(c("fir_to"), 1, 4),
        ),
        ("fir_high", "d2.C2", "FIR crossing above FL600", t == "xing-fir", c("fir_fl") > 600),
        (
            "no_distance",
            "d3.G WP5.6",
            "event without a distance measurement",
            any_event,
            c("d_val").isNull(),
        ),
        (
            "no_time",
            "d3.G WP5.6",
            "event without a time-passed measurement",
            any_event,
            c("t_val").isNull(),
        ),
        (
            "dist_before_pos",
            "d3.E",
            "distance given before the track's first positioned report",
            c("d_val").isNotNull() & c("p0_ts").isNotNull(),
            c("ts") < c("p0_ts"),
        ),
        (
            "dist_no_pos_track",
            "d3.E",
            "distance 0 on a track with no position at all",
            c("d_val").isNotNull() & (c("n_pos") == 0),
            c("d_val") == 0,
        ),
        (
            "stale_distance",
            "d3.B",
            (
                "event without a position more than 120 s after the last positioned report (its"
                " distance is stale)"
            ),
            ~has_pos & c("pp_ts").isNotNull() & c("d_val").isNotNull(),
            c("ts") - c("pp_ts") > 120,
        ),
        (
            "time_vs_first",
            "d3.D",
            (
                "time passed differs from the event time minus the track's first report by more"
                " than 60 s"
            ),
            c("t_val").isNotNull(),
            F.abs(c("t_val") - (c("ts") - c("t0"))) > 60,
        ),
        (
            "time_vs_first_pos",
            "d3.E WP5.7",
            (
                "time passed differs from the event time minus the first positioned report by more"
                " than 60 s"
            ),
            c("t_val").isNotNull() & c("p0_ts").isNotNull(),
            F.abs(c("t_val") - (c("ts") - c("p0_ts"))) > 60,
        ),
        (
            "last_seen_tail",
            "d2.C7 WP4.10",
            "last_seen more than 60 s after the last positioned report",
            (t == "last_seen") & c("p1_ts").isNotNull(),
            c("ts") - c("p1_ts") > 60,
        ),
        (
            "first_seen_lead",
            "d2.C8 WP4.10",
            "first_seen more than 60 s before the first positioned report",
            (t == "first_seen") & c("p0_ts").isNotNull(),
            c("p0_ts") - c("ts") > 60,
        ),
        (
            "seen_window_end",
            "d2.C9",
            "last_seen at the end of the reports read (the next day 23:59:59)",
            t == "last_seen",
            c("ts") >= F.lit(f"{DAY} 00:00:00").cast("timestamp").cast("double") + 2 * 86400 - 1,
        ),
        # ---- work package 6: surface events, holding, point merge
        (
            "layout_air",
            "b2.2 b3.1 WP6.1",
            (
                "surface event on a report not flagged on the ground and faster than 50 kt"
                " (reported or from positions)"
            ),
            layout & at_rep,
            ~gnd & ((kt > 50) | (pos_kt > 50)),
        ),
        (
            "layout_ground_only",
            "b2.4 b3.3 WP6.2",
            "surface event of a track that never leaves the ground",
            layout,
            c("any_air") == 0,
        ),
        (
            "layout_ground_only_vehicle",
            "b3.3 WP6.2",
            (
                "surface event of a track that never leaves the ground, with no aircraft type or a"
                " vehicle type"
            ),
            layout,
            (c("any_air") == 0)
            & (c("typecode").isNull() | c("typecode").isin("SERV", "GRND", "GND")),
        ),
        (
            "layout_at_edge",
            "b2.3 b3.9 WP6.1",
            "entry at the track's first report, exit at its last report",
            layout,
            (entry & (F.abs(c("ts") - c("t0")) <= 1)) | (exit_ & (F.abs(c("ts") - c("t1")) <= 1)),
        ),
        (
            "hold_arrival",
            "e1.1 WP6.3",
            "holding followed by a landing at the destination within 60 min and 150 NM",
            hold_end,
            arrival_hold,
        ),
        (
            "hold_further",
            "e1.1 WP6.3",
            "holding followed by a landing later or further away",
            hold_end,
            after_land & ~arrival_hold,
        ),
        ("hold_none", "e1.1 WP6.3", "holding not followed by a landing", hold_end, ~after_land),
        (
            "hold_racetrack_round",
            "e1.2",
            "holding labelled racetrack with under 15% straight reports",
            hold_end,
            (c("pattern") == "racetrack") & (c("straight_pct") < 15),
        ),
        (
            "hold_many",
            "e1.1",
            "holding on a track with three or more episodes",
            hold_end,
            c("n_hold") >= 3,
        ),
        ("hold_long", "e1.1", "holding longer than one hour", hold_end, c("i_duration_s") > 3600),
        (
            "hold_low",
            "e1.4",
            "holding less than 2,500 ft above the destination (or departure) field",
            isin("holding-start", "holding-end")
            & alt_ok
            & F.coalesce(c("ades_elev"), c("adep_elev")).isNotNull(),
            c("alt") - F.coalesce(c("ades_elev"), c("adep_elev")) < 2500,
        ),
        (
            "pms_exit_early",
            "e2.C1",
            "point-merge leg exit more than 15 s before the heading turns by 10 degrees",
            (t == "pms-leg-exit") & (c("turn_seen_s") > 15),
            c("turn_dt").isNull() | (c("turn_dt") > 15),
        ),
        (
            "pms_mp_no_leg_climb",
            "e2.C5",
            "merge point without a leg, climbing (more than 500 ft/min)",
            (t == "pms-merge-point") & (c("via_leg") == "false"),
            at_rep & (fpm > 500),
        ),
        (
            "pms_mp_no_leg_descent",
            "e2.C5",
            "merge point without a leg, descending (more than 500 ft/min)",
            (t == "pms-merge-point") & (c("via_leg") == "false"),
            at_rep & (fpm < -500),
        ),
        (
            "pms_mp_no_leg_level",
            "e2.C5",
            "merge point without a leg, level (within 500 ft/min)",
            (t == "pms-merge-point") & (c("via_leg") == "false"),
            at_rep & (F.abs(fpm) <= 500),
        ),
        (
            "pms_mp_after_to",
            "e2.C5",
            "merge point in the 20 min after a take-off of the track from the same airport",
            t == "pms-merge-point",
            (c("atot_apt") == c("apt")) & (c("ts") - c("atot_ts")).between(0, 1200),
        ),
        (
            "pms_mp_many",
            "e2.C5",
            "merge point on a track with more than one merge point",
            t == "pms-merge-point",
            c("n_pms_mp") > 1,
        ),
        (
            "pms_mp_low_slow",
            "e2.C5",
            "merge point less than 2,500 ft above the field or slower than 150 kt",
            (t == "pms-merge-point") & alt_ok & at_rep,
            (c("alt") - F.coalesce(c("apt_elev"), F.lit(0.0)) < 2500) | (kt < 150),
        ),
    ]


def count_event_counters(X, counters) -> list[tuple]:
    """``(counter, item, n, base)`` for every counter of :func:`event_counters`, per item."""
    from pyspark.sql import functions as F

    aggs = []
    for i, (name, _f, _m, base, odd) in enumerate(counters):
        b = F.coalesce(base, F.lit(False))
        o = b & F.coalesce(odd, F.lit(False))
        aggs += [F.sum(b.cast("int")).alias(f"b{i}"), F.sum(o.cast("int")).alias(f"n{i}")]
    out = []
    for r in X.groupBy("item").agg(*aggs).collect():
        for i, (name, *_rest) in enumerate(counters):
            if r[f"b{i}"]:
                out.append((name, r["item"], r[f"n{i}"], r[f"b{i}"]))
    return out


def count_other(fr: dict, inp: dict) -> list[tuple]:
    """The counters that need more than one event: pairs, sequences, passages, intervals."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    E, T, M = fr["E"], fr["T"], fr["M"]
    c = F.col
    out = []

    # Level segments: each level-start with the event that follows it (c1.6).
    lv = E.filter(c("type").isin(*LEVEL)).select("flight_id", "type", "ts")
    w = Window.partitionBy("flight_id").orderBy(
        "ts", F.when(c("type") == "level-start", 0).otherwise(1)
    )
    lv = lv.withColumn("_nt", F.lead("type").over(w)).withColumn("_nts", F.lead("ts").over(w))
    seg = lv.filter((c("type") == "level-start") & (c("_nt") == "level-end")).withColumn(
        "_d", c("_nts") - c("ts")
    )
    r = seg.agg(
        F.count("*").alias("b"),
        F.sum((c("_d") <= 20).cast("int")).alias("n20"),
        F.sum((c("_d") == 0).cast("int")).alias("n0"),
    ).first()
    out += [
        ("level_20s", "level-start", r["n20"], r["b"]),
        ("level_0s", "level-start", r["n0"], r["b"]),
    ]

    # Surface passages: an entry with its exit on the same element and the
    # time in use between them (b2.1), and elements passed five times (b3.4).
    L = E.filter(c("type").rlike(LAYOUT_RE)).select(
        "flight_id",
        "ts",
        "osm_id",
        "tiu",
        F.regexp_extract("type", LAYOUT_RE, 1).alias("_side"),
        F.regexp_extract("type", LAYOUT_RE, 2).alias("_kind"),
    )
    en = L.filter(c("_side") == "entry").withColumn("_k", F.round(c("ts") + c("tiu")))
    ex = L.filter(c("_side") == "exit").withColumn("_k", F.round(c("ts")))
    key = ["flight_id", "osm_id", "_kind", "_k"]
    no_exit = en.join(ex.select(*key).distinct(), key, "left_anti").groupBy("_kind").count()
    en2 = en.withColumn("_k", F.round(c("ts")))
    ex2 = ex.withColumn("_k", F.round(c("ts") - c("tiu")))
    no_entry = ex2.join(en2.select(*key).distinct(), key, "left_anti").groupBy("_kind").count()
    n_en = {r["_kind"]: r["count"] for r in en.groupBy("_kind").count().collect()}
    n_ex = {r["_kind"]: r["count"] for r in ex.groupBy("_kind").count().collect()}
    for r in no_exit.collect():
        out.append(("layout_no_exit", f"entry-{r['_kind']}", r["count"], n_en.get(r["_kind"], 0)))
    for r in no_entry.collect():
        out.append(("layout_no_entry", f"exit-{r['_kind']}", r["count"], n_ex.get(r["_kind"], 0)))
    for k in n_en:
        if not any(o[0] == "layout_no_exit" and o[1] == f"entry-{k}" for o in out):
            out.append(("layout_no_exit", f"entry-{k}", 0, n_en[k]))
    for k in n_ex:
        if not any(o[0] == "layout_no_entry" and o[1] == f"exit-{k}" for o in out):
            out.append(("layout_no_entry", f"exit-{k}", 0, n_ex[k]))
    per = en.groupBy("flight_id", "osm_id", "_kind").count()
    for r in (
        per.groupBy("_kind")
        .agg(F.count("*").alias("b"), F.sum((c("count") >= 5).cast("int")).alias("n"))
        .collect()
    ):
        out.append(("layout_5_passages", f"element ({r['_kind']})", r["n"], r["b"]))

    # Go-arounds with a ground report inside the excursion (a3.G1).
    ga = E.filter(c("type") == "go-around").select("_eid", "flight_id", "ts", "excursion_s")
    j = F.broadcast(ga).join(
        T.withColumnRenamed("track_id", "flight_id").withColumnRenamed("ts", "rts"), "flight_id"
    )
    j = j.filter(F.abs(c("rts") - c("ts")) <= F.coalesce(c("excursion_s"), F.lit(0.0)))
    touch = c("on_ground") | (c("baro_altitude_c").isNull() & (c("velocity") < 25))
    hit = j.groupBy("_eid").agg(F.max(F.coalesce(touch, F.lit(False)).cast("int")).alias("_t"))
    r = (
        ga.join(hit, "_eid", "left")
        .agg(F.count("*").alias("b"), F.sum(F.coalesce(c("_t"), F.lit(0))).alias("n"))
        .first()
    )
    out.append(("ga_ground", "go-around", r["n"] or 0, r["b"]))

    # Distance along the events of each flight (d3.A, d3.C).
    d = fr["X"].filter(c("d_val").isNotNull()).select("flight_id", "ts", "d_val")
    w = Window.partitionBy("flight_id").orderBy("ts", "d_val")
    d = (
        d.withColumn("_pd", F.lag("d_val").over(w))
        .withColumn("_pt", F.lag("ts").over(w))
        .filter(c("_pd").isNotNull())
    )
    step = c("d_val") - c("_pd")
    dt = c("ts") - c("_pt")
    r = d.agg(
        F.count("*").alias("b"),
        F.sum((step < -0.05).cast("int")).alias("down"),
        F.sum(((step > 20) & ((dt <= 0) | (step / dt * 3600 > 1000))).cast("int")).alias("fast"),
        F.countDistinct(F.when(step < -0.05, c("flight_id"))).alias("down_f"),
    ).first()
    nf = d.select("flight_id").distinct().count()
    out += [
        ("dist_down", "all events", r["down"], r["b"]),
        ("dist_down_flights", "flights", r["down_f"], nf),
        ("dist_fast", "all events", r["fast"], r["b"]),
    ]

    # Measurement rows with no value (WP5.6).
    for r in (
        M.groupBy("type")
        .agg(F.count("*").alias("b"), F.sum(c("value").isNull().cast("int")).alias("n"))
        .collect()
    ):
        out.append(("meas_null", r["type"], r["n"], r["b"]))

    # Point-merge legs split by a gap: the next leg of the same system starts
    # within 180 s of the exit at about the same distance (e2.C2).
    legs = E.filter(c("type").isin("pms-leg-entry", "pms-leg-exit")).select(
        "flight_id", "type", "ts", "pms_id", "leg_seq", "leg_dist"
    )
    ex = legs.filter(c("type") == "pms-leg-exit").alias("x")
    en = legs.filter(c("type") == "pms-leg-entry").select(
        "flight_id",
        "pms_id",
        (c("leg_seq") - 1).alias("leg_seq"),
        c("ts").alias("_ets"),
        c("leg_dist").alias("_ed"),
    )
    jj = ex.join(en, ["flight_id", "pms_id", "leg_seq"], "left").withColumn(
        "_split",
        ((c("_ets") - c("ts")).between(0, 180) & (F.abs(c("_ed") - c("leg_dist")) <= 1)).cast(
            "int"
        ),
    )
    r = (
        jj.groupBy("flight_id", "pms_id", "leg_seq")
        .agg(F.max(F.coalesce("_split", F.lit(0))).alias("_s"))
        .agg(F.count("*").alias("b"), F.sum("_s").alias("n"))
        .first()
    )
    out.append(("pms_split", "pms-leg-exit", r["n"] or 0, r["b"]))

    # Info fields added by the corrections, wherever they appear.
    for field, value in INFO_FLAGS:
        v = _info(field)
        df = E.filter(v.isNotNull()).groupBy("item", v.alias("_v")).count()
        if value is not None:
            df = df.filter(c("_v") == value)
        base = {
            r["item"]: r["count"]
            for r in (
                E.filter(
                    c("item").isin(
                        [r2["item"] for r2 in df.select("item").distinct().collect()] or ["-"]
                    )
                )
                .groupBy("item")
                .count()
                .collect()
            )
        }
        for r in df.collect():
            out.append((f"info_{field}_{r['_v']}", r["item"], r["count"], base.get(r["item"], 0)))
    return out


def count_flight_list(fr: dict, inp: dict) -> list[tuple]:
    """The flight-list counters: work package 4, and the list's block, ring and runway columns."""
    from pyspark.sql import functions as F

    c = F.col
    FLX, S, TE, apt = fr["FLX"], fr["S"], fr["TE"], fr["apt"]
    dup = apt.filter(F.lower(c("name")).contains("duplicate")).select(c("ident").alias("_dup"))
    dup_ids = [r["_dup"] for r in dup.collect()]
    f = FLX.join(S.withColumnRenamed("track_id", "flight_id"), "flight_id", "left").join(
        TE, "flight_id", "left"
    )
    win = (
        fr["win"]
        .filter(c("_fl").isNotNull())
        .select(
            F.split("_fl", r"\|").getItem(0).alias("flight_id"),
            F.split("_fl", r"\|").getItem(1).alias("_side"),
            "hdg_fast",
            "hdg_n",
        )
    )
    wd = win.filter(c("_side") == "dep").select(
        "flight_id", c("hdg_fast").alias("_hd"), c("hdg_n").alias("_hdn")
    )
    wa = win.filter(c("_side") == "arr").select(
        "flight_id", c("hdg_fast").alias("_ha"), c("hdg_n").alias("_han")
    )
    f = f.join(wd, "flight_id", "left").join(wa, "flight_id", "left")
    both = c("adep").isNotNull() & c("ades").isNotNull()
    same = both & (c("adep") == c("ades"))
    from_rwy = (c("adep_source") == "runway_event") | (c("ades_source") == "runway_event")
    d_first = _hav_nm(c("p0_lat"), c("p0_lon"), c("adep_lat"), c("adep_lon"))
    d_last = _hav_nm(c("p1_lat"), c("p1_lon"), c("ades_lat"), c("ades_lon"))
    lit = F.lit(True)
    specs = [
        ("fl_same", "f1.1 f2.F", "departure equal to destination", both, same),
        (
            "fl_same_rwy",
            "f1.1",
            "departure equal to destination, an airport from a runway event",
            both & from_rwy,
            same,
        ),
        (
            "fl_same_land_first",
            "f1.1 f2.F",
            "departure equal to destination, landing before take-off",
            same,
            c("fl_aldt") < c("fl_atot"),
        ),
        (
            "fl_same_to_first",
            "f1.1",
            "departure equal to destination, take-off before landing",
            same,
            c("fl_atot") <= c("fl_aldt"),
        ),
        (
            "fl_same_one_missing",
            "f1.1",
            "departure equal to destination, take-off or landing time missing",
            same,
            c("fl_atot").isNull() | c("fl_aldt").isNull(),
        ),
        (
            "fl_land_before_to",
            "f3.2 f2.F",
            "landing time before take-off time",
            c("fl_atot").isNotNull() & c("fl_aldt").isNotNull(),
            c("fl_aldt") < c("fl_atot"),
        ),
        (
            "fl_aobt_after_atot",
            "f3.2",
            "AOBT after ATOT",
            c("fl_aobt").isNotNull() & c("fl_atot").isNotNull(),
            c("fl_aobt") > c("fl_atot"),
        ),
        (
            "fl_aibt_before_aldt",
            "f3.2",
            "AIBT before ALDT",
            c("fl_aibt").isNotNull() & c("fl_aldt").isNotNull(),
            c("fl_aibt") < c("fl_aldt"),
        ),
        (
            "fl_taxi_long",
            "f3.3",
            "taxi-out or taxi-in over 90 min",
            (c("fl_aobt").isNotNull() & c("fl_atot").isNotNull())
            | (c("fl_aibt").isNotNull() & c("fl_aldt").isNotNull()),
            (c("fl_atot") - c("fl_aobt") > 5400) | (c("fl_aibt") - c("fl_aldt") > 5400),
        ),
        (
            "fl_dup_airport",
            "f1.8 d1.5 WP4.5",
            'an airport named "(Duplicate)"',
            c("adep").isNotNull() | c("ades").isNotNull(),
            c("adep").isin(*dup_ids or ["-"]) | c("ades").isin(*dup_ids or ["-"]),
        ),
        (
            "fl_cs_spaces",
            "f2.H WP4.6",
            "call sign with trailing spaces",
            c("flt_id").isNotNull(),
            c("flt_id") != F.trim(c("flt_id")),
        ),
        (
            "fl_sup_dep_noapt",
            "f2.B WP4.7",
            "superseded departure flag on a row without ADEP",
            c("sup_dep") == True,  # noqa: E712
            c("adep").isNull(),
        ),
        (
            "fl_sup_arr_noapt",
            "f2.B WP4.7",
            "superseded arrival flag on a row without ADES",
            c("sup_arr") == True,  # noqa: E712
            c("ades").isNull(),
        ),
        (
            "fl_sup_flew",
            "f2.C WP4.7",
            "superseded departure on a track more than 1,000 ft above its ADEP",
            c("sup_dep").eqNullSafe(True)
            & c("adep").isNotNull()
            & c("adep_elev").isNotNull(),  # noqa: E712
            c("max_h_ft") - c("adep_elev") > 1000,
        ),
        (
            "fl_ghost_dep",
            "f2.E WP4.7",
            "ADEP not counted as a movement although the track climbed above 10,000 ft",
            c("adep").isNotNull() & (c("sup_dep") == False),  # noqa: E712
            c("mov_dep").eqNullSafe(False) & (c("max_h_ft") > 10000),
        ),  # noqa: E712
        (
            "fl_first_seen",
            "d2.C8 f2.A WP4.10",
            "first_seen differs from the first_seen event (more than 1 s)",
            c("fs").isNotNull() & c("fs_ev").isNotNull(),
            F.abs(c("fs") - c("fs_ev")) > 1,
        ),
        (
            "fl_last_seen",
            "d2.C8 f2.A WP4.10",
            "last_seen differs from the last_seen event (more than 1 s)",
            c("ls").isNotNull() & c("ls_ev").isNotNull(),
            F.abs(c("ls") - c("ls_ev")) > 1,
        ),
        (
            "fl_last_seen_60",
            "d2.C8 f2.A WP4.10",
            "last_seen differs from the last_seen event by more than 60 s",
            c("ls").isNotNull() & c("ls_ev").isNotNull(),
            F.abs(c("ls") - c("ls_ev")) > 60,
        ),
        (
            "fl_adep_rwy_far",
            "f1.1 WP4.1",
            "ADEP from a runway event more than 30 NM from the first positioned report",
            (c("adep_source") == "runway_event")
            & c("p0_lat").isNotNull()
            & c("adep_lat").isNotNull(),
            d_first > 30,
        ),
        (
            "fl_ades_rwy_far",
            "f1.1 WP4.1",
            "ADES from a runway event more than 30 NM from the last positioned report",
            (c("ades_source") == "runway_event")
            & c("p1_lat").isNotNull()
            & c("ades_lat").isNotNull(),
            d_last > 30,
        ),
        (
            "fl_ades_far",
            "f1.2 WP4.2",
            "ADES more than 30 NM from the last positioned report",
            c("ades").isNotNull() & c("p1_lat").isNotNull() & c("ades_lat").isNotNull(),
            d_last > 30,
        ),
        (
            "fl_adep_far",
            "f1.2",
            "ADEP more than 30 NM from the first positioned report",
            c("adep").isNotNull() & c("p0_lat").isNotNull() & c("adep_lat").isNotNull(),
            d_first > 30,
        ),
        (
            "fl_adep_legacy_only",
            "f1.4 WP4.1",
            (
                "ADEP from a runway event that rests on a legacy take-off only (no line-up, no"
                " take-off-roll)"
            ),
            c("adep_source") == "runway_event",
            (c("atot_method") == "legacy") & (F.coalesce(c("n_roll"), F.lit(0)) == 0),
        ),
        (
            "fl_ades_legacy_only",
            "f1.4 WP4.1",
            "ADES from a runway event that rests on a legacy landing only (no runway-vacated)",
            c("ades_source") == "runway_event",
            (c("aldt_method") == "legacy") & (F.coalesce(c("n_vacated"), F.lit(0)) == 0),
        ),
        (
            "fl_short_arrival_dep",
            "f1.6 WP4.4",
            "ADEP equal to ADES on a short arrival fragment (no take-off, a landing, under 20 min)",
            both,
            same & c("fl_atot").isNull() & c("fl_aldt").isNotNull() & (c("dur_min") < 20),
        ),
        (
            "fl_no_position",
            "f2.D",
            "row of a track with no position at all",
            lit,
            F.coalesce(c("n_pos"), F.lit(0)) == 0,
        ),
        (
            "fl_ring_wrong_side",
            "f3.6 WP4.9",
            "C40_DEP before ATOT or C40_ARR after ALDT",
            (c("c40_dep").isNotNull() & c("fl_atot").isNotNull())
            | (c("c40_arr").isNotNull() & c("fl_aldt").isNotNull()),
            (c("c40_dep") < c("fl_atot")) | (c("c40_arr") > c("fl_aldt")),
        ),
        (
            "fl_stand_none",
            "f3.4 WP3.7",
            'STND_DEP or STND_ARR named "None" (or "nan", empty)',
            c("stnd_dep").isNotNull() | c("stnd_arr").isNotNull(),
            F.coalesce(c("stnd_dep"), F.lit("x")).isin(*NO_NAME)
            | F.coalesce(c("stnd_arr"), F.lit("x")).isin(*NO_NAME),
        ),
        (
            "fl_rwy_dep_heading",
            "f3.1 WP2.2",
            "RWY_DEP against the heading in the 30 s around ATOT",
            c("rwy_dep_brg").isNotNull() & (c("_hdn") > 0),
            _angdiff(c("_hd"), c("rwy_dep_brg")) > 90,
        ),
        (
            "fl_rwy_arr_heading",
            "f3.1 WP2.2",
            "RWY_ARR against the heading in the 30 s around ALDT",
            c("rwy_arr_brg").isNotNull() & (c("_han") > 0),
            _angdiff(c("_ha"), c("rwy_arr_brg")) > 90,
        ),
        (
            "fl_adep_p_far",
            "f1.10 WP4.11",
            "ADEP_P names an airport more than 30 NM from the first positioned report",
            F.coalesce(c("adep_p"), F.lit("")) != "",
            c("_adep_p_far") > 0,
        ),
    ]
    # ADEP_P entries farther than 30 NM from the first positioned report.
    ex = f.select(
        "flight_id",
        "p0_lat",
        "p0_lon",
        F.explode(F.split(F.coalesce(c("adep_p"), F.lit("")), r",\s*")).alias("ident"),
    )
    ex = ex.filter(c("ident") != "").join(
        F.broadcast(apt.select("ident", "a_lat", "a_lon")), "ident", "left"
    )
    far = ex.groupBy("flight_id").agg(
        F.sum((_hav_nm(c("p0_lat"), c("p0_lon"), c("a_lat"), c("a_lon")) > 30).cast("int")).alias(
            "_adep_p_far"
        )
    )
    f = f.join(far, "flight_id", "left")
    aggs = []
    for i, (_n, _f, _m, base, odd) in enumerate(specs):
        b = F.coalesce(base, F.lit(False))
        aggs += [
            F.sum(b.cast("int")).alias(f"b{i}"),
            F.sum((b & F.coalesce(odd, F.lit(False))).cast("int")).alias(f"n{i}"),
        ]
    r = f.agg(*aggs).first()
    out = [(s[0], "flight list", r[f"n{i}"], r[f"b{i}"]) for i, s in enumerate(specs)]
    # By source pair, departure equal to destination.
    for rr in (
        f.filter(both)
        .groupBy("adep_source", "ades_source")
        .agg(F.count("*").alias("b"), F.sum(same.cast("int")).alias("n"))
        .collect()
    ):
        out.append(
            (
                "fl_same_by_source",
                f"flight list ({rr['adep_source']} / {rr['ades_source']})",
                rr["n"],
                rr["b"],
            )
        )
    # Rows with no day, no address, no first time (f2.G).
    fa = inp["flight_list_all"]
    day = F.lit(DAY).cast("date")
    r = (
        fa.filter((c("DOF") == day) | c("DOF").isNull())
        .agg(
            F.count("*").alias("b"),
            F.sum(c("DOF").isNull().cast("int")).alias("nd"),
            F.sum(c("ICAO24").isNull().cast("int")).alias("ni"),
            F.sum(c("FIRST_SEEN").isNull().cast("int")).alias("nf"),
        )
        .first()
    )
    out += [
        ("fl_null_day", "flight list", r["nd"], r["b"]),
        ("fl_null_icao24", "flight list", r["ni"], r["b"]),
        ("fl_null_first_seen", "flight list", r["nf"], r["b"]),
    ]
    # Tracks with events but no flight-list row.
    ev_ids = fr["E"].select("flight_id").distinct()
    r = (
        ev_ids.join(FLX.select("flight_id", F.lit(1).alias("_in")), "flight_id", "left")
        .agg(F.count("*").alias("b"), F.sum(c("_in").isNull().cast("int")).alias("n"))
        .first()
    )
    out.append(("ev_no_fl_row", "tracks with events", r["n"], r["b"]))
    # The unscheduled-airport penalty at an airport where the aircraft stands (f1.7).
    cand = inp["candidates"]
    if "role" in cand.columns:
        from pyspark.sql import Window

        w = Window.partitionBy("track_id", "role").orderBy(c("dist_nm").asc())
        near = cand.withColumn("_r", F.row_number().over(w)).filter(c("_r") == 1)
        near = near.filter(
            (c("apt_scheduled") == "no")
            & (F.coalesce(c("on_ground"), F.lit(False)) | (c("dist_nm") <= 2))
        )
        near = near.join(FLX.select(c("flight_id").alias("track_id"), "adep", "ades"), "track_id")
        for role, side in (("adep", "adep"), ("ades", "ades")):
            nr = near.filter(F.lower(c("role")).contains(role))
            r = nr.agg(
                F.count("*").alias("b"),
                F.sum((~c("apt_ident").eqNullSafe(c(side))).cast("int")).alias("n"),
            ).first()
            out.append((f"fl_unscheduled_{role}", "flight list", r["n"] or 0, r["b"]))
    return out, {s[0]: (s[1], s[2]) for s in specs}


#: Words and findings of the counters of :func:`count_other` and :func:`count_flight_list`
#: that are not in their own lists.
OTHER_TEXT = {
    "level_20s": ("c1.6 WP1.5", "level segment of 20 s or less"),
    "level_0s": ("c1.6 WP1.5", "level segment of 0 s (one report)"),
    "layout_no_exit": ("b2.1 WP6.1", "entry without an exit on the same element"),
    "layout_no_entry": ("b2.1 WP6.1", "exit without an entry on the same element"),
    "layout_5_passages": ("b3.4 WP6.1", "element passed five times or more by one track"),
    "ga_ground": (
        "a3.G1 WP2.7",
        (
            "go-around with a ground report (flagged, or slower than 50 kt without altitude) in the"
            " excursion"
        ),
    ),
    "dist_down": ("d3.C WP5.5", "distance lower than at the previous event (by more than 0.05 NM)"),
    "dist_down_flights": ("d3.C", "flight whose distance goes down between two events"),
    "dist_fast": (
        "d3.A d3.B WP5.5",
        "distance step of more than 20 NM implying more than 1,000 kt",
    ),
    "meas_null": ("c2.5 WP5.6", "measurement row with no value"),
    "pms_split": (
        "e2.C2 WP6.4",
        (
            "leg followed within 180 s by the next leg of the same system at the same distance (a"
            " split leg)"
        ),
    ),
    "fl_same_by_source": ("f1.1", "departure equal to destination, by ADEP / ADES source"),
    "fl_null_day": ("f2.G WP4.8", "row with a NULL day (DOF), among the day's rows and those"),
    "fl_null_icao24": ("f2.G WP4.8", "row with no ICAO24"),
    "fl_null_first_seen": ("f2.G", "row with no FIRST_SEEN"),
    "ev_no_fl_row": ("hint 4", "track with events but no flight-list row"),
    "fl_unscheduled_adep": (
        "f1.7 WP4.3",
        (
            "nearest airport to the first report is unscheduled, aircraft on the ground or within 2"
            " NM, not named ADEP"
        ),
    ),
    "fl_unscheduled_ades": (
        "f1.7 WP4.3",
        (
            "nearest airport to the last report is unscheduled, aircraft on the ground or within 2"
            " NM, not named ADES"
        ),
    ),
}


# --------------------------------------------------------------------------
# score: section i, against airport records
# --------------------------------------------------------------------------


#: item -> (event condition, record kind, side of the flight, one event per record).
def record_items():
    from pyspark.sql import functions as F

    t, m, d = F.col("type"), F.col("method"), F.col("direction")
    return {
        "AOBT": (t == "AOBT", "AOBT", "dep", False),
        "AIBT": (t == "AIBT", "AIBT", "arr", False),
        "ATOT": (t == "ATOT", "ATOT", "dep", False),
        "ATOT (acdm)": ((t == "ATOT") & (m == "acdm"), "ATOT", "dep", False),
        "ATOT (legacy)": ((t == "ATOT") & (m == "legacy"), "ATOT", "dep", False),
        "ALDT": (t == "ALDT", "ALDT", "arr", False),
        "ALDT (acdm)": ((t == "ALDT") & (m == "acdm"), "ALDT", "arr", False),
        "ALDT (legacy)": ((t == "ALDT") & (m == "legacy"), "ALDT", "arr", False),
        "take-off-roll (vs take-off record)": (t == "take-off-roll", "ATOT", "dep", False),
        "line-up (vs take-off record)": (t == "line-up", "ATOT", "dep", False),
        "runway-vacated (vs landing record)": (t == "runway-vacated", "ALDT", "arr", False),
        "xing-runway-threshold (vs landing record)": (
            t == "xing-runway-threshold",
            "ALDT",
            "arr",
            False,
        ),
        "xing-40nm inbound": ((t == "xing-40nm") & (d == "inbound"), "C40_ARR", "arr", False),
        "xing-100nm inbound": ((t == "xing-100nm") & (d == "inbound"), "C100_ARR", "arr", False),
        "xing-40nm outbound": ((t == "xing-40nm") & (d == "outbound"), "C40_DEP", "dep", False),
        "xing-100nm outbound": ((t == "xing-100nm") & (d == "outbound"), "C100_DEP", "dep", False),
        "exit-parking_position (vs off-block record, nearest per record)": (
            t == "exit-parking_position",
            "AOBT",
            "dep",
            True,
        ),
        "entry-parking_position (vs in-block record, nearest per record)": (
            t == "entry-parking_position",
            "AIBT",
            "arr",
            True,
        ),
    }


def airport_records(spark):
    """The airport records of the month in long form: one row per (movement, kind of time)."""
    import events_gt
    from pyspark.sql import functions as F

    b = events_gt.bridge(
        events_gt.load_apdf(spark, MONTH, full=False), events_gt.load_flights(spark, MONTH)
    )
    b = b.filter(
        F.col("icao24").isNotNull() & (F.to_date("MVT_TIME_UTC") == F.lit(DAY).cast("date"))
    )
    kinds = {
        "AOBT": ("DEP", "BLOCK_TIME_UTC"),
        "ATOT": ("DEP", "MVT_TIME_UTC"),
        "ALDT": ("ARR", "MVT_TIME_UTC"),
        "AIBT": ("ARR", "BLOCK_TIME_UTC"),
        "C40_DEP": ("DEP", "C40_CROSS_TIME"),
        "C100_DEP": ("DEP", "C100_CROSS_TIME"),
        "C40_ARR": ("ARR", "C40_CROSS_TIME"),
        "C100_ARR": ("ARR", "C100_CROSS_TIME"),
    }
    parts = []
    for kind, (phase, col) in kinds.items():
        parts.append(
            b.filter((F.col("SRC_PHASE") == phase) & F.col(col).isNotNull()).select(
                F.lit(kind).alias("kind"),
                F.col("icao24").alias("_a"),
                F.col("callsign").alias("_c"),
                F.when(F.lit(phase) == "DEP", F.col("ADEP_ICAO"))
                .otherwise(F.col("ADES_ICAO"))
                .alias("rec_apt"),
                F.col(col).cast("double").alias("rec_s"),
                (F.second(F.col(col)) != 0).alias("subminute"),
                F.col("AP_C_RWY").alias("rec_rwy"),
                F.col("AP_C_STND").alias("rec_stand"),
            )
        )
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out


def match_records(spark, fr: dict):
    """Events of the record items, each with the nearest record of its flight, airport and kind.

    Records are looked for within 6 h.

    Returns ``(matched pandas frame, events per item, records per item,
    records found per item, flight-list rows with their records)``.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    c = F.col
    E, FLX = fr["E"], fr["FLX"]
    recs = airport_records(spark)
    # The scope's records: those whose aircraft and call sign the scope's
    # flight list holds within 6 h (the sample holds every matched track).
    pop = (
        recs.join(
            FLX.select(c("icao24").alias("_a"), c("cs").alias("_c"), "fs", "ls"), ["_a", "_c"]
        )
        .filter(c("rec_s").between(c("fs") - RECORD_SEARCH_S, c("ls") + RECORD_SEARCH_S))
        .select("kind", "_a", "_c", "rec_apt", "rec_s", "subminute", "rec_rwy", "rec_stand")
        .dropDuplicates(["kind", "_a", "_c", "rec_apt", "rec_s"])
        .persist()
    )
    n_rec = {r["kind"]: r["count"] for r in pop.groupBy("kind").count().collect()}
    items = record_items()
    parts = []
    for item, (cond, kind, side, _per_rec) in items.items():
        parts.append(
            E.filter(cond).select(
                F.lit(item).alias("item"),
                F.lit(kind).alias("kind"),
                F.lit(side).alias("side"),
                "_eid",
                "id",
                "flight_id",
                "type",
                "ts",
                "apt",
                "runway",
                "stand",
                "info",
                "lat",
                "lon",
                "alt",
            )
        )
    ev = parts[0]
    for p in parts[1:]:
        ev = ev.unionByName(p)
    n_ev = {r["item"]: r["count"] for r in ev.groupBy("item").count().collect()}
    ev = ev.join(
        FLX.select("flight_id", c("icao24").alias("_a"), c("cs").alias("_c"), "adep", "ades"),
        "flight_id",
        "left",
    )
    ev = ev.withColumn(
        "_apt", F.coalesce(c("apt"), F.when(c("side") == "dep", c("adep")).otherwise(c("ades")))
    )
    j = ev.join(pop, ["kind", "_a", "_c"]).filter(
        (c("rec_apt") == c("_apt")) & (F.abs(c("ts") - c("rec_s")) <= RECORD_SEARCH_S)
    )
    j = j.withColumn("diff_s", c("ts") - c("rec_s"))
    w = Window.partitionBy("item", "_eid").orderBy(F.abs("diff_s"), "rec_s")
    j = j.withColumn("_r", F.row_number().over(w)).filter(c("_r") == 1).drop("_r")
    per_record = [i for i, v in items.items() if v[3]]
    wr = Window.partitionBy("item", "kind", "_a", "_c", "rec_apt", "rec_s").orderBy(
        F.abs("diff_s"), "ts", "id"
    )
    j = j.withColumn("_rr", F.row_number().over(wr))
    j = j.filter(~c("item").isin(per_record) | (c("_rr") == 1)).drop("_rr")
    matched = j.select(
        "item",
        "kind",
        "id",
        "flight_id",
        "type",
        "ts",
        "rec_s",
        "diff_s",
        "subminute",
        "rec_apt",
        "runway",
        "rec_rwy",
        "stand",
        "rec_stand",
        "info",
        "lat",
        "lon",
        "_a",
        "_c",
    ).toPandas()
    found = (
        matched.drop_duplicates(["item", "_a", "_c", "rec_apt", "rec_s"])
        .groupby("item")
        .size()
        .to_dict()
    )
    records = {i: n_rec.get(v[1], 0) for i, v in items.items()}
    # The flight list's runway and stand against the records of its own airports.
    flr = []
    for kind, side, anchor, rwy, stand in (
        ("ATOT", "adep", F.coalesce("fl_atot", "fl_aobt", "fs"), "rwy_dep", "stnd_dep"),
        ("ALDT", "ades", F.coalesce("fl_aldt", "fl_aibt", "ls"), "rwy_arr", "stnd_arr"),
    ):
        f = FLX.select(
            "flight_id",
            c("icao24").alias("_a"),
            c("cs").alias("_c"),
            c(side).alias("_apt"),
            anchor.alias("_t"),
            c(rwy).alias("named_rwy"),
            c(stand).alias("named_stand"),
        )
        jj = f.join(pop.filter(c("kind") == kind), ["_a", "_c"]).filter(
            (c("rec_apt") == c("_apt")) & (F.abs(c("_t") - c("rec_s")) <= RECORD_SEARCH_S)
        )
        ww = Window.partitionBy("flight_id").orderBy(F.abs(c("_t") - c("rec_s")))
        jj = jj.withColumn("_r", F.row_number().over(ww)).filter(c("_r") == 1)
        flr.append(
            jj.select(
                F.lit(kind).alias("kind"),
                "flight_id",
                "named_rwy",
                "rec_rwy",
                "named_stand",
                "rec_stand",
            ).toPandas()
        )
    pop.unpersist()
    import pandas as pd

    return matched, n_ev, records, found, pd.concat(flr, ignore_index=True)


def section_i(matched, n_ev, records, found, flr) -> list[dict]:
    """Section i rows: times against the records, runways and stands against the records."""
    import pandas as pd

    rows = record_lines(matched, n_ev, records, found, "a1 b1 d1 f3.8")
    cats = ("exact", "reciprocal", "other", "none named", "no record runway")
    rw = []
    for item, src in (("ATOT", "ATOT"), ("ALDT", "ALDT")):
        m = matched[matched["item"] == src]
        rw.append(
            pd.DataFrame(
                {
                    "item": (
                        f"{item} event runway (departures)"
                        if item == "ATOT"
                        else f"{item} event runway (arrivals)"
                    ),
                    "cat": [runway_agreement(a, b) for a, b in zip(m["runway"], m["rec_rwy"])],
                }
            )
        )
    for kind, label in (("ATOT", "RWY_DEP (flight list)"), ("ALDT", "RWY_ARR (flight list)")):
        m = flr[flr["kind"] == kind]
        rw.append(
            pd.DataFrame(
                {
                    "item": label,
                    "cat": [runway_agreement(a, b) for a, b in zip(m["named_rwy"], m["rec_rwy"])],
                }
            )
        )
    rw = pd.concat(rw, ignore_index=True)
    rows += category_lines("i", rw, "item", "cat", cats, "runway ", "a1.1 a2.C1 f3.1")
    st = []
    for item, src in (("AOBT", "AOBT"), ("AIBT", "AIBT")):
        m = matched[matched["item"] == src]
        st.append(
            pd.DataFrame(
                {
                    "item": f"{item} event stand",
                    "cat": [stand_agreement(a, b) for a, b in zip(m["stand"], m["rec_stand"])],
                }
            )
        )
    for kind, label in (("ATOT", "STND_DEP (flight list)"), ("ALDT", "STND_ARR (flight list)")):
        m = flr[flr["kind"] == kind]
        st.append(
            pd.DataFrame(
                {
                    "item": label,
                    "cat": [
                        stand_agreement(a, b) for a, b in zip(m["named_stand"], m["rec_stand"])
                    ],
                }
            )
        )
    st = pd.concat(st, ignore_index=True)
    rows += category_lines(
        "i",
        st,
        "item",
        "cat",
        ("equal", "differs", "none named", "no record stand"),
        "stand ",
        "b1.5 f3.4",
    )
    return rows


# --------------------------------------------------------------------------
# score: section ii, against Network Manager
# --------------------------------------------------------------------------


def nm_frame(spark, fr: dict):
    """Flight-list rows with their Network Manager flight.

    Same aircraft and call sign, the nearest off-block time within 6 h.
    """
    import events_gt
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    c = F.col
    FLX = fr["FLX"]
    nm = (
        events_gt.load_flights(spark, MONTH)
        .select(
            F.lower("AIRCRAFT_ADDRESS").alias("_a"),
            F.trim("AIRCRAFT_ID").alias("_c"),
            c("AOBT_3").cast("double").alias("_nt"),
            c("ADEP").alias("nm_adep"),
            c("ADES").alias("nm_ades"),
            c("AIRCRAFT_TYPE_ICAO_ID").alias("nm_type"),
            c("RTE_LEN_3").cast("double").alias("route_nm"),
        )
        .filter(c("_a").isNotNull() & c("_nt").isNotNull())
    )
    last_d = (
        fr["X"]
        .filter(c("d_val").isNotNull())
        .groupBy("flight_id")
        .agg(F.max_by("d_val", "ts").alias("distance_nm"))
    )
    f = FLX.select(
        "flight_id",
        c("icao24").alias("_a"),
        c("cs").alias("_c"),
        "adep",
        "ades",
        "adep_source",
        "ades_source",
        "typecode",
        F.coalesce("fl_aobt", "fl_atot", "fs").alias("_t"),
    )
    j = f.join(nm, ["_a", "_c"]).filter(F.abs(c("_t") - c("_nt")) <= RECORD_SEARCH_S)
    w = Window.partitionBy("flight_id").orderBy(F.abs(c("_t") - c("_nt")))
    j = (
        j.withColumn("_r", F.row_number().over(w))
        .filter(c("_r") == 1)
        .join(last_d, "flight_id", "left")
    )
    return j.select(
        "flight_id",
        "adep",
        "ades",
        "adep_source",
        "ades_source",
        "nm_adep",
        "nm_ades",
        "typecode",
        "nm_type",
        "distance_nm",
        "route_nm",
    ).toPandas()


# --------------------------------------------------------------------------
# score: sections iv and v
# --------------------------------------------------------------------------


def section_iv(fr: dict, inp: dict) -> list[dict]:
    from pyspark.sql import functions as F

    rows = []
    for r in fr["E"].groupBy("item").count().orderBy("item").collect():
        rows.append(row("iv", r["item"], "events", r["count"], r["count"]))
    n_ev = fr["E"].count()
    rows.append(row("iv", "all events", "events", n_ev, n_ev))
    fl = inp["flight_list"]
    n = fl.count()
    rows.append(row("iv", "flight list", "flights", n, n))
    rows.append(
        row(
            "iv",
            "flight list",
            "tracks with events",
            fr["E"].select("flight_id").distinct().count(),
            n,
        )
    )
    aggs = []
    for f in fl.schema.fields:
        v = F.col(f.name)
        ok = (
            v.isNotNull() & (F.trim(v.cast("string")) != "")
            if f.dataType.typeName() == "string"
            else v.isNotNull()
        )
        aggs.append(F.sum(ok.cast("int")).alias(f.name))
    r = fl.agg(*aggs).first()
    for f in fl.schema.fields:
        rows.append(row("iv", f"flight list {f.name}", "share filled", share(r[f.name] or 0, n), n))
    return rows


def section_v(meta: dict) -> list[dict]:
    rows = []
    for step, s in sorted((meta or {}).get("steps_timed", {}).items()):
        rows.append(row("v", f"step {step}", "seconds", s.get("seconds"), ""))
        rows.append(row("v", f"step {step}", "executor-seconds", s.get("executor_s"), ""))
    if meta and meta.get("seconds") is not None:
        rows.append(row("v", "run", "seconds (start to end)", meta["seconds"], ""))
    return rows


def events_seconds(meta: dict):
    """Seconds of the events step (step 04) of a run, or None."""
    s = (meta or {}).get("steps_timed", {}).get("04")
    return None if s is None else s.get("seconds")


# --------------------------------------------------------------------------
# score
# --------------------------------------------------------------------------

#: Event types whose events ``compare`` looks for when a confirmed event is lost.
COMPARED_TYPES = (
    "AOBT",
    "AIBT",
    "ATOT",
    "ALDT",
    "take-off-roll",
    "line-up",
    "runway-vacated",
    "xing-runway-threshold",
    "xing-40nm",
    "xing-100nm",
    "exit-parking_position",
    "entry-parking_position",
)


def load_meta(label: str) -> dict:
    p = local_dir(label) / "run.json"
    return json.loads(p.read_text()) if p.exists() else {}


def score(args) -> int:
    """The scorecard of one run (see the module text)."""
    check_env_file(REPO)
    meta = load_meta(args.label)
    scope = args.scope or meta.get("scope")
    if scope not in ("sample", "day"):
        raise SystemExit(f"no run.json for {args.label}: give --scope sample or day")
    out = local_dir(args.label)
    out.mkdir(parents=True, exist_ok=True)
    t0 = time.monotonic()
    with spark_lock(f"score {args.label}"):
        log = OUT / "eventlog" / f"score_{args.label}"
        spark = spark_session(f"corrections scorecard {args.label}", eventlog=log)
        sc = spark.sparkContext
        try:
            sc.setJobGroup("step-frames", "frames")
            import events_gt

            events_gt.assert_utc_session(spark)
            spark.conf.set("spark.sql.shuffle.partitions", "208" if scope == "day" else "104")
            inp = score_inputs(spark, args.label, scope)
            fr = build_frames(spark, inp)
            print(f"  {time.monotonic() - t0:.0f} s: frames built", flush=True)
            sc.setJobGroup("step-i", "section i")
            matched, n_ev, records, found, flr = match_records(spark, fr)
            rows = section_i(matched, n_ev, records, found, flr)
            print(f"  {time.monotonic() - t0:.0f} s: section i", flush=True)
            sc.setJobGroup("step-ii", "section ii")
            rows += nm_lines(nm_frame(spark, fr))
            print(f"  {time.monotonic() - t0:.0f} s: section ii", flush=True)
            sc.setJobGroup("step-iii", "section iii")
            counters = event_counters()
            counts = count_event_counters(fr["X"], counters)
            text = {n: (f, m) for n, f, m, _b, _o in counters}
            print(f"  {time.monotonic() - t0:.0f} s: event counters", flush=True)
            counts += count_other(fr, inp)
            fl_counts, fl_text = count_flight_list(fr, inp)
            counts += fl_counts
            text.update(fl_text)
            text.update(OTHER_TEXT)
            for n, *_ in counts:
                if n.startswith("info_") and n not in text:
                    text[n] = ("DECISIONS", "info." + n[len("info_") :].replace("_", " = ", 1))
            rows += counter_rows(
                counts, {k: v[0] for k, v in text.items()}, {k: v[1] for k, v in text.items()}
            )
            print(f"  {time.monotonic() - t0:.0f} s: other counters", flush=True)
            sc.setJobGroup("step-iv", "section iv")
            rows += section_iv(fr, inp)
            # The events of the record types, for ``compare``.
            keep = (
                fr["E"]
                .filter(F_col_isin("type", COMPARED_TYPES))
                .select("id", "flight_id", "type", "ts")
                .toPandas()
            )
        finally:
            spark.stop()
    import day_timing

    stages = day_timing.stage_rows(day_timing.event_log_lines(log), f"score {args.label}")
    for part in ("frames", "i", "ii", "iii", "iv"):
        mine = [r for r in stages if r["step"] == part]
        print(
            f"  part {part}: {sum(float(r['seconds'] or 0) for r in mine):.0f} s in stages, "
            f"{sum(float(r['executor_run_s'] or 0) for r in mine):.0f} executor-seconds"
        )
    day_timing.print_summary(stages, 6)
    rows += section_v(meta)
    # One row per confirmed event: an event can stand for two items (ATOT and
    # "ATOT (acdm)"); the first item is kept.
    conf = matched[matched["diff_s"].abs() <= CONFIRMED_S].rename(
        columns={"ts": "event_time", "id": "event_id", "rec_s": "record_s"}
    )
    conf = conf.drop_duplicates(["event_id", "flight_id", "type", "event_time"])
    conf.drop(columns=["_a", "_c"]).to_parquet(out / f"confirmed_{args.label}.parquet", index=False)
    keep.rename(columns={"ts": "event_time", "id": "event_id"}).to_parquet(
        out / f"events_{args.label}.parquet", index=False
    )
    write_scorecard(out / f"scorecard_{args.label}.csv", rows)
    md = scorecard_markdown(args.label, rows, meta)
    (out / f"scorecard_{args.label}.md").write_text(md)
    print(md)
    print(f"\nscorecard: {out / f'scorecard_{args.label}.csv'} ({time.monotonic() - t0:.0f} s)")
    return 0


def F_col_isin(name, values):  # noqa: N802 -- a Spark condition, built late
    from pyspark.sql import functions as F

    return F.col(name).isin(*values)


# --------------------------------------------------------------------------
# compare and agree
# --------------------------------------------------------------------------


def _scorecard_path(label: str) -> Path:
    p = local_dir(label) / f"scorecard_{label}.csv"
    if not p.exists():
        raise SystemExit(f"no scorecard for {label}: run `score {label}` first ({p})")
    return p


def clock(x) -> str:
    """Seconds since 1970 as ``HH:MM:SS`` (UTC); a dash when missing."""
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "–"
    return datetime.fromtimestamp(x, timezone.utc).strftime("%H:%M:%S")


def compare_markdown(before: str, after: str, rows: list[dict], lost, reasons, timing) -> str:
    lines = [f"# {before} against {after}", ""]
    n_lost = int((lost["fate"] == "lost").sum()) if len(lost) else 0
    n_moved = len(lost) - n_lost
    lines.append(
        f"Confirmed events (within 60 s of their airport record) of {before}: lost in {after}"
        f" {n_lost}, moved but still within 60 s {n_moved}."
    )
    if timing[0] and timing[1]:
        lines.append(
            f"Events step: {timing[0]:.0f} s against {timing[1]:.0f} s"
            f" ({100 * (timing[1] / timing[0] - 1):+.1f}%)."
        )
    lines.append(
        "Verdict: "
        + ("; ".join(reasons) if reasons else "no confirmed event lost, not slower")
        + "."
    )
    lines.append("")
    changed = [r for r in rows if r["change"] not in (None, 0, 0.0)]
    lines.append(
        f"{len(changed)} of {len(rows)} scorecard lines changed; unchanged lines are left out"
        " below."
    )
    lines.append("")
    for sec, title in SECTIONS.items():
        rs = [r for r in changed if r["section"] == sec]
        if not rs:
            continue
        lines += [f"## {sec}. {title}", ""]
        if sec == "iii":
            lines += [
                "| item | odd case | before | after | change | share before | share after |",
                "|---|---|---:|---:|---:|---:|---:|",
            ]
            for r in rs:
                lines.append(
                    f"| {r['item']} | {r['measure']} | {fmt_md(r['before'])} | {fmt_md(r['after'])}"
                    f" | {fmt_md(r['change'])} | {fmt_md(r['before_share'], True)} |"
                    f" {fmt_md(r['after_share'], True)} |"
                )
        else:
            sh = is_share_measure
            lines += ["| item | measure | before | after | change |", "|---|---|---:|---:|---:|"]
            for r in rs:
                s_ = sh(r["measure"])
                lines.append(
                    f"| {r['item']} | {r['measure']} | {fmt_md(r['before'], s_)} |"
                    f" {fmt_md(r['after'], s_)} | {fmt_md(r['change'], s_)} |"
                )
        lines.append("")
    if len(lost):
        lines += [
            "## Confirmed events lost or moved",
            "",
            "| fate | type | flight | before | after (nearest) | record | record airport | info |",
            "|---|---|---|---|---|---|---|---|",
        ]
        for _, r in lost.head(200).iterrows():
            ts = clock
            lines.append(
                f"| {r['fate']} | {r['type']} | {r['flight_id']} | {ts(r['event_time'])} |"
                f" {ts(r['after_time'])} | {ts(r['record_s'])} | {r['rec_apt']} |"
                f" {str(r['info'])[:120]} |"
            )
        lines.append("")
    return "\n".join(lines)


def content_comparison(before: str, after: str) -> list[dict]:
    """Every event type and the flight list compared on content, as ``day_compare.py`` does."""
    import day_compare
    from pyspark.sql import functions as F

    rows = []
    spark = spark_session(f"corrections scorecard compare {before} {after}")
    try:
        day = F.lit(DAY).cast("date")

        def events(label):
            root = run_root(label)
            ev = spark.read.parquet(f"{root}/opdi_flight_events").filter(F.col("dof") == day)
            m = spark.read.parquet(f"{root}/opdi_measurements").filter(F.col("dof") == day)
            m = m.groupBy(F.col("milestone_id").alias("id")).agg(
                F.max(F.when(F.col("type") == "Distance flown (NM)", F.col("value"))).alias(
                    "distance_flown_nm"
                ),
                F.max(F.when(F.col("type") == "Time Passed (s)", F.col("value"))).alias(
                    "time_passed_s"
                ),
            )
            return ev.join(m, "id", "left").select(
                F.col("id").alias("event_id"),
                "flight_id",
                "type",
                "event_time",
                "longitude",
                "latitude",
                F.col("altitude").alias("altitude_ft"),
                "info",
                "distance_flown_nm",
                "time_passed_s",
            )

        rows += day_compare.compare_events(events(before), events(after))
        fb, fa = (
            spark.read.parquet(f"{run_root(x)}/opdi_flight_list").filter(F.col("DOF") == day)
            for x in (before, after)
        )
        rows += day_compare.compare_content(fb, fa, ["ID"], "flight list")
    finally:
        spark.stop()
    return rows


def compare(args) -> int:
    """Two scorecards side by side, the confirmed events lost, the content, and the verdict."""
    import pandas as pd

    before, after = args.before, args.after
    rb, ra = read_scorecard(_scorecard_path(before)), read_scorecard(_scorecard_path(after))
    mb, ma = load_meta(before), load_meta(after)
    if mb.get("scope") and ma.get("scope") and mb["scope"] != ma["scope"]:
        raise SystemExit(
            f"{before} is a {mb['scope']} run and {after} a {ma['scope']} run: compare runs of one"
            " scope"
        )
    out = OUT / "compare" / f"{before}_vs_{after}"
    out.mkdir(parents=True, exist_ok=True)
    rows = compare_rows(rb, ra)
    with (out / "scorecards.csv").open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]) if rows else ["section"])
        w.writeheader()
        for r in rows:
            w.writerow({k: fmt_csv(v) for k, v in r.items()})
    conf = pd.read_parquet(local_dir(before) / f"confirmed_{before}.parquet")
    ev_after = pd.read_parquet(local_dir(after) / f"events_{after}.parquet")
    lost = lost_confirmed(conf, ev_after)
    lost.to_csv(out / "confirmed_lost_or_moved.csv", index=False)
    n_lost = int((lost["fate"] == "lost").sum()) if len(lost) else 0
    timing = (events_seconds(mb), events_seconds(ma))
    code, reasons = verdict(n_lost, args.allow, *timing)
    if not args.no_content:
        with spark_lock(f"compare {before} {after}"):
            content = content_comparison(before, after)
        import day_compare

        day_compare.write_rows(out / "content.csv", content)
        same = [
            r
            for r in content
            if r["part"] == "events" and r.get("matched_share") == 1.0 and not r.get("note")
        ]
        print(
            f"content: {len(same)} event types unchanged in every field; see {out / 'content.csv'}"
        )
    md = compare_markdown(before, after, rows, lost, reasons, timing)
    (out / "compare.md").write_text(md)
    print(md)
    print(f"\nfiles in {out}")
    print(
        f"exit {code}: "
        + ("; ".join(reasons) if reasons else "no confirmed event lost, not slower")
    )
    return code


def agree(args) -> int:
    """How well the sample's shares agree with the full day's, line by line (no Spark)."""
    s = read_scorecard(_scorecard_path(args.sample))
    d = read_scorecard(_scorecard_path(args.day))
    rows = agreement_rows(s, d, args.tolerance)
    out = OUT / "compare" / f"{args.sample}_vs_{args.day}_agreement.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        for r in rows:
            w.writerow({k: fmt_csv(v) for k, v in r.items()})
    from collections import Counter

    by = Counter((r["section"], r["verdict"]) for r in rows)
    for k, v in sorted(by.items()):
        print(f"  section {k[0]:4s} {k[1]:24s} {v}")
    lines = [
        f"# Sample {args.sample} against the day {args.day}",
        "",
        "| section | item | measure | sample | day | sample count | day count | verdict |",
        "|---|---|---|---:|---:|---:|---:|---|",
    ]
    for r in rows:
        lines.append(
            f"| {r['section']} | {r['item']} | {r['measure']} | {fmt_md(r['sample'], True)} |"
            f" {fmt_md(r['day'], True)} | {fmt_md(r['sample_count'])} | {fmt_md(r['day_count'])} |"
            f" {r['verdict']} |"
        )
    out.with_suffix(".md").write_text("\n".join(lines) + "\n")
    print(f"written {out} and {out.with_suffix('.md')}")
    return 0


# --------------------------------------------------------------------------
# Scoring arithmetic (pandas and plain Python; tested in
# tests/benchmarks/test_corrections_scorecard.py)
# --------------------------------------------------------------------------

#: Scorecard columns. ``count`` is the number of cases ``value`` rests on.
COLUMNS = ("section", "item", "measure", "value", "count", "finding")
SECTIONS = {
    "i": "against airport records",
    "ii": "against Network Manager",
    "iii": "odd cases",
    "iv": "counts and fill rates",
    "v": "time",
}
#: Seconds within which an event counts as confirmed by its airport record.
CONFIRMED_S = 60.0
WITHIN_S = (30.0, 60.0, 300.0)
#: The events step may not become slower than this (5%).
SLOWER_LIMIT = 0.05
#: Stand names that mean "no name".
NO_NAME = ("None", "none", "nan", "NaN", "")


def row(section, item, measure, value, count, finding="") -> dict:
    return {
        "section": section,
        "item": item,
        "measure": measure,
        "value": value,
        "count": count,
        "finding": finding,
    }


def share(n, base):
    """``n / base``, or None when ``base`` is 0."""
    return None if not base else n / base


def runway_parts(designator):
    """``("09", "L")`` from ``"9L"``, ``"09L"`` or ``" 09l "``; None for anything else."""
    if designator is None or (isinstance(designator, float) and math.isnan(designator)):
        return None
    m = re.fullmatch(r"\s*0*(\d{1,2})\s*([LRCGlrcg]?)\s*", str(designator))
    if not m or not 1 <= int(m.group(1)) <= 36:
        return None
    return f"{int(m.group(1)):02d}", m.group(2).upper()


def reciprocal_runway(designator):
    """The designator of the other end of the same runway (``"09L"`` -> ``"27R"``)."""
    p = runway_parts(designator)
    if p is None:
        return None
    num = (int(p[0]) + 17) % 36 + 1
    side = {"L": "R", "R": "L"}.get(p[1], p[1])
    return f"{num:02d}{side}"


def runway_agreement(named, record) -> str:
    """``exact``, ``reciprocal``, ``other``, ``none named`` or ``no record runway``."""
    r = runway_parts(record)
    if r is None:
        return "no record runway"
    n = runway_parts(named)
    if n is None:
        return "none named"
    if n == r:
        return "exact"
    if f"{n[0]}{n[1]}" == reciprocal_runway(f"{r[0]}{r[1]}"):
        return "reciprocal"
    return "other"


def stand_key(name):
    """A stand name for comparison: upper case, no spaces, no leading zeros; None when empty."""
    if name is None or (isinstance(name, float) and math.isnan(name)):
        return None
    s = re.sub(r"\s+", "", str(name)).upper()
    if s in {x.upper() for x in NO_NAME}:
        return None
    return s.lstrip("0") or "0"


def stand_agreement(named, record) -> str:
    """``equal``, ``differs``, ``none named`` or ``no record stand``."""
    r = stand_key(record)
    if r is None:
        return "no record stand"
    n = stand_key(named)
    if n is None:
        return "none named"
    return "equal" if n == r else "differs"


def angle_diff(a, b):
    """Smallest angle between two directions, degrees (0..180)."""
    return abs((a - b + 540.0) % 360.0 - 180.0)


def record_lines(matched, events: dict, records: dict, found: dict, finding="") -> list[dict]:
    """Section i rows of each item, from the events matched to a record.

    Args:
        matched: frame with ``item``, ``diff_s`` (event minus record, s) and
            ``subminute`` (the record gives seconds), one row per event that
            has a record.
        events: item -> events of the item.
        records: item -> airport records of the day in the scope.
        found: item -> records matched by at least one event.
    """
    import numpy as np

    out = []
    for item in events:
        m = matched[matched["item"] == item] if len(matched) else matched
        d = m["diff_s"].astype(float).to_numpy() if len(m) else np.array([])
        a = np.abs(d)
        n = len(d)
        out.append(row("i", item, "events", events[item], events[item], finding))
        out.append(row("i", item, "records", records.get(item, 0), records.get(item, 0), finding))
        out.append(row("i", item, "events with a record", n, events[item], finding))
        out.append(
            row(
                "i",
                item,
                "share of records found",
                share(found.get(item, 0), records.get(item, 0)),
                records.get(item, 0),
                finding,
            )
        )
        for w in WITHIN_S:
            out.append(
                row("i", item, f"share within {w:.0f} s", share(int((a <= w).sum()), n), n, finding)
            )
        sub = m["subminute"].astype(bool).to_numpy() if n else np.array([], dtype=bool)
        out.append(
            row(
                "i",
                item,
                "share within 60 s, records with seconds",
                share(int(((a <= 60) & sub).sum()), int(sub.sum())),
                int(sub.sum()),
                finding,
            )
        )
        out.append(
            row(
                "i",
                item,
                "median absolute difference s",
                float(np.median(a)) if n else None,
                n,
                finding,
            )
        )
        out.append(
            row(
                "i",
                item,
                "p90 absolute difference s",
                float(np.percentile(a, 90)) if n else None,
                n,
                finding,
            )
        )
        near = d[a <= 300]
        out.append(
            row(
                "i",
                item,
                "mean difference s, within 300 s",
                float(near.mean()) if len(near) else None,
                len(near),
                finding,
            )
        )
    return out


def category_lines(
    section, frame, item_col, cat_col, categories, measure_prefix, finding=""
) -> list[dict]:
    """Rows with the share of each category per item (``count`` = rows of the item)."""
    out = []
    for item, g in frame.groupby(item_col, sort=True):
        n = len(g)
        counts = g[cat_col].value_counts()
        for c in categories:
            k = int(counts.get(c, 0))
            out.append(row(section, item, f"{measure_prefix}{c}", share(k, n), n, finding))
    return out


def nm_lines(fl) -> list[dict]:
    """Section ii rows from the flight-list rows matched to a Network Manager flight.

    ``fl`` has ``adep``, ``ades``, ``adep_source``, ``ades_source``,
    ``nm_adep``, ``nm_ades``, ``typecode``, ``nm_type``, ``distance_nm``
    (published distance at the flight's last event) and ``route_nm``.
    """
    import numpy as np
    import pandas as pd

    out = []
    for side in ("adep", "ades"):
        f = fl.copy()
        f["verdict"] = np.where(
            f[side].isna(), "none", np.where(f[side] == f[f"nm_{side}"], "right", "wrong")
        )
        f["source"] = f[f"{side}_source"].fillna("(none)")
        for src, g in [("all", f)] + sorted(f.groupby("source"), key=lambda x: x[0]):
            n = len(g)
            for v in ("right", "wrong", "none"):
                out.append(
                    row(
                        "ii",
                        f"{side.upper()} source {src}",
                        f"share {v}",
                        share(int((g["verdict"] == v).sum()), n),
                        n,
                        "f1",
                    )
                )
    t = fl.dropna(subset=["nm_type"])
    has = t["typecode"].notna()
    n = len(t)
    out.append(
        row(
            "ii",
            "aircraft type",
            "share agrees",
            share(int((has & (t["typecode"] == t["nm_type"])).sum()), n),
            n,
            "f2.I",
        )
    )
    out.append(
        row(
            "ii",
            "aircraft type",
            "share differs",
            share(int((has & (t["typecode"] != t["nm_type"])).sum()), n),
            n,
            "f2.I",
        )
    )
    out.append(row("ii", "aircraft type", "share missing", share(int((~has).sum()), n), n, "f2.I"))
    r = fl.dropna(subset=["distance_nm", "route_nm"])
    r = r[r["route_nm"] > 0]
    ratio = (r["distance_nm"] / r["route_nm"]).to_numpy()
    n = len(ratio)
    for q in (5, 25, 50, 75, 95):
        out.append(
            row(
                "ii",
                "distance flown / route length",
                f"p{q}",
                float(np.percentile(ratio, q)) if n else None,
                n,
                "d3.A",
            )
        )
    out.append(
        row(
            "ii",
            "distance flown / route length",
            "share above 1.5",
            share(int((ratio > 1.5).sum()), n),
            n,
            "d3.A",
        )
    )
    out.append(
        row(
            "ii",
            "distance flown / route length",
            "share below 0.5",
            share(int((ratio < 0.5).sum()), n),
            n,
            "d3.A",
        )
    )
    _ = pd
    return out


def counter_rows(counts, finding_of: dict, measure_of: dict) -> list[dict]:
    """Section iii rows from ``(counter, item, n, base)`` tuples: value = n, count = base."""
    out = []
    for name, item, n, base in counts:
        if not base:
            continue
        out.append(
            row(
                "iii",
                item,
                measure_of.get(name, name),
                int(n or 0),
                int(base),
                finding_of.get(name, ""),
            )
        )
    return out


def value_share(r) -> float | None:
    """The share a row stands for: the value itself, or value / count for an odd-case counter."""
    if r["section"] == "iii":
        return share(float(r["value"]), float(r["count"])) if r["count"] not in ("", None) else None
    v = r["value"]
    return None if v in ("", None) else float(v)


def read_scorecard(path: Path) -> list[dict]:
    with path.open() as fh:
        return list(csv.DictReader(fh))


def write_scorecard(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(COLUMNS))
        w.writeheader()
        for r in rows:
            w.writerow({k: fmt_csv(r.get(k)) for k in COLUMNS})


def fmt_csv(v):
    if isinstance(v, float):
        if math.isnan(v):
            return ""
        return f"{v:.6g}" if abs(v) < 1e6 else f"{v:.0f}"
    return "" if v is None else v


def fmt_md(v, share_like=False) -> str:
    if v in (None, ""):
        return "–"
    try:
        x = float(v)
    except (TypeError, ValueError):
        return str(v)
    if share_like:
        return f"{100 * x:.1f}%"
    if x == int(x) and abs(x) < 1e15:
        return f"{int(x):,}"
    return f"{x:,.1f}" if abs(x) >= 10 else f"{x:.3g}"


def is_share_measure(measure: str) -> bool:
    return (
        measure.startswith("share") or measure.startswith("runway ") or measure.startswith("stand ")
    )


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def pivot_tables(rows: list[dict]) -> list[str]:
    """Rows of one section as tables with one line per item and one column per measure.

    Items with the same measures share a table. A last column ``n`` is added
    when every measure of an item rests on the same count.
    """
    order: list[str] = []
    by_item: dict = {}
    for r in rows:
        if r["item"] not in by_item:
            order.append(r["item"])
            by_item[r["item"]] = []
        by_item[r["item"]].append(r)
    groups: dict = {}
    for item in order:
        sig = tuple(r["measure"] for r in by_item[item])
        groups.setdefault(sig, []).append(item)
    out = []
    for sig, items in groups.items():
        counts = {item: {str(r["count"]) for r in by_item[item]} for item in items}
        same = all(len(c) == 1 for c in counts.values())
        head = ["item", *sig] + (["n"] if same else [])
        out.append("| " + " | ".join(head) + " |")
        out.append("|---|" + "---:|" * (len(head) - 1))
        for item in items:
            cells = [fmt_md(r["value"], is_share_measure(r["measure"])) for r in by_item[item]]
            if same:
                cells.append(fmt_md(next(iter(counts[item]))))
            out.append(f"| {item} | " + " | ".join(cells) + " |")
        out.append("")
    return out


def counter_table(rows: list[dict], top: int = 6) -> list[str]:
    """Section iii as one line per odd case: the total and the items with the most cases."""
    order: list[str] = []
    by: dict = {}
    for r in rows:
        if r["measure"] not in by:
            order.append(r["measure"])
            by[r["measure"]] = []
        by[r["measure"]].append(r)
    out = [
        "| odd case | finding | n | of | share | items with the most (n of) |",
        "|---|---|---:|---:|---:|---|",
    ]
    for m in order:
        rs = by[m]
        n = sum(_num(r["value"]) or 0 for r in rs)
        base = sum(_num(r["count"]) or 0 for r in rs)
        if len(rs) == 1:
            items = rs[0]["item"]
        else:
            worst = sorted(rs, key=lambda r: -(_num(r["value"]) or 0))[:top]
            items = "; ".join(
                f"{r['item']} {fmt_md(r['value'])} of {fmt_md(r['count'])}"
                for r in worst
                if (_num(r["value"]) or 0) > 0
            )
            items = items or f"0 in {len(rs)} items"
        out.append(
            f"| {m} | {rs[0].get('finding', '')} | {fmt_md(n)} | {fmt_md(base)} |"
            f" {fmt_md(share(n, base), True)} | {items} |"
        )
    out.append("")
    return out


def scorecard_markdown(label: str, rows: list[dict], meta: dict | None = None) -> str:
    """The scorecard as markdown: tables of items (i, ii, iv, v), one line per odd case (iii)."""
    lines = [f"# Scorecard {label}", ""]
    if meta:
        lines.append(
            f"Scope {meta.get('scope', '?')}, steps {' '.join(meta.get('steps', []))}, code "
            f"{meta.get('branch', '?')} {str(meta.get('commit', ''))[:10]}"
            + (" (with uncommitted changes)" if meta.get("dirty") else "")
            + (f", settings {' '.join(meta['set'])}" if meta.get("set") else "")
            + "."
        )
        lines.append("")
    for sec, title in SECTIONS.items():
        rs = [r for r in rows if r["section"] == sec]
        if not rs:
            continue
        lines += [f"## {sec}. {title}", ""]
        if sec == "iii":
            lines.append("n odd cases of the items counted; the CSV has every item of every case.")
            lines.append("")
            lines += counter_table(rs)
        else:
            lines += pivot_tables(rs)
    return "\n".join(lines)


def compare_rows(before: list[dict], after: list[dict]) -> list[dict]:
    """Two scorecards side by side: per (section, item, measure) both values and the change.

    For odd-case counters the shares are compared as well as the numbers.
    A line in only one scorecard counts as 0 (counters) or empty in the other.
    """
    key = lambda r: (r["section"], r["item"], r["measure"])  # noqa: E731
    b = {key(r): r for r in before}
    a = {key(r): r for r in after}
    out = []
    order = list(b) + [k for k in a if k not in b]
    for k in order:
        rb, ra = b.get(k), a.get(k)

        def num(r):
            if r is None:
                return 0.0 if k[0] == "iii" else None
            return None if r["value"] in ("", None) else float(r["value"])

        vb, va = num(rb), num(ra)
        sb = value_share(rb) if rb else (0.0 if k[0] == "iii" else None)
        sa = value_share(ra) if ra else (0.0 if k[0] == "iii" else None)
        out.append(
            {
                "section": k[0],
                "item": k[1],
                "measure": k[2],
                "before": vb,
                "after": va,
                "change": None if vb is None or va is None else va - vb,
                "before_count": rb["count"] if rb else "",
                "after_count": ra["count"] if ra else "",
                "before_share": sb if k[0] == "iii" else "",
                "after_share": sa if k[0] == "iii" else "",
                "finding": (rb or ra).get("finding", ""),
            }
        )
    return out


def lost_confirmed(before, after, tolerance_s: float = 1.0):
    """The events airport records confirm in ``before`` that ``after`` lost or moved.

    Both frames have ``flight_id``, ``type``, ``event_time`` (s) and
    ``event_id``; ``before`` holds only confirmed events (within 60 s of
    their record) with ``record_s``, ``diff_s`` and ``info``. An event is
    kept when ``after`` has the same flight and type within ``tolerance_s``.
    Otherwise it is ``moved`` when ``after`` has the same flight and type
    confirmed within 60 s of the same record or elsewhere, and ``lost`` when
    the type is missing for the flight or no longer within 60 s of the record.
    """
    import pandas as pd

    if before is None or len(before) == 0:
        return pd.DataFrame(columns=list(before.columns if before is not None else []) + ["fate"])
    a = after[["flight_id", "type", "event_time"]].rename(columns={"event_time": "after_time"})
    j = before.merge(a, on=["flight_id", "type"], how="left")
    j["_d"] = (j["after_time"] - j["event_time"]).abs()
    j["_rec"] = (j["after_time"] - j["record_s"]).abs()
    j = j.sort_values(["_d"], na_position="last")
    first = (
        j.groupby(["flight_id", "type", "event_id", "event_time"], dropna=False, sort=False)
        .head(1)
        .copy()
    )
    kept = first["_d"] <= tolerance_s
    # the nearest event of the same type to the record, among the after events
    near = (
        j.sort_values(["_rec"], na_position="last")
        .groupby(["flight_id", "type", "event_id", "event_time"], dropna=False, sort=False)
        .head(1)
    )
    near = near.set_index(["flight_id", "type", "event_id", "event_time"])["_rec"]
    first = first.set_index(["flight_id", "type", "event_id", "event_time"])
    first["after_nearest_record_s"] = near
    first = first.reset_index()
    out = first[~kept.to_numpy()].copy()
    still = out["after_nearest_record_s"] <= CONFIRMED_S
    out["fate"] = ["moved (still within 60 s)" if s else "lost" for s in still]
    out.loc[out["after_time"].isna(), "fate"] = "lost"
    return (
        out.drop(columns=["_d", "_rec"])
        .sort_values(["fate", "type", "flight_id"])
        .reset_index(drop=True)
    )


def verdict(
    lost: int, allow: int, before_s, after_s, limit: float = SLOWER_LIMIT
) -> tuple[int, list[str]]:
    """Exit code of ``compare`` and the reasons: lost confirmed events, or a slower events step."""
    reasons = []
    if lost > allow:
        reasons.append(f"{lost} confirmed event(s) lost (allowed {allow})")
    if before_s and after_s and after_s > before_s * (1 + limit):
        reasons.append(
            f"events step {after_s:.0f} s against {before_s:.0f} s:"
            f" {100 * (after_s / before_s - 1):+.1f}% (limit +{100 * limit:.0f}%)"
        )
    return (1 if reasons else 0), reasons


def agreement_rows(sample: list[dict], day: list[dict], tolerance: float = 0.02) -> list[dict]:
    """Per scorecard line: the share on the sample and on the day, and whether they agree.

    Shares agree when they differ by at most ``tolerance`` (absolute, 2
    points by default) or by at most a fifth of the day's share. Lines with
    fewer than 30 cases on the sample are marked as too few.
    """
    key = lambda r: (r["section"], r["item"], r["measure"])  # noqa: E731
    d = {key(r): r for r in day}
    out = []
    for r in sample:
        k = key(r)
        if k[0] not in ("i", "ii", "iii") or k not in d:
            continue
        ss, sd = value_share(r), value_share(d[k])
        if ss is None or sd is None or not is_share_or_counter(r):
            continue
        n = float(r["count"] or 0)
        if n < 30:
            verdict_ = "too few on the sample"
        elif abs(ss - sd) <= tolerance or abs(ss - sd) <= 0.2 * abs(sd):
            verdict_ = "agrees"
        else:
            verdict_ = "differs"
        out.append(
            {
                "section": k[0],
                "item": k[1],
                "measure": k[2],
                "sample": ss,
                "day": sd,
                "difference": ss - sd,
                "sample_count": r["count"],
                "day_count": d[k]["count"],
                "verdict": verdict_,
            }
        )
    return out


def is_share_or_counter(r) -> bool:
    return r["section"] == "iii" or is_share_measure(r["measure"])


#: The baseline runs: the "before" of every comparison, never deleted here.
KEPT_RUNS = ("base", "base_day")


def clean(args) -> int:
    """Delete the tables of one run on the bucket (not the baselines) and say how much."""
    if args.label in KEPT_RUNS:
        raise SystemExit(f"{args.label} is a baseline run and is kept")
    root = run_root(args.label)
    print(f"deleted {s3_delete(root):.2f} GB under {root}")
    return 0


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="command", required=True)
    s = sub.add_parser("subset", help="write the verification sample (once)")
    s.add_argument("--redo-step03", action="store_true", help="run step 03 of the day again")
    r = sub.add_parser("run", help="run steps 04 and 04b (or 03 04 04b) of one scope")
    r.add_argument("--label", required=True)
    r.add_argument("--scope", choices=("sample", "day"), required=True)
    r.add_argument("--steps", nargs="+", default=["04", "04b"])
    r.add_argument("--code", default=None, help="worktree whose src/ runs (default: this one)")
    r.add_argument("--set", action="append", help="a setting group.field=value (repeatable)")
    r.add_argument("--replace", action="store_true", help="delete and run again an existing label")
    c = sub.add_parser("score", help="the scorecard of one run")
    c.add_argument("label")
    c.add_argument(
        "--scope", choices=("sample", "day"), default=None, help="when the run has no run.json"
    )
    k = sub.add_parser(
        "compare", help="two scorecards side by side, lost confirmed events, verdict"
    )
    k.add_argument("before")
    k.add_argument("after")
    k.add_argument(
        "--allow", type=int, default=0, help="confirmed events that may be lost (agreed exceptions)"
    )
    k.add_argument(
        "--no-content", action="store_true", help="skip the content comparison (no Spark)"
    )
    g = sub.add_parser("agree", help="the sample's shares against the full day's (no Spark)")
    g.add_argument("sample")
    g.add_argument("day")
    g.add_argument("--tolerance", type=float, default=0.02)
    m = sub.add_parser("render", help="write the markdown of a scorecard again from its CSV")
    m.add_argument("label")
    d = sub.add_parser("clean", help="delete the tables of one run on the bucket")
    d.add_argument("label")
    sub.add_parser("_step03", help=argparse.SUPPRESS)
    args = ap.parse_args(argv)
    if args.command == "_step03":
        return _step03(args)
    if args.command == "subset":
        return subset(args)
    if args.command == "run":
        return run(args)
    if args.command == "score":
        return score(args)
    if args.command == "compare":
        return compare(args)
    if args.command == "agree":
        return agree(args)
    if args.command == "clean":
        return clean(args)
    if args.command == "render":
        p = _scorecard_path(args.label)
        p.with_suffix(".md").write_text(
            scorecard_markdown(args.label, read_scorecard(p), load_meta(args.label))
        )
        print(p.with_suffix(".md"))
        return 0
    raise SystemExit(f"unknown command {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
