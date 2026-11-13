#!/usr/bin/env python
"""Does adding ADS-C position reports change what today's pipeline produces?

ADS-C (Automatic Dependent Surveillance - Contract) is a second, sparse source
of position reports: mostly over oceans, one report every 5 to 20 minutes per
aircraft. The pipeline can merge these reports into the ADS-B reports before it
builds tracks (``AdscConfig.enabled``, off by default; step 01b). This script
runs today's default pipeline twice on the same aircraft and the same day, once
without ADS-C (the run ``base``) and once with it (the run ``merged``), and
compares tracks, flight list and flight events.

**The aircraft.** Only aircraft that sent at least one ADS-C report on the day.
The tracks of an aircraft are built from its own reports only, so an aircraft
without ADS-C gets identical tracks in both runs and leaves the difference
between the runs unchanged. Leaving them out keeps the tables small. All
totals therefore describe this group of aircraft, not the whole fleet.

**Same inputs, same code.** Step 01b (ADS-C) and step 01 (ADS-B) run once and
write under ``input/``; both runs read these tables. The two runs differ in
``adsc.enabled`` and nothing else (:func:`build_config`). The run ``base``
never reads the ADS-C table.

**Where it writes.** Only under ``s3a://eurocontrol/opdi-world/research/adsc_v2/``:
``input/`` (the two report tables) and ``base/``, ``merged/`` (the tables each
run derives). :func:`check_write_path` raises on any other path. Reference
tables are read from ``s3a://eurocontrol/opdi-world`` and the FIR reference from
the airspace events study (``research/airspace_events/ref``); neither is ever
written.

**Space.** The bucket has a quota of 200 GB with only a few GB free. Each run's
``osn_tracks`` is deleted as soon as step 02a has read it (``osn_tracks_clean``
holds the same reports), and the ADS-B reports under ``input/`` are deleted
once both runs have built their tracks. ``run`` stops before Spark starts when
less than ``--need-gb`` is free.

Subcommands::

    export PYSPARK_DRIVER_PYTHON=$PWD/.venv310/bin/python
    unset PYSPARK_PYTHON     # executors use the opdi-spark image's python
    .venv310/bin/python -u benchmarks/adsc_effect.py run --day 2026-06-14 --run all
    .venv310/bin/python -u benchmarks/adsc_effect.py compare --day 2026-06-14
    .venv310/bin/python -u benchmarks/adsc_effect.py clean            # lists only
    .venv310/bin/python -u benchmarks/adsc_effect.py clean --yes      # deletes

``run --run inputs|base|merged|all`` runs one part or all of them in order;
``--steps`` runs a subset of a run's steps (for a re-run after a failure). The
last line of the output is ``run exit <code>``.

``compare`` reads the tables of both runs, reduces them with Spark to small
frames, and writes CSV files with a provenance entry each
(``benchmarks/provenance.py``). The arithmetic on the small frames is plain
pandas and is tested in ``tests/benchmarks/test_adsc_effect.py``.
"""

import argparse
import json
import re
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

# The European box of the earlier study, for the flow labels of the reference
# flights (into, out of or within Europe).
from opdi.coverage import EUROPE_BBOX  # noqa: E402

# --- where things are ----------------------------------------------------------

PREFIX = "s3a://eurocontrol/opdi-world/research/adsc_v2"
REFERENCE_WAREHOUSE = "s3a://eurocontrol/opdi-world"
FIR_REFERENCE = "s3a://eurocontrol/opdi-world/research/airspace_events/ref/opdi_h3_airspace_ref"

#: The only place this script writes or deletes. Written out rather than built
#: from :data:`PREFIX`, so that a wrong ``PREFIX`` is noticed.
ALLOWED_WRITE_ROOT = re.compile(r"^s3a://eurocontrol/opdi-world/research/adsc_v2/")

#: Network Manager flights (one file per month, committed under ``reference/``).
NM_FLIGHTS = REPO / "reference" / "flights_{month}.parquet"

RESULTS_ROOT = REPO / "benchmarks" / "out" / "adsc_effect"

RUNS = ("base", "merged")

#: Reference tables, read from :data:`REFERENCE_WAREHOUSE` and never written.
PASSTHROUGH = (
    "h3_airport_detection_zones",
    "h3_runway_zones",
    "hexaero_airport_layouts",
    "oa_airports",
    "oa_runways",
    "osn_aircraft_db",
    "coverage_marker",
)

#: The FIR reference, read from :data:`FIR_REFERENCE` and never written. The
#: worldwide warehouse does not hold one yet.
FIR_TABLE = "opdi_h3_airspace_ref"

#: Written once under ``input/`` and read by both runs.
SHARED_INPUTS = ("osn_statevectors_v2", "osn_adsc_statevectors")

#: What each run builds, under ``<run>/``.
DERIVED = (
    "osn_tracks",
    "osn_tracks_clean",
    "opdi_flight_list",
    "opdi_endpoint_candidates",
    "opdi_flight_list_milestones_staging",
    "opdi_flight_events",
    "opdi_measurements",
    "opdi_flight_events_rejected",
)

ALL_TABLES = tuple(sorted(PASSTHROUGH + (FIR_TABLE,) + SHARED_INPUTS + DERIVED))

#: Steps that write the shared inputs, in order. 01b first: step 01 keeps the
#: ADS-B reports of the aircraft found in the ADS-C table.
INPUT_STEPS = ("01b", "01")

#: Steps of each run.
RUN_STEPS = ("02", "02a", "03", "04", "04b")

QUOTA_GB = 200.0

#: Free space needed before a run starts, in GB (10^9 bytes). The largest
#: moment of the whole sequence is estimated at about 2.5 GB (see the report of
#: the study plan); 3 GB leaves a margin.
NEED_GB = 3.0

#: Spark settings for S3 reads that wait instead of failing on a slow answer.
S3_PATIENCE = {
    "spark.hadoop.fs.s3a.connection.request.timeout": "600000",
    "spark.hadoop.fs.s3a.connection.timeout": "600000",
    "spark.hadoop.fs.s3a.attempts.maximum": "10",
    "spark.hadoop.fs.s3a.retry.limit": "10",
    "spark.hadoop.fs.s3a.retry.interval": "2s",
}

ADSC_SOURCE = "ADS-C"

# --- what is measured --------------------------------------------------------

#: A silence longer than this, in seconds, inside a track is a hole: no event
#: detector places anything across it (``EventConfig.track_hole_seconds``).
HOLE_SECONDS = 1800.0

#: Gaps shorter than this, in minutes, are not counted as reception gaps.
GAP_FLOOR_MINUTES = 5.0

#: Upper edges of the gap length classes, in minutes.
GAP_EDGES_MINUTES = (10.0, 20.0, 30.0, 60.0, 120.0, 240.0)

#: Time around a reference flight's block times within which a track counts as
#: belonging to it (the block times are on the ground, a track starts and ends
#: with reception).
OVERLAP_SLACK = pd.Timedelta(minutes=30)

#: For false joins: a track belongs to a flight's airborne part when it holds
#: time between off-block plus this margin and on-block minus it.
INNER_MARGIN = pd.Timedelta(minutes=10)

#: Events of one aircraft and type, one in each run, further apart than this
#: are not paired.
MATCH_TOLERANCE_SECONDS = 3600

#: Upper edges of the classes of a moved event's time change, in seconds.
SHIFT_EDGES_SECONDS = (10.0, 60.0, 300.0, 1800.0, 3600.0)

#: A FIR crossing placed between two reports further apart than this, in
#: seconds, has an uncertain time (the event sits on the first report inside).
FIR_BRACKET_LIMIT_SECONDS = 300.0

#: FIRs that lie mostly over the ocean and where ADS-B reception is thin. Used
#: only to split the FIR table; every FIR is reported.
OCEANIC_FIRS = frozenset(
    {
        "EGGXFIR",  # Shanwick
        "CZQXFIR",  # Gander
        "BIRDFIR",  # Reykjavik
        "BGGLFIR",  # Nuuk (Sondrestrom)
        "LPPOFIR",  # Santa Maria
        "KZNYFIR",  # New York (with its oceanic part)
        "KZWYFIR",  # New York Oceanic East
        "TTZPFIR",  # Piarco
        "SBAOFIR",  # Atlantico
        "GVSCFIR",  # Sal
        "GOOOFIR",  # Dakar (with its oceanic part)
        "KZAKFIR",  # Oakland Oceanic
        "PAZAFIR",  # Anchorage (with its oceanic part)
        "NZZOFIR",  # Auckland Oceanic
        "NFFFFIR",  # Nadi
        "NTTTFIR",  # Tahiti
        "FIMMFIR",  # Mauritius
        "FSSSFIR",  # Seychelles
        "AYPMFIR",  # Port Moresby
        "ANAUFIR",  # Nauru
        "RJJJFIR",  # Fukuoka (with its oceanic part)
    }
)

# =============================================================================
# Part 1: running the pipeline
# =============================================================================


def table_path(run: str, table: str) -> str:
    """Absolute S3 path of ``table`` for ``run`` (``base`` or ``merged``).

    The shared inputs do not depend on the run; that is what makes them
    shared.
    """
    if run not in RUNS:
        raise ValueError(f"unknown run {run!r}; expected one of {RUNS}")
    if table in SHARED_INPUTS:
        return f"{PREFIX}/input/{table}"
    if table in DERIVED:
        return f"{PREFIX}/{run}/{table}"
    if table == FIR_TABLE:
        return FIR_REFERENCE
    raise RuntimeError(
        f"table {table!r} is not a study table; reference tables resolve to "
        f"{REFERENCE_WAREHOUSE}"
    )


def check_write_path(path: str) -> None:
    """Raise unless ``path`` lies under the study folder. Not an assert, so
    ``python -O`` cannot remove it."""
    if not ALLOWED_WRITE_ROOT.match(path):
        raise PermissionError(f"refusing to write or delete {path!r}: only {PREFIX}/ is allowed")


def build_config(run: str, executors: int = 8):
    """The configuration of one run: today's worldwide default, ADS-C on or off.

    The two runs differ in ``adsc.enabled`` and nothing else.
    """
    from opdi.config import OPDIConfig

    if run not in RUNS:
        raise ValueError(f"unknown run {run!r}; expected one of {RUNS}")
    cfg = OPDIConfig.for_environment("opensky", worldwide=True)
    cfg.project.warehouse_path = REFERENCE_WAREHOUSE
    # The study folder holds the tables directly, without a release folder.
    cfg.project.release = ""
    cfg.adsc.enabled = run == "merged"
    cfg.spark.executor_instances = str(executors)
    return cfg


def sample_airframes(df, sample_mod: int):
    """Whole aircraft, one in ``sample_mod`` (all of them when it is 1)."""
    from pyspark.sql import functions as F

    if sample_mod <= 1:
        return df
    return df.where(F.abs(F.xxhash64("icao24")) % int(sample_mod) == 0)


def study_airframes(adsc, day: date, sample_mod: int = 1):
    """The aircraft of the study: those with an ADS-C report on ``day``."""
    from pyspark.sql import functions as F

    on_day = adsc.where(F.col("event_time_day") == F.lit(day.isoformat()).cast("date"))
    return sample_airframes(on_day.select("icao24").distinct(), sample_mod)


_ORIGINAL: Dict[str, object] = {}


def redirect(run: str, day: date, sample_mod: int = 1) -> None:
    """Point the pipeline's tables at the study folder for ``run``.

    Study tables resolve under :data:`PREFIX`, the FIR reference to
    :data:`FIR_REFERENCE`, every other table to the reference warehouse. Every
    write and every deletion is checked with :func:`check_write_path`. The
    ADS-B reports are cut to the study aircraft as they are written.
    """
    from opdi.utils.storage import StorageManager

    if not _ORIGINAL:
        _ORIGINAL.update(
            path=StorageManager._s3_path,
            write=StorageManager.write_table,
            drop=StorageManager.drop_partitions,
        )
    orig_path, orig_write, orig_drop = _ORIGINAL["path"], _ORIGINAL["write"], _ORIGINAL["drop"]

    def _s3_path(self, table_name):
        if table_name in PASSTHROUGH:
            return orig_path(self, table_name)
        return table_path(run, table_name)

    def write_table(self, df, table_name, mode="append", partition_by=None, partition_values=None):
        if table_name in PASSTHROUGH or table_name == FIR_TABLE:
            raise PermissionError(f"{table_name} is a reference table and is never written")
        check_write_path(_s3_path(self, table_name))
        if table_name == "osn_statevectors_v2":
            adsc = self.spark.read.parquet(table_path(run, "osn_adsc_statevectors"))
            df = df.join(study_airframes(adsc, day, sample_mod), "icao24", "left_semi")
        return orig_write(self, df, table_name, mode, partition_by, partition_values)

    def drop_partitions(self, table_name, column, values):
        check_write_path(_s3_path(self, table_name))
        return orig_drop(self, table_name, column, values)

    StorageManager._s3_path = _s3_path
    StorageManager.write_table = write_table
    StorageManager.drop_partitions = drop_partitions


def restore() -> None:
    """Undo :func:`redirect`. Safe to call more than once."""
    from opdi.utils.storage import StorageManager

    if _ORIGINAL:
        StorageManager._s3_path = _ORIGINAL.pop("path")
        StorageManager.write_table = _ORIGINAL.pop("write")
        StorageManager.drop_partitions = _ORIGINAL.pop("drop")


def window_days(day: date) -> List[str]:
    """The three days of reports a day's tracks are built from: D-1, D, D+1."""
    return [(day + timedelta(days=k)).isoformat() for k in (-1, 0, 1)]


def check_before_fetch(adsb_days: Iterable[str], adsc_days: Iterable[str], day: date) -> None:
    """Raise unless step 01 may fetch the ADS-B reports for ``day``.

    The ADS-C table must hold ``day`` (it gives the list of aircraft), and the
    ADS-B table must be empty: days already present were cut to an earlier
    list of aircraft, and step 01 does not fetch a day that is present.
    """
    adsb_days = sorted(adsb_days)
    if adsb_days:
        raise RuntimeError(
            f"the ADS-B input already holds {', '.join(adsb_days)}, cut to an earlier list "
            f"of aircraft. Delete it first: adsc_effect.py clean --part inputs --yes"
        )
    if day.isoformat() not in set(adsc_days):
        raise RuntimeError(
            f"the ADS-C input does not hold {day}; run step 01b first (run --run inputs)"
        )


def check_before_run(adsb_days: Iterable[str], adsc_days: Iterable[str], day: date) -> None:
    """Raise unless both inputs hold the three days a run reads."""
    need = set(window_days(day))
    missing_b = sorted(need - set(adsb_days))
    missing_c = sorted(need - set(adsc_days))
    if missing_b or missing_c:
        raise RuntimeError(
            f"inputs incomplete for {day}: ADS-B lacks {missing_b or 'nothing'}, "
            f"ADS-C lacks {missing_c or 'nothing'}. Run: adsc_effect.py run --run inputs"
        )


def record_timing(path: Path, run: str, step: str, seconds: float, study_gb: float) -> None:
    """Write the duration of ``step`` in ``run`` to the CSV file at ``path``.

    One row per run and step: a re-run replaces its own row.
    """
    path = Path(path)
    rows = pd.read_csv(path, dtype={"step": str}) if path.is_file() else pd.DataFrame()
    if len(rows):
        rows = rows[~((rows["run"] == run) & (rows["step"] == step))]
    new = pd.DataFrame(
        [{"run": run, "step": step, "seconds": round(seconds, 1), "study_gb": round(study_gb, 3)}]
    )
    rows = pd.concat([rows, new], ignore_index=True) if len(rows) else new
    path.parent.mkdir(parents=True, exist_ok=True)
    rows.sort_values(["run", "step"]).to_csv(path, index=False)


def _hadoop(spark, path: str):
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark._jsc.hadoopConfiguration()), p


def folder_gb(spark, path: str) -> float:
    """Bytes under ``path``, in GB; 0 when it does not exist."""
    fs, p = _hadoop(spark, path)
    return fs.getContentSummary(p).getLength() / 1e9 if fs.exists(p) else 0.0


def bucket_free_gb(spark, quota_gb: float = QUOTA_GB) -> float:
    """Quota minus the bytes stored in the whole bucket, in GB."""
    return quota_gb - folder_gb(spark, "s3a://eurocontrol/")


def delete_folder(spark, path: str) -> bool:
    """Delete one folder of the study. Raises outside the study folder."""
    check_write_path(path)
    fs, p = _hadoop(spark, path)
    if not fs.exists(p):
        return False
    return bool(fs.delete(p, True))


def _partitions(spark, path: str, column: str) -> List[str]:
    fs, p = _hadoop(spark, path)
    if not fs.exists(p):
        return []
    out = []
    for status in fs.listStatus(p):
        name = status.getPath().getName()
        if status.isDirectory() and name.startswith(f"{column}="):
            out.append(name.split("=", 1)[1])
    return sorted(out)


def run_part(spark, part: str, day: date, executors: int, steps, sample_mod: int, args) -> None:
    """Run the steps of one part (``inputs``, ``base`` or ``merged``)."""
    from opdi.periodrun import run_day_step
    from opdi.utils.storage import StorageManager

    run = "merged" if part == "inputs" else part
    cfg = build_config(run, executors)
    timings = Path(args.results_dir or RESULTS_ROOT / day.isoformat()) / "timings.csv"
    adsb_path = table_path(run, "osn_statevectors_v2")
    adsc_path = table_path(run, "osn_adsc_statevectors")

    print("=" * 72)
    print(f"  part       : {part}   (adsc.enabled={cfg.adsc.enabled})")
    print(f"  day        : {day}")
    print(f"  steps      : {' '.join(steps)}")
    print(f"  track rule : {cfg.segmentation.method}")
    print(f"  coverage   : {cfg.coverage.label}")
    print(f"  reference  : {REFERENCE_WAREHOUSE}  (read only)")
    print(f"  FIRs       : {FIR_REFERENCE}  (read only)")
    print(f"  writes     : {PREFIX}/{'input' if part == 'inputs' else part}")
    print(f"  aircraft   : ADS-C on {day}" + (f", one in {sample_mod}" if sample_mod > 1 else ""))
    print("=" * 72, flush=True)

    redirect(run, day, sample_mod)
    try:
        storage = StorageManager(spark, cfg)
        problem = storage.check_coverage()
        if problem:
            raise RuntimeError(problem)
        log_dir = str(REPO / "OPDI_live" / "logs" / f"adsc-v2-{run}")
        for step in steps:
            if part == "inputs" and step == "01":
                check_before_fetch(
                    _partitions(spark, adsb_path, "event_time_day"),
                    _partitions(spark, adsc_path, "event_time_day"),
                    day,
                )
            if part != "inputs" and step == "02":
                check_before_run(
                    _partitions(spark, adsb_path, "event_time_day"),
                    _partitions(spark, adsc_path, "event_time_day"),
                    day,
                )
            print(f"\n>>> {part} step {step} day {day}", flush=True)
            started = time.monotonic()
            run_day_step(spark, cfg, step, day, {"log_dir": log_dir})
            seconds = time.monotonic() - started
            if part != "inputs" and step == "02a" and not args.keep_tracks:
                delete_folder(spark, table_path(run, "osn_tracks"))
                print("    deleted osn_tracks (osn_tracks_clean holds the same reports)")
            if part == "merged" and step == "02" and args.drop_inputs:
                other = table_path("base", "osn_tracks_clean")
                if day.isoformat() in _partitions(spark, other, "dof"):
                    delete_folder(spark, adsb_path)
                    print("    deleted the ADS-B input (both runs have built their tracks)")
            study_gb = folder_gb(spark, PREFIX)
            record_timing(timings, part, step, seconds, study_gb)
            print(
                f"<<< {part} step {step} day {day} done in {seconds:.0f} s; "
                f"study folder {study_gb:.2f} GB",
                flush=True,
            )
    finally:
        restore()


def cmd_run(args) -> int:
    from opdi.utils.spark_helpers import SparkSessionManager

    day = date.fromisoformat(args.day)
    parts = ("inputs",) + RUNS if args.run == "all" else (args.run,)
    if args.steps and len(parts) > 1:
        raise SystemExit("--steps needs a single part (--run inputs, base or merged)")

    for part in parts:
        # A new session for each part, so that nothing one run registered or
        # cached in its session (temporary views, cached frames) reaches the
        # other.
        cfg = build_config("base" if part == "base" else "merged", args.executors)
        spark = SparkSessionManager.create_session(
            app_name=f"adsc effect {part} {day}",
            config=cfg,
            distributed=True,
            extra_configs=S3_PATIENCE,
        )
        try:
            free = bucket_free_gb(spark)
            print(f"bucket: {free:.1f} GB free of {QUOTA_GB:.0f} GB", flush=True)
            if free < args.need_gb:
                raise RuntimeError(
                    f"only {free:.1f} GB free; this run needs {args.need_gb:.1f} GB. "
                    "Free space first."
                )
            steps = args.steps or (INPUT_STEPS if part == "inputs" else RUN_STEPS)
            run_part(spark, part, day, args.executors, steps, args.sample_mod, args)
        finally:
            spark.stop()
    return 0


# --- clean -------------------------------------------------------------------

#: What ``clean --part`` deletes, relative to :data:`PREFIX`.
CLEAN_PARTS = {
    "all": "",
    "inputs": "input/",
    "adsb-input": "input/osn_statevectors_v2/",
    "base": "base/",
    "merged": "merged/",
}


def clean_keys(keys: Iterable[str], part: str = "all") -> List[str]:
    """The S3 keys of ``keys`` that ``clean --part`` deletes.

    Raises if any of them lies outside the study folder, so a wrong listing
    deletes nothing.
    """
    root = PREFIX.replace("s3a://eurocontrol/", "") + "/" + CLEAN_PARTS[part]
    out = []
    for key in keys:
        if not ALLOWED_WRITE_ROOT.match(f"s3a://eurocontrol/{key}"):
            raise PermissionError(f"refusing to delete {key!r}: outside {PREFIX}/")
        if key.startswith(root):
            out.append(key)
    return out


def _s3_client():
    import boto3

    env = {}
    for line in (REPO / ".env").read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip().strip('"').strip("'")
    return boto3.client(
        "s3",
        endpoint_url="https://s3.opensky-network.org",
        aws_access_key_id=env["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=env["AWS_SECRET_ACCESS_KEY"],
    )


def cmd_clean(args) -> int:
    """List, and with ``--yes`` delete, the study's tables. Nothing else."""
    s3 = _s3_client()
    root = PREFIX.replace("s3a://eurocontrol/", "") + "/" + CLEAN_PARTS[args.part]
    listed, sizes = [], {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket="eurocontrol", Prefix=root):
        for o in page.get("Contents", []):
            listed.append(o["Key"])
            sizes[o["Key"]] = o["Size"]
    keys = clean_keys(listed, args.part)
    total = sum(sizes[k] for k in keys) / 1e9
    print(f"{len(keys)} objects, {total:.2f} GB under s3a://eurocontrol/{root}")
    if not args.yes:
        print("nothing deleted (add --yes to delete)")
        return 0
    for i in range(0, len(keys), 1000):
        chunk = keys[i : i + 1000]
        s3.delete_objects(Bucket="eurocontrol", Delete={"Objects": [{"Key": k} for k in chunk]})
    print(f"deleted {len(keys)} objects ({total:.2f} GB)")
    return 0


# =============================================================================
# Part 2: comparing the two runs -- the pandas arithmetic (tested without Spark)
# =============================================================================


def gap_class(minutes: pd.Series) -> pd.Series:
    """Length class of each gap: ``5-10 min``, ..., ``240+ min``.

    Gaps shorter than :data:`GAP_FLOOR_MINUTES` get NaN.
    """
    edges = [GAP_FLOOR_MINUTES, *GAP_EDGES_MINUTES, np.inf]
    labels = [
        f"{lo:g}-{hi:g} min" if np.isfinite(hi) else f"{lo:g}+ min"
        for lo, hi in zip(edges[:-1], edges[1:])
    ]
    return pd.cut(minutes, bins=edges, labels=labels, right=False).astype(object)


def gap_table(gaps: pd.DataFrame) -> pd.DataFrame:
    """Reception gaps by length class and run.

    ``gaps`` has ``run`` and ``gap_s`` (one row per gap inside a track).
    Returns ``run``, ``gap_class``, ``gaps``, ``gap_hours``.
    """
    g = gaps.assign(gap_class=gap_class(gaps["gap_s"] / 60.0)).dropna(subset=["gap_class"])
    out = (
        g.groupby(["run", "gap_class"], sort=False)
        .agg(gaps=("gap_s", "size"), gap_hours=("gap_s", lambda s: s.sum() / 3600.0))
        .reset_index()
    )
    order = {
        label: i for i, label in enumerate(gap_class(pd.Series([7.0, 15, 25, 45, 90, 180, 300])))
    }
    return out.sort_values(["run", "gap_class"], key=lambda c: c.map(order).fillna(c)).reset_index(
        drop=True
    )


def adsc_position(gap_before_s: pd.Series, gap_after_s: pd.Series) -> pd.Series:
    """Where each ADS-C report lies relative to the ADS-B reports of its aircraft.

    ``gap_before_s`` / ``gap_after_s``: seconds to the nearest ADS-B report of
    the same aircraft before / after it (NaN when there is none).

    * ``before_first_adsb`` / ``after_last_adsb``: outside the ADS-B reports of
      the day, so it can only lengthen a track at one end (or make a track of
      its own);
    * ``in_hole``: more than :data:`HOLE_SECONDS` from ADS-B on both sides, so
      it lies in a gap that could split a track or interrupt event detection;
    * ``in_gap``: more than :data:`GAP_FLOOR_MINUTES` from ADS-B on both sides
      but not in a hole;
    * ``near_adsb``: within that of an ADS-B report on at least one side; it
      adds little beyond the ADS-B reports.
    * ``no_adsb``: the aircraft has no ADS-B report at all.
    """
    b = pd.to_numeric(gap_before_s, errors="coerce")
    a = pd.to_numeric(gap_after_s, errors="coerce")
    floor = GAP_FLOOR_MINUTES * 60.0
    out = pd.Series("near_adsb", index=b.index, dtype=object)
    both_far = (b > floor) & (a > floor)
    out[both_far] = "in_gap"
    out[(b > HOLE_SECONDS) & (a > HOLE_SECONDS)] = "in_hole"
    out[b.isna() & a.notna()] = "before_first_adsb"
    out[a.isna() & b.notna()] = "after_last_adsb"
    out[a.isna() & b.isna()] = "no_adsb"
    return out


def adsc_report_table(positions: pd.DataFrame, counts: Dict[str, int]) -> pd.DataFrame:
    """The ADS-C reports: totals, then the count and share in each position.

    ``positions`` has one row per ADS-C report kept in the merged tracks, with
    ``icao24`` and ``position`` (from :func:`adsc_position`). ``counts`` holds
    totals measured elsewhere (rows in the input table, ...); they come first.
    """
    rows = [{"metric": k, "value": v, "share": np.nan} for k, v in counts.items()]
    n = len(positions)
    rows.append({"metric": "kept_in_merged_tracks", "value": n, "share": np.nan})
    rows.append(
        {
            "metric": "aircraft_kept_in_merged_tracks",
            "value": int(positions["icao24"].nunique()) if n else 0,
            "share": np.nan,
        }
    )
    for label in (
        "in_hole",
        "in_gap",
        "near_adsb",
        "before_first_adsb",
        "after_last_adsb",
        "no_adsb",
    ):
        k = int((positions["position"] == label).sum()) if n else 0
        rows.append({"metric": f"position_{label}", "value": k, "share": k / n if n else np.nan})
    return pd.DataFrame(rows)


def overlap_counts(truth: pd.DataFrame, tracks: pd.DataFrame, slack=OVERLAP_SLACK) -> pd.Series:
    """Number of tracks of each reference flight's aircraft that overlap its
    block window (widened by ``slack``). Indexed by ``flight_key``; 0 when none.
    """
    j = truth[["flight_key", "icao24", "t_off", "t_on"]].merge(
        tracks[["icao24", "track_id", "first_seen", "last_seen"]], on="icao24"
    )
    hit = (j["first_seen"] <= j["t_on"] + slack) & (j["last_seen"] >= j["t_off"] - slack)
    n = j[hit].groupby("flight_key")["track_id"].nunique()
    return n.reindex(truth["flight_key"]).fillna(0).astype(int)


def tracks_per_flight_table(truth: pd.DataFrame, tracks_by_run: Dict[str, pd.DataFrame]):
    """Reference flights by number of tracks, per run and flow.

    Returns ``run``, ``flow``, ``flights``, ``one_track``, ``no_track``,
    ``two_or_more``, ``one_track_share``; plus a per-flight frame with the
    count in each run (columns ``tracks_<run>``).
    """
    per_flight = truth[["flight_key", "icao24", "flow"]].copy()
    rows = []
    for run, tracks in tracks_by_run.items():
        n = overlap_counts(truth, tracks).values
        per_flight[f"tracks_{run}"] = n
        for flow, g in per_flight.groupby("flow"):
            k = g[f"tracks_{run}"]
            rows.append(
                {
                    "run": run,
                    "flow": flow,
                    "flights": len(g),
                    "one_track": int((k == 1).sum()),
                    "no_track": int((k == 0).sum()),
                    "two_or_more": int((k >= 2).sum()),
                    "one_track_share": float((k == 1).mean()) if len(g) else np.nan,
                }
            )
    return pd.DataFrame(rows), per_flight


def changed_flights_table(per_flight: pd.DataFrame) -> pd.DataFrame:
    """Flights whose number of tracks changed between the runs, by direction."""
    b, m = per_flight["tracks_base"], per_flight["tracks_merged"]
    rows = []
    for flow, g in per_flight.assign(b=b, m=m).groupby("flow"):
        rows.append(
            {
                "flow": flow,
                "flights": len(g),
                "became_one_track": int(((g["b"] != 1) & (g["m"] == 1)).sum()),
                "stopped_being_one_track": int(((g["b"] == 1) & (g["m"] != 1)).sum()),
                "fewer_tracks": int((g["m"] < g["b"]).sum()),
                "more_tracks": int((g["m"] > g["b"]).sum()),
            }
        )
    return pd.DataFrame(rows)


def false_joins(truth: pd.DataFrame, tracks: pd.DataFrame, margin=INNER_MARGIN) -> pd.DataFrame:
    """Tracks that hold the airborne part of two or more reference flights.

    A track holds a flight's airborne part when it has time between off-block
    plus ``margin`` and on-block minus ``margin``. Two flights of one aircraft
    do not overlap in time, so such a track has joined two flights into one.
    Returns ``track_id``, ``icao24``, ``flights``.
    """
    t = truth[["flight_key", "icao24", "t_off", "t_on"]]
    t = t[(t["t_on"] - t["t_off"]) > 2 * margin]
    j = tracks[["track_id", "icao24", "first_seen", "last_seen"]].merge(t, on="icao24")
    inner = (j["first_seen"] <= j["t_on"] - margin) & (j["last_seen"] >= j["t_off"] + margin)
    n = j[inner].groupby(["track_id", "icao24"])["flight_key"].nunique().rename("flights")
    return n[n >= 2].reset_index()


def false_join_table(truth: pd.DataFrame, tracks_by_run: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Per run: tracks, tracks joining two or more flights, and those with ADS-C."""
    rows = []
    for run, tracks in tracks_by_run.items():
        fj = false_joins(truth, tracks)
        with_adsc = tracks.loc[tracks["adsc_reports"] > 0, "track_id"]
        rows.append(
            {
                "run": run,
                "tracks": len(tracks),
                "tracks_with_adsc": int(len(with_adsc)),
                "false_joins": len(fj),
                "false_joins_with_adsc": int(fj["track_id"].isin(set(with_adsc)).sum()),
                "flights_in_false_joins": int(fj["flights"].sum()) if len(fj) else 0,
            }
        )
    return pd.DataFrame(rows)


def great_circle_nm(lat1, lon1, lat2, lon2):
    """Great-circle distance in nautical miles (Earth radius 6371 km)."""
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dlat = p2 - p1
    dlon = np.radians(lon2) - np.radians(lon1)
    a = np.sin(dlat / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dlon / 2) ** 2
    return 2 * 6371.0 * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0))) / 1.852


def distance_table(per_flight: pd.DataFrame) -> pd.DataFrame:
    """Distance flown against the airports' great circle and the reference.

    ``per_flight``: one row per (reference flight, run) with ``run``,
    ``flight_key``, ``distance_nm`` (the pipeline's distance flown, summed
    over the flight's tracks), ``gc_nm`` (great circle between the reference
    airports), ``ref_nm`` (the reference route length, may be NaN) and
    ``base_hole`` (the flight has a gap over :data:`HOLE_SECONDS` in the run
    ``base``). Only flights with ``base_hole`` are summarised, and only those
    with a distance in both runs, so both runs describe the same flights.
    """
    f = per_flight[per_flight["base_hole"] & per_flight["distance_nm"].notna()]
    both = f.groupby("flight_key")["run"].nunique()
    f = f[f["flight_key"].isin(both[both == len(RUNS)].index)]
    rows = []
    for run, g in f.groupby("run"):
        to_gc = g["distance_nm"] / g["gc_nm"].where(g["gc_nm"] > 0)
        to_ref = g["distance_nm"] / g["ref_nm"].where(g["ref_nm"] > 0)
        rows.append(
            {
                "run": run,
                "flights": len(g),
                "median_distance_nm": float(g["distance_nm"].median()),
                "median_ratio_to_great_circle": float(to_gc.median()),
                "share_below_great_circle": float((to_gc < 1.0).mean()),
                "flights_with_reference": int(to_ref.notna().sum()),
                "median_ratio_to_reference": float(to_ref.median()),
                "share_within_5pct_of_reference": float(
                    ((to_ref - 1.0).abs() <= 0.05)[to_ref.notna()].mean()
                ),
            }
        )
    return pd.DataFrame(rows)


def flight_gap_table(per_flight: pd.DataFrame) -> pd.DataFrame:
    """Reception gaps of the reference flights, per run and flow.

    ``per_flight``: one row per (flight, run) with ``run``, ``flow``,
    ``reports``, ``adsc_reports``, ``max_gap_s`` and ``holes`` (NaN when the
    run has no report of the flight). Returns the flights seen, those with a
    hole, the holes, the median longest gap in minutes and the flights with at
    least one ADS-C report.
    """
    rows = []
    for (run, flow), g in per_flight.groupby(["run", "flow"]):
        seen = g[g["reports"].fillna(0) > 0]
        rows.append(
            {
                "run": run,
                "flow": flow,
                "flights": len(g),
                "flights_seen": len(seen),
                "flights_with_hole": int((seen["holes"] > 0).sum()),
                "holes": int(seen["holes"].sum()),
                "median_longest_gap_min": float(seen["max_gap_s"].median() / 60.0),
                "flights_with_adsc": int((seen["adsc_reports"] > 0).sum()),
            }
        )
    return pd.DataFrame(rows)


def shift_class(seconds: pd.Series) -> pd.Series:
    """Class of the size of a time change: ``0 s``, ``<=10 s``, ..., ``<=3600 s``."""
    s = seconds.abs()
    edges = [0.0, *SHIFT_EDGES_SECONDS]
    out = pd.Series("0 s", index=s.index, dtype=object)
    for lo, hi in zip(edges[:-1], edges[1:]):
        out[(s > lo) & (s <= hi)] = f"<={hi:g} s"
    out[s > edges[-1]] = f">{edges[-1]:g} s"
    out[s.isna()] = np.nan
    return out


def events_table(status: pd.DataFrame, moved: pd.DataFrame) -> pd.DataFrame:
    """One row per event type (and position): counts in each class and shares.

    ``status``: ``type``, ``in_base_hole``, ``status`` (``same_time``,
    ``moved``, ``only_base``, ``only_merged``), ``n``.
    ``moved``: ``type``, ``in_base_hole``, ``median_shift_s``, ``p90_shift_s``
    (absolute changes of the moved events).
    """
    keys = ["type", "in_base_hole"]
    wide = status.pivot_table(index=keys, columns="status", values="n", aggfunc="sum", fill_value=0)
    for c in ("same_time", "moved", "only_base", "only_merged"):
        if c not in wide.columns:
            wide[c] = 0
    wide = wide[["same_time", "moved", "only_base", "only_merged"]].reset_index()
    wide.columns.name = None
    wide["base_events"] = wide["same_time"] + wide["moved"] + wide["only_base"]
    wide["merged_events"] = wide["same_time"] + wide["moved"] + wide["only_merged"]
    wide["unchanged_share"] = wide["same_time"] / wide["base_events"].where(wide["base_events"] > 0)
    wide = wide.merge(moved, on=keys, how="left")
    return wide.sort_values(keys).reset_index(drop=True)


def fir_table(fir: pd.DataFrame) -> pd.DataFrame:
    """FIR crossings into each FIR, per run, with the oceanic label.

    ``fir``: ``run``, ``fir_to``, ``crossings``, ``bracket_over_limit``,
    ``median_bracket_s``. Returns one row per FIR with both runs side by side.
    """
    wide = fir.pivot_table(
        index="fir_to",
        columns="run",
        values=["crossings", "bracket_over_limit", "median_bracket_s"],
        aggfunc="sum",
        fill_value=0,
    )
    wide.columns = [f"{a}_{b}" for a, b in wide.columns]
    wide = wide.reset_index()
    for run in RUNS:
        for c in ("crossings", "bracket_over_limit", "median_bracket_s"):
            if f"{c}_{run}" not in wide.columns:
                wide[f"{c}_{run}"] = 0
    wide["oceanic"] = wide["fir_to"].isin(OCEANIC_FIRS)
    wide["added"] = wide["crossings_merged"] - wide["crossings_base"]
    return wide.sort_values(["oceanic", "added"], ascending=[False, False]).reset_index(drop=True)


def rejected_table(rejected: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    """Events removed by each plausibility check, per run, with the share of
    the events of that type the run detected (kept plus removed).

    ``rejected``: ``run``, ``reject_rule``, ``type``, ``n``.
    ``events``: ``run``, ``type``, ``n`` (kept events).
    """
    kept = events.rename(columns={"n": "kept"})
    out = rejected.merge(kept, on=["run", "type"], how="left").fillna({"kept": 0})
    out["removed_share"] = out["n"] / (out["n"] + out["kept"])
    return out.rename(columns={"n": "removed"}).sort_values(["reject_rule", "type", "run"])


def timing_table(timings: pd.DataFrame) -> pd.DataFrame:
    """Seconds per step in each run side by side, and the time ADS-C adds."""
    t = timings[timings["run"].isin(RUNS)]
    wide = t.pivot_table(index="step", columns="run", values="seconds", aggfunc="sum")
    wide = wide.reindex(columns=list(RUNS)).reset_index()
    wide.columns.name = None
    wide["added_s"] = wide["merged"] - wide["base"]
    inputs = timings[timings["run"] == "inputs"][["step", "seconds"]]
    inputs = inputs.rename(columns={"seconds": "shared_inputs"})
    return wide.merge(inputs, on="step", how="outer").sort_values("step").reset_index(drop=True)


def load_reference_flights(path, days: Iterable[str], airframes) -> pd.DataFrame:
    """Network Manager flights of ``airframes`` with off-block on ``days``.

    UTC times, one row per flight, ``flight_key`` numbering the rows (so two
    legs with one call sign stay apart), and ``ref_nm`` = ``RTE_LEN_3``, the
    route length of the Network Manager's model of the flown trajectory.
    """
    cols = ["AIRCRAFT_ADDRESS", "AIRCRAFT_ID", "ADEP", "ADES", "AOBT_3", "ARVT_3", "RTE_LEN_3"]
    t = pd.read_parquet(path, columns=cols).dropna(
        subset=["AIRCRAFT_ADDRESS", "ADEP", "ADES", "AOBT_3", "ARVT_3"]
    )
    t = pd.DataFrame(
        {
            "icao24": t["AIRCRAFT_ADDRESS"].str.lower().str.strip(),
            "callsign": t["AIRCRAFT_ID"].str.strip(),
            "gt_adep": t["ADEP"].str.strip(),
            "gt_ades": t["ADES"].str.strip(),
            "t_off": pd.to_datetime(t["AOBT_3"], utc=True).dt.tz_localize(None),
            "t_on": pd.to_datetime(t["ARVT_3"], utc=True).dt.tz_localize(None),
            "ref_nm": pd.to_numeric(t["RTE_LEN_3"], errors="coerce"),
        }
    )
    t = t[t["t_off"].dt.strftime("%Y-%m-%d").isin(set(days)) & t["icao24"].isin(set(airframes))]
    t = t.sort_values(["icao24", "t_off"]).reset_index(drop=True)
    return t.assign(flight_key=np.arange(len(t)))


def inside_day(truth: pd.DataFrame, day: str) -> pd.DataFrame:
    """Flights that leave and arrive on ``day``: every track that holds them
    starts on that day, so they are complete in the tables of one day."""
    d = truth["t_off"].dt.strftime("%Y-%m-%d").eq(day) & truth["t_on"].dt.strftime("%Y-%m-%d").eq(
        day
    )
    return truth[d].reset_index(drop=True)


# =============================================================================
# Part 3: comparing the two runs -- Spark reductions (not tested without Spark)
# =============================================================================


def _seconds(column: str):
    from pyspark.sql import functions as F

    return F.unix_timestamp(F.col(column)).cast("double")


def _is_adsc(df):
    from pyspark.sql import functions as F

    if "source" in df.columns:
        return F.coalesce(F.col("source") == F.lit(ADSC_SOURCE), F.lit(False))
    return F.lit(False)


def track_summaries(tracks):
    """One row per track: aircraft, first and last report, reports, ADS-C
    reports, distance flown, longest gap and number of holes."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy("track_id").orderBy("event_time")
    gap = _seconds("event_time") - F.lag(_seconds("event_time")).over(w)
    return (
        tracks.withColumn("_gap", gap)
        .withColumn("_adsc", _is_adsc(tracks).cast("int"))
        .groupBy("track_id")
        .agg(
            F.first("icao24").alias("icao24"),
            F.min("event_time").alias("first_seen"),
            F.max("event_time").alias("last_seen"),
            F.count(F.lit(1)).alias("reports"),
            F.sum("_adsc").alias("adsc_reports"),
            F.max("cumulative_distance_nm").alias("distance_nm"),
            F.coalesce(F.max("_gap"), F.lit(0.0)).alias("max_gap_s"),
            F.sum(F.when(F.col("_gap") > HOLE_SECONDS, 1).otherwise(0)).alias("holes"),
        )
    )


def track_gaps(tracks):
    """Every gap of at least :data:`GAP_FLOOR_MINUTES` inside a track."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy("track_id").orderBy("event_time")
    gap = _seconds("event_time") - F.lag(_seconds("event_time")).over(w)
    return (
        tracks.select("track_id", "icao24", "event_time")
        .withColumn("gap_s", gap)
        .where(F.col("gap_s") >= GAP_FLOOR_MINUTES * 60.0)
        .select("track_id", "icao24", F.col("event_time").alias("gap_end"), "gap_s")
    )


def flight_measures(spark, tracks, truth: pd.DataFrame):
    """Per reference flight, from the reports of its aircraft between its
    block times: reports, ADS-C reports, longest gap, holes, number of tracks
    and the distance flown (per track, the change of ``cumulative_distance_nm``
    over the flight's time, summed over its tracks)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    t = spark.createDataFrame(truth[["flight_key", "icao24", "t_off", "t_on"]])
    j = tracks.join(F.broadcast(t), "icao24").where(
        (F.col("event_time") >= F.col("t_off")) & (F.col("event_time") <= F.col("t_on"))
    )
    w = Window.partitionBy("flight_key").orderBy("event_time")
    gap = _seconds("event_time") - F.lag(_seconds("event_time")).over(w)
    j = j.withColumn("_gap", gap).withColumn("_adsc", _is_adsc(tracks).cast("int"))
    per_flight = j.groupBy("flight_key").agg(
        F.count(F.lit(1)).alias("reports"),
        F.sum("_adsc").alias("adsc_reports"),
        F.coalesce(F.max("_gap"), F.lit(0.0)).alias("max_gap_s"),
        F.sum(F.when(F.col("_gap") > HOLE_SECONDS, 1).otherwise(0)).alias("holes"),
        F.countDistinct("track_id").alias("tracks_in_window"),
    )
    per_piece = j.groupBy("flight_key", "track_id").agg(
        (F.max("cumulative_distance_nm") - F.min("cumulative_distance_nm")).alias("d")
    )
    distance = per_piece.groupBy("flight_key").agg(F.sum("d").alias("distance_nm"))
    return per_flight.join(distance, "flight_key", "left")


def adsc_positions(merged_tracks):
    """Each ADS-C report kept in the merged tracks, with the seconds to the
    nearest ADS-B report of the same aircraft before and after it."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy("icao24").orderBy("event_time")
    t = _seconds("event_time")
    adsb_t = F.when(~_is_adsc(merged_tracks), t)
    prev_b = F.last(adsb_t, ignorenulls=True).over(w.rowsBetween(Window.unboundedPreceding, -1))
    next_b = F.first(adsb_t, ignorenulls=True).over(w.rowsBetween(1, Window.unboundedFollowing))
    return (
        merged_tracks.select("icao24", "track_id", "event_time", "source")
        .withColumn("gap_before_s", t - prev_b)
        .withColumn("gap_after_s", next_b - t)
        .where(_is_adsc(merged_tracks))
        .select("icao24", "track_id", "event_time", "gap_before_s", "gap_after_s")
    )


def match_events(base_events, merged_events, tracks_icao, base_holes):
    """Pair the events of the two runs and give each a status.

    Events are paired within one aircraft and one type when each is the
    other's nearest in time, at most :data:`MATCH_TOLERANCE_SECONDS` apart.
    Track ids cannot be the key: adding ADS-C changes them. Returns one row per
    pair or unpaired event: ``type``, ``in_base_hole``, ``status``,
    ``shift_s`` (merged minus base, for pairs).

    ``in_base_hole`` is true when the event (either side of a pair) lies inside
    a hole of the run ``base`` of its aircraft: there, only ADS-C reports can
    have produced or moved it.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    def keyed(ev, tag):
        return (
            ev.join(tracks_icao, ev["flight_id"] == tracks_icao["track_id"])
            .select(
                F.col("icao24"),
                F.col("type"),
                F.col("id").alias(f"id_{tag}"),
                _seconds("event_time").alias(f"t_{tag}"),
            )
            .transform(lambda d: flag_holes(d, base_holes, f"t_{tag}", f"hole_{tag}"))
        )

    b, m = keyed(base_events, "b"), keyed(merged_events, "m")
    pairs = (
        b.join(m, ["icao24", "type"])
        .withColumn("_d", F.abs(F.col("t_m") - F.col("t_b")))
        .where(F.col("_d") <= MATCH_TOLERANCE_SECONDS)
    )
    rb = F.row_number().over(Window.partitionBy("id_b").orderBy("_d", "id_m"))
    rm = F.row_number().over(Window.partitionBy("id_m").orderBy("_d", "id_b"))
    matched = (
        pairs.withColumn("_rb", rb)
        .withColumn("_rm", rm)
        .where((F.col("_rb") == 1) & (F.col("_rm") == 1))
    )
    paired = matched.select(
        "type",
        (F.col("hole_b") | F.col("hole_m")).alias("in_base_hole"),
        F.when(F.col("_d") == 0, F.lit("same_time")).otherwise(F.lit("moved")).alias("status"),
        (F.col("t_m") - F.col("t_b")).alias("shift_s"),
    )
    only_b = b.join(matched.select("id_b"), "id_b", "left_anti").select(
        "type",
        F.col("hole_b").alias("in_base_hole"),
        F.lit("only_base").alias("status"),
        F.lit(None).cast("double").alias("shift_s"),
    )
    only_m = m.join(matched.select("id_m"), "id_m", "left_anti").select(
        "type",
        F.col("hole_m").alias("in_base_hole"),
        F.lit("only_merged").alias("status"),
        F.lit(None).cast("double").alias("shift_s"),
    )
    return paired.unionByName(only_b).unionByName(only_m)


def base_holes_frame(base_gaps):
    """The holes of the run ``base``: aircraft, start and end (seconds)."""
    from pyspark.sql import functions as F

    return base_gaps.where(F.col("gap_s") > HOLE_SECONDS).select(
        "icao24",
        (_seconds("gap_end") - F.col("gap_s")).alias("hole_start"),
        _seconds("gap_end").alias("hole_end"),
    )


def flag_holes(df, holes, t_col: str, out_col: str):
    """Add ``out_col``: whether ``t_col`` (seconds) lies strictly inside a hole
    of the same aircraft."""
    from pyspark.sql import functions as F

    hit = (
        df.select("icao24", t_col)
        .distinct()
        .join(F.broadcast(holes), "icao24")
        .where((F.col(t_col) > F.col("hole_start")) & (F.col(t_col) < F.col("hole_end")))
        .select("icao24", t_col)
        .distinct()
        .withColumn(out_col, F.lit(True))
    )
    return df.join(hit, ["icao24", t_col], "left").fillna({out_col: False})


def read_day_or_empty(spark, path: str, column: str, day: str):
    """The rows of ``day`` of the table at ``path``; an empty frame with the
    removed-events columns when the table was never written (no event was
    removed)."""
    from pyspark.sql import functions as F

    fs, p = _hadoop(spark, path)
    if fs.exists(p):
        return spark.read.parquet(path).where(F.col(column) == F.lit(day).cast("date"))
    return spark.createDataFrame([], "reject_rule string, type string, dof date")


# --- compare -------------------------------------------------------------------

#: Code whose change makes the staged numbers stale.
CODE_PATHS = (
    "benchmarks/adsc_effect.py",
    "benchmarks/adsc_flight_list_truth.py",
    "src/opdi/ingestion/osn_adsc.py",
    "src/opdi/pipeline/tracks.py",
    "src/opdi/pipeline/segmentation/methods.py",
    "src/opdi/cleaning/native.py",
    "src/opdi/pipeline/events.py",
    "src/opdi/pipeline/fir.py",
    "src/opdi/pipeline/plausibility.py",
    "src/opdi/pipeline/flights.py",
    "src/opdi/config/adsc.py",
    "src/opdi/config/cleaning.py",
    "src/opdi/config/segmentation.py",
    "src/opdi/config/events.py",
)


def _write(out: Path, name: str, frame: pd.DataFrame, argv, inputs, tables) -> None:
    import provenance

    frame.to_csv(out / name, index=False)
    provenance.record(
        out,
        name,
        "benchmarks/adsc_effect.py",
        argv,
        [REPO / p for p in CODE_PATHS],
        inputs={**inputs, "rows": int(len(frame))},
        input_tables=tables,
        notes=(
            "Two runs of today's default pipeline on the aircraft with ADS-C on the day: "
            f"base (ADS-C off) and merged (ADS-C on), under {PREFIX}."
        ),
    )
    print(f"wrote {out / name} ({len(frame)} rows)", flush=True)


def cmd_compare(args) -> int:
    import adsc_flight_list_truth as fl
    from pyspark.sql import functions as F

    from opdi.utils.spark_helpers import SparkSessionManager

    day = date.fromisoformat(args.day)
    d = day.isoformat()
    out = Path(args.results_dir or RESULTS_ROOT / d)
    out.mkdir(parents=True, exist_ok=True)
    argv = sys.argv[1:]

    cfg = build_config("base", args.executors)
    spark = SparkSessionManager.create_session(
        app_name=f"adsc effect compare {d}",
        config=cfg,
        distributed=not args.local,
        extra_configs=S3_PATIENCE,
    )
    try:
        tables = {}
        for run in RUNS:
            tracks = spark.read.parquet(table_path(run, "osn_tracks_clean")).where(
                F.col("dof") == F.lit(d).cast("date")
            )
            keep = ["icao24", "track_id", "event_time", "cumulative_distance_nm"]
            keep += ["source"] if "source" in tracks.columns else []
            tables[run] = {
                "tracks": tracks.select(*keep).cache(),
                "flights": spark.read.parquet(table_path(run, "opdi_flight_list")).where(
                    F.col("DOF") == F.lit(d).cast("date")
                ),
                "events": spark.read.parquet(table_path(run, "opdi_flight_events")).where(
                    F.col("dof") == F.lit(d).cast("date")
                ),
                "rejected": read_day_or_empty(
                    spark, table_path(run, "opdi_flight_events_rejected"), "dof", d
                ),
            }
        input_tables = [
            f"{table_path(r, t)}" for r in RUNS for t in ("osn_tracks_clean", "opdi_flight_events")
        ]
        inputs = {"day": d}

        # Tracks, gaps and the ADS-C reports.
        summaries = {r: track_summaries(tables[r]["tracks"]).toPandas() for r in RUNS}
        gaps = {r: track_gaps(tables[r]["tracks"]) for r in RUNS}
        gaps_pd = pd.concat(
            [gaps[r].select("gap_s").toPandas().assign(run=r) for r in RUNS], ignore_index=True
        )
        airframes = sorted(set(summaries["base"]["icao24"]) | set(summaries["merged"]["icao24"]))
        inputs.update(
            {f"tracks_{r}": int(len(summaries[r])) for r in RUNS},
            aircraft=len(airframes),
        )
        _write(out, "gaps.csv", gap_table(gaps_pd), argv, inputs, input_tables)

        adsc_in = spark.read.parquet(table_path("merged", "osn_adsc_statevectors"))
        counts = {
            "input_reports_three_days": adsc_in.count(),
            "input_reports_on_day": adsc_in.where(
                F.col("event_time_day") == F.lit(d).cast("date")
            ).count(),
            "input_aircraft_on_day": adsc_in.where(F.col("event_time_day") == F.lit(d).cast("date"))
            .select("icao24")
            .distinct()
            .count(),
            "study_aircraft": len(airframes),
        }
        pos = adsc_positions(tables["merged"]["tracks"]).toPandas()
        pos["position"] = adsc_position(pos["gap_before_s"], pos["gap_after_s"])
        _write(out, "adsc_reports.csv", adsc_report_table(pos, counts), argv, inputs, input_tables)

        # Reference flights.
        month = day.strftime("%Y%m")
        nm = str(NM_FLIGHTS).format(month=month)
        days3 = window_days(day)
        truth_all = load_reference_flights(nm, days3, airframes)
        airports = (
            spark.read.parquet(f"{REFERENCE_WAREHOUSE}/oa_airports")
            .select("ident", "latitude_deg", "longitude_deg")
            .toPandas()
        )
        min_lon, min_lat, max_lon, max_lat = EUROPE_BBOX
        in_box = set(
            airports.loc[
                airports["longitude_deg"].between(min_lon, max_lon)
                & airports["latitude_deg"].between(min_lat, max_lat),
                "ident",
            ]
        )
        truth_all = fl.label_far_end(truth_all, in_box)
        truth = inside_day(truth_all, d)
        inputs.update(reference_flights=len(truth), reference_file=Path(nm).name)

        tracks_pd = {
            r: summaries[r].assign(
                first_seen=lambda x: pd.to_datetime(x["first_seen"]),
                last_seen=lambda x: pd.to_datetime(x["last_seen"]),
            )
            for r in RUNS
        }
        table, per_flight = tracks_per_flight_table(truth, tracks_pd)
        _write(out, "tracks_per_flight.csv", table, argv, inputs, input_tables)
        _write(
            out,
            "tracks_per_flight_changes.csv",
            changed_flights_table(per_flight),
            argv,
            inputs,
            input_tables,
        )
        _write(
            out,
            "false_joins.csv",
            false_join_table(truth_all, tracks_pd),
            argv,
            inputs,
            input_tables,
        )

        # Airports of the flight list against the reference.
        lists = {}
        for r in RUNS:
            f = (
                tables[r]["flights"]
                .select(
                    F.lower("ICAO24").alias("icao24"),
                    F.trim("FLT_ID").alias("callsign"),
                    F.col("ADEP").alias("adep"),
                    F.col("ADES").alias("ades"),
                    F.col("FIRST_SEEN").alias("first_seen"),
                    F.col("LAST_SEEN").alias("last_seen"),
                )
                .toPandas()
            )
            lists[r] = f
        both = fl.both_ends_table(
            truth,
            fl.match_overlap(truth, lists["base"]),
            fl.match_overlap(truth, lists["merged"]),
            fl.rows_per_flight(truth, lists["base"]),
            fl.rows_per_flight(truth, lists["merged"]),
        )
        _write(out, "airports_found.csv", both, argv, inputs, input_tables)
        sides = pd.concat(
            [
                fl.pair_table(
                    truth,
                    fl.match_callsign(truth, lists["base"]),
                    fl.match_callsign(truth, lists["merged"]),
                    "callsign",
                ),
                fl.pair_table(
                    truth,
                    fl.match_overlap(truth, lists["base"]),
                    fl.match_overlap(truth, lists["merged"]),
                    "overlap",
                ),
            ],
            ignore_index=True,
        )
        _write(out, "airports_by_side.csv", sides, argv, inputs, input_tables)

        # Distance flown of the flights with a hole.
        coords = airports.drop_duplicates("ident").set_index("ident")
        gc = great_circle_nm(
            truth["gt_adep"].map(coords["latitude_deg"]),
            truth["gt_adep"].map(coords["longitude_deg"]),
            truth["gt_ades"].map(coords["latitude_deg"]),
            truth["gt_ades"].map(coords["longitude_deg"]),
        )
        measures = {r: flight_measures(spark, tables[r]["tracks"], truth).toPandas() for r in RUNS}
        base_hole = measures["base"].set_index("flight_key")["holes"] > 0
        rows = []
        for r in RUNS:
            m = (
                truth[["flight_key", "flow", "ref_nm"]]
                .assign(gc_nm=gc.values)
                .merge(measures[r], on="flight_key", how="left")
            )
            m["run"] = r
            m["base_hole"] = m["flight_key"].map(base_hole).fillna(False).astype(bool)
            rows.append(m)
        per_flight_d = pd.concat(rows, ignore_index=True)
        _write(out, "distance.csv", distance_table(per_flight_d), argv, inputs, input_tables)
        _write(out, "flight_gaps.csv", flight_gap_table(per_flight_d), argv, inputs, input_tables)
        _write(out, "flights.csv", per_flight_d, argv, inputs, input_tables)

        # Events.
        icao = (
            tables["base"]["tracks"]
            .select("track_id", "icao24")
            .distinct()
            .unionByName(tables["merged"]["tracks"].select("track_id", "icao24").distinct())
            .distinct()
        )
        holes = base_holes_frame(gaps["base"]).cache()
        matched = match_events(
            tables["base"]["events"], tables["merged"]["events"], icao, holes
        ).cache()
        status = matched.groupBy("type", "in_base_hole", "status").agg(F.count(F.lit(1)).alias("n"))
        moved = (
            matched.where(F.col("status") == "moved")
            .groupBy("type", "in_base_hole")
            .agg(
                F.percentile_approx(F.abs("shift_s"), 0.5).alias("median_shift_s"),
                F.percentile_approx(F.abs("shift_s"), 0.9).alias("p90_shift_s"),
                F.max(F.abs("shift_s")).alias("max_shift_s"),
            )
        )
        ev = events_table(status.toPandas(), moved.toPandas())
        _write(out, "events_by_type.csv", ev, argv, inputs, input_tables)
        shifts = matched.where(F.col("status") == "moved").select("type", "shift_s").toPandas()
        shifts["shift_class"] = shift_class(shifts["shift_s"])
        shift_counts = shifts.groupby(["type", "shift_class"]).size().rename("n").reset_index()
        _write(out, "events_moved.csv", shift_counts, argv, inputs, input_tables)

        # FIR crossings.
        fir_rows = []
        for r in RUNS:
            x = (
                tables[r]["events"]
                .where(F.col("type") == "xing-fir")
                .select(
                    F.get_json_object("info", "$.fir_to").alias("fir_to"),
                    F.get_json_object("info", "$.bracket_s").cast("double").alias("bracket_s"),
                )
                .groupBy("fir_to")
                .agg(
                    F.count(F.lit(1)).alias("crossings"),
                    F.sum(
                        F.when(F.col("bracket_s") > FIR_BRACKET_LIMIT_SECONDS, 1).otherwise(0)
                    ).alias("bracket_over_limit"),
                    F.percentile_approx("bracket_s", 0.5).alias("median_bracket_s"),
                )
                .toPandas()
                .assign(run=r)
            )
            fir_rows.append(x)
        _write(out, "fir_crossings.csv", fir_table(pd.concat(fir_rows)), argv, inputs, input_tables)

        # Plausibility checks.
        rej = pd.concat(
            [
                tables[r]["rejected"]
                .groupBy("reject_rule", "type")
                .count()
                .toPandas()
                .rename(columns={"count": "n"})
                .assign(run=r)
                for r in RUNS
            ],
            ignore_index=True,
        )
        kept = pd.concat(
            [
                tables[r]["events"]
                .groupBy("type")
                .count()
                .toPandas()
                .rename(columns={"count": "n"})
                .assign(run=r)
                for r in RUNS
            ],
            ignore_index=True,
        )
        _write(
            out, "plausibility_removed.csv", rejected_table(rej, kept), argv, inputs, input_tables
        )

        # Processing time.
        timings_file = out / "timings.csv"
        if timings_file.is_file():
            _write(
                out,
                "processing_time.csv",
                timing_table(pd.read_csv(timings_file, dtype={"step": str})),
                argv,
                inputs,
                [],
            )
        print(json.dumps(inputs, indent=1, default=str))
    finally:
        spark.stop()
    return 0


# =============================================================================


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="command", required=True)

    r = sub.add_parser("run", help="run the pipeline for one day")
    r.add_argument("--day", required=True, help="YYYY-MM-DD")
    r.add_argument("--run", choices=("inputs", *RUNS, "all"), required=True)
    r.add_argument("--steps", nargs="+", default=None, help="a subset of the part's steps")
    r.add_argument("--executors", type=int, default=8)
    r.add_argument("--sample-mod", type=int, default=1, help="keep one aircraft in N")
    r.add_argument("--need-gb", type=float, default=NEED_GB, help="free space needed, GB")
    r.add_argument("--keep-tracks", action="store_true", help="keep each run's osn_tracks")
    r.add_argument(
        "--no-drop-inputs",
        dest="drop_inputs",
        action="store_false",
        help="keep the ADS-B input after both runs have built their tracks",
    )
    r.add_argument("--results-dir", default=None, help="where timings.csv is written")

    c = sub.add_parser("compare", help="compare the two runs of one day")
    c.add_argument("--day", required=True, help="YYYY-MM-DD")
    c.add_argument("--executors", type=int, default=4)
    c.add_argument("--local", action="store_true", help="Spark on the driver only")
    c.add_argument("--results-dir", default=None)

    k = sub.add_parser("clean", help="list or delete the study's tables")
    k.add_argument("--part", choices=tuple(CLEAN_PARTS), default="all")
    k.add_argument("--yes", action="store_true", help="delete (otherwise only list)")

    args = ap.parse_args(argv)
    code = 1
    try:
        code = {"run": cmd_run, "compare": cmd_compare, "clean": cmd_clean}[args.command](args)
    finally:
        print(f"run exit {code}", flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
