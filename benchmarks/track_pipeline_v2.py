"""Benchmark a segmentation by running the **real pipeline** on it.

Version 1 of this study scored segmentations in a harness: it read a track
table someone else had built, re-partitioned the samples, and compared the
partitions. That measures the algorithm but not the pipeline -- and the two
differ in ways that turned out to matter. V1's own baseline could not
reproduce the published ``track_id`` (the ids were built from raw altitudes,
the harness read cleaned ones), and every arm's ADEP/ADES number came from a
flight list fed by a redirect rather than by step 02.

This module runs the steps instead. For each method it executes

    step 02   TrackProcessor.process_month   -> osn_tracks
    step 02a  TrackCleaner.process_month     -> osn_tracks_clean
    step 03   FlightListProcessor            -> opdi_flight_list  (two arms only)

with ``config.segmentation.method`` set, then scores the result: against
Network Manager ground truth as a clustering comparison -- airborne and
gate-to-gate both, see :data:`FLIGHT_LIST_METHODS`'s neighbour comment and
``score_segmentation`` -- and, for the two arms in :data:`FLIGHT_LIST_METHODS`,
on ADEP/ADES. The only thing that differs between methods is that one config
field.

**Step 03 runs for two of the three arms.** ``airframe_only`` is the
segmentation ablation's midpoint: ``standard`` is ``airframe_only`` plus the
callsign-change break, and the point of running the midpoint is to split that
total into the two changes that produced it -- a question the *segmentation*
score answers on its own. ADEP/ADES asks a different question ("does shipping
this change ADEP/ADES") that only needs the before and the after, so
``airframe_only`` gets segmentation scoring, cleaning, null-rates and extents,
but not the flight list.

**Nothing production is touched.** Every table name is redirected under
``research/tcv2/<method>/`` by patching ``StorageManager._s3_path``, and a
write guard refuses anything landing outside ``research/``. The guard checks
the resolved path rather than the table name, because the redirect means the
two differ -- a lesson from v1, where guarding the name rejected legitimate
redirected writes and caught a genuine production write only by luck.

**One method at a time, and deleted after scoring.** A materialised track table
is ~3.4 GB per day and its cleaned copy ~3.3 GB, and the bucket is shared with
another project. Streaming -- build, score, delete, next -- is what keeps the
peak at one method's worth rather than three, and it is kept even now that the
quota is known to be 200 GB rather than 100, because the peak is what a
concurrent run of somebody else's job has to fit alongside.

Because no two methods ever coexist on S3, anything the study needs to compare
*across* methods has to be summarised to a file before the cleanup. That is
what :func:`export_track_extents` is for.

    python benchmarks/track_pipeline_v2.py --period 2025 --days 2025-06-05 \\
        --methods legacy airframe_only standard \\
        --results-dir ../opdi-portal/papers/track-construction-v2/data
"""

import argparse
import csv
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

import adep_ades  # noqa: E402
import osn_sample  # noqa: E402
import provenance  # noqa: E402
import track_diagnostics  # noqa: E402
import track_truth  # noqa: E402
from flight_list_v7 import load_predictions  # noqa: E402
from osn_sample import build_spark, load_dotenv  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from track_continuity import extents_name  # noqa: E402
from track_methods import BUCKET, BUCKET_QUOTA_GB, s3_client  # noqa: E402
from track_score import score_arm_gated, track_extents  # noqa: E402

from opdi.config import OPDIConfig  # noqa: E402

#: Every table the run materialises, in the order the steps write them.
#:
#: `opdi_endpoint_candidates` is here because step 03 writes it, not because
#: anything reads it afterwards -- and it is written mode="overwrite". Left out
#: of the redirect it resolves to the production cache, so a run would delete
#: real data and then feed each arm the previous arm's candidates. The write
#: guard caught exactly that on the first end-to-end attempt. A table this list
#: forgets is a table the guard has to stop, and the guard stops the whole run.
TABLES = ("osn_tracks", "osn_tracks_clean", "opdi_flight_list",
          "opdi_endpoint_candidates")

RESEARCH_ROOT = "research/tcv2"

#: Comment 7: ADEP/ADES for the before and the after only. `airframe_only` is
#: the segmentation ablation's midpoint and has no downstream question of its
#: own, so it skips step 03 -- the expensive step.
FLIGHT_LIST_METHODS = ["legacy", "standard"]

#: State vectors are step 02's input and do not depend on the segmentation, so
#: they are ingested once and shared by every method rather than rebuilt per
#: method. Sharing is also what makes the comparison honest: both methods
#: segment the *same* bytes, so a difference between them cannot be an artefact
#: of two ingests that saw the upstream feed at different moments.
SHARED_SV = f"{RESEARCH_ROOT}/_shared/osn_statevectors_v2"
SV_TABLE = "osn_statevectors_v2"

PERIODS = {
    "2025": {"month": date(2025, 6, 1),
             "days": ["2025-06-05", "2025-06-06", "2025-06-07"],
             "months": ["202506"]},
    "2024": {"month": date(2024, 6, 1),
             "days": ["2024-06-05", "2024-06-06", "2024-06-07"],
             "months": ["202406"]},
}


def table_for(method: str, name: str) -> str:
    """Storage-relative table this run uses in place of *name*."""
    return f"{RESEARCH_ROOT}/{method}/{name}"


def redirect_tables(method: str) -> None:
    """Point the pipeline's three tables at this method's own copies.

    ``_s3_path`` rather than the table name: ``table_ref`` registers a Spark
    temp view named after the table, and ``research/tcv2/standard/osn_tracks``
    is not a legal SQL identifier. Mapping the path leaves the view names alone
    and moves only where the bytes are.

    Re-entrant by design -- called once per method, and each call replaces the
    previous mapping rather than stacking on it.
    """
    from opdi.utils.storage import StorageManager

    orig = getattr(StorageManager, "_tcv2_orig_s3_path", None)
    if orig is None:
        orig = StorageManager._s3_path
        StorageManager._tcv2_orig_s3_path = orig

    mapping = {name: table_for(method, name) for name in TABLES}
    mapping[SV_TABLE] = SHARED_SV

    def _s3_path(self, name, *a, **kw):
        return orig(self, mapping.get(name, name), *a, **kw)

    StorageManager._s3_path = _s3_path
    print(f"  tables redirected under {RESEARCH_ROOT}/{method}/")


def guard_writes(allowed_prefix: str = "research/") -> None:
    """Refuse any write that would land outside ``research/``.

    Checks the resolved destination, not the table name. With the redirect
    active the two differ -- a write of ``osn_tracks`` lands under
    ``research/tcv2/...`` -- so guarding the name would reject every legitimate
    write here while catching a real production write only because the
    production table happens not to be called ``research/``-anything.
    """
    from opdi.utils.storage import StorageManager

    if getattr(StorageManager, "_tcv2_guarded", False):
        return
    orig_write = StorageManager.write_table

    def write_table(self, df, table_name, *a, **kw):
        name = str(table_name)
        dest = self._s3_path(name)
        allowed_root = f"{self.base_path}/{allowed_prefix}"
        if not (name.startswith(allowed_prefix) or dest.startswith(allowed_root)):
            raise RuntimeError(
                f"refusing to write {name!r} (resolves to {dest!r}): this "
                f"benchmark writes only under {allowed_prefix!r}")
        print(f"  -> writing {name} -> {dest}")
        return orig_write(self, df, table_name, *a, **kw)

    StorageManager.write_table = write_table
    StorageManager._tcv2_guarded = True


def delete_prefix(s3, prefix: str) -> tuple:
    """Delete every object under *prefix*. Returns (objects, bytes).

    Single-object deletes: the OpenSky endpoint rejects batch ``DeleteObjects``
    with ``MissingContentMD5``. Every key is re-checked against the prefix
    before it goes, and the prefix itself must sit under this study's own root
    -- the bucket holds another project's data and there is no undo.
    """
    root = f"opdi/{RESEARCH_ROOT}/"
    if not prefix.startswith(root):
        raise ValueError(f"refusing to delete outside {root}: {prefix!r}")
    n = freed = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            key = o["Key"]
            assert key.startswith(prefix), f"refusing to delete outside {prefix}: {key}"
            s3.delete_object(Bucket=BUCKET, Key=key)
            n += 1
            freed += o["Size"]
    return n, freed


def delete_method(s3, method: str) -> tuple:
    """Delete the three tables this method wrote, leaving the shared slice."""
    return delete_prefix(s3, f"opdi/{RESEARCH_ROOT}/{method}/")


def bucket_free_gb(s3) -> float:
    """Quota minus measured usage.

    The quota comes from ``track_methods.BUCKET_QUOTA_GB`` rather than a
    literal here. It was a literal ``100.0``, and when the owner raised the
    real quota to 200 GB that stale copy made a bucket holding 98.5 GB report
    1.5 GB free -- so the run aborted at its free-space gate, before doing any
    work, with a message that reads as a full bucket rather than as a constant
    nobody updated twice. One definition, in the module that documents it.
    """
    total = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET):
        for o in page.get("Contents", []):
            total += o["Size"]
    return BUCKET_QUOTA_GB - total / 1e9


def sv_exists(s3, days) -> bool:
    """Has the shared state-vector slice already been ingested?"""
    r = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"opdi/{SHARED_SV}/", MaxKeys=2)
    return r.get("KeyCount", 0) > 0


def ingest_statevectors(spark, cfg, days) -> None:
    """Step 01, over just the sampled days, into the shared slice.

    Reads the hourly partitions under
    ``s3a://opensky-hdfs-backup/tables_v4/state_vectors`` -- the same upstream
    source the production ingest reads -- applies the pipeline's own bounding
    box and column mapping, and writes ``osn_statevectors_v2``, which the
    redirect has pointed at the shared slice.

    This is why an end-to-end run is possible at all: ``osn_statevectors_v2``
    is not resident in our bucket, so the first attempt at step 02 failed with
    PATH_NOT_FOUND. Materialising a month of it would be ~100 GB; a day of the
    ECAC box is a few.
    """
    from opdi.ingestion.osn_statevectors import StateVectorIngestion

    start = date.fromisoformat(min(days))
    end = date.fromisoformat(max(days)) + timedelta(days=1)
    ing = StateVectorIngestion(spark, cfg)
    ing.create_table_if_not_exists()
    ing.ingest_from_s3(start, end)


def build_tracks(spark, cfg, period, days) -> None:
    """Step 02, over just the sampled days."""
    from opdi.pipeline.tracks import TrackProcessor

    start = f"{min(days)} 00:00:00"
    end = f"{(date.fromisoformat(max(days)) + timedelta(days=1))} 00:00:00"
    proc = TrackProcessor(spark, cfg)
    proc.create_table_if_not_exists()
    # skip_if_processed=False: the month log lives with the pipeline, and a
    # second method must not be skipped because the first recorded the month.
    proc.process_month(period["month"], skip_if_processed=False,
                       window=(start, end))


def clean_tracks(spark, cfg, period) -> None:
    """Step 02a. The month filter is harmless -- the table holds only our days."""
    from opdi.cleaning.cleaner import TrackCleaner

    cleaner = TrackCleaner(spark, cfg)
    cleaner.create_table_if_not_exists()
    cleaner.process_month(period["month"], skip_if_processed=False)


def build_flight_list(spark, cfg, period, airports_hex_path) -> None:
    """Step 03.

    ``skip_if_processed=False`` for the same reason steps 02 and 02a pass it,
    and this call was missing it. The processed-month log is a **local parquet
    file** under ``OPDI_live/logs/``, so it is untouched by the per-method S3
    cleanup in ``main``'s ``finally`` -- the marker outlives the table it
    describes. Two ways that breaks a run, and this study hit the second:

    * across methods, ``legacy`` records 2025-06 and ``standard`` then skips
      its own flight list entirely;
    * across runs, a month recorded by any earlier invocation -- including one
      that later died -- makes every future run skip step 03 while its tables
      have long since been deleted, so ``score_adep_ades`` reads a path that
      does not exist and the run dies with ``PATH_NOT_FOUND``.

    Progress tracking is right for the production pipeline, whose tables
    persist. It is wrong here, where each arm's tables live and die inside one
    method's iteration, so the skip is disabled rather than the log
    redirected: there is no state worth resuming onto.
    """
    from opdi.pipeline.flights import FlightListProcessor

    proc = FlightListProcessor(spark, cfg)
    proc.create_table_if_not_exists()
    proc.process_date_range(
        period["month"], period["month"], airports_hex_path,
        skip_if_processed=False,
    )


def read_cleaned(spark, method, days):
    """This method's ``osn_tracks_clean``, restricted to the sampled days.

    One definition for the three things that read it -- the clustering score,
    the extents export and the null-rate report -- so that all three describe
    the same population. Filtering on ``to_date(event_time)`` rather than a
    partition path because the table is not day-partitioned on disk; see
    ``track_methods.PERIODS``.
    """
    return (
        spark.read.parquet(f"s3a://{BUCKET}/opdi/{table_for(method, 'osn_tracks_clean')}")
        .filter(F.to_date("event_time").isin(days))
    )


def export_track_extents(spark, method, period, days, results_dir) -> dict:
    """One row per track -- ``track_id, icao24, t_start, t_end, n_points``.

    **This has to happen before the per-method cleanup, and that is the whole
    reason it exists.** The runner never lets two methods' tables coexist on
    S3, so by the time ``standard`` has been built there is nothing of
    ``legacy`` left to compare it against. This summary is small enough to keep
    -- a few MB per arm against a few GB of table -- and it carries exactly
    what a cross-arm comparison needs. Without it the continuity question could
    not be answered at all without doubling the peak footprint.

    ``F.min("icao24")`` is exact rather than arbitrary: every arm in this study
    groups on ``icao24`` first, so a ``track_id`` belongs to one airframe by
    construction and the aggregate has one value to choose from. If an arm ever
    grouped otherwise this would silently start picking, which is why it is
    said here.

    Returns the totals for the caller's row, so the sample count in the CSV and
    the denominator behind the null rates come from one pass rather than two
    that could disagree.
    """
    ext = (
        read_cleaned(spark, method, days)
        .groupBy("track_id")
        .agg(
            F.min("icao24").alias("icao24"),
            F.date_format(F.min("event_time"), "yyyy-MM-dd HH:mm:ss").alias("t_start"),
            F.date_format(F.max("event_time"), "yyyy-MM-dd HH:mm:ss").alias("t_end"),
            F.count(F.lit(1)).alias("n_points"),
        )
        .orderBy("track_id")
    )
    fields = ["track_id", "icao24", "t_start", "t_end", "n_points"]
    rows = ext.collect()

    results_dir.mkdir(parents=True, exist_ok=True)
    out = results_dir / extents_name(method, period)
    with out.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r[k] for k in fields})

    n_samples = sum(r["n_points"] for r in rows)
    print(f"  extents: {len(rows)} tracks, {n_samples} samples -> {out.name}")
    return {"n_tracks_exported": len(rows), "n_samples": n_samples}


def score_segmentation(spark, method, period, days) -> dict:
    """Clustering comparison of the pipeline's own cleaned tracks.

    Comment 4: every arm is scored twice -- airborne (``[t_off, t_land]``) and
    gate-to-gate (``t_off_block``/``t_in_block``) -- so the row also carries
    the ``gate_*`` columns. The airborne interval never sees a sample at the
    stand or on a taxiway, so it cannot say whether taxi-out and taxi-in
    samples landed in the right track; the gate interval can, because it is a
    superset. See ``track_score.score_arm_gated`` for what that superset can do
    to the rate (it is not guaranteed to move in either direction) and
    ``track_truth.overlap_join`` for the ``bounds`` argument that selects it.

    ``load_flight_intervals`` is Task 11's version, which carries
    ``t_off_block``/``t_in_block`` on every row; if a future ``track_truth.py``
    ever dropped them this join would fail loudly rather than silently score
    the gate interval as a second copy of the airborne one.

    Unlike ``track_methods.py``'s V1 arms job, this call site has no
    ``run_arm``-style scorer hook to bind against -- ``score_segmentation`` is
    called directly, with ``assign``/``gt`` already in scope -- so there is no
    arity trap to close over here; ``matched_gate`` is simply built next to
    ``matched`` and handed to :func:`track_score.score_arm_gated` by name,
    which is the same pattern ``ff38531`` wired for V1's ``_score_with_gate``.
    """
    assign = read_cleaned(spark, method, days).select(
        "icao24", "event_time", "track_id")
    gt = track_truth.load_flight_intervals(spark, period["months"], days)
    extents = track_extents(assign)
    matched = track_truth.overlap_join(assign, gt)
    matched_gate = track_truth.overlap_join(
        assign, gt, bounds=("t_off_block", "t_in_block"))
    return score_arm_gated(matched, extents, matched_gate)


def score_adep_ades(spark, method, period, days, k) -> dict:
    """ADEP/ADES of the flight list this run's own steps produced."""
    pred, ident = load_predictions(spark, table_for(method, "opdi_flight_list"))
    gt = adep_ades.load_ground_truth(spark, period["months"], days)
    return adep_ades.score(pred, ident, gt, k=k)


def write_rows_csv(path: Path, rows: list) -> None:
    """(Re)write *path* from *rows*, header = the union of every row's keys.

    Comment 7's trap: ``airframe_only`` never runs :func:`score_adep_ades`, so
    its row is missing every ADEP/ADES key that a ``FLIGHT_LIST_METHODS`` row
    carries. A header taken from whichever row happens to run first is not
    safe either way -- a flight-list-method row first means the ADEP/ADES
    columns silently read as blank for every other row that lacks them (not
    wrong, but only right by the accident of ``legacy`` being first in
    practice), and an ``airframe_only``-first run crashes outright the moment
    ``csv.DictWriter`` sees a row with keys its header never declared. Taking
    the header from the union of every row collected so far -- recomputed and
    the whole file rewritten after each arm -- makes the result independent of
    ``--methods`` order. Rewriting a handful of rows after every arm is
    negligible next to the tens of minutes the arm itself took, so there is no
    cost to buying that independence back this way rather than pre-declaring
    the columns statically -- which would silently stop matching this module's
    scorers the day one of them adds or renames a field.
    """
    fieldnames = sorted({k for row in rows for k in row})
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--period", choices=sorted(PERIODS), default="2025")
    ap.add_argument("--methods", nargs="+",
                    default=["legacy", "airframe_only", "standard"],
                    help="in ablation order. `standard` is `airframe_only` "
                         "plus the callsign-change break, so running the "
                         "middle arm is what splits the total gain into the "
                         "two changes that produced it; drop it and the study "
                         "reports a sum it cannot decompose")
    ap.add_argument("--days", nargs="+", default=None,
                    help="override the period's day list; one day is ~6.7 GB "
                         "of materialised tables, three is ~20 GB")
    ap.add_argument("--results-dir", type=Path, required=True)
    ap.add_argument("--out-name", default=None)
    ap.add_argument("--executors", type=int, default=10)
    ap.add_argument("--ui-port", type=int, default=4066)
    ap.add_argument("--k", type=float, default=2.0)
    ap.add_argument("--airports-hex-path",
                    default="data/airport_hex/zones_res7_processed.parquet")
    ap.add_argument("--keep", action="store_true",
                    help="do not delete a method's tables after scoring it")
    ap.add_argument("--keep-sv", action="store_true",
                    help="leave the shared state-vector slice behind so a "
                         "follow-up run skips the ingest; costs a few GB on a "
                         "shared bucket until removed")
    ap.add_argument("--min-free-gb", type=float, default=8.0,
                    help="abort before a method if less than this is free")
    args = ap.parse_args()

    out_name = args.out_name or f"pipeline_{args.period}.csv"
    sys.stdout.reconfigure(line_buffering=True)
    load_dotenv()
    s3 = s3_client()

    period = PERIODS[args.period]
    days = args.days or period["days"]

    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(6, "9g")
    spark.sparkContext.setLogLevel("ERROR")
    guard_writes()

    args.results_dir.mkdir(parents=True, exist_ok=True)
    out = args.results_dir / out_name
    rows = []

    try:
        for method in args.methods:
            free = bucket_free_gb(s3)
            print(f"\n=== {method} ({args.period}, {len(days)} day(s)) === "
                  f"{free:.2f} GB free")
            if free < args.min_free_gb:
                raise SystemExit(
                    f"only {free:.2f} GB free, need {args.min_free_gb:.2f} -- "
                    f"a method materialises ~6.7 GB per day and a write that "
                    f"exceeds quota fails after the work is done")

            cfg = OPDIConfig.for_environment("opensky")
            cfg.segmentation.method = method
            redirect_tables(method)

            # Once, before the first method. Both methods then segment exactly
            # the same state vectors.
            if not sv_exists(s3, days):
                print("  ingesting shared state vectors (step 01)...")
                ingest_statevectors(spark, cfg, days)

            started = datetime.utcnow()
            try:
                build_tracks(spark, cfg, period, days)

                # **State vectors are dropped here, between step 02 and step
                # 02a, and the timing is the whole point.** Nothing downstream
                # reads them -- 02a reads osn_tracks, 03 reads
                # osn_tracks_clean -- so holding them costs 3.3 GB exactly
                # while cleaning needs room to write its own 3.3 GB.
                #
                # Keeping them through the run put the bucket at ~99.7 GB of a
                # 100 GB quota, and the write died partway through with
                # NoSuchUpload: the endpoint aborting a multipart upload it
                # could not complete. That reads as a transient S3 fault and is
                # not one -- it happened twice, at the same step, with the same
                # arithmetic behind it. Dropping them here takes the peak from
                # ~10 GB to ~6.8 GB.
                #
                # --keep-sv still works and is still useful for a *rerun* that
                # wants to skip the 23-minute ingest -- but it is the wrong
                # choice on a first run, which is how this was learned.
                if not (args.keep or args.keep_sv):
                    n, freed = delete_prefix(s3, f"opdi/{SHARED_SV}/")
                    print(f"  -- dropped state vectors before cleaning: "
                          f"{n} objects, {freed / 1e9:.2f} GB "
                          f"({bucket_free_gb(s3):.2f} GB now free)")

                clean_tracks(spark, cfg, period)

                # Same reasoning one step later: step 03 reads the cleaned
                # table, so the raw one is dead weight from here.
                if not args.keep:
                    n, freed = delete_prefix(
                        s3, f"opdi/{table_for(method, 'osn_tracks')}/")
                    print(f"  -- dropped raw tracks before the flight list: "
                          f"{n} objects, {freed / 1e9:.2f} GB "
                          f"({bucket_free_gb(s3):.2f} GB now free)")

                row = {"method": method, "period": args.period,
                       "days": len(days)}
                row.update(score_segmentation(spark, method, period, days))

                # Comment 7: step 03 -- the expensive step -- and its ADEP/ADES
                # score run only for the before and the after. `airframe_only`
                # is the segmentation ablation's midpoint and has no
                # downstream question of its own; its row simply has no
                # ADEP/ADES keys, which is what write_rows_csv's union header
                # is for.
                if method in FLIGHT_LIST_METHODS:
                    build_flight_list(spark, cfg, period, args.airports_hex_path)
                    row.update(score_adep_ades(spark, method, period, days, args.k))

                # Both of these read osn_tracks_clean, so both must run before
                # the `finally` below deletes it -- and after the flight-list
                # block above, so a method that fails at step 03 leaves no
                # half-summary claiming to describe a run that did not finish.
                row.update(export_track_extents(
                    spark, method, period, days, args.results_dir))
                row.update(track_diagnostics.null_rates(
                    read_cleaned(spark, method, days)))

                row["minutes"] = round(
                    (datetime.utcnow() - started).total_seconds() / 60.0, 1)
                rows.append(row)
                # Rewritten whole, not appended -- see write_rows_csv for why
                # a row-by-row appending writer is not safe once one arm's row
                # can have keys another arm's row does not.
                write_rows_csv(out, rows)
                for k, v in row.items():
                    print(f"  {k:24} {v}")
            finally:
                # In a finally: a method that dies mid-build must not leave
                # ~6.7 GB parked on a shared bucket, and the next method needs
                # the space more than this one needs its wreckage.
                if not args.keep:
                    n, freed = delete_method(s3, method)
                    print(f"  -- deleted {n} objects ({freed / 1e9:.2f} GB) "
                          f"for {method}")
    finally:
        spark.stop()
        # The shared slice outlives the per-method loop by design -- both
        # methods read it -- so it is dropped here, after the last one.
        # --keep-sv leaves it for a follow-up run to reuse, which saves the
        # ingest but keeps several GB resident on a shared bucket.
        if not (args.keep or args.keep_sv):
            n, freed = delete_prefix(s3, f"opdi/{SHARED_SV}/")
            print(f"-- deleted {n} shared state-vector objects "
                  f"({freed / 1e9:.2f} GB)")

    if rows:
        provenance.record(
            args.results_dir, out_name,
            script="benchmarks/track_pipeline_v2.py", argv=sys.argv[1:],
            code_paths=["benchmarks/track_pipeline_v2.py",
                        "benchmarks/track_truth.py", "benchmarks/track_score.py",
                        "benchmarks/adep_ades.py", "benchmarks/osn_sample.py",
                        "benchmarks/flight_list_v7.py",
                        # null_rates lives here, and its output is in the row.
                        "benchmarks/track_diagnostics.py",
                        # extents_name lives here, so it decides the filename
                        # the continuity job goes looking for.
                        "benchmarks/track_continuity.py",
                        "src/opdi/ingestion/osn_statevectors.py",
                        "src/opdi/pipeline/tracks.py",
                        "src/opdi/cleaning/cleaner.py",
                        "src/opdi/cleaning/native.py",
                        "src/opdi/pipeline/flights.py",
                        # __init__ decides which implementation
                        # `assign_track_id` resolves to, so it can change every
                        # number here while base.py and methods.py stay
                        # byte-identical.
                        "src/opdi/pipeline/segmentation/__init__.py",
                        "src/opdi/pipeline/segmentation/base.py",
                        "src/opdi/pipeline/segmentation/methods.py",
                        "src/opdi/config.py"],
            notes=f"Real pipeline steps 02, 02a per method, 03 for "
                  f"{FLIGHT_LIST_METHODS} only. Scored airborne and "
                  f"gate-to-gate. days={days}.",
        )
        print(f"\n-> {out}")


if __name__ == "__main__":
    main()
