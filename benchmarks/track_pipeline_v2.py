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
    step 03   FlightListProcessor            -> opdi_flight_list

with ``config.segmentation.method`` set, then scores the result twice: against
Network Manager ground truth as a clustering comparison, and on ADEP/ADES. The
only thing that differs between methods is that one config field.

**Nothing production is touched.** Every table name is redirected under
``research/tcv2/<method>/`` by patching ``StorageManager._s3_path``, and a
write guard refuses anything landing outside ``research/``. The guard checks
the resolved path rather than the table name, because the redirect means the
two differ -- a lesson from v1, where guarding the name rejected legitimate
redirected writes and caught a genuine production write only by luck.

**One method at a time, and deleted after scoring.** A materialised track table
is ~3.4 GB per day and its cleaned copy ~3.3 GB; the bucket has single-digit GB
free and is shared with another project. Two methods resident at once does not
fit, so this streams: build, score, delete, next.

    python benchmarks/track_pipeline_v2.py --period 2025 --days 2025-06-05 \\
        --methods legacy standard \\
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
import track_truth  # noqa: E402
from flight_list_v7 import load_predictions  # noqa: E402
from osn_sample import build_spark, load_dotenv  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from track_methods import BUCKET, s3_client  # noqa: E402
from track_score import score_arm, track_extents  # noqa: E402

from opdi.config import OPDIConfig  # noqa: E402

#: The three tables a run materialises, in the order the steps write them.
TABLES = ("osn_tracks", "osn_tracks_clean", "opdi_flight_list")

RESEARCH_ROOT = "research/tcv2"

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


def delete_method(s3, method: str) -> tuple:
    """Delete every object this method wrote. Returns (objects, bytes).

    Single-object deletes: the OpenSky endpoint rejects batch ``DeleteObjects``
    with ``MissingContentMD5``. Every key is re-checked against this method's
    own prefix first -- the bucket holds another project's data and there is no
    undo.
    """
    prefix = f"opdi/{RESEARCH_ROOT}/{method}/"
    n = freed = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            key = o["Key"]
            assert key.startswith(prefix), f"refusing to delete outside {prefix}: {key}"
            s3.delete_object(Bucket=BUCKET, Key=key)
            n += 1
            freed += o["Size"]
    return n, freed


def bucket_free_gb(s3) -> float:
    total = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET):
        for o in page.get("Contents", []):
            total += o["Size"]
    return 100.0 - total / 1e9


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
    """Step 03."""
    from opdi.pipeline.flights import FlightListProcessor

    proc = FlightListProcessor(spark, cfg)
    proc.create_table_if_not_exists()
    proc.process_date_range(period["month"], period["month"], airports_hex_path)


def score_segmentation(spark, method, period, days) -> dict:
    """Clustering comparison of the pipeline's own cleaned tracks."""
    assign = (
        spark.read.parquet(f"s3a://{BUCKET}/opdi/{table_for(method, 'osn_tracks_clean')}")
        .select("icao24", "event_time", "track_id")
        .filter(F.to_date("event_time").isin(days))
    )
    gt = track_truth.load_flight_intervals(spark, period["months"], days)
    extents = track_extents(assign)
    matched = track_truth.overlap_join(assign, gt)
    return score_arm(matched, extents)


def score_adep_ades(spark, method, period, days, k) -> dict:
    """ADEP/ADES of the flight list this run's own steps produced."""
    pred, ident = load_predictions(spark, table_for(method, "opdi_flight_list"))
    gt = adep_ades.load_ground_truth(spark, period["months"], days)
    return adep_ades.score(pred, ident, gt, k=k)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--period", choices=sorted(PERIODS), default="2025")
    ap.add_argument("--methods", nargs="+", default=["legacy", "standard"])
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
    rows, writer, fh = [], None, out.open("w", newline="")

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

            started = datetime.utcnow()
            try:
                build_tracks(spark, cfg, period, days)
                clean_tracks(spark, cfg, period)
                build_flight_list(spark, cfg, period, args.airports_hex_path)

                row = {"method": method, "period": args.period,
                       "days": len(days)}
                row.update(score_segmentation(spark, method, period, days))
                row.update(score_adep_ades(spark, method, period, days, args.k))
                row["minutes"] = round(
                    (datetime.utcnow() - started).total_seconds() / 60.0, 1)
                rows.append(row)

                if writer is None:
                    writer = csv.DictWriter(fh, fieldnames=sorted(row))
                    writer.writeheader()
                writer.writerow(row)
                fh.flush()
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
        fh.close()
        spark.stop()

    if rows:
        provenance.record(
            args.results_dir, out_name,
            script="benchmarks/track_pipeline_v2.py", argv=sys.argv[1:],
            code_paths=["benchmarks/track_pipeline_v2.py",
                        "benchmarks/track_truth.py", "benchmarks/track_score.py",
                        "benchmarks/adep_ades.py",
                        "src/opdi/pipeline/tracks.py",
                        "src/opdi/cleaning/cleaner.py",
                        "src/opdi/cleaning/native.py",
                        "src/opdi/pipeline/flights.py",
                        "src/opdi/pipeline/segmentation/base.py",
                        "src/opdi/pipeline/segmentation/methods.py",
                        "src/opdi/config.py"],
            notes=f"Real pipeline steps 02, 02a, 03 per method. days={days}.",
        )
        print(f"\n-> {out}")


if __name__ == "__main__":
    main()
