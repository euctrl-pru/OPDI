"""
Rebuild the state-vector sample and the tracks derived from it.

Used when the *sampler* changes, which is the one case where a normal
incremental run is wrong: the existing rows were produced by a different rule
and must be replaced, not added to.

Both ``osn_statevectors_v2`` and ``osn_tracks`` are written with the default
append mode, because that is correct for the daily pipeline -- each run adds
days the table does not have. For a rebuild it is exactly wrong: re-running the
same days would double the table, and a doubled table does not fail. It just
weights every aggregate that reads it, silently. So the first write to each
table here is forced to ``overwrite`` and subsequent writes append, which
replaces the table once and then extends it.

Both tables are unpartitioned, so an overwrite replaces the whole table. That
is checked before anything is written rather than assumed.

The progress logs are cleared first, since they exist precisely to stop a day
being processed twice.

    python benchmarks/rebuild_sample.py --start 2025-06-05 --end 2025-06-07
"""

import argparse
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))
sys.path.insert(0, str(REPO / "src"))

import osn_sample
from osn_sample import build_spark, load_dotenv

#: Progress logs that would otherwise skip the days being rebuilt.
LOGS = [
    "OPDI_live/logs/01_osn_statevectors_etl.log",
    "OPDI_live/logs/02_osn-tracks-etl-log.parquet",
    "OPDI_live/logs/03_osn-endpoint_candidates-etl-log.parquet",
    "OPDI_live/logs/03_osn-flight_table-etl-log-v2.parquet",
]


def force_first_write_overwrite(tables):
    """Make the first write to each named table replace it.

    Scoped to the named tables and to the first write only. A blanket
    overwrite would turn a multi-day loop into "keep the last day", which is a
    worse failure than the one being fixed because the table still looks
    populated.
    """
    from opdi.utils.storage import StorageManager

    orig = StorageManager.write_table
    seen = set()

    def write_table(self, df, table_name, *a, **kw):
        if table_name in tables and table_name not in seen:
            seen.add(table_name)
            kw["mode"] = "overwrite"
            print(f"  -> REPLACING {table_name} (first write of the rebuild)")
        return orig(self, df, table_name, *a, **kw)

    StorageManager.write_table = write_table


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", default="2025-06-05")
    ap.add_argument("--end", default="2025-06-07")
    ap.add_argument("--results-dir", default=None, help="unused; accepted so "
                    "the regeneration chain can call this like any job")
    ap.add_argument("--executors", type=int, default=10)
    ap.add_argument("--ui-port", type=int, default=4054)
    ap.add_argument("--skip-ingest", action="store_true")
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would be replaced and exit")
    args = ap.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
    load_dotenv()

    import provenance as pv

    # --start/--end are INCLUSIVE here, matching how every other job in this
    # study names its days. `ingest_from_s3` takes an *exclusive* end, so the
    # extra day is added at the call site rather than left to the caller to
    # remember. Passing the last day straight through ingests one day fewer,
    # which does not fail -- it just makes the sample smaller than every figure
    # it will be compared against.
    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_inclusive = datetime.strptime(args.end, "%Y-%m-%d").date()
    ingest_end = end_inclusive + timedelta(days=1)
    n_days = (ingest_end - start).days

    print("=== targets, before ===")
    for t in ("osn_statevectors_v2", "osn_tracks"):
        i = pv.s3_identity(f"s3a://eurocontrol/opdi/{t}")
        print(f"  {t:<24}{i.get('objects', 0):>6} objects  "
              f"{i.get('bytes', 0) / 1e9:>7.2f} GB")
    if args.dry_run:
        print("\ndry run: nothing written")
        return

    for lg in LOGS:
        p = REPO / lg
        if p.exists():
            (shutil.rmtree if p.is_dir() else Path.unlink)(p)
            print(f"  cleared {lg}")

    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(6, "10g", distributed=True)
    spark.sparkContext.setLogLevel("ERROR")

    from opdi.config import OPDIConfig
    from opdi.ingestion.osn_statevectors import StateVectorIngestion
    from opdi.pipeline.tracks import TrackProcessor

    cfg = OPDIConfig.for_environment("opensky")
    print(f"\ndecimation rule: {cfg.ingestion.decimation}")
    force_first_write_overwrite({"osn_statevectors_v2", "osn_tracks"})

    if not args.skip_ingest:
        print(f"\n=== 01 ingest === {n_days} day(s): {start} .. "
              f"{end_inclusive} (exclusive end {ingest_end})")
        StateVectorIngestion(spark, cfg).ingest_from_s3(
            start_date=start, end_date=ingest_end)

    print("\n=== 02 tracks ===")
    # process_date_range iterates *months*, not days. Passing day-level dates
    # happens to work because both fall in one month, but naming the month is
    # what the function actually means.
    month = start.replace(day=1)
    tp = TrackProcessor(spark, cfg)
    tp.create_table_if_not_exists()
    tp.process_date_range(month, month)

    print("\n=== targets, after ===")
    for t in ("osn_statevectors_v2", "osn_tracks"):
        i = pv.s3_identity(f"s3a://eurocontrol/opdi/{t}")
        print(f"  {t:<24}{i.get('objects', 0):>6} objects  "
              f"{i.get('bytes', 0) / 1e9:>7.2f} GB")
    spark.stop()


if __name__ == "__main__":
    main()
