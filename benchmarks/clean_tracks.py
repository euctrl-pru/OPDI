"""
Run pipeline step 02a -- trajectory cleaning -- over a study period.

A thin wrapper around ``TrackCleaner`` so the regeneration chain can call it
like any other stage. It runs the pipeline's own cleaner rather than applying
``clean_tracks`` inline, because the whole point of wiring 02a in is that the
flight list reads what the pipeline step *produces*: an inline transform would
measure the cleaning rule while leaving the step itself still feeding nothing.

The second period is a research copy under ``research/tracks``, so the source
and target tables are redirected rather than the pipeline being edited. The
redirect is a wrapper around ``StorageManager``, which keeps it out of the
production code path entirely.

    python benchmarks/clean_tracks.py --period 2025
    python benchmarks/clean_tracks.py --period 2024
"""

import argparse
import sys
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))
sys.path.insert(0, str(REPO / "src"))

import osn_sample
from osn_sample import build_spark, load_dotenv

#: Each period names its own source and target. The 2024 tables live under
#: ``research/`` because that period is a study copy, not pipeline output.
PERIODS = {
    "2025": {
        "month": date(2025, 6, 1),
        "source": "osn_tracks",
        "target": "osn_tracks_clean",
        "log": "OPDI_live/logs/02a_track_cleaning.log",
    },
    "2024": {
        "month": date(2024, 6, 1),
        "source": "research/tracks",
        "target": "research/tracks_clean",
        "log": "OPDI_live/logs/02a_track_cleaning_2024.log",
    },
}


def redirect(source: str, target: str) -> None:
    """Point the cleaner's table names at this period's copies, and replace.

    Wrapping ``StorageManager`` rather than editing ``cleaner.py`` keeps the
    research period out of the production code path. Reads and writes are
    mapped by the *same* dictionary, so a source that is redirected and a target
    that is not cannot happen.

    The **first** write to the target is forced to ``overwrite``. ``cleaner.py``
    writes with the default mode, which is append -- correct for the daily
    pipeline, where each run adds a month the table does not have, and exactly
    wrong for a rebuild of the same month. Re-running would double the table,
    and a doubled table does not fail: it silently weights every aggregate that
    reads it. Only the first write, so a multi-month loop still extends rather
    than keeping only the last month.
    """
    from opdi.utils.storage import StorageManager
    from opdi.cleaning import cleaner as cl

    if getattr(StorageManager, "_v7_clean_redirect", False):
        return
    orig_read, orig_write = StorageManager.read_table, StorageManager.write_table
    mapping = {cl.SOURCE_TABLE: source, cl.TARGET_TABLE: target}
    replaced = set()

    def read_table(self, name, *a, **kw):
        return orig_read(self, mapping.get(name, name), *a, **kw)

    def write_table(self, df, name, *a, **kw):
        mapped = mapping.get(name, name)
        if mapped == target and target not in replaced:
            replaced.add(target)
            kw["mode"] = "overwrite"
            print(f"  -> REPLACING {mapped} (first write of this rebuild)")
        else:
            print(f"  -> writing {mapped}")
        return orig_write(self, df, mapped, *a, **kw)

    def table_ref(self, name, *a, **kw):
        return orig_ref(self, mapping.get(name, name), *a, **kw)

    orig_ref = StorageManager.table_ref
    StorageManager.read_table = read_table
    StorageManager.write_table = write_table
    StorageManager.table_ref = table_ref
    StorageManager._v7_clean_redirect = True


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--period", choices=sorted(PERIODS), default="2025")
    ap.add_argument("--results-dir", default=None,
                    help="unused; accepted so the chain can call this like a job")
    ap.add_argument("--executors", type=int, default=10)
    ap.add_argument("--ui-port", type=int, default=4056)
    ap.add_argument("--dry-run", action="store_true",
                    help="report sizes and the tables involved, then exit")
    args = ap.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
    load_dotenv()
    period = PERIODS[args.period]

    import provenance as pv

    print(f"period {args.period}: {period['source']} -> {period['target']}")
    for t in (period["source"], period["target"]):
        i = pv.s3_identity(f"s3a://eurocontrol/opdi/{t}")
        print(f"  before  {t:<24}{i.get('objects', 0):>6} objects  "
              f"{i.get('bytes', 0) / 1e9:>7.2f} GB")

    # Cleaning adds a second copy of the tracks. Said out loud before the write
    # rather than discovered as a failure partway through it.
    total = pv.s3_identity("s3a://eurocontrol/opdi")
    print(f"  bucket  {total.get('objects', 0):,} objects, "
          f"{total.get('bytes', 0) / 1e9:.2f} GB before this stage")
    if args.dry_run:
        print("\ndry run: nothing written")
        return

    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(6, "10g", distributed=True)
    spark.sparkContext.setLogLevel("ERROR")

    from opdi.config import OPDIConfig
    from opdi.cleaning.cleaner import TrackCleaner

    cfg = OPDIConfig.for_environment("opensky")
    print(f"\ncleaning enabled={cfg.cleaning.enabled} "
          f"pandas_stage={cfg.cleaning.enable_pandas_stage}")
    if not cfg.cleaning.enabled:
        raise SystemExit(
            "cleaning is disabled in the config, so this stage would write "
            "nothing and report success. Enable it or drop the stage.")

    redirect(period["source"], period["target"])
    c = TrackCleaner(spark, cfg, log_file_path=period["log"])
    c.create_table_if_not_exists()
    # process_month, not process_date_range: one month, and naming it is
    # clearer than an end bound that has to be exclusive.
    c.process_month(period["month"], skip_if_processed=False)

    for t in (period["source"], period["target"]):
        i = pv.s3_identity(f"s3a://eurocontrol/opdi/{t}")
        print(f"  after   {t:<24}{i.get('objects', 0):>6} objects  "
              f"{i.get('bytes', 0) / 1e9:>7.2f} GB")
    total = pv.s3_identity("s3a://eurocontrol/opdi")
    print(f"  bucket  {total.get('objects', 0):,} objects, "
          f"{total.get('bytes', 0) / 1e9:.2f} GB after")
    spark.stop()


if __name__ == "__main__":
    main()
