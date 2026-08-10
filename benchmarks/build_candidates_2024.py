"""
Build the endpoint candidate cache for the second period.

The endpoint sweeps run over a cached candidate table, which exists for 2025
and not for 2024 -- so the values that actually ship for departures have never
been checked on a second period, while the trend values, which were harder
fought, have been. That is the wrong way round, and this closes it.

The obstacle is that the 2024 tracks pre-date H3 indexing. Rather than index
5.8 GB of tracks to reach two rows each, this materialises **only the endpoint
rows** -- first and last sample per track -- with the index computed on those,
about half a million rows. The pipeline's own
:meth:`FlightListProcessor.build_endpoint_candidates` then runs against that,
unmodified: it takes first and last per track, which for a table already
reduced to first and last is the identity.

So the candidate table for 2024 is built by the same pipeline code as the one
for 2025, not by a reimplementation of it -- which is the whole point, given
what this study found the last time a benchmark and the pipeline disagreed.

    python benchmarks/build_candidates_2024.py
"""

import argparse
import sys
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))
sys.path.insert(0, str(REPO / "src"))

from pyspark.sql import Window
from pyspark.sql import functions as F

import osn_sample
from osn_sample import build_spark, load_dotenv

TRACKS_2024 = "s3a://eurocontrol/opdi/research/tracks"
ENDS_2024 = "research/tracks_2024_ends"
CAND_2024 = "research/cand_2024"


def redirect(ends_table: str, cand_table: str):
    """Point the pipeline's table names at the 2024 copies.

    Wrapping StorageManager rather than editing the pipeline keeps this out of
    the production code path entirely. The write guard is deliberate: the
    candidate builder's default target is a production prefix, and a dropped
    argument would overwrite it.
    """
    from opdi.utils.storage import StorageManager

    if getattr(StorageManager, "_c24", False):
        return
    orig_read, orig_write = StorageManager.read_table, StorageManager.write_table
    mapping = {"osn_tracks": ends_table, "opdi_endpoint_candidates": cand_table}

    def read_table(self, name, *a, **kw):
        return orig_read(self, mapping.get(name, name), *a, **kw)

    def write_table(self, df, name, *a, **kw):
        target = mapping.get(name, name)
        if not target.startswith("research/"):
            raise RuntimeError(f"refusing to write {target!r} outside research/")
        print(f"  -> writing {target}")
        return orig_write(self, df, target, *a, **kw)

    StorageManager.read_table = read_table
    StorageManager.write_table = write_table
    StorageManager._c24 = True


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", nargs="+",
                    default=["2024-06-05", "2024-06-06", "2024-06-07"])
    ap.add_argument("--results-dir", default=None,
                    help="unused; accepted so the regeneration chain can call "
                         "this like any other job")
    ap.add_argument("--executors", type=int, default=10)
    ap.add_argument("--ui-port", type=int, default=4053)
    ap.add_argument("--skip-ends", action="store_true",
                    help="reuse an existing endpoint table")
    args = ap.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
    load_dotenv()
    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(6, "8g", distributed=True)
    spark.sparkContext.setLogLevel("ERROR")

    from opdi.config import OPDIConfig
    from opdi.pipeline.flights import FlightListProcessor

    cfg = OPDIConfig.for_environment("opensky")

    if not args.skip_ends:
        import h3_pyspark

        tr = (spark.read.parquet(TRACKS_2024)
              .filter(F.to_date("event_time").isin(args.days))
              .dropna(subset=["lat", "lon", "track_id"]))
        w = Window.partitionBy("track_id").orderBy("event_time")
        ends = (tr.withColumn("_rn", F.row_number().over(w))
                .withColumn("_rr", F.row_number().over(
                    w.orderBy(F.col("event_time").desc())))
                .filter((F.col("_rn") == 1) | (F.col("_rr") == 1))
                .drop("_rn", "_rr"))
        # Index only what survives: two rows per track rather than 5.8 GB.
        ends = (ends.withColumn("_res", F.lit(7))
                .withColumn("h3_res_7",
                            h3_pyspark.geo_to_h3("lat", "lon", "_res"))
                .drop("_res"))
        if "on_ground" not in ends.columns:
            ends = ends.withColumn("on_ground", F.lit(False))
        n = ends.count()
        print(f"endpoint rows for {len(args.days)} days: {n:,}")
        (ends.write.mode("overwrite")
         .parquet(f"s3a://eurocontrol/opdi/{ENDS_2024}"))

    redirect(ENDS_2024, CAND_2024)
    proc = FlightListProcessor(
        spark, cfg, log_dir=str(REPO / "OPDI_live" / "logs" / "cand2024"))
    proc.build_endpoint_candidates(date(2024, 6, 1), rebuild=True)

    cand = spark.read.parquet(f"s3a://eurocontrol/opdi/{CAND_2024}")
    print(f"candidates written: {cand.count():,}")
    spark.stop()


if __name__ == "__main__":
    main()
