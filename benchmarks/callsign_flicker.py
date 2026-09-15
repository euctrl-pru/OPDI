"""Are sub-minute track splits caused by a callsign that flickers and reverts?

`recommended` breaks on callsign_change, gap > 30 min, or gap > 15 min below
5,000 ft. A split with a sub-minute gap can only be the callsign rule, and
1,457 of 1,765 such splits on 2026-06-01 had the *same* resolved FLT_ID on both
halves. That is consistent with a transient value -- but the resolved callsign
is a per-track summary, so it cannot tell a flicker from a genuine change the
flight list then smooths away. Only the raw per-sample sequence can.

Read-only. Writes nothing.
"""
import argparse

from pyspark.sql import functions as F, Window

from opdi.config import OPDIConfig
from opdi.utils.spark_helpers import SparkSessionManager
from opdi.utils.storage import StorageManager


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--day", default="2026-06-01")
    ap.add_argument("--warehouse", default="s3a://eurocontrol/opdi-prod")
    ap.add_argument("--executors", type=int, default=6)
    args = ap.parse_args()

    config = OPDIConfig.for_environment("opensky")
    config.project.warehouse_path = args.warehouse
    config.spark.executor_instances = args.executors
    spark = SparkSessionManager.create_session(
        app_name="callsign flicker", config=config, distributed=True
    )
    storage = StorageManager(spark, config)

    sv = (
        storage.read_table("osn_tracks_clean")
        .filter(F.col("dof").cast("date") == F.lit(args.day))
        .select("icao24", "track_id", "event_time", "callsign")
    )

    # The raw callsign sequence per airframe, ignoring blanks -- a blank is
    # "not transmitted", not "changed to nothing", and the arm already treats
    # it that way.
    real = F.when(F.trim(F.coalesce(F.col("callsign"), F.lit(""))) != "",
                  F.trim(F.col("callsign")))
    w = Window.partitionBy("icao24").orderBy("event_time")
    seq = (
        sv.withColumn("_cs", real)
        .filter(F.col("_cs").isNotNull())
        .withColumn("_prev", F.lag("_cs").over(w))
        .withColumn("_next", F.lead("_cs").over(w))
        .withColumn("_prev_t", F.lag("event_time").over(w))
        .withColumn("_next_t", F.lead("event_time").over(w))
    )

    changes = seq.filter(F.col("_prev").isNotNull() & (F.col("_cs") != F.col("_prev")))
    total = changes.count()

    # A flicker: the value differs from the one before AND from the one after,
    # and the one before equals the one after. X -> Y -> X.
    flicker = changes.filter(
        F.col("_next").isNotNull()
        & (F.col("_cs") != F.col("_next"))
        & (F.col("_prev") == F.col("_next"))
    )
    n_flicker = flicker.count()

    # How long the transient value held. A flicker that persists for minutes is
    # a different animal from one that lasts a single sample, and the debounce
    # threshold has to be chosen against this distribution rather than guessed.
    held = flicker.withColumn(
        "_held_s",
        F.col("_next_t").cast("long") - F.col("event_time").cast("long"),
    )

    print(f"day {args.day}")
    print(f"  raw callsign changes            {total:,}")
    print(f"  of which X -> Y -> X (flicker)  {n_flicker:,}"
          f"  ({100 * n_flicker / max(total, 1):.1f}%)")
    print("\n  how long the transient value held, seconds:")
    held.selectExpr(
        "percentile_approx(_held_s, 0.5) AS p50",
        "percentile_approx(_held_s, 0.9) AS p90",
        "percentile_approx(_held_s, 0.99) AS p99",
        "max(_held_s) AS max",
    ).show(truncate=False)

    print("  sample of flickers:")
    flicker.select("icao24", "_prev", "_cs", "_next", "event_time").show(15, False)

    spark.stop()


if __name__ == "__main__":
    main()
