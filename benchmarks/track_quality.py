"""
Measure how well ``_add_track_id`` separates real flights.

``tracks.py:_add_track_id`` is frozen -- ``track_id`` continuity with every
published OPDI release depends on it -- so this measures rather than changes it.
The point is to know what a v2 of the algorithm would have to fix, and how much
it would be worth.

The algorithm groups on ``SHA2(icao24 || callsign)`` and splits a group when
there is a gap of over 30 minutes, or over 15 minutes below 5,000 ft. Four
things can go wrong with that, and each shows up differently against ground
truth:

* **Fragmentation** -- one real flight becomes several tracks. A reception gap
  in the cruise over water is enough. Endpoint-based ADEP/ADES detection then
  sees a track that starts mid-air, which is exactly the case it cannot answer.
* **Merging** -- several real flights become one track. A turnaround shorter
  than the gap threshold, or one where the aircraft parks above 5,000 ft field
  elevation so the 15-minute rule never fires.
* **Callsign coupling** -- the group key includes the callsign, so a callsign
  change mid-flight splits the track, and a *missing* callsign collapses every
  such segment for an airframe into one group.
* **Window truncation** -- tracks clipped by the edge of the processed period.
  Production processes a month at a time and puts the year and month in the
  track_id, so a flight airborne at midnight on the 1st is split in two by
  construction. Here the same effect appears at the edges of the sample.

    python benchmarks/track_quality.py --days 2025-06-05 2025-06-06 2025-06-07 \
        --months 202506
"""

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

import osn_sample
from osn_sample import build_spark, load_dotenv
from adep_ades import (
    OUT_BASE, TRACKS_BASE, load_ground_truth, track_identity,
)

OUT = f"{OUT_BASE}/track_quality"


def track_summary(sv: DataFrame) -> DataFrame:
    """One row per track: extent, size, and the raw callsign as broadcast."""
    return sv.groupBy("track_id").agg(
        F.count(F.lit(1)).alias("n_samples"),
        F.min("event_time").alias("t_start"),
        F.max("event_time").alias("t_end"),
        F.first("icao24").alias("icao24"),
        F.first("callsign").alias("callsign_raw"),
        F.min("baro_altitude").alias("alt_min_m"),
        F.max("baro_altitude").alias("alt_max_m"),
    ).withColumn(
        "duration_h",
        (F.unix_timestamp("t_end") - F.unix_timestamp("t_start")) / 3600.0,
    )


def measure(spark: SparkSession, sv: DataFrame, gt: DataFrame, days: list) -> dict:
    ts = track_summary(sv).cache()
    n_tracks = ts.count()

    # -- callsign hygiene -------------------------------------------------
    cs = ts.select(
        F.sum(F.when(F.col("callsign_raw").isNull(), 1).otherwise(0)).alias("null_cs"),
        F.sum(F.when(F.trim(F.col("callsign_raw")) == "", 1).otherwise(0)).alias("blank_cs"),
        F.sum(
            F.when(F.col("callsign_raw") != F.trim(F.col("callsign_raw")), 1).otherwise(0)
        ).alias("padded_cs"),
    ).first()

    # -- duration / size outliers ----------------------------------------
    sz = ts.select(
        F.sum(F.when(F.col("n_samples") == 1, 1).otherwise(0)).alias("single_sample"),
        F.sum(F.when(F.col("duration_h") > 18, 1).otherwise(0)).alias("over_18h"),
        F.sum(F.when(F.col("duration_h") < 0.0834, 1).otherwise(0)).alias("under_5min"),
        F.expr("percentile_approx(duration_h, 0.5)").alias("median_h"),
        F.expr("percentile_approx(duration_h, 0.99)").alias("p99_h"),
        F.max("duration_h").alias("max_h"),
    ).first()

    # -- window truncation ------------------------------------------------
    # Tracks alive at the edge of the sampled period were cut by the window,
    # not by the aircraft. Production has the same effect at month boundaries.
    lo = F.lit(f"{min(days)} 00:00:00").cast("timestamp")
    hi = F.lit(f"{max(days)} 23:59:59").cast("timestamp")
    tr = ts.select(
        F.sum(
            F.when(F.unix_timestamp("t_start") - F.unix_timestamp(lo) < 300, 1).otherwise(0)
        ).alias("starts_at_edge"),
        F.sum(
            F.when(F.unix_timestamp(hi) - F.unix_timestamp("t_end") < 300, 1).otherwise(0)
        ).alias("ends_at_edge"),
    ).first()

    # -- fragmentation and merging against ground truth -------------------
    ident = track_identity(sv)
    j = gt.join(ident, ["icao24", "callsign", "day"], "inner")
    per_flight = j.groupBy("icao24", "callsign", "day", "gt_adep", "gt_ades").agg(
        F.count(F.lit(1)).alias("tracks_per_flight")
    )
    frag = per_flight.select(
        F.count(F.lit(1)).alias("matched_flights"),
        F.sum(F.when(F.col("tracks_per_flight") > 1, 1).otherwise(0)).alias("fragmented"),
        F.expr("percentile_approx(tracks_per_flight, 0.99)").alias("p99_tracks"),
        F.max("tracks_per_flight").alias("max_tracks"),
    ).first()

    per_track = j.groupBy("track_id").agg(F.count(F.lit(1)).alias("flights_per_track"))
    merge = per_track.select(
        F.count(F.lit(1)).alias("matched_tracks"),
        F.sum(F.when(F.col("flights_per_track") > 1, 1).otherwise(0)).alias("merged"),
        F.max("flights_per_track").alias("max_flights"),
    ).first()

    ts.unpersist()
    return {
        "n_tracks": n_tracks,
        "callsign_null": cs["null_cs"], "callsign_blank": cs["blank_cs"],
        "callsign_padded": cs["padded_cs"],
        "single_sample": sz["single_sample"], "over_18h": sz["over_18h"],
        "under_5min": sz["under_5min"], "median_duration_h": float(sz["median_h"] or 0),
        "p99_duration_h": float(sz["p99_h"] or 0), "max_duration_h": float(sz["max_h"] or 0),
        "starts_at_window_edge": tr["starts_at_edge"],
        "ends_at_window_edge": tr["ends_at_edge"],
        "matched_flights": frag["matched_flights"], "fragmented_flights": frag["fragmented"],
        "p99_tracks_per_flight": int(frag["p99_tracks"] or 0),
        "max_tracks_per_flight": int(frag["max_tracks"] or 0),
        "matched_tracks": merge["matched_tracks"], "merged_tracks": merge["merged"],
        "max_flights_per_track": int(merge["max_flights"] or 0),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", nargs="+", required=True)
    ap.add_argument("--months", nargs="+", required=True)
    ap.add_argument("--aircraft", default="nonrotor")
    ap.add_argument("--executors", type=int, default=8)
    ap.add_argument("--ui-port", type=int, default=4041)
    args = ap.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
    load_dotenv()
    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(6, "9g")
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "96")

    sv = spark.read.parquet(*[
        f"{TRACKS_BASE}/aircraft={args.aircraft}/day={d}" for d in args.days
    ])
    gt = load_ground_truth(spark, args.months, args.days)
    stats = measure(spark, sv, gt, args.days)

    print("\n=== track_id quality ===")
    for k, v in stats.items():
        print(f"  {k:26} {v:,}" if isinstance(v, int) else f"  {k:26} {v:,.2f}")

    stats["days"] = ",".join(args.days)
    stats["months"] = ",".join(args.months)
    tag = f"{len(args.days)}d-{min(args.days)}"
    spark.createDataFrame([stats]).coalesce(1).write.mode("overwrite").parquet(
        f"{OUT}/{tag}"
    )
    print(f"\n-> {OUT}/{tag}")
    spark.stop()


if __name__ == "__main__":
    main()
