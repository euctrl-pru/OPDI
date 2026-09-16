"""Are the debounced arm's 259 "2+persist" tracks genuine merges or artefacts?

Go/no-go probe for `debounced` (hold=30s). A fragmentation benchmark found 259
tracks under `debounced` containing TWO distinct real callsigns that EACH
persist >= 30s within the track, versus 47 under `recommended`. This script
characterises those 259 tracks directly from the per-sample callsign sequence,
rather than trusting the aggregate delta.

Read-only. Writes nothing.
"""
import argparse

from pyspark.sql import functions as F, Window

from opdi.config import OPDIConfig, SegmentationConfig
from opdi.pipeline.segmentation import SegmentationParams, assign_track_id
from opdi.pipeline.segmentation.methods import ARMS
from opdi.utils.spark_helpers import SparkSessionManager
from opdi.utils.storage import StorageManager


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--day", default="2026-06-01")
    ap.add_argument("--warehouse", default="s3a://eurocontrol/opdi-prod")
    ap.add_argument("--executors", type=int, default=6)
    ap.add_argument("--hold", type=float, default=30.0)
    args = ap.parse_args()

    config = OPDIConfig.for_environment("opensky")
    config.project.warehouse_path = args.warehouse
    config.spark.executor_instances = args.executors
    spark = SparkSessionManager.create_session(
        app_name="debounced 2+persist", config=config, distributed=True
    )
    storage = StorageManager(spark, config)

    sv = (
        storage.read_table("osn_tracks_clean")
        .filter(F.col("dof").cast("date") == F.lit(args.day))
        .select(
            "icao24", "track_id", "event_time", "callsign", "baro_altitude", "velocity"
        )
    ).cache()

    params = SegmentationParams.from_config(
        SegmentationConfig(callsign_min_persistence_seconds=args.hold)
    )
    tracked = assign_track_id(sv, ARMS["debounced"](), params).cache()

    real = F.when(
        F.trim(F.coalesce(F.col("callsign"), F.lit(""))) != "",
        F.trim(F.col("callsign")),
    )
    per_cs = (
        tracked.withColumn("_cs", real)
        .filter(F.col("_cs").isNotNull())
        .groupBy("track_id", "_cs")
        .agg(
            F.count(F.lit(1)).alias("n_samples"),
            F.min("event_time").alias("first_ts"),
            F.max("event_time").alias("last_ts"),
        )
        .withColumn(
            "span_s",
            F.col("last_ts").cast("long") - F.col("first_ts").cast("long"),
        )
        .filter(F.col("span_s") >= 30)
    ).cache()

    track_counts = per_cs.groupBy("track_id").agg(
        F.count(F.lit(1)).alias("n_persisted_cs")
    )
    target_tracks = track_counts.filter(F.col("n_persisted_cs") >= 2)
    n_target = target_tracks.count()
    print(f"day {args.day}  hold={args.hold}s")
    print(f"  tracks with 2+ persisted (>=30s) callsigns: {n_target:,}")

    detail = (
        per_cs.join(target_tracks.select("track_id"), "track_id", "inner")
        .withColumn(
            "rank", F.row_number().over(
                Window.partitionBy("track_id").orderBy("first_ts")
            )
        )
    ).cache()

    # For each track, build ordered pairs (cs1 = first-appearing, cs2 = next)
    # to compute similarity and span-based classification, using only the
    # FIRST TWO persisted callsigns per track for the pairwise stats (a track
    # with 3+ persisted callsigns is rare -- print those separately).
    pair = (
        detail.filter(F.col("rank") == 1)
        .select(
            "track_id",
            F.col("_cs").alias("cs1"),
            F.col("n_samples").alias("n1"),
            F.col("span_s").alias("span1"),
        )
        .join(
            detail.filter(F.col("rank") == 2).select(
                "track_id",
                F.col("_cs").alias("cs2"),
                F.col("n_samples").alias("n2"),
                F.col("span_s").alias("span2"),
            ),
            "track_id",
        )
    ).cache()

    n_pairs = pair.count()
    print(f"  tracks with >=2 persisted callsigns (pair basis): {n_pairs:,}")
    if n_pairs != n_target:
        print(
            "  NOTE: n_pairs != n_target -- some tracks have 3+ persisted"
            " callsigns; pairwise stats use only the first two by appearance."
        )

    pair = pair.withColumn(
        "lev", F.levenshtein(F.col("cs1"), F.col("cs2"))
    ).withColumn(
        "prefix_rel",
        F.col("cs2").startswith(F.col("cs1")) | F.col("cs1").startswith(F.col("cs2")),
    ).withColumn(
        "string_similar", (F.col("lev") <= 2) | F.col("prefix_rel")
    ).withColumn(
        "short_span", F.least(F.col("span1"), F.col("span2"))
    ).withColumn(
        "long_span", F.greatest(F.col("span1"), F.col("span2"))
    ).withColumn(
        "brief_plus_real",
        (F.col("short_span") < 120) & (F.col("long_span") > 300),
    ).withColumn(
        "both_long", (F.col("span1") > 300) & (F.col("span2") > 300)
    ).withColumn(
        "genuine_merge_signature",
        F.col("both_long") & ~F.col("string_similar"),
    ).cache()

    n = pair.count()
    a = pair.filter(F.col("string_similar")).count()
    b = pair.filter(F.col("brief_plus_real")).count()
    c = pair.filter(F.col("genuine_merge_signature")).count()

    print(f"\n  summary over {n:,} tracks (first-two-callsigns basis):")
    print(f"    (a) string-similar (lev<=2 or prefix)        {a:>4}  ({100*a/max(n,1):5.1f}%)")
    print(f"    (b) short<120s & long>300s (brief artefact)  {b:>4}  ({100*b/max(n,1):5.1f}%)")
    print(f"    (c) both>300s & string-dissimilar (GENUINE)  {c:>4}  ({100*c/max(n,1):5.1f}%)")

    print("\n  genuine-merge-signature sample rows (up to 10):")
    (
        pair.filter(F.col("genuine_merge_signature"))
        .select("track_id", "cs1", "n1", "span1", "cs2", "n2", "span2", "lev")
        .orderBy(F.desc("long_span"))
        .show(10, False)
    )

    print("\n  sample of ~30 tracks, ordered callsigns with counts/spans:")
    sample_tracks = [r["track_id"] for r in target_tracks.limit(30).collect()]
    detail.filter(F.col("track_id").isin(sample_tracks)).orderBy(
        "track_id", "rank"
    ).select("track_id", "rank", "_cs", "n_samples", "span_s").show(120, False)

    spark.stop()


if __name__ == "__main__":
    main()
