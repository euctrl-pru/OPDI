"""Does `debounced` fuse MORE genuine flights than `recommended` already does?

Follow-up to debounced_2plus_persist.py. That probe found 259 tracks under
`debounced` (hold=30s) with 2+ callsigns persisting >= 30s, and manual
inspection showed the raw "both span > 300s and string-dissimilar" criterion
(131 tracks) was contaminated by sparse, low-sample-count garbled fragments
that happen to span a long time without being densely present.

This refines the criterion with a density floor -- span >= 300s AND
>= 100 samples -- and, critically, applies the SAME floor to `recommended`
(hold=0), on the SAME 2026-06-01 data, so the two arms are measured on one
yardstick. The debounce only changes behaviour for *non-persisted* callsign
excursions; for two dense, persisted callsigns it should behave identically
to `recommended`, so the comparison -- not the absolute count -- is what
decides whether debouncing introduces a new class of merge.

Read-only. Writes nothing.
"""
import argparse

from pyspark.sql import functions as F, Window

from opdi.config import OPDIConfig, SegmentationConfig
from opdi.pipeline.segmentation import SegmentationParams, assign_track_id
from opdi.pipeline.segmentation.methods import ARMS
from opdi.utils.spark_helpers import SparkSessionManager
from opdi.utils.storage import StorageManager

SPAN_FLOOR_S = 300
SAMPLE_FLOOR = 100


def analyze_arm(sv, arm_name: str, hold: float, params_base):
    params = SegmentationParams.from_config(
        SegmentationConfig(callsign_min_persistence_seconds=hold)
    )
    tracked = assign_track_id(sv, ARMS[arm_name](), params).cache()
    n_tracks = tracked.select("track_id").distinct().count()

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
        # Density floor: dense AND long-spanning, independent of hold, so
        # `recommended` (hold=0) and `debounced` (hold=30) are measured on
        # the same "persisted callsign" yardstick.
        .filter((F.col("span_s") >= SPAN_FLOOR_S) & (F.col("n_samples") >= SAMPLE_FLOOR))
    ).cache()

    track_counts = per_cs.groupBy("track_id").agg(
        F.count(F.lit(1)).alias("n_dense")
    )
    target_tracks = track_counts.filter(F.col("n_dense") >= 2)

    detail = (
        per_cs.join(target_tracks.select("track_id"), "track_id", "inner")
        .withColumn(
            "rank",
            F.row_number().over(Window.partitionBy("track_id").orderBy("first_ts")),
        )
    ).cache()

    pair = (
        detail.filter(F.col("rank") == 1)
        .select("track_id", F.col("_cs").alias("cs1"), F.col("n_samples").alias("n1"), F.col("span_s").alias("span1"))
        .join(
            detail.filter(F.col("rank") == 2).select(
                "track_id", F.col("_cs").alias("cs2"), F.col("n_samples").alias("n2"), F.col("span_s").alias("span2")
            ),
            "track_id",
        )
    ).withColumn("lev", F.levenshtein(F.col("cs1"), F.col("cs2"))).withColumn(
        "prefix_rel",
        F.col("cs2").startswith(F.col("cs1")) | F.col("cs1").startswith(F.col("cs2")),
    ).withColumn(
        "string_dissimilar", (F.col("lev") > 2) & (~F.col("prefix_rel"))
    ).filter(F.col("string_dissimilar")).cache()

    n_genuine = pair.count()

    print(f"\n=== arm={arm_name}  hold={hold}s ===")
    print(f"  total tracks: {n_tracks:,}")
    print(f"  tracks with 2+ refined-dense genuine-merge callsigns"
          f" (span>={SPAN_FLOOR_S}s, samples>={SAMPLE_FLOOR}, lev>2 & not prefix): {n_genuine:,}")
    print(f"  sample genuine-merge tracks (up to 5):")
    pair.select("track_id", "cs1", "n1", "span1", "cs2", "n2", "span2", "lev").orderBy(
        F.desc("span1")
    ).show(5, False)

    tracked.unpersist()
    per_cs.unpersist()
    detail.unpersist()
    return n_tracks, n_genuine


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
        app_name="debounced vs recommended merge", config=config, distributed=True
    )
    storage = StorageManager(spark, config)

    sv = (
        storage.read_table("osn_tracks_clean")
        .filter(F.col("dof").cast("date") == F.lit(args.day))
        .select(
            "icao24", "track_id", "event_time", "callsign", "baro_altitude", "velocity"
        )
    ).cache()
    sv.count()  # materialize once, shared by both arms

    n_rec, g_rec = analyze_arm(sv, "recommended", 0.0, None)
    n_deb, g_deb = analyze_arm(sv, "debounced", 30.0, None)

    ratio = g_deb / max(g_rec, 1)
    print("\n=== COMPARISON ===")
    print(f"  recommended (hold=0):  total_tracks={n_rec:,}  refined_genuine_merges={g_rec:,}")
    print(f"  debounced   (hold=30): total_tracks={n_deb:,}  refined_genuine_merges={g_deb:,}")
    print(f"  ratio debounced/recommended: {ratio:.2f}x")
    if 0.8 <= ratio <= 1.2:
        print("  VERDICT: counts approx equal (within 20%) -- debounce introduces"
              " no new genuine merges; the refined-merge population is pre-existing"
              " and orthogonal to this change.")
    elif ratio > 1.2:
        print("  VERDICT: debounced substantially EXCEEDS recommended -- debounce"
              " is fusing additional genuine flights. Real regression.")
    else:
        print("  VERDICT: debounced is substantially BELOW recommended -- unexpected,"
              " investigate.")

    spark.stop()


if __name__ == "__main__":
    main()
