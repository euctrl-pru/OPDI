"""Does the debounce cut fragmentation without merging distinct flights?

One metric and one guard, both internal, because the APDF quantities that could measure this
directly cannot be trusted for it: AOBT and AIBT are frequently rounded to the
minute, and ADS-B presence does not share an end with the block times -- a
transponder may stay on long after on-block or come on well before off-block.

  fragmentation      tracks per (icao24, resolved callsign, day). The defect
                     raises it directly, so the fix must lower it.
Milestone pairing -- the share of tracks carrying both an ATOT and an ALDT --
is the other internal metric, but it cannot be computed here: this script runs
segmentation only, and milestones need step 04. It is measured in Task 5, where
a full pipeline run into the research prefix has produced events.

Guard: `tracks_2plus_persisted_callsigns` must stay near recommended value. A debounce that merged
two flights with genuinely different callsigns would lower fragmentation and be
badly wrong, and nothing else here would say so.

IMPORTANT LIMITATION: this benchmark re-segments *cleaned* state vectors
(``osn_tracks_clean``), but cleaning was itself partitioned by the track_id
that the shipped (``legacy``/``recommended``) arm produced. So a candidate
arm's re-derived boundaries do not align with the boundaries cleaning actually
used -- a candidate arm that would keep more of a track together never gets
the chance to clean it as one track. For counting tracks this is second-order
and acceptable, but it is NOT a from-raw measurement: it measures re-
segmentation of already-cleaned-and-split data, not segmentation from raw
state vectors. Task 5, which runs a full pipeline into the research prefix,
is the from-raw measurement -- do not present the two as equivalent.
"""
import argparse

from pyspark.sql import functions as F

from opdi.config import OPDIConfig, SegmentationConfig
from opdi.pipeline.segmentation import SegmentationParams, assign_track_id
from opdi.pipeline.segmentation.methods import ARMS
from opdi.utils.spark_helpers import SparkSessionManager
from opdi.utils.storage import StorageManager

RESEARCH = "s3a://eurocontrol/opdi/research"


def measure(sv, arm: str, hold: float):
    params = SegmentationParams.from_config(
        SegmentationConfig(callsign_min_persistence_seconds=hold)
    )
    tracked = assign_track_id(sv, ARMS[arm](), params)

    real = F.when(F.trim(F.coalesce(F.col("callsign"), F.lit(""))) != "",
                  F.trim(F.col("callsign")))
    per_track = tracked.groupBy("track_id").agg(
        F.first("icao24", ignorenulls=True).alias("icao24"),
        # `collect_set` rather than `first`: the guard below needs to know a
        # track holds two *different* callsigns, which a single value hides.
        F.collect_set(real).alias("callsigns"),
    )
    # A track's representative callsign, for grouping tracks into flights.
    # `F.get(..., 0)` on the sorted set is null-tolerant -- an all-blank track
    # yields an empty set and NULL here rather than throwing -- and
    # deterministic, unlike indexing the unordered `collect_set` directly.
    # Tracks with no real callsign group under NULL, which is correct: they
    # cannot be identified as a flight.
    rep = F.get(F.sort_array(F.col("callsigns")), 0)
    per_flight = per_track.withColumn("cs", rep).groupBy("icao24", "cs") \
        .agg(F.count(F.lit(1)).alias("n_tracks"))

    # THE GUARD. A track that swallowed a flicker legitimately contains the
    # real callsign AND the transient garble, so counting *distinct raw*
    # callsigns per track cannot tell a suppressed flicker from a genuine merge
    # of two real flights -- both show set size 2. It must instead count
    # distinct *persisted* callsigns: a callsign whose samples span at least
    # `hold` seconds within the track. A garble spans one-to-a-few samples
    # (< hold); a genuinely merged second flight spans minutes. So this stays
    # near recommended value if the arm only ever swallows flickers, and rises
    # to thousands only if it is wrongly fusing distinct flights -- the one
    # thing the verification must be able to detect.
    ts = F.unix_timestamp("event_time")
    cs_span = (
        tracked.groupBy("track_id", real.alias("cs"))
        .agg((F.max(ts) - F.min(ts)).alias("span_s"))
        .filter(F.col("cs").isNotNull() & (F.col("span_s") >= F.lit(hold)))
    )
    merged_persisted = (
        cs_span.groupBy("track_id").agg(F.count(F.lit(1)).alias("n_cs"))
        .filter(F.col("n_cs") > 1).count()
    )
    return {
        "arm": arm,
        "hold_s": hold,
        "tracks": tracked.select("track_id").distinct().count(),
        "tracks_per_flight": per_flight.agg(F.avg("n_tracks")).collect()[0][0],
        # Informative, not the guard: flicker-containing tracks show here, so a
        # rise is expected and correct for the debounced arm.
        "tracks_2plus_raw_callsigns":
            per_track.filter(F.size("callsigns") > 1).count(),
        # The guard. Must stay near recommended value.
        "tracks_2plus_persisted_callsigns": merged_persisted,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--day", default="2026-06-01")
    ap.add_argument("--warehouse", default="s3a://eurocontrol/opdi-prod")
    ap.add_argument("--holds", type=float, nargs="+", default=[15.0, 30.0, 60.0])
    ap.add_argument("--executors", type=int, default=8)
    args = ap.parse_args()

    config = OPDIConfig.for_environment("opensky")
    config.project.warehouse_path = args.warehouse
    config.spark.executor_instances = args.executors
    spark = SparkSessionManager.create_session(
        app_name="track fragmentation", config=config, distributed=True
    )
    storage = StorageManager(spark, config)

    sv = (
        storage.read_table("osn_tracks_clean")
        .filter(F.col("dof").cast("date") == F.lit(args.day))
    ).cache()

    rows = [measure(sv, "recommended", 0.0)]
    rows += [measure(sv, "debounced", h) for h in args.holds]

    print(f"\n{'arm':12s} {'hold':>5s} {'tracks':>9s} {'per flt':>8s} "
          f"{'2+raw':>7s} {'2+persist':>10s}")
    for r in rows:
        print(f"{r['arm']:12s} {r['hold_s']:5.0f} {r['tracks']:9,} "
              f"{r['tracks_per_flight']:8.3f} {r['tracks_2plus_raw_callsigns']:7,} "
              f"{r['tracks_2plus_persisted_callsigns']:10,}")
    print("\n`2+persist` is the guard: distinct callsigns each lasting >= hold "
          "in one track.\nIt must stay near recommended value -- a rise means "
          "the arm is fusing genuine\nflights. `2+raw` counts flicker-containing "
          "tracks and is EXPECTED to rise.")

    spark.stop()


if __name__ == "__main__":
    main()
