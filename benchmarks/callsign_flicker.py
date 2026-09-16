"""Are sub-minute track splits caused by a callsign that flickers and reverts?

`recommended` breaks on callsign_change, gap > 30 min, or gap > 15 min below
5,000 ft. A split with a sub-minute gap can only be the callsign rule, and
1,457 of 1,765 such splits on 2026-06-01 had the *same* resolved FLT_ID on both
halves. That is consistent with a transient value -- but the resolved callsign
is a per-track summary, so it cannot tell a flicker from a genuine change the
flight list then smooths away. Only the raw per-sample sequence can.

Read-only against OSN tables. With ``--out-name`` and ``--results-dir`` it also
writes the boundary-cause breakdown and the flicker-holding-time stats as one
CSV row, so ``regenerate_track_v3.py`` can stage it for the paper.
"""
import argparse
import csv
from pathlib import Path

from pyspark.sql import functions as F, Window

from opdi.config import OPDIConfig
from opdi.pipeline.segmentation import SegmentationParams, assign_track_id
from opdi.pipeline.segmentation.base import FT_PER_M
from opdi.pipeline.segmentation.methods import recommended
from opdi.utils.spark_helpers import SparkSessionManager
from opdi.utils.storage import StorageManager


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--day", default="2026-06-01")
    ap.add_argument("--warehouse", default="s3a://eurocontrol/opdi-prod")
    ap.add_argument("--executors", type=int, default=6)
    ap.add_argument("--results-dir", type=Path, default=None,
                     help="stage the boundary-cause breakdown here as a CSV "
                          "row, if --out-name is also given")
    ap.add_argument("--out-name", default=None,
                     help="CSV filename under --results-dir")
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
        .select("icao24", "track_id", "event_time", "callsign", "baro_altitude", "velocity")
    ).cache()

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
    held_stats = held.selectExpr(
        "percentile_approx(_held_s, 0.5) AS p50",
        "percentile_approx(_held_s, 0.9) AS p90",
        "percentile_approx(_held_s, 0.99) AS p99",
        "max(_held_s) AS max",
    )
    print("\n  how long the transient value held, seconds:")
    held_stats.show(truncate=False)
    held_row = held_stats.collect()[0]

    print("  sample of flickers:")
    flicker.select("icao24", "_prev", "_cs", "_next", "event_time").show(15, False)

    # -- Task 1b: does flicker actually explain the shipped rule's track
    # boundaries, rather than just being arithmetically sufficient to? -------
    #
    # 14.6% of raw callsign changes above is the wrong denominator: most raw
    # changes are legitimate leg-to-leg callsign changes that *should* split a
    # track, so comparing flickers against that population says nothing about
    # whether flicker causes spurious splits. The question that bears on the
    # claim is: of the boundaries the shipped rule actually draws, how many are
    # attributable to a flicker?
    params = SegmentationParams.from_config(config)
    tracked = assign_track_id(sv, recommended(), params)

    w3 = Window.partitionBy("icao24").orderBy("event_time")
    boundaries = (
        tracked.withColumn("_lag_tid", F.lag("track_id").over(w3))
        .withColumn("_lag_ts", F.lag("event_time").over(w3))
        .filter(F.col("_lag_tid").isNotNull() & (F.col("track_id") != F.col("_lag_tid")))
        .withColumn(
            "_gap_min",
            (F.col("event_time").cast("long") - F.col("_lag_ts").cast("long")) / 60.0,
        )
        .withColumn("_alt_ft", F.col("baro_altitude") * F.lit(FT_PER_M))
        .select("icao24", "event_time", "_gap_min", "_alt_ft")
    )

    # A boundary that is not gap-caused can only be a callsign_change break --
    # `recommended`'s break_expr is exactly callsign_change OR the two legacy
    # gap rules, so nothing else can have produced it. What remains to
    # distinguish is whether the callsign_change was a flicker's revert leg
    # (Y -> X, the boundary this frame's `_next_t` marks), a flicker's
    # excursion leg (X -> Y, `event_time` in `flicker`, which reverts and so is
    # not a genuine change), or a real, non-reverting change.
    flicker_x = flicker.select("icao24", F.col("_next_t").alias("event_time")).distinct()
    flicker_y = flicker.select("icao24", "event_time").distinct()

    classified = (
        boundaries.join(
            flicker_x.withColumn("_is_flicker_x", F.lit(True)),
            ["icao24", "event_time"], "left",
        ).join(
            flicker_y.withColumn("_is_flicker_y", F.lit(True)),
            ["icao24", "event_time"], "left",
        )
    ).withColumn(
        "_class",
        F.when(F.col("_gap_min") > params.gap_minutes, F.lit("gap_gt_30min"))
        .when(
            (F.col("_gap_min") > params.low_alt_gap_minutes)
            & (F.col("_alt_ft") < params.low_alt_ft),
            F.lit("gap_gt_15min_low_alt"),
        )
        .when(F.col("_is_flicker_x").isNotNull(), F.lit("flicker"))
        .when(F.col("_is_flicker_y").isNotNull(), F.lit("unexplained"))  # reverts -> not genuine
        .otherwise(F.lit("genuine_callsign_change")),
    )

    print("\n  track boundaries under `recommended`, classified by cause:")
    counts = (
        classified.groupBy("_class").count().orderBy(F.desc("count")).collect()
    )
    n_boundaries = sum(r["count"] for r in counts)
    for r in counts:
        print(f"    {r['_class']:<24} {r['count']:>8,}"
              f"  ({100 * r['count'] / max(n_boundaries, 1):5.1f}%)")
    print(f"    {'TOTAL':<24} {n_boundaries:>8,}")

    # Fix round 1, finding 1 (code review): where two flickers abut with no
    # stable sample between them (X,Y,X,Y,X -- LBT808 and MSR730 above show an
    # airframe garbling its callsign repeatedly), a single boundary can be
    # BOTH the revert leg of one flicker and the excursion leg of the next.
    # `.when()` precedence resolves that silently to `flicker`, which is fine
    # for the 23.1% headline (it sums both buckets either way), but it means
    # the exact 5,241-boundary match against the first pass's flicker count is
    # not, on its own, proof of a clean one-to-one correspondence -- some of
    # it could be collisions washing out. Count it rather than assume it.
    n_collision = classified.filter(
        F.col("_is_flicker_x").isNotNull() & F.col("_is_flicker_y").isNotNull()
    ).count()
    print(f"\n  boundaries matching BOTH a flicker's revert leg and another"
          f" flicker's excursion leg: {n_collision:,}")
    if n_collision == 0:
        print("  -> no collisions: the flicker-bucket count is a clean 1:1"
              " match against the first-pass flicker count.")
    else:
        print("  -> collisions present: the flicker-bucket count and the"
              " first-pass flicker count agreeing exactly is not, by itself,"
              " proof of a 1:1 correspondence -- some boundaries satisfy both.")

    if args.results_dir and args.out_name:
        # One row, wide: every quantity this script measures, so the paper
        # reads one CSV rather than several. `_class` labels are the raw
        # bucket names the classifier above assigns -- "flicker" is the
        # revert leg (this boundary's event_time matches a flicker's
        # `_next_t`), "unexplained" is the excursion leg (matches the
        # flicker's own event_time but not already claimed by a revert): both
        # are flicker-caused, the paper just names them more plainly.
        row = {
            "day": args.day,
            "raw_callsign_changes": total,
            "raw_flicker_changes": n_flicker,
            "raw_flicker_pct": round(100 * n_flicker / max(total, 1), 1),
            "held_p50_s": held_row["p50"],
            "held_p90_s": held_row["p90"],
            "held_p99_s": held_row["p99"],
            "held_max_s": held_row["max"],
            "n_boundaries": n_boundaries,
            "n_collision": n_collision,
            "collision_pct": round(100 * n_collision / max(n_boundaries, 1), 1),
        }
        for r in counts:
            cls = r["_class"]
            row[f"boundary_{cls}"] = r["count"]
            row[f"boundary_{cls}_pct"] = round(100 * r["count"] / max(n_boundaries, 1), 1)

        args.results_dir.mkdir(parents=True, exist_ok=True)
        out = args.results_dir / args.out_name
        with out.open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=sorted(row))
            w.writeheader()
            w.writerow(row)
        print(f"\n  staged {out}")

    spark.stop()


if __name__ == "__main__":
    main()
