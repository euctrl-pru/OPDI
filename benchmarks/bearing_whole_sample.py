"""
Apply the bearing test to every flight, not only to the ones the gate refused.

Version 5 used bearing alignment as a *rescue*: it could only add answers, so it
could only add correct ones and wrong ones in some ratio. That framing asks one
question -- "should we answer this flight after all?" -- and leaves two others
untouched:

* **veto**   -- require alignment on flights the gate already accepts. Bearing
                can now *remove* answers, so it can strip wrong ones. This is
                the only variant that can raise accuracy.
* **replace** -- alignment instead of the distance and height gate, not as well
                as. Tests whether the geometry the abstention encodes is
                carrying anything bearing does not.
* **rerank** -- choose *which* aerodrome by alignment rather than by effective
                distance. Version 4.5 argued the abstention is a gate and never
                a naming rule, and that arrivals fail by misnaming; this is the
                naming rule that argument implies.

The first two need alignment for the best candidate only. The third needs it for
every candidate of every track, which is the expensive one: the course is per
track, so the cost is a broadcast join of ~230k courses onto the candidate
cache, then one angle per candidate row.

    python benchmarks/bearing_whole_sample.py --results-dir <dir>
"""

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import Window
from pyspark.sql import functions as F

import osn_sample
from osn_sample import build_spark, load_dotenv
from adep_ades import (
    airport_locations, label_ground_truth, load_ground_truth, score, AIRPORTS,
)
from benchmark_modes import identities_from_candidates
from abstained_vertical import bearing_deg, angle_between, CANDIDATES, TRACKS

OP_RADIUS_NM, OP_HEIGHT_FT, OP_PENALTY_NM = 30.0, 15000.0, 10.0
WINDOW_MIN = 7
GATES_DEG = (0.10, 0.25, 1.0, 3.0)


def courses(spark, ends):
    """Per (track, role): the far point of the window and the course flown.

    ``ends`` carries one row per (track_id, role) with the endpoint's time. The
    course is the bearing from the far edge of the window to the endpoint --
    which is the direction of travel for an arrival and the reverse course for
    a departure, because the window sits on the other side of the fix. Both
    read zero against a correctly identified aerodrome; see abstained_vertical.
    """
    tr_all = spark.read.parquet(TRACKS)
    tr = (tr_all.select("track_id", "event_time", "lat", "lon")
          .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
          .join(F.broadcast(ends), "track_id", "inner")
          .withColumn("_off", F.abs(F.unix_timestamp("t_end")
                                    - F.unix_timestamp("event_time")))
          .filter(F.col("_off") <= WINDOW_MIN * 60))
    g = (tr.groupBy("track_id", "role").agg(
            F.count(F.lit(1)).alias("n"),
            F.min("_off").alias("o0"), F.max("_off").alias("o1"),
            F.min_by("lat", "_off").alias("y_near"),
            F.min_by("lon", "_off").alias("x_near"),
            F.max_by("lat", "_off").alias("y_far"),
            F.max_by("lon", "_off").alias("x_far"))
         .filter("n >= 5")
         .filter(F.col("o1") - F.col("o0") >= 60))
    return g.withColumn("course",
                        bearing_deg(F.col("y_far"), F.col("x_far"),
                                    F.col("y_near"), F.col("x_near"))) \
            .select("track_id", "role", "y_far", "x_far", "course")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--months", nargs="+", default=["202506"])
    ap.add_argument("--days", nargs="+",
                    default=["2025-06-05", "2025-06-06", "2025-06-07"])
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--executors", type=int, default=8)
    ap.add_argument("--ui-port", type=int, default=4041)
    args = ap.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
    load_dotenv()
    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(6, "8g", distributed=True)
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)

    cand = spark.read.parquet(CANDIDATES)
    apt_pos = (spark.read.parquet(AIRPORTS)
               .select(F.col("ident").alias("apt_ident"),
                       F.col("latitude_deg").cast("double").alias("apt_lat"),
                       F.col("longitude_deg").cast("double").alias("apt_lon"))
               .dropDuplicates(["apt_ident"]))

    ends = (cand.select("track_id", "role", F.col("event_time").alias("t_end"))
            .dropDuplicates(["track_id", "role"]))
    crs = courses(spark, ends).cache()
    print(f"courses computed for {crs.count():,} (track, role) pairs")

    # Alignment for every candidate, not only the chosen one.
    c = (cand.join(F.broadcast(apt_pos), "apt_ident", "left")
         .join(crs, ["track_id", "role"], "left")
         .withColumn("align_deg",
                     angle_between(F.col("course"),
                                   bearing_deg(F.col("y_far"), F.col("x_far"),
                                               F.col("apt_lat"), F.col("apt_lon")))))
    penalty = F.when(F.col("apt_scheduled") == "yes", F.lit(0.0)).otherwise(
        F.lit(OP_PENALTY_NM))
    c = c.withColumn("_eff", F.col("dist_nm") + penalty).cache()
    print(f"candidates with alignment: {c.count():,}")

    gate = (F.col("dist_nm") <= OP_RADIUS_NM) & (
        F.col("on_ground") | (F.col("elev_known")
                              & (F.col("agl_ft") <= OP_HEIGHT_FT)))

    def pick(order_cols):
        w = Window.partitionBy("track_id", "role").orderBy(*order_cols)
        return (c.withColumn("_r", F.row_number().over(w))
                .filter(F.col("_r") == 1)
                .withColumn("gate_ok", gate))

    by_dist = pick([F.col("_eff").asc_nulls_last()]).cache()
    by_align = pick([F.col("align_deg").asc_nulls_last(),
                     F.col("_eff").asc_nulls_last()]).cache()

    ident = identities_from_candidates(cand)
    gt = label_ground_truth(load_ground_truth(spark, args.months, args.days),
                            airport_locations(spark)).cache()
    print(f"ground-truth flights: {gt.count():,}")

    def predict(best, accept):
        apt = F.when(accept, F.col("apt_ident")).when(F.col("at_border"), F.lit("OOA"))
        return (best.withColumn("apt", apt)
                .groupBy("track_id").pivot("role", ["adep", "ades"])
                .agg(F.first("apt")))

    rows = []
    def run(label, best, accept):
        m = score(predict(best, accept), ident, gt)
        m["model"] = label
        rows.append(m)
        print(f"  {label:<34} ADEP {m['adep_coverage']:6.2%}/{m['adep_accuracy']:6.2%}"
              f"/{m['adep_overall']:6.2%}   ADES {m['ades_coverage']:6.2%}/"
              f"{m['ades_accuracy']:6.2%}/{m['ades_overall']:6.2%}")

    aligned = lambda d: F.coalesce(F.col("align_deg") <= d, F.lit(False))

    print("\nscoring:")
    run("base", by_dist, F.col("gate_ok"))
    for d in GATES_DEG:
        run(f"rescue: gate OR align<={d}", by_dist, F.col("gate_ok") | aligned(d))
    for d in GATES_DEG:
        run(f"veto: gate AND align<={d}", by_dist, F.col("gate_ok") & aligned(d))
    for d in GATES_DEG:
        run(f"replace: align<={d} only", by_dist, aligned(d))
    # Naming rule: choose the aerodrome the track points at, then gate as usual.
    run("rerank by alignment, then gate", by_align, F.col("gate_ok"))
    for d in GATES_DEG[:2]:
        run(f"rerank, gate OR align<={d}", by_align, F.col("gate_ok") | aligned(d))

    spark.createDataFrame(rows).toPandas().to_csv(out / "whole_sample.csv", index=False)
    print(f"\nwritten to {out}")
    spark.stop()


if __name__ == "__main__":
    main()
