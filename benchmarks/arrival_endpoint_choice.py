"""
Choose the best endpoint-family setting for arrivals, with and without bearing.

Version 5.1 recommends keeping `trend` for arrivals because nothing beats it.
This script answers the different question that arises once that is overruled
for consistency of method: given that arrivals will use the endpoint rule, which
radius, height and bearing rescue is best?

"Best" needs an objective. The exchange rate the study has used throughout --
a wrong aerodrome corrupts two movement counts and is invisible, a silence
corrupts one and arrives as a filterable null -- makes it

    score(k) = correct - k * wrong

with k = 2 as the working value, reported at 1 and 3 as well so the choice can
be seen to be robust or not.

    python benchmarks/arrival_endpoint_choice.py --results-dir <dir>
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
from abstained_vertical import bearing_deg, angle_between, CANDIDATES
from bearing_whole_sample import courses

PENALTY_NM = 10.0

#: Bases worth testing: the departure setting, the arrival optimum at k=2 and
#: k=3 from the radius x height sweep, and the version 4 published setting.
BASES = [(30, 15000), (30, 10000), (20, 8000), (40, 15000), (40, 10000)]

BEARING_GATES = (None, 0.10, 0.25, 1.0)


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
    apt = (spark.read.parquet(AIRPORTS)
           .select(F.col("ident").alias("apt_ident"),
                   F.col("latitude_deg").cast("double").alias("apt_lat"),
                   F.col("longitude_deg").cast("double").alias("apt_lon"))
           .dropDuplicates(["apt_ident"]))
    ends = (cand.select("track_id", "role", F.col("event_time").alias("t_end"))
            .dropDuplicates(["track_id", "role"]))
    crs = courses(spark, ends)

    pen = F.when(F.col("apt_scheduled") == "yes", F.lit(0.0)).otherwise(F.lit(PENALTY_NM))
    c = (cand.join(F.broadcast(apt), "apt_ident", "left")
         .join(crs, ["track_id", "role"], "left")
         .withColumn("_eff", F.col("dist_nm") + pen)
         .withColumn("align_deg",
                     angle_between(F.col("course"),
                                   bearing_deg(F.col("y_far"), F.col("x_far"),
                                               F.col("apt_lat"), F.col("apt_lon")))))
    w = Window.partitionBy("track_id", "role").orderBy(F.col("_eff").asc_nulls_last())
    best = c.withColumn("_r", F.row_number().over(w)).filter(F.col("_r") == 1).cache()
    print(f"best candidates: {best.count():,}")

    ident = identities_from_candidates(cand)
    gt = label_ground_truth(load_ground_truth(spark, args.months, args.days),
                            airport_locations(spark)).cache()
    N = gt.count()
    print(f"ground-truth flights: {N:,}\n")

    rows = []
    print(f"{'setting':<34}{'cov':>8}{'acc':>8}{'correct':>9}{'wrong':>8}"
          f"{'k=1':>9}{'k=2':>9}{'k=3':>9}")
    for radius, height in BASES:
        gate = (F.col("dist_nm") <= float(radius)) & (
            F.col("on_ground") | (F.col("elev_known")
                                  & (F.col("agl_ft") <= float(height))))
        for deg in BEARING_GATES:
            accept = gate if deg is None else (
                gate | F.coalesce(F.col("align_deg") <= deg, F.lit(False)))
            apt_col = (F.when(accept, F.col("apt_ident"))
                       .when(F.col("at_border"), F.lit("OOA")))
            pred = (best.withColumn("apt", apt_col)
                    .groupBy("track_id").pivot("role", ["adep", "ades"])
                    .agg(F.first("apt")))
            m = score(pred, ident, gt)
            n_gt = m["n_ground_truth"]
            cor = round(m["ades_overall"] * n_gt)
            wr = round(m["ades_coverage"] * n_gt) - cor
            lab = f"{radius} NM / {height:,} ft" + ("" if deg is None else f" + {deg}deg")
            rows.append({"radius_nm": radius, "height_ft": height,
                         "bearing_deg": -1.0 if deg is None else deg,
                         "coverage": m["ades_coverage"], "accuracy": m["ades_accuracy"],
                         "overall": m["ades_overall"], "correct": cor, "wrong": wr,
                         "score_k1": cor - wr, "score_k2": cor - 2 * wr,
                         "score_k3": cor - 3 * wr})
            print(f"{lab:<34}{m['ades_coverage']:>8.2%}{m['ades_accuracy']:>8.2%}"
                  f"{cor:>9,}{wr:>8,}{cor-wr:>9,}{cor-2*wr:>9,}{cor-3*wr:>9,}")

    spark.createDataFrame(rows).toPandas().to_csv(out / "arrival_choice.csv", index=False)
    print(f"\nwritten to {out}")
    spark.stop()


if __name__ == "__main__":
    main()
