"""
Does bucket sampling plus the tuned arrival rule close the gap to `trend`?

Three arms, all scored on the same day and the same ground truth:

1. **trend on modulo data** -- production as it stands.
2. **endpoint 30 NM / 10,000 ft + bearing 0.1 deg, on modulo data** -- the best
   endpoint-family setting for arrivals.
3. **the same rule on bucket-decimated data** -- adding the sampling change on
   top.

The decimation study measured bucket sampling as worth about +55 correct
arrivals per day at the 30 NM / 15,000 ft setting, so the expectation is that it
narrows the gap without closing it. Expectation is not measurement.

One day only: the bucket-sampled pipeline was run for 2025-06-05 alone, so every
arm is restricted to it and the counts are a third of the three-day figures
quoted elsewhere.

    python benchmarks/arrivals_bucket_vs_trend.py --results-dir <dir>
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
from benchmark_modes import identities_from_candidates, from_flight_list
from abstained_vertical import bearing_deg, angle_between

MODULO_CAND = "s3a://eurocontrol/opdi/opdi_endpoint_candidates"
MODULO_TRACKS = "s3a://eurocontrol/opdi/osn_tracks"
BUCKET_CAND = "s3a://eurocontrol/opdi/research/cand_bucket"
BUCKET_TRACKS = "s3a://eurocontrol/opdi/research/tracks_bucket"

RADIUS_NM, HEIGHT_FT, PENALTY_NM = 30.0, 10000.0, 10.0
ALIGN_DEG, WINDOW_MIN = 0.10, 7


def courses_from(spark, tracks_path, ends):
    tr = (spark.read.parquet(tracks_path)
          .select("track_id", "event_time", "lat", "lon")
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
         .filter("n >= 5").filter(F.col("o1") - F.col("o0") >= 60))
    return (g.withColumn("course", bearing_deg(F.col("y_far"), F.col("x_far"),
                                               F.col("y_near"), F.col("x_near")))
            .select("track_id", "role", "y_far", "x_far", "course"))


def tuned_predictions(spark, cand_path, tracks_path, apt, days):
    cand = (spark.read.parquet(cand_path)
            .filter(F.to_date("event_time").isin(days)))
    ends = (cand.select("track_id", "role", F.col("event_time").alias("t_end"))
            .dropDuplicates(["track_id", "role"]))
    crs = courses_from(spark, tracks_path, ends)
    pen = F.when(F.col("apt_scheduled") == "yes", F.lit(0.0)).otherwise(F.lit(PENALTY_NM))
    c = (cand.join(F.broadcast(apt), "apt_ident", "left")
         .join(crs, ["track_id", "role"], "left")
         .withColumn("_eff", F.col("dist_nm") + pen)
         .withColumn("align_deg",
                     angle_between(F.col("course"),
                                   bearing_deg(F.col("y_far"), F.col("x_far"),
                                               F.col("apt_lat"), F.col("apt_lon")))))
    w = Window.partitionBy("track_id", "role").orderBy(F.col("_eff").asc_nulls_last())
    best = c.withColumn("_r", F.row_number().over(w)).filter(F.col("_r") == 1)
    gate = (F.col("dist_nm") <= RADIUS_NM) & (
        F.col("on_ground") | (F.col("elev_known") & (F.col("agl_ft") <= HEIGHT_FT)))
    accept = gate | F.coalesce(F.col("align_deg") <= ALIGN_DEG, F.lit(False))
    apt_col = F.when(accept, F.col("apt_ident")).when(F.col("at_border"), F.lit("OOA"))
    pred = (best.withColumn("apt", apt_col)
            .groupBy("track_id").pivot("role", ["adep", "ades"]).agg(F.first("apt")))
    return pred, identities_from_candidates(cand)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--months", nargs="+", default=["202506"])
    ap.add_argument("--days", nargs="+", default=["2025-06-05"])
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

    apt = (spark.read.parquet(AIRPORTS)
           .select(F.col("ident").alias("apt_ident"),
                   F.col("latitude_deg").cast("double").alias("apt_lat"),
                   F.col("longitude_deg").cast("double").alias("apt_lon"))
           .dropDuplicates(["apt_ident"]))
    gt = label_ground_truth(load_ground_truth(spark, args.months, args.days),
                            airport_locations(spark)).cache()
    N = gt.count()
    print(f"ground-truth flights on {args.days}: {N:,}\n")

    rows = []
    def run(label, pred, ident):
        m = score(pred, ident, gt)
        n = m["n_ground_truth"]
        cor = round(m["ades_overall"] * n); wr = round(m["ades_coverage"] * n) - cor
        m["model"] = label; m["ades_correct"] = cor; m["ades_wrong"] = wr
        rows.append(m)
        print(f"  {label:<44}{m['ades_coverage']:>8.2%}{m['ades_accuracy']:>8.2%}"
              f"{cor:>9,}{wr:>8,}{cor-2*wr:>10,}")

    print(f"  {'arm':<44}{'cov':>8}{'acc':>8}{'correct':>9}{'wrong':>8}{'score@2':>10}")
    p, i = from_flight_list(spark, "trend")
    run("trend, modulo data (production)", p, i)
    p, i = tuned_predictions(spark, MODULO_CAND, MODULO_TRACKS, apt, args.days)
    run("endpoint 30/10,000 + 0.1deg, modulo data", p, i)
    p, i = tuned_predictions(spark, BUCKET_CAND, BUCKET_TRACKS, apt, args.days)
    run("endpoint 30/10,000 + 0.1deg, bucket data", p, i)

    spark.createDataFrame(rows).toPandas().to_csv(out / "bucket_vs_trend.csv", index=False)
    print(f"\nwritten to {out}")
    spark.stop()


if __name__ == "__main__":
    main()
