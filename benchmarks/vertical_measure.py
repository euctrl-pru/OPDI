"""
Which vertical measure can be trusted: broadcast ``vert_rate``, or altitude slope?

The motion study found the two disagree in sign on a large share of arrivals and
could not say which was wrong, so it drew no vertical conclusion. This settles
it by introducing a third measure that is more robust than either:

* ``vr_mean``    -- the broadcast vertical rate, averaged over the window.
* ``slope_2pt``  -- (altitude at the window edge nearest the endpoint minus
                    altitude at the far edge) / elapsed. Two samples only.
* ``slope_ols``  -- ordinary least squares of altitude on time across *every*
                    sample in the window. The adjudicator: it uses all the data
                    and is insensitive to either edge being unrepresentative.

Whichever of the first two agrees with the OLS fit is the one to use.

The candidate explanation is staleness. The decimation study measured that a
third of low-altitude rows carry a position already recorded, and a
carried-forward row repeats its vertical rate rather than measuring one. So
agreement is also reported banded by how fresh the window is, using
``age = event_time - last_pos_update``. If the measures agree on fresh windows
and diverge on stale ones, the cause is established rather than guessed.

    python benchmarks/vertical_measure.py --results-dir <dir>
"""

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import functions as F

import osn_sample
from osn_sample import build_spark, load_dotenv
from abstained_vertical import best_candidate, CANDIDATES, TRACKS, M_S_TO_FT_MIN, M_TO_FT

WINDOWS_MIN = (5, 10, 20)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
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
    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)

    cand = spark.read.parquet(CANDIDATES)
    blk = (best_candidate(cand)
           .filter("role = 'ades' and abstained")
           .select("track_id", F.col("event_time").alias("t_end"))
           .cache())
    print(f"abstained arrival block: {blk.count():,} flights")

    tr_all = spark.read.parquet(TRACKS)
    alt = "baro_altitude_c" if "baro_altitude_c" in tr_all.columns else "baro_altitude"
    tr = (tr_all
          .select("track_id", "event_time", "vert_rate", "last_pos_update",
                  F.col(alt).alias("alt_m"))
          .filter(F.col("alt_m").isNotNull())
          .join(F.broadcast(blk), "track_id", "inner")
          .withColumn("_off", F.abs(F.unix_timestamp("t_end")
                                    - F.unix_timestamp("event_time")))
          .withColumn("_t", F.unix_timestamp("event_time").cast("double"))
          .withColumn("_age", F.col("event_time").cast("double")
                      - F.col("last_pos_update").cast("double")))

    rows = []
    for mins in WINDOWS_MIN:
        w = tr.filter(F.col("_off") <= mins * 60)
        # OLS slope of altitude on time: (n*Sxy - Sx*Sy) / (n*Sxx - Sx^2).
        # Pure aggregates, so it stays native Spark.
        g = (w.groupBy("track_id").agg(
                F.count(F.lit(1)).alias("n"),
                F.avg(F.col("vert_rate") * M_S_TO_FT_MIN).alias("vr_mean"),
                F.min("_off").alias("off_min"), F.max("_off").alias("off_max"),
                F.min_by("alt_m", "_off").alias("a_near"),
                F.max_by("alt_m", "_off").alias("a_far"),
                F.sum("_t").alias("sx"), F.sum("alt_m").alias("sy"),
                F.sum(F.col("_t") * F.col("alt_m")).alias("sxy"),
                F.sum(F.col("_t") * F.col("_t")).alias("sxx"),
                F.avg("_age").alias("age_mean"),
                F.avg(F.when(F.col("_age") <= 5, 1.0).otherwise(0.0)).alias("fresh_share"),
                F.avg(F.when(F.col("vert_rate") == 0, 1.0).otherwise(0.0)).alias("vr_zero_share"),
             )
             .filter("n >= 5")
             .withColumn("elapsed_s", F.col("off_max") - F.col("off_min"))
             .filter("elapsed_s >= 60"))
        den = F.col("n") * F.col("sxx") - F.col("sx") * F.col("sx")
        g = (g.filter(den != 0)
             # OLS is metres per second; ft/min needs *3.28084*60. Positive is
             # a climb as time runs on, which for an arrival window means the
             # aircraft was going up -- same convention as slope_2pt below.
             .withColumn("slope_ols",
                         (F.col("n") * F.col("sxy") - F.col("sx") * F.col("sy")) / den
                         * M_TO_FT * 60.0)
             # a_near is at the endpoint, i.e. latest in time for an arrival.
             .withColumn("slope_2pt",
                         (F.col("a_near") - F.col("a_far")) * M_TO_FT
                         / (F.col("elapsed_s") / 60.0)))
        g.cache()
        n = g.count()

        agree = lambda a, b: F.avg(F.when(F.signum(a) == F.signum(b), 1.0).otherwise(0.0))
        stats = g.agg(
            agree(F.col("vr_mean"), F.col("slope_ols")).alias("vr_vs_ols"),
            agree(F.col("slope_2pt"), F.col("slope_ols")).alias("t2pt_vs_ols"),
            agree(F.col("vr_mean"), F.col("slope_2pt")).alias("vr_vs_2pt"),
            F.avg("vr_zero_share").alias("vr_zero"),
            F.avg("fresh_share").alias("fresh"),
            F.expr("percentile_approx(age_mean, 0.5)").alias("age_p50"),
        ).first()
        print(f"\n== {mins} min window, {n:,} flights ==")
        print(f"   vert_rate  vs OLS : {stats['vr_vs_ols']:.1%}")
        print(f"   2-point    vs OLS : {stats['t2pt_vs_ols']:.1%}   <- higher is the trustworthy one")
        print(f"   vert_rate  vs 2pt : {stats['vr_vs_2pt']:.1%}")
        print(f"   mean share of rows with vert_rate exactly 0: {stats['vr_zero']:.1%}")
        print(f"   mean share of rows fresher than 5 s: {stats['fresh']:.1%}, "
              f"median mean-age {stats['age_p50']:.0f} s")

        # Banded by freshness: if the measures agree on fresh windows and
        # diverge on stale ones, staleness is the cause rather than a guess.
        band = (F.when(F.col("fresh_share") >= 0.9, F.lit("4. >=90% fresh"))
                .when(F.col("fresh_share") >= 0.6, F.lit("3. 60-90%"))
                .when(F.col("fresh_share") >= 0.3, F.lit("2. 30-60%"))
                .otherwise(F.lit("1. <30% fresh")))
        by = (g.withColumn("b", band).groupBy("b")
              .agg(F.count(F.lit(1)).alias("flights"),
                   agree(F.col("vr_mean"), F.col("slope_ols")).alias("vr_vs_ols"),
                   agree(F.col("slope_2pt"), F.col("slope_ols")).alias("t2pt_vs_ols"))
              .orderBy("b"))
        print(f"   {'freshness':>16} {'flights':>9} {'vert_rate vs OLS':>18} {'2-point vs OLS':>16}")
        for r in by.collect():
            print(f"   {r['b']:>16} {r['flights']:>9,} {r['vr_vs_ols']:>17.1%} {r['t2pt_vs_ols']:>15.1%}")
            rows.append({"window_min": mins, "freshness": r["b"], "flights": r["flights"],
                         "vr_vs_ols": r["vr_vs_ols"], "t2pt_vs_ols": r["t2pt_vs_ols"],
                         "overall_vr_vs_ols": stats["vr_vs_ols"],
                         "overall_t2pt_vs_ols": stats["t2pt_vs_ols"],
                         "vr_zero_share": stats["vr_zero"],
                         "fresh_share": stats["fresh"]})
        g.unpersist()

    spark.createDataFrame(rows).toPandas().to_csv(out / "vertical_measure.csv", index=False)
    print(f"\nwritten to {out}")
    spark.stop()


if __name__ == "__main__":
    main()
