"""
Score the base detection rule against variants that add motion evidence.

The base is version 4.5's recommendation: `endpoint` at 30 NM, 15,000 ft, with a
10 NM scheduled-service penalty. Where its abstention refuses a candidate, the
variants get a second chance from the trajectory:

* **+bearing** -- accept the refused candidate if the aircraft's course over a
  window points within a few degrees of it.
* **+bearing +rate** -- as above, and additionally require that the aircraft was
  descending (arrivals) or climbing (departures) across the window.

The rate arm is included because it was asked for, not because it is expected to
help: the vertical-measure study found that in this population `vert_rate` and
the altitude slope agree in sign at close to chance, because roughly half the
rows carry values held over from an earlier measurement. A bearing between two
genuinely measured positions survives that; a *rate* does not, because it
divides by an elapsed time the timestamps get wrong.

Unlike the block-level sweeps, this scores the whole flight population, so the
numbers are directly comparable with the published headline figures.

    python benchmarks/motion_model_compare.py --results-dir <dir>
"""

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import functions as F

import osn_sample
from osn_sample import build_spark, load_dotenv
from adep_ades import (
    airport_locations, label_ground_truth, load_ground_truth, score, AIRPORTS,
)
from benchmark_modes import identities_from_candidates
from abstained_vertical import (
    best_candidate, bearing_deg, angle_between, CANDIDATES, TRACKS, M_TO_FT,
)

#: (label, window minutes, angle gate degrees)
BEARING_VARIANTS = [
    ("7 min / 0.10 deg", 7, 0.10),
    ("7 min / 0.25 deg", 7, 0.25),
    ("7 min / 0.50 deg", 7, 0.50),
    ("7 min / 0.75 deg", 7, 0.75),
    ("7 min / 1 deg", 7, 1.0),
    ("7 min / 2 deg", 7, 2.0),
    ("7 min / 3 deg", 7, 3.0),
    ("20 min / 1 deg", 20, 1.0),
    ("30 min / 1 deg", 30, 1.0),
]

#: Window for the vertical test, and the sense required per role.
RATE_WINDOW_MIN = 20


def motion_features(spark, blk, mins, role):
    """Bearing alignment and OLS altitude slope per track, over `mins` minutes."""
    tr_all = spark.read.parquet(TRACKS)
    alt = "baro_altitude_c" if "baro_altitude_c" in tr_all.columns else "baro_altitude"
    tr = (tr_all
          .select("track_id", "event_time", "lat", "lon", F.col(alt).alias("alt_m"))
          .filter(F.col("alt_m").isNotNull() & F.col("lat").isNotNull())
          .join(F.broadcast(blk), "track_id", "inner")
          .withColumn("_off", F.abs(F.unix_timestamp("t_end")
                                    - F.unix_timestamp("event_time")))
          .filter(F.col("_off") <= mins * 60)
          .withColumn("_t", F.unix_timestamp("event_time").cast("double")))
    g = (tr.groupBy("track_id").agg(
            F.count(F.lit(1)).alias("n"),
            F.min("_off").alias("o0"), F.max("_off").alias("o1"),
            F.min_by("lat", "_off").alias("y_near"), F.min_by("lon", "_off").alias("x_near"),
            F.max_by("lat", "_off").alias("y_far"), F.max_by("lon", "_off").alias("x_far"),
            F.min("apt_lat_x").alias("ay"), F.min("apt_lon_x").alias("ax"),
            F.sum("_t").alias("sx"), F.sum("alt_m").alias("sy"),
            F.sum(F.col("_t") * F.col("alt_m")).alias("sxy"),
            F.sum(F.col("_t") * F.col("_t")).alias("sxx"))
         .filter("n >= 5")
         .withColumn("elapsed_s", F.col("o1") - F.col("o0"))
         .filter("elapsed_s >= 60"))
    den = F.col("n") * F.col("sxx") - F.col("sx") * F.col("sx")
    return (g.filter(den != 0)
            .withColumn("slope_ols",
                        (F.col("n") * F.col("sxy") - F.col("sx") * F.col("sy")) / den
                        * M_TO_FT * 60.0)
            .withColumn("align_deg",
                        angle_between(
                            bearing_deg(F.col("y_far"), F.col("x_far"),
                                        F.col("y_near"), F.col("x_near")),
                            bearing_deg(F.col("y_far"), F.col("x_far"),
                                        F.col("ay"), F.col("ax"))))
            .select("track_id", "align_deg", "slope_ols"))


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
    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)

    cand = spark.read.parquet(CANDIDATES)
    best = best_candidate(cand).cache()
    ident = identities_from_candidates(cand)
    gt = label_ground_truth(load_ground_truth(spark, args.months, args.days),
                            airport_locations(spark)).cache()
    print(f"ground-truth flights: {gt.count():,}")

    apt_pos = (spark.read.parquet(AIRPORTS)
               .select(F.col("ident").alias("apt_ident"),
                       F.col("latitude_deg").cast("double").alias("apt_lat_x"),
                       F.col("longitude_deg").cast("double").alias("apt_lon_x"))
               .dropDuplicates(["apt_ident"]))

    # Motion features are only needed where the base rule abstains, which is
    # what keeps this affordable.
    feats = {}
    for role in ("adep", "ades"):
        blk = (best.filter((F.col("role") == role) & F.col("abstained"))
               .select("track_id", "apt_ident", F.col("event_time").alias("t_end"))
               .join(F.broadcast(apt_pos), "apt_ident", "left")
               .select("track_id", "t_end", "apt_lat_x", "apt_lon_x"))
        for mins in sorted({m for _, m, _ in BEARING_VARIANTS} | {RATE_WINDOW_MIN}):
            feats[(role, mins)] = motion_features(spark, blk, mins, role).cache()
            print(f"  motion features {role} {mins} min: "
                  f"{feats[(role, mins)].count():,} tracks")

    def predictions(rescue):
        """rescue(role, mins) -> Column deciding whether to accept a refused
        candidate, or None for the base rule."""
        sides = []
        for role in ("adep", "ades"):
            b = best.filter(F.col("role") == role)
            if rescue is None:
                b = b.withColumn("_rescue", F.lit(False))
            else:
                mins, cond = rescue(role)
                b = (b.join(feats[(role, mins)], "track_id", "left")
                     .withColumn("_rescue", F.coalesce(cond, F.lit(False))))
            apt = (F.when(F.col("gate_ok") | F.col("_rescue"), F.col("apt_ident"))
                   .when(F.col("at_border"), F.lit("OOA")))
            sides.append(b.select("track_id", F.lit(role).alias("role"),
                                  apt.alias("apt")))
        u = sides[0].unionByName(sides[1])
        return u.groupBy("track_id").pivot("role", ["adep", "ades"]).agg(F.first("apt"))

    rows = []
    def run(label, rescue):
        m = score(predictions(rescue), ident, gt)
        m["model"] = label
        rows.append(m)
        print(f"  {label:<28} ADEP {m['adep_coverage']:6.2%}/{m['adep_accuracy']:6.2%}"
              f"/{m['adep_overall']:6.2%}   ADES {m['ades_coverage']:6.2%}/"
              f"{m['ades_accuracy']:6.2%}/{m['ades_overall']:6.2%}")

    print("\nscoring:")
    run("base (30 NM / 15,000 ft)", None)
    for lab, mins, deg in BEARING_VARIANTS:
        run(f"+bearing {lab}",
            lambda role, mins=mins, deg=deg: (mins, F.col("align_deg") <= deg))
    # The rate arm: same bearing gate, plus a demand that the aircraft was
    # going the right way vertically -- down toward an arrival, up away from a
    # departure.
    for lab, mins, deg in [BEARING_VARIANTS[4], BEARING_VARIANTS[6]]:
        def rescue(role, mins=mins, deg=deg):
            vert = (F.col("slope_ols") < 0) if role == "ades" else (F.col("slope_ols") > 0)
            return (mins, (F.col("align_deg") <= deg) & vert)
        run(f"+bearing {lab} +rate", rescue)

    spark.createDataFrame(rows).toPandas().to_csv(out / "model_compare.csv", index=False)
    print(f"\nwritten to {out}")
    spark.stop()


if __name__ == "__main__":
    main()
