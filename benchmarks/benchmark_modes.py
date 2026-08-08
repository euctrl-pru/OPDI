"""
Benchmark the pipeline's ADEP/ADES modes against Network Manager ground truth.

Consumes what the pipeline produced -- it does not reimplement detection. The
three headline numbers come from the per-mode flight lists written by
``FlightListProcessor.process_dai``; the sweeps come from the cached endpoint
candidate table, so varying a threshold is a filter and a re-rank rather than a
pipeline run.

    python benchmarks/benchmark_modes.py --months 202506 \\
        --days 2025-06-05 2025-06-06 2025-06-07 --results-dir <dir>
"""

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

import osn_sample
from osn_sample import build_spark, load_dotenv
from adep_ades import (
    airport_locations, label_ground_truth, load_ground_truth, score,
    per_airport_counts, error_pairs,
)

FL_BASE = "s3a://eurocontrol/opdi/research/flight_list_{mode}"
CANDIDATES = "s3a://eurocontrol/opdi/opdi_endpoint_candidates"
MODES = ("trend", "nearest", "endpoint")

#: Radius grid, on the zone table's own band boundaries.
RADII_NM = (5, 10, 20, 30, 40, 60, 80, 100, 110)

#: Height above field elevation. 0 means on-ground only; the sentinel admits
#: any height, which is what makes the endpoint rule collapse into `nearest`.
HEIGHTS_FT = (0, 500, 1000, 2000, 5000, 8000, 10000, 15000, 20000, 1e9)

#: Scheduled-service penalties. 0 reproduces the unbiased ranking exactly and
#: is the control for the tie-break.
PENALTIES_NM = (0, 5, 10, 15, 20, 30)


def from_flight_list(spark: SparkSession, mode: str):
    """(predictions, identities) from a per-mode flight list."""
    fl = spark.read.parquet(FL_BASE.format(mode=mode))
    pred = fl.select(
        F.col("ID").alias("track_id"),
        F.col("ADEP").alias("adep"),
        F.col("ADES").alias("ades"),
    )
    ident = fl.select(
        F.col("ID").alias("track_id"),
        F.lower(F.col("ICAO24")).alias("icao24"),
        F.trim(F.col("FLT_ID")).alias("callsign"),
        F.to_date(F.col("FIRST_SEEN")).alias("day"),
        F.col("FIRST_SEEN").alias("t_start"),
    )
    return pred, ident


def predictions_from_candidates(
    cand: DataFrame, radius_nm: float, height_ft: float, penalty_nm: float
) -> DataFrame:
    """Apply the endpoint rule to cached candidates at one parameter setting.

    Mirrors ``FlightListProcessor.classify_endpoints`` in ``endpoint`` mode.
    Kept here rather than imported so a sweep cell is a plain DataFrame
    operation -- the pipeline method is written to produce a flight list, which
    is more work than a sweep needs.
    """
    penalty = F.when(F.col("apt_scheduled") == "yes", F.lit(0.0)).otherwise(
        F.lit(float(penalty_nm))
    )
    c = cand.withColumn("_eff", F.col("dist_nm") + penalty)
    w = Window.partitionBy("track_id", "role").orderBy(F.col("_eff").asc_nulls_last())
    best = c.withColumn("_r", F.row_number().over(w)).filter(F.col("_r") == 1)

    ok = F.col("dist_nm") <= float(radius_nm)
    ok = ok & (
        F.col("on_ground")
        | (F.col("elev_known") & (F.col("agl_ft") <= float(height_ft)))
    )
    apt = F.when(ok, F.col("apt_ident")).otherwise(
        F.when(F.col("at_border"), F.lit("OOA"))
    )
    best = best.withColumn("apt", apt)

    return (
        best.groupBy("track_id")
        .pivot("role", ["adep", "ades"])
        .agg(F.first("apt"))
    )


def identities_from_candidates(cand: DataFrame) -> DataFrame:
    """track_id -> icao24, callsign, day, using the departure-side endpoint."""
    dep = cand.filter(F.col("role") == "adep")
    w = Window.partitionBy("track_id").orderBy("event_time")
    return (
        dep.withColumn("_r", F.row_number().over(w))
        .filter(F.col("_r") == 1)
        .select(
            "track_id",
            F.lower("icao24").alias("icao24"),
            F.trim("flight_id").alias("callsign"),
            F.to_date("event_time").alias("day"),
            F.col("event_time").alias("t_start"),
        )
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--months", nargs="+", default=["202506"])
    ap.add_argument("--days", nargs="+", required=True)
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--executors", type=int, default=4)
    ap.add_argument("--ui-port", type=int, default=4041)
    ap.add_argument("--skip-sweeps", action="store_true")
    ap.add_argument("--local", action="store_true",
                    help="run in local mode. The benchmark inputs are small -- "
                         "7.5M cached candidates, 233k flight records, 95k "
                         "ground-truth flights -- so it does not need the "
                         "cluster, and the namespace is shared.")
    ap.add_argument("--cores", type=int, default=6)
    ap.add_argument("--driver-memory", default="8g")
    args = ap.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
    load_dotenv()
    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(args.cores, args.driver_memory, distributed=not args.local)
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "96")

    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)

    gt = load_ground_truth(spark, args.months, args.days)
    gt = label_ground_truth(gt, airport_locations(spark)).cache()
    print(f"ground-truth flights: {gt.count():,}")

    # -- headline: the three modes, as the pipeline actually produced them ---
    rows = []
    for mode in MODES:
        pred, ident = from_flight_list(spark, mode)
        m = score(pred, ident, gt)
        m["mode"] = mode
        rows.append(m)
        print(f"  {mode:9} ADEP cov {m['adep_coverage']:6.2%} acc {m['adep_accuracy']:6.2%}"
              f"   ADES cov {m['ades_coverage']:6.2%} acc {m['ades_accuracy']:6.2%}"
              f"   in-area acc {m['adep_inarea_accuracy']:6.2%}")
    spark.createDataFrame(rows).toPandas().to_csv(out / "mode_comparison.csv", index=False)

    if args.skip_sweeps:
        spark.stop()
        return

    cand = spark.read.parquet(CANDIDATES).cache()
    ident_c = identities_from_candidates(cand)
    print(f"cached candidates: {cand.count():,}")

    # -- radius x height ----------------------------------------------------
    grid = []
    for r in RADII_NM:
        for h in HEIGHTS_FT:
            p = predictions_from_candidates(cand, r, h, penalty_nm=10.0)
            m = score(p, ident_c, gt)
            m.update(radius_nm=float(r), height_ft=float(h), penalty_nm=10.0)
            grid.append(m)
            print(f"  r<={r:>3} h<={h:>10.0f}  ADEP {m['adep_coverage']:6.2%}/"
                  f"{m['adep_accuracy']:6.2%}   ADES {m['ades_coverage']:6.2%}/"
                  f"{m['ades_accuracy']:6.2%}")
    spark.createDataFrame(grid).toPandas().to_csv(out / "sweep_radius_height.csv", index=False)

    # -- scheduled-service penalty -----------------------------------------
    pen = []
    for q in PENALTIES_NM:
        p = predictions_from_candidates(cand, 40.0, 15000.0, penalty_nm=q)
        m = score(p, ident_c, gt)
        m.update(radius_nm=40.0, height_ft=15000.0, penalty_nm=float(q))
        pen.append(m)
        print(f"  penalty {q:>3} NM   ADEP {m['adep_coverage']:6.2%}/{m['adep_accuracy']:6.2%}"
              f"   ADES {m['ades_coverage']:6.2%}/{m['ades_accuracy']:6.2%}")
    spark.createDataFrame(pen).toPandas().to_csv(out / "sweep_penalty.csv", index=False)

    # -- cross-checks against the reference ---------------------------------
    pred, ident = from_flight_list(spark, "endpoint")
    apt = airport_locations(spark)
    per_airport_counts(pred, ident, gt).toPandas().to_csv(
        out / "per_airport.csv", index=False
    )
    print(f"\nwritten to {out}")
    spark.stop()


if __name__ == "__main__":
    main()
