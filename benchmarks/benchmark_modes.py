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
    airport_locations, airport_types, label_ground_truth, load_ground_truth,
    score, per_airport_counts, per_type_counts, error_pairs,
)

FL_BASE = "s3a://eurocontrol/opdi/research/flight_list_{mode}"
#: Default candidate cache. Overridable with --candidates so the same sweeps
#: can be run against a second period, whose cache lives elsewhere.
CANDIDATES = "s3a://eurocontrol/opdi/opdi_endpoint_candidates"
MODES = ("trend", "nearest", "endpoint")

#: Radius grid, on the zone table's own band boundaries.
RADII_NM = (5, 10, 20, 30, 40, 60, 80, 100, 110)

#: Height above field elevation. 0 means on-ground only; the sentinel admits
#: any height, which is what makes the endpoint rule collapse into `nearest`.
#: The grid runs to 40,000 ft -- above typical cruise, so the curve is followed
#: all the way to where it flattens rather than being cut off mid-rise at
#: 20,000 ft, where 6 points of coverage still separated it from unrestricted.
HEIGHTS_FT = (0, 500, 1000, 2000, 5000, 8000, 10000, 15000, 20000,
              25000, 30000, 35000, 40000, 1e9)

#: Scheduled-service penalties. 0 reproduces the unbiased ranking exactly and
#: is the control for the tie-break.
PENALTIES_NM = (0, 5, 10, 15, 20, 30)

#: Approach-cone slopes, ft of height allowed per NM of distance. The physical
#: anchors are the 3-degree glideslope at 318 ft/NM and the "three times the
#: flight level" descent rule of thumb at about 333 ft/NM; the sweep brackets
#: both by an order of magnitude in each direction so the optimum is not
#: assumed.
CONE_SLOPES_FT_NM = (150, 200, 250, 300, 333, 400, 500, 650, 800, 1000, 1500)

#: Outer caps for the cone. At 333 ft/NM a 110 NM cap is already a 36,600 ft
#: ceiling, so the cone is the binding constraint and the cap only bounds the
#: join.
CONE_RADII_NM = (40, 60, 110)


def from_flight_list(spark: SparkSession, mode: str):
    """(predictions, identities) from a per-mode flight list.

    .. warning::

       This reads a table the *pipeline* wrote, not the candidate cache. If
       that table predates the parameters under test, every metric derived from
       it -- the mode comparison and both cross-checks -- silently describes an
       older run while looking freshly computed. The sweeps in this module read
       the candidate cache instead and do not have that failure mode.

       Rebuild the flight lists with ``process_dai`` before trusting these.
    """
    path = FL_BASE.format(mode=mode)
    fl = spark.read.parquet(path)
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


def predictions_cone(
    cand: DataFrame, radius_nm: float, slope_ft_nm: float, penalty_nm: float,
    floor_ft: float = 1000.0,
) -> DataFrame:
    """The endpoint rule with an approach cone instead of a rectangle.

    The box rule accepts an endpoint when it is inside `radius_nm` *and* below
    `height_ft`, treating the two as independent. They are not. An aircraft
    descending toward an aerodrome trades height for distance at roughly a
    constant rate -- the 3-degree glideslope is 318 ft/NM, and the "three times
    the flight level" rule of thumb is about 333 ft/NM -- so the endpoints that
    genuinely belong to an aerodrome lie in a cone, not a box.

    The two shapes disagree in both corners, and both disagreements matter:

    * **close and high** is inside the box and outside the cone. An aircraft at
      15,000 ft overhead an aerodrome is overflying it, not using it. The box
      accepts this and it is a pure source of wrong labels.
    * **far and low** is outside the box and inside the cone. An arrival whose
      reception dies at 8,000 ft and 30 NM is on profile and is a good
      candidate. The box rejects it once the radius is tight.

    ``floor_ft`` keeps the cone from collapsing to nothing at the threshold, so
    an endpoint on or beside the runway is always admitted.
    """
    penalty = F.when(F.col("apt_scheduled") == "yes", F.lit(0.0)).otherwise(
        F.lit(float(penalty_nm))
    )
    c = cand.withColumn("_eff", F.col("dist_nm") + penalty)
    w = Window.partitionBy("track_id", "role").orderBy(F.col("_eff").asc_nulls_last())
    best = c.withColumn("_r", F.row_number().over(w)).filter(F.col("_r") == 1)

    ceiling = F.greatest(
        F.lit(float(floor_ft)), F.col("dist_nm") * F.lit(float(slope_ft_nm))
    )
    ok = (F.col("dist_nm") <= float(radius_nm)) & (
        F.col("on_ground") | (F.col("elev_known") & (F.col("agl_ft") <= ceiling))
    )
    apt = F.when(ok, F.col("apt_ident")).otherwise(
        F.when(F.col("at_border"), F.lit("OOA"))
    )
    return (
        best.withColumn("apt", apt)
        .groupBy("track_id")
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
    ap.add_argument("--candidates", default=CANDIDATES,
                    help="candidate cache to sweep (default: the 2025 table)")
    ap.add_argument("--sweeps-only", action="store_true",
                    help="skip the mode comparison and cross-checks, "
                         "which read flight lists another run wrote")
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
    #
    # Skippable, and skipped by the V6 regeneration chain. This block reads the
    # flight lists under research/, which some *other* run wrote at whatever
    # parameters were current then; the V6 report gets its mode comparison from
    # flight_list_v6.py, which builds the tables it scores. Leaving this on
    # would spend cluster time producing numbers nothing reads, and leave a
    # second mode_comparison.csv lying around for someone to pick up by
    # mistake.
    # Skipped by the V6 regeneration chain. This block reads the flight lists
    # under research/, which some *other* run wrote at whatever parameters were
    # current then; the V6 report takes its mode comparison from
    # flight_list_v6.py, which builds the tables it scores. Leaving it on spends
    # cluster time producing numbers nothing reads, and leaves a second
    # mode_comparison.csv for someone to pick up by mistake.
    if args.sweeps_only:
        print("\n  skipping the mode comparison and cross-checks (--sweeps-only):\n"
              "  they read pipeline output written by another run.")
    else:
        print("\n  NOTE: the mode comparison and cross-checks below read the\n"
              "  flight lists under research/, not the candidate cache. Confirm\n"
              "  they were written with the parameters under test.")
        rows = []
        for mode in MODES:
            pred, ident = from_flight_list(spark, mode)
            m = score(pred, ident, gt)
            m["mode"] = mode
            rows.append(m)
            print(f"  {mode:9} ADEP cov {m['adep_coverage']:6.2%} "
                  f"acc {m['adep_accuracy']:6.2%}"
                  f"   ADES cov {m['ades_coverage']:6.2%} "
                  f"acc {m['ades_accuracy']:6.2%}")
        spark.createDataFrame(rows).toPandas().to_csv(
            out / "mode_comparison.csv", index=False)

        pred, ident = from_flight_list(spark, "endpoint")
        per_airport_counts(pred, ident, gt).toPandas().to_csv(
            out / "per_airport.csv", index=False)
        pt = per_type_counts(pred, ident, gt, airport_types(spark)).toPandas()
        pt.to_csv(out / "per_type.csv", index=False)

    if args.skip_sweeps:
        print(f"\nwritten to {out}")
        spark.stop()
        return

    cand = spark.read.parquet(args.candidates).cache()
    ident_c = identities_from_candidates(cand)
    print(f"cached candidates: {cand.count():,}")

    # -- approach cone, as an alternative to the rectangle -------------------
    # Cheap enough to always run: a handful of cells over the same cache.
    cone = []
    for r in CONE_RADII_NM:
        for k in CONE_SLOPES_FT_NM:
            p = predictions_cone(cand, r, k, penalty_nm=10.0)
            m = score(p, ident_c, gt)
            m.update(radius_nm=float(r), slope_ft_nm=float(k), penalty_nm=10.0)
            cone.append(m)
            print(f"  cone r<={r:>3} k={k:>5.0f} ft/NM  ADEP {m['adep_coverage']:6.2%}/"
                  f"{m['adep_accuracy']:6.2%}   ADES {m['ades_coverage']:6.2%}/"
                  f"{m['ades_accuracy']:6.2%}")
    spark.createDataFrame(cone).toPandas().to_csv(out / "sweep_cone.csv", index=False)

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

    print(f"\nwritten to {out}")
    spark.stop()


if __name__ == "__main__":
    main()
