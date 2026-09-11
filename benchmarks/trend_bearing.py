"""
Does a bearing test improve `trend`? The comparison never made.

Version 5 applied bearing alignment to the *endpoint* family -- rescue, veto,
replace and rerank -- and version 6 re-measured all four, decisively against.
But `trend` was never tested with bearing at all, and it is the algorithm that
actually ships for arrivals. The omission matters because the two methods fail
differently:

* `endpoint` fails by **abstaining**: its gate refuses a fix that is too far or
  too high, so the natural bearing variant is a *rescue* -- answer anyway when
  the trajectory points at the aerodrome.
* `trend` fails by **misnaming**: it has no gate, so it almost always answers,
  and its errors are candidates in roughly the same place as the right one.
  The natural variant there is a *rerank* -- among aerodromes the votes admit,
  prefer the one the track is actually pointing at.

That is the test here, and it is a fairer one than the endpoint rerank, which
had to choose among every aerodrome within 110 NM. Trend's candidate set is
already filtered by the vote rule, so alignment is asked a much narrower
question: not "which aerodrome is this", but "which of these two or three".

Three variants, against the shipped trend configuration:

* **rerank**  -- choose by alignment, then by effective distance.
* **tie-break** -- choose by distance as now, but let alignment settle
  candidates whose effective distances are within a few miles of each other.
  The conservative version: it can only change an answer that was close.
* **veto** -- keep the distance choice, but abstain when the best candidate is
  badly misaligned. The only variant that can raise accuracy.

    python benchmarks/trend_bearing.py --results-dir <dir>
"""

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

import osn_sample
from osn_sample import build_spark, load_dotenv
from adep_ades import (
    airport_locations, label_ground_truth, load_ground_truth, score, AIRPORTS,
)
from abstained_vertical import bearing_deg, angle_between, TRACKS
# The paired cache, not v6's `research/trend_votes`.
#
# This job needs `up_60` / `dn_60` / `dist_60`, and the paired cache carries
# the whole flight-level family alongside the above-field one -- so it holds a
# superset of what v6's cache did.
#
# It used to import v6's cache while v6.2's registry declared the paired one as
# its input. The declaration and the behaviour disagreed, and nothing noticed
# until v6's cache was deleted from the bucket and this job died on
# PATH_NOT_FOUND. A declared dependency that is not the real one is worse than
# an undeclared one, because it reads as verified.
from trend_sweep_agl import CACHE, haversine_nm

#: The shipped trend configuration, which this is trying to improve on.
FL_CAP, MARGIN, RADIUS_NM, PENALTY_NM = 60, 2, 20.0, 10.0

#: Minutes of trajectory used to establish the course near the endpoint. 7 is
#: the interior optimum found when the bearing test was swept on the endpoint
#: family; re-using it keeps the two studies comparable.
WINDOW_MIN = 7

#: Alignment gates, degrees. The endpoint study found accuracy rising
#: monotonically to its 0.1 floor, so the same range is offered here.
GATES_DEG = (0.10, 0.25, 1.0, 3.0, 10.0)

#: Effective-distance band, in NM, within which the tie-break variant is
#: allowed to prefer alignment over distance.
TIE_BANDS_NM = (0.5, 1.0, 2.0, 5.0)


def endpoint_courses(spark, ends: DataFrame) -> DataFrame:
    """Direction of travel near each track's first/last fix.

    The course is the bearing from the far edge of the window to the endpoint,
    which is the direction of travel for an arrival and the reverse course for
    a departure -- the window sits on the far side of the fix in both cases, so
    both read zero against a correctly identified aerodrome.
    """
    tr = (spark.read.parquet(TRACKS)
          .select("track_id", "event_time", "lat", "lon")
          .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
          .join(F.broadcast(ends), "track_id", "inner")
          .withColumn("_off", F.abs(F.unix_timestamp("t_end")
                                    - F.unix_timestamp("event_time")))
          .filter(F.col("_off") <= WINDOW_MIN * 60))
    g = (tr.groupBy("track_id", "status").agg(
            F.count(F.lit(1)).alias("n"),
            F.min("_off").alias("o0"), F.max("_off").alias("o1"),
            F.min_by("lat", "_off").alias("y_near"),
            F.min_by("lon", "_off").alias("x_near"),
            F.max_by("lat", "_off").alias("y_far"),
            F.max_by("lon", "_off").alias("x_far"))
         # Too few samples, or too short a baseline, and the bearing is noise
         # rather than a course.
         .filter("n >= 5")
         .filter(F.col("o1") - F.col("o0") >= 60))
    return (g.withColumn("course", bearing_deg(F.col("y_far"), F.col("x_far"),
                                               F.col("y_near"), F.col("x_near")))
            .select("track_id", "status", "y_near", "x_near", "course"))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--months", nargs="+", default=["202506"])
    ap.add_argument("--days", nargs="+",
                    default=["2025-06-05", "2025-06-06", "2025-06-07"])
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--executors", type=int, default=10)
    ap.add_argument("--ui-port", type=int, default=4052)
    args = ap.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
    load_dotenv()
    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(6, "8g", distributed=True)
    spark.sparkContext.setLogLevel("ERROR")
    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)

    # --- trend's surviving candidates at the shipped setting ---------------
    votes = spark.read.parquet(CACHE)
    up, dn = F.col(f"up_{FL_CAP}"), F.col(f"dn_{FL_CAP}")
    dist = F.col(f"dist_{FL_CAP}")
    v = (votes.filter(dist.isNotNull() & (dist <= RADIUS_NM))
         .withColumn("status", F.when(up > dn + MARGIN, F.lit("adep"))
                     .when(dn > up + MARGIN, F.lit("ades")))
         .filter(F.col("status").isNotNull())
         .withColumn("dist_nm", dist))
    pen = F.when(F.col("apt_scheduled") == "yes", F.lit(0.0)).otherwise(
        F.lit(PENALTY_NM))
    v = v.withColumn("_eff", F.col("dist_nm") + pen).cache()
    print(f"surviving (track, aerodrome, role) rows: {v.count():,}")

    # A track's endpoint time per role: earliest sample for a departure,
    # latest for an arrival. Taken from the vote cache's own bounds, so no
    # second pass over the tracks is needed to find them.
    ends = (v.groupBy("track_id", "status")
            .agg(F.when(F.first("status") == "adep", F.min("t_first"))
                 .otherwise(F.max("t_last")).alias("t_end")))
    crs = endpoint_courses(spark, ends).cache()
    print(f"courses computed for {crs.count():,} (track, role) pairs")

    apt_pos = (spark.read.parquet(AIRPORTS)
               .select(F.col("ident").alias("apt_ident"),
                       F.col("latitude_deg").cast("double").alias("apt_lat"),
                       F.col("longitude_deg").cast("double").alias("apt_lon"))
               .dropDuplicates(["apt_ident"]))

    c = (v.join(F.broadcast(apt_pos), "apt_ident", "left")
         .join(crs, ["track_id", "status"], "left")
         .withColumn("align_deg",
                     angle_between(F.col("course"),
                                   bearing_deg(F.col("y_near"), F.col("x_near"),
                                               F.col("apt_lat"), F.col("apt_lon"))))
         .cache())
    print(f"candidates with alignment: {c.count():,}")

    gt = label_ground_truth(load_ground_truth(spark, args.months, args.days),
                            airport_locations(spark)).cache()
    print(f"ground-truth flights: {gt.count():,}")

    # Every track that survived the vote rule, not only the ones with a
    # departure. `trend` classifies each (track, aerodrome) pair independently,
    # so a track can carry an arrival and no departure at all -- filtering to
    # `status == "adep"` would drop those tracks from the identity table, and a
    # track absent from it never matches ground truth, so its arrival would be
    # scored as a silence. That would understate exactly the coverage this
    # benchmark exists to measure.
    ident = (c.groupBy("track_id").agg(
                 F.min("icao24").alias("icao24"),
                 # Trimmed. ADS-B callsigns are space-padded to eight
                 # characters and the ground-truth callsign is not, so an
                 # untrimmed join matches nothing at all -- which shows up as
                 # every variant scoring exactly 0.00%, not as an error.
                 F.trim(F.min("flight_id")).alias("callsign"),
                 F.min("t_first").alias("t_start"))
             .withColumn("icao24", F.lower("icao24"))
             .withColumn("day", F.to_date("t_start")))

    def emit(best, accept=None):
        apt = F.col("apt_ident") if accept is None else \
            F.when(accept, F.col("apt_ident"))
        return (best.withColumn("apt", apt)
                .groupBy("track_id").pivot("status", ["adep", "ades"])
                .agg(F.first("apt")))

    def pick(order_cols):
        w = Window.partitionBy("track_id", "status").orderBy(*order_cols)
        return c.withColumn("_r", F.row_number().over(w)).filter(F.col("_r") == 1)

    rows = []

    def run(label, pred):
        m = score(pred, ident, gt)
        if m["adep_coverage"] == 0 and m["ades_coverage"] == 0:
            raise SystemExit(
                f"{label!r} scored zero coverage on both roles. That is not a "
                f"result -- with {gt.count():,} ground-truth flights and a "
                f"non-empty candidate set it means the identity join matched "
                f"nothing. Check icao24 case and callsign padding.")
        m["model"] = label
        rows.append(m)
        print(f"  {label:<38} ADEP {m['adep_coverage']:6.2%}/{m['adep_accuracy']:6.2%}"
              f"   ADES {m['ades_coverage']:6.2%}/{m['ades_accuracy']:6.2%}")

    by_dist = pick([F.col("_eff").asc_nulls_last()])
    print("\nscoring:")
    run("base: trend as shipped", emit(by_dist))

    # rerank -- alignment first, distance only to break its ties.
    run("rerank by alignment",
        emit(pick([F.col("align_deg").asc_nulls_last(), F.col("_eff").asc_nulls_last()])))

    # tie-break -- distance decides unless two candidates are within a band,
    # in which case alignment settles it. The conservative variant.
    for band in TIE_BANDS_NM:
        w_min = Window.partitionBy("track_id", "status")
        banded = (c.withColumn("_best", F.min("_eff").over(w_min))
                  .withColumn("_close", F.col("_eff") <= F.col("_best") + band))
        w = Window.partitionBy("track_id", "status").orderBy(
            F.col("_close").desc(),                      # inside the band first
            F.when(F.col("_close"), F.col("align_deg")).asc_nulls_last(),
            F.col("_eff").asc_nulls_last())
        run(f"tie-break by alignment within {band:g} NM",
            emit(banded.withColumn("_r", F.row_number().over(w))
                 .filter(F.col("_r") == 1)))

    # veto -- keep the distance choice, abstain when badly misaligned.
    for g_ in GATES_DEG:
        run(f"veto: abstain when align > {g_:g} deg",
            emit(by_dist, accept=F.coalesce(F.col("align_deg") <= g_, F.lit(False))))

    spark.createDataFrame(rows).toPandas().to_csv(
        out / "trend_bearing.csv", index=False)
    print(f"\nwritten to {out}")
    spark.stop()


if __name__ == "__main__":
    main()
