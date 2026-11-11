#!/usr/bin/env python
"""What the ADS-C-only tracks of the airframe subset actually are.

``adsc_coverage_gain.py`` counts tracks made only of ADS-C. A count cannot say
whether such a track is an airframe ADS-B never heard, or a stretch of a flight
ADS-B did hear, cut off from it by the segmentation's gap rule. The first is new
coverage; the second is a fragment.

Also writes the full feed's denominator, because the subset's totals describe a
selected population: every airframe in it has ADS-C.

    python benchmarks/adsc_subset_breakdown.py --day 2026-06-05 \
        --reference-day 2026-06-14 --results-dir benchmarks/out/adsc_worldwide_subset
"""

import argparse
import csv
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

from adsc_coverage_gain import arm_summary  # noqa: E402
from pyspark.sql import DataFrame, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

PREFIX = "s3a://eurocontrol/opdi-world/research/adsc_airframes"
FULL_TRACKS = "s3a://eurocontrol/opdi-world/osn_tracks_clean"


def classify_adsc_tracks(merged: DataFrame, adsb_airframes: DataFrame) -> DataFrame:
    """One row per ADS-C-only track: its airframe, span, and whether ADS-B knew it.

    ``adsb_airframes`` is one ``icao24`` column: every airframe with an ADS-B
    sample in the arm. Nearest ADS-B is deliberately not measured here: a
    fragment is defined by the airframe having ADS-B at all that day.
    """
    per_track = (merged.groupBy("track_id", "icao24")
                 .agg(F.count("*").alias("samples"),
                      F.sum((F.col("source") == "ADS-B").cast("long")).alias("n_adsb"),
                      F.min("event_time").alias("t0"),
                      F.max("event_time").alias("t1")))
    only = per_track.where(F.col("n_adsb") == 0)
    known = adsb_airframes.select("icao24").distinct().withColumn("_known", F.lit(True))
    return (only.join(known, "icao24", "left")
            .withColumn("airframe_heard_by_adsb", F.coalesce("_known", F.lit(False)))
            .withColumn("span_s", F.unix_timestamp("t1") - F.unix_timestamp("t0"))
            .drop("_known"))


def hole_kind(gap_before_col, gap_after_col, inf: float = 1.0e9):
    """Interior hole, an extension past one end, or no ADS-B on the track at all."""
    before_none = gap_before_col >= inf
    after_none = gap_after_col >= inf
    return (F.when(before_none & after_none, "no_adsb_on_track")
            .when(before_none | after_none, "extends_track")
            .otherwise("interior_hole"))


def adsc_placement(merged: DataFrame, hole_seconds: int = 300) -> DataFrame:
    """Each ADS-C sample's place relative to ADS-B on its own track."""
    inf = 1.0e9
    t = merged.withColumn("_t", F.unix_timestamp("event_time").cast("double"))
    adsb_t = F.when(F.col("source") == "ADS-B", F.col("_t"))
    w = Window.partitionBy("track_id").orderBy("_t")
    before = w.rowsBetween(Window.unboundedPreceding, -1)
    after = w.rowsBetween(1, Window.unboundedFollowing)
    out = (t.withColumn("_prev", F.max(adsb_t).over(before))
           .withColumn("_next", F.min(adsb_t).over(after))
           .where(F.col("source") == "ADS-C")
           .withColumn("gap_before_s",
                       F.coalesce(F.col("_t") - F.col("_prev"), F.lit(inf)))
           .withColumn("gap_after_s",
                       F.coalesce(F.col("_next") - F.col("_t"), F.lit(inf))))
    return (out
            .withColumn("kind", hole_kind(F.col("gap_before_s"), F.col("gap_after_s")))
            .withColumn("in_hole",
                        (F.col("gap_before_s") >= hole_seconds)
                        & (F.col("gap_after_s") >= hole_seconds)))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--day", required=True)
    ap.add_argument("--reference-day", required=True,
                    help="a day of the full worldwide table, for the denominator")
    ap.add_argument("--prefix", default=PREFIX)
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--executors", type=int, default=6)
    args = ap.parse_args()

    import osn_sample
    from osn_sample import build_spark, load_dotenv

    load_dotenv()
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(4, "8g", distributed=True)
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    rows = []

    def add(metric, value):
        rows.append({"metric": metric, "value": value})
        print(f"  {metric}: {value}", flush=True)

    try:
        merged = (spark.read.parquet(f"{args.prefix}/merged/osn_tracks_clean")
                  .where(F.col("dof").cast("string") == args.day).cache())
        window_adsb = (spark.read.parquet(f"{args.prefix}/input/osn_statevectors_v2")
                       .select("icao24").distinct())
        day_adsb = merged.where(F.col("source") == "ADS-B").select("icao24").distinct()

        only = classify_adsc_tracks(merged, day_adsb).cache()
        add("adsc_only_tracks", only.count())
        add("adsc_only_tracks_airframe_has_adsb_track_that_day",
            only.where("airframe_heard_by_adsb").count())
        add("adsc_only_tracks_airframe_has_no_adsb_track_that_day",
            only.where("not airframe_heard_by_adsb").count())
        add("adsc_only_tracks_single_sample", only.where("samples = 1").count())
        add("adsc_only_track_hours",
            (only.agg(F.sum("span_s")).first()[0] or 0) / 3600.0)
        add("adsc_only_airframes_no_adsb_in_three_day_window",
            only.select("icao24").distinct()
            .join(window_adsb, "icao24", "left_anti").count())

        placed = adsc_placement(merged).cache()
        for r in placed.groupBy("kind").agg(
                F.count("*").alias("n"),
                F.sum(F.col("in_hole").cast("long")).alias("in_hole")).collect():
            add(f"adsc_samples_{r['kind']}", r["n"])
            add(f"adsc_samples_{r['kind']}_beyond_hole_threshold", r["in_hole"])

        interior = placed.where("kind = 'interior_hole' and in_hole").withColumn(
            "smaller_side_s", F.least("gap_before_s", "gap_after_s"))
        q = interior.agg(F.expr("percentile_approx(smaller_side_s, 0.5)"),
                         F.expr("percentile_approx(smaller_side_s, 0.9)")).first()
        add("interior_hole_smaller_side_median_s", q[0])
        add("interior_hole_smaller_side_p90_s", q[1])

        full = (spark.read.parquet(FULL_TRACKS)
                .where(F.col("dof").cast("string") == args.reference_day))
        s = arm_summary(full).first().asDict()
        add("full_feed_reference_day", args.reference_day)
        for k in ("rows", "tracks", "track_hours"):
            add(f"full_feed_{k}", s[k])
        add("full_feed_airframes", full.select("icao24").distinct().count())
    finally:
        spark.stop()

    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)
    name = "adsc_subset_breakdown.csv"
    with open(out / name, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["metric", "value"])
        w.writeheader()
        w.writerows(rows)

    import provenance
    from regenerate_adsc_v1 import CORE
    provenance.record(
        out, name, "benchmarks/adsc_subset_breakdown.py", sys.argv[1:],
        ["benchmarks/adsc_subset_breakdown.py", "benchmarks/adsc_coverage_gain.py",
         *CORE],
        inputs={"day": args.day, "reference_day": args.reference_day},
        notes="Reads the one-day worldwide airframe subset built by "
              "run_adsc_day.py --worldwide --adsc-airframes-only.")
    print(f"  wrote {out / name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
