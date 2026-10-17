#!/usr/bin/env python
"""What the ADS-C merge did to the trajectories, arm against arm.

Three questions, in the order they have to be answered:

1. **Did anything arrive?** Rows, tracks and track-hours per arm, and how many
   of each carry an ADS-C sample. If this is zero the study is over and the
   milestone scores are two measurements of the same thing.
2. **Did it arrive where coverage was missing?** An ADS-C sample whose nearest
   ADS-B neighbour on *both* sides is further than ``hole_seconds`` away is
   closing a reception hole; one sitting inside a dense stretch is adding a
   point to a trajectory that already had one. Only the first is coverage.
3. **Did it do harm?** A track holding two distinct real callsigns is two
   flights fused into one. This is the guard the A9 debounce study used, and it
   is the plausible way a gap-bridging sample hurts: an ADS-C report during a
   turnaround can hold a gap below the 30-minute split threshold and join an
   arrival to the next departure.

Arms cannot be joined on ``track_id``: the merge changes it, which is the point
of merging before segmentation. ``icao24`` and the calendar day is the key that
survives, so the extension figures are per airframe-day rather than per track.

    python benchmarks/adsc_coverage_gain.py --days 2026-06-05 2026-06-06 \
        2026-06-07 --results-dir /tmp/gain
"""

import argparse
import csv
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

ARM_PREFIX = "s3a://eurocontrol/opdi/research/adsc"

#: A reception hole, in seconds. 300 is ``CleaningConfig.segment_gap_seconds``
#: -- the same threshold ``add_segment_id`` uses to decide a detector must not
#: interpolate across a gap. Using one number for both means "a hole ADS-C
#: closed" and "a hole that marks a discontinuity" are the same hole.
HOLE_SECONDS = 300


def _source_col(df: DataFrame):
    """``source``, or the constant ADS-B where the column does not exist.

    The baseline arm writes no ``source`` column -- deliberately, see
    ``TrackProcessor._merge_adsc``. Both arms have to go through the same
    summary or the comparison is between two different measurements.
    """
    if "source" in df.columns:
        return F.col("source")
    return F.lit("ADS-B")


def arm_summary(tracks: DataFrame) -> DataFrame:
    """One row describing one arm."""
    src = _source_col(tracks)
    is_adsc = (src == F.lit("ADS-C"))
    df = tracks.withColumn("_adsc", is_adsc.cast("long"))

    per_track = df.groupBy("track_id").agg(
        F.count(F.lit(1)).alias("_n"),
        F.max("_adsc").alias("_has_adsc"),
        F.min("event_time").alias("_first"),
        F.max("event_time").alias("_last"),
        F.countDistinct(
            F.when(F.trim(F.coalesce(F.col("callsign"), F.lit(""))) != "",
                   F.trim(F.col("callsign")))
        ).alias("_callsigns"),
    )

    # Two aggregations, crossJoined: `rows`, `rows_adsc` and `segments` are
    # row-level facts and the rest are track-level, so computing both from one
    # grouping would either double-count the rows or lose the track medians.
    totals = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum("_adsc").alias("rows_adsc"),
        F.countDistinct("segment_id").alias("segments"),
    )
    per_track_totals = per_track.agg(
        F.count(F.lit(1)).alias("tracks"),
        F.sum("_has_adsc").alias("tracks_with_adsc"),
        F.expr("percentile_approx(_n, 0.5)").alias("median_samples_per_track"),
        (F.sum(F.unix_timestamp("_last") - F.unix_timestamp("_first"))
         / F.lit(3600.0)).alias("track_hours"),
        F.sum((F.col("_callsigns") >= 2).cast("long"))
        .alias("tracks_with_two_callsigns"),
    )
    return totals.crossJoin(per_track_totals)


def bridged_holes(tracks: DataFrame, hole_seconds: int = HOLE_SECONDS) -> DataFrame:
    """ADS-C samples that closed a reception hole.

    The neighbour distance is measured to the nearest **ADS-B** sample of the
    same track on each side, not to the nearest sample of any source: two
    consecutive ADS-C reports ten minutes apart are both inside the same hole,
    and measuring them against each other would report the hole as closed
    twice and six hundred seconds narrower than it is.
    """
    src = _source_col(tracks)
    t = (tracks
         .withColumn("_t", F.unix_timestamp("event_time").cast("double"))
         .withColumn("_src", src))
    adsb_t = F.when(F.col("_src") == F.lit("ADS-B"), F.col("_t"))

    before = Window.partitionBy("track_id").orderBy("_t").rowsBetween(
        Window.unboundedPreceding, -1)
    after = Window.partitionBy("track_id").orderBy("_t").rowsBetween(
        1, Window.unboundedFollowing)

    out = (t
           .withColumn("_prev_adsb", F.max(adsb_t).over(before))
           .withColumn("_next_adsb", F.min(adsb_t).over(after))
           .where(F.col("_src") == F.lit("ADS-C"))
           .withColumn("gap_before_s", F.col("_t") - F.col("_prev_adsb"))
           .withColumn("gap_after_s", F.col("_next_adsb") - F.col("_t")))

    # Both sides, not either: a sample `hole_seconds` after the last ADS-B
    # report and five seconds before the next one is inside a dense stretch
    # seen from one direction only.
    #
    # A missing neighbour -- the sample extends the track past its last ADS-B
    # report -- counts as an infinite gap. That is the clearest case of added
    # coverage, and treating NULL as "not a hole" would discard exactly it.
    inf = F.lit(1.0e9)
    return (out
            .withColumn("gap_before_s", F.coalesce(F.col("gap_before_s"), inf))
            .withColumn("gap_after_s", F.coalesce(F.col("gap_after_s"), inf))
            .where((F.col("gap_before_s") >= F.lit(float(hole_seconds)))
                   & (F.col("gap_after_s") >= F.lit(float(hole_seconds))))
            .select("track_id", "icao24", "event_time",
                    "gap_before_s", "gap_after_s"))


def extension_by_airframe(base: DataFrame, merged: DataFrame) -> DataFrame:
    """How much earlier and later each airframe-day is seen in *merged*.

    Positive ``last_shift_s`` means merged sees the airframe longer; negative
    ``first_shift_s`` means it sees it earlier. Both are reported signed, so a
    regression -- the merge somehow losing an end -- is visible rather than
    clipped to zero.
    """
    def ends(df, tag):
        return (df
                .withColumn("day", F.to_date("event_time"))
                .groupBy("icao24", "day")
                .agg(F.min("event_time").alias(f"first_{tag}"),
                     F.max("event_time").alias(f"last_{tag}")))

    b, m = ends(base, "base"), ends(merged, "merged")
    return (b.join(m, ["icao24", "day"], "inner")
            .select(
                "icao24", "day",
                (F.unix_timestamp("first_merged")
                 - F.unix_timestamp("first_base")).cast("double")
                .alias("first_shift_s"),
                (F.unix_timestamp("last_merged")
                 - F.unix_timestamp("last_base")).cast("double")
                .alias("last_shift_s"),
            ))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", nargs="+", required=True)
    ap.add_argument("--prefix", default=ARM_PREFIX)
    ap.add_argument("--hole-seconds", type=int, default=HOLE_SECONDS)
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--gain-name", default="adsc_gain_2026.csv")
    ap.add_argument("--holes-name", default="adsc_holes_2026.csv")
    ap.add_argument("--cores", type=int, default=4)
    ap.add_argument("--driver-memory", default="8g")
    ap.add_argument("--executors", type=int, default=6)
    args = ap.parse_args()

    import osn_sample
    from osn_sample import build_spark, load_dotenv

    load_dotenv()
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(args.cores, args.driver_memory, distributed=True)
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    gain_rows, hole_rows = [], []
    try:
        days = F.array(*[F.lit(d) for d in args.days])
        arms = {}
        for arm in ("base", "merged"):
            df = (spark.read.parquet(f"{args.prefix}/{arm}/osn_tracks_clean")
                  .where(F.array_contains(days, F.col("dof").cast("string"))))
            arms[arm] = df.cache()
            r = arm_summary(arms[arm]).collect()[0].asDict()
            r["arm"] = arm
            r["days"] = " ".join(args.days)
            gain_rows.append(r)
            print(f"  {arm}: {r['rows']:,} rows, {r['tracks']:,} tracks, "
                  f"{r['rows_adsc']:,} ADS-C", flush=True)

        hole_rows = [r.asDict()
                     for r in bridged_holes(arms["merged"],
                                            args.hole_seconds).collect()]
        print(f"  bridged holes: {len(hole_rows):,}", flush=True)

        ext = extension_by_airframe(arms["base"], arms["merged"])
        shifts = ext.agg(
            F.count(F.lit(1)).alias("airframe_days"),
            F.expr("percentile_approx(first_shift_s, 0.5)")
            .alias("first_shift_median_s"),
            F.expr("percentile_approx(last_shift_s, 0.5)")
            .alias("last_shift_median_s"),
            F.sum((F.col("first_shift_s") < 0).cast("long")).alias("seen_earlier"),
            F.sum((F.col("last_shift_s") > 0).cast("long")).alias("seen_later"),
        ).collect()[0].asDict()
        gain_rows.append({"arm": "extension", "days": " ".join(args.days),
                          **shifts})
    finally:
        spark.stop()

    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)

    def write(name, rows):
        if not rows:
            print(f"  {name}: no rows -- writing a header-only file, which is "
                  f"a result and not a missing output")
        fields = sorted({k for r in rows for k in r}) or ["arm"]
        with open(out / name, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)
        print(f"  wrote {out / name} ({len(rows)} rows)")

    write(args.gain_name, gain_rows)
    write(args.holes_name, hole_rows)

    import provenance
    for name in (args.gain_name, args.holes_name):
        provenance.record(
            out, name, "benchmarks/adsc_coverage_gain.py", sys.argv[1:],
            ["benchmarks/adsc_coverage_gain.py",
             "src/opdi/ingestion/osn_adsc.py",
             "src/opdi/pipeline/tracks.py",
             "src/opdi/cleaning/native.py"],
            inputs={"days": args.days, "hole_seconds": args.hole_seconds},
            input_tables=[f"{args.prefix}/base/osn_tracks_clean",
                          f"{args.prefix}/merged/osn_tracks_clean"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
