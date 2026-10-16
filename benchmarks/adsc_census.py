#!/usr/bin/env python
"""What the ADS-C feed actually contains, per sampled day.

The ADS-C fusion study's framing rests on three measured facts -- the feed is
sparse (~600 s per airframe), high (97.6% of in-box rows above FL300) and
oceanic (74.5% of in-box rows west of 10W) -- and on a fourth, negative one:
the interesting columns (``ias``, ``tas``, ``mach``, ``oat``, ``wd``/``ws``,
``nav_altitude_mcp``, ``registration``, ``ac_type``, ``alt_geom``) are empty.
All four were first measured on 2026-06-01 alone. This job re-measures them
across the archive, because "empty on one day in June 2026" and "never
populated since 2023" are different claims and only the second one justifies
not building a passthrough column for them.

Reads the raw topic directly -- no pipeline table, no reference data, no ground
truth -- so it needs S3 credentials and nothing else. No pipeline code can
change its answer, which is why its job in ``regenerate_adsc_v1.py`` declares
no dependencies beyond this file.

    python benchmarks/adsc_census.py --days 2026-06-01 2026-03-01 \
        --out-name adsc_census.csv --results-dir /tmp/census --local
"""

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from opdi.coverage import EUROPE_BBOX

#: The raw topic. Hourly ``hour=<unix seconds>`` partitions holding snappy
#: **parquet**, despite the ``.avro`` in the topic name -- the files carry the
#: Avro schema ``org.opensky.avro.readsb.Aircraft`` in their footer.
ADSC_BASE = "s3a://readsb-aircraft-states/topics/readsb.aircraft.adsc.avro"

#: Columns whose fill rate decides whether a passthrough struct is worth
#: building. Listed explicitly rather than taken from the frame, so a column
#: that disappears from the feed reads as 0% rather than vanishing from the
#: report: an absent row and a zero mean different things and only one of them
#: is a measurement.
TRACKED = (
    "time", "hex", "lat", "lon", "alt_baro_ft", "alt_geom", "track", "gs",
    "baro_rate", "flight", "squawk", "ias", "tas", "mach", "mag_heading",
    "true_heading", "roll", "track_rate", "geom_rate", "oat", "tat", "wd",
    "ws", "nav_qnh", "nav_altitude_mcp", "nav_altitude_fms", "nav_heading",
    "registration", "ac_type", "ac_desc", "category", "on_ground",
)


def census(df: DataFrame, bbox) -> DataFrame:
    """One row describing *df*. See the module docstring for why each column.

    ``median_cadence_s`` is the median over **per-airframe** medians, not over
    all gaps: one airframe with 467 samples would otherwise set the headline
    number for a day with 1,855 airframes.
    """
    from pyspark.sql import Window

    min_lon, min_lat, max_lon, max_lat = bbox
    in_box = (
        (F.col("lon") >= min_lon) & (F.col("lon") <= max_lon)
        & (F.col("lat") >= min_lat) & (F.col("lat") <= max_lat)
    )

    w = Window.partitionBy("hex").orderBy("time")
    # Zero gaps are excluded, not averaged in: a 0 s step is an at-least-once
    # redelivery of one report, counted separately as `exact_duplicate_rows`.
    # `percentile_approx` is nearest-rank rather than interpolating, so over
    # {0, 600} it returns 0 -- leaving the duplicates in would report a
    # ten-minute feed as a 1 Hz one.
    gaps = (
        df.withColumn("_gap", F.col("time") - F.lag("time").over(w))
        .where(F.col("_gap").isNotNull() & (F.col("_gap") > 0))
        .groupBy("hex")
        .agg(F.expr("percentile_approx(_gap, 0.5)").alias("_median"))
    )
    cadence = gaps.agg(
        F.expr("percentile_approx(_median, 0.5)").alias("median_cadence_s")
    )

    # An exact duplicate is the same airframe, instant and position twice --
    # Kafka at-least-once delivery, not a second observation.
    dupes = df.groupBy("hex", "time", "lat", "lon").count()
    dupe_rows = dupes.agg(
        F.coalesce(F.sum(F.col("count") - 1), F.lit(0))
        .cast("long").alias("exact_duplicate_rows")
    )

    fills = [
        (
            (F.sum(F.col(c).isNotNull().cast("double")) * 100.0 / F.count("*"))
            if c in df.columns
            else F.lit(0.0)
        ).alias(f"fill_{c}")
        for c in TRACKED
    ]

    # The altitude and geography bands are counted **inside the box**, not over
    # the whole feed. That is the population OPDI would ingest, so it is the
    # one a claim about what ADS-C can reach has to be made over. Counting them
    # globally gives a similar-looking percentage for a different population --
    # and two tables quoting "97.6% above FL300" about different denominators
    # is worse than one table quoting nothing.
    base = df.agg(
        F.count("*").alias("rows"),
        F.sum(in_box.cast("long")).alias("rows_in_box"),
        F.countDistinct("hex").alias("airframes"),
        F.countDistinct(F.when(in_box, F.col("hex"))).alias("airframes_in_box"),
        F.sum((in_box & (F.col("alt_baro_ft") > 30000)).cast("long"))
        .alias("in_box_above_fl300"),
        F.sum((in_box & (F.col("alt_baro_ft") < 10000)).cast("long"))
        .alias("in_box_below_10000ft"),
        F.sum((in_box & (F.col("lon") < -10.0)).cast("long"))
        .alias("in_box_west_of_10w"),
        F.sum((in_box & F.col("on_ground")).cast("long"))
        .alias("in_box_on_ground")
        if "on_ground" in df.columns else F.lit(0).cast("long").alias("in_box_on_ground"),
        *fills,
    )
    return base.crossJoin(cadence).crossJoin(dupe_rows)


def hour_paths(base: str, day) -> list:
    """The 24 hourly partition paths for *day*, in order.

    Every object in the topic sits under ``hour=<unix seconds>``, one directory
    per UTC hour. Paths are handed to Spark as explicit leaf directories, not
    as a parent root plus a filter: partition discovery only looks *below* the
    path it is given, so a leaf path yields no ``hour`` partition column --
    which matters because the Avro schema already carries a data column of that
    name and the two would collide with "Found duplicate column(s)".
    """
    import datetime as dt

    start = dt.datetime(day.year, day.month, day.day, tzinfo=dt.timezone.utc)
    return [f"{base}/hour={int((start + dt.timedelta(hours=h)).timestamp())}"
            for h in range(24)]


def existing(spark, paths: list) -> list:
    """The subset of *paths* that exists.

    The archive has 24 gaps longer than a day since 2023-07 and averages 22.3
    of 24 hourly partitions per day in 2026, so a missing hour is normal.
    ``spark.read.parquet`` raises PATH_NOT_FOUND on the first absent one, which
    would make a routine gap look like a credentials or bucket failure.
    """
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    out = []
    for p in paths:
        path = jvm.org.apache.hadoop.fs.Path(p)
        if path.getFileSystem(hconf).exists(path):
            out.append(p)
    return out


def read_day(spark, base: str, day):
    """``(frame, n_partitions)`` for one day, or ``(None, 0)``.

    The partition count comes back with the frame because it is provenance, not
    a log line: a day that read 18 of 24 hours is a different measurement from
    one that read all 24, and nothing in the census output would otherwise say
    which happened.
    """
    paths = existing(spark, hour_paths(base, day))
    if not paths:
        return None, 0
    print(f"  {day}: {len(paths)}/24 hourly partitions present", flush=True)
    return spark.read.option("mergeSchema", "true").parquet(*paths), len(paths)


def main() -> int:
    import csv
    import datetime as dt

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", nargs="+", required=True,
                    help="YYYY-MM-DD, one census row each")
    ap.add_argument("--base", default=ADSC_BASE)
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--out-name", default="adsc_census.csv")
    ap.add_argument("--cores", type=int, default=4)
    ap.add_argument("--driver-memory", default="6g")
    ap.add_argument(
        "--local", action="store_true",
        help="Local Spark instead of the OSN cluster. The default for this "
             "job: the whole topic is ~5 MB per day, so a distributed session "
             "costs minutes of executor startup to read what fits in a "
             "driver's heap several times over. Every other benchmark here "
             "reads pipeline tables and genuinely needs the cluster.")
    ap.add_argument("--distributed", dest="local", action="store_false",
                    help="Force the cluster session.")
    ap.set_defaults(local=True)
    args = ap.parse_args()

    from osn_sample import build_spark, load_dotenv

    load_dotenv()
    spark = build_spark(args.cores, args.driver_memory,
                        distributed=not args.local)
    rows, partitions = [], {}
    try:
        for d in args.days:
            day = dt.date.fromisoformat(d)
            df, n_parts = read_day(spark, args.base, day)
            if df is None:
                print(f"  {day}: no partitions at all; skipped")
                partitions[d] = 0
                continue
            r = census(df, EUROPE_BBOX).collect()[0].asDict()
            r["day"] = d
            r["hour_partitions"] = n_parts
            rows.append(r)
            partitions[d] = n_parts
    finally:
        spark.stop()

    if not rows:
        raise SystemExit("no day produced a census row -- nothing written")

    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)
    fields = ["day"] + [k for k in rows[0] if k != "day"]
    with open(out / args.out_name, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    # `input_tables` is deliberately empty, and the day/partition map carries
    # the data dependency instead.
    #
    # `provenance.s3_identity` paginates every object under the prefix it is
    # given. The ADS-C topic root holds 16,115 hourly partitions of ~250
    # objects each -- roughly four million keys, four thousand list requests,
    # and growing every hour. Declaring it would cost minutes per run to
    # produce a number that changes whenever *any* day of the archive changes,
    # which says nothing about the eleven days this job actually read.
    #
    # The hour-partition count per day is the dependency that matters: it is
    # what distinguishes "this day was measured whole" from "this day was
    # measured from eighteen of its twenty-four hours", and it is already
    # known from the listing the read had to do anyway.
    import provenance
    provenance.record(out, args.out_name, "benchmarks/adsc_census.py",
                      sys.argv[1:], ["benchmarks/adsc_census.py"],
                      inputs={"days": args.days, "base": args.base,
                              "hour_partitions_per_day": partitions},
                      input_tables=[])
    print(f"wrote {out / args.out_name} ({len(rows)} day(s))")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
