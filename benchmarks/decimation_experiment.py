"""
Compare the two 5 s decimation rules on raw OSN state vectors.

The ingest thins with ``event_time % 5 == 0`` -- a *fixed-phase* sampler. Where
reception is dense that keeps exactly one sample per 5 s window and is correct.
Where it is sparse it keeps a sample only if one exists at the one second per
window congruent to 0, so it can delete an aircraft entirely.

The alternative bins time into ``floor(t/5)`` and keeps one row per non-empty
bin.

There is a complication the row counts alone cannot see. The OSN
``state_vectors`` table is already resampled to 1 Hz, and it **carries values
forward**: a row can exist for every second even when no new position was
received, with ``lastPosUpdate`` recording when the position was actually
measured. So a rule can retain a row and still retain no information. This
script therefore measures two different things:

1. **Row retention** -- does either rule keep more rows, banded by altitude and
   by measured row density.
2. **Information retention** -- for each 5 s bin, how stale is the row the
   modulo rule keeps, against the freshest row available in that same bin.
   ``age = time - lastPosUpdate``. If the archive is dense but stale, (1) shows
   nothing and (2) shows the real cost.

Plus wall-clock for each rule, so the extra shuffle is measured not guessed.

    python benchmarks/decimation_experiment.py --hours 3 --results-dir <dir>
"""

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import osn_sample
from osn_sample import build_spark, load_dotenv

ARCHIVE = "s3a://opensky-hdfs-backup/tables_v4/state_vectors"

#: The OPDI ingestion bounding box -- min_lon, min_lat, max_lon, max_lat.
BBOX = (-25.86653, 26.74617, 49.65699, 70.25976)

INTERVAL = 5

#: Archive column -> OPDI name. The archive is camelCase.
RENAME = {"time": "event_time", "baroAltitude": "baro_altitude",
          "onGround": "on_ground", "vertRate": "vert_rate",
          "lastPosUpdate": "last_pos_update"}

KEEP = ["icao24", "callsign", "event_time", "lat", "lon", "baro_altitude",
        "velocity", "vert_rate", "on_ground", "last_pos_update"]

ALT_BANDS = [(0, 2000), (2000, 5000), (5000, 10000), (10000, 20000), (20000, 30000)]


def read_hours(spark: SparkSession, start: datetime, hours: int) -> DataFrame:
    paths = [f"{ARCHIVE}/hour={int(start.timestamp()) + 3600 * i}" for i in range(hours)]
    print(f"reading {hours} hourly partitions from {start:%Y-%m-%d %H:%M} UTC")
    df = spark.read.option("mergeSchema", "true").parquet(*paths)
    for src, dst in RENAME.items():
        if src in df.columns:
            df = df.withColumnRenamed(src, dst)
    keep = [c for c in KEEP if c in df.columns]
    missing = [c for c in KEEP if c not in df.columns]
    if missing:
        print(f"  absent from the archive schema: {missing}")
    min_lon, min_lat, max_lon, max_lat = BBOX
    return df.select(*keep).filter(
        (F.col("lon") >= min_lon) & (F.col("lon") <= max_lon)
        & (F.col("lat") >= min_lat) & (F.col("lat") <= max_lat)
        & F.col("event_time").isNotNull() & F.col("icao24").isNotNull()
    )


def modulo_rule(df: DataFrame) -> DataFrame:
    """What the ingest does today: a narrow filter, no shuffle."""
    return df.filter((F.col("event_time") % INTERVAL) == 0)


def bucket_rule(df: DataFrame) -> DataFrame:
    """One row per (aircraft, 5 s bin), keeping the last sample in the bin.

    ``max`` over a struct compares field by field, so event_time first selects
    the latest row in the bin. A hash aggregate with map-side partial
    aggregation, not a sort-based window, and native Spark throughout.
    """
    rest = [c for c in df.columns if c != "event_time"]
    binned = df.withColumn("_bin", F.col("event_time") - (F.col("event_time") % INTERVAL))
    return (binned.groupBy("icao24", "_bin")
            .agg(F.max(F.struct("event_time", *rest)).alias("_s"))
            .select("_s.*"))


def alt_band(col_ft):
    band = F.lit("6. 30,000+")
    for i, (lo, hi) in enumerate(reversed(ALT_BANDS)):
        n = len(ALT_BANDS) - i
        band = F.when(col_ft < hi, F.lit(f"{n}. {lo:,}-{hi:,}")).otherwise(band)
    return F.when(col_ft.isNull(), F.lit("0. (null)")).otherwise(band)


def timed(label, fn):
    t0 = time.perf_counter()
    v = fn()
    dt = time.perf_counter() - t0
    print(f"  {label:<26} {v:>14,}  {dt:8.1f} s")
    return v, dt


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default="2025-06-05")
    ap.add_argument("--start-hour", type=int, default=6)
    ap.add_argument("--hours", type=int, default=3)
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--executors", type=int, default=6)
    ap.add_argument("--ui-port", type=int, default=4041)
    ap.add_argument("--local", action="store_true")
    ap.add_argument("--cores", type=int, default=6)
    ap.add_argument("--driver-memory", default="8g")
    args = ap.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
    load_dotenv()
    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(args.cores, args.driver_memory, distributed=not args.local)
    spark.sparkContext.setLogLevel("ERROR")

    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)
    d = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    base = read_hours(spark, d.replace(hour=args.start_hour), args.hours)

    # -- 1. timing -----------------------------------------------------------
    # Both rules read the same S3 objects, so the read is common and the
    # difference is the cost of the shuffle. Deliberately not cached: the
    # production job reads from S3 too.
    print("\ntiming, cold read from S3 for each rule:")
    n_base, t_base = timed("bbox only (baseline)", lambda: base.count())
    n_mod, t_mod = timed("modulo  (current)", lambda: modulo_rule(base).count())
    n_buc, t_buc = timed("bucket  (proposed)", lambda: bucket_rule(base).count())
    spark.createDataFrame([
        {"rule": "bbox_only", "rows": n_base, "seconds": t_base},
        {"rule": "modulo", "rows": n_mod, "seconds": t_mod},
        {"rule": "bucket", "rows": n_buc, "seconds": t_buc},
    ]).toPandas().to_csv(out / "timing.csv", index=False)
    print(f"\n  rows   bucket / modulo = {n_buc / max(n_mod,1):.3f}x")
    print(f"  wall   bucket vs modulo = {(t_buc - t_mod)/max(t_mod,1e-9):+.0%}")
    print(f"  net of the common read: modulo {t_mod-t_base:+.1f} s, bucket {t_buc-t_base:+.1f} s")

    ft = F.col("baro_altitude") * 3.28084
    band = alt_band(ft)

    # -- 2. row retention by altitude ---------------------------------------
    a = modulo_rule(base).withColumn("b", band).groupBy("b").count() \
        .withColumnRenamed("count", "modulo_rows")
    c = bucket_rule(base).withColumn("b", band).groupBy("b").count() \
        .withColumnRenamed("count", "bucket_rows")
    by_alt = (a.join(c, "b", "outer")
              .withColumn("ratio", F.col("bucket_rows") / F.col("modulo_rows"))
              .orderBy("b"))
    print("\nrow retention by altitude band (ft):")
    by_alt.show(20, truncate=False)
    by_alt.toPandas().to_csv(out / "by_altitude.csv", index=False)

    # -- 3. row density: is the archive actually 1 Hz? ----------------------
    minute = F.col("event_time") - (F.col("event_time") % 60)
    dens = (base.groupBy("icao24", minute.alias("_min"))
            .agg(F.countDistinct("event_time").alias("n_sec"))
            .withColumn("p", F.col("n_sec") / F.lit(60.0)))
    pband = (F.when(F.col("p") >= 0.9, F.lit("6. 0.9-1.0 (dense)"))
             .when(F.col("p") >= 0.7, F.lit("5. 0.7-0.9"))
             .when(F.col("p") >= 0.5, F.lit("4. 0.5-0.7"))
             .when(F.col("p") >= 0.3, F.lit("3. 0.3-0.5"))
             .when(F.col("p") >= 0.1, F.lit("2. 0.1-0.3"))
             .otherwise(F.lit("1. <0.1 (sparse)")))
    by_p = (dens.withColumn("pb", pband).groupBy("pb")
            .agg(F.count(F.lit(1)).alias("aircraft_minutes"),
                 F.sum("n_sec").alias("source_rows"),
                 F.avg("p").alias("mean_p"))
            .orderBy("pb"))
    print("\nrow density per aircraft-minute (is the archive really 1 Hz?):")
    by_p.show(20, truncate=False)
    by_p.toPandas().to_csv(out / "by_density.csv", index=False)

    # -- 4. information retention: staleness ---------------------------------
    # age = how many seconds old the position in this row is. A row carried
    # forward by the archive has a large age and contains nothing new.
    if "last_pos_update" in base.columns:
        aged = base.withColumn(
            "age", F.col("event_time") - F.col("last_pos_update").cast("double"))
        binned = aged.withColumn(
            "_bin", F.col("event_time") - (F.col("event_time") % INTERVAL))
        per_bin = (binned.groupBy("icao24", "_bin")
                   .agg(F.min("age").alias("age_best"),
                        F.min(F.when((F.col("event_time") % INTERVAL) == 0,
                                     F.col("age"))).alias("age_modulo"),
                        F.max(F.when((F.col("event_time") % INTERVAL) == 0,
                                     F.lit(1)).otherwise(0)).alias("has_phase0"),
                        F.first(ft).alias("alt_ft"))
                   .withColumn("penalty",
                               F.col("age_modulo") - F.col("age_best")))
        per_bin.cache()
        n_bins = per_bin.count()
        by_stale = (per_bin.withColumn("b", alt_band(F.col("alt_ft")))
                    .groupBy("b")
                    .agg(F.count(F.lit(1)).alias("bins"),
                         F.avg("age_best").alias("mean_age_best_s"),
                         F.avg("age_modulo").alias("mean_age_modulo_s"),
                         F.avg("penalty").alias("mean_penalty_s"),
                         F.avg(F.when(F.col("has_phase0") == 0, 1.0).otherwise(0.0))
                         .alias("share_bins_no_phase0"),
                         F.avg(F.when(F.col("penalty") > 5, 1.0).otherwise(0.0))
                         .alias("share_penalty_gt_5s"))
                    .orderBy("b"))
        print(f"\ninformation retention, {n_bins:,} aircraft-bins:")
        print("  age_best   = freshest position available in the 5 s bin")
        print("  age_modulo = age of the row the modulo rule actually keeps")
        print("  penalty    = extra staleness the fixed phase costs")
        by_stale.show(20, truncate=False)
        by_stale.toPandas().to_csv(out / "staleness.csv", index=False)
    else:
        print("\nlastPosUpdate absent -- staleness analysis skipped")

    # -- 5. endpoint lag -----------------------------------------------------
    ends = (base.groupBy("icao24")
            .agg(F.max("event_time").alias("t_true"),
                 F.max(F.when((F.col("event_time") % INTERVAL) == 0,
                              F.col("event_time"))).alias("t_mod"),
                 F.count(F.lit(1)).alias("n_rows"))
            .withColumn("lag_modulo_s", F.col("t_true") - F.col("t_mod")))
    ends.cache()
    n_ac = ends.count()
    lost = ends.filter(F.col("t_mod").isNull()).count()
    seen = ends.filter(F.col("t_mod").isNotNull())
    q = seen.approxQuantile("lag_modulo_s", [0.5, 0.9, 0.99], 0.01)
    mean_lag = seen.agg(F.avg("lag_modulo_s")).first()[0] or 0.0
    print(f"\nendpoint lag, {n_ac:,} aircraft:")
    print(f"  erased entirely by modulo: {lost:,} ({lost/max(n_ac,1):.2%})")
    print(f"  lag of modulo's last row: mean {mean_lag:.1f} s, "
          f"median {q[0]:.0f} s, p90 {q[1]:.0f} s, p99 {q[2]:.0f} s")
    print(f"  lag of bucket's last row: 0 s by construction")
    spark.createDataFrame([{
        "aircraft": n_ac, "erased_by_modulo": lost,
        "erased_share": lost / max(n_ac, 1), "lag_mean_s": float(mean_lag),
        "lag_p50_s": q[0], "lag_p90_s": q[1], "lag_p99_s": q[2],
    }]).toPandas().to_csv(out / "endpoint_lag.csv", index=False)

    print(f"\nwritten to {out}")
    spark.stop()


if __name__ == "__main__":
    main()
