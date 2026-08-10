"""
Sweep the `trend` algorithm's parameters, which have never been tuned.

`trend` has four constants that were chosen once and never measured:

* ``MAX_FL = 40`` -- only state vectors below FL40 are considered. Version 4
  identified this as the binding constraint on its coverage, so it is the first
  thing worth moving.
* ``DETECTION_RADIUS_NM = 30`` -- the zone radius.
* the smoothing window, ``rowsBetween(-2, 2)``.
* the vote margin of 4, about 20 s of consistent movement at 5 s sampling.

It also picks the nearest surviving aerodrome with **no scheduled-service
penalty**, unlike the endpoint rule -- and version 3 found that preference
removed an entire class of departure error, so its absence here is worth
testing.

Sweeping directly would mean one pass over the tracks per cell. Instead this
makes one pass that caches, per (track, aerodrome), the take-off and landing
vote counts *at several flight-level caps at once* as conditional sums, plus the
minimum distance at each cap. Every combination of cap, margin, radius and
penalty is then a filter and a comparison over a small table -- the same trick
that made the endpoint sweeps affordable.

One approximation: the altitude smoothing is computed over the samples below the
widest cap and the votes are then counted within each narrower cap, where the
pipeline would smooth only the samples it keeps. The window spans two samples
either side, so this differs only where a track crosses a cap boundary, and only
for the samples immediately adjacent to it.

    python benchmarks/trend_sweep.py --build --results-dir <dir>
"""

import argparse
import sys
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

import osn_sample
from osn_sample import build_spark, load_dotenv
from adep_ades import (
    airport_locations, label_ground_truth, load_ground_truth, score,
)

TRACKS = "s3a://eurocontrol/opdi/osn_tracks"
ZONES = "s3a://eurocontrol/opdi/h3_airport_detection_zones"
CACHE = "s3a://eurocontrol/opdi/research/trend_votes"

#: Caps to cache votes at. 40 is production.
FL_CAPS = (20, 30, 40, 60, 80, 100, 120, 150, 200)
#: Zone bands to cache out to. 30 NM is production. Wider than any radius
#: swept, so the radius stays a query-time filter rather than a rebuild.
CACHE_RADIUS_NM = 80.0

#: Swept in stage 1, at a fixed penalty.
MARGINS = (0, 2, 4, 8, 16)
RADII_NM = (20.0, 30.0, 40.0, 60.0, 80.0)

#: Stage 2 sweeps the penalty at each role's winning cell, matching the range
#: the endpoint study used so the two are comparable.
PENALTIES_NM = (0.0, 5.0, 10.0, 15.0, 20.0, 30.0)
PENALTY_STAGE1 = 10.0

EARTH_R_NM = 3440.065


def haversine_nm(lat1, lon1, lat2, lon2):
    dlat, dlon = F.radians(lat2 - lat1), F.radians(lon2 - lon1)
    a = (F.sin(dlat / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2)
    return F.lit(2 * EARTH_R_NM) * F.asin(F.sqrt(F.least(a, F.lit(1.0))))


def build_cache(spark, days) -> DataFrame:
    """One expensive pass: vote counts per (track, aerodrome) at every cap."""
    sv = (spark.read.parquet(TRACKS)
          .filter(F.to_date("event_time").isin(days))
          .dropna(subset=["lat", "lon", "baro_altitude", "track_id"])
          .withColumnRenamed("callsign", "flight_id")
          .fillna({"flight_id": ""})
          .withColumn("flight_level",
                      (F.col("baro_altitude") * 3.28084 / 100).cast("int"))
          .filter(F.col("flight_level") <= max(FL_CAPS))
          .select("track_id", "icao24", "flight_id", "event_time", "lat", "lon",
                  "flight_level", "baro_altitude", "h3_res_7"))

    z = spark.read.parquet(ZONES)
    rc = next((c for c in ("apt_max_c_radius_nm", "max_c_radius_nm") if c in z.columns))
    hexc = next((c for c in ("apt_hex_id", "hex_id") if c in z.columns))
    idc = next((c for c in ("apt_ident", "ident") if c in z.columns))
    latc = next((c for c in ("apt_latitude_deg", "latitude_deg") if c in z.columns))
    lonc = next((c for c in ("apt_longitude_deg", "longitude_deg") if c in z.columns))
    schc = next((c for c in ("apt_scheduled", "scheduled_service") if c in z.columns))
    z = (z.filter(F.col(rc) <= CACHE_RADIUS_NM)
         .select(F.col(hexc).alias("_hex"), F.col(idc).alias("apt_ident"),
                 F.col(latc).alias("apt_lat"), F.col(lonc).alias("apt_lon"),
                 F.col(schc).alias("apt_scheduled")))

    j = (sv.join(z, sv.h3_res_7 == z._hex, "inner")
         .withColumn("dist_nm", haversine_nm(F.col("lat"), F.col("lon"),
                                             F.col("apt_lat"), F.col("apt_lon"))))

    part = ["icao24", "flight_id", "track_id", "apt_ident"]
    w_avg = Window.partitionBy(part).orderBy("event_time").rowsBetween(-2, 2)
    w_lag = Window.partitionBy(part).orderBy("event_time")
    j = (j.withColumn("_sm", F.avg("baro_altitude").over(w_avg))
         .withColumn("_d", F.col("_sm") - F.lag("_sm").over(w_lag)))

    aggs = []
    for cap in FL_CAPS:
        inc = F.col("flight_level") <= cap
        aggs += [
            F.sum(F.when(inc & (F.col("_d") > 0), 1).otherwise(0)).alias(f"up_{cap}"),
            F.sum(F.when(inc & (F.col("_d") < 0), 1).otherwise(0)).alias(f"dn_{cap}"),
            F.min(F.when(inc, F.col("dist_nm"))).alias(f"dist_{cap}"),
        ]
    aggs += [F.first("apt_scheduled", ignorenulls=True).alias("apt_scheduled"),
             F.min("event_time").alias("t_first"), F.max("event_time").alias("t_last")]
    return j.groupBy(*part).agg(*aggs)


def predictions(votes: DataFrame, cap: int, margin: int, radius: float,
                penalty_nm: float) -> DataFrame:
    """Apply the trend rule at one parameter setting."""
    up, dn, dist = F.col(f"up_{cap}"), F.col(f"dn_{cap}"), F.col(f"dist_{cap}")
    v = votes.filter(dist.isNotNull() & (dist <= radius))
    v = v.withColumn("status",
                     F.when(up > dn + margin, F.lit("adep"))
                     .when(dn > up + margin, F.lit("ades")))
    v = v.filter(F.col("status").isNotNull())
    # Aerodrome choice: nearest surviving candidate, optionally with the
    # scheduled-service preference the endpoint rule uses and trend does not.
    pen = F.when(F.col("apt_scheduled") == "yes", F.lit(0.0)).otherwise(
        F.lit(float(penalty_nm)))
    v = v.withColumn("_eff", dist + pen)
    w = Window.partitionBy("track_id", "status").orderBy(F.col("_eff").asc_nulls_last())
    best = v.withColumn("_r", F.row_number().over(w)).filter(F.col("_r") == 1)
    return (best.groupBy("track_id").pivot("status", ["adep", "ades"])
            .agg(F.first("apt_ident")))


def identities(votes: DataFrame) -> DataFrame:
    w = Window.partitionBy("track_id").orderBy("t_first")
    return (votes.withColumn("_r", F.row_number().over(w)).filter(F.col("_r") == 1)
            .select("track_id", F.lower("icao24").alias("icao24"),
                    F.trim("flight_id").alias("callsign"),
                    F.to_date("t_first").alias("day"),
                    F.col("t_first").alias("t_start")))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--months", nargs="+", default=["202506"])
    ap.add_argument("--days", nargs="+",
                    default=["2025-06-05", "2025-06-06", "2025-06-07"])
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--build", action="store_true", help="rebuild the vote cache")
    ap.add_argument("--executors", type=int, default=10)
    ap.add_argument("--ui-port", type=int, default=4041)
    args = ap.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
    load_dotenv()
    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(6, "8g", distributed=True)
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "300")
    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)

    if args.build:
        print("building the vote cache (one pass over the tracks)...")
        build_cache(spark, args.days).write.mode("overwrite").parquet(CACHE)
    votes = spark.read.parquet(CACHE).cache()
    print(f"vote cache: {votes.count():,} (track, aerodrome) pairs")

    ident = identities(votes)
    gt = label_ground_truth(load_ground_truth(spark, args.months, args.days),
                            airport_locations(spark)).cache()
    N = gt.count()
    print(f"ground-truth flights: {N:,}\n")

    N = gt.count()

    def cell(cap, margin, radius, pen):
        # `score` returns the counts exactly; they used to be reconstructed
        # here as round(ratio * n), which is off by up to a flight either way
        # and made two settings a few flights apart impossible to rank.
        m = score(predictions(votes, cap, margin, radius, pen), ident, gt)
        m.update(fl_cap=cap, margin=margin, radius_nm=radius, penalty_nm=pen)
        return m

    def show(m):
        print(f"{m['fl_cap']:>7}{m['margin']:>8}{m['radius_nm']:>8.0f}"
              f"{m['penalty_nm']:>9.0f}"
              f"{m['adep_coverage']:>10.2%}{m['adep_accuracy']:>10.2%}"
              f"{m['adep_score']:>11,}"
              f"{m['ades_coverage']:>10.2%}{m['ades_accuracy']:>10.2%}"
              f"{m['ades_score']:>11,}")

    HDR = (f"{'FL cap':>7}{'margin':>8}{'radius':>8}{'penalty':>9}"
           f"{'ADEP cov':>10}{'ADEP acc':>10}{'ADEP s@2':>11}"
           f"{'ADES cov':>10}{'ADES acc':>10}{'ADES s@2':>11}")

    # Stage 0: the legacy setting exactly as production ran it -- FL40,
    # margin 4, 30 NM and *no* penalty. Stage 1 sweeps at a fixed penalty of
    # 10, so without this the baseline every result is compared against would
    # not appear anywhere in the output.
    rows = []
    print(f"stage 0: the legacy setting\n{HDR}")
    m = cell(40, 4, 30.0, 0.0)
    m["stage"] = 0
    m["legacy"] = True
    rows.append(m)
    show(m)
    print()

    # Stage 1: the geometry, at a fixed penalty. Sweeping the penalty inside
    # this grid would be 1,350 cells for a parameter that the endpoint study
    # found to be near-independent of the others -- so it gets its own pass at
    # the winning cell instead, which is how the endpoint sweeps did it.
    print(f"stage 1: FL x radius x margin at penalty {PENALTY_STAGE1:g} NM\n{HDR}")
    for cap in FL_CAPS:
        for margin in MARGINS:
            for radius in RADII_NM:
                m = cell(cap, margin, radius, PENALTY_STAGE1)
                m["stage"] = 1
                rows.append(m)
                show(m)

    # Stage 2: the penalty, at whichever cell won for each role.
    print(f"\nstage 2: penalty sweep at each role's best cell\n{HDR}")
    for role in ("adep", "ades"):
        best = max((m for m in rows if m["stage"] == 1),
                   key=lambda m: m[f"{role}_score"])
        for pen in PENALTIES_NM:
            if pen == PENALTY_STAGE1:
                continue
            m = cell(best["fl_cap"], best["margin"], best["radius_nm"], pen)
            m["stage"] = 2
            m["stage2_role"] = role
            rows.append(m)
            show(m)

    spark.createDataFrame(rows).toPandas().to_csv(out / "trend_sweep.csv", index=False)
    print(f"\nwritten to {out}")
    spark.stop()


if __name__ == "__main__":
    main()
