"""
Sweep `trend`'s altitude cut on **both datums**: v6.1's fork of trend_sweep.py.

`trend_sweep.py` is V6's and stays exactly as it is. It is fingerprinted by
V6's two vote-cache stages and by its bearing job, so editing it would mark a
published paper's figures stale for a change that paper never made. Forking is
the convention here -- the same reason `flight_list_v7.py` exists beside
`flight_list_v6.py`.

What this fork adds: a second family of vote columns counted against **height
above field elevation** rather than flight level, built in the same pass. The
cache already keyed on (track, aerodrome) and already applied each cap as a
conditional sum *after* the aerodrome join, so the second family costs an
elevation join and more aggregate columns -- not a second pass over the tracks.

The cache lands at `research/trend_votes_agl`, leaving V6's
`research/trend_votes` untouched so V6 stays reproducible.

What follows is trend_sweep.py's own description of the sweep, unchanged
because it still applies.

---

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

The altitude smoothing is computed over the samples below the widest cap and the
votes are then counted within each narrower cap. This used to be an
approximation -- the pipeline smoothed only the samples it kept, so the two
differed for the samples immediately adjacent to a cap boundary. It is no
longer one: ``DetectionConfig.trend_smooth_before_cut`` makes the pipeline
smooth first as well, which closed one of the two remaining differences between
this sweep and the code that ships.

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
from tables import table, fixed
from osn_sample import build_spark, load_dotenv
from adep_ades import (
    airport_locations, label_ground_truth, load_ground_truth, score,
)
from elevation_bands import airport_elevations

TRACKS = table("osn_tracks")
ZONES = table("h3_airport_detection_zones")
CACHE = table("research/trend_votes_agl")

#: The second period's tracks. Built by earlier versions of this study and
#: still present, so 2024 needs neither an ingest nor a track rebuild -- but
#: they pre-date H3 indexing, so `h3_res_7` has to be computed at read time.
TRACKS_2024 = fixed("research/tracks")

#: Caps to cache votes at. 40 is what the published algorithm used.
#:
#: Mutable, and set from ``--fl-caps`` in :func:`main` before anything reads it.
#: The cache carries one vote pair per cap in a single pass, so extending the
#: range costs one wider pre-filter rather than one pass per cap -- which is
#: what makes a fourteen-cap grid affordable at all.
FL_CAPS = (20, 30, 40, 60, 80, 100, 120, 150, 200)

#: Ceilings in **feet above field elevation** -- the analogue of FL_CAPS, and
#: what the shipped `trend_max_height_ft` is swept over.
#:
#: 6100 rather than 6000 is deliberate, and is the datum arm's control.
#: `flight_level` is an integer cast, so FL60 admits everything below 6,100 ft.
#: Comparing 6,000 ft above field against FL60 would move the ceiling and the
#: datum at the same time, and the arm is meant to move exactly one thing.
#:
#: Mutable, like FL_CAPS, and set from ``--height-caps`` before anything reads
#: it -- the cache's column names encode these, so the builder and the reader
#: must agree by construction rather than by argument passing.
HEIGHT_CAPS = (2000, 3000, 4000, 6100, 8000, 10000, 12000, 15000, 20000)
#: Zone bands to cache out to. 30 NM is production. Wider than any radius
#: swept, so the radius stays a query-time filter rather than a rebuild.
CACHE_RADIUS_NM = 80.0

#: Swept in stage 1, at a fixed penalty.
MARGINS = (0, 2, 4, 8, 16)
#: Extended below 20 NM after the first pass put *both* roles' optimum on the
#: grid floor. The reason is visible in the counts: over 20-80 NM `correct`
#: moves by ~160 flights while `wrong` doubles, so under any k the score falls
#: monotonically outward and the argmax was the smallest radius offered rather
#: than a real peak. Going lower is free -- `predictions` filters the cached
#: `dist_{cap}`, so a tighter radius is a query-time filter, not a rebuild.
RADII_NM = (5.0, 10.0, 15.0, 20.0, 30.0, 40.0, 60.0, 80.0)

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


def build_cache(spark, days, tracks: str = TRACKS, add_h3: bool = False) -> DataFrame:
    """One expensive pass: vote counts per (track, aerodrome) at every cap.

    ``add_h3`` computes the resolution-7 index rather than reading it. The
    second period's tracks pre-date H3 indexing, and ``geo_to_h3`` is a
    row-at-a-time Python UDF, so it is applied *after* the flight-level filter
    -- indexing points the sweep will never look at is the expensive half of
    the job.
    """
    # The zone table first, because the pre-filter's width depends on it.
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

    # Field elevation, joined onto the zone table rather than onto the state
    # vectors: small against small, once, instead of once per sample.
    elev = airport_elevations(spark)
    z = (z.join(F.broadcast(elev), z.apt_ident == elev._apt, "left")
         .drop("_apt").withColumnRenamed("_elev_ft", "apt_elev_ft"))

    # Wide enough for both families. The above-field caps reach higher in
    # *pressure* altitude than the flight-level caps do -- 20,000 ft above a
    # 6,000 ft field is FL260 -- so pre-filtering at max(FL_CAPS) would
    # silently truncate the top of the height sweep.
    #
    # The bound is the highest above-field cap plus the highest field **in the
    # zone table**, not in OurAirports as a whole. The reference carries fields
    # above 14,000 ft that the bounding box excludes, and widening the scan for
    # aerodromes no sample can join to would cost roughly a hundred flight
    # levels for nothing. Same union bound, and same reasoning, as the
    # pipeline's own pre-filter.
    max_elev = z.select(F.max("apt_elev_ft")).first()[0] or 0.0
    ceiling_fl = int((max(HEIGHT_CAPS) + max_elev) / 100) + 1
    prefilter_fl = max(max(FL_CAPS), ceiling_fl)
    print(f"vote-cache pre-filter: FL{prefilter_fl} "
          f"(highest matchable field {max_elev:,.0f} ft)")

    sv = (spark.read.parquet(tracks)
          .filter(F.to_date("event_time").isin(days))
          .dropna(subset=["lat", "lon", "baro_altitude", "track_id"])
          .withColumnRenamed("callsign", "flight_id")
          .fillna({"flight_id": ""})
          .withColumn("flight_level",
                      (F.col("baro_altitude") * 3.28084 / 100).cast("int"))
          .filter(F.col("flight_level") <= prefilter_fl))

    if add_h3:
        import h3_pyspark

        sv = (sv.withColumn("_res", F.lit(7))
                .withColumn("h3_res_7", h3_pyspark.geo_to_h3("lat", "lon", "_res"))
                .drop("_res"))

    sv = sv.select("track_id", "icao24", "flight_id", "event_time", "lat", "lon",
                   "flight_level", "baro_altitude", "h3_res_7")

    j = (sv.join(z, sv.h3_res_7 == z._hex, "inner")
         .withColumn("dist_nm", haversine_nm(F.col("lat"), F.col("lon"),
                                             F.col("apt_lat"), F.col("apt_lon"))))

    return add_height_votes(j)


def add_height_votes(j: DataFrame) -> DataFrame:
    """Vote counts per (track, aerodrome) at every cap, on both datums.

    Both families come out of one pass. ``_sm`` is a centred rolling mean of
    barometric altitude and ``_d`` its first difference, so a vote is the sign
    of ``_d``; the caps differ only in which samples are *admitted* to vote,
    which is the entire content of the datum change -- and is why one pass can
    serve both.

    Sharing the pass matters for more than cost: the two families are then
    counted from the same smoothed series, so a difference between them cannot
    be an artefact of having smoothed twice.
    """
    part = ["icao24", "flight_id", "track_id", "apt_ident"]
    w_avg = Window.partitionBy(part).orderBy("event_time").rowsBetween(-2, 2)
    w_lag = Window.partitionBy(part).orderBy("event_time")
    j = (j.withColumn("_sm", F.avg("baro_altitude").over(w_avg))
         .withColumn("_d", F.col("_sm") - F.lag("_sm").over(w_lag))
         # Mirrors `flights.height_above_field` exactly, coalesce included: the
         # sweep must admit the same samples production does, or a tuned cap
         # would not transfer to the pipeline.
         .withColumn("_agl_ft",
                     F.col("baro_altitude") * F.lit(3.28084)
                     - F.coalesce(F.col("apt_elev_ft"), F.lit(0.0))))

    aggs = []
    for cap in FL_CAPS:
        inc = F.col("flight_level") <= cap
        aggs += [
            F.sum(F.when(inc & (F.col("_d") > 0), 1).otherwise(0)).alias(f"up_{cap}"),
            F.sum(F.when(inc & (F.col("_d") < 0), 1).otherwise(0)).alias(f"dn_{cap}"),
            F.min(F.when(inc, F.col("dist_nm"))).alias(f"dist_{cap}"),
        ]
    for cap in HEIGHT_CAPS:
        inc = F.col("_agl_ft") <= F.lit(float(cap))
        aggs += [
            F.sum(F.when(inc & (F.col("_d") > 0), 1).otherwise(0)).alias(f"up_agl_{cap}"),
            F.sum(F.when(inc & (F.col("_d") < 0), 1).otherwise(0)).alias(f"dn_agl_{cap}"),
            F.min(F.when(inc, F.col("dist_nm"))).alias(f"dist_agl_{cap}"),
        ]
    aggs += [F.first("apt_scheduled", ignorenulls=True).alias("apt_scheduled"),
             F.first("apt_elev_ft", ignorenulls=True).alias("apt_elev_ft"),
             F.min("event_time").alias("t_first"), F.max("event_time").alias("t_last")]
    return j.groupBy(*part).agg(*aggs)


def predictions(votes: DataFrame, cap: int, margin: int, radius: float,
                penalty_nm: float, datum: str = "field") -> DataFrame:
    """Apply the trend rule at one parameter setting, on one datum.

    ``datum`` selects the column family, and *cap* is read in that family's
    units: feet above field elevation for ``"field"``, a flight level for
    ``"msl"``. Passing one's cap with the other's datum reads a column that
    does not exist, which is the loud failure -- the quiet one, reading the
    wrong family silently, is what the explicit argument prevents.
    """
    if datum not in ("field", "msl"):
        raise ValueError(
            f"datum must be 'field' or 'msl', got {datum!r}. Defaulting here "
            f"would score one datum and label it the other."
        )
    suffix = f"agl_{cap}" if datum == "field" else f"{cap}"
    up, dn, dist = (F.col(f"up_{suffix}"), F.col(f"dn_{suffix}"),
                    F.col(f"dist_{suffix}"))
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
    ap.add_argument("--build-only", action="store_true",
                    help="build the vote cache and stop, without sweeping. The "
                         "cache is an input several jobs share, so building it "
                         "is a separate step from scoring 371 cells over it.")
    ap.add_argument("--tracks", default=TRACKS,
                    help=f"track table to build the cache from (default {TRACKS}; "
                         f"use {TRACKS_2024} for the 2024 period)")
    ap.add_argument("--add-h3", action="store_true",
                    help="compute h3_res_7 rather than read it, for tracks that "
                         "pre-date H3 indexing")
    ap.add_argument("--cache", default=CACHE, help="where the vote cache lives")
    ap.add_argument("--out-name", default="trend_sweep.csv")
    ap.add_argument("--fl-caps", nargs="+", type=int, default=None,
                    help="flight-level caps to cache and sweep. The cache is "
                         "built for exactly these, so a sweep asking for a cap "
                         "the cache was not built with has no column to read "
                         "-- pass the same list to both.")
    ap.add_argument("--height-caps", nargs="+", type=int, default=None,
                    help="above-field ceilings in feet to cache and sweep. Same "
                         "contract as --fl-caps: the cache is built for exactly "
                         "these, so pass the same list to builder and sweep.")
    ap.add_argument("--datum", choices=("field", "msl"), default="field",
                    help="which datum to sweep. 'field' sweeps --height-caps in "
                         "feet above field elevation; 'msl' sweeps --fl-caps as "
                         "flight levels, reproducing trend_sweep.py's arm.")
    ap.add_argument("--executors", type=int, default=10)
    ap.add_argument("--ui-port", type=int, default=4041)
    args = ap.parse_args()

    sys.stdout.reconfigure(line_buffering=True)

    # Set before build_votes or the sweep reads it. Module globals rather than
    # parameters because the cache's *column names* encode the caps, so the
    # builder and the reader must agree by construction and not by argument
    # passing.
    if args.fl_caps:
        global FL_CAPS
        FL_CAPS = tuple(sorted(set(args.fl_caps)))
    if args.height_caps:
        global HEIGHT_CAPS
        HEIGHT_CAPS = tuple(sorted(set(args.height_caps)))
    print(f"flight-level caps: {', '.join(f'FL{c}' for c in FL_CAPS)}")
    print(f"above-field caps:  {', '.join(f'{c:,} ft' for c in HEIGHT_CAPS)}")
    print(f"sweeping datum:    {args.datum}")

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
        (build_cache(spark, args.days, tracks=args.tracks, add_h3=args.add_h3)
         .write.mode("overwrite").parquet(args.cache))
    if args.build_only:
        print("vote cache built; stopping before the sweep (--build-only)")
        spark.stop()
        return

    votes = spark.read.parquet(args.cache).cache()
    print(f"vote cache: {votes.count():,} (track, aerodrome) pairs")

    ident = identities(votes)
    gt = label_ground_truth(load_ground_truth(spark, args.months, args.days),
                            airport_locations(spark)).cache()
    N = gt.count()
    print(f"ground-truth flights: {N:,}\n")

    N = gt.count()

    # The cap list the sweep walks, chosen by datum. `fl_cap` stays the column
    # name in the output so the two arms tabulate the same way -- but it is
    # read in the datum's own units, and `datum` is carried on every row so a
    # reader cannot mistake 6,100 ft for FL6100.
    caps = HEIGHT_CAPS if args.datum == "field" else FL_CAPS
    legacy_cap = 6100 if args.datum == "field" else 40

    def cell(cap, margin, radius, pen):
        # `score` returns the counts exactly; they used to be reconstructed
        # here as round(ratio * n), which is off by up to a flight either way
        # and made two settings a few flights apart impossible to rank.
        m = score(predictions(votes, cap, margin, radius, pen, datum=args.datum),
                  ident, gt)
        m.update(fl_cap=cap, margin=margin, radius_nm=radius, penalty_nm=pen,
                 datum=args.datum)
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
    print(f"stage 1: cap x radius x margin at penalty {PENALTY_STAGE1:g} NM\n{HDR}")
    for cap in caps:
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

    spark.createDataFrame(rows).toPandas().to_csv(out / args.out_name, index=False)
    print(f"\nwritten to {out}")
    spark.stop()


if __name__ == "__main__":
    main()
