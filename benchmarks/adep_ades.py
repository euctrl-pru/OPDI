"""
ADEP/ADES methodology comparison against EUROCONTROL ground truth.

Research harness. Builds tracks from the sampled state vectors, runs several
candidate ADEP/ADES detection methodologies over them, and scores each against
``flights_tidy`` from ``reference/``.

Everything reads from and writes to S3. See ``benchmarks/DATASETS.md``.

    python benchmarks/adep_ades.py --days 2025-06-05 --methods all

Design notes
------------
*Airport matching is great-circle, not H3.* The production flight list uses H3
res-7 cells, which quantise distance into hexagons and make a clean radius
sweep impossible. Here the detection radius is a free parameter, so distance is
computed exactly. To keep the track x airport join tractable it is bucketed on
integer lat/lon degree cells first (1 deg ~ 60 NM, so a 3x3 neighbourhood
covers any radius up to ~40 NM), then filtered on exact haversine distance.

*Track building reuses the frozen algorithm.* ``TrackProcessor._add_track_id``
is marked CRITICAL - DO NOT MODIFY because ``track_id`` continuity with
published data depends on it. It is imported and called, never reimplemented.
"""

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

import osn_sample
from osn_sample import build_spark, load_dotenv

SV_BASE = "s3a://eurocontrol/opdi/research/statevectors"
AIRPORTS = "s3a://eurocontrol/opdi/oa_airports"
AIRCRAFT_DB = "s3a://eurocontrol/opdi/osn_aircraft_db"
OUT_BASE = "s3a://eurocontrol/opdi/research/adep_ades"
TRACKS_BASE = "s3a://eurocontrol/opdi/research/tracks"

NM_PER_DEG = 60.0
EARTH_R_NM = 3440.065

#: OurAirports `type` values, coarsest-first. traffic's own docs show that
#: leaving heliports in makes `infer_airport("landing")` return a helipad
#: instead of EHAM, so this is a first-class swept parameter, not a detail.
AIRPORT_SETS = {
    "large": ["large_airport"],
    "large_medium": ["large_airport", "medium_airport"],
    "all_airports": ["large_airport", "medium_airport", "small_airport"],
    "with_heliports": [
        "large_airport",
        "medium_airport",
        "small_airport",
        "heliport",
    ],
}

#: ICAO Doc 8643 class first character. L=landplane, A=amphibian are aeroplanes;
#: H=helicopter, G=gyrocopter, T=tiltrotor are not. Helicopters are 9.8% of
#: classified airframes in the OSN DB, so this matters.
AEROPLANE_CLASSES = ("L", "A")


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------

def load_airports(spark: SparkSession, airport_set: str) -> DataFrame:
    types = AIRPORT_SETS[airport_set]
    apt = (
        spark.read.parquet(AIRPORTS)
        .filter(F.col("type").isin(types))
        .filter(F.col("ident").isNotNull())
        .select(
            F.col("ident").alias("apt"),
            F.col("type").alias("apt_type"),
            F.col("latitude_deg").cast("double").alias("apt_lat"),
            F.col("longitude_deg").cast("double").alias("apt_lon"),
            F.col("elevation_ft").cast("double").alias("apt_elev_ft"),
        )
        .filter(F.col("apt_lat").isNotNull() & F.col("apt_lon").isNotNull())
    )
    # Replicate each airport into its 3x3 degree-cell neighbourhood so the
    # later equi-join cannot miss a match that sits just across a cell border.
    offs = spark.createDataFrame(
        [(dy, dx) for dy in (-1, 0, 1) for dx in (-1, 0, 1)], "dy int, dx int"
    )
    return (
        apt.crossJoin(offs)
        .withColumn("cell_lat", F.floor("apt_lat").cast("int") + F.col("dy"))
        .withColumn("cell_lon", F.floor("apt_lon").cast("int") + F.col("dx"))
        .drop("dy", "dx")
    )


def load_state_vectors(spark: SparkSession, days: list) -> DataFrame:
    paths = [f"{SV_BASE}/day={d}" for d in days]
    return spark.read.parquet(*paths)


def filter_to_aeroplanes(
    spark: SparkSession, sv: DataFrame, include_unknown: bool
) -> DataFrame:
    """Drop helicopters, gyrocopters, tiltrotors and ground vehicles.

    Helicopters are 9.8% of classified airframes. ``icao_aircraft_class`` is
    present on ~73% of the aircraft DB, so *unknown* handling is a real
    trade-off rather than a detail: excluding unknowns protects precision at
    the cost of coverage, and coverage is the headline number here.
    """
    db = spark.read.parquet(AIRCRAFT_DB).select(
        F.lower(F.col("icao24")).alias("_ic"),
        F.substring(F.col("icao_aircraft_class"), 1, 1).alias("ac_class"),
    )
    out = sv.join(db, F.lower(sv.icao24) == F.col("_ic"), "left").drop("_ic")
    known = F.col("ac_class").isin(list(AEROPLANE_CLASSES))
    return out.filter(known | F.col("ac_class").isNull() if include_unknown else known)


def build_tracks(spark: SparkSession, sv: DataFrame) -> DataFrame:
    """Apply the frozen OPDI track-splitting algorithm."""
    from opdi.config import OPDIConfig
    from opdi.pipeline.tracks import TrackProcessor

    proc = TrackProcessor(spark, OPDIConfig.for_environment("local"))
    return proc._add_track_id(sv)


# ---------------------------------------------------------------------------
# Track/airport proximity
# ---------------------------------------------------------------------------

def haversine_nm(lat1, lon1, lat2, lon2):
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = (
        F.sin(dlat / 2) ** 2
        + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    )
    return F.lit(2 * EARTH_R_NM) * F.asin(F.sqrt(F.least(a, F.lit(1.0))))


def nearby_airports(
    sv: DataFrame, apt: DataFrame, radius_nm: float, max_fl: float
) -> DataFrame:
    """Samples within *radius_nm* of an airport and below *max_fl*."""
    low = sv.withColumn("fl", F.col("baro_altitude") * 3.28084 / 100.0)
    if max_fl is not None:
        low = low.filter(F.col("fl").isNull() | (F.col("fl") <= max_fl))
    low = low.withColumn("cell_lat", F.floor("lat").cast("int")).withColumn(
        "cell_lon", F.floor("lon").cast("int")
    )
    joined = low.join(F.broadcast(apt), ["cell_lat", "cell_lon"], "inner")
    return joined.withColumn(
        "dist_nm", haversine_nm(F.col("lat"), F.col("lon"), F.col("apt_lat"), F.col("apt_lon"))
    ).filter(F.col("dist_nm") <= radius_nm)


# ---------------------------------------------------------------------------
# Methodologies -- each returns (track_id, adep, ades)
# ---------------------------------------------------------------------------

def m1_traffic_endpoints(sv: DataFrame, apt: DataFrame, **kw) -> DataFrame:
    """M1 -- traffic's method: nearest airport to the first/last sample.

    Recreates ``TakeoffAirportInference`` / ``LandingAirportInference``
    (``traffic/algorithms/metadata/airports.py``), which sort by timestamp and
    call ``guess_airport`` on ``iloc[0]`` and ``iloc[-1]``. No altitude, no
    on_ground, no vertical trend -- purely the closest airport to one point.
    """
    w = Window.partitionBy("track_id").orderBy("event_time")
    ends = (
        sv.withColumn("_rn", F.row_number().over(w))
        .withColumn("_rr", F.row_number().over(w.orderBy(F.col("event_time").desc())))
        .filter((F.col("_rn") == 1) | (F.col("_rr") == 1))
        .withColumn("role", F.when(F.col("_rn") == 1, "adep").otherwise("ades"))
        .select("track_id", "role", "lat", "lon")
    )
    ends = ends.withColumn("cell_lat", F.floor("lat").cast("int")).withColumn(
        "cell_lon", F.floor("lon").cast("int")
    )
    j = ends.join(F.broadcast(apt), ["cell_lat", "cell_lon"], "inner").withColumn(
        "dist_nm", haversine_nm(F.col("lat"), F.col("lon"), F.col("apt_lat"), F.col("apt_lon"))
    )
    best = Window.partitionBy("track_id", "role").orderBy("dist_nm")
    j = j.withColumn("_r", F.row_number().over(best)).filter(F.col("_r") == 1)
    return _pivot_roles(j)


def m2_on_ground(sv: DataFrame, apt: DataFrame, radius_nm=30.0, max_fl=40.0, **kw):
    """M2 -- the airport at the surface samples bracketing the airborne phase.

    `on_ground` is the strongest single signal available, and the production
    algorithm selects it into ``columns_of_interest`` then never uses it
    (``flights.py:190``).

    Anchoring matters. Taking simply the first and last surface sample of a
    track scores ~20% accuracy, because when a track only has surface coverage
    at the departure end -- common, since OSN ground reception is patchy away
    from receivers -- the *last* surface sample is still at the departure
    airport, and gets emitted as the destination. Wrong, and confidently so.

    So the airborne phase is located first, and only surface samples strictly
    before it can name an ADEP, only those strictly after it an ADES. A side
    with no qualifying samples yields NULL rather than a guess.
    """
    near = nearby_airports(sv, apt, radius_nm, max_fl)
    # One airport per sample: the nearest. Otherwise a sample within range of
    # several aerodromes votes several times.
    per_sample = Window.partitionBy("track_id", "event_time").orderBy("dist_nm")
    near = near.withColumn("_a", F.row_number().over(per_sample)).filter(F.col("_a") == 1)

    tw = Window.partitionBy("track_id")
    airborne_t = F.when(~F.col("on_ground"), F.col("event_time"))
    near = near.withColumn("t_air_first", F.min(airborne_t).over(tw)).withColumn(
        "t_air_last", F.max(airborne_t).over(tw)
    )

    ground = near.filter(F.col("on_ground") & F.col("t_air_first").isNotNull())
    dep = ground.filter(F.col("event_time") < F.col("t_air_first"))
    arr = ground.filter(F.col("event_time") > F.col("t_air_last"))

    # Nearest airport at the last pre-flight / first post-flight surface sample.
    wd = Window.partitionBy("track_id").orderBy(
        F.col("event_time").desc(), F.col("dist_nm").asc()
    )
    wa = Window.partitionBy("track_id").orderBy(
        F.col("event_time").asc(), F.col("dist_nm").asc()
    )
    dep = (
        dep.withColumn("_r", F.row_number().over(wd))
        .filter(F.col("_r") == 1)
        .select("track_id", F.col("apt").alias("adep"))
    )
    arr = (
        arr.withColumn("_r", F.row_number().over(wa))
        .filter(F.col("_r") == 1)
        .select("track_id", F.col("apt").alias("ades"))
    )
    return dep.join(arr, "track_id", "full_outer")


def m3_vert_rate(sv: DataFrame, apt: DataFrame, radius_nm=30.0, max_fl=40.0, **kw):
    """M3 -- sign of measured vertical rate near the airport.

    Uses ``vert_rate`` directly rather than differencing a smoothed altitude,
    which is what the production algorithm does. ``vert_rate`` is also selected
    and never used (``flights.py:190``)."""
    near = nearby_airports(sv, apt, radius_nm, max_fl)
    agg = near.groupBy("track_id", "apt").agg(
        F.sum(F.when(F.col("vert_rate") > 0.5, 1).otherwise(0)).alias("up"),
        F.sum(F.when(F.col("vert_rate") < -0.5, 1).otherwise(0)).alias("down"),
        F.min("dist_nm").alias("dist_nm"),
        F.min("event_time").alias("t_first"),
    )
    agg = agg.withColumn("score", F.col("up") - F.col("down"))
    dep = agg.filter(F.col("score") > 0)
    arr = agg.filter(F.col("score") < 0)
    wd = Window.partitionBy("track_id").orderBy(F.col("score").desc(), F.col("dist_nm"))
    wa = Window.partitionBy("track_id").orderBy(F.col("score").asc(), F.col("dist_nm"))
    dep = dep.withColumn("_r", F.row_number().over(wd)).filter(F.col("_r") == 1).select(
        "track_id", F.col("apt").alias("adep")
    )
    arr = arr.withColumn("_r", F.row_number().over(wa)).filter(F.col("_r") == 1).select(
        "track_id", F.col("apt").alias("ades")
    )
    return dep.join(arr, "track_id", "full_outer")


def m5_min_alt_closest(sv: DataFrame, apt: DataFrame, radius_nm=30.0, max_fl=40.0, **kw):
    """M5 -- airport where the track is lowest and nearest, split by whether
    that happens in the first or last half of the track. Robust where surface
    coverage is absent entirely."""
    near = nearby_airports(sv, apt, radius_nm, max_fl)
    tw = Window.partitionBy("track_id")
    near = near.withColumn("t0", F.min("event_time").over(tw)).withColumn(
        "t1", F.max("event_time").over(tw)
    )
    mid = F.unix_timestamp("t0") + (F.unix_timestamp("t1") - F.unix_timestamp("t0")) / 2
    near = near.withColumn(
        "role", F.when(F.unix_timestamp("event_time") <= mid, "adep").otherwise("ades")
    )
    best = Window.partitionBy("track_id", "role").orderBy(
        F.col("baro_altitude").asc_nulls_last(), F.col("dist_nm").asc()
    )
    return _pivot_roles(
        near.withColumn("_r", F.row_number().over(best)).filter(F.col("_r") == 1)
    )


def m0_current(sv: DataFrame, apt: DataFrame, radius_nm=30.0, max_fl=40.0, **kw):
    """M0 -- the production algorithm's logic: sign of a 5-sample rolling-mean
    altitude difference, needing a margin of 4, ambiguous dropped
    (``flights.py:_categorize_landing_take_off``)."""
    near = nearby_airports(sv, apt, radius_nm, max_fl)
    w = Window.partitionBy("track_id", "apt").orderBy("event_time")
    roll = w.rowsBetween(-4, 0)
    near = near.withColumn("alt_ma", F.avg("baro_altitude").over(roll))
    near = near.withColumn("d", F.col("alt_ma") - F.lag("alt_ma").over(w))
    agg = near.groupBy("track_id", "apt").agg(
        F.sum(F.when(F.col("d") > 0, 1).otherwise(0)).alias("up"),
        F.sum(F.when(F.col("d") < 0, 1).otherwise(0)).alias("down"),
        F.min("dist_nm").alias("dist_nm"),
    )
    agg = agg.withColumn("margin", F.col("up") - F.col("down"))
    dep = agg.filter(F.col("margin") > 4)
    arr = agg.filter(F.col("margin") < -4)
    wd = Window.partitionBy("track_id").orderBy(F.col("margin").desc(), F.col("dist_nm"))
    wa = Window.partitionBy("track_id").orderBy(F.col("margin").asc(), F.col("dist_nm"))
    dep = dep.withColumn("_r", F.row_number().over(wd)).filter(F.col("_r") == 1).select(
        "track_id", F.col("apt").alias("adep")
    )
    arr = arr.withColumn("_r", F.row_number().over(wa)).filter(F.col("_r") == 1).select(
        "track_id", F.col("apt").alias("ades")
    )
    return dep.join(arr, "track_id", "full_outer")


def _pivot_roles(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("track_id")
        .pivot("role", ["adep", "ades"])
        .agg(F.first("apt"))
        .withColumnRenamed("adep", "adep")
        .withColumnRenamed("ades", "ades")
    )


def m6_cascade(sv: DataFrame, apt: DataFrame, radius_nm=30.0, max_fl=40.0, **kw):
    """M6 -- precision-ordered cascade over the single-signal methods.

    Measured on 2025-06-05, the methods form a clean precision/coverage ladder:
    on_ground 96.6% accurate over 24.8% of flights, vert_rate 97.4% over 57.4%,
    the production vote 98.3% over 54.8%, min-altitude 82.2% over 69.3%, and
    traffic's endpoint guess 87.5% over 72.7%.

    So take the most precise answer available for each flight and fall through
    to the next when it is silent. This keeps the accuracy of the strong
    signals where they fire and traffic's near-total coverage everywhere else,
    which is the trade the production algorithm gets backwards by dropping
    every ambiguous case outright (``flights.py:282``).

    ``ladder_rank`` records which rung answered, so the paper can report where
    the coverage actually comes from, and it doubles as a confidence proxy.
    """
    parts = [
        ("m2", m2_on_ground(sv, apt, radius_nm=radius_nm, max_fl=max_fl)),
        ("m3", m3_vert_rate(sv, apt, radius_nm=radius_nm, max_fl=max_fl)),
        ("m0", m0_current(sv, apt, radius_nm=radius_nm, max_fl=max_fl)),
        ("m5", m5_min_alt_closest(sv, apt, radius_nm=radius_nm, max_fl=max_fl)),
        ("m1", m1_traffic_endpoints(sv, apt)),
    ]
    out = None
    for tag, df in parts:
        df = df.select(
            "track_id",
            F.col("adep").alias(f"adep_{tag}"),
            F.col("ades").alias(f"ades_{tag}"),
        )
        out = df if out is None else out.join(df, "track_id", "full_outer")
    tags = [t for t, _ in parts]
    return out.select(
        "track_id",
        F.coalesce(*[F.col(f"adep_{t}") for t in tags]).alias("adep"),
        F.coalesce(*[F.col(f"ades_{t}") for t in tags]).alias("ades"),
        F.coalesce(
            *[F.when(F.col(f"adep_{t}").isNotNull(), F.lit(t)) for t in tags]
        ).alias("ladder_rank"),
    )


METHODS = {
    "M0_current": m0_current,
    "M1_traffic": m1_traffic_endpoints,
    "M2_on_ground": m2_on_ground,
    "M3_vert_rate": m3_vert_rate,
    "M5_min_alt": m5_min_alt_closest,
    "M6_cascade": m6_cascade,
}


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def track_identity(sv: DataFrame) -> DataFrame:
    """(track_id -> icao24, callsign, date) for joining to ground truth."""
    w = Window.partitionBy("track_id").orderBy("event_time")
    return (
        sv.withColumn("_r", F.row_number().over(w))
        .filter(F.col("_r") == 1)
        .select(
            "track_id",
            F.lower("icao24").alias("icao24"),
            F.trim("callsign").alias("callsign"),
            F.to_date("event_time").alias("day"),
            F.col("event_time").alias("t_start"),
        )
    )


def load_ground_truth(spark: SparkSession, months: list, days: list = None) -> DataFrame:
    frames = []
    for m in months:
        p = str(REPO / "reference" / f"flights_{m}.parquet")
        frames.append(
            spark.read.parquet(p).select(
                F.lower(F.col("AIRCRAFT_ADDRESS")).alias("icao24"),
                F.trim(F.col("AIRCRAFT_ID")).alias("callsign"),
                F.col("ADEP").alias("gt_adep"),
                F.col("ADES").alias("gt_ades"),
                F.col("AOBT_3").alias("gt_aobt"),
            )
        )
    gt = frames[0]
    for f_ in frames[1:]:
        gt = gt.unionByName(f_)
    gt = gt.filter(F.col("icao24").isNotNull()).withColumn("day", F.to_date("gt_aobt"))
    # Restrict to the days actually sampled. Scoring one day of tracks against a
    # whole month of ground truth makes every coverage figure ~1/30 of its real
    # value -- the denominator has to be the flights we could possibly have seen.
    if days:
        gt = gt.filter(F.col("day").isin([str(d) for d in days]))
    return gt


def score(pred: DataFrame, ident: DataFrame, gt: DataFrame) -> dict:
    """Coverage and accuracy against ground truth.

    Ground truth is the denominator: coverage is the fraction of real flights
    for which the method emits *any* answer. 2.42% of (icao24, callsign, date)
    keys collide -- same-day multi-leg rotations -- so ties are broken on
    proximity to AOBT_3 rather than left to chance.
    """
    p = ident.join(pred, "track_id", "left")
    j = gt.join(p, ["icao24", "callsign", "day"], "left")
    w = Window.partitionBy("icao24", "callsign", "day", "gt_adep", "gt_ades").orderBy(
        F.abs(F.unix_timestamp("t_start") - F.unix_timestamp("gt_aobt")).asc_nulls_last()
    )
    j = j.withColumn("_r", F.row_number().over(w)).filter(F.col("_r") == 1)

    agg = j.agg(
        F.count(F.lit(1)).alias("n_gt"),
        F.sum(F.when(F.col("adep").isNotNull(), 1).otherwise(0)).alias("adep_any"),
        F.sum(F.when(F.col("ades").isNotNull(), 1).otherwise(0)).alias("ades_any"),
        F.sum(F.when(F.col("adep") == F.col("gt_adep"), 1).otherwise(0)).alias("adep_ok"),
        F.sum(F.when(F.col("ades") == F.col("gt_ades"), 1).otherwise(0)).alias("ades_ok"),
    ).first()
    n = agg["n_gt"] or 1
    return {
        "n_ground_truth": agg["n_gt"],
        "adep_coverage": agg["adep_any"] / n,
        "ades_coverage": agg["ades_any"] / n,
        "adep_accuracy": (agg["adep_ok"] / agg["adep_any"]) if agg["adep_any"] else 0.0,
        "ades_accuracy": (agg["ades_ok"] / agg["ades_any"]) if agg["ades_any"] else 0.0,
        "adep_overall": agg["adep_ok"] / n,
        "ades_overall": agg["ades_ok"] / n,
    }


def _materialise_tracks(spark: SparkSession, args) -> DataFrame:
    """Build tracks once, persist to S3, read back. Idempotent per day."""
    keep = "unk" if args.include_unknown_aircraft else "known"
    out = f"{TRACKS_BASE}/aircraft={keep}"
    todo = []
    for d in args.days:
        p = f"{out}/day={d}"
        if _s3_exists(spark, p + "/_SUCCESS"):
            print(f"tracks {d}: already in S3")
        else:
            todo.append(d)
    for d in todo:
        print(f"tracks {d}: building ...")
        sv = load_state_vectors(spark, [d])
        sv = filter_to_aeroplanes(spark, sv, args.include_unknown_aircraft)
        build_tracks(spark, sv).write.mode("overwrite").parquet(f"{out}/day={d}")
    paths = [f"{out}/day={d}" for d in args.days]
    df = spark.read.parquet(*paths)
    print(f"tracks ready: {df.count():,} state vectors")
    return df


def _s3_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark._jsc.hadoopConfiguration()).exists(p)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", nargs="+", required=True)
    ap.add_argument("--months", nargs="+", default=["202506"])
    ap.add_argument("--methods", nargs="+", default=["all"])
    ap.add_argument("--airport-set", default="large_medium", choices=list(AIRPORT_SETS))
    ap.add_argument("--radius-nm", type=float, default=30.0)
    ap.add_argument("--max-fl", type=float, default=40.0)
    ap.add_argument("--include-unknown-aircraft", action="store_true")
    ap.add_argument("--ui-port", type=int, default=4041,
                    help="Spark UI port; proxied at /proxy/<port>/. Defaults to "
                         "4041 so it does not collide with a concurrent sampler.")
    ap.add_argument("--cores", type=int, default=6)
    ap.add_argument("--driver-memory", default="9g")
    args = ap.parse_args()

    load_dotenv()
    osn_sample.UI_PORT = args.ui_port
    spark = build_spark(args.cores, args.driver_memory)
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "96")

    # This container is capped at 16 GB (cgroup memory.max), so caching tens of
    # millions of rows in the driver is not an option. Tracks are materialised
    # to S3 once and every method reads them back from there.
    sv = _materialise_tracks(spark, args)

    apt = load_airports(spark, args.airport_set)
    ident = track_identity(sv)
    gt = load_ground_truth(spark, args.months, args.days)
    print(f"ground-truth flights: {gt.count():,}")

    names = list(METHODS) if args.methods == ["all"] else args.methods
    rows = []
    for name in names:
        print(f"\n--- {name} ---")
        pred = METHODS[name](sv, apt, radius_nm=args.radius_nm, max_fl=args.max_fl)
        m = score(pred, ident, gt)
        m["method"] = name
        m["airport_set"] = args.airport_set
        m["radius_nm"] = args.radius_nm
        m["max_fl"] = args.max_fl
        rows.append(m)
        print(
            f"  ADEP cov {m['adep_coverage']:6.2%} acc {m['adep_accuracy']:6.2%} "
            f"overall {m['adep_overall']:6.2%}   |   "
            f"ADES cov {m['ades_coverage']:6.2%} acc {m['ades_accuracy']:6.2%} "
            f"overall {m['ades_overall']:6.2%}"
        )

    res = spark.createDataFrame(rows)
    tag = f"{args.airport_set}_r{int(args.radius_nm)}_fl{int(args.max_fl)}"
    res.coalesce(1).write.mode("overwrite").parquet(f"{OUT_BASE}/results/{tag}")
    print(f"\nresults -> {OUT_BASE}/results/{tag}")
    spark.stop()


if __name__ == "__main__":
    main()
