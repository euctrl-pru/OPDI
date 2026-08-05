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
#: Ground truth mirrored to S3. The committed git-lfs copy in reference/ lives
#: on the driver's local disk, which the remote K8s executor pods cannot see --
#: reading it by local path works in local[*] mode and fails on the cluster.
REFERENCE_BASE = "s3a://eurocontrol/opdi/research/reference"

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

def endpoint_candidates(sv: DataFrame, apt: DataFrame) -> DataFrame:
    """Nearest airport to the track's first and last sample, with the evidence.

    The evidence columns -- how far the endpoint is from that airport, how high
    it is, whether it is on the surface -- are what M7 abstains on. M1 ignores
    them and accepts every candidate.
    """
    w = Window.partitionBy("track_id").orderBy("event_time")
    ends = (
        sv.withColumn("_rn", F.row_number().over(w))
        .withColumn("_rr", F.row_number().over(w.orderBy(F.col("event_time").desc())))
        .filter((F.col("_rn") == 1) | (F.col("_rr") == 1))
        .withColumn("role", F.when(F.col("_rn") == 1, "adep").otherwise("ades"))
        .select("track_id", "role", "lat", "lon", "baro_altitude", "on_ground")
    )
    ends = ends.withColumn("cell_lat", F.floor("lat").cast("int")).withColumn(
        "cell_lon", F.floor("lon").cast("int")
    )
    j = ends.join(F.broadcast(apt), ["cell_lat", "cell_lon"], "inner").withColumn(
        "dist_nm", haversine_nm(F.col("lat"), F.col("lon"), F.col("apt_lat"), F.col("apt_lon"))
    )
    best = Window.partitionBy("track_id", "role").orderBy("dist_nm")
    return j.withColumn("_r", F.row_number().over(best)).filter(F.col("_r") == 1)


def pivot_endpoints(cand: DataFrame) -> DataFrame:
    """One row per track: adep/ades plus each side's evidence columns."""
    p = (
        cand.groupBy("track_id")
        .pivot("role", ["adep", "ades"])
        .agg(
            F.first("apt").alias("apt"),
            F.first("dist_nm").alias("dist_nm"),
            # Height above the aerodrome, not above the ellipsoid: a 1,000 ft
            # cut-off means nothing at an airport sitting at 5,000 ft.
            F.first(
                F.col("baro_altitude") * 3.28084 - F.coalesce(F.col("apt_elev_ft"), F.lit(0.0))
            ).alias("agl_ft"),
            F.first("on_ground").alias("on_ground"),
        )
    )
    for side in ("adep", "ades"):
        p = p.withColumnRenamed(f"{side}_apt", side)
    return p


def m1_traffic_endpoints(sv: DataFrame, apt: DataFrame, **kw) -> DataFrame:
    """M1 -- traffic's method: nearest airport to the first/last sample.

    Recreates ``TakeoffAirportInference`` / ``LandingAirportInference``
    (``traffic/algorithms/metadata/airports.py``), which sort by timestamp and
    call ``guess_airport`` on ``iloc[0]`` and ``iloc[-1]``. No altitude, no
    on_ground, no vertical trend -- purely the closest airport to one point.
    """
    return pivot_endpoints(endpoint_candidates(sv, apt)).select("track_id", "adep", "ades")


def m7_endpoint_abstain(
    sv: DataFrame,
    apt: DataFrame,
    max_endpoint_dist_nm: float = 10.0,
    max_endpoint_agl_ft: float = 5000.0,
    require_on_ground: bool = False,
    _endpoints: DataFrame = None,
    **kw,
) -> DataFrame:
    """M7 -- M1, but abstaining when the endpoint is not credibly at an airport.

    The cascade experiment showed that no vertical-trend signal improves on
    nearest-airport-to-endpoint *on the flights where both fire* -- M1 is the
    better namer everywhere (``diagnose_cascade``, 2025-06-05). What M1 lacks
    is the judgement to stay silent: it names an airport for a track that
    begins in the cruise just as confidently as for one that begins at a gate,
    and on that marginal set it is only ~42% right.

    So the open question is not which signal names the airport. It is when to
    abstain. That is a decision about the endpoint itself -- its distance to
    the aerodrome and its height above it -- not about the trajectory shape.

    The defaults are the measured operating point (``sweep_abstention``,
    2025-06-05): 10 NM and 5,000 ft AGL gives 58.79% ADEP coverage at 99.40%
    accuracy, beating the production algorithm on *both* axes at once (54.78%
    at 98.25%). Height does nearly all the work -- relaxing the radius from
    10 NM to 40 NM moves ADEP coverage by 0.2 pp -- which is why the radius is
    generous and the height cut is not.
    """
    p = _endpoints if _endpoints is not None else pivot_endpoints(endpoint_candidates(sv, apt))
    for side in ("adep", "ades"):
        ok = F.col(f"{side}_dist_nm") <= max_endpoint_dist_nm
        if require_on_ground:
            ok = ok & F.col(f"{side}_on_ground")
        else:
            # A missing barometric altitude on a surface sample is normal, so
            # on_ground satisfies the height test outright.
            ok = ok & (
                F.col(f"{side}_on_ground")
                | (F.col(f"{side}_agl_ft") <= max_endpoint_agl_ft)
            )
        p = p.withColumn(side, F.when(ok, F.col(side)))
    return p.select("track_id", "adep", "ades")


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


#: The single-signal methods, addressable by short tag so a cascade can be
#: specified as an ordered list on the command line and swept.
RUNGS = {
    "m0": m0_current,
    "m1": m1_traffic_endpoints,
    "m2": m2_on_ground,
    "m3": m3_vert_rate,
    "m5": m5_min_alt_closest,
}

#: Default cascade order, sorted by *measured unconditional accuracy* on
#: 2025-06-05: m0 98.3%, m3 97.4%, m2 96.6%, m1 87.5%, m5 82.2%.
#:
#: The first version of this list ran m5 before m1, on the assumption that the
#: broader-coverage method belonged last. That is the wrong axis -- a cascade
#: must be ordered by precision, because a rung that fires overrules every rung
#: below it. Placing the 82% method above the 88% one silently overwrote good
#: answers with worse ones on the ~65% of flights where both fire.
DEFAULT_LADDER = ("m0", "m3", "m2", "m1", "m5")


def cascade_wide(
    sv: DataFrame, apt: DataFrame, ladder=DEFAULT_LADDER, radius_nm=30.0, max_fl=40.0
) -> DataFrame:
    """Every rung's answer side by side, plus the cascaded pick and its source.

    Keeping the per-rung columns (rather than collapsing straight to a
    coalesce) is what makes the cascade auditable: ``diagnose_cascade`` can
    then ask, for the flights each rung claimed, whether it beat the rung it
    displaced.
    """
    out = None
    for tag in ladder:
        df = RUNGS[tag](sv, apt, radius_nm=radius_nm, max_fl=max_fl).select(
            "track_id",
            F.col("adep").alias(f"adep_{tag}"),
            F.col("ades").alias(f"ades_{tag}"),
        )
        out = df if out is None else out.join(df, "track_id", "full_outer")

    def pick(side):
        return F.coalesce(*[F.col(f"{side}_{t}") for t in ladder]).alias(side)

    def src(side):
        return F.coalesce(
            *[F.when(F.col(f"{side}_{t}").isNotNull(), F.lit(t)) for t in ladder]
        ).alias(f"{side}_src")

    return out.select(
        "*", pick("adep"), pick("ades"), src("adep"), src("ades")
    )


def m6_cascade(sv: DataFrame, apt: DataFrame, radius_nm=30.0, max_fl=40.0, ladder=None, **kw):
    """M6 -- precision-ordered cascade over the single-signal methods.

    Take the most precise answer available for each flight and fall through to
    the next rung when it is silent. This keeps the accuracy of the strong
    signals where they fire and traffic's near-total coverage everywhere else,
    which is the trade the production algorithm gets backwards by dropping
    every ambiguous case outright (``flights.py:282``).

    The two sides are resolved independently -- a flight can take its ADEP from
    ``on_ground`` and its ADES from ``vert_rate``. That is deliberate: OSN
    surface coverage is asymmetric, and a track with a receiver at the origin
    but none at the destination should not have its good ADEP discarded for the
    sake of a matched pair.

    Note that a cascade's *conditional* accuracy is not bounded below by its
    rungs' unconditional accuracies: each rung is scored here only on the
    flights the rungs above it could not answer, which are the harder ones.
    A cascade legitimately scores below every rung it is built from. What it
    must not do is score below its own fallback rung used alone -- that means
    a rung is actively overwriting better answers, which is what
    ``diagnose_cascade`` tests for.
    """
    ladder = tuple(ladder or DEFAULT_LADDER)
    return cascade_wide(sv, apt, ladder, radius_nm, max_fl).select(
        "track_id", "adep", "ades", "adep_src", "ades_src"
    )


METHODS = {
    "M0_current": m0_current,
    "M1_traffic": m1_traffic_endpoints,
    "M2_on_ground": m2_on_ground,
    "M3_vert_rate": m3_vert_rate,
    "M5_min_alt": m5_min_alt_closest,
    "M6_cascade": m6_cascade,
    "M7_abstain": m7_endpoint_abstain,
}

#: Abstention grid for M7. Distances in NM from the track endpoint to the
#: nearest aerodrome; heights in ft above that aerodrome's elevation.
ABSTAIN_DIST_NM = (1.0, 2.0, 3.0, 5.0, 10.0, 20.0, 40.0, 1e9)
ABSTAIN_AGL_FT = (0.0, 500.0, 1000.0, 2000.0, 5000.0, 1e9)


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
        p = f"{REFERENCE_BASE}/flights_{m}.parquet"
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


def align_to_ground_truth(pred: DataFrame, ident: DataFrame, gt: DataFrame) -> DataFrame:
    """One row per ground-truth flight, with the method's prediction attached.

    Ground truth is the denominator throughout: a flight the method never saw
    counts against it as a miss, not as an absent row. 2.42% of
    (icao24, callsign, date) keys collide -- same-day multi-leg rotations -- so
    ties are broken on proximity to AOBT_3 rather than left to chance.
    """
    p = ident.join(pred, "track_id", "left")
    j = gt.join(p, ["icao24", "callsign", "day"], "left")
    w = Window.partitionBy("icao24", "callsign", "day", "gt_adep", "gt_ades").orderBy(
        F.abs(F.unix_timestamp("t_start") - F.unix_timestamp("gt_aobt")).asc_nulls_last()
    )
    return j.withColumn("_r", F.row_number().over(w)).filter(F.col("_r") == 1)


def score(pred: DataFrame, ident: DataFrame, gt: DataFrame) -> dict:
    """Coverage and accuracy against ground truth."""
    j = align_to_ground_truth(pred, ident, gt)

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


def diagnose_cascade(
    sv: DataFrame,
    apt: DataFrame,
    ident: DataFrame,
    gt: DataFrame,
    ladder=DEFAULT_LADDER,
    radius_nm=30.0,
    max_fl=40.0,
    fallback="m1",
) -> DataFrame:
    """Per-rung attribution: who answered, were they right, and did they help?

    For every ground-truth flight the cascade answered, group by the rung that
    supplied the answer and compare that rung's hit rate against what
    *fallback* would have said on the very same flights. ``lift`` is the
    difference. A negative lift is the failure mode a cascade is prone to and
    that an aggregate accuracy number cannot show: a rung firing confidently
    and overwriting a better answer from further down the ladder.
    """
    wide = cascade_wide(sv, apt, tuple(ladder), radius_nm, max_fl)
    j = align_to_ground_truth(wide, ident, gt)
    n_gt = j.count()

    frames = []
    for side in ("adep", "ades"):
        agg = (
            j.filter(F.col(f"{side}_src").isNotNull())
            .groupBy(F.col(f"{side}_src").alias("rung"))
            .agg(
                F.count(F.lit(1)).alias("n_answered"),
                F.sum(F.when(F.col(side) == F.col(f"gt_{side}"), 1).otherwise(0)).alias(
                    "n_correct"
                ),
                F.sum(
                    F.when(F.col(f"{side}_{fallback}") == F.col(f"gt_{side}"), 1).otherwise(0)
                ).alias("n_fallback_correct"),
            )
        )
        frames.append(
            agg.withColumn("side", F.lit(side))
            .withColumn("share_of_gt", F.col("n_answered") / F.lit(n_gt))
            .withColumn("accuracy", F.col("n_correct") / F.col("n_answered"))
            .withColumn(
                "fallback_accuracy", F.col("n_fallback_correct") / F.col("n_answered")
            )
            .withColumn("lift", F.col("accuracy") - F.col("fallback_accuracy"))
        )

    order = {t: i for i, t in enumerate(ladder)}
    out = frames[0].unionByName(frames[1])
    return out.withColumn(
        "ladder_pos",
        F.coalesce(*[F.when(F.col("rung") == t, F.lit(i)) for t, i in order.items()]),
    ).orderBy("side", "ladder_pos")


def sweep_abstention(
    spark: SparkSession, sv: DataFrame, apt: DataFrame, ident: DataFrame, gt: DataFrame
) -> DataFrame:
    """Coverage/accuracy curve for M7 over the distance x height grid.

    The endpoint frame is computed once and reused for every cell -- it is one
    row per track, so it caches comfortably, and the whole sweep costs one pass
    over the state vectors rather than one per parameter combination.
    """
    endpoints = pivot_endpoints(endpoint_candidates(sv, apt)).cache()
    print(f"endpoint candidates: {endpoints.count():,} tracks")
    rows = []
    for d in ABSTAIN_DIST_NM:
        for a in ABSTAIN_AGL_FT:
            pred = m7_endpoint_abstain(
                sv, apt, max_endpoint_dist_nm=d, max_endpoint_agl_ft=a, _endpoints=endpoints
            )
            m = score(pred, ident, gt)
            m["max_endpoint_dist_nm"] = d
            m["max_endpoint_agl_ft"] = a
            rows.append(m)
            print(
                f"  d<={d:>8.0f} NM  agl<={a:>8.0f} ft   "
                f"ADEP cov {m['adep_coverage']:6.2%} acc {m['adep_accuracy']:6.2%} "
                f"overall {m['adep_overall']:6.2%}   |   "
                f"ADES cov {m['ades_coverage']:6.2%} acc {m['ades_accuracy']:6.2%} "
                f"overall {m['ades_overall']:6.2%}"
            )
    endpoints.unpersist()
    return spark.createDataFrame(rows)


def _print_diagnosis(rows, fallback: str) -> None:
    print(
        f"\n{'side':5} {'rung':5} {'answered':>9} {'of GT':>7} "
        f"{'acc':>7} {fallback + ' acc':>8} {'lift':>7}"
    )
    for r in rows:
        print(
            f"{r['side']:5} {r['rung']:5} {r['n_answered']:9,} "
            f"{r['share_of_gt']:7.2%} {r['accuracy']:7.2%} "
            f"{r['fallback_accuracy']:8.2%} {r['lift']:+7.2%}"
        )


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
    ap.add_argument("--ladder", nargs="+", default=list(DEFAULT_LADDER),
                    choices=list(RUNGS),
                    help="cascade order for M6, most precise first")
    ap.add_argument("--diagnose-cascade", action="store_true",
                    help="attribute the cascade's answers to the rung that "
                         "supplied them and report lift over the control")
    ap.add_argument("--sweep-abstain", action="store_true",
                    help="sweep M7's endpoint distance x height grid and write "
                         "the coverage/accuracy curve")
    ap.add_argument("--control", default=None, choices=list(RUNGS),
                    help="rung to measure each rung's lift against on the "
                         "flights it claimed. Defaults to the last rung; set "
                         "it to the strongest single method to test whether a "
                         "rung earns its place at all.")
    ap.add_argument("--ui-port", type=int, default=4041,
                    help="Spark UI port; proxied at /proxy/<port>/. Defaults to "
                         "4041 so it does not collide with a concurrent sampler.")
    ap.add_argument("--cores", type=int, default=6)
    ap.add_argument("--driver-memory", default="9g")
    ap.add_argument("--executors", type=int, default=osn_sample.RESEARCH_EXECUTORS,
                    help="K8s executors to request (ceiling ~12 for the quota)")
    args = ap.parse_args()

    load_dotenv()
    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
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

    if args.sweep_abstain:
        sw = sweep_abstention(spark, sv, apt, ident, gt)
        out = f"{OUT_BASE}/abstain_sweep/{args.airport_set}"
        sw.coalesce(1).write.mode("overwrite").parquet(out)
        print(f"\nabstention sweep -> {out}")

    names = {"all": list(METHODS), "none": []}.get(
        args.methods[0] if len(args.methods) == 1 else "", args.methods
    )
    rows = []
    for name in names:
        print(f"\n--- {name} ---")
        kw = {"ladder": args.ladder} if name == "M6_cascade" else {}
        pred = METHODS[name](sv, apt, radius_nm=args.radius_nm, max_fl=args.max_fl, **kw)
        m = score(pred, ident, gt)
        m["method"] = name
        m["airport_set"] = args.airport_set
        m["radius_nm"] = args.radius_nm
        m["max_fl"] = args.max_fl
        m["ladder"] = "-".join(args.ladder) if name == "M6_cascade" else ""
        rows.append(m)
        print(
            f"  ADEP cov {m['adep_coverage']:6.2%} acc {m['adep_accuracy']:6.2%} "
            f"overall {m['adep_overall']:6.2%}   |   "
            f"ADES cov {m['ades_coverage']:6.2%} acc {m['ades_accuracy']:6.2%} "
            f"overall {m['ades_overall']:6.2%}"
        )

    tag = f"{args.airport_set}_r{int(args.radius_nm)}_fl{int(args.max_fl)}"

    if args.diagnose_cascade:
        control = args.control or args.ladder[-1]
        ladder = list(args.ladder)
        if control not in ladder:
            ladder.append(control)   # needed as a column, not as a rung that fires
        diag = diagnose_cascade(
            sv, apt, ident, gt, ladder, args.radius_nm, args.max_fl, control
        ).cache()
        _print_diagnosis(diag.collect(), control)
        diag.coalesce(1).write.mode("overwrite").parquet(
            f"{OUT_BASE}/cascade_diag/{tag}_{'-'.join(ladder)}_vs_{control}"
        )

    if rows:
        res = spark.createDataFrame(rows)
        res.coalesce(1).write.mode("overwrite").parquet(f"{OUT_BASE}/results/{tag}")
        print(f"\nresults -> {OUT_BASE}/results/{tag}")
    spark.stop()


if __name__ == "__main__":
    main()
