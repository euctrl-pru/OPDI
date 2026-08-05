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

from pyspark.sql import Column, DataFrame, SparkSession, Window
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
#: H=helicopter, G=gyrocopter, T=tiltrotor are not.
AEROPLANE_CLASSES = ("L", "A")

#: Classes to *exclude*. The filter is a denylist, not an allowlist: only
#: confirmed non-aeroplanes are dropped, and an airframe with no recorded class
#: is kept.
#:
#: The first version had this backwards, keeping only confirmed L and A.
#: `icao_aircraft_class` is populated on just 60.9% of the aircraft database, so
#: that allowlist discarded 47.74% of all airframes to remove rotorcraft, which
#: are 8.64% -- in a study whose headline problem is under-detection. An
#: unclassified airframe is overwhelmingly likely to be an aeroplane; excluding
#: it costs real coverage and buys almost no precision.
NON_AEROPLANE_CLASSES = ("H", "G", "T")

#: ADS-B emitter categories that are not aircraft at all. Not currently
#: available: `osn_aircraft_db` carries no category column and neither does the
#: raw OSN state-vector schema, so surface vehicles cannot be excluded by
#: declaration at all -- only behaviourally. Kept here so that the filter works
#: the day step 00e retains the field.
SURFACE_VEHICLE_PATTERNS = ("Surface Vehicle", "Ground Obstruction", "Point Obstacle")

#: Out-of-area sentinel. A flight whose origin or destination lies outside the
#: ingestion bounding box cannot have that aerodrome named from this data, and
#: saying so is a different -- and correct -- answer from staying silent.
OOA = "OOA"

#: Ingestion bounding box, from ``StateVectorIngestion.DEFAULT_BBOX``.
BBOX = (-25.86653, 26.74617, 49.65699, 70.25976)  # min_lon, min_lat, max_lon, max_lat

#: How close to the bbox edge an endpoint must be to read as "left the area"
#: rather than "stopped being seen". One degree of latitude is 60 NM; the
#: coverage boundary is not sharp, because reception falls off before the
#: nominal edge, so this is deliberately generous.
BORDER_MARGIN_NM = 30.0


def at_border(lat: Column, lon: Column, margin_nm: float = BORDER_MARGIN_NM) -> Column:
    """True where a position sits within *margin_nm* of the ingestion bbox edge.

    Longitude degrees shrink with latitude, so the longitude margin is scaled
    by 1/cos(lat) -- at 60 degrees north a degree of longitude is half a degree
    of latitude on the ground, and an unscaled margin would be twice as strict
    in the north as in the south.
    """
    min_lon, min_lat, max_lon, max_lat = BBOX
    dlat = margin_nm / NM_PER_DEG
    dlon = dlat / F.greatest(F.cos(F.radians(lat)), F.lit(0.1))
    return (
        (lat <= F.lit(min_lat) + dlat)
        | (lat >= F.lit(max_lat) - dlat)
        | (lon <= F.lit(min_lon) + dlon)
        | (lon >= F.lit(max_lon) - dlon)
    )


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
    spark: SparkSession, sv: DataFrame, strict: bool = False
) -> DataFrame:
    """Drop confirmed non-aeroplanes, keeping anything unidentified.

    Two independent signals, both denylists:

    * ``icao_aircraft_class`` (ICAO Doc 8643) -- drop H, G, T.
    * ``category_description`` (ADS-B emitter category) -- drop surface
      vehicles and obstructions. Sparse, but unambiguous where present, and it
      is the only field that names a ground vehicle at all.

    ``strict=True`` restores the original allowlist behaviour (keep only
    confirmed L/A) so the two can be compared rather than argued about.
    """
    cols = spark.read.parquet(AIRCRAFT_DB).columns
    sel = [
        F.lower(F.col("icao24")).alias("_ic"),
        F.substring(F.col("icao_aircraft_class"), 1, 1).alias("ac_class"),
    ]
    # The column was added to step 00e after the first study; tolerate its
    # absence rather than failing on an older reference table.
    has_cat = "category_description" in cols
    if has_cat:
        sel.append(F.col("category_description").alias("ac_category"))
    db = spark.read.parquet(AIRCRAFT_DB).select(*sel)

    out = sv.join(db, F.lower(sv.icao24) == F.col("_ic"), "left").drop("_ic")
    if not has_cat:
        out = out.withColumn("ac_category", F.lit(None).cast("string"))

    if strict:
        return out.filter(F.col("ac_class").isin(list(AEROPLANE_CLASSES)))

    is_rotorcraft = F.col("ac_class").isin(list(NON_AEROPLANE_CLASSES))
    is_surface = F.lit(False)
    for pat in SURFACE_VEHICLE_PATTERNS:
        is_surface = is_surface | F.col("ac_category").contains(pat)
    # coalesce: a NULL class or category must not make the predicate NULL and
    # silently drop the row -- unknown means keep.
    return out.filter(
        ~F.coalesce(is_rotorcraft, F.lit(False)) & ~F.coalesce(is_surface, F.lit(False))
    )


def build_tracks(spark: SparkSession, sv: DataFrame, clean: bool = False) -> DataFrame:
    """Apply the frozen OPDI track-splitting algorithm, optionally then cleaning.

    The cleaning ablation is a real question, not a formality. Stage 3 of
    ``cleaning/native.py`` masks *stale broadcasts* -- consecutive identical
    positions, on the reasoning that ADS-B sends position and velocity in
    separate message types, so a repeated value means repeated rather than
    measured. An aircraft parked at a gate broadcasts exactly that. If the
    stage nulls those samples, it removes the evidence the endpoint methods
    depend on, and cleaning would make ADEP/ADES detection *worse*.
    """
    from opdi.config import OPDIConfig
    from opdi.pipeline.tracks import TrackProcessor

    proc = TrackProcessor(spark, OPDIConfig.for_environment("local"))
    tracks = proc._add_track_id(sv)
    if not clean:
        return tracks

    from opdi.cleaning.native import clean_tracks

    return clean_tracks(tracks, OPDIConfig.for_environment("local").cleaning)


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
        .withColumn("at_border", at_border(F.col("lat"), F.col("lon")))
        .select("track_id", "role", "lat", "lon", "baro_altitude", "on_ground", "at_border")
    )
    ends = ends.withColumn("cell_lat", F.floor("lat").cast("int")).withColumn(
        "cell_lon", F.floor("lon").cast("int")
    )
    # LEFT, not inner: an endpoint over open sea has no aerodrome in its cell
    # neighbourhood, and those are exactly the endpoints that carry the
    # out-of-area signal. An inner join discards them before they can be read.
    j = ends.join(F.broadcast(apt), ["cell_lat", "cell_lon"], "left").withColumn(
        "dist_nm", haversine_nm(F.col("lat"), F.col("lon"), F.col("apt_lat"), F.col("apt_lon"))
    )
    best = Window.partitionBy("track_id", "role").orderBy(F.col("dist_nm").asc_nulls_last())
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
            #
            # elevation_ft is OurAirports, missing on 3.09% of large+medium
            # aerodromes. `elev_known` carries that so the height test can be
            # relaxed rather than silently treating a missing elevation as sea
            # level -- which at a 5,000 ft field is wrong by the whole cut-off.
            F.first(
                F.col("baro_altitude") * 3.28084 - F.coalesce(F.col("apt_elev_ft"), F.lit(0.0))
            ).alias("agl_ft"),
            F.first(F.col("apt_elev_ft").isNotNull()).alias("elev_known"),
            F.first("on_ground").alias("on_ground"),
            F.first("at_border").alias("at_border"),
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
    max_endpoint_dist_nm: float = 20.0,
    max_endpoint_agl_ft: float = 15000.0,
    require_on_ground: bool = False,
    ooa: bool = True,
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
    per_side = {
        "adep": (
            kw.get("adep_dist_nm", max_endpoint_dist_nm),
            kw.get("adep_agl_ft", max_endpoint_agl_ft),
        ),
        "ades": (
            kw.get("ades_dist_nm", max_endpoint_dist_nm),
            kw.get("ades_agl_ft", max_endpoint_agl_ft),
        ),
    }
    for side in ("adep", "ades"):
        d, h = per_side[side]
        ok = F.col(f"{side}_dist_nm") <= d
        if require_on_ground:
            ok = ok & F.col(f"{side}_on_ground")
        else:
            # A missing barometric altitude on a surface sample is normal, so
            # on_ground satisfies the height test outright. Where the aerodrome
            # elevation is unknown, agl_ft is really MSL: fall back to the
            # ground flag rather than compare a height against the wrong datum.
            ok = ok & (
                F.col(f"{side}_on_ground")
                | (F.col(f"{side}_elev_known") & (F.col(f"{side}_agl_ft") <= h))
            )
        if ooa:
            # Order matters: the border test comes first and carries no height
            # condition. A flight leaving the area is at cruise level by
            # definition, so any height cut-off would reject exactly the
            # endpoints this branch exists to catch.
            p = p.withColumn(
                side,
                F.when(F.col(f"{side}_at_border"), F.lit(OOA)).otherwise(
                    F.when(ok, F.col(side))
                ),
            )
        else:
            p = p.withColumn(side, F.when(ok, F.col(side)))
    return p.select("track_id", "adep", "ades")


def m8_extrapolated_endpoint(
    sv: DataFrame,
    apt: DataFrame,
    adep_dist_nm: float = 20.0,
    adep_agl_ft: float = 15000.0,
    ades_dist_nm: float = 20.0,
    ades_agl_ft: float = 15000.0,
    fit_seconds: float = 300.0,
    max_project_nm: float = 60.0,
    ooa: bool = True,
    **kw,
) -> DataFrame:
    """M8 -- project the trajectory to the ground before naming an aerodrome.

    M7's weakness on arrivals is that a track's last sample is usually not a
    landing but a loss of reception, somewhere on the approach. The aircraft's
    intent at that moment is nevertheless visible: it is descending, on a
    heading, at a known rate. Continuing that descent to field elevation gives
    an estimated touchdown point, which is a far better thing to match an
    aerodrome against than the last place a receiver happened to hear it.

    The projection is deliberately crude -- constant ground speed, constant
    track, constant vertical rate over the last *fit_seconds* -- because the
    honest alternative is a full trajectory prediction and the last few minutes
    of an approach are close to straight anyway. It is capped at
    *max_project_nm*: beyond that the aircraft may still turn, and a long
    extrapolation would invent an answer rather than recover one.

    Departures get the mirror treatment, projecting backwards to the ground
    from the first samples of a climb.
    """
    w = Window.partitionBy("track_id").orderBy("event_time")
    wr = Window.partitionBy("track_id").orderBy(F.col("event_time").desc())
    marked = (
        sv.withColumn("_rn", F.row_number().over(w))
        .withColumn("_rr", F.row_number().over(wr))
        .withColumn("_t0", F.min("event_time").over(Window.partitionBy("track_id")))
        .withColumn("_t1", F.max("event_time").over(Window.partitionBy("track_id")))
    )
    dep = marked.filter(
        F.unix_timestamp("event_time") - F.unix_timestamp("_t0") <= fit_seconds
    ).withColumn("role", F.lit("adep"))
    arr = marked.filter(
        F.unix_timestamp("_t1") - F.unix_timestamp("event_time") <= fit_seconds
    ).withColumn("role", F.lit("ades"))

    seg = dep.unionByName(arr)
    # Anchor sample per side: the outermost one, which is what gets projected.
    anchor = (F.col("role") == "adep") & (F.col("_rn") == 1) | (
        (F.col("role") == "ades") & (F.col("_rr") == 1)
    )
    g = seg.groupBy("track_id", "role").agg(
        F.max(F.when(anchor, F.col("lat"))).alias("lat"),
        F.max(F.when(anchor, F.col("lon"))).alias("lon"),
        F.max(F.when(anchor, F.col("baro_altitude"))).alias("alt_m"),
        F.max(F.when(anchor, F.col("heading"))).alias("heading"),
        F.max(F.when(anchor, F.col("on_ground"))).alias("on_ground"),
        F.max(F.when(anchor, F.col("velocity"))).alias("gs_ms"),
        F.avg("vert_rate").alias("vs_ms"),
    )

    # Time to reach the ground at the observed vertical rate. Sign convention:
    # arrivals descend (vs < 0) forwards in time, departures climb (vs > 0)
    # backwards in time -- both give a positive time-to-ground.
    signed_vs = F.when(F.col("role") == "ades", -F.col("vs_ms")).otherwise(F.col("vs_ms"))
    t_ground = F.when(signed_vs > 0.5, F.col("alt_m") / signed_vs)
    reach_nm = F.least(t_ground * F.col("gs_ms") * 0.000539957, F.lit(max_project_nm))

    # Project along the track. Departures project backwards, hence the reversal.
    brg = F.radians(
        F.when(F.col("role") == "adep", F.col("heading") + F.lit(180.0)).otherwise(
            F.col("heading")
        )
    )
    dlat = reach_nm * F.cos(brg) / NM_PER_DEG
    dlon = reach_nm * F.sin(brg) / (
        NM_PER_DEG * F.greatest(F.cos(F.radians(F.col("lat"))), F.lit(0.1))
    )
    g = (
        g.withColumn("proj_nm", F.coalesce(reach_nm, F.lit(0.0)))
        .withColumn("lat", F.col("lat") + F.coalesce(dlat, F.lit(0.0)))
        .withColumn("lon", F.col("lon") + F.coalesce(dlon, F.lit(0.0)))
        .withColumn("at_border", at_border(F.col("lat"), F.col("lon")))
        .withColumn("cell_lat", F.floor("lat").cast("int"))
        .withColumn("cell_lon", F.floor("lon").cast("int"))
    )
    j = g.join(F.broadcast(apt), ["cell_lat", "cell_lon"], "left").withColumn(
        "dist_nm", haversine_nm(F.col("lat"), F.col("lon"), F.col("apt_lat"), F.col("apt_lon"))
    )
    best = Window.partitionBy("track_id", "role").orderBy(F.col("dist_nm").asc_nulls_last())
    j = j.withColumn("_r", F.row_number().over(best)).filter(F.col("_r") == 1)

    p = (
        j.groupBy("track_id")
        .pivot("role", ["adep", "ades"])
        .agg(
            F.first("apt").alias("apt"),
            F.first("dist_nm").alias("dist_nm"),
            F.first("at_border").alias("at_border"),
        )
    )
    for side, d in (("adep", adep_dist_nm), ("ades", ades_dist_nm)):
        p = p.withColumnRenamed(f"{side}_apt", side)
        ok = F.col(f"{side}_dist_nm") <= d
        expr = F.when(ok, F.col(side))
        if ooa:
            expr = F.when(F.col(f"{side}_at_border"), F.lit(OOA)).otherwise(expr)
        p = p.withColumn(side, expr)
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
    "M8_extrapolate": m8_extrapolated_endpoint,
}

#: Abstention grid for M7. Distances in NM from the track endpoint to the
#: nearest aerodrome; heights in ft above that aerodrome's elevation.
#:
#: The first version of this grid stepped from 5,000 ft straight to
#: unrestricted, which turned out to hide the answer for arrivals. At 5,000 ft
#: M7 beats the production algorithm on ADEP but *loses* 2-3 pp of ADES
#: coverage; across that one missing interval ADES accuracy falls from 97.97%
#: to 83.94%, so whether an intermediate cut dominates on arrivals too was
#: simply not measured. Arrivals plausibly need a higher cut than departures:
#: the last sample of a track is more often a loss of reception during descent
#: than a landing, and that happens well above 5,000 ft.
ABSTAIN_DIST_NM = (1.0, 2.0, 3.0, 5.0, 10.0, 20.0, 40.0, 1e9)
ABSTAIN_AGL_FT = (
    0.0, 500.0, 1000.0, 2000.0, 5000.0, 8000.0, 10000.0, 15000.0, 20000.0, 1e9
)


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


def airport_locations(spark: SparkSession) -> DataFrame:
    """Every aerodrome OurAirports knows, for locating ground-truth codes.

    Deliberately unrestricted by type: this is used to decide whether a
    ground-truth ADEP/ADES lies inside the ingestion area, and a flight to a
    small field or a heliport is still a flight to somewhere real.
    """
    return (
        spark.read.parquet(AIRPORTS)
        .select(
            F.col("ident").alias("_apt"),
            F.col("latitude_deg").cast("double").alias("_lat"),
            F.col("longitude_deg").cast("double").alias("_lon"),
        )
        .filter(F.col("_apt").isNotNull() & F.col("_lat").isNotNull())
    )


def _in_area(lat: Column, lon: Column) -> Column:
    min_lon, min_lat, max_lon, max_lat = BBOX
    return lat.between(min_lat, max_lat) & lon.between(min_lon, max_lon)


def label_ground_truth(gt: DataFrame, apt_loc: DataFrame) -> DataFrame:
    """Rewrite out-of-area ground-truth aerodromes to the OOA sentinel.

    A flight from Dubai to Amsterdam has a real ADEP, but not one any European
    ADS-B feed can name -- the aircraft entered the area already airborne. The
    correct answer for such a flight is "out of area", not the ICAO code, and
    scoring it against the code makes every method look worse than it is while
    hiding the flights it genuinely missed.

    An ICAO code absent from OurAirports is also treated as out of area. That
    is the pragmatic reading: the codes that fail to resolve are overwhelmingly
    non-European, and a code we cannot locate is one we could not have matched
    a trajectory endpoint to either.
    """
    for side in ("adep", "ades"):
        gt = (
            gt.join(
                apt_loc.select(
                    F.col("_apt").alias(f"_{side}_apt"),
                    F.col("_lat").alias(f"_{side}_lat"),
                    F.col("_lon").alias(f"_{side}_lon"),
                ),
                gt[f"gt_{side}"] == F.col(f"_{side}_apt"),
                "left",
            )
            .withColumn(
                f"gt_{side}_in_area",
                F.coalesce(
                    _in_area(F.col(f"_{side}_lat"), F.col(f"_{side}_lon")), F.lit(False)
                ),
            )
            .withColumn(
                f"gt_{side}",
                F.when(F.col(f"gt_{side}_in_area"), F.col(f"gt_{side}")).otherwise(
                    F.lit(OOA)
                ),
            )
            .drop(f"_{side}_apt", f"_{side}_lat", f"_{side}_lon")
        )
    return gt


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


def per_airport_counts(pred: DataFrame, ident: DataFrame, gt: DataFrame) -> DataFrame:
    """Departure and arrival counts per aerodrome, method against ground truth.

    A stricter test than flight-level accuracy, and the one an operational user
    actually cares about: whether OPDI reports the right *number* of movements
    at an aerodrome. Flight-level errors can cancel -- a departure misattributed
    from A to B leaves both counts wrong but the total right -- so the two
    measures fail in different ways and both are worth reporting.

    Only meaningful where ground truth is complete, i.e. the ~90 aerodromes
    APDF covers, so aerodromes absent from ground truth are dropped rather than
    reported as infinite error.
    """
    j = align_to_ground_truth(pred, ident, gt)
    frames = []
    for side, role in (("adep", "departures"), ("ades", "arrivals")):
        agg = (
            j.groupBy(F.col(f"gt_{side}").alias("airport"))
            .agg(
                F.count(F.lit(1)).alias("n_truth"),
                F.sum(F.when(F.col(side).isNotNull(), 1).otherwise(0)).alias("n_predicted_here"),
                F.sum(F.when(F.col(side) == F.col(f"gt_{side}"), 1).otherwise(0)).alias("n_correct"),
            )
            .filter(F.col("airport").isNotNull())
        )
        # What the method assigns *to* this aerodrome, including flights that
        # truly belong elsewhere -- the count an operational user would see.
        assigned = (
            j.groupBy(F.col(side).alias("airport"))
            .agg(F.count(F.lit(1)).alias("n_assigned"))
            .filter(F.col("airport").isNotNull())
        )
        frames.append(
            agg.join(assigned, "airport", "left")
            .withColumn("n_assigned", F.coalesce("n_assigned", F.lit(0)))
            .withColumn("role", F.lit(role))
            .withColumn("recall", F.col("n_correct") / F.col("n_truth"))
            .withColumn("count_ratio", F.col("n_assigned") / F.col("n_truth"))
        )
    return frames[0].unionByName(frames[1]).orderBy(F.col("n_truth").desc())


def error_pairs(
    pred: DataFrame, ident: DataFrame, gt: DataFrame, apt: DataFrame, top: int = 40
) -> DataFrame:
    """Where a method is wrong, what did it say instead, and how far away is it?

    Accuracy near 100% is only reassuring if the residual is scattered. A
    concentrated residual -- the same wrong aerodrome, over and over -- is a
    systematic confusion with a physical cause, usually a second aerodrome
    close enough to the first that the nearest-endpoint rule cannot separate
    them. Those are fixable; scattered errors mostly are not.

    Reports each (truth, predicted) pair with the great-circle distance between
    the two aerodromes, which is the diagnostic: a few miles means a
    neighbouring-field confusion, hundreds means something else entirely.
    """
    j = align_to_ground_truth(pred, ident, gt)
    loc = (
        apt.select("apt", "apt_lat", "apt_lon").dropDuplicates(["apt"])
    )
    frames = []
    for side in ("adep", "ades"):
        e = j.filter(
            F.col(side).isNotNull()
            & F.col(f"gt_{side}").isNotNull()
            & (F.col(side) != F.col(f"gt_{side}"))
        ).select(
            F.col(f"gt_{side}").alias("truth"),
            F.col(side).alias("predicted"),
        )
        agg = e.groupBy("truth", "predicted").agg(F.count(F.lit(1)).alias("n"))
        agg = (
            agg.join(
                loc.select(
                    F.col("apt").alias("truth"),
                    F.col("apt_lat").alias("t_lat"),
                    F.col("apt_lon").alias("t_lon"),
                ),
                "truth",
                "left",
            )
            .join(
                loc.select(
                    F.col("apt").alias("predicted"),
                    F.col("apt_lat").alias("p_lat"),
                    F.col("apt_lon").alias("p_lon"),
                ),
                "predicted",
                "left",
            )
            .withColumn(
                "apart_nm",
                haversine_nm(F.col("t_lat"), F.col("t_lon"), F.col("p_lat"), F.col("p_lon")),
            )
            .withColumn("side", F.lit(side))
            .select("side", "truth", "predicted", "n", "apart_nm")
        )
        frames.append(agg)
    out = frames[0].unionByName(frames[1])
    return out.orderBy(F.col("n").desc()).limit(top)


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
    """Build tracks once, persist to S3, read back. Idempotent per day.

    Cleaned and raw tracks are separate prefixes, so the ablation is a matter
    of pointing at a different one rather than of rebuilding.
    """
    keep = "strict" if getattr(args, "strict_aircraft", False) else "nonrotor"
    if getattr(args, "clean", False):
        keep += "_clean"
    out = f"{TRACKS_BASE}/aircraft={keep}"
    todo = []
    for d in args.days:
        p = f"{out}/day={d}"
        if _s3_exists(spark, p + "/_SUCCESS"):
            print(f"tracks {d}: already in S3")
        else:
            todo.append(d)
    for d in todo:
        print(f"tracks {d}: building{' + cleaning' if args.clean else ''} ...")
        sv = load_state_vectors(spark, [d])
        sv = filter_to_aeroplanes(spark, sv, strict=getattr(args, "strict_aircraft", False))
        build_tracks(spark, sv, clean=args.clean).write.mode("overwrite").parquet(
            f"{out}/day={d}"
        )
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
    ap.add_argument("--strict-aircraft", action="store_true",
                    help="keep only confirmed L/A airframes (the v1 allowlist) "
                         "instead of dropping only confirmed non-aeroplanes")
    ap.add_argument("--no-ooa", action="store_true",
                    help="score out-of-area aerodromes as misses, as v1 did, "
                         "instead of labelling them OOA on both sides")
    ap.add_argument("--clean", action="store_true",
                    help="apply the step-02a cleaning stages to the tracks "
                         "before detection (ablation; writes a separate prefix)")
    ap.add_argument("--ladder", nargs="+", default=list(DEFAULT_LADDER),
                    choices=list(RUNGS),
                    help="cascade order for M6, most precise first")
    ap.add_argument("--diagnose-cascade", action="store_true",
                    help="attribute the cascade's answers to the rung that "
                         "supplied them and report lift over the control")
    ap.add_argument("--error-pairs", default=None, choices=list(METHODS),
                    help="report the (truth, predicted) confusions this method "
                         "makes, with the distance between the two aerodromes")
    ap.add_argument("--per-airport", default=None, choices=list(METHODS),
                    help="write per-aerodrome departure/arrival counts for this "
                         "method against APDF")
    ap.add_argument("--sweep-abstain", action="store_true",
                    help="sweep M7's endpoint distance x height grid and write "
                         "the coverage/accuracy curve")
    ap.add_argument("--control", default=None, choices=list(RUNGS),
                    help="rung to measure each rung's lift against on the "
                         "flights it claimed. Defaults to the last rung; set "
                         "it to the strongest single method to test whether a "
                         "rung earns its place at all.")
    ap.add_argument("--ui-port", type=int, default=4041,
                    help="Spark UI port; proxied at /proxy/<port>/. Set it if a "
                         "stale UI still holds 4040/4041, since Spark falls back "
                         "silently and the proxy path is fixed. It does NOT allow "
                         "a concurrent job -- see DATASETS.md.")
    ap.add_argument("--cores", type=int, default=6)
    ap.add_argument("--driver-memory", default="9g")
    ap.add_argument("--executors", type=int, default=osn_sample.RESEARCH_EXECUTORS,
                    help="K8s executors to request (ceiling ~12 for the quota)")
    args = ap.parse_args()

    # Line-buffer stdout: redirected to a log, the per-method result lines
    # would otherwise not appear until the job ends.
    sys.stdout.reconfigure(line_buffering=True)

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
    if not args.no_ooa:
        gt = label_ground_truth(gt, airport_locations(spark)).cache()
        n_ooa = gt.filter(
            (F.col("gt_adep") == OOA) | (F.col("gt_ades") == OOA)
        ).count()
        print(f"ground-truth flights: {gt.count():,} "
              f"({n_ooa:,} with an out-of-area ADEP or ADES)")
    else:
        print(f"ground-truth flights: {gt.count():,}")

    # The sample is part of the tag. A 3-day run must not silently overwrite the
    # 1-day result it is meant to be compared against, and neither must the
    # cleaning ablation overwrite the raw run.
    tag = "_".join(
        [
            f"{len(args.days)}d-{min(args.days)}",
            args.airport_set,
            f"r{int(args.radius_nm)}",
            f"fl{int(args.max_fl)}",
        ]
        + (["clean"] if args.clean else [])
    )

    if args.sweep_abstain:
        sw = (
            sweep_abstention(spark, sv, apt, ident, gt)
            .withColumn("days", F.lit(",".join(args.days)))
            .withColumn("cleaned", F.lit(bool(args.clean)))
        )
        out = f"{OUT_BASE}/abstain_sweep/{tag}"
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
        m["days"] = ",".join(args.days)
        m["cleaned"] = bool(args.clean)
        rows.append(m)
        print(
            f"  ADEP cov {m['adep_coverage']:6.2%} acc {m['adep_accuracy']:6.2%} "
            f"overall {m['adep_overall']:6.2%}   |   "
            f"ADES cov {m['ades_coverage']:6.2%} acc {m['ades_accuracy']:6.2%} "
            f"overall {m['ades_overall']:6.2%}"
        )

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

    if args.error_pairs:
        kw = {"ladder": args.ladder} if args.error_pairs == "M6_cascade" else {}
        pred = METHODS[args.error_pairs](
            sv, apt, radius_nm=args.radius_nm, max_fl=args.max_fl, **kw
        )
        ep = error_pairs(pred, ident, gt, apt).cache()
        print(f"\ntop confusions, {args.error_pairs}:")
        print(f"  {'side':5} {'truth':6} {'predicted':10} {'n':>6} {'apart':>9}")
        for r in ep.collect():
            apart = "—" if r["apart_nm"] is None else f"{r['apart_nm']:8.1f} NM"
            print(f"  {r['side']:5} {r['truth']:6} {r['predicted']:10} {r['n']:6,} {apart:>9}")
        out = f"{OUT_BASE}/error_pairs/{tag}_{args.error_pairs}"
        ep.coalesce(1).write.mode("overwrite").parquet(out)
        print(f"error pairs -> {out}")

    if args.per_airport:
        kw = {"ladder": args.ladder} if args.per_airport == "M6_cascade" else {}
        pred = METHODS[args.per_airport](
            sv, apt, radius_nm=args.radius_nm, max_fl=args.max_fl, **kw
        )
        pa = per_airport_counts(pred, ident, gt).cache()
        print(f"\nper-aerodrome counts, {args.per_airport} (top 15 by movements):")
        for r in pa.limit(15).collect():
            print(
                f"  {r['airport']:6} {r['role']:10} truth {r['n_truth']:6,}  "
                f"assigned {r['n_assigned']:6,}  ratio {r['count_ratio']:6.2f}  "
                f"recall {r['recall']:6.2%}"
            )
        out = f"{OUT_BASE}/per_airport/{tag}_{args.per_airport}"
        pa.coalesce(1).write.mode("overwrite").parquet(out)
        print(f"per-aerodrome counts -> {out}")

    if rows:
        res = spark.createDataFrame(rows)
        res.coalesce(1).write.mode("overwrite").parquet(f"{OUT_BASE}/results/{tag}")
        print(f"\nresults -> {OUT_BASE}/results/{tag}")
    spark.stop()


if __name__ == "__main__":
    main()
