"""Runway identification and touchdown/lift-off times (T08 ATOT, T17 ALDT).

A native port of ``traffic``'s ``TrackBasedRunwayDetection``
(``traffic/src/traffic/algorithms/navigation/takeoff.py:148-341``), which is a
filter, a median and a broadcast join -- no geometry library, no OpenAP, and no
runtime download. Its polygon-based sibling is deliberately *not* ported: that
one needs OpenAP for its phase call and shapely for a trapeze, and recurses into
a second alignment pass.

Runway geometry comes from ``oa_runways``, which step 00d has been generating
and nothing has ever read. It is the same OurAirports table ``traffic``
downloads at runtime, so this adds no dependency and nothing to warm on an
offline executor.

Bearings are computed from the two threshold positions rather than taken from
``le_heading_degT``/``he_heading_degT``. Those columns are frequently null in
OurAirports and, where present, are sometimes magnetic rather than true; the
positions are the thing OurAirports is reliable about, and ``traffic`` derives
its own bearings the same way for the same reason.
"""

from typing import TYPE_CHECKING, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

if TYPE_CHECKING:  # pragma: no cover - import cycle guard
    from opdi.config import EventConfig

from opdi.pipeline.flights import angle_between, bearing_deg, haversine_nm

FT_PER_M = 3.28084
FTMIN_PER_MPS = 196.850394
KT_PER_MPS = 1.94384


def runway_thresholds(storage) -> Optional[DataFrame]:
    """One row per runway *threshold*: the end an aircraft rolls from.

    A physical runway appears twice, once per direction, because that is the
    unit a movement is reported against -- APDF's ``AP_C_RWY`` names a
    direction, not a strip.
    """
    if not storage.table_exists("oa_runways"):
        return None

    rwy = storage.read_table("oa_runways").filter(
        (F.col("closed").isNull() | (F.col("closed") == False))  # noqa: E712
        & F.col("le_latitude_deg").isNotNull()
        & F.col("he_latitude_deg").isNotNull()
    )

    le = rwy.select(
        F.col("airport_ident").alias("apt_ident"),
        F.col("le_ident").alias("rwy_ident"),
        F.col("le_latitude_deg").cast("double").alias("thr_lat"),
        F.col("le_longitude_deg").cast("double").alias("thr_lon"),
        bearing_deg(
            F.col("le_latitude_deg"), F.col("le_longitude_deg"),
            F.col("he_latitude_deg"), F.col("he_longitude_deg"),
        ).alias("rwy_bearing"),
    )
    he = rwy.select(
        F.col("airport_ident").alias("apt_ident"),
        F.col("he_ident").alias("rwy_ident"),
        F.col("he_latitude_deg").cast("double").alias("thr_lat"),
        F.col("he_longitude_deg").cast("double").alias("thr_lon"),
        bearing_deg(
            F.col("he_latitude_deg"), F.col("he_longitude_deg"),
            F.col("le_latitude_deg"), F.col("le_longitude_deg"),
        ).alias("rwy_bearing"),
    )
    return le.unionByName(he).filter(F.col("rwy_ident").isNotNull())


def detect_runway_movements(
    sdf: DataFrame,
    ends: DataFrame,
    thresholds: DataFrame,
    config: "EventConfig",
) -> DataFrame:
    """Name the runway and time the movement, per (track, role).

    ``ends`` is one row per (track_id, apt_ident, role) with the aerodrome
    position and elevation; ``role`` is ``"departure"`` or ``"arrival"``.

    The gates are traffic's: within ``runway_max_dist_nm`` of the aerodrome,
    below ``runway_max_height_ft`` above the field, groundspeed above
    ``runway_min_groundspeed_kt``, and a vertical rate beyond
    ``runway_min_vert_rate_ftmin`` in the direction the role implies. What
    survives is the initial climb or the final descent; its median track is
    then matched against each runway's bearing.

    Timing follows from the same set rather than a second pass: the earliest
    surviving sample of a departure is the lift-off, the latest of an arrival
    is the touchdown. Both are proxies, and the benchmark's job is to report
    their bias against APDF rather than to assume it is zero.
    """
    work = sdf.join(F.broadcast(ends), on="track_id", how="inner")

    height_ft = F.col("baro_altitude_c") * FT_PER_M - F.coalesce(
        F.col("apt_elevation_ft"), F.lit(0.0)
    )
    roc = F.col("vert_rate") * FTMIN_PER_MPS
    gs_kt = F.col("velocity") * KT_PER_MPS
    dist_nm = haversine_nm(
        F.col("lat"), F.col("lon"), F.col("apt_lat"), F.col("apt_lon")
    )

    climbing = roc > F.lit(config.runway_min_vert_rate_ftmin)
    descending = roc < -F.lit(config.runway_min_vert_rate_ftmin)

    work = work.filter(
        (dist_nm < F.lit(config.runway_max_dist_nm))
        & (height_ft < F.lit(config.runway_max_height_ft))
        & (gs_kt > F.lit(config.runway_min_groundspeed_kt))
        & F.when(F.col("role") == "departure", climbing).otherwise(descending)
    )

    # traffic takes the median track of the surviving samples. The median, not
    # the mean: a single spurious heading during rotation would drag a mean off
    # the centreline, and the runway is chosen by nearest bearing.
    # The aerodrome position is carried through the grouping because the
    # parallel-runway tie-break needs it downstream; it is constant per
    # apt_ident, so grouping on it adds no rows.
    agg = work.groupBy("track_id", "apt_ident", "role", "apt_lat", "apt_lon").agg(
        F.expr("percentile_approx(heading, 0.5)").alias("median_track"),
        F.min("event_time").alias("first_time"),
        F.max("event_time").alias("last_time"),
        F.count(F.lit(1)).alias("n_samples"),
    )
    # Parenthesised deliberately: `&` binds tighter than `>=` in Python, so
    # without them this reads as `n_samples >= (4 & isNotNull)` -- which is a
    # type error here, but would silently be a different filter if the operands
    # happened to be compatible.
    agg = agg.filter((F.col("n_samples") >= F.lit(4)) & F.col("median_track").isNotNull())

    cand = agg.join(F.broadcast(thresholds), on="apt_ident", how="inner")
    cand = cand.withColumn(
        "bearing_error", angle_between(F.col("median_track"), F.col("rwy_bearing"))
    ).filter(F.col("bearing_error") <= F.lit(config.runway_max_bearing_deg))

    # Nearest bearing wins. Parallel runways share one to within a degree, so
    # cross-track distance to each centreline breaks the tie -- the same
    # discriminator traffic uses shapely for, in closed form.
    cand = cand.withColumn(
        "thr_dist_nm",
        haversine_nm(F.col("thr_lat"), F.col("thr_lon"), F.col("apt_lat"), F.col("apt_lon")),
    )
    from pyspark.sql.window import Window

    best = Window.partitionBy("track_id", "role").orderBy(
        F.col("bearing_error").asc(), F.col("thr_dist_nm").asc(), F.col("rwy_ident").asc()
    )
    return (
        cand.withColumn("_r", F.row_number().over(best))
        .filter(F.col("_r") == 1)
        .drop("_r")
    )
