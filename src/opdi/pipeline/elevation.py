"""Field-elevation helpers shared by the runway, layout and vertical families.

A leaf module: it imports nothing from ``events.py``, so ``events.py`` (and,
via it, ``runway_ops.py`` and ``layout.py``) can import this without a cycle.
"""

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, to_timestamp

from opdi.utils.datetime_helpers import get_start_end_of_month
from opdi.utils.storage import StorageManager

FT_PER_M = 3.28084


def attach_field_elevation(
    sdf: DataFrame, month: date, storage: "StorageManager"
) -> DataFrame:
    """Attach the field elevation of each track's ADEP and ADES.

    Ground membership has to be measured against the field, not the ellipsoid:
    ``baro_altitude_c`` is uncorrected pressure altitude, so a fixed 200 ft
    cut-off is unreachable at any aerodrome above ~200 ft AMSL, and every
    ``take-off`` and ``landing`` event for those flights simply never existed.

    **Both** ends are attached rather than one chosen per sample. A track is
    only ever on the ground at one of its two aerodromes, and cruise sits far
    above both, so taking the more permissive of the two memberships is correct
    without needing a per-sample distance to decide which end applies -- which
    would mean joining aerodrome coordinates to every state vector.

    Columns are left NULL when the flight list names no aerodrome or
    OurAirports has no elevation for it; the caller coalesces to zero, which is
    exactly today's behaviour.
    """
    if not storage.table_exists("opdi_flight_list"):
        return sdf

    start_ts, end_ts = get_start_end_of_month(month)
    fl = (
        storage.read_table("opdi_flight_list")
        .filter((col("dof") >= to_timestamp(lit(start_ts))) & (col("dof") < to_timestamp(lit(end_ts))))
        .select(col("id").alias("_fl_id"), col("adep"), col("ades"))
    )

    if storage.table_exists("oa_airports"):
        elev = storage.read_table("oa_airports").select(
            col("ident").alias("_ident"),
            col("elevation_ft").cast("double").alias("_elev"),
        )
        fl = (
            fl.join(F.broadcast(elev), fl.adep == col("_ident"), "left")
            .withColumnRenamed("_elev", "elev_adep_ft")
            .drop("_ident")
        )
        fl = (
            fl.join(F.broadcast(elev), fl.ades == col("_ident"), "left")
            .withColumnRenamed("_elev", "elev_ades_ft")
            .drop("_ident")
        )
    else:
        fl = fl.withColumn("elev_adep_ft", lit(None).cast("double")).withColumn(
            "elev_ades_ft", lit(None).cast("double")
        )

    fl = fl.select("_fl_id", "elev_adep_ft", "elev_ades_ft")
    return sdf.join(
        F.broadcast(fl), sdf.track_id == col("_fl_id"), "left"
    ).drop("_fl_id")


def height_above_field_ft(sdf):
    """Height above field elevation, in feet.

    ``baro_altitude_c`` is uncorrected pressure altitude in metres. The
    *smaller* of the two field-relative heights is taken, for the reason
    ``attach_field_elevation`` documents: a track is on the ground at only one
    of its two aerodromes and cruise sits far above both, so the wrong end can
    only ever make the height larger. Taking the smaller is therefore the
    permissive choice for a ground test and needs no per-sample distance to
    decide which end applies -- which would mean joining aerodrome coordinates
    to every state vector.

    NULL elevations coalesce to zero, which reproduces the behaviour of every
    caller that used a bare pressure altitude before this existed.
    """
    alt_ft = F.col("baro_altitude_c") * F.lit(FT_PER_M)
    return F.least(
        alt_ft - F.coalesce(F.col("elev_adep_ft"), F.lit(0.0)),
        alt_ft - F.coalesce(F.col("elev_ades_ft"), F.lit(0.0)),
    )
