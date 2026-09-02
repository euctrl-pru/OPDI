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
    """Height above field elevation, in feet -- the *permissive* reading.

    ``baro_altitude_c`` is uncorrected pressure altitude in metres. The
    *smaller* of the two field-relative heights is taken, for the reason
    ``attach_field_elevation`` documents: a track is on the ground at only one
    of its two aerodromes and cruise sits far above both, so the wrong end can
    only ever make the height larger. Taking the smaller is therefore the
    permissive choice for a ground test and needs no per-sample distance to
    decide which end applies -- which would mean joining aerodrome coordinates
    to every state vector.

    .. warning::

       **Only use this for a membership test that is allowed to be
       permissive** -- "is this sample near enough to the ground to be matched
       against an airport layout", "is this sample GND for the phase
       classifier". It is *wrong* for anything that compares the height
       against a threshold and reports the answer, because the two aerodromes
       of one flight rarely share an elevation: measured against the higher
       field, a movement at the lower one has a negative height everywhere and
       a 15 ft threshold is never reached, while a movement at the higher one
       reaches it only ``delta_elevation`` feet late. The first failure is
       silent -- the detector abstains and publishes nothing -- and the second
       is a systematic bias in one direction.

       Threshold logic must name the aerodrome it is talking about and use
       :func:`height_above_aerodrome_ft` (or, where the aerodrome is fixed by
       the detector's own definition, :func:`height_above_elevation_ft`).

    NULL elevations coalesce to zero, which reproduces the behaviour of every
    caller that used a bare pressure altitude before this existed.
    """
    alt_ft = F.col("baro_altitude_c") * F.lit(FT_PER_M)
    return F.least(
        alt_ft - F.coalesce(F.col("elev_adep_ft"), F.lit(0.0)),
        alt_ft - F.coalesce(F.col("elev_ades_ft"), F.lit(0.0)),
    )


def height_above_elevation_ft(elevation):
    """Height of ``baro_altitude_c`` above a *stated* field elevation, in feet.

    ``elevation`` is a column: the caller has already decided which aerodrome
    the height is measured against. A NULL elevation coalesces to zero, i.e.
    back to a bare pressure altitude, rather than nulling the height and
    removing the flight from the detector.
    """
    return F.col("baro_altitude_c") * F.lit(FT_PER_M) - F.coalesce(
        elevation, F.lit(0.0)
    )


def field_elevation_ft(sdf, apt_ident):
    """The elevation of the aerodrome ``apt_ident`` names, for this track.

    The flight list gives each track exactly two aerodromes, so the lookup is a
    two-armed ``when`` over columns already on the frame -- no third join, and
    no per-sample distance to guess with.

    ``apt_ident`` may name neither: the layout join also admits the *proximity*
    aerodromes ``adep_p``/``ades_p``, for which no elevation was attached. The
    fallback is then the higher of the two known fields, which is exactly the
    elevation :func:`height_above_field_ft` implies -- the permissive reading,
    kept as the fallback because an unknown field is precisely the case where
    nothing better can be said.

    Returns the permissive fallback for the whole frame when ``adep``/``ades``
    are absent, so a caller that never joined the flight list keeps working.
    """
    fallback = F.greatest(
        F.coalesce(F.col("elev_adep_ft"), F.lit(0.0)),
        F.coalesce(F.col("elev_ades_ft"), F.lit(0.0)),
    ) if "elev_adep_ft" in sdf.columns and "elev_ades_ft" in sdf.columns else F.lit(0.0)

    if "adep" not in sdf.columns or "ades" not in sdf.columns:
        return fallback
    return (
        F.when(apt_ident == F.col("adep"), F.coalesce(F.col("elev_adep_ft"), fallback))
        .when(apt_ident == F.col("ades"), F.coalesce(F.col("elev_ades_ft"), fallback))
        .otherwise(fallback)
    )


def height_above_aerodrome_ft(sdf, apt_ident):
    """Height above the field elevation of the aerodrome ``apt_ident`` names.

    The reference every runway threshold is measured against: a departure from
    a 24 ft field and an arrival at a 1,416 ft one are both measured against
    their own ground, so ``runway_airborne_height_ft`` means 15 ft AGL at both
    rather than 15 ft above whichever of the two happens to be higher.
    """
    return height_above_elevation_ft(field_elevation_ft(sdf, apt_ident))
