"""Great-circle geometry as Spark column expressions.

Distances in nautical miles and bearings in degrees from true north, from
positions in degrees of latitude and longitude. Shared by the flight list,
the event families and the track rules.
"""

from pyspark.sql import functions as F
from pyspark.sql.functions import atan2, cos, lit, radians, sin

#: Mean Earth radius, in nautical miles.
EARTH_R_NM = 3440.065


def bearing_deg(lat1, lon1, lat2, lon2):
    """Initial great-circle bearing from point 1 to point 2, in degrees."""
    d_lon = radians(lon2 - lon1)
    y = sin(d_lon) * cos(radians(lat2))
    x = cos(radians(lat1)) * sin(radians(lat2)) - sin(radians(lat1)) * cos(radians(lat2)) * cos(
        d_lon
    )
    return (F.degrees(atan2(y, x)) + 360.0) % 360.0


def angle_between(a, b):
    """Smallest absolute angle between two bearings, 0 to 180 degrees.

    Uses ``pmod`` rather than ``%``: Spark's remainder keeps the sign of the
    dividend, so ``(-350) % 360`` is ``-350`` rather than ``10``.
    """
    return F.abs(F.pmod(a - b + 180.0, lit(360.0)) - 180.0)


def haversine_nm(lat1, lon1, lat2, lon2):
    """Great-circle distance in nautical miles between two positions.

    Haversine formula with an Earth radius of 3440.065 NM. A NULL coordinate
    gives NULL. The clamp of the arcsine argument uses ``when``, not
    ``F.least``, because ``least`` skips NULLs and would turn a missing
    position into the antipodal distance (10,807 NM).
    """
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    clamped = F.when(a > lit(1.0), lit(1.0)).otherwise(a)
    return lit(2 * EARTH_R_NM) * F.asin(F.sqrt(clamped))
