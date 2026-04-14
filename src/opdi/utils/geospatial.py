"""
Geospatial utility functions for OPDI pipeline.

Provides functions for distance calculations, coordinate transformations,
and geometry operations used throughout the aviation data pipeline.
"""

import math
from typing import Tuple
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


# Earth radius constants
EARTH_RADIUS_KM = 6371.0
EARTH_RADIUS_NM = 3440.065  # Nautical miles
KM_TO_NM = 1.0 / 1.852
NM_TO_KM = 1.852
NM_TO_METERS = 1852.0
METERS_TO_NM = 1.0 / 1852.0


def haversine_distance(
    lat1: float, lon1: float, lat2: float, lon2: float, unit: str = "nm"
) -> float:
    """
    Calculate the great circle distance between two points on Earth using the Haversine formula.

    Args:
        lat1: Latitude of first point in degrees
        lon1: Longitude of first point in degrees
        lat2: Latitude of second point in degrees
        lon2: Longitude of second point in degrees
        unit: Output unit - "km", "nm" (nautical miles), or "m" (meters)

    Returns:
        Distance between the two points in the specified unit

    Example:
        >>> # Distance from Brussels (EBBR) to Paris (LFPG)
        >>> dist = haversine_distance(50.9014, 4.4844, 49.0097, 2.5478, unit="nm")
        >>> print(f"{dist:.1f} NM")
        138.5 NM
    """
    # Convert degrees to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Haversine formula
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # Calculate distance
    distance_km = EARTH_RADIUS_KM * c

    # Convert to requested unit
    if unit == "km":
        return distance_km
    elif unit == "nm":
        return distance_km * KM_TO_NM
    elif unit == "m":
        return distance_km * 1000.0
    else:
        raise ValueError(f"Unknown unit: {unit}. Use 'km', 'nm', or 'm'.")


def add_cumulative_distance(
    df: DataFrame,
    lat_col: str = "lat",
    lon_col: str = "lon",
    track_id_col: str = "track_id",
    time_col: str = "event_time",
) -> DataFrame:
    """
    Calculate the great circle distance between consecutive points and cumulative distance.

    Uses native PySpark functions for distributed computation of distances along tracks.

    Args:
        df: Input Spark DataFrame
        lat_col: Name of latitude column (default: "lat")
        lon_col: Name of longitude column (default: "lon")
        track_id_col: Name of track ID column for partitioning (default: "track_id")
        time_col: Name of time column for ordering (default: "event_time")

    Returns:
        DataFrame with additional columns:
            - segment_distance_nm: Distance from previous point in nautical miles
            - cumulative_distance_nm: Total distance from track start

    Example:
        >>> df_with_distance = add_cumulative_distance(tracks_df)
        >>> df_with_distance.select("track_id", "cumulative_distance_nm").show()
    """
    # Define window specs
    window_lag = Window.partitionBy(track_id_col).orderBy(time_col)
    window_cumsum = (
        Window.partitionBy(track_id_col)
        .orderBy(time_col)
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    # Convert degrees to radians
    df = df.withColumn("lat_rad", F.radians(F.col(lat_col)))
    df = df.withColumn("lon_rad", F.radians(F.col(lon_col)))

    # Get previous row's latitude and longitude
    df = df.withColumn("prev_lat_rad", F.lag("lat_rad").over(window_lag))
    df = df.withColumn("prev_lon_rad", F.lag("lon_rad").over(window_lag))

    # Haversine formula in PySpark
    df = df.withColumn(
        "a",
        F.sin((F.col("lat_rad") - F.col("prev_lat_rad")) / 2) ** 2
        + F.cos(F.col("prev_lat_rad"))
        * F.cos(F.col("lat_rad"))
        * F.sin((F.col("lon_rad") - F.col("prev_lon_rad")) / 2) ** 2,
    )

    df = df.withColumn("c", 2 * F.atan2(F.sqrt(F.col("a")), F.sqrt(1 - F.col("a"))))

    # Distance in kilometers
    df = df.withColumn("distance_km", EARTH_RADIUS_KM * F.col("c"))

    # Convert to nautical miles
    df = df.withColumn("segment_distance_nm", F.col("distance_km") * KM_TO_NM)

    # Calculate cumulative distance
    df = df.withColumn(
        "cumulative_distance_nm", F.sum("segment_distance_nm").over(window_cumsum)
    )

    # Drop temporary columns
    df = df.drop("lat_rad", "lon_rad", "prev_lat_rad", "prev_lon_rad", "a", "c", "distance_km")

    return df


def calculate_bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the initial bearing (forward azimuth) between two points.

    Args:
        lat1: Latitude of first point in degrees
        lon1: Longitude of first point in degrees
        lat2: Latitude of second point in degrees
        lon2: Longitude of second point in degrees

    Returns:
        Bearing in degrees (0-360), where 0° is North, 90° is East, etc.

    Example:
        >>> bearing = calculate_bearing(50.9014, 4.4844, 49.0097, 2.5478)
        >>> print(f"Bearing: {bearing:.1f}°")
        Bearing: 235.4°
    """
    # Convert to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    dlon = lon2_rad - lon1_rad

    x = math.sin(dlon) * math.cos(lat2_rad)
    y = math.cos(lat1_rad) * math.sin(lat2_rad) - math.sin(lat1_rad) * math.cos(
        lat2_rad
    ) * math.cos(dlon)

    initial_bearing = math.atan2(x, y)

    # Convert to degrees and normalize to 0-360
    initial_bearing = math.degrees(initial_bearing)
    compass_bearing = (initial_bearing + 360) % 360

    return compass_bearing


def destination_point(
    lat: float, lon: float, bearing: float, distance_nm: float
) -> Tuple[float, float]:
    """
    Calculate destination point given start point, bearing, and distance.

    Args:
        lat: Starting latitude in degrees
        lon: Starting longitude in degrees
        bearing: Bearing in degrees (0-360)
        distance_nm: Distance in nautical miles

    Returns:
        Tuple of (destination_latitude, destination_longitude) in degrees

    Example:
        >>> # Point 100 NM north of Brussels
        >>> dest_lat, dest_lon = destination_point(50.9014, 4.4844, 0, 100)
    """
    # Convert to radians
    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)
    bearing_rad = math.radians(bearing)

    # Convert distance to radians (using Earth radius in NM)
    angular_distance = distance_nm / EARTH_RADIUS_NM

    # Calculate destination point
    dest_lat_rad = math.asin(
        math.sin(lat_rad) * math.cos(angular_distance)
        + math.cos(lat_rad) * math.sin(angular_distance) * math.cos(bearing_rad)
    )

    dest_lon_rad = lon_rad + math.atan2(
        math.sin(bearing_rad) * math.sin(angular_distance) * math.cos(lat_rad),
        math.cos(angular_distance) - math.sin(lat_rad) * math.sin(dest_lat_rad),
    )

    # Convert back to degrees
    dest_lat = math.degrees(dest_lat_rad)
    dest_lon = math.degrees(dest_lon_rad)

    # Normalize longitude to -180 to 180
    dest_lon = ((dest_lon + 180) % 360) - 180

    return dest_lat, dest_lon


def meters_to_flight_level(meters: float) -> int:
    """
    Convert altitude in meters to flight level.

    Flight level is altitude in hundreds of feet, so FL100 = 10,000 feet.

    Args:
        meters: Altitude in meters

    Returns:
        Flight level (integer)

    Example:
        >>> meters_to_flight_level(3048)  # 10,000 feet
        100
    """
    feet = meters * 3.28084
    return int(feet / 100)


def flight_level_to_meters(flight_level: int) -> float:
    """
    Convert flight level to altitude in meters.

    Args:
        flight_level: Flight level (e.g., 100 for FL100)

    Returns:
        Altitude in meters

    Example:
        >>> flight_level_to_meters(100)
        3048.0
    """
    feet = flight_level * 100
    return feet / 3.28084
