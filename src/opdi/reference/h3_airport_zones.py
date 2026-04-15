"""
H3 airport detection zone generator.

Creates concentric hexagonal rings around airports for flight detection.
Each airport gets rings at configurable radii (default: 0-40 NM) encoded
as H3 hexagons at resolution 7.

Ported from: OPDI-live/python/v2.0.0/00_create_h3_airport_detection_areas.py
"""

import json
import math
import os
from typing import List, Optional

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col, lit, array_except, explode
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    ArrayType,
)
import h3
import h3_pyspark

from opdi.config import OPDIConfig


def generate_circle_polygon(
    lon: float, lat: float, radius_nautical_miles: float, num_points: int = 360
) -> str:
    """
    Generate a GeoJSON polygon approximating a circle around a point.

    Uses the destination-point formula to compute points along a circle
    at a given radius from a center coordinate.

    Args:
        lon: Center longitude in decimal degrees.
        lat: Center latitude in decimal degrees.
        radius_nautical_miles: Circle radius in nautical miles.
        num_points: Number of polygon vertices (higher = smoother circle).

    Returns:
        GeoJSON Polygon string.
    """
    radius_km = radius_nautical_miles * 1.852

    def degrees_to_radians(degrees):
        return degrees * math.pi / 180

    def calculate_point(lon, lat, distance_km, bearing):
        R = 6371.01  # Earth's radius in km
        lat_rad = degrees_to_radians(lat)
        lon_rad = degrees_to_radians(lon)
        distance_rad = distance_km / R
        bearing_rad = degrees_to_radians(bearing)

        lat_new_rad = math.asin(
            math.sin(lat_rad) * math.cos(distance_rad)
            + math.cos(lat_rad) * math.sin(distance_rad) * math.cos(bearing_rad)
        )
        lon_new_rad = lon_rad + math.atan2(
            math.sin(bearing_rad) * math.sin(distance_rad) * math.cos(lat_rad),
            math.cos(distance_rad) - math.sin(lat_rad) * math.sin(lat_new_rad),
        )

        return [math.degrees(lon_new_rad), math.degrees(lat_new_rad)]

    points = [
        calculate_point(lon, lat, radius_km, (360 / num_points) * i)
        for i in range(num_points)
    ]
    points.append(points[0])  # Close the polygon

    return json.dumps({"type": "Polygon", "coordinates": [points]})


class AirportDetectionZoneGenerator:
    """
    Generates H3 hexagonal detection zones around airports.

    Creates concentric rings around each airport at configurable radii,
    encoded as H3 hexagons. These zones are used by the flight list
    pipeline to detect departures and arrivals.

    The default configuration creates 6 concentric rings at 0-5, 5-10,
    10-20, 20-30, and 30-40 NM from the airport reference point.

    Args:
        spark: Active SparkSession.
        config: OPDI configuration object.
        resolution: H3 resolution for hexagons (default: from config).
        num_points: Number of points for circle polygon approximation.
        radii_nm: List of ring boundary radii in nautical miles.

    Example:
        >>> generator = AirportDetectionZoneGenerator(spark, config)
        >>> df = generator.generate()
        >>> generator.save_to_parquet("data/airport_hex/zones_res7.parquet")
    """

    # European bounding box with offset for edge airports
    BBOX_OFFSET = 3  # degrees
    LAT_MIN = 26.74617
    LAT_MAX = 70.25976
    LON_MIN = -25.86653
    LON_MAX = 49.65699

    AIRPORT_SCHEMA = StructType([
        StructField("id", StringType(), True),
        StructField("ident", StringType(), True),
        StructField("type", StringType(), True),
        StructField("name", StringType(), True),
        StructField("latitude_deg", FloatType(), True),
        StructField("longitude_deg", FloatType(), True),
        StructField("elevation_ft", FloatType(), True),
        StructField("continent", StringType(), True),
        StructField("iso_country", StringType(), True),
        StructField("iso_region", StringType(), True),
        StructField("municipality", StringType(), True),
        StructField("scheduled_service", StringType(), True),
        StructField("gps_code", StringType(), True),
        StructField("iata_code", StringType(), True),
        StructField("local_code", StringType(), True),
        StructField("home_link", StringType(), True),
        StructField("wikipedia_link", StringType(), True),
        StructField("keywords", StringType(), True),
    ])

    def __init__(
        self,
        spark: SparkSession,
        config: OPDIConfig,
        resolution: Optional[int] = None,
        num_points: int = 720,
        radii_nm: Optional[List[float]] = None,
    ):
        self.spark = spark
        self.config = config
        self.resolution = resolution or config.h3.airport_detection_resolution
        self.num_points = num_points
        self.radii_nm = radii_nm or [0, 5, 10, 20, 30, 40]
        self._result_df: Optional[pd.DataFrame] = None

        # Register UDF
        self._circle_udf = udf(generate_circle_polygon, StringType())

    def _load_airports(
        self,
        airports_url: str = "https://davidmegginson.github.io/ourairports-data/airports.csv",
    ) -> DataFrame:
        """
        Load airport data from OurAirports and filter to European bounding box.

        Args:
            airports_url: URL to OurAirports airports CSV.

        Returns:
            Spark DataFrame with airport data filtered to Europe.
        """
        df_apt = pd.read_csv(airports_url)

        # European bounding box filter
        offset = self.BBOX_OFFSET
        f_lat = df_apt.latitude_deg.between(
            self.LAT_MIN - offset, self.LAT_MAX + offset
        )
        f_lon = df_apt.longitude_deg.between(
            self.LON_MIN - offset, self.LON_MAX + offset
        )
        df_apt = df_apt[f_lat & f_lon]

        # Ensure column types
        df_apt.columns = df_apt.columns.astype(str)
        df_apt = df_apt.astype({
            "latitude_deg": "float64",
            "longitude_deg": "float64",
            "elevation_ft": "float64",
        })

        return self.spark.createDataFrame(df_apt, schema=self.AIRPORT_SCHEMA)

    def _build_ring_config(self) -> DataFrame:
        """
        Build Spark DataFrame defining concentric ring boundaries.

        Returns:
            Spark DataFrame with ring configuration (area_type, min/max radii).
        """
        area_type = [f"C{x + 10}" for x in self.radii_nm]

        df = pd.DataFrame({
            "max_resolution": self.resolution,
            "number_of_points_c": self.num_points,
            "area_type": area_type,
            "min_c_radius_nm": self.radii_nm,
        })

        df["max_c_radius_nm"] = (
            df["min_c_radius_nm"].shift(-1).fillna(np.max(self.radii_nm) + 10)
        )
        df[["min_c_radius_nm", "max_c_radius_nm"]] = df[
            ["min_c_radius_nm", "max_c_radius_nm"]
        ].astype(float)
        df["m_col"] = 1

        schema = StructType([
            StructField("max_resolution", IntegerType(), True),
            StructField("number_of_points_c", IntegerType(), True),
            StructField("area_type", StringType(), True),
            StructField("min_c_radius_nm", FloatType(), True),
            StructField("max_c_radius_nm", FloatType(), True),
            StructField("m_col", IntegerType(), True),
        ])

        return self.spark.createDataFrame(df, schema=schema)

    def generate(
        self,
        airports_url: str = "https://davidmegginson.github.io/ourairports-data/airports.csv",
    ) -> pd.DataFrame:
        """
        Generate H3 detection zones for all airports in the European bounding box.

        Each airport-ring combination produces a set of H3 hex IDs formed by
        subtracting the inner circle hexagons from the outer circle hexagons,
        creating a ring-shaped detection zone.

        Args:
            airports_url: URL to OurAirports airports CSV.

        Returns:
            Pandas DataFrame with airport detection zone hex IDs.
            Columns include all airport metadata plus area_type and hex_id.
        """
        print("Loading airports...")
        airports_df = self._load_airports(airports_url)

        print("Building ring configuration...")
        ring_config = self._build_ring_config()

        # Cross join airports with ring configuration
        airports_m = airports_df.withColumn("m_col", lit(1)).join(
            ring_config, on="m_col", how="left"
        )

        print(f"Generating H3 zones at resolution {self.resolution}...")
        result = (
            airports_m.withColumn(
                "inner_circle_polygon",
                self._circle_udf(
                    col("longitude_deg"),
                    col("latitude_deg"),
                    col("min_c_radius_nm"),
                    col("number_of_points_c"),
                ),
            )
            .withColumn(
                "outer_circle_polygon",
                self._circle_udf(
                    col("longitude_deg"),
                    col("latitude_deg"),
                    col("max_c_radius_nm"),
                    col("number_of_points_c"),
                ),
            )
            .withColumn(
                "inner_circle_hex_ids",
                h3_pyspark.polyfill(
                    col("inner_circle_polygon"), col("max_resolution"), lit(True)
                ),
            )
            .withColumn(
                "outer_circle_hex_ids",
                h3_pyspark.polyfill(
                    col("outer_circle_polygon"), col("max_resolution"), lit(True)
                ),
            )
            .withColumn(
                "hex_id",
                array_except(
                    col("outer_circle_hex_ids"), col("inner_circle_hex_ids")
                ),
            )
            .drop(
                "inner_circle_polygon",
                "outer_circle_polygon",
                "inner_circle_hex_ids",
                "outer_circle_hex_ids",
            )
            .toPandas()
        )

        self._result_df = result
        print(f"Generated {len(result)} airport-ring combinations.")
        return result

    def save_to_parquet(self, output_path: str) -> None:
        """
        Save generated detection zones to a parquet file.

        Args:
            output_path: Path to output parquet file.

        Raises:
            RuntimeError: If generate() has not been called first.
        """
        if self._result_df is None:
            raise RuntimeError("Call generate() before saving.")

        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        self._result_df.to_parquet(output_path)
        print(f"Saved detection zones to {output_path}")

    def prepare_for_flight_list(
        self,
        max_radius_nm: float = 30.0,
        airport_types: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Prepare detection zone data for use by the flight list pipeline.

        Filters zones to the specified maximum radius and airport types,
        explodes hex arrays, adds H3 coordinates, and computes distance
        from airport center.

        Args:
            max_radius_nm: Maximum detection radius in nautical miles.
            airport_types: List of airport types to include
                (default: large, medium, small).

        Returns:
            Pandas DataFrame ready for use in flight list generation.
            Columns: apt_ident, apt_hex_id, distance_from_center,
            apt_latitude_deg, apt_longitude_deg.

        Raises:
            RuntimeError: If generate() has not been called first.
        """
        if self._result_df is None:
            raise RuntimeError("Call generate() before preparing for flight list.")

        if airport_types is None:
            airport_types = ["large_airport", "medium_airport", "small_airport"]

        df = self._result_df.copy()

        # Filter by airport type and radius
        df = df[df["type"].isin(airport_types)]
        df = df[df["max_c_radius_nm"] <= max_radius_nm]

        # Explode hex arrays and remove nulls
        df = df[["ident", "hex_id", "latitude_deg", "longitude_deg"]].explode("hex_id")
        df = df[~df.hex_id.isna()]

        # Get H3 coordinates for each hex
        df["geo"] = df["hex_id"].apply(lambda h: h3.h3_to_geo(h))
        df["lat"] = df["geo"].apply(lambda g: g[0])
        df["lon"] = df["geo"].apply(lambda g: g[1])
        df = df.drop("geo", axis=1)

        # European bounding box filter
        f_lat = np.logical_and(df.lat >= self.LAT_MIN, df.lat <= self.LAT_MAX)
        f_lon = np.logical_and(df.lon >= self.LON_MIN, df.lon <= self.LON_MAX)
        df = df[np.logical_and(f_lat, f_lon)]

        # Add center hex ID for each airport
        df["center_hex_id"] = df.apply(
            lambda row: h3.geo_to_h3(
                row["latitude_deg"], row["longitude_deg"], resolution=self.resolution
            ),
            axis=1,
        )

        # Prefix column names
        df.columns = ["apt_" + x for x in df.columns]

        # Calculate H3 grid distance from center
        def calc_dist(h1, h2):
            try:
                return h3.h3_distance(h1, h2)
            except Exception:
                return None

        df["distance_from_center"] = df.apply(
            lambda row: calc_dist(row["apt_hex_id"], row["apt_center_hex_id"]),
            axis=1,
        )
        df = df[~pd.isnull(df["distance_from_center"])]

        # Select final columns
        df = df[
            [
                "apt_ident",
                "apt_hex_id",
                "distance_from_center",
                "apt_latitude_deg",
                "apt_longitude_deg",
            ]
        ]

        return df
