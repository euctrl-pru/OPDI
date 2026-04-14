"""
Transform: Build concentric H3 hex rings around airports for detection areas.

Creates hexagonal grids at H3 resolution 7 in concentric rings (0-10 NM,
10-20 NM, etc.) around each airport's reference point.  These are used
downstream to associate state-vector positions with nearby airports.
"""

from __future__ import annotations

import json
import math
from typing import Optional

import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, array_except, udf
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
import h3_pyspark

from opdi.ingestion.ourairports import fetch_airport_data


# ---------------------------------------------------------------------------
# Geometry helper
# ---------------------------------------------------------------------------

def generate_circle_polygon(
    lon: float,
    lat: float,
    radius_nm: float,
    num_points: int = 360,
) -> str:
    """Generate a GeoJSON polygon circle around a point with radius in nautical miles."""
    radius_km = radius_nm * 1.852

    def _deg2rad(deg):
        return deg * math.pi / 180

    def _calc_point(lon_, lat_, dist_km, bearing):
        R = 6371.01
        lat_r = _deg2rad(lat_)
        lon_r = _deg2rad(lon_)
        d_r = dist_km / R
        b_r = _deg2rad(bearing)
        new_lat = math.asin(
            math.sin(lat_r) * math.cos(d_r)
            + math.cos(lat_r) * math.sin(d_r) * math.cos(b_r)
        )
        new_lon = lon_r + math.atan2(
            math.sin(b_r) * math.sin(d_r) * math.cos(lat_r),
            math.cos(d_r) - math.sin(lat_r) * math.sin(new_lat),
        )
        return [math.degrees(new_lon), math.degrees(new_lat)]

    pts = [_calc_point(lon, lat, radius_km, (360 / num_points) * i) for i in range(num_points)]
    pts.append(pts[0])
    return json.dumps({"type": "Polygon", "coordinates": [pts]})


_udf_circle = udf(generate_circle_polygon, StringType())


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_airport_detection_areas(
    spark: SparkSession,
    output_path: str = "data/airport_hex/airport_concentric_c_hex_res_7_new.parquet",
    max_resolution: int = 7,
    num_points: int = 720,
    radii_nm: Optional[list[int]] = None,
) -> pd.DataFrame:
    """
    Build H3 concentric-ring detection areas around European airports.

    Parameters
    ----------
    spark : SparkSession
    output_path : str
        Where to write the resulting parquet file.
    max_resolution : int
        H3 resolution for the hex fill.
    num_points : int
        Number of polygon vertices for the circle approximation.
    radii_nm : list[int], optional
        Inner radii of each ring in nautical miles (outer is the next ring's
        inner radius).  Defaults to ``[0, 5, 10, 20, 30, 40]``.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns per airport / ring / hex_id.
    """
    if radii_nm is None:
        radii_nm = [0, 5, 10, 20, 30, 40]

    area_types = [f"C{r + 10}" for r in radii_nm]
    ring_df = pd.DataFrame({
        "max_resolution": max_resolution,
        "number_of_points_c": num_points,
        "area_type": area_types,
        "min_c_radius_nm": radii_nm,
    })
    ring_df["max_c_radius_nm"] = ring_df["min_c_radius_nm"].shift(-1).fillna(max(radii_nm) + 10)
    ring_df[["min_c_radius_nm", "max_c_radius_nm"]] = ring_df[
        ["min_c_radius_nm", "max_c_radius_nm"]
    ].astype(float)
    ring_df["m_col"] = 1

    ring_schema = StructType([
        StructField("max_resolution", IntegerType(), True),
        StructField("number_of_points_c", IntegerType(), True),
        StructField("area_type", StringType(), True),
        StructField("min_c_radius_nm", FloatType(), True),
        StructField("max_c_radius_nm", FloatType(), True),
        StructField("m_col", IntegerType(), True),
    ])
    ring_sdf = spark.createDataFrame(ring_df, schema=ring_schema)

    # Airports
    apt_pdf = fetch_airport_data()
    apt_schema = StructType([
        StructField("id", StringType()),
        StructField("ident", StringType()),
        StructField("type", StringType()),
        StructField("name", StringType()),
        StructField("latitude_deg", FloatType()),
        StructField("longitude_deg", FloatType()),
        StructField("elevation_ft", FloatType()),
        StructField("continent", StringType()),
        StructField("iso_country", StringType()),
        StructField("iso_region", StringType()),
        StructField("municipality", StringType()),
        StructField("scheduled_service", StringType()),
        StructField("gps_code", StringType()),
        StructField("iata_code", StringType()),
        StructField("local_code", StringType()),
        StructField("home_link", StringType()),
        StructField("wikipedia_link", StringType()),
        StructField("keywords", StringType()),
    ])
    airports_sdf = spark.createDataFrame(apt_pdf, schema=apt_schema)
    airports_m = airports_sdf.withColumn("m_col", lit(1)).join(ring_sdf, on="m_col", how="left")

    result = (
        airports_m
        .withColumn(
            "inner_circle_polygon",
            _udf_circle(col("longitude_deg"), col("latitude_deg"),
                        col("min_c_radius_nm"), col("number_of_points_c")),
        )
        .withColumn(
            "outer_circle_polygon",
            _udf_circle(col("longitude_deg"), col("latitude_deg"),
                        col("max_c_radius_nm"), col("number_of_points_c")),
        )
        .withColumn(
            "inner_circle_hex_ids",
            h3_pyspark.polyfill(col("inner_circle_polygon"), col("max_resolution"), lit(True)),
        )
        .withColumn(
            "outer_circle_hex_ids",
            h3_pyspark.polyfill(col("outer_circle_polygon"), col("max_resolution"), lit(True)),
        )
        .withColumn("hex_id", array_except(col("outer_circle_hex_ids"), col("inner_circle_hex_ids")))
        .drop(
            "inner_circle_polygon", "outer_circle_polygon",
            "inner_circle_hex_ids", "outer_circle_hex_ids",
        )
        .toPandas()
    )

    result.to_parquet(output_path)
    return result
