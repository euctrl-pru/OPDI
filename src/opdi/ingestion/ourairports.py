"""
Ingestion: OurAirports open data.

Downloads CSVs from ourairports.com and writes them into Spark/Hive tables.
Also provides a helper to fetch airport reference data as a pandas DataFrame
(used by several downstream transforms).
"""

from __future__ import annotations

import urllib.request
import os
from datetime import datetime
from typing import Optional

import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

SCHEMA_AIRPORTS = StructType([
    StructField("id", IntegerType(), True),
    StructField("ident", StringType(), True),
    StructField("type", StringType(), True),
    StructField("name", StringType(), True),
    StructField("latitude_deg", DoubleType(), True),
    StructField("longitude_deg", DoubleType(), True),
    StructField("elevation_ft", IntegerType(), True),
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

SCHEMA_RUNWAYS = StructType([
    StructField("id", IntegerType(), True),
    StructField("airport_ref", IntegerType(), True),
    StructField("airport_ident", StringType(), True),
    StructField("length_ft", IntegerType(), True),
    StructField("width_ft", IntegerType(), True),
    StructField("surface", StringType(), True),
    StructField("lighted", BooleanType(), True),
    StructField("closed", BooleanType(), True),
    StructField("le_ident", StringType(), True),
    StructField("le_latitude_deg", DoubleType(), True),
    StructField("le_longitude_deg", DoubleType(), True),
    StructField("le_elevation_ft", IntegerType(), True),
    StructField("le_heading_degT", DoubleType(), True),
    StructField("le_displaced_threshold_ft", IntegerType(), True),
    StructField("he_ident", StringType(), True),
    StructField("he_latitude_deg", DoubleType(), True),
    StructField("he_longitude_deg", DoubleType(), True),
    StructField("he_elevation_ft", IntegerType(), True),
    StructField("he_heading_degT", DoubleType(), True),
    StructField("he_displaced_threshold_ft", IntegerType(), True),
])

SCHEMA_NAVAIDS = StructType([
    StructField("id", IntegerType(), True),
    StructField("filename", StringType(), True),
    StructField("ident", StringType(), True),
    StructField("name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("frequency_khz", IntegerType(), True),
    StructField("latitude_deg", DoubleType(), True),
    StructField("longitude_deg", DoubleType(), True),
    StructField("elevation_ft", IntegerType(), True),
    StructField("iso_country", StringType(), True),
    StructField("dme_frequency_khz", IntegerType(), True),
    StructField("dme_channel", StringType(), True),
    StructField("dme_latitude_deg", DoubleType(), True),
    StructField("dme_longitude_deg", DoubleType(), True),
    StructField("dme_elevation_ft", IntegerType(), True),
    StructField("slaved_variation_deg", DoubleType(), True),
    StructField("magnetic_variation_deg", DoubleType(), True),
    StructField("usageType", StringType(), True),
    StructField("power", StringType(), True),
    StructField("associated_airport", StringType(), True),
])

SCHEMA_AIRPORT_FREQUENCIES = StructType([
    StructField("id", IntegerType(), True),
    StructField("airport_ref", IntegerType(), True),
    StructField("airport_ident", StringType(), True),
    StructField("type", StringType(), True),
    StructField("description", StringType(), True),
    StructField("frequency_mhz", DoubleType(), True),
])

SCHEMA_COUNTRIES = StructType([
    StructField("id", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", StringType(), True),
    StructField("continent", StringType(), True),
    StructField("wikipedia_link", StringType(), True),
    StructField("keywords", StringType(), True),
])

SCHEMA_REGIONS = StructType([
    StructField("id", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("local_code", StringType(), True),
    StructField("name", StringType(), True),
    StructField("continent", StringType(), True),
    StructField("iso_country", StringType(), True),
    StructField("wikipedia_link", StringType(), True),
    StructField("keywords", StringType(), True),
])

SCHEMAS = {
    "airports": SCHEMA_AIRPORTS,
    "runways": SCHEMA_RUNWAYS,
    "navaids": SCHEMA_NAVAIDS,
    "airport_frequencies": SCHEMA_AIRPORT_FREQUENCIES,
    "countries": SCHEMA_COUNTRIES,
    "regions": SCHEMA_REGIONS,
}

URLS = {
    "airports": "https://ourairports.com/data/airports.csv",
    "runways": "https://ourairports.com/data/runways.csv",
    "navaids": "https://ourairports.com/data/navaids.csv",
    "airport_frequencies": "https://ourairports.com/data/airport-frequencies.csv",
    "countries": "https://ourairports.com/data/countries.csv",
    "regions": "https://ourairports.com/data/regions.csv",
}


# ---------------------------------------------------------------------------
# Ingest function
# ---------------------------------------------------------------------------

def ingest_ourairports(
    spark: SparkSession,
    target_project: str = "project_aiu",
    tmp_csv_path: str = "data.csv",
) -> None:
    """
    Download all OurAirports CSVs and insert into Hive tables.

    Parameters
    ----------
    spark : SparkSession
    target_project : str
        Database/project that hosts the ``oa_*`` tables.
    tmp_csv_path : str
        Temporary file path for CSV download.
    """
    for name, url in URLS.items():
        urllib.request.urlretrieve(url, tmp_csv_path)
        df = spark.read.csv(tmp_csv_path, header=True, schema=SCHEMAS[name])
        df.write.mode("overwrite").insertInto(f"{target_project}.oa_{name}")

    if os.path.exists(tmp_csv_path):
        os.remove(tmp_csv_path)


# ---------------------------------------------------------------------------
# Helper: fetch airports as pandas DataFrame
# ---------------------------------------------------------------------------

def fetch_airport_data(
    url: str = "https://davidmegginson.github.io/ourairports-data/airports.csv",
    europe_bbox: bool = True,
    offset: float = 3.0,
) -> pd.DataFrame:
    """
    Download the OurAirports airports CSV and optionally filter to Europe.

    Parameters
    ----------
    url : str
        Source URL for airports.csv.
    europe_bbox : bool
        If True, filter to a European bounding box (±offset degrees).
    offset : float
        Degrees of padding around the Europe bounding box.

    Returns
    -------
    pd.DataFrame
    """
    df = pd.read_csv(url)

    if europe_bbox:
        f_lat = df.latitude_deg.between(26.74617 - offset, 70.25976 + offset)
        f_lon = df.longitude_deg.between(-25.86653 - offset, 49.65699 + offset)
        df = df[f_lat & f_lon]

    df.columns = df.columns.astype(str)
    for c in ("latitude_deg", "longitude_deg", "elevation_ft"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df
