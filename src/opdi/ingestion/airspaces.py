"""
Ingestion: Airspace reference data (ANSP, FIR, countries).

Reads parquet files from PRU Atlas and processes them into H3-indexed airspace
reference tables.
"""

from __future__ import annotations

from typing import Optional

import pandas as pd
import h3
import shapely
import shapely.geometry
import shapely.wkt

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# Default source URLs
# ---------------------------------------------------------------------------

DEFAULT_ANSP_URLS = [
    "https://raw.githubusercontent.com/euctrl-pru/pruatlas/master/inst/extdata/ansps_ace_406.parquet",
    "https://raw.githubusercontent.com/euctrl-pru/pruatlas/master/inst/extdata/ansps_ace_481.parquet",
    "https://raw.githubusercontent.com/euctrl-pru/pruatlas/master/inst/extdata/ansps_ace_524.parquet",
]

DEFAULT_FIR_URLS = [
    "https://raw.githubusercontent.com/euctrl-pru/pruatlas/master/inst/extdata/firs_nm_524.parquet",
]

DEFAULT_COUNTRY_URLS = [
    "https://raw.githubusercontent.com/euctrl-pru/pruatlas/master/inst/extdata/countries50m.parquet",
]


# ---------------------------------------------------------------------------
# AIRAC mapping
# ---------------------------------------------------------------------------

CFMU_AIRAC = {
    406: ("2015-11-12", "2015-12-10"),
    481: ("2021-8-12", "2021-9-9"),
    524: ("2024-11-28", "2024-12-26"),
}


# ---------------------------------------------------------------------------
# H3 helpers
# ---------------------------------------------------------------------------

def _fill_geometry(geometry_wkt: str, res: int = 7) -> list[set[str]]:
    return [
        h3.polyfill(shapely.geometry.mapping(geom), res, geo_json_conformant=True)
        for geom in shapely.wkt.loads(geometry_wkt).geoms
    ]


def _fill_geometry_compact(geometry_wkt: str, res: int = 7) -> list[set[str]]:
    return [
        h3.compact(
            h3.polyfill(shapely.geometry.mapping(geom), res, geo_json_conformant=True)
        )
        for geom in shapely.wkt.loads(geometry_wkt).geoms
    ]


def _get_coords(hex_id: str) -> tuple[float, float]:
    try:
        return h3.h3_to_geo(hex_id)
    except Exception:
        return (0.0, 0.0)


# ---------------------------------------------------------------------------
# Processing helpers (ANSP / FIR / country)
# ---------------------------------------------------------------------------

def process_ansp_airspaces(df: pd.DataFrame, compact: bool = False) -> pd.DataFrame:
    """Process ANSP airspace DataFrame into H3-indexed rows."""
    df = df.copy()
    df["validity_start"] = df["airac_cfmu"].map(lambda v: CFMU_AIRAC[v][0])
    df["validity_end"] = df["airac_cfmu"].map(lambda v: CFMU_AIRAC[v][1])

    df = df[
        ["airspace_type", "name", "code", "airac_cfmu",
         "validity_start", "validity_end", "min_fl", "max_fl", "geometry_wkt"]
    ]
    df["min_fl"] = df["min_fl"].astype(int)
    df["max_fl"] = df["max_fl"].astype(int)

    filler = _fill_geometry_compact if compact else _fill_geometry
    df["h3_res_7"] = df["geometry_wkt"].apply(filler)
    df = df.drop(columns=["geometry_wkt"])
    df = df.explode("h3_res_7").explode("h3_res_7")
    df = df[df["h3_res_7"].apply(lambda v: isinstance(v, str))]

    try:
        df["h3_res_7_lat"], df["h3_res_7_lon"] = zip(
            *df["h3_res_7"].apply(_get_coords)
        )
    except ValueError:
        df["h3_res_7_lat"] = None
        df["h3_res_7_lon"] = None

    return df


def process_fir_airspaces(df: pd.DataFrame, compact: bool = False) -> pd.DataFrame:
    """Process FIR airspace DataFrame into H3-indexed rows."""
    df = df.copy()
    df["validity_start"] = df["airac_cfmu"].map(lambda v: CFMU_AIRAC[v][0])
    df["validity_end"] = df["airac_cfmu"].map(lambda v: CFMU_AIRAC[v][1])

    df = df[
        ["airspace_type", "name", "code", "airac_cfmu",
         "validity_start", "validity_end", "min_fl", "max_fl", "geometry_wkt"]
    ]
    df["min_fl"] = df["min_fl"].astype(int)
    df["max_fl"] = df["max_fl"].astype(int)

    filler = _fill_geometry_compact if compact else _fill_geometry
    df["h3_res_7"] = df["geometry_wkt"].apply(filler)
    df = df.drop(columns=["geometry_wkt"])
    df = df.explode("h3_res_7").explode("h3_res_7")
    df = df[df["h3_res_7"].apply(lambda v: isinstance(v, str))]

    try:
        df["h3_res_7_lat"], df["h3_res_7_lon"] = zip(
            *df["h3_res_7"].apply(_get_coords)
        )
    except ValueError:
        df["h3_res_7_lat"] = None
        df["h3_res_7_lon"] = None

    return df


def process_country_airspace(df: pd.DataFrame, compact: bool = False) -> pd.DataFrame:
    """Process country airspace DataFrame into H3-indexed rows."""
    df = df.rename(columns={"admin": "name", "iso_a3": "code"}).copy()
    df["min_fl"] = 0
    df["max_fl"] = 999
    df["airac_cfmu"] = -1
    df["validity_start"] = "1900-1-1"
    df["validity_end"] = "2100-1-1"
    df["airspace_type"] = "COUNTRY"

    df = df[
        ["airspace_type", "name", "code", "airac_cfmu",
         "validity_start", "validity_end", "min_fl", "max_fl", "geometry_wkt"]
    ]
    df["min_fl"] = df["min_fl"].astype(int)
    df["max_fl"] = df["max_fl"].astype(int)

    filler = _fill_geometry_compact if compact else _fill_geometry
    df["h3_res_7"] = df["geometry_wkt"].apply(filler)
    df = df.drop(columns=["geometry_wkt"])
    df = df.explode("h3_res_7").explode("h3_res_7")
    df = df[df["h3_res_7"].apply(lambda v: isinstance(v, str))]

    try:
        df["h3_res_7_lat"], df["h3_res_7_lon"] = zip(
            *df["h3_res_7"].apply(_get_coords)
        )
    except ValueError:
        df["h3_res_7_lat"] = None
        df["h3_res_7_lon"] = None

    return df


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------

_AIRSPACE_SCHEMA = StructType([
    StructField("airspace_type", StringType(), True),
    StructField("name", StringType(), True),
    StructField("code", StringType(), True),
    StructField("airac_cfmu", StringType(), True),
    StructField("validity_start", StringType(), True),
    StructField("validity_end", StringType(), True),
    StructField("min_fl", IntegerType(), True),
    StructField("max_fl", IntegerType(), True),
    StructField("h3_res_7", StringType(), True),
    StructField("h3_res_7_lat", DoubleType(), True),
    StructField("h3_res_7_lon", DoubleType(), True),
])


def _insert_airspace_rows(
    spark: SparkSession,
    pdf: pd.DataFrame,
    project: str,
    processor,
) -> None:
    """Process each row individually and append to Iceberg."""
    for i in range(len(pdf)):
        row = pdf.iloc[[i]]
        result = processor(row, compact=False)
        sdf = spark.createDataFrame(result.to_dict(orient="records"), _AIRSPACE_SCHEMA)
        sdf = sdf.withColumn("validity_start", col("validity_start").cast(TimestampType()))
        sdf = sdf.withColumn("validity_end", col("validity_end").cast(TimestampType()))
        sdf.writeTo(f"`{project}`.`opdi_h3_airspace_ref`").append()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def ingest_airspaces(
    spark: SparkSession,
    project: str = "project_opdi",
    ansp_urls: Optional[list[str]] = None,
    fir_urls: Optional[list[str]] = None,
    country_urls: Optional[list[str]] = None,
) -> None:
    """
    Ingest ANSP, FIR and country airspace reference data.

    Parameters
    ----------
    spark : SparkSession
    project : str
        Target Iceberg project.
    ansp_urls, fir_urls, country_urls : list[str], optional
        Override default source URLs.
    """
    for url in (ansp_urls or DEFAULT_ANSP_URLS):
        print(f"Processing ANSP: {url}")
        df = pd.read_parquet(url)
        _insert_airspace_rows(spark, df, project, process_ansp_airspaces)

    for url in (fir_urls or DEFAULT_FIR_URLS):
        print(f"Processing FIR: {url}")
        df = pd.read_parquet(url)
        _insert_airspace_rows(spark, df, project, process_fir_airspaces)

    for url in (country_urls or DEFAULT_COUNTRY_URLS):
        print(f"Processing Country: {url}")
        df = pd.read_parquet(url)
        _insert_airspace_rows(spark, df, project, process_country_airspace)
