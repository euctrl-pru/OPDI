"""
Transform: Airport ground layouts via OSM → H3 hexagons.

Fetches aeroway features from OpenStreetMap for each airport ICAO code, converts
geometries to polygons, and fills them with H3 hexagons at resolution 12.
"""

from __future__ import annotations

import os
import re
import traceback
from typing import Optional

import h3
import pandas as pd
import numpy as np
from shapely.geometry import Polygon, LineString
from shapely.ops import transform
from pyproj import Transformer

import osmnx as ox

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from opdi.ingestion.ourairports import fetch_airport_data


# ---------------------------------------------------------------------------
# OSM retrieval
# ---------------------------------------------------------------------------

def retrieve_osm_data(icao_code: str) -> pd.DataFrame:
    """Fetch aeroway features from OSM for the given ICAO airport code."""
    gdf = ox.features.features_from_place(
        f"{icao_code} Airport",
        tags={
            "aeroway": [
                "taxiway", "runway", "apron", "hangar",
                "threshold", "parking_position", "deicing_pad",
            ]
        },
    )
    if gdf.empty:
        raise ValueError(f"No data returned for {icao_code}")
    return gdf


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------

def _is_number(s) -> bool:
    try:
        if pd.isna(s):
            return False
        float(s)
        return True
    except (ValueError, TypeError):
        return False


def _safe_float(s):
    if _is_number(s):
        return float(s)
    if pd.isna(s):
        return None
    numeric = re.sub(r"[^-0-9.]+", "", str(s))
    try:
        return float(numeric)
    except ValueError:
        return None


_DEFAULT_WIDTHS = {
    "taxiway": 30,
    "runway": 45,
    "apron": 20,
    "hangar": 20,
    "threshold": 45,
    "parking_position": 25,
    "deicing_pad": 60,
}


def _fill_missing_width(aeroway: str, width):
    if pd.isnull(width):
        return _DEFAULT_WIDTHS.get(aeroway)
    return _safe_float(width)


def _buffer_geometry(geometry, width_m: float, always_xy: bool = True):
    transformer_to_utm = Transformer.from_crs("EPSG:4326", "EPSG:32633", always_xy=always_xy)
    transformer_to_wgs = Transformer.from_crs("EPSG:32633", "EPSG:4326", always_xy=always_xy)
    geom_utm = transform(transformer_to_utm.transform, geometry)
    geom_buf = geom_utm.buffer(width_m / 2)
    return transform(transformer_to_wgs.transform, geom_buf)


def _convert_to_polygon(geometry, geom_type: str, width_m: float) -> list:
    if geom_type == "Polygon":
        return [geometry]
    if geom_type == "MultiPolygon":
        return list(geometry.geoms)
    if geom_type in ("LineString", "Point"):
        return [_buffer_geometry(geometry, width_m)]
    return []


def _polygon_to_h3(poly, resolution: int) -> set[str]:
    coords = [(lat, lon) for lon, lat in poly.exterior.coords]
    return h3.polyfill({"type": "Polygon", "coordinates": [coords]}, resolution)


def _clean_str(s):
    try:
        if "ft" in str(s):
            return str(float(re.sub(r"[^0-9.]", "", s)) * 0.3048)
        if "mi" in str(s):
            return str(float(re.sub(r"[^0-9.]", "", s)) * 1609.34)
        return float(re.sub(r"[^0-9.]", "", str(s)))
    except Exception:
        return s


# ---------------------------------------------------------------------------
# Main processing function
# ---------------------------------------------------------------------------

_COLUMN_TYPES = {
    "hexaero_apt_icao": str,
    "hexaero_h3_id": str,
    "hexaero_latitude": float,
    "hexaero_longitude": float,
    "hexaero_res": int,
    "hexaero_aeroway": str,
    "hexaero_length": float,
    "hexaero_ref": str,
    "hexaero_surface": str,
    "hexaero_width": float,
    "hexaero_osm_id": int,
    "hexaero_type": str,
}


def hexagonify_airport(icao_code: str, resolution: int = 12) -> pd.DataFrame:
    """
    Fetch OSM data for an airport, convert features to H3 hexagons.

    Parameters
    ----------
    icao_code : str
        ICAO airport code (e.g. ``"EBBR"``).
    resolution : int
        H3 resolution (default 12 for ground layouts).

    Returns
    -------
    pd.DataFrame
        Rows of hexagons with metadata columns prefixed ``hexaero_``.
    """
    df = retrieve_osm_data(icao_code).reset_index()
    df["apt_icao"] = icao_code
    df["geom_type"] = df["geometry"].apply(lambda g: g.geom_type)

    if "width" not in df.columns:
        df["width"] = None

    df["width"] = df.apply(lambda r: _fill_missing_width(r["aeroway"], r["width"]), axis=1)
    df["polygon_geometries"] = df.apply(
        lambda r: _convert_to_polygon(r["geometry"], r["geom_type"], r["width"]), axis=1
    )
    df = df.explode("polygon_geometries")

    df["hex_id"] = df.apply(
        lambda r: _polygon_to_h3(r["polygon_geometries"], resolution), axis=1
    )
    df = df.explode("hex_id")
    df = df[~df.hex_id.isna()]

    df["hex_latitude"], df["hex_longitude"] = zip(*df["hex_id"].apply(h3.h3_to_geo))
    df["hex_res"] = resolution

    keep = ["apt_icao", "hex_id", "hex_latitude", "hex_longitude", "hex_res"]
    extra = [c for c in df.columns if c not in keep + ["geometry", "polygon_geometries"]]
    df = df[keep + extra]
    df = df.rename(columns={"hex_id": "h3_id", "id": "osm_id", "element": "type"})
    df.columns = ["hexaero_" + c.replace("hex_", "") for c in df.columns]

    for col_name, dtype in _COLUMN_TYPES.items():
        if col_name in df.columns:
            if dtype in (float, int):
                df[col_name] = df[col_name].apply(_clean_str).astype(dtype)
            else:
                df[col_name] = df[col_name].astype(dtype)
        else:
            df[col_name] = None

    return df[list(_COLUMN_TYPES.keys())]


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------

_LAYOUT_SCHEMA = StructType([
    StructField("hexaero_apt_icao", StringType(), True),
    StructField("hexaero_h3_id", StringType(), True),
    StructField("hexaero_latitude", DoubleType(), True),
    StructField("hexaero_longitude", DoubleType(), True),
    StructField("hexaero_res", IntegerType(), True),
    StructField("hexaero_aeroway", StringType(), True),
    StructField("hexaero_length", DoubleType(), True),
    StructField("hexaero_ref", StringType(), True),
    StructField("hexaero_surface", StringType(), True),
    StructField("hexaero_width", DoubleType(), True),
    StructField("hexaero_osm_id", LongType(), True),
    StructField("hexaero_type", StringType(), True),
])


def build_airport_layouts(
    spark: SparkSession,
    project: str = "project_opdi",
    resolution: int = 12,
    success_log: str = "logs/00_hexaero_layout_progress_success.parquet",
    failed_log: str = "logs/00_hexaero_layout_progress_failed.parquet",
    troublesome: Optional[list[str]] = None,
) -> None:
    """
    Iterate over all European airports and build H3 ground layouts.

    Results are appended to ``{project}.hexaero_airport_layouts``.

    Parameters
    ----------
    spark : SparkSession
    project : str
    resolution : int
        H3 resolution for ground layout hexagons.
    success_log, failed_log : str
        Parquet log files tracking progress.
    troublesome : list[str], optional
        ICAO codes to skip.
    """
    airports = fetch_airport_data()
    troublesome = troublesome or ["SPLO"]

    if os.path.isfile(success_log):
        done = pd.read_parquet(success_log).apt.tolist()
    else:
        done = []

    failed_apts, failed_errors = [], []

    for icao in airports.ident.tolist():
        if icao in troublesome or icao in done:
            continue

        try:
            df = hexagonify_airport(icao, resolution=resolution)
            sdf = spark.createDataFrame(df.to_dict(orient="records"), _LAYOUT_SCHEMA)
            sdf = sdf.repartition("hexaero_apt_icao").orderBy("hexaero_apt_icao")
            sdf.writeTo(f"`{project}`.`hexaero_airport_layouts`").append()

            done.append(icao)
            pd.DataFrame({"apt": done}).to_parquet(success_log)

        except Exception as exc:
            print(f"Failed {icao}: {exc}")
            traceback.print_exc()
            failed_apts.append(icao)
            failed_errors.append(str(exc))
            pd.DataFrame({"apt": failed_apts, "error": failed_errors}).to_parquet(failed_log)
