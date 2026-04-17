"""
H3 airport ground layout generator.

Retrieves airport infrastructure data (runways, taxiways, aprons, hangars)
from OpenStreetMap via osmnx, converts geometries to H3 hexagons at
resolution 12, and stores the results for use in airport event detection.

Ported from: OPDI-live/python/v2.0.0/00_create_h3_airport_layouts.py
"""

import os
import re
import traceback
from typing import Dict, List, Optional, Set, Tuple

import h3
import osmnx as ox
import pandas as pd
from pyproj import Transformer
from shapely.geometry import LineString, MultiPolygon, Point, Polygon
from shapely.ops import transform

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from opdi.config import OPDIConfig


# Default buffer widths (meters) by aeroway type
DEFAULT_AEROWAY_WIDTHS: Dict[str, float] = {
    "taxiway": 30,
    "runway": 45,
    "apron": 20,
    "hangar": 20,
    "threshold": 45,
    "parking_position": 25,
    "deicing_pad": 60,
}

AEROWAY_TAGS = [
    "taxiway",
    "runway",
    "apron",
    "hangar",
    "threshold",
    "parking_position",
    "deicing_pad",
]

HEXAERO_SCHEMA = StructType([
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

COLUMN_TYPES: Dict[str, type] = {
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


def retrieve_osm_data(icao_code: str) -> "gpd.GeoDataFrame":
    """
    Retrieve aeroway features from OpenStreetMap for an airport.

    Args:
        icao_code: ICAO airport code (e.g., 'EBBR').

    Returns:
        GeoDataFrame of aeroway features.

    Raises:
        ValueError: If no data is returned for the airport.
    """
    gdf = ox.features.features_from_place(
        f"{icao_code} Airport",
        tags={"aeroway": AEROWAY_TAGS},
    )
    if gdf.empty:
        raise ValueError(f"No data returned for {icao_code}")
    return gdf


def buffer_geometry(
    line: LineString, width_m: float, always_xy: bool = True
) -> Polygon:
    """
    Buffer a LineString geometry to create a Polygon with a specified width.

    Projects to EPSG:3395 (meters), applies the buffer, then projects back
    to EPSG:4326 (degrees).

    Args:
        line: LineString geometry to buffer.
        width_m: Buffer width in meters.
        always_xy: Ensure transformer follows (x, y) coordinate order.

    Returns:
        Buffered geometry as a Polygon.
    """
    transformer_to_meters = Transformer.from_crs(
        "EPSG:4326", "EPSG:3395", always_xy=always_xy
    )
    transformer_to_degrees = Transformer.from_crs(
        "EPSG:3395", "EPSG:4326", always_xy=always_xy
    )
    line_in_meters = transform(transformer_to_meters.transform, line)
    buffered_line = line_in_meters.buffer(width_m)
    return transform(transformer_to_degrees.transform, buffered_line)


def is_number(s) -> bool:
    """Check if a value is a valid number."""
    try:
        if pd.isna(s):
            return False
        float(s)
        return True
    except (ValueError, TypeError):
        return False


def safe_convert_to_float(s) -> Optional[float]:
    """
    Safely convert a string to float, stripping non-numeric characters.

    Args:
        s: Value to convert.

    Returns:
        Float value or None if conversion fails.
    """
    if is_number(s):
        return float(s)
    if pd.isna(s):
        return None
    numeric_part = re.sub(r"[^-0-9.]+", "", str(s))
    try:
        return float(numeric_part)
    except ValueError:
        return None


def fill_missing_width(aeroway: str, width_m) -> Optional[float]:
    """
    Fill missing width values based on aeroway type defaults.

    Args:
        aeroway: Type of aeroway (runway, taxiway, etc.).
        width_m: Existing width value (may be None/NaN).

    Returns:
        Width in meters, using default if original is missing.
    """
    if pd.isnull(width_m):
        return DEFAULT_AEROWAY_WIDTHS.get(aeroway, None)
    return safe_convert_to_float(width_m)


def convert_to_polygon(geometry, geom_type: str, width_m: float) -> List[Polygon]:
    """
    Convert a geometry to a list of Polygons, buffering if necessary.

    Args:
        geometry: Input geometry (Polygon, MultiPolygon, LineString, or Point).
        geom_type: Geometry type string.
        width_m: Buffer width for non-polygon geometries.

    Returns:
        List of Polygon geometries.
    """
    if geom_type == "Polygon":
        return [geometry]
    if geom_type == "MultiPolygon":
        return [poly for poly in geometry.geoms]
    if geom_type in ["LineString", "Point"]:
        return [buffer_geometry(geometry, width_m, always_xy=True)]
    print(f"Geometry of type {geom_type} not supported.")
    return []


def polygon_to_h3(poly: Polygon, resolution: int) -> Set[str]:
    """
    Convert a Polygon to a set of H3 hexagon indices.

    Args:
        poly: Shapely Polygon.
        resolution: H3 resolution.

    Returns:
        Set of H3 index strings covering the polygon.
    """
    exterior_coords = [(lat, lon) for lon, lat in poly.exterior.coords]
    geojson_polygon = {"type": "Polygon", "coordinates": [exterior_coords]}
    return h3.polyfill(geojson_polygon, resolution)


def clean_str(s) -> str:
    """
    Clean a string value, converting feet/miles to meters.

    Args:
        s: Input string potentially containing unit suffixes.

    Returns:
        Cleaned string with values converted to meters.
    """
    try:
        s = str(s)
        if "ft" in s:
            result = float(re.sub(r"[^0-9.]", "", s)) * 0.3048
            return str(result)
        if "mi" in s:
            result = float(re.sub(r"[^0-9.]", "", s)) * 1609.34
            return str(result)
        return float(re.sub(r"[^0-9.]", "", s))
    except Exception:
        return s


def hexagonify_airport(apt_icao: str, resolution: int = 12) -> pd.DataFrame:
    """
    Process a single airport: fetch OSM data, create polygons, convert to H3.

    Args:
        apt_icao: ICAO airport code.
        resolution: H3 resolution (default: 12 for ~307m hexagons).

    Returns:
        DataFrame with H3 hexagons for the airport's infrastructure.
        Columns: hexaero_apt_icao, hexaero_h3_id, hexaero_latitude,
        hexaero_longitude, hexaero_res, hexaero_aeroway, hexaero_length,
        hexaero_ref, hexaero_surface, hexaero_width, hexaero_osm_id,
        hexaero_type.
    """
    df = retrieve_osm_data(apt_icao).reset_index()
    df["apt_icao"] = apt_icao
    df["geom_type"] = df["geometry"].apply(lambda geom: geom.geom_type)

    if "width" not in df.columns:
        df["width"] = None

    df["width"] = df.apply(
        lambda row: fill_missing_width(row["aeroway"], row["width"]), axis=1
    )

    # Handle multiple polygons per feature
    df["polygon_geometries"] = df.apply(
        lambda row: convert_to_polygon(row["geometry"], row["geom_type"], row["width"]),
        axis=1,
    )
    df = df.explode("polygon_geometries")

    # Convert polygons to H3
    df["hex_id"] = df.apply(
        lambda row: polygon_to_h3(row["polygon_geometries"], resolution), axis=1
    )
    df = df.explode("hex_id")
    df = df[~df.hex_id.isna()]

    # Add H3 coordinates
    df["hex_latitude"], df["hex_longitude"] = zip(
        *df["hex_id"].apply(h3.h3_to_geo)
    )
    df["hex_res"] = resolution

    # Reorganize columns
    s = ["apt_icao", "hex_id", "hex_latitude", "hex_longitude", "hex_res"]
    df = df[s + [x for x in df.columns if x not in s + ["geometry", "polygon_geometries"]]]
    df = df.rename({"hex_id": "h3_id", "id": "osm_id", "element": "type"}, axis=1)
    df.columns = ["hexaero_" + x.replace("hex_", "") for x in df.columns]

    # Type conversion and validation
    for column, dtype in COLUMN_TYPES.items():
        if column in df.columns:
            if dtype in [float, int]:
                df[column] = df[column].apply(clean_str).astype(dtype)
            else:
                df[column] = df[column].astype(dtype)
        else:
            df[column] = None
    df = df[COLUMN_TYPES.keys()]

    return df


class AirportLayoutGenerator:
    """
    Generates H3 hexagonal representations of airport ground infrastructure.

    Retrieves aeroway features (runways, taxiways, aprons, hangars) from
    OpenStreetMap for each airport, converts them to H3 hexagons at
    resolution 12, and writes the results to an Iceberg table.

    Processing is resumable: airports that have already been processed
    are tracked in a progress log and skipped on subsequent runs.

    Args:
        spark: Active SparkSession.
        config: OPDI configuration object.
        resolution: H3 resolution (default: from config, typically 12).
        log_dir: Directory for progress tracking files.

    Example:
        >>> generator = AirportLayoutGenerator(spark, config)
        >>> generator.process_all()
    """

    # OPDI state vector coverage bounding box
    BBOX_OFFSET = 3  # degrees
    LAT_MIN = 26.74617
    LAT_MAX = 70.25976
    LON_MIN = -25.86653
    LON_MAX = 49.65699

    def __init__(
        self,
        spark: SparkSession,
        config: OPDIConfig,
        resolution: Optional[int] = None,
        log_dir: str = "OPDI_live/logs",
    ):
        self.spark = spark
        self.config = config
        self.project = config.project.project_name
        self.resolution = resolution or config.h3.airport_layout_resolution
        self.log_dir = log_dir

        self._success_log = os.path.join(log_dir, "00_hexaero_layout_progress_success.parquet")
        self._failed_log = os.path.join(log_dir, "00_hexaero_layout_progress_failed.parquet")

        os.makedirs(log_dir, exist_ok=True)

    def _load_processed_airports(self) -> List[str]:
        """Load list of successfully processed airport ICAO codes."""
        if os.path.isfile(self._success_log):
            return pd.read_parquet(self._success_log).apt.to_list()
        return []

    def _mark_success(self, apt_icao: str, processed: List[str]) -> None:
        """Mark an airport as successfully processed."""
        processed.append(apt_icao)
        pd.DataFrame({"apt": processed}).to_parquet(self._success_log)

    def _mark_failed(self, apt_icao: str, error: str, failed: List, errors: List) -> None:
        """Record a failed airport with its error message."""
        failed.append(apt_icao)
        errors.append(str(error))
        pd.DataFrame({"apt": failed, "error": errors}).to_parquet(self._failed_log)

    def fetch_airport_list(
        self,
        airports_url: str = "https://davidmegginson.github.io/ourairports-data/airports.csv",
        airport_types: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Fetch and filter the list of airports to process.

        Args:
            airports_url: URL to OurAirports airports CSV.
            airport_types: Airport types to include
                (default: large_airport, medium_airport).

        Returns:
            DataFrame with columns: ident, latitude_deg, longitude_deg,
            elevation_ft, type.
        """
        if airport_types is None:
            airport_types = ["large_airport", "medium_airport"]

        airports_df = pd.read_csv(airports_url)
        airports_df = airports_df[airports_df["type"].isin(airport_types)][
            ["ident", "latitude_deg", "longitude_deg", "elevation_ft", "type"]
        ]

        offset = self.BBOX_OFFSET
        f_lat = airports_df.latitude_deg.between(
            self.LAT_MIN - offset, self.LAT_MAX + offset
        )
        f_lon = airports_df.longitude_deg.between(
            self.LON_MIN - offset, self.LON_MAX + offset
        )
        airports_df = airports_df[f_lat & f_lon]

        print(f"There are {len(airports_df)} airports to process (within OPDI bbox)...")
        return airports_df

    def process_airport(self, apt_icao: str) -> Optional[pd.DataFrame]:
        """
        Process a single airport and write results to Iceberg.

        Args:
            apt_icao: ICAO airport code.

        Returns:
            DataFrame with H3 layout data, or None on failure.
        """
        try:
            df = hexagonify_airport(apt_icao, resolution=self.resolution)
            sdf = self.spark.createDataFrame(df.to_dict(orient="records"), HEXAERO_SCHEMA)
            sdf = sdf.repartition("hexaero_apt_icao").orderBy("hexaero_apt_icao")
            sdf.writeTo(f"`{self.project}`.`hexaero_airport_layouts`").append()
            return df
        except Exception as e:
            print(f"Failed to process {apt_icao}. Error: {e}")
            print(traceback.format_exc())
            return None

    def process_all(
        self,
        airports_url: str = "https://davidmegginson.github.io/ourairports-data/airports.csv",
        airport_types: Optional[List[str]] = None,
        troublesome_airports: Optional[List[str]] = None,
    ) -> Tuple[List[str], List[str]]:
        """
        Process all airports, skipping already-processed ones.

        Args:
            airports_url: URL to OurAirports airports CSV.
            airport_types: Airport types to include.
            troublesome_airports: ICAO codes to skip (known problematic).

        Returns:
            Tuple of (successful_airports, failed_airports) lists.
        """
        if troublesome_airports is None:
            troublesome_airports = ["SPLO"]

        airports_df = self.fetch_airport_list(airports_url, airport_types)
        processed_success = self._load_processed_airports()
        processed_failed = []
        processed_errors = []

        for apt_icao in airports_df.ident.to_list():
            print(f"Processing {apt_icao}...")

            if apt_icao in troublesome_airports:
                print("... Not processing as troublesome")
                continue

            if apt_icao in processed_success:
                print(f"Airport {apt_icao} is already processed - Skipping...")
                continue

            result = self.process_airport(apt_icao)
            if result is not None:
                self._mark_success(apt_icao, processed_success)
            else:
                self._mark_failed(
                    apt_icao, "See logs", processed_failed, processed_errors
                )

        return processed_success, processed_failed

    def create_table_if_not_exists(self) -> None:
        """Create the hexaero_airport_layouts Iceberg table if it doesn't exist."""
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.project}`.`hexaero_airport_layouts` (
            hexaero_apt_icao STRING COMMENT 'ICAO airport code',
            hexaero_h3_id STRING COMMENT 'H3 hex index at resolution 12',
            hexaero_latitude DOUBLE COMMENT 'H3 hex center latitude',
            hexaero_longitude DOUBLE COMMENT 'H3 hex center longitude',
            hexaero_res INT COMMENT 'H3 resolution',
            hexaero_aeroway STRING COMMENT 'OSM aeroway type (runway, taxiway, etc.)',
            hexaero_length DOUBLE COMMENT 'Feature length in meters',
            hexaero_ref STRING COMMENT 'Feature reference (e.g., runway designation)',
            hexaero_surface STRING COMMENT 'Surface type',
            hexaero_width DOUBLE COMMENT 'Feature width in meters',
            hexaero_osm_id BIGINT COMMENT 'OpenStreetMap feature ID',
            hexaero_type STRING COMMENT 'OSM element type'
        )
        USING iceberg
        COMMENT 'H3 airport ground layouts from OpenStreetMap data.'
        """
        self.spark.sql(create_sql)
        print(f"Table {self.project}.hexaero_airport_layouts created/verified.")
