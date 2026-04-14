"""
H3 airspace encoding module.

Converts airspace definitions (ANSPs, FIRs, country boundaries) from PRU Atlas
into H3 hexagonal grids at resolution 7. Supports both compact and full
polyfill modes.

Ported from: OPDI-live/python/v2.0.0/00_create_h3_airspaces.py
"""

from typing import Dict, List, Optional, Tuple

import h3
import pandas as pd
import shapely
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

from opdi.config import OPDIConfig

# Default data source URLs from PRU Atlas
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

# CFMU AIRAC cycle to validity date mapping
CFMU_AIRAC: Dict[int, List[str]] = {
    406: ["2015-11-12", "2015-12-10"],
    481: ["2021-8-12", "2021-9-9"],
    524: ["2024-11-28", "2024-12-26"],
}

AIRSPACE_SCHEMA = StructType([
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


def fill_geometry(geometry_wkt: str, res: int = 7) -> list:
    """
    Convert a WKT geometry string to H3 hexagon sets (full polyfill).

    Args:
        geometry_wkt: WKT representation of the geometry (may be MultiPolygon).
        res: H3 resolution.

    Returns:
        List of sets of H3 indices, one per sub-geometry.
    """
    return [
        h3.polyfill(shapely.geometry.mapping(x), res, geo_json_conformant=True)
        for x in shapely.wkt.loads(geometry_wkt).geoms
    ]


def fill_geometry_compact(geometry_wkt: str, res: int = 7) -> list:
    """
    Convert a WKT geometry string to compact H3 hexagon sets.

    Args:
        geometry_wkt: WKT representation of the geometry.
        res: H3 resolution.

    Returns:
        List of compacted sets of H3 indices.
    """
    return [
        h3.compact(
            h3.polyfill(shapely.geometry.mapping(x), res, geo_json_conformant=True)
        )
        for x in shapely.wkt.loads(geometry_wkt).geoms
    ]


def get_coords(h: str) -> Tuple[float, float]:
    """
    Get geographic coordinates for an H3 index.

    Args:
        h: H3 index string.

    Returns:
        Tuple of (latitude, longitude).
    """
    try:
        return h3.h3_to_geo(h)
    except Exception:
        print(f"Hex {h} did not result in coords using h3.h3_to_geo(hex)")
        return 0.0, 0.0


def _process_airspace_df(
    df: pd.DataFrame,
    airspace_type: str,
    compact: bool = False,
    cfmu_airac: Optional[Dict[int, List[str]]] = None,
) -> pd.DataFrame:
    """
    Process an airspace DataFrame by adding H3 indices and validity dates.

    Handles ANSP, FIR, and COUNTRY airspace types with appropriate
    column mapping and AIRAC validity lookups.

    Args:
        df: Input DataFrame with geometry_wkt column.
        airspace_type: One of 'ANSP', 'FIR', or 'COUNTRY'.
        compact: Whether to use compact H3 representation.
        cfmu_airac: AIRAC cycle to validity mapping.

    Returns:
        DataFrame with H3 indices and coordinates.
    """
    if cfmu_airac is None:
        cfmu_airac = CFMU_AIRAC

    df = df.copy()

    if airspace_type == "COUNTRY":
        df = df.rename({"admin": "name", "iso_a3": "code"}, axis=1)
        df["min_fl"] = 0
        df["max_fl"] = 999
        df["airac_cfmu"] = -1
        df["validity_start"] = "1900-1-1"
        df["validity_end"] = "2100-1-1"
        df["airspace_type"] = "COUNTRY"
    else:
        df["validity_start"] = df["airac_cfmu"].map(lambda x: cfmu_airac[x][0])
        df["validity_end"] = df["airac_cfmu"].map(lambda x: cfmu_airac[x][1])

    df = df[
        [
            "airspace_type",
            "name",
            "code",
            "airac_cfmu",
            "validity_start",
            "validity_end",
            "min_fl",
            "max_fl",
            "geometry_wkt",
        ]
    ]

    df["min_fl"] = df["min_fl"].apply(int)
    df["max_fl"] = df["max_fl"].apply(int)

    fill_fn = fill_geometry_compact if compact else fill_geometry
    df["h3_res_7"] = df["geometry_wkt"].apply(fill_fn)
    df = df.drop(columns=["geometry_wkt"])

    # Explode nested lists (MultiPolygon -> individual hexes)
    df = df.explode("h3_res_7").explode("h3_res_7")
    df = df[df["h3_res_7"].apply(lambda x: isinstance(x, str))]

    try:
        df["h3_res_7_lat"], df["h3_res_7_lon"] = zip(
            *df["h3_res_7"].apply(get_coords)
        )
    except ValueError:
        print(f"The following hexes did not result in coords: {df.h3_res_7.to_list()}")
        df["h3_res_7_lat"] = None
        df["h3_res_7_lon"] = None

    return df


class AirspaceH3Generator:
    """
    Converts airspace polygon definitions to H3 hexagonal grids.

    Processes three types of airspaces:
    - ANSP (Air Navigation Service Provider) boundaries
    - FIR (Flight Information Region) boundaries
    - Country boundaries

    Each airspace polygon is converted to H3 hexagons at resolution 7
    and stored in an Iceberg table with validity dates from AIRAC cycles.

    Args:
        spark: Active SparkSession.
        config: OPDI configuration object.
        compact: Whether to use compact H3 representation.

    Example:
        >>> generator = AirspaceH3Generator(spark, config)
        >>> generator.process_all()
    """

    def __init__(
        self,
        spark: SparkSession,
        config: OPDIConfig,
        compact: bool = False,
    ):
        self.spark = spark
        self.config = config
        self.project = config.project.project_name
        self.compact = compact

    def _write_to_table(self, result_df: pd.DataFrame) -> None:
        """Write a processed airspace DataFrame to the Iceberg table."""
        spark_df = self.spark.createDataFrame(
            result_df.to_dict(orient="records"), AIRSPACE_SCHEMA
        )
        spark_df = spark_df.select(
            col("airspace_type"),
            col("name"),
            col("code"),
            col("airac_cfmu"),
            col("validity_start"),
            col("validity_end"),
            col("min_fl"),
            col("max_fl"),
            col("h3_res_7"),
            col("h3_res_7_lat"),
            col("h3_res_7_lon"),
        )
        spark_df = spark_df.withColumn(
            "validity_start", col("validity_start").cast(TimestampType())
        )
        spark_df = spark_df.withColumn(
            "validity_end", col("validity_end").cast(TimestampType())
        )
        spark_df.writeTo(f"`{self.project}`.`opdi_h3_airspace_ref`").append()

    def process_ansp(
        self, urls: Optional[List[str]] = None
    ) -> None:
        """
        Process ANSP airspace boundaries.

        Args:
            urls: List of parquet URLs for ANSP data.
                Defaults to PRU Atlas ANSP datasets.
        """
        urls = urls or DEFAULT_ANSP_URLS
        for filepath in urls:
            print(f"Processing ANSP: {filepath}")
            ansp_df = pd.read_parquet(filepath)
            for i in range(len(ansp_df)):
                row_df = ansp_df.iloc[[i]]
                print(f"  Processing: {row_df.name.values[0]}")
                result = _process_airspace_df(row_df, "ANSP", compact=self.compact)
                self._write_to_table(result)

    def process_fir(
        self, urls: Optional[List[str]] = None
    ) -> None:
        """
        Process FIR (Flight Information Region) boundaries.

        Args:
            urls: List of parquet URLs for FIR data.
                Defaults to PRU Atlas FIR datasets.
        """
        urls = urls or DEFAULT_FIR_URLS
        for filepath in urls:
            print(f"Processing FIR: {filepath}")
            fir_df = pd.read_parquet(filepath)
            for i in range(len(fir_df)):
                row_df = fir_df.iloc[[i]]
                print(f"  Processing: {row_df.name.values[0]}")
                result = _process_airspace_df(row_df, "FIR", compact=self.compact)
                self._write_to_table(result)

    def process_countries(
        self, urls: Optional[List[str]] = None
    ) -> None:
        """
        Process country boundary airspaces.

        Args:
            urls: List of parquet URLs for country boundary data.
                Defaults to PRU Atlas countries dataset.
        """
        urls = urls or DEFAULT_COUNTRY_URLS
        for filepath in urls:
            print(f"Processing countries: {filepath}")
            ctry_df = pd.read_parquet(filepath)
            for i in range(len(ctry_df)):
                row_df = ctry_df.iloc[[i]]
                print(f"  Processing: {row_df.admin.values[0]}")
                result = _process_airspace_df(row_df, "COUNTRY", compact=self.compact)
                self._write_to_table(result)

    def process_all(
        self,
        ansp_urls: Optional[List[str]] = None,
        fir_urls: Optional[List[str]] = None,
        country_urls: Optional[List[str]] = None,
    ) -> None:
        """
        Process all airspace types (ANSP, FIR, countries).

        Args:
            ansp_urls: URLs for ANSP data.
            fir_urls: URLs for FIR data.
            country_urls: URLs for country boundary data.
        """
        self.process_ansp(ansp_urls)
        self.process_fir(fir_urls)
        self.process_countries(country_urls)

    def create_table_if_not_exists(self) -> None:
        """Create the opdi_h3_airspace_ref Iceberg table if it doesn't exist."""
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.project}`.`opdi_h3_airspace_ref` (
            airspace_type STRING COMMENT 'Airspace type (ANSP, FIR, COUNTRY)',
            name STRING COMMENT 'Airspace name',
            code STRING COMMENT 'Airspace code',
            airac_cfmu STRING COMMENT 'AIRAC CFMU cycle number',
            validity_start TIMESTAMP COMMENT 'Validity start date',
            validity_end TIMESTAMP COMMENT 'Validity end date',
            min_fl INT COMMENT 'Minimum flight level',
            max_fl INT COMMENT 'Maximum flight level',
            h3_res_7 STRING COMMENT 'H3 index at resolution 7',
            h3_res_7_lat DOUBLE COMMENT 'H3 hex center latitude',
            h3_res_7_lon DOUBLE COMMENT 'H3 hex center longitude'
        )
        USING iceberg
        COMMENT 'H3-encoded airspace reference data (ANSP, FIR, country boundaries).'
        """
        self.spark.sql(create_sql)
        print(f"Table {self.project}.opdi_h3_airspace_ref created/verified.")
