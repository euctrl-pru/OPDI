"""
OurAirports reference data ingestion.

Downloads and loads airport reference datasets from OurAirports into
Spark tables. Covers airports, runways, navaids, frequencies, countries,
and regions.

Ported from: OPDI-live/python/v2.0.0/00_etl_ourairports.py
"""

import os
import urllib.request
from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from opdi.config import OPDIConfig

# Dataset URLs
DEFAULT_URLS: Dict[str, str] = {
    "airports": "https://ourairports.com/data/airports.csv",
    "runways": "https://ourairports.com/data/runways.csv",
    "navaids": "https://ourairports.com/data/navaids.csv",
    "airport_frequencies": "https://ourairports.com/data/airport-frequencies.csv",
    "countries": "https://ourairports.com/data/countries.csv",
    "regions": "https://ourairports.com/data/regions.csv",
}

# Spark schemas for each dataset
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

SCHEMAS: Dict[str, StructType] = {
    "airports": SCHEMA_AIRPORTS,
    "runways": SCHEMA_RUNWAYS,
    "navaids": SCHEMA_NAVAIDS,
    "airport_frequencies": SCHEMA_AIRPORT_FREQUENCIES,
    "countries": SCHEMA_COUNTRIES,
    "regions": SCHEMA_REGIONS,
}


class OurAirportsIngestion:
    """
    ETL pipeline for OurAirports reference data.

    Downloads CSV datasets from OurAirports, loads them into Spark
    DataFrames with proper schemas, and writes them to database tables.

    Handles 6 datasets: airports, runways, navaids, airport_frequencies,
    countries, and regions.

    Args:
        spark: Active SparkSession.
        config: OPDI configuration object.
        target_database: Database to write tables to.  Defaults to the
            project database from *config* (``config.project.project_name``).
        temp_dir: Directory for temporary CSV downloads.

    Example:
        >>> ingestion = OurAirportsIngestion(spark, config)
        >>> ingestion.ingest_all()
    """

    def __init__(
        self,
        spark: SparkSession,
        config: OPDIConfig,
        target_database: Optional[str] = None,
        temp_dir: str = ".",
    ):
        self.spark = spark
        self.config = config
        self.target_database = target_database or config.project.project_name
        self.temp_dir = temp_dir
        self._temp_file = os.path.join(temp_dir, "ourairports_temp.csv")

    def _get_create_table_sql(self, table_name: str) -> str:
        """Generate CREATE TABLE SQL for a given OurAirports dataset."""
        today = datetime.today().strftime("%d %B %Y")
        db = self.target_database

        # Map of table names to their CREATE TABLE SQL
        sqls = {
            "airports": f"""
                CREATE TABLE IF NOT EXISTS `{db}`.`oa_airports` (
                    id INT, ident STRING, type STRING, name STRING,
                    latitude_deg DOUBLE, longitude_deg DOUBLE, elevation_ft INT,
                    continent STRING, iso_country STRING, iso_region STRING,
                    municipality STRING, scheduled_service STRING,
                    gps_code STRING, iata_code STRING, local_code STRING,
                    home_link STRING, wikipedia_link STRING, keywords STRING
                ) COMMENT 'OurAirports airports data. Last updated: {today}.'
                STORED AS parquet TBLPROPERTIES ('transactional'='false')
            """,
            "runways": f"""
                CREATE TABLE IF NOT EXISTS `{db}`.`oa_runways` (
                    id INT, airport_ref INT, airport_ident STRING,
                    length_ft INT, width_ft INT, surface STRING,
                    lighted BOOLEAN, closed BOOLEAN,
                    le_ident STRING, le_latitude_deg DOUBLE, le_longitude_deg DOUBLE,
                    le_elevation_ft INT, le_heading_degT DOUBLE, le_displaced_threshold_ft INT,
                    he_ident STRING, he_latitude_deg DOUBLE, he_longitude_deg DOUBLE,
                    he_elevation_ft INT, he_heading_degT DOUBLE, he_displaced_threshold_ft INT
                ) COMMENT 'OurAirports runways data. Last updated: {today}.'
                STORED AS parquet TBLPROPERTIES ('transactional'='false')
            """,
            "navaids": f"""
                CREATE TABLE IF NOT EXISTS `{db}`.`oa_navaids` (
                    id INT, filename STRING, ident STRING, name STRING, type STRING,
                    frequency_khz INT, latitude_deg DOUBLE, longitude_deg DOUBLE,
                    elevation_ft INT, iso_country STRING,
                    dme_frequency_khz INT, dme_channel STRING,
                    dme_latitude_deg DOUBLE, dme_longitude_deg DOUBLE, dme_elevation_ft INT,
                    slaved_variation_deg DOUBLE, magnetic_variation_deg DOUBLE,
                    usageType STRING, power STRING, associated_airport STRING
                ) COMMENT 'OurAirports navaids data. Last updated: {today}.'
                STORED AS parquet TBLPROPERTIES ('transactional'='false')
            """,
            "airport_frequencies": f"""
                CREATE TABLE IF NOT EXISTS `{db}`.`oa_airport_frequencies` (
                    id INT, airport_ref INT, airport_ident STRING,
                    type STRING, description STRING, frequency_mhz DOUBLE
                ) COMMENT 'OurAirports airport frequencies. Last updated: {today}.'
                STORED AS parquet TBLPROPERTIES ('transactional'='false')
            """,
            "countries": f"""
                CREATE TABLE IF NOT EXISTS `{db}`.`oa_countries` (
                    id INT, code STRING, name STRING, continent STRING,
                    wikipedia_link STRING, keywords STRING
                ) COMMENT 'OurAirports countries data. Last updated: {today}.'
                STORED AS parquet TBLPROPERTIES ('transactional'='false')
            """,
            "regions": f"""
                CREATE TABLE IF NOT EXISTS `{db}`.`oa_regions` (
                    id INT, code STRING, local_code STRING, name STRING,
                    continent STRING, iso_country STRING,
                    wikipedia_link STRING, keywords STRING
                ) COMMENT 'OurAirports regions data. Last updated: {today}.'
                STORED AS parquet TBLPROPERTIES ('transactional'='false')
            """,
        }
        return sqls[table_name]

    def create_tables(self) -> None:
        """Create all OurAirports tables if they don't already exist."""
        db = self.target_database

        for dataset in SCHEMAS:
            sql = self._get_create_table_sql(dataset)
            self.spark.sql(sql)
            print(f"Created/verified {db}.oa_{dataset}")

    def ingest_dataset(
        self,
        name: str,
        url: Optional[str] = None,
    ) -> int:
        """
        Download and ingest a single OurAirports dataset.

        Args:
            name: Dataset name (airports, runways, navaids, etc.).
            url: Override URL for the CSV download.

        Returns:
            Number of rows ingested.
        """
        url = url or DEFAULT_URLS[name]
        schema = SCHEMAS[name]

        print(f"Downloading {name} from {url}...")
        urllib.request.urlretrieve(url, self._temp_file)

        df = self.spark.read.csv(self._temp_file, header=True, schema=schema)
        row_count = df.count()

        table_name = f"{self.target_database}.oa_{name}"
        df.write.mode("overwrite").insertInto(table_name)
        print(f"  Ingested {row_count:,} rows into {table_name}")

        return row_count

    def ingest_all(
        self,
        urls: Optional[Dict[str, str]] = None,
    ) -> Dict[str, int]:
        """
        Download and ingest all OurAirports datasets.

        Tables are created if they don't exist, then overwritten with
        fresh data.

        Args:
            urls: Override URLs for each dataset.

        Returns:
            Dictionary mapping dataset names to row counts.

        Example:
            >>> ingestion = OurAirportsIngestion(spark, config)
            >>> stats = ingestion.ingest_all()
            >>> print(stats)
            {'airports': 76543, 'runways': 45678, ...}
        """
        urls = urls or DEFAULT_URLS

        self.create_tables()

        stats = {}
        for name in SCHEMAS:
            url = urls.get(name, DEFAULT_URLS[name])
            stats[name] = self.ingest_dataset(name, url)

        # Cleanup temp file
        if os.path.exists(self._temp_file):
            os.remove(self._temp_file)

        print("\nOurAirports ingestion complete.")
        print("-" * 40)
        for name, count in stats.items():
            print(f"  {name:<25} {count:>10,} rows")
        print("-" * 40)

        return stats
