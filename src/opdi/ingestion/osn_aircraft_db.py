"""
OpenSky Network aircraft database ingestion module.

Downloads aircraft metadata from OpenSky Network and loads into Iceberg tables.
"""

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, trim
from typing import Optional

from opdi.config import OPDIConfig


class AircraftDatabaseIngestion:
    """
    Handles ingestion of OpenSky Network aircraft database.

    The aircraft database contains metadata about aircraft including:
    - ICAO 24-bit address
    - Registration number
    - Aircraft model and type
    - Operator information
    """

    def __init__(
        self,
        spark: SparkSession,
        config: OPDIConfig,
        url: Optional[str] = None,
    ):
        """
        Initialize aircraft database ingestion.

        Args:
            spark: Active SparkSession
            config: OPDI configuration object
            url: URL to aircraft database CSV. If None, uses config default
        """
        self.spark = spark
        self.config = config
        self.project = config.project.project_name
        self.url = url or config.ingestion.osn_aircraft_db_url

    def download_and_convert(self) -> DataFrame:
        """
        Download aircraft database CSV and convert to Spark DataFrame.

        Returns:
            Spark DataFrame with aircraft metadata

        Raises:
            Exception: If download or conversion fails
        """
        print(f"Downloading aircraft database from {self.url}...")

        # Download with pandas (handles CSV parsing well)
        pandas_df = pd.read_csv(
            self.url,
            quotechar="'",
            on_bad_lines="error",
            low_memory=False,
        )

        print(f"Downloaded {len(pandas_df)} aircraft records.")

        # Convert all values to strings (handle nulls properly)
        def conv_str(x):
            if pd.isnull(x):
                return None
            return str(x)

        # Select and convert relevant columns
        pandas_df = pandas_df[
            [
                "icao24",
                "registration",
                "model",
                "typecode",
                "icaoAircraftClass",
                "operatorIcao",
            ]
        ].map(conv_str)

        # Convert to Spark DataFrame
        spark_df = self.spark.createDataFrame(pandas_df.to_dict(orient="records"))

        # Rename columns to snake_case
        renamed_df = spark_df.select(
            col("icao24"),
            col("registration"),
            col("model"),
            col("typecode"),
            col("icaoAircraftClass").alias("icao_aircraft_class"),
            col("operatorIcao").alias("icao_operator"),
        )

        # Clean up: trim whitespace and convert empty strings to null
        cleaned_df = renamed_df.select(
            [trim(when(col(c) == "", None).otherwise(col(c))).alias(c) for c in renamed_df.columns]
        )

        return cleaned_df

    def write_to_table(self, df: DataFrame, mode: str = "append") -> None:
        """
        Write aircraft database to Iceberg table.

        Args:
            df: DataFrame to write
            mode: Write mode - "append" or "overwrite"
        """
        table_name = f"`{self.project}`.`osn_aircraft_db`"

        if mode == "append":
            df.writeTo(table_name).append()
        elif mode == "overwrite":
            df.writeTo(table_name).overwrite()
        else:
            raise ValueError(f"Invalid mode: {mode}. Use 'append' or 'overwrite'.")

        print(f"Written {df.count()} records to {table_name} (mode: {mode}).")

    def create_table_if_not_exists(self) -> None:
        """
        Create the osn_aircraft_db Iceberg table if it doesn't exist.

        This should be run once before first ingestion.
        """
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.project}`.`osn_aircraft_db` (
          icao24 STRING COMMENT '24-bit ICAO transponder ID',
          registration STRING COMMENT 'Aircraft registration number (tail number)',
          model STRING COMMENT 'Aircraft model designation',
          typecode STRING COMMENT 'ICAO aircraft type code',
          icao_aircraft_class STRING COMMENT 'ICAO aircraft classification',
          icao_operator STRING COMMENT 'ICAO operator code'
        )
        USING iceberg
        COMMENT 'OpenSky Network aircraft database metadata'
        """

        self.spark.sql(create_table_sql)
        print(f"Table {self.project}.osn_aircraft_db created/verified.")

    def ingest(self, mode: str = "append") -> int:
        """
        Run the complete ingestion workflow.

        Downloads the aircraft database and writes to Iceberg table.

        Args:
            mode: Write mode - "append" or "overwrite"

        Returns:
            Number of records ingested

        Example:
            >>> from opdi.ingestion import AircraftDatabaseIngestion
            >>> from opdi.utils.spark_helpers import get_spark
            >>> from opdi.config import OPDIConfig
            >>>
            >>> config = OPDIConfig.for_environment("live")
            >>> spark = get_spark("live", "Aircraft DB Ingestion")
            >>> ingestion = AircraftDatabaseIngestion(spark, config)
            >>> count = ingestion.ingest(mode="overwrite")
            >>> print(f"Ingested {count} aircraft records")
        """
        # Download and convert
        df = self.download_and_convert()

        record_count = df.count()

        # Write to table
        self.write_to_table(df, mode=mode)

        print(f"Aircraft database ingestion complete: {record_count} records")
        return record_count

    def get_aircraft_info(self, icao24: str) -> Optional[dict]:
        """
        Look up aircraft information by ICAO 24-bit address.

        Args:
            icao24: ICAO 24-bit transponder ID (e.g., "a12b34")

        Returns:
            Dictionary with aircraft info, or None if not found

        Example:
            >>> ingestion = AircraftDatabaseIngestion(spark, config)
            >>> info = ingestion.get_aircraft_info("a12b34")
            >>> if info:
            ...     print(f"Registration: {info['registration']}")
        """
        table_name = f"`{self.project}`.`osn_aircraft_db`"

        result = (
            self.spark.table(table_name)
            .filter(col("icao24") == icao24.lower())
            .limit(1)
            .collect()
        )

        if result:
            return result[0].asDict()
        return None
