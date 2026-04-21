"""
OpenSky Network state vectors ingestion module.

Downloads and processes state vector data from OpenSky Network's MinIO server
and writes to Iceberg tables with proper partitioning.
"""

import os
import subprocess
import time
from typing import List, Set, Dict
from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, from_unixtime

from opdi.config import OPDIConfig


class StateVectorIngestion:
    """
    Handles ingestion of OpenSky Network state vectors from MinIO storage.

    This class manages the complete workflow of downloading state vector parquet files
    from OpenSky's S3-compatible MinIO server, processing them, and writing to
    Iceberg tables with daily partitioning.
    """

    # Column name mapping from camelCase (OSN) to snake_case (OPDI standard)
    COLUMN_MAPPING = {
        "eventTime": "event_time",
        "icao24": "icao24",
        "lat": "lat",
        "lon": "lon",
        "velocity": "velocity",
        "heading": "heading",
        "vertRate": "vert_rate",
        "callsign": "callsign",
        "onGround": "on_ground",
        "alert": "alert",
        "spi": "spi",
        "squawk": "squawk",
        "baroAltitude": "baro_altitude",
        "geoAltitude": "geo_altitude",
        "lastPosUpdate": "last_pos_update",
        "lastContact": "last_contact",
        "serials": "serials",
    }

    def __init__(
        self,
        spark: SparkSession,
        config: OPDIConfig,
        local_download_path: str = "OPDI_live/data/ec-datadump",
        log_file_path: str = "OPDI_live/logs/01_osn_statevectors_etl.log",
    ):
        """
        Initialize state vector ingestion.

        Args:
            spark: Active SparkSession
            config: OPDI configuration object
            local_download_path: Local directory for temporary file downloads
            log_file_path: Path to file tracking processed files
        """
        self.spark = spark
        self.config = config
        self.local_download_path = local_download_path
        self.log_file_path = log_file_path
        self.project = config.project.project_name
        self.batch_size = config.ingestion.batch_size

        # Ensure directories exist
        os.makedirs(local_download_path, exist_ok=True)
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    def _execute_shell_command(self, command: str) -> tuple[str, str]:
        """
        Execute a shell command and return stdout and stderr.

        Args:
            command: Shell command to execute

        Returns:
            Tuple of (stdout, stderr) as strings
        """
        process = subprocess.Popen(
            command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()
        return stdout.decode().strip(), stderr.decode().strip()

    def setup_minio_client(self) -> bool:
        """
        Set up MinIO client (mc) for accessing OpenSky Network data.

        Requires OSN_USERNAME and OSN_KEY environment variables to be set.

        Returns:
            True if setup successful, False otherwise

        Raises:
            EnvironmentError: If OSN credentials are not set
        """
        if "OSN_USERNAME" not in os.environ or "OSN_KEY" not in os.environ:
            raise EnvironmentError(
                "OSN_USERNAME and OSN_KEY environment variables must be set. "
                "Obtain credentials from OpenSky Network."
            )

        print("Setting up MinIO client...")
        self._execute_shell_command(
            "curl -O https://dl.min.io/client/mc/release/linux-amd64/mc"
        )
        self._execute_shell_command("chmod +x mc")

        stdout, stderr = self._execute_shell_command(
            "./mc alias set opensky https://s3.opensky-network.org $OSN_USERNAME $OSN_KEY"
        )

        if stderr and "error" in stderr.lower():
            print(f"Error setting up MinIO: {stderr}")
            return False

        print("MinIO client configured successfully.")
        return True

    def list_available_files(
        self,
        start_date: date,
        end_date: date,
    ) -> List[str]:
        """
        List available state vector files on OpenSky MinIO server.

        Files are named ``states_YYYY-MM-DD-HH.parquet``.  Only files
        whose date falls within ``[start_date, end_date)`` are returned.

        Args:
            start_date: Include files on or after this date.
            end_date: Include files before this date.

        Returns:
            List of file paths on MinIO server
        """
        import re

        print("Listing available files on OpenSky MinIO...")
        stdout, stderr = self._execute_shell_command(
            './mc find opensky/ec-datadump/ --path "*/states_*.parquet"'
        )

        if stderr:
            print(f"Warning while listing files: {stderr}")

        filtered_files = []
        for f in stdout.split("\n"):
            m = re.search(r"states_(\d{4}-\d{2}-\d{2})-\d{2}\.parquet", f)
            if not m:
                continue
            file_date = date.fromisoformat(m.group(1))
            if start_date <= file_date < end_date:
                filtered_files.append(f)

        print(f"Found {len(filtered_files)} files matching {start_date} to {end_date}.")
        return filtered_files

    def load_processed_files(self) -> Set[str]:
        """
        Load the set of already processed files from log.

        Returns:
            Set of processed file names
        """
        if os.path.exists(self.log_file_path):
            with open(self.log_file_path, "r") as f:
                return set(f.read().splitlines())
        return set()

    def mark_files_processed(self, file_names: List[str]) -> None:
        """
        Mark files as processed by appending to log file.

        Args:
            file_names: List of file names to mark as processed
        """
        with open(self.log_file_path, "a") as f:
            for file_name in file_names:
                f.write(file_name + "\n")

    def remove_partial_files(self) -> None:
        """
        Remove partially downloaded files (``*.parquet.part.minio``).

        MinIO creates ``.part`` files during download that may remain if
        download is interrupted. This cleans them up before processing.
        """
        try:
            files = os.listdir(self.local_download_path)
        except FileNotFoundError:
            return

        for filename in files:
            if filename.endswith(".parquet.part.minio"):
                file_path = os.path.join(self.local_download_path, filename)
                os.remove(file_path)
                print(f"Removed partial file: {filename}")

    def download_files(self, file_paths: List[str]) -> List[str]:
        """
        Download files from MinIO to local storage.

        Args:
            file_paths: List of full MinIO file paths to download

        Returns:
            List of successfully downloaded file names
        """
        downloaded_files = []
        processed_files = self.load_processed_files()

        for file_path in file_paths:
            file_name = file_path.split("/")[-1]

            if file_name in processed_files:
                continue

            local_file_path = os.path.join(self.local_download_path, file_name)
            cp_command = f'./mc cp "{file_path}" {local_file_path}'
            out, err = self._execute_shell_command(cp_command)

            if err:
                print(f"Error downloading {file_name}: {err}")
            else:
                downloaded_files.append(file_name)

        return downloaded_files

    def process_and_write_batch(self, file_names: List[str]) -> None:
        """
        Process downloaded files and write to Iceberg table.

        Args:
            file_names: List of file names to process (must be in local_download_path)
        """
        if not file_names:
            return

        # Read all files in the local folder
        df = self.spark.read.option("mergeSchema", "true").parquet(self.local_download_path)

        # Rename columns from camelCase to snake_case
        for camel_case, snake_case in self.COLUMN_MAPPING.items():
            df = df.withColumnRenamed(camel_case, snake_case)

        # Handle legacy 'time' column
        if "time" in df.columns:
            df = df.withColumnRenamed("time", "event_time")

        # Convert Unix timestamp to Spark timestamp
        df = df.withColumn("event_time", from_unixtime(col("event_time")).cast("timestamp"))

        # Add partition column
        df_with_partition = df.withColumn("event_time_day", to_date(col("event_time")))

        # Repartition for efficient write
        df_partitioned = df_with_partition.repartition("event_time_day").orderBy(
            "event_time_day"
        )

        # Drop partition column (will be added automatically by Iceberg)
        df_cleaned = df_partitioned.drop("event_time_day")

        # Write to Iceberg table
        table_name = f"`{self.project}`.`osn_statevectors_v2`"
        df_cleaned.writeTo(table_name).append()

        print(f"Written {df_cleaned.count()} records to {table_name}")

    def cleanup_local_files(self, file_names: List[str]) -> None:
        """
        Delete local files after successful processing to save disk space.

        Args:
            file_names: List of file names to delete
        """
        for file_name in file_names:
            local_file_path = os.path.join(self.local_download_path, file_name)
            if os.path.exists(local_file_path):
                os.remove(local_file_path)

    def ingest(
        self,
        start_date: date,
        end_date: date,
        dry_run: bool = False,
    ) -> Dict[str, int]:
        """
        Run the complete ingestion workflow.

        Downloads state vectors in batches, processes them, and writes to Iceberg.

        Args:
            start_date: Ingest files on or after this date.
            end_date: Ingest files before this date.
            dry_run: If True, only list files without downloading/processing

        Returns:
            Dictionary with statistics: {'files_processed': N, 'files_skipped': M}

        Example:
            >>> from opdi.ingestion import StateVectorIngestion
            >>> from opdi.utils.spark_helpers import get_spark
            >>> from opdi.config import OPDIConfig
            >>>
            >>> config = OPDIConfig.for_environment("live")
            >>> spark = get_spark("live", "State Vector Ingestion")
            >>> ingestion = StateVectorIngestion(spark, config)
            >>> stats = ingestion.ingest(start_date=date(2024, 1, 1), end_date=date(2024, 2, 1))
        """
        # Setup MinIO client
        if not self.setup_minio_client():
            raise RuntimeError("Failed to set up MinIO client")

        # List available files
        files_to_download = self.list_available_files(start_date, end_date)
        processed_files = self.load_processed_files()

        # Filter out already processed files
        pending_files = [
            f for f in files_to_download if f.split("/")[-1] not in processed_files
        ]

        print(f"Total files: {len(files_to_download)}")
        print(f"Already processed: {len(files_to_download) - len(pending_files)}")
        print(f"To process: {len(pending_files)}")

        if dry_run:
            print("Dry run - no files will be downloaded.")
            return {"files_processed": 0, "files_skipped": len(files_to_download)}

        # Process in batches
        files_processed = 0

        for i in range(0, len(pending_files), self.batch_size):
            batch_num = i // self.batch_size
            total_batches = (len(pending_files) + self.batch_size - 1) // self.batch_size

            print(f"\n=== Processing batch {batch_num + 1} of {total_batches} ===")

            batch_files = pending_files[i : i + self.batch_size]

            # Download batch
            downloaded_files = self.download_files(batch_files)

            if not downloaded_files:
                continue

            # Clean up partial downloads
            time.sleep(1)  # Brief pause for file system consistency
            self.remove_partial_files()

            # Process and write to Iceberg
            try:
                self.process_and_write_batch(downloaded_files)

                # Clean up local files
                self.cleanup_local_files(downloaded_files)

                # Mark as processed
                self.mark_files_processed(downloaded_files)

                files_processed += len(downloaded_files)
                print(f"Batch complete. Processed {len(downloaded_files)} files.")

            except Exception as e:
                print(f"Error processing batch: {e}")
                # Files remain in local folder and won't be marked as processed
                # Can be retried on next run
                raise

        print(f"\n=== Ingestion complete ===")
        print(f"Files processed: {files_processed}")

        return {
            "files_processed": files_processed,
            "files_skipped": len(files_to_download) - len(pending_files),
        }

    def create_table_if_not_exists(self) -> None:
        """
        Create the osn_statevectors_v2 Iceberg table if it doesn't exist.

        This should be run once before first ingestion.
        """
        from datetime import date

        today = date.today().strftime("%d %B %Y")

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.project}`.`osn_statevectors_v2` (
          event_time TIMESTAMP COMMENT 'Timestamp for which the state vector was valid.',
          icao24 STRING COMMENT '24-bit ICAO transponder ID for tracking airframes.',
          lat DOUBLE COMMENT 'Last known latitude of the aircraft.',
          lon DOUBLE COMMENT 'Last known longitude of the aircraft.',
          velocity DOUBLE COMMENT 'Speed over ground in meters per second.',
          heading DOUBLE COMMENT 'Direction of movement (track angle) from geographic north.',
          vert_rate DOUBLE COMMENT 'Vertical speed in meters per second.',
          callsign STRING COMMENT 'Callsign broadcast by the aircraft.',
          on_ground BOOLEAN COMMENT 'Surface positions (true) or airborne (false).',
          alert BOOLEAN COMMENT 'Special ATC indicator.',
          spi BOOLEAN COMMENT 'Special ATC indicator.',
          squawk STRING COMMENT '4-digit transponder code for ATC identification.',
          baro_altitude DOUBLE COMMENT 'Altitude measured by barometer (meters).',
          geo_altitude DOUBLE COMMENT 'Altitude from GNSS/GPS sensor (meters).',
          last_pos_update DOUBLE COMMENT 'Unix timestamp of position age.',
          last_contact DOUBLE COMMENT 'Unix timestamp of last signal received.',
          serials ARRAY<INT> COMMENT 'List of ADS-B receiver serials.'
        )
        USING iceberg
        PARTITIONED BY (days(event_time))
        COMMENT 'OpenSky Network state vectors. Last updated: {today}.'
        """

        self.spark.sql(create_table_sql)
        print(f"Table {self.project}.osn_statevectors_v2 created/verified.")
