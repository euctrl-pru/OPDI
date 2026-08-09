"""
OpenSky Network state vectors ingestion module.

Downloads and processes state vector data from OpenSky Network's MinIO server
and writes to Iceberg tables with proper partitioning.
"""

import os
import subprocess
import time
from typing import List, Optional, Set, Dict, Tuple

from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, from_unixtime

from opdi.config import OPDIConfig
from opdi.utils.storage import StorageManager

#: Thinning rules for :meth:`StateVectorIngestion._apply_filters`.
#: ``modulo`` is what every published OPDI dataset was built with.
DECIMATION_MODULO = "modulo"
DECIMATION_BUCKET = "bucket"


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

    # Default OPDI bounding box: SW=[lon, lat] NE=[lon, lat]
    DEFAULT_BBOX: Tuple[float, float, float, float] = (-25.86653, 26.74617, 49.65699, 70.25976)

    def __init__(
        self,
        spark: SparkSession,
        config: OPDIConfig,
        local_download_path: str = "OPDI_live/data/ec-datadump",
        log_file_path: str = "OPDI_live/logs/01_osn_statevectors_etl.log",
        bbox: Optional[Tuple[float, float, float, float]] = None,
        time_interval: int = 5,
        decimation: str = DECIMATION_MODULO,
    ):
        """
        Initialize state vector ingestion.

        Args:
            spark: Active SparkSession
            config: OPDI configuration object
            local_download_path: Local directory for temporary file downloads
            log_file_path: Path to file tracking processed files
            bbox: Bounding box as (min_lon, min_lat, max_lon, max_lat).
                  Defaults to OPDI European coverage area.
                  Pass None to use the default, or False-y value to disable.
            time_interval: Thinning interval in seconds. Defaults to 5. Set to 1
                  to keep every row.
            decimation: Which thinning rule to apply. ``"modulo"`` (default)
                  keeps rows where ``event_time % time_interval == 0``;
                  ``"bucket"`` keeps one row per (aircraft, interval bin). See
                  :meth:`_apply_filters` for why the default is what it is.
        """
        self.spark = spark
        self.config = config
        self.storage = StorageManager(spark, config)
        self.local_download_path = local_download_path
        self.log_file_path = log_file_path
        self.project = config.project.project_name
        self.batch_size = config.ingestion.batch_size
        self.bbox = bbox if bbox is not None else self.DEFAULT_BBOX
        self.time_interval = time_interval
        if decimation not in (DECIMATION_MODULO, DECIMATION_BUCKET):
            raise ValueError(
                f"decimation must be {DECIMATION_MODULO!r} or {DECIMATION_BUCKET!r}, "
                f"got {decimation!r}"
            )
        self.decimation = decimation

        # Ensure directories exist
        os.makedirs(local_download_path, exist_ok=True)
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    def _apply_filters(self, df: DataFrame) -> DataFrame:
        """Apply bounding box and thinning filters to raw state vectors.

        Must be called before the event_time column is converted from
        Unix timestamp to Spark timestamp.

        Two thinning rules are available.

        ``modulo`` keeps the row at the one second per interval that is
        congruent to zero. It is a *fixed-phase* sampler, so on a feed whose
        rows arrive at arbitrary seconds it would delete far more than one row
        in ``time_interval``. **This is the default because it is what every
        published OPDI dataset was built with**, and because on the OSN archive
        it costs nothing: that table is a complete 1 Hz grid, so the phase
        always lands on a row.

        ``bucket`` bins time into ``floor(t / time_interval)`` and keeps one row
        per (aircraft, bin) -- the last, so a track's final observation is
        preserved exactly. This is the rule the modulo filter is an
        approximation of, and it is correct on a sparse feed.

        Measured on three hours of the OSN archive, bucket keeps 1.002x the rows
        of modulo and costs ~13% more wall clock, so it is offered rather than
        adopted. See ``benchmarks/decimation_experiment.py`` and the
        state-vector decimation study for the measurement. Switching rules
        changes ``track_id`` for every downstream row, so it is not a drop-in
        change to a published pipeline.
        """
        if self.bbox:
            min_lon, min_lat, max_lon, max_lat = self.bbox
            df = df.filter(
                (col("lon") >= min_lon) & (col("lon") <= max_lon)
                & (col("lat") >= min_lat) & (col("lat") <= max_lat)
            )

        if self.time_interval > 1:
            if self.decimation == DECIMATION_MODULO:
                df = df.filter((col("event_time") % self.time_interval) == 0)
            else:
                df = self._bucket_decimate(df)

        return df

    def _bucket_decimate(self, df: DataFrame) -> DataFrame:
        """Keep the last row of each (aircraft, interval bin).

        ``max`` over a struct compares field by field, so putting event_time
        first selects the latest row in the bin. That makes this a hash
        aggregate with map-side partial aggregation rather than a sort-based
        window -- one shuffle, and native Spark throughout.
        """
        rest = [c for c in df.columns if c != "event_time"]
        binned = df.withColumn(
            "_bin", col("event_time") - (col("event_time") % self.time_interval)
        )
        return (
            binned.groupBy("icao24", "_bin")
            .agg(F.max(F.struct("event_time", *rest)).alias("_s"))
            .select("_s.*")
        )

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

        # Apply bounding box and time interval filters (before timestamp conversion)
        df = self._apply_filters(df)

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

        self.storage.write_table(df_cleaned, "osn_statevectors_v2")

        print(f"Written {df_cleaned.count()} records to osn_statevectors_v2")

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

    def ingest_from_s3(
        self,
        start_date: date,
        end_date: date,
        s3_base_path: str = "s3a://opensky-hdfs-backup/tables_v4/state_vectors",
    ) -> int:
        """
        Ingest state vectors by reading directly from the OpenSky S3 bucket.

        This method bypasses the MinIO CLI download and reads parquet
        partitions directly via Spark's S3A filesystem. Intended for
        the ``opensky`` environment.

        Args:
            start_date: First day to ingest (inclusive).
            end_date: Last day to ingest (exclusive).
            s3_base_path: S3 prefix for hourly state-vector partitions.

        Returns:
            Number of rows ingested.

        Example:
            >>> ingestion = StateVectorIngestion(spark, config)
            >>> rows = ingestion.ingest_from_s3(date(2025, 8, 1), date(2025, 8, 2))
        """
        from datetime import datetime, timedelta

        # Build list of hourly partition paths
        current = datetime(start_date.year, start_date.month, start_date.day)
        end_dt = datetime(end_date.year, end_date.month, end_date.day)
        paths = []
        while current < end_dt:
            hour_ts = int(current.timestamp())
            paths.append(f"{s3_base_path}/hour={hour_ts}")
            current += timedelta(hours=1)

        print(f"Reading {len(paths)} hourly partitions from S3 ({start_date} to {end_date})...")
        df = self.spark.read.option("mergeSchema", "true").parquet(*paths)

        # Rename columns from camelCase to snake_case
        for camel_case, snake_case in self.COLUMN_MAPPING.items():
            if camel_case in df.columns:
                df = df.withColumnRenamed(camel_case, snake_case)

        # Handle legacy 'time' column
        if "time" in df.columns:
            df = df.withColumnRenamed("time", "event_time")

        # Apply bounding box and time interval filters (before timestamp conversion)
        df = self._apply_filters(df)

        # Convert Unix timestamp to Spark timestamp
        df = df.withColumn("event_time", from_unixtime(col("event_time")).cast("timestamp"))

        row_count = df.count()
        print(f"Read {row_count:,} state vectors.")

        # Write via StorageManager
        self.storage.write_table(df, "osn_statevectors_v2", mode="overwrite")
        print(f"Written to osn_statevectors_v2 ({row_count:,} rows).")

        return row_count

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

        self.storage.create_table(create_table_sql)
        print(f"Table {self.project}.osn_statevectors_v2 created/verified.")
