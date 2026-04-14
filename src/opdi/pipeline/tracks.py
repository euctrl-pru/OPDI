"""
Track processing and enrichment module.

Transforms raw state vectors into structured flight tracks with:
- Track ID generation (SHA256-based)
- Track splitting based on time gaps
- H3 geospatial encoding
- Distance calculations
- Altitude cleaning
"""

import os
from datetime import date, datetime
from typing import List, Optional
import pandas as pd

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    lit, lag, when, to_date, concat, avg, abs as f_abs, col,
    unix_timestamp, to_timestamp, substring, sha2, concat_ws,
    year, month, sin, cos, radians, atan2, sqrt, sum as f_sum
)
import h3_pyspark

from opdi.config import OPDIConfig
from opdi.utils.datetime_helpers import get_start_end_of_month
from opdi.utils.h3_helpers import h3_list_prep


class TrackProcessor:
    """
    Processes raw state vectors into structured flight tracks.

    This class implements the core track creation logic including:
    - Unique track ID generation using SHA256 hashing
    - Track splitting based on time gaps and altitude
    - H3 hexagonal encoding at multiple resolutions
    - Cumulative distance calculation
    - Altitude outlier detection and smoothing

    CRITICAL: The track ID generation algorithm must remain unchanged
    as it ensures consistency with historical data and downstream systems.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: OPDIConfig,
        log_file_path: str = "OPDI_live/logs/02_osn-tracks-etl-log.parquet",
    ):
        """
        Initialize track processor.

        Args:
            spark: Active SparkSession
            config: OPDI configuration object
            log_file_path: Path to parquet file tracking processed months
        """
        self.spark = spark
        self.config = config
        self.project = config.project.project_name
        self.h3_resolutions = config.h3.track_resolutions
        self.log_file_path = log_file_path

        # Track splitting thresholds from config
        self.gap_threshold_minutes = config.ingestion.track_gap_threshold_minutes
        self.gap_low_alt_minutes = config.ingestion.track_gap_low_altitude_minutes
        self.low_altitude_meters = config.ingestion.track_gap_low_altitude_meters

        # Altitude cleaning threshold
        self.max_vertical_rate_mps = config.ingestion.max_vertical_rate_mps
        self.altitude_smoothing_window_minutes = (
            config.ingestion.altitude_smoothing_window_minutes
        )

        # Ensure log directory exists
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    def _load_processed_months(self) -> List[date]:
        """Load list of already processed months from log file."""
        if os.path.isfile(self.log_file_path):
            df = pd.read_parquet(self.log_file_path)
            return df.months.to_list()
        return []

    def _mark_month_processed(self, month: date) -> None:
        """Mark a month as processed in the log file."""
        processed_months = self._load_processed_months()
        if month not in processed_months:
            processed_months.append(month)
            processed_df = pd.DataFrame({"months": processed_months})
            processed_df.to_parquet(self.log_file_path)

    def _add_track_id(self, df: DataFrame) -> DataFrame:
        """
        Add unique track ID to each state vector.

        Track ID generation algorithm (CRITICAL - DO NOT MODIFY):
        1. Create group_id: SHA2(icao24 + callsign, 256) truncated to 16 chars
        2. Split tracks based on time gaps:
           - Gap > 30 minutes, OR
           - Gap > 15 minutes AND altitude < 1524m (5000 ft)
        3. Calculate offset for each split within a group
        4. Final track_id: {group_id}_{offset}_{year}_{month}

        Args:
            df: DataFrame with state vectors

        Returns:
            DataFrame with track_id column added
        """
        # Add timestamp column
        df = df.withColumn("event_time_ts", to_timestamp("event_time"))

        # Create group ID: SHA2 hash of icao24 + callsign (first 16 characters)
        # This groups together flights with same aircraft and callsign
        df = df.withColumn(
            "group_id", substring(sha2(concat_ws("", "icao24", "callsign"), 256), 1, 16)
        )

        # Define window for time-based calculations
        window_spec = Window.partitionBy("group_id").orderBy("event_time_ts")

        # Calculate time gap from previous point in same group
        df = df.withColumn("prev_event_time_ts", lag("event_time_ts").over(window_spec))
        df = df.withColumn(
            "time_gap_minutes",
            (unix_timestamp("event_time_ts") - unix_timestamp("prev_event_time_ts")) / 60,
        )

        # Track split condition:
        # 1. Gap > 30 minutes (aircraft likely landed and took off again)
        # 2. Gap > 15 minutes AND low altitude (< 1524m / 5000ft)
        #    This catches cases where transponder stays on during ground operations
        track_split_condition = (col("time_gap_minutes") > self.gap_threshold_minutes) | (
            (col("time_gap_minutes") > self.gap_low_alt_minutes)
            & (col("baro_altitude") < self.low_altitude_meters)
        )

        df = df.withColumn("new_group_flag", when(track_split_condition, 1).otherwise(0))

        # Calculate cumulative offset (number of splits before this point)
        group_window_spec = (
            Window.partitionBy("group_id")
            .orderBy("event_time_ts")
            .rowsBetween(Window.unboundedPreceding, 0)
        )
        df = df.withColumn("offset", f_sum("new_group_flag").over(group_window_spec))

        # Generate final track ID
        # Format: {group_id}_{offset}_{year}_{month}
        # The year/month suffix prevents tracks spanning multiple months from being merged
        df = df.withColumn(
            "track_id",
            concat(
                col("group_id"),
                lit("_"),
                col("offset"),
                lit("_"),
                year("event_time_ts").cast("string"),
                lit("_"),
                month("event_time_ts").cast("string"),
            ),
        )

        # Clean up temporary columns
        df = df.drop("event_time_ts", "group_id", "prev_event_time_ts",
                     "time_gap_minutes", "new_group_flag", "offset")

        return df

    def _add_h3_encoding(self, df: DataFrame) -> DataFrame:
        """
        Add H3 hexagonal grid indices at configured resolutions.

        Args:
            df: DataFrame with lat/lon columns

        Returns:
            DataFrame with h3_res_{resolution} columns added
        """
        for h3_resolution in self.h3_resolutions:
            df = df.withColumn("h3_resolution", lit(h3_resolution))
            df = df.withColumn(
                f"h3_res_{h3_resolution}",
                h3_pyspark.geo_to_h3("lat", "lon", "h3_resolution"),
            )

        # Drop temporary resolution column
        df = df.drop("h3_resolution")

        return df

    def _add_cumulative_distance(self, df: DataFrame) -> DataFrame:
        """
        Calculate segment and cumulative distances using Haversine formula.

        Uses PySpark native functions for distributed computation.

        Args:
            df: DataFrame with lat, lon, track_id, event_time columns

        Returns:
            DataFrame with segment_distance_nm and cumulative_distance_nm columns
        """
        # Define window specifications
        window_lag = Window.partitionBy("track_id").orderBy("event_time")
        window_cumsum = (
            Window.partitionBy("track_id")
            .orderBy("event_time")
            .rowsBetween(Window.unboundedPreceding, 0)
        )

        # Convert degrees to radians
        df = df.withColumn("lat_rad", radians(col("lat")))
        df = df.withColumn("lon_rad", radians(col("lon")))

        # Get previous point's coordinates
        df = df.withColumn("prev_lat_rad", lag("lat_rad").over(window_lag))
        df = df.withColumn("prev_lon_rad", lag("lon_rad").over(window_lag))

        # Haversine formula
        df = df.withColumn(
            "a",
            sin((col("lat_rad") - col("prev_lat_rad")) / 2) ** 2
            + cos(col("prev_lat_rad"))
            * cos(col("lat_rad"))
            * sin((col("lon_rad") - col("prev_lon_rad")) / 2) ** 2,
        )

        df = df.withColumn("c", 2 * atan2(sqrt(col("a")), sqrt(1 - col("a"))))

        # Distance in kilometers (Earth radius = 6371 km)
        df = df.withColumn("distance_km", 6371 * col("c"))

        # Convert to nautical miles (1 NM = 1.852 km)
        df = df.withColumn("segment_distance_nm", col("distance_km") / 1.852)

        # Calculate cumulative distance
        df = df.withColumn(
            "cumulative_distance_nm", f_sum("segment_distance_nm").over(window_cumsum)
        )

        # Drop temporary columns
        df = df.drop(
            "lat_rad", "lon_rad", "prev_lat_rad", "prev_lon_rad", "a", "c", "distance_km"
        )

        return df

    def _add_clean_altitude(self, df: DataFrame, col_name: str) -> DataFrame:
        """
        Clean altitude data by removing unrealistic climb/descent rates.

        Detects and replaces altitude values with unrealistic vertical rates
        (> 25.4 m/s or ~5000 ft/min) using a rolling average.

        Args:
            df: DataFrame with altitude and event_time columns
            col_name: Name of altitude column (e.g., "geo_altitude" or "baro_altitude")

        Returns:
            DataFrame with {col_name}_c column containing cleaned altitude
        """
        # Define window for rate of climb calculation
        window_spec = Window.partitionBy("track_id").orderBy("event_time")

        # Get previous altitude and time
        df = df.withColumn(f"prev_{col_name}", lag(col_name).over(window_spec))
        df = df.withColumn("prev_event_time", lag("event_time").over(window_spec))

        # Calculate time difference in seconds
        df = df.withColumn(
            "time_diff",
            (unix_timestamp("event_time") - unix_timestamp("prev_event_time")).cast(
                "double"
            ),
        )

        # Calculate altitude change and rate of climb
        df = df.withColumn("altitude_diff", col(col_name) - col(f"prev_{col_name}"))
        df = df.withColumn("rate_of_climb", col("altitude_diff") / col("time_diff"))

        # Create window for rolling average (5 minutes = 300 seconds)
        time_window = self.altitude_smoothing_window_minutes * 60
        df = df.withColumn("event_time_epoch", unix_timestamp("event_time").cast("bigint"))
        window_spec_avg = (
            Window.partitionBy("track_id")
            .orderBy("event_time_epoch")
            .rangeBetween(-time_window, time_window)
        )

        # Calculate rolling average
        df = df.withColumn(f"smoothed_{col_name}", avg(col_name).over(window_spec_avg))

        # Replace unrealistic values with smoothed values
        # Threshold: 25.4 m/s = 5000 ft/min
        df = df.withColumn(
            f"{col_name}_c",
            when(
                f_abs(col("rate_of_climb")) > self.max_vertical_rate_mps,
                col(f"smoothed_{col_name}"),
            ).otherwise(col(col_name)),
        )

        # Drop temporary columns
        df = df.drop(
            f"smoothed_{col_name}",
            f"prev_{col_name}",
            "prev_event_time",
            "time_diff",
            "altitude_diff",
            "rate_of_climb",
            "event_time_epoch",
        )

        return df

    def process_month(self, month: date, skip_if_processed: bool = True) -> None:
        """
        Process tracks for a single month.

        Args:
            month: Date representing the first day of the month to process
            skip_if_processed: If True, skip if month already processed

        Example:
            >>> from datetime import date
            >>> processor = TrackProcessor(spark, config)
            >>> processor.process_month(date(2024, 1, 1))
        """
        # Check if already processed
        if skip_if_processed and month in self._load_processed_months():
            print(f"Month {month.strftime('%Y-%m')} already processed. Skipping.")
            return

        print(f"Processing tracks for month {month.strftime('%Y-%m')}... ({datetime.now()})")

        # Prepare H3 column names
        h3_columns = h3_list_prep(self.h3_resolutions)

        # Get month boundaries
        start_time, end_time = get_start_end_of_month(month)
        start_time_str = datetime.utcfromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S")
        end_time_str = datetime.utcfromtimestamp(end_time).strftime("%Y-%m-%d %H:%M:%S")

        # Read raw state vectors for the month
        df_month = self.spark.sql(
            f"""
            SELECT * FROM `{self.project}`.`osn_statevectors_v2`
            WHERE (event_time >= TIMESTAMP('{start_time_str}'))
              AND (event_time < TIMESTAMP('{end_time_str}'));
        """
        )

        # Store original column names + new columns we'll add
        original_columns = df_month.columns + ["track_id"] + h3_columns

        # Step 1: Add track ID
        df_month = self._add_track_id(df_month)

        # Step 2: Add H3 encoding
        df_month = self._add_h3_encoding(df_month)

        # Select only the columns we want (drops temporary columns)
        df_month = df_month.select(original_columns)

        # Step 3: Add distance calculations
        df_month = self._add_cumulative_distance(df_month)

        # Step 4: Clean altitude data
        df_month = self._add_clean_altitude(df_month, col_name="geo_altitude")
        df_month = self._add_clean_altitude(df_month, col_name="baro_altitude")

        # Prepare for write: add partition column and repartition
        df_month = df_month.withColumn("event_time_day", to_date(col("event_time")))
        df_month = df_month.repartition("event_time_day").orderBy("event_time_day")

        # Drop partition column (Iceberg will handle it)
        df_month = df_month.drop("event_time_day")

        # Write to Iceberg table
        df_month.writeTo(f"`{self.project}`.`osn_tracks`").append()

        # Clean up memory
        df_month.unpersist(blocking=True)
        self.spark.catalog.clearCache()

        # Mark as processed
        self._mark_month_processed(month)

        print(
            f"Month {month.strftime('%Y-%m')} processing complete. ({datetime.now()})"
        )

    def process_date_range(
        self, start_month: date, end_month: date, skip_if_processed: bool = True
    ) -> None:
        """
        Process tracks for a range of months.

        Args:
            start_month: First month to process (first day of month)
            end_month: Last month to process (first day of month)
            skip_if_processed: If True, skip already processed months

        Example:
            >>> from datetime import date
            >>> processor = TrackProcessor(spark, config)
            >>> processor.process_date_range(
            ...     date(2024, 1, 1),
            ...     date(2024, 3, 1)
            ... )
        """
        from opdi.utils.datetime_helpers import generate_months

        months = generate_months(start_month, end_month)

        print(f"Processing {len(months)} months: {start_month} to {end_month}")

        for month in months:
            self.process_month(month, skip_if_processed=skip_if_processed)

        print(f"Completed processing {len(months)} months.")

    def create_table_if_not_exists(self) -> None:
        """
        Create the osn_tracks Iceberg table if it doesn't exist.

        This should be run once before first track processing.
        """
        from datetime import date

        today = date.today().strftime("%d %B %Y")

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.project}`.`osn_tracks` (
          event_time TIMESTAMP COMMENT 'Timestamp of state vector',
          icao24 STRING COMMENT '24-bit ICAO transponder ID',
          lat DOUBLE COMMENT 'Latitude (degrees)',
          lon DOUBLE COMMENT 'Longitude (degrees)',
          velocity DOUBLE COMMENT 'Ground speed (m/s)',
          heading DOUBLE COMMENT 'Track angle from north (degrees)',
          vert_rate DOUBLE COMMENT 'Vertical rate (m/s)',
          callsign STRING COMMENT 'Aircraft callsign',
          on_ground BOOLEAN COMMENT 'On ground flag',
          alert BOOLEAN COMMENT 'ATC alert flag',
          spi BOOLEAN COMMENT 'ATC SPI flag',
          squawk STRING COMMENT 'Transponder code',
          baro_altitude DOUBLE COMMENT 'Barometric altitude (m)',
          geo_altitude DOUBLE COMMENT 'GNSS altitude (m)',
          last_pos_update DOUBLE COMMENT 'Position age (unix timestamp)',
          last_contact DOUBLE COMMENT 'Last contact (unix timestamp)',
          serials ARRAY<INT> COMMENT 'Receiver serials',
          track_id STRING COMMENT 'Unique track identifier',
          h3_res_7 STRING COMMENT 'H3 index at resolution 7',
          h3_res_12 STRING COMMENT 'H3 index at resolution 12',
          segment_distance_nm DOUBLE COMMENT 'Distance from previous point (NM)',
          cumulative_distance_nm DOUBLE COMMENT 'Total track distance (NM)',
          geo_altitude_c DOUBLE COMMENT 'Cleaned GNSS altitude (m)',
          baro_altitude_c DOUBLE COMMENT 'Cleaned barometric altitude (m)'
        )
        USING iceberg
        PARTITIONED BY (days(event_time))
        COMMENT 'Flight tracks derived from state vectors. Last updated: {today}.'
        """

        self.spark.sql(create_table_sql)
        print(f"Table {self.project}.osn_tracks created/verified.")
