"""
Parquet export module for OPDI data extraction.

Provides functionality to export OPDI tables (flight list, events, measurements)
to parquet files with configurable time intervals.
"""

import os
from datetime import date
from typing import List, Tuple, Optional
from dateutil.relativedelta import relativedelta
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F

from opdi.config import OPDIConfig


class ParquetExporter:
    """
    Exports OPDI tables to parquet files.

    Supports different interval strategies:
    - Monthly intervals for flight lists
    - N-day intervals for events and measurements
    """

    def __init__(
        self,
        spark: SparkSession,
        config: OPDIConfig,
        output_dir: str,
        interval_days: int = 10,
    ):
        """
        Initialize parquet exporter.

        Args:
            spark: Active SparkSession
            config: OPDI configuration object
            output_dir: Base directory for output files
            interval_days: Interval size in days for events/measurements
        """
        self.spark = spark
        self.config = config
        self.project = config.project.project_name
        self.output_dir = output_dir
        self.interval_days = interval_days

        # Create output subdirectories
        for subdir in ["flight_list", "flight_events", "measurements"]:
            os.makedirs(os.path.join(output_dir, subdir), exist_ok=True)

    @staticmethod
    def safe_to_pandas(df: DataFrame) -> pd.DataFrame:
        """
        Safely convert Spark DataFrame to pandas by casting timestamps to strings.

        Args:
            df: PySpark DataFrame

        Returns:
            pandas DataFrame

        Example:
            >>> pdf = ParquetExporter.safe_to_pandas(spark_df)
        """
        for name, dtype in df.dtypes:
            if dtype == "timestamp":
                df = df.withColumn(name, col(name).cast("string"))
        return df.toPandas()

    def generate_intervals(
        self,
        start_date: date,
        end_date: date,
        step_days: int,
    ) -> List[Tuple[date, date]]:
        """
        Generate rolling date intervals of fixed length.

        Args:
            start_date: Interval start
            end_date: Interval end
            step_days: Step size in days

        Returns:
            List of (start_date, end_date) tuples

        Example:
            >>> exporter = ParquetExporter(spark, config, "/data/exports")
            >>> intervals = exporter.generate_intervals(
            ...     date(2024, 1, 1),
            ...     date(2024, 1, 31),
            ...     10
            ... )
            >>> print(len(intervals))  # 4 intervals
        """
        intervals = []
        current_start = start_date
        step = relativedelta(days=step_days)

        while current_start < end_date:
            current_end = min(current_start + step, end_date)
            intervals.append((current_start, current_end))
            current_start = current_end

        return intervals

    def generate_month_intervals(
        self,
        start_date: date,
        end_date: date,
    ) -> List[Tuple[date, date]]:
        """
        Generate full-month intervals.

        The first interval begins at the first day of start_date's month.
        The last interval ends at the first day of the month containing end_date,
        or at end_date if end_date is exactly on a month boundary.

        Args:
            start_date: Start date
            end_date: End date (exclusive upper bound)

        Returns:
            List of (month_start, month_end) tuples

        Example:
            >>> intervals = exporter.generate_month_intervals(
            ...     date(2024, 1, 15),
            ...     date(2024, 3, 10)
            ... )
            >>> # Returns: [(2024-01-01, 2024-02-01), (2024-02-01, 2024-03-01), (2024-03-01, 2024-04-01)]
        """
        if end_date <= start_date:
            return []

        # Align to first of month
        current = date(start_date.year, start_date.month, 1)

        # If end_date is on a month boundary, stop at end_date
        # Otherwise stop at first of the next month after end_date's month
        end_month_start = date(end_date.year, end_date.month, 1)
        if end_date == end_month_start:
            stop = end_date
        else:
            stop = end_month_start + relativedelta(months=1)

        intervals = []
        while current < stop:
            nxt = current + relativedelta(months=1)
            intervals.append((current, min(nxt, stop)))
            current = nxt

        return intervals

    def export_flight_list(
        self,
        month_start: date,
        month_end: date,
        version: str = "v0.0.2",
        overwrite: bool = False,
    ) -> Optional[str]:
        """
        Export OPDI flight list for a monthly interval.

        Files are written to:
            {output_dir}/flight_list/flight_list_YYYYMM.parquet

        Args:
            month_start: Month interval start (1st of month)
            month_end: Month interval end (1st of next month or stop)
            version: Version string to add to records
            overwrite: Whether to overwrite existing files

        Returns:
            Path to output file if created, None if skipped

        Example:
            >>> exporter.export_flight_list(
            ...     date(2024, 1, 1),
            ...     date(2024, 2, 1),
            ...     version="v0.0.2"
            ... )
        """
        file_start = month_start.strftime("%Y%m")
        file_path = os.path.join(self.output_dir, "flight_list", f"flight_list_{file_start}.parquet")

        if os.path.isfile(file_path) and not overwrite:
            print(f"Skipping FLIGHT_LIST {file_start} (already exists)")
            return None

        print(f"Extracting FLIGHT_LIST {file_start}")

        sql = f"""
        SELECT
            id,
            icao24,
            flt_id,
            dof,
            adep,
            ades,
            adep_p,
            ades_p,
            registration,
            model,
            typecode,
            icao_aircraft_class,
            icao_operator,
            first_seen,
            last_seen,
            version
        FROM `{self.project}`.`opdi_flight_list`
        WHERE first_seen >= TO_DATE('{month_start}')
          AND first_seen < TO_DATE('{month_end}')
        """

        df = self.spark.sql(sql).withColumn("version", lit(version))
        df = df.withColumn("id", F.xxhash64("id"))
        pdf = self.safe_to_pandas(df)

        if "first_seen" in pdf.columns:
            pdf = pdf.sort_values("first_seen").reset_index(drop=True)

        pdf.to_parquet(file_path)
        print(f"  Saved {len(pdf):,} flights to {file_path}")

        return file_path

    def export_flight_events(
        self,
        start_date: date,
        end_date: date,
        version: str = "v0.0.2",
        overwrite: bool = False,
    ) -> Optional[str]:
        """
        Export OPDI flight events for a date interval.

        Files are written to:
            {output_dir}/flight_events/flight_events_YYYYMMDD_YYYYMMDD.parquet

        Args:
            start_date: Interval start
            end_date: Interval end
            version: Version string to add to records
            overwrite: Whether to overwrite existing files

        Returns:
            Path to output file if created, None if skipped
        """
        file_start = start_date.strftime("%Y%m%d")
        file_end = end_date.strftime("%Y%m%d")
        file_path = os.path.join(
            self.output_dir,
            "flight_events",
            f"flight_events_{file_start}_{file_end}.parquet"
        )

        if os.path.isfile(file_path) and not overwrite:
            print(f"Skipping EVENT {file_start} → {file_end} (already exists)")
            return None

        print(f"Extracting EVENT {file_start} → {file_end}")

        sql = f"""
        WITH flight_list AS (
            SELECT id AS track_id
            FROM `{self.project}`.`opdi_flight_list`
            WHERE first_seen >= TO_DATE('{start_date}')
              AND first_seen < TO_DATE('{end_date}')
        )
        SELECT *
        FROM `{self.project}`.`opdi_flight_events`
        WHERE flight_id IN (SELECT track_id FROM flight_list)
        """

        df = self.spark.sql(sql).withColumn("version", lit(version))
        df = df.withColumn("id", F.xxhash64("id"))
        df = df.withColumn("flight_id", F.xxhash64('flight_id'))
        pdf = self.safe_to_pandas(df)

        pdf.to_parquet(file_path)
        print(f"  Saved {len(pdf):,} events to {file_path}")

        return file_path

    def export_measurements(
        self,
        start_date: date,
        end_date: date,
        version: str = "v0.0.2",
        overwrite: bool = False,
    ) -> Optional[str]:
        """
        Export OPDI measurements for a date interval.

        Files are written to:
            {output_dir}/measurements/measurements_YYYYMMDD_YYYYMMDD.parquet

        Args:
            start_date: Interval start
            end_date: Interval end
            version: Version string to add to records
            overwrite: Whether to overwrite existing files

        Returns:
            Path to output file if created, None if skipped
        """
        file_start = start_date.strftime("%Y%m%d")
        file_end = end_date.strftime("%Y%m%d")
        file_path = os.path.join(
            self.output_dir,
            "measurements",
            f"measurements_{file_start}_{file_end}.parquet"
        )

        if os.path.isfile(file_path) and not overwrite:
            print(f"Skipping MEASUREMENT {file_start} → {file_end} (already exists)")
            return None

        print(f"Extracting MEASUREMENT {file_start} → {file_end}")

        sql = f"""
        WITH flight_list AS (
            SELECT id AS track_id
            FROM `{self.project}`.`opdi_flight_list`
            WHERE first_seen >= TO_DATE('{start_date}')
              AND first_seen < TO_DATE('{end_date}')
        ),
        event_table AS (
            SELECT id
            FROM `{self.project}`.`opdi_flight_events`
            WHERE flight_id IN (SELECT track_id FROM flight_list)
        )
        SELECT *
        FROM `{self.project}`.`opdi_measurements`
        WHERE milestone_id IN (SELECT id FROM event_table)
        """

        df = (
            self.spark.sql(sql)
            .withColumnRenamed("milestone_id", "event_id")
            .withColumn("version", lit(version))
        )

        df = df.withColumn("id", F.xxhash64("id"))
        df = df.withColumn("event_id", F.xxhash64('event_id'))

        pdf = self.safe_to_pandas(df)
        pdf.to_parquet(file_path)
        print(f"  Saved {len(pdf):,} measurements to {file_path}")

        return file_path

    def export_all(
        self,
        start_date: date,
        end_date: date,
        export_flight_list: bool = True,
        export_flight_events: bool = True,
        export_measurements: bool = True,
        version: str = "v0.0.2",
        overwrite: bool = False,
        last_n_months: Optional[int] = None,
    ) -> dict:
        """
        Export all OPDI tables for a date range.

        Args:
            start_date: Export start date
            end_date: Export end date
            export_flight_list: Whether to export flight list
            export_flight_events: Whether to export flight events
            export_measurements: Whether to export measurements
            version: Version string to add to records
            overwrite: Whether to overwrite existing files
            last_n_months: If set, only export last N months

        Returns:
            Dictionary with statistics:
            {
                'flight_list_files': int,
                'event_files': int,
                'measurement_files': int
            }

        Example:
            >>> from datetime import date
            >>> exporter = ParquetExporter(spark, config, "/data/exports")
            >>> stats = exporter.export_all(
            ...     date(2024, 1, 1),
            ...     date(2024, 3, 1),
            ...     last_n_months=2
            ... )
            >>> print(f"Exported {stats['flight_list_files']} flight list files")
        """
        # Apply last N months filter
        if last_n_months:
            cutoff_date = date.today() - relativedelta(months=last_n_months)
            if start_date < cutoff_date:
                start_date = cutoff_date

        stats = {
            'flight_list_files': 0,
            'event_files': 0,
            'measurement_files': 0,
        }

        # Export flight lists (monthly intervals)
        if export_flight_list:
            month_intervals = self.generate_month_intervals(start_date, end_date)
            if last_n_months:
                month_intervals = [
                    (s, e) for s, e in month_intervals
                    if e > cutoff_date
                ]

            print(f"\nExporting {len(month_intervals)} flight list files...")
            for month_start, month_end in month_intervals:
                result = self.export_flight_list(month_start, month_end, version, overwrite)
                if result:
                    stats['flight_list_files'] += 1

        # Export events and measurements (N-day intervals)
        day_intervals = self.generate_intervals(start_date, end_date, self.interval_days)
        if last_n_months:
            day_intervals = [
                (s, e) for s, e in day_intervals
                if e > cutoff_date
            ]

        if export_flight_events:
            print(f"\nExporting {len(day_intervals)} flight event files...")
            for interval_start, interval_end in day_intervals:
                result = self.export_flight_events(interval_start, interval_end, version, overwrite)
                if result:
                    stats['event_files'] += 1

        if export_measurements:
            print(f"\nExporting {len(day_intervals)} measurement files...")
            for interval_start, interval_end in day_intervals:
                result = self.export_measurements(interval_start, interval_end, version, overwrite)
                if result:
                    stats['measurement_files'] += 1

        print("\n" + "=" * 60)
        print("EXPORT SUMMARY")
        print("=" * 60)
        print(f"Flight list files:     {stats['flight_list_files']}")
        print(f"Event files:           {stats['event_files']}")
        print(f"Measurement files:     {stats['measurement_files']}")
        print("=" * 60)

        return stats
