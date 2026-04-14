"""
Date and time utility functions for OPDI pipeline.

Provides functions for generating date ranges, converting dates to Unix timestamps,
and filtering Spark DataFrames by time windows.
"""

from datetime import datetime, date, timedelta
from typing import List, Tuple
import calendar
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def generate_months(start_date: date, end_date: date) -> List[date]:
    """
    Generate a list of dates corresponding to the first day of each month between two dates.

    Args:
        start_date: The starting date
        end_date: The ending date

    Returns:
        List of date objects for the first day of each month within the specified range

    Example:
        >>> generate_months(date(2024, 1, 1), date(2024, 3, 1))
        [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)]
    """
    current = start_date
    months = []
    while current <= end_date:
        months.append(current)
        # Increment month
        month = current.month
        year = current.year
        if month == 12:
            current = date(year + 1, 1, 1)
        else:
            current = date(year, month + 1, 1)
    return months


def generate_intervals(
    start_date: date, end_date: date, step_days: int = 10
) -> List[Tuple[date, date]]:
    """
    Generate a list of date intervals with specified step size.

    Args:
        start_date: The starting date
        end_date: The ending date
        step_days: Number of days in each interval (default: 10)

    Returns:
        List of (start, end) date tuples representing intervals

    Example:
        >>> generate_intervals(date(2024, 1, 1), date(2024, 1, 25), step_days=10)
        [(date(2024, 1, 1), date(2024, 1, 10)),
         (date(2024, 1, 11), date(2024, 1, 20)),
         (date(2024, 1, 21), date(2024, 1, 25))]
    """
    intervals = []
    current_start = start_date

    while current_start <= end_date:
        current_end = min(current_start + timedelta(days=step_days - 1), end_date)
        intervals.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)

    return intervals


def get_start_end_of_month(dt: date) -> Tuple[float, float]:
    """
    Return Unix timestamps for the first and last second of the given month.

    Args:
        dt: Date object for the desired month

    Returns:
        Tuple of (first_second_timestamp, last_second_timestamp) as floats

    Example:
        >>> get_start_end_of_month(date(2024, 1, 1))
        (1704067200.0, 1706745599.0)  # Jan 1 00:00:00 to Jan 31 23:59:59
    """
    year = dt.year
    month = dt.month

    first_second = datetime(year, month, 1, 0, 0, 0)
    last_day = calendar.monthrange(year, month)[1]
    last_second = datetime(year, month, last_day, 23, 59, 59)

    return first_second.timestamp(), last_second.timestamp()


def get_data_within_timeframe(
    spark: SparkSession,
    table_name: str,
    month: date,
    time_col: str = "event_time",
    unix_time: bool = True,
) -> DataFrame:
    """
    Retrieve records from a Spark table within the given monthly timeframe.

    Args:
        spark: Active SparkSession object
        table_name: Name of the Spark table to query
        month: Start date of the month (first day)
        time_col: Name of the time column to filter on (default: 'event_time')
        unix_time: Whether the time column is already in Unix timestamp format (default: True)

    Returns:
        DataFrame containing the records within the specified timeframe

    Example:
        >>> df = get_data_within_timeframe(spark, "project_opdi.osn_tracks", date(2024, 1, 1))
        >>> df.count()  # Returns number of tracks in January 2024
    """
    # Convert dates to POSIX time (seconds since epoch)
    start_posix, stop_posix = get_start_end_of_month(month)

    # Load the table
    df = spark.table(table_name)

    if unix_time:
        # Filter records based on time column that's already in Unix format
        filtered_df = df.filter((F.col(time_col) >= start_posix) & (F.col(time_col) < stop_posix))
    else:
        # Convert timestamp to Unix time first, then filter
        df = df.withColumn("unix_time", F.unix_timestamp(time_col))
        filtered_df = df.filter(
            (F.col("unix_time") >= start_posix) & (F.col("unix_time") < stop_posix)
        )

    return filtered_df


def get_data_within_interval(
    spark: SparkSession,
    table_name: str,
    start_date: date,
    end_date: date,
    time_col: str = "event_time",
    unix_time: bool = True,
) -> DataFrame:
    """
    Retrieve records from a Spark table within a specific date interval.

    Args:
        spark: Active SparkSession object
        table_name: Name of the Spark table to query
        start_date: Start date of the interval (inclusive)
        end_date: End date of the interval (inclusive)
        time_col: Name of the time column to filter on (default: 'event_time')
        unix_time: Whether the time column is already in Unix timestamp format (default: True)

    Returns:
        DataFrame containing the records within the specified interval

    Example:
        >>> df = get_data_within_interval(
        ...     spark, "project_opdi.flight_events",
        ...     date(2024, 1, 1), date(2024, 1, 10)
        ... )
    """
    # Convert dates to Unix timestamps (start of start_date, end of end_date)
    start_timestamp = datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0).timestamp()
    end_timestamp = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59).timestamp()

    # Load the table
    df = spark.table(table_name)

    if unix_time:
        # Filter records based on time column that's already in Unix format
        filtered_df = df.filter(
            (F.col(time_col) >= start_timestamp) & (F.col(time_col) <= end_timestamp)
        )
    else:
        # Convert timestamp to Unix time first, then filter
        df = df.withColumn("unix_time", F.unix_timestamp(time_col))
        filtered_df = df.filter(
            (F.col("unix_time") >= start_timestamp) & (F.col("unix_time") <= end_timestamp)
        )

    return filtered_df
