"""
Shared utility functions for the OPDI pipeline.

Includes date/time helpers, Spark session builder, and common configuration.
"""

from __future__ import annotations

import calendar
from datetime import date, datetime
from typing import Optional

from pyspark.sql import SparkSession, functions as F


# ---------------------------------------------------------------------------
# Date / time helpers
# ---------------------------------------------------------------------------

def generate_months(start_date: date, end_date: date) -> list[date]:
    """Generate first-of-month dates between *start_date* and *end_date* (inclusive)."""
    current = start_date
    months: list[date] = []
    while current <= end_date:
        months.append(current)
        month = current.month
        year = current.year
        if month == 12:
            current = date(year + 1, 1, 1)
        else:
            current = date(year, month + 1, 1)
    return months


def get_start_end_of_month(d: date) -> tuple[float, float]:
    """Return POSIX timestamps for the first and last second of *d*'s month."""
    year, month = d.year, d.month
    first_second = datetime(year, month, 1, 0, 0, 0)
    last_day = calendar.monthrange(year, month)[1]
    last_second = datetime(year, month, last_day, 23, 59, 59)
    return first_second.timestamp(), last_second.timestamp()


def get_data_within_timeframe(
    spark: SparkSession,
    table_name: str,
    month: date,
    time_col: str = "event_time",
    unix_time: bool = False,
):
    """
    Load a Spark table and filter to the calendar month of *month*.

    Parameters
    ----------
    spark : SparkSession
    table_name : str
        Fully-qualified Spark table name.
    month : date
        Any date within the target month (typically the 1st).
    time_col : str
        Column that carries the timestamp.
    unix_time : bool
        If True, the column contains Unix epoch seconds (bigint).
        If False, the column is a Spark timestamp.
    """
    start_posix, stop_posix = get_start_end_of_month(month)
    df = spark.table(table_name)

    if unix_time:
        return df.filter(
            (F.col(time_col) >= start_posix) & (F.col(time_col) < stop_posix)
        )
    else:
        from pyspark.sql.functions import lit, to_timestamp

        start_ts = to_timestamp(lit(start_posix))
        end_ts = to_timestamp(lit(stop_posix))
        return df.filter(
            (F.col(time_col) >= start_ts) & (F.col(time_col) < end_ts)
        )


# ---------------------------------------------------------------------------
# Spark session builder
# ---------------------------------------------------------------------------

# Default Spark configuration tuned for the OPDI workloads on CDP.
DEFAULT_SPARK_CONFIG: dict[str, str] = {
    "spark.hadoop.fs.azure.ext.cab.required.group": "eur-app-opdi",
    "spark.kerberos.access.hadoopFileSystems": (
        "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged"
    ),
    "spark.executor.extraClassPath": (
        "/opt/spark/optional-lib/iceberg-spark-runtime-3.3_2.12-1.3.1.1.20.7216.0-70.jar"
    ),
    "spark.driver.extraClassPath": (
        "/opt/spark/optional-lib/iceberg-spark-runtime-3.3_2.12-1.3.1.1.20.7216.0-70.jar"
    ),
    "spark.sql.catalog.spark_catalog.type": "hive",
    "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
    "spark.sql.iceberg.handle-timestamp-without-timezone": "true",
    "spark.sql.catalog.spark_catalog.warehouse": (
        "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged"
    ),
    "spark.driver.cores": "1",
    "spark.driver.memory": "8G",
    "spark.executor.memory": "12G",
    "spark.executor.memoryOverhead": "3G",
    "spark.executor.cores": "2",
    "spark.executor.instances": "3",
    "spark.dynamicAllocation.maxExecutors": "15",
    "spark.network.timeout": "800s",
    "spark.executor.heartbeatInterval": "400s",
    "spark.driver.maxResultSize": "6g",
    "spark.shuffle.compress": "true",
    "spark.shuffle.spill.compress": "true",
    "spark.ui.showConsoleProgress": "false",
}


def build_spark_session(
    app_name: str = "OPDI",
    extra_config: Optional[dict[str, str]] = None,
) -> SparkSession:
    """
    Create a SparkSession with OPDI defaults.

    Parameters
    ----------
    app_name : str
        Spark application name.
    extra_config : dict, optional
        Additional config entries (will override defaults).
    """
    merged = {**DEFAULT_SPARK_CONFIG, **(extra_config or {})}

    builder = SparkSession.builder.appName(app_name)
    for key, value in merged.items():
        builder = builder.config(key, value)

    return builder.enableHiveSupport().getOrCreate()
