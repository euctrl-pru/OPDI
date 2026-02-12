from IPython.display import display, HTML
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

import os
import sys
from datetime import date

from dateutil.relativedelta import relativedelta

import pandas as pd
import numpy as np

# ------------------------------------------------------------------
# NumPy compatibility fix for PySpark pandas conversion
# ------------------------------------------------------------------
if not hasattr(np, "object0"):
    np.object0 = np.object_

# ------------------------------------------------------------------
# Add python helper folder
# ------------------------------------------------------------------
sys.path.append("/home/cdsw/OPDI_live/python/v2.0.0/")
from helperfunctions import *  # noqa: F401,F403


# ------------------------------------------------------------------
# SETTINGS
# ------------------------------------------------------------------
project = "project_opdi"

extract_flight_list = True
extract_flight_events = True
extract_measurements = True

export_version = "v002"

v_long_flist = "v0.0.2"
v_long_events = "v0.0.2"
v_long_measures = "v0.0.2"

# ------------------------------------------------------------------
# DRY RUN FLAG
# ------------------------------------------------------------------
DRY_RUN = False  # SET TO FALSE TO EXECUTE

# ------------------------------------------------------------------
# INTERVAL CONFIGURATION
# ------------------------------------------------------------------
interval_start = date(2022, 1, 1)
interval_end = date.today()
interval_days = 10

# ------------------------------------------------------------------
# LAST X MONTHS FILTER
# ------------------------------------------------------------------
LAST_X_MONTHS = 4
cutoff_date = date.today() - relativedelta(months=LAST_X_MONTHS)

# ------------------------------------------------------------------
# OUTPUT PATHS
# ------------------------------------------------------------------
opath = f"/home/cdsw/OPDI_live/data/OPDI/{export_version}"

for subdir in ["flight_list", "flight_events", "measurements"]:
    os.makedirs(os.path.join(opath, subdir), exist_ok=True)

# ------------------------------------------------------------------
# SPARK SESSION
# ------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("OPDI extraction")
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.hadoop.fs.azure.ext.cab.required.group", "eur-app-opdi")
    .config(
        "spark.kerberos.access.hadoopFileSystems",
        "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged",
    )
    .config(
        "spark.executor.extraClassPath",
        "/opt/spark/optional-lib/iceberg-spark-runtime-3.3_2.12-1.3.1.1.20.7216.0-70.jar",
    )
    .config(
        "spark.driver.extraClassPath",
        "/opt/spark/optional-lib/iceberg-spark-runtime-3.3_2.12-1.3.1.1.20.7216.0-70.jar",
    )
    .config("spark.sql.catalog.spark_catalog.type", "hive")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
    .config(
        "spark.sql.catalog.spark_catalog.warehouse",
        "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged",
    )
    .config("spark.driver.cores", "1")
    .config("spark.driver.memory", "14G")
    .config("spark.executor.memory", "8G")
    .config("spark.executor.memoryOverhead", "3G")
    .config("spark.executor.cores", "2")
    .config("spark.executor.instances", "3")
    .config("spark.dynamicAllocation.maxExecutors", "15")
    .config("spark.network.timeout", "800s")
    .config("spark.executor.heartbeatInterval", "400s")
    .config("spark.driver.maxResultSize", "6g")
    .config("spark.shuffle.compress", "true")
    .config("spark.shuffle.spill.compress", "true")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .enableHiveSupport()
    .getOrCreate()
)

# Get environment variables
engine_id = os.getenv("CDSW_ENGINE_ID")
domain = os.getenv("CDSW_DOMAIN")

# Format the URL
url = f"https://spark-{engine_id}.{domain}"

# Display the clickable URL
print(HTML(f'<a href="{url}">{url}</a>'))

# ------------------------------------------------------------------
# SAFE Spark → Pandas conversion
# ------------------------------------------------------------------
def safe_to_pandas(df):
    """
    Safely convert a Spark DataFrame to a Pandas DataFrame by casting timestamp
    columns to string before calling toPandas().

    Args:
        df: PySpark DataFrame.

    Returns:
        pandas.DataFrame
    """
    for name, dtype in df.dtypes:
        if dtype == "timestamp":
            df = df.withColumn(name, col(name).cast("string"))
    return df.toPandas()


# ------------------------------------------------------------------
# INTERVAL GENERATION (10-day) FOR EVENTS/MEASUREMENTS
# ------------------------------------------------------------------
def generate_intervals(start_date, end_date, step_days):
    """
    Generate rolling date intervals of fixed length.

    Args:
        start_date: Interval start as datetime.date.
        end_date: Interval end as datetime.date.
        step_days: Step size in days.

    Returns:
        List of (start_date, end_date) tuples.
    """
    intervals = []
    current_start = start_date
    step = relativedelta(days=step_days)

    while current_start < end_date:
        current_end = min(current_start + step, end_date)
        intervals.append((current_start, current_end))
        current_start = current_end

    return intervals


# ------------------------------------------------------------------
# MONTH INTERVAL GENERATION FOR FLIGHT LIST
# ------------------------------------------------------------------
def generate_month_intervals(start_date, end_date):
    """
    Generate full-month intervals [month_start, next_month_start) that cover the
    time span from start_date to end_date.

    The first interval begins at the first day of start_date's month.
    The last interval ends at the first day of the month containing end_date,
    or at end_date if end_date is exactly on a month boundary.

    Args:
        start_date: Start date.
        end_date: End date (exclusive upper bound for extraction).

    Returns:
        List of (month_start, month_end) tuples.
    """
    if end_date <= start_date:
        return []

    # align to first of month
    current = date(start_date.year, start_date.month, 1)

    # If end_date is on a month boundary, we stop at end_date; otherwise we
    # stop at first of the next month after end_date's month.
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


# ------------------------------------------------------------------
# BUILD INTERVALS
# ------------------------------------------------------------------
intervals_10d = generate_intervals(
    start_date=interval_start,
    end_date=interval_end,
    step_days=interval_days,
)
intervals_10d = [(s, e) for s, e in intervals_10d if e > cutoff_date]

intervals_months = generate_month_intervals(
    start_date=interval_start,
    end_date=interval_end,
)
intervals_months = [(s, e) for s, e in intervals_months if e > cutoff_date]

# ------------------------------------------------------------------
# DRY-RUN PRINTOUT
# ------------------------------------------------------------------
print("\n========== DRY RUN INTERVAL CHECK ==========\n")
print(f"Cutoff date (last {LAST_X_MONTHS} months): {cutoff_date}\n")

print("---- Flight list (MONTH intervals) ----\n")
for s, e in intervals_months:
    fs = s.strftime("%Y%m%d")
    fe = e.strftime("%Y%m%d")
    print(f"{s:%B %d, %Y} - {e:%B %d, %Y}")
    print(f"  flight_list_{fs}_{fe}.parquet\n")

print("---- Flight events / measurements (10-DAY intervals) ----\n")
for s, e in intervals_10d:
    fs = s.strftime("%Y%m%d")
    fe = e.strftime("%Y%m%d")
    print(f"{s:%B %d, %Y} - {e:%B %d, %Y}")
    print(f"  flight_events_{fs}_{fe}.parquet")
    print(f"  measurements_{fs}_{fe}.parquet\n")

print("===========================================\n")

if DRY_RUN:
    print("DRY_RUN=True → No Spark jobs executed.")

# ------------------------------------------------------------------
# FLIGHT LIST EXTRACTION (FULL MONTHS)
# ------------------------------------------------------------------
def process_and_save_flight_list(month_start, month_end):
    """
    Extract OPDI flight list records for a given monthly interval and save to parquet.

    Files are written to:
        {opath}/flight_list/flight_list_YYYYMMDD_YYYYMMDD.parquet

    Args:
        month_start: Month interval start as datetime.date (1st of month).
        month_end: Month interval end as datetime.date (1st of next month or stop).
    """
    file_start = month_start.strftime("%Y%m%d")
    file_end = month_end.strftime("%Y%m%d")
    file_path = f"{opath}/flight_list/flight_list_{file_start}_{file_end}.parquet"

    if os.path.isfile(file_path):
        print(f"Skipping FLIGHT_LIST {file_start} → {file_end}")
        return

    print(f"Extracting FLIGHT_LIST {file_start} → {file_end}")

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
    FROM `{project}`.`opdi_flight_list`
    WHERE first_seen >= TO_DATE('{month_start}')
      AND first_seen < TO_DATE('{month_end}')
    """

    df = spark.sql(sql).withColumn("version", lit(v_long_flist))
    pdf = safe_to_pandas(df)

    if "first_seen" in pdf.columns:
        pdf = pdf.sort_values("first_seen").reset_index(drop=True)

    pdf.to_parquet(file_path)


if not DRY_RUN and extract_flight_list:
    for s, e in intervals_months:
        process_and_save_flight_list(s, e)

# ------------------------------------------------------------------
# EVENT TABLE EXTRACTION (10-DAY)
# ------------------------------------------------------------------
def process_and_save_events(start_date, end_date):
    """
    Extract OPDI flight events for a given date interval (based on flight_list ids)
    and save to parquet.
    """
    file_start = start_date.strftime("%Y%m%d")
    file_end = end_date.strftime("%Y%m%d")
    file_path = f"{opath}/flight_events/flight_events_{file_start}_{file_end}.parquet"

    if os.path.isfile(file_path):
        print(f"Skipping EVENT {file_start} → {file_end}")
        return

    print(f"Extracting EVENT {file_start} → {file_end}")

    sql = f"""
    WITH flight_list AS (
        SELECT id AS track_id
        FROM `{project}`.`opdi_flight_list`
        WHERE first_seen >= TO_DATE('{start_date}')
          AND first_seen < TO_DATE('{end_date}')
    )
    SELECT *
    FROM `{project}`.`opdi_flight_events`
    WHERE flight_id IN (SELECT track_id FROM flight_list)
    """

    df = spark.sql(sql).withColumn("version", lit(v_long_events))
    safe_to_pandas(df).to_parquet(file_path)


if not DRY_RUN and extract_flight_events:
    for s, e in intervals_10d:
        process_and_save_events(s, e)

# ------------------------------------------------------------------
# MEASUREMENT TABLE EXTRACTION (10-DAY)
# ------------------------------------------------------------------
def process_and_save_measurements(start_date, end_date):
    """
    Extract OPDI measurements for a given date interval (based on events linked
    to flights in the interval) and save to parquet.
    """
    file_start = start_date.strftime("%Y%m%d")
    file_end = end_date.strftime("%Y%m%d")
    file_path = f"{opath}/measurements/measurements_{file_start}_{file_end}.parquet"

    if os.path.isfile(file_path):
        print(f"Skipping MEASUREMENT {file_start} → {file_end}")
        return

    print(f"Extracting MEASUREMENT {file_start} → {file_end}")

    sql = f"""
    WITH flight_list AS (
        SELECT id AS track_id
        FROM `{project}`.`opdi_flight_list`
        WHERE first_seen >= TO_DATE('{start_date}')
          AND first_seen < TO_DATE('{end_date}')
    ),
    event_table AS (
        SELECT id
        FROM `{project}`.`opdi_flight_events`
        WHERE flight_id IN (SELECT track_id FROM flight_list)
    )
    SELECT *
    FROM `{project}`.`opdi_measurements`
    WHERE milestone_id IN (SELECT id FROM event_table)
    """

    df = (
        spark.sql(sql)
        .withColumnRenamed("milestone_id", "event_id")
        .withColumn("version", lit(v_long_measures))
    )

    safe_to_pandas(df).to_parquet(file_path)


if not DRY_RUN and extract_measurements:
    for s, e in intervals_10d:
        process_and_save_measurements(s, e)
