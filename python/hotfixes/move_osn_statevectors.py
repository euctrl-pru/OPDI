from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, from_unixtime
from datetime import datetime, date, timedelta
import subprocess
import os, shutil, time
import os.path
import dateutil.relativedelta
import calendar
import pandas as pd


# Settings
## Config
project = "project_opdi"
start_month = date(2022, 1, 1)
import_data = True
move_data = True

## Which months to process
today = date.today()
end_month = today - dateutil.relativedelta.relativedelta(months=1) # We work on the d-1 months

# Getting today's date formatted
today = today.strftime('%d %B %Y')

# Spark Session Initialization
#shutil.copy("/runtime-addons/cmladdon-2.0.40-b150/log4j.properties", "/etc/spark/conf/") # Setting logging properties
spark = SparkSession.builder \
    .appName("OPDI Ingestion") \
    .config("spark.log.level", "ERROR") \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.hadoop.fs.azure.ext.cab.required.group", "eur-app-opdi") \
    .config("spark.kerberos.access.hadoopFileSystems", "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged") \
    .config("spark.executor.extraClassPath", "/opt/spark/optional-lib/iceberg-spark-runtime-3.3_2.12-1.3.1.1.20.7216.0-70.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark/optional-lib/iceberg-spark-runtime-3.3_2.12-1.3.1.1.20.7216.0-70.jar") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged") \
    .config("spark.driver.cores", "1") \
    .config("spark.driver.memory", "8G") \
    .config("spark.executor.memory", "12G") \
    .config("spark.executor.memoryOverhead", "3G") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "3") \
    .config("spark.dynamicAllocation.maxExecutors", "4") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "400s") \
    .config("spark.driver.maxResultSize", "6g") \
    .config("spark.shuffle.compress", "true") \
    .config("spark.shuffle.spill.compress", "true") \
    .enableHiveSupport() \
    .getOrCreate()

def generate_months(start_date, end_date):
    """Generate a list of dates corresponding to the first day of each month between two dates.

    Args:
    start_date (datetime.date): The starting date.
    end_date (datetime.date): The ending date.

    Returns:
    list: A list of date objects for the first day of each month within the specified range.
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

def get_start_end_of_month(date):
    """Return a datetime object for the first and last second  of the given month and year."""
    year = date.year
    month = date.month
    
    first_second = datetime(year, month, 1, 0, 0, 0)
    last_day = calendar.monthrange(year, month)[1]
    last_second = datetime(year, month, last_day, 23, 59, 59)
    return first_second.timestamp(), last_second.timestamp()

def move_data(project, month):
    print(f"Adding statevectors for {project}.osn_statevectors to {project}.osn_statevectors_v2 table for month {month}")

    start_time, end_time = get_start_end_of_month(month)

    df = spark.table(f"{project}.osn_statevectors")
    df = df.filter(
        (df.event_time >= from_unixtime(lit(start_time)).cast("timestamp")) &
        (df.event_time < from_unixtime(lit(end_time)).cast("timestamp"))
    )

    # Add event_time_day column derived from event_time        
    df_with_partition = df.withColumn("event_time_day", to_date(col("event_time")))
    df_partitioned = df_with_partition.repartition("event_time_day").orderBy("event_time_day")
    
    # Drop event_time_day before writing
    df_cleaned = df_partitioned.drop("event_time_day")
    
    # Write the data for the month
    df_cleaned.writeTo(f"`{project}`.`osn_statevectors_v2`").append()
    

if move_data:

  #spark.sql(create_clustered_db)

  to_process_months = generate_months(start_month, end_month)

  ## Load logs
  fpath = 'logs/01_osn-statevectors-moving-to-v2.parquet'
  if os.path.isfile(fpath):
    processed_months = pd.read_parquet(fpath).months.to_list()
  else:
    processed_months = []


  ## Process loop
  for month in to_process_months:
    print(f'Processing month: {month}')
    if month in processed_months:
      print(f'Already processed {month}')
      continue
    else:
      move_data(project, month)
      processed_months.append(month)

      ## Logging
      processed_df = pd.DataFrame({'months':processed_months})
      processed_df.to_parquet(fpath)

# Stop the SparkSession
spark.stop()
