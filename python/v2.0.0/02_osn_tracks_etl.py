!pip install h3==3.7.4
!pip install h3_pyspark

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    lit, lag, when, to_date, concat, avg, abs, col, unix_timestamp, to_timestamp
)
from pyspark.sql import DataFrame
import os
import pandas as pd
from datetime import datetime, date
import dateutil.relativedelta
import calendar
import h3_pyspark
import gc

# Settings
## Config
project = "project_opdi"
h3_resolutions = [7, 12]
start_month = date(2022, 1, 1)

## Which months to process
today = date.today()
end_month = today - dateutil.relativedelta.relativedelta(months=1) # We work on the d-1 months

# Getting today's date formatted
today = today.strftime('%d %B %Y')
#    .config("spark.log.level", "ERROR") \
#    .config("spark.ui.showConsoleProgress", "false") \
# Spark Session Initialization
spark = SparkSession.builder \
    .appName("OPDI Ingestion") \
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
    .config("spark.driver.memory", "5G") \
    .config("spark.executor.memory", "16G") \
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

# Helperfunctions
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

def h3_list_prep(h3_resolutions):
  h3_res = []
  for h3_resolution in h3_resolutions:
      h3_res.append(f"h3_res_{h3_resolution}")
  return h3_res
  

# Distance helper function
def add_cumulative_distance(df: DataFrame) -> DataFrame:
    """
    Calculate the great circle distance between consecutive points in nautical miles
    and the cumulative distance for each track, using native PySpark functions.

    Parameters:
    df (DataFrame): Input Spark DataFrame. Assumes columns "lat", "lon", "track_id", "event_time".

    Returns:
    DataFrame: DataFrame with additional columns "distance_nm" and "cumulative_distance_nm".
    """

    # Define a window spec for lag operation and cumulative sum
    windowSpecLag = Window.partitionBy("track_id").orderBy("event_time")
    windowSpecCumSum = Window.partitionBy("track_id").orderBy("event_time").rowsBetween(Window.unboundedPreceding, 0)

    # Convert degrees to radians using PySpark native function
    df = df.withColumn("lat_rad", F.radians(F.col("lat")))
    df = df.withColumn("lon_rad", F.radians(F.col("lon")))

    # Getting previous row's latitude and longitude
    df = df.withColumn("prev_lat_rad", F.lag("lat_rad").over(windowSpecLag))
    df = df.withColumn("prev_lon_rad", F.lag("lon_rad").over(windowSpecLag))

    # Compute the great circle distance using haversine formula in PySpark native functions
    df = df.withColumn("a", 
                       F.sin((F.col("lat_rad") - F.col("prev_lat_rad")) / 2)**2 + 
                       F.cos(F.col("prev_lat_rad")) * F.cos(F.col("lat_rad")) * 
                       F.sin((F.col("lon_rad") - F.col("prev_lon_rad")) / 2)**2)

    df = df.withColumn("c", 2 * F.atan2(F.sqrt(F.col("a")), F.sqrt(1 - F.col("a"))))

    # Radius of Earth in kilometers is 6371
    df = df.withColumn("distance_km", 6371 * F.col("c"))

    # Convert distance to nautical miles; 1 nautical mile = 1 / 1.852 km
    df = df.withColumn("segment_distance_nm", F.col("distance_km") / 1.852)

    # Calculate the cumulative distance
    df = df.withColumn("cumulative_distance_nm", F.sum("segment_distance_nm").over(windowSpecCumSum))

    # Drop temporary columns used for calculations
    df = df.drop("lat_rad", "lon_rad", "prev_lat_rad", "prev_lon_rad", "a", "c", "distance_km")

    return df

def add_clean_altitude(df, col_name):
    """
    Cleans altitude data by removing unrealistic climb/descent rates.

    Parameters:
    df (DataFrame): The input DataFrame containing altitude and event_time columns.
    col_name (str): The column name for altitude (e.g., "geo_altitude" or "baro_altitude").

    Returns:
    DataFrame: A DataFrame with cleaned altitude values.
    """
    # Define the acceptable rate of climb/descent threshold (25.4 meters per second, i.e. 5000ft/min)
    rate_threshold = 25.4

    # Calculate the rate of climb/descent in meters per second for each track_id
    window_spec = Window.partitionBy("track_id").orderBy("event_time")
    df = df.withColumn(f"prev_{col_name}", lag(col_name).over(window_spec))
    df = df.withColumn("prev_event_time", lag("event_time").over(window_spec))

    # Calculate time difference as seconds
    df = df.withColumn("time_diff", (unix_timestamp("event_time") - unix_timestamp("prev_event_time")).cast("double"))
    df = df.withColumn("altitude_diff", (col(col_name) - col(f"prev_{col_name}")))
    df = df.withColumn("rate_of_climb", col("altitude_diff") / col("time_diff"))

    # Create a window for calculating the rolling average
    time_window = 300  # Define a time window for the rolling average (e.g., 300 seconds or 5 min)
    df = df.withColumn("event_time_epoch", unix_timestamp("event_time").cast("bigint"))
    window_spec_avg = Window.partitionBy("track_id").orderBy("event_time_epoch").rangeBetween(-time_window, time_window)

    # Calculate the rolling average (smoothing)
    df = df.withColumn(f"smoothed_{col_name}", avg(col_name).over(window_spec_avg))

    # Replace unrealistic climb/descent points with the smoothed values
    df = df.withColumn(
        f"{col_name}_c",
        when(abs(col("rate_of_climb")) > rate_threshold, col(f"smoothed_{col_name}")).otherwise(col(col_name)),
    )

    # Drop the temporary columns
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


def process_tracks(project, h3_resolutions, month):
  
  print(f"Processing tracks for month {month}.. ({datetime.now()})")
  h3_res = h3_list_prep(h3_resolutions)
  
  start_time, end_time = get_start_end_of_month(month)
  
  start_time_str = datetime.utcfromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')
  end_time_str = datetime.utcfromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')
    
  
  # Read raw data 
  df_month = spark.sql(f"""
        SELECT * FROM `{project}`.`osn_statevectors` 
        WHERE (event_time >= TIMESTAMP('{start_time_str}')) 
          AND (event_time < TIMESTAMP('{end_time_str}'));
    """)

  # Save the original column names for later and add track_id
  original_columns = df_month.columns + ['track_id'] + h3_res
  
  # ADD TRACK_ID
  ## Add Timestamp
  df_month = df_month.withColumn("event_time_ts", F.to_timestamp("event_time"))
  
  ## Create a SHA value for each icao24, callsign combination and groupby
  df_month = df_month.withColumn("group_id", F.sha2(F.concat_ws("", "icao24", "callsign"), 256))
  windowSpec = Window.partitionBy("group_id").orderBy("event_time_ts")

  ## Calculate the difference between each two timestamps
  ## If there's a difference of more than 30 minutes... Then we divide it in multiple tracks
  df_month = df_month.withColumn("prev_event_time_ts", F.lag(df_month["event_time_ts"]).over(windowSpec))
  df_month = df_month.withColumn("time_gap_minutes", 
                                 (F.unix_timestamp("event_time_ts") - F.unix_timestamp("prev_event_time_ts"))/60)
  track_split_condition = (
      (F.col("time_gap_minutes") > 30) | # When the gap is larger than 30 minutes, we identify it as two tracks.. 
      ((F.col("time_gap_minutes") > 15) & (F.col("baro_altitude") < 1524)) # When the gap is larger than 10 minutes and the baro_altitude is smaller than 1524 m, we identify two tracks because the aircraft operator might not turn off the signal for very long?
  )
  df_month = df_month.withColumn("new_group_flag", F.when(track_split_condition, 1).otherwise(0))
  groupWindowSpec = Window.partitionBy("group_id").orderBy("event_time_ts").rowsBetween(Window.unboundedPreceding, 0)
  df_month = df_month.withColumn("offset", F.sum("new_group_flag").over(groupWindowSpec))

  # We add identifiers for each month change. This is a limitation as flights going over two months will be splitted
  # However, it's necessary as the same icao24, callsign and offset flag might happen in multiple months and thus these would be 
  # Identified as a single flight. We want to split this

  df_month = df_month.withColumn("track_id", F.concat(
    F.col("group_id"), 
    F.lit("_"), 
    F.col("offset"), 
    F.lit("_"), 
    F.year("event_time_ts").cast("string"), 
    F.lit("_"),
    F.month("event_time_ts").cast("string")
  ))

  # ADD H3 TAGS
  for h3_resolution in h3_resolutions:
    df_month = df_month.withColumn("h3_resolution", lit(h3_resolution))
    df_month = df_month.withColumn(f"h3_res_{h3_resolution}", h3_pyspark.geo_to_h3('lat', 'lon', 'h3_resolution'))

  # Drop unnecessary and additional columns to retain only the original ones
  df_month = df_month.select(original_columns)

  # Add the distance variables
  df_month = add_cumulative_distance(df_month)

  # Add the cleaned geo_altitude and baro_altitude
  df_month = add_clean_altitude(df_month, col_name = "geo_altitude")
  df_month = add_clean_altitude(df_month, col_name = "baro_altitude")

  # Prep for insert
  df_month = df_month.withColumn("event_time_day", to_date(col("event_time")))
  df_month = df_month.repartition("event_time_day").orderBy("event_time_day")

  # Drop event_time_day before writing
  df_month = df_month.drop("event_time_day")
    
  # Write data for the month to the database
  df_month.writeTo(f"`{project}`.`osn_tracks`").append()
  
  # Unpersist the DataFrame
  df_month.unpersist(blocking=True)

  # Clear any additional cached data
  spark.catalog.clearCache()
  
  # Clear garbage
  gc.collect()
  
# Actual processing

to_process_months = generate_months(start_month, end_month)

## Load logs
fpath = 'logs/02_osn-tracks-etl-log.parquet'
if os.path.isfile(fpath):
  processed_months = pd.read_parquet(fpath).months.to_list()
else:
  processed_months = []


## Process loop
for month in to_process_months:
  print(f'Processing month: {month}')
  if month in processed_months:
    continue
  else:
    # Process data
    process_tracks(project, h3_resolutions, month)
    
    ## Logging
    processed_months.append(month)
    processed_df = pd.DataFrame({'months':processed_months})
    processed_df.to_parquet(fpath)

# Stop the SparkSession
spark.stop()