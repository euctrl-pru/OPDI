!pip install h3
!pip install h3_pyspark

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
import shutil
from datetime import datetime, date
import dateutil.relativedelta
import calendar
import h3_pyspark
import os
import pandas as pd
from datetime import datetime, date
import dateutil.relativedelta
import calendar

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

# Settings
## Config
project = "project_opdi"
max_h3_resolution = 12
start_month = date(2022, 1, 1)

## Which months to process
today = date.today()
end_month = today - dateutil.relativedelta.relativedelta(months=1) # We work on the d-1 months

# Getting today's date formatted
today = today.strftime('%d %B %Y')

# Spark Session Initialization
spark = SparkSession.builder \
    .appName("OSN Tracks Calculation") \
    .config("spark.log.level", "ERROR") \
    .config("spark.hadoop.fs.azure.ext.cab.required.group", "eur-app-opdi") \
    .config("spark.kerberos.access.hadoopFileSystems", "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged") \
    .config("spark.sql.shuffle.partitions", "2000") \
    .config("spark.default.parallelism", "2000") \
    .config("spark.driver.cores", "1") \
    .config("spark.driver.memory", "12G") \
    .config("spark.executor.memory", "10G") \
    .config("spark.executor.memoryOverhead", "3G") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.dynamicAllocation.minExecutors", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "5") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "400s") \
    .enableHiveSupport() \
    .getOrCreate()

# Helperfunctions

def h3_query_prep(project, max_h3_resolution):
  # Create OSN tracks db
  h3_resolution = max_h3_resolution
  h3_res_sql = ""
  h3_res = []

  while h3_resolution >= 0:
      h3_res_sql = h3_res_sql + f"h3_res_{h3_resolution} STRING COMMENT 'H3 cell identifier for lat and lon with H3 resolution {h3_resolution}.',\n"
      h3_res.append(f"h3_res_{h3_resolution}")
      h3_resolution = h3_resolution - 1

  h3_res_str = ', '.join(h3_res)

  create_osn_tracks_sql = f"""
    CREATE TABLE IF NOT EXISTS `{project}`.`osn_tracks` (
        event_time BIGINT COMMENT 'This column contains the unix (aka POSIX or epoch) timestamp for which the state vector was valid.',
        icao24 STRING COMMENT 'This column contains the 24-bit ICAO transponder ID which can be used to track specific airframes over different flights.',
        lat DOUBLE COMMENT 'This column contains the last known latitude of the aircraft.',
        lon DOUBLE COMMENT 'This column contains the last known longitude of the aircraft.',
        velocity DOUBLE COMMENT 'This column contains the speed over ground of the aircraft in meters per second.',
        heading DOUBLE COMMENT 'This column represents the direction of movement (track angle in degrees) as the clockwise angle from the geographic north.',
        vert_rate DOUBLE COMMENT 'This column contains the vertical speed of the aircraft in meters per second.',
        callsign STRING COMMENT 'This column contains the callsign that was broadcast by the aircraft.',
        on_ground BOOLEAN COMMENT 'This flag indicates whether the aircraft is broadcasting surface positions (true) or airborne positions (false).',
        alert BOOLEAN COMMENT 'This flag is a special indicator used in ATC.',
        spi BOOLEAN COMMENT 'This flag is a special indicator used in ATC.',
        squawk STRING COMMENT 'This 4-digit octal number is another transponder code which is used by ATC and pilots for identification purposes and indication of emergencies.',
        baro_altitude DOUBLE COMMENT 'This column indicates the aircrafts altitude. As the names suggest, baroaltitude is the altitude measured by the barometer (in meter).',
        geo_altitude DOUBLE COMMENT 'This column indicates the aircrafts altitude. As the names suggest, geoaltitude is determined using the GNSS (GPS) sensor (in meter).',
        last_pos_update DOUBLE COMMENT 'This unix timestamp indicates the age of the position.',
        last_contact DOUBLE COMMENT 'This unix timestamp indicates the time at which OpenSky received the last signal of the aircraft.',
        serials ARRAY<INT> COMMENT 'The serials column is a list of serials of the ADS-B receivers which received the message.',
        track_id STRING COMMENT 'Unique identifier for the associated flight tracks in opdi_flight_table_with_id.',
        {h3_res_sql}
        segment_distance_nm DOUBLE COMMENT 'The distance from the previous statevector in nautic miles.',
        cumulative_distance_nm DOUBLE COMMENT 'The cumulative distance from the start in nautic miles.'
    )
    COMMENT '`{project}`.`osn_statevectors` with added track_ids (generated based on callsign, icao24 grouping with 30 min signal gap intolerance) and H3 tags. Last updated: {today}.'
    STORED AS parquet
    TBLPROPERTIES ('transactional'='false');
    """
  
  create_osn_tracks_clustered_sql = f"""
    CREATE TABLE IF NOT EXISTS `{project}`.`osn_tracks_clustered` (
        event_time BIGINT COMMENT 'This column contains the unix (aka POSIX or epoch) timestamp for which the state vector was valid.',
        icao24 STRING COMMENT 'This column contains the 24-bit ICAO transponder ID which can be used to track specific airframes over different flights.',
        lat DOUBLE COMMENT 'This column contains the last known latitude of the aircraft.',
        lon DOUBLE COMMENT 'This column contains the last known longitude of the aircraft.',
        velocity DOUBLE COMMENT 'This column contains the speed over ground of the aircraft in meters per second.',
        heading DOUBLE COMMENT 'This column represents the direction of movement (track angle in degrees) as the clockwise angle from the geographic north.',
        vert_rate DOUBLE COMMENT 'This column contains the vertical speed of the aircraft in meters per second.',
        callsign STRING COMMENT 'This column contains the callsign that was broadcast by the aircraft.',
        on_ground BOOLEAN COMMENT 'This flag indicates whether the aircraft is broadcasting surface positions (true) or airborne positions (false).',
        alert BOOLEAN COMMENT 'This flag is a special indicator used in ATC.',
        spi BOOLEAN COMMENT 'This flag is a special indicator used in ATC.',
        squawk STRING COMMENT 'This 4-digit octal number is another transponder code which is used by ATC and pilots for identification purposes and indication of emergencies.',
        baro_altitude DOUBLE COMMENT 'This column indicates the aircrafts altitude. As the names suggest, baroaltitude is the altitude measured by the barometer (in meter).',
        geo_altitude DOUBLE COMMENT 'This column indicates the aircrafts altitude. As the names suggest, geoaltitude is determined using the GNSS (GPS) sensor (in meter).',
        last_pos_update DOUBLE COMMENT 'This unix timestamp indicates the age of the position.',
        last_contact DOUBLE COMMENT 'This unix timestamp indicates the time at which OpenSky received the last signal of the aircraft.',
        serials ARRAY<INT> COMMENT 'The serials column is a list of serials of the ADS-B receivers which received the message.',
        track_id STRING COMMENT 'Unique identifier for the associated flight tracks in `{project}`.`opdi_flight_table`.',
        {h3_res_sql}
        segment_distance_nm DOUBLE COMMENT 'The distance from the previous statevector in nautic miles.',
        cumulative_distance_nm DOUBLE COMMENT 'The cumulative distance from the start in nautic miles.'
    )
    COMMENT 'Clustered `{project}`.`osn_statevectors` with added track_ids (generated based on callsign, icao24 grouping with 30 min signal gap intolerance) and H3 tags. Last updated: {today}.'
    CLUSTERED BY (track_id, event_time, icao24, callsign, baro_altitude, {h3_res_str}) INTO 4096 BUCKETS
    STORED AS parquet
    TBLPROPERTIES ('transactional'='false');"""
  
  # Printing and creating in HUE because spark statements below don't work from CML
  print(create_osn_tracks_sql)
  print(create_osn_tracks_clustered_sql)
  
  #spark.sql(f"DROP TABLE IF EXISTS `{project}`.`osn_h3_statevectors`;")
  #spark.sql(create_osn_tracks_sql)
  
  return h3_res_sql, h3_res


# Distance helper function
def calculate_cumulative_distance(df: DataFrame) -> DataFrame:
    """
    
    DEPRECIATED -> This is now done in step 04
    
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
  
def process_tracks(project, max_h3_resolution, month):
  
  print(f"Processing tracks for month {month}.. ({datetime.now()})")
  h3_res_sql, h3_res = h3_query_prep(project, max_h3_resolution)
  
  start_time, end_time = get_start_end_of_month(month)
  
  
  
  # Read raw data 
  df_month = spark.sql(f"""
        SELECT * FROM `{project}`.`osn_statevectors_clustered` 
        WHERE (event_time >= {start_time}) AND (event_time < {end_time});""")

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
  
  h3_resolution = max_h3_resolution

  while h3_resolution >= 0:
      if h3_resolution == max_h3_resolution:
          df_month = df_month.withColumn("h3_resolution", lit(h3_resolution))
          df_month = df_month.withColumn(f"h3_res_{h3_resolution}", h3_pyspark.geo_to_h3('lat', 'lon', 'h3_resolution'))
      else: 
          df_month = df_month.withColumn("h3_resolution", lit(h3_resolution))
          df_month = df_month.withColumn(f"h3_res_{h3_resolution}", h3_pyspark.h3_to_parent(F.col(f"h3_res_{max_h3_resolution}"), F.lit(h3_resolution)))

      h3_resolution = h3_resolution - 1

  # Drop unnecessary and additional columns to retain only the original ones
  df_month = df_month.select(original_columns)

  # Add the distance variables
  df_month = calculate_cumulative_distance(df_month)

  # Write data for the month to the database
  df_month.write.mode("overwrite").insertInto(f"`{project}`.`osn_tracks`")
  
  # Write to clustered DB
  spark.sql(f"""
    INSERT INTO TABLE `{project}`.`osn_tracks_clustered`
    SELECT * FROM `{project}`.`osn_tracks`
    WHERE event_time >= {start_time}
    AND event_time < {end_time}
    """)

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
    process_tracks(project, max_h3_resolution, month)
    
    ## Logging
    processed_months.append(month)
    processed_df = pd.DataFrame({'months':processed_months})
    processed_df.to_parquet(fpath)

# Stop the SparkSession
spark.stop()