from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from datetime import datetime, date
import pandas as pd
import h3_pyspark
import os.path
import dateutil.relativedelta
import calendar

# Settings
## Config
project = "project_opdi"
max_h3_resolution = 12
start_month = date(2023, 1, 1)

## Which months to process
today = date.today()
end_month = today - dateutil.relativedelta.relativedelta(months=2) # We work on the d-2months

# Getting today's date formatted
today = today.strftime('%d %B %Y')

# Spark Session Initialization
spark = SparkSession.builder \
    .appName("OSN statevectors H3 addition") \
    .config("spark.log.level", "ERROR")\
    .config("spark.hadoop.fs.azure.ext.cab.required.group", "eur-app-opdi") \
    .config("spark.kerberos.access.hadoopFileSystems", "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged") \
    .config("spark.driver.cores", "1") \
    .config("spark.driver.memory", "8G") \
    .config("spark.executor.memory", "5G") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "6") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "400s") \
    .enableHiveSupport() \
    .getOrCreate()

def h3_query_prep(project, max_h3_resolution):
  # Create OSN tracks db
  h3_resolution = max_h3_resolution
  h3_res_sql = ""
  h3_res = []

  while h3_resolution >= 0:
      if h3_resolution != 0:
          h3_res_sql = h3_res_sql + f"h3_res_{h3_resolution} STRING COMMENT 'H3 cell identifier for lat and lon with H3 resolution {h3_resolution}.',"
      else: 
          h3_res_sql = h3_res_sql + f"h3_res_{h3_resolution} STRING COMMENT 'H3 cell identifier for lat and lon with H3 resolution {h3_resolution}.'"
      h3_res.append(f"h3_res_{h3_resolution}")
      h3_resolution = h3_resolution - 1

  h3_res_str = ', '.join(h3_res)

  create_osn_tracks_sql = f"""
      CREATE TABLE IF NOT EXISTS `{project}`.`osn_h3_statevectors` (
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
          {h3_res_sql}
      )
      COMMENT '`{project}`.`osn_statevectors_clustered` with added H3 tags (generated based on callsign, icao24 grouping with 30 min signal gap intolerance). Last updated: {today}.'
      STORED AS parquet
      TBLPROPERTIES ('transactional'='false');
  """

  #spark.sql(f"DROP TABLE IF EXISTS `{project}`.`osn_h3_statevectors`;")
  #spark.sql(create_osn_tracks_sql)
  
  return h3_res_sql, h3_res


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
  
def add_h3_to_sv(project, max_h3_resolution, month):
  
  print(f"Adding H3 statevectors to {project} with max resolution {max_h3_resolution} for month {month}")
  
  h3_res_sql, h3_res = h3_query_prep(project, max_h3_resolution)
  
  start_time, end_time = get_start_end_of_month(month)
  
  ## Read raw data 
  df = spark.sql(f"""
        SELECT * FROM `{project}`.`osn_statevectors` 
        WHERE (event_time >= {start_time}) AND (event_time < {end_time});""")

  # Save the original column names for later and add track_id
  original_columns = df.columns + h3_res

  h3_resolution = max_h3_resolution

  while h3_resolution >= 0:
      if h3_resolution == max_h3_resolution:
          df = df.withColumn("h3_resolution", lit(h3_resolution))
          df = df.withColumn(f"h3_res_{h3_resolution}", h3_pyspark.geo_to_h3('lat', 'lon', 'h3_resolution'))
      else: 
          df = df.withColumn("h3_resolution", lit(h3_resolution))
          df = df.withColumn(f"h3_res_{h3_resolution}", h3_pyspark.h3_to_parent(F.col(f"h3_res_{max_h3_resolution}"), F.lit(h3_resolution)))

      h3_resolution = h3_resolution - 1

  # Drop unnecessary and additional columns to retain only the original ones
  df = df.select(original_columns)

  # Write data for the month to the database
  df.write.mode("append").insertInto(f"`{project}`.`osn_h3_statevectors`")

# Actual processing

to_process_months = generate_months(start_month, end_month)

## Load logs
fpath = 'logs/02_osn-statevectors-h3-addition-log.parquet'
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
    add_h3_to_sv(project, max_h3_resolution, month)
    processed_months.append(month)
    
## Logging
processed_df = pd.DataFrame({'months':processed_months})
processed_df.to_parquet(fpath)