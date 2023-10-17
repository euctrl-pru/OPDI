!pip install h3
!pip install h3_pyspark

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
import shutil
from datetime import datetime
import h3_pyspark

# Settings
project = "project_aiu"
h3_resolution = 11

# Getting today's date
today = datetime.today().strftime('%d %B %Y')

# Spark Session Initialization
shutil.copy("/runtime-addons/cmladdon-2.0.40-b150/log4j.properties", "/etc/spark/conf/") # Setting logging properties
spark = SparkSession.builder \
    .appName("OSN tracks ETL") \
    .config("spark.log.level", "ERROR")\
    .config("spark.ui.showConsoleProgress", "false")\
    .config("spark.hadoop.fs.azure.ext.cab.required.group", "eur-app-aiu-dev") \
    .config("spark.kerberos.access.hadoopFileSystems", "abfs://storage-fs@cdpdldev0.dfs.core.windows.net/data/project/aiu.db/unmanaged") \
    .config("spark.driver.cores", "1") \
    .config("spark.driver.memory", "10G") \
    .config("spark.executor.memory", "8G") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "5") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "400s") \
    .enableHiveSupport() \
    .getOrCreate()

# Read raw data 
df = spark.sql(f"SELECT * FROM `{project}`.`osn_statevectors_clustered`;")

# Create OSN tracks db
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
        track_id STRING COMMENT 'Unique identifier for the associated flight tracks in osn_flight_table_with_id.',
        h3_id_res_{h3_resolution} STRING COMMENT 'H3 cell identifier for lat and lon with H3 resolution {h3_resolution}.'
    )
    COMMENT '`{project}`.`osn_statevectors_clustered` with added track_ids (generated based on callsign, icao24 grouping with 30 min signal gap intolerance). Last updated: {today}.'
    STORED AS parquet
    TBLPROPERTIES ('transactional'='false');
"""

spark.sql(f"DROP TABLE IF EXISTS `{project}`.`osn_tracks`;")
spark.sql(create_osn_tracks_sql)

# Save the original column names for later and add track_id
original_columns = df.columns + ['track_id', f'h3_id_res_{h3_resolution}'] 

# Convert 'event_time' to datetime
df = df.withColumn("event_time_ts", F.to_timestamp("event_time"))

# Extract the year and month from event_time_ts to create a 'month_id'
df = df.withColumn("year", F.year("event_time_ts"))
df = df.withColumn("month", F.month("event_time_ts"))
df = df.withColumn("month_id", F.concat_ws("-", F.col("year"), F.col("month")))

# List unique month_ids
unique_month_ids = [row['month_id'] for row in df.select("month_id").distinct().collect()]

# Padding single-digit months with zeros
#unique_month_ids = [date if len(date.split('-')[1]) == 2 else f"{date.split('-')[0]}-0{date.split('-')[1]}" for date in unique_month_ids]

#unique_month_ids = sorted(unique_month_ids)

print(f"These are the unique month_ids to be processed: {unique_month_ids}")
print("")

# Loop through months - Otherwise it's too big to process at once... -> Find solution?
for idx, month_id in enumerate(unique_month_ids):
    print("="*30)
    print(f"Working on month_id nr. {idx} ({month_id}) out of {len(unique_month_ids)}.")
    print("="*30)
    print("")
    
    # Filter data for the current month_id
    df_month = df.filter(F.col("month_id") == month_id)
    
    # Create a SHA value for each icao24, callsign combination and groupby
    df_month = df_month.withColumn("group_id", F.sha2(F.concat_ws("", "icao24", "callsign"), 256))
    windowSpec = Window.partitionBy("group_id").orderBy("event_time_ts")
    
    # Calculate the difference between each two timestamps
    # If there's a difference of more than 30 minutes... Then we divide it in multiple tracks
    df_month = df_month.withColumn("prev_event_time_ts", F.lag(df_month["event_time_ts"]).over(windowSpec))
    df_month = df_month.withColumn("time_gap_minutes", 
                                   (F.unix_timestamp("event_time_ts") - F.unix_timestamp("prev_event_time_ts"))/60)
    track_split_condition = (
        (F.col("time_gap_minutes") > 30) | # When the gap is larger than 30 minutes, we identify it as two tracks.. 
        ((F.col("time_gap_minutes") > 10) & (F.col("geo_altitude") < 1000)) # When the gap is larger than 10 minutes and the geo_altitude is larger than 1000 m, we identify two tracks.
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
                                       
    # Add H3 id
    df_month = df_month.withColumn("h3_resolution", lit(h3_resolution))
    df_month = df_month.withColumn(f"h3_id_res_{h3_resolution}", h3_pyspark.geo_to_h3('lat', 'lon', 'h3_resolution'))
    
    # Drop unnecessary and additional columns to retain only the original ones
    df_month = df_month.select(original_columns)
    
    # Write data for the month to the database
    df_month.write.mode("append").insertInto(f"`{project}`.`osn_tracks`")


# Create clustered version of the database.. 

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
        track_id STRING COMMENT 'Unique identifier for the associated flight tracks in `{project}`.`osn_flight_table`.',
        h3_id_res_{h3_resolution} STRING COMMENT 'H3 cell identifier for lat and lon with H3 resolution {h3_resolution}.'
    )
    COMMENT 'Clustered `{project}`.`osn_statevectors_clustered` with added track_ids (generated based on callsign, icao24 grouping with 30 min signal gap intolerance). Last updated: {today}.'
    CLUSTERED BY (track_id, event_time, icao24, callsign, geo_altitude, h3_id_res_{h3_resolution}) INTO 4096 BUCKETS
    STORED AS parquet
    TBLPROPERTIES ('transactional'='false');"""

spark.sql(f"DROP TABLE IF EXISTS `{project}`.`osn_tracks_clustered`;")
spark.sql(create_osn_tracks_clustered_sql)

spark.sql(f"""
  INSERT INTO TABLE `{project}`.`osn_tracks_clustered` 
  SELECT * FROM `{project}`.`osn_tracks`;
""")

# Cleanup
spark.sql(f"""DROP TABLE IF EXISTS `{project}`.`osn_tracks`;""")

# Stop the SparkSession
spark.stop()