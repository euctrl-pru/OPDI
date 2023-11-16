from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,from_unixtime, min, max, to_date, pandas_udf, col, PandasUDFType, lit, round
from pyspark.sql.types import DoubleType, StructType, StructField
from pyspark.sql import functions as F
from pyspark.sql import Window

import os, time
import subprocess
import os,shutil
from datetime import datetime
import pandas as pd
import numpy as np

# Settings
project = "project_aiu"

# Getting today's date
today = datetime.today().strftime('%d %B %Y')

# Spark Session Initialization
shutil.copy("/runtime-addons/cmladdon-2.0.40-b150/log4j.properties", "/etc/spark/conf/") # Setting logging properties
spark = SparkSession.builder \
    .appName("OSN ADEP ADES Identification") \
    .config("spark.log.level", "ERROR")\
    .config("spark.hadoop.fs.azure.ext.cab.required.group", "eur-app-aiu-dev") \
    .config("spark.kerberos.access.hadoopFileSystems", "abfs://storage-fs@cdpdldev0.dfs.core.windows.net/data/project/aiu.db/unmanaged") \
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

# Calculate proximity data

# Load the necessary tables/dataframes
osn_tracks_df = spark.sql(
    f"SELECT event_time, icao24, lat as lat_flight, lon as lon_flight, velocity, heading, vert_rate, callsign, on_ground, geo_altitude, track_id FROM {project}.osn_tracks_clustered WHERE geo_altitude <= 3048;" # 3048 m = 10.000 ft 
)

airport_distance_reference_df = spark.sql(
    f"SELECT lat as lat_airport, lon as lon_airport, airport_ident, distance FROM {project}.airport_distance_reference WHERE distance <= 50;"
)

oa_airports_df = spark.sql(
    "SELECT ident, elevation_ft FROM project_aiu.oa_airports;"
)

# Round 'lat' and 'lon' to 3 decimal places
osn_tracks_df = osn_tracks_df.withColumn('lat_flight', round(osn_tracks_df['lat_flight'], 3))
osn_tracks_df = osn_tracks_df.withColumn('lon_flight', round(osn_tracks_df['lon_flight'], 3))

# Join osn_ec_datadump with airport_distance_reference
merged_df = osn_tracks_df.join(
    airport_distance_reference_df,
    (osn_tracks_df['lat_flight'] == airport_distance_reference_df['lat_airport']) &
    (osn_tracks_df['lon_flight'] == airport_distance_reference_df['lon_airport'])
)

# Join the resulting dataframe with oa_airports
merged_df = merged_df.join(
    oa_airports_df,
    merged_df['airport_ident'] == oa_airports_df['ident']
)

# Compute aircraft height above the airport
# Convert geo_altitude from meters to feet before subtracting elevation_ft
merged_df = merged_df.withColumn(
    "height_above_airport",
    (merged_df['geo_altitude']*3.28084 - merged_df['elevation_ft']).alias('height_above_airport')
)

# Filter out flights at a height of more than 10,000 feet
flights_below_10000_ft_df = merged_df.filter(merged_df['height_above_airport'] <= 10000)

# Select all relevant columns
flights_below_10000_ft_df = flights_below_10000_ft_df.select("event_time", "icao24", "lat_flight", "lon_flight", "velocity", "heading", "vert_rate", "callsign", "on_ground", "geo_altitude", "airport_ident", "ident", "elevation_ft", "height_above_airport", "distance", "track_id")

flights_below_10000_ft_df = flights_below_10000_ft_df.orderBy(['icao24', 'callsign', 'airport_ident', 'event_time', 'track_id']).na.drop(subset=['callsign','track_id'])

# Section to allocate airports... 

def categorize_by_vert_rate_spark(df):
    """Categorize the vertical rate into 'Take-off', 'Landing', or 'Ambiguous'."""
    avg_vert_rate = df.groupBy(['icao24', 'callsign', 'track_id', 'airport_ident']).agg(F.avg('vert_rate').alias('avg_vert_rate'))
    avg_vert_rate = avg_vert_rate.withColumn('status',
                                            F.when(avg_vert_rate['avg_vert_rate'] > 2, 'Take-off')
                                            .when(avg_vert_rate['avg_vert_rate'] < 2, 'Landing')
                                            .otherwise('Ambiguous'))
    return df.join(avg_vert_rate, on=['icao24', 'callsign', 'track_id', 'airport_ident'], how='left')

def compute_distance_event_times_spark(df):
    """Compute event times for min and max distances."""
    window_spec = Window.partitionBy(['icao24', 'callsign', 'track_id', 'airport_ident', 'status'])
    df = df.withColumn('min_distance', F.min('distance').over(window_spec))
    df_min = df.filter(df.distance == df.min_distance).select(
        ['icao24', 'callsign', 'track_id', 'airport_ident', 'status', 'event_time', 'min_distance']).withColumnRenamed('event_time', 'event_time_min_distance')
    return df_min


def process_and_pivot_data_spark(df):
    """Process the dataframe to remove 'Ambiguous' rows, pivot, and select specific columns."""
    df = df.filter(df.status != "Ambiguous")
    df_pivot = df.groupBy(['icao24', 'callsign', 'track_id']).pivot('status').agg(
        F.first('airport_ident').alias('airport_ident'),
        F.first('min_distance').alias('min_distance'),
        F.first('event_time_min_distance').alias('event_time_min_distance'),
    )
    
    # Select and rename
    df_pivot = df_pivot.select(
        col('track_id').alias('TRACK_ID'),
        col('icao24').alias('ICAO24'),
        col('callsign').alias('FLT_ID'),
        col('first_seen').alias('FIRST_SEEN'),
        col('last_seen').alias('LAST_SEEN'),
        col('source').alias('SOURCE'),
        col('Take-off_airport_ident').alias('ADEP'),
        col('Take-off_min_distance').alias('ADEP_MIN_DISTANCE_KM'),
        col('Take-off_event_time_min_distance').alias('ADEP_MIN_DISTANCE_TIME'),
        col('Landing_airport_ident').alias('ADES'),
        col('Landing_min_distance').alias('ADES_MIN_DISTANCE_KM'),
        col('Landing_event_time_min_distance').alias('ADES_MIN_DISTANCE_TIME')
    )
  
    #df_pivot = df_pivot.withColumn('ADEP_MIN_DISTANCE_TIME', to_timestamp('ADEP_MIN_DISTANCE_TIME'))
    #df_pivot = df_pivot.withColumn('ADES_MIN_DISTANCE_TIME', to_timestamp('ADES_MIN_DISTANCE_TIME'))

    return df_pivot

# Main process
flight_data_spark = categorize_by_vert_rate_spark(flights_below_10000_ft_df)

# Calculate the DOF and first_seen & last_seen for each track
window_spec_track = Window.partitionBy('track_id')
flight_data_spark = flight_data_spark.withColumn('event_date', to_date(from_unixtime('event_time')))
flight_data_spark = flight_data_spark.withColumn('DOF', min('event_date').over(window_spec_track))
flight_data_spark = flight_data_spark.withColumn('first_seen', min('event_time').over(window_spec_track))
flight_data_spark = flight_data_spark.withColumn('last_seen', max('event_time').over(window_spec_track))

# Add a constant column for SOURCE
flight_data_spark = flight_data_spark.withColumn('source', F.lit('OSN'))

# Continue main process
df_spark = compute_distance_event_times_spark(flight_data_spark)
df_pivot_spark = process_and_pivot_data_spark(df_spark)


# Insert

spark.sql(f"""DROP TABLE IF EXISTS `{project}`.osn_flight_table""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {project}.osn_flight_table (
    TRACK_ID STRING COMMENT 'Unique identifier for the associated flight tracks in `{project}`.`osn_flight_table`.',
    ICAO24 STRING COMMENT 'Unique ICAO 24-bit address of the transponder in hexadecimal string representation.',
    FLT_ID STRING COMMENT 'Flight trajectory identificator, the callsign.',
    DOF DATE COMMENT 'The date of flight (DOF), this is identified as the date of the first statevector in the available trajectory.', 
    FIRST_SEEN BIGINT COMMENT 'The timestamp of the first available statevector (unix time)',
    LAST_SEEN BIGINT COMMENT 'The timestamp of the last available statevector (unix time)',
    SOURCE STRING COMMENT 'The source from which the flight was extracted (OSN: OpenSky Network, NM: EUROCONTROL Network Manager, ...).',
    ADEP STRING COMMENT 'The best guess for the aerodrome of departure (ADEP) of the flight based on the available flight trajectory.',
    ADEP_MIN_DISTANCE_KM FLOAT COMMENT 'Minimum distance the ADS-B signal is picked up from the airport of departure in kilometers.',
    ADEP_MIN_DISTANCE_TIME BIGINT COMMENT 'Timestamp at which the minimum distance from ADEP was recorded.',
    ADES STRING COMMENT 'The best guess for the aerodrome of destination (ADES) of the flight based on the available flight trajectory.',
    ADES_MIN_DISTANCE_KM FLOAT COMMENT 'Minimum distance the ADS-B signal is picked up from the airport of destination in kilometers.',
    ADES_MIN_DISTANCE_TIME BIGINT COMMENT 'Timestamp at which the minimum distance from ADES was recorded.'
)
COMMENT 'A flight table constructed from OpenSky Netwerk (OSN) ADS-B statevectors. Last updated: {today}.'
STORED AS parquet
TBLPROPERTIES ('transactional'='false');""")

df_pivot_spark = df_pivot_spark.write.mode('overwrite').saveAsTable(f"{project}.osn_flight_table")

