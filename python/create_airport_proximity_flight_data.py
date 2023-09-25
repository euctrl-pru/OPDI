import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType, lit
from pyspark.sql.types import DoubleType, StructType, StructField
from pyspark.sql.functions import round
from IPython.display import display, HTML
import time
import os

# Hotfix
!cp /runtime-addons/cmladdon-2.0.40-b150/log4j.properties /etc/spark/conf/

# Create a Spark session
spark = SparkSession\
    .builder\
    .appName("geodistance")\
    .config("spark.hadoop.fs.azure.ext.cab.required.group","eur-app-aiu-dev")\
    .config("spark.yarn.access.hadoopFileSystems","abfs://storage-fs@cdpdldev0.dfs.core.windows.net/")\
    .config("spark.driver.cores","1")\
    .config("spark.driver.memory","8G")\
    .config("spark.executor.memory","5G")\
    .config("spark.executor.cores","1")\
    .config("spark.executor.instances","2")\
    .config("spark.dynamicAllocation.maxExecutors", "6")\
    .config("spark.rpc.message.maxSize", "200")\
    .getOrCreate()

# Get environment variables
engine_id = os.getenv('CDSW_ENGINE_ID')
domain = os.getenv('CDSW_DOMAIN')

# Format the URL
url = f"https://spark-{engine_id}.{domain}"

# Display the clickable URL
display(HTML(f'<a href="{url}">{url}</a>'))

# Create table and transport data
spark.sql("""DROP TABLE IF EXISTS project_aiu.osn_tracks_clustered;""")
spark.sql("""
CREATE TABLE project_aiu.osn_tracks_clustered (
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
    track_id STRING COMMENT 'Unique identifier for the associated flight tracks in osn_flight_table_with_id.'
)
COMMENT 'project_aiu.osn_ec_datadump copy with added track_ids (generated based on callsign, icao24 grouping with 30 min signal gap intolerance.'
CLUSTERED BY (event_time, icao24, callsign, track_id, baro_altitude) INTO 4096 BUCKETS
STORED AS parquet
TBLPROPERTIES ('transactional'='false');
""")

spark.sql("""
INSERT OVERWRITE TABLE project_aiu.osn_tracks_clustered 
SELECT * FROM project_aiu.osn_tracks;""")

spark.sql("""
DROP TABLE project_aiu.osn_tracks;""")

# Calculate proximity data

# Load the necessary tables/dataframes
osn_tracks_df = spark.sql(
    "SELECT event_time, icao24, lat as lat_flight, lon as lon_flight, velocity, heading, vert_rate, callsign, on_ground, baro_altitude, track_id FROM project_aiu.osn_tracks_clustered WHERE baro_altitude <= 3048;"
)

airport_distance_reference_df = spark.sql(
    "SELECT lat as lat_airport, lon as lon_airport, airport_ident, distance FROM project_aiu.airport_distance_reference WHERE distance <= 50;"
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
# Convert baro_altitude from meters to feet before subtracting elevation_ft
merged_df = merged_df.withColumn(
    "height_above_airport",
    (merged_df['baro_altitude']*3.28084 - merged_df['elevation_ft']).alias('height_above_airport')
)

# Filter out flights at a height of more than 10,000 feet
flights_below_10000_ft_df = merged_df.filter(merged_df['height_above_airport'] <= 10000)

# Select all relevant columns
flights_below_10000_ft_df = flights_below_10000_ft_df.select("event_time", "icao24", "lat_flight", "lon_flight", "velocity", "heading", "vert_rate", "callsign", "on_ground", "baro_altitude", "airport_ident", "ident", "elevation_ft", "height_above_airport", "distance", "track_id")

spark.sql("""DROP TABLE IF EXISTS project_aiu.airport_proximity_flight_data;""")

spark.sql(
"""
CREATE TABLE project_aiu.airport_proximity_flight_data (
  event_time BIGINT COMMENT 'Timestamp of the event',
  icao24 STRING COMMENT '24-bit ICAO transponder ID to track specific airframes over different flights.',
  lat DOUBLE COMMENT 'Latitude of the flight, rounded to 3 decimal places.',
  lon DOUBLE COMMENT 'Longitude of the flight, rounded to 3 decimal places.',
  velocity DOUBLE COMMENT 'Velocity of the flight',
  heading DOUBLE COMMENT 'Heading of the flight',
  vert_rate DOUBLE COMMENT 'Vertical rate of the flight',
  callsign STRING COMMENT 'Callsign of the flight',
  on_ground BOOLEAN COMMENT 'Whether the flight is on the ground',
  baro_altitude DOUBLE COMMENT 'Aircraft altitude measured by the barometer (in meter).',
  airport_ident STRING COMMENT 'Identifier of the airport ICAO.',
  ident STRING COMMENT 'The text identifier used in the OurAirports URL, which is the ICAO code.',
  elevation_ft INT COMMENT 'The airport elevation MSL in feet.',
  height_above_airport DOUBLE COMMENT 'Aircraft height above the airport, computed as (baro_altitude - elevation_ft).',
  distance DOUBLE COMMENT 'Distance to the airport',
  track_id STRING COMMENT 'The track ID related to the original OSN track.'
)
COMMENT 'Merged table containing flight data below 10000 feet near airports.'
STORED AS parquet
TBLPROPERTIES ('transactional'='false');
""")

# Now you have a dataframe (flights_below_10000_ft_df) containing only the flights below 10000 feet
flights_below_10000_ft_df.write.insertInto("project_aiu.airport_proximity_flight_data", overwrite=True)

