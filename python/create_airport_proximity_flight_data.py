import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType, lit
from pyspark.sql.types import DoubleType, StructType, StructField
from pyspark.sql.functions import round
from IPython.display import display, HTML
import time
import os

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

# Load the necessary tables/dataframes
osn_ec_datadump_df = spark.sql(
    "SELECT event_time, icao24, lat as lat_flight, lon as lon_flight, velocity, heading, vert_rate, callsign, on_ground, baro_altitude FROM project_aiu.osn_ec_datadump WHERE baro_altitude <= 3048 LIMIT 100000000;"
)

airport_distance_reference_df = spark.sql(
    "SELECT lat as lat_airport, lon as lon_airport, airport_ident, distance FROM project_aiu.airport_distance_reference WHERE distance <= 50;"
)

oa_airports_df = spark.sql(
    "SELECT ident, elevation_ft FROM project_aiu.oa_airports;"
)

# Round 'lat' and 'lon' to 3 decimal places
osn_ec_datadump_df = osn_ec_datadump_df.withColumn('lat_flight', round(osn_ec_datadump_df['lat_flight'], 3))
osn_ec_datadump_df = osn_ec_datadump_df.withColumn('lon_flight', round(osn_ec_datadump_df['lon_flight'], 3))

# Join osn_ec_datadump with airport_distance_reference
merged_df = osn_ec_datadump_df.join(
    airport_distance_reference_df,
    (osn_ec_datadump_df['lat_flight'] == airport_distance_reference_df['lat_airport']) &
    (osn_ec_datadump_df['lon_flight'] == airport_distance_reference_df['lon_airport'])
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
flights_below_10000_ft_df = flights_below_10000_ft_df.select("event_time", "icao24", "lat_flight", "lon_flight", "velocity", "heading", "vert_rate", "callsign", "on_ground", "baro_altitude", "airport_ident", "ident", "elevation_ft", "height_above_airport", "distance")

# Now you have a dataframe (flights_below_10000_ft_df) containing only the flights below 10000 feet
flights_below_10000_ft_df.write.insertInto("project_aiu.airport_proximity_flight_data", overwrite=True)

