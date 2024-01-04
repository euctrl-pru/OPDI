import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType, lit
from pyspark.sql.types import DoubleType, StructType, StructField
import numpy as np
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
#display(HTML(f'<a href="{url}">{url}</a>'))

def create_local_grid_spark(lat_center, lon_center, lat_step_deg=0.0001, lon_step_deg=0.0001, radius_km=10, airport_ident=None):
    lat_step = lat_step_deg
    lon_step = lon_step_deg

    # Determine the number of decimal places in lat_step_deg and lon_step_deg
    lat_decimal_places = len(str(lat_step_deg).split('.')[1])
    lon_decimal_places = len(str(lon_step_deg).split('.')[1])

    lat_step_m = lat_step * 111.2 * 1000 # approximately 111.2km per degree
    lon_step_m = lon_step * 111.2 * 1000 * np.cos(np.radians(lat_center)) # the size of a degree of longitude depends on the latitude

    print(f"Each latitude step is about {lat_step_m} meters.")
    print(f"Each longitude step is about {lon_step_m} meters.")

    num_lat_points = int(2 * radius_km / (lat_step_m / 1000)) # since radius is in km
    num_lon_points = int(2 * radius_km / (lon_step_m / 1000)) # since radius is in km

    df = spark.range(num_lat_points).select(F.col("id").alias("lat_index")).crossJoin(
        spark.range(num_lon_points).select(F.col("id").alias("lon_index"))
    )
    
    df = df.withColumn("lat", F.round(lat_center + (F.col("lat_index") - num_lat_points/2) * lat_step, lat_decimal_places)).withColumn(
        "lon", F.round(lon_center + (F.col("lon_index") - num_lon_points/2) * lon_step, lon_decimal_places)
    ).drop("lat_index", "lon_index")

    if airport_ident is not None:
        df = df.withColumn("airport_ident", F.lit(airport_ident))
        
    return df


spark = SparkSession.builder.getOrCreate()

airports_df = spark.sql("""
    SELECT ident, latitude_deg, longitude_deg, elevation_ft, type
    FROM project_aiu.oa_airports
    WHERE (ident LIKE 'E%' OR ident LIKE 'L%' OR ident LIKE 'U%')
    AND (type = 'large_airport');
""")

from pyspark.sql import functions as F

def spark_haversine(lat1, lon1, lat2, lon2):
    # Convert all inputs to radians
    lat1, lon1, lat2, lon2 = [x * F.lit(np.pi / 180) for x in [lat1, lon1, lat2, lon2]]
    
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    
    a = F.sin(dlat / 2)**2 + F.cos(lat1) * F.cos(lat2) * F.sin(dlon / 2)**2
    c = 2 * F.asin(F.sqrt(a))
    r = 6371  # Radius of earth in kilometers
    return c * r

for row in airports_df.rdd.collect():
    print(f"Processing [{row['ident']}].")
    start_time = time.time()
    grid_df = create_local_grid_spark(row['latitude_deg'], row['longitude_deg'], lat_step_deg=0.001, lon_step_deg=0.001, radius_km=1.852*100, airport_ident=row['ident'])

    # Instead of using literals, add the central latitude and longitude as new columns
    grid_df = grid_df.withColumn('central_lat', F.lit(row['latitude_deg']))
    grid_df = grid_df.withColumn('central_lon', F.lit(row['longitude_deg']))

    # Now apply the haversine function using these new columns
    grid_df = grid_df.withColumn("distance", spark_haversine(grid_df['central_lat'], grid_df['central_lon'], grid_df['lat'], grid_df['lon']))

    # Drop the 'central_lat' and 'central_lon' columns
    grid_df = grid_df.drop('central_lat', 'central_lon')
    
    # Write the grid dataframe to the table
    grid_df.write.insertInto("project_aiu.airport_distance_reference", overwrite=False)
    
    end_time = time.time()

    execution_time = end_time - start_time
    print(f"The create_local_grid for [{row['ident']}] execution took [{execution_time} seconds].")