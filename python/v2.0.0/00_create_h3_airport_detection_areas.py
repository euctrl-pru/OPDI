import pandas as pd
import numpy as np
import json
import math
import pandas as pd
pd.DataFrame.iteritems = pd.DataFrame.items

from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col, lit, array_except
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import h3
import h3_pyspark
#.config("spark.ui.showConsoleProgress", "false") \
# Spark Session Initialization
spark = SparkSession.builder \
    .appName("OPDI Flight Table") \
    .config("spark.hadoop.fs.azure.ext.cab.required.group", "eur-app-opdi") \
    .config("spark.kerberos.access.hadoopFileSystems", "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged") \
    .config("spark.executor.extraClassPath", "/opt/spark/optional-lib/iceberg-spark-runtime-3.3_2.12-1.3.1.1.20.7216.0-70.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark/optional-lib/iceberg-spark-runtime-3.3_2.12-1.3.1.1.20.7216.0-70.jar") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged") \
    .config("spark.executor.instances", "3") \
    .config("spark.dynamicAllocation.maxExecutors", "20") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "400s") \
    .enableHiveSupport() \
    .getOrCreate()


def generate_circle_polygon(lon, lat, radius_nautical_miles, num_points=360):
    """
    Generate a polygon in GeoJSON format around a given latitude and longitude
    with a specified radius in nautical miles.
    """
    radius_km = radius_nautical_miles * 1.852
    
    def degrees_to_radians(degrees):
        return degrees * math.pi / 180
    
    def calculate_point(lon, lat, distance_km, bearing):
        R = 6371.01  # Earth's radius in kilometers
        lat_rad = degrees_to_radians(lat)
        lon_rad = degrees_to_radians(lon)
        distance_rad = distance_km / R
        bearing_rad = degrees_to_radians(bearing)
        
        lat_new_rad = math.asin(math.sin(lat_rad) * math.cos(distance_rad) +
                                math.cos(lat_rad) * math.sin(distance_rad) * math.cos(bearing_rad))
        lon_new_rad = lon_rad + math.atan2(math.sin(bearing_rad) * math.sin(distance_rad) * math.cos(lat_rad),
                                           math.cos(distance_rad) - math.sin(lat_rad) * math.sin(lat_new_rad))
        
        return [math.degrees(lon_new_rad), math.degrees(lat_new_rad)]
    
    points = [calculate_point(lon, lat, radius_km, (360 / num_points) * i) for i in range(num_points)]
    points.append(points[0])  # Close the polygon
    
    return json.dumps({"type": "Polygon", "coordinates": [points]})


generate_circle_polygon_udf = udf(generate_circle_polygon, StringType())

df_apt = pd.read_csv('https://davidmegginson.github.io/ourairports-data/airports.csv')

offset = 3  # degrees
f_lat = (df_apt.latitude_deg.between(26.74617 - offset, 70.25976 + offset))
f_lon = (df_apt.longitude_deg.between(-25.86653 - offset, 49.65699 + offset))
df_apt = df_apt[f_lat & f_lon]

# Convert column names to string type to avoid issues
df_apt.columns = df_apt.columns.astype(str)

# Cast numerical columns to float explicitly to avoid conversion issues
df_apt = df_apt.astype({
    "latitude_deg": "float64",
    "longitude_deg": "float64",
    "elevation_ft": "float64"  # In case elevation is present
})


# Define schema for PySpark DataFrame
schema = StructType([
    StructField("id", StringType(), True),
    StructField("ident", StringType(), True),
    StructField("type", StringType(), True),
    StructField("name", StringType(), True),
    StructField("latitude_deg", FloatType(), True),
    StructField("longitude_deg", FloatType(), True),
    StructField("elevation_ft", FloatType(), True),
    StructField("continent", StringType(), True),
    StructField("iso_country", StringType(), True),
    StructField("iso_region", StringType(), True),
    StructField("municipality", StringType(), True),
    StructField("scheduled_service", StringType(), True),
    StructField("gps_code", StringType(), True),
    StructField("iata_code", StringType(), True),
    StructField("local_code", StringType(), True),
    StructField("home_link", StringType(), True),
    StructField("wikipedia_link", StringType(), True),
    StructField("keywords", StringType(), True),
])

# Convert pandas DataFrame to PySpark DataFrame using schema
airports_df = spark.createDataFrame(df_apt, schema=schema)

max_resolution = 7
num_points = 720
radia_nm = [0, 5, 10, 20, 30, 40]
area_type = [f"C{x+10}" for x in radia_nm]

df = pd.DataFrame({
    'max_resolution': max_resolution,
    'number_of_points_c': num_points,
    'area_type': area_type,
    'min_c_radius_nm': radia_nm
})

df['max_c_radius_nm'] = df['min_c_radius_nm'].shift(-1).fillna(np.max(radia_nm) + 10)
df[['min_c_radius_nm', 'max_c_radius_nm']] = df[['min_c_radius_nm', 'max_c_radius_nm']].astype(float)
df['m_col'] = 1

schema = StructType([
    StructField("max_resolution", IntegerType(), True),
    StructField("number_of_points_c", IntegerType(), True),
    StructField("area_type", StringType(), True),
    StructField("min_c_radius_nm", FloatType(), True),
    StructField("max_c_radius_nm", FloatType(), True),
    StructField("m_col", IntegerType(), True)
])

sdf = spark.createDataFrame(df, schema=schema)
airports_df_m = airports_df.withColumn('m_col', lit(1)).join(sdf, on='m_col', how='left')

res = (airports_df_m
    .withColumn("inner_circle_polygon", generate_circle_polygon_udf(col('longitude_deg'), col('latitude_deg'), col('min_c_radius_nm'), col('number_of_points_c')))
    .withColumn("outer_circle_polygon", generate_circle_polygon_udf(col('longitude_deg'), col('latitude_deg'), col('max_c_radius_nm'), col('number_of_points_c')))
    .withColumn("inner_circle_hex_ids", h3_pyspark.polyfill(col("inner_circle_polygon"), col("max_resolution"), lit(True)))
    .withColumn("outer_circle_hex_ids", h3_pyspark.polyfill(col("outer_circle_polygon"), col("max_resolution"), lit(True)))
    .withColumn("hex_id", array_except(col("outer_circle_hex_ids"), col("inner_circle_hex_ids")))
    .drop("inner_circle_polygon", "outer_circle_polygon", "inner_circle_hex_ids", "outer_circle_hex_ids")
    .toPandas()
)

res.to_parquet('../../data/airport_hex/airport_concentric_c_hex_res_7_new.parquet')