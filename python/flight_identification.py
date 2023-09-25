# pyspark.sql imports
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    udf, pandas_udf, PandasUDFType, lit, col,
    to_timestamp, expr, from_unixtime
)
from pyspark.sql import functions as F, SparkSession, Window
from pyspark.sql.types import DoubleType, StructType, StructField, LongType

# Other imports
import pandas as pd
import numpy as np
from IPython.display import display, HTML
import time
import os
from pyspark.sql.functions import broadcast

# Create a Spark session
spark = SparkSession\
    .builder\
    .appName("geodistance")\
    .config("spark.hadoop.fs.azure.ext.cab.required.group","eur-app-aiu-dev")\
    .config("spark.yarn.access.hadoopFileSystems","abfs://storage-fs@cdpdldev0.dfs.core.windows.net/")\
    .config("spark.driver.cores","1")\
    .config("spark.driver.memory","10G")\
    .config("spark.executor.memory","8G")\
    .config("spark.executor.cores","1")\
    .config("spark.executor.instances","2")\
    .config("spark.dynamicAllocation.maxExecutors", "6")\
    .config("spark.rpc.message.maxSize", "200")\
    .config("spark.sql.shuffle.partitions", "400")\
    .getOrCreate()

#spark.sql("""DROP TABLE IF EXISTS project_aiu.airport_proximity_flight_data_clustered;""")

#spark.sql("""
#CREATE TABLE project_aiu.airport_proximity_flight_data_clustered (
#  event_time TIMESTAMP COMMENT 'Timestamp of the event',
#  icao24 STRING COMMENT '24-bit ICAO transponder ID to track specific airframes over different flights.',
#  lat DOUBLE COMMENT 'Latitude of the flight, rounded to 3 decimal places.',
#  lon DOUBLE COMMENT 'Longitude of the flight, rounded to 3 decimal places.',
#  velocity DOUBLE COMMENT 'Velocity of the flight',
#  heading DOUBLE COMMENT 'Heading of the flight',
#  vert_rate DOUBLE COMMENT 'Vertical rate of the flight',
#  callsign STRING COMMENT 'Callsign of the flight',
#  on_ground BOOLEAN COMMENT 'Whether the flight is on the ground',
#  baro_altitude DOUBLE COMMENT 'Aircraft altitude measured by the barometer (in meter).',
#  airport_ident STRING COMMENT 'Identifier of the airport ICAO.',
#  ident STRING COMMENT 'The text identifier used in the OurAirports URL, which is the ICAO code.',
#  elevation_ft INT COMMENT 'The airport elevation MSL in feet.',
#  height_above_airport DOUBLE COMMENT 'Aircraft height above the airport, computed as (baro_altitude - elevation_ft).',
#  distance DOUBLE COMMENT 'Distance to the airport',
#  track_id STRING COMMENT 'Unique identifier for the associated flight tracks in osn_flight_table_with_id'
#)
#CLUSTERED BY (event_time, icao24, callsign, track_id, airport_ident, distance) INTO 2048 BUCKETS
#COMMENT 'Merged table containing flight data below 10000 feet near airports.'
#STORED AS parquet
#TBLPROPERTIES ('transactional'='false');
#""")

#spark.sql("""
#INSERT OVERWRITE TABLE project_aiu.airport_proximity_flight_data_clustered 
#SELECT * FROM project_aiu.airport_proximity_flight_data;
#""")

#spark.sql("""
#DROP TABLE project_aiu.airport_proximity_flight_data;
#""")


# Get environment variables
engine_id = os.getenv('CDSW_ENGINE_ID')
domain = os.getenv('CDSW_DOMAIN')

# Format the URL
#url = f"https://spark-{engine_id}.{domain}"

# Display the clickable URL
#display(HTML(f'<a href="{url}">{url}</a>'))

def load_flight_data_spark():
    """Load and preprocess flight data from Spark."""
    flight_data = spark.sql(
        f"SELECT DISTINCT * FROM `project_aiu`.`airport_proximity_flight_data_clustered`;"
    )
    return flight_data.orderBy(['icao24', 'callsign', 'airport_ident', 'event_time', 'track_id']).na.drop(subset=['callsign','track_id']) # Check if both need to be na

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
        col('icao24').alias('ICAO24'),
        col('callsign').alias('FLT_ID'),
        col('track_id').alias('TRACK_ID'),
        col('Take-off_airport_ident').alias('ADEP'),
        col('Take-off_min_distance').alias('ADEP_MIN_DISTANCE_KM'),
        col('Take-off_event_time_min_distance').alias('ADEP_MIN_DISTANCE_TIME'),
        col('Landing_airport_ident').alias('ADES'),
        col('Landing_min_distance').alias('ADES_MIN_DISTANCE_KM'),
        col('Landing_event_time_min_distance').alias('ADES_MIN_DISTANCE_TIME')
    )
  
    df_pivot = df_pivot.withColumn('ADEP_MIN_DISTANCE_TIME', to_timestamp('ADEP_MIN_DISTANCE_TIME'))
    df_pivot = df_pivot.withColumn('ADES_MIN_DISTANCE_TIME', to_timestamp('ADES_MIN_DISTANCE_TIME'))

    return df_pivot

# Main process
flight_data_spark = load_flight_data_spark()
flight_data_spark = categorize_by_vert_rate_spark(flight_data_spark)
df_spark = compute_distance_event_times_spark(flight_data_spark)
df_pivot_spark = process_and_pivot_data_spark(df_spark)
df_pivot_spark = df_pivot_spark.write.mode('overwrite').saveAsTable(f"project_aiu.osn_flight_table")




















## Assuming you've read in your dataframes
#osn_flight_table = spark.table("project_aiu.osn_flight_table")
#osn_ec_datadump = spark.table("project_aiu.osn_ec_datadump")

## Assign unique IDs to osn_flight_table
#osn_flight_table_with_id = osn_flight_table.withColumn("unique_id", F.monotonically_increasing_id().cast(LongType()))

## First check: Ensure unique IDs are assigned correctly
#osn_flight_table_with_id.show()

## Persisting the unique_id back to osn_flight_table
#osn_flight_table_with_id.write.mode("overwrite").insertInto("project_aiu.osn_flight_table_with_id")

## Filter osn_ec_datadump for only the 10th day of 2023
#start_time = "2023-01-10 00:00:00"
#end_time = "2023-01-11 00:00:00"
#osn_ec_datadump = spark.table("project_aiu.osn_ec_datadump").filter(
#    (from_unixtime(col("event_time")) >= lit(start_time)) &
#    (from_unixtime(col("event_time")) < lit(end_time))
#)

## Filter osn_flight_table_with_id for the same day
#osn_flight_table_with_id = osn_flight_table_with_id.filter(
#    (col("adep_min_distance_time") >= lit(start_time)) &
#    (col("adep_min_distance_time") < lit(end_time))
#)

## Second check: Ensure there are rows left after filtering
#print("Rows in osn_flight_table_with_id after filtering:", osn_flight_table_with_id.count())
















## Adjust the batch size and repartition size based on one day's worth of data
#batch_size = 100000
#osn_ec_datadump = osn_ec_datadump.repartition(100)

## Count the total number of rows in osn_ec_datadump for the specific day
#total_rows = osn_ec_datadump.count()

## Calculate the number of batches
#num_batches = int(total_rows / batch_size) + 1

#print(f"Batch size: {batch_size}")
#print(f"Total rows: {total_rows}")
#print(f"Number of batches: {num_batches}")

#for i in range(num_batches):
#    # Extract a batch from osn_ec_datadump
#    batch_df = osn_ec_datadump.limit(batch_size)

#    # Convert the event_time column of the batch_df to a timestamp
#    event_time_as_timestamp = from_unixtime(batch_df.event_time)
#    
#    # Perform the join operation on the batch
#    result_batch_df = batch_df.join(
#        broadcast(osn_flight_table_with_id),
#        (batch_df.icao24 == osn_flight_table_with_id.icao24) & 
#        (batch_df.callsign == osn_flight_table_with_id.flt_id) &
#        (event_time_as_timestamp.between(
#            osn_flight_table_with_id.adep_min_distance_time,
#            osn_flight_table_with_id.ades_min_distance_time)
#        ),
#        'left'
#    ).select(batch_df["*"], osn_flight_table_with_id["unique_id"])
#    
#    result_batch_df.show()
#    
#    # Write the results of this batch to the osn_trajectories table
#    result_batch_df.write.mode("append").insertInto("project_aiu.osn_trajectories")
#    
#    # Exclude the processed batch from osn_ec_datadump for the next iteration
#    osn_ec_datadump = osn_ec_datadump.subtract(batch_df)

