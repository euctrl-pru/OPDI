# pyspark.sql imports
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    udf, pandas_udf, PandasUDFType, lit, col,
    to_timestamp, expr, from_unixtime
)
from pyspark.sql import functions as F, SparkSession, Window
from pyspark.sql.types import DoubleType, StructType, StructField

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
    .config("spark.sql.shuffle.partitions", "800")\
    .getOrCreate()

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
        f"SELECT DISTINCT * FROM `project_aiu`.`airport_proximity_flight_data`;"
    )
    flight_data = flight_data.withColumn('event_time', F.from_unixtime('event_time', format='yyyy-MM-dd HH:mm:ss'))
    return flight_data.orderBy(['icao24', 'callsign', 'airport_ident', 'event_time']).na.drop(subset=['callsign'])

def categorize_by_vert_rate_spark(df):
    """Categorize the vertical rate into 'Take-off', 'Landing', or 'Ambiguous'."""
    avg_vert_rate = df.groupBy(['icao24', 'callsign', 'airport_ident']).agg(F.avg('vert_rate').alias('avg_vert_rate'))
    avg_vert_rate = avg_vert_rate.withColumn('status',
                                            F.when(avg_vert_rate['avg_vert_rate'] > 2, 'Take-off')
                                            .when(avg_vert_rate['avg_vert_rate'] < 2, 'Landing')
                                            .otherwise('Ambiguous'))
    return df.join(avg_vert_rate, on=['icao24', 'callsign', 'airport_ident'], how='left')

def compute_distance_event_times_spark(df):
    """Compute event times for min and max distances."""
    window_spec = Window.partitionBy(['icao24', 'callsign', 'airport_ident', 'status'])
    df = df.withColumn('min_distance', F.min('distance').over(window_spec))
    df = df.withColumn('max_distance', F.max('distance').over(window_spec))

    df_min = df.filter(df.distance == df.min_distance).select(
        ['icao24', 'callsign', 'airport_ident', 'status', 'event_time', 'min_distance']).withColumnRenamed('event_time', 'event_time_min_distance')
    df_max = df.filter(df.distance == df.max_distance).select(
        ['icao24', 'callsign', 'airport_ident', 'status', 'event_time', 'max_distance']).withColumnRenamed('event_time', 'event_time_max_distance')

    return df_min.join(df_max, on=['icao24', 'callsign', 'airport_ident', 'status'], how='inner')


def compute_flight_ids_spark(df):
    """Compute flight IDs based on time difference threshold."""
    window_spec = Window.partitionBy(['icao24', 'callsign', 'status']).orderBy('event_time_min_distance')
    df = df.withColumn('time_diff', F.lag('event_time_min_distance').over(window_spec) - df['event_time_min_distance'])
    df = df.withColumn('flight_increment', F.when(df['time_diff'] > 7200, 1).otherwise(0)) # 7200 seconds = 2 hours
    df = df.withColumn('flight_id', F.sum('flight_increment').over(window_spec) + 1)
    return df

def process_and_pivot_data_spark(df):
    """Process the dataframe to remove 'Ambiguous' rows, pivot, and select specific columns."""
    df = df.filter(df.status != "Ambiguous")
    df_pivot = df.groupBy(['icao24', 'callsign']).pivot('status').agg(
        F.first('airport_ident').alias('airport_ident'),
        F.first('min_distance').alias('min_distance'),
        F.first('event_time_min_distance').alias('event_time_min_distance'),
    )
    
    # Select and rename
    df_pivot = df_pivot.select(
        col('icao24').alias('ICAO24'),
        col('callsign').alias('FLT_ID'),
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
#flight_data_spark = load_flight_data_spark()
#flight_data_spark = categorize_by_vert_rate_spark(flight_data_spark)
#grouped_df_spark = compute_distance_event_times_spark(flight_data_spark)
#df_spark = compute_flight_ids_spark(grouped_df_spark)
#df_pivot_spark = process_and_pivot_data_spark(df_spark)
#df_pivot_spark = df_pivot_spark.write.mode('overwrite').insertInto(f"project_aiu.osn_flight_table")

# Assuming you've read in your dataframes
osn_ec_datadump = spark.table("project_aiu.osn_ec_datadump").dropDuplicates()
osn_flight_table = spark.table("project_aiu.osn_flight_table").dropDuplicates()

# Assign unique IDs to osn_flight_table
osn_flight_table_with_id = osn_flight_table.withColumn("unique_id", F.monotonically_increasing_id())

# Persisting the unique_id back to osn_flight_table
osn_flight_table_with_id.write.mode("overwrite").insertInto("project_aiu.osn_flight_table_with_id")

from pyspark.sql.functions import from_unixtime, broadcast, expr

# Define the batch size based on available memory and resources
batch_size = 500000000  # Adjust this value as necessary

osn_ec_datadump = osn_ec_datadump.repartition(1000)  # Adjust the number of partitions as necessary

# Count the total number of rows in osn_ec_datadump
total_rows = osn_ec_datadump.count()

# Calculate the number of batches
num_batches = int(total_rows / batch_size) + 1

print(f"Batch size: {batch_size}")
print(f"Total rows: {total_rows}")
print(f"Number of batches: {num_batches}")

for i in range(num_batches):
    # Extract a batch from osn_ec_datadump
    batch_df = osn_ec_datadump.limit(batch_size)

    # Convert the event_time column of the batch_df to a timestamp
    event_time_as_timestamp = from_unixtime(batch_df.event_time)
    
    # Perform the join operation on the batch
    result_batch_df = batch_df.join(
        broadcast(osn_flight_table_with_id),  # Broadcasting smaller DF
        (batch_df.icao24 == osn_flight_table_with_id.icao24) & 
        (batch_df.callsign == osn_flight_table_with_id.flt_id) &
        (event_time_as_timestamp.between(
            osn_flight_table_with_id.adep_min_distance_time - expr("INTERVAL 1800 SECONDS"),
            osn_flight_table_with_id.ades_min_distance_time + expr("INTERVAL 1800 SECONDS"))
        ),
        'left'
    ).select(batch_df["*"], osn_flight_table_with_id["unique_id"])

    # Write the results of this batch to the osn_trajectories table
    result_batch_df.write.mode("append").insertInto("project_aiu.osn_trajectories")
    
    # Exclude the processed batch from osn_ec_datadump for the next iteration
    osn_ec_datadump = osn_ec_datadump.subtract(batch_df)
