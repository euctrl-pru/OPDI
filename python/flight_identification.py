from pyspark.sql import functions as F
from pyspark.sql import Window
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

from pyspark.sql.functions import col
from pyspark.sql.functions import to_timestamp

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
flight_data_spark = load_flight_data_spark()
flight_data_spark = categorize_by_vert_rate_spark(flight_data_spark)
grouped_df_spark = compute_distance_event_times_spark(flight_data_spark)
df_spark = compute_flight_ids_spark(grouped_df_spark)
df_pivot_spark = process_and_pivot_data_spark(df_spark)
df_pivot_spark = df_pivot_spark.write.mode('overwrite').insertInto(f"project_aiu.osn_flight_table")