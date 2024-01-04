from IPython.display import display, HTML
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, pandas_udf, col, PandasUDFType, lit, round
from pyspark.sql.types import DoubleType, StructType, StructField
from pyspark.sql import functions as F
from pyspark.sql import Window

import os, time
import subprocess
import os,shutil
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# Settings

## Project settings
project = "project_aiu"

## Date range
start_date = datetime.strptime('2023-07-01', '%Y-%m-%d')
end_date = datetime.strptime('2023-09-30', '%Y-%m-%d')

## Versions
export_version = 'v004'

v_long_flist = 'v0.0.1' # Flight list
v_short_flist = 'v001'

v_long_events = 'v0.0.1'
v_short_events = 'v001'

v_long_measures = 'v0.0.1'
v_short_measures = 'v001'

# Getting today's date
today = datetime.today().strftime('%d %B %Y')

# Makedirs

try: 
    os.mkdir(f'/home/cdsw/python/data/{export_version}/')
except OSError as error: 
    print(error)  

try: 
    os.mkdir(f'/home/cdsw/python/data/{export_version}/flight_list/')
except OSError as error: 
    print(error)  
    
try: 
    os.mkdir(f'/home/cdsw/python/data/{export_version}/flight_events/')
except OSError as error: 
    print(error)  
    
try: 
    os.mkdir(f'/home/cdsw/python/data/{export_version}/measurements/')
except OSError as error: 
    print(error)  


# Spark Session Initialization
shutil.copy("/runtime-addons/cmladdon-2.0.40-b150/log4j.properties", "/etc/spark/conf/") # Setting logging properties
spark = SparkSession.builder \
    .appName("Extract OPDI") \
    .config("spark.log.level", "ERROR")\
    .config("spark.hadoop.fs.azure.ext.cab.required.group", "eur-app-aiu-dev") \
    .config("spark.kerberos.access.hadoopFileSystems", "abfs://storage-fs@cdpdldev0.dfs.core.windows.net/data/project/aiu.db/unmanaged") \
    .config("spark.driver.cores", "1") \
    .config("spark.driver.memory", "11G") \
    .config("spark.executor.memory", "7G") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "6") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "400s") \
    .config("spark.driver.maxResultSize", "4g") \
    .enableHiveSupport() \
    .getOrCreate()
# Get environment variables
engine_id = os.getenv('CDSW_ENGINE_ID')
domain = os.getenv('CDSW_DOMAIN')

# Format the URL
url = f"https://spark-{engine_id}.{domain}"

# Display the clickable URL
display(HTML(f'<a href="{url}">{url}</a>'))

##################
# FLIGHT TABLE   #
##################

print()
print()
print("Flight Table time")
print()

print(f'Extracting flight table from {start_date} until {end_date}...')

flight_table_sql = f"""
SELECT * 
FROM `project_aiu`.`osn_flight_table` 
WHERE to_date(from_unixtime(first_seen)) >= '{start_date.strftime('%Y-%m-%d')}' 
AND to_date(from_unixtime(first_seen)) < '{end_date.strftime('%Y-%m-%d')}'
"""

try:
    # Execute the query and convert to pandas DataFrame
    df = spark.sql(flight_table_sql).toPandas()
    df_mod = df.loc[:,['TRACK_ID', 'ADEP', 'ADES', 'ICAO24', 'FLT_ID','FIRST_SEEN','LAST_SEEN', 'DOF']].rename({
    'TRACK_ID':'id', 'ICAO_24':'icao24', 'FIRST_SEEN':'first_seen', 'LAST_SEEN':'last_seen'},axis=1)

    df_mod['first_seen'] = df_mod['first_seen'].apply(lambda l:pd.Timestamp(l, unit='s'))
    df_mod['last_seen'] = df_mod['last_seen'].apply(lambda l:pd.Timestamp(l, unit='s'))
    df_mod = df_mod.sort_values('first_seen').reset_index(drop=True)
    df_mod['version'] = f'flight_list_{v_long_flist}'
    df_mod.to_csv(f'/home/cdsw/python/data/v002/flight_list/flight_table_{v_short_flist}.csv.gz', compression='gzip',index=False)
    df_mod.to_parquet(f'/home/cdsw/python/data/v002/flight_list/flight_table_{v_short_flist}.parquet')
    
except Exception as e:
    print(f"Error executing query: {e}")
    
    
################
# EVENT TABLE  #
################

print()
print()
print("Event Table time")
print()

# Function to process and save the data for a given date interval
def process_and_save_data(start_date, end_date):
    # Convert start_date and end_date to strings for SQL query
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    print(f"Extracting EVENT table {start_date_str} until {end_date_str}...")
    
    # SQL query
    milestone_sql = f"""
    WITH flight_table AS (
        SELECT TRACK_ID
        FROM `project_aiu`.`osn_flight_table` 
        WHERE to_date(from_unixtime(first_seen)) >= '{start_date_str}' 
        AND to_date(from_unixtime(first_seen)) < '{end_date_str}' 
    ) 

    SELECT * 
    FROM `project_aiu`.`osn_milestones`
    WHERE osn_milestones.flight_id IN (SELECT TRACK_ID FROM flight_table)
    """

    # Execute the query and convert to Pandas DataFrame
    df = spark.sql(milestone_sql).toPandas()

    # Rename column, convert event_time, and add version
    df_mod = df.rename({'milestone_type': 'type'}, axis=1)
    df_mod['event_time'] = df_mod['event_time'].apply(lambda l: pd.Timestamp(l, unit='s'))
    df_mod['version'] = f'flight_events_{v_long_events}'

    # Save the DataFrame as a Parquet file
    file_path = f'/home/cdsw/python/data/v002/flight_events/flight_events_{start_date_str}_{end_date_str}.parquet'
    df_mod.to_parquet(file_path, index=False)

# Loop from 2022-05-01 until 2022-07-01 in 10 days intervals
start_period = start_date
end_period = end_date
interval = timedelta(days=10)

while start_period < end_period:
    end_date = start_period + interval
    process_and_save_data(start_period, end_date)
    start_period = end_date

######################
# MEASUREMENT TABLE  #
######################

print()
print()
print("Measurement Table time")
print()

# Function to process and save the data for a given date interval
def process_and_save_data(start_date, end_date):
    # Convert start_date and end_date to strings for SQL query
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    print(f'Extracting measurement table {start_date_str} until {end_date_str}...')
    
    # SQL query
    measurement_sql = f"""
    WITH 
        flight_table AS (
            SELECT TRACK_ID
            FROM `project_aiu`.`osn_flight_table` 
            WHERE to_date(from_unixtime(first_seen)) >= '{start_date_str}' 
            AND to_date(from_unixtime(first_seen)) < '{end_date_str}' 
        ),
        event_table AS (
            SELECT id 
            FROM `project_aiu`.`osn_milestones`
            WHERE osn_milestones.flight_id IN (SELECT TRACK_ID FROM flight_table)
        )

    SELECT * 
    FROM `project_aiu`.`osn_measurements`
    WHERE osn_measurements.milestone_id in (SELECT id from event_table) 
    """

    # Execute the query and convert to Pandas DataFrame
    df = spark.sql(measurement_sql).toPandas()

    # Rename column and add version
    df_mod = df.rename({'milestone_id': 'event_id'}, axis=1)
    df_mod['version'] = f'measurements_{v_long_measures}'

    # Save the DataFrame as a Parquet file
    file_path = f'/home/cdsw/python/data/v002/measurements/measurements_{start_date_str}_{end_date_str}.parquet'
    df_mod.to_parquet(file_path, index=False)

# Loop from start_period until end_period in 10 days intervals
start_period = start_date
end_period = end_date
interval = timedelta(days=10)

while start_period < end_period:
    end_date = start_period + interval
    # Adjust end_date to not exceed the end_period
    if end_date > end_period:
        end_date = end_period
    process_and_save_data(start_period, end_date)
    start_period = end_date
