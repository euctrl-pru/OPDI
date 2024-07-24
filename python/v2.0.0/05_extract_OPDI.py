from IPython.display import display, HTML 
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, pandas_udf, col, PandasUDFType, lit, round
from pyspark.sql.types import DoubleType, StructType, StructField
from pyspark.sql import functions as F
from pyspark.sql import Window

import os, time
import subprocess
import os,shutil
from datetime import datetime, timedelta, date
import pandas as pd
import numpy as np

# Adding python folder
import sys
sys.path.append('/home/cdsw/python/v2.0.0/')
from helperfunctions import *

# Settings

## Project settings
project = "project_opdi"

## Date range
start_date = date(2022, 1, 1)
end_date = date(2024, 7, 1)

## Versions
export_version = 'v002'

v_long_flist = 'v0.0.2' # Flight list
v_short_flist = 'v002'

v_long_events = 'v0.0.2' # Flight events
v_short_events = 'v002'

v_long_measures = 'v0.0.2' # Measurements
v_short_measures = 'v002'

# Getting today's date
today = datetime.today().strftime('%d %B %Y')

# Makedirs

opath = f"/home/cdsw/data/OPDI/{export_version}"

try:
    os.makedirs(f'{opath}/flight_list/', exist_ok=True)
    os.makedirs(f'{opath}/flight_events/', exist_ok=True)
    os.makedirs(f'{opath}/measurements/', exist_ok=True)
except OSError as error: 
    print(error)  

# Spark Session Initialization
spark = SparkSession.builder \
    .appName("OPDI Extraction") \
    .config("spark.log.level", "ERROR")\
    .config("spark.ui.showConsoleProgress", "false")\
    .config("spark.hadoop.fs.azure.ext.cab.required.group", "eur-app-opdi") \
    .config("spark.kerberos.access.hadoopFileSystems", "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged") \
    .config("spark.driver.cores", "1") \
    .config("spark.driver.memory", "10G") \
    .config("spark.executor.memory", "10G") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "10") \
    .config("spark.network.timeout", "800s")\
    .config("spark.executor.heartbeatInterval", "400s") \
    .config('spark.ui.showConsoleProgress', False) \
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
months = generate_months(start_date, end_date)

for month in months:
    start_month_unix, end_month_unix = get_start_end_of_month(month)
    start_month_str = pd.Timestamp(start_month_unix, unit = 's').strftime('%Y%m%d')
    end_month_str = pd.Timestamp(end_month_unix, unit = 's').strftime('%Y%m%d')
    
    month_str = pd.Timestamp(start_month_unix, unit = 's').strftime('%Y%m')
    
    try:
        # Execute the query and convert to pandas DataFrame
        df = get_data_within_timeframe(spark, table_name = 'project_opdi.opdi_flight_list', month = month, time_col = 'first_seen', unix_time = False).toPandas()
        df_mod = df.loc[:, ['id', 'adep', 'ades', 'icao24', 'flt_id', 'first_seen', 'last_seen', 'dof']].rename({'flt_id':'FLT_ID', 'dof':'DOF'},axis=1)
        df_mod = df_mod.sort_values('first_seen').reset_index(drop=True)
        df_mod['version'] = f'flight_list_{v_long_flist}'
        df_mod.to_csv(f'{opath}/flight_list/flight_list_{v_short_flist}_{month_str}.csv.gz', compression='gzip',index=False)
        df_mod.to_parquet(f'{opath}/flight_list/flight_list_{v_short_flist}_{month_str}.parquet')

    except Exception as e:
        print(f"Error executing query: {e}")
    
    
################
# EVENT TABLE  #
################

#print()
#print()
#print("Event Table time")
#print()

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
#start_period = start_date
#end_period = end_date
#interval = timedelta(days=10)

#while start_period < end_period:
#    end_date = start_period + interval
#    process_and_save_data(start_period, end_date)
#    start_period = end_date

######################
# MEASUREMENT TABLE  #
######################

#print()
#print()
#print("Measurement Table time")
#print()

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
#start_period = start_date
#end_period = end_date
#interval = timedelta(days=10)

#while start_period < end_period:
#    end_date = start_period + interval
#    # Adjust end_date to not exceed the end_period
#    if end_date > end_period:
#        end_date = end_period
#    process_and_save_data(start_period, end_date)
#    start_period = end_date
