from IPython.display import display, HTML 
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_unixtime, pandas_udf, col, PandasUDFType, lit, round
from pyspark.sql.types import DoubleType, StructType, StructField
from pyspark.sql import functions as F
from pyspark.sql import Window

import os, time
import subprocess
import os,shutil
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import os.path
from pyspark.sql.types import TimestampType

# Adding python folder
import sys
sys.path.append('/home/cdsw/OPDI_live/python/v2.0.0/')
from helperfunctions import *

# Settings

## Project settings
project = "project_opdi"
extract_flight_list = True
extract_flight_events = True
extract_measurements = True

## Date range
start_date = date(2025, 4, 15)
end_date = date(2025, 7, 4)

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

opath = f"/home/cdsw/OPDI_live/data/OPDI/{export_version}"

try:
    os.makedirs(f'{opath}/flight_list/', exist_ok=True)
    os.makedirs(f'{opath}/flight_events/', exist_ok=True)
    os.makedirs(f'{opath}/measurements/', exist_ok=True)
except OSError as error: 
    print(error)  

# Spark Session Initialization
spark = SparkSession.builder \
    .appName("OPDI extraction") \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.hadoop.fs.azure.ext.cab.required.group", "eur-app-opdi") \
    .config("spark.kerberos.access.hadoopFileSystems", "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged") \
    .config("spark.executor.extraClassPath", "/opt/spark/optional-lib/iceberg-spark-runtime-3.3_2.12-1.3.1.1.20.7216.0-70.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark/optional-lib/iceberg-spark-runtime-3.3_2.12-1.3.1.1.20.7216.0-70.jar") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged") \
    .config("spark.driver.cores", "1") \
    .config("spark.driver.memory", "14G") \
    .config("spark.executor.memory", "8G") \
    .config("spark.executor.memoryOverhead", "3G") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "3") \
    .config("spark.dynamicAllocation.maxExecutors", "15") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "400s") \
    .config("spark.driver.maxResultSize", "6g") \
    .config("spark.shuffle.compress", "true") \
    .config("spark.shuffle.spill.compress", "true") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
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

print(f'[[Extracting FLIGHT v2 list from {start_date} until {end_date}...]]')
months = generate_months(start_date, end_date)

for month in months:
    start_month_unix, end_month_unix = get_start_end_of_month(month)
    start_month_str = pd.Timestamp(start_month_unix, unit = 's').strftime('%Y%m%d')
    end_month_str = pd.Timestamp(end_month_unix, unit = 's').strftime('%Y%m%d')
    
    month_str = pd.Timestamp(start_month_unix, unit = 's').strftime('%Y%m')
    
    filename_csv = f'{opath}/flight_list/flight_list_{month_str}.csv.gz'
    filename_parquet = f'{opath}/flight_list/flight_list_{month_str}.parquet'
    
    print(f"Extracting flight list {start_month_str} until {end_month_str}...")
    
    if os.path.isfile(filename_csv) or os.path.isfile(filename_parquet):
        print('Files exist already - SKIPPING...')
        continue
    
    try:
        # Execute the query and convert to pandas DataFrame
        df = get_data_within_timeframe(spark, table_name = f'{project}.opdi_flight_list', 
                                       month = month, time_col = 'first_seen', unix_time = False)
        df = df.toPandas()
        df_mod = df.sort_values('first_seen').reset_index(drop=True)
        df = df.loc[:,['id', 'icao24', 'flt_id', 'dof', 'adep', 'ades', 'adep_p', 'ades_p',
       'registration', 'model', 'typecode', 'icao_aircraft_class',
       'icao_operator', 'first_seen', 'last_seen', 'version']]
        df_mod.to_parquet(filename_parquet)

    except Exception as e:
        print(f"Error executing query: {e}")
    
    
################
# EVENT TABLE  #
################
# Path for Azure Blob File System (ABFS)

print()
print()
print("Event table time")
print(f'[[Extracting EVENT table from {start_date} until {end_date}...]]')
print()

# Function to process and save the data for a given date interval
def process_and_save_data_events(start_date, end_date):
    # Convert start_date and end_date to strings for SQL query
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    file_date_str_start = start_date.strftime('%Y%m%d')
    file_date_str_end = end_date.strftime('%Y%m%d')
    file_path_csv = f'{opath}/flight_events/flight_events_{file_date_str_start}_{file_date_str_end}.csv.gz'
    file_path_parquet = f'{opath}/flight_events/flight_events_{file_date_str_start}_{file_date_str_end}.parquet'
    
    print(f"Extracting EVENT table {start_date_str} until {end_date_str}...")
    
    if os.path.isfile(file_path_csv) or os.path.isfile(file_path_parquet):
        print('Files exist already - SKIPPING...')
        return None
    
    # SQL query
    milestone_sql = f"""
    WITH flight_list AS (
        SELECT id as track_id
        FROM `{project}`.`opdi_flight_list` 
        WHERE first_seen >= TO_DATE('{start_date_str}') 
        AND first_seen < TO_DATE('{end_date_str}') 
    ) 

    SELECT * 
    FROM `{project}`.`opdi_flight_events`
    WHERE opdi_flight_events.flight_id IN (SELECT track_id FROM flight_list)
    """

     # Execute the query and load data into a PySpark DataFrame
    df_mod = spark.sql(milestone_sql)
    df_mod = df_mod.withColumn("version", lit(f"{v_long_events}"))
    df_mod = df_mod.toPandas()
    
    # Save in datalake
    df_mod.to_parquet(file_path_parquet)

# Loop from 2022-05-01 until 2022-07-01 in 10 days intervals
start_period = start_date
end_period = end_date
interval = relativedelta(days=10)

while start_period < end_period:
    end_date = start_period + interval
    process_and_save_data_events(start_period, end_date)
    start_period = end_date

######################
# MEASUREMENT TABLE  #
######################

print()
print()
print("Measurement Table time")
print(f'[[Extracting MEASUREMENT table from {start_date} until {end_date}...]]')
print()

# Function to process and save the data for a given date interval
def process_and_save_data_measurements(start_date, end_date):
    # Convert start_date and end_date to strings for SQL query
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    file_date_str_start = start_date.strftime('%Y%m%d')
    file_date_str_end = end_date.strftime('%Y%m%d')
    file_path_parquet = f'{opath}/measurements/measurements_{file_date_str_start}_{file_date_str_end}.parquet'
    file_path_csv = f'{opath}/measurements/measurements_{file_date_str_start}_{file_date_str_end}.csv.gz'
    
    print(f'Extracting MEASUREMENT table {start_date_str} until {end_date_str}...')
    
    if os.path.isfile(file_path_csv) or os.path.isfile(file_path_parquet):
        print('Files exist already - SKIPPING...')
        return None
    
    # SQL query
    measurement_sql = f"""
    WITH 
        flight_list AS (
            SELECT id as track_id
            FROM `{project}`.`opdi_flight_list` 
            WHERE first_seen >= TO_DATE('{start_date_str}') 
            AND first_seen < TO_DATE('{end_date_str}') 
        ),
        event_table AS (
            SELECT id 
            FROM `{project}`.`opdi_flight_events`
            WHERE opdi_flight_events.flight_id IN (SELECT track_id FROM flight_list)
        )

    SELECT * 
    FROM `{project}`.`opdi_measurements`
    WHERE opdi_measurements.milestone_id in (SELECT id from event_table) 
    """

    # Execute the query and convert to Pandas DataFrame
    df_mod = spark.sql(measurement_sql)
    df_mod = df_mod.withColumnRenamed("milestone_id", "event_id")
    df_mod = df_mod.withColumn("version", lit(f"{v_long_measures}"))
    df_mod = df_mod.toPandas()
    
    # Save in datalake
    df_mod.to_parquet(file_path_parquet)


# Loop from start_period until end_period in 10 days intervals
start_period = start_date
end_period = end_date
interval = relativedelta(days=10)

while start_period < end_period:
    end_date = start_period + interval
    # Adjust end_date to not exceed the end_period
    if end_date > end_period:
        end_date = end_period
    process_and_save_data_measurements(start_period, end_date)
    start_period = end_date
    


