
# Spark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import date_format

# Regular imports
from IPython.display import display, HTML
import os, time,shutil
import subprocess
from datetime import datetime, date, timedelta
import pandas as pd
import dateutil.relativedelta
import calendar
from typing import List
import math
import calendar
from pyspark.sql.functions import col
import numpy as np
np.object0 = object

# Settings
project = "project_opdi"

# Getting today's date
today = datetime.today().strftime('%d %B %Y')

# Setting logging properties
#shutil.copy("/runtime-addons/cmladdon-2.0.40-b154/log4j.properties", "/etc/spark/conf/") 

# Spark Session Initialization
spark = SparkSession.builder \
    .appName("OPDI Ingestion") \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.hadoop.fs.azure.ext.cab.required.group", "eur-app-opdi") \
    .config("spark.kerberos.access.hadoopFileSystems", "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged") \
    .config("spark.executor.extraClassPath", "/opt/spark/optional-lib/iceberg-spark-runtime-3.3_2.12-1.3.1.1.22.7216.0-241.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark/optional-lib/iceberg-spark-runtime-3.3_2.12-1.3.1.1.22.7216.0-241.jar") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged") \
    .config("spark.driver.cores", "1") \
    .config("spark.driver.memory", "2G") \
    .config("spark.executor.memory", "5G") \
    .config("spark.executor.memoryOverhead", "3G") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "3") \
    .config("spark.dynamicAllocation.maxExecutors", "20") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "400s") \
    .config("spark.driver.maxResultSize", "6g") \
    .config("spark.shuffle.compress", "true") \
    .config("spark.shuffle.spill.compress", "true") \
    .enableHiveSupport() \
    .getOrCreate()

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")


# Settings
# Get environment variables
engine_id = os.getenv('CDSW_ENGINE_ID')
domain = os.getenv('CDSW_DOMAIN')

# Format the URL
url = f"https://spark-{engine_id}.{domain}"

# Display the clickable URL
display(HTML(f'<a href="{url}">{url}</a>'))


def fetch_tracks(
    project,
    spark, 
    start_date, 
    end_date, 
    ades, 
    adep):
    
    # assigned regular string date
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    # displaying unix timestamp after conversion
    start_date_unix = int(time.mktime(start_date.timetuple()))
    end_date_unix = int(time.mktime(end_date.timetuple()))

    start_time_unix = start_date_unix - 1.5*24*60*60
    end_time_unix = end_date_unix + 1.5*24*60*60

    ades_sql = ''
    adep_sql = ''
    
    if pd.isnull(ades):
        ades = '*'
    else:
        ades_sql = f"AND oft.ADES = '{ades}'"
    
    if pd.isnull(adep):
        adep = '*'
    else:
        adep_sql = f"AND oft.ADEP = '{adep}'"
    
    path = f'data/{adep}-to-{ades}/'
    os.makedirs(path,exist_ok=True)
    fname = f'adep_{adep}_ades_{ades}_{start_date_str}.parquet'
    
    if os.path.exists(path + f"tracks_{fname}") and os.path.exists(path + f"opdi_{fname}"):
        print(f'File {fname} exists already.')
        return None
    
    query_tracks = f"""
    SELECT otc.*, oft.ICAO24, oft.FLT_ID, oft.DOF, oft.ADEP, oft.ADES, oft.REGISTRATION, oft.MODEL, oft.TYPECODE, oft.ICAO_AIRCRAFT_CLASS, oft.ICAO_OPERATOR
    FROM (
        SELECT *
        FROM {project}.osn_tracks
        WHERE event_time BETWEEN FROM_UNIXTIME({int(start_time_unix)}) AND FROM_UNIXTIME({int(end_time_unix)})
        ) otc
    JOIN {project}.opdi_flight_list oft 
        ON otc.track_id = oft.id
    WHERE oft.DOF >= TO_DATE('{start_date_str}') AND oft.DOF < TO_DATE('{end_date_str}')
    {ades_sql} 
    {adep_sql};"""
    
    query_events = f"""
    SELECT 
        oft.ICAO24, 
        oft.FLT_ID, 
        oft.DOF, 
        oft.ADEP, 
        oft.ADES, 
        oft.REGISTRATION, 
        oft.MODEL, 
        oft.TYPECODE, 
        oft.ICAO_AIRCRAFT_CLASS, 
        oft.ICAO_OPERATOR,
        otc.*, 
        odm.id as MEASUREMENT_ID,
        odm.milestone_id as MEASUREMENT_EVENT_ID,
        odm.type AS MEASUREMENT_TYPE,
        odm.value AS MEASUREMENT_VALUE,
        odm.version AS MEASUREMENT_VERSION
    FROM (
        SELECT *
        FROM {project}.opdi_flight_events
        WHERE event_time BETWEEN FROM_UNIXTIME({int(start_time_unix)}) AND FROM_UNIXTIME({int(end_time_unix)})
    ) otc
    JOIN {project}.opdi_flight_list oft 
        ON otc.flight_id = oft.id
    LEFT JOIN {project}.opdi_measurements odm
        ON odm.milestone_id = otc.id
    WHERE oft.DOF >= TO_DATE('{start_date_str}')
      AND oft.DOF < TO_DATE('{end_date_str}')
      {ades_sql}
      {adep_sql};
    """
    
    #print(query_tracks)
    #print(query_events)
    
    df_tracks = spark.sql(query_tracks)
    
    datetime_columns = [field for field, dtype in df_tracks.dtypes if dtype == 'timestamp']
    for column in datetime_columns:
        df_tracks = df_tracks.withColumn(column, date_format(column, "yyyy-MM-dd HH:mm:ss"))
    
    df_tracks_pd = df_tracks.toPandas()
    df_tracks_pd.to_parquet(path + f"tracks_{fname}")
    
    df_events = spark.sql(query_events)
    
    datetime_columns = [field for field, dtype in df_events.dtypes if dtype == 'timestamp']
    for column in datetime_columns:
        df_events = df_events.withColumn(column, date_format(column, "yyyy-MM-dd HH:mm:ss"))
    
    df_events_pd = df_events.toPandas()
    df_events_pd.to_parquet(path + f"opdi_{fname}")


def get_first_days_of_months(start_date: datetime, end_date: datetime) -> List[datetime]:
    """
    Given two datetimes, return a list of the first and last days of each month in between, including the given ones.

    Parameters:
    start_date (datetime): The start date.
    end_date (datetime): The end date.

    Returns:
    List[datetime]: A list of datetime objects representing the first and last days of each month in the range.
    """
    if start_date > end_date:
        raise ValueError("Start date must be before or equal to end date")

    result = []

    # Iterate through each month in the range
    current_date = start_date
    while current_date <= end_date:
        # Get the first day of the current month
        first_day = current_date.replace(day=1)
        result.append(first_day)
        
        # Move to the next month
        current_date = first_day + timedelta(days=32)
        current_date = current_date.replace(day=1)

    # Remove duplicates and sort the list
    result = sorted(list(set(result)))

    return result
  
# Processing...  
start = datetime(2024, 2, 1)
end = datetime(2025, 4, 30)
days_list = get_first_days_of_months(start, end)

#days_list.sort(reverse=True)

print("Starting ADES/ADEP: UGTB / UGKO / UGSB")
for i in range(len(days_list)):
    start_date = days_list[i]
    end_date = days_list[i+1]
    print(f"Processing: {start_date} - {end_date}")
    print("Processing UGTB")
    fetch_tracks(project, spark, start_date, end_date, ades = 'UGTB', adep = None)
    fetch_tracks(project, spark, start_date, end_date, adep = 'UGTB', ades = None)
    print("Processing UGKO")
    fetch_tracks(project, spark, start_date, end_date, ades = 'UGKO', adep = None)
    fetch_tracks(project, spark, start_date, end_date, adep = 'UGKO', ades = None)
    print("Processing UGSB")
    fetch_tracks(project, spark, start_date, end_date, ades = 'UGSB', adep = None)
    fetch_tracks(project, spark, start_date, end_date, adep = 'UGSB', ades = None)