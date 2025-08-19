# Spark imports
from pyspark.sql import SparkSession, DataFrame

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
    .config("spark.executor.extraClassPath", "/opt/spark/optional-lib/iceberg-spark-runtime-3.3_2.12-1.3.1.1.20.7216.0-70.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark/optional-lib/iceberg-spark-runtime-3.3_2.12-1.3.1.1.20.7216.0-70.jar") \
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

# Settings
# Get environment variables
engine_id = os.getenv('CDSW_ENGINE_ID')
domain = os.getenv('CDSW_DOMAIN')

# Format the URL
url = f"https://spark-{engine_id}.{domain}"

# Display the clickable URL
display(HTML(f'<a href="{url}">{url}</a>'))

def haversine(lat1, lon1, lat2, lon2):
    # Radius of the Earth in kilometers
    R = 6371.0
    
    # Convert latitude and longitude from degrees to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    # Differences in coordinates
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    # Haversine formula
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    # Distance in kilometers
    distance = R * c
    return distance

def fetch_tracks(
    project,
    spark, 
    date, 
    ades, 
    adep,
    apt, # filter params
    apt_lat,
    apt_lon):
    
    # assigned regular string date
    date_str = date.strftime('%Y-%m-%d')
    

    # displaying unix timestamp after conversion
    date_unix = int(time.mktime(date.timetuple()))

    start_time_unix = date_unix - 1.5*24*60*60
    end_time_unix = date_unix + 1.5*24*60*60

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
    fname = path + f'tracks_adep_{adep}_ades_{ades}_{date_str}.parquet'
    
    if os.path.exists(fname):
        print(f'File {fname} exists already.')
        return None
    
    query = f"""
    SELECT otc.*, oft.ICAO24, oft.FLT_ID, oft.DOF, oft.ADEP, oft.ADES, oft.REGISTRATION, oft.MODEL, oft.TYPECODE, oft.ICAO_AIRCRAFT_CLASS, oft.ICAO_OPERATOR
    FROM (
        SELECT *
        FROM {project}.osn_tracks
        WHERE event_time BETWEEN FROM_UNIXTIME({int(start_time_unix)}) AND FROM_UNIXTIME({int(end_time_unix)})
        ) otc
    JOIN {project}.opdi_flight_list oft 
        ON otc.track_id = oft.id
    WHERE oft.DOF = TO_DATE('{date_str}')
    {ades_sql} 
    {adep_sql};"""
    
    df = spark.sql(query).toPandas()

    df[f'distance_from_{apt}_NM'] = df.apply(lambda l: haversine(l['lat'], l['lon'], apt_lat, apt_lon), axis = 1)/1.852
    #df = df[df[f'distance_from_{apt}_NM']<200]
    
    df.to_parquet(fname)


def get_all_days_between(start_date: datetime, end_date: datetime) -> List[datetime]:
    """
    Given two datetimes (at 00:00:00), return a list of all days in between, including the given ones.
    
    Parameters:
    start_date (datetime): The start date.
    end_date (datetime): The end date.
    
    Returns:
    List[datetime]: A list of datetime objects representing each day in the range.
    """
    if start_date > end_date:
        raise ValueError("Start date must be before or equal to end date")
    
    delta = end_date - start_date
    all_days = [start_date + timedelta(days=i) for i in range(delta.days + 1)]
    
    return all_days

# Processing...  
start = datetime(2024, 6, 1)
end = datetime(2024, 6, 30)
days_list = get_all_days_between(start, end)

days_list.sort(reverse=True)

print("Starting ADES/ADEP: EGLL / EHAM / EDDF")
for date in days_list:
    print(f"Processing: {date}")
    print("Processing EGLL")
    fetch_tracks(project, spark, date, ades = 'EGLL', adep = None, apt = "EGLL", apt_lat = 51.4775, apt_lon = 0.04472)
    fetch_tracks(project, spark, date, adep = 'EGLL', ades = None, apt = "EGLL", apt_lat = 51.4775, apt_lon = 0.04472)
    print("Processing EHAM")
    fetch_tracks(project, spark, date, ades = 'EHAM', adep = None, apt = "EHAM", apt_lat = 52.3080555555555, apt_lon = 4.76416666666667)
    fetch_tracks(project, spark, date, adep = 'EHAM', ades = None, apt = "EHAM", apt_lat = 52.3080555555555, apt_lon = 4.76416666666667)
    print("Processing EDDF")
    fetch_tracks(project, spark, date, ades = 'EDDF', adep = None, apt = "EDDF", apt_lat = 50.0333333333333, apt_lon = 8.57055555555555)
    fetch_tracks(project, spark, date, adep = 'EDDF', ades = None, apt = "EDDF", apt_lat = 50.0333333333333, apt_lon = 8.57055555555555)