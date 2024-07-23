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


# Settings
project = "project_opdi"

# Getting today's date
today = datetime.today().strftime('%d %B %Y')

# Setting logging properties
#shutil.copy("/runtime-addons/cmladdon-2.0.40-b154/log4j.properties", "/etc/spark/conf/") 

# Spark Session Initialization
spark = SparkSession.builder \
    .appName("OSN Flight Table") \
    .config("spark.log.level", "ERROR")\
    .config("spark.hadoop.fs.azure.ext.cab.required.group", "eur-app-opdi") \
    .config("spark.kerberos.access.hadoopFileSystems", "abfs://storage-fs@cdpdldev0.dfs.core.windows.net,abfs://storage-fs@cdpdllive.dfs.core.windows.net") \
    .config("spark.driver.cores", "1") \
    .config("spark.driver.memory", "8G") \
    .config("spark.executor.memory", "5G") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "10") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "400s") \
    .config("spark.driver.maxResultSize", "4g") \
    .config('spark.ui.showConsoleProgress', False) \
    .enableHiveSupport() \
    .getOrCreate()

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
    date, 
    ades, 
    adep):
    
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
    
    path = f'data/retrievals/{adep}-to-{ades}/'
    os.makedirs(path,exist_ok=True)
    fname = path + f'tracks_adep_{adep}_ades_{ades}_{date_str}.parquet'
    
    if os.path.exists(fname):
        print(f'File {fname} exists already.')
        return None
    
    query = f"""
    SELECT otc.* 
    FROM (
        SELECT *
        FROM {project}.osn_tracks_clustered
        WHERE event_time BETWEEN {int(start_time_unix)} AND {int(end_time_unix)}
        ) otc
    JOIN {project}.opdi_flight_list oft 
        ON otc.track_id = oft.id
    WHERE oft.DOF = TO_DATE('{date_str}')
    {ades_sql} 
    {adep_sql};"""
    
    df = spark.sql(query).toPandas()
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
start = datetime(2024, 3, 1)
end = datetime(2024, 6, 30)
days_list = get_all_days_between(start, end)

print("Starting ADES: LPPT and EIDW")
for date in days_list:
    print(f"Processing: {date} for LPPT")
    fetch_tracks(project, spark, date, ades = 'LPPT', adep = None)
    print(f"Processing: {date} for EIDW")
    fetch_tracks(project, spark, date, ades = 'EIDW', adep = None)