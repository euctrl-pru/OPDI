import pandas as pd
import h3pandas
from os import listdir
from os.path import isfile, join
from pyspark.sql import SparkSession

# Spark Session Initialization
spark = SparkSession.builder \
    .appName("Upload H3 Runways") \
    .config("spark.log.level", "ERROR")\
    .config("spark.ui.showConsoleProgress", "false")\
    .config("spark.hadoop.fs.azure.ext.cab.required.group", "eur-app-aiu") \
    .config("spark.kerberos.access.hadoopFileSystems", "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/aiu.db/unmanaged") \
    .config("spark.driver.cores", "1") \
    .config("spark.driver.memory", "8G") \
    .config("spark.executor.memory", "6G") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "6") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "400s") \
    .enableHiveSupport() \
    .getOrCreate()

def preprocess_rwy_data(df):
    columns = ['airport_ident', 'le_ident', 'le_heading_degT',
           'he_ident', 'he_heading_degT', 'gate_id', 'hex_id', 'gate_id_nr']
    df = df[columns]
    df = df.set_index('hex_id')
    df = df.h3.h3_get_resolution()
    df = df.h3.h3_to_geo()
    df['hex_lon'] = df.geometry.apply(lambda l:l.x)
    df['hex_lat'] = df.geometry.apply(lambda l:l.y)
    df = df.drop(['geometry'],axis=1).reset_index()
    df = df.rename({'h3_resolution':'hex_res'}, axis=1)
    reorder_cols = ['airport_ident', 'le_ident', 'le_heading_degT',
           'he_ident', 'he_heading_degT', 'hex_id', 'hex_res', 'hex_lon', 'hex_lat', 'gate_id', 'gate_id_nr']
    return(df[reorder_cols])


def load_rwy_data_to_dl(spark, pdf, project, table):
    sdf = spark.createDataFrame(pdf)
    sdf.write.mode("append").insertInto(f"`{project}`.`{table}`")
    return None

rwy_files_path = 'data/runway_hex'
rwy_files = [f for f in listdir(rwy_files_path) if isfile(join(rwy_files_path, f))]

for filename in rwy_files: 
    print(f"Processing file: {filename}")
    try: 
      # Extract
      pdf = pd.read_parquet(f'{rwy_files_path}/{filename}')

      # Transform
      pdf = preprocess_rwy_data(pdf)

      # Load
      load_rwy_data_to_dl(spark, pdf, project = 'project_aiu', table = 'opdi_runway_hexagons')
      
    except Exception as e:
      print(f"Failed to process [{filename}], error: {e}.")