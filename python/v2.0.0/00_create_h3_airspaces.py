# Import libs
import sys
import os
import pandas as pd
from datetime import datetime
import shapely 
import h3

# Spark stuff
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("OPDI Ingestion") \
    .config("spark.log.level", "ERROR") \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.hadoop.fs.azure.ext.cab.required.group", "eur-app-opdi") \
    .config("spark.kerberos.access.hadoopFileSystems", "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged") \
    .config("spark.executor.extraClassPath", "/opt/spark/optional-lib/iceberg-spark-runtime-3.3_2.12-1.3.1.1.20.7216.0-70.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark/optional-lib/iceberg-spark-runtime-3.3_2.12-1.3.1.1.20.7216.0-70.jar") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged") \
    .config("spark.rpc.message.maxSize", "2047") \
    .config("spark.driver.cores", "1") \
    .config("spark.driver.memory", "6G") \
    .config("spark.executor.memory", "12G") \
    .config("spark.executor.memoryOverhead", "3G") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "3") \
    .config("spark.dynamicAllocation.maxExecutors", "4") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "400s") \
    .config("spark.driver.maxResultSize", "6g") \
    .config("spark.shuffle.compress", "true") \
    .config("spark.shuffle.spill.compress", "true") \
    .enableHiveSupport() \
    .getOrCreate()

# Add custom libs
sys.path.append(os.path.expanduser('~/libs'))
from geotools import *

# settings
project = 'project_opdi'
ansp = [
    'https://raw.githubusercontent.com/euctrl-pru/pruatlas/master/inst/extdata/ansps_ace_406.parquet',
    'https://raw.githubusercontent.com/euctrl-pru/pruatlas/master/inst/extdata/ansps_ace_481.parquet',
    'https://raw.githubusercontent.com/euctrl-pru/pruatlas/master/inst/extdata/ansps_ace_524.parquet']
firs = [
    #'https://raw.githubusercontent.com/euctrl-pru/pruatlas/master/inst/extdata/firs_nm_406.parquet',
    #'https://raw.githubusercontent.com/euctrl-pru/pruatlas/master/inst/extdata/firs_nm_481.parquet',
    'https://raw.githubusercontent.com/euctrl-pru/pruatlas/master/inst/extdata/firs_nm_524.parquet']
countries = [
    'https://raw.githubusercontent.com/euctrl-pru/pruatlas/master/inst/extdata/countries50m.parquet']

# Helper functions

def fill_geometry(geometry, res = 7):
     return [h3.polyfill(shapely.geometry.mapping(x), res, geo_json_conformant=True) for x in shapely.wkt.loads(geometry).geoms]

def fill_geometry_compact(geometry, res = 7):
     return [h3.compact(h3.polyfill(shapely.geometry.mapping(x), res, geo_json_conformant=True)) for x in shapely.wkt.loads(geometry).geoms]

def get_coords(h):
  try:
    return h3.h3_to_geo(h)
  except:
    print(f"Hex {h} did not result in coords using h3.h3_to_geo(hex)")
    return 0, 0
  
def process_ansp_airspaces(ansp_df, compact=False):
    """Processes ANSP airspaces by adding H3 indices and ensuring correct data formats."""
    
    cfmu_airac = {
        406: ["2015-11-12", "2015-12-10"],
        481: ["2021-8-12", "2021-9-9"],
        524: ["2024-11-28", "2024-12-26"]
    }

    # Avoid SettingWithCopyWarning by using .loc
    ansp_df = ansp_df.copy()
    
    ansp_df.loc[:, 'validity_start'] = ansp_df['airac_cfmu'].map(lambda l: cfmu_airac[l][0])
    ansp_df.loc[:, 'validity_end'] = ansp_df['airac_cfmu'].map(lambda l: cfmu_airac[l][1])

    ansp_df = ansp_df[[
        'airspace_type', 'name', 'code', 'airac_cfmu',
        'validity_start', 'validity_end', 'min_fl', 'max_fl', 'geometry_wkt'
    ]]
    
    ansp_df['min_fl'] = ansp_df['min_fl'].apply(int)
    ansp_df['max_fl'] = ansp_df['max_fl'].apply(int)

    if compact:
        ansp_df.loc[:, 'h3_res_7'] = ansp_df["geometry_wkt"].apply(lambda l: fill_geometry_compact(l))
    else:
        ansp_df.loc[:, 'h3_res_7'] = ansp_df["geometry_wkt"].apply(lambda l: fill_geometry(l))

    ansp_df = ansp_df.drop(columns=["geometry_wkt"])
    
    ansp_df = ansp_df.explode('h3_res_7').explode('h3_res_7')
    ansp_df = ansp_df[ansp_df['h3_res_7'].apply(lambda l:type(l) == str)]
    try:
      ansp_df["h3_res_7_lat"], ansp_df["h3_res_7_lon"] = zip(*ansp_df["h3_res_7"].apply(lambda l: get_coords(l)))
    except: 
      print(f'The following hexes did not result in coords: {ansp_df.h3_res_7.to_list()}')
      ansp_df["h3_res_7_lat"] = None
      ansp_df["h3_res_7_lon"] = None
    return ansp_df

def process_fir_airspaces(fir_df, compact = False):
    cfmu_airac = {
        406: ["2015-11-12", "2015-12-10"],
        481: ["2021-8-12", "2021-9-9"],
        524: ["2024-11-28", "2024-12-26"]
    }
    
    # Avoid SettingWithCopyWarning by using .loc
    fir_df = fir_df.copy()
    
    fir_df.loc[:,'validity_start'] = fir_df['airac_cfmu'].apply(lambda l: cfmu_airac[l][0])
    fir_df.loc[:,'validity_end'] = fir_df['airac_cfmu'].apply(lambda l: cfmu_airac[l][1])
    
    #fir_df = fir_df.rename({'id':'code'},axis=1)
    
    fir_df = fir_df[[
        'airspace_type',
        'name',
        'code',
        'airac_cfmu',
        'validity_start',
        'validity_end',
        'min_fl',
        'max_fl',
        'geometry_wkt'
    ]]
    
    fir_df.loc[:,'min_fl'] = fir_df['min_fl'].apply(int)
    fir_df.loc[:,'max_fl'] = fir_df['max_fl'].apply(int)
    
    if compact:
        fir_df.loc[:, 'h3_res_7'] = fir_df.geometry_wkt.apply(lambda l: fill_geometry_compact(l))
    else:
        fir_df.loc[:, 'h3_res_7'] = fir_df.geometry_wkt.apply(lambda l: fill_geometry(l))
    fir_df = fir_df.loc[:, fir_df.columns != 'geometry_wkt'] 
    fir_df = fir_df.explode('h3_res_7').explode('h3_res_7')
    fir_df = fir_df[fir_df['h3_res_7'].apply(lambda l:type(l) == str)]
    try:
      fir_df.loc[:,"h3_res_7_lat"], fir_df.loc[:,"h3_res_7_lon"] = zip(*fir_df["h3_res_7"].apply(lambda l: get_coords(l)))
    except: 
      print(f'The following hexes did not result in coords: {fir_df.h3_res_7.to_list()}')
      fir_df.loc[:,"h3_res_7_lat"] = None
      fir_df.loc[:,"h3_res_7_lon"] = None
    return fir_df

def process_country_airspace(ctry_df, compact = False):
    ctry_df = ctry_df.rename({
        'admin':'name',
        'iso_a3':'code'
    }, axis = 1)
    
    # Avoid SettingWithCopyWarning by using .loc
    ctry_df = ctry_df.copy()
    
    ctry_df.loc[:,'min_fl'] = 0
    ctry_df.loc[:,'max_fl'] = 999
    ctry_df.loc[:,'airac_cfmu'] = -1
    ctry_df.loc[:,'validity_start'] = "1900-1-1"
    ctry_df.loc[:,'validity_end'] = "2100-1-1"
    
    
    ctry_df.loc[:,'airspace_type'] = 'COUNTRY'
    
    ctry_df = ctry_df[[
        'airspace_type',
        'name',
        'code',
        'airac_cfmu',
        'validity_start',
        'validity_end',
        'min_fl',
        'max_fl',
        'geometry_wkt'
    ]]
    
        
    ctry_df.loc[:,'min_fl'] = ctry_df['min_fl'].apply(int)
    ctry_df.loc[:,'max_fl'] = ctry_df['max_fl'].apply(int)
    
    if compact:
        ctry_df.loc[:, 'h3_res_7'] = ctry_df.geometry_wkt.apply(lambda l: fill_geometry_compact(l))
    else:
        ctry_df.loc[:, 'h3_res_7'] = ctry_df.geometry_wkt.apply(lambda l: fill_geometry(l))
    ctry_df = ctry_df.loc[:, ctry_df.columns != 'geometry_wkt'] 
    ctry_df = ctry_df.explode('h3_res_7').explode('h3_res_7')
    ctry_df = ctry_df[ctry_df['h3_res_7'].apply(lambda l:type(l) == str)]
    try:
      ctry_df.loc[:,"h3_res_7_lat"], ctry_df.loc[:,"h3_res_7_lon"] = zip(*ctry_df["h3_res_7"].apply(lambda l: get_coords(l)))
    except: 
      print(f'The following hexes did not result in coords: {ctry_df.h3_res_7.to_list()}')
      ctry_df["h3_res_7_lat"] = None
      ctry_df["h3_res_7_lon"] = None
    return ctry_df

def process_and_insert_ansp_airspaces(ansp_df, spark):
    for i in range(len(ansp_df)):
        single_row_df = ansp_df.iloc[[i]]  # Keep it as a DataFrame
        print("Processing row with name:", single_row_df.name.values[0])
        result = process_ansp_airspaces(single_row_df, compact = False)
  
        schema = StructType([
            StructField("airspace_type", StringType(), True),
            StructField("name", StringType(), True),
            StructField("code", StringType(), True),
            StructField("airac_cfmu", StringType(), True),
            StructField("validity_start", StringType(), True),
            StructField("validity_end", StringType(), True),
            StructField("min_fl", IntegerType(), True),
            StructField("max_fl", IntegerType(), True),
            StructField("h3_res_7", StringType(), True),
            StructField("h3_res_7_lat", DoubleType(), True),
            StructField("h3_res_7_lon", DoubleType(), True)])

        
        spark_df = spark.createDataFrame(result.to_dict(orient='records'), schema)
        spark_df = spark_df.select(
            col("airspace_type"),
            col("name"),
            col("code"),
            col("airac_cfmu"),
            col("validity_start"),
            col("validity_end"),
            col('min_fl'),
            col('max_fl'),
            col('h3_res_7'),
            col('h3_res_7_lat'),
            col('h3_res_7_lon')
        )
        
        spark_df = spark_df.withColumn("validity_start", col("validity_start").cast(TimestampType()))
        spark_df = spark_df.withColumn("validity_end", col("validity_end").cast(TimestampType()))

        spark_df.writeTo(f"`{project}`.`opdi_h3_airspace_ref`").append()

def process_and_insert_fir_airspaces(fir_df, spark):
    for i in range(len(fir_df)):
        single_row_df = fir_df.iloc[[i]]  # Keep it as a DataFrame
        print("Processing row with name:", single_row_df.name.values[0])
        result = process_fir_airspaces(single_row_df, compact = False)
        
        schema = StructType([
            StructField("airspace_type", StringType(), True),
            StructField("name", StringType(), True),
            StructField("code", StringType(), True),
            StructField("airac_cfmu", StringType(), True),
            StructField("validity_start", StringType(), True),
            StructField("validity_end", StringType(), True),
            StructField("min_fl", IntegerType(), True),
            StructField("max_fl", IntegerType(), True),
            StructField("h3_res_7", StringType(), True),
            StructField("h3_res_7_lat", DoubleType(), True),
            StructField("h3_res_7_lon", DoubleType(), True)])

        
        spark_df = spark.createDataFrame(result.to_dict(orient='records'), schema)
        spark_df = spark_df.select(
            col("airspace_type"),
            col("name"),
            col("code"),
            col("airac_cfmu"),
            col("validity_start"),
            col("validity_end"),
            col('min_fl'),
            col('max_fl'),
            col('h3_res_7'),
            col('h3_res_7_lat'),
            col('h3_res_7_lon')
        )
        
        spark_df = spark_df.withColumn("validity_start", col("validity_start").cast(TimestampType()))
        spark_df = spark_df.withColumn("validity_end", col("validity_end").cast(TimestampType()))

        spark_df.writeTo(f"`{project}`.`opdi_h3_airspace_ref`").append()

def process_and_insert_country_airspaces(ctry_df, spark):
    for i in range(len(ctry_df)):
        single_row_df = ctry_df.iloc[[i]]  # Keep it as a DataFrame
        print("Processing row with name:", single_row_df.admin.values[0])
        result = process_country_airspace(single_row_df, compact = False)
        
        schema = StructType([
            StructField("airspace_type", StringType(), True),
            StructField("name", StringType(), True),
            StructField("code", StringType(), True),
            StructField("airac_cfmu", StringType(), True),
            StructField("validity_start", StringType(), True),
            StructField("validity_end", StringType(), True),
            StructField("min_fl", IntegerType(), True),
            StructField("max_fl", IntegerType(), True),
            StructField("h3_res_7", StringType(), True),
            StructField("h3_res_7_lat", DoubleType(), True),
            StructField("h3_res_7_lon", DoubleType(), True)])

        
        spark_df = spark.createDataFrame(result.to_dict(orient='records'), schema)
        spark_df = spark_df.select(
            col("airspace_type"),
            col("name"),
            col("code"),
            col("airac_cfmu"),
            col("validity_start"),
            col("validity_end"),
            col('min_fl'),
            col('max_fl'),
            col('h3_res_7'),
            col('h3_res_7_lat'),
            col('h3_res_7_lon')
        )
        
        spark_df = spark_df.withColumn("validity_start", col("validity_start").cast(TimestampType()))
        spark_df = spark_df.withColumn("validity_end", col("validity_end").cast(TimestampType()))

        
        spark_df.writeTo(f"`{project}`.`opdi_h3_airspace_ref`").append()


# Processing part
compact_output = False
#for filepath in ansp:
#    print(f"Processing: {filepath}")
#    ansp_df = pd.read_parquet(filepath)
#    process_and_insert_ansp_airspaces(ansp_df, spark)
#
#    if compact_output:
#        # Compacted data goes to a file for inspection
#        dest = "~/data/airspace_data/ansp/"
#        output_path = f"{dest}/{filepath.split('/')[-1].split('.')[0]}_h3_compact.parquet"
#        df = process_ansp_airspaces(ansp_df, compact = True)
#        df.to_parquet(output_path)

for filepath in firs:
    print(f"Processing: {filepath}")
    fir_df = pd.read_parquet(filepath)
    #process_and_insert_fir_airspaces(fir_df, spark)

    if compact_output:
        # Compacted data goes to a file for inspection
        dest = "~/data/airspace_data/fir/"
        output_path = f"{dest}/{filepath.split('/')[-1].split('.')[0]}_h3_compact.parquet"
        df = process_fir_airspaces(fir_df, compact =True)
        df.to_parquet(output_path)

#for filepath in countries:
#    print(f"Processing: {filepath}")
#    ctry_df = pd.read_parquet(filepath)
#    process_and_insert_country_airspaces(ctry_df, spark)

#    if compact_output:
#        # Compacted data goes to a file for inspection
#        dest = "~/data/airspace_data/countries/"
#        output_path = f"{dest}/{filepath.split('/')[-1].split('.')[0]}_h3_compact.parquet"
#        df = process_country_airspace(ctry_df, compact = True)
#        df.to_parquet(output_path)