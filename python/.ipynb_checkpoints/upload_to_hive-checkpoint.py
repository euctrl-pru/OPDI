from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("project_aiu")\
    .config("spark.hadoop.fs.azure.ext.cab.required.group","eur-app-aiu-dev")\
    .config("spark.yarn.access.hadoopFileSystems","abfs://storage-fs@cdpdldev0.dfs.core.windows.net/data/project/aiu.db/unmanaged")\
    .config("spark.driver.cores","1")\
    .config("spark.driver.memory","8G")\
    .config("spark.executor.memory","5G")\
    .config("spark.executor.cores","1")\
    .config("spark.executor.instances","2")\
    .config("spark.dynamicAllocation.maxExecutors", "6")\
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "400s") \
    .config("spark.sql.catalogImplementation","hive")\
    .getOrCreate()

spark.sql("SET spark.sql.hive.convertMetastoreParquet=false")
spark.sql("SET spark.sql.hive.metastorePartitionPruning=true")

directory_path = "data/ec-datadump/"

# read all parquet files in the directory
df = spark.read.parquet(directory_path)

column_name_mapping = {
    "eventTime": "event_time",
    "icao24": "icao24",
    "lat": "lat",
    "lon": "lon",
    "velocity": "velocity",
    "heading": "heading",
    "vertRate": "vert_rate",
    "callsign": "callsign",
    "onGround": "on_ground",
    "alert": "alert",
    "spi": "spi",
    "squawk": "squawk",
    "baroAltitude": "baro_altitude",
    "geoAltitude": "geo_altitude",
    "lastPosUpdate": "last_pos_update",
    "lastContact": "last_contact",
    "serials": "serials"
}

# Apply the renaming:
for camel_case, snake_case in column_name_mapping.items():
    df = df.withColumnRenamed(camel_case, snake_case)
    
# Write the DataFrame to the Hive table
df.write\
  .bucketBy(256, "icao24", "callsign", "squawk", "event_time")\
  .sortBy("icao24", "callsign", "squawk", "event_time")\
  .mode("ignore")\
  .saveAsTable("project_aiu.osn_ec_datadump")

# Stop the SparkSession
spark.stop()