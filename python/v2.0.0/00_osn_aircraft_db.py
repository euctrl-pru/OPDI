from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, regexp_replace
import pandas as pd

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
    .config("spark.driver.cores", "1") \
    .config("spark.driver.memory", "2G") \
    .config("spark.executor.memory", "4G") \
    .config("spark.executor.memoryOverhead", "1G") \
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

# Step 1: Download the CSV file, load the dataset using pandas & convert all to str
url = "https://s3.opensky-network.org/data-samples/metadata/aircraft-database-complete-2024-10.csv"
pandas_df = pd.read_csv(url, quotechar='\'', on_bad_lines='error', low_memory=False)

def conv_str(x):
    if pd.isnull(x):
        return None
    return str(x)
pandas_df = pandas_df[['icao24', 'registration', 'model', 'typecode', 'icaoAircraftClass', 'operatorIcao']].map(conv_str)

# Step 2: Convert the pandas DataFrame to a Spark DataFrame
spark_df = spark.createDataFrame(pandas_df.to_dict(orient='records'))

# Step 3: Select and rename columns
selected_df = spark_df.select(
    col("icao24"),
    col("registration"),
    col("model"),
    col("typecode"),
    col("icaoAircraftClass").alias("icao_aircraft_class"),
    col("operatorIcao").alias("icao_operator")
)

# Step 4: Some cleanup
cleaned_df = selected_df.select([
    trim(when(col(c) == "", None).otherwise(col(c))).alias(c) for c in selected_df.columns
])

# Step 5: Dump to Iceberg table
cleaned_df.writeTo("project_opdi.osn_aircraft_db").append()

print("Data has been successfully processed and inserted into the Iceberg table.")