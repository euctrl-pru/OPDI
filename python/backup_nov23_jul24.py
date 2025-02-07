from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from datetime import datetime, timedelta
import os

# Initialize a Spark session with Hive support
spark = SparkSession.builder \
    .appName("OPDI Extraction") \
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
    .config("spark.driver.memory", "8G") \
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

# Define start and end times in UNIX timestamp format
excl_month_start = 1698796800  # Replace with actual start time
excl_month_end = 1719791999    # Replace with actual end time

start_time = 1640995200
end_time = 1732579200

# Convert UNIX timestamps to datetime for easier manipulation
start_date = datetime.utcfromtimestamp(start_time)
end_date = datetime.utcfromtimestamp(end_time)

# Log file path
log_file = "processed_months.log"

# Function to read processed months from log file
def get_processed_months(log_file):
    if os.path.exists(log_file):
        with open(log_file, "r") as file:
            return set(line.strip() for line in file.readlines())
    return set()

# Function to log a processed month
def log_processed_month(log_file, month):
    with open(log_file, "a") as file:
        file.write(f"{month}\n")

# Get already processed months
processed_months = get_processed_months(log_file)

# Process and dump month by month
current_date = start_date
while current_date <= end_date:
    # Calculate start and end of the current month
    month_start = int(current_date.timestamp())
    next_month = current_date + timedelta(days=32)
    next_month = next_month.replace(day=1)
    month_end = int(next_month.timestamp()) - 1  # End of the current month

    # Format the current month for logging
    month_str = current_date.strftime("%Y-%m")

    # Skip processing if the month is already logged
    if month_str in processed_months:
        print(f"Skipping already processed month: {month_str}")
    else:
        # Query for the specific month
        query = f"""
        SELECT 
            CAST(FROM_UNIXTIME(event_time) AS TIMESTAMP) AS event_time,
            icao24,
            lat,
            lon,
            velocity,
            heading,
            vert_rate,
            callsign,
            on_ground,
            alert,
            spi,
            squawk,
            baro_altitude,
            geo_altitude,
            last_pos_update,
            last_contact,
            serials
        FROM project_opdi.osn_statevectors 
        WHERE event_time BETWEEN {month_start} AND {month_end} 
          AND event_time IS NOT NULL
          AND event_time NOT BETWEEN {excl_month_start} AND {excl_month_end};
        """
        df = spark.sql(query)

        # Add event_time_day column derived from event_time        
        df_with_partition = df.withColumn("event_time_day", to_date(col("event_time").cast("timestamp")))
        df_partitioned = df_with_partition.repartition("event_time_day").orderBy("event_time_day")

        # Drop event_time_day before writing
        df_cleaned = df_partitioned.drop("event_time_day")

        # Write the data for the month
        df_cleaned.writeTo("project_opdi.osn_statevectors_iceberg").append()

        # Log the processed month
        log_processed_month(log_file, month_str)
        print(f"Processed and logged data for {month_str}")

    # Move to the next month
    current_date = next_month

# Stop the Spark session
spark.stop()
