from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, from_unixtime
from datetime import datetime, date, timedelta
import subprocess
import os, shutil, time
import os.path
import dateutil.relativedelta
import calendar
import pandas as pd


# Settings
## Config
project = "project_opdi"
start_month = date(2022, 1, 1)
import_data = True


## Which months to process
today = date.today()
end_month = today - dateutil.relativedelta.relativedelta(months=1) # We work on the d-1 months

# Getting today's date formatted
today = today.strftime('%d %B %Y')

# Spark Session Initialization
#shutil.copy("/runtime-addons/cmladdon-2.0.40-b150/log4j.properties", "/etc/spark/conf/") # Setting logging properties
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

# Helper function
def execute_shell_command(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    return stdout.decode().strip(), stderr.decode().strip()

def table_exists(spark, database_name, table_name):
    """
    Check if a table exists in a given database.

    Parameters:
    - spark: SparkSession object
    - database_name: Name of the database
    - table_name: Name of the table to check

    Returns:
    - True if table exists, False otherwise
    """
    tables = [t.name for t in spark.catalog.listTables(database_name)]
    return table_name in tables
  
# Setting up MinIO client (MC)

def setup_mc():
  # Note: You should have a OSN_USERNAME and OSN_KEY environment variable set up to connect to the MinIO instance.
  execute_shell_command('curl -O https://dl.min.io/client/mc/release/linux-amd64/mc')
  execute_shell_command('chmod +x mc')
  execute_shell_command('./mc alias set opensky https://s3.opensky-network.org $OSN_USERNAME $OSN_KEY')

def list_mc_files():
  # Execute mc find command to list files
  stdout, _ = execute_shell_command('./mc find opensky/ec-datadump/ --path "*/states_*.parquet"')
  files_to_download = stdout.split('\n')
  files_to_download = [file for file in files_to_download if '2022-' in file or '2023-' in file or '2024-' in file or '2025-' in file]
  return(files_to_download)

# Create the OSN EC data table to dump in the data
create_db_sql = f"""
CREATE TABLE IF NOT EXISTS `{project}`.`osn_statevectors` (
  event_time BIGINT COMMENT 'This column contains the unix (aka POSIX or epoch) timestamp for which the state vector was valid.',
  icao24 STRING COMMENT 'This column contains the 24-bit ICAO transponder ID which can be used to track specific airframes over different flights.',
  lat DOUBLE COMMENT 'This column contains the last known latitude of the aircraft.',
  lon DOUBLE COMMENT 'This column contains the last known longitude of the aircraft.',
  velocity DOUBLE COMMENT 'This column contains the speed over ground of the aircraft in meters per second.',
  heading DOUBLE COMMENT 'This column represents the direction of movement (track angle in degrees) as the clockwise angle from the geographic north.',
  vert_rate DOUBLE COMMENT 'This column contains the vertical speed of the aircraft in meters per second.',
  callsign STRING COMMENT 'This column contains the callsign that was broadcast by the aircraft.',
  on_ground BOOLEAN COMMENT 'This flag indicates whether the aircraft is broadcasting surface positions (true) or airborne positions (false).',
  alert BOOLEAN COMMENT 'This flag is a special indicator used in ATC.',
  spi BOOLEAN COMMENT 'This flag is a special indicator used in ATC.',
  squawk STRING COMMENT 'This 4-digit octal number is another transponder code which is used by ATC and pilots for identification purposes and indication of emergencies.',
  baro_altitude DOUBLE COMMENT 'This column indicates the aircrafts altitude. As the names suggest, baroaltitude is the altitude measured by the barometer (in meter).',
  geo_altitude DOUBLE COMMENT 'This column indicates the aircrafts altitude. As the names suggest, geoaltitude is determined using the GNSS (GPS) sensor (in meter).',
  last_pos_update DOUBLE COMMENT 'This unix timestamp indicates the age of the position.',
  last_contact DOUBLE COMMENT 'This unix timestamp indicates the time at which OpenSky received the last signal of the aircraft.',
  serials ARRAY<INT> COMMENT 'The serials column is a list of serials of the ADS-B receivers which received the message.'
)
COMMENT 'OpenSky Network EUROCONTROL datadump (for PRU). Last updated: {today}.'
STORED AS parquet
TBLPROPERTIES ('transactional'='false');
"""

#spark.sql(f"""DROP TABLE IF EXISTS `{project}`.`osn_statevectors`;""") 
#spark.sql(create_db_sql)

if import_data:
  # File listing
  setup_mc()
  files_to_download = list_mc_files()

  # Initialize path variables
  local_folder_path = 'data/ec-datadump'
  processed_files_path = 'logs/01_osn_statevectors_etl.log'

  # Read the list of processed files if available
  if os.path.exists(processed_files_path):
      with open(processed_files_path, 'r') as f:
          processed_files = set(f.read().splitlines())
  else:
      processed_files = set()

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

  # Download files in chunks of 500 - Each file is about 10 MB max -> 5GB 
  print("Starting ETL..")

  def remove_files_in_directory(directory_path, extension):
      """
      Remove files in the specified directory that have the given extension.

      Parameters:
          directory_path (str): The path to the directory.
          extension (str): The file extension to look for.

      Returns:
          None
      """
      # List all files in the directory
      try:
          files = os.listdir(directory_path)
      except FileNotFoundError:
          print(f"The directory {directory_path} does not exist.")
          return

      # Loop through each file
      for filename in files:
          # Check if the file has the specified extension
          if filename.endswith(extension):
              # Create the full file path
              file_path = os.path.join(directory_path, filename)

              # Remove the file
              os.remove(file_path)

              # Print the name of the file that was removed
              print(f"Removed: {file_path}")

  # Directory path
  directory_path = "data/ec-datadump/"

  # File extension to look for
  extension_part = ".parquet.part.minio"

  # Loop
  chunksize = 250

  for i in range(0, len(files_to_download), chunksize):
      print(f"Processing chunk {i/chunksize} out of {round(len(files_to_download)/chunksize)}")
      downloaded_files = []
      for file in files_to_download[i:i+chunksize]:
          file_name = file.split("/")[-1]
          if file_name not in processed_files:
              local_file_path = os.path.join(local_folder_path, file_name)
              cp_command = f'./mc cp "{file}" {local_file_path}'
              out, err = execute_shell_command(cp_command)

              if err:
                  print(f"Error for {cp_command}: {err}")
              else:
                  downloaded_files.append(file_name)

      # Prevent partial files to halt upload.. 
      time.sleep(1)
      remove_files_in_directory(directory_path, extension_part) # Delete partially downloaded files -> Will print output

      # Perform a bulk read using Spark
      if downloaded_files:
          df = spark.read.option("mergeSchema", "true").parquet(local_folder_path)
          
          # Renaming stuff
          for camel_case, snake_case in column_name_mapping.items():
              df = df.withColumnRenamed(camel_case, snake_case)
          
          df = df.withColumnRenamed('time', 'event_time')
          
          # Cast 'event_time' from int (Unix timestamp) to timestamp
          df = df.withColumn("event_time", from_unixtime(col("event_time")).cast("timestamp"))
          
          # Add event_time_day column derived from event_time        
          df_with_partition = df.withColumn("event_time_day", to_date(col("event_time")))
          df_partitioned = df_with_partition.repartition("event_time_day").orderBy("event_time_day")

          # Drop event_time_day before writing
          df_cleaned = df_partitioned.drop("event_time_day")

          # Write the data for the month
          df_cleaned.writeTo(f"`{project}`.`osn_statevectors_v2`").append()

          # Delete the local copies to save space
          for file_name in downloaded_files:
              local_file_path = os.path.join(local_folder_path, file_name)
              os.remove(local_file_path)

          # Update the log of processed files
          with open(processed_files_path, 'a') as f:
              for file_name in downloaded_files:
                  f.write(file_name + '\n')
                  processed_files.add(file_name)


# Stop the SparkSession
spark.stop()