from pyspark.sql import SparkSession
import os, time
import subprocess
import os,shutil
from datetime import datetime

# Settings
project = "project_aiu"

# Getting today's date
today = datetime.today().strftime('%d %B %Y')







import xml.etree.ElementTree as ET
import datetime
import os
import requests
import json
from requests_kerberos import HTTPKerberosAuth
import time

# !grep  -A1 fs.azure.ext.cab.address /etc/hadoop/conf/core-site.xml  | tail -1

def get_token():
    id_broker_group="eur-app-aiu-dev"
    id_broker='https://dl-dev-idbroker0.az-dev.x9er-zkvz.cloudera.site:8444/gateway/'
    r = requests.get(f"{id_broker}dt/knoxtoken/api/v1/token", auth=HTTPKerberosAuth())
    token=r.json()['access_token']

    url = f"{id_broker}azure-cab/cab/api/v1/credentials/group/{id_broker_group}"
    headers = {
            'Authorization': "Bearer "+ token,
            'cache-control': "no-cache",
            'Accept': 'application/json' 
    }
    token = requests.request("GET", url, headers=headers).json()['access_token']
    return token

def get_adls_gen2_folder_contents(storage_account, container, folder, access_token):
    """
    List the contents of a folder in an Azure Data Lake Storage Gen2 filesystem.
    
    Args:
        storage_account (str): The storage account name.
        container (str): The container name within the ADLS Gen2 account.
        folder (str): The folder path to list contents of.
        access_token (str): The OAuth2 access token for authentication.
    
    Returns:
        response.content: The HTTP response content from the Azure Storage REST API.
    """
    date = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
    url = f'https://{storage_account}.dfs.core.windows.net/{container}?resource=filesystem&recursive=false&directory={folder}'
    headers = {
        'Authorization': "Bearer " + access_token,
        'x-ms-version': '2019-12-12',
        'x-ms-date': date,
    }
    response = requests.get(url, headers=headers)
    return response.json()

# Example usage
storage_account = "cdpdldev0"  # Adjusted to the account mentioned in the HDFS command
container = "storage-fs"  # Kept as is, per your initial input
folder = 'data/project/aiu.other'  # Adjusted folder path based on the HDFS command output
access_token = get_token()  # Replace YOUR_ACCESS_TOKEN with your actual access token

contents = get_adls_gen2_folder_contents(storage_account, container, folder, access_token)
print(contents)













# # Spark Session Initialization
# # shutil.copy("/runtime-addons/cmladdon-2.0.40-b150/log4j.properties", "/etc/spark/conf/") # Setting logging properties
# #spark = SparkSession.builder \
# #    .appName("OSN statevectors ETL") \
# #    .config("spark.log.level", "ERROR")\
# #    .config("spark.hadoop.fs.azure.ext.cab.required.group", "eur-app-aiu-dev") \
# #    .config("spark.kerberos.access.hadoopFileSystems", "abfs://storage-fs@cdpdldev0.dfs.core.windows.net/data/project/aiu.db/unmanaged") \
# #    .config("spark.driver.cores", "1") \
# #    .config("spark.driver.memory", "8G") \
# #    .config("spark.executor.memory", "5G") \
# #    .config("spark.executor.cores", "1") \
# #    .config("spark.executor.instances", "2") \
# #    .config("spark.dynamicAllocation.maxExecutors", "6") \
# #    .config("spark.network.timeout", "800s") \
# #    .config("spark.executor.heartbeatInterval", "400s") \
# #    .enableHiveSupport() \
# #    .getOrCreate()

# spark = SparkSession\
#     .builder\
#     .getOrCreate()

# # Helper function
# def execute_shell_command(command):
#     process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#     stdout, stderr = process.communicate()
#     return stdout.decode().strip(), stderr.decode().strip()

# print(f"hadoop: {execute_shell_command('which hadoop')}")
# print(f"hdfs: {execute_shell_command('which hdfs')}")

# def table_exists(spark, database_name, table_name):
#     """
#     Check if a table exists in a given database.

#     Parameters:
#     - spark: SparkSession object
#     - database_name: Name of the database
#     - table_name: Name of the table to check

#     Returns:
#     - True if table exists, False otherwise
#     """
#     tables = [t.name for t in spark.catalog.listTables(database_name)]
#     return table_name in tables
  
# # Setting up MinIO client (MC)
# # Note: You should have a OSN_USERNAME and OSN_KEY environment variable set up to connect to the MinIO instance.
# print('Starting minio client')
# execute_shell_command('curl -O https://dl.min.io/client/mc/release/linux-amd64/mc')
# execute_shell_command('chmod +x mc')
# execute_shell_command('./mc alias set opensky https://s3.opensky-network.org k4tAd3Qr1h7x8uGn DLGuWfiVn3xyN5CHOC9TANredkW5ifSE')

# # Execute mc find command to list files
# stdout, _ = execute_shell_command('./mc find opensky/ec-datadump/ --path "*/states_*.parquet"')
# files_to_download = stdout.split('\n')

# files_to_download = [file for file in files_to_download if '/2024-' in file] #temp filter

# print(files_to_download)

# # Create the OSN EC data table to dump in the data
# create_db_sql = f"""
# CREATE TABLE IF NOT EXISTS `{project}`.`osn_statevectors` (
#   event_time BIGINT COMMENT 'This column contains the unix (aka POSIX or epoch) timestamp for which the state vector was valid.',
#   icao24 STRING COMMENT 'This column contains the 24-bit ICAO transponder ID which can be used to track specific airframes over different flights.',
#   lat DOUBLE COMMENT 'This column contains the last known latitude of the aircraft.',
#   lon DOUBLE COMMENT 'This column contains the last known longitude of the aircraft.',
#   velocity DOUBLE COMMENT 'This column contains the speed over ground of the aircraft in meters per second.',
#   heading DOUBLE COMMENT 'This column represents the direction of movement (track angle in degrees) as the clockwise angle from the geographic north.',
#   vert_rate DOUBLE COMMENT 'This column contains the vertical speed of the aircraft in meters per second.',
#   callsign STRING COMMENT 'This column contains the callsign that was broadcast by the aircraft.',
#   on_ground BOOLEAN COMMENT 'This flag indicates whether the aircraft is broadcasting surface positions (true) or airborne positions (false).',
#   alert BOOLEAN COMMENT 'This flag is a special indicator used in ATC.',
#   spi BOOLEAN COMMENT 'This flag is a special indicator used in ATC.',
#   squawk STRING COMMENT 'This 4-digit octal number is another transponder code which is used by ATC and pilots for identification purposes and indication of emergencies.',
#   baro_altitude DOUBLE COMMENT 'This column indicates the aircrafts altitude. As the names suggest, baroaltitude is the altitude measured by the barometer (in meter).',
#   geo_altitude DOUBLE COMMENT 'This column indicates the aircrafts altitude. As the names suggest, geoaltitude is determined using the GNSS (GPS) sensor (in meter).',
#   last_pos_update DOUBLE COMMENT 'This unix timestamp indicates the age of the position.',
#   last_contact DOUBLE COMMENT 'This unix timestamp indicates the time at which OpenSky received the last signal of the aircraft.',
#   serials ARRAY<INT> COMMENT 'The serials column is a list of serials of the ADS-B receivers which received the message.'
# )
# COMMENT 'OpenSky Network EUROCONTROL datadump (for PRU). Last updated: {today}.'
# STORED AS parquet
# TBLPROPERTIES ('transactional'='false');
# """

# spark.sql(f"""DROP TABLE IF EXISTS `{project}`.`osn_statevectors`;""") 
# spark.sql(create_db_sql)

# # # Initialize path variables
# local_folder_path = 'data/ec-datadump'
# processed_files_path = 'data/processed_files_test.log'
# remote_path = 'ec-datadump'
# # If the clustered table does not exist, we need to start from scratch so we delete the previously used log file...
# if not table_exists(spark, project, "osn_statevectors_clustered"):
#     # If the table does not exist, remove the log file
#     if os.path.exists(processed_files_path):
#         os.remove(processed_files_path)

# # Read the list of processed files if available
# if os.path.exists(processed_files_path):
#     with open(processed_files_path, 'r') as f:
#         processed_files = set(f.read().splitlines())
# else:
#     processed_files = set()

# column_name_mapping = {
#     "eventTime": "event_time",
#     "icao24": "icao24",
#     "lat": "lat",
#     "lon": "lon",
#     "velocity": "velocity",
#     "heading": "heading",
#     "vertRate": "vert_rate",
#     "callsign": "callsign",
#     "onGround": "on_ground",
#     "alert": "alert",
#     "spi": "spi",
#     "squawk": "squawk",
#     "baroAltitude": "baro_altitude",
#     "geoAltitude": "geo_altitude",
#     "lastPosUpdate": "last_pos_update",
#     "lastContact": "last_contact",
#     "serials": "serials"
# }

# # Download files in chunks of 500 - Each file is about 10 MB max -> 5GB 
# print("Starting ETL..")

# def remove_files_in_directory(directory_path, extension):
#     """
#     Remove files in the specified directory that have the given extension.
    
#     Parameters:
#         directory_path (str): The path to the directory.
#         extension (str): The file extension to look for.
        
#     Returns:
#         None
#     """
#     # List all files in the directory
#     try:
#         files = os.listdir(directory_path)
#     except FileNotFoundError:
#         print(f"The directory {directory_path} does not exist.")
#         return

#     # Loop through each file
#     for filename in files:
#         # Check if the file has the specified extension
#         if filename.endswith(extension):
#             # Create the full file path
#             file_path = os.path.join(directory_path, filename)
            
#             # Remove the file
#             os.remove(file_path)
            
#             # Print the name of the file that was removed
#             print(f"Removed: {file_path}")

# # Directory path
# directory_path = "data/ec-datadump/"

# # File extension to look for
# extension_part = ".parquet.part.minio"

# # Loop
# chunksize = 500

# import os

# # Make subfolder on hdfs 
# project_group = "eur-app-aiu-dev"
# subfolder_name = "processed_data"  # Change this to your desired subfolder name
# remote_base_path = "abfs://storage-fs@cdpdldev0.dfs.core.windows.net/data/project/aiu.other"
# remote_path = f"{remote_base_path}/{subfolder_name}"
# # Construct the HDFS command to create the subfolder
# hdfs_mkdir_command = f'hdfs dfs -Dfs.azure.ext.cab.required.group={project_group} -mkdir -p {remote_path}'

# # Execute the command
# stdout, stderr = execute_shell_command(hdfs_mkdir_command)
# if stderr:
#     print(f"Error creating subfolder in HDFS: {stderr}")
# else:
#     print(f"Subfolder created in HDFS: {remote_path}")

# def upload_files_to_hdfs(local_folder_path):
#     """
#     Upload all files in a local directory to a specified HDFS path.

#     Parameters:
#         local_folder_path (str): Path to the local directory containing the files.
#     """
#     project_group = "eur-app-aiu-dev"
#     subfolder_name = "ec-datadump"  # Change this to your desired subfolder name
#     remote_base_path = "abfs://storage-fs@cdpdldev0.dfs.core.windows.net/data/project/aiu.other"
#     remote_path = f"{remote_base_path}/{subfolder_name}"
    
#     # Get a list of files in the local directory
#     files = os.listdir(local_folder_path)

#     # Loop through each file and upload it to HDFS
#     for file_name in files:
#         local_file_path = os.path.join(local_folder_path, file_name)
#         # Construct the HDFS command to upload the file
#         hdfs_put_command = f'hdfs dfs -Dfs.azure.ext.cab.required.group={project_group} -put {local_file_path} {remote_path}'

#         # Execute the command
#         stdout, stderr = execute_shell_command(hdfs_put_command)
#         if stderr:
#             print(f"Error uploading file to HDFS: {stderr}")
#         else:
#             print(f"Uploaded {file_name} to HDFS: {remote_path}/{file_name}")

# for i in range(0, len(files_to_download), chunksize):
#     print(f"Processing chunk {i/chunksize} out of {round(len(files_to_download)/500)}")
#     downloaded_files = []
#     for file in files_to_download[i:i+chunksize]:
#         file_name = file.split("/")[-1]
#         if file_name not in processed_files:
#             local_file_path = os.path.join(local_folder_path, file_name)
#             cp_command = f'./mc cp "{file}" {local_file_path}'
#             out, err = execute_shell_command(cp_command)

#             if err:
#                 print(f"Error for {cp_command}: {err}")
#             else:
#                 downloaded_files.append(file_name)
    
#     # Upload files to HDFS 
#     upload_files_to_hdfs(local_folder_path)

#     # Prevent partial files to halt upload.. 
#     time.sleep(3)
#     remove_files_in_directory(directory_path, extension_part) # Delete partially downloaded files -> Will print output
    
    # # Perform a bulk read using Spark
    # if downloaded_files:
    #     df = spark.read.option("mergeSchema", "true").parquet(local_folder_path)
    #     for camel_case, snake_case in column_name_mapping.items():
    #         df = df.withColumnRenamed(camel_case, snake_case)
    #     df.write.mode("append").insertInto(f"`{project}`.`osn_statevectors`")

    #     # Delete the local copies to save space
    #     for file_name in downloaded_files:
    #         local_file_path = os.path.join(local_folder_path, file_name)
    #         os.remove(local_file_path)

    #     # Update the log of processed files
    #     with open(processed_files_path, 'a') as f:
    #         for file_name in downloaded_files:
    #             f.write(file_name + '\n')
    #             processed_files.add(file_name)

# # Create clustered version of osn_statevectorsdump.. 

# print("Starting clustering..")

# spark.sql(f"""
# CREATE TABLE IF NOT EXISTS `{project}`.`osn_statevectors_clustered` (
#   event_time BIGINT COMMENT 'This column contains the unix (aka POSIX or epoch) timestamp for which the state vector was valid.',
#   icao24 STRING COMMENT 'This column contains the 24-bit ICAO transponder ID which can be used to track specific airframes over different flights.',
#   lat DOUBLE COMMENT 'This column contains the last known latitude of the aircraft.',
#   lon DOUBLE COMMENT 'This column contains the last known longitude of the aircraft.',
#   velocity DOUBLE COMMENT 'This column contains the speed over ground of the aircraft in meters per second.',
#   heading DOUBLE COMMENT 'This column represents the direction of movement (track angle in degrees) as the clockwise angle from the geographic north.',
#   vert_rate DOUBLE COMMENT 'This column contains the vertical speed of the aircraft in meters per second.',
#   callsign STRING COMMENT 'This column contains the callsign that was broadcast by the aircraft.',
#   on_ground BOOLEAN COMMENT 'This flag indicates whether the aircraft is broadcasting surface positions (true) or airborne positions (false).',
#   alert BOOLEAN COMMENT 'This flag is a special indicator used in ATC.',
#   spi BOOLEAN COMMENT 'This flag is a special indicator used in ATC.',
#   squawk STRING COMMENT 'This 4-digit octal number is another transponder code which is used by ATC and pilots for identification purposes and indication of emergencies.',
#   baro_altitude DOUBLE COMMENT 'This column indicates the aircrafts altitude. As the names suggest, baroaltitude is the altitude measured by the barometer (in meter).',
#   geo_altitude DOUBLE COMMENT 'This column indicates the aircrafts altitude. As the names suggest, geoaltitude is determined using the GNSS (GPS) sensor (in meter).',
#   last_pos_update DOUBLE COMMENT 'This unix timestamp indicates the age of the position.',
#   last_contact DOUBLE COMMENT 'This unix timestamp indicates the time at which OpenSky received the last signal of the aircraft.',
#   serials ARRAY<INT> COMMENT 'The serials column is a list of serials of the ADS-B receivers which received the message.'
# )
# COMMENT 'OpenSky Network EUROCONTROL datadump (for PRU) clustered. Last updated: {today}.'
# CLUSTERED BY (icao24, callsign, event_time, geo_altitude) INTO 4096 BUCKETS
# STORED AS parquet
# TBLPROPERTIES ('transactional'='false');
# """)

# spark.sql(f"""
#   INSERT INTO TABLE `{project}`.`osn_statevectors_clustered` 
#   SELECT * FROM `{project}`.`osn_statevectors`;
# """)

# # Cleanup
# #spark.sql(f"""DROP TABLE IF EXISTS `{project}`.`osn_statevectors`;""")

# alter_sql = f"""
#     ALTER TABLE `{project}`.`osn_statevectors_clustered`
#     SET TBLPROPERTIES('comment' = 'OpenSky Network EUROCONTROL datadump (for PRU) clustered. Last updated: {today}');
#     """
# spark.sql(alter_sql)

# Stop the SparkSession
# spark.stop()
