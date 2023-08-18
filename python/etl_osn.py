from pyspark.sql import SparkSession
import os
import subprocess

def execute_shell_command(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    return stdout.decode().strip(), stderr.decode().strip()

# Setting up mc commands
execute_shell_command('curl -O https://dl.min.io/client/mc/release/linux-amd64/mc')
execute_shell_command('chmod +x mc')
execute_shell_command('./mc alias set opensky https://s3.opensky-network.org $OSN_USERNAME $OSN_KEY')

# Execute mc find command to list files
stdout, _ = execute_shell_command('./mc find opensky/ec-datadump/ --path "*/states_*.parquet"')
files_to_download = stdout.split('\n')

# Load the processed files list
processed_files_path = 'processed_files.log'
if os.path.exists(processed_files_path):
    with open(processed_files_path, 'r') as f:
        processed_files = f.read().splitlines()
else:
    processed_files = []

# Spark Session Initialization
spark = SparkSession.builder \
    .appName("project_aiu") \
    .config("spark.hadoop.fs.azure.ext.cab.required.group", "eur-app-aiu-dev") \
    .config("spark.yarn.access.hadoopFileSystems", "abfs://storage-fs@cdpdldev0.dfs.core.windows.net/data/project/aiu.db/unmanaged") \
    .config("spark.driver.cores", "1") \
    .config("spark.driver.memory", "8G") \
    .config("spark.executor.memory", "5G") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "6") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "400s") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.enforce.bucketing", "true") \
    .getOrCreate()

spark.sql("SET spark.sql.hive.convertMetastoreParquet=false")
spark.sql("SET spark.sql.hive.metastorePartitionPruning=true")

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

for file in files_to_download:
    file_name = file.split("/")[-1]
    if file_name not in processed_files:
        local_file_path = f'data/ec-datadump/{file_name}'
        cp_command = f'./mc cp "{file}" {local_file_path}'
        out, err = execute_shell_command(cp_command)

        if err:
            print(f"Error for {cp_command}: {err}")

        # Process the file using Spark immediately after download
        df = spark.read.parquet(local_file_path)
        for camel_case, snake_case in column_name_mapping.items():
            df = df.withColumnRenamed(camel_case, snake_case)
        df.write.mode("append").insertInto("project_aiu.osn_ec_datadump")

        # Delete the local copy to save space
        os.remove(local_file_path)

        # Update the log of processed files after successful load to Hive
        with open(processed_files_path, 'a') as f:
            f.write(file_name + '\n')

# Stop the SparkSession
spark.stop()