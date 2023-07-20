import pyspark
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import os

spark = SparkSession\
    .builder\
    .appName("project_aiu")\
    .config("spark.hadoop.fs.azure.ext.cab.required.group","eur-app-aiu-dev")\
    .config("spark.yarn.access.hadoopFileSystems","abfs://storage-fs@cdpdldev0.dfs.core.windows.net/data/project/aiu.db/unmanaged")\
    .getOrCreate()

# Truncate the Hive table
spark.sql("TRUNCATE TABLE project_aiu.osn_ec_datadump;")

directory_path = "data/ec-datadump/"

for file_name in os.listdir(directory_path):
    if file_name.endswith('.parquet'):
        file_path = os.path.join(directory_path, file_name)
        
        # read each parquet file in the directory
        df = pd.read_parquet(file_path)

        # Convert the NumPy array to a list in the 'serials' column
        df['serials'] = df['serials'].apply(lambda arr: arr.tolist() if isinstance(arr, np.ndarray) else arr)

        # Convert the pandas DataFrame to a Spark DataFrame
        spark_df = spark.createDataFrame(df)

        # Write the Spark DataFrame to the Hive table
        spark_df.write.mode("ignore").insertInto("project_aiu.osn_ec_datadump")

# Stop the SparkSession
spark.stop()
