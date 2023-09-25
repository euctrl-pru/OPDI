from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# SparkSession creation logic (your existing code)
spark = SparkSession\
    .builder\
    .appName("project_aiu")\
    .config("spark.hadoop.fs.azure.ext.cab.required.group","eur-app-aiu-dev")\
    .config("spark.yarn.access.hadoopFileSystems","abfs://storage-fs@cdpdldev0.dfs.core.windows.net/")\
    .config("spark.driver.cores","1")\
    .config("spark.driver.memory","8G")\
    .config("spark.executor.memory","10G")\
    .config("spark.executor.cores","1")\
    .config("spark.executor.instances","2")\
    .config("spark.dynamicAllocation.maxExecutors", "5")\
    .enableHiveSupport() \
    .getOrCreate()

# Read data
df = spark.sql(f"SELECT * FROM `project_aiu`.`osn_ec_datadump_bucketed_full`;")

# Save the original column names for later and add track_id
original_columns = df.columns + ['track_id']

# Convert 'event_time' to datetime
df = df.withColumn("event_time_ts", F.to_timestamp("event_time"))

# Extract the year and month from event_time_ts to create a 'month_id'
df = df.withColumn("year", F.year("event_time_ts"))
df = df.withColumn("month", F.month("event_time_ts"))
df = df.withColumn("month_id", F.concat_ws("-", F.col("year"), F.col("month")))

# List unique month_ids
unique_month_ids = [row['month_id'] for row in df.select("month_id").distinct().collect()]

# Padding single-digit months with zeros
unique_month_ids = [date if len(date.split('-')[1]) == 2 else f"{date.split('-')[0]}-0{date.split('-')[1]}" for date in unique_month_ids]

unique_month_ids = sorted(unique_month_ids)

print(f"These are the unique month_ids to be processed: {unique_month_ids}")
print("")

for month_id in unique_month_ids:
    print("="*30)
    print(f"Working on month_id: {month_id}")
    print("="*30)
    print("")
    # Filter data for the current month_id
    df_month = df.filter(F.col("month_id") == month_id)
    
    # Rest of the original logic
    df_month = df_month.withColumn("group_id", F.sha2(F.concat_ws("", "icao24", "callsign"), 256))
    windowSpec = Window.partitionBy("group_id").orderBy("event_time_ts")
    df_month = df_month.withColumn("prev_event_time_ts", F.lag(df_month["event_time_ts"]).over(windowSpec))
    df_month = df_month.withColumn("time_gap_minutes", 
                                   (F.unix_timestamp("event_time_ts") - F.unix_timestamp("prev_event_time_ts"))/60)
    df_month = df_month.withColumn("new_group_flag", F.when(F.col("time_gap_minutes") > 30, 1).otherwise(0))
    groupWindowSpec = Window.partitionBy("group_id").orderBy("event_time_ts").rowsBetween(Window.unboundedPreceding, 0)
    df_month = df_month.withColumn("offset", F.sum("new_group_flag").over(groupWindowSpec))
    df_month = df_month.withColumn("track_id", F.concat(
      F.col("group_id"), 
      F.lit("_"), 
      F.col("offset"), 
      lit("_"), 
      year("event_time").cast("string"), 
      lit("_"),
      month("event_time").cast("string")
    )
                                  )

    # Drop unnecessary and additional columns to retain only the original ones
    df_month = df_month.select(original_columns)
    # Write data for the month to the database
    df_month.write.mode("append").insertInto(f"`project_aiu`.`osn_tracks`")
