# Regular imports
import pandas as pd 
import numpy as np
import time
import os
import shutil
from datetime import datetime
from openap.phase import FlightPhase
from IPython.display import display, HTML

# Spark imports
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType, lit, col, when, lag, lead, min, max, sum as Fsum, explode, array, monotonically_increasing_id, round
from pyspark.sql.types import DoubleType, StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window

# Hotfix
#!cp /runtime-addons/cmladdon-2.0.40-b150/log4j.properties /etc/spark/conf/

# Spark Session Initialization
#shutil.copy("/runtime-addons/cmladdon-2.0.40-b150/log4j.properties", "/etc/spark/conf/") # Setting logging properties
spark = SparkSession.builder \
    .appName("OPDI flight events and measurements ETL") \
    .config("spark.log.level", "ERROR")\
    .config("spark.ui.showConsoleProgress", "false")\
    .config("spark.hadoop.fs.azure.ext.cab.required.group", "eur-app-aiu") \
    .config("spark.kerberos.access.hadoopFileSystems", "abfs://storage-fs@cdpdllive0.dfs.core.windows.net/data/project/opdi.db/unmanaged") \
    .config("spark.driver.cores", "1") \
    .config("spark.driver.memory", "10G") \
    .config("spark.executor.memory", "10G") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "5") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "400s") \
    .enableHiveSupport() \
    .getOrCreate()

# Database prep

# Settings
project = "project_opdi"
recreate_flight_event_table = False
recreate_measurement_table = False

## Range for processing
start_date = datetime.strptime('2022-01-01', '%Y-%m-%d')
end_date = datetime.strptime('2023-08-31', '%Y-%m-%d')


# Getting today's date
today = datetime.today().strftime('%d %B %Y')

create_flight_events_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{project}`.`opdi_flight_events` (

        id STRING COMMENT 'Primary Key: Unique milestone identifier for each record.',
        flight_id STRING COMMENT 'Foreign Key: Identifier linking the flight event to the flight trajectory table.',

        milestone_type STRING COMMENT 'Type of the flight event (milestone) being recorded.',
        event_time BIGINT COMMENT 'Timestamp for the flight event.',
        longitude DOUBLE COMMENT 'Longitude coordinate of the flight event.',
        latitude DOUBLE COMMENT 'Latitude coordinate of the flight event.',
        altitude DOUBLE COMMENT 'Altitude at which the flight event occurred.',
        
        source STRING COMMENT 'Source of the trajectory data.',
        version STRING COMMENT 'Version of the flight event (milestone) determination algorithm.',
        info STRING COMMENT 'Additional information or description of the flight event.'
    )
    COMMENT '`{project}`.`opdi_flight_events` table containing various OSN trajectory flight events. Last updated: {today}.'
    STORED AS parquet
    TBLPROPERTIES ('transactional'='false');
"""

print(f' Query to create OPDI Flight Events table:\n {create_flight_events_table_sql}')
if recreate_flight_event_table:
    spark.sql(f"DROP TABLE IF EXISTS `{project}`.`opdi_flight_events`;")
    spark.sql(create_flight_events_table_sql)

create_measurement_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{project}`.`opdi_measurements` (
        
        id STRING COMMENT 'Primary Key: Unique measurement identifier for each record.',
        milestone_id STRING COMMENT 'Foreign Key: Identifier linking the measurement to the corresponding event in the flight events table.',
        
        type STRING COMMENT 'Type of measurement, e.g. flown distance or fuel consumption.',
        value DOUBLE COMMENT 'Value of the measurement.',
        version STRING COMMENT 'The version of the measurement calculation.'
    )
    COMMENT '`{project}`.opdi_measurements` table containing various measurements for OPDI flight events. Last updated: {today}.'
    STORED AS parquet
    TBLPROPERTIES ('transactional'='false');
"""

print(f' Query to create OSN Measurements table:\n {create_measurement_table_sql}')
if recreate_measurement_table: 
    spark.sql(f"DROP TABLE IF EXISTS `{project}`.`osn_measurements`;")
    spark.sql(create_measurement_table_sql)

def calculate_horizontal_segment_events(sdf_input):
    df = sdf_input

    # Extracting OpenAP phases
    ## Defining variables
    df = df.withColumn("altitude_ft", col("baro_altitude") * 3.28084)
    df = df.withColumn("roc_ft_min", col("vert_rate") * 196.850394)
    df = df.withColumn("speed_kt", col("velocity") * 1.94384)

    ## Define schema for UDF
    schema = StructType([
        StructField("track_id", StringType()),
        #StructField("icao24", StringType()),
        #StructField("callsign", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("event_time", IntegerType()),
        #StructField("baro_altitude", DoubleType()),
        #StructField("velocity", DoubleType()),
        #StructField("vert_rate", DoubleType()),
        StructField("cumulative_distance_nm", DoubleType()),
        StructField("cumulative_time_s", IntegerType()),
        StructField("altitude_ft", DoubleType()),
        StructField("roc_ft_min", DoubleType()),
        StructField("speed_kt", DoubleType()),
        StructField("flight_phase", StringType())
    ])

    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def get_flight_phases(pdf):
        try:
            fp = FlightPhase()
            fp.set_trajectory(pdf['event_time'], pdf['altitude_ft'], pdf['speed_kt'], pdf['roc_ft_min'])
            pdf['flight_phase'] = fp.phaselabel()
        except: 
            pdf['flight_phase'] = None
        
        return pdf

    ## Apply UDF to get flight phases
    df_with_phases = df.groupby("track_id").apply(get_flight_phases)

    ## Window specification for detecting phase changes and for TOC/TOD
    windowSpecPhase = Window.partitionBy("track_id").orderBy("event_time")
    windowSpecTOC = Window.partitionBy("track_id").orderBy("event_time").rowsBetween(Window.unboundedPreceding, 0)
    windowSpecTOD = Window.partitionBy("track_id").orderBy("event_time").rowsBetween(0, Window.unboundedFollowing)

    ## Detect phase changes
    df_with_phases = df_with_phases.withColumn("prev_phase", lag("flight_phase", 1, "None").over(windowSpecPhase))
    df_with_phases = df_with_phases.withColumn("next_phase", lead("flight_phase", 1, "None").over(windowSpecPhase))

    ## Identify TOC and TOD for 'CR' phases
    df_with_phases = df_with_phases.withColumn("first_cr_time", min(when(col("flight_phase") == "CR", col("event_time"))).over(windowSpecTOC))
    df_with_phases = df_with_phases.withColumn("last_cr_time", max(when(col("flight_phase") == "CR", col("event_time"))).over(windowSpecTOD))

    ## Create event types

    ### Define a window spec for a cumulative sum, partitioned by track_id
    windowSpecCumulative = Window.partitionBy("track_id").orderBy("event_time").rowsBetween(Window.unboundedPreceding, 0)

    ### Column to indicate the start of a level segment
    start_of_segment = (F.col("flight_phase").isin("CR", "LVL")) & (F.col("prev_phase") != F.col("flight_phase"))
    df_with_phases = df_with_phases.withColumn("start_of_segment", start_of_segment)

    ### Cumulative count of level segment starts
    df_with_phases = df_with_phases.withColumn("segment_count", F.sum(F.when(F.col("start_of_segment"), 1).otherwise(0)).over(windowSpecCumulative))

    ### Update milestone_types to include segment number
    df_with_phases = df_with_phases.withColumn(
        "milestone_types",
        F.when(F.col("event_time") == F.col("first_cr_time"), F.array(F.lit("level-start"), F.lit("top-of-climb")))
         .when(F.col("event_time") == F.col("last_cr_time"), F.array(F.lit('level-end'), F.lit("top-of-descent")))
         .when(start_of_segment, F.array(F.lit("level-start")))
         .when((F.col("flight_phase").isin("CR", "LVL")) & (F.col("next_phase") != F.col("flight_phase")), F.array(F.lit('level-end')))
         .when((F.col("prev_phase") == "GND") & (F.col("next_phase") == "CL"), F.array(F.lit("take-off")))
         .when((F.col("prev_phase") == "DE") & (F.col("flight_phase") == "GND"), F.array(F.lit("landing")))
         .otherwise(F.array())
    )

    ## Explode the event_types array to create rows for each event
    df_exploded = df_with_phases.select("*", explode(col("milestone_types")).alias("type"))

    ## Filter out rows with no relevant events
    df_exploded = df_exploded.filter("type IS NOT NULL")

    ## Reformat
    df_openap_events = df_exploded.select(
        col('track_id'),
        col('type'),
        col('event_time'),
        col('lon'),
        col('lat'),
        col('altitude_ft'),
        col('cumulative_distance_nm'),
        col('cumulative_time_s')
    )

    df_openap_events = df_openap_events.dropDuplicates(['track_id', 'type', 'event_time'])
    
    return df_openap_events

# Flight levels crossing extractions

# Your existing DataFrame loading code
def calculate_vertical_crossing_events(sdf_input):
    df = sdf_input

    ## Convert meters to flight level and create a new column
    df = df.withColumn("altitude_ft", col("baro_altitude") * 3.28084)
    df = df.withColumn("FL", (F.col("baro_altitude") * 3.28084 / 100).cast(DoubleType())).cache()

    # Smoothen altitude.. 
    window_spec_avg = Window.partitionBy("track_id").orderBy("event_time").rowsBetween(-5, 5)
    #df = df.withColumn("FL", F.avg("FL").over(window_spec_avg))

    ## Window specification to order by event_time for each track_id
    window_spec = Window.partitionBy("track_id").orderBy("event_time")

    ## Add columns with lead FL values
    df = df.withColumn("next_FL", F.lead("FL").over(window_spec))

    ## Define the conditions for a crossing for each flight level of interest
    crossing_conditions = [
        (F.col("FL") < 50) & (F.col("next_FL") >= 50), # going up
        (F.col("FL") >= 50) & (F.col("next_FL") < 50), # going down
        (F.col("FL") < 70) & (F.col("next_FL") >= 70), # going up
        (F.col("FL") >= 70) & (F.col("next_FL") < 70), # going down
        (F.col("FL") < 100) & (F.col("next_FL") >= 100), # going up
        (F.col("FL") >= 100) & (F.col("next_FL") < 100), # going down
        (F.col("FL") < 245) & (F.col("next_FL") >= 245), # going up
        (F.col("FL") >= 245) & (F.col("next_FL") < 245), # going down

    ]

    ## Create a crossing column using multiple when conditions
    crossing_column = F.when(crossing_conditions[0], 50)
    for condition, flight_level in zip(crossing_conditions[1:], [50, 70, 70, 100, 100, 245, 245]):
        crossing_column = crossing_column.when(condition, flight_level)

    df = df.withColumn("crossing", crossing_column)

    ## Filter out only the rows where a crossing was detected
    crossing_points = df.filter(col("crossing").isNotNull())

    ## Filter out only the rows where the absolute difference between crossing and FL is less than 10 -> This filters out obvious errors
    crossing_points = crossing_points.filter(F.abs(F.col("crossing") - F.col("FL")) < 10)

    ## Identify first and last crossings by using window functions again
    ## Note that we need a separate window specification for descending order
    window_spec_crossing_asc = Window.partitionBy("track_id", "crossing").orderBy("event_time")
    window_spec_crossing_desc = Window.partitionBy("track_id", "crossing").orderBy(F.col("event_time").desc())

    crossing_points = crossing_points.withColumn(
        "row_number_asc",
        F.row_number().over(window_spec_crossing_asc)
    )
    crossing_points = crossing_points.withColumn(
        "row_number_desc",
        F.row_number().over(window_spec_crossing_desc)
    )

    ## Filter first and last crossings
    first_crossings = crossing_points.filter(F.col("row_number_asc") == 1)
    last_crossings = crossing_points.filter(F.col("row_number_desc") ==  1)

    ## Drop the temporary columns used for row numbering
    first_crossings = first_crossings.drop("row_number_asc", "row_number_desc")
    last_crossings = last_crossings.drop("row_number_asc", "row_number_desc")

    ## Add a label to indicate first or last crossing
    first_crossings = first_crossings.withColumn("type", F.concat(F.lit("first-xing-fl"), F.col('crossing').cast("string")))
    last_crossings = last_crossings.withColumn("type", F.concat(F.lit("last-xing-fl"), F.col('crossing').cast("string")))

    ## Combine the first and last crossing data
    all_crossings = first_crossings.union(last_crossings)

    ## Clean up
    df_lvl_events = all_crossings.select(
        col('track_id'),
        col('type'),
        col('event_time'),
        col('lon'),
        col('lat'),
        col('altitude_ft'),
        col('cumulative_distance_nm'),
        col('cumulative_time_s')
    )

    df_lvl_events = df_lvl_events.dropDuplicates(['track_id', 'type', 'event_time', 'lon', 'lat', 'altitude_ft', 'cumulative_distance_nm', 'cumulative_time_s'])
    
    return df_lvl_events

# Measurement helper functions

def add_time_measure(sdf_input):
    # Define the window spec partitioned by 'track_id'
    window_spec = Window.partitionBy('track_id')

    # Add a new column 'cumulative_time_s'
    sdf_input = sdf_input.withColumn(
        'min_event_time',
        F.min('event_time').over(window_spec)
    ).withColumn(
        'cumulative_time_s',
        (F.col('event_time') - F.col('min_event_time'))  # Calculate difference in seconds
    )

    # Drop the 'first_event_time' as it is no longer needed
    sdf_input = sdf_input.drop('min_event_time')
    
    return(sdf_input)

# Distance helper function
def add_distance_measure(df: DataFrame) -> DataFrame:
    """
    Calculate the great circle distance between consecutive points in nautical miles
    and the cumulative distance for each track, using native PySpark functions.

    Parameters:
    df (DataFrame): Input Spark DataFrame. Assumes columns "lat", "lon", "track_id", "event_time".

    Returns:
    DataFrame: DataFrame with additional columns "distance_nm" and "cumulative_distance_nm".
    """

    # Define a window spec for lag operation and cumulative sum
    windowSpecLag = Window.partitionBy("track_id").orderBy("event_time")
    windowSpecCumSum = Window.partitionBy("track_id").orderBy("event_time").rowsBetween(Window.unboundedPreceding, 0)

    # Convert degrees to radians using PySpark native function
    df = df.withColumn("lat_rad", F.radians(F.col("lat")))
    df = df.withColumn("lon_rad", F.radians(F.col("lon")))

    # Getting previous row's latitude and longitude
    df = df.withColumn("prev_lat_rad", F.lag("lat_rad").over(windowSpecLag))
    df = df.withColumn("prev_lon_rad", F.lag("lon_rad").over(windowSpecLag))

    # Compute the great circle distance using haversine formula in PySpark native functions
    df = df.withColumn("a", 
                       F.sin((F.col("lat_rad") - F.col("prev_lat_rad")) / 2)**2 + 
                       F.cos(F.col("prev_lat_rad")) * F.cos(F.col("lat_rad")) * 
                       F.sin((F.col("lon_rad") - F.col("prev_lon_rad")) / 2)**2)

    df = df.withColumn("c", 2 * F.atan2(F.sqrt(F.col("a")), F.sqrt(1 - F.col("a"))))

    # Radius of Earth in kilometers is 6371
    df = df.withColumn("distance_km", 6371 * F.col("c"))

    # Convert distance to nautical miles; 1 nautical mile = 1 / 1.852 km
    df = df.withColumn("segment_distance_nm", 
                       F.when(F.col("distance_km").isNull(), 0).otherwise(F.col("distance_km") / 1.852))

    # Calculate the cumulative distance
    df = df.withColumn("cumulative_distance_nm", F.sum("segment_distance_nm").over(windowSpecCumSum))

    # Drop temporary columns used for calculations
    df = df.drop("lat_rad", "lon_rad", "prev_lat_rad", "prev_lon_rad", "a", "c", "distance_km")

    return df

# Final ETL - Combining the datasets and writing away.. 

def etl_flight_events_and_measures(sdf_input, batch_id):
    # Add measures
    sdf_input = add_distance_measure(sdf_input)
    sdf_input = add_time_measure(sdf_input)
    
    # Calculate flight events    
    df_horizontal_events = calculate_horizontal_segment_events(sdf_input)
    df_vertical_events = calculate_vertical_crossing_events(sdf_input)
    
    # Formatting
    df_events = df_horizontal_events.union(df_vertical_events)
    df_events = df_events.withColumn('source', F.lit('OSN'))
    df_events = df_events.withColumn('version', F.lit('events_v0.0.2'))
    df_events = df_events.withColumn('info', F.lit(''))

    df_events = df_events.withColumn("id_tmp", F.concat(F.lit(batch_id),monotonically_increasing_id().cast('string'))).select(
        col('id_tmp'),
        col('track_id'),
        col('type'),
        col('event_time'),
        col('lon'),
        col('lat'),
        col('altitude_ft'),
        col('source'),
        col('version'),
        col('info'),
        col('cumulative_distance_nm'),
        col('cumulative_time_s')
    )

    # Milestones table
    df_milestones = df_events.select(
        col('id_tmp').alias('id'),
        col('track_id').alias('track_id'),
        col('type').alias('type'),
        col('event_time').alias('event_time'),
        col('lon').alias('longitude'),
        col('lat').alias('latitude'),
        col('altitude_ft').alias('altitude'),
        col('source').alias('source'),
        col('version').alias('version'),
        col('info').alias('info'),
    )
    df_milestones.write.mode("append").insertInto(f"`{project}`.`opdi_flight_events`")

    # Measurements table 
    ## Add Distance flown (NM) - df measure
    df_measurements_df = df_events.withColumn('type',F.lit('Distance flown (NM)'))
    df_measurements_df = df_measurements_df.withColumn('version',F.lit('distance_v0.0.2'))
    df_measurements_df = df_measurements_df.withColumn("id", F.concat(F.lit(batch_id),monotonically_increasing_id().cast('string'))).select(
        col('id'),
        col('id_tmp').alias('milestone_id'),
        col('type'),
        col('cumulative_distance_nm').alias('value'),
        col('version')
    )
    df_measurements_df.write.mode("append").insertInto(f"`{project}`.`opdi_measurements`")
    
    ## Add Time Passed - df measure
    df_measurements_tp = df_events.withColumn('type',F.lit('Time Passed (s)'))
    df_measurements_tp = df_measurements_tp.withColumn('version',F.lit('time_v0.0.1'))
    df_measurements_tp = df_measurements_tp.withColumn("id", F.concat(F.lit(batch_id),monotonically_increasing_id().cast('string'))).select(
        col('id'),
        col('id_tmp').alias('milestone_id'),
        col('type'),
        col('cumulative_time_s').alias('value'),
        col('version')
    )
    df_measurements_tp.write.mode("append").insertInto(f"`{project}`.`opdi_measurements`")
    
# Run code.. 

from datetime import datetime, timedelta

def get_previous_month(dt):
    """
    Get the first day of the previous month.

    :param dt: The current date.
    :return: A datetime object representing the first day of the previous month.
    """
    year, month = dt.year - (dt.month == 1), dt.month - 1 if dt.month > 1 else 12
    return datetime(year, month, 1)

def get_previous_day(date):
    """
    Returns the previous day's date for the given date.

    :param date: The current date.
    :return: Date object representing the previous day.
    """
    return date - timedelta(days=1)

print('-'*30)
print(f'Starting milestone extraction process for timeframe {start_date} until {end_date}...')

## Load logs
fpath = 'logs/04_osn-flight_events-etl-log.parquet'
if os.path.isfile(fpath):
    processed_days = pd.read_parquet(fpath).days.to_list()
else:
    processed_days = []

# Loop through time range in daily batches, moving backwards
current_date = end_date
while current_date >= start_date:        
    previous_day_date = get_previous_day(current_date)
    previous_day_timestamp = int(previous_day_date.timestamp())
    
    print(f'Currently processing {previous_day_date} - {current_date}')
    if current_date in processed_days:
        print('Already processed')
        current_date = previous_day_date
        continue
    
    # Perform the ETL operation for the current day
    sdf_input = spark.sql(f"""
        SELECT track_id, icao24, callsign, lat, lon, event_time, baro_altitude, velocity, vert_rate, cumulative_distance_nm
        FROM `{project}`.`osn_tracks_clustered`
        WHERE track_id IN (
            SELECT track_id
            FROM `{project}`.`osn_tracks_clustered`
            GROUP BY track_id
            HAVING MIN(event_time) BETWEEN {previous_day_timestamp} AND {int(current_date.timestamp())}
        )""").persist()

    etl_openap_flight_events(sdf_input, batch_id=previous_day_date.strftime("%Y%m%d") + '_')
    sdf_input.unpersist()
    
    processed_days.append(current_date)
    processed_days_df = pd.DataFrame({'days':processed_days})
    processed_days_df.to_parquet(fpath)
    
    # Move to the previous day
    current_date = previous_day_date

# Cleanup
# Uncomment the following line if you want to drop the table
# spark.sql(f"""DROP TABLE IF EXISTS `{project}`.`osn_tracks`;""")

# Stop the SparkSession
spark.stop()