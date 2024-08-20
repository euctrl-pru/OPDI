# Regular imports
import pandas as pd 
import numpy as np
import time
import os
import shutil
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import calendar
from IPython.display import display, HTML

# Spark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    avg, udf, abs, col, split, array, array_remove, lit, lag, when, lead,
    min, max, sum as Fsum, explode, monotonically_increasing_id, round,
    to_timestamp, concat, filter, pandas_udf, PandasUDFType, struct, to_json, concat_ws, unix_timestamp
)


# Hotfix
#!cp /runtime-addons/cmladdon-2.0.40-b150/log4j.properties /etc/spark/conf/

# Spark Session Initialization
#shutil.copy("/runtime-addons/cmladdon-2.0.40-b150/log4j.properties", "/etc/spark/conf/") # Setting logging properties
spark = SparkSession.builder \
    .appName("OPDI flight events and measurements ETL") \
    .config("spark.log.level", "ERROR")\
    .config("spark.ui.showConsoleProgress", "false")\
    .config("spark.hadoop.fs.azure.ext.cab.required.group", "eur-app-opdi") \
    .config("spark.kerberos.access.hadoopFileSystems", "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged") \
    .config("spark.driver.cores", "1") \
    .config("spark.driver.memory", "5G") \
    .config("spark.executor.memory", "8G") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "15") \
    .config("spark.network.timeout", "800s")\
    .config("spark.executor.heartbeatInterval", "400s") \
    .config('spark.ui.showConsoleProgress', False) \
    .enableHiveSupport() \
    .getOrCreate()

# Database prep

# Settings
project = "project_opdi"
recreate_flight_event_table = False
recreate_measurement_table = False

## Range for processing
start_date = datetime.strptime('2022-01-01', '%Y-%m-%d')
end_date = datetime.strptime('2024-07-01', '%Y-%m-%d')


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

def smooth_baro_alt(df):
    # Define the acceptable rate of climb/descent threshold (25.4 meters per second, i.e. 5000ft/min)
    rate_threshold = 25.4

    # Calculate the rate of climb/descent in meters per second for each track_id
    window_spec = Window.partitionBy("track_id").orderBy("event_time")
    df = df.withColumn("prev_baro_altitude", lag("baro_altitude").over(window_spec))
    df = df.withColumn("prev_event_time", lag("event_time").over(window_spec))

    df = df.withColumn("time_diff", (col("event_time") - col("prev_event_time")))
    df = df.withColumn("altitude_diff", (col("baro_altitude") - col("prev_baro_altitude")))
    df = df.withColumn("rate_of_climb", col("altitude_diff") / col("time_diff"))

    # Create a window for calculating the rolling average
    # Define a time window for the rolling average (e.g., 300 seconds or 5 min)
    time_window = 300

    # Create a window specification for calculating the rolling average based on time
    window_spec_avg = Window.partitionBy("track_id").orderBy("event_time").rangeBetween(-time_window, time_window)

    # Calculate the rolling average (smoothing)
    df = df.withColumn("smoothed_baro_altitude", avg("baro_altitude").over(window_spec_avg))

    # Replace unrealistic climb/descent points with the smoothed values
    df = df.withColumn("baro_altitude",
                       when((abs(col("rate_of_climb")) > rate_threshold),
                            col("smoothed_baro_altitude")).otherwise(col("baro_altitude")))

    # Drop the temporary columns
    df = df.drop("smoothed_baro_altitude", "prev_baro_altitude", "prev_event_time", "time_diff", "altitude_diff", "rate_of_climb")
    return df

# Helper function for calculating horizontal segments events

def zmf(col, a, b):
    """Zero-order membership function (ZMF)."""
    return F.when(col <= a, 1).when(col >= b, 0).otherwise((b - col) / (b - a))

def gaussmf(col, mean, sigma):
    """Gaussian membership function (GaussMF)."""
    return F.exp(-((col - mean) ** 2) / (2 * sigma ** 2))

def smf(col, a, b):
    """S-shaped membership function (SMF)."""
    return F.when(col <= a, 0).when(col >= b, 1).otherwise((col - a) / (b - a))

def calculate_horizontal_segment_events(sdf_input):
    """
    Calculate horizontal segment events for flight data.
    
    Args:
        sdf_input (DataFrame): Input Spark DataFrame containing flight data.
        
    Returns:
        DataFrame: DataFrame with calculated segment events.
    """
    df = sdf_input.select(
        "track_id", "lat", "lon", "event_time", "baro_altitude",
        "vert_rate", "velocity", "cumulative_distance_nm", "cumulative_time_s"
    )

    # Extracting OpenAP phases
    df = df.withColumn("alt", col("baro_altitude") * 3.28084)
    df = df.withColumn("roc", col("vert_rate") * 196.850394)
    df = df.withColumn("spd", col("velocity") * 1.94384)

    # Apply membership functions to the DataFrame
    df = df.withColumn("alt_gnd", zmf(col("alt"), 0, 200))
    df = df.withColumn("alt_lo", gaussmf(col("alt"), 10000, 10000))
    df = df.withColumn("alt_hi", gaussmf(col("alt"), 35000, 20000))
    df = df.withColumn("roc_zero", gaussmf(col("roc"), 0, 100))
    df = df.withColumn("roc_plus", smf(col("roc"), 10, 1000))
    df = df.withColumn("roc_minus", zmf(col("roc"), -1000, -10))
    df = df.withColumn("spd_hi", gaussmf(col("spd"), 600, 100))
    df = df.withColumn("spd_md", gaussmf(col("spd"), 300, 100))
    df = df.withColumn("spd_lo", gaussmf(col("spd"), 0, 50))

    # Cache the DataFrame after initial transformations
    df.cache()

    # Define window specification for time windows
    window_spec = Window.partitionBy("track_id").orderBy("event_time").rangeBetween(Window.unboundedPreceding, 0)

    # Aggregate and apply fuzzy logic within each window
    df = df.withColumn("alt_mean", F.avg("alt").over(window_spec))
    df = df.withColumn("spd_mean", F.avg("spd").over(window_spec))
    df = df.withColumn("roc_mean", F.avg("roc").over(window_spec))

    # Apply fuzzy logic rules
    df = df.withColumn("rule_ground", F.least(col("alt_gnd"), col("roc_zero"), col("spd_lo")))
    df = df.withColumn("rule_climb", F.least(col("alt_lo"), col("roc_plus"), col("spd_md")))
    df = df.withColumn("rule_descent", F.least(col("alt_lo"), col("roc_minus"), col("spd_md")))
    df = df.withColumn("rule_cruise", F.least(col("alt_hi"), col("roc_zero"), col("spd_hi")))
    df = df.withColumn("rule_level", F.least(col("alt_lo"), col("roc_zero"), col("spd_md")))

    # Aggregate and determine phase label
    df = df.withColumn("aggregated", F.greatest(
        col("rule_ground"), col("rule_climb"), col("rule_descent"),
        col("rule_cruise"), col("rule_level"))
    )

    df = df.withColumn(
        "flight_phase", 
        F.when(col("aggregated") == col("rule_ground"), "GND")
         .when(col("aggregated") == col("rule_climb"), "CL")
         .when(col("aggregated") == col("rule_descent"), "DE")
         .when(col("aggregated") == col("rule_cruise"), "CR")
         .when(col("aggregated") == col("rule_level"), "LVL")
    )

    df = df.withColumnRenamed("alt", "altitude_ft") \
           .withColumnRenamed("roc", "roc_ft_min") \
           .withColumnRenamed("spd", "speed_kt")

    df = df.select(
        "track_id", "lat", "lon", "event_time", "cumulative_distance_nm",
        "cumulative_time_s", "altitude_ft", "roc_ft_min", "speed_kt", "flight_phase"
    )

    # Cache the DataFrame before applying window functions for phase detection
    df.cache()

    # Window specification for detecting phase changes and for TOC/TOD
    window_spec_phase = Window.partitionBy("track_id").orderBy("event_time")
    window_spec_cumulative = Window.partitionBy("track_id").orderBy("event_time").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    # Detect phase changes
    df_with_phases = df.withColumn("prev_phase", lag("flight_phase", 1, "None").over(window_spec_phase))
    df_with_phases = df_with_phases.withColumn("next_phase", lead("flight_phase", 1, "None").over(window_spec_phase))

    # Cache the DataFrame after phase detection
    df_with_phases.cache()

    # Identify TOC and TOD for 'CR' phases
    df_with_phases = df_with_phases.withColumn("first_cr_time", min(when(col("flight_phase") == "CR", col("event_time"))).over(window_spec_cumulative))
    df_with_phases = df_with_phases.withColumn("last_cr_time", max(when(col("flight_phase") == "CR", col("event_time"))).over(window_spec_cumulative))

    # Create event types
    start_of_segment = (col("flight_phase").isin("CR", "LVL")) & (col("prev_phase") != col("flight_phase"))
    df_with_phases = df_with_phases.withColumn("start_of_segment", start_of_segment)

    df_with_phases = df_with_phases.withColumn("segment_count", F.sum(when(col("start_of_segment"), 1).otherwise(0)).over(window_spec_cumulative))

    df_with_phases = df_with_phases.withColumn(
        "milestone_types",
        F.when(col("event_time") == col("first_cr_time"), F.array(F.lit("level-start"), F.lit("top-of-climb")))
         .when(col("event_time") == col("last_cr_time"), F.array(F.lit("level-end"), F.lit("top-of-descent")))
         .when(start_of_segment, F.array(F.lit("level-start")))
         .when((col("flight_phase").isin("CR", "LVL")) & (col("next_phase") != col("flight_phase")), F.array(F.lit('level-end')))
         .when((col("prev_phase") == "GND") & (col("next_phase") == "CL"), F.array(F.lit("take-off")))
         .when((col("prev_phase") == "DE") & (col("flight_phase") == "GND"), F.array(F.lit("landing")))
         .otherwise(F.array())
    )

    # Explode the event_types array to create rows for each event
    df_exploded = df_with_phases.select("*", explode(col("milestone_types")).alias("type"))

    # Filter out rows with no relevant events
    df_exploded = df_exploded.filter("type IS NOT NULL")

    # Reformat
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
    
    # Add info
    df_openap_events = df_openap_events.withColumn('info', F.lit(''))
    
    return df_openap_events



# Flight levels crossing extractions
def calculate_vertical_crossing_events(sdf_input):
    """
    Calculate vertical crossing events for flight data.
    
    Args:
        sdf_input (DataFrame): Input Spark DataFrame containing flight data.
        
    Returns:
        DataFrame: DataFrame with calculated vertical crossing events.
    """
    df = sdf_input.select(
        "track_id", "lat", "lon", "event_time", "baro_altitude",
        "vert_rate", "velocity", "cumulative_distance_nm", "cumulative_time_s"
    )

    # Convert meters to flight level and create a new column
    df = df.withColumn("altitude_ft", col("baro_altitude") * 3.28084)
    df = df.withColumn("FL", (col("baro_altitude") * 3.28084 / 100).cast(DoubleType()))

    # Window specification to order by event_time for each track_id
    window_spec = Window.partitionBy("track_id").orderBy("event_time")

    # Add columns with lead FL values
    df = df.withColumn("next_FL", F.lead("FL").over(window_spec)).cache()

    # Define the conditions for a crossing for each flight level of interest
    crossing_conditions = [
        (col("FL") < 50) & (col("next_FL") >= 50),  # going up
        (col("FL") >= 50) & (col("next_FL") < 50),  # going down
        (col("FL") < 70) & (col("next_FL") >= 70),  # going up
        (col("FL") >= 70) & (col("next_FL") < 70),  # going down
        (col("FL") < 100) & (col("next_FL") >= 100),  # going up
        (col("FL") >= 100) & (col("next_FL") < 100),  # going down
        (col("FL") < 245) & (col("next_FL") >= 245),  # going up
        (col("FL") >= 245) & (col("next_FL") < 245),  # going down
    ]

    # Create a crossing column using multiple when conditions
    crossing_column = F.when(crossing_conditions[0], 50)
    for condition, flight_level in zip(crossing_conditions[1:], [50, 70, 70, 100, 100, 245, 245]):
        crossing_column = crossing_column.when(condition, flight_level)

    df = df.withColumn("crossing", crossing_column)

    # Filter out only the rows where a crossing was detected
    crossing_points = df.filter(col("crossing").isNotNull())

    # Filter out only the rows where the absolute difference between crossing and FL is less than 10
    crossing_points = crossing_points.filter(F.abs(col("crossing") - col("FL")) < 10).cache()

    # Identify first and last crossings by using window functions again
    # Note that we need a separate window specification for descending order
    window_spec_crossing = Window.partitionBy("track_id", "crossing").orderBy("event_time")
    window_spec_crossing_desc = Window.partitionBy("track_id", "crossing").orderBy(col("event_time").desc())

    crossing_points = crossing_points.withColumn("row_number_asc", F.row_number().over(window_spec_crossing))
    crossing_points = crossing_points.withColumn("row_number_desc", F.row_number().over(window_spec_crossing_desc))

    # Cache the DataFrame after applying window functions for row numbers
    crossing_points.cache()

    # Filter first and last crossings
    first_crossings = crossing_points.filter(col("row_number_asc") == 1)
    last_crossings = crossing_points.filter(col("row_number_desc") == 1)

    # Drop the temporary columns used for row numbering
    first_crossings = first_crossings.drop("row_number_asc", "row_number_desc")
    last_crossings = last_crossings.drop("row_number_asc", "row_number_desc")

    # Add a label to indicate first or last crossing
    first_crossings = first_crossings.withColumn("type", F.concat(F.lit("first-xing-fl"), col('crossing').cast("string")))
    last_crossings = last_crossings.withColumn("type", F.concat(F.lit("last-xing-fl"), col('crossing').cast("string")))

    # Combine the first and last crossing data
    all_crossings = first_crossings.union(last_crossings)

    # Clean up
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

    df_lvl_events = df_lvl_events.dropDuplicates([
        'track_id', 'type', 'event_time', 'lon', 'lat', 'altitude_ft', 'cumulative_distance_nm', 'cumulative_time_s'
    ])
    
    # Add info
    df_lvl_events = df_lvl_events.withColumn('info', F.lit(''))
    
    return df_lvl_events

def calculate_firstseen_lastseen_events(sdf_input):
    """
    Calculate the first seen and last seen events for each track_id.

    Args:
        sdf_input (DataFrame): Input Spark DataFrame containing flight data.
        
    Returns:
        DataFrame: DataFrame with calculated first seen and last seen events.
    """
    # Select necessary columns
    df = sdf_input.select(
        "track_id", "lat", "lon", "event_time", "baro_altitude",
        "vert_rate", "velocity", "cumulative_distance_nm", "cumulative_time_s"
    )
    
    df = df.withColumn("altitude_ft", col("baro_altitude") * 3.28084)
    
    # Window specification to order by event_time for each track_id
    window_spec = Window.partitionBy("track_id").orderBy("event_time")
    window_spec_desc = Window.partitionBy("track_id").orderBy(F.col("event_time").desc())

    # Add row numbers for ascending and descending order of event_time
    df = df.withColumn("row_number_asc", F.row_number().over(window_spec))
    df = df.withColumn("row_number_desc", F.row_number().over(window_spec_desc))

    # Filter first and last seen events
    first_seen = df.filter(F.col("row_number_asc") == 1).drop("row_number_asc", "row_number_desc")
    last_seen = df.filter(F.col("row_number_desc") == 1).drop("row_number_asc", "row_number_desc")

    # Add a label to indicate first or last seen event
    first_seen = first_seen.withColumn("type", F.lit("first_seen"))
    last_seen = last_seen.withColumn("type", F.lit("last_seen"))

    # Combine the first and last seen data
    all_seen_events = first_seen.union(last_seen)

    # Clean up the DataFrame
    df_seen_events = all_seen_events.select(
        "track_id",
        "type",
        "event_time",
        "lon",
        "lat",
        "altitude_ft",
        "cumulative_distance_nm",
        "cumulative_time_s"
    )

    # Drop duplicates
    df_seen_events = df_seen_events.dropDuplicates([
        "track_id", "type", "event_time", "lon", "lat", "altitude_ft", "cumulative_distance_nm", "cumulative_time_s"
    ])
    
    # Add info
    df_seen_events = df_seen_events.withColumn('info', F.lit(''))
    
    return df_seen_events

def calculate_airport_events(sv, month):

    flight_list = get_data_within_timeframe(
        spark, 
        'project_opdi.opdi_flight_list', 
        month, 
        time_col='dof', 
        unix_time=False
    ).select(
        ['id', 'adep', 'ades', 'adep_p', 'ades_p']
    )

    # Create the 'apt' column by combining and filtering out null values
    flight_list = flight_list.withColumn("adep", when(col("adep").isNull(), lit("")).otherwise(col("adep")))
    flight_list = flight_list.withColumn("ades", when(col("ades").isNull(), lit("")).otherwise(col("ades")))
    flight_list = flight_list.withColumn("adep_p", when(col("adep_p").isNull(), lit("")).otherwise(col("adep_p")))
    flight_list = flight_list.withColumn("ades_p", when(col("ades_p").isNull(), lit("")).otherwise(col("ades_p")))

    # Create the 'apt' column by combining and filtering out empty strings
    flight_list = flight_list.withColumn(
        'apt', 
        F.concat(F.array(F.col("adep"), F.col("ades")), F.split(F.col("adep_p"), ', '), F.split(F.col("ades_p"), ', '))
    ).withColumn(
        'apt', F.array_remove(col('apt'), "")
    ).select(['id', 'apt'])


    # Filter out rows with missing crucial data
    sv_f = sv.dropna(subset=['lat', 'lon', 'baro_altitude'])

    # Rename callsign to flight_id (official term)
    sv_f = sv_f.withColumnRenamed("callsign", "flight_id")

    # Replace missing 'flight_id' with empty string
    sv_f = sv_f.fillna({'flight_id': ''})

    # Convert event_time from posixtime to datetime 
    sv_f = sv_f.withColumn('event_time', F.to_timestamp(F.col('event_time')))

    # Add flight level extracted from baro_altitude
    sv_f = sv_f.withColumn('altitude_ft', col('baro_altitude') * 3.28084)
    sv_f = sv_f.withColumn('flight_level', col('altitude_ft') / 100)

    # Select columns of interest
    columns_of_interest = [
        'track_id', 'icao24', 'flight_id', 'event_time', 'lat', 'lon',  'altitude_ft', 'flight_level',
        'heading', 'vert_rate', 'h3_res_12', 'cumulative_distance_nm', 'cumulative_time_s'
    ]
    sv_f = sv_f.select(columns_of_interest)

    # Filter out low altitude statevectors 
    sv_low_alt = sv_f.filter(col('flight_level') <= 20).cache()

    # Merge with defined airports from flight_list
    sv_nearby_apt = sv_low_alt.join(flight_list, sv.track_id == flight_list.id, how="inner")

    # Load runway spark dataframe (rwy_sdf) 
    apt_sdf = spark.table('project_opdi.hexaero_airport_layouts')

    # Using the apt_ident provided by the first 'airport merge' speeds up the merge dramatically
    df_labelled = sv_nearby_apt.join(
        apt_sdf, 
        (sv_nearby_apt.h3_res_12 == apt_sdf.hexaero_h3_id) & 
        F.array_contains(sv_nearby_apt.apt, apt_sdf.hexaero_apt_icao), 
        "inner")

    # Define window specification to partition by id and hexaero_id, and order by time
    window_spec = Window.partitionBy("track_id", "hexaero_osm_id").orderBy("event_time")

    # Calculate time difference between each row and the previous row
    df_labelled = df_labelled.withColumn("time_diff", (col("event_time").cast("long") - lag(col("event_time").cast("long"), 1).over(window_spec)))

    # Identify rows where a new track starts (assuming a new track if the gap is more than 5 minutes)
    df_labelled = df_labelled.withColumn("new_trace", when(col("time_diff") > 300, lit(1)).otherwise(lit(0)))

    # Cumulative sum to assign a new ID each time 'new_trace' is True
    df_labelled = df_labelled.withColumn("trace_id", Fsum(col("new_trace")).over(window_spec))
    df_labelled = df_labelled.drop("time_diff", "new_trace")

    # Calculate entry and exit times with cumulative distances
    result = df_labelled.groupBy(
        "track_id", "icao24", "flight_id", "hexaero_apt_icao", "hexaero_osm_id", "hexaero_aeroway", "hexaero_ref", "hexaero_avg_heading", "trace_id"
    ).agg(
        F.min("event_time").alias("entry_time"),
        F.max("event_time").alias("exit_time"),
        F.first("lat").alias("entry_lat"), 
        F.last("lat").alias("exit_lat"),
        F.first("lon").alias("entry_lon"), 
        F.last("lon").alias("exit_lon"),
        F.first("altitude_ft").alias("entry_altitude_ft"),
        F.last("altitude_ft").alias("exit_altitude_ft"),
        F.first("cumulative_distance_nm").alias("entry_cumulative_distance_nm"),
        F.last("cumulative_distance_nm").alias("exit_cumulative_distance_nm"),
        F.first("cumulative_time_s").alias("entry_cumulative_time_s"),
        F.last("cumulative_time_s").alias("exit_cumulative_time_s")
    )

    # Add time in use for each element
    result = result.withColumn("time_in_use_seconds", col("exit_time").cast("long") - col("entry_time").cast("long"))
    result = result.filter(col("time_in_use_seconds") != 0)

    result = result.withColumnRenamed("hexaero_osm_id", "osm_id") \
                         .withColumnRenamed("hexaero_aeroway", "osm_aeroway") \
                         .withColumnRenamed("hexaero_ref", "osm_ref") \
                         .withColumnRenamed("hexaero_apt_icao", "osm_airport") \
                         .withColumnRenamed("hexaero_avg_heading", "osm_avg_heading")

    # Add 'info' column
    result = result.withColumn("info", to_json(struct(
        col("osm_id"),
        col("osm_aeroway"),
        col("osm_ref"),
        col("osm_airport"),
        col("osm_avg_heading").alias('opdi_avg_heading'),
        col("time_in_use_seconds").alias("opdi_time_in_use_s"), 
        col("icao24").alias("osn_icao24"),
        col("flight_id").alias("osn_flight_id")
    )))

    # Add 'entry_type' and 'exit_type' columns
    result = result.withColumn("entry_type", concat_ws('-', lit('entry'), col("osm_aeroway"))) \
                         .withColumn("exit_type", concat_ws('-', lit('exit'), col("osm_aeroway")))

    result.cache()

    # Select and rename entry events
    entry_events = result.select(
        col("track_id"),
        col("entry_time").alias("event_time"),
        col("entry_lon").alias("lon"),
        col("entry_lat").alias("lat"),
        col("entry_altitude_ft").alias("altitude_ft"),
        col("entry_cumulative_distance_nm").alias("cumulative_distance_nm"),
        col("entry_cumulative_time_s").alias("cumulative_time_s"),
        col("entry_type").alias("type"),
        col("info")
    )

    # Select and rename exit events
    exit_events = result.select(
        col("track_id"),
        col("exit_time").alias("event_time"),
        col("exit_lon").alias("lon"),
        col("exit_lat").alias("lat"),
        col("exit_altitude_ft").alias("altitude_ft"),
        col("exit_cumulative_distance_nm").alias("cumulative_distance_nm"),
        col("exit_cumulative_time_s").alias("cumulative_time_s"),
        col("exit_type").alias("type"),
        col("info")
    )

    # Concatenate entry and exit events
    apt_events = entry_events.unionByName(exit_events)
    
    # Event time back to unix
    apt_events = apt_events.withColumn('event_time', unix_timestamp(F.col('event_time')))
    
    # Clean up the DataFrame
    apt_events = apt_events.select(
        "track_id",
        "type",
        "event_time",
        "lon",
        "lat",
        "altitude_ft",
        "cumulative_distance_nm",
        "cumulative_time_s",
        "info"
    )

    # Drop duplicates
    apt_events = apt_events.dropDuplicates([
        "track_id", "type", "event_time", "lon", "lat", "altitude_ft", "cumulative_distance_nm", "cumulative_time_s"
    ])
    
    return apt_events

# Measurement helper functions

def add_time_measure(sdf_input):
    """
    Add a cumulative time measure to the input Spark DataFrame.
    
    Args:
        sdf_input (DataFrame): Input Spark DataFrame containing flight data.
        
    Returns:
        DataFrame: DataFrame with added cumulative time measure.
    """
    # Define the window spec partitioned by 'track_id'
    window_spec = Window.partitionBy('track_id').orderBy('event_time')

    # Add a new column 'cumulative_time_s' based on the minimum event time per track_id
    sdf_input = sdf_input.withColumn('min_event_time', F.min('event_time').over(window_spec))

    # Calculate the cumulative time in seconds
    sdf_input = sdf_input.withColumn(
        'cumulative_time_s',
        (F.col('event_time').cast("long") - F.col('min_event_time').cast("long"))
    )

    # Drop the 'min_event_time' as it is no longer needed
    sdf_input = sdf_input.drop('min_event_time')

    return sdf_input


# Distance helper function
def add_distance_measure(df):
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

def etl_flight_events_and_measures(
        sdf_input, 
        batch_id, 
        month,
        calc_vertical = True, 
        calc_horizontal = True,
        calc_hexaero = True, 
        calc_seen = True):
    
    # Smooth baro_alt              
    sdf_input = smooth_baro_alt(sdf_input)
    
    # Add measures
    sdf_input = add_distance_measure(sdf_input)
    sdf_input = add_time_measure(sdf_input)
    
    # Cache
    sdf_input.cache()
    
    if calc_horizontal:
        print(f"Calculating horizontal events (phase) for batch_id: {batch_id}")
        df_events = calculate_horizontal_segment_events(sdf_input)
    else:
        df_events = None
        
    if calc_vertical:
        print(f"Calculating vertical events (FL crossings) for batch_id: {batch_id}")
        df_vertical_events = calculate_vertical_crossing_events(sdf_input)
        
        if pd.isnull(df_events):
            df_events = df_vertical_events
        else: 
            df_events = df_events.union(df_vertical_events)
    
    if calc_hexaero:
        print(f"Calculating hexaero events (airport events) for batch_id: {batch_id}")
        df_hexaero_events = calculate_airport_events(sdf_input, month)
        
        if pd.isnull(df_events):
            df_events = df_hexaero_events
        else: 
            df_events = df_events.union(df_hexaero_events)
    
    if calc_seen:
        print(f"Calculating first_seen/last_seen events for batch_id: {batch_id}")
        df_seen_events = calculate_firstseen_lastseen_events(sdf_input)
        
        if pd.isnull(df_events):
            df_events = df_seen_events
        else: 
            df_events = df_events.union(df_seen_events)
    
    df_events.cache()
    df_events = df_events.withColumn('source', F.lit('OSN'))
    df_events = df_events.withColumn('version', F.lit('events_v0.0.2'))

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
    #df_milestones.toPandas().to_parquet('events_test.parquet')
    
    # Measurements table 
    ## Add Distance flown (NM) - df measure
    df_measurements_df = df_events.withColumn('type',F.lit('Distance flown (NM)'))
    df_measurements_df = df_measurements_df.withColumn('version',F.lit('distance_v0.0.2'))
    df_measurements_df = df_measurements_df.withColumn("id", F.concat(F.lit(batch_id + "_d_"),monotonically_increasing_id().cast('string'))).select(
        col('id'),
        col('id_tmp').alias('milestone_id'),
        col('type'),
        col('cumulative_distance_nm').alias('value'),
        col('version')
    )

    ## Add Time Passed - df measure
    df_measurements_tp = df_events.withColumn('type',F.lit('Time Passed (s)'))
    df_measurements_tp = df_measurements_tp.withColumn('version',F.lit('time_v0.0.1'))
    df_measurements_tp = df_measurements_tp.withColumn("id", F.concat(F.lit(batch_id + "_t_"),monotonically_increasing_id().cast('string'))).select(
        col('id'),
        col('id_tmp').alias('milestone_id'),
        col('type'),
        col('cumulative_time_s').alias('value'),
        col('version')
    )
    
    # Merge
    df_measurements = df_measurements_df.union(df_measurements_tp)
    df_measurements.write.mode("append").insertInto(f"`{project}`.`opdi_measurements`")
    #df_measurements.toPandas().to_parquet('measurements_test.parquet')
    
    df_measurements_tp.write.mode("append").insertInto(f"`{project}`.`opdi_measurements`")
    return None
    
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


def generate_months(start_date, end_date):
    """Generate a list of dates corresponding to the first day of each month between two dates.

    Args:
    start_date (datetime.date): The starting date.
    end_date (datetime.date): The ending date.

    Returns:
    list: A list of date objects for the first day of each month within the specified range.
    """
    current = start_date
    months = []
    while current <= end_date:
        months.append(current)
        # Increment month
        month = current.month
        year = current.year
        if month == 12:
            current = date(year + 1, 1, 1)
        else:
            current = date(year, month + 1, 1)
    return months

def get_start_end_of_month(date):
    """Return a datetime object for the first and last second  of the given month and year."""
    year = date.year
    month = date.month
    
    first_second = datetime(year, month, 1, 0, 0, 0)
    last_day = calendar.monthrange(year, month)[1]
    last_second = datetime(year, month, last_day, 23, 59, 59)
    return first_second.timestamp(), last_second.timestamp()


def get_data_within_timeframe(spark, table_name, month, time_col = 'event_time', unix_time = True):
    """
    Retrieves records from a specified Spark table within the given timeframe.

    Args:
    spark (SparkSession): The SparkSession object.
    table_name (str): The name of the Spark table to query.
    month (str): The start date of a month in datetime format.

    Returns:
    pyspark.sql.dataframe.DataFrame: A DataFrame containing the records within the specified timeframe.
    """
    # Convert dates to POSIX time (seconds since epoch)
    start_posix,stop_posix = get_start_end_of_month(month)

    # Load the table
    df = spark.read.table(table_name)
    
    if unix_time:
        # Filter records based on event_time posix column
        df = df.filter((col(time_col) >= start_posix) & (col(time_col) < stop_posix))
    else: 
        df = df.withColumn('unix_time', unix_timestamp(time_col))
        df = df.filter((col('unix_time') >= start_posix) & ((col('unix_time') < stop_posix)))
                                
    return df
  
# Actual processing
start_month = date(2022, 1, 1)
end_month = date(2024, 7, 1)

to_process_months = generate_months(start_month, end_month)
#to_process_months.reverse()

## Load logs Horizontal
fpath_horizontal = 'logs/04_osn-flight_event-horizontal-etl-log.parquet'
if os.path.isfile(fpath_horizontal):
    processed_months_horizontal = pd.read_parquet(fpath_horizontal).months.to_list()
else:
    processed_months_horizontal = []

## Load logs vertical
fpath_vertical = 'logs/04_osn-flight_event-vertical-etl-log.parquet'
if os.path.isfile(fpath_vertical):
    processed_months_vertical = pd.read_parquet(fpath_vertical).months.to_list()
else:
    processed_months_vertical = []

## Load hexaero airports
fpath_hexaero = 'logs/04_osn-flight_event-hexaero_airport-log.parquet'
if os.path.isfile(fpath_hexaero):
    processed_months_hexaero = pd.read_parquet(fpath_hexaero).months.to_list()
else:
    processed_months_hexaero = []

## Load first-seen / last-seen logs 
fpath_seen = 'logs/04_osn-flight_event-first_seen_last_seen-log.parquet'
if os.path.isfile(fpath_seen):
    processed_months_seen = pd.read_parquet(fpath_seen).months.to_list()
else:
    processed_months_seen = []

# Process loops
for month in to_process_months:
    print(f'Processing flight events for month: {month}')
    calc_horizontal = True
    calc_vertical = True
    calc_hexaero = True
    calc_seen = True
    
    if month in processed_months_horizontal:
        print('Month horizontal processed already (fuzzy labelling)')
        calc_horizontal = False
    
    if month in processed_months_vertical:
        print('Month vertical processed already (FLs)')
        calc_vertical = False
        
    if month in processed_months_hexaero:
        print('Month hexaero airports already processed')
        calc_hexaero = False
        
    if month in processed_months_hexaero:
        print('Month first-seen / last-seen already processed')
        calc_seen = False
        
    if (not calc_horizontal and not calc_vertical and not calc_hexaero and not calc_seen):
        print('All elements for this month are already processed..')
        continue
    
    else:
        # Perform the ETL operation for the current day
        sdf_input = get_data_within_timeframe( # State Vectors sv
            spark = spark, 
            table_name = f'{project}.osn_tracks_clustered', 
            month = month).select("track_id","lat","lon","event_time","baro_altitude","velocity","vert_rate", "callsign", "icao24", "heading", "h3_res_12").cache()

        etl_flight_events_and_measures(sdf_input, 
                                       batch_id=month.strftime("%Y%m%d") + '_', 
                                       month = month,
                                       calc_vertical = calc_vertical, 
                                       calc_horizontal = calc_horizontal,
                                       calc_hexaero = calc_hexaero,
                                       calc_seen = calc_seen
                                      )
        
        ## Logging
        if calc_horizontal:
            processed_months_horizontal.append(month)
            processed_df = pd.DataFrame({'months':processed_months_horizontal})
            processed_df.to_parquet(fpath_horizontal)
        if calc_vertical:
            processed_months_vertical.append(month)
            processed_df = pd.DataFrame({'months':processed_months_vertical})
            processed_df.to_parquet(fpath_vertical)
        if calc_hexaero:
            processed_months_hexaero.append(month)
            processed_df = pd.DataFrame({'months':processed_months_hexaero})
            processed_df.to_parquet(fpath_hexaero)
        if calc_vertical:
            processed_months_seen.append(month)
            processed_df = pd.DataFrame({'months':processed_months_seen})
            processed_df.to_parquet(fpath_seen)
          
    spark.catalog.clearCache()

# Stop the SparkSession
spark.stop()