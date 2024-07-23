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
from pyspark.sql.functions import avg, udf, abs, col, avg, lag, when,  pandas_udf, PandasUDFType, lit, col, when, lag, lead, min, max, sum as Fsum, explode, array, monotonically_increasing_id, round
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
    .config("spark.hadoop.fs.azure.ext.cab.required.group", "eur-app-opdi") \
    .config("spark.kerberos.access.hadoopFileSystems", "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged") \
    .config("spark.driver.cores", "1") \
    .config("spark.driver.memory", "10G") \
    .config("spark.executor.memory", "10G") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "10") \
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

from pyspark.sql import Window, functions as F
from pyspark.sql.functions import col, lag, lead, min, max, when, explode

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

    return df_openap_events



def calculate_horizontal_segment_events_old(sdf_input):
    df = sdf_input
    
    # Extracting OpenAP phases
    ## Defining variables
    df = df.withColumn("altitude_ft", col("baro_altitude") * 3.28084)
    df = df.withColumn("roc_ft_min", col("vert_rate") * 196.850394)
    df = df.withColumn("speed_kt", col("velocity") * 1.94384)

    ## Define schema for UDF
    df = df.select("track_id", "lat", "lon", "event_time", "altitude_ft", "roc_ft_min", "speed_kt")

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
        #StructField("cumulative_distance_nm", DoubleType()),
        #StructField("cumulative_time_s", IntegerType()),
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
from pyspark.sql import Window, functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col

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

    return df_lvl_events


# Measurement helper functions

from pyspark.sql import Window, functions as F

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

def etl_flight_events_and_measures(sdf_input, batch_id, calc_vertical = True, calc_horizontal = True):
    
    # Smooth baro_alt              
    sdf_input = smooth_baro_alt(sdf_input)
    
    # Add measures
    sdf_input = add_distance_measure(sdf_input)
    sdf_input = add_time_measure(sdf_input)
    
    # Cache
    sdf_input.cache()
    
    # Calculate flight events 
    if (calc_vertical and calc_horizontal): 
      print(f"Calculating both vertical and horizontal events for batch_id: {batch_id}")
      df_horizontal_events = calculate_horizontal_segment_events(sdf_input)
      df_vertical_events = calculate_vertical_crossing_events(sdf_input)
    
      # Cache
      df_horizontal_events.cache()
      df_vertical_events.cache()
    
      # Formatting
      df_events = df_horizontal_events.union(df_vertical_events)
    
    if (calc_vertical and not calc_horizontal):
      print(f"Calculating vertical events for batch_id: {batch_id}")
      df_vertical_events = calculate_vertical_crossing_events(sdf_input)
    
      # Cache
      df_vertical_events.cache()
    
      # Formatting
      df_events = df_vertical_events
      
    if (not calc_vertical and calc_horizontal): 
      print(f"Calculating horizontal events for batch_id: {batch_id}")
      df_horizontal_events = calculate_horizontal_segment_events(sdf_input)
      
      # Cache
      df_horizontal_events.cache()
      
      # Formatting
      df_events = df_horizontal_events
    
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
#fpath = 'logs/04_osn-flight_events-etl-day-by-day-log.parquet'
#if os.path.isfile(fpath):
#    processed_days = pd.read_parquet(fpath).days.to_list()
#else:
#    processed_days = []

# Loop through time range in daily batches, moving backwards
current_date = end_date
#while current_date >= start_date:        
#    previous_day_date = get_previous_day(current_date)
#    previous_day_timestamp = int(previous_day_date.timestamp())
#    
#    print(f'Currently processing {previous_day_date} - {current_date}')
#    if current_date in processed_days:
#        print('Already processed')
#        current_date = previous_day_date
#        continue
#    
#    # Perform the ETL operation for the current day
#    sdf_input = spark.sql(f"""
#        SELECT track_id, icao24, callsign, lat, lon, event_time, baro_altitude, velocity, vert_rate, cumulative_distance_nm
#        FROM `{project}`.`osn_tracks_clustered`
#        WHERE track_id IN (
#            SELECT track_id
#            FROM `{project}`.`osn_tracks_clustered`
#            GROUP BY track_id
#            HAVING MIN(event_time) BETWEEN {previous_day_timestamp} AND {int(current_date.timestamp())}
#        )""").cache()
#
#    etl_flight_events_and_measures(sdf_input, batch_id=previous_day_date.strftime("%Y%m%d") + '_')
#    
#    processed_days.append(current_date)
#    processed_days_df = pd.DataFrame({'days':processed_days})
#    processed_days_df.to_parquet(fpath)
#    
#    # Move to the previous day
#    current_date = previous_day_date

    
# Custom functions
from datetime import datetime, date
import dateutil.relativedelta
import calendar

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


## Montly processing test
# Custom functions
from datetime import datetime, date
import dateutil.relativedelta
import calendar

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
    df = spark.table(table_name)
    
    if unix_time == True:
        # Filter records based on event_time posix column
        filtered_df = df.filter((col(time_col) >= start_posix) & (col(time_col) < stop_posix))
    else: 
        df = df.withColumn('unix_time', unix_timestamp(time_col))
        filtered_df = df.filter((col('unix_time') >= start_posix) & ((col('unix_time') < stop_posix)))
                                
    return filtered_df
  
  
# Actual processing
start_month = date(2022, 1, 1)
end_month = date(2024, 7, 1)

to_process_months = generate_months(start_month, end_month)
to_process_months.reverse()

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


# Process loops
for month in to_process_months:
    print(f'Processing flight events for month: {month}')
    calc_horizontal = True
    calc_vertical = True
    if month in processed_months_horizontal:
        print('Month horizontal processed already (fuzzy labelling)')
        calc_horizontal = False
    
    if month in processed_months_vertical:
        print('Month vertical processed already (FLs)')
        calc_vertical = False
        
    if (not calc_horizontal and not calc_vertical):
        print('Month vertical and horizontal already processed..')
        continue
    
    else:
        # Perform the ETL operation for the current day
        sdf_input = get_data_within_timeframe( # State Vectors sv
            spark = spark, 
            table_name = f'{project}.osn_tracks_clustered', 
            month = month).cache()

        etl_flight_events_and_measures(sdf_input, batch_id=month.strftime("%Y%m%d") + '_', 
                                       calc_vertical = calc_vertical, calc_horizontal = calc_horizontal)
        
        ## Logging
        if calc_horizontal:
          processed_months_horizontal.append(month)
          processed_df = pd.DataFrame({'months':processed_months_horizontal})
          processed_df.to_parquet(fpath_horizontal)
        if calc_vertical:
          processed_months_vertical.append(month)
          processed_df = pd.DataFrame({'months':processed_months_vertical})
          processed_df.to_parquet(fpath_vertical)
# Cleanup
# Uncomment the following line if you want to drop the table
# spark.sql(f"""DROP TABLE IF EXISTS `{project}`.`osn_tracks`;""")

# Stop the SparkSession
spark.stop()