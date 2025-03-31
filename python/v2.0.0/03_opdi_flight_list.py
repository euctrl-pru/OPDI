!pip install h3==3.7.4
!pip install h3_pyspark
!pip install h3pandas 

# Spark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, pandas_udf, col, PandasUDFType, lit, round, array_contains, from_unixtime
from pyspark.sql.functions import col, radians, sin, cos, sqrt, atan2, array, collect_list, struct, row_number, expr
from pyspark.sql.functions import monotonically_increasing_id, row_number, col
from pyspark.sql.types import DoubleType, StructType, StructField
from pyspark.sql.functions import when, broadcast, split, col, concat_ws,  min, max, to_date, unix_timestamp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Regular imports
from IPython.display import display, HTML
import os, time
import subprocess
import os,shutil
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import h3pandas
import h3

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
    """
    Return the Unix timestamp for the first and last second of the given month and year.

    Args:
        date (datetime): A datetime object representing any date within the desired month.

    Returns:
        tuple: A tuple containing the Unix timestamp of the first second and last second of the month.
    """
    year = date.year
    month = date.month
    
    # Calculate first and last second of the month
    first_second = datetime(year, month, 1, 0, 0, 0)
    last_day = calendar.monthrange(year, month)[1]
    last_second = datetime(year, month, last_day, 23, 59, 59)
    
    return first_second.timestamp(), last_second.timestamp()

# Settings
project = "project_opdi"
resolution = 7

start_month = date(2022, 1, 1)
end_month = date(2025, 3, 1)

# Getting today's date
today = datetime.today().strftime('%d %B %Y')

# Spark Session Initialization
spark = SparkSession.builder \
    .appName("OPDI Flight Table") \
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
    .config("spark.executor.memory", "8G") \
    .config("spark.executor.memoryOverhead", "3G") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "3") \
    .config("spark.dynamicAllocation.maxExecutors", "20") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "400s") \
    .config("spark.driver.maxResultSize", "6g") \
    .config("spark.shuffle.compress", "true") \
    .config("spark.shuffle.spill.compress", "true") \
    .enableHiveSupport() \
    .getOrCreate()

## Query testing sample function

from pyspark.sql.functions import col, lit, from_unixtime, to_timestamp

def get_data_within_timeframe(spark, table_name, month, time_col='event_time'):
    """
    Retrieves records from a specified Spark table within the given timeframe.

    Args:
        spark (SparkSession): The SparkSession object.
        table_name (str): The name of the Spark table to query.
        month (str): The start date of a month in the format 'YYYY-MM-DD'.
        time_col (str): The column name containing timestamp data (default: 'event_time').

    Returns:
        pyspark.sql.dataframe.DataFrame: A DataFrame containing the records within the specified timeframe.
    """
    # Convert the start and end of the month to Unix timestamps
    start_date, end_date = get_start_end_of_month(month)

    # Convert Unix timestamps to Spark timestamp format
    start_date_ts = to_timestamp(lit(start_date))
    end_date_ts = to_timestamp(lit(end_date))

    # Load the table
    df = spark.table(table_name)

    # Filter records based on the timestamp column
    filtered_df = df.filter((col(time_col) >= start_date_ts) & (col(time_col) < end_date_ts))

    return filtered_df


def load_airports_hex(spark, resolution = 7):
    """
    Creates the reference spark dataframe for airports which contains hexaero grids around each airport. 
    The hexaero grid around the airport has a 30NM radius. 

    Args:
    spark (SparkSession): The SparkSession object.
    resolution: The h3 resolution we want to work at. 

    Returns:
    pyspark.sql.dataframe.DataFrame: A DataFrame containing the hexaero grid for all european airports.
    """
    try:
        # In case I ran the function this dataset should exist.
        df_apt = pd.read_parquet(f'data/airport_hex/airport_concentric_c_hex_res_{resolution}_processed.arrow')
        sdf_apt = spark.createDataFrame(df_apt.to_dict(orient='records'))
        return sdf_apt
    
    except Exception as e:
        print(e)
        
        # Create the dataset if not exists from source
        df_apt = pd.read_parquet(f'data/airport_hex/airport_concentric_c_hex_res_{resolution}.arrow')
        
        # Filter out relevant hexagons
        df_apt = df_apt[df_apt.max_c_radius_nm<=30]
        df_apt = df_apt[['ident','hex_id', 'latitude_deg', 'longitude_deg']].explode('hex_id')
        df_apt = df_apt[~df_apt.hex_id.isna()]

        # Getting h3 positions of hexagons 
        df_apt['geo'] = df_apt['hex_id'].apply(lambda l:h3.h3_to_geo(l))
        df_apt['lat'] = df_apt['geo'].apply(lambda l:l[0])
        df_apt['lon'] = df_apt['geo'].apply(lambda l:l[1])
        df_apt = df_apt.drop('geo',axis=1)

        # OSN bounding box filter for europe
        f_lat = np.logical_and(df_apt.lat >=26.74617, df_apt.lat <= 70.25976)  
        f_lon = np.logical_and(df_apt.lon >=-25.86653, df_apt.lon <= 49.65699)  
        f = np.logical_and(f_lat, f_lon)
        df_apt = df_apt[f]

        # Add center hex ids
        df_apt['center_hex_id'] = df_apt.apply(lambda l: h3.geo_to_h3(l['latitude_deg'], l['longitude_deg'], resolution=resolution), axis=1)

        # Renaming columns 
        df_apt.columns = ['apt_' + x for x in df_apt.columns]

        def calc_dist(h1,h2):
            try:
                return h3.h3_distance(h1,h2)
            except:
                return None

        df_apt['distance_from_center'] = df_apt.apply(lambda l:calc_dist(l['apt_hex_id'], l['apt_center_hex_id']), axis=1)
        df_apt = df_apt[~pd.isnull(df_apt['distance_from_center'])]
        df_apt = df_apt[['apt_ident', 'apt_hex_id', 'distance_from_center', 'apt_latitude_deg', 'apt_longitude_deg']]
        df_apt.to_parquet(f'data/airport_hex/airport_concentric_c_hex_res_{resolution}_processed.arrow')

        # Creating spark df
        sdf_apt = spark.createDataFrame(df_apt.to_dict(orient='records'))
        return sdf_apt

def fetch_and_label_sv(spark, project, month, sdf_apt):
    """
    A function to 
        1. Fetch the statevector data (including track_ids) 
        2. Rename columns & complete flight_ids with ''.
        3. Add flight levels from baroaltitude.
        4. Add first_seen, last_seen, DOF
        5. Filter on height (below 40NM). 
        6. Merge airports in vicinity (within 30NM of OurAirports airport reference point - i.e., latitude_deg, longitude_deg). 
    
    Reasoning: 
    For an incoming aircraft, landing in the center of this cylinder would be at an angle of 3 degrees. 
    Assuming we want to see - at least - the flight coming down from FL10 to see the landing, we need thus to have a minimal distance tan(3) = 3048/? => ? = 3048/tan(3) = 58159 meter = 31,403348 nautical miles. 
    For outgoing landing, the take-off angle is much steeper, up to 15 degrees. Let's say we thus look up to FL40. We'll thus look into concentric circles 30NM around the airport and a max flight level FL40 (which covers higher outgoing angles). 

    Args:
    spark (SparkSession): The SparkSession object.
    project (str): The project in which we are working (usually 'project_opdi')
    month: The first date of the month we want to process (first of january 2023 will process the timeframe 2023-01-01 00:00:00 until 2023-01-31 23:59:59). 

    Returns:
    pyspark.sql.dataframe.DataFrame: A DataFrame containing the statevectors within the bounds (30NM around airports and below FL40) which are labelled by airport.
    """
    
    # Custom setting, see reasoning above.
    max_FL = 40
    
    # Fetch data
    sv = get_data_within_timeframe( # State Vectors sv
        spark = spark, 
        table_name = f'{project}.osn_tracks', 
        month = month)

    # Filter out rows with missing crucial data
    sv_f = sv.dropna(subset=['lat', 'lon', 'baro_altitude', 'track_id'])

    # Rename callsign to flight_id (official term)
    sv_f = sv_f.withColumnRenamed("callsign", "flight_id")

    # Replace missing 'flight_id' with empty string
    sv_f = sv_f.fillna({'flight_id': ''})

    # Convert event_time from posixtime to datetime 
    sv_f = sv_f.withColumn('event_time', F.to_timestamp(F.col('event_time')))

    # Add flight level proxy extracted from baro_altitude
    sv_f = sv_f.withColumn('flight_level', (col('baro_altitude') * 3.28084 / 100).cast('int'))

    # Select columns of interest
    columns_of_interest = [
        'track_id', 'icao24', 'flight_id', 'event_time', 'lat', 'lon',  'flight_level', 'baro_altitude',
        'heading', 'vert_rate',  'on_ground', 'h3_res_7'
    ]
    sv_f = sv_f.select(columns_of_interest)

    # Calculate the DOF and first_seen & last_seen for each track
    window_spec_track = Window.partitionBy('track_id')
    sv_f = sv_f.withColumn('first_seen', min('event_time').over(window_spec_track))
    sv_f = sv_f.withColumn('last_seen', max('event_time').over(window_spec_track))
    sv_f = sv_f.withColumn('DOF', to_date('first_seen'))

    # Filter out low altitude statevectors during landing/take-off (below FL20)
    # Implicitly we filter out any flights which are never noted below FL20
    sv_low_alt = sv_f.filter(col('flight_level') <= max_FL)

    # Merge airports hexaero grid (sdf_apt) onto statevectors to see which tracks are within 30 NM (+-h3 res 7 inaccuracy). 
    sv_nearby_apt = sv_low_alt.join(sdf_apt, (sv_low_alt.h3_res_7 == sdf_apt.apt_hex_id), "left")
    return sv_nearby_apt

# Determine landing or take-off depending on vert_rate
def categorize_landing_take_off(df):
    """
    Categorize the flight into 'take-off', 'landing', or 'ambiguous'.
    
    Args:
    df (spark dataframe): The spark dataframe coming out of fetch_and_label_sv.

    Returns:
    pyspark.sql.dataframe.DataFrame: The original dataframe with in addition a column 'status' indicating landing/take-off/ambiguous.
    """
        
    
    # Window specification for rolling average
    window_spec = Window.partitionBy(['icao24', 'flight_id', 'track_id', 'apt_ident']).orderBy('event_time').rowsBetween(-2, 2)

    # Calculate moving average of altitude
    df_m = df.withColumn('smoothed_altitude', F.avg('baro_altitude').over(window_spec))

    # Window specification for calculating altitude change
    window_spec_lag = Window.partitionBy(['icao24', 'flight_id', 'track_id', 'apt_ident']).orderBy('event_time')

    # Calculate change in smoothed altitude
    df_m = df_m.withColumn('altitude_change', F.col('smoothed_altitude') - F.lag('smoothed_altitude').over(window_spec_lag))

    # Determine if it's a take-off or landing
    df_m = df_m.withColumn('trajectory_type', 
                       F.when(F.col('altitude_change') > 0, 'take-off')
                        .when(F.col('altitude_change') < 0, 'landing')
                        .otherwise('constant altitude'))

    # Aggregate each track to a single 'flight' to determine overall flight type
    flight_type_df = df_m.groupBy(['icao24', 'flight_id', 'track_id', 'apt_ident']).agg(
        F.sum(F.when(F.col('trajectory_type') == 'take-off', 1).otherwise(0)).alias('take_off_count'),
        F.sum(F.when(F.col('trajectory_type') == 'landing', 1).otherwise(0)).alias('landing_count')
    )

    # Determine overall flight type based on majority segments including ambiguous count
    flight_type_df = flight_type_df.withColumn('status', 
                                               # We get statevectors every 5 sec -> +4 requires at least 20 s in one state
                                               F.when(F.col('take_off_count') > (F.col('landing_count') + 4), 'take-off')  
                                              .when(F.col('landing_count') > (F.col('take_off_count') + 4), 'landing')
                                              .otherwise('ambiguous'))
    
    return df.join(flight_type_df, on=['icao24', 'flight_id', 'track_id', 'apt_ident'], how='left')



def compute_flight_table(df):
    """
    Create the flight table .
    
    Args:
    df (spark dataframe): The spark dataframe coming out of categorize_landing_take_off.

    Returns:
    pyspark.sql.dataframe.DataFrame: The flight table as specified in opdi.aero.
    """
    
    # Filter out ambiguous landings/take-offs from flights (likely helicopters / go-arounds)
    df = df.filter(df.status != "ambiguous")
    
    # Calculate the statevector with the minimal distance from the airport center hex id
    window_spec = Window.partitionBy(['icao24', 'flight_id', 'track_id', 'status'])
    df = df.withColumn('min_distance', F.min('distance_from_center').over(window_spec))
    
    df_min_distance = df.filter(df.distance_from_center == df.min_distance)
    
    df_min_distance = df_min_distance.select(
        [
            'icao24', 'flight_id', 'track_id', 
            'apt_ident', 'apt_longitude_deg', 'apt_latitude_deg', 'DOF','first_seen', 
            'last_seen', 'status', 'event_time', 'lat', 'lon', 
            'min_distance', 'take_off_count', 'landing_count'])
    
    window_spec = Window.partitionBy(['icao24', 'flight_id', 'track_id', 'apt_ident', 'status'])
    df_min_distance = df_min_distance.withColumn(
        'min_time', F.min('event_time').over(window_spec)).withColumn(
        'max_time', F.max('event_time').over(window_spec))
    
    df_take_off = df_min_distance.filter((F.col('status') == F.lit('take-off')) & (F.col('event_time') == F.col('min_time')))
    df_landing = df_min_distance.filter((F.col('status') == F.lit('landing')) & (F.col('event_time') == F.col('max_time')))
    df_ambiguous = df_min_distance.filter((F.col('status') == F.lit('ambiguous')) & (F.col('event_time') == F.col('max_time')))
    
    flight_table = df_take_off.union(df_landing).union(df_ambiguous)
    
    # Sometimes there's still multiple airports for a landing / take-off 
    # In this case we'll select the airport with the minimal distance in NM  

    # Radius of the Earth in km
    R = 6371.0

    # Haversine formula implemented in PySpark
    flight_table = flight_table.withColumn("lat1", radians(col("lat"))) \
           .withColumn("lon1", radians(col("lon"))) \
           .withColumn("lat2", radians(col("apt_latitude_deg"))) \
           .withColumn("lon2", radians(col("apt_longitude_deg"))) \
           .withColumn("dlat", col("lat2") - col("lat1")) \
           .withColumn("dlon", col("lon2") - col("lon1")) \
           .withColumn("a", sin(col("dlat") / 2) ** 2 + cos(col("lat1")) * cos(col("lat2")) * sin(col("dlon") / 2) ** 2) \
           .withColumn("c", 2 * atan2(sqrt(col("a")), sqrt(1 - col("a")))) \
           .withColumn("distance_km", R * col("c"))

    # Define the key columns
    key_columns = ['icao24', 'flight_id', 'track_id', 'status', 'first_seen', 'last_seen']

    # Window specification to find the closest airport
    windowSpec = Window.partitionBy(key_columns).orderBy(col("distance_km"))

    # Add row numbers to identify the closest airport
    df_with_row_numbers = flight_table.withColumn("row_number", row_number().over(windowSpec))

    # Filter out the most likely airport and keep the rest as potential airports
    filtered_df = df_with_row_numbers.withColumn("is_most_likely", col("row_number") == 1)

    # Aggregate to find the most likely airport and collect potential airports
    result_df = filtered_df.groupBy(key_columns) \
        .agg(
            expr("first(apt_ident) as most_likely_airport"),
            collect_list(expr("case when not is_most_likely then apt_ident end")).alias("potential_airports")
        )

    # Select the final columns to keep
    result_df = result_df.select(
        *key_columns,
        col("most_likely_airport"),
        col("potential_airports")
    )
    
    
    # Filter take-offs and landings
    take_offs = result_df.filter(col('status') == 'take-off')
    landings = result_df.filter(col('status') == 'landing')

    # Rename columns
    take_offs = take_offs.withColumnRenamed('most_likely_airport', 'ADEP')\
                         .withColumnRenamed('potential_airports', 'ADEP_P')

    landings = landings.withColumnRenamed('most_likely_airport', 'ADES')\
                       .withColumnRenamed('potential_airports', 'ADES_P')

    # Define key columns
    key_cols = ['icao24', 'flight_id', 'track_id', 'first_seen', 'last_seen']

    # Merge the DataFrames
    flight_table = take_offs.drop('status')\
                            .join(landings.drop('status'), on=key_cols, how='outer')
    
    # Add a new column 'DOF' which is the date part of 'first_seen'
    flight_table = flight_table.withColumn('DOF', to_date(col('first_seen')))
    
    # Rename
    flight_table = flight_table.withColumnRenamed('track_id', 'id')\
                                .withColumnRenamed('icao24', 'ICAO24')\
                                .withColumnRenamed('flight_id', 'FLT_ID')
    
    # Add version
    flight_table = flight_table.withColumn('version', F.lit('v2.0.0'))
    
    # Concatenate ADEP_P and ADES_P
    flight_table = flight_table.withColumn("ADEP_P", concat_ws(", ", col("ADEP_P")))
    flight_table = flight_table.withColumn("ADES_P", concat_ws(", ", col("ADES_P")))
    
    # Final cleaning
    flight_table = flight_table.select(
        'id',
        'ADEP', 
        'ADES', 
        'ADEP_P', 
        'ADES_P', 
        'ICAO24', 
        'FLT_ID', 
        'first_seen', 
        'last_seen', 
        'DOF', 
        'version')
    return(flight_table)


def add_osn_aircraft_db_data(spark, flight_table):
    """
    Join flight_table with osn_aircraft_db and resolve ambiguous column names.

    Args:
    spark (SparkSession): The SparkSession object.
    flight_table (DataFrame): The flight table DataFrame.

    Returns:
    DataFrame: A merged DataFrame with osn_aircraft_db data and resolved column ambiguity.
    """
    # Load the `osn_aircraft_db` table
    osn_aircraft_db = spark.table("project_opdi.osn_aircraft_db")

    # Perform the join operation with explicit table prefix for ambiguous columns
    merged_table = flight_table.alias("ft").join(
        osn_aircraft_db.alias("adb"),
        col("ft.ICAO24") == col("adb.icao24"),
        "left"
    )

    # Convert all column names to uppercase and avoid ambiguity
    merged_table_uppercase = merged_table.select(
        *[col(f"ft.{c}").alias(c.upper()) for c in flight_table.columns],
        *[col(f"adb.{c}").alias(c.upper()) for c in osn_aircraft_db.columns if c != "icao24"]
    )

    # Select relevant columns for the final table
    final_table = merged_table_uppercase.select(
        "ID", 
        "ICAO24", 
        "FLT_ID", 
        "DOF", 
        "ADEP", 
        "ADES", 
        "ADEP_P", 
        "ADES_P", 
        "REGISTRATION", 
        "MODEL", 
        "TYPECODE", 
        "ICAO_AIRCRAFT_CLASS", 
        "ICAO_OPERATOR", 
        "FIRST_SEEN", 
        "LAST_SEEN", 
        "VERSION"
    )

    return final_table



def process_flight_table_DAI(spark, project, month):
    
    # Process Departures / Arrivals / Internal flights in the bbox

    sdf_apt = load_airports_hex(spark, resolution = 7)

    sv_nearby_apt = fetch_and_label_sv(spark, project, month, sdf_apt)

    sv_nearby_apt = categorize_landing_take_off(sv_nearby_apt)

    flight_table = compute_flight_table(sv_nearby_apt)

    flight_table = add_osn_aircraft_db_data(spark, flight_table)

    # Prep for insert
    flight_table = flight_table.withColumn("DOF_day", to_date(col("DOF")))
    flight_table = flight_table.repartition("DOF_day").orderBy("DOF_day")
    
    # Drop event_time_day before writing
    flight_table = flight_table.drop("DOF_day")
    
    # Write data for the month to the database
    flight_table.writeTo(f"`{project}`.`opdi_flight_list`").append()
    
    
# Overfligths

def fetch_and_aggregate_sv_overflight(spark, project, month):
    
    # Fetch statevector data
    sv = get_data_within_timeframe(
        spark=spark,
        table_name=f'{project}.osn_tracks',
        month=month
    )
    
    # Fetch established flight table data
    fl = get_data_within_timeframe(
        spark=spark,
        table_name=f'{project}.opdi_flight_list',
        month=month,
        time_col='first_seen'
    ).select('id')

    # Define a window specification for track_id partitioning
    window_spec_track = Window.partitionBy('track_id')

    # Add necessary columns and transformations
    sv = sv.withColumn('event_time', F.to_timestamp(F.col('event_time')))
    sv = sv.withColumn('first_seen', min('event_time').over(window_spec_track))
    sv = sv.withColumn('last_seen', max('event_time').over(window_spec_track))

    # Filter to keep only the rows where first_seen equals event_time
    sv_f = sv.filter(col('first_seen') == col('event_time'))

    # Add the 'DOF' column and rename 'track_id' to 'id'
    sv_f = sv_f.withColumn('event_date', to_date('event_time'))
    sv_f = sv_f.withColumn('DOF', min('event_date').over(window_spec_track))
    sv_f = sv_f.withColumnRenamed('track_id', 'id')
    sv_f = sv_f.withColumnRenamed('icao24', 'ICAO24')
    sv_f = sv_f.withColumnRenamed('callsign', 'FLT_ID')

    # Add empty columns 'ADEP', 'ADES', 'ADEP_P', 'ADES_P', and a column 'version' with value 'v2.0.0'
    sv_f = sv_f.withColumn('ADEP', lit(None).cast('string'))
    sv_f = sv_f.withColumn('ADES', lit(None).cast('string'))
    sv_f = sv_f.withColumn('ADEP_P', lit(None).cast('string'))
    sv_f = sv_f.withColumn('ADES_P', lit(None).cast('string'))
    sv_f = sv_f.withColumn('version', lit('v2.0.0'))
    
    # Select columns
    sv_f = sv_f.select(
        'id',
        'ADEP', 
        'ADES', 
        'ADEP_P', 
        'ADES_P', 
        'ICAO24', 
        'FLT_ID', 
        'first_seen', 
        'last_seen', 
        'DOF', 
        'version')
    
    # Broadcast the fl dataframe
    fl_broadcast = broadcast(fl)
    
    # Perform the left anti join using the broadcasted fl dataframe
    sv_f = sv_f.join(fl_broadcast, sv_f.id == fl.id, "left_anti")
    
    # Filter out short ADS-B signals < 5 min - There's _a lot_ of garbage <1min points
    sv_f = sv_f.filter((unix_timestamp("last_seen") - unix_timestamp("first_seen")) >= 300)
    
    return sv_f
                                
                                  
def process_flight_table_O(spark, project, month):
    
    # Process overflights
    flight_table = fetch_and_aggregate_sv_overflight(spark, project, month)

    # Add osn aircraft db
    flight_table = add_osn_aircraft_db_data(spark, flight_table)

    # Prep for insert
    flight_table = flight_table.withColumn("DOF_day", to_date(col("DOF")))
    flight_table = flight_table.repartition("DOF_day").orderBy("DOF_day")
    
    # Drop event_time_day before writing
    flight_table = flight_table.drop("DOF_day")

    flight_table.writeTo(f"`{project}`.`opdi_flight_list`").append()


to_process_months = generate_months(start_month, end_month)

## Load logs
fpath_DAI = 'logs/03_osn-flight_table-etl-log.parquet'
if os.path.isfile(fpath_DAI):
    processed_months_DAI = pd.read_parquet(fpath_DAI).months.to_list()
else:
    processed_months_DAI = []

fpath_O = 'logs/03_osn-flight_table-overflights-etl-log.parquet'
if os.path.isfile(fpath_O):
    processed_months_O = pd.read_parquet(fpath_O).months.to_list()
else:
    processed_months_O = []
    
## Process loops
for month in to_process_months:
    print(f'Processing DAI month: {month}')
    if month in processed_months_DAI:
        print('Month DAI processed already')
        continue
    else:
        process_flight_table_DAI(spark, project, month)

        ## Logging
        processed_months_DAI.append(month)
        processed_DAI_df = pd.DataFrame({'months':processed_months_DAI})
        processed_DAI_df.to_parquet(fpath_DAI)

for month in to_process_months:
    print(f'Processing O month: {month}')
    if month in processed_months_O:
        print('Month O processed already')
        continue
    else:
        process_flight_table_O(spark, project, month)

        ## Logging
        processed_months_O.append(month)
        processed_O_df = pd.DataFrame({'months':processed_months_O})
        processed_O_df.to_parquet(fpath_O)

# Stop the SparkSession
spark.stop()