from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from datetime import datetime
import h3_pyspark

# Settings
project = "project_aiu"
max_h3_resolution = 12

# Getting today's date
today = datetime.today().strftime('%d %B %Y')

# Spark Session Initialization
spark = SparkSession.builder \
    .appName("OSN statevectors H3 addition") \
    .config("spark.log.level", "ERROR")\
    .config("spark.ui.showConsoleProgress", "false")\
    .config("spark.hadoop.fs.azure.ext.cab.required.group", "eur-app-aiu-dev") \
    .config("spark.kerberos.access.hadoopFileSystems", "abfs://storage-fs@cdpdldev0.dfs.core.windows.net/data/project/aiu.db/unmanaged") \
    .config("spark.driver.cores", "1") \
    .config("spark.driver.memory", "8G") \
    .config("spark.executor.memory", "6G") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "12") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "400s") \
    .enableHiveSupport() \
    .getOrCreate()

## Read raw data 
df = spark.sql(f"SELECT * FROM `{project}`.`osn_statevectors_clustered`;")

# Create OSN tracks db
h3_resolution = max_h3_resolution
h3_res_sql = ""
h3_res = []

while h3_resolution >= 0:
    if h3_resolution != 0:
        h3_res_sql = h3_res_sql + f"h3_res_{h3_resolution} STRING COMMENT 'H3 cell identifier for lat and lon with H3 resolution {h3_resolution}.',"
    else: 
        h3_res_sql = h3_res_sql + f"h3_res_{h3_resolution} STRING COMMENT 'H3 cell identifier for lat and lon with H3 resolution {h3_resolution}.'"
    h3_res.append(f"h3_res_{h3_resolution}")
    h3_resolution = h3_resolution - 1

h3_res_str = ', '.join(h3_res)

create_osn_tracks_sql = f"""
    CREATE TABLE IF NOT EXISTS `{project}`.`osn_h3_statevectors` (
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
        serials ARRAY<INT> COMMENT 'The serials column is a list of serials of the ADS-B receivers which received the message.',
        {h3_res_sql}
    )
    COMMENT '`{project}`.`osn_statevectors_clustered` with added H3 tags (generated based on callsign, icao24 grouping with 30 min signal gap intolerance). Last updated: {today}.'
    STORED AS parquet
    TBLPROPERTIES ('transactional'='false');
"""

spark.sql(f"DROP TABLE IF EXISTS `{project}`.`osn_h3_statevectors`;")
spark.sql(create_osn_tracks_sql)

# Save the original column names for later and add track_id
original_columns = df.columns + h3_res

h3_resolution = max_h3_resolution

while h3_resolution >= 0:
    if h3_resolution == max_h3_resolution:
        df = df.withColumn("h3_resolution", lit(h3_resolution))
        df = df.withColumn(f"h3_res_{h3_resolution}", h3_pyspark.geo_to_h3('lat', 'lon', 'h3_resolution'))
    else: 
        df = df.withColumn("h3_resolution", lit(h3_resolution))
        df = df.withColumn(f"h3_res_{h3_resolution}", h3_pyspark.h3_to_parent(F.col(f"h3_res_{max_h3_resolution}"), F.lit(h3_resolution)))

    h3_resolution = h3_resolution - 1

# Drop unnecessary and additional columns to retain only the original ones
df = df.select(original_columns)

# Write data for the month to the database
df.write.mode("append").insertInto(f"`{project}`.`osn_h3_statevectors`")