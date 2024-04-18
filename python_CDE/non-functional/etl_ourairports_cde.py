import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType
import os
from datetime import datetime

# Settings
today = datetime.today().strftime('%d %B %Y')

# Set the SparkSession as provided

spark = SparkSession\
    .builder\
    .getOrCreate()

# Create tables SQL 
oa_airport_frequencies_sql = f"""    
    CREATE TABLE `project_aiu`.`oa_airport_frequencies` (
      id INT COMMENT 'Internal OurAirports integer identifier for the frequency. This will stay persistent, even if the radio frequency or description changes.',
      airport_ref INT COMMENT 'Internal integer foreign key matching the id column for the associated airport in oa_airports.',
      airport_ident STRING COMMENT 'Externally-visible string foreign key matching the ident column for the associated airport in oa_airports.',
      type STRING COMMENT 'A code for the frequency type. Some common values are "TWR" (tower), "ATF" or "CTAF" (common traffic frequency), "GND" (ground control), "RMP" (ramp control), "ATIS" (automated weather), "RCO" (remote radio outlet), "ARR" (arrivals), "DEP" (departures), "UNICOM" (monitored ground station), and "RDO" (a flight-service station).',
      description STRING COMMENT 'A description of the frequency, typically the way a pilot would open a call on it.',
      frequency_mhz DOUBLE COMMENT 'Radio voice frequency in megahertz. Note that the same frequency may appear multiple times for an airport, serving different functions.'
    )
    COMMENT 'OurAirports airport frequencies data (for PRU). Last updated: {today}.' 
    STORED AS parquet
    TBLPROPERTIES ('transactional'='false');
"""

oa_airports_sql = f"""
    CREATE TABLE `project_aiu`.`oa_airports` (
      id INT COMMENT 'Internal OurAirports integer identifier for the airport. This will stay persistent, even if the airport code changes.',
      ident STRING COMMENT 'The text identifier used in the OurAirports URL. This will be the ICAO code if available. Otherwise, it will be a local airport code (if no conflict), or if nothing else is available, an internally-generated code starting with the ISO2 country code, followed by a dash and a four-digit number.',
      type STRING COMMENT 'The type of the airport. Allowed values are "closed_airport", "heliport", "large_airport", "medium_airport", "seaplane_base", and "small_airport".',
      name STRING COMMENT 'The official airport name, including "Airport", "Airstrip", etc.',
      latitude_deg DOUBLE COMMENT 'The airport latitude in decimal degrees (positive for north).',
      longitude_deg DOUBLE COMMENT 'The airport longitude in decimal degrees (positive for east).',
      elevation_ft DOUBLE COMMENT 'The airport elevation MSL in feet (not metres).',
      continent STRING COMMENT 'The code for the continent where the airport is (primarily) located.',
      iso_country STRING COMMENT 'The two-character ISO 3166:1-alpha2 code for the country where the airport is (primarily) located.',
      iso_region STRING COMMENT 'An alphanumeric code for the high-level administrative subdivision of a country where the airport is primarily located (e.g. province, governorate), prefixed by the ISO2 country code and a hyphen.',
      municipality STRING COMMENT 'The primary municipality that the airport serves (when available).',
      scheduled_service STRING COMMENT '"yes" if the airport currently has scheduled airline service; "no" otherwise.',
      gps_code STRING COMMENT 'The code that an aviation GPS database (such as Jeppesens or Garmins) would normally use for the airport. This will always be the ICAO code if one exists.',
      iata_code STRING COMMENT 'The three-letter IATA code for the airport (if it has one).',
      local_code STRING COMMENT 'The local country code for the airport, if different from the gps_code and iata_code fields (used mainly for US airports).',
      home_link STRING COMMENT 'URL of the airports official home page on the web, if one exists.',
      wikipedia_link STRING COMMENT 'URL of the airports page on Wikipedia, if one exists.',
      keywords STRING COMMENT 'Extra keywords/phrases to assist with search, comma-separated. May include former names for the airport, alternate codes, names in other languages, nearby tourist destinations, etc.'
    )
    COMMENT 'OurAirports airports data (for PRU). Last updated: {today}.'
    STORED AS parquet
    TBLPROPERTIES ('transactional'='false');
"""

oa_countries_sql = f"""
    CREATE TABLE `project_aiu`.`oa_countries` (
      id INT COMMENT 'Internal OurAirports integer identifier for the country. This will stay persistent, even if the country name or code changes.',
      code STRING COMMENT 'The two-character ISO 3166:1-alpha2 code for the country.',
      name STRING COMMENT 'The common English-language name for the country.',
      continent STRING COMMENT 'The code for the continent where the country is (primarily) located.',
      wikipedia_link STRING COMMENT 'Link to the Wikipedia article about the country.',
      keywords STRING COMMENT 'A comma-separated list of search keywords/phrases related to the country.'
    )
    COMMENT 'OurAirports airport countries data (for PRU). Last updated: {today}.'
    STORED AS parquet
    TBLPROPERTIES ('transactional'='false');
"""

oa_navaids_sql = f"""
    CREATE TABLE `project_aiu`.`oa_navaids` (
      id INT COMMENT 'Internal OurAirports integer identifier for the navaid. This will stay persistent, even if the navaid identifier or frequency changes.',
      filename STRING COMMENT 'Unique string identifier constructed from the navaid name and country, and used in the OurAirports URL.',
      ident STRING COMMENT 'The 1-3 character identifer that the navaid transmits.',
      name STRING COMMENT 'The name of the navaid, excluding its type.',
      type STRING COMMENT 'The type of the navaid.',
      frequency_khz INT COMMENT 'The frequency of the navaid in kilohertz.',
      latitude_deg DOUBLE COMMENT 'The latitude of the navaid in decimal degrees (negative for south).',
      longitude_deg DOUBLE COMMENT 'The longitude of the navaid in decimal degrees (negative for west).',
      elevation_ft DOUBLE COMMENT 'The navaids elevation MSL in feet (not metres).',
      iso_country STRING COMMENT 'The two-character ISO 3166:1-alpha2 code for the country that operates the navaid.',
      dme_frequency_khz DOUBLE COMMENT 'The paired VHF frequency for the DME (or TACAN) in kilohertz.',
      dme_channel STRING COMMENT 'The DME channel (an alternative way of tuning distance-measuring equipment).',
      dme_latitude_deg DOUBLE COMMENT 'The latitude of the associated DME in decimal degrees (negative for south). If missing, assume that the value is the same as latitude_deg.',
      dme_longitude_deg DOUBLE COMMENT 'The longitude of the associated DME in decimal degrees (negative for west). If missing, assume that the value is the same as longitude_deg.',
      dme_elevation_ft DOUBLE COMMENT 'The associated DME transmitters elevation MSL in feet. If missing, assume that its the same value as elevation_ft.',
      slaved_variation_deg DOUBLE COMMENT 'The magnetic variation adjustment built into a VORs, VOR-DMEs, or TACANs radials.',
      magnetic_variation_deg DOUBLE COMMENT 'The actual magnetic variation at the navaids location.',
      usageType STRING COMMENT 'The primary function of the navaid in the airspace system.',
      power STRING COMMENT 'The power-output level of the navaid.',
      associated_airport STRING COMMENT 'The OurAirports text identifier (usually the ICAO code) for an airport associated with the navaid.'
    )
    COMMENT 'OurAirports airport naivaids data (for PRU). Last updated: {today}.'
    STORED AS parquet
    TBLPROPERTIES ('transactional'='false');
"""

oa_regions_sql = f"""
    CREATE TABLE `project_aiu`.`oa_regions` (
      id INT COMMENT 'Internal OurAirports integer identifier for the region. This will stay persistent, even if the region code changes.',
      code STRING COMMENT 'Local_code prefixed with the country code to make a globally-unique identifier.',
      local_code STRING COMMENT 'The local code for the administrative subdivision.',
      name STRING COMMENT 'The common English-language name for the administrative subdivision.',
      continent STRING COMMENT 'A code for the continent to which the region belongs.',
      iso_country STRING COMMENT 'The two-character ISO 3166:1-alpha2 code for the country containing the administrative subdivision.',
      wikipedia_link STRING COMMENT 'A link to the Wikipedia article describing the subdivision.',
      keywords STRING COMMENT 'A comma-separated list of keywords to assist with search. May include former names for the region, and/or the region name in other languages.'
    )
    COMMENT 'OurAirports airport regions data (for PRU). Last updated: {today}.'
    STORED AS parquet
    TBLPROPERTIES ('transactional'='false');
"""

oa_runways_sql = f"""    
    CREATE TABLE `project_aiu`.`oa_runways` (
      id INT COMMENT 'Internal OurAirports integer identifier for the runway. This will stay persistent, even if the runway numbering changes.',
      airport_ref INT COMMENT 'Internal integer foreign key matching the id column for the associated airport in ao_airports.',
      airport_ident STRING COMMENT 'Externally-visible string foreign key matching the ident column for the associated airport in ao_airports.',
      length_ft DOUBLE COMMENT 'Length of the full runway surface (including displaced thresholds, overrun areas, etc) in feet.',
      width_ft DOUBLE COMMENT 'Width of the runway surface in feet.',
      surface STRING COMMENT 'Code for the runway surface type. Some common values include "ASP" (asphalt), "TURF" (turf), "CON" (concrete), "GRS" (grass), "GRE" (gravel), "WATER" (water), and "UNK" (unknown).',
      lighted BOOLEAN COMMENT '1 if the surface is lighted at night, 0 otherwise.',
      closed BOOLEAN COMMENT '1 if the runway surface is currently closed, 0 otherwise.',
      le_ident STRING COMMENT 'Identifier for the low-numbered end of the runway.',
      le_latitude_deg DOUBLE COMMENT 'Latitude of the centre of the low-numbered end of the runway, in decimal degrees (positive is north), if available.',
      le_longitude_deg DOUBLE COMMENT 'Longitude of the centre of the low-numbered end of the runway, in decimal degrees (positive is east), if available.',
      le_elevation_ft DOUBLE COMMENT 'Elevation above MSL of the low-numbered end of the runway in feet.',
      le_heading_degT DOUBLE COMMENT 'Heading of the low-numbered end of the runway in degrees true (not magnetic).',
      le_displaced_threshold_ft DOUBLE COMMENT 'Length of the displaced threshold (if any) for the low-numbered end of the runway, in feet.',
      he_ident STRING COMMENT 'Identifier for the high-numbered end of the runway.',
      he_latitude_deg DOUBLE COMMENT 'Latitude of the centre of the high-numbered end of the runway, in decimal degrees (positive is north), if available.',
      he_longitude_deg DOUBLE COMMENT 'Longitude of the centre of the high-numbered end of the runway, in decimal degrees (positive is east), if available.',
      he_elevation_ft DOUBLE COMMENT 'Elevation above MSL of the high-numbered end of the runway in feet.',
      he_heading_degT DOUBLE COMMENT 'Heading of the high-numbered end of the runway in degrees true (not magnetic).',
      he_displaced_threshold_ft DOUBLE COMMENT 'Length of the displaced threshold (if any) for the high-numbered end of the runway, in feet.'
    )
    COMMENT 'OurAirports airport runways data (for PRU). Last updated: {today}.'
    STORED AS parquet
    TBLPROPERTIES ('transactional'='false');
"""

# Drop old tables... 
spark.sql("DROP TABLE IF EXISTS `project_aiu`.`oa_airport_frequencies`;")
spark.sql("DROP TABLE IF EXISTS `project_aiu`.`oa_airports`;")
spark.sql("DROP TABLE IF EXISTS `project_aiu`.`oa_countries`;")
spark.sql("DROP TABLE IF EXISTS `project_aiu`.`oa_navaids`;")
spark.sql("DROP TABLE IF EXISTS `project_aiu`.`oa_regions`;")
spark.sql("DROP TABLE IF EXISTS `project_aiu`.`oa_runways`;")

# Create new tables...
spark.sql(oa_airport_frequencies_sql)
spark.sql(oa_airports_sql)
spark.sql(oa_countries_sql)
spark.sql(oa_navaids_sql)
spark.sql(oa_regions_sql)
spark.sql(oa_runways_sql)

# Define schemas for each dataset

schema_airports = StructType([
    StructField("id", IntegerType(), True),
    StructField("ident", StringType(), True),
    StructField("type", StringType(), True),
    StructField("name", StringType(), True),
    StructField("latitude_deg", DoubleType(), True),
    StructField("longitude_deg", DoubleType(), True),
    StructField("elevation_ft", DoubleType(), True),
    StructField("continent", StringType(), True),
    StructField("iso_country", StringType(), True),
    StructField("iso_region", StringType(), True),
    StructField("municipality", StringType(), True),
    StructField("scheduled_service", StringType(), True),
    StructField("gps_code", StringType(), True),
    StructField("iata_code", StringType(), True),
    StructField("local_code", StringType(), True),
    StructField("home_link", StringType(), True),
    StructField("wikipedia_link", StringType(), True),
    StructField("keywords", StringType(), True)
])

# RUNWAYS
schema_runways = StructType([
    StructField("id", IntegerType(), True),
    StructField("airport_ref", IntegerType(), True),
    StructField("airport_ident", StringType(), True),
    StructField("length_ft", DoubleType(), True),
    StructField("width_ft", DoubleType(), True),
    StructField("surface", StringType(), True),
    StructField("lighted", BooleanType(), True),
    StructField("closed", BooleanType(), True),
    StructField("le_ident", StringType(), True),
    StructField("le_latitude_deg", DoubleType(), True),
    StructField("le_longitude_deg", DoubleType(), True),
    StructField("le_elevation_ft", DoubleType(), True),
    StructField("le_heading_degT", DoubleType(), True),
    StructField("le_displaced_threshold_ft", DoubleType(), True),
    StructField("he_ident", StringType(), True),
    StructField("he_latitude_deg", DoubleType(), True),
    StructField("he_longitude_deg", DoubleType(), True),
    StructField("he_elevation_ft", DoubleType(), True),
    StructField("he_heading_degT", DoubleType(), True),
    StructField("he_displaced_threshold_ft", DoubleType(), True),
])


# NAVAIDS
schema_navaids = StructType([
    StructField("id", IntegerType(), True),
    StructField("filename", StringType(), True),
    StructField("ident", StringType(), True),
    StructField("name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("frequency_khz", IntegerType(), True),
    StructField("latitude_deg", DoubleType(), True),
    StructField("longitude_deg", DoubleType(), True),
    StructField("elevation_ft", DoubleType(), True),
    StructField("iso_country", StringType(), True),
    StructField("dme_frequency_khz", DoubleType(), True),
    StructField("dme_channel", StringType(), True),
    StructField("dme_latitude_deg", DoubleType(), True),
    StructField("dme_longitude_deg", DoubleType(), True),
    StructField("dme_elevation_ft", DoubleType(), True),
    StructField("slaved_variation_deg", DoubleType(), True),
    StructField("magnetic_variation_deg", DoubleType(), True),
    StructField("usageType", StringType(), True),
    StructField("power", StringType(), True),
    StructField("associated_airport", StringType(), True)
])

# AIRPORT FREQUENCIES
schema_airport_frequencies = StructType([
    StructField("id", IntegerType(), True),
    StructField("airport_ref", IntegerType(), True),
    StructField("airport_ident", StringType(), True),
    StructField("type", StringType(), True),
    StructField("description", StringType(), True),
    StructField("frequency_mhz", DoubleType(), True),
])

# COUNTRIES
schema_countries = StructType([
    StructField("id", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", StringType(), True),
    StructField("continent", StringType(), True),
    StructField("wikipedia_link", StringType(), True),
    StructField("keywords", StringType(), True),
])

# REGIONS
schema_regions = StructType([
    StructField("id", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("local_code", StringType(), True),
    StructField("name", StringType(), True),
    StructField("continent", StringType(), True),
    StructField("iso_country", StringType(), True),
    StructField("wikipedia_link", StringType(), True),
    StructField("keywords", StringType(), True),
])


schemas = {
    'airports': schema_airports,
    'runways': schema_runways,
    'navaids': schema_navaids,
    'airport_frequencies': schema_airport_frequencies,
    'countries': schema_countries,
    'regions': schema_regions
}

# Define URLs for each dataset
urls = {
    'airports': "https://ourairports.com/data/airports.csv",
    'runways': "https://ourairports.com/data/runways.csv",
    'navaids': "https://ourairports.com/data/navaids.csv",
    'airport_frequencies': "https://ourairports.com/data/airport-frequencies.csv",
    'countries': "https://ourairports.com/data/countries.csv",
    'regions': "https://ourairports.com/data/regions.csv"
}

# Download each dataset, create corresponding Spark DataFrames, and write them into Spark tables
for name, url in urls.items():
    print(f'Requesing  {name} via URL {url}')
    # Download the file from `url` and save it locally in memory under `pdf`
    pdf = pd.read_csv(url)

    if 'lighted' in pdf.columns:
        pdf['lighted'] = pdf['lighted'].apply(bool)

    if 'closed' in pdf.columns:
        pdf['closed'] = pdf['closed'].apply(bool)
    
    # Convert the Pandas DataFrame to a Spark DataFrame with the appropriate schema
    sdf = spark.createDataFrame(pdf, schema=schemas[name])

    # Write the DataFrame into a Spark table
    sdf.write.mode('overwrite').insertInto(f"project_aiu.oa_{name}")

spark.stop()