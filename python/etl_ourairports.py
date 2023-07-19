from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType
from pyspark.sql import SparkSession

# Set the SparkSession as provided
spark = SparkSession\
    .builder\
    .appName("project_aiu")\
    .config("spark.hadoop.fs.azure.ext.cab.required.group","eur-app-aiu-dev")\
    .config("spark.yarn.access.hadoopFileSystems","abfs://storage-fs@cdpdldev0.dfs.core.windows.net/")\
    .config("spark.driver.cores","1")\
    .config("spark.driver.memory","8G")\
    .config("spark.executor.memory","5G")\
    .config("spark.executor.cores","1")\
    .config("spark.executor.instances","2")\
    .config("spark.dynamicAllocation.maxExecutors", "6")\
    .getOrCreate()

# AIRPORTS

schema_airports = StructType([
    StructField("id", IntegerType(), True),
    StructField("ident", StringType(), True),
    StructField("type", StringType(), True),
    StructField("name", StringType(), True),
    StructField("latitude_deg", DoubleType(), True),
    StructField("longitude_deg", DoubleType(), True),
    StructField("elevation_ft", IntegerType(), True),
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

df_airports = spark.read.csv("https://ourairports.com/data/airports.csv", header=True, schema=schema_airports)
df_airports.write.mode('overwrite').saveAsTable("ao_airports")

# RUNWAYS

schema_runways = StructType([
    StructField("id", IntegerType(), True),
    StructField("airport_ref", IntegerType(), True),
    StructField("airport_ident", StringType(), True),
    StructField("length_ft", IntegerType(), True),
    StructField("width_ft", IntegerType(), True),
    StructField("surface", StringType(), True),
    StructField("lighted", BooleanType(), True),
    StructField("closed", BooleanType(), True),
    StructField("le_ident", StringType(), True),
    StructField("le_latitude_deg", DoubleType(), True),
    StructField("le_longitude_deg", DoubleType(), True),
    StructField("le_elevation_ft", IntegerType(), True),
    StructField("le_heading_degT", DoubleType(), True),
    StructField("le_displaced_threshold_ft", IntegerType(), True),
    StructField("he_ident", StringType(), True),
    StructField("he_latitude_deg", DoubleType(), True),
    StructField("he_longitude_deg", DoubleType(), True),
    StructField("he_elevation_ft", IntegerType(), True),
    StructField("he_heading_degT", DoubleType(), True),
    StructField("he_displaced_threshold_ft", IntegerType(), True),
])

df_runways = spark.read.csv("https://ourairports.com/data/runways.csv", header=True, schema=schema_runways)
df_runways.write.mode('overwrite').saveAsTable("oa_runways")

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
    StructField("elevation_ft", IntegerType(), True),
    StructField("iso_country", StringType(), True),
    StructField("dme_frequency_khz", IntegerType(), True),
    StructField("dme_channel", StringType(), True),
    StructField("dme_latitude_deg", DoubleType(), True),
    StructField("dme_longitude_deg", DoubleType(), True),
    StructField("dme_elevation_ft", IntegerType(), True),
    StructField("slaved_variation_deg", DoubleType(), True),
    StructField("magnetic_variation_deg", DoubleType(), True),
    StructField("usageType", StringType(), True),
    StructField("power", StringType(), True),
    StructField("associated_airport", StringType(), True)
])

df_navaids = spark.read.csv("https://ourairports.com/data/navaids.csv", header=True, schema=schema_navaids)
df_navaids.write.mode('overwrite').saveAsTable("oa_navaids")

# AIRPORT FREQUENCIES

schema_airport_frequencies = StructType([
    StructField("id", IntegerType(), True),
    StructField("airport_ref", IntegerType(), True),
    StructField("airport_ident", StringType(), True),
    StructField("type", StringType(), True),
    StructField("description", StringType(), True),
    StructField("frequency_mhz", DoubleType(), True),
])

df_airport_frequencies = spark.read.csv("https://ourairports.com/data/airport_frequencies.csv", header=True, schema=schema_airport_frequencies)
df_airport_frequencies.write.mode('overwrite').saveAsTable("oa_airport_frequencies")

# COUNTRIES

schema_countries = StructType([
    StructField("id", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", StringType(), True),
    StructField("continent", StringType(), True),
    StructField("wikipedia_link", StringType(), True),
    StructField("keywords", StringType(), True),
])

df_countries = spark.read.csv("https://ourairports.com/data/countries.csv", header=True, schema=schema_countries)
df_countries.write.mode('overwrite').saveAsTable("oa_countries")

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

df_regions = spark.read.csv("https://ourairports.com/data/regions.csv", header=True, schema=schema_regions)
df_regions.write.mode('overwrite').saveAsTable("oa_regions")

spark.stop()