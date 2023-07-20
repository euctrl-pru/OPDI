import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType
import os

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

# Define schemas for each dataset
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

# Define local path where file will be downloaded
local_path = "data.csv"

# Download each dataset, create corresponding Spark DataFrames, and write them into Spark tables
for name, url in urls.items():
    # Download the file from `url` and save it locally under `local_path`
    urllib.request.urlretrieve(url, local_path)

    # Create a DataFrame from the downloaded CSV file
    df = spark.read.csv(local_path, header=True, schema=schemas[name])

    # Write the DataFrame into a Spark table
    df.write.mode('overwrite').insertInto(f"project_aiu.oa_{name}")

# Remove the downloaded file
os.remove(local_path)

spark.stop()
