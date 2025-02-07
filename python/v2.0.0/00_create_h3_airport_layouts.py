# Required imports
import os
import pandas as pd
from shapely.geometry import MultiPolygon, Polygon, LineString
from shapely.ops import transform
from pyproj import Transformer
import re
import h3
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType
import traceback
from pyspark.sql import SparkSession
import osmnx as ox


# Settings
project = "project_opdi"
resolution = 12

# Spark Session Initialization
spark = SparkSession.builder \
    .appName("HexAero Ground Layout generator") \
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


def retrieve_osm_data(icao_code):
    try:
        gdf = ox.features.features_from_place(
            f"{icao_code} Airport",
            tags={'aeroway': ['taxiway', 'runway', 'apron', 'hangar',
                              'threshold', 'parking_position', 'deicing_pad']}
        )
        if gdf.empty:
            raise ValueError(f"No data returned for {icao_code}")
        return gdf
    except Exception as e:
        print(f"Failed to retrieve OSM data for {icao_code}: {e}")
        raise

def fetch_airport_data():
    """
    Fetches airport data from the OurAirports dataset and filters it for large and medium airports.

    Returns:
    pd.DataFrame: Filtered DataFrame containing airport information.
    """
    airports_df = pd.read_csv('https://davidmegginson.github.io/ourairports-data/airports.csv')
    airports_df = airports_df[
        (airports_df['type'].isin(['large_airport', 'medium_airport']))
    ][['ident', 'latitude_deg', 'longitude_deg', 'elevation_ft', 'type']]
    print(f"There are {len(airports_df)} airports to process...")
    return airports_df

def buffer_geometry(line, width_m, always_xy=True):
    """
    Adds a buffer to a LineString to create a Polygon with a specified width in meters.

    Parameters:
    line (LineString): LineString to buffer.
    width_m (float): Buffer width in meters.
    always_xy (bool): Ensures transformer follows (x, y) coordinate order.

    Returns:
    Polygon: Buffered geometry as a Polygon.
    """
    transformer_to_meters = Transformer.from_crs("EPSG:4326", "EPSG:3395", always_xy=always_xy)
    transformer_to_degrees = Transformer.from_crs("EPSG:3395", "EPSG:4326", always_xy=always_xy)
    line_in_meters = transform(transformer_to_meters.transform, line)
    buffered_line = line_in_meters.buffer(width_m)
    return transform(transformer_to_degrees.transform, buffered_line)

def is_number(s):
    """
    Checks if the input string is a valid number.

    Parameters:
    s (str): Input string to check.

    Returns:
    bool: True if valid number, False otherwise.
    """
    try:
        if pd.isna(s):
            return False
        float(s)
        return True
    except ValueError:
        return False

def safe_convert_to_float(s):
    """
    Safely converts a string to a float.

    Parameters:
    s (str): String to convert.

    Returns:
    float or None: Float representation of the string, or None if conversion fails.
    """
    if is_number(s):
        return float(s)
    if pd.isna(s):
        return None
    numeric_part = re.sub(r"[^-0-9.]+", "", s)
    try:
        return float(numeric_part)
    except ValueError:
        return None

def fill_missing_width(aeroway, width_m):
    """
    Fills missing width values based on the aeroway type.

    Parameters:
    aeroway (str): Type of aeroway.
    width_m (float or None): Existing width value.

    Returns:
    float: Filled width value.
    """
    if pd.isnull(width_m):
        defaults = {
            'taxiway': 30,
            'runway': 45,
            'apron': 20,
            'hangar': 20,
            'threshold': 45,
            'parking_position': 25,
            'deicing_pad': 60
        }
        return defaults.get(aeroway, None)
    return safe_convert_to_float(width_m)



def convert_to_polygon(geometry, geom_type, width_m):
    """
    Converts a geometry to a Polygon, buffering it if necessary.

    Parameters:
    geometry: Input geometry (Polygon, MultiPolygon, LineString, or Point).
    geom_type (str): Geometry type.
    width_m (float): Buffer width.

    Returns:
    list: List of Polygons. For MultiPolygon, returns all polygons as a list.
    """
    if geom_type == 'Polygon':
        return [geometry]  # Return as a single-item list
    if geom_type == 'MultiPolygon':
        return [poly for poly in geometry.geoms]  # Return all polygons in MultiPolygon
    if geom_type in ['LineString', 'Point']:
        return [buffer_geometry(geometry, width_m, always_xy=True)]  # Buffer and return as list
    print(f"Geometry of type {geom_type} not supported.")
    return []


def polygon_to_h3(poly, resolution):
    """
    Converts a Polygon to a set of H3 indices.

    Parameters:
    poly (Polygon): Polygon to convert.
    resolution (int): H3 resolution.

    Returns:
    set: H3 indices covering the Polygon.
    """
    exterior_coords = [(lat, lon) for lon, lat in poly.exterior.coords]
    geojson_polygon = {"type": "Polygon", "coordinates": [exterior_coords]}
    return h3.polyfill(geojson_polygon, resolution)

def clean_str(s):
    """
    Cleans a string and converts units (ft/mi) to meters.

    Parameters:
    s (str): Input string.

    Returns:
    str: Cleaned and converted string.
    """
    try:
        if 'ft' in s:
            result = float(re.sub(r'[^0-9.]', '', s)) * 0.3048
            return str(result)
        if 'mi' in s:
            result = float(re.sub(r'[^0-9.]', '', s)) * 1609.34
            return str(result)
        return float(re.sub(r'[^0-9.]', '', s))
    except Exception:
        return s

def hexagonify_airport(apt_icao, resolution=12):
    """
    Processes an airport by fetching its OSM data, creating polygons for its features,
    and converting them to H3 hexagons.

    Parameters:
    apt_icao (str): ICAO code of the airport.
    resolution (int): H3 resolution.

    Returns:
    pd.DataFrame: DataFrame with processed airport features and H3 hexagons.
    """
    df = retrieve_osm_data(apt_icao).reset_index()
    df['apt_icao'] = apt_icao
    df['geom_type'] = df['geometry'].apply(lambda l: l.geom_type)
    
    if 'width' not in df.columns:
      df['width'] = None
    
    df['width'] = df.apply(lambda l: fill_missing_width(l['aeroway'], l['width']), axis=1)
    
    # Handle multiple polygons per feature
    df['polygon_geometries'] = df.apply(lambda l: convert_to_polygon(l['geometry'], l['geom_type'], l['width']), axis=1)
    df = df.explode('polygon_geometries')  # Explode MultiPolygons into individual Polygons
    
    # Convert polygons to H3
    df['hex_id'] = df.apply(lambda l: polygon_to_h3(l['polygon_geometries'], resolution), axis=1)
    df = df.explode('hex_id')
    df = df[~df.hex_id.isna()]
    
    df['hex_latitude'], df['hex_longitude'] = zip(*df['hex_id'].apply(h3.h3_to_geo))
    df['hex_res'] = resolution
    
    # Reorganize columns
    s = ['apt_icao', 'hex_id', 'hex_latitude', 'hex_longitude', 'hex_res']
    df = df[s + [x for x in df.columns if x not in s + ['geometry', 'polygon_geometries']]]
    df = df.rename({'hex_id': 'h3_id', 'id': 'osm_id', 'element': 'type'}, axis=1)
    df.columns = ['hexaero_' + x.replace('hex_', '') for x in df.columns]
    
    # Type conversion and validation
    column_type = {
        'hexaero_apt_icao': str,
        'hexaero_h3_id': str,
        'hexaero_latitude': float,
        'hexaero_longitude': float,
        'hexaero_res': int,
        'hexaero_aeroway': str,
        'hexaero_length': float,
        'hexaero_ref': str,
        'hexaero_surface': str,
        'hexaero_width': float,
        'hexaero_osm_id': int,
        'hexaero_type': str
    }
    for column in column_type.keys():
        if column in df.columns:
            if column_type[column] in [float, int]:
                df[column] = df[column].apply(clean_str).astype(column_type[column])
            else:
                df[column] = df[column].astype(column_type[column])
        else:
            df[column] = None
    df = df[column_type.keys()]
    return df


# Fetch airport data
airports_df = fetch_airport_data() 
#airports_df = airports_df.loc[airports_df.ident.isin(['EBBR', 'LTFM', 'LFPG', 'EDDM', 'EFHK']), :]

## Load logs
fpath_success = 'logs/00_hexaero_layout_progress_success.parquet'
if os.path.isfile(fpath_success):
    processed_apt_success = pd.read_parquet(fpath_success).apt.to_list()
else:
    processed_apt_success = []

fpath_failed = 'logs/00_hexaero_layout_progress_failed.parquet'
processed_apt_failed = []
processed_apt_errpr = []

troublesome_airports = ['SPLO']

for apt_icao in airports_df.ident.to_list():
    print(f"Processing {apt_icao}...")
    
    if apt_icao in troublesome_airports:
      print('... Not processing as troublesome')
      continue
    # Radius around which airport elements are searched within OSM
    
    if apt_icao in processed_apt_success:
        print(f'Airport {apt_icao} is already processed - Skipping...')
        print()
        continue
    else:
        try:
            df = hexagonify_airport(apt_icao, resolution = resolution)
                        
            # Define the schema
            schema = StructType([
                StructField("hexaero_apt_icao", StringType(), True),
                StructField("hexaero_h3_id", StringType(), True),
                StructField("hexaero_latitude", DoubleType(), True),
                StructField("hexaero_longitude", DoubleType(), True),
                StructField("hexaero_res", IntegerType(), True),
                StructField("hexaero_aeroway", StringType(), True),
                StructField("hexaero_length", DoubleType(), True),
                StructField("hexaero_ref", StringType(), True),
                StructField("hexaero_surface", StringType(), True),
                StructField("hexaero_width", DoubleType(), True),
                StructField("hexaero_osm_id", LongType(), True),
                StructField("hexaero_type", StringType(), True)
            ])

            sdf = spark.createDataFrame(df.to_dict(orient='records'), schema)
            sdf = sdf.repartition("hexaero_apt_icao").orderBy("hexaero_apt_icao")
            sdf.writeTo(f"`{project}`.`hexaero_airport_layouts`").append()
            
            ## Logging
            processed_apt_success.append(apt_icao)
            processed_apt_success_df = pd.DataFrame({'apt':processed_apt_success})
            processed_apt_success_df.to_parquet(fpath_success)
        
        except Exception as e: 
            print(f"Failed to process {apt_icao}. Error: {e}")
            print(traceback.format_exc())
            print()
            processed_apt_failed.append(apt_icao)
            processed_apt_errpr.append(str(e))  # Convert exception to string
            processed_apt_failed_df = pd.DataFrame({'apt':processed_apt_failed, 'error':processed_apt_errpr})
            processed_apt_failed_df.to_parquet(fpath_failed)
            continue