!pip install folium geojson h3 scipy shapely
# Viz
import traceback
import folium
from geojson import Feature, Point, FeatureCollection
import json
import pandas as pd
import matplotlib
import h3
import os.path
import numpy as np
from scipy.stats import circmean
from pyspark.sql import SparkSession
import requests
from shapely.geometry import LineString, Polygon
from shapely.ops import transform
from functools import partial
from pyproj import Transformer
import re
import glob
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Settings
project = "project_opdi"


# Spark Session Initialization
spark = SparkSession.builder \
    .appName("HexAero Runway Layout generator") \
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


def hexagons_dataframe_to_geojson(df_hex, file_output=None):
    """
    Produce the GeoJSON for a dataframe, constructing the geometry from the "hex_id" column
    and including all other columns as properties.
    """
    list_features = []

    for i, row in df_hex.iterrows():
        try:
            geometry_for_row = {"type": "Polygon", "coordinates": [h3.h3_to_geo_boundary(h=row["hex_id"], geo_json=True)]}
            properties = row.to_dict()  # Convert all columns to a dictionary
            properties.pop("hex_id", None)  # Remove hex_id as it's already used in geometry
            feature = Feature(geometry=geometry_for_row, id=row["hex_id"], properties=properties)
            list_features.append(feature)
        except Exception as e:
            print(f"An exception occurred for hex {row['hex_id']}: {e}")

    feat_collection = FeatureCollection(list_features)
    geojson_result = json.dumps(feat_collection)
    return geojson_result


airports_df = pd.read_csv('https://davidmegginson.github.io/ourairports-data/airports.csv')

# Filter the DataFrame
airports_df = airports_df[
    (airports_df['type'].isin(['large_airport', 'medium_airport']))][['ident', 'latitude_deg', 'longitude_deg', 'elevation_ft', 'type']]

print(f"There are {len(airports_df)} airports to process...")

def line_to_polygon(line, width_m, always_xy=True):
    """
    Convert a LineString to a Polygon with a specified width in meters.

    Parameters:
    line (LineString): The LineString object to be converted.
    width_m (float): The width of the line in meters.

    Returns:
    Polygon: A Polygon object representing the LineString with the specified width.
    """
    # Define the projection transformer
    # WGS 84 (latitude and longitude) to World Mercator projection
    transformer_to_meters = Transformer.from_crs("EPSG:4326", "EPSG:3395", always_xy=always_xy)
    transformer_to_degrees = Transformer.from_crs("EPSG:3395", "EPSG:4326", always_xy=always_xy)

    # Transform the LineString to World Mercator projection (meters)
    line_in_meters = transform(transformer_to_meters.transform, line)

    # Buffer the line in meters
    buffered_line = line_in_meters.buffer(width_m)

    # Transform the buffered line back to WGS 84 (degrees)
    buffered_line_in_degrees = transform(transformer_to_degrees.transform, buffered_line)

    return buffered_line_in_degrees

def lines_to_polygons(lines, standard_width, always_xy = True):
    """
    Convert a collection of LineStrings to Polygons with a specified width in meters.

    Parameters:
    lines [((LineString), (float))]: The LineString object to be converted and 
    the width of the line in meters.
    standard_width float: When the width is not available, this standard width will be used.
    
    Returns:
    array<Polygon>: An array of Polygon objects representing the LineString with the specified width.
    """
    polygons = []
    for x in lines:
        line = x[0]
        width = x[1]
        if pd.isna(width):
            width = standard_width
        
        polygon = line_to_polygon(line, width, always_xy=True)
        
        # Sometimes it happens that xy coordinates are switched and the polygon is empty...
        # This causes the polygon to be empty and thus the area to be zero...
        if polygon.area == 0: 
            print('Coordinates for this airport are switched from the \'normal OSM\' (longitude, latitude) to (latitude longitude), examine at OSM!')
            polygon = line_to_polygon(line, width, always_xy=False) 
        
        polygons.append(polygon)
    return polygons


def query_features(lat, lon, feature_type, radius=5000):
    """
    Query OpenStreetMap for aeroway features and return a DataFrame with metadata and geometries.

    Args:
    lat (float): Latitude of the center point.
    lon (float): Longitude of the center point.
    feature_type (str): Type of the aeroway feature (e.g., 'runway', 'taxiway').
    radius (int): Radius in meters around the point to search for the feature.

    Returns:
    DataFrame: A DataFrame with columns for metadata and geometry.
    """
    overpass_url = "http://overpass-api.de/api/interpreter"
    overpass_query = f"""
    [out:json];
    way["aeroway"="{feature_type}"](around:{radius},{lat},{lon});
    out body;
    >;
    out skel qt;
    """
    response = requests.get(overpass_url, params={'data': overpass_query})
    data = response.json()

    features = []
    for element in data['elements']:
        if element['type'] == 'way':
            nodes = {node['id']: (node['lat'],node['lon']) for node in data['elements'] if node['type'] == 'node'}
            points = [nodes[node_id] for node_id in element['nodes'] if node_id in nodes]
            linestring = LineString(points)

            feature_data = element.get('tags', {})
            feature_data.update({
                'id': element.get('id'),
                'type': element.get('type'),
                'geometry': linestring
                # Add other metadata fields here if needed
            })
            features.append(feature_data)

    return pd.DataFrame(features)

def shapely_polygon_to_geojson(polygon):
    """
    Convert a Shapely Polygon to a GeoJSON-like dictionary.

    Parameters:
    polygon (shapely.geometry.polygon.Polygon): The Shapely Polygon to convert.

    Returns:
    dict: A dictionary representing the Polygon in GeoJSON format.
    """
    # Extract the exterior of the polygon and convert it to a list of coordinates
    exterior_coords = list(polygon.exterior.coords)
    
    # Create a GeoJSON-like structure
    geojson_polygon = {
        "type": "Polygon",
        "coordinates": [exterior_coords]
    }

    return geojson_polygon

def polygon_to_h3(poly, resolution):
    """
    Convert a Shapely Polygon to a set of H3 indices.

    Parameters:
    poly (shapely.geometry.polygon.Polygon): The Shapely Polygon to convert.
    resolution (int): The H3 resolution.

    Returns:
    set: A set of H3 indices covering the Polygon.
    """
    geojson_poly = shapely_polygon_to_geojson(poly)
    return h3.polyfill(geojson_poly, resolution)

def is_number(s):
    """
    Checks if the input string is a valid number.
    
    Parameters:
    - s: The string to check.
    
    Returns:
    - True if the string is a number, False otherwise.
    """
    try:
        if pd.isna(s):
            return None
            
        float(s)  # Try converting the string to float
        return True
    except ValueError:
        return False

def safe_convert_to_float(s):
    """
    Safely converts a string to a float by first checking if it's already a valid number.
    If not, it removes non-numeric characters (except for the decimal point) and then tries to convert.
    
    Parameters:
    - s: The string to convert.
    
    Returns:
    - A float representation of the string if conversion is possible, None otherwise.
    """
    if is_number(s):
        return float(s)  # Return the float value directly if it's already a number
    
    if pd.isna(s):
        return None
    
    # If not a valid number, try removing non-numeric characters and convert
    
    numeric_part = re.sub(r"[^-0-9.]+", "", s)
    
    try:
        if pd.isna(numeric_part):
            return None
        return float(numeric_part)
    except ValueError:
        return None


def calculate_bearing(pointA, pointB):
    """
    Calculate the bearing between two points.

    Parameters:
    - pointA: A tuple containing the longitude and latitude of the first point.
    - pointB: A tuple containing the longitude and latitude of the second point.

    Returns:
    - Bearing in degrees from the North.
    """
    lon1, lat1 = np.radians(pointA)
    lon2, lat2 = np.radians(pointB)

    dLon = lon2 - lon1
    x = np.sin(dLon) * np.cos(lat2)
    y = np.cos(lat1) * np.sin(lat2) - np.sin(lat1) * np.cos(lat2) * np.cos(dLon)

    bearing = np.arctan2(x, y)
    bearing = np.degrees(bearing)
    bearing = (bearing + 360) % 360

    return bearing

def average_heading_linestring(linestring):
    """
    Calculate the average heading of a LineString.

    Parameters:
    - linestring: A shapely.geometry.linestring.LineString object.

    Returns:
    - The average heading of the linestring in degrees.
    """
    points = list(linestring.coords)
    bearings = []

    for i in range(len(points) - 1):
        bearing = calculate_bearing(points[i], points[i + 1])
        bearings.append(bearing)

    average_bearing = np.rad2deg(circmean(np.deg2rad(bearings))) if bearings else None
    return average_bearing


def hexagonify_airport(
    apt_icao,
    radius,
    airports_df,
    resolution=12
):
    """
    Hexagonifies the airport features within a given radius.

    Parameters:
    - apt_icao (str): ICAO identifier of the airport.
    - radius (float): Radius (in meters) for querying features around the airport.
    - airports_df (pd.DataFrame): DataFrame containing airport data with latitude and longitude.
    - resolution (int, optional): H3 resolution for hexagons. Default is 12.

    Returns:
    - df (pd.DataFrame): DataFrame containing hexagonified features.
    - latitude (float): Latitude of the airport.
    - longitude (float): Longitude of the airport.
    """
    # Validate airport existence
    airport_row = airports_df[airports_df['ident'] == apt_icao]
    if airport_row.empty:
        raise ValueError(f"No airport found with ICAO identifier {apt_icao}")

    latitude = airport_row.latitude_deg.values[0]
    longitude = airport_row.longitude_deg.values[0]

    features = ['runway', 'taxiway', 'parking_position', 'hangar','threshold','apron','deicing_pad']

    # Define standard widths and multipliers in an ad-hoc DataFrame
    feature_widths = pd.DataFrame({
        'feature': features,
        'standard_width': [50, 20, 20, 20, 20, 20, 35],
        'mp_width': [1, 1, 1, 1, 1, 1, 1]
    })

    def filter_empty_na_columns(df):
        """
        Filters out columns that are completely empty or contain only NA values from a DataFrame.

        Parameters:
        - df: pandas.DataFrame

        Returns:
        - Filtered DataFrame with no completely empty or all-NA columns.
        """
        df_filtered = df.dropna(axis=1, how='all')
        df_filtered = df_filtered.loc[:, (df_filtered != '').any(axis=0)]
        return df_filtered

    dfs = []
    for feature in features:
        dfs.append(query_features(latitude, longitude, feature, radius))

    dfs_filtered = []

    for idx, (feature, df) in enumerate(zip(features, dfs)):
        # Extract standard width and multiplier width from the DataFrame
        standard_width = feature_widths.loc[feature_widths['feature'] == feature, 'standard_width'].values[0]
        mp_width = feature_widths.loc[feature_widths['feature'] == feature, 'mp_width'].values[0]

        if df.empty:
            df['geometry'] = pd.Series(dtype='object')

        if 'width' not in df.columns:
            df['width'] = pd.Series(dtype='float')

        df['width'] = df['width'].apply(safe_convert_to_float)
        df['width'] = df['width'].apply(lambda l: None if pd.isna(l) else l * mp_width)
        df['polygon'] = lines_to_polygons(list(zip(df.geometry.to_list(), df.width.to_list())), standard_width * mp_width)
        df['hex_id'] = df['polygon'].apply(lambda l: polygon_to_h3(l, resolution))
        df['avg_heading'] = df.geometry.apply(average_heading_linestring)
        df['color_type'] = idx * idx * 1000

        # Apply the filtering function to each DataFrame
        df = filter_empty_na_columns(df)

        dfs_filtered.append(df)

    # Concatenate the filtered DataFrames
    df = pd.concat(dfs_filtered, ignore_index=True)
    
    # Check if the DataFrame is empty before attempting to explode
    if df.empty:
      print(f"No data available for airport {apt_icao}. Skipping...")
      return None, latitude, longitude  # Or handle appropriately for your use case

    
    df = df.explode('hex_id')
    df = df[~df.hex_id.isna()]

    df['apt_icao'] = apt_icao
    df['hex_latitude'], df['hex_longitude'] = zip(*df['hex_id'].apply(h3.h3_to_geo))
    df['hex_res'] = resolution

    return df, latitude, longitude

## Load logs
fpath_success = 'logs/00_hexaero_layout_progress_success.parquet'
if os.path.isfile(fpath_success):
    processed_apt_success = pd.read_parquet(fpath_success).apt.to_list()
else:
    processed_apt_success = []

fpath_failed = 'logs/00_hexaero_layout_progress_failed.parquet'
processed_apt_failed = []
processed_apt_errpr = []

for apt_icao in airports_df.ident.to_list():
    print(f"Processing {apt_icao}...")
    # Radius around which airport elements are searched within OSM
    radius = 5000 
    res = 12
    if apt_icao in processed_apt_success:
        print(f'Airport {apt_icao} is already processed - Skipping...')
        print()
        continue
    else:
        try: 

            df, latitude, longitude = hexagonify_airport(
                apt_icao, 
                radius,
                airports_df,
                resolution = res)

            s = ['apt_icao', 'hex_id', 'hex_latitude', 'hex_longitude', 'hex_res']
            df = df[s + [x for x in df.columns if x not in s + ['geometry', 'polygon']]]
            df = df.rename({'hex_id':'h3_id', 'id':'osm_id'},axis=1) 
            df.columns = ['hexaero_' + x.replace('hex_', '') for x in df.columns]
            
            column_type = {
                'hexaero_apt_icao' : str,
                'hexaero_h3_id' : str,
                'hexaero_latitude' : float,
                'hexaero_longitude' : float,
                'hexaero_res' : int,
                'hexaero_aeroway' : str,
                'hexaero_length' : float,
                'hexaero_ref' : str,
                'hexaero_surface' : str,
                'hexaero_width' : float,
                'hexaero_osm_id' : int,
                'hexaero_type' : str,
                'hexaero_avg_heading' : float} 
            
            import re
            
            def clean_str(s):
                # Also converts units in ft/mi to m
                if 'ft' in s:
                    result = float(re.sub(r'[^0-9.]', '', s))
                    result = str(result*0.3048)
                if 'mi' in s: 
                    result = float(re.sub(r'[^0-9.]', '', s))
                    result = str(result*1609.34)
                return result

            for column in column_type.keys():
                if column in df.columns:
                    if column_type[column] == float or column_type[column] == int:
                        df[column] = df[column].apply(clean_str)
                        df[column] = df[column].astype(column_type[column])
                    else:
                        df[column] = df[column].astype(column_type[column])
                else: 
                    df[column] = None
            
            df = df[column_type.keys()]
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
                StructField("hexaero_osm_id", IntegerType(), True),
                StructField("hexaero_type", StringType(), True),
                StructField("hexaero_avg_heading", DoubleType(), True)
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
            processed_apt_errpr.append(e)
            processed_apt_failed_df = pd.DataFrame({'apt':processed_apt_failed, 'error':processed_apt_errpr})
            processed_apt_failed_df.to_parquet(fpath_failed)
            continue


