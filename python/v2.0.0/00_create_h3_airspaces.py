# Import libs
import sys
import os
import pandas as pd
from datetime import datetime
import shapely 
import h3

# Add custom libs
sys.path.append(os.path.expanduser('~/libs'))
from geotools import *

# settings
ansp = [
    'https://raw.githubusercontent.com/euctrl-pru/pruatlas/master/data/ansps_ace_406.parquet',
    'https://raw.githubusercontent.com/euctrl-pru/pruatlas/master/data/ansps_ace_481.parquet']
firs = [
    'https://raw.githubusercontent.com/euctrl-pru/pruatlas/master/data/firs_nm_406.parquet',
    'https://raw.githubusercontent.com/euctrl-pru/pruatlas/master/data/firs_nm_481.parquet']
countries = [
    'https://raw.githubusercontent.com/euctrl-pru/pruatlas/master/data/countries50m.parquet']

# Helper functions

def fill_geometry(geometry, res = 7):
     return [h3.polyfill(shapely.geometry.mapping(x), res, geo_json_conformant=True) for x in shapely.wkt.loads(geometry).geoms]

def fill_geometry_compact(geometry, res = 7):
     return [h3.compact(h3.polyfill(shapely.geometry.mapping(x), res, geo_json_conformant=True)) for x in shapely.wkt.loads(geometry).geoms]

def process_ansp_airspaces(filepath, compact = False):
    ansp_df = pd.read_parquet(filepath)
    
    cfmu_airac = {
        406 : [datetime(2015, 11, 12), datetime(2015, 12, 10)],
        481 : [datetime(2021, 8, 12), datetime(2021,9, 9)]
    }
    
    ansp_df['validity_start'] = ansp_df.airac_cfmu.apply(lambda l: cfmu_airac[l][0])
    ansp_df['validity_end'] = ansp_df.airac_cfmu.apply(lambda l: cfmu_airac[l][1])
    
    ansp_df = ansp_df[[
        'airspace_type',
        'name',
        'code',
        'airac_cfmu',
        'validity_start',
        'validity_end',
        'min_fl',
        'max_fl',
        'geometry_wkt'
    ]]
    if compact:
        ansp_df.loc[:, 'h3_res_7'] = ansp_df.geometry_wkt.apply(lambda l: fill_geometry_compact(l))
    else:
        ansp_df.loc[:, 'h3_res_7'] = ansp_df.geometry_wkt.apply(lambda l: fill_geometry(l))
    ansp_df = ansp_df.loc[:, ansp_df.columns != 'geometry_wkt'] 
    ansp_df = ansp_df.explode('h3_res_7').explode('h3_res_7')
    return ansp_df


def process_fir_airspaces(filepath, compact = False):
    fir_df = pd.read_parquet(filepath)
    cfmu_airac = {
        406 : [datetime(2015, 11, 12), datetime(2015, 12, 10)],
        481 : [datetime(2021, 8, 12), datetime(2021,9, 9)]
    }
    
    fir_df['validity_start'] = fir_df.airac_cfmu.apply(lambda l: cfmu_airac[l][0])
    fir_df['validity_end'] = fir_df.airac_cfmu.apply(lambda l: cfmu_airac[l][1])
    
    fir_df = fir_df.rename({'id':'code'},axis=1)
    
    fir_df = fir_df[[
        'airspace_type',
        'name',
        'code',
        'airac_cfmu',
        'validity_start',
        'validity_end',
        'min_fl',
        'max_fl',
        'geometry_wkt'
    ]]
    
    if compact:
        fir_df.loc[:, 'h3_res_7'] = fir_df.geometry_wkt.apply(lambda l: fill_geometry_compact(l))
    else:
        fir_df.loc[:, 'h3_res_7'] = fir_df.geometry_wkt.apply(lambda l: fill_geometry(l))
    fir_df = fir_df.loc[:, fir_df.columns != 'geometry_wkt'] 
    fir_df = fir_df.explode('h3_res_7').explode('h3_res_7')
    return fir_df

def process_country_airspace(filepath, compact = False):
    ctry_df = pd.read_parquet(filepath)
    
    ctry_df = ctry_df.rename({
        'admin':'name',
        'iso_a3':'code'
    }, axis = 1)
    ctry_df['min_fl'] = 0
    ctry_df['max_fl'] = 999
    ctry_df['airac_cfmu'] = -1
    ctry_df['validity_start'] = datetime(1900,1,1)
    ctry_df['validity_end'] = datetime(2100,1,1)
    ctry_df['airspace_type'] = 'COUNTRY'
    
    ctry_df = ctry_df[[
        'airspace_type',
        'name',
        'code',
        'airac_cfmu',
        'validity_start',
        'validity_end',
        'min_fl',
        'max_fl',
        'geometry_wkt'
    ]]
    
    if compact:
        ctry_df.loc[:, 'h3_res_7'] = ctry_df.geometry_wkt.apply(lambda l: fill_geometry_compact(l))
    else:
        ctry_df.loc[:, 'h3_res_7'] = ctry_df.geometry_wkt.apply(lambda l: fill_geometry(l))
    ctry_df = ctry_df.loc[:, ctry_df.columns != 'geometry_wkt'] 
    ctry_df = ctry_df.explode('h3_res_7').explode('h3_res_7')
    return ctry_df

# Processing part
for filepath in ansp:
    print(f"Processing: {filepath}")
    # Uncompacted data goes in the database
    #df = process_ansp_airspaces(filepath, compact = False)
    ## .. write to db .. ## 
    
    # Compacted data goes to a file for inspection
    dest = "~/data/airspace_data/ansp/"
    output_path = f"{dest}/{filepath.split('/')[-1].split('.')[0]}_h3_compact.parquet"
    df = process_ansp_airspaces(filepath, compact = False)
    df.to_parquet(output_path)

for filepath in fir:
    print(f"Processing: {filepath}")
    # Uncompacted data goes in the database
    #df = process_fir_airspaces(filepath, compact = False)
    ## .. write to db .. ## 
    
    # Compacted data goes to a file for inspection
    dest = "~/data/airspace_data/fir/"
    output_path = f"{dest}/{filepath.split('/')[-1].split('.')[0]}_h3_compact.parquet"
    df = process_ansp_airspaces(filepath, compact = False)
    df.to_parquet(output_path)

for filepath in countries:
    print(f"Processing: {filepath}")
    # Uncompacted data goes in the database
    #df = process_fir_airspaces(filepath, compact = False)
    ## .. write to db .. ## 
    
    # Compacted data goes to a file for inspection
    dest = "~/data/airspace_data/countries/"
    output_path = f"{dest}/{filepath.split('/')[-1].split('.')[0]}_h3_compact.parquet"
    df = process_ansp_airspaces(filepath, compact = False)
    df.to_parquet(output_path)