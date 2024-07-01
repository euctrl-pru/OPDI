import pandas as pd
import traffic
from traffic.data import opensky
import h3

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# Trajectories
df = pd.read_parquet('../data/2023-08-02-11.parquet')
df['id'] = df['icao24'] + '-' + df['callsign'] + '-' + df['time'].dt.date.apply(str)
df = df[['id', 'time', 'icao24', 'callsign', 'lat', 'lon', 'baroaltitude']]

# Add hex_ids 

df['hex_id_5'] = df.apply(lambda l:h3.geo_to_h3(l['lat'],l['lon'], 5),axis=1)
df['hex_id_11'] = df.apply(lambda l:h3.geo_to_h3(l['lat'],l['lon'], 11),axis=1)

# Convert altitudes to ft and FL

df['baroaltitude_ft'] = df['baroaltitude']*3.28084
df['baroaltitude_fl'] = df['baroaltitude']*3.28084/100

# Filter out low altitude statevectors

df_low = df[df['baroaltitude_ft'] < 5000] 

# Read airport_hexagonifications

apt_hex = pd.read_parquet('../data/airport_hex/airport_hex_res_5.parquet')
apt_hex = apt_hex[apt_hex['type']=='large_airport']

# Create list of possible arrival / departure airports
arr_dep_apt = df_low.merge(apt_hex, left_on='hex_id_5', right_on='hex_id', how='inner')
arr_dep_apt = arr_dep_apt.groupby('id_x')['ident'].apply(set).reset_index()
arr_dep_apt['ident'] = arr_dep_apt['ident'].apply(list)
arr_dep_apt = arr_dep_apt.rename({'id_x':'id', 'ident':'potential_apt'},axis=1)

df_low = df_low.merge(arr_dep_apt, on = 'id', how = 'left')
id_example = '4009d8-BAW3ET-2023-08-02' 
apts_example = ['EGLL', 'EHAM']

def tracks_to_hex(df_low, id_example, apts_example):
    #print(df_low)
    #print(id_example)
    #print(apts_example)
    df_single = df_low[df_low['id'] == id_example]

    core_cols_single = ['id', 'time', 'lat', 'lon', 'hex_id_11', 'baroaltitude_fl']

    df_single = df_single[core_cols_single]
    df_single = df_single.reset_index()

    core_cols_rwy = ['id', 'airport_ref', 'airport_ident', 'gate_id', 'hex_id', 'gate_id_nr','le_ident','he_ident']

    df_rwys = []

    for apt in apts_example:
        df_rwy = pd.read_parquet(f'../data/runway_hex/{apt}.parquet')
        df_rwys.append(df_rwy)

    df_rwys = pd.concat(df_rwys)
    df_rwys = df_rwys[core_cols_rwy]

    df_hex_rwy = df_single.merge(df_rwys,left_on='hex_id_11', right_on='hex_id', how='left')

    result = df_hex_rwy.groupby(['id_x','airport_ident', 'gate_id','le_ident','he_ident'])['time'].agg([min,max]).reset_index().sort_values('min')
    return result

dfs = arr_dep_apt.apply(lambda l: tracks_to_hex(df_low, l['id'],l['potential_apt']),axis=1).to_list()

result = pd.concat(dfs)

def clean_gate(gate_id):
    if gate_id == 'runway_hexagons':
        return 'runway_hexagons',0
    else:
        return '_'.join(gate_id.split('_')[:4]), int(gate_id.split('_')[4])

result['gate_type'], result['gate_distance_from_rwy_nm'] = zip(*result.gate_id.apply(clean_gate))





## Determining arrival / departure... 

result = result.reset_index(drop=True)

result_min = result.loc[result.groupby(['id_x', 'airport_ident', 'le_ident', 'he_ident'])['gate_distance_from_rwy_nm'].idxmin()]
result_max = result.loc[result.groupby(['id_x', 'airport_ident', 'le_ident', 'he_ident'])['gate_distance_from_rwy_nm'].idxmax()] 

# Copy the DataFrame to avoid modifying the original unintentionally
result_copy = result.copy()

# Compute the minimum and maximum 'gate_distance_from_rwy_nm' for each group
min_values = result.groupby(['id_x', 'airport_ident', 'le_ident', 'he_ident'])['gate_distance_from_rwy_nm'].transform('min')
max_values = result.groupby(['id_x', 'airport_ident', 'le_ident', 'he_ident'])['gate_distance_from_rwy_nm'].transform('max')

# Add these as new columns to the DataFrame
result_copy['min_gate_distance'] = min_values
result_copy['max_gate_distance'] = max_values

# Now, you can filter rows where 'gate_distance_from_rwy_nm' matches the min or max values
# To specifically keep rows with the minimum value:
result_min = result_copy[result_copy['gate_distance_from_rwy_nm'] == result_copy['min_gate_distance']]

# To specifically keep rows with the maximum value:
result_max = result_copy[result_copy['gate_distance_from_rwy_nm'] == result_copy['max_gate_distance']]


cols_of_interest = ['id_x', 'airport_ident', 'le_ident', 'he_ident', 'min', 'gate_distance_from_rwy_nm']
result_min = result_min[cols_of_interest].rename({'min':'time_entry_min_distance', 'gate_distance_from_rwy_nm':'min_gate_distance_from_rwy_nm'},axis=1)
result_max = result_max[cols_of_interest].rename({'min':'time_entry_max_distance', 'gate_distance_from_rwy_nm':'max_gate_distance_from_rwy_nm'},axis=1)

det = result_min.merge(result_max, on=['id_x', 'airport_ident', 'le_ident', 'he_ident'], how='outer')

det['time_since_minimum_distance'] = det['time_entry_min_distance']-det['time_entry_max_distance']

det['time_since_minimum_distance_s'] = det['time_since_minimum_distance'].dt.total_seconds()

det['status'] = det['time_since_minimum_distance_s'].apply(lambda l: 'arrival' if l > 0 else 'departure')
det['status'] = det['status'].fillna('undetermined')

det = det[['id_x', 'airport_ident', 'le_ident', 'he_ident','status']]

gb_cols = ['id_x', 'airport_ident', 'le_ident', 'he_ident', 'gate_type']
result = result.groupby(gb_cols).agg(
    entry_time_approach_area=('min', 'min'),
    exit_time_approach_area=('max', 'max'),
    intersected_subsections=('gate_distance_from_rwy_nm', 'count'),
    minimal_distance_runway=('gate_distance_from_rwy_nm', 'min'),
    maximal_distance_runway=('gate_distance_from_rwy_nm', 'max')
)
result = result.reset_index()

rwy_result_cols = ['id_x', 'airport_ident', 'le_ident', 'he_ident']

rwy_result = result[rwy_result_cols + ['gate_type']]
rwy_result = rwy_result[rwy_result['gate_type']=='runway_hexagons']
rwy_result = rwy_result[rwy_result_cols]
rwy_result['runway_detected'] = True

result = result.merge(rwy_result, on=rwy_result_cols, how = 'left')

result['runway_detected'] = result['runway_detected'].fillna(False)

result = result[result['gate_type']!='runway_hexagons']

result['high_number_intersections'] = result['intersected_subsections']>5

result['low_minimal_distance'] = result['minimal_distance_runway']<5

result['score'] = result['runway_detected'].apply(int)*1.5 + result['high_number_intersections'].apply(int) + result['low_minimal_distance'].apply(int)
result = result.reset_index(drop=True)

result = result.merge(det,on=['id_x','airport_ident','le_ident','he_ident'], how ='left')

result['status'] = result['status'].fillna('undetermined')

result['rwy'] = result['le_ident'] + '/' + result['he_ident']

rwy_winner = result.loc[result.groupby(['id_x','airport_ident'])['score'].idxmax()]
rwy_winner['score'] = rwy_winner['score'].apply(str)
rwy_winner = rwy_winner.groupby(['id_x','airport_ident'])['le_ident', 'he_ident', 'rwy','score', 'status'].agg(', '.join).reset_index()
rwy_winner = rwy_winner.rename({
    'id_x':'id',
    'rwy' : 'likely_rwy',
    'score': 'likely_rwy_score',
    'status': 'likely_rwy_status'
    }, axis=1)
rwy_losers = result.loc[~result.index.isin(rwy_winner.index)]
rwy_losers['score'] = rwy_losers['score'].apply(str)
rwy_losers = rwy_losers.groupby(['id_x','airport_ident'])['le_ident', 'he_ident', 'rwy','score', 'status'].agg(', '.join).reset_index()

rwy_losers = rwy_losers.rename({
    'id_x':'id',
    'rwy' : 'potential_other_rwys',
    'score': 'potential_other_rwy_scores',
    'status': 'potential_other_rwy_status'
    }, axis=1)[['id', 'airport_ident', 'potential_other_rwys', 'potential_other_rwy_scores', 'potential_other_rwy_status']]

rwy_determined = rwy_winner.merge(rwy_losers, on=['id','airport_ident'], how='left')

rwy_determined