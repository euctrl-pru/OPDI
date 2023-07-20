from traffic.data.adsb.opensky_impala import Impala
import pandas as pd
from pathlib import Path
from traffic.data import opensky
import itertools
from datetime import datetime

cache_dir_path = "/Users/quintengoens/opensky_cache"
cache_dir = Path(cache_dir_path)

def fetch_data(start_time, end_time, ADEP, ADES):
    # If the file already exists, do not rerun
    print()
    print("-"*20)
    print(f"Fetching data for period {start_time} - {end_time}...")
    if Path(f"data/full/borealis-osn-3dpi_{ADEP}_{ADES}_{start_time}_{end_time}.parquet.gz").exists():
        return None
    
    df_dep = opensky.history(
        start=start_time,
        stop=end_time,
        departure_airport=ADEP,
        arrival_airport=ADES,
    )
    
    df_arr = opensky.history(
        start=start_time,
        stop=end_time,
        departure_airport=ADES,
        arrival_airport=ADEP,
    )
    
    if pd.isnull(df_dep) and pd.isnull(df_arr):
        df = pd.DataFrame()
        print("Both are NULL")

    if pd.isnull(df_dep) and not pd.isnull(df_arr):
        df = df_arr.data
        print("df_dep is NULL")

    if pd.isnull(df_arr) and not pd.isnull(df_dep):
        df = df_dep.data
        print("df_arr is NULL")

    if not pd.isnull(df_arr) and not pd.isnull(df_dep):
        print("both not NULL")
        df = pd.concat([df_dep.data, df_arr.data])
    
    # Convert timestamp column(s) to a lower precision (like microseconds)
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].values.astype('datetime64[us]')
    
    # Write to compressed parquet file
    df.to_parquet(f"data/full/borealis-osn-3dpi_{ADEP}_{ADES}_{start_time}_{end_time}.parquet.gz", compression="gzip")
    print("-"*20)
    print()
    return None


# Apply the function on all the airport combinations possible 
apts = ["EGLL", "EIDW", "EKCH", "BIRK", "ESSA"]

# Calculate all the combinations of apt pairs
start_time = "2023-01-01 00:00"
end_time = "2023-06-19 00:00"

fetch_data(start_time, end_time, "EGLL", "EIDW")

for apt1, apt2 in list(itertools.combinations(apts, 2)):
    print(apt1, apt2, datetime.now())
    #%time fetch_data(start_time, end_time, apt1, apt2)
    
    print("Done!")
    print()