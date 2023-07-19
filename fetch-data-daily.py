from traffic.data.adsb.opensky_impala import Impala
import pandas as pd
from pathlib import Path
from traffic.data import opensky
import itertools
from datetime import datetime, timedelta

cache_dir_path = "/Users/quintengoens/opensky_cache"
cache_dir = Path(cache_dir_path)

def fetch_data(start_time, end_time, ADEP, ADES):
    # Convert to datetime
    start_time = pd.to_datetime(start_time)
    end_time = pd.to_datetime(end_time)
    
    # Generate a list of dates from start_time to end_time
    date_range = pd.date_range(start_time, end_time)
    
    for date in date_range:
        
        # Format date for file naming and for opensky.history input
        date_str = date.strftime('%Y-%m-%d %H:%M')
        
        # Calculate the stop time for this day
        stop_time_str = (date + timedelta(days=1)).strftime('%Y-%m-%d %H:%M')
        
        print()
        print("-"*20)
        print(f"Fetching data for period {date_str} - {stop_time_str}...")
        
        # If the file already exists, do not rerun
        if Path(f"data/daily/borealis-osn-3dpi_{ADEP}_{ADES}_{date_str}_{stop_time_str}.parquet.gz").exists():
            continue
        df_dep = opensky.history(
            start=date_str,
            stop=stop_time_str,
            departure_airport=ADEP,
            arrival_airport=ADES,
            progressbar=False
        )
        
        df_arr = opensky.history(
            start=date_str,
            stop=stop_time_str,
            departure_airport=ADES,
            arrival_airport=ADEP,
            progressbar=False
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
        df.to_parquet(f"data/daily/borealis-osn-3dpi_{ADEP}_{ADES}_{date_str}_{stop_time_str}.parquet.gz", compression="gzip")
        print("-"*20)
        print()
    return None


# Apply the function on all the airport combinations possible 
#apts = ["EGLL", "EKCH", "BIRK", "ESSA"] # "EIDW",
apts = ["EIDW", "ESSA"] # "EIDW",
# Calculate all the combinations of apt pairs
start_time = "2023-01-01 00:00"
end_time = "2023-06-01 00:00"

for apt1, apt2 in list(itertools.combinations(apts, 2)):
    print(apt1, apt2, datetime.now())
    fetch_data(start_time, end_time, apt1, apt2)
    
    print("Done!")
    print()
