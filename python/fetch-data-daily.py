from dask import delayed, compute
from traffic.data.adsb.opensky_impala import Impala
import pandas as pd
from pathlib import Path
from traffic.data import opensky
import itertools
from datetime import datetime, timedelta
import logging
import os

logging.basicConfig(filename="output.log", level=logging.INFO)

cache_dir_path = "/Users/quintengoens/opensky_cache"
cache_dir = Path(cache_dir_path)

log_file_path = 'output.log'
if os.path.exists(log_file_path):
  os.remove(log_file_path)

def setup_logger():
    logger = logging.getLogger('my_logger')
    if not logger.hasHandlers():  # Prevent logging setup duplication
        logger.setLevel(logging.INFO)
        fh = logging.FileHandler('output.log')
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    return logger

def fetch_data(start_time, end_time, ADEP, ADES):
    # Convert to datetime
    logging = setup_logger()
    start_time = pd.to_datetime(start_time)
    end_time = pd.to_datetime(end_time)
    
    # Generate a list of dates from start_time to end_time
    date_range = pd.date_range(start_time, end_time)
    
    for date in date_range:
        
        # Format date for file naming and for opensky.history input
        date_str = date.strftime('%Y-%m-%d %H:%M')
        
        # Calculate the stop time for this day
        stop_time_str = (date + timedelta(days=1)).strftime('%Y-%m-%d %H:%M')
        
        logging.info("")
        logging.info("-"*20)
        logging.info(f"Fetching data for period {date_str} - {stop_time_str} for {ADEP} - {ADES}...")
        
        # If the file already exists, do not rerun
        if Path(f"data/daily/borealis-osn-3dpi_{ADEP}_{ADES}_{date_str}_{stop_time_str}.parquet.gz").exists():
            logging.info(f"File exists already: data/daily/borealis-osn-3dpi_{ADEP}_{ADES}_{date_str}_{stop_time_str}.parquet.gz")
            continue
            
        try: 
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
              logging.info("Both are NULL")

          if pd.isnull(df_dep) and not pd.isnull(df_arr):
              df = df_arr.data
              logging.info("df_dep is NULL")

          if pd.isnull(df_arr) and not pd.isnull(df_dep):
              df = df_dep.data
              logging.info("df_arr is NULL")

          if not pd.isnull(df_arr) and not pd.isnull(df_dep):
              logging.info("both not NULL")
              df = pd.concat([df_dep.data, df_arr.data])

          # Convert timestamp column(s) to a lower precision (like microseconds)
          for col in df.columns:
              if pd.api.types.is_datetime64_any_dtype(df[col]):
                  df[col] = df[col].values.astype('datetime64[us]')

          # Write to compressed parquet file
          df.to_parquet(f"data/daily/borealis-osn-3dpi_{ADEP}_{ADES}_{date_str}_{stop_time_str}.parquet.gz", compression="gzip")
          logging.info("-"*20)
          logging.info("")
        except Exception as e: 
          logging.info(f"Request failed with exception: {e}")
          logging.info("-"*20)
          logging.info("")
    return None

if __name__ == "__main__":
    # Generate airport combinations
    start_time = "2023-04-01 00:00"
    end_time = "2023-08-01 00:00"
    apts = ["ESSA", "EGLL", "EKCH", "EFHK", "ENGM", "EETN", "EYVA", "BIRK", "EIDW"]
    combinations = list(itertools.combinations(apts, 2))
    
    # Initialize Dask delayed objects
    delayed_tasks = []
    
    for apt1, apt2 in combinations:
        print(apt1, apt2, datetime.now())
        
        # Use dask.delayed to wrap fetch_data function
        # Use dask.delayed to wrap fetch_data function
        delayed_task = delayed(fetch_data)(start_time, end_time, apt1, apt2)
        
        delayed_tasks.append(delayed_task)
    
    # Run tasks in parallel using 3 processes
    compute(*delayed_tasks, scheduler="processes", num_workers=3)