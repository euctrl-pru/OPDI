import os
import pandas as pd
import glob

def clean_and_save_data(input_dir: str, output_dir: str):
    """
    Reads each .parquet file from the input directory, removes duplicate rows,
    and writes the cleaned data to .parquet and .csv.gz files in the output directory
    using the same filename as the original files. Skips files that already exist in the output 
    directory and are larger than 200MB.

    Parameters:
    input_dir (str): Directory containing the .parquet files to read.
    output_dir (str): Directory where the cleaned files will be saved.
    """
    # Create the output directory if it does not exist
    os.makedirs(output_dir, exist_ok=True)

    # Get all .parquet files in the input directory
    parquet_files = glob.glob(os.path.join(input_dir, "*.parquet"))

    # Process each .parquet file individually
    for file in parquet_files:
        # Extract the base filename without the extension
        base_filename = os.path.basename(file).split('.')[0]

        # Define the output paths for .parquet and .csv.gz files
        parquet_output_path = os.path.join(output_dir, f'{base_filename}.parquet')
        csv_output_path = os.path.join(output_dir, f'{base_filename}.csv.gz')

        # Check if output files already exist and are larger than 200MB
        if (os.path.exists(parquet_output_path) and os.path.getsize(parquet_output_path) > 200 * 1024 * 1024) and \
           (os.path.exists(csv_output_path) and os.path.getsize(csv_output_path) > 200 * 1024 * 1024):
            print(f"Skipping {file} as cleaned files already exist and are larger than 200MB.")
            continue

        print(f"Processing {file}")

        # Read the .parquet file into a DataFrame
        df = pd.read_parquet(file)
        
        # Drop duplicate rows
        df.drop_duplicates(inplace=True)

        # Write the cleaned DataFrame to .parquet
        df.to_parquet(parquet_output_path)

        # Write the cleaned DataFrame to .csv.gz
        df.to_csv(csv_output_path, index=False, compression='gzip')

if __name__ == "__main__":
    input_directory = '/home/cdsw/data/OPDI/v002/measurements'
    output_directory = '/home/cdsw/data/OPDI/v002/measurements_clean/'
    clean_and_save_data(input_directory, output_directory)
