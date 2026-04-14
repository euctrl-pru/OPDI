import time
from pyspark.sql import SparkSession
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta
import subprocess
import os
import re


# ---------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------
PROJECT = "project_opdi"

TABLE = "project_opdi.osn_statevectors_v2"

# Generate output filenames with today's date
TODAY_DATE = datetime.now().strftime('%Y-%m-%d')
OUTPUT_CSV = f"daily_rows_counts_{TODAY_DATE}.csv"
OUTPUT_PLOT = f"daily_rows_visualization_{TODAY_DATE}.html"

# Data quality threshold
SUSPICIOUS_THRESHOLD = 25000000

# Known outage periods (UTC) - data loss due to technical issues
KNOWN_OUTAGES = [
    ("2023-01-02 23:00:00", "2023-01-03 10:00:00"),
    ("2023-01-18 11:00:00", "2023-01-23 07:00:00"),
    ("2023-06-21 13:00:00", "2023-06-21 22:00:00"),
    ("2023-11-15 06:00:00", "2023-11-16 08:00:00"),
    ("2023-11-20 01:00:00", "2023-11-20 03:00:00"),
    ("2023-12-02 08:00:00", "2023-12-05 03:00:00"),
    ("2024-05-20 10:00:00", "2024-05-21 05:00:00"),
]

# ---------------------------------------------------------------------
# Spark Session Initialization (unchanged)
# ---------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("Validation") \
    .config("spark.hadoop.fs.azure.ext.cab.required.group", "eur-app-opdi") \
    .config("spark.kerberos.access.hadoopFileSystems", "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged") \
    .config("spark.executor.extraClassPath", "/opt/spark/optional-lib/iceberg-spark-runtime-3.3_2.12-1.3.1.1.20.7216.0-70.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark/optional-lib/iceberg-spark-runtime-3.3_2.12-1.3.1.1.20.7216.0-70.jar") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "abfs://storage-fs@cdpdllive.dfs.core.windows.net/data/project/opdi.db/unmanaged") \
    .config("spark.sql.sources.useV1SourceList", "iceberg") \
    .config("spark.sql.iceberg.vectorization.enabled", "false") \
    .config("spark.sql.files.maxPartitionBytes", "536870912") \
    .config("spark.sql.files.openCostInBytes", "4194304") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64MB") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
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

# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------
def execute_shell_command(command: str) -> tuple:
    """
    Execute a shell command and return stdout and stderr.

    Parameters
    ----------
    command : str
        Shell command to execute.

    Returns
    -------
    tuple
        (stdout, stderr) as strings.
    """
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    return stdout.decode().strip(), stderr.decode().strip()


def setup_mc() -> bool:
    """
    Set up MinIO client (mc) for accessing OpenSky bucket.
    Requires OSN_USERNAME and OSN_KEY environment variables.

    Returns
    -------
    bool
        True if setup successful, False otherwise.
    """
    if not os.path.exists('./mc'):
        print("Downloading MinIO client...")
        execute_shell_command('curl -O https://dl.min.io/client/mc/release/linux-amd64/mc')
        execute_shell_command('chmod +x mc')

    username = os.getenv('OSN_USERNAME')
    key = os.getenv('OSN_KEY')

    if not username or not key:
        print("WARNING: OSN_USERNAME or OSN_KEY environment variables not set")
        return False

    stdout, stderr = execute_shell_command(f'./mc alias set opensky https://s3.opensky-network.org {username} {key}')
    if stderr and 'error' in stderr.lower():
        print(f"Error setting up MinIO client: {stderr}")
        return False

    return True


def check_known_outage(date: pd.Timestamp) -> str:
    """
    Check if a date falls within any known outage period.

    Parameters
    ----------
    date : pd.Timestamp
        Date to check.

    Returns
    -------
    str
        Outage period if date is affected, empty string otherwise.
    """
    for start_str, end_str in KNOWN_OUTAGES:
        start = pd.to_datetime(start_str)
        end = pd.to_datetime(end_str)

        # Check if any part of the day overlaps with outage period
        day_start = date.replace(hour=0, minute=0, second=0)
        day_end = date.replace(hour=23, minute=59, second=59)

        if (day_start <= end) and (day_end >= start):
            return f"{start_str} to {end_str}"

    return ""


def get_minio_files_for_date(date: pd.Timestamp) -> dict:
    """
    Query MinIO bucket for files available on a specific date.

    Parameters
    ----------
    date : pd.Timestamp
        Date to query.

    Returns
    -------
    dict
        Dictionary with 'file_count' and 'hours' (list of hours with data).
    """
    date_str = date.strftime('%Y-%m-%d')

    # Query MinIO for files matching the date pattern
    command = f'./mc ls opensky/ec-datadump/ --recursive | grep "states_{date_str}"'
    stdout, stderr = execute_shell_command(command)

    if not stdout:
        return {'file_count': 0, 'hours': []}

    # Parse filenames to extract hours
    files = stdout.split('\n')
    hours = set()

    for file_line in files:
        # Extract filename from ls output
        match = re.search(r'states_(\d{4}-\d{2}-\d{2}-\d{2})\.parquet', file_line)
        if match:
            datetime_str = match.group(1)
            hour = datetime_str.split('-')[-1]
            hours.add(int(hour))

    sorted_hours = sorted(list(hours))
    return {'file_count': len(files), 'hours': sorted_hours}


def analyze_missing_data(df: pd.DataFrame, check_minio: bool = True) -> pd.DataFrame:
    """
    Analyze missing or suspicious data and add diagnostic columns.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with date and row_count columns.
    check_minio : bool
        Whether to check MinIO bucket for file availability.

    Returns
    -------
    pd.DataFrame
        DataFrame with additional diagnostic columns.
    """
    print("\nAnalyzing data quality...")

    # Add analysis columns
    df['is_suspicious'] = (df['row_count'] < SUSPICIOUS_THRESHOLD)
    df['known_outage'] = df['date'].apply(check_known_outage)
    df['minio_file_count'] = 0
    df['minio_hours_available'] = ""

    # Setup MinIO if needed
    mc_available = False
    if check_minio:
        mc_available = setup_mc()
        if not mc_available:
            print("MinIO client not available - skipping bucket checks")

    # Analyze suspicious dates
    suspicious_dates = df[df['is_suspicious']]
    print(f"Found {len(suspicious_dates)} suspicious dates (row_count < {SUSPICIOUS_THRESHOLD:,} or == 0)")

    if mc_available and len(suspicious_dates) > 0:
        print("Checking MinIO bucket for suspicious dates...")
        for idx, row in suspicious_dates.iterrows():
            date = row['date']
            minio_info = get_minio_files_for_date(date)
            df.loc[idx, 'minio_file_count'] = minio_info['file_count']
            df.loc[idx, 'minio_hours_available'] = ','.join(map(str, minio_info['hours']))

            if (idx - suspicious_dates.index[0]) % 10 == 0:
                print(f"  Processed {idx - suspicious_dates.index[0] + 1}/{len(suspicious_dates)} dates...")

    # Create explanation column
    def create_explanation(row):
        if not row['is_suspicious']:
            return "OK"

        explanations = []
        if row['row_count'] == 0:
            explanations.append("No data")
        elif row['row_count'] < SUSPICIOUS_THRESHOLD:
            explanations.append(f"Low count ({row['row_count']:,})")

        if row['known_outage']:
            explanations.append(f"Known outage: {row['known_outage']}")

        if row['minio_file_count'] > 0:
            explanations.append(f"MinIO: {row['minio_file_count']} files, hours: {row['minio_hours_available']}")
        elif mc_available:
            explanations.append("MinIO: No files found")

        return "; ".join(explanations) if explanations else "Suspicious"

    df['explanation'] = df.apply(create_explanation, axis=1)

    print(f"Analysis complete. {len(df[df['known_outage'] != ''])} dates affected by known outages.")

    return df


def fetch_daily_row_counts(spark_session: SparkSession, table: str) -> pd.DataFrame:
    """
    Fetch daily row counts from the specified table.
    Benefits from partition pruning since table is partitioned by day.

    Parameters
    ----------
    spark_session : SparkSession
        Active Spark session.
    table : str
        Fully qualified table name.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: date, row_count
    """
    print(f"\nFetching daily row counts from {table}...\n")

    # Query leverages day-partitioned table structure for efficient scanning
    query = f"""
    SELECT
        DATE(event_time) AS date,
        COUNT(*) AS row_count
    FROM {table}
    GROUP BY DATE(event_time)
    ORDER BY date
    """

    df_spark = spark_session.sql(query)
    df_pandas = df_spark.toPandas()

    print(f"Retrieved {len(df_pandas)} days of data")
    print(f"Date range: {df_pandas['date'].min()} to {df_pandas['date'].max()}")
    print(f"Total rows: {df_pandas['row_count'].sum():,}")

    return df_pandas


def fill_missing_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill in missing dates with 0 row counts.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with date and row_count columns.

    Returns
    -------
    pd.DataFrame
        DataFrame with all dates filled in.
    """
    # Convert date to datetime
    df['date'] = pd.to_datetime(df['date'])

    # Create complete date range
    date_range = pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')

    # Create complete dataframe
    complete_df = pd.DataFrame({'date': date_range})

    # Merge with original data, filling missing values with 0
    result_df = complete_df.merge(df, on='date', how='left')
    result_df['row_count'] = result_df['row_count'].fillna(0).astype(int)

    # Sort by date
    result_df = result_df.sort_values('date').reset_index(drop=True)

    missing_count = len(result_df) - len(df)
    if missing_count > 0:
        print(f"Filled {missing_count} missing dates with 0 row counts")

    return result_df


def save_to_csv(df: pd.DataFrame, output_path: str) -> None:
    """
    Save DataFrame to CSV file.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to save.
    output_path : str
        Path to output CSV file.
    """
    df.to_csv(output_path, index=False)
    print(f"\nData saved to: {output_path}")


def visualize_daily_counts(df: pd.DataFrame, output_path: str) -> None:
    """
    Create and save interactive Plotly visualization of daily row counts with data quality indicators.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with date, row_count, and analysis columns.
    output_path : str
        Path to save the HTML plot.
    """
    # Convert date to datetime for better plotting
    df['date'] = pd.to_datetime(df['date'])

    # Determine color for each bar based on data quality
    colors = []
    for _, row in df.iterrows():
        if row['row_count'] == 0:
            colors.append('red')  # No data
        elif row['known_outage'] != '':
            colors.append('orange')  # Known outage
        elif row['is_suspicious']:
            colors.append('yellow')  # Suspicious but no known reason
        else:
            colors.append('steelblue')  # Normal

    # Create hover text with explanation
    hover_texts = []
    for _, row in df.iterrows():
        hover = f"<b>Date:</b> {row['date'].strftime('%Y-%m-%d')}<br>"
        hover += f"<b>Row Count:</b> {row['row_count']:,.0f}<br>"
        hover += f"<b>Status:</b> {row['explanation']}<br>"
        hover_texts.append(hover)

    # Create interactive bar chart
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df['date'],
        y=df['row_count'],
        marker=dict(
            color=colors,
            line=dict(color='darkblue', width=0.5)
        ),
        hovertemplate='%{text}<extra></extra>',
        text=hover_texts
    ))

    # Add horizontal line for suspicious threshold
    fig.add_hline(
        y=SUSPICIOUS_THRESHOLD,
        line_dash="dash",
        line_color="red",
        annotation_text=f"Suspicious Threshold ({SUSPICIOUS_THRESHOLD:,.0f})",
        annotation_position="right"
    )

    fig.update_layout(
        title={
            'text': 'Daily Row Counts - OpenSky Network State Vectors<br><sub>Color: Blue=OK, Yellow=Suspicious, Orange=Known Outage, Red=No Data</sub>',
            'font': {'size': 18},
            'x': 0.5,
            'xanchor': 'center'
        },
        xaxis=dict(
            title='Date',
            titlefont=dict(size=14, weight='bold'),
            gridcolor='lightgray',
            gridwidth=0.5
        ),
        yaxis=dict(
            title='Row Count',
            titlefont=dict(size=14, weight='bold'),
            gridcolor='lightgray',
            gridwidth=0.5,
            separatethousands=True
        ),
        hovermode='closest',
        plot_bgcolor='white',
        height=700,
        margin=dict(l=80, r=40, t=120, b=80)
    )

    # Save as HTML
    fig.write_html(output_path)
    print(f"Interactive visualization saved to: {output_path}")

    # Display summary statistics
    print("\nSummary Statistics:")
    print("-" * 50)
    print(f"Average daily rows:      {df['row_count'].mean():>15,.0f}")
    print(f"Median daily rows:       {df['row_count'].median():>15,.0f}")
    print(f"Min daily rows:          {df['row_count'].min():>15,.0f}")
    print(f"Max daily rows:          {df['row_count'].max():>15,.0f}")
    print(f"Std deviation:           {df['row_count'].std():>15,.0f}")
    print("-" * 50)
    print(f"Total days:              {len(df):>15,}")
    print(f"Suspicious days:         {df['is_suspicious'].sum():>15,}")
    print(f"Known outage days:       {(df['known_outage'] != '').sum():>15,}")
    print(f"Days with zero data:     {(df['row_count'] == 0).sum():>15,}")
    print("-" * 50)


# ---------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------
if __name__ == "__main__":
    start_time = time.time()

    # Check if CSV already exists
    if os.path.exists(OUTPUT_CSV):
        print(f"\nFound existing CSV: {OUTPUT_CSV}")
        print("Reading data from CSV instead of querying Spark...\n")

        # Read existing CSV
        df_daily = pd.read_csv(OUTPUT_CSV)
        df_daily['date'] = pd.to_datetime(df_daily['date'])

        # Check if analysis columns already exist
        if 'is_suspicious' not in df_daily.columns:
            print("Analysis columns not found in CSV. Running analysis...")
            # Keep only basic columns for re-analysis
            df_daily = df_daily[['date', 'row_count']]
            df_daily = analyze_missing_data(df_daily, check_minio=True)
            save_to_csv(df_daily, OUTPUT_CSV)
        else:
            print("CSV already contains analysis data. Skippinag re-analysis.")
    else:
        print("\nCSV not found. Fetching data from Spark...")

        # Fetch daily row counts
        df_daily = fetch_daily_row_counts(spark, TABLE)

        # Fill in missing dates with 0 counts
        df_daily = fill_missing_dates(df_daily)

        # Analyze missing/suspicious data
        df_daily = analyze_missing_data(df_daily, check_minio=True)

        # Store results to CSV with analysis
        save_to_csv(df_daily, OUTPUT_CSV)

    # Create and save enhanced visualization
    visualize_daily_counts(df_daily, OUTPUT_PLOT)

    elapsed = time.time() - start_time
    print(f"\nCompleted in {elapsed:.2f} seconds\n")
