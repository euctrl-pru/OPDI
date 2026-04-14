"""
Advanced statistics and data quality monitoring for OPDI.

Provides comprehensive data quality analysis including:
- Daily row count trends
- Anomaly detection (suspicious low counts)
- Known outage tracking
- MinIO bucket availability checks
- Interactive Plotly visualizations
"""

import os
import re
import subprocess
from datetime import datetime
from typing import List, Tuple, Dict, Optional
import pandas as pd
import plotly.graph_objects as go
from pyspark.sql import SparkSession

from opdi.config import OPDIConfig


class AdvancedStatsCollector:
    """
    Advanced statistics and data quality monitoring.

    Tracks daily row counts, detects anomalies, and cross-references
    with known outages and MinIO bucket availability.
    """

    # Known outage periods (UTC) - data loss due to technical issues
    DEFAULT_KNOWN_OUTAGES = [
        ("2023-01-02 23:00:00", "2023-01-03 10:00:00"),
        ("2023-01-18 11:00:00", "2023-01-23 07:00:00"),
        ("2023-06-21 13:00:00", "2023-06-21 22:00:00"),
        ("2023-11-15 06:00:00", "2023-11-16 08:00:00"),
        ("2023-11-20 01:00:00", "2023-11-20 03:00:00"),
        ("2023-12-02 08:00:00", "2023-12-05 03:00:00"),
        ("2024-05-20 10:00:00", "2024-05-21 05:00:00"),
    ]

    def __init__(
        self,
        spark: SparkSession,
        config: OPDIConfig,
        suspicious_threshold: int = 25_000_000,
        known_outages: Optional[List[Tuple[str, str]]] = None,
    ):
        """
        Initialize advanced stats collector.

        Args:
            spark: Active SparkSession
            config: OPDI configuration object
            suspicious_threshold: Row count threshold below which data is suspicious
            known_outages: List of (start_datetime, end_datetime) outage periods
        """
        self.spark = spark
        self.config = config
        self.project = config.project.project_name
        self.suspicious_threshold = suspicious_threshold
        self.known_outages = known_outages or self.DEFAULT_KNOWN_OUTAGES

    def fetch_daily_row_counts(self, table_name: str, date_column: str = "event_time") -> pd.DataFrame:
        """
        Fetch daily row counts from a table.

        Leverages partition pruning for efficient scanning.

        Args:
            table_name: Table name (without project prefix)
            date_column: Column to use for date grouping

        Returns:
            DataFrame with columns: date, row_count

        Example:
            >>> collector = AdvancedStatsCollector(spark, config)
            >>> df = collector.fetch_daily_row_counts("osn_statevectors_v2")
            >>> print(f"Date range: {df['date'].min()} to {df['date'].max()}")
        """
        full_table_name = f"{self.project}.{table_name}"
        print(f"\nFetching daily row counts from {full_table_name}...")

        query = f"""
        SELECT
            DATE({date_column}) AS date,
            COUNT(*) AS row_count
        FROM {full_table_name}
        GROUP BY DATE({date_column})
        ORDER BY date
        """

        df_spark = self.spark.sql(query)
        df_pandas = df_spark.toPandas()

        print(f"Retrieved {len(df_pandas)} days of data")
        print(f"Date range: {df_pandas['date'].min()} to {df_pandas['date'].max()}")
        print(f"Total rows: {df_pandas['row_count'].sum():,}")

        return df_pandas

    def fill_missing_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fill in missing dates with 0 row counts.

        Args:
            df: DataFrame with date and row_count columns

        Returns:
            DataFrame with all dates filled in
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

    def check_known_outage(self, date: pd.Timestamp) -> str:
        """
        Check if a date falls within any known outage period.

        Args:
            date: Date to check

        Returns:
            Outage period string if date is affected, empty string otherwise
        """
        for start_str, end_str in self.known_outages:
            start = pd.to_datetime(start_str)
            end = pd.to_datetime(end_str)

            # Check if any part of the day overlaps with outage period
            day_start = date.replace(hour=0, minute=0, second=0)
            day_end = date.replace(hour=23, minute=59, second=59)

            if (day_start <= end) and (day_end >= start):
                return f"{start_str} to {end_str}"

        return ""

    def setup_minio_client(self) -> bool:
        """
        Set up MinIO client (mc) for accessing OpenSky bucket.

        Requires OSN_USERNAME and OSN_KEY environment variables.

        Returns:
            True if setup successful, False otherwise
        """
        if not os.path.exists('./mc'):
            print("Downloading MinIO client...")
            self._execute_shell_command('curl -O https://dl.min.io/client/mc/release/linux-amd64/mc')
            self._execute_shell_command('chmod +x mc')

        username = os.getenv('OSN_USERNAME')
        key = os.getenv('OSN_KEY')

        if not username or not key:
            print("WARNING: OSN_USERNAME or OSN_KEY environment variables not set")
            return False

        stdout, stderr = self._execute_shell_command(
            f'./mc alias set opensky https://s3.opensky-network.org {username} {key}'
        )
        if stderr and 'error' in stderr.lower():
            print(f"Error setting up MinIO client: {stderr}")
            return False

        return True

    def get_minio_files_for_date(self, date: pd.Timestamp) -> Dict[str, any]:
        """
        Query MinIO bucket for files available on a specific date.

        Args:
            date: Date to query

        Returns:
            Dictionary with 'file_count' and 'hours' (list of hours with data)
        """
        date_str = date.strftime('%Y-%m-%d')

        # Query MinIO for files matching the date pattern
        command = f'./mc ls opensky/ec-datadump/ --recursive | grep "states_{date_str}"'
        stdout, stderr = self._execute_shell_command(command)

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

    def analyze_missing_data(self, df: pd.DataFrame, check_minio: bool = True) -> pd.DataFrame:
        """
        Analyze missing or suspicious data and add diagnostic columns.

        Args:
            df: DataFrame with date and row_count columns
            check_minio: Whether to check MinIO bucket for file availability

        Returns:
            DataFrame with additional diagnostic columns:
            - is_suspicious: bool
            - known_outage: str
            - minio_file_count: int
            - minio_hours_available: str
            - explanation: str
        """
        print("\nAnalyzing data quality...")

        # Add analysis columns
        df['is_suspicious'] = (df['row_count'] < self.suspicious_threshold)
        df['known_outage'] = df['date'].apply(self.check_known_outage)
        df['minio_file_count'] = 0
        df['minio_hours_available'] = ""

        # Setup MinIO if needed
        mc_available = False
        if check_minio:
            mc_available = self.setup_minio_client()
            if not mc_available:
                print("MinIO client not available - skipping bucket checks")

        # Analyze suspicious dates
        suspicious_dates = df[df['is_suspicious']]
        print(f"Found {len(suspicious_dates)} suspicious dates (row_count < {self.suspicious_threshold:,})")

        if mc_available and len(suspicious_dates) > 0:
            print("Checking MinIO bucket for suspicious dates...")
            for idx, row in suspicious_dates.iterrows():
                date = row['date']
                minio_info = self.get_minio_files_for_date(date)
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
            elif row['row_count'] < self.suspicious_threshold:
                explanations.append(f"Low count ({row['row_count']:,})")

            if row['known_outage']:
                explanations.append(f"Known outage: {row['known_outage']}")

            if row['minio_file_count'] > 0:
                explanations.append(
                    f"MinIO: {row['minio_file_count']} files, hours: {row['minio_hours_available']}"
                )
            elif mc_available:
                explanations.append("MinIO: No files found")

            return "; ".join(explanations) if explanations else "Suspicious"

        df['explanation'] = df.apply(create_explanation, axis=1)

        print(f"Analysis complete. {len(df[df['known_outage'] != ''])} dates affected by known outages.")

        return df

    def visualize_daily_counts(
        self,
        df: pd.DataFrame,
        output_path: str,
        title: str = "Daily Row Counts - OpenSky Network State Vectors"
    ) -> None:
        """
        Create and save interactive Plotly visualization of daily row counts.

        Args:
            df: DataFrame with date, row_count, and analysis columns
            output_path: Path to save the HTML plot
            title: Chart title

        Example:
            >>> collector.visualize_daily_counts(
            ...     df,
            ...     "daily_counts.html",
            ...     "Daily Counts - State Vectors"
            ... )
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
            y=self.suspicious_threshold,
            line_dash="dash",
            line_color="red",
            annotation_text=f"Suspicious Threshold ({self.suspicious_threshold:,.0f})",
            annotation_position="right"
        )

        fig.update_layout(
            title={
                'text': f'{title}<br><sub>Color: Blue=OK, Yellow=Suspicious, Orange=Known Outage, Red=No Data</sub>',
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
        self._print_summary_statistics(df)

    def _print_summary_statistics(self, df: pd.DataFrame) -> None:
        """Print summary statistics for data quality analysis."""
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

    def _execute_shell_command(self, command: str) -> Tuple[str, str]:
        """
        Execute a shell command and return stdout and stderr.

        Args:
            command: Shell command to execute

        Returns:
            (stdout, stderr) as strings
        """
        process = subprocess.Popen(
            command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()
        return stdout.decode().strip(), stderr.decode().strip()

    def generate_quality_report(
        self,
        table_name: str,
        output_csv: str,
        output_html: str,
        date_column: str = "event_time",
        check_minio: bool = True,
    ) -> pd.DataFrame:
        """
        Generate complete data quality report.

        Args:
            table_name: Table to analyze (without project prefix)
            output_csv: Path to save CSV report
            output_html: Path to save HTML visualization
            date_column: Column to use for date grouping
            check_minio: Whether to check MinIO bucket

        Returns:
            DataFrame with complete analysis

        Example:
            >>> collector = AdvancedStatsCollector(spark, config)
            >>> df = collector.generate_quality_report(
            ...     "osn_statevectors_v2",
            ...     "daily_counts.csv",
            ...     "daily_counts.html"
            ... )
        """
        # Fetch daily counts
        df = self.fetch_daily_row_counts(table_name, date_column)

        # Fill missing dates
        df = self.fill_missing_dates(df)

        # Analyze data quality
        df = self.analyze_missing_data(df, check_minio=check_minio)

        # Save CSV
        df.to_csv(output_csv, index=False)
        print(f"\nData saved to: {output_csv}")

        # Create visualization
        self.visualize_daily_counts(df, output_html)

        return df
