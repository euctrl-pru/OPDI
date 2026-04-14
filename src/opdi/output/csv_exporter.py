"""
CSV export and data cleanup module.

Provides deduplication and export functionality for converting
parquet files to compressed CSV format.
"""

import os
import glob
from typing import Optional
import pandas as pd


class CSVExporter:
    """
    Exports parquet files to CSV.gz format with deduplication.

    This class handles the final data cleanup step, removing duplicate
    rows and exporting data in both parquet and compressed CSV formats.
    """

    def __init__(
        self,
        input_dir: str,
        output_dir: str,
        size_threshold_mb: int = 200,
    ):
        """
        Initialize CSV exporter.

        Args:
            input_dir: Directory containing input parquet files
            output_dir: Directory for output files
            size_threshold_mb: Skip files larger than this size (MB)
        """
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.size_threshold_mb = size_threshold_mb
        self.size_threshold_bytes = size_threshold_mb * 1024 * 1024

        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

    def _should_skip_file(self, base_filename: str) -> bool:
        """
        Check if file should be skipped based on existing output file sizes.

        Args:
            base_filename: Base filename without extension

        Returns:
            True if both output files exist and are larger than threshold
        """
        parquet_path = os.path.join(self.output_dir, f"{base_filename}.parquet")
        csv_path = os.path.join(self.output_dir, f"{base_filename}.csv.gz")

        if os.path.exists(parquet_path) and os.path.exists(csv_path):
            parquet_size = os.path.getsize(parquet_path)
            csv_size = os.path.getsize(csv_path)

            if (parquet_size > self.size_threshold_bytes and
                csv_size > self.size_threshold_bytes):
                return True

        return False

    def clean_and_export_file(self, input_file: str) -> tuple[int, int]:
        """
        Clean and export a single parquet file.

        Args:
            input_file: Path to input parquet file

        Returns:
            Tuple of (original_rows, cleaned_rows)
        """
        # Extract base filename
        base_filename = os.path.basename(input_file).split(".")[0]

        # Check if should skip
        if self._should_skip_file(base_filename):
            print(
                f"Skipping {input_file} - cleaned files exist and are "
                f"larger than {self.size_threshold_mb}MB"
            )
            return (0, 0)

        print(f"Processing {input_file}...")

        # Read parquet file
        df = pd.read_parquet(input_file)
        original_rows = len(df)

        # Remove duplicates
        df.drop_duplicates(inplace=True)
        cleaned_rows = len(df)

        duplicates_removed = original_rows - cleaned_rows
        if duplicates_removed > 0:
            print(f"  Removed {duplicates_removed:,} duplicate rows")

        # Output paths
        parquet_output = os.path.join(self.output_dir, f"{base_filename}.parquet")
        csv_output = os.path.join(self.output_dir, f"{base_filename}.csv.gz")

        # Write outputs
        df.to_parquet(parquet_output)
        df.to_csv(csv_output, index=False, compression="gzip")

        print(f"  Exported: {cleaned_rows:,} rows")
        print(f"    → {parquet_output}")
        print(f"    → {csv_output}")

        return (original_rows, cleaned_rows)

    def clean_and_export_all(self) -> dict:
        """
        Clean and export all parquet files in input directory.

        Returns:
            Dictionary with statistics:
            {
                'files_processed': int,
                'files_skipped': int,
                'total_original_rows': int,
                'total_cleaned_rows': int,
                'duplicates_removed': int
            }

        Example:
            >>> exporter = CSVExporter(
            ...     "data/measurements",
            ...     "data/measurements_clean"
            ... )
            >>> stats = exporter.clean_and_export_all()
            >>> print(f"Processed {stats['files_processed']} files")
            >>> print(f"Removed {stats['duplicates_removed']} duplicates")
        """
        # Get all parquet files
        parquet_files = glob.glob(os.path.join(self.input_dir, "*.parquet"))

        if not parquet_files:
            print(f"No parquet files found in {self.input_dir}")
            return {
                "files_processed": 0,
                "files_skipped": 0,
                "total_original_rows": 0,
                "total_cleaned_rows": 0,
                "duplicates_removed": 0,
            }

        print(f"Found {len(parquet_files)} parquet files")
        print(f"Input directory: {self.input_dir}")
        print(f"Output directory: {self.output_dir}")
        print(f"Size threshold: {self.size_threshold_mb} MB\n")

        files_processed = 0
        files_skipped = 0
        total_original = 0
        total_cleaned = 0

        for file in parquet_files:
            original, cleaned = self.clean_and_export_file(file)

            if original == 0 and cleaned == 0:
                files_skipped += 1
            else:
                files_processed += 1
                total_original += original
                total_cleaned += cleaned

        duplicates_removed = total_original - total_cleaned

        # Print summary
        print("\n" + "=" * 60)
        print("CLEANUP SUMMARY")
        print("=" * 60)
        print(f"Files processed: {files_processed}")
        print(f"Files skipped: {files_skipped}")
        print(f"Total original rows: {total_original:,}")
        print(f"Total cleaned rows: {total_cleaned:,}")
        print(f"Duplicates removed: {duplicates_removed:,}")
        print("=" * 60)

        return {
            "files_processed": files_processed,
            "files_skipped": files_skipped,
            "total_original_rows": total_original,
            "total_cleaned_rows": total_cleaned,
            "duplicates_removed": duplicates_removed,
        }


def clean_and_save_data(input_dir: str, output_dir: str, size_threshold_mb: int = 200) -> None:
    """
    Standalone function to clean and export parquet files.

    Args:
        input_dir: Directory containing parquet files
        output_dir: Directory for cleaned output
        size_threshold_mb: Skip files larger than this (MB)

    Example:
        >>> from opdi.output.csv_exporter import clean_and_save_data
        >>> clean_and_save_data("data/measurements", "data/measurements_clean")
    """
    exporter = CSVExporter(input_dir, output_dir, size_threshold_mb)
    exporter.clean_and_export_all()
