"""
Basic statistics collection for OPDI tables.

Provides simple row count and basic statistics for monitoring
data pipeline health and completeness.
"""

from typing import List, Dict
from pyspark.sql import SparkSession

from opdi.config import OPDIConfig


class BasicStatsCollector:
    """
    Collects basic statistics for OPDI tables.

    Provides simple row counts and table existence checks for
    monitoring pipeline execution and data availability.
    """

    DEFAULT_TABLES = [
        "opdi_flight_list",
        "opdi_flight_events",
        "opdi_measurements",
    ]

    def __init__(self, spark: SparkSession, config: OPDIConfig):
        """
        Initialize basic stats collector.

        Args:
            spark: Active SparkSession
            config: OPDI configuration object
        """
        self.spark = spark
        self.config = config
        self.project = config.project.project_name

    def get_table_count(self, table_name: str) -> int:
        """
        Get row count for a single table.

        Args:
            table_name: Table name (without project prefix)

        Returns:
            Number of rows in the table

        Raises:
            Exception: If table doesn't exist
        """
        full_table_name = f"{self.project}.{table_name}"
        count = self.spark.sql(f"SELECT COUNT(*) AS cnt FROM {full_table_name}").collect()[0]["cnt"]
        return count

    def get_all_table_counts(self, tables: List[str] = None) -> Dict[str, int]:
        """
        Get row counts for multiple tables.

        Args:
            tables: List of table names. If None, uses DEFAULT_TABLES

        Returns:
            Dictionary mapping table names to row counts

        Example:
            >>> collector = BasicStatsCollector(spark, config)
            >>> counts = collector.get_all_table_counts()
            >>> print(counts)
            {'opdi_flight_list': 1500000, 'opdi_flight_events': 3000000}
        """
        if tables is None:
            tables = self.DEFAULT_TABLES

        results = {}
        for table in tables:
            try:
                count = self.get_table_count(table)
                results[table] = count
            except Exception as e:
                print(f"Error getting count for {table}: {e}")
                results[table] = -1  # Indicate error

        return results

    def print_summary(self, tables: List[str] = None) -> None:
        """
        Print formatted summary of table counts.

        Args:
            tables: List of table names. If None, uses DEFAULT_TABLES

        Example:
            >>> collector = BasicStatsCollector(spark, config)
            >>> collector.print_summary()

            Row counts per table
            ----------------------------------------
            opdi_flight_list                      1,500,000
            opdi_flight_events                    3,000,000
            opdi_measurements                     5,000,000
            ----------------------------------------
        """
        if tables is None:
            tables = self.DEFAULT_TABLES

        print("\nRow counts per table")
        print("-" * 60)

        for table in tables:
            full_table_name = f"{self.project}.{table}"
            try:
                count = self.get_table_count(table)
                print(f"{full_table_name:<45} {count:>12,}")
            except Exception as e:
                print(f"{full_table_name:<45} {'ERROR':>12}")
                print(f"  └─ {str(e)}")

        print("-" * 60)

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists.

        Args:
            table_name: Table name (without project prefix)

        Returns:
            True if table exists, False otherwise
        """
        try:
            full_table_name = f"{self.project}.{table_name}"
            self.spark.sql(f"DESCRIBE {full_table_name}")
            return True
        except Exception:
            return False

    def get_table_schema(self, table_name: str) -> List[tuple]:
        """
        Get schema information for a table.

        Args:
            table_name: Table name (without project prefix)

        Returns:
            List of (column_name, data_type) tuples
        """
        full_table_name = f"{self.project}.{table_name}"
        schema_df = self.spark.sql(f"DESCRIBE {full_table_name}")
        return [(row.col_name, row.data_type) for row in schema_df.collect()]


def print_table_counts(spark: SparkSession, tables: List[str], project: str = "project_opdi") -> None:
    """
    Standalone function to print table counts.

    Args:
        spark: Active SparkSession
        tables: List of fully qualified table names (e.g., "project_opdi.opdi_flight_list")
        project: Project name (default: "project_opdi")

    Example:
        >>> from opdi.monitoring.basic_stats import print_table_counts
        >>> print_table_counts(spark, ["project_opdi.opdi_flight_list"])
    """
    print("\nRow counts per table")
    print("-" * 60)

    for table in tables:
        try:
            count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table}").collect()[0]["cnt"]
            print(f"{table:<45} {count:>12,}")
        except Exception as e:
            print(f"{table:<45} {'ERROR':>12}")
            print(f"  └─ {str(e)}")

    print("-" * 60)
