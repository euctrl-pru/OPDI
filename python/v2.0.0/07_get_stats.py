import time
from pyspark.sql import SparkSession


# ---------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------
PROJECT = "project_opdi"

TABLES = [
    "project_opdi.opdi_flight_list",
    "project_opdi.opdi_flight_events",
    "project_opdi.opdi_measurements",
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
def print_table_counts(spark_session: SparkSession, tables: list[str]) -> None:
    """
    Print record counts for a list of tables.

    Parameters
    ----------
    spark_session : SparkSession
        Active Spark session.
    tables : list[str]
        Fully qualified table names.
    """
    print("\nRow counts per table\n" + "-" * 40)

    for table in tables:
        count = spark_session.sql(
            f"SELECT COUNT(*) AS cnt FROM {table}"
        ).collect()[0]["cnt"]

        print(f"{table:<45} {count:>12,}")

    print("-" * 40)
    
# ---------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------
if __name__ == "__main__":
    start_time = time.time()

    print_table_counts(spark, TABLES)

    elapsed = time.time() - start_time
    print(f"\nCompleted in {elapsed:.2f} seconds\n")
