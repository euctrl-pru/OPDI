"""
Storage abstraction layer for OPDI pipeline.

Provides environment-aware table read/write operations. In Iceberg/Hive
mode (dev, live) tables are managed via the catalog. In S3 mode (opensky)
tables are stored as parquet directories on S3.
"""

from pyspark.sql import SparkSession, DataFrame
from opdi.config import OPDIConfig


class StorageManager:
    """
    Abstracts table I/O so pipeline steps work against both
    Iceberg catalogs and plain S3 parquet directories.
    """

    def __init__(self, spark: SparkSession, config: OPDIConfig):
        self.spark = spark
        self.config = config
        self.project = config.project.project_name
        self.use_s3 = not config.spark.enable_iceberg
        self.base_path = config.project.warehouse_path
        self._registered_views: set = set()

    def _s3_path(self, table_name: str) -> str:
        return f"{self.base_path}/{table_name}"

    def table_ref(self, table_name: str) -> str:
        """
        Return a reference usable in ``spark.sql()`` queries.

        * Iceberg mode: returns backtick-quoted ```project`.`table```
        * S3 mode: reads parquet from S3, registers a temp view, returns the view name
        """
        if self.use_s3:
            if table_name not in self._registered_views:
                df = self.spark.read.parquet(self._s3_path(table_name))
                df.createOrReplaceTempView(table_name)
                self._registered_views.add(table_name)
            return table_name
        return f"`{self.project}`.`{table_name}`"

    def read_table(self, table_name: str) -> DataFrame:
        """Read a full table as a DataFrame."""
        if self.use_s3:
            return self.spark.read.parquet(self._s3_path(table_name))
        return self.spark.table(f"{self.project}.{table_name}")

    def write_table(
        self, df: DataFrame, table_name: str, mode: str = "append"
    ) -> None:
        """
        Write *df* to *table_name*.

        Args:
            df: Spark DataFrame to persist.
            table_name: Bare table name (no project prefix).
            mode: ``"append"`` | ``"overwrite"`` | ``"insert_overwrite"``
                  (the last one maps to Hive-style insertInto).
        """
        if self.use_s3:
            s3_mode = "overwrite" if mode in ("overwrite", "insert_overwrite") else "append"
            df.write.mode(s3_mode).parquet(self._s3_path(table_name))
        else:
            qualified = f"`{self.project}`.`{table_name}`"
            if mode == "append":
                df.writeTo(qualified).append()
            elif mode == "overwrite":
                df.writeTo(qualified).overwrite()
            elif mode == "insert_overwrite":
                df.write.mode("overwrite").insertInto(f"{self.project}.{table_name}")

    def create_table(self, sql: str) -> None:
        """Run DDL (CREATE TABLE). No-op in S3 mode."""
        if not self.use_s3:
            self.spark.sql(sql)

    def table_exists(self, table_name: str) -> bool:
        if self.use_s3:
            try:
                self.spark.read.parquet(self._s3_path(table_name)).schema
                return True
            except Exception:
                return False
        try:
            self.spark.sql(f"DESCRIBE `{self.project}`.`{table_name}`")
            return True
        except Exception:
            return False
