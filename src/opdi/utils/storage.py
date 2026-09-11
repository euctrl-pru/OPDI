"""
Storage abstraction layer for OPDI pipeline.

Provides environment-aware table read/write operations. In Iceberg/Hive
mode (dev, live) tables are managed via the catalog. In S3 mode (opensky)
tables are stored as parquet directories on S3.
"""

from typing import Optional, Sequence

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

    WRITE_MODES = ("append", "overwrite", "insert_overwrite")

    def write_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str,
        partition_by: Optional[Sequence[str]] = None,
    ) -> None:
        """
        Write *df* to *table_name*.

        Args:
            df: Spark DataFrame to persist.
            table_name: Bare table name (no project prefix).
            mode: ``"append"`` | ``"overwrite"`` | ``"insert_overwrite"``
                  (the last one maps to Hive-style insertInto).
            partition_by: Columns to partition the written table by. When set
                **and** ``mode`` is ``"overwrite"``, only the partitions present
                in *df* are replaced; everything else in the table is left
                alone. That is what makes publishing one day at a time possible
                -- see the note below.

        ``mode`` is **required and has no default**. It used to default to
        ``"append"``, which meant a caller that had not thought about
        re-processing got the option that silently doubles a table: nothing
        errors, the rows are simply counted twice, and every aggregate reading
        them is wrong by an amount nothing reports. Three separate places in
        this repo -- ``rebuild_sample.py``, ``benchmarks/clean_tracks.py`` and
        the V7 chain -- carry a workaround for that default. Requiring the
        argument is what stops the *next* call site needing a fourth.

        .. warning::

           ``insert_overwrite`` does **not** mean "replace this month".
           In Iceberg mode it overwrites the partitions the DataFrame touches,
           and in S3 mode it overwrites the entire parquet directory. For a
           table partitioned by something other than time -- as
           ``opdi_flight_events`` is, by ``(type, version)`` -- it therefore
           destroys every other month. Re-processing safely needs the month to
           be a partition, which is a layout change to published data, or a
           read-filter-rewrite of the whole table. See
           ``benchmarks/EVENTS_RUN_LOG.md`` decision 15.

           ``partition_by`` is the way out of that, and it is why this argument
           exists: with the day as a partition column, ``mode="overwrite"``
           replaces only the days *df* contains. Spark's default is the
           opposite -- ``static`` partition overwrite deletes the whole table
           before writing -- so the writer sets ``partitionOverwriteMode`` to
           ``dynamic`` explicitly rather than relying on a session setting that
           some other caller may not have made. Scoped to this write, so it
           cannot change the behaviour of anything else in the session.
        """
        if mode not in self.WRITE_MODES:
            # Previously an unrecognised mode fell through every branch in the
            # Iceberg path and wrote nothing at all, reporting success. A typo
            # in a mode string was a silent no-op.
            raise ValueError(
                f"Unknown write mode {mode!r} for table {table_name!r}; "
                f"expected one of {', '.join(self.WRITE_MODES)}."
            )

        if partition_by:
            # Checked before the write, not discovered by it. Spark's
            # partitionBy on an absent column raises deep inside the job with a
            # message that names the analysis plan rather than the mistake, and
            # a caller who typo'd a column would otherwise be one silent
            # unpartitioned write away from having the next overwrite delete
            # every day at once.
            missing = [c for c in partition_by if c not in df.columns]
            if missing:
                raise ValueError(
                    f"partition column(s) {missing} not in the DataFrame for "
                    f"table {table_name!r}; it has {df.columns}."
                )

        if self.use_s3:
            s3_mode = "overwrite" if mode in ("overwrite", "insert_overwrite") else "append"
            writer = df.write.mode(s3_mode)
            if partition_by:
                writer = writer.partitionBy(*partition_by).option(
                    "partitionOverwriteMode", "dynamic"
                )
            writer.parquet(self._s3_path(table_name))
        else:
            qualified = f"`{self.project}`.`{table_name}`"
            if mode == "append":
                df.writeTo(qualified).append()
            elif mode == "overwrite":
                # overwritePartitions replaces only the partitions the frame
                # touches; plain overwrite replaces the table. The Iceberg
                # equivalent of the dynamic mode set above.
                if partition_by:
                    df.writeTo(qualified).overwritePartitions()
                else:
                    df.writeTo(qualified).overwrite()
            else:
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
