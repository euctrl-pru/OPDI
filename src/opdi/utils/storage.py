"""
Storage abstraction layer for OPDI pipeline.

Provides environment-aware table read/write operations. In Iceberg/Hive
mode (dev, live) tables are managed via the catalog. In S3 mode (opensky)
tables are stored as parquet directories on S3.
"""

from typing import Any, Mapping, Optional, Sequence

from pyspark.sql import SparkSession, DataFrame
from opdi.config import OPDIConfig


#: What Hive -- and therefore Spark -- calls the directory holding rows whose
#: partition column is NULL.
HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__"


def _partition_dir_value(value):
    """The directory name Spark writes a partition value to.

    A NULL does not become ``col=None``; it becomes
    ``col=__HIVE_DEFAULT_PARTITION__``. Formatting it naively builds a path
    that does not exist, so the delete finds nothing, silently succeeds, and
    the append that follows lands *beside* the rows it was supposed to
    replace -- duplicating every one of them, once per run. That is how a
    flight list with 746 dateless rows came to hold two copies of each after a
    single re-enrichment.
    """
    return HIVE_DEFAULT_PARTITION if value is None else value


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
        partition_values: Optional[Sequence[Mapping[str, Any]]] = None,
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
            path = self._s3_path(table_name)

            if partition_by and s3_mode == "overwrite":
                # Replace the partitions by deleting them, then appending.
                #
                # Spark's own `partitionOverwriteMode=dynamic` is the obvious
                # way to do this and does not work here: the S3A **magic
                # committer**, which this cluster uses so that writes to S3 are
                # correct without a rename, refuses it outright --
                #
                #   java.io.IOException: PathOutputCommitter does not support
                #   dynamicPartitionOverwrite: MagicCommitter{...}
                #
                # It is not a configuration to relax. The magic committer
                # cannot express "replace these prefixes atomically", so Spark
                # declines rather than writing something half-replaced.
                #
                # Deleting the prefixes ourselves and appending is what dynamic
                # overwrite does internally, minus the atomicity. The window
                # matters and is stated rather than hidden: a crash between the
                # delete and the write leaves that day absent. That is
                # recoverable -- the day is simply re-run, and the runner's
                # state file will not have marked it done -- whereas the
                # alternative, appending without deleting, silently doubles the
                # day and nothing reports it.
                persisted = self._delete_partitions(
                    df, path, partition_by, partition_values
                )
                s3_mode = "append"
            else:
                persisted = False

            writer = df.write.mode(s3_mode)
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            writer.parquet(path)
            if persisted:
                # Released only after the write, which is the consumer the
                # persist exists for.
                df.unpersist()
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

    def _delete_partitions(self, df: DataFrame, path: str, partition_by, values=None) -> bool:
        """Remove the Hive-style partition directories *df* is about to write.

        Only the partitions present in *df* are touched; every other day in the
        table is left alone. The values come from the frame itself, so a day
        that produced no rows is not deleted -- which is correct: there is
        nothing to replace it with, and removing it would turn "this run wrote
        nothing" into "this day no longer exists".
        """
        if values is not None:
            # The caller knew, so nothing has to be computed to find out.
            #
            # Probing the frame for its partition values evaluates it, and the
            # write then evaluates it again. Persisting instead of probing only
            # trades one cost for another: measured, it bought 4% on step 04
            # and cost 8% on step 02a, whose output is 3.7 GB and cheap to
            # recompute. Every day-scoped write already knows its day, so the
            # honest fix is to pass it rather than to rediscover it.
            self._delete_partition_paths(path, partition_by, values)
            return False

        # Persisted first, because this collect evaluates the frame.
        #
        # Asking which partitions a frame will write means computing the frame.
        # Without persisting, the subsequent .parquet() computes it a *second*
        # time -- the whole step, twice. Measured on step 04: stages 80, 81 and
        # 82 were 71% of its 62 minutes, all three attributed to this line, all
        # three duplicates of work the write then repeated.
        #
        # MEMORY_AND_DISK rather than the default MEMORY_ONLY: a frame too big
        # for memory would otherwise be silently recomputed, which is the exact
        # failure this is here to remove.
        from pyspark import StorageLevel

        df.persist(StorageLevel.MEMORY_AND_DISK)
        values = df.select(*partition_by).distinct().collect()
        if not values:
            return True

        self._delete_partition_paths(path, partition_by, values)
        return True

    def _delete_partition_paths(self, path, partition_by, values) -> None:
        """Remove one Hive-style directory per partition value."""
        jvm = self.spark._jvm
        hconf = self.spark._jsc.hadoopConfiguration()
        for row in values:
            parts = "/".join(
                f"{c}={_partition_dir_value(row[c])}" for c in partition_by
            )
            target = jvm.org.apache.hadoop.fs.Path(f"{path}/{parts}")
            fs = target.getFileSystem(hconf)
            if fs.exists(target):
                fs.delete(target, True)

    def list_partitions(self, table_name: str, column: str) -> list:
        """The partition values currently present, as strings.

        Reads the directory names rather than the data: a Hive-style layout
        encodes the value in the path, so this costs one listing and no scan.
        Returns an empty list when the table does not exist yet, which is the
        normal state on the first day of a campaign.
        """
        if not self.use_s3:
            raise NotImplementedError("partition listing is S3-mode only")
        jvm = self.spark._jvm
        hconf = self.spark._jsc.hadoopConfiguration()
        root = jvm.org.apache.hadoop.fs.Path(self._s3_path(table_name))
        fs = root.getFileSystem(hconf)
        if not fs.exists(root):
            return []
        out = []
        for st in fs.listStatus(root):
            name = st.getPath().getName()
            if st.isDirectory() and name.startswith(f"{column}="):
                out.append(name.split("=", 1)[1])
        return sorted(out)

    def drop_partitions(self, table_name: str, column: str, values) -> int:
        """Delete specific partitions. Returns how many were removed."""
        if not values:
            return 0
        path = self._s3_path(table_name)
        self._delete_partition_paths(path, [column], [{column: v} for v in values])
        return len(values)

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
