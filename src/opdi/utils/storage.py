"""Reading and writing the pipeline's tables.

:class:`StorageManager` gives every step the same table operations whatever
the storage: Iceberg tables in a Spark catalog (the ``local`` environment by
default), or plain Parquet folders under the release root (the ``opensky``
environment, or ``spark.enable_iceberg = False``). It also keeps the
progress records of a run and the coverage marker of a warehouse.
"""

import json
import os
import time
from typing import Any, Mapping, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from opdi.config import OPDIConfig

#: Local folder for the progress records when the tables are in a catalog
#: rather than in a warehouse folder.
LOCAL_STATE_DIR = "OPDI_live/state"


#: The folder name Hive, and so Spark, gives to the rows whose partition
#: column is NULL.
HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__"


def _partition_dir_value(value):
    """The folder name Spark writes a partition value to.

    A NULL becomes ``col=__HIVE_DEFAULT_PARTITION__``, not ``col=None``. A
    delete that built the path naively would find nothing, and the append
    after it would add a second copy of the rows it meant to replace.
    """
    return HIVE_DEFAULT_PARTITION if value is None else value


class StorageManager:
    """Table operations for the pipeline steps, on Iceberg or on Parquet folders.

    Args:
        spark: Active Spark session.
        config: OPDI configuration; ``config.spark.enable_iceberg`` chooses
            the storage and ``config.project.release_root`` the folder.
    """

    def __init__(self, spark: SparkSession, config: OPDIConfig):
        self.spark = spark
        self.config = config
        self.project = config.project.project_name
        self.use_s3 = not config.spark.enable_iceberg
        self.base_path = config.project.release_root
        self._registered_views: set = set()

    def _s3_path(self, table_name: str) -> str:
        return f"{self.base_path}/{table_name}"

    def table_ref(self, table_name: str) -> str:
        """A name of the table for ``spark.sql()`` queries.

        With Iceberg, ```project`.`table```. With Parquet folders, the table is
        read and registered as a temporary view, whose name is returned.
        """
        if self.use_s3:
            if table_name not in self._registered_views:
                df = self._read_parquet(table_name)
                df.createOrReplaceTempView(table_name)
                self._registered_views.add(table_name)
            return table_name
        return f"`{self.project}`.`{table_name}`"

    #: Tables whose day folders may differ in their columns. Days of the
    #: flight list can lack ``ADEP_SOURCE`` and ``ADES_SOURCE`` in the rows of
    #: one of the two passes of step 03. These tables are read with the
    #: columns of all files merged, so a column missing from one file reads as
    #: NULL there instead of being dropped from the whole read.
    MERGE_SCHEMA_TABLES = ("opdi_flight_list",)

    def _read_parquet(self, table_name: str) -> DataFrame:
        reader = self.spark.read
        if table_name in self.MERGE_SCHEMA_TABLES:
            reader = reader.option("mergeSchema", "true")
        return reader.parquet(self._s3_path(table_name))

    def read_table(self, table_name: str) -> DataFrame:
        """Read a full table as a DataFrame."""
        if self.use_s3:
            return self._read_parquet(table_name)
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
        """Write ``df`` to ``table_name``.

        Args:
            df: Spark DataFrame to write.
            table_name: Table name, without the project prefix.
            mode: ``"append"``, ``"overwrite"`` or ``"insert_overwrite"`` (Hive
                ``insertInto``). Required, with no default, so that every
                caller decides whether a second run adds rows or replaces them.
            partition_by: Columns to partition the table by. With
                ``mode="overwrite"``, only the partitions present in ``df``
                are replaced and every other partition is kept, which is what
                lets one day be written at a time.
            partition_values: The partition values ``df`` holds, when the
                caller knows them (for example the day being processed); saves
                computing ``df`` once to find them.

        Raises:
            ValueError: If ``mode`` is unknown, or a ``partition_by`` column
                is not in ``df``.

        .. warning::

           ``insert_overwrite`` replaces the partitions ``df`` touches (Iceberg)
           or the whole folder (Parquet), not "this month". For a table
           partitioned by something other than time it therefore removes
           other months too. Use ``partition_by`` with ``mode="overwrite"``
           to replace days.
        """
        if mode not in self.WRITE_MODES:
            # An unknown mode would otherwise write nothing and report success.
            raise ValueError(
                f"Unknown write mode {mode!r} for table {table_name!r}; "
                f"expected one of {', '.join(self.WRITE_MODES)}."
            )

        if partition_by:
            # Checked before the write: Spark would fail deep inside the job,
            # with a message about its plan rather than the missing column.
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
                # Spark's dynamic partition overwrite cannot be used: the S3A
                # "magic" committer, which writes to S3 without renames,
                # refuses it ("PathOutputCommitter does not support
                # dynamicPartitionOverwrite").
                #
                # Deleting the folders and appending is what dynamic overwrite
                # does, without its atomicity: a failure between the delete
                # and the write leaves that day missing. The day is then run
                # again, as the state file does not mark it done. Appending
                # without deleting would double the day instead.
                persisted = self._delete_partitions(df, path, partition_by, partition_values)
                s3_mode = "append"
            else:
                persisted = False

            writer = df.write.mode(s3_mode)
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            writer.parquet(path)
            if persisted:
                # Released after the write, which is what it was kept for.
                df.unpersist()
        else:
            qualified = f"`{self.project}`.`{table_name}`"
            if mode == "append":
                df.writeTo(qualified).append()
            elif mode == "overwrite":
                # overwritePartitions replaces only the partitions the frame
                # touches; plain overwrite replaces the table.
                if partition_by:
                    df.writeTo(qualified).overwritePartitions()
                else:
                    # DataFrameWriterV2.overwrite() needs a condition;
                    # lit(True) replaces everything, which is what an
                    # unpartitioned overwrite means.
                    df.writeTo(qualified).overwrite(F.lit(True))
            else:
                df.write.mode("overwrite").insertInto(f"{self.project}.{table_name}")

    def _delete_partitions(self, df: DataFrame, path: str, partition_by, values=None) -> bool:
        """Remove the partition folders ``df`` is about to write.

        Only the partitions present in ``df`` are removed. A day that
        produced no rows is not removed: there is nothing to replace it with.

        Returns:
            True when ``df`` was persisted to find its partitions (the caller
            releases it after the write).
        """
        if values is not None:
            # The caller knows the values, so the frame is not computed to
            # find them.
            self._delete_partition_paths(path, partition_by, values)
            return False

        # Persisted first, because finding the partitions computes the frame,
        # and the write would otherwise compute it a second time. With
        # MEMORY_AND_DISK, a frame too large for memory is kept on disk
        # rather than computed again.
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
            parts = "/".join(f"{c}={_partition_dir_value(row[c])}" for c in partition_by)
            target = jvm.org.apache.hadoop.fs.Path(f"{path}/{parts}")
            fs = target.getFileSystem(hconf)
            if fs.exists(target):
                fs.delete(target, True)

    def list_partitions(self, table_name: str, column: str) -> list:
        """The partition values present, as strings, read from the folder names.

        One folder listing, no data read. Empty when the table does not exist
        yet. Parquet folders only.
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
        """Remove the partitions ``values`` of ``column``; returns how many."""
        if not values:
            return 0
        path = self._s3_path(table_name)
        self._delete_partition_paths(path, [column], [{column: v} for v in values])
        return len(values)

    def create_table(self, sql: str) -> None:
        """Run a ``CREATE TABLE`` statement. Does nothing with Parquet folders."""
        if not self.use_s3:
            self.spark.sql(sql)

    def table_exists(self, table_name: str) -> bool:
        """Whether the table exists and can be read."""
        if self.use_s3:
            try:
                self.spark.read.parquet(self._s3_path(table_name)).schema  # noqa: B018
                return True
            except Exception:
                return False
        try:
            self.spark.sql(f"DESCRIBE `{self.project}`.`{table_name}`")
            return True
        except Exception:
            return False

    COVERAGE_MARKER = "coverage_marker"
    """Name of the one-row table that records which coverage wrote a warehouse.

    Not ``_coverage_marker``: Spark skips paths whose name starts with ``_``
    or ``.``, so such a table could look absent and the check would never
    refuse anything.
    """

    def write_coverage_marker(self) -> None:
        """Record the coverage of this warehouse (worldwide or European).

        Overwrites: the coverage is a property of the warehouse, not a history
        of runs.
        """
        # With Iceberg, write_table's overwrite expects the table to exist, so
        # it is created first (nothing happens with Parquet folders).
        self.create_table(f"""
            CREATE TABLE IF NOT EXISTS `{self.project}`.`{self.COVERAGE_MARKER}` (
              coverage STRING COMMENT 'Coverage of this warehouse: europe, worldwide or custom',
              bbox ARRAY<DOUBLE> COMMENT 'min_lon, min_lat, max_lon, max_lat; NULL for worldwide',
              airport_types ARRAY<STRING> COMMENT 'Airport types of the reference tables'
            )
            USING iceberg
            COMMENT 'One row: the coverage that wrote this warehouse.'
            """)
        cov = self.config.coverage
        df = self.spark.createDataFrame(
            [
                (
                    cov.label,
                    [float(x) for x in cov.bbox] if cov.bbox else None,
                    list(cov.airport_types),
                )
            ],
            "coverage string, bbox array<double>, airport_types array<string>",
        )
        self.write_table(df, self.COVERAGE_MARKER, mode="overwrite")

    def read_coverage_marker(self) -> Optional[dict]:
        """The coverage marker, or ``None`` if this warehouse has none.

        A marker that is absent, unreadable or empty reads as none, so that a
        read error never blocks a run.
        """
        try:
            rows = self.read_table(self.COVERAGE_MARKER).limit(1).collect()
        except Exception:
            return None
        if not rows:
            return None
        r = rows[0]
        return {
            "coverage": r["coverage"],
            "bbox": list(r["bbox"]) if r["bbox"] is not None else None,
            "airport_types": list(r["airport_types"]),
        }

    def check_coverage(self) -> Optional[str]:
        """A message when this run's coverage differs from the warehouse's, else ``None``.

        A warehouse without a marker (a first run) is not a problem.
        """
        marker = self.read_coverage_marker()
        if marker is None:
            return None
        cov = self.config.coverage
        if marker["coverage"] == cov.label and marker["bbox"] == (
            [float(x) for x in cov.bbox] if cov.bbox else None
        ):
            return None
        return (
            f"coverage mismatch at {self.base_path}: the warehouse was written "
            f"with coverage {marker['coverage']!r} (bbox {marker['bbox']}) and "
            f"this run is {cov.label!r} (bbox "
            f"{list(cov.bbox) if cov.bbox else None}). The rows of two coverages "
            "cannot be told apart once they are in one warehouse, and the "
            "flight identifiers of one coverage do not match those of the "
            "other. Point this run at a different warehouse, or delete the "
            "existing tables if they are no longer wanted."
        )

    # --- progress records -----------------------------------------------------
    #
    # The pipeline records which (step, day) units are finished, and its steps
    # keep small progress logs. Both are kept in the warehouse, under
    # ``<release root>/_state/``, so that any machine that runs the next unit
    # sees what the previous one did. The folder name starts with an
    # underscore, so Spark never reads it as a table.

    @property
    def state_path(self) -> str:
        """The folder of the progress records.

        ``<release root>/_state`` when the tables are in a warehouse folder
        (the ``opensky`` environment), otherwise the local folder
        :data:`LOCAL_STATE_DIR`.
        """
        if self.use_s3:
            return f"{self.base_path.rstrip('/')}/_state"
        return "file://" + os.path.abspath(LOCAL_STATE_DIR)

    def _hadoop_path(self, path: str):
        jvm = self.spark._jvm
        p = jvm.org.apache.hadoop.fs.Path(path)
        return p.getFileSystem(self.spark._jsc.hadoopConfiguration()), p

    def _done_path(self, key: str) -> str:
        if "/" in key or not key:
            raise ValueError(f"a progress key must be a non-empty name without '/': {key!r}")
        return f"{self.state_path}/done/{key}.json"

    def mark_done(self, key: str, seconds: float = 0.0) -> None:
        """Record that the unit ``key`` (for example ``02@2026-06-14``) is finished.

        Args:
            key: Name of the unit.
            seconds: How long the unit took, in seconds.
        """
        fs, p = self._hadoop_path(self._done_path(key))
        out = fs.create(p, True)
        try:
            body = json.dumps(
                {
                    "seconds": round(float(seconds), 1),
                    "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }
            )
            out.write(bytearray(body.encode("utf-8")))
        finally:
            out.close()

    def is_done(self, key: str) -> bool:
        """Whether the unit ``key`` has been recorded as finished."""
        fs, p = self._hadoop_path(self._done_path(key))
        return bool(fs.exists(p))

    def done_keys(self) -> list:
        """The names of every unit recorded as finished, for example ``02@2026-06-14``."""
        fs, folder = self._hadoop_path(f"{self.state_path}/done")
        if not fs.exists(folder):
            return []
        names = []
        for status in fs.listStatus(folder):
            name = status.getPath().getName()
            if status.isFile() and name.endswith(".json"):
                names.append(name[: -len(".json")])
        return sorted(names)

    def clear_done(self, key: str) -> None:
        """Forget that the unit ``key`` was finished, so that it runs again."""
        fs, p = self._hadoop_path(self._done_path(key))
        if fs.exists(p):
            fs.delete(p, False)

    def _logs_path(self) -> str:
        return f"{self.state_path}/logs"

    def pull_logs(self, local_dir: str) -> int:
        """Copy the progress logs kept in the warehouse into ``local_dir``.

        The steps read and write their progress logs as local files. Call
        this before a step and :meth:`push_logs` after it.

        Returns:
            The number of files copied.
        """
        os.makedirs(local_dir, exist_ok=True)
        fs, folder = self._hadoop_path(self._logs_path())
        if not fs.exists(folder):
            return 0
        jvm = self.spark._jvm
        n = 0
        for status in fs.listStatus(folder):
            if not status.isFile():
                continue
            target = os.path.join(os.path.abspath(local_dir), status.getPath().getName())
            if os.path.exists(target):
                os.remove(target)
            fs.copyToLocalFile(False, status.getPath(), jvm.org.apache.hadoop.fs.Path(target), True)
            n += 1
        return n

    def push_logs(self, local_dir: str) -> int:
        """Copy the progress logs in ``local_dir`` into the warehouse.

        Returns:
            The number of files copied.
        """
        if not os.path.isdir(local_dir):
            return 0
        fs, folder = self._hadoop_path(self._logs_path())
        fs.mkdirs(folder)
        jvm = self.spark._jvm
        n = 0
        for name in sorted(os.listdir(local_dir)):
            source = os.path.join(os.path.abspath(local_dir), name)
            if not os.path.isfile(source) or name.endswith(".crc"):
                continue
            fs.copyFromLocalFile(
                False,
                True,
                jvm.org.apache.hadoop.fs.Path("file://" + source),
                jvm.org.apache.hadoop.fs.Path(f"{self._logs_path()}/{name}"),
            )
            n += 1
        return n
