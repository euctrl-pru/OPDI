"""Build a reference table from many small pandas frames without holding them all.

Steps 00b (airport layouts) and 00f (runway grid) work through the airports
one at a time on the driver and get one pandas frame per airport. For the
whole world these frames do not fit in the driver's memory together, and a
build that stops near the end would lose everything. :class:`StagedTable`
writes them to a building table in batches and moves the result to the final
table in one Spark job at the end.
"""

from typing import List, Optional

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

#: Rows collected on the driver before a batch is written.
BATCH_ROWS = 400_000

#: Number of files of a finished table.
TABLE_FILES = 8


class StagedTable:
    """A table built in batches in ``<table>_building`` and finished in one write.

    The final table is written only by :meth:`finish`, so it is either absent
    (or the previous one) or complete: a build that stops part way never
    leaves a table that looks finished. What was written before the stop stays
    in the building table, and the next build continues after it
    (:meth:`done_keys`).

    Args:
        spark: The Spark session.
        storage: A :class:`opdi.utils.storage.StorageManager`.
        table: Name of the final table.
        schema: Schema of the rows.
        key: Column naming the unit a frame belongs to (the airport).
        batch_rows: Rows collected before a batch is written.
        files: Number of files of the final table.
    """

    def __init__(
        self,
        spark: SparkSession,
        storage,
        table: str,
        schema: StructType,
        key: str,
        batch_rows: int = BATCH_ROWS,
        files: int = TABLE_FILES,
    ):
        self.spark = spark
        self.storage = storage
        self.table = table
        self.building = f"{table}_building"
        self.schema = schema
        self.key = key
        self.batch_rows = batch_rows
        self.files = files
        self._frames: List[pd.DataFrame] = []
        self._rows = 0
        self._has_batches: Optional[bool] = None

    def _building_exists(self) -> bool:
        if self._has_batches is None:
            self._has_batches = bool(self.storage.table_exists(self.building))
        return self._has_batches

    def done_keys(self) -> List[str]:
        """The keys whose rows an earlier, unfinished build already wrote."""
        if not self._building_exists():
            return []
        rows = self.storage.read_table(self.building).select(self.key).distinct().collect()
        return sorted(r[0] for r in rows if r[0] is not None)

    def add(self, frame: pd.DataFrame) -> None:
        """Add the rows of one unit; a batch is written when enough are collected."""
        if frame is None or not len(frame):
            return
        self._frames.append(frame)
        self._rows += len(frame)
        if self._rows >= self.batch_rows:
            self.flush()

    def flush(self) -> None:
        """Write the collected rows to the building table as one file."""
        if not self._frames:
            return
        batch = pd.concat(self._frames, ignore_index=True)
        sdf = self.spark.createDataFrame(batch, self.schema).coalesce(1)
        self.storage.write_table(
            sdf, self.building, mode="append" if self._building_exists() else "overwrite"
        )
        print(f"  {self.building}: wrote a batch of {len(batch):,} rows")
        self._has_batches = True
        self._frames, self._rows = [], 0

    def finish(self) -> int:
        """Write the final table from the building table and remove the latter.

        Returns:
            The number of rows of the final table, 0 when no unit gave rows
            (the final table is then left as it was).
        """
        self.flush()
        if not self._building_exists():
            return 0
        built = self.storage.read_table(self.building)
        # Each unit's rows together and in order: the files stay small and a
        # reader that wants one airport finds it in one place.
        final = built.repartition(self.files, self.key).sortWithinPartitions(self.key)
        self.storage.write_table(final, self.table, mode="overwrite")
        rows = self.storage.read_table(self.table).count()
        self.storage.delete_table(self.building)
        self._has_batches = False
        print(f"  {self.table}: {rows:,} rows in {self.files} files")
        return rows
