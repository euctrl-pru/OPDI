"""Publishing one day of OPDI data.

For each day, this step writes two files into the published folder:

* ``flight_list/flight_list_YYYYMMDD.parquet``: one row per flight that started
  that day.
* ``flight_events/flight_events_YYYYMMDD.parquet``: one row per event of those
  flights. Each row carries the event's measurements as columns and the columns
  of its flight. A flight keeps all its events in the file of the day it
  started, also the events that happen after midnight.

It also keeps two small files up to date: a monthly flight index
(``index/flight_index_YYYYMM.parquet``), which tells which file holds a flight,
and the catalogue (``catalogue.parquet``), with one row per published day.

The columns are declared in :mod:`opdi.output.schema`.

How the files are written, so that readers can find rows quickly:

* Each file is a single Parquet file compressed with zstd.
* Rows are sorted: the flight list by departure airport and first-seen time,
  the events by flight and time. Each row group (a block of rows that a reader
  can skip as a whole) holds about 100,000 rows, and its minimum and maximum
  values let a reader skip the blocks it does not need.
* Bloom filters (a compact test that tells a reader a value is certainly not in
  a block) are stored for the flight identifier, the aircraft address, the call
  sign, the two airports and, in the events file, the event type.
* A file is first written under a temporary name and then renamed, so a reader
  never sees a half-written file. Publishing a day again replaces its files,
  its catalogue row and its index rows.
"""

import datetime as dt
import os
import subprocess
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from opdi.output.schema import (
    CATALOGUE,
    EVENT_COLUMNS,
    FLIGHT_EVENTS,
    FLIGHT_INDEX,
    FLIGHT_LIST,
    MEASUREMENT_COLUMNS,
    RELEASE,
    Column,
)

FLIGHT_LIST_TABLE = "opdi_flight_list"
EVENTS_TABLE = "opdi_flight_events"
MEASUREMENTS_TABLE = "opdi_measurements"

#: Number of rows in one row group of a published file.
ROW_GROUP_ROWS = 100_000

#: Upper limit on the size of a row group, in bytes. It is set high so that the
#: row count above decides where a row group ends.
_ROW_GROUP_MAX_BYTES = 1024 * 1024 * 1024

FLIGHT_LIST_SORT = ("adep", "first_seen")
FLIGHT_EVENTS_SORT = ("flight_id", "event_time")
FLIGHT_LIST_BLOOM = ("flight_id", "icao24", "flt_id", "adep", "ades")
FLIGHT_EVENTS_BLOOM = FLIGHT_LIST_BLOOM + ("type",)


def _ddl(columns: Iterable[Column]) -> str:
    return ", ".join(f"`{c.name}` {c.dtype}" for c in columns)


@dataclass(frozen=True)
class PublishedLayout:
    """Where the published files of a release are.

    Attributes:
        root: The published folder, for example
            ``s3a://eurocontrol/opdi/v2.0.0/published``.
    """

    root: str

    @classmethod
    def for_storage(cls, storage) -> "PublishedLayout":
        """The published folder beside the pipeline's working tables."""
        return cls(f"{storage.base_path.rstrip('/')}/published")

    def flight_list(self, day: dt.date) -> str:
        """Path of the flight list file of ``day``."""
        return f"{self.root}/flight_list/flight_list_{day:%Y%m%d}.parquet"

    def flight_events(self, day: dt.date) -> str:
        """Path of the flight events file of ``day``."""
        return f"{self.root}/flight_events/flight_events_{day:%Y%m%d}.parquet"

    def flight_index(self, month: dt.date) -> str:
        """Path of the flight index of the month that contains ``month``."""
        return f"{self.root}/index/flight_index_{month:%Y%m}.parquet"

    @property
    def catalogue(self) -> str:
        """Path of the catalogue."""
        return f"{self.root}/catalogue.parquet"


# --- building the tables ----------------------------------------------------

def build_flight_list(flight_list: DataFrame) -> DataFrame:
    """The published flight list from the pipeline's working flight list.

    Renames the columns to their published names, gives them their declared
    types and order, and adds the ``release`` column.

    Args:
        flight_list: Rows of the working table ``opdi_flight_list``.

    Returns:
        The rows with the columns of :data:`~opdi.output.schema.FLIGHT_LIST`.

    Raises:
        ValueError: If a working column is missing, for example because the
            milestone columns have not been added yet (step 04b).
    """
    present = {c.upper() for c in flight_list.columns}
    wanted = [c for c in FLIGHT_LIST if c.name != "release"]
    missing = [c.source for c in wanted if c.source.upper() not in present]
    if missing:
        raise ValueError(
            f"the flight list is missing the column(s) {missing}; run steps 03 and "
            "04b for this day before publishing it."
        )
    return flight_list.select(
        *[F.col(c.source).cast(c.dtype).alias(c.name) for c in wanted],
        F.lit(RELEASE).alias("release"),
    )


def _h3_res_5() -> F.Column:
    from opdi.pipeline.tracks import _geo_to_h3_cell

    return _geo_to_h3_cell(F.col("latitude"), F.col("longitude"), F.lit(5))


def build_flight_events(
    events: DataFrame, measurements: DataFrame, flight_list: DataFrame
) -> DataFrame:
    """The published flight events from the pipeline's working tables.

    Every event becomes one row. Its measurements become columns: an event
    without a measurement of some kind has NULL in that column, never 0. The
    flight's columns are then added from the flight list, so each row can be
    used on its own.

    Args:
        events: Rows of the working table ``opdi_flight_events``.
        measurements: Rows of the working table ``opdi_measurements`` for the
            same events.
        flight_list: Rows of the working table ``opdi_flight_list`` that
            contain the events' flights. Extra flights do no harm.

    Returns:
        The rows with the columns of :data:`~opdi.output.schema.FLIGHT_EVENTS`.
        The ``dof`` column is the day of the event's flight as recorded on the
        event, which is the day of the file it is published in.
    """
    measured = measurements.groupBy(F.col("milestone_id").alias("_milestone_id")).agg(
        *[
            F.first(
                F.when(F.col("type") == F.lit(c.source), F.col("value")), ignorenulls=True
            ).cast("double").alias(c.name)
            for c in MEASUREMENT_COLUMNS
        ]
    )

    flights = (
        build_flight_list(flight_list)
        .drop("dof", "release")
        .withColumnRenamed("version", "flight_version")
        .withColumnRenamed("flight_id", "_flight_id")
        .dropDuplicates(["_flight_id"])
    )

    renamed = {"event_id": "id", "altitude_ft": "altitude"}
    event_part = events.select(
        *[
            F.col(renamed.get(c.name, c.name)).alias(c.name)
            for c in EVENT_COLUMNS
            if c.name != "h3_res_5"
        ],
        F.col("dof").alias("dof"),
    ).withColumn("h3_res_5", _h3_res_5())

    joined = (
        event_part.join(measured, F.col("event_id") == F.col("_milestone_id"), "left")
        .join(flights, F.col("flight_id") == F.col("_flight_id"), "left")
        .withColumn("release", F.lit(RELEASE))
    )
    return joined.select(*[F.col(c.name).cast(c.dtype).alias(c.name) for c in FLIGHT_EVENTS])


# --- writing ------------------------------------------------------------------

def _fs(spark: SparkSession, path: str):
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark._jsc.hadoopConfiguration()), p


def _exists(spark: SparkSession, path: str) -> bool:
    fs, p = _fs(spark, path)
    return fs.exists(p)


def _size(spark: SparkSession, path: str) -> int:
    fs, p = _fs(spark, path)
    return int(fs.getFileStatus(p).getLen())


def write_single_file(
    df: DataFrame,
    path: str,
    sort_by: Sequence[str] = (),
    bloom_filters: Sequence[str] = (),
    row_group_rows: int = ROW_GROUP_ROWS,
) -> None:
    """Write ``df`` as exactly one Parquet file at ``path``.

    The rows are written into ``<path>.tmp/`` first. The single file found
    there is then moved to ``path``, replacing any file already there, and the
    temporary folder is removed. An empty frame gives an empty file that still
    has all the columns.

    Args:
        df: The rows to write.
        path: The final file path.
        sort_by: Columns to sort the rows by.
        bloom_filters: Columns to store a Bloom filter for.
        row_group_rows: Number of rows per row group.

    Raises:
        RuntimeError: If the temporary folder does not hold exactly one file.
    """
    spark = df.sparkSession
    tmp = f"{path}.tmp"
    fs, final = _fs(spark, path)
    tmp_path = spark._jvm.org.apache.hadoop.fs.Path(tmp)
    if fs.exists(tmp_path):
        fs.delete(tmp_path, True)

    out = df.coalesce(1)
    if sort_by:
        out = out.sortWithinPartitions(*sort_by)
    writer = (
        out.write.mode("overwrite")
        .option("compression", "zstd")
        .option("parquet.block.row.count.limit", str(int(row_group_rows)))
        .option("parquet.block.size", str(_ROW_GROUP_MAX_BYTES))
    )
    for c in bloom_filters:
        # The Parquet writer leaves out the Bloom filter of a column that is
        # stored with a dictionary, so the dictionary is turned off for these
        # columns to make sure every row group has its filter.
        writer = writer.option(f"parquet.bloom.filter.enabled#{c}", "true")
        writer = writer.option(f"parquet.enable.dictionary#{c}", "false")
    writer.parquet(tmp)

    parts = [
        s.getPath()
        for s in fs.listStatus(tmp_path)
        if s.getPath().getName().startswith("part-")
        and s.getPath().getName().endswith(".parquet")
    ]
    if len(parts) != 1:
        raise RuntimeError(f"expected one data file in {tmp}, found {len(parts)}")
    if fs.exists(final):
        fs.delete(final, False)
    fs.mkdirs(final.getParent())
    if not fs.rename(parts[0], final):
        raise RuntimeError(f"could not move {parts[0]} to {path}")
    fs.delete(tmp_path, True)


# --- index and catalogue ------------------------------------------------------

def _month_files(spark: SparkSession, layout: PublishedLayout, month: dt.date) -> list:
    folder = f"{layout.root}/flight_list"
    fs, p = _fs(spark, folder)
    if not fs.exists(p):
        return []
    prefix = f"flight_list_{month:%Y%m}"
    return sorted(
        f"{folder}/{s.getPath().getName()}"
        for s in fs.listStatus(p)
        if s.isFile()
        and s.getPath().getName().startswith(prefix)
        and s.getPath().getName().endswith(".parquet")
    )


def update_index(spark: SparkSession, layout: PublishedLayout, month: dt.date) -> int:
    """Rebuild the flight index of the month that contains ``month``.

    The index is built again from every published flight list file of that
    month, so it never holds a flight twice.

    Args:
        spark: The Spark session.
        layout: Where the published files are.
        month: Any day of the month.

    Returns:
        The number of flights in the index.
    """
    files = _month_files(spark, layout, month)
    names = [c.name for c in FLIGHT_INDEX if c.name != "file"]
    if files:
        index = spark.read.parquet(*files).select(
            *names,
            F.concat(F.lit("flight_list/"), F.col("_metadata.file_name")).alias("file"),
        )
    else:
        index = spark.createDataFrame([], _ddl(FLIGHT_INDEX))
    index = index.select(*[F.col(c.name).cast(c.dtype) for c in FLIGHT_INDEX])
    write_single_file(
        index, layout.flight_index(month), sort_by=("flight_id",),
        bloom_filters=("flight_id", "icao24", "flt_id"),
    )
    return spark.read.parquet(layout.flight_index(month)).count()


def update_catalogue(
    spark: SparkSession,
    layout: PublishedLayout,
    day: dt.date,
    counts: Dict,
    commit: str,
) -> None:
    """Write the catalogue row of ``day``, replacing any earlier row for it.

    Args:
        spark: The Spark session.
        layout: Where the published files are.
        day: The published day.
        counts: The day's figures, as returned by :func:`publish_day`.
        commit: Identifier of the software version that published the day.
    """
    row = (
        day,
        int(counts["flights"]),
        int(counts["events"]),
        counts["first_event"],
        counts["last_event"],
        int(counts["flight_list_bytes"]),
        int(counts["flight_events_bytes"]),
        RELEASE,
        counts["flight_list_version"],
        list(counts["event_versions"]),
        commit,
        dt.datetime.now(dt.timezone.utc).replace(tzinfo=None),
    )
    new = spark.createDataFrame([row], _ddl(CATALOGUE))
    if _exists(spark, layout.catalogue):
        old = spark.read.parquet(layout.catalogue).filter(F.col("day") != F.lit(day))
        new = old.select(*[c.name for c in CATALOGUE]).unionByName(new)
    write_single_file(new, layout.catalogue, sort_by=("day",))


# --- one day ----------------------------------------------------------------

def _day_rows(storage, table: str, column: str, first: dt.date, last: dt.date) -> DataFrame:
    if not storage.table_exists(table):
        raise ValueError(f"the working table {table} does not exist; run the pipeline first.")
    df = storage.read_table(table)
    if first == last:
        return df.filter(F.col(column) == F.lit(first))
    return df.filter(F.col(column).between(F.lit(first), F.lit(last)))


def publish_day(
    spark: SparkSession,
    storage,
    day: dt.date,
    layout: PublishedLayout,
    commit: str,
    row_group_rows: int = ROW_GROUP_ROWS,
) -> Dict:
    """Publish the flights that started on ``day``.

    Reads the day's rows of the working tables, writes the day's flight list
    and flight events files, then rebuilds the month's flight index and
    replaces the day's catalogue row. Both files are written also when the day
    has no flights; they are then empty but have all their columns.

    Args:
        spark: The Spark session.
        storage: The pipeline's storage (:class:`opdi.utils.storage.StorageManager`),
            used to read the working tables.
        day: The day to publish.
        layout: Where the published files go.
        commit: Identifier of the software version that publishes the day.
        row_group_rows: Number of rows per row group.

    Returns:
        The day's figures: ``flights``, ``events``, ``first_event``,
        ``last_event``, ``flight_list_bytes``, ``flight_events_bytes``,
        ``flight_list_version``, ``event_versions`` and ``index_flights`` (the
        number of flights in the month's index).
    """
    one_day = dt.timedelta(days=1)
    flight_rows = _day_rows(storage, FLIGHT_LIST_TABLE, "DOF", day, day)
    event_rows = _day_rows(storage, EVENTS_TABLE, "dof", day, day)
    measurement_rows = _day_rows(storage, MEASUREMENTS_TABLE, "dof", day, day)
    # The flight of an event is looked up in a three-day window, because the
    # date recorded on the flight and on its events can differ by a day.
    nearby_flights = _day_rows(storage, FLIGHT_LIST_TABLE, "DOF", day - one_day, day + one_day)

    write_single_file(
        build_flight_list(flight_rows), layout.flight_list(day),
        sort_by=FLIGHT_LIST_SORT, bloom_filters=FLIGHT_LIST_BLOOM,
        row_group_rows=row_group_rows,
    )
    write_single_file(
        build_flight_events(event_rows, measurement_rows, nearby_flights),
        layout.flight_events(day),
        sort_by=FLIGHT_EVENTS_SORT, bloom_filters=FLIGHT_EVENTS_BLOOM,
        row_group_rows=row_group_rows,
    )

    # The figures are read back from the written files, which is cheap and
    # describes exactly what was published.
    fl = spark.read.parquet(layout.flight_list(day))
    ev = spark.read.parquet(layout.flight_events(day))
    fl_versions = sorted(r["version"] for r in fl.select("version").distinct().collect()
                         if r["version"] is not None)
    ev_stats = ev.agg(
        F.count(F.lit(1)).alias("n"),
        F.min("event_time").alias("first"),
        F.max("event_time").alias("last"),
        F.sort_array(F.collect_set("version")).alias("versions"),
    ).collect()[0]
    counts = {
        "flights": fl.count(),
        "events": int(ev_stats["n"]),
        "first_event": ev_stats["first"],
        "last_event": ev_stats["last"],
        "flight_list_bytes": _size(spark, layout.flight_list(day)),
        "flight_events_bytes": _size(spark, layout.flight_events(day)),
        "flight_list_version": ",".join(fl_versions) if fl_versions else None,
        "event_versions": list(ev_stats["versions"] or []),
    }

    counts["index_flights"] = update_index(spark, layout, day)
    update_catalogue(spark, layout, day, counts, commit)
    return counts


def pipeline_commit() -> str:
    """Identifier of the OPDI software version that is running.

    Taken from the ``OPDI_COMMIT`` environment variable when it is set (for
    example in a container image), otherwise from git when the package runs
    from a git checkout, otherwise ``"unknown"``.
    """
    env = os.environ.get("OPDI_COMMIT")
    if env:
        return env
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            capture_output=True, text=True, timeout=10, check=True,
        )
        return out.stdout.strip() or "unknown"
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def publish_range(spark: SparkSession, config, start: dt.date, end: dt.date,
                  layout: Optional[PublishedLayout] = None) -> None:
    """Publish every day from ``start`` to ``end``, both included.

    Args:
        spark: The Spark session.
        config: The pipeline configuration.
        start: First day.
        end: Last day.
        layout: Where the published files go. By default, the ``published``
            folder beside the working tables.
    """
    from opdi.utils.storage import StorageManager

    storage = StorageManager(spark, config)
    layout = layout or PublishedLayout.for_storage(storage)
    commit = pipeline_commit()
    day = start
    while day <= end:
        counts = publish_day(spark, storage, day, layout, commit)
        print(f"  published {day}: {counts['flights']:,} flights, "
              f"{counts['events']:,} events -> {layout.root}")
        day += dt.timedelta(days=1)
