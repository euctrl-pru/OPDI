"""Publishing one day of OPDI data.

For each day, this step writes two files into the published folder:

* ``flight_list/flight_list_YYYYMMDD.parquet``: one row per flight that started
  that day.
* ``flight_events/flight_events_YYYYMMDD.parquet``: one row per event of those
  flights. Each row carries the event's measurements as columns and the columns
  of its flight. A flight keeps all its events in the file of the day it
  started, also the events that happen after midnight. An event whose flight is
  not in any flight list is published in the file of the date stored on the
  event, with empty (NULL) flight columns.

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
  a block) are stored for the three identifiers a reader looks up one value
  of: the flight identifier, the aircraft address and the call sign. Columns
  with few distinct values, such as the airports and the event type, are
  stored with a dictionary (a list of the distinct values, which each row then
  refers to), and the dictionary already tells a reader which values a block
  holds.
* A file is first written completely under ``<release root>/_state/tmp/`` and
  then moved into place, so a reader never sees a partly written file. While
  a file is replaced it is missing for a moment, and between the two moves of
  a day a reader can see the new flight list beside the old events file.
  Publishing a day again replaces its files, its catalogue row and its index
  rows, and after a stop part way it ends in the correct state.
"""

import datetime as dt
import os
import subprocess
import uuid
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
#: Columns that get a Bloom filter. Only identifiers with many distinct
#: values: for them a dictionary does not help a reader, and a Bloom filter
#: does.
FLIGHT_LIST_BLOOM = ("flight_id", "icao24", "flt_id")
FLIGHT_EVENTS_BLOOM = FLIGHT_LIST_BLOOM


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

    @property
    def state(self) -> str:
        """The progress folder beside the published folder, ``<release root>/_state``."""
        return f"{self.root.rstrip('/').rsplit('/', 1)[0]}/_state"

    @property
    def tmp(self) -> str:
        """Folder of the temporary files of publishing, ``<release root>/_state/tmp``.

        Outside the published folder, so a reader of the published folders
        never sees a temporary file, and in the same bucket, so moving a file
        from there into place stays a copy inside the storage.
        """
        return f"{self.state}/tmp"


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
        The ``dof`` column is the date of the event's flight in the flight
        list, which is the day of the file it is published in. For an event
        whose flight is not in ``flight_list`` it is the date stored on the
        event.
    """
    measured = measurements.groupBy(F.col("milestone_id").alias("_milestone_id")).agg(
        *[
            F.first(F.when(F.col("type") == F.lit(c.source), F.col("value")), ignorenulls=True)
            .cast("double")
            .alias(c.name)
            for c in MEASUREMENT_COLUMNS
        ]
    )

    flights = (
        build_flight_list(flight_list)
        .drop("release")
        .withColumnRenamed("dof", "_flight_dof")
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
        F.col("dof").alias("_event_dof"),
    ).withColumn("h3_res_5", _h3_res_5())

    joined = (
        event_part.join(measured, F.col("event_id") == F.col("_milestone_id"), "left")
        .join(flights, F.col("flight_id") == F.col("_flight_id"), "left")
        .withColumn("dof", F.coalesce(F.col("_flight_dof"), F.col("_event_dof")))
        .withColumn("release", F.lit(RELEASE))
    )
    return joined.select(*[F.col(c.name).cast(c.dtype).alias(c.name) for c in FLIGHT_EVENTS])


# --- writing ------------------------------------------------------------------


def _fs(spark: SparkSession, path: str):
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark._jsc.hadoopConfiguration()), p


class _Files:
    """The file operations of publishing, on any Hadoop file system.

    Every change to a published folder goes through :meth:`rename` and
    :meth:`delete`, so a test can make one of them fail and check that
    publishing again repairs the result.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def exists(self, path: str) -> bool:
        fs, p = _fs(self.spark, path)
        return bool(fs.exists(p))

    def rename(self, source: str, target: str) -> None:
        fs, src = _fs(self.spark, source)
        _, dst = _fs(self.spark, target)
        fs.mkdirs(dst.getParent())
        if not fs.rename(src, dst):
            raise RuntimeError(f"could not move {source} to {target}")

    def delete(self, path: str, recursive: bool = False) -> None:
        fs, p = _fs(self.spark, path)
        if fs.exists(p):
            fs.delete(p, recursive)

    def list_names(self, folder: str) -> list:
        fs, p = _fs(self.spark, folder)
        if not fs.exists(p):
            return []
        return sorted(s.getPath().getName() for s in fs.listStatus(p))


def _exists(spark: SparkSession, path: str) -> bool:
    return _Files(spark).exists(path)


def _size(spark: SparkSession, path: str) -> int:
    fs, p = _fs(spark, path)
    return int(fs.getFileStatus(p).getLen())


def _rows(spark: SparkSession, path: str) -> int:
    """The number of rows of a Parquet file, read from its footer."""
    return spark.read.parquet(path).count()


def write_temporary_file(
    df: DataFrame,
    folder: str,
    sort_by: Sequence[str] = (),
    bloom_filters: Sequence[str] = (),
    row_group_rows: int = ROW_GROUP_ROWS,
) -> str:
    """Write ``df`` as exactly one Parquet file into the new folder ``folder``.

    Args:
        df: The rows to write.
        folder: A folder that does not exist yet; it is created.
        sort_by: Columns to sort the rows by.
        bloom_filters: Columns to store a Bloom filter for. These columns are
            stored without a dictionary; every other column keeps one.
        row_group_rows: Number of rows per row group.

    Returns:
        The path of the file written.

    Raises:
        RuntimeError: If the folder does not end up holding exactly one file.
    """
    spark = df.sparkSession
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
        # columns to make sure every row group has its filter. The expected
        # number of distinct values sizes the filter: a row group cannot hold
        # more distinct values than rows.
        writer = writer.option(f"parquet.bloom.filter.enabled#{c}", "true")
        writer = writer.option(f"parquet.bloom.filter.expected.ndv#{c}", str(int(row_group_rows)))
        writer = writer.option(f"parquet.enable.dictionary#{c}", "false")
    writer.parquet(folder)

    parts = [
        name
        for name in _Files(spark).list_names(folder)
        if name.startswith("part-") and name.endswith(".parquet")
    ]
    if len(parts) != 1:
        raise RuntimeError(f"expected one data file in {folder}, found {len(parts)}")
    return f"{folder}/{parts[0]}"


def _aside(path: str, tmp_root: str) -> str:
    """Where the old file at ``path`` is kept while the new one replaces it."""
    return f"{tmp_root}/aside/{path.rsplit('/', 1)[1]}"


def recover(files: _Files, path: str, tmp_root: str) -> None:
    """Finish a replacement of ``path`` that stopped part way.

    When an old file was moved aside and the new one never arrived, the old
    file is put back. When both are there, the old one is removed.
    """
    aside = _aside(path, tmp_root)
    if not files.exists(aside):
        return
    if files.exists(path):
        files.delete(aside)
    else:
        files.rename(aside, path)


def replace_file(files: _Files, new: str, path: str, tmp_root: str) -> None:
    """Move the file ``new`` to ``path``, replacing the file already there.

    The old file is first moved aside (under ``tmp_root``), then the new one
    is moved in, and only then is the old one removed. When moving the new
    file in fails, the old one is moved back. ``path`` is therefore missing
    only for the short time between the two moves.
    """
    recover(files, path, tmp_root)
    aside = _aside(path, tmp_root)
    had_old = files.exists(path)
    if had_old:
        files.rename(path, aside)
    try:
        files.rename(new, path)
    except Exception:
        if had_old and not files.exists(path):
            files.rename(aside, path)
        raise
    if had_old:
        files.delete(aside)


def _tmp_folder(tmp_root: str, label: str) -> str:
    return f"{tmp_root}/{label}-{uuid.uuid4().hex}"


def write_single_file(
    df: DataFrame,
    path: str,
    sort_by: Sequence[str] = (),
    bloom_filters: Sequence[str] = (),
    row_group_rows: int = ROW_GROUP_ROWS,
    tmp_root: Optional[str] = None,
) -> None:
    """Write ``df`` as exactly one Parquet file at ``path``.

    The rows are written into a new temporary folder first, under
    ``tmp_root`` (by default beside ``path``), with a name of its own so that
    two writers never share it. The single file found there then replaces
    any file at ``path`` (see :func:`replace_file`), and the temporary
    folder is removed. An empty frame gives an empty file that still has all
    the columns.

    Args:
        df: The rows to write.
        path: The final file path.
        sort_by: Columns to sort the rows by.
        bloom_filters: Columns to store a Bloom filter for.
        row_group_rows: Number of rows per row group.
        tmp_root: Folder for the temporary files.

    Raises:
        RuntimeError: If the temporary folder does not hold exactly one file.
    """
    files = _Files(df.sparkSession)
    if tmp_root is None:
        tmp_root = path.rsplit("/", 1)[0] + "/_tmp"
    folder = _tmp_folder(tmp_root, path.rsplit("/", 1)[1])
    try:
        part = write_temporary_file(df, folder, sort_by, bloom_filters, row_group_rows)
        replace_file(files, part, path, tmp_root)
    finally:
        files.delete(folder, True)


# --- one publisher at a time ------------------------------------------------------

#: Name of the lock file, in ``<release root>/_state/``.
LOCK_NAME = "publish.lock"

#: Age in hours after which a lock is reported as left behind by a stopped
#: run. Publishing one day takes minutes; the Airflow tasks stop after six
#: hours at most.
STALE_LOCK_HOURS = 6.0

#: The command that removes a lock left behind.
CLEAR_LOCK_COMMAND = "python -m opdi.orchestration clear-lock"


class PublishLockedError(RuntimeError):
    """Another publish holds the lock of the release, or a stopped one left it."""


def _lock_path(layout: PublishedLayout) -> str:
    return f"{layout.state}/{LOCK_NAME}"


def _read_lock(spark: SparkSession, path: str) -> dict:
    import json

    fs, p = _fs(spark, path)
    stream = fs.open(p)
    try:
        scanner = spark._jvm.java.util.Scanner(stream, "UTF-8").useDelimiter("\\A")
        text = scanner.next() if scanner.hasNext() else ""
    finally:
        stream.close()
    try:
        return json.loads(text)
    except ValueError:
        return {}


class PublishLock:
    """Hold the lock of a release while publishing.

    The lock is a file, ``<release root>/_state/publish.lock``, created only
    when no such file exists. A second publish into the same release fails
    while it is there. A lock older than :data:`STALE_LOCK_HOURS` is reported
    with the command that removes it (:data:`CLEAR_LOCK_COMMAND`); it is
    never removed without that command, because the run that holds it may
    still be writing.

    Used as ``with PublishLock(spark, layout): ...``.
    """

    def __init__(self, spark: SparkSession, layout: PublishedLayout):
        self.spark = spark
        self.layout = layout
        self.path = _lock_path(layout)

    def __enter__(self):
        """Take the lock, or raise :class:`PublishLockedError`."""
        import json
        import socket

        now = dt.datetime.now(dt.timezone.utc)
        fs, p = _fs(self.spark, self.path)
        fs.mkdirs(p.getParent())
        body = json.dumps(
            {"host": socket.gethostname(), "pid": os.getpid(), "since": now.isoformat()}
        )
        try:
            out = fs.create(p, False)
        except Exception as exc:  # noqa: BLE001 - the file exists: someone holds it
            holder = _read_lock(self.spark, self.path) if fs.exists(p) else {}
            raise PublishLockedError(self._message(holder, now)) from exc
        try:
            out.write(bytearray(body.encode("utf-8")))
        finally:
            out.close()
        # The lock is held: temporary folders left by a publish that stopped
        # are removed. The old files kept aside are not: they are put back by
        # the next replacement of their file (see recover).
        files = _Files(self.spark)
        for name in files.list_names(self.layout.tmp):
            if name != "aside":
                files.delete(f"{self.layout.tmp}/{name}", True)
        return self

    def _message(self, holder: dict, now: dt.datetime) -> str:
        since = holder.get("since")
        age = None
        if since:
            try:
                age = (now - dt.datetime.fromisoformat(since)).total_seconds() / 3600
            except ValueError:
                age = None
        who = f"host {holder.get('host', '?')}, process {holder.get('pid', '?')}"
        if age is None or age >= STALE_LOCK_HOURS:
            return (
                f"the lock {self.path} ({who}, since {since or 'an unknown time'}) is older "
                f"than {STALE_LOCK_HOURS:g} hours: a publish probably stopped without "
                "removing it. Check that no OPDI command is still running, then remove it "
                f"with: {CLEAR_LOCK_COMMAND} (with the same --warehouse)"
            )
        return (
            f"another publish holds the lock {self.path} ({who}, since {since}, "
            f"{age * 60:.0f} minutes ago). Run one publish at a time."
        )

    def __exit__(self, *exc):
        """Release the lock, also when publishing failed."""
        fs, p = _fs(self.spark, self.path)
        if fs.exists(p):
            fs.delete(p, False)
        return False


def clear_lock(spark: SparkSession, layout: PublishedLayout) -> bool:
    """Remove the publish lock of a release. Returns whether there was one."""
    files = _Files(spark)
    path = _lock_path(layout)
    if not files.exists(path):
        return False
    files.delete(path)
    return True


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
        index,
        layout.flight_index(month),
        sort_by=("flight_id",),
        bloom_filters=("flight_id", "icao24", "flt_id"),
        tmp_root=layout.tmp,
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
        # With its zone, so that Spark stores this instant whatever the zone
        # of the machine.
        dt.datetime.now(dt.timezone.utc),
    )
    new = spark.createDataFrame([row], _ddl(CATALOGUE))
    if _exists(spark, layout.catalogue):
        old = spark.read.parquet(layout.catalogue).filter(F.col("day") != F.lit(day))
        new = old.select(*[c.name for c in CATALOGUE]).unionByName(new)
    write_single_file(new, layout.catalogue, sort_by=("day",), tmp_root=layout.tmp)


# --- one day ----------------------------------------------------------------


def _day_rows(storage, table: str, column: str, first: dt.date, last: dt.date) -> DataFrame:
    if not storage.table_exists(table):
        raise ValueError(f"the working table {table} does not exist; run the pipeline first.")
    df = storage.read_table(table)
    if first == last:
        return df.filter(F.col(column) == F.lit(first))
    return df.filter(F.col(column).between(F.lit(first), F.lit(last)))


def _day_events(storage, day: dt.date):
    """The working events and measurements published in ``day``'s file.

    The working tables store each event under a date of their own, which can
    be one day earlier or later than the date of its flight in the flight
    list. The file of ``day`` therefore takes, from the three days around it,
    every event whose flight is in ``day``'s flight list, plus the events
    stored under ``day`` whose flight is in none of the three days' flight
    lists.

    Returns:
        The events, their measurements and the flight-list rows of the three
        days.
    """
    one_day = dt.timedelta(days=1)
    first, last = day - one_day, day + one_day
    nearby_flights = _day_rows(storage, FLIGHT_LIST_TABLE, "DOF", first, last)
    events = _day_rows(storage, EVENTS_TABLE, "dof", first, last)
    measurements = _day_rows(storage, MEASUREMENTS_TABLE, "dof", first, last)

    flight_days = nearby_flights.select(
        F.col("ID").alias("_flight_id"), F.col("DOF").alias("_flight_dof")
    ).dropDuplicates(["_flight_id"])
    chosen = (
        events.join(flight_days, F.col("flight_id") == F.col("_flight_id"), "left")
        .filter(
            (F.col("_flight_dof") == F.lit(day))
            | (F.col("_flight_dof").isNull() & (F.col("dof") == F.lit(day)))
        )
        .drop("_flight_id", "_flight_dof")
    )
    chosen_ids = chosen.select(F.col("id").alias("_event_id")).distinct()
    chosen_measurements = measurements.join(
        chosen_ids, F.col("milestone_id") == F.col("_event_id"), "left_semi"
    )
    return chosen, chosen_measurements, nearby_flights


def publish_day(
    spark: SparkSession,
    storage,
    day: dt.date,
    layout: PublishedLayout,
    commit: str,
    row_group_rows: int = ROW_GROUP_ROWS,
    allow_empty: bool = False,
) -> Dict:
    """Publish the flights that started on ``day``.

    Reads the day's rows of the working tables and writes the day's flight
    list and flight events files (the events of exactly the flights in the
    flight list file, see :func:`_day_events`), then rebuilds the month's
    flight index and replaces the day's catalogue row.

    Both files are first written to temporary files under
    :attr:`PublishedLayout.tmp`. Only when both are complete are they moved
    into place, one after the other (see :func:`replace_file`). A file is
    therefore missing for a moment while it is replaced, and for that moment
    a reader can see the new flight list beside the old events file. When
    publishing stops part way, publishing the day again ends in the correct
    state.

    Args:
        spark: The Spark session.
        storage: The pipeline's storage (:class:`opdi.utils.storage.StorageManager`),
            used to read the working tables.
        day: The day to publish.
        layout: Where the published files go.
        commit: Identifier of the software version that publishes the day.
        row_group_rows: Number of rows per row group.
        allow_empty: Replace a published file that has rows with an empty
            one. Without it, that is refused.

    Returns:
        The day's figures: ``flights``, ``events``, ``first_event``,
        ``last_event``, ``flight_list_bytes``, ``flight_events_bytes``,
        ``flight_list_version``, ``event_versions`` and ``index_flights`` (the
        number of flights in the month's index).

    Raises:
        ValueError: If the day's flight list has rows but no working event is
            stored under the day (the events were removed, or step 04 did not
            run), if a published file with rows would be replaced by an empty
            one and ``allow_empty`` is not set, or if the session's time zone
            is not UTC.
        PublishLockedError: If another publish into the same release holds the
            lock, or a stopped one left it (see :class:`PublishLock`).
    """
    zone = spark.conf.get("spark.sql.session.timeZone")
    if zone not in ("UTC", "Etc/UTC", "Z", "+00:00"):
        raise ValueError(
            f"the Spark session works in the time zone {zone!r}, not UTC: the days of the "
            "published files would not be UTC days. Create the session with "
            "opdi.utils.spark_helpers.SparkSessionManager, or set "
            "spark.sql.session.timeZone to UTC."
        )
    with PublishLock(spark, layout):
        return _publish_day(spark, storage, day, layout, commit, row_group_rows, allow_empty)


def _publish_day(spark, storage, day, layout, commit, row_group_rows, allow_empty) -> Dict:
    files = _Files(spark)
    flight_rows = _day_rows(storage, FLIGHT_LIST_TABLE, "DOF", day, day)
    event_rows, measurement_rows, nearby_flights = _day_events(storage, day)

    flights_count = flight_rows.count()
    if flights_count:
        stored = _day_rows(storage, EVENTS_TABLE, "dof", day, day).limit(1).count()
        if not stored:
            raise ValueError(
                f"the flight list of {day} has {flights_count:,} flights, but the working "
                f"events of that day ({EVENTS_TABLE} dof={day}) are missing: they were "
                "removed, or step 04 did not run. Publishing now would give a flight "
                "events file without them. Build the day again with: "
                f"python -m opdi.orchestration day --day {day} --force"
            )

    targets = {"flight_list": layout.flight_list(day), "flight_events": layout.flight_events(day)}
    for path in targets.values():
        recover(files, path, layout.tmp)

    folder = _tmp_folder(layout.tmp, f"publish-{day:%Y%m%d}")
    try:
        new = {
            "flight_list": write_temporary_file(
                build_flight_list(flight_rows),
                f"{folder}/flight_list",
                sort_by=FLIGHT_LIST_SORT,
                bloom_filters=FLIGHT_LIST_BLOOM,
                row_group_rows=row_group_rows,
            ),
            "flight_events": write_temporary_file(
                build_flight_events(event_rows, measurement_rows, nearby_flights),
                f"{folder}/flight_events",
                sort_by=FLIGHT_EVENTS_SORT,
                bloom_filters=FLIGHT_EVENTS_BLOOM,
                row_group_rows=row_group_rows,
            ),
        }
        if not allow_empty:
            for name, path in targets.items():
                if files.exists(path) and _rows(spark, new[name]) == 0:
                    old_rows = _rows(spark, path)
                    if old_rows:
                        raise ValueError(
                            f"refusing to replace {path} ({old_rows:,} rows) with an empty "
                            f"file: the working tables hold no {name.replace('_', ' ')} "
                            f"for {day}. If the day really is empty, publish it with "
                            "allow_empty=True (--allow-empty on the command line)."
                        )
        for name, path in targets.items():
            replace_file(files, new[name], path, layout.tmp)
    finally:
        files.delete(folder, True)

    # The figures are read back from the written files, which is cheap and
    # describes exactly what was published.
    fl = spark.read.parquet(layout.flight_list(day))
    ev = spark.read.parquet(layout.flight_events(day))
    fl_versions = sorted(
        r["version"] for r in fl.select("version").distinct().collect() if r["version"] is not None
    )
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
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
        return out.stdout.strip() or "unknown"
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def publish_range(
    spark: SparkSession,
    config,
    start: dt.date,
    end: dt.date,
    layout: Optional[PublishedLayout] = None,
) -> None:
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
        print(
            f"  published {day}: {counts['flights']:,} flights, "
            f"{counts['events']:,} events -> {layout.root}"
        )
        day += dt.timedelta(days=1)
