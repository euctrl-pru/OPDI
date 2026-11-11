"""Read the published OPDI files.

OPDI publishes two Parquet files per day (see :mod:`opdi.output.schema` for
every column):

* ``flight_list/flight_list_YYYYMMDD.parquet``: one row per flight that
  started that day.
* ``flight_events/flight_events_YYYYMMDD.parquet``: one row per event of those
  flights, with the event's measurements and its flight's columns.

This module reads them into pandas tables and gives back the separate flight
list, events and measurements tables in the layout of release v0.0.2
(:func:`as_flight_list_v0`, :func:`as_events`, :func:`as_measurements`). In
those tables the identifiers are text and do not match the identifiers of
release v0.0.2, which were 64-bit integers, and the ``version`` column holds
the version of the method that produced each row. It
needs pyarrow and pandas, and DuckDB only for :func:`duckdb_views`. It does
not need Spark. Install everything with ``pip install "opdi[reader]"``.

Example::

    from datetime import date
    from opdi import published

    flights = published.read_flight_list(date(2026, 6, 14))
    events = published.read_flight_events(date(2026, 6, 14), types=["ATOT"])
    measurements = published.as_measurements(published.read_flight_events(date(2026, 6, 14)))

Files are found under a base address, which can be a web address
(``https://...``), an S3 address (``s3://bucket/folder/``, also written
``s3a://``) or a local folder (``/data/opdi/`` or ``file:///data/opdi/``).
"""

import glob
import io
import os
import urllib.error
import urllib.request
from datetime import date, timedelta
from typing import List, Optional, Sequence

from opdi.output.schema import (
    CLASSIC_EVENT_COLUMNS,
    CLASSIC_FLIGHT_LIST,
    CLASSIC_MEASUREMENT_COLUMNS,
    CLASSIC_MEASUREMENTS_ALWAYS,
    MEASUREMENT_COLUMNS,
    MEASUREMENT_SUFFIX,
    MEASUREMENT_VERSION,
    RELEASE,
)

#: Where the published files of the release can be downloaded. Not final yet;
#: set the ``OPDI_PUBLISHED_URL`` environment variable, or pass ``base`` to a
#: function, to read from another place.
PUBLISHED_BASE_URL = f"https://www.eurocontrol.int/performance/data/download/OPDI/{RELEASE}/"

#: Environment variable that replaces :data:`PUBLISHED_BASE_URL`.
URL_VARIABLE = "OPDI_PUBLISHED_URL"

#: The two daily tables.
TABLES = ("flight_list", "flight_events")

_READER_EXTRA = 'pip install "opdi[reader]"'


def base_url(base: Optional[str] = None) -> str:
    """The base address the files are read from, ending with ``/``.

    Args:
        base: An address to use. When omitted, the ``OPDI_PUBLISHED_URL``
            environment variable if set, else :data:`PUBLISHED_BASE_URL`.
    """
    chosen = base or os.environ.get(URL_VARIABLE) or PUBLISHED_BASE_URL
    return chosen if chosen.endswith("/") else chosen + "/"


def file_url(table: str, day: date, base: Optional[str] = None) -> str:
    """Address of one day's file.

    Args:
        table: ``"flight_list"`` or ``"flight_events"``.
        day: The day.
        base: Base address; see :func:`base_url`.

    Raises:
        ValueError: If ``table`` is not one of the two tables.
    """
    if table not in TABLES:
        raise ValueError(f"table must be one of {TABLES}, not {table!r}")
    return f"{base_url(base)}{table}/{table}_{day:%Y%m%d}.parquet"


def _days(start: date, end: Optional[date]) -> List[date]:
    end = start if end is None else end
    if end < start:
        raise ValueError(f"end ({end}) is before start ({start})")
    return [start + timedelta(days=n) for n in range((end - start).days + 1)]


def _local_path(url: str) -> Optional[str]:
    if url.startswith("file://"):
        return url[len("file://") :]
    if "://" not in url:
        return url
    return None


def _read_one(url: str, columns: Optional[Sequence[str]], filters):
    import pyarrow.parquet as pq

    columns = list(columns) if columns is not None else None
    local = _local_path(url)
    if local is not None:
        if not os.path.isfile(local):
            raise FileNotFoundError(f"no published file at {url}")
        return pq.read_table(local, columns=columns, filters=filters)
    if url.startswith(("http://", "https://")):
        try:
            with urllib.request.urlopen(url) as response:
                data = response.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise FileNotFoundError(f"no published file at {url}") from exc
            raise
        return pq.read_table(io.BytesIO(data), columns=columns, filters=filters)
    if url.startswith(("s3://", "s3a://")):
        from pyarrow import fs

        filesystem, path = fs.FileSystem.from_uri("s3://" + url.split("://", 1)[1])
        try:
            return pq.read_table(path, filesystem=filesystem, columns=columns, filters=filters)
        except FileNotFoundError as exc:
            raise FileNotFoundError(f"no published file at {url}") from exc
    raise ValueError(
        f"cannot read {url}: the address must start with https://, s3:// or "
        "file://, or be a local path"
    )


def _read(table, start, end, base, columns, filters=None):
    import pyarrow as pa

    tables = [_read_one(file_url(table, d, base), columns, filters) for d in _days(start, end)]
    return pa.concat_tables(tables).to_pandas()


def read_flight_list(
    start: date,
    end: Optional[date] = None,
    base: Optional[str] = None,
    columns: Optional[Sequence[str]] = None,
):
    """The flight list of one day or of a range of days.

    Args:
        start: First day.
        end: Last day, included. When omitted, only ``start`` is read.
        base: Base address; see :func:`base_url`.
        columns: Columns to read. When omitted, every column.

    Returns:
        A pandas DataFrame with one row per flight.

    Raises:
        FileNotFoundError: If the file of a day is not there.
    """
    return _read("flight_list", start, end, base, columns)


def read_flight_events(
    start: date,
    end: Optional[date] = None,
    base: Optional[str] = None,
    columns: Optional[Sequence[str]] = None,
    types: Optional[Sequence[str]] = None,
):
    """The flight events of one day or of a range of days.

    Each row is one event, with its measurements as columns and the columns
    of its flight.

    Args:
        start: First day.
        end: Last day, included. When omitted, only ``start`` is read.
        base: Base address; see :func:`base_url`.
        columns: Columns to read. When omitted, every column.
        types: Event types to keep, for example ``["ATOT", "ALDT"]``. When
            omitted, every type.

    Returns:
        A pandas DataFrame with one row per event.

    Raises:
        FileNotFoundError: If the file of a day is not there.
    """
    filters = [("type", "in", list(types))] if types else None
    return _read("flight_events", start, end, base, columns, filters)


def as_events(df):
    """The events table in the layout of release v0.0.2.

    The identifiers are text and do not match those of release v0.0.2, and
    ``version`` holds the version of the method of each event type.

    Args:
        df: Rows read with :func:`read_flight_events`.

    Returns:
        A pandas DataFrame with the columns ``id``, ``flight_id``, ``type``,
        ``event_time``, ``longitude``, ``latitude``, ``altitude`` (in feet),
        ``source``, ``version`` and ``info``.
    """
    return df[list(CLASSIC_EVENT_COLUMNS)].rename(columns=CLASSIC_EVENT_COLUMNS)


def as_measurements(df):
    """The measurements table in the layout of release v0.0.2: one row per measurement.

    As in release v0.0.2, every event has one row for the distance flown and
    one for the time passed, also when the value is empty (NULL). The other
    measurement types, which release v0.0.2 did not have, give a row only
    when they have a value. The ``id`` of a row is the event id followed by
    the suffix of its measurement type. The identifiers are text and do not
    match those of release v0.0.2, and ``version`` holds the version of the
    method of each measurement type.

    Args:
        df: Rows read with :func:`read_flight_events`.

    Returns:
        A pandas DataFrame with the columns ``id``, ``event_id``, ``type``,
        ``value`` and ``version``, in the order of the measurement columns
        and then of the rows of ``df``.
    """
    import pandas as pd

    parts = []
    for c in MEASUREMENT_COLUMNS:
        if c.name not in df.columns:
            continue
        keep = df[c.name].notna()
        if c.source in CLASSIC_MEASUREMENTS_ALWAYS:
            keep = df["event_id"].notna()
        rows = df.loc[keep, ["event_id", c.name]]
        parts.append(
            pd.DataFrame(
                {
                    "id": rows["event_id"] + MEASUREMENT_SUFFIX[c.source],
                    "event_id": rows["event_id"],
                    "type": c.source,
                    "value": rows[c.name].astype("float64"),
                    "version": MEASUREMENT_VERSION[c.source],
                }
            )
        )
    if not parts:
        return pd.DataFrame(
            {name: pd.Series(dtype="object") for name in CLASSIC_MEASUREMENT_COLUMNS}
        )
    return pd.concat(parts, ignore_index=True)[list(CLASSIC_MEASUREMENT_COLUMNS)]


def as_flight_list_v0(df):
    """The flight list in the layout of release v0.0.2.

    The columns of release v0.0.2, lower case and in its order: ``id``,
    ``icao24``, ``flt_id``, ``dof``, ``adep``, ``ades``, ``adep_p``,
    ``ades_p``, ``registration``, ``model``, ``typecode``,
    ``icao_aircraft_class``, ``icao_operator``, ``first_seen``,
    ``last_seen`` and ``version``. The ``id`` is text and does not match the
    identifiers of release v0.0.2, and ``version`` holds the version of the
    flight list method.

    Args:
        df: Rows read with :func:`read_flight_list`.
    """
    return df[list(CLASSIC_FLIGHT_LIST)].rename(columns=CLASSIC_FLIGHT_LIST)


def _duckdb_address(url: str) -> str:
    if url.startswith("file://"):
        return url[len("file://") :]
    if url.startswith("s3a://"):
        return "s3://" + url[len("s3a://") :]
    return url


def _duckdb_files(table: str, base, start, end) -> str:
    if start is None:
        return "'" + _duckdb_address(f"{base_url(base)}{table}/*.parquet") + "'"
    files = [_duckdb_address(file_url(table, d, base)) for d in _days(start, end)]
    return "[" + ", ".join("'" + f.replace("'", "''") + "'" for f in files) + "]"


def _months(start: date, end: Optional[date]) -> List[date]:
    """The first day of every month from ``start`` to ``end``."""
    months = []
    for d in _days(start, end):
        first = d.replace(day=1)
        if first not in months:
            months.append(first)
    return months


def _duckdb_index_files(base, start, end) -> Optional[str]:
    """The index files to read, or None when a local folder has no index."""
    root = base_url(base)
    local = _local_path(root)
    if start is None:
        if local is not None and not glob.glob(os.path.join(local, "index", "*.parquet")):
            return None
        return "'" + _duckdb_address(f"{root}index/*.parquet") + "'"
    files = [f"{root}index/flight_index_{m:%Y%m}.parquet" for m in _months(start, end)]
    if local is not None:
        files = [f for f in files if os.path.isfile(_local_path(f))]
        if not files:
            return None
    return "[" + ", ".join("'" + _duckdb_address(f).replace("'", "''") + "'" for f in files) + "]"


def duckdb_views(
    base: Optional[str] = None,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> str:
    """DuckDB statements that define views over published files.

    The views have the same names as the Trino tables and views:

    * ``flight_list`` and ``flight_events``: the files as published.
    * ``flight_index``: the monthly flight index, which tells which daily
      file holds a flight. Left out when a local folder has no index.
    * ``flight_list_v0``, ``events`` and ``measurements``: the separate
      flight list, events and measurements tables in the layout of release
      v0.0.2 (see :func:`as_flight_list_v0`, :func:`as_events` and
      :func:`as_measurements`, which give the same rows).

    Run the text in a DuckDB connection::

        import duckdb
        con = duckdb.connect()
        con.execute(published.duckdb_views(start=date(2026, 6, 14)))
        con.sql("select type, count(*) from events group by type")

    Reading over ``https://`` or ``s3://`` needs DuckDB's ``httpfs``
    extension, which DuckDB loads by itself when it is available.

    Args:
        base: Base address; see :func:`base_url`.
        start: First day. When omitted, every file in the folders is read,
            which works for a local folder or S3 but not over ``https://``.
        end: Last day, included. When omitted, only ``start`` is read.

    Returns:
        The SQL text.

    Raises:
        ImportError: If DuckDB is not installed.
    """
    try:
        import duckdb  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            f"the DuckDB views need the duckdb package; install it with {_READER_EXTRA}"
        ) from exc
    from opdi.sql.render import (
        FLIGHT_EVENTS_FILES,
        FLIGHT_INDEX_FILES,
        FLIGHT_INDEX_VIEW_START,
        FLIGHT_LIST_FILES,
    )

    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql", "views.sql")
    with open(path, encoding="utf-8") as handle:
        text = handle.read()
    index = _duckdb_index_files(base, start, end)
    if index is None:
        text = text[: text.index(FLIGHT_INDEX_VIEW_START)]
    else:
        text = text.replace(FLIGHT_INDEX_FILES, index)
    return text.replace(FLIGHT_LIST_FILES, _duckdb_files("flight_list", base, start, end)).replace(
        FLIGHT_EVENTS_FILES, _duckdb_files("flight_events", base, start, end)
    )
