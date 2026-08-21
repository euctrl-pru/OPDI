"""Shared fixtures for the OPDI test suite.

Everything here runs against a local Spark session. No cluster, no Hive, no
Iceberg, no credentials -- the suite must be runnable on a laptop and in CI.
"""

import datetime as dt
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

# PYSPARK_PYTHON names the interpreter for the *workers*. On the cluster that
# must be `python3` -- the image's interpreter, whose path this venv does not
# share. Under pytest the workers are local, so the same value picks up the
# system 3.13 against a 3.10 driver and every Spark test dies with
# PYTHON_VERSION_MISMATCH. Pin it to the interpreter actually running the
# suite, before any SparkSession is built.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks"))
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

#: Minimal ``osn_tracks``-shaped schema: everything the cleaning stages touch.
TRACK_SCHEMA = StructType(
    [
        StructField("event_time", TimestampType(), True),
        StructField("track_id", StringType(), True),
        StructField("icao24", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("baro_altitude", DoubleType(), True),
        StructField("geo_altitude", DoubleType(), True),
        StructField("velocity", DoubleType(), True),
        StructField("heading", DoubleType(), True),
        StructField("vert_rate", DoubleType(), True),
        StructField("on_ground", BooleanType(), True),
        StructField("last_contact", DoubleType(), True),
        StructField("callsign", StringType(), True),
        StructField("near_airport", BooleanType(), True),
        StructField("field_elev_ft", DoubleType(), True),
    ]
)

_EPOCH = dt.datetime(2024, 6, 1, 12, 0, 0)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("opdi-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def make_track(
    spark: SparkSession,
    samples: List[Dict[str, Any]],
    track_id: str = "trk-1",
):
    """Build a one-track DataFrame from a list of partial sample dicts.

    Each dict may set ``t`` (seconds after an arbitrary epoch) plus any track
    column. Anything unset gets a benign default, so a test only has to state
    the defect it cares about.
    """
    rows = []
    for i, sample in enumerate(samples):
        offset = sample.get("t", i)
        rows.append(
            (
                _EPOCH + dt.timedelta(seconds=float(offset)),
                sample.get("track_id", track_id),
                sample.get("icao24", "abc123"),
                _as_float(sample.get("lat", 50.0)),
                _as_float(sample.get("lon", 4.0)),
                _as_float(sample.get("baro_altitude", 10000.0)),
                _as_float(sample.get("geo_altitude", 10000.0)),
                _as_float(sample.get("velocity", 200.0)),
                _as_float(sample.get("heading", 90.0)),
                _as_float(sample.get("vert_rate", 0.0)),
                sample.get("on_ground", False),
                _as_float(sample.get("last_contact", float(offset))),
                sample.get("callsign", "TEST123"),
                sample.get("near_airport", True),
                _as_float(sample.get("field_elev_ft", 0.0)),
            )
        )
    return spark.createDataFrame(rows, schema=TRACK_SCHEMA)


def _as_float(value: Any) -> Optional[float]:
    return None if value is None else float(value)


def column_values(df, name: str) -> List[Any]:
    """Values of *name* ordered by ``event_time`` -- the natural read order."""
    return [r[name] for r in df.orderBy("event_time").select(name).collect()]
