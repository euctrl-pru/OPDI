"""
Ingestion: OpenSky Network state-vectors from MinIO / S3.

Handles MinIO client setup, file listing, downloading parquet files in chunks,
schema normalisation and writing into the ``osn_statevectors_v2`` Iceberg table.
"""

from __future__ import annotations

import os
import subprocess
import time
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_date


# ---------------------------------------------------------------------------
# MinIO helpers
# ---------------------------------------------------------------------------

def _execute_shell(command: str) -> tuple[str, str]:
    proc = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    stdout, stderr = proc.communicate()
    return stdout.decode().strip(), stderr.decode().strip()


def setup_mc() -> None:
    """Download and configure the MinIO client (``mc``)."""
    _execute_shell("curl -O https://dl.min.io/client/mc/release/linux-amd64/mc")
    _execute_shell("chmod +x mc")
    _execute_shell("./mc alias set opensky https://s3.opensky-network.org $OSN_USERNAME $OSN_KEY")


def list_mc_files(year_filter: Optional[list[str]] = None) -> list[str]:
    """Return parquet file paths matching *year_filter* from the EC datadump bucket."""
    stdout, _ = _execute_shell('./mc find opensky/ec-datadump/ --path "*/states_*.parquet"')
    files = stdout.split("\n")
    if year_filter:
        files = [f for f in files if any(y in f for y in year_filter)]
    return files


def _remove_partial_files(directory: str, suffix: str = ".parquet.part.minio") -> None:
    """Remove partially-downloaded MinIO files."""
    try:
        for fn in os.listdir(directory):
            if fn.endswith(suffix):
                path = os.path.join(directory, fn)
                os.remove(path)
    except FileNotFoundError:
        pass


# ---------------------------------------------------------------------------
# Column mapping (camelCase → snake_case)
# ---------------------------------------------------------------------------

COLUMN_NAME_MAP: dict[str, str] = {
    "eventTime": "event_time",
    "icao24": "icao24",
    "lat": "lat",
    "lon": "lon",
    "velocity": "velocity",
    "heading": "heading",
    "vertRate": "vert_rate",
    "callsign": "callsign",
    "onGround": "on_ground",
    "alert": "alert",
    "spi": "spi",
    "squawk": "squawk",
    "baroAltitude": "baro_altitude",
    "geoAltitude": "geo_altitude",
    "lastPosUpdate": "last_pos_update",
    "lastContact": "last_contact",
    "serials": "serials",
}


# ---------------------------------------------------------------------------
# Ingest function
# ---------------------------------------------------------------------------

def ingest_statevectors(
    spark: SparkSession,
    project: str = "project_opdi",
    local_folder: str = "OPDI_live/data/ec-datadump",
    log_path: str = "OPDI_live/logs/01_osn_statevectors_etl.log",
    chunksize: int = 250,
    year_filter: Optional[list[str]] = None,
) -> None:
    """
    Download state-vector parquet files from MinIO in chunks, normalise column
    names and append to the ``osn_statevectors_v2`` Iceberg table.

    Parameters
    ----------
    spark : SparkSession
    project : str
        Target Iceberg project/database.
    local_folder : str
        Temporary download directory.
    log_path : str
        Path to the ETL progress log.
    chunksize : int
        Number of files to download per batch.
    year_filter : list[str], optional
        E.g. ``["2024-", "2025-"]`` to restrict which years to fetch.
    """
    setup_mc()
    files_to_download = list_mc_files(year_filter=year_filter)

    # Read already-processed filenames
    if os.path.exists(log_path):
        with open(log_path) as fh:
            processed_files = set(fh.read().splitlines())
    else:
        processed_files = set()

    os.makedirs(local_folder, exist_ok=True)

    for i in range(0, len(files_to_download), chunksize):
        downloaded: list[str] = []
        for remote_file in files_to_download[i : i + chunksize]:
            file_name = remote_file.split("/")[-1]
            if file_name in processed_files:
                continue
            local_path = os.path.join(local_folder, file_name)
            _, err = _execute_shell(f'./mc cp "{remote_file}" {local_path}')
            if err:
                print(f"Error: {err}")
            else:
                downloaded.append(file_name)

        time.sleep(1)
        _remove_partial_files(local_folder)

        if not downloaded:
            continue

        df = spark.read.option("mergeSchema", "true").parquet(local_folder)

        # Normalise column names
        for old, new in COLUMN_NAME_MAP.items():
            df = df.withColumnRenamed(old, new)
        df = df.withColumnRenamed("time", "event_time")

        # Cast event_time to timestamp
        df = df.withColumn("event_time", from_unixtime(col("event_time")).cast("timestamp"))

        # Partition by day for efficient downstream queries
        df = (
            df.withColumn("event_time_day", to_date(col("event_time")))
            .repartition("event_time_day")
            .orderBy("event_time_day")
            .drop("event_time_day")
        )

        df.writeTo(f"`{project}`.`osn_statevectors_v2`").append()

        # Cleanup local files and update log
        for fn in downloaded:
            os.remove(os.path.join(local_folder, fn))
        with open(log_path, "a") as fh:
            for fn in downloaded:
                fh.write(fn + "\n")
                processed_files.add(fn)
