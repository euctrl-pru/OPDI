"""
Ingestion: OpenSky Network aircraft database.

Downloads the aircraft metadata CSV from S3, cleans it and writes to an Iceberg
table.
"""

from __future__ import annotations

import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when


DEFAULT_URL = (
    "https://s3.opensky-network.org/data-samples/metadata/"
    "aircraft-database-complete-2024-10.csv"
)


def _conv_str(x):
    if pd.isnull(x):
        return None
    return str(x)


def ingest_aircraft_db(
    spark: SparkSession,
    project: str = "project_opdi",
    url: str = DEFAULT_URL,
) -> None:
    """
    Download the OSN aircraft database CSV, clean it and append to
    ``{project}.osn_aircraft_db``.

    Parameters
    ----------
    spark : SparkSession
    project : str
        Target Iceberg project/database.
    url : str
        URL of the aircraft database CSV.
    """
    pdf = pd.read_csv(url, quotechar="'", on_bad_lines="error", low_memory=False)
    pdf = pdf[
        ["icao24", "registration", "model", "typecode", "icaoAircraftClass", "operatorIcao"]
    ].map(_conv_str)

    sdf = spark.createDataFrame(pdf.to_dict(orient="records"))
    sdf = sdf.select(
        col("icao24"),
        col("registration"),
        col("model"),
        col("typecode"),
        col("icaoAircraftClass").alias("icao_aircraft_class"),
        col("operatorIcao").alias("icao_operator"),
    )

    # Trim whitespace and convert empty strings to NULL
    cleaned = sdf.select(
        *[trim(when(col(c) == "", None).otherwise(col(c))).alias(c) for c in sdf.columns]
    )

    cleaned.writeTo(f"`{project}`.`osn_aircraft_db`").append()
