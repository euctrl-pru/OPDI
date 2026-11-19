"""The curated Point Merge System table (``data/point_merge_systems.csv``).

One row per merge point: where it is, how far the sequencing legs sit from
it, which levels the legs are flown at, and when the system was in operation.
The point-merge detector in ``pipeline/point_merge.py`` tests every arrival
at a listed airport against these rows. The table ships inside the package
because it is small, curated and versioned with the code; ``data/README.md``
says where each number comes from.
"""

import datetime as dt
from importlib.resources import files
from typing import Optional

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

PMS_CSV = str(files("opdi.reference") / "data" / "point_merge_systems.csv")

PMS_SCHEMA = StructType(
    [
        StructField("apt_icao", StringType(), False),
        StructField("pms_id", StringType(), False),
        StructField("merge_point", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("legs_mp_min_nm", DoubleType(), True),
        StructField("legs_mp_max_nm", DoubleType(), True),
        StructField("leg_level_min_ft", DoubleType(), True),
        StructField("leg_level_max_ft", DoubleType(), True),
        StructField("valid_from", DateType(), True),
        StructField("valid_to", DateType(), True),
        StructField("source", StringType(), True),
        StructField("confidence", StringType(), False),
    ]
)


def _read(path: Optional[str]) -> pd.DataFrame:
    df = pd.read_csv(
        path or PMS_CSV,
        dtype={
            "apt_icao": str,
            "pms_id": str,
            "merge_point": str,
            "source": str,
            "confidence": str,
        },
    )
    for c in (
        "lat",
        "lon",
        "legs_mp_min_nm",
        "legs_mp_max_nm",
        "leg_level_min_ft",
        "leg_level_max_ft",
    ):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ("valid_from", "valid_to"):
        df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
    df = df.astype(object).where(df.notna(), None)
    for c in (
        "lat",
        "lon",
        "legs_mp_min_nm",
        "legs_mp_max_nm",
        "leg_level_min_ft",
        "leg_level_max_ft",
    ):
        df[c] = df[c].map(lambda v: None if v is None else float(v))
    return df


def load_point_merge_systems(spark: SparkSession, path: Optional[str] = None) -> DataFrame:
    """Every merge point with coordinates (``confidence`` high or medium)."""
    pdf = _read(path)
    pdf = pdf[pdf["confidence"].isin(["high", "medium"])]
    rows = [tuple(r[c] for c in PMS_SCHEMA.fieldNames()) for _, r in pdf.iterrows()]
    return spark.createDataFrame(rows, PMS_SCHEMA)


def point_merge_systems_for_day(
    spark: SparkSession, day: dt.date, path: Optional[str] = None
) -> DataFrame:
    """The merge points in operation on ``day``."""
    ref = load_point_merge_systems(spark, path)
    d = F.lit(day)
    return ref.filter(
        (F.col("valid_from").isNull() | (F.col("valid_from") <= d))
        & (F.col("valid_to").isNull() | (F.col("valid_to") >= d))
    )
