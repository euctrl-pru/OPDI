"""
Transform: Track identification, H3 tagging, distance & altitude cleaning.

Takes raw state-vectors and produces enriched tracks with:
- ``track_id`` based on icao24+callsign with 30-min gap splitting
- H3 hex IDs at configurable resolutions
- Cumulative great-circle distance (NM)
- Cleaned geo/baro altitudes (unrealistic climb rates smoothed)
"""

from __future__ import annotations

import gc
import os
from datetime import date, datetime
from typing import Optional

import pandas as pd

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    abs as F_abs,
    avg,
    col,
    lag,
    lit,
    to_date,
    unix_timestamp,
    when,
)
import h3_pyspark

from opdi.utils import generate_months, get_start_end_of_month


# ---------------------------------------------------------------------------
# Distance helper
# ---------------------------------------------------------------------------

def add_cumulative_distance(df: DataFrame) -> DataFrame:
    """
    Haversine-based cumulative great-circle distance in nautical miles,
    partitioned by ``track_id`` and ordered by ``event_time``.
    """
    win_lag = Window.partitionBy("track_id").orderBy("event_time")
    win_cum = (
        Window.partitionBy("track_id")
        .orderBy("event_time")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    df = (
        df
        .withColumn("lat_rad", F.radians(col("lat")))
        .withColumn("lon_rad", F.radians(col("lon")))
        .withColumn("prev_lat_rad", lag("lat_rad").over(win_lag))
        .withColumn("prev_lon_rad", lag("lon_rad").over(win_lag))
        .withColumn(
            "a",
            F.sin((col("lat_rad") - col("prev_lat_rad")) / 2) ** 2
            + F.cos(col("prev_lat_rad")) * F.cos(col("lat_rad"))
            * F.sin((col("lon_rad") - col("prev_lon_rad")) / 2) ** 2,
        )
        .withColumn("c", 2 * F.atan2(F.sqrt(col("a")), F.sqrt(1 - col("a"))))
        .withColumn("distance_km", 6371 * col("c"))
        .withColumn("segment_distance_nm", col("distance_km") / 1.852)
        .withColumn("cumulative_distance_nm", F.sum("segment_distance_nm").over(win_cum))
        .drop("lat_rad", "lon_rad", "prev_lat_rad", "prev_lon_rad", "a", "c", "distance_km")
    )
    return df


# ---------------------------------------------------------------------------
# Altitude cleaning helper
# ---------------------------------------------------------------------------

def add_clean_altitude(df: DataFrame, col_name: str) -> DataFrame:
    """
    Replace unrealistic climb/descent points (>5 000 ft/min ≈ 25.4 m/s)
    with a rolling average.
    """
    rate_threshold = 25.4  # m/s
    time_window = 300  # seconds for rolling average

    win_track = Window.partitionBy("track_id").orderBy("event_time")

    df = (
        df
        .withColumn(f"prev_{col_name}", lag(col_name).over(win_track))
        .withColumn("prev_event_time", lag("event_time").over(win_track))
        .withColumn(
            "time_diff",
            (unix_timestamp("event_time") - unix_timestamp("prev_event_time")).cast("double"),
        )
        .withColumn("altitude_diff", col(col_name) - col(f"prev_{col_name}"))
        .withColumn("rate_of_climb", col("altitude_diff") / col("time_diff"))
    )

    df = df.withColumn("event_time_epoch", unix_timestamp("event_time").cast("bigint"))
    win_avg = (
        Window.partitionBy("track_id")
        .orderBy("event_time_epoch")
        .rangeBetween(-time_window, time_window)
    )
    df = df.withColumn(f"smoothed_{col_name}", avg(col_name).over(win_avg))

    df = df.withColumn(
        f"{col_name}_c",
        when(F_abs(col("rate_of_climb")) > rate_threshold, col(f"smoothed_{col_name}"))
        .otherwise(col(col_name)),
    )

    df = df.drop(
        f"smoothed_{col_name}",
        f"prev_{col_name}",
        "prev_event_time",
        "time_diff",
        "altitude_diff",
        "rate_of_climb",
        "event_time_epoch",
    )
    return df


# ---------------------------------------------------------------------------
# Main track processing
# ---------------------------------------------------------------------------

def process_tracks(
    spark: SparkSession,
    project: str,
    h3_resolutions: list[int],
    month: date,
) -> None:
    """
    Build tracks for a single calendar month and append to ``osn_tracks``.

    Steps:
    1. Read state-vectors for the month.
    2. Assign ``track_id`` (icao24 + callsign hash, 30-min gap split).
    3. Tag each row with H3 hex IDs at requested resolutions.
    4. Compute cumulative distance.
    5. Clean ``geo_altitude`` and ``baro_altitude``.
    6. Write to Iceberg.

    Parameters
    ----------
    spark : SparkSession
    project : str
        E.g. ``"project_opdi"``.
    h3_resolutions : list[int]
        E.g. ``[7, 12]``.
    month : date
        First day of the month to process.
    """
    start_ts, end_ts = get_start_end_of_month(month)
    start_str = datetime.utcfromtimestamp(start_ts).strftime("%Y-%m-%d %H:%M:%S")
    end_str = datetime.utcfromtimestamp(end_ts).strftime("%Y-%m-%d %H:%M:%S")

    df = spark.sql(f"""
        SELECT * FROM `{project}`.`osn_statevectors_v2`
        WHERE event_time >= TIMESTAMP('{start_str}')
          AND event_time < TIMESTAMP('{end_str}')
    """)

    h3_cols = [f"h3_res_{r}" for r in h3_resolutions]
    original_columns = df.columns + ["track_id"] + h3_cols

    # --- Track ID assignment ---
    df = df.withColumn("event_time_ts", F.to_timestamp("event_time"))
    df = df.withColumn(
        "group_id",
        F.substring(F.sha2(F.concat_ws("", "icao24", "callsign"), 256), 1, 16),
    )

    win_group = Window.partitionBy("group_id").orderBy("event_time_ts")
    df = df.withColumn("prev_event_time_ts", lag("event_time_ts").over(win_group))
    df = df.withColumn(
        "time_gap_minutes",
        (unix_timestamp("event_time_ts") - unix_timestamp("prev_event_time_ts")) / 60,
    )

    split_cond = (
        (col("time_gap_minutes") > 30)
        | ((col("time_gap_minutes") > 15) & (col("baro_altitude") < 1524))
    )
    df = df.withColumn("new_group_flag", when(split_cond, 1).otherwise(0))

    cum_win = (
        Window.partitionBy("group_id")
        .orderBy("event_time_ts")
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    df = df.withColumn("offset", F.sum("new_group_flag").over(cum_win))

    df = df.withColumn(
        "track_id",
        F.concat(
            col("group_id"), lit("_"), col("offset"),
            lit("_"), F.year("event_time_ts").cast("string"),
            lit("_"), F.month("event_time_ts").cast("string"),
        ),
    )

    # --- H3 tagging ---
    for res in h3_resolutions:
        df = df.withColumn("h3_resolution", lit(res))
        df = df.withColumn(f"h3_res_{res}", h3_pyspark.geo_to_h3("lat", "lon", "h3_resolution"))

    df = df.select(original_columns)

    # --- Distance ---
    df = add_cumulative_distance(df)

    # --- Altitude cleaning ---
    df = add_clean_altitude(df, "geo_altitude")
    df = add_clean_altitude(df, "baro_altitude")

    # --- Write ---
    df = (
        df.withColumn("event_time_day", to_date(col("event_time")))
        .repartition("event_time_day")
        .orderBy("event_time_day")
        .drop("event_time_day")
    )
    df.writeTo(f"`{project}`.`osn_tracks`").append()

    df.unpersist(blocking=True)
    spark.catalog.clearCache()
    gc.collect()


# ---------------------------------------------------------------------------
# Batch runner
# ---------------------------------------------------------------------------

def run_tracks_pipeline(
    spark: SparkSession,
    project: str = "project_opdi",
    h3_resolutions: Optional[list[int]] = None,
    start_month: Optional[date] = None,
    end_month: Optional[date] = None,
    log_path: str = "logs/02_osn-tracks-etl-log.parquet",
) -> None:
    """
    Process tracks for all unprocessed months in the given range.

    Parameters
    ----------
    spark : SparkSession
    project : str
    h3_resolutions : list[int]
    start_month, end_month : date
    log_path : str
        Parquet log tracking processed months.
    """
    import dateutil.relativedelta

    if h3_resolutions is None:
        h3_resolutions = [7, 12]
    if start_month is None:
        start_month = date(2022, 1, 1)
    if end_month is None:
        end_month = date.today() - dateutil.relativedelta.relativedelta(months=1)

    months = generate_months(start_month, end_month)

    if os.path.isfile(log_path):
        done = pd.read_parquet(log_path).months.tolist()
    else:
        done = []

    for m in months:
        if m in done:
            continue
        print(f"Processing tracks for {m} … ({datetime.now()})")
        process_tracks(spark, project, h3_resolutions, m)
        done.append(m)
        pd.DataFrame({"months": done}).to_parquet(log_path)
