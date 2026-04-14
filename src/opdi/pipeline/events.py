"""
Flight events and measurements ETL module.

Detects and records flight events (milestones) from track data.

Event types:

* Horizontal segment events -- flight phases (GND, CL, DE, CR, LVL),
  top-of-climb, top-of-descent, take-off, landing using fuzzy logic
* Vertical crossing events -- flight level crossings (FL50, FL70, FL100, FL245)
* Airport events -- entry/exit of runway, taxiway, apron via H3 layout matching
* First/last seen events per track

Also produces measurement records (distance flown, time passed) linked
to each event.

Ported from ``OPDI-live/python/v2.0.0/04_opdi_flight_events_etl.py``.
"""

import os
from datetime import date, datetime
from typing import List, Optional

import pandas as pd

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    abs as f_abs,
    avg,
    col,
    concat,
    concat_ws,
    explode,
    filter,
    lag,
    lead,
    lit,
    max as f_max,
    min as f_min,
    monotonically_increasing_id,
    row_number,
    split,
    struct,
    sum as f_sum,
    to_json,
    to_timestamp,
    when,
)
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

from opdi.config import OPDIConfig
from opdi.utils.datetime_helpers import generate_months, get_start_end_of_month


# ======================================================================
# Fuzzy membership functions for flight phase classification
# ======================================================================

def zmf(column, a, b):
    """Zero-order membership function (Z-shaped). Returns 1 below a, 0 above b."""
    return F.when(column <= a, 1).when(column >= b, 0).otherwise((b - column) / (b - a))


def gaussmf(column, mean, sigma):
    """Gaussian membership function. Peak at mean, width controlled by sigma."""
    return F.exp(-((column - mean) ** 2) / (2 * sigma ** 2))


def smf(column, a, b):
    """S-shaped membership function. Returns 0 below a, 1 above b."""
    return F.when(column <= a, 0).when(column >= b, 1).otherwise((column - a) / (b - a))


# ======================================================================
# Event calculation functions
# ======================================================================

def calculate_horizontal_segment_events(sdf_input: DataFrame) -> DataFrame:
    """
    Detect flight phase transitions and horizontal segment events.

    Uses fuzzy logic membership functions to classify each state vector
    into flight phases (GND, CL, DE, CR, LVL) based on altitude,
    rate of climb, and speed. Detects phase transitions to produce
    events: level-start, level-end, top-of-climb, top-of-descent,
    take-off, landing.

    Args:
        sdf_input: Input DataFrame with track data including
            baro_altitude_c, vert_rate, velocity.

    Returns:
        DataFrame of detected events with columns: track_id, type,
        event_time, lon, lat, altitude_ft, cumulative_distance_nm,
        cumulative_time_s, info.
    """
    df = sdf_input.select(
        "track_id", "lat", "lon", "event_time", "baro_altitude_c",
        "vert_rate", "velocity", "cumulative_distance_nm", "cumulative_time_s",
    )

    # Convert units: meters -> feet, m/s -> ft/min, m/s -> knots
    df = df.withColumn("alt", col("baro_altitude_c") * 3.28084)
    df = df.withColumn("roc", col("vert_rate") * 196.850394)
    df = df.withColumn("spd", col("velocity") * 1.94384)

    # Apply fuzzy membership functions
    df = df.withColumn("alt_gnd", zmf(col("alt"), 0, 200))
    df = df.withColumn("alt_lo", gaussmf(col("alt"), 10000, 10000))
    df = df.withColumn("alt_hi", gaussmf(col("alt"), 35000, 20000))
    df = df.withColumn("roc_zero", gaussmf(col("roc"), 0, 100))
    df = df.withColumn("roc_plus", smf(col("roc"), 10, 1000))
    df = df.withColumn("roc_minus", zmf(col("roc"), -1000, -10))
    df = df.withColumn("spd_hi", gaussmf(col("spd"), 600, 100))
    df = df.withColumn("spd_md", gaussmf(col("spd"), 300, 100))
    df = df.withColumn("spd_lo", gaussmf(col("spd"), 0, 50))

    df.cache()

    # Fuzzy logic rules
    df = df.withColumn("rule_ground", F.least(col("alt_gnd"), col("roc_zero"), col("spd_lo")))
    df = df.withColumn("rule_climb", F.least(col("alt_lo"), col("roc_plus"), col("spd_md")))
    df = df.withColumn("rule_descent", F.least(col("alt_lo"), col("roc_minus"), col("spd_md")))
    df = df.withColumn("rule_cruise", F.least(col("alt_hi"), col("roc_zero"), col("spd_hi")))
    df = df.withColumn("rule_level", F.least(col("alt_lo"), col("roc_zero"), col("spd_md")))

    # Determine phase by maximum rule activation
    df = df.withColumn(
        "aggregated",
        F.greatest(
            col("rule_ground"), col("rule_climb"), col("rule_descent"),
            col("rule_cruise"), col("rule_level"),
        ),
    )

    df = df.withColumn(
        "flight_phase",
        F.when(col("aggregated") == col("rule_ground"), "GND")
        .when(col("aggregated") == col("rule_climb"), "CL")
        .when(col("aggregated") == col("rule_descent"), "DE")
        .when(col("aggregated") == col("rule_cruise"), "CR")
        .when(col("aggregated") == col("rule_level"), "LVL"),
    )

    df = (
        df.withColumnRenamed("alt", "altitude_ft")
        .withColumnRenamed("roc", "roc_ft_min")
        .withColumnRenamed("spd", "speed_kt")
    )

    df = df.select(
        "track_id", "lat", "lon", "event_time", "cumulative_distance_nm",
        "cumulative_time_s", "altitude_ft", "roc_ft_min", "speed_kt", "flight_phase",
    )

    df.cache()

    # Detect phase transitions
    window_phase = Window.partitionBy("track_id").orderBy("event_time")
    window_cumulative = (
        Window.partitionBy("track_id")
        .orderBy("event_time")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    df = df.withColumn("prev_phase", lag("flight_phase", 1, "None").over(window_phase))
    df = df.withColumn("next_phase", lead("flight_phase", 1, "None").over(window_phase))

    df.cache()

    # Identify TOC and TOD (first/last cruise points)
    df = df.withColumn(
        "first_cr_time",
        f_min(when(col("flight_phase") == "CR", col("event_time"))).over(window_cumulative),
    )
    df = df.withColumn(
        "last_cr_time",
        f_max(when(col("flight_phase") == "CR", col("event_time"))).over(window_cumulative),
    )

    start_of_segment = (col("flight_phase").isin("CR", "LVL")) & (
        col("prev_phase") != col("flight_phase")
    )
    df = df.withColumn("start_of_segment", start_of_segment)
    df = df.withColumn(
        "segment_count",
        f_sum(when(col("start_of_segment"), 1).otherwise(0)).over(window_cumulative),
    )

    # Create event type arrays
    df = df.withColumn(
        "milestone_types",
        F.when(
            col("event_time") == col("first_cr_time"),
            F.array(lit("level-start"), lit("top-of-climb")),
        )
        .when(
            col("event_time") == col("last_cr_time"),
            F.array(lit("level-end"), lit("top-of-descent")),
        )
        .when(start_of_segment, F.array(lit("level-start")))
        .when(
            (col("flight_phase").isin("CR", "LVL")) & (col("next_phase") != col("flight_phase")),
            F.array(lit("level-end")),
        )
        .when(
            (col("prev_phase") == "GND") & (col("next_phase") == "CL"),
            F.array(lit("take-off")),
        )
        .when(
            (col("prev_phase") == "DE") & (col("flight_phase") == "GND"),
            F.array(lit("landing")),
        )
        .otherwise(F.array()),
    )

    # Explode to one row per event
    df_exploded = df.select("*", explode(col("milestone_types")).alias("type"))
    df_exploded = df_exploded.filter("type IS NOT NULL")

    df_events = df_exploded.select(
        col("track_id"), col("type"), col("event_time"),
        col("lon"), col("lat"), col("altitude_ft"),
        col("cumulative_distance_nm"), col("cumulative_time_s"),
    )

    df_events = df_events.dropDuplicates(["track_id", "type", "event_time"])
    df_events = df_events.withColumn("info", lit(""))

    return df_events


def calculate_vertical_crossing_events(sdf_input: DataFrame) -> DataFrame:
    """
    Detect flight level crossing events (FL50, FL70, FL100, FL245).

    For each track, identifies the first and last time the aircraft
    crosses each monitored flight level boundary.

    Args:
        sdf_input: Input DataFrame with baro_altitude_c.

    Returns:
        DataFrame of crossing events with columns: track_id, type,
        event_time, lon, lat, altitude_ft, cumulative_distance_nm,
        cumulative_time_s, info.
    """
    df = sdf_input.select(
        "track_id", "lat", "lon", "event_time", "baro_altitude_c",
        "vert_rate", "velocity", "cumulative_distance_nm", "cumulative_time_s",
    )

    df = df.withColumn("altitude_ft", col("baro_altitude_c") * 3.28084)
    df = df.withColumn("FL", (col("baro_altitude_c") * 3.28084 / 100).cast(DoubleType()))

    window_spec = Window.partitionBy("track_id").orderBy("event_time")
    df = df.withColumn("next_FL", F.lead("FL").over(window_spec)).cache()

    # Define crossing conditions for each FL
    crossing_conditions = [
        ((col("FL") < 50) & (col("next_FL") >= 50), 50),
        ((col("FL") >= 50) & (col("next_FL") < 50), 50),
        ((col("FL") < 70) & (col("next_FL") >= 70), 70),
        ((col("FL") >= 70) & (col("next_FL") < 70), 70),
        ((col("FL") < 100) & (col("next_FL") >= 100), 100),
        ((col("FL") >= 100) & (col("next_FL") < 100), 100),
        ((col("FL") < 245) & (col("next_FL") >= 245), 245),
        ((col("FL") >= 245) & (col("next_FL") < 245), 245),
    ]

    crossing_column = F.when(crossing_conditions[0][0], crossing_conditions[0][1])
    for cond, fl_value in crossing_conditions[1:]:
        crossing_column = crossing_column.when(cond, fl_value)

    df = df.withColumn("crossing", crossing_column)
    crossing_points = df.filter(col("crossing").isNotNull())
    crossing_points = crossing_points.filter(f_abs(col("crossing") - col("FL")) < 10).cache()

    # First and last crossings per FL
    window_asc = Window.partitionBy("track_id", "crossing").orderBy("event_time")
    window_desc = Window.partitionBy("track_id", "crossing").orderBy(col("event_time").desc())

    crossing_points = crossing_points.withColumn("row_asc", row_number().over(window_asc))
    crossing_points = crossing_points.withColumn("row_desc", row_number().over(window_desc))
    crossing_points.cache()

    first_crossings = (
        crossing_points.filter(col("row_asc") == 1)
        .drop("row_asc", "row_desc")
        .withColumn("type", concat(lit("first-xing-fl"), col("crossing").cast("string")))
    )
    last_crossings = (
        crossing_points.filter(col("row_desc") == 1)
        .drop("row_asc", "row_desc")
        .withColumn("type", concat(lit("last-xing-fl"), col("crossing").cast("string")))
    )

    all_crossings = first_crossings.union(last_crossings)

    df_events = all_crossings.select(
        "track_id", "type", "event_time", "lon", "lat",
        "altitude_ft", "cumulative_distance_nm", "cumulative_time_s",
    )
    df_events = df_events.dropDuplicates([
        "track_id", "type", "event_time", "lon", "lat",
        "altitude_ft", "cumulative_distance_nm", "cumulative_time_s",
    ])
    df_events = df_events.withColumn("info", lit(""))

    return df_events


def calculate_firstseen_lastseen_events(sdf_input: DataFrame) -> DataFrame:
    """
    Calculate first_seen and last_seen events for each track.

    Args:
        sdf_input: Input DataFrame with track data.

    Returns:
        DataFrame with first/last seen events.
    """
    df = sdf_input.select(
        "track_id", "lat", "lon", "event_time", "baro_altitude_c",
        "vert_rate", "velocity", "cumulative_distance_nm", "cumulative_time_s",
    )

    df = df.withColumn("altitude_ft", col("baro_altitude_c") * 3.28084)

    window_asc = Window.partitionBy("track_id").orderBy("event_time")
    window_desc = Window.partitionBy("track_id").orderBy(col("event_time").desc())

    df = df.withColumn("row_asc", row_number().over(window_asc))
    df = df.withColumn("row_desc", row_number().over(window_desc))

    first_seen = (
        df.filter(col("row_asc") == 1)
        .drop("row_asc", "row_desc")
        .withColumn("type", lit("first_seen"))
    )
    last_seen = (
        df.filter(col("row_desc") == 1)
        .drop("row_asc", "row_desc")
        .withColumn("type", lit("last_seen"))
    )

    all_events = first_seen.union(last_seen)

    df_events = all_events.select(
        "track_id", "type", "event_time", "lon", "lat",
        "altitude_ft", "cumulative_distance_nm", "cumulative_time_s",
    )
    df_events = df_events.dropDuplicates([
        "track_id", "type", "event_time", "lon", "lat",
        "altitude_ft", "cumulative_distance_nm", "cumulative_time_s",
    ])
    df_events = df_events.withColumn("info", lit(""))

    return df_events


def calculate_airport_events(
    sv: DataFrame, month: date, spark: SparkSession, project: str
) -> DataFrame:
    """
    Detect airport infrastructure entry/exit events using H3 layout matching.

    Matches low-altitude track points against airport layout H3 hexagons
    (resolution 12) to detect when aircraft enter and exit runways,
    taxiways, aprons, and other ground infrastructure.

    Args:
        sv: Input tracks DataFrame.
        month: Month being processed (for flight list lookup).
        spark: Active SparkSession.
        project: Project/database name.

    Returns:
        DataFrame of airport entry/exit events.
    """
    from opdi.utils.datetime_helpers import get_start_end_of_month

    start_ts, end_ts = get_start_end_of_month(month)
    start_lit = to_timestamp(lit(start_ts))
    end_lit = to_timestamp(lit(end_ts))

    flight_list = (
        spark.table(f"{project}.opdi_flight_list")
        .filter((col("dof") >= start_lit) & (col("dof") < end_lit))
        .select("id", "adep", "ades", "adep_p", "ades_p")
    )

    # Build airport set per flight
    for c in ["adep", "ades", "adep_p", "ades_p"]:
        flight_list = flight_list.withColumn(
            c, when(col(c).isNull(), lit("")).otherwise(col(c))
        )

    flight_list = flight_list.withColumn(
        "apt",
        F.concat(
            F.array(col("adep"), col("ades")),
            split(col("adep_p"), ", "),
            split(col("ades_p"), ", "),
        ),
    ).withColumn("apt", F.array_remove(col("apt"), "")).select("id", "apt")

    sv_f = sv.dropna(subset=["lat", "lon", "baro_altitude_c"])
    sv_f = sv_f.withColumnRenamed("callsign", "flight_id")
    sv_f = sv_f.fillna({"flight_id": ""})
    sv_f = sv_f.withColumn("altitude_ft", col("baro_altitude_c") * 3.28084)
    sv_f = sv_f.withColumn("flight_level", col("altitude_ft") / 100)

    columns = [
        "track_id", "icao24", "flight_id", "event_time", "lat", "lon",
        "altitude_ft", "flight_level", "heading", "vert_rate",
        "h3_res_12", "cumulative_distance_nm", "cumulative_time_s",
    ]
    sv_f = sv_f.select(columns)

    sv_low_alt = sv_f.filter(col("flight_level") <= 20).cache()
    sv_nearby_apt = sv_low_alt.join(flight_list, sv.track_id == flight_list.id, "inner")

    apt_sdf = spark.table(f"{project}.hexaero_airport_layouts")

    df_labelled = sv_nearby_apt.join(
        apt_sdf,
        (sv_nearby_apt.h3_res_12 == apt_sdf.hexaero_h3_id)
        & F.array_contains(sv_nearby_apt.apt, apt_sdf.hexaero_apt_icao),
        "inner",
    )

    # Detect separate traces (gap > 5 min = new trace)
    window_spec = Window.partitionBy("track_id", "hexaero_osm_id").orderBy("event_time")
    df_labelled = df_labelled.withColumn(
        "time_diff",
        col("event_time").cast("long") - lag(col("event_time").cast("long"), 1).over(window_spec),
    )
    df_labelled = df_labelled.withColumn(
        "new_trace", when(col("time_diff") > 300, lit(1)).otherwise(lit(0))
    )
    df_labelled = df_labelled.withColumn(
        "trace_id", f_sum(col("new_trace")).over(window_spec)
    )
    df_labelled = df_labelled.drop("time_diff", "new_trace")

    # Aggregate entry/exit times
    result = df_labelled.groupBy(
        "track_id", "icao24", "flight_id",
        "hexaero_apt_icao", "hexaero_osm_id", "hexaero_aeroway", "hexaero_ref", "trace_id",
    ).agg(
        f_min("event_time").alias("entry_time"),
        f_max("event_time").alias("exit_time"),
        F.first("lat").alias("entry_lat"),
        F.last("lat").alias("exit_lat"),
        F.first("lon").alias("entry_lon"),
        F.last("lon").alias("exit_lon"),
        F.first("altitude_ft").alias("entry_altitude_ft"),
        F.last("altitude_ft").alias("exit_altitude_ft"),
        F.first("cumulative_distance_nm").alias("entry_cumulative_distance_nm"),
        F.last("cumulative_distance_nm").alias("exit_cumulative_distance_nm"),
        F.first("cumulative_time_s").alias("entry_cumulative_time_s"),
        F.last("cumulative_time_s").alias("exit_cumulative_time_s"),
    )

    result = result.withColumn(
        "time_in_use_seconds",
        col("exit_time").cast("long") - col("entry_time").cast("long"),
    )
    result = result.filter(col("time_in_use_seconds") != 0)

    result = (
        result
        .withColumnRenamed("hexaero_osm_id", "osm_id")
        .withColumnRenamed("hexaero_aeroway", "osm_aeroway")
        .withColumnRenamed("hexaero_ref", "osm_ref")
        .withColumnRenamed("hexaero_apt_icao", "osm_airport")
    )

    result = result.withColumn("info", to_json(struct(
        col("osm_id"), col("osm_aeroway"), col("osm_ref"), col("osm_airport"),
        col("time_in_use_seconds").alias("opdi_time_in_use_s"),
        col("icao24").alias("osn_icao24"),
        col("flight_id").alias("osn_flight_id"),
    )))

    result = result.withColumn("entry_type", concat_ws("-", lit("entry"), col("osm_aeroway")))
    result = result.withColumn("exit_type", concat_ws("-", lit("exit"), col("osm_aeroway")))
    result.cache()

    entry_events = result.select(
        col("track_id"),
        col("entry_time").alias("event_time"),
        col("entry_lon").alias("lon"),
        col("entry_lat").alias("lat"),
        col("entry_altitude_ft").alias("altitude_ft"),
        col("entry_cumulative_distance_nm").alias("cumulative_distance_nm"),
        col("entry_cumulative_time_s").alias("cumulative_time_s"),
        col("entry_type").alias("type"),
        col("info"),
    )
    exit_events = result.select(
        col("track_id"),
        col("exit_time").alias("event_time"),
        col("exit_lon").alias("lon"),
        col("exit_lat").alias("lat"),
        col("exit_altitude_ft").alias("altitude_ft"),
        col("exit_cumulative_distance_nm").alias("cumulative_distance_nm"),
        col("exit_cumulative_time_s").alias("cumulative_time_s"),
        col("exit_type").alias("type"),
        col("info"),
    )

    apt_events = entry_events.unionByName(exit_events)
    apt_events = apt_events.select(
        "track_id", "type", "event_time", "lon", "lat",
        "altitude_ft", "cumulative_distance_nm", "cumulative_time_s", "info",
    )
    apt_events = apt_events.dropDuplicates([
        "track_id", "type", "event_time", "lon", "lat",
        "altitude_ft", "cumulative_distance_nm", "cumulative_time_s",
    ])

    return apt_events


# ======================================================================
# Measurement helper functions
# ======================================================================

def add_time_measure(sdf_input: DataFrame) -> DataFrame:
    """Add cumulative time (seconds from track start) to each state vector."""
    window_spec = Window.partitionBy("track_id").orderBy("event_time")
    sdf_input = sdf_input.withColumn("min_event_time", f_min("event_time").over(window_spec))
    sdf_input = sdf_input.withColumn(
        "cumulative_time_s",
        col("event_time").cast("long") - col("min_event_time").cast("long"),
    )
    return sdf_input.drop("min_event_time")


def add_distance_measure(df: DataFrame) -> DataFrame:
    """
    Calculate segment and cumulative distance (NM) using Haversine formula.

    Uses native PySpark functions for distributed computation.

    Args:
        df: DataFrame with lat, lon, track_id, event_time columns.

    Returns:
        DataFrame with segment_distance_nm and cumulative_distance_nm.
    """
    window_lag = Window.partitionBy("track_id").orderBy("event_time")
    window_cumsum = (
        Window.partitionBy("track_id")
        .orderBy("event_time")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    df = df.withColumn("lat_rad", F.radians(col("lat")))
    df = df.withColumn("lon_rad", F.radians(col("lon")))
    df = df.withColumn("prev_lat_rad", lag("lat_rad").over(window_lag))
    df = df.withColumn("prev_lon_rad", lag("lon_rad").over(window_lag))

    df = df.withColumn(
        "a",
        F.sin((col("lat_rad") - col("prev_lat_rad")) / 2) ** 2
        + F.cos(col("prev_lat_rad"))
        * F.cos(col("lat_rad"))
        * F.sin((col("lon_rad") - col("prev_lon_rad")) / 2) ** 2,
    )
    df = df.withColumn("c", 2 * F.atan2(F.sqrt(col("a")), F.sqrt(1 - col("a"))))
    df = df.withColumn("distance_km", 6371 * col("c"))
    df = df.withColumn(
        "segment_distance_nm",
        when(col("distance_km").isNull(), 0).otherwise(col("distance_km") / 1.852),
    )
    df = df.withColumn("cumulative_distance_nm", f_sum("segment_distance_nm").over(window_cumsum))
    df = df.drop("lat_rad", "lon_rad", "prev_lat_rad", "prev_lon_rad", "a", "c", "distance_km")

    return df


# ======================================================================
# Main orchestrator class
# ======================================================================

class FlightEventProcessor:
    """
    Orchestrates the extraction of flight events and measurements.

    Combines horizontal (phase), vertical (FL crossing), airport, and
    first/last-seen event detection into a single processing pipeline.
    Writes results to opdi_flight_events and opdi_measurements tables.

    Args:
        spark: Active SparkSession.
        config: OPDI configuration object.
        log_dir: Directory for processing progress logs.

    Example:
        >>> processor = FlightEventProcessor(spark, config)
        >>> processor.process_date_range(date(2024, 1, 1), date(2024, 6, 1))
    """

    def __init__(
        self,
        spark: SparkSession,
        config: OPDIConfig,
        log_dir: str = "OPDI_live/logs",
    ):
        self.spark = spark
        self.config = config
        self.project = config.project.project_name
        self.log_dir = log_dir

        self._log_paths = {
            "horizontal": os.path.join(log_dir, "04_osn-flight_event-horizontal-etl-log.parquet"),
            "vertical": os.path.join(log_dir, "04_osn-flight_event-vertical-etl-log.parquet"),
            "hexaero": os.path.join(log_dir, "04_osn-flight_event-hexaero_airport-log.parquet"),
            "seen": os.path.join(log_dir, "04_osn-flight_event-first_seen_last_seen-log.parquet"),
        }

        os.makedirs(log_dir, exist_ok=True)

    def _load_processed(self, key: str) -> List[date]:
        """Load processed months for a specific event type."""
        path = self._log_paths[key]
        if os.path.isfile(path):
            return pd.read_parquet(path).months.to_list()
        return []

    def _mark_processed(self, key: str, month: date) -> None:
        """Mark a month as processed for a specific event type."""
        processed = self._load_processed(key)
        if month not in processed:
            processed.append(month)
            pd.DataFrame({"months": processed}).to_parquet(self._log_paths[key])

    def _get_data_within_timeframe(
        self, table_name: str, month: date, time_col: str = "event_time"
    ) -> DataFrame:
        """Retrieve records within a monthly timeframe."""
        start_ts, end_ts = get_start_end_of_month(month)
        start_lit = to_timestamp(lit(start_ts))
        end_lit = to_timestamp(lit(end_ts))
        df = self.spark.table(table_name)
        return df.filter((col(time_col) >= start_lit) & (col(time_col) < end_lit))

    def _etl_flight_events_and_measures(
        self,
        sdf_input: DataFrame,
        batch_id: str,
        month: date,
        calc_vertical: bool = True,
        calc_horizontal: bool = True,
        calc_hexaero: bool = True,
        calc_seen: bool = True,
    ) -> None:
        """
        Core ETL: calculate events and measurements, write to tables.

        Args:
            sdf_input: Input tracks DataFrame.
            batch_id: Batch identifier prefix for generated IDs.
            month: Month being processed.
            calc_vertical: Whether to calculate FL crossing events.
            calc_horizontal: Whether to calculate phase events.
            calc_hexaero: Whether to calculate airport events.
            calc_seen: Whether to calculate first/last seen events.
        """
        sdf_input = add_distance_measure(sdf_input)
        sdf_input = add_time_measure(sdf_input)
        sdf_input.cache()

        df_events = None

        if calc_horizontal:
            print(f"Calculating horizontal events (phase) for batch: {batch_id}")
            df_events = calculate_horizontal_segment_events(sdf_input)

        if calc_vertical:
            print(f"Calculating vertical events (FL crossings) for batch: {batch_id}")
            df_vertical = calculate_vertical_crossing_events(sdf_input)
            df_events = df_vertical if df_events is None else df_events.union(df_vertical)

        if calc_hexaero:
            print(f"Calculating airport events for batch: {batch_id}")
            df_hexaero = calculate_airport_events(
                sdf_input, month, self.spark, self.project
            )
            df_events = df_hexaero if df_events is None else df_events.union(df_hexaero)

        if calc_seen:
            print(f"Calculating first_seen/last_seen events for batch: {batch_id}")
            df_seen = calculate_firstseen_lastseen_events(sdf_input)
            df_events = df_seen if df_events is None else df_events.union(df_seen)

        if df_events is None:
            return

        df_events.cache()
        df_events = df_events.withColumn("source", lit("OSN"))
        df_events = df_events.withColumn("version", lit("events_v0.0.2"))

        df_events = df_events.withColumn(
            "id_tmp",
            concat(lit(batch_id), monotonically_increasing_id().cast("string")),
        ).select(
            col("id_tmp"), col("track_id"), col("type"), col("event_time"),
            col("lon"), col("lat"), col("altitude_ft"), col("source"),
            col("version"), col("info"), col("cumulative_distance_nm"),
            col("cumulative_time_s"),
        )

        # Write milestones (flight events)
        df_milestones = df_events.select(
            col("id_tmp").alias("id"),
            col("track_id").alias("flight_id"),
            col("type"),
            col("event_time"),
            col("lon").alias("longitude"),
            col("lat").alias("latitude"),
            col("altitude_ft").alias("altitude"),
            col("source"),
            col("version"),
            col("info"),
        )
        df_milestones = df_milestones.repartition("type", "version").orderBy("type", "version")
        df_milestones.writeTo(f"`{self.project}`.`opdi_flight_events`").append()

        # Write measurements (distance + time)
        df_dist = (
            df_events.withColumn("type", lit("Distance flown (NM)"))
            .withColumn("version", lit("distance_v0.0.2"))
            .withColumn(
                "id",
                concat(lit(batch_id + "_d_"), monotonically_increasing_id().cast("string")),
            )
            .select(
                col("id"),
                col("id_tmp").alias("milestone_id"),
                col("type"),
                col("cumulative_distance_nm").alias("value"),
                col("version"),
            )
        )

        df_time = (
            df_events.withColumn("type", lit("Time Passed (s)"))
            .withColumn("version", lit("time_v0.0.1"))
            .withColumn(
                "id",
                concat(lit(batch_id + "_t_"), monotonically_increasing_id().cast("string")),
            )
            .select(
                col("id"),
                col("id_tmp").alias("milestone_id"),
                col("type"),
                col("cumulative_time_s").alias("value"),
                col("version"),
            )
        )

        df_measurements = df_dist.union(df_time)
        df_measurements = df_measurements.repartition("type", "version").orderBy("type", "version")
        df_measurements.writeTo(f"`{self.project}`.`opdi_measurements`").append()

    def process_month(self, month: date, skip_if_processed: bool = True) -> None:
        """
        Process all flight events for a single month.

        Args:
            month: Month to process.
            skip_if_processed: Skip event types already processed.
        """
        print(f"Processing flight events for month: {month}")

        calc_horizontal = month not in self._load_processed("horizontal")
        calc_vertical = month not in self._load_processed("vertical")
        calc_hexaero = month not in self._load_processed("hexaero")
        calc_seen = month not in self._load_processed("seen")

        if not any([calc_horizontal, calc_vertical, calc_hexaero, calc_seen]):
            if skip_if_processed:
                print("All events for this month already processed.")
                return

        sdf_input = (
            self._get_data_within_timeframe(f"{self.project}.osn_tracks", month)
            .select(
                "track_id", "lat", "lon", "event_time", "baro_altitude_c",
                "velocity", "vert_rate", "callsign", "icao24", "heading", "h3_res_12",
            )
            .cache()
        )

        batch_id = month.strftime("%Y%m%d") + "_"

        self._etl_flight_events_and_measures(
            sdf_input,
            batch_id=batch_id,
            month=month,
            calc_vertical=calc_vertical,
            calc_horizontal=calc_horizontal,
            calc_hexaero=calc_hexaero,
            calc_seen=calc_seen,
        )

        # Update progress logs
        if calc_horizontal:
            self._mark_processed("horizontal", month)
        if calc_vertical:
            self._mark_processed("vertical", month)
        if calc_hexaero:
            self._mark_processed("hexaero", month)
        if calc_seen:
            self._mark_processed("seen", month)

        self.spark.catalog.clearCache()

    def process_date_range(
        self,
        start_month: date,
        end_month: date,
        skip_if_processed: bool = True,
    ) -> None:
        """
        Process flight events for a range of months.

        Args:
            start_month: First month to process.
            end_month: Last month to process.
            skip_if_processed: Skip already processed months/event types.

        Example:
            >>> processor = FlightEventProcessor(spark, config)
            >>> processor.process_date_range(date(2024, 1, 1), date(2024, 6, 1))
        """
        months = generate_months(start_month, end_month)
        print(f"Processing flight events for {len(months)} months...")

        for month in months:
            self.process_month(month, skip_if_processed)

        print(f"Flight event processing complete for {start_month} to {end_month}.")

    def create_tables_if_not_exist(self) -> None:
        """Create opdi_flight_events and opdi_measurements Iceberg tables."""
        today = datetime.today().strftime("%d %B %Y")

        events_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.project}`.`opdi_flight_events` (
            id STRING COMMENT 'Unique event identifier',
            flight_id STRING COMMENT 'Track/flight identifier',
            type STRING COMMENT 'Event type (take-off, landing, level-start, etc.)',
            event_time TIMESTAMP COMMENT 'Event timestamp',
            longitude DOUBLE COMMENT 'Event longitude',
            latitude DOUBLE COMMENT 'Event latitude',
            altitude DOUBLE COMMENT 'Event altitude in feet',
            source STRING COMMENT 'Data source (OSN)',
            version STRING COMMENT 'Processing version',
            info STRING COMMENT 'Additional info (JSON for airport events)'
        )
        USING iceberg
        PARTITIONED BY (type, version)
        COMMENT 'OPDI flight events (milestones). Last updated: {today}.'
        """

        measurements_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.project}`.`opdi_measurements` (
            id STRING COMMENT 'Unique measurement identifier',
            milestone_id STRING COMMENT 'Associated event identifier',
            type STRING COMMENT 'Measurement type (Distance flown, Time Passed)',
            value DOUBLE COMMENT 'Measurement value',
            version STRING COMMENT 'Processing version'
        )
        USING iceberg
        PARTITIONED BY (type, version)
        COMMENT 'OPDI measurements linked to flight events. Last updated: {today}.'
        """

        self.spark.sql(events_sql)
        print(f"Table {self.project}.opdi_flight_events created/verified.")

        self.spark.sql(measurements_sql)
        print(f"Table {self.project}.opdi_measurements created/verified.")
