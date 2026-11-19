"""Shared utility functions for the OPDI pipeline.

Provides datetime helpers, Spark session management, geospatial
calculations, and H3 hexagonal indexing utilities.
"""

from opdi.utils.datetime_helpers import (
    generate_intervals,
    generate_months,
    get_data_within_interval,
    get_data_within_timeframe,
    get_start_end_of_month,
)
from opdi.utils.geospatial import (
    add_cumulative_distance,
    calculate_bearing,
    destination_point,
    flight_level_to_meters,
    haversine_distance,
    meters_to_flight_level,
)
from opdi.utils.h3_helpers import (
    compact_h3_set,
    get_h3_coords,
    h3_distance,
    h3_list_prep,
    hex_ring,
    is_valid_h3_index,
    k_ring,
    polyfill_geojson,
    uncompact_h3_set,
)
from opdi.utils.spark_helpers import SparkSessionManager, get_spark

__all__ = [
    # datetime
    "generate_months",
    "generate_intervals",
    "get_start_end_of_month",
    "get_data_within_timeframe",
    "get_data_within_interval",
    # spark
    "SparkSessionManager",
    "get_spark",
    # geospatial
    "haversine_distance",
    "add_cumulative_distance",
    "calculate_bearing",
    "destination_point",
    "meters_to_flight_level",
    "flight_level_to_meters",
    # h3
    "h3_list_prep",
    "get_h3_coords",
    "compact_h3_set",
    "uncompact_h3_set",
    "h3_distance",
    "k_ring",
    "hex_ring",
    "polyfill_geojson",
    "is_valid_h3_index",
]
