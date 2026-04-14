"""Shared utility functions for the OPDI pipeline.

Provides datetime helpers, Spark session management, geospatial
calculations, and H3 hexagonal indexing utilities.
"""

from opdi.utils.datetime_helpers import (
    generate_months,
    generate_intervals,
    get_start_end_of_month,
    get_data_within_timeframe,
    get_data_within_interval,
)
from opdi.utils.spark_helpers import SparkSessionManager, get_spark
from opdi.utils.geospatial import (
    haversine_distance,
    add_cumulative_distance,
    calculate_bearing,
    destination_point,
    meters_to_flight_level,
    flight_level_to_meters,
)
from opdi.utils.h3_helpers import (
    h3_list_prep,
    get_h3_coords,
    compact_h3_set,
    uncompact_h3_set,
    h3_distance,
    k_ring,
    hex_ring,
    polyfill_geojson,
    is_valid_h3_index,
)

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
