"""Reference data generators for OPDI pipeline.

Provides H3 hexagonal encoding for airport detection zones,
airport ground layouts, and airspace boundaries.
"""

from opdi.reference.h3_airport_zones import (
    AirportDetectionZoneGenerator,
    generate_circle_polygon,
)
from opdi.reference.h3_airport_layouts import (
    AirportLayoutGenerator,
    hexagonify_airport,
    retrieve_osm_data,
)
from opdi.reference.h3_airspaces import (
    AirspaceH3Generator,
    fill_geometry,
    fill_geometry_compact,
)

__all__ = [
    "AirportDetectionZoneGenerator",
    "AirportLayoutGenerator",
    "AirspaceH3Generator",
    "generate_circle_polygon",
    "hexagonify_airport",
    "retrieve_osm_data",
    "fill_geometry",
    "fill_geometry_compact",
]
