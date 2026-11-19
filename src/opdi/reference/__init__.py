"""Step 00: the reference tables the pipeline steps read.

H3 airport detection zones (00a), airport layouts from OpenStreetMap (00b),
airspaces for the FIR crossings (00c), the runway and approach grid (00f),
and the curated point merge table. OurAirports (00d) and the aircraft
database (00e) are read by :mod:`opdi.ingestion`.
"""

from opdi.reference.h3_airport_layouts import (
    AirportLayoutGenerator,
    hexagonify_airport,
    retrieve_osm_data,
)
from opdi.reference.h3_airport_zones import (
    AirportDetectionZoneGenerator,
    generate_circle_polygon,
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
