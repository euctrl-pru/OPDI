"""Core pipeline transformation modules for OPDI.

Provides the main data transformation pipeline:
- Track creation from raw state vectors
- Flight list generation (departures, arrivals, overflights)
- Flight event detection (phases, crossings, airport events)
"""

from opdi.pipeline.tracks import TrackProcessor
from opdi.pipeline.flights import FlightListProcessor
from opdi.pipeline.events import FlightEventProcessor

__all__ = [
    "TrackProcessor",
    "FlightListProcessor",
    "FlightEventProcessor",
]
