"""Data ingestion modules for OPDI pipeline.

Provides connectors for loading data from external sources:
OpenSky Network state vectors, aircraft database, and OurAirports.
"""

from opdi.ingestion.osn_statevectors import StateVectorIngestion
from opdi.ingestion.osn_aircraft_db import AircraftDatabaseIngestion
from opdi.ingestion.ourairports import OurAirportsIngestion

__all__ = [
    "StateVectorIngestion",
    "AircraftDatabaseIngestion",
    "OurAirportsIngestion",
]
