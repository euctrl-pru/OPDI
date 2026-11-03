"""Data ingestion modules for OPDI pipeline.

Provides connectors for loading data from external sources:
OpenSky Network state vectors, the ADS-C topic, the aircraft database,
and OurAirports.
"""

from opdi.ingestion.osn_adsc import AdscIngestion
from opdi.ingestion.osn_statevectors import StateVectorIngestion
from opdi.ingestion.osn_aircraft_db import AircraftDatabaseIngestion
from opdi.ingestion.ourairports import OurAirportsIngestion

__all__ = [
    "AdscIngestion",
    "StateVectorIngestion",
    "AircraftDatabaseIngestion",
    "OurAirportsIngestion",
]
