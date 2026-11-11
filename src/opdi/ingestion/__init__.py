"""Reading the external data the pipeline starts from.

The OpenSky Network state vectors (step 01) and ADS-C reports (step 01b),
the OpenSky aircraft database and the OurAirports airport and runway data
(both part of step 00, the reference tables).
"""

from opdi.ingestion.osn_adsc import AdscIngestion
from opdi.ingestion.osn_aircraft_db import AircraftDatabaseIngestion
from opdi.ingestion.osn_statevectors import StateVectorIngestion
from opdi.ingestion.ourairports import OurAirportsIngestion

__all__ = [
    "AdscIngestion",
    "StateVectorIngestion",
    "AircraftDatabaseIngestion",
    "OurAirportsIngestion",
]
