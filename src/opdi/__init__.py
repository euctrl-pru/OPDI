"""OPDI, the Open Performance Data Initiative of EUROCONTROL.

OPDI turns the position reports that aircraft broadcast (ADS-B, collected by
the OpenSky Network) into open data for air traffic performance research: a
flight list (one row per flight, with its departure and destination airports
and main times) and flight events (take-off, landing, level segments,
crossings of flight levels and distance circles, and more), each with its
measurements.

The pipeline runs on Apache Spark. It is configured with
:class:`opdi.config.OPDIConfig` and run one day at a time with
``python -m opdi.periodrun`` or ``python -m opdi.orchestration``. The
published files are read without Spark with :mod:`opdi.published`.

Quick start::

    from datetime import date
    from opdi import published

    flights = published.read_flight_list(date(2026, 6, 14))

Every method and its validation is described on the OPDI website,
:data:`PORTAL_URL`.
"""

import importlib

__version__ = "2.0.0"
__author__ = "EUROCONTROL"

#: The OPDI website. Each method has a page under ``content/methods/`` (for
#: example ``content/methods/track-identity.html``) and the data are
#: described under ``content/data/``.
PORTAL_URL = "https://www.opdi.aero"

#: Public names of the package and the module that defines each one. They are
#: imported only when first used, so that ``import opdi.published`` and
#: ``import opdi.output.schema`` work without PySpark installed.
_LAZY = {
    "OPDIConfig": "opdi.config",
    "ProjectConfig": "opdi.config",
    "SparkConfig": "opdi.config",
    "H3Config": "opdi.config",
    "IngestionConfig": "opdi.config",
}


def __getattr__(name):
    if name in _LAZY:
        value = getattr(importlib.import_module(_LAZY[name]), name)
        globals()[name] = value
        return value
    raise AttributeError(f"module 'opdi' has no attribute {name!r}")


def __dir__():
    return sorted(set(globals()) | set(_LAZY))


__all__ = [
    "__version__",
    "PORTAL_URL",
    "OPDIConfig",
    "ProjectConfig",
    "SparkConfig",
    "H3Config",
    "IngestionConfig",
]
