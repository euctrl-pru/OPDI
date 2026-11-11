"""
OPDI - Open Performance Data Initiative
========================================

A Python package for processing OpenSky Network aviation data through
modular ETL pipelines.  Provides ingestion, transformation, and output
layers for European air-traffic data analysis.

Quick start::

    from opdi.config import OPDIConfig
    from opdi.utils.spark_helpers import get_spark

    config = OPDIConfig.for_environment("opensky")
    spark  = get_spark("opensky")

Full pipeline::

    from opdi.runner import run_pipeline
    run_pipeline(env="opensky", start_date=date(2024, 1, 1), end_date=date(2024, 6, 1))
"""

__version__ = "2.0.0"
__author__ = "EUROCONTROL"

import importlib

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
    "OPDIConfig",
    "ProjectConfig",
    "SparkConfig",
    "H3Config",
    "IngestionConfig",
]
