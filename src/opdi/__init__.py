"""
OPDI - Open Performance Data Initiative
========================================

A Python package for processing OpenSky Network aviation data through
modular ETL pipelines.  Provides ingestion, transformation, and output
layers for European air-traffic data analysis.

Quick start::

    from opdi.config import OPDIConfig
    from opdi.utils.spark_helpers import get_spark

    config = OPDIConfig.for_environment("dev")
    spark  = get_spark("dev")

Full pipeline::

    from opdi.runner import run_pipeline
    run_pipeline(env="live", start_date=date(2024, 1, 1), end_date=date(2024, 6, 1))
"""

__version__ = "2.0.0"
__author__ = "EUROCONTROL"

from opdi.config import OPDIConfig, ProjectConfig, SparkConfig, H3Config, IngestionConfig

__all__ = [
    "__version__",
    "OPDIConfig",
    "ProjectConfig",
    "SparkConfig",
    "H3Config",
    "IngestionConfig",
]
