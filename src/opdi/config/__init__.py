"""Settings of the OPDI pipeline.

Every setting of the pipeline is a field of a dataclass in this module, with
its default value. :class:`OPDIConfig` holds one instance of each group
(storage, Spark, ingestion, cleaning, track building, airport detection,
events) and :meth:`OPDIConfig.for_environment` builds a ready-made
configuration for one of the two environments, ``local`` and ``opensky``.

Thresholds are written in aviation units, with the unit in the field name
(``endpoint_radius_nm``, ``baro_altitude_d1_max_ft_s``), because that is how
the methods are described and how the published data is expressed. The
position reports themselves are stored in SI units (metres, metres per
second), as OpenSky delivers them; where a threshold meets such a column, the
column is scaled to the threshold's unit at the point of comparison.

The presets :meth:`DetectionConfig.legacy` and :meth:`EventConfig.legacy`,
together with ``segmentation.method = "legacy"`` and
``ingestion.decimation = "modulo"``, reproduce the published release v0.0.2.
Each method is described on the OPDI website, ``https://www.opdi.aero``.
"""

# The .env file is read first, before any setting reads the environment.
from opdi.config.dotenv import _load_dotenv  # noqa: I001
from opdi.config.adsc import AdscConfig
from opdi.config.cleaning import CleaningConfig
from opdi.config.detection import DetectionConfig
from opdi.config.events import EventConfig
from opdi.config.h3 import H3Config
from opdi.config.ingestion import IngestionConfig
from opdi.config.environments import (
    OPENSKY_DEFAULT_WAREHOUSE,
    WAREHOUSE_VARIABLE,
    OPDIConfig,
    _worldwide_warehouse,
)
from opdi.config.project import ProjectConfig
from opdi.config.segmentation import STUDY_SEGMENTATION_METHODS, SegmentationConfig
from opdi.config.spark import SparkConfig
from opdi.coverage import CoverageConfig
from opdi.output.schema import RELEASE

__all__ = [
    "AdscConfig",
    "CleaningConfig",
    "CoverageConfig",
    "DetectionConfig",
    "EventConfig",
    "H3Config",
    "IngestionConfig",
    "OPDIConfig",
    "OPENSKY_DEFAULT_WAREHOUSE",
    "ProjectConfig",
    "RELEASE",
    "STUDY_SEGMENTATION_METHODS",
    "SegmentationConfig",
    "SparkConfig",
    "WAREHOUSE_VARIABLE",
    "_load_dotenv",
    "_worldwide_warehouse",
]
