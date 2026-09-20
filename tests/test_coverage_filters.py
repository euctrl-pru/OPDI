"""Every geographic filter in the pipeline asks one object for its box.

Six sites used to keep private copies. These tests are the reason they cannot
drift back apart: each asserts that changing `config.coverage` changes what the
site does, which a private copy would not.
"""
import pytest
from pyspark.sql import functions as F

from opdi.config import OPDIConfig
from opdi.coverage import EUROPE_BBOX
from opdi.ingestion.osn_statevectors import StateVectorIngestion

#: (lat, lon, inside-Europe) triples. NZAA and KJFK are the two that matter:
#: one is beyond the box in longitude, the other in both.
PLACES = [
    (50.90, 4.48, True),    # EBBR
    (40.64, -73.78, False),  # KJFK
    (-37.01, 174.79, False), # NZAA
    (1.36, 103.99, False),   # WSSS
]


def _sv(spark):
    return spark.createDataFrame(
        [(lat, lon, 1700000000) for lat, lon, _ in PLACES],
        "lat double, lon double, event_time long",
    )


def test_the_european_ingest_keeps_only_european_rows(spark):
    ing = StateVectorIngestion(spark, OPDIConfig.for_environment("opensky"),
                               time_interval=1)
    assert ing.bbox == EUROPE_BBOX
    assert ing._apply_filters(_sv(spark)).count() == 1


def test_the_worldwide_ingest_keeps_every_row(spark):
    ing = StateVectorIngestion(
        spark, OPDIConfig.for_environment("opensky", worldwide=True),
        time_interval=1)
    assert ing.bbox is None
    assert ing._apply_filters(_sv(spark)).count() == len(PLACES)


def test_an_explicit_box_still_overrides_the_configuration(spark):
    """The decimation and segmentation benchmarks pass their own box. That must
    keep working, and it must beat the configuration rather than be beaten."""
    ing = StateVectorIngestion(
        spark, OPDIConfig.for_environment("opensky", worldwide=True),
        bbox=(-10.0, 45.0, 10.0, 55.0), time_interval=1)
    assert ing._apply_filters(_sv(spark)).count() == 1


def test_an_explicit_none_means_worldwide_not_the_default(spark):
    """The collision this fix exists for: `None` used to mean 'use the default
    European box', so there was no way to ask for worldwide by argument."""
    ing = StateVectorIngestion(
        spark, OPDIConfig.for_environment("opensky"), bbox=None, time_interval=1)
    assert ing.bbox is None
    assert ing._apply_filters(_sv(spark)).count() == len(PLACES)
