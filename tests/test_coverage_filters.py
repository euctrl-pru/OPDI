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


def test_no_reference_generator_keeps_a_private_box():
    from opdi.reference.h3_airport_zones import AirportDetectionZoneGenerator
    from opdi.reference.h3_airport_layouts import AirportLayoutGenerator
    from opdi.reference.h3_runway_grid import RunwayGridGenerator
    for cls in (AirportDetectionZoneGenerator, AirportLayoutGenerator,
                RunwayGridGenerator):
        for attr in ("LAT_MIN", "LAT_MAX", "LON_MIN", "LON_MAX", "BBOX_OFFSET"):
            assert not hasattr(cls, attr), f"{cls.__name__}.{attr} still exists"


@pytest.mark.parametrize("worldwide,expect", [(False, {"EBBR"}),
                                              (True, {"EBBR", "KJFK", "NZAA", "WSSS"})])
def test_the_zone_airport_filter_follows_coverage(spark, worldwide, expect):
    from opdi.reference.h3_airport_zones import AirportDetectionZoneGenerator
    cfg = OPDIConfig.for_environment("opensky", worldwide=worldwide)
    gen = AirportDetectionZoneGenerator(spark, cfg)
    apt = spark.createDataFrame(
        [("EBBR", "large_airport", 50.90, 4.48),
         ("KJFK", "large_airport", 40.64, -73.78),
         ("NZAA", "large_airport", -37.01, 174.79),
         ("WSSS", "large_airport", 1.36, 103.99),
         ("EBCI", "small_airport", 50.46, 4.45)],
        "ident string, type string, latitude_deg double, longitude_deg double",
    )
    got = {r.ident for r in gen.filter_airports(apt).collect()}
    assert got == expect, "small_airport must be excluded in both modes"


def test_the_offset_asymmetry_between_filter_airports_and_cell_filter_is_pinned(spark):
    """`filter_airports` uses the coverage box plus a 3-degree offset;
    `cell_filter` uses the bare box. Deliberate -- ingestion clips state
    vectors to the bare box, so a cell in the margin can never match one --
    and arrived at by reverting a change that would have altered a published
    European reference table. Every other `cell_filter` test above runs
    worldwide, where the offset is irrelevant, so nothing pins this under
    European coverage; a future "unification" to `offset=True` everywhere
    would pass the whole suite while silently adding unselectable rows to a
    published table."""
    from opdi.reference.h3_airport_zones import AirportDetectionZoneGenerator
    gen = AirportDetectionZoneGenerator(spark, OPDIConfig.for_environment("opensky"))
    apt = spark.createDataFrame(
        [("MARGIN", "large_airport", 71.5, 20.0)],  # 1.24 deg past max_lat=70.25976
        "ident string, type string, latitude_deg double, longitude_deg double",
    )
    assert {r.ident for r in gen.filter_airports(apt).collect()} == {"MARGIN"}
    cells = spark.createDataFrame([(71.5, 20.0)], "lat double, lon double")
    assert cells.filter(gen.cell_filter(F.col("lat"), F.col("lon"))).count() == 0


def test_the_cell_centre_filter_follows_coverage_too(spark):
    """The sixth copy, and the one that would have made the other five look
    ineffective: `prepare_for_flight_list_spark` filters the H3 cells, so a
    European filter there drops every worldwide cell no matter how many
    airports the generator was given."""
    from opdi.reference.h3_airport_zones import AirportDetectionZoneGenerator
    gen = AirportDetectionZoneGenerator(
        spark, OPDIConfig.for_environment("opensky", worldwide=True))
    cells = spark.createDataFrame(
        [(50.9, 4.48), (-37.0, 174.8)], "lat double, lon double")
    assert cells.filter(gen.cell_filter(F.col("lat"), F.col("lon"))).count() == 2
