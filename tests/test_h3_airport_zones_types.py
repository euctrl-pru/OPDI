"""Which airports get detection zones.

The published `h3_airport_detection_zones` contains large and medium airports
and nothing else -- measured on the released table: 134,116 large and 331,624
medium rows in a 25-file sample, and not one heliport, small field or closed
aerodrome. Every other reference step agrees: `h3_runway_grid` and
`h3_airport_layouts` both build for `["large_airport", "medium_airport"]`.

The generator's filter was a bounding box alone. Inside that box OurAirports
lists 13,973 aerodromes -- 6,782 small, 3,820 heliports, 1,952 *closed* -- and
only 1,357 large or medium. So a rebuild produced ten times the zones the
pipeline is tuned against, which is both a silent change to the ADEP/ADES
candidate set and, at H3 resolution 7 out to 110 NM, enough data to OOM every
executor in the namespace.
"""
import pytest

from opdi.config import OPDIConfig
from opdi.reference.h3_airport_zones import AirportDetectionZoneGenerator


def _airports(spark, rows):
    return spark.createDataFrame(
        rows, "ident string, type string, latitude_deg double, longitude_deg double"
    )


def _gen(spark):
    return AirportDetectionZoneGenerator(spark, OPDIConfig())


# A point comfortably inside the European box.
IN_BOX = (50.9, 4.48)


def test_only_large_and_medium_airports_get_zones(spark):
    df = _airports(spark, [
        ("EBBR", "large_airport", *IN_BOX),
        ("EBCI", "medium_airport", *IN_BOX),
        ("EBXX", "small_airport", *IN_BOX),
        ("EBHE", "heliport", *IN_BOX),
        ("EBCL", "closed", *IN_BOX),
        ("EBSB", "seaplane_base", *IN_BOX),
        ("EBBP", "balloonport", *IN_BOX),
    ])
    kept = {r["ident"] for r in _gen(spark)._filter_airports_spark(df).collect()}
    assert kept == {"EBBR", "EBCI"}


def test_a_closed_aerodrome_is_not_a_destination(spark):
    """1,952 of the aerodromes in the box are closed. A flight cannot arrive at
    one, so a detection zone around it can only produce wrong answers."""
    df = _airports(spark, [("EBCL", "closed", *IN_BOX)])
    assert _gen(spark)._filter_airports_spark(df).count() == 0


def test_the_bounding_box_still_applies(spark):
    """Type filtering is in addition to the box, not instead of it."""
    df = _airports(spark, [
        ("EBBR", "large_airport", *IN_BOX),
        ("KJFK", "large_airport", 40.64, -73.78),   # outside the box
    ])
    kept = {r["ident"] for r in _gen(spark)._filter_airports_spark(df).collect()}
    assert kept == {"EBBR"}


def test_the_type_set_matches_the_other_reference_builds(spark):
    """h3_runway_grid and h3_airport_layouts both build for exactly these two.
    Three reference tables keyed to different airport sets would join to each
    other with silent gaps."""
    assert AirportDetectionZoneGenerator.AIRPORT_TYPES == (
        "large_airport", "medium_airport",
    )


def test_the_type_set_can_be_widened_deliberately(spark):
    """Overridable, so including small fields stays possible -- but as a
    decision someone makes, not as the default nobody chose."""
    gen = _gen(spark)
    gen.airport_types = ("large_airport", "small_airport")
    df = _airports(spark, [
        ("EBBR", "large_airport", *IN_BOX),
        ("EBXX", "small_airport", *IN_BOX),
        ("EBCI", "medium_airport", *IN_BOX),
    ])
    kept = {r["ident"] for r in gen._filter_airports_spark(df).collect()}
    assert kept == {"EBBR", "EBXX"}
