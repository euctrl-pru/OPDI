"""Zone geometry where longitude wraps and latitude runs out.

Every number here was measured on h3-py 4.5.0 before the fix, so each assertion
is a fact about the defect rather than a hope about the repair.
"""
import json

import h3
import pytest

from opdi.reference.h3_airport_zones import generate_circle_polygon


def _cells(lon, lat, radius_nm, res=7):
    gj = json.loads(generate_circle_polygon(lon, lat, radius_nm))
    return h3.polygon_to_cells(
        h3.LatLngPoly([(p[1], p[0]) for p in gj["coordinates"][0]]), res)


def test_every_vertex_is_a_valid_longitude():
    """Unnormalised longitudes are the whole defect. 179.34 + a 40 NM radius
    produced a vertex at 180.035, which H3 reads as a coordinate it has no
    handling for rather than as 179.965W."""
    gj = json.loads(generate_circle_polygon(179.34, -16.47, 110))
    lons = [p[0] for p in gj["coordinates"][0]]
    assert max(lons) <= 180.0
    assert min(lons) >= -180.0


def test_a_ring_on_the_dateline_is_not_a_third_short():
    """Measured before the fix: 14,986 cells against a control of 26,530 at the
    same latitude away from the dateline -- 44% missing, no error raised. The
    control is the right comparison because ring size varies with latitude, so
    an absolute count would only pin this one test machine."""
    control = len(_cells(4.0, -16.47, 110))
    dateline = len(_cells(179.9, -16.47, 110))
    assert dateline == pytest.approx(control, rel=0.10)


def test_the_western_side_of_the_dateline_works_too():
    """179.5W, not 179.5E. The two sides fail differently -- one overshoots
    +180 and the other undershoots -180 -- so one case does not cover both.
    Measured before the fix: 18,014 cells against a control of 26,530."""
    control = len(_cells(4.0, -16.47, 110))
    assert len(_cells(-179.5, -16.47, 110)) == pytest.approx(control, rel=0.10)


def test_a_small_ring_clear_of_the_dateline_is_unchanged():
    """The regression guard. NFFN at 177.44E/17.76S never crosses 180 even at
    110 NM, so the fix must not move it: 28,438 cells, measured on the unfixed
    code at exactly these coordinates."""
    assert len(_cells(177.44, -17.76, 110)) == pytest.approx(28438, rel=0.02)


def test_the_european_geometry_is_untouched():
    """Every published zone came from this path.

    Pinned to an absolute count measured on the *unfixed* code -- 2,185 cells
    for a 30 NM ring at EBBR, res 7 -- because the whole risk of this task is
    that a longitude change reaches inside the box. A relative comparison
    against another call of the same function would move with it and prove
    nothing.
    """
    assert len(_cells(4.48, 50.90, 30)) == 2185
    gj = json.loads(generate_circle_polygon(4.48, 50.90, 30))
    lons = [p[0] for p in gj["coordinates"][0]]
    assert min(lons) == pytest.approx(3.6877, abs=1e-3)
    assert max(lons) == pytest.approx(5.2723, abs=1e-3)


def test_a_pole_aerodrome_is_excluded_rather_than_built_wrong(spark):
    """NZSP is at latitude -90.0 and OurAirports calls it a medium airport.

    A circle around a pole is a latitude ring sweeping every longitude, which
    H3's polygon fill cannot express as a cap -- it produces a band, or
    nothing, depending on vertex order. Excluding it is honest; building it is
    a zone that claims to be somewhere it is not.
    """
    from opdi.config import OPDIConfig
    from opdi.reference.h3_airport_zones import AirportDetectionZoneGenerator
    from pyspark.sql import functions as F

    gen = AirportDetectionZoneGenerator(
        spark, OPDIConfig.for_environment("opensky", worldwide=True))
    apt = spark.createDataFrame(
        [("NZSP", "medium_airport", -90.0, 0.0),
         ("CYLT", "medium_airport", 82.52, -62.28),
         ("EBBR", "large_airport", 50.90, 4.48)],
        "ident string, type string, latitude_deg double, longitude_deg double",
    )
    got = {r.ident for r in gen.filter_airports(apt).collect()}
    assert got == {"CYLT", "EBBR"}, "85 deg is the limit; CYLT at 82.5 is fine"
