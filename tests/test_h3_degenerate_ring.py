"""A zero-radius ring is not an error.

`radii_nm` starts at 0, and `generate_circle_polygon` at radius 0 returns the
same coordinate `num_points` times. h3 v3 answered that with an empty set,
which is why it never surfaced and why the published detection-zone table was
built without trouble. v4 raises `H3FailedError`, so once the executors were
upgraded the innermost ring of the very first airport killed the stage.

Empty is the right answer rather than an error: a zero-radius circle encloses
no area, so it contains no cells. The annulus it bounds is emitted by the next
radius up -- which is why the published table's smallest `max_c_radius_nm` is
5, and why nothing is lost by skipping it.
"""
import json

import pytest

from opdi.reference import h3_airport_zones as Z

KM_PER_NM = 1.852
LON, LAT = 4.48, 50.9


def _cells(radius_nm):
    gj = Z.generate_circle_polygon(LON, LAT, radius_nm * KM_PER_NM, 32)
    return Z._polyfill_geojson_udf.func(gj, 7)


def test_a_zero_radius_ring_yields_no_cells_instead_of_raising():
    assert _cells(0) == []


def test_the_zero_radius_polygon_really_is_degenerate():
    """Pins the cause, so a future change to generate_circle_polygon that
    stopped collapsing would make this test obsolete rather than silently
    leaving dead defensive code."""
    gj = Z.generate_circle_polygon(LON, LAT, 0.0, 32)
    ring = json.loads(gj)["coordinates"][0]
    assert len({tuple(p) for p in ring}) == 1


@pytest.mark.parametrize("radius_nm,at_least", [(5, 100), (40, 5000), (110, 50000)])
def test_real_radii_are_unaffected(radius_nm, at_least):
    """The skip must catch only the degenerate case."""
    cells = _cells(radius_nm)
    assert cells is not None and len(cells) >= at_least


def test_every_configured_radius_can_be_polyfilled():
    """The whole ladder, as production configures it -- 0 included.

    This is the test that would have caught the failure before it cost a
    cluster run: the first entry of `radii_nm` is 0.
    """
    radii = list(range(0, 45, 5)) + list(range(50, 120, 10))
    assert radii[0] == 0, "if 0 is gone, this test is guarding nothing"
    for nm in radii:
        cells = _cells(nm)
        assert cells is not None, f"radius {nm} NM returned NULL"


def test_the_ring_build_is_spread_widely_enough_to_fit_in_an_executor():
    """Row count is a bad proxy for cost when a row holds 98,250 cells.

    The cross join is 21,712 rows, which Spark's default parallelism splits
    into 8 or 9 tasks -- roughly 7.7 GB of cell strings each, against a 14 GB
    executor running two of them at once. Measured: every executor OOMKilled
    with exit 137.
    """
    from opdi.reference.h3_airport_zones import AirportDetectionZoneGenerator as G

    rows = 16 * 1357
    per_task = rows / G.ZONE_BUILD_PARTITIONS
    assert per_task < 25, (
        f"{per_task:.0f} rows per task; one row can hold 1.6 MB of cells and "
        "each polyfills two circles"
    )
