"""Offline, exact-value tests for the h3 v4 port of opdi.reference.h3_airport_layouts.

These do not hit OSM/osmnx. They pin down the polyfill footgun described in
the plan's Global Constraints: h3 v3's ``polyfill(geojson, res)`` consumed
GeoJSON ``[lng, lat]`` rings, while v4's ``h3.LatLngPoly`` always takes
``[lat, lng]`` tuples. Getting the order wrong yields an empty or silently
wrong cell set, so every assertion here checks against a direct v4 call
rather than a hand-picked expected cell.
"""

import h3
import pytest
from shapely.geometry import Polygon

from opdi.reference import h3_airport_layouts as L

# A ~200m box near EBBR (Brussels), expressed directly as [lat, lng] pairs.
RING_LATLNG = [
    (50.900, 4.480),
    (50.900, 4.484),
    (50.902, 4.484),
    (50.902, 4.480),
]


def test_polyfill_uses_latlng_order_not_geojson():
    """_polyfill_latlng must match a direct h3 v4 polygon_to_cells call."""
    poly = h3.LatLngPoly(RING_LATLNG)
    expected = set(h3.polygon_to_cells(poly, 12))
    got = set(L._polyfill_latlng(RING_LATLNG, 12))
    assert got == expected
    assert len(got) > 0


def test_polyfill_latlng_wrong_order_yields_different_or_empty_result():
    """The footgun made concrete: swapping to [lng, lat] must NOT silently
    produce the same cell set (it should differ or come up empty)."""
    ring_lnglat = [(lng, lat) for lat, lng in RING_LATLNG]
    correct = set(L._polyfill_latlng(RING_LATLNG, 12))
    wrong = set(L._polyfill_latlng(ring_lnglat, 12))
    assert wrong != correct


def test_polygon_to_h3_swaps_shapely_lng_lat_exterior_correctly():
    """polygon_to_h3 takes a shapely Polygon, whose .exterior.coords are
    (lng, lat) -- the classic GeoJSON/shapely x,y order. The ported function
    must swap to [lat, lng] before calling h3, and the resulting cell set
    must equal calling _polyfill_latlng directly on the pre-swapped ring."""
    # shapely Polygon built from (lng, lat) pairs, matching RING_LATLNG.
    ring_lnglat = [(lng, lat) for lat, lng in RING_LATLNG]
    shapely_poly = Polygon(ring_lnglat)

    got = L.polygon_to_h3(shapely_poly, 12)
    expected = set(L._polyfill_latlng(RING_LATLNG, 12))

    assert got == expected
    assert len(got) > 0


def test_polygon_to_h3_returns_str_cells():
    ring_lnglat = [(lng, lat) for lat, lng in RING_LATLNG]
    shapely_poly = Polygon(ring_lnglat)
    got = L.polygon_to_h3(shapely_poly, 12)
    assert all(isinstance(c, str) for c in got)
    assert all(h3.is_valid_cell(c) for c in got)
