"""Exact-value tests for opdi.utils.h3_helpers against the installed h3 v4 API.

Each ported helper is checked against a direct h3 v4 call on a known point/cell
so a wrong v3->v4 mapping (or a coordinate-order footgun in the polyfill path)
fails loudly rather than silently returning an empty or shifted result.
"""

import h3
import pytest

from opdi.utils import h3_helpers

# A point at EBBR (Brussels), used throughout for a stable, hand-checkable cell.
EBBR_LAT, EBBR_LON, RES = 50.9014, 4.4844, 9


def test_get_h3_coords_matches_v4_cell_to_latlng():
    cell = h3.latlng_to_cell(EBBR_LAT, EBBR_LON, RES)
    lat, lon = h3_helpers.get_h3_coords(cell)
    exp_lat, exp_lng = h3.cell_to_latlng(cell)
    assert (lat, lon) == (exp_lat, exp_lng)
    # sanity: order is (lat, lon), not swapped
    assert abs(lat - EBBR_LAT) < 0.01
    assert abs(lon - EBBR_LON) < 0.01


def test_k_ring_matches_v4_grid_disk():
    cell = h3.latlng_to_cell(EBBR_LAT, EBBR_LON, RES)
    assert h3_helpers.k_ring(cell, 1) == set(h3.grid_disk(cell, 1))
    assert isinstance(h3_helpers.k_ring(cell, 1), set)


def test_hex_ring_matches_v4_grid_ring():
    cell = h3.latlng_to_cell(EBBR_LAT, EBBR_LON, RES)
    assert h3_helpers.hex_ring(cell, 1) == set(h3.grid_ring(cell, 1))
    assert isinstance(h3_helpers.hex_ring(cell, 1), set)
    assert len(h3_helpers.hex_ring(cell, 1)) == 6


def test_h3_to_parent_matches_v4_cell_to_parent():
    cell = h3.latlng_to_cell(EBBR_LAT, EBBR_LON, RES)
    assert h3_helpers.h3_to_parent(cell, RES - 1) == h3.cell_to_parent(cell, RES - 1)


def test_h3_to_children_matches_v4_cell_to_children():
    parent_cell = h3.latlng_to_cell(EBBR_LAT, EBBR_LON, RES - 1)
    assert h3_helpers.h3_to_children(parent_cell, RES) == set(
        h3.cell_to_children(parent_cell, RES)
    )


def test_h3_distance_matches_v4_grid_distance():
    cell = h3.latlng_to_cell(EBBR_LAT, EBBR_LON, RES)
    neighbor = next(iter(h3.grid_ring(cell, 1)))
    assert h3_helpers.h3_distance(cell, neighbor) == h3.grid_distance(cell, neighbor)


def test_get_h3_resolution_matches_v4_get_resolution():
    cell = h3.latlng_to_cell(EBBR_LAT, EBBR_LON, RES)
    assert h3_helpers.get_h3_resolution(cell) == h3.get_resolution(cell) == RES


def test_compact_and_uncompact_round_trip_matches_v4():
    parent_cell = h3.latlng_to_cell(EBBR_LAT, EBBR_LON, RES - 1)
    children = set(h3.cell_to_children(parent_cell, RES))

    compacted = h3_helpers.compact_h3_set(children)
    assert compacted == set(h3.compact_cells(children))
    assert compacted == {parent_cell}

    uncompacted = h3_helpers.uncompact_h3_set(compacted, RES)
    assert uncompacted == set(h3.uncompact_cells(compacted, RES))
    assert uncompacted == children


def test_is_valid_h3_index_matches_v4_is_valid_cell():
    cell = h3.latlng_to_cell(EBBR_LAT, EBBR_LON, RES)
    assert h3_helpers.is_valid_h3_index(cell) == h3.is_valid_cell(cell) is True
    assert h3_helpers.is_valid_h3_index("not-a-cell") is False


def test_polyfill_geojson_footgun_lat_lng_order():
    """v3 geo_json_conformant=True took [lng, lat] rings; v4 h3.LatLngPoly takes
    [lat, lng] tuples. A polygon around EBBR must yield the same cell set as a
    direct h3.polygon_to_cells(h3.LatLngPoly(...)) call built with the correct
    [lat, lng] ordering -- and must NOT be empty (the silent failure mode when
    the order is swapped).
    """
    # GeoJSON ring: [lon, lat] pairs, as callers already pass in.
    geojson_polygon = {
        "type": "Polygon",
        "coordinates": [[
            [4.4, 50.8], [4.5, 50.8], [4.5, 50.9], [4.4, 50.9], [4.4, 50.8],
        ]],
    }
    result = h3_helpers.polyfill_geojson(geojson_polygon, resolution=7, geo_json_conformant=True)

    # Build the expected set directly with v4, converting the GeoJSON [lon, lat]
    # ring to the [lat, lon] tuples h3.LatLngPoly expects.
    latlng_ring = [(lat, lon) for lon, lat in geojson_polygon["coordinates"][0]]
    expected = set(h3.polygon_to_cells(h3.LatLngPoly(latlng_ring), 7))

    assert result == expected
    assert len(result) > 0


def test_polyfill_geojson_non_conformant_lat_lng_input():
    """geo_json_conformant=False path: caller already passes [lat, lon] pairs."""
    latlng_polygon = {
        "type": "Polygon",
        "coordinates": [[
            [50.8, 4.4], [50.8, 4.5], [50.9, 4.5], [50.9, 4.4], [50.8, 4.4],
        ]],
    }
    result = h3_helpers.polyfill_geojson(latlng_polygon, resolution=7, geo_json_conformant=False)

    latlng_ring = [(lat, lon) for lat, lon in latlng_polygon["coordinates"][0]]
    expected = set(h3.polygon_to_cells(h3.LatLngPoly(latlng_ring), 7))

    assert result == expected
    assert len(result) > 0


def test_h3_list_prep_unaffected_by_h3_version():
    assert h3_helpers.h3_list_prep([7, 12]) == ["h3_res_7", "h3_res_12"]
