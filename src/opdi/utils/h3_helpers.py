"""
H3 hexagonal indexing utility functions for OPDI pipeline.

Provides helper functions for working with H3 geospatial indices,
including column name generation, coordinate extraction, and set operations.

Ported to the h3 v4 API (installed version 4.5.0). Cell identity is
version-independent -- v3 and v4 produce the same cell string for the same
lat/lng/resolution -- so this port only changes *how* each result is computed,
never the public names, signatures, or return types callers already rely on.
"""

from typing import List, Set, Tuple
import h3


def h3_list_prep(h3_resolutions: List[int]) -> List[str]:
    """
    Generate H3 column names for a list of resolutions.

    Args:
        h3_resolutions: List of H3 resolution levels (e.g., [7, 12])

    Returns:
        List of column names formatted as "h3_res_{resolution}"

    Example:
        >>> h3_list_prep([7, 12])
        ['h3_res_7', 'h3_res_12']
    """
    h3_res = []
    for h3_resolution in h3_resolutions:
        h3_res.append(f"h3_res_{h3_resolution}")
    return h3_res


def get_h3_coords(h3_index: str) -> Tuple[float, float]:
    """
    Get latitude and longitude coordinates for an H3 index.

    Args:
        h3_index: H3 hexagon index string

    Returns:
        Tuple of (latitude, longitude) in degrees

    Example:
        >>> lat, lon = get_h3_coords('871fb46655fffff')
        >>> print(f"Lat: {lat:.4f}, Lon: {lon:.4f}")
    """
    lat, lon = h3.cell_to_latlng(h3_index)
    return lat, lon


def compact_h3_set(h3_set: Set[str]) -> Set[str]:
    """
    Compact a set of H3 indices by merging child hexagons into parents where possible.

    This reduces the number of hexagons needed to represent the same area,
    improving storage and query performance.

    Args:
        h3_set: Set of H3 index strings

    Returns:
        Compacted set of H3 indices

    Example:
        >>> h3_indices = {'871fb46655fffff', '871fb46656fffff', ...}
        >>> compacted = compact_h3_set(h3_indices)
        >>> len(compacted) < len(h3_indices)  # Compacted set is smaller
        True
    """
    try:
        return set(h3.compact_cells(h3_set))
    except Exception:
        # If compaction fails, return original set
        return h3_set


def uncompact_h3_set(h3_set: Set[str], target_resolution: int) -> Set[str]:
    """
    Uncompact a set of H3 indices to a target resolution.

    Expands parent hexagons into their children at the specified resolution.

    Args:
        h3_set: Set of H3 index strings
        target_resolution: Target H3 resolution level (0-15)

    Returns:
        Uncompacted set of H3 indices at target resolution

    Example:
        >>> compacted = {'861fb467fffffff'}  # Resolution 6
        >>> uncompacted = uncompact_h3_set(compacted, 7)  # Expand to resolution 7
        >>> len(uncompacted) > len(compacted)  # More hexagons at finer resolution
        True
    """
    return set(h3.uncompact_cells(h3_set, target_resolution))


def h3_distance(h3_index1: str, h3_index2: str) -> int:
    """
    Calculate the grid distance between two H3 indices.

    Grid distance is the number of hexagon steps between two indices.
    Both indices must be at the same resolution.

    Args:
        h3_index1: First H3 index
        h3_index2: Second H3 index

    Returns:
        Number of hexagon steps between the two indices

    Example:
        >>> dist = h3_distance('871fb46655fffff', '871fb46656fffff')
        >>> print(f"Grid distance: {dist} hexagons")
    """
    return h3.grid_distance(h3_index1, h3_index2)


def get_h3_resolution(h3_index: str) -> int:
    """
    Get the resolution level of an H3 index.

    Args:
        h3_index: H3 hexagon index string

    Returns:
        Resolution level (0-15)

    Example:
        >>> res = get_h3_resolution('871fb46655fffff')
        >>> print(f"Resolution: {res}")
        Resolution: 7
    """
    return h3.get_resolution(h3_index)


def k_ring(h3_index: str, k: int) -> Set[str]:
    """
    Get all hexagons within k steps of the given hexagon (including the center).

    Args:
        h3_index: Center H3 hexagon index
        k: Number of steps (radius)

    Returns:
        Set of H3 indices within k steps

    Example:
        >>> # Get hexagon and its immediate neighbors (k=1)
        >>> neighbors = k_ring('871fb46655fffff', 1)
        >>> len(neighbors)  # Center + 6 neighbors
        7
    """
    return set(h3.grid_disk(h3_index, k))


def hex_ring(h3_index: str, k: int) -> Set[str]:
    """
    Get hexagons exactly k steps away from the given hexagon (hollow ring).

    Args:
        h3_index: Center H3 hexagon index
        k: Number of steps (radius)

    Returns:
        Set of H3 indices exactly k steps away

    Example:
        >>> # Get only the immediate neighbors (not the center)
        >>> ring = hex_ring('871fb46655fffff', 1)
        >>> len(ring)  # Just the 6 neighbors
        6
    """
    return set(h3.grid_ring(h3_index, k))


def h3_to_parent(h3_index: str, parent_resolution: int) -> str:
    """
    Get the parent hexagon at a coarser resolution.

    Args:
        h3_index: Child H3 index
        parent_resolution: Resolution of parent (must be less than child resolution)

    Returns:
        Parent H3 index at specified resolution

    Example:
        >>> child = '871fb46655fffff'  # Resolution 7
        >>> parent = h3_to_parent(child, 6)
        >>> print(f"Parent: {parent}")
    """
    return h3.cell_to_parent(h3_index, parent_resolution)


def h3_to_children(h3_index: str, child_resolution: int) -> Set[str]:
    """
    Get all child hexagons at a finer resolution.

    Args:
        h3_index: Parent H3 index
        child_resolution: Resolution of children (must be greater than parent resolution)

    Returns:
        Set of child H3 indices at specified resolution

    Example:
        >>> parent = '861fb467fffffff'  # Resolution 6
        >>> children = h3_to_children(parent, 7)
        >>> print(f"Number of children: {len(children)}")
        Number of children: 7
    """
    return set(h3.cell_to_children(h3_index, child_resolution))


def _ring_to_latlng(ring: List[List[float]], geo_json_conformant: bool) -> List[Tuple[float, float]]:
    """
    Convert one GeoJSON coordinate ring to the [lat, lng] tuples h3.LatLngPoly
    expects.

    THE FOOTGUN: v3's ``polyfill(..., geo_json_conformant=True)`` consumed
    GeoJSON-ordered rings, i.e. ``[lng, lat]`` pairs. v4's ``h3.LatLngPoly``
    always takes ``[lat, lng]`` tuples, regardless of the caller's input
    convention. So when ``geo_json_conformant`` is True the two coordinates in
    each pair must be swapped before handing them to h3; when False, the
    caller already passed ``[lat, lng]`` and no swap is needed. Getting this
    backwards yields an empty or silently wrong cell set -- see
    tests/test_h3_helpers.py::test_polyfill_geojson_footgun_lat_lng_order.
    """
    if geo_json_conformant:
        return [(coord[1], coord[0]) for coord in ring]
    return [(coord[0], coord[1]) for coord in ring]


def polyfill_geojson(geojson_geometry: dict, resolution: int, geo_json_conformant: bool = False) -> Set[str]:
    """
    Fill a GeoJSON geometry with H3 hexagons at the specified resolution.

    Args:
        geojson_geometry: GeoJSON geometry dict (Polygon or MultiPolygon)
        resolution: H3 resolution level (0-15)
        geo_json_conformant: Whether coordinates are in GeoJSON format [lon, lat] vs [lat, lon]

    Returns:
        Set of H3 indices covering the geometry

    Example:
        >>> polygon = {
        ...     "type": "Polygon",
        ...     "coordinates": [[
        ...         [4.4, 50.8], [4.5, 50.8], [4.5, 50.9], [4.4, 50.9], [4.4, 50.8]
        ...     ]]
        ... }
        >>> hexagons = polyfill_geojson(polygon, resolution=7, geo_json_conformant=True)
    """
    geom_type = geojson_geometry["type"]
    coordinates = geojson_geometry["coordinates"]

    if geom_type == "Polygon":
        polygons = [coordinates]
    elif geom_type == "MultiPolygon":
        polygons = coordinates
    else:
        raise ValueError(f"Unsupported geometry type for polyfill_geojson: {geom_type!r}")

    cells: Set[str] = set()
    for rings in polygons:
        outer, *holes = rings
        outer_latlng = _ring_to_latlng(outer, geo_json_conformant)
        holes_latlng = [_ring_to_latlng(hole, geo_json_conformant) for hole in holes]
        shape = h3.LatLngPoly(outer_latlng, *holes_latlng)
        cells.update(h3.polygon_to_cells(shape, resolution))

    return cells


def is_valid_h3_index(h3_index: str) -> bool:
    """
    Check if a string is a valid H3 index.

    Args:
        h3_index: String to validate

    Returns:
        True if valid H3 index, False otherwise

    Example:
        >>> is_valid_h3_index('871fb46655fffff')
        True
        >>> is_valid_h3_index('invalid')
        False
    """
    return h3.is_valid_cell(h3_index)
