"""Aeroway geometry from a local ``.osm.pbf`` extract.

Step 00b used to reach the public Overpass API once per airport, through
``osmnx.features_from_place``, which first resolves the airport *name* through
Nominatim and then sends Overpass a polygon predicate. That is two rate-limited
services per aerodrome, and it fails silently: a name that does not resolve
returns no data rather than an error, which is why five of twenty airports came
back empty during the flight-events-v4 campaign -- one of them an airport that
demonstrably has stands. At 1,353 aerodromes the public endpoint simply refuses.

Reading a Geofabrik extract removes both services. The file is read once, all
``aeroway`` features are kept, and each is assigned to an aerodrome from
reference data we already hold. See ``docs/industrialization-plan.md`` §3.
"""
from typing import List, Optional

import geopandas as gpd
import osmium
import pandas as pd
from shapely import wkb as shapely_wkb

from opdi.reference.h3_airport_layouts import AEROWAY_TAGS

#: Tags copied off each feature. `width`, `ref`, `surface` and `length` are what
#: `hexagonify_airport` reads; anything else would be dropped by the schema.
_TAGS = ("aeroway", "width", "ref", "surface", "length")

_WKB = osmium.geom.WKBFactory()


def _tags_of(obj) -> dict:
    return {k: obj.tags.get(k) for k in _TAGS}


def read_aeroway_features(pbf_path: str) -> gpd.GeoDataFrame:
    """Every ``aeroway`` feature in *pbf_path*, as shapely geometry.

    Returns the same shape ``retrieve_osm_data`` returns after
    ``reset_index()``: ``element``, ``id``, ``geometry`` plus the tag columns.
    Closed ways tagged as areas (aprons, stands) come back as polygons and open
    ways (taxiway and runway centrelines) as linestrings, which is exactly what
    ``convert_to_polygon`` expects to switch on.

    ``with_areas()`` makes osmium emit an assembled ``Area`` for a closed way
    or multipolygon relation *in addition to* the original ``Way`` -- both
    come out of the same iteration. Left alone, an apron or stand mapped as a
    closed way would be kept twice: once correctly as a Polygon (from the
    Area) and once as a LineString (from the Way) that `convert_to_polygon`
    would buffer into a thin sliver in the wrong shape. Way areas carry
    `orig_id() == way_id`; relation areas carry `orig_id() == relation_id`
    encoded as `2*id(+1)` internally, which is why `orig_id()` -- not `id` --
    is the right join key back to the way. Whenever an Area's orig_id matches
    a Way's id, the Way is dropped and the Area kept. Open ways (taxiway and
    runway centrelines) have no Area counterpart and are unaffected.
    """
    rows: List[dict] = []

    # `with_areas()` makes osmium assemble multipolygon relations and closed
    # ways into areas, which is how OSM encodes an apron. Without it those
    # features arrive as bare ways and an apron becomes a thin buffered line.
    fp = (
        osmium.FileProcessor(pbf_path)
        .with_areas()
        .with_filter(osmium.filter.KeyFilter("aeroway"))
    )
    for obj in fp:
        if obj.tags.get("aeroway") not in AEROWAY_TAGS:
            continue
        try:
            if isinstance(obj, osmium.osm.Area):
                geom = shapely_wkb.loads(_WKB.create_multipolygon(obj), hex=True)
                element, oid = "area", obj.orig_id()
            elif isinstance(obj, osmium.osm.Way):
                geom = shapely_wkb.loads(_WKB.create_linestring(obj), hex=True)
                element, oid = "way", obj.id
            elif isinstance(obj, osmium.osm.Node):
                geom = shapely_wkb.loads(_WKB.create_point(obj), hex=True)
                element, oid = "node", obj.id
            else:
                continue
        except Exception:
            # A feature whose nodes are outside the extract cannot be built.
            # Skipping it is right: it is geometry we do not have, and the
            # alternative is a partial shape that would rasterise to cells in
            # the wrong place.
            continue
        if geom is None or geom.is_empty:
            continue
        rows.append({"element": element, "id": int(oid), "geometry": geom,
                     **_tags_of(obj)})

    if not rows:
        raise ValueError(f"no aeroway features found in {pbf_path}")

    df = pd.DataFrame(rows)

    # Drop the Way half of any (Area, Way) pair that shares an orig_id -- see
    # the docstring. Only ways whose id collides with an area's orig_id are
    # affected; open ways (taxiway/runway centrelines) have no area and
    # survive untouched.
    area_ids = set(df.loc[df["element"] == "area", "id"])
    is_duplicate_way = (df["element"] == "way") & df["id"].isin(area_ids)
    df = df[~is_duplicate_way]

    return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
