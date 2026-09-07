"""Reading aeroway geometry out of a local OSM extract.

Every test uses the Luxembourg extract, which is ~40 MB and contains ELLX --
a real aerodrome with a runway, taxiways and stands. Using a real extract
rather than a synthetic one is deliberate: the failure modes this code has are
about how OSM actually encodes aprons and stands (closed ways that are areas,
multipolygon relations, missing width tags), and a hand-built fixture would
encode this author's assumptions about that rather than the reality.
"""
import os
import pytest

pytest.importorskip("osmium")

from opdi.reference.pbf_source import read_aeroway_features

PBF = "/home/jupyter/work/osm/luxembourg-latest.osm.pbf"

pytestmark = pytest.mark.skipif(
    not os.path.exists(PBF), reason="Luxembourg extract not downloaded (Task 1)"
)


def test_reads_the_aeroway_families_the_layout_grid_needs():
    """AEROWAY_TAGS has seven members. Luxembourg (ELLX), a small regional
    aerodrome, genuinely lacks `threshold` and `deicing_pad` mappings -- that
    was confirmed by scanning the raw file with a plain KeyFilter, independent
    of this reader's element-type branching, and finding zero objects tagged
    with either key. The other five must all come through."""
    gdf = read_aeroway_features(PBF)
    kinds = set(gdf["aeroway"])
    for tag in ("runway", "taxiway", "apron", "hangar", "parking_position"):
        assert tag in kinds, f"{tag} missing from a real extract that has it"
    assert "parking_position" in kinds, (
        "stands are the whole point: AOBT/AIBT are anchored on parking_position"
    )


def test_every_feature_carries_the_identity_the_schema_needs():
    """`hexagonify_airport` renames `id`->`hexaero_osm_id` and
    `element`->`hexaero_type`. A source that omits either yields a table with
    nulls there and nothing downstream checks it."""
    gdf = read_aeroway_features(PBF)
    for col in ("element", "id", "geometry", "aeroway"):
        assert col in gdf.columns, f"missing {col}"
    assert gdf["id"].notna().all()
    assert gdf["element"].isin(["node", "way", "area", "relation"]).all()


def test_geometries_are_usable_shapes():
    """`convert_to_polygon` switches on `geom_type` and buffers LineStrings by
    width. Anything it cannot name is silently dropped later."""
    gdf = read_aeroway_features(PBF)
    types = set(gdf.geometry.geom_type)
    assert types <= {"LineString", "Polygon", "MultiPolygon", "Point"}, types
    assert gdf.geometry.is_valid.all() or gdf.geometry.isna().sum() == 0


def test_no_tag_is_silently_dropped_by_element_type_branching():
    """`read_aeroway_features` only branches on `osmium.osm.Area`, `Way` and
    `Node`. If a future extract (or a future osmium version) emitted an
    aeroway feature as some other native type -- a `Relation` that
    `with_areas()` did not assemble, say -- the current code would silently
    skip it via the `else: continue` branch, and the only symptom would be a
    missing family at some aerodrome downstream, not a test failure here.

    This test scans the raw file with a plain KeyFilter and no `with_areas()`
    -- so every object appears exactly once as its native OSM type -- and
    checks that every native `way` feature (Luxembourg has no aeroway
    `relation` or `node` at all, confirmed by this same scan) reappears in
    the GeoDataFrame, either directly (open way) or absorbed into an Area
    (closed way). A raw count of zero is a genuine absence in this extract,
    not evidence the reader is broken -- see the AEROWAY_TAGS test above for
    `threshold`/`deicing_pad`, which are exactly that case.
    """
    import osmium
    from collections import Counter

    from opdi.reference.h3_airport_layouts import AEROWAY_TAGS

    raw_native = Counter()
    fp = osmium.FileProcessor(PBF).with_filter(osmium.filter.KeyFilter("aeroway"))
    for obj in fp:
        tag = obj.tags.get("aeroway")
        if tag not in AEROWAY_TAGS:
            continue
        if isinstance(obj, (osmium.osm.Node, osmium.osm.Way, osmium.osm.Relation)):
            raw_native[tag] += 1

    gdf = read_aeroway_features(PBF)
    kept = gdf["aeroway"].value_counts().to_dict()

    for tag, raw_count in raw_native.items():
        assert kept.get(tag, 0) == raw_count, (
            f"{tag}: {raw_count} raw native objects but only "
            f"{kept.get(tag, 0)} survived into the GeoDataFrame -- a tag is "
            "being lost by element-type branching, not just deduplicated"
        )


def test_no_way_survives_when_its_area_counterpart_is_present():
    """`with_areas()` makes osmium emit an assembled Area for closed ways and
    multipolygon relations, but the original Way is also emitted by the same
    iteration. A closed way kept as both a Polygon (from the area) and a
    LineString (from the way) would have the LineString buffered into a thin
    sliver in the wrong shape by `convert_to_polygon`. The Way must be dropped
    whenever an Area with the same orig_id exists."""
    gdf = read_aeroway_features(PBF)
    area_ids = set(gdf.loc[gdf["element"] == "area", "id"])
    way_ids = set(gdf.loc[gdf["element"] == "way", "id"])
    assert area_ids & way_ids == set(), (
        "way ids duplicated by an area of the same orig_id: "
        f"{area_ids & way_ids}"
    )
