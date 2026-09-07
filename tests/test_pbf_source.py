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

# Applied only to the tests below that actually read the Luxembourg extract.
# The synthetic-fixture tests further down build their own tiny `.osm` files
# and must keep running even if the Luxembourg download were ever removed --
# skipping them along with the Luxembourg tests would hide a real regression
# in exactly the id-collision/error-reporting/node-geometry paths those tests
# exist to catch.
needs_luxembourg = pytest.mark.skipif(
    not os.path.exists(PBF), reason="Luxembourg extract not downloaded (Task 1)"
)


@needs_luxembourg
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


@needs_luxembourg
def test_every_feature_carries_the_identity_the_schema_needs():
    """`hexagonify_airport` renames `id`->`hexaero_osm_id` and
    `element`->`hexaero_type`. A source that omits either yields a table with
    nulls there and nothing downstream checks it."""
    gdf = read_aeroway_features(PBF)
    for col in ("element", "id", "geometry", "aeroway"):
        assert col in gdf.columns, f"missing {col}"
    assert gdf["id"].notna().all()
    assert gdf["element"].isin(["node", "way", "area", "relation"]).all()


@needs_luxembourg
def test_geometries_are_usable_shapes():
    """`convert_to_polygon` switches on `geom_type` and buffers LineStrings by
    width. Anything it cannot name is silently dropped later."""
    gdf = read_aeroway_features(PBF)
    types = set(gdf.geometry.geom_type)
    assert types <= {"LineString", "Polygon", "MultiPolygon", "Point"}, types
    assert gdf.geometry.is_valid.all() or gdf.geometry.isna().sum() == 0


@needs_luxembourg
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


@needs_luxembourg
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


# --- Synthetic-fixture tests -----------------------------------------------
#
# The three tests below exercise failure modes that Luxembourg happens not to
# contain (zero aeroway relations, zero geometry-build failures, zero bare
# aeroway nodes), so they use small hand-built `.osm` XML files instead. That
# is exactly the situation the module docstring above warns against in
# general ("a hand-built fixture would encode this author's assumptions"),
# but here the assumption under test -- id-namespace collisions, malformed
# geometry, node-mapped stands -- is specific enough that a synthetic fixture
# is the only way to hit it deterministically.


def _write_osm(tmp_path, name: str, body: str) -> str:
    path = tmp_path / name
    path.write_text(
        "<?xml version='1.0' encoding='UTF-8'?>\n<osm version=\"0.6\">\n"
        + body
        + "\n</osm>\n"
    )
    return str(path)


def test_relation_derived_area_does_not_shadow_an_unrelated_way_with_the_same_id(
    tmp_path,
):
    """pyosmium's own docs say `orig_id()` is "not necessarily unique... way
    or relation which have an overlapping id space" -- a relation-derived
    area's `orig_id()` can collide with an unrelated way's `id` purely by
    coincidence, because way ids and relation ids are separate namespaces.

    This fixture reproduces that collision directly: a multipolygon relation
    id=20 tagged `aeroway=apron`, and a completely unrelated way id=20 tagged
    `aeroway=taxiway`. The relation assembles into an Area with
    `orig_id() == 20` and `from_way() == False`. A de-duplication join on
    `orig_id()` alone treats that as "the way half of this area, drop it" and
    silently deletes the taxiway -- exactly the failure mode this whole plan
    exists to catch, and it produced no trace: no exception, no missing row
    count, just one fewer taxiway than the file actually has.
    """
    pbf = _write_osm(
        tmp_path,
        "id_collision.osm",
        """
  <node id="1" lat="49.000" lon="6.000"/>
  <node id="2" lat="49.000" lon="6.001"/>
  <node id="3" lat="49.001" lon="6.001"/>
  <node id="4" lat="49.001" lon="6.000"/>
  <node id="5" lat="49.002" lon="6.002"/>
  <node id="6" lat="49.003" lon="6.003"/>
  <way id="20">
    <nd ref="5"/>
    <nd ref="6"/>
    <tag k="aeroway" v="taxiway"/>
  </way>
  <way id="100">
    <nd ref="1"/>
    <nd ref="2"/>
    <nd ref="3"/>
    <nd ref="4"/>
    <nd ref="1"/>
  </way>
  <relation id="20">
    <member type="way" ref="100" role="outer"/>
    <tag k="type" v="multipolygon"/>
    <tag k="aeroway" v="apron"/>
  </relation>""",
    )
    gdf = read_aeroway_features(pbf)
    ways = gdf[gdf["element"] == "way"]
    assert ((ways["id"] == 20) & (ways["aeroway"] == "taxiway")).any(), (
        "the unrelated taxiway way (id=20) was dropped -- a relation-derived "
        "area's orig_id() is being treated as a way id it does not own"
    )
    areas = gdf[gdf["element"] == "area"]
    assert (areas["aeroway"] == "apron").any(), "the relation-derived apron area is missing too"


def test_geometry_build_failures_are_counted_and_reported(tmp_path, capsys):
    """A way referencing a node the extract does not contain cannot be built
    into a geometry -- `create_linestring` raises. Skipping it is correct
    (see the module docstring), but the only prior evidence such a skip
    happened at all was a one-off manual count on the 40 MB Luxembourg file,
    which is not part of the shipped code path. At Europe scale a systematic
    geometry-build failure across a whole aeroway family must leave a trace
    in the function's own output, broken down per tag, or it is invisible.
    """
    pbf = _write_osm(
        tmp_path,
        "missing_node.osm",
        """
  <node id="1" lat="49.000" lon="6.000"/>
  <node id="2" lat="49.000" lon="6.001"/>
  <way id="200">
    <nd ref="1"/>
    <nd ref="999"/>
    <tag k="aeroway" v="taxiway"/>
  </way>
  <way id="201">
    <nd ref="1"/>
    <nd ref="2"/>
    <tag k="aeroway" v="runway"/>
  </way>""",
    )
    gdf = read_aeroway_features(pbf)
    # The broken taxiway must not appear...
    assert not (gdf["aeroway"] == "taxiway").any()
    # ...but the good runway must still come through -- the failure is
    # isolated to the one feature that could not be built.
    assert (gdf["aeroway"] == "runway").any()

    out = capsys.readouterr().out
    assert "taxiway" in out, (
        "no report of the failed geometry build -- a systematic failure "
        "across a whole family would be silent at Europe scale"
    )
    assert "1" in out


def test_node_mapped_parking_position_becomes_a_point(tmp_path):
    """Stands are commonly mapped as bare nodes at larger airports -- exactly
    the family AOBT/AIBT ride on -- but Luxembourg has zero aeroway nodes, so
    the `Node`/`Point` branch was previously exercised by nothing."""
    pbf = _write_osm(
        tmp_path,
        "stand_node.osm",
        """
  <node id="42" lat="49.000" lon="6.000">
    <tag k="aeroway" v="parking_position"/>
    <tag k="ref" v="A1"/>
  </node>""",
    )
    gdf = read_aeroway_features(pbf)
    assert len(gdf) == 1
    row = gdf.iloc[0]
    assert row["element"] == "node"
    assert row["id"] == 42
    assert row["aeroway"] == "parking_position"
    assert row["geometry"].geom_type == "Point"
    assert (row["geometry"].x, row["geometry"].y) == (6.0, 49.0)
