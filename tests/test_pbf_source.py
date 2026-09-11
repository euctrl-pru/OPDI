"""Reading aeroway geometry out of a local OSM extract.

Every test uses the Luxembourg extract, which is ~40 MB and contains ELLX --
a real aerodrome with a runway, taxiways and stands. Using a real extract
rather than a synthetic one is deliberate: the failure modes this code has are
about how OSM actually encodes aprons and stands (closed ways that are areas,
multipolygon relations, missing width tags), and a hand-built fixture would
encode this author's assumptions about that rather than the reality.
"""
import os

import pandas as pd
import pytest

pytest.importorskip("osmium")

from opdi.reference.pbf_source import read_aeroway_features
from opdi.reference.h3_airport_layouts import AEROWAY_TAGS
AEROWAY_TAGS_SET = set(AEROWAY_TAGS)

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


# --- Aerodrome assignment ---------------------------------------------------

from opdi.reference.pbf_source import PbfLayoutSource, airport_boxes, read_aerodromes


class _Storage:
    """`oa_airports` and `oa_runways` as the generator reads them."""

    def __init__(self, spark):
        self._t = {
            "oa_airports": spark.createDataFrame(
                [("ELLX", 49.6233, 6.2044, "large_airport"),
                 ("EBBR", 50.9014, 4.4844, "large_airport")],
                "ident string, latitude_deg double, longitude_deg double, type string",
            ),
            "oa_runways": spark.createDataFrame(
                [("ELLX", 49.6266, 6.1867, 49.6200, 6.2247),
                 ("EBBR", 50.9010, 4.4700, 50.9060, 4.5000)],
                "airport_ident string, le_latitude_deg double, le_longitude_deg double, "
                "he_latitude_deg double, he_longitude_deg double",
            ),
        }

    def table_exists(self, name):
        return name in self._t

    def read_table(self, name):
        return self._t[name]


def test_a_box_is_built_from_the_runway_extent(spark):
    """Runways bound an airport's long axis, so their extent plus a margin
    encloses the aprons, stands and taxiways that sit between them. The margin
    is what makes it an envelope rather than a line."""
    boxes = airport_boxes(_Storage(spark)).set_index("ident")
    ellx = boxes.loc["ELLX"]
    assert ellx.lat_min < 49.6200 and ellx.lat_max > 49.6266
    assert ellx.lon_min < 6.1867 and ellx.lon_max > 6.2247


@needs_luxembourg
def test_aerodrome_polygons_are_read_with_their_icao_code():
    """ELLX is mapped as a closed way tagged `aeroway=aerodrome`, `icao=ELLX`.

    This is the polygon Overpass used to fetch by geocoding the airport's name.
    Reading it from the extract makes the key exact rather than a name lookup,
    which is the failure that returned nothing for five of twenty airports.
    """
    ad = read_aerodromes(PBF)
    assert "ELLX" in set(ad["icao"])
    row = ad[ad["icao"] == "ELLX"].iloc[0]
    assert row.geometry.geom_type in ("Polygon", "MultiPolygon")
    assert row.geometry.area > 0


def test_multi_value_icao_tag_is_split_into_one_row_per_code(tmp_path):
    """OSM's semicolon convention for a field shared by civil and military
    use -- Sion is tagged `icao='LSGS;LSMS'`. Upper-casing the whole string
    without splitting it means neither 'LSGS' nor 'LSMS' alone ever matches,
    and it is worse than simply missing: containment does not care what the
    icao string says, so the polygon still claims every feature inside it --
    those features become invisible to both real airports, and because the
    aerodrome is not *unassigned*, the box fallback is never even tried.
    """
    pbf = _write_osm(
        tmp_path,
        "multi_icao.osm",
        """
  <node id="1" lat="49.000" lon="6.000"/>
  <node id="2" lat="49.000" lon="6.001"/>
  <node id="3" lat="49.001" lon="6.001"/>
  <node id="4" lat="49.001" lon="6.000"/>
  <way id="60">
    <nd ref="1"/>
    <nd ref="2"/>
    <nd ref="3"/>
    <nd ref="4"/>
    <nd ref="1"/>
    <tag k="aeroway" v="aerodrome"/>
    <tag k="icao" v="LSGS;LSMS"/>
  </way>""",
    )
    ad = read_aerodromes(pbf)
    assert set(ad["icao"]) == {"LSGS", "LSMS"}, (
        "a semicolon-joined icao tag must yield one row per code, not one "
        "row keyed by the whole unsplit string"
    )
    assert len(ad) == 2, "both codes must share the geometry, not merge into one row"
    lsgs = ad[ad["icao"] == "LSGS"].iloc[0]
    lsms = ad[ad["icao"] == "LSMS"].iloc[0]
    assert lsgs.geometry.equals(lsms.geometry)


def test_multi_value_icao_tag_accepts_comma_and_strips_whitespace(tmp_path):
    """The same convention has been seen written with a comma and stray
    spaces in the wild; both are cheap to accept alongside the semicolon
    Sion actually uses."""
    pbf = _write_osm(
        tmp_path,
        "multi_icao_comma.osm",
        """
  <node id="1" lat="50.000" lon="7.000"/>
  <node id="2" lat="50.000" lon="7.001"/>
  <node id="3" lat="50.001" lon="7.001"/>
  <node id="4" lat="50.001" lon="7.000"/>
  <way id="61">
    <nd ref="1"/>
    <nd ref="2"/>
    <nd ref="3"/>
    <nd ref="4"/>
    <nd ref="1"/>
    <tag k="aeroway" v="aerodrome"/>
    <tag k="icao" v=" EDXX , EDYY "/>
  </way>""",
    )
    ad = read_aerodromes(pbf)
    assert set(ad["icao"]) == {"EDXX", "EDYY"}


@needs_luxembourg
def test_features_are_assigned_by_containment_in_the_aerodrome_polygon(spark):
    """The whole point of the change: a feature belongs to the airport whose
    boundary encloses it, not to the airport whose rectangle it happens to
    fall in."""
    src = PbfLayoutSource(PBF, _Storage(spark))
    ellx = src.features_for("ELLX")
    assert len(ellx) > 0
    assert set(ellx["aeroway"]) <= AEROWAY_TAGS_SET
    # Every returned feature really is inside the polygon it was assigned to.
    poly = read_aerodromes(PBF).set_index("icao").loc["ELLX"].geometry
    assert ellx.geometry.representative_point().within(poly).all()


@needs_luxembourg
def test_an_aerodrome_absent_from_the_extract_returns_nothing(spark):
    src = PbfLayoutSource(PBF, _Storage(spark))
    assert len(src.features_for("EBBR")) == 0


class _NodeAndPolygonStorage:
    """AAAA gets a real runway-extent box; BBBB does too, but the two are
    ~222 km apart so their boxes cannot overlap. The nearest-aerodrome guard
    is exercised separately (`_OverlappingStorage`); this fixture isolates
    just the polygon-vs-node split."""

    def __init__(self, spark):
        self._t = {
            "oa_airports": spark.createDataFrame(
                [("AAAA", 49.0000, 6.0000, "large_airport"),
                 ("BBBB", 51.0000, 8.0000, "large_airport")],
                "ident string, latitude_deg double, longitude_deg double, type string",
            ),
            "oa_runways": spark.createDataFrame(
                [("AAAA", 48.9990, 5.9990, 49.0010, 6.0010),
                 ("BBBB", 50.9990, 7.9990, 51.0010, 8.0010)],
                "airport_ident string, le_latitude_deg double, le_longitude_deg double, "
                "he_latitude_deg double, he_longitude_deg double",
            ),
        }

    def table_exists(self, name):
        return name in self._t

    def read_table(self, name):
        return self._t[name]


def test_an_aerodrome_with_only_a_node_falls_back_to_its_bbox(tmp_path, spark):
    """486 of the extract's icao-tagged aerodrome features are bare nodes, and
    a node encloses nothing. Those aerodromes must still get a grid, by the
    runway-extent box -- otherwise switching to polygons would silently drop
    every airport OSM has not drawn a boundary for.

    The fixture is synthetic so the two paths can be exercised side by side:
    one aerodrome mapped as a polygon (AAAA), one as a bare node (BBBB), each
    with a stand.
    """
    pbf = _write_osm(
        tmp_path,
        "node_fallback.osm",
        """
  <node id="1" lat="49.0000" lon="6.0000"/>
  <node id="2" lat="49.0000" lon="6.0020"/>
  <node id="3" lat="49.0020" lon="6.0020"/>
  <node id="4" lat="49.0020" lon="6.0000"/>
  <way id="10">
    <nd ref="1"/>
    <nd ref="2"/>
    <nd ref="3"/>
    <nd ref="4"/>
    <nd ref="1"/>
    <tag k="aeroway" v="aerodrome"/>
    <tag k="icao" v="AAAA"/>
  </way>
  <node id="5" lat="49.0010" lon="6.0010">
    <tag k="aeroway" v="parking_position"/>
    <tag k="ref" v="A1"/>
  </node>
  <node id="6" lat="51.0000" lon="8.0000">
    <tag k="aeroway" v="aerodrome"/>
    <tag k="icao" v="BBBB"/>
  </node>
  <node id="7" lat="51.0002" lon="8.0002">
    <tag k="aeroway" v="parking_position"/>
    <tag k="ref" v="B1"/>
  </node>""",
    )
    src = PbfLayoutSource(pbf, _NodeAndPolygonStorage(spark))
    aaaa = src.features_for("AAAA")
    bbbb = src.features_for("BBBB")
    assert len(aaaa) == 1 and (aaaa["ref"] == "A1").all(), (
        "AAAA has a real aerodrome polygon and must be assigned by containment"
    )
    assert len(bbbb) == 1 and (bbbb["ref"] == "B1").all(), (
        "BBBB's aerodrome is mapped as a bare node -- it has no area, so the "
        "stand must still reach it through the runway-extent box fallback"
    )
    report = src.assignment_report().set_index("icao")
    assert report.loc["AAAA", "method"] == "polygon"
    assert report.loc["BBBB", "method"] == "bbox"


class _PolygonAndBoxStorage:
    def __init__(self, spark):
        self._t = {
            "oa_airports": spark.createDataFrame(
                [("CCCC", 51.0005, 8.0005, "large_airport")],
                "ident string, latitude_deg double, longitude_deg double, type string",
            ),
            "oa_runways": spark.createDataFrame(
                [("CCCC", 50.9950, 7.9950, 51.0050, 8.0050)],
                "airport_ident string, le_latitude_deg double, le_longitude_deg double, "
                "he_latitude_deg double, he_longitude_deg double",
            ),
        }

    def table_exists(self, name):
        return name in self._t

    def read_table(self, name):
        return self._t[name]


def test_the_polygon_path_wins_where_both_are_available(tmp_path, spark):
    """An aerodrome with a polygon must NOT also pick up features its box would
    have caught but its boundary excludes. Otherwise the fallback quietly
    re-imposes the weakness the polygon was adopted to remove."""
    pbf = _write_osm(
        tmp_path,
        "polygon_wins.osm",
        """
  <node id="1" lat="51.0000" lon="8.0000"/>
  <node id="2" lat="51.0000" lon="8.0010"/>
  <node id="3" lat="51.0010" lon="8.0010"/>
  <node id="4" lat="51.0010" lon="8.0000"/>
  <way id="30">
    <nd ref="1"/>
    <nd ref="2"/>
    <nd ref="3"/>
    <nd ref="4"/>
    <nd ref="1"/>
    <tag k="aeroway" v="aerodrome"/>
    <tag k="icao" v="CCCC"/>
  </way>
  <node id="5" lat="51.0005" lon="8.0005">
    <tag k="aeroway" v="parking_position"/>
    <tag k="ref" v="INSIDE"/>
  </node>
  <node id="6" lat="51.0030" lon="8.0030">
    <tag k="aeroway" v="parking_position"/>
    <tag k="ref" v="OUTSIDE"/>
  </node>""",
    )
    src = PbfLayoutSource(pbf, _PolygonAndBoxStorage(spark))
    cccc = src.features_for("CCCC")
    refs = set(cccc["ref"])
    assert "INSIDE" in refs, "the stand inside the polygon must be assigned"
    assert "OUTSIDE" not in refs, (
        "the stand sits outside CCCC's boundary but inside the box the "
        "runway extent would have drawn -- the box must not be consulted "
        "for an aerodrome that already has a polygon"
    )


class _EmptyPolygonStorage:
    """ZZZZ's registry runway coordinates (~52.0300N, 9.0300E) are ~3.3 km
    from where its OSM aerodrome polygon is drawn (~52.0005N, 9.0005E) --
    the EFIT shape: a real icao-tagged boundary that simply does not
    contain any of the airport's mapped aeroway features."""

    def __init__(self, spark):
        self._t = {
            "oa_airports": spark.createDataFrame(
                [("ZZZZ", 52.0300, 9.0300, "medium_airport")],
                "ident string, latitude_deg double, longitude_deg double, type string",
            ),
            "oa_runways": spark.createDataFrame(
                [("ZZZZ", 52.0290, 9.0290, 52.0310, 9.0310)],
                "airport_ident string, le_latitude_deg double, le_longitude_deg double, "
                "he_latitude_deg double, he_longitude_deg double",
            ),
        }

    def table_exists(self, name):
        return name in self._t

    def read_table(self, name):
        return self._t[name]


def test_an_aerodrome_whose_polygon_contains_no_features_falls_back_to_its_bbox(
    tmp_path, spark
):
    """EFIT: an icao-tagged aerodrome polygon exists in the extract, but
    every one of the airport's aeroway features sits outside it -- about
    3 km away in the real case. Before this fix, "has a polygon" alone was
    enough to exclude an aerodrome from the box pass, so an empty polygon
    meant the aerodrome got nothing at all, permanently -- worse than an
    aerodrome with no polygon, which at least reaches the box. The fix:
    only a polygon that actually covered >=1 feature excludes its
    aerodrome from the box pass.
    """
    pbf = _write_osm(
        tmp_path,
        "empty_polygon.osm",
        """
  <node id="1" lat="52.0000" lon="9.0000"/>
  <node id="2" lat="52.0000" lon="9.0010"/>
  <node id="3" lat="52.0010" lon="9.0010"/>
  <node id="4" lat="52.0010" lon="9.0000"/>
  <way id="70">
    <nd ref="1"/>
    <nd ref="2"/>
    <nd ref="3"/>
    <nd ref="4"/>
    <nd ref="1"/>
    <tag k="aeroway" v="aerodrome"/>
    <tag k="icao" v="ZZZZ"/>
  </way>
  <node id="5" lat="52.0290" lon="9.0290"/>
  <node id="6" lat="52.0310" lon="9.0310"/>
  <way id="71">
    <nd ref="5"/>
    <nd ref="6"/>
    <tag k="aeroway" v="runway"/>
  </way>""",
    )
    src = PbfLayoutSource(pbf, _EmptyPolygonStorage(spark))
    zzzz = src.features_for("ZZZZ")
    assert len(zzzz) == 1 and (zzzz["aeroway"] == "runway").all(), (
        "ZZZZ has an aerodrome polygon, but the polygon encloses none of "
        "its aeroway features -- the runway must still be recovered "
        "through the box fallback rather than being permanently lost"
    )
    report = src.assignment_report().set_index("icao")
    assert report.loc["ZZZZ", "method"] == "bbox", (
        "the polygon produced nothing, so ZZZZ's actual features came "
        "from the box pass -- the report should say so, not credit the "
        "empty polygon with work it did not do"
    )


class _EmptyStorage:
    """No airports in scope. These tests exercise only the polygon-overlap
    tie-break, which is resolved entirely within pass 1 (the sjoin) and never
    consults `airport_boxes` -- an aerodrome absent from `oa_airports`
    entirely can still win a polygon assignment."""

    def __init__(self, spark):
        self._t = {
            "oa_airports": spark.createDataFrame(
                [], "ident string, latitude_deg double, longitude_deg double, type string",
            ),
            "oa_runways": spark.createDataFrame(
                [], "airport_ident string, le_latitude_deg double, le_longitude_deg double, "
                "he_latitude_deg double, he_longitude_deg double",
            ),
        }

    def table_exists(self, name):
        return name in self._t

    def read_table(self, name):
        return self._t[name]


def test_an_overlapping_feature_is_assigned_to_the_smaller_polygon(tmp_path, spark):
    """Two aerodrome polygons can genuinely overlap in OSM. A feature whose
    representative point lands inside both must go to the smaller one -- the
    more specific boundary -- not to whichever happens to come first for some
    unrelated reason. SMAL is wholly inside LRGE, and the stand sits in the
    middle of SMAL, so it is inside both."""
    pbf = _write_osm(
        tmp_path,
        "overlap_area.osm",
        """
  <node id="1" lat="50.0000" lon="9.0000"/>
  <node id="2" lat="50.0000" lon="9.0100"/>
  <node id="3" lat="50.0100" lon="9.0100"/>
  <node id="4" lat="50.0100" lon="9.0000"/>
  <way id="40">
    <nd ref="1"/>
    <nd ref="2"/>
    <nd ref="3"/>
    <nd ref="4"/>
    <nd ref="1"/>
    <tag k="aeroway" v="aerodrome"/>
    <tag k="icao" v="LRGE"/>
  </way>
  <node id="5" lat="50.0040" lon="9.0040"/>
  <node id="6" lat="50.0040" lon="9.0060"/>
  <node id="7" lat="50.0060" lon="9.0060"/>
  <node id="8" lat="50.0060" lon="9.0040"/>
  <way id="41">
    <nd ref="5"/>
    <nd ref="6"/>
    <nd ref="7"/>
    <nd ref="8"/>
    <nd ref="5"/>
    <tag k="aeroway" v="aerodrome"/>
    <tag k="icao" v="SMAL"/>
  </way>
  <node id="9" lat="50.0050" lon="9.0050">
    <tag k="aeroway" v="parking_position"/>
    <tag k="ref" v="OVERLAP"/>
  </node>""",
    )
    src = PbfLayoutSource(pbf, _EmptyStorage(spark))
    large = src.features_for("LRGE")
    small = src.features_for("SMAL")
    assert "OVERLAP" not in set(large["ref"]), (
        "the stand is inside both polygons but must not go to the larger, "
        "less specific one"
    )
    assert "OVERLAP" in set(small["ref"]), (
        "the smaller, more specific polygon must claim an overlapping feature"
    )


def test_equal_area_overlap_is_assigned_to_the_alphabetically_first_icao(
    tmp_path, spark
):
    """Two equal-area polygons leave the area sort with nothing to decide, so
    the tie-break has to be a deterministic rule, not accident of file or
    join order. BBBB's way is written before AAAA's in the fixture, so a
    passing test cannot be explained by file order alone -- only the
    icao-ascending tie-break explains AAAA winning."""
    pbf = _write_osm(
        tmp_path,
        "overlap_tie.osm",
        """
  <node id="10" lat="51.0000" lon="10.0000"/>
  <node id="11" lat="51.0000" lon="10.0020"/>
  <node id="12" lat="51.0020" lon="10.0020"/>
  <node id="13" lat="51.0020" lon="10.0000"/>
  <way id="50">
    <nd ref="10"/>
    <nd ref="11"/>
    <nd ref="12"/>
    <nd ref="13"/>
    <nd ref="10"/>
    <tag k="aeroway" v="aerodrome"/>
    <tag k="icao" v="BBBB"/>
  </way>
  <node id="14" lat="51.0010" lon="10.0010"/>
  <node id="15" lat="51.0010" lon="10.0030"/>
  <node id="16" lat="51.0030" lon="10.0030"/>
  <node id="17" lat="51.0030" lon="10.0010"/>
  <way id="51">
    <nd ref="14"/>
    <nd ref="15"/>
    <nd ref="16"/>
    <nd ref="17"/>
    <nd ref="14"/>
    <tag k="aeroway" v="aerodrome"/>
    <tag k="icao" v="AAAA"/>
  </way>
  <node id="18" lat="51.0015" lon="10.0015">
    <tag k="aeroway" v="parking_position"/>
    <tag k="ref" v="TIE"/>
  </node>""",
    )
    src = PbfLayoutSource(pbf, _EmptyStorage(spark))
    aaaa = src.features_for("AAAA")
    bbbb = src.features_for("BBBB")
    assert "TIE" in set(aaaa["ref"]), (
        "equal-area overlap must go to the alphabetically first icao"
    )
    assert "TIE" not in set(bbbb["ref"])


@needs_luxembourg
def test_the_source_reads_the_extract_once(spark):
    """1,353 airports must not mean 1,353 passes over a 30 GB file."""
    src = PbfLayoutSource(PBF, _Storage(spark))
    src.features_for("ELLX")
    before = src._read_count
    src.features_for("ELLX")
    src.features_for("EBBR")
    assert src._read_count == before, "the extract was re-read"


class _OverlappingStorage:
    """Two aerodromes close enough that their boxes genuinely overlap.

    AAAA at 49.0000N and BBBB at 49.0080N are ~0.89 km apart. With
    ``BOX_MARGIN_KM = 1.5`` each box extends roughly 1.5 km beyond its own
    short runway, so the two boxes cover a shared band from about 48.9945N to
    49.0145N -- unlike ELLX/EBBR (~200 km apart, from Belgium), this is close
    enough that the box test alone cannot distinguish them.
    """

    def __init__(self, spark):
        self._t = {
            "oa_airports": spark.createDataFrame(
                [("AAAA", 49.0000, 6.0000, "medium_airport"),
                 ("BBBB", 49.0080, 6.0000, "medium_airport")],
                "ident string, latitude_deg double, longitude_deg double, type string",
            ),
            "oa_runways": spark.createDataFrame(
                [("AAAA", 49.0000, 6.0000, 49.0010, 6.0010),
                 ("BBBB", 49.0080, 6.0000, 49.0090, 6.0010)],
                "airport_ident string, le_latitude_deg double, le_longitude_deg double, "
                "he_latitude_deg double, he_longitude_deg double",
            ),
        }

    def table_exists(self, name):
        return name in self._t

    def read_table(self, name):
        return self._t[name]


@needs_luxembourg
def test_hexagonify_uses_the_pbf_source_when_given_one(spark):
    """The seam. `hexagonify_airport` is unchanged apart from where its
    features come from: same widths, same polygon conversion, same H3, same
    twelve columns."""
    from opdi.reference.h3_airport_layouts import HEXAERO_SCHEMA, hexagonify_airport

    src = PbfLayoutSource(PBF, _Storage(spark))
    df = hexagonify_airport("ELLX", resolution=12, source=src)

    assert len(df) > 0
    # On the PBF path `element`/`id` are already real columns (never in the
    # index -- see `PbfLayoutSource.features_for`'s `_OUTPUT_COLUMNS`), so
    # `reset_index(drop=True)` here is a no-op on them either way; this
    # column-list check pins the *other* direction of the flag mistake --
    # `drop=False` would leave a spurious `index` column from promoting the
    # default RangeIndex -- not the drop-destroys-the-data direction, which
    # only the Overpass branch can exhibit (see the test directly below).
    assert list(df.columns) == [f.name for f in HEXAERO_SCHEMA.fields]
    assert set(df["hexaero_apt_icao"]) == {"ELLX"}
    assert (df["hexaero_res"] == 12).all()
    assert "parking_position" in set(df["hexaero_aeroway"])

    # Not a pin of the reset_index flag on this branch (see above) -- just
    # the ordinary "the seam produced usable identity columns" check.
    assert df["hexaero_osm_id"].notna().all(), "hexaero_osm_id is null on the PBF path"
    assert df["hexaero_type"].notna().all(), "hexaero_type is null on the PBF path"
    assert df["hexaero_osm_id"].map(type).eq(int).all(), "hexaero_osm_id is not int"
    assert df["hexaero_type"].isin(["node", "way", "area", "relation"]).all(), (
        "hexaero_type is not a valid OSM element type"
    )

    # First end-to-end run of the full pipeline (read -> assign -> widths ->
    # polygon conversion -> H3) -- a family silently lost between
    # `features_for` and the H3 output would not show up in any earlier task.
    counts = df.groupby("hexaero_aeroway").size()
    print(f"ELLX per-family H3 cell counts:\n{counts}")
    assert len(counts) > 1, "only one aeroway family reached the H3 output"


def _fake_overpass_gdf():
    """Shaped exactly like `retrieve_osm_data`'s return: `element` and `id`
    live in a `MultiIndex` (osmnx's own `.set_index(["element", "id"])` in
    `osmnx/features.py`), not as columns. This is the frame
    `reset_index(drop=source is not None)` must NOT drop on -- dropping would
    silently discard the index and leave `hexaero_osm_id`/`hexaero_type`
    null, which nothing downstream checks. Calling the real Overpass endpoint
    to get this shape is not an option: it has refused this host.
    """
    import geopandas as gpd
    from shapely.geometry import LineString, Point

    index = pd.MultiIndex.from_tuples(
        [("way", 111), ("node", 222)], names=["element", "id"]
    )
    data = {
        "geometry": [
            LineString([(6.20, 49.62), (6.21, 49.63)]),
            Point(6.22, 49.64),
        ],
        "aeroway": ["taxiway", "parking_position"],
        "width": [None, None],
        "ref": [None, "A1"],
        "surface": ["asphalt", None],
        "length": [None, None],
    }
    return gpd.GeoDataFrame(data, index=index, geometry="geometry", crs="EPSG:4326")


def test_hexagonify_pins_reset_index_on_the_overpass_branch(monkeypatch):
    """The branch that actually matters for the `reset_index(drop=...)`
    flag: on the Overpass path, `element`/`id` arrive in the index, so
    `drop=True` there would destroy them. `source=None` routes through
    `retrieve_osm_data`, monkeypatched here to the fake above so no HTTP call
    is made.
    """
    import opdi.reference.h3_airport_layouts as mod

    monkeypatch.setattr(mod, "retrieve_osm_data", lambda icao: _fake_overpass_gdf())

    df = mod.hexagonify_airport("EBBR", resolution=12, source=None)

    assert list(df.columns) == [f.name for f in mod.HEXAERO_SCHEMA.fields]
    assert df["hexaero_osm_id"].notna().all(), "hexaero_osm_id is null on the Overpass path"
    assert df["hexaero_type"].notna().all(), "hexaero_type is null on the Overpass path"
    assert set(df["hexaero_osm_id"]) == {111, 222}
    assert set(df["hexaero_type"]) == {"way", "node"}


def test_a_feature_in_overlapping_boxes_goes_to_the_nearer_aerodrome_only(
    tmp_path, spark
):
    """The overlap case the ELLX/EBBR test cannot exercise, because nothing in
    the Luxembourg extract is anywhere near Belgium. Here the two aerodromes
    are ~0.89 km apart, so their boxes genuinely overlap (see
    `_OverlappingStorage`), and a single taxiway node sits at 49.0020N --
    inside *both* boxes, ~0.22 km from AAAA and ~0.67 km from BBBB. The box
    filter alone would return it from both `features_for` calls; only the
    nearest-aerodrome guard keeps it out of BBBB's result."""
    pbf = _write_osm(
        tmp_path,
        "overlap.osm",
        """
  <node id="1" lat="49.0020" lon="6.0005">
    <tag k="aeroway" v="taxiway"/>
  </node>""",
    )
    src = PbfLayoutSource(pbf, _OverlappingStorage(spark))
    aaaa = src.features_for("AAAA")
    bbbb = src.features_for("BBBB")
    assert len(aaaa) == 1, "the feature is nearer to AAAA and must be assigned to it"
    assert len(bbbb) == 0, (
        "the feature is inside BBBB's box too, but it is farther from BBBB "
        "than from AAAA -- the nearest-aerodrome guard must exclude it"
    )
