"""Pre-filtering an OSM extract to aeroway geometry, before ``with_areas()``
ever sees it.

Two tests replay Task 2 and Task 3's measured Luxembourg counts through the
filter to prove nothing downstream can tell the file was filtered. The third
is synthetic and targets the one failure mode a real extract cannot be
trusted to exercise deterministically: a multipolygon apron whose member ways
carry no ``aeroway`` tag of their own.
"""
import os

import pytest

pytest.importorskip("osmium")

from opdi.reference.pbf_filter import filter_aeroway_pbf
from opdi.reference.pbf_source import read_aerodromes, read_aeroway_features

PBF = "/home/jupyter/work/osm/luxembourg-latest.osm.pbf"

needs_luxembourg = pytest.mark.skipif(
    not os.path.exists(PBF), reason="Luxembourg extract not downloaded (Task 1)"
)


@needs_luxembourg
def test_the_filtered_extract_yields_the_same_aeroway_features(tmp_path):
    """The filter is only correct if nothing downstream can tell the
    difference. Task 2 measured Luxembourg at 116 taxiway, 5 runway, 57 apron,
    6 hangar, 194 parking_position, 0 threshold, 0 deicing_pad -- so the
    filtered file must reproduce that exactly, not approximately."""
    dst = tmp_path / "aeroway-lux.osm.pbf"
    stats = filter_aeroway_pbf(PBF, str(dst))
    assert dst.exists() and stats["bytes_out"] < stats["bytes_in"]

    before = read_aeroway_features(PBF)
    after = read_aeroway_features(str(dst))
    assert (after["aeroway"].value_counts().sort_index()
            .equals(before["aeroway"].value_counts().sort_index()))
    assert set(zip(after["element"], after["id"])) == set(zip(before["element"], before["id"]))


@needs_luxembourg
def test_aerodrome_polygons_survive_the_filter(tmp_path):
    """Task 3 assigns features by containment in the `aeroway=aerodrome`
    polygon. If the filter drops the boundary, or keeps it as an unassemblable
    fragment, every feature becomes unassigned and the grid silently empties."""
    dst = tmp_path / "aeroway-lux.osm.pbf"
    filter_aeroway_pbf(PBF, str(dst))
    before, after = read_aerodromes(PBF), read_aerodromes(str(dst))
    assert set(after["icao"]) == set(before["icao"])
    for icao in before["icao"]:
        a = after.set_index("icao").loc[icao].geometry
        b = before.set_index("icao").loc[icao].geometry
        assert a.equals(b) or a.symmetric_difference(b).area < 1e-12


def _write_osm(tmp_path, name: str, body: str) -> str:
    path = tmp_path / name
    path.write_text(
        "<?xml version='1.0' encoding='UTF-8'?>\n<osm version=\"0.6\">\n"
        + body
        + "\n</osm>\n"
    )
    return str(path)


def test_a_relation_member_way_is_kept_even_though_it_is_untagged(tmp_path):
    """The trap. An apron mapped as a multipolygon relation has member ways
    that carry no `aeroway` tag of their own. Filter on the tag alone and the
    members vanish, the relation cannot be assembled, and the apron
    disappears -- at every airport that maps aprons this way, silently.

    Mutation-verified: comment out Pass B's member-way collection in
    `pbf_filter.py` and this test fails, because the relation's own two ways
    (100, 101) never carry an `aeroway` tag -- only the relation does.
    """
    src = _write_osm(
        tmp_path,
        "untagged_members.osm",
        """
  <node id="1" lat="49.0000" lon="6.0000"/>
  <node id="2" lat="49.0000" lon="6.0010"/>
  <node id="3" lat="49.0010" lon="6.0010"/>
  <node id="4" lat="49.0010" lon="6.0000"/>
  <way id="100">
    <nd ref="1"/>
    <nd ref="2"/>
  </way>
  <way id="101">
    <nd ref="2"/>
    <nd ref="3"/>
    <nd ref="4"/>
    <nd ref="1"/>
  </way>
  <relation id="50">
    <member type="way" ref="100" role="outer"/>
    <member type="way" ref="101" role="outer"/>
    <tag k="type" v="multipolygon"/>
    <tag k="aeroway" v="apron"/>
  </relation>""",
    )
    dst = tmp_path / "filtered.osm.pbf"
    filter_aeroway_pbf(src, str(dst))

    gdf = read_aeroway_features(str(dst))
    areas = gdf[gdf["element"] == "area"]
    assert (areas["aeroway"] == "apron").any(), (
        "the relation-derived apron area is missing -- its untagged member "
        "ways were dropped by the filter, so with_areas() could not "
        "assemble the multipolygon"
    )
