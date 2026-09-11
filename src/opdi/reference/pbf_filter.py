"""Pre-filter an OSM extract down to aeroway geometry.

``read_aeroway_features`` and ``read_aerodromes`` (``pbf_source.py``) both call
``with_areas()``, which needs osmium to hold node *locations* for the whole
file so it can assemble closed ways and multipolygon relations into areas.
For a small extract like Luxembourg that is a non-issue. For the 34.9 GB
Europe extract it is fatal under this container's 16 GB cgroup cap: the
default in-memory node index is OOM-killed, and the disk-backed alternative
(``sparse_file_array``) holds memory down to 3.1 GB but grows a 57 GB on-disk
index and had not finished assembling areas after 79 minutes -- rejected, see
the Task 4B brief.

The fix is to never call ``with_areas()`` on the big file at all. Filtering
first to *only* the objects aeroway geometry needs shrinks Europe down to a
file the default in-memory index handles comfortably, and everything
downstream -- ``read_aeroway_features``, ``read_aerodromes``, ``PbfLayoutSource``
-- runs unmodified against the filtered file in seconds instead of minutes.

A PBF is laid out in a fixed order: all nodes, then all ways, then all
relations. That ordering is why a relation cannot be resolved against its
member ways in one streaming pass -- a relation is only ever read *after* the
ways it references -- and it is why every one of the passes below is scoped
to a single entity type: `FileProcessor`'s `entities` argument skips the
other blocks entirely at the source, and stacking `osmium.filter.IdFilter` or
`KeyFilter` on top keeps the accept/reject test itself in C++. That matters
at Europe's scale: an early version of this module ran an *unfiltered*
`FileProcessor(src_path, WAY)` over Pass B and paid to decode every one of
Europe's ~500 million ways into a Python object just to test one `in` check
against an id set -- throughput fell from ~74 MB/s to ~3 MB/s. `IdFilter`
(pyosmium 4.3.1) rejects the ones that do not match before they ever cross
into Python, the same way `KeyFilter` already did in Pass A. `IdFilter`
itself needs a plain iterable of ids, not an `IdSet` -- `IdSet` is not
iterable -- which is what `_IdCollector` below exists to provide alongside
still-cheap membership testing.

The trap this exists to avoid: an apron or terminal apron is frequently
mapped as a multipolygon *relation* tagged ``aeroway=apron`` whose member ways
carry no ``aeroway`` tag of their own -- the tag lives on the relation, not on
the geometry. Filtering on the ``aeroway`` key alone keeps the relation but
drops its untagged member ways, and a relation with no members cannot be
assembled: the apron silently disappears. Pass B exists solely to close that
gap, by re-scanning ways -- restricted to just the member ids Pass A found --
after Pass A has seen every relation and knows which way ids it references.
"""
import array
import os
import time
from typing import Dict

import osmium

_NODE = osmium.osm.osm_entity_bits.NODE
_WAY = osmium.osm.osm_entity_bits.WAY
_RELATION = osmium.osm.osm_entity_bits.RELATION


class _IdCollector:
    """A de-duplicated, insertion-ordered set of ids that is both cheap to
    test membership of and cheap to hand to ``osmium.filter.IdFilter``.

    ``osmium.index.IdSet`` alone would cover the membership test, but it is
    not iterable, and ``IdFilter`` requires an iterable. ``array('q')`` keeps
    the ordered copy needed for that at 8 bytes per id -- tens of millions of
    node ids stay in the hundreds of MB, not the multiple of that a Python
    list of boxed ints would cost.
    """

    __slots__ = ("_seen", "_ids")

    def __init__(self):
        self._seen = osmium.index.IdSet()
        self._ids = array.array("q")

    def add(self, obj_id: int) -> None:
        if not self._seen.get(obj_id):
            self._seen.set(obj_id)
            self._ids.append(obj_id)

    def __contains__(self, obj_id: int) -> bool:
        return bool(self._seen.get(obj_id))

    def __len__(self) -> int:
        return len(self._ids)

    def ids(self) -> array.array:
        return self._ids


def filter_aeroway_pbf(src_path: str, dst_path: str) -> Dict[str, float]:
    """Write the subset of *src_path* that aeroway geometry needs to *dst_path*.

    Kept, in file order:

    * every node referenced by a kept way, or itself tagged ``aeroway``
      (bare-node stands are common -- see ``pbf_source.read_aeroway_features``);
    * every way tagged ``aeroway``, plus every way that is a member of a
      kept relation, even if the way itself carries no ``aeroway`` tag
      (the multipolygon-apron trap, see the module docstring);
    * every relation tagged ``aeroway``.

    Returns ``nodes``, ``ways``, ``relations`` (counts written), ``bytes_in``,
    ``bytes_out`` and ``seconds``. Never calls ``with_areas()`` -- that is
    precisely the memory behaviour this filter exists to avoid triggering on
    the unfiltered file. Callers run ``with_areas()`` afterwards, on the much
    smaller output.
    """
    t0 = time.monotonic()
    bytes_in = os.path.getsize(src_path)

    keep_nodes = _IdCollector()
    keep_ways = _IdCollector()
    keep_rels = _IdCollector()
    # Relation member-way ids are only ever used once, to build Pass B's
    # IdFilter -- a plain set is enough and keeps _IdCollector's bookkeeping
    # (and array.array copy) out of a structure that does not need it.
    member_way_ids: set = set()

    # --- Pass A: ways and relations tagged `aeroway`. Restricting `entities`
    # at the source skips the node blocks entirely, and `KeyFilter` rejects
    # in C++, so only the few hundred thousand tagged survivors ever become
    # Python objects.
    fp_a = (
        osmium.FileProcessor(src_path, _WAY | _RELATION)
        .with_filter(osmium.filter.KeyFilter("aeroway"))
    )
    for obj in fp_a:
        if isinstance(obj, osmium.osm.Way):
            keep_ways.add(obj.id)
            for n in obj.nodes:
                keep_nodes.add(n.ref)
        elif isinstance(obj, osmium.osm.Relation):
            keep_rels.add(obj.id)
            for m in obj.members:
                if m.type == "w":
                    member_way_ids.add(m.ref)

    # --- Pass A2: bare nodes tagged `aeroway` (a `parking_position` stand is
    # frequently mapped this way). Same C++-filtered shape as Pass A, scoped
    # to the node blocks instead.
    fp_a2 = (
        osmium.FileProcessor(src_path, _NODE)
        .with_filter(osmium.filter.KeyFilter("aeroway"))
    )
    for obj in fp_a2:
        keep_nodes.add(obj.id)

    # --- Pass B: relation member ways Pass A's tag filter dropped because
    # they carry no `aeroway` tag of their own -- the multipolygon-apron
    # trap (see module docstring). `IdFilter` restricts the scan to exactly
    # those ids in C++, rather than decoding every way in the file to ask
    # `id in keep_ways` in Python.
    pending = [wid for wid in member_way_ids if wid not in keep_ways]
    if pending:
        fp_b = (
            osmium.FileProcessor(src_path, _WAY)
            .with_filter(osmium.filter.IdFilter(array.array("q", pending)))
        )
        for obj in fp_b:
            keep_ways.add(obj.id)
            for n in obj.nodes:
                keep_nodes.add(n.ref)

    # --- Pass C: nodes, ways, relations, each its own entity-scoped,
    # id-filtered scan -- written in that order, which `SimpleWriter`
    # requires and which three single-entity scans already produce for
    # free. A single unfiltered pass over the whole file would put every
    # object Europe has -- nodes included, by far the largest block -- through
    # Python just to be rejected; three `IdFilter`-scoped scans reject
    # everything not kept in C++ instead.
    n_nodes = n_ways = n_rels = 0
    writer = osmium.SimpleWriter(dst_path)
    try:
        fp_c1 = (
            osmium.FileProcessor(src_path, _NODE)
            .with_filter(osmium.filter.IdFilter(keep_nodes.ids()))
        )
        for obj in fp_c1:
            writer.add_node(obj)
            n_nodes += 1

        fp_c2 = (
            osmium.FileProcessor(src_path, _WAY)
            .with_filter(osmium.filter.IdFilter(keep_ways.ids()))
        )
        for obj in fp_c2:
            writer.add_way(obj)
            n_ways += 1

        fp_c3 = (
            osmium.FileProcessor(src_path, _RELATION)
            .with_filter(osmium.filter.IdFilter(keep_rels.ids()))
        )
        for obj in fp_c3:
            writer.add_relation(obj)
            n_rels += 1
    finally:
        writer.close()

    bytes_out = os.path.getsize(dst_path)
    seconds = time.monotonic() - t0

    return {
        "nodes": n_nodes,
        "ways": n_ways,
        "relations": n_rels,
        "bytes_in": bytes_in,
        "bytes_out": bytes_out,
        "seconds": seconds,
    }
