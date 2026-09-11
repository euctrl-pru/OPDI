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
import re
import warnings
from collections import Counter
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


def _split_icao_codes(raw: Optional[str]) -> List[str]:
    """OSM's semicolon convention for an ``icao`` field shared by more than
    one use -- Sion is tagged ``icao='LSGS;LSMS'``, civil and military
    sharing one field. A comma is accepted too, for the same convention seen
    written that way elsewhere. Splitting (rather than upper-casing the whole
    string as a single key) matters twice over: neither code would otherwise
    ever match a lookup for ``LSGS`` or ``LSMS`` alone, and the polygon would
    still silently claim every feature inside it regardless -- containment
    does not care what the icao string says -- so both real airports would
    end up with nothing, and because the aerodrome is not *unassigned*, the
    box fallback would never even be tried for either of them.
    """
    if not raw:
        return []
    seen: List[str] = []
    for part in re.split(r"[;,]", raw):
        code = part.strip().upper()
        if code and code not in seen:
            seen.append(code)
    return seen


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
    would buffer into a thin sliver in the wrong shape. A way-derived Area's
    `orig_id()` gives back that way's id, which is why `orig_id()` -- not
    `id` -- is the right join key back to the way. Whenever a *way-derived*
    Area's orig_id matches a Way's id, the Way is dropped and the Area kept.

    Only way-derived areas may shadow a way this way. pyosmium documents
    `orig_id()` as "not necessarily unique... way or relation which have an
    overlapping id space": way ids and relation ids are separate namespaces,
    so a relation-derived area's `orig_id()` can coincide with the id of a
    completely unrelated way. `from_way()` distinguishes the two cases, and
    only areas where it is true contribute to the drop set -- a relation-
    derived area never had an original way to shadow. Open ways (taxiway and
    runway centrelines) have no Area counterpart at all and are unaffected
    either way.
    """
    rows: List[dict] = []
    # orig_ids of areas assembled from a *way* (not a relation) -- see the
    # docstring above for why only these may suppress a duplicate Way row.
    way_derived_area_ids: set = set()
    # Per-tag count of features whose geometry could not be built (e.g. a
    # way referencing a node outside the extract). Reported at the end so a
    # systematic failure across a whole aeroway family is visible rather
    # than silently swallowed by the `except` below.
    build_failures: Counter = Counter()

    # `with_areas()` makes osmium assemble multipolygon relations and closed
    # ways into areas, which is how OSM encodes an apron. Without it those
    # features arrive as bare ways and an apron becomes a thin buffered line.
    fp = (
        osmium.FileProcessor(pbf_path)
        .with_areas()
        .with_filter(osmium.filter.KeyFilter("aeroway"))
    )
    for obj in fp:
        tag = obj.tags.get("aeroway")
        if tag not in AEROWAY_TAGS:
            continue
        try:
            if isinstance(obj, osmium.osm.Area):
                geom = shapely_wkb.loads(_WKB.create_multipolygon(obj), hex=True)
                element, oid = "area", obj.orig_id()
                if obj.from_way():
                    way_derived_area_ids.add(oid)
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
            # the wrong place. Counted below so the skip is visible.
            build_failures[tag] += 1
            continue
        if geom is None or geom.is_empty:
            continue
        rows.append({"element": element, "id": int(oid), "geometry": geom,
                     **_tags_of(obj)})

    if build_failures:
        total = sum(build_failures.values())
        breakdown = ", ".join(
            f"{t}: {c}" for t, c in sorted(build_failures.items())
        )
        print(
            f"read_aeroway_features: skipped {total} feature(s) in {pbf_path} "
            f"whose geometry could not be built ({breakdown})"
        )

    if not rows:
        raise ValueError(f"no aeroway features found in {pbf_path}")

    df = pd.DataFrame(rows)

    # Drop the Way half of any (Area, Way) pair that shares an orig_id -- see
    # the docstring. Only ways whose id collides with a *way-derived* area's
    # orig_id are affected; open ways (taxiway/runway centrelines) and ways
    # that merely share a numeric id with an unrelated relation survive.
    is_duplicate_way = (df["element"] == "way") & df["id"].isin(
        way_derived_area_ids
    )
    df = df[~is_duplicate_way]

    return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")


def read_aerodromes(pbf_path: str) -> gpd.GeoDataFrame:
    """OSM's own aerodrome boundary polygons, keyed by ICAO code.

    ``aeroway=aerodrome`` is OSM's tag for the airport boundary itself -- the
    same polygon Overpass used to fetch by first geocoding the airport's
    *name* through Nominatim. Reading it here from the extract keys it on the
    ``icao`` tag instead, which is exact where the geocode step was a name
    lookup that failed silently for five of twenty airports during the
    flight-events-v4 campaign.

    Returns ``icao``, ``name``, ``element``, ``id``, ``geometry`` -- one row
    per aerodrome feature whose geometry could be assembled as a polygon.
    Bare nodes carry no area and are excluded here; ``PbfLayoutSource`` falls
    back to the runway-extent box for those.

    Reuses both traps solved in ``read_aeroway_features``: geometry-build
    failures are counted rather than swallowed, and ``with_areas()`` emits
    the assembled ``Area`` *and* the original closed ``Way`` for the same
    boundary -- only the Area is kept, and a way whose id never turns up
    among the *way-derived* areas' ``orig_id()`` is counted as a failed
    assembly rather than silently dropped.
    """
    rows: List[dict] = []
    way_ids_ok: set = set()
    candidate_way_ids: dict = {}  # way id -> [icao, ...], for failed-assembly detection
    build_failures: Counter = Counter()

    fp = (
        osmium.FileProcessor(pbf_path)
        .with_areas()
        .with_filter(osmium.filter.KeyFilter("aeroway"))
    )
    for obj in fp:
        if obj.tags.get("aeroway") != "aerodrome":
            continue
        codes = _split_icao_codes(obj.tags.get("icao"))
        if not codes:
            continue
        if isinstance(obj, osmium.osm.Area):
            try:
                geom = shapely_wkb.loads(_WKB.create_multipolygon(obj), hex=True)
            except Exception:
                for code in codes:
                    build_failures[code] += 1
                continue
            if geom is None or geom.is_empty:
                continue
            oid = obj.orig_id()
            if obj.from_way():
                way_ids_ok.add(oid)
            # A multi-value icao tag (Sion: 'LSGS;LSMS') gets one row per
            # code, all sharing this same geometry -- see
            # `_split_icao_codes`. Almost always a single-element list.
            for code in codes:
                rows.append({"icao": code, "name": obj.tags.get("name"),
                             "element": "area", "id": int(oid), "geometry": geom})
        elif isinstance(obj, osmium.osm.Way):
            # The raw closed way behind an assembled Area, or -- if
            # assembly failed -- the only trace of this aerodrome's
            # boundary. Recorded, not appended: matched against
            # `way_ids_ok` after the loop rather than turned into a row
            # directly, so it is never double-counted alongside its Area.
            candidate_way_ids[obj.id] = codes
        elif isinstance(obj, osmium.osm.Node):
            # A bare node has no area. Not a failure -- the fallback box
            # handles it -- so it is neither counted nor rowed here.
            continue
        else:
            continue

    for way_id, codes in candidate_way_ids.items():
        if way_id not in way_ids_ok:
            for code in codes:
                build_failures[code] += 1

    if build_failures:
        total = sum(build_failures.values())
        breakdown = ", ".join(f"{k}: {c}" for k, c in sorted(build_failures.items()))
        print(
            f"read_aerodromes: {total} aerodrome(s) in {pbf_path} whose "
            f"boundary could not be assembled as a polygon ({breakdown})"
        )

    if not rows:
        return gpd.GeoDataFrame(
            columns=["icao", "name", "element", "id", "geometry"],
            geometry="geometry", crs="EPSG:4326",
        )

    df = pd.DataFrame(rows)
    return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")


import math

from pyspark.sql import functions as F

#: Margin around the runway extent, in kilometres. Aprons, stands and hangars
#: sit off the runway axis; 1.5 km covers them at the largest aerodromes without
#: reaching a neighbouring field at typical separations.
BOX_MARGIN_KM = 1.5

#: Fallback half-size when an aerodrome has no usable runway coordinates, so it
#: still gets a box rather than being dropped.
FALLBACK_HALF_KM = 3.0

_KM_PER_DEG_LAT = 111.0


def _deg_lon(km: float, lat: float) -> float:
    return km / (_KM_PER_DEG_LAT * max(0.05, math.cos(math.radians(lat))))


def airport_boxes(storage, airport_types=None) -> pd.DataFrame:
    """One bounding box per aerodrome, from its runway extent.

    Overpass derived the search area from a geocoded place polygon. That is the
    step being removed, so the area has to come from reference data instead:
    ``oa_runways`` holds both thresholds of every runway, and a runway bounds
    the airport's long axis.
    """
    airport_types = airport_types or ["large_airport", "medium_airport"]
    apt = (
        storage.read_table("oa_airports")
        .filter(F.col("type").isin(airport_types))
        .select("ident", "latitude_deg", "longitude_deg")
        .toPandas()
        .rename(columns={"latitude_deg": "apt_lat", "longitude_deg": "apt_lon"})
    )
    rwy = (
        storage.read_table("oa_runways")
        .select("airport_ident", "le_latitude_deg", "le_longitude_deg",
                "he_latitude_deg", "he_longitude_deg")
        .toPandas()
    )
    lat = pd.concat([rwy["le_latitude_deg"], rwy["he_latitude_deg"]])
    lon = pd.concat([rwy["le_longitude_deg"], rwy["he_longitude_deg"]])
    ident = pd.concat([rwy["airport_ident"], rwy["airport_ident"]])
    ext = (
        pd.DataFrame({"ident": ident, "lat": lat, "lon": lon})
        .dropna()
        .groupby("ident")
        .agg(lat_min=("lat", "min"), lat_max=("lat", "max"),
             lon_min=("lon", "min"), lon_max=("lon", "max"))
        .reset_index()
    )
    out = apt.merge(ext, on="ident", how="left")

    have = out["lat_min"].notna()
    m_lat = BOX_MARGIN_KM / _KM_PER_DEG_LAT
    out.loc[have, "lat_min"] -= m_lat
    out.loc[have, "lat_max"] += m_lat
    out.loc[have, "lon_min"] -= [
        _deg_lon(BOX_MARGIN_KM, v) for v in out.loc[have, "apt_lat"]
    ]
    out.loc[have, "lon_max"] += [
        _deg_lon(BOX_MARGIN_KM, v) for v in out.loc[have, "apt_lat"]
    ]

    # No runway coordinates: a square around the aerodrome point, so it is
    # still built rather than silently absent from the table.
    miss = ~have
    f_lat = FALLBACK_HALF_KM / _KM_PER_DEG_LAT
    out.loc[miss, "lat_min"] = out.loc[miss, "apt_lat"] - f_lat
    out.loc[miss, "lat_max"] = out.loc[miss, "apt_lat"] + f_lat
    out.loc[miss, "lon_min"] = out.loc[miss, "apt_lon"] - [
        _deg_lon(FALLBACK_HALF_KM, v) for v in out.loc[miss, "apt_lat"]
    ]
    out.loc[miss, "lon_max"] = out.loc[miss, "apt_lon"] + [
        _deg_lon(FALLBACK_HALF_KM, v) for v in out.loc[miss, "apt_lat"]
    ]
    return out


#: Columns the seam contract requires. `hexagonify_airport` renames
#: `id -> hexaero_osm_id` and `element -> hexaero_type`; nothing downstream
#: tolerates the join artefacts `geopandas.sjoin` or the helper columns below
#: add, so every return path -- including the empty ones -- is restricted to
#: exactly this list before it leaves the class.
_OUTPUT_COLUMNS = ["element", "id", "geometry", "aeroway", "width", "ref",
                    "surface", "length"]


class PbfLayoutSource:
    """Per-airport aeroway features, served from one pass over the extract.

    The extract is read on first use and held in memory. Europe's aeroway
    features are a small fraction of the file -- tens of megabytes as
    geometry -- so this is affordable, and the alternative (a pass per airport)
    would be 1,353 passes over 30 GB.

    Assignment is containment in the aerodrome's own OSM boundary polygon
    (``read_aerodromes``), computed once for every feature in a single spatial
    join. An aerodrome with no polygon in the extract -- most commonly because
    it is mapped as a bare node, which encloses nothing -- falls back to the
    runway-extent box with the nearest-aerodrome guard, applied only to
    features the polygon pass left unassigned so the box's looser boundary
    never overrides a real one.
    """

    def __init__(self, pbf_path: str, storage, airport_types=None):
        self.pbf_path = pbf_path
        self._boxes = airport_boxes(storage, airport_types)
        self._features: Optional[gpd.GeoDataFrame] = None
        self._aerodromes: Optional[gpd.GeoDataFrame] = None
        self._assigned_icao: Optional[pd.Series] = None
        # icaos whose own polygon actually covered >=1 feature -- see
        # `_compute_assignment`. Distinct from "has a polygon at all":
        # EFIT has one, but it encloses none of EFIT's aeroway features.
        self._polygon_covered_icaos: set = set()
        self._read_count = 0

    def _load(self) -> gpd.GeoDataFrame:
        if self._features is None:
            self._features = read_aeroway_features(self.pbf_path)
            self._aerodromes = read_aerodromes(self.pbf_path)
            self._read_count += 1
            self._assigned_icao = self._compute_assignment()
        return self._features

    def _compute_assignment(self) -> pd.Series:
        feats = self._features
        assigned = pd.Series(pd.NA, index=feats.index, dtype=object)

        # --- Pass 1: containment in the aerodrome's own polygon -------------
        aerodromes = self._aerodromes
        if aerodromes is not None and len(aerodromes):
            rep_points = gpd.GeoDataFrame(
                geometry=feats.geometry.representative_point(),
                index=feats.index, crs=feats.crs,
            )
            ad = aerodromes[["icao", "geometry"]].copy()
            # Area in degrees^2 is not a physical area, but it is a valid
            # relative ordering for picking the smaller of two overlapping
            # boundaries -- projecting to a metric CRS would not change which
            # one is smaller. The warning geopandas raises for this is
            # expected and would otherwise fire on every full build.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                ad["_area"] = ad.geometry.area
            joined = gpd.sjoin(rep_points, ad, predicate="within", how="inner")
            if len(joined):
                # A point landing in more than one polygon (airports do
                # overlap in OSM) goes to the smallest-area polygon, ties
                # broken by icao ascending -- the smaller boundary is the
                # more specific one.
                joined = joined.sort_values(["_area", "icao"])
                joined = joined[~joined.index.duplicated(keep="first")]
                assigned.loc[joined.index] = joined["icao"].to_numpy()

        # icaos whose own polygon actually covered >=1 feature -- not just
        # icaos that merely *have* a polygon. A polygon can be geometrically
        # real but enclose nothing of the airport it names (EFIT: every one
        # of its aeroway features sits ~3 km outside the drawn boundary).
        # Treating "has a polygon" as reason enough to skip the box pass left
        # such an aerodrome with nothing at all, permanently -- worse off
        # than an aerodrome with no polygon, which at least reaches the box.
        polygon_covered_icaos = set(assigned.dropna().unique())
        self._polygon_covered_icaos = polygon_covered_icaos

        # --- Pass 2: runway-extent box, for aerodromes with no polygon OR
        # whose polygon covered nothing, only for features pass 1 left
        # unassigned. ---------------------------------------------------
        box_only = self._boxes[~self._boxes["ident"].isin(polygon_covered_icaos)]
        unassigned = assigned.isna()
        if unassigned.any() and len(box_only):
            sub = feats.loc[unassigned]
            reps = sub.geometry.representative_point()
            lat = reps.y
            lon = reps.x
            b = self._boxes  # nearest-guard compares against every box, not
                              # just the box-only ones, so a polygon aerodrome
                              # whose own boundary missed the point still
                              # keeps a closer neighbour's box from claiming it
            for _, r in box_only.iterrows():
                inside = lat.between(r.lat_min, r.lat_max) & lon.between(
                    r.lon_min, r.lon_max
                )
                if not inside.any():
                    continue
                idx = lat.index[inside]
                d_this = (lat.loc[idx] - r.apt_lat) ** 2 + (
                    (lon.loc[idx] - r.apt_lon) * math.cos(math.radians(r.apt_lat))
                ) ** 2
                nearest = pd.Series(True, index=idx)
                for _, o in b[b["ident"] != r.ident].iterrows():
                    d_other = (lat.loc[idx] - o.apt_lat) ** 2 + (
                        (lon.loc[idx] - o.apt_lon)
                        * math.cos(math.radians(o.apt_lat))
                    ) ** 2
                    nearest &= d_this <= d_other
                claim = idx[nearest.to_numpy()]
                # Never steal a feature pass 1 already gave to a polygon.
                claim = claim[assigned.loc[claim].isna()]
                assigned.loc[claim] = r.ident

        return assigned

    def features_for(self, apt_icao: str) -> gpd.GeoDataFrame:
        """Features assigned to *apt_icao*, by polygon containment or --
        where the aerodrome has no polygon -- the runway-extent box."""
        self._load()
        out = self._features.loc[self._assigned_icao == apt_icao, _OUTPUT_COLUMNS]
        return gpd.GeoDataFrame(out, geometry="geometry", crs=self._features.crs)

    def assignment_report(self) -> pd.DataFrame:
        """Per aerodrome in scope: which method actually produced its
        features, and how many it received. Not a seam contract -- this is
        what Task 6 reads before committing to a full build.

        "polygon" means the aerodrome's own boundary covered >=1 feature.
        An aerodrome whose polygon exists but is empty (EFIT) is reported as
        "bbox" -- that is genuinely where its features (if any) came from --
        not "polygon", which would credit a boundary that did no work.
        """
        self._load()
        counts = self._assigned_icao.value_counts()
        rows = []
        for _, b in self._boxes.iterrows():
            ident = b["ident"]
            if ident in self._polygon_covered_icaos:
                method = "polygon"
            elif pd.notna(b.get("lat_min")):
                method = "bbox"
            else:
                method = "none"
            rows.append({
                "icao": ident, "method": method,
                "n_features": int(counts.get(ident, 0)),
            })
        return pd.DataFrame(rows)
