"""
H3 airspace encoding (step 00c).

Turns airspace volumes (FIRs worldwide, ANSP areas, country boundaries) from
PRU Atlas into a **compact** H3 reference: each polygon is filled at
``H3Config.airspace_resolution`` and compacted, so the interior is a
few coarse cells and only the boundary ring is stored at full resolution.
``pipeline/fir.py`` joins a sample's resolution-7 cell and its ancestors
against this table to find the volume the sample is in.

What this fixes against the earlier port (``OPDI-live 00_create_h3_airspaces``):

* the table was overwritten once *per polygon*, so only the last one survived;
  a layer is now written once;
* polygons crossing the antimeridian were filled on the wrong side of the
  planet; they are split at 180 degrees first;
* placeholder rows (``XXXX``, the ``EGGX`` "REROUTING EXTENSION") are dropped,
  sub-volumes of one code are dissolved, and touching bands of one code are
  merged, so a crossing between two rows of one FIR is never emitted;
* the FIR source is worldwide (336 volumes in AIRAC 524), so the layer is
  built under worldwide coverage too.

Definition cycle. Only three NM FIR cycles are public (PRU Atlas 406, 481,
524); the newest, **524, valid 2024-11-28 to 2024-12-26**, is used for every
day and is stamped on every row (``airac_cfmu``, ``validity_start``,
``validity_end``), and from there on every ``xing-fir`` event.
"""

from typing import Dict, Iterable, List, Optional, Tuple

import h3
import pandas as pd
import shapely
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from shapely.geometry import MultiPolygon, Polygon, box
from shapely.ops import unary_union

from opdi.config import OPDIConfig
from opdi.utils.h3_helpers import compact_h3_set, polyfill_geojson
from opdi.utils.storage import StorageManager

# Default data source URLs from PRU Atlas
DEFAULT_ANSP_URLS = [
    "https://raw.githubusercontent.com/euctrl-pru/pruatlas/master/inst/extdata/ansps_ace_406.parquet",
    "https://raw.githubusercontent.com/euctrl-pru/pruatlas/master/inst/extdata/ansps_ace_481.parquet",
    "https://raw.githubusercontent.com/euctrl-pru/pruatlas/master/inst/extdata/ansps_ace_524.parquet",
]

DEFAULT_FIR_URLS = [
    "https://raw.githubusercontent.com/euctrl-pru/pruatlas/master/inst/extdata/firs_nm_524.parquet",
]

DEFAULT_COUNTRY_URLS = [
    "https://raw.githubusercontent.com/euctrl-pru/pruatlas/master/inst/extdata/countries50m.parquet",
]

# CFMU AIRAC cycle to validity date mapping
CFMU_AIRAC: Dict[int, List[str]] = {
    406: ["2015-11-12", "2015-12-10"],
    481: ["2021-08-12", "2021-09-09"],
    524: ["2024-11-28", "2024-12-26"],
}

#: Rows of the NM FIR file that are not airspace: a catch-all code and a
#: rerouting construct. Kept in the data would claim cells real FIRs own.
PLACEHOLDER_CODES = ("XXXX",)
PLACEHOLDER_NAME_FRAGMENTS = ("REROUTING EXTENSION",)

AIRSPACE_SCHEMA = StructType([
    StructField("airspace_type", StringType(), True),
    StructField("name", StringType(), True),
    StructField("code", StringType(), True),
    StructField("airac_cfmu", StringType(), True),
    StructField("validity_start", StringType(), True),
    StructField("validity_end", StringType(), True),
    StructField("min_fl", IntegerType(), True),
    StructField("max_fl", IntegerType(), True),
    StructField("area_km2", DoubleType(), True),
    StructField("h3_cell", StringType(), True),
    StructField("h3_res", IntegerType(), True),
])

TABLE = "opdi_h3_airspace_ref"


# ---------------------------------------------------------------------------
# Geometry
# ---------------------------------------------------------------------------

def _crosses_antimeridian(poly: Polygon) -> bool:
    xs = [x for x, *_ in poly.exterior.coords]
    return any(abs(b - a) > 180.0 for a, b in zip(xs, xs[1:]))


def _shift_east(poly: Polygon) -> Polygon:
    """Rewrite a dateline-crossing ring in [0, 360) longitudes."""
    def shift(ring):
        return [((x + 360.0) if x < 0 else x, y) for x, y, *_ in ring.coords]
    return Polygon(shift(poly.exterior), [shift(r) for r in poly.interiors])


def _polygons(geom) -> List[Polygon]:
    """The polygon parts of any geometry, dropping lines and points a cut or a
    union can leave behind (a boundary that touches the cut line is one)."""
    if geom is None or geom.is_empty:
        return []
    if isinstance(geom, Polygon):
        return [geom] if geom.area > 0 else []
    if hasattr(geom, "geoms"):
        return [p for g in geom.geoms for p in _polygons(g)]
    return []


def split_antimeridian(geom):
    """Split every polygon whose ring jumps across 180 degrees into a western
    and an eastern part, each inside [-180, 180]. Other geometry is returned
    as is. ``h3.polygon_to_cells`` fills the complement of such a ring."""
    out = []
    for poly in _polygons(geom):
        if not _crosses_antimeridian(poly):
            out.append(poly)
            continue
        shifted = _shift_east(poly)
        west = shifted.intersection(box(0, -90, 180, 90))
        east = shapely.affinity.translate(shifted.intersection(box(180, -90, 360, 90)), xoff=-360.0)
        out.extend(_polygons(west))
        out.extend(_polygons(east))
    if not out:
        return Polygon()
    return MultiPolygon(out) if len(out) != 1 else out[0]


def compact_cells(geometry_wkt: str, res: int) -> List[Tuple[str, int]]:
    """Fill a WKT (Multi)Polygon at ``res`` and compact: ``(cell, resolution)`` pairs."""
    geom = split_antimeridian(shapely.from_wkt(geometry_wkt))
    cells = polyfill_geojson(shapely.geometry.mapping(geom), res, geo_json_conformant=True)
    compact = compact_h3_set(cells)
    return [(c, h3.get_resolution(c)) for c in sorted(compact)]


def fill_geometry(geometry_wkt: str, res: int = 7) -> list:
    """Full polyfill per sub-polygon of a WKT MultiPolygon (kept for callers
    of the earlier port; the generator itself uses ``compact_cells``)."""
    return [
        polyfill_geojson(shapely.geometry.mapping(x), res, geo_json_conformant=True)
        for x in shapely.from_wkt(geometry_wkt).geoms
    ]


def fill_geometry_compact(geometry_wkt: str, res: int = 7) -> list:
    """Compact polyfill per sub-polygon of a WKT MultiPolygon."""
    return [compact_h3_set(cells) for cells in fill_geometry(geometry_wkt, res)]


def get_coords(h: str) -> Tuple[float, float]:
    """Centre of an H3 cell, ``(0.0, 0.0)`` for an invalid index."""
    try:
        return h3.cell_to_latlng(h)
    except Exception:
        print(f"Hex {h} did not result in coords using h3.cell_to_latlng(hex)")
        return 0.0, 0.0


def _area_km2(geom) -> float:
    """Approximate area from the equal-area projection of the centroid latitude."""
    import math

    lat = geom.centroid.y
    deg_km = 111.32
    return float(geom.area * deg_km * deg_km * math.cos(math.radians(lat)))


# ---------------------------------------------------------------------------
# Volumes
# ---------------------------------------------------------------------------

def dissolve_volumes(df: pd.DataFrame) -> pd.DataFrame:
    """One row per ``(code, FL band)`` with the union geometry and ``area_km2``.

    Drops placeholder rows, unions rows that share ``(code, min_fl, max_fl)``,
    and merges bands of one code that touch or overlap (``150-245`` and
    ``245-999`` become ``150-999``). Bands with a gap between them stay apart.
    """
    df = df.copy()
    keep = ~df["code"].isin(PLACEHOLDER_CODES)
    for frag in PLACEHOLDER_NAME_FRAGMENTS:
        keep &= ~df["name"].fillna("").str.upper().str.contains(frag)
    df = df[keep]
    df["min_fl"] = df["min_fl"].astype(int)
    df["max_fl"] = df["max_fl"].astype(int)
    df["geom"] = df["geometry_wkt"].map(shapely.from_wkt)
    rows = []
    for code, group in df.groupby("code", sort=True):
        bands = sorted(
            ({"min_fl": int(rec["min_fl"]), "max_fl": int(rec["max_fl"]), "geoms": [rec["geom"]],
              "name": rec["name"], "airac_cfmu": rec["airac_cfmu"],
              "airspace_type": rec["airspace_type"]}
             for rec in group.to_dict(orient="records")),
            key=lambda b: (b["min_fl"], b["max_fl"]),
        )
        merged = []
        for band in bands:
            if merged and band["min_fl"] <= merged[-1]["max_fl"]:
                merged[-1]["max_fl"] = max(merged[-1]["max_fl"], band["max_fl"])
                merged[-1]["geoms"].extend(band["geoms"])
            else:
                merged.append(band)
        for band in merged:
            geom = unary_union(band["geoms"])
            rows.append({
                "airspace_type": band["airspace_type"], "name": band["name"], "code": code,
                "airac_cfmu": band["airac_cfmu"], "min_fl": band["min_fl"], "max_fl": band["max_fl"],
                "area_km2": _area_km2(geom), "geometry_wkt": shapely.to_wkt(geom),
            })
    return pd.DataFrame(rows, columns=["airspace_type", "name", "code", "airac_cfmu", "min_fl",
                                       "max_fl", "area_km2", "geometry_wkt"])


def fill_volumes(volumes: pd.DataFrame, res: int) -> pd.DataFrame:
    """One row per compact cell of every volume: the volume columns plus
    ``h3_cell`` and ``h3_res``."""
    out = []
    for r in volumes.itertuples(index=False):
        for cell, cell_res in compact_cells(r.geometry_wkt, res):
            out.append({
                "airspace_type": r.airspace_type, "name": r.name, "code": r.code,
                "airac_cfmu": r.airac_cfmu, "min_fl": r.min_fl, "max_fl": r.max_fl,
                "area_km2": r.area_km2, "h3_cell": cell, "h3_res": cell_res,
            })
    return pd.DataFrame(out, columns=["airspace_type", "name", "code", "airac_cfmu", "min_fl",
                                      "max_fl", "area_km2", "h3_cell", "h3_res"])


def overlap_report(cells: pd.DataFrame) -> pd.DataFrame:
    """Pairs of different codes that claim the same full-resolution cell with
    overlapping FL bands, with how many such cells: ``code_a, code_b, cells``.

    Computed at the finest resolution present by uncompacting, so a coarse
    interior cell of one volume is compared against the fine boundary cells
    of another.
    """
    if cells.empty:
        return pd.DataFrame(columns=["code_a", "code_b", "cells"])
    res = int(cells["h3_res"].max())
    rows = []
    for (code, lo, hi), g in cells.groupby(["code", "min_fl", "max_fl"]):
        for c in h3.uncompact_cells(list(g["h3_cell"]), res):
            rows.append((c, code, int(lo), int(hi)))
    full = pd.DataFrame(rows, columns=["cell", "code", "lo", "hi"])
    joined = full.merge(full, on="cell", suffixes=("_a", "_b"))
    joined = joined[(joined["code_a"] < joined["code_b"])
                    & (joined["lo_a"] < joined["hi_b"]) & (joined["lo_b"] < joined["hi_a"])]
    report = joined.groupby(["code_a", "code_b"]).size().reset_index(name="cells")
    return report.sort_values("cells", ascending=False).reset_index(drop=True)


def _with_validity(volumes: pd.DataFrame, airspace_type: str,
                   cfmu_airac: Optional[Dict[int, List[str]]] = None) -> pd.DataFrame:
    cfmu_airac = cfmu_airac or CFMU_AIRAC
    df = volumes.copy()
    df["airspace_type"] = airspace_type
    if airspace_type == "COUNTRY":
        df["airac_cfmu"] = "-1"
        df["validity_start"] = "1900-01-01"
        df["validity_end"] = "2100-01-01"
    else:
        df["validity_start"] = df["airac_cfmu"].map(lambda x: cfmu_airac[int(x)][0])
        df["validity_end"] = df["airac_cfmu"].map(lambda x: cfmu_airac[int(x)][1])
        df["airac_cfmu"] = df["airac_cfmu"].map(lambda x: str(int(x)))
    return df


def _country_frame(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename({"admin": "name", "iso_a3": "code"}, axis=1).copy()
    df["min_fl"] = 0
    df["max_fl"] = 999
    df["airac_cfmu"] = -1
    df["airspace_type"] = "COUNTRY"
    return df


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------

class AirspaceH3Generator:
    """Builds ``opdi_h3_airspace_ref``: one layer per airspace type, each
    written once, partitioned by ``airspace_type`` so the layers do not
    clobber each other."""

    def __init__(self, spark: SparkSession, config: OPDIConfig, storage=None):
        self.spark = spark
        self.config = config
        self.storage = storage or StorageManager(spark, config)
        self.project = config.project.project_name
        self.res = int(config.h3.airspace_resolution)

    # -- one layer ---------------------------------------------------------

    def _write_layer(self, cells: pd.DataFrame, airspace_type: str) -> None:
        cols = [f.name for f in AIRSPACE_SCHEMA.fields]
        records = [tuple(r[c] for c in cols) for r in cells[cols].to_dict(orient="records")]
        spark_df = self.spark.createDataFrame(records, AIRSPACE_SCHEMA)
        spark_df = (
            spark_df.withColumn("validity_start", F.col("validity_start").cast(TimestampType()))
            .withColumn("validity_end", F.col("validity_end").cast(TimestampType()))
            .filter(F.col("airspace_type") == airspace_type)
        )
        print(f"  {airspace_type}: {len(cells)} compact cells from {cells['code'].nunique()} volumes")
        self.storage.write_table(
            spark_df, TABLE, mode="overwrite", partition_by=["airspace_type"],
            partition_values=[{"airspace_type": airspace_type}],
        )

    def _process_layer(self, frames: Iterable[pd.DataFrame], airspace_type: str,
                       res: Optional[int] = None, report_overlaps: bool = False) -> pd.DataFrame:
        res = res or self.res
        volumes = pd.concat([dissolve_volumes(f) for f in frames], ignore_index=True)
        volumes = _with_validity(volumes, airspace_type)
        cells = fill_volumes(volumes, res)
        for c in ("validity_start", "validity_end"):
            cells[c] = cells["code"].map(dict(zip(volumes["code"], volumes[c])))
        if report_overlaps:
            report = overlap_report(cells)
            if len(report):
                print(f"  {airspace_type}: {len(report)} code pairs claim the same cell "
                      f"with overlapping bands ({int(report['cells'].sum())} cells):")
                print(report.head(20).to_string(index=False))
            else:
                print(f"  {airspace_type}: no overlapping volumes")
        self._write_layer(cells, airspace_type)
        return cells

    # -- the three layers --------------------------------------------------

    def process_fir(self, urls: Optional[List[str]] = None,
                    frames: Optional[List[pd.DataFrame]] = None, res: Optional[int] = None) -> pd.DataFrame:
        frames = frames or [pd.read_parquet(u) for u in (urls or DEFAULT_FIR_URLS)]
        print(f"Processing FIR layer ({len(frames)} source file(s))")
        return self._process_layer(frames, "FIR", res=res, report_overlaps=True)

    def process_ansp(self, urls: Optional[List[str]] = None,
                     frames: Optional[List[pd.DataFrame]] = None, res: Optional[int] = None) -> pd.DataFrame:
        frames = frames or [pd.read_parquet(u) for u in (urls or DEFAULT_ANSP_URLS)]
        print(f"Processing ANSP layer ({len(frames)} source file(s))")
        return self._process_layer(frames, "ANSP", res=res)

    def process_countries(self, urls: Optional[List[str]] = None,
                          frames: Optional[List[pd.DataFrame]] = None, res: Optional[int] = None) -> pd.DataFrame:
        frames = frames or [_country_frame(pd.read_parquet(u)) for u in (urls or DEFAULT_COUNTRY_URLS)]
        print(f"Processing COUNTRY layer ({len(frames)} source file(s))")
        return self._process_layer(frames, "COUNTRY", res=res)

    def process_all(self, ansp_urls=None, fir_urls=None, country_urls=None) -> None:
        self.process_fir(fir_urls)
        self.process_ansp(ansp_urls)
        self.process_countries(country_urls)

    def create_table_if_not_exists(self) -> None:
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.project}`.`{TABLE}` (
            airspace_type STRING COMMENT 'Airspace type (ANSP, FIR, COUNTRY)',
            name STRING COMMENT 'Airspace name',
            code STRING COMMENT 'Airspace code',
            airac_cfmu STRING COMMENT 'CFMU AIRAC cycle the polygon was exported in',
            validity_start TIMESTAMP COMMENT 'Start of that cycle',
            validity_end TIMESTAMP COMMENT 'End of that cycle',
            min_fl INT COMMENT 'Lower flight level (inclusive)',
            max_fl INT COMMENT 'Upper flight level (exclusive)',
            area_km2 DOUBLE COMMENT 'Polygon area, used to rank overlapping volumes',
            h3_cell STRING COMMENT 'Compact H3 cell',
            h3_res INT COMMENT 'Resolution of that cell'
        )
        USING iceberg
        PARTITIONED BY (airspace_type)
        COMMENT 'Compact H3 airspace reference (FIR worldwide, ANSP, country).'
        """
        self.storage.create_table(create_sql)
        print(f"Table {self.project}.{TABLE} created/verified.")
