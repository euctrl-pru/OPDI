"""
H3 runway + approach-corridor grid, from ``oa_runways``.

Universal runway coverage: rasterises *every* large+medium airport's runway
rectangle and approach corridor to H3 resolution-12 cells, purely from
OurAirports' ``oa_runways`` positions. This is the pruning grid the runway
detector (``pipeline/runway_ops.py``, task 5 of this plan) joins state-vector
samples against, replacing the curated 20-airport ``hexaero_airport_layouts``
dependency for that family -- ``hexaero_airport_layouts`` needs an OSM fetch
per airport and was only ever built for 5 of the 20 study airports; this is
geometry-only, no network, and covers all ~1,353 large+medium airports in the
OPDI bbox in one pass.

Two zones per physical runway ("strip"):

* ``"runway"`` -- the rectangle between the two thresholds, width from
  ``width_ft`` (default 45 m, matching
  ``h3_airport_layouts.DEFAULT_AEROWAY_WIDTHS["runway"]``).
* ``"approach"`` -- a trapezoid extending ``APPROACH_NM`` outward from EACH
  threshold along the reciprocal bearing (away from the runway), widening
  from the runway's own half-width at the threshold to
  ``APPROACH_FAR_HALF_WIDTH_NM`` at the far end. Both ends get one, since
  arrivals can use either direction of a runway.

Driver-side, not a Spark UDF (load-bearing): this worktree's top-level
``opdi.py`` shadows ``src/opdi`` on Spark *workers'* sys.path, so a UDF
closing over any ``opdi.*`` symbol fails there with ``ModuleNotFoundError``
(reproduced in ``tests/test_h3_zones_v4.py::
test_udf_closing_over_opdi_symbol_fails_on_worker_local_spark``). The
generator sidesteps this the way ``h3_airport_layouts.AirportLayoutGenerator``
does: iterate airports/runways in plain Python/pandas on the driver, and only
hand Spark a already-built list of rows via ``spark.createDataFrame``. Every
function in this module that touches geometry (``runway_cells``, the
``opdi.utils.geospatial`` calls it makes) runs on the driver only.
"""

import os
import traceback
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

import h3
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from opdi.config import OPDIConfig
from opdi.utils.geospatial import calculate_bearing, destination_point
from opdi.utils.storage import StorageManager

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: How far back from each threshold, along the reciprocal bearing, the
#: approach corridor extends -- roughly a short final.
APPROACH_NM = 3.0

#: Half-width of the approach corridor at its far end (``APPROACH_NM`` out).
#: The near end tapers down to the runway's own half-width, so the corridor
#: is a trapezoid rather than a uniform-width strip.
APPROACH_FAR_HALF_WIDTH_NM = 0.5

#: Runway width fallback when ``oa_runways.width_ft`` is null. 45 m, the same
#: default ``h3_airport_layouts.DEFAULT_AEROWAY_WIDTHS["runway"]`` uses for a
#: runway aeroway with no OSM-tagged width -- documented here rather than
#: imported so this module has no dependency on that one.
DEFAULT_WIDTH_FT = 147.64

#: Runway length fallback, used ONLY when one threshold's coordinates are
#: missing and the far end must be derived from the other end's heading and
#: this length (see ``_endpoint_or_derived``). A generic long-haul runway.
DEFAULT_LENGTH_FT = 8000.0

#: Exact: 1 NM = 1852 m = 1852 / 0.3048 ft.
FT_PER_NM = 1852.0 / 0.3048

RUNWAY_ZONE = "runway"
APPROACH_ZONE = "approach"

TABLE_NAME = "h3_runway_zones"

RUNWAY_GRID_SCHEMA = StructType([
    StructField("h3_id", StringType(), True),
    StructField("apt_icao", StringType(), True),
    StructField("strip_id", StringType(), True),
    StructField("le_ident", StringType(), True),
    StructField("he_ident", StringType(), True),
    StructField("zone", StringType(), True),
])


class NullGeometryError(ValueError):
    """Raised when a runway strip has no derivable threshold geometry.

    Callers catch this per-strip and skip with a logged reason rather than
    letting one bad row abort the whole airport.
    """


@dataclass(frozen=True)
class RunwayCell:
    """One rasterised cell, tagged with which geometry produced it."""

    h3_id: str
    zone: str


# ---------------------------------------------------------------------------
# Pure geometry -- the part covered by the exact-geometry tests
# ---------------------------------------------------------------------------


def _is_missing(x) -> bool:
    """True for None/NaN/NaT -- the shapes a value from a ``.toPandas()``
    column (float64 with nulls, or a plain Python None from a dict row) can
    take. ``pd.isna`` handles all of them; it only raises for array-likes,
    which never reach here (every caller passes a scalar)."""
    try:
        return bool(pd.isna(x))
    except (TypeError, ValueError):
        return x is None


def _polyfill_ring(ring: List[Tuple[float, float]], res: int) -> Set[str]:
    """[lat, lng] ring -> H3 cells.

    THE FOOTGUN (see the plan's Global Constraints and
    ``h3_airport_layouts._polyfill_latlng``): h3 v4's ``h3.LatLngPoly`` always
    takes ``[lat, lng]`` tuples, never GeoJSON's ``[lng, lat]``.
    ``destination_point`` already returns ``(lat, lon)``, so every ring built
    in this module is in the right order by construction -- there is no
    swap step here because there is nothing to swap.
    """
    poly = h3.LatLngPoly(list(ring))
    return set(h3.polygon_to_cells(poly, res))


def runway_cells(
    thr_lat: float,
    thr_lon: float,
    far_lat: float,
    far_lon: float,
    width_ft: Optional[float] = None,
    approach_nm: float = APPROACH_NM,
    res: int = 12,
) -> List[RunwayCell]:
    """Rasterise one runway direction: its rectangle plus its own-end
    approach corridor.

    ``thr``/``far`` are the two physical threshold positions -- *this*
    direction's threshold and the opposite one. The rectangle is symmetric in
    the two ends, so call this twice (swapping ``thr``/``far``) to get the
    approach corridor at BOTH ends of one physical runway; the two rectangle
    results come out identical and the caller de-duplicates.

    Geometry is great-circle throughout (``opdi.utils.geospatial.
    calculate_bearing``/``destination_point``), not a flat 1 NM = 1/60 degree
    approximation. The tests hand-verify against that flat approximation only
    because they use a north-south runway, for which it is exact.

    Args:
        thr_lat, thr_lon: This direction's threshold.
        far_lat, far_lon: The opposite threshold.
        width_ft: Runway width in feet. ``None``/``NaN`` -> ``DEFAULT_WIDTH_FT``.
        approach_nm: Approach corridor length, back from ``thr``.
        res: H3 resolution.

    Returns:
        One ``RunwayCell`` per covered cell, deduplicated, "runway" cells
        taking precedence over "approach" where the two geometries overlap
        (a cell right at the threshold can fall in both).
    """
    if _is_missing(width_ft):
        width_ft = DEFAULT_WIDTH_FT
    half_width_nm = (float(width_ft) / 2.0) / FT_PER_NM

    rwy_bearing = calculate_bearing(thr_lat, thr_lon, far_lat, far_lon)
    left_bearing = (rwy_bearing - 90.0) % 360.0
    right_bearing = (rwy_bearing + 90.0) % 360.0
    reciprocal_bearing = (rwy_bearing + 180.0) % 360.0

    thr_left = destination_point(thr_lat, thr_lon, left_bearing, half_width_nm)
    thr_right = destination_point(thr_lat, thr_lon, right_bearing, half_width_nm)
    far_left = destination_point(far_lat, far_lon, left_bearing, half_width_nm)
    far_right = destination_point(far_lat, far_lon, right_bearing, half_width_nm)

    rectangle_ring = [thr_left, thr_right, far_right, far_left]
    rectangle_cells = _polyfill_ring(rectangle_ring, res)

    approach_far_lat, approach_far_lon = destination_point(
        thr_lat, thr_lon, reciprocal_bearing, approach_nm
    )
    approach_far_left = destination_point(
        approach_far_lat, approach_far_lon, left_bearing, APPROACH_FAR_HALF_WIDTH_NM
    )
    approach_far_right = destination_point(
        approach_far_lat, approach_far_lon, right_bearing, APPROACH_FAR_HALF_WIDTH_NM
    )

    approach_ring = [thr_left, thr_right, approach_far_right, approach_far_left]
    approach_cells = _polyfill_ring(approach_ring, res)

    cells: Dict[str, str] = {}
    for c in rectangle_cells:
        cells[c] = RUNWAY_ZONE
    for c in approach_cells:
        cells.setdefault(c, APPROACH_ZONE)

    return [RunwayCell(h3_id=cid, zone=zone) for cid, zone in cells.items()]


def _endpoint_or_derived(
    lat,
    lon,
    other_lat,
    other_lon,
    heading_from_other_deg,
    length_ft,
) -> Optional[Tuple[float, float]]:
    """Resolve one threshold's position.

    OurAirports almost always carries both thresholds' coordinates directly
    (verified for every study airport), so the common path is just "return
    ``(lat, lon)``". The fallback -- deriving a missing threshold from the
    OTHER threshold's position, a heading FROM that other threshold TOWARD
    this one, and a length (``length_ft``, defaulting to
    ``DEFAULT_LENGTH_FT`` when that too is null) -- exists for the rarer rows
    elsewhere in the full 1,353-airport set where only one end is recorded.

    Returns ``None`` when neither this end's own coordinates nor a derivable
    heading + the other end's coordinates are available -- the strip is then
    unbuildable and the caller raises ``NullGeometryError``.
    """
    if not (_is_missing(lat) or _is_missing(lon)):
        return (float(lat), float(lon))
    if _is_missing(heading_from_other_deg) or _is_missing(other_lat) or _is_missing(other_lon):
        return None
    length = DEFAULT_LENGTH_FT if _is_missing(length_ft) else float(length_ft)
    length_nm = length / FT_PER_NM
    return destination_point(
        float(other_lat), float(other_lon), float(heading_from_other_deg), length_nm
    )


def _strip_cells(row, resolution: int) -> Dict[str, str]:
    """Build the merged {h3_id: zone} map for one ``oa_runways`` row (one
    physical runway, both directions).

    Calls ``runway_cells`` twice -- once per direction -- so both thresholds
    get their own approach corridor; the two calls' rectangles are identical
    and merge away. "runway" wins over "approach" on any overlapping cell,
    since on-strip is the stronger claim.

    Raises ``NullGeometryError`` (caught by the caller, per strip) when
    neither threshold's geometry is derivable at all.
    """
    le_pos = _endpoint_or_derived(
        row.get("le_latitude_deg"), row.get("le_longitude_deg"),
        row.get("he_latitude_deg"), row.get("he_longitude_deg"),
        row.get("he_heading_degT"), row.get("length_ft"),
    )
    he_pos = _endpoint_or_derived(
        row.get("he_latitude_deg"), row.get("he_longitude_deg"),
        row.get("le_latitude_deg"), row.get("le_longitude_deg"),
        row.get("le_heading_degT"), row.get("length_ft"),
    )
    if le_pos is None or he_pos is None:
        raise NullGeometryError(
            "no derivable threshold geometry: "
            f"le=({row.get('le_latitude_deg')}, {row.get('le_longitude_deg')}), "
            f"he=({row.get('he_latitude_deg')}, {row.get('he_longitude_deg')}), "
            f"headings=({row.get('le_heading_degT')}, {row.get('he_heading_degT')})"
        )

    width_ft = row.get("width_ft")
    from_le = runway_cells(le_pos[0], le_pos[1], he_pos[0], he_pos[1], width_ft, res=resolution)
    from_he = runway_cells(he_pos[0], he_pos[1], le_pos[0], le_pos[1], width_ft, res=resolution)

    merged: Dict[str, str] = {}
    for c in from_le + from_he:
        if c.zone == RUNWAY_ZONE:
            merged[c.h3_id] = RUNWAY_ZONE
    for c in from_le + from_he:
        if c.zone != RUNWAY_ZONE:
            merged.setdefault(c.h3_id, c.zone)
    return merged


def _strip_id(row, apt_icao: str) -> str:
    """A stable id for one physical runway, shared by both directions.

    ``oa_runways.id`` is OurAirports' own surrogate key -- already unique per
    physical-runway row across the whole dataset -- so it is used directly
    when present. The ident-pair fallback only fires if ``id`` itself is
    missing, which should not happen for a real OurAirports extract but keeps
    this from raising on a hand-built test fixture that omits it.
    """
    rid = row.get("id")
    if not _is_missing(rid):
        return str(int(rid))
    return f"{apt_icao}:{row.get('le_ident')}-{row.get('he_ident')}"


# ---------------------------------------------------------------------------
# Generator -- modeled on h3_airport_layouts.AirportLayoutGenerator's shape
# ---------------------------------------------------------------------------


class RunwayGridGenerator:
    """Builds ``h3_runway_zones`` for every large+medium airport in the OPDI
    bbox, from ``oa_runways`` (already-ingested, no network fetch).

    Shape mirrors ``h3_airport_layouts.AirportLayoutGenerator``: a bbox
    filter to large+medium airports, per-airport processing with success/
    failed progress logs so a run is resumable, and a
    ``create_table_if_not_exists``/entry-point pair a runner step calls.

    Unlike ``AirportLayoutGenerator.process_airport`` -- which writes with
    ``mode="overwrite"`` on every single airport, which in S3 mode replaces
    the *entire* table's contents with just that one airport's rows each
    time -- this generator writes the FIRST successful airport of a run with
    ``overwrite`` (establishing a clean table) and every airport after that
    with ``append``, so a full run's table genuinely accumulates every
    airport rather than ending up with only the last one's rows. On a
    resumed run (a success log already on disk) even the first write is an
    ``append``, since the table from the earlier run is assumed still there.

    Args:
        spark: Active SparkSession.
        config: OPDI configuration object.
        resolution: H3 resolution (default: ``config.h3.airport_layout_resolution``,
            12 -- the same grid resolution ``hexaero_airport_layouts`` uses,
            so the two are directly comparable).
        log_dir: Directory for progress tracking files.
        storage: Optional pre-built ``StorageManager``-shaped object (tests
            inject a stub here instead of the default, which needs a real
            Spark catalog or S3 path).

    Example:
        >>> generator = RunwayGridGenerator(spark, config)
        >>> generator.create_table_if_not_exists()
        >>> success, failed = generator.save_prepared_to_table()
    """

    # OPDI state vector coverage bounding box -- identical to
    # AirportLayoutGenerator's, so the two steps cover the same airports.
    BBOX_OFFSET = 3  # degrees
    LAT_MIN = 26.74617
    LAT_MAX = 70.25976
    LON_MIN = -25.86653
    LON_MAX = 49.65699

    def __init__(
        self,
        spark: SparkSession,
        config: OPDIConfig,
        resolution: Optional[int] = None,
        log_dir: str = "OPDI_live/logs",
        storage=None,
    ):
        self.spark = spark
        self.config = config
        self.storage = storage if storage is not None else StorageManager(spark, config)
        self.project = config.project.project_name
        self.resolution = resolution or config.h3.airport_layout_resolution
        self.log_dir = log_dir
        self.table_name = TABLE_NAME

        self._success_log = os.path.join(log_dir, "00f_runway_grid_progress_success.parquet")
        self._failed_log = os.path.join(log_dir, "00f_runway_grid_progress_failed.parquet")

        os.makedirs(log_dir, exist_ok=True)

    # -- progress log, same shape as AirportLayoutGenerator ----------------

    def _load_processed_airports(self) -> List[str]:
        if os.path.isfile(self._success_log):
            return pd.read_parquet(self._success_log).apt.to_list()
        return []

    def _mark_success(self, apt_icao: str, processed: List[str]) -> None:
        processed.append(apt_icao)
        pd.DataFrame({"apt": processed}).to_parquet(self._success_log)

    def _mark_failed(self, apt_icao: str, error: str, failed: List, errors: List) -> None:
        failed.append(apt_icao)
        errors.append(str(error))
        pd.DataFrame({"apt": failed, "error": errors}).to_parquet(self._failed_log)

    # -- airport / runway sourcing, no network ------------------------------

    def fetch_airport_list(self, airport_types: Optional[List[str]] = None) -> pd.DataFrame:
        """Large+medium airports within the OPDI bbox.

        Unlike ``AirportLayoutGenerator.fetch_airport_list``, this reads the
        already-ingested ``oa_airports`` table (step 00d, which runs before
        this step) instead of the public OurAirports CSV -- this step is
        geometry-only and must not add a network dependency, and the CSV is
        not reachable from the OSN cluster anyway (see ``_step_00a_airport_zones``
        in ``runner.py`` for the same reasoning).
        """
        if airport_types is None:
            airport_types = ["large_airport", "medium_airport"]

        if not self.storage.table_exists("oa_airports"):
            print("  oa_airports table not found; cannot build the runway grid.")
            return pd.DataFrame(columns=["ident", "latitude_deg", "longitude_deg", "type"])

        apt = self.storage.read_table("oa_airports").select(
            "ident", "latitude_deg", "longitude_deg", "type"
        )
        offset = self.BBOX_OFFSET
        apt = apt.filter(
            F.col("type").isin(airport_types)
            & F.col("latitude_deg").between(self.LAT_MIN - offset, self.LAT_MAX + offset)
            & F.col("longitude_deg").between(self.LON_MIN - offset, self.LON_MAX + offset)
        )
        airports_df = apt.toPandas()
        print(f"There are {len(airports_df)} airports to process (within OPDI bbox)...")
        return airports_df

    def fetch_runways(self, apt_idents: Optional[List[str]]) -> pd.DataFrame:
        """All ``oa_runways`` rows for the given airport idents."""
        if not self.storage.table_exists("oa_runways"):
            print("  oa_runways table not found; cannot build the runway grid.")
            return pd.DataFrame()
        rwy = self.storage.read_table("oa_runways")
        if apt_idents is not None:
            rwy = rwy.filter(F.col("airport_ident").isin(list(apt_idents)))
        return rwy.toPandas()

    # -- per-airport processing ---------------------------------------------

    def process_airport(self, apt_icao: str, rwy_rows: pd.DataFrame) -> pd.DataFrame:
        """Build the grid rows for one airport's runway strips.

        Returns an (possibly empty) DataFrame -- empty is a legitimate result
        (every strip at this airport was null-geometry, each individually
        logged and skipped) and is NOT a failure; only an exception escaping
        the whole airport counts as one.
        """
        records = []
        for _, row in rwy_rows.iterrows():
            strip_id = _strip_id(row, apt_icao)
            try:
                cells = _strip_cells(row, self.resolution)
            except NullGeometryError as e:
                print(f"  Skipping runway {apt_icao} {row.get('le_ident')}/"
                      f"{row.get('he_ident')} (strip {strip_id}): {e}")
                continue
            for h3_id, zone in cells.items():
                records.append({
                    "h3_id": h3_id,
                    "apt_icao": apt_icao,
                    "strip_id": strip_id,
                    "le_ident": row.get("le_ident"),
                    "he_ident": row.get("he_ident"),
                    "zone": zone,
                })
        return pd.DataFrame.from_records(
            records, columns=["h3_id", "apt_icao", "strip_id", "le_ident", "he_ident", "zone"]
        )

    def process_all(
        self,
        airport_types: Optional[List[str]] = None,
        troublesome_airports: Optional[List[str]] = None,
    ) -> Tuple[List[str], List[str]]:
        """Process every large+medium airport in the bbox, skipping any
        already recorded as successful in the progress log.

        Returns:
            Tuple of (successful_airports, failed_airports) lists.
        """
        if troublesome_airports is None:
            troublesome_airports = []

        airports_df = self.fetch_airport_list(airport_types)
        processed_success = self._load_processed_airports()
        processed_failed: List[str] = []
        processed_errors: List[str] = []

        # First write of a fresh run establishes the table; a resumed run
        # (a success log already present) treats the table as already
        # holding those airports' rows and only appends from here.
        first_write = not processed_success

        idents = airports_df["ident"].tolist() if not airports_df.empty else []
        rwy_all = self.fetch_runways(idents) if idents else pd.DataFrame()

        for apt_icao in idents:
            if apt_icao in troublesome_airports:
                print(f"Airport {apt_icao} is troublesome - Skipping...")
                continue
            if apt_icao in processed_success:
                print(f"Airport {apt_icao} is already processed - Skipping...")
                continue

            print(f"Processing {apt_icao}...")
            rwy_rows = (
                rwy_all[rwy_all["airport_ident"] == apt_icao]
                if not rwy_all.empty else pd.DataFrame()
            )
            try:
                pdf = self.process_airport(apt_icao, rwy_rows)
            except Exception as e:
                print(f"Failed to process {apt_icao}. Error: {e}")
                print(traceback.format_exc())
                self._mark_failed(apt_icao, str(e), processed_failed, processed_errors)
                continue

            if not pdf.empty:
                sdf = self.spark.createDataFrame(pdf.to_dict(orient="records"), RUNWAY_GRID_SCHEMA)
                mode = "overwrite" if first_write else "append"
                self.storage.write_table(sdf, self.table_name, mode=mode)
                first_write = False

            self._mark_success(apt_icao, processed_success)

        return processed_success, processed_failed

    def save_prepared_to_table(
        self,
        airport_types: Optional[List[str]] = None,
        troublesome_airports: Optional[List[str]] = None,
    ) -> Tuple[List[str], List[str]]:
        """Entry point a runner step calls -- naming mirrors
        ``AirportDetectionZoneGenerator.save_prepared_to_table`` (step 00a);
        internally it is the resumable per-airport loop modeled on
        ``AirportLayoutGenerator.process_all`` (step 00b).
        """
        return self.process_all(
            airport_types=airport_types, troublesome_airports=troublesome_airports
        )

    def create_table_if_not_exists(self) -> None:
        """Create the ``h3_runway_zones`` Iceberg table if it doesn't exist.
        No-op in S3 mode (``StorageManager.create_table``)."""
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.project}`.`{self.table_name}` (
            h3_id STRING COMMENT 'H3 cell id at resolution 12',
            apt_icao STRING COMMENT 'Airport ident (oa_runways.airport_ident)',
            strip_id STRING COMMENT 'Stable id for one physical runway; both directions share it',
            le_ident STRING COMMENT 'Low-end runway designator',
            he_ident STRING COMMENT 'High-end runway designator',
            zone STRING COMMENT '"runway" (on-strip rectangle) or "approach" (corridor before a threshold)'
        )
        USING iceberg
        COMMENT 'Universal H3 runway + approach-corridor grid, rasterised from oa_runways for every large+medium airport in the OPDI bbox. Replaces hexaero_airport_layouts as the pruning grid for the runway detection family.'
        """
        self.storage.create_table(create_sql)
        print(f"Table {self.project}.{self.table_name} created/verified.")
