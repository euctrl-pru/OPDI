"""Offline, exact-value tests for the h3 v4 port of the remaining direct h3
usage: ``reference/h3_airport_zones.py`` (step 00a), ``reference/h3_airspaces.py``
(step 00c), and ``pipeline/tracks.py``'s H3-index column expression.

Also settles the ``h3_pyspark`` compatibility question the plan flagged as
open. Investigation (recorded in the task report): ``h3_pyspark`` 0.0.9 wraps
h3 *v3* names internally (``h3.geo_to_h3``, ``h3.h3_to_geo``, ``h3.polyfill``,
...). None of those exist on the installed h3 4.5.0 -- calling
``h3_pyspark.geo_to_h3`` inside a real local Spark session raises
``AttributeError: module 'h3' has no attribute 'geo_to_h3'`` at task
execution time (confirmed empirically, not just via ``hasattr``). So
``h3_pyspark`` is BROKEN on v4 and every call site was replaced with a plain
Spark UDF wrapping native h3 v4 -- mirroring what h3_pyspark itself did
(UDF-based), per the plan's guidance to avoid ``applyInPandas``/``pandas_udf``.
"""

import json

import h3
import pytest
from conftest import make_track

from opdi.config import OPDIConfig
from opdi.pipeline.tracks import TrackProcessor
from opdi.reference import h3_airport_zones as Z
from opdi.reference import h3_airspaces as A


# ---------------------------------------------------------------------------
# h3_pyspark is broken on h3 4.5.0 -- pin the failure mode down so a future
# h3_pyspark upgrade that silently fixes this doesn't leave a dead assumption
# in the codebase unnoticed.
# ---------------------------------------------------------------------------


def test_h3_pyspark_geo_to_h3_is_broken_on_h3_v4(spark):
    """Documents *why* h3_pyspark was dropped in favour of native UDFs.

    If this ever starts passing, h3_pyspark has been upgraded to support h3
    v4 and the native-UDF replacements in tracks.py / h3_airport_zones.py
    could be reconsidered (not required to change).
    """
    import h3_pyspark
    from pyspark.sql import functions as F

    df = spark.createDataFrame([(50.9014, 4.4844)], ["lat", "lon"])
    with pytest.raises(Exception) as excinfo:
        df.withColumn(
            "h3", h3_pyspark.geo_to_h3(F.col("lat"), F.col("lon"), F.lit(9))
        ).collect()
    assert "geo_to_h3" in str(excinfo.value)


# ---------------------------------------------------------------------------
# The worker sys.path shadowing constraint that justifies why
# h3_airport_zones._polyfill_geojson_udf (and every other UDF in that module)
# is self-contained and never closes over an opdi.utils.h3_helpers symbol.
# This is CASE (a): the failure reproduces under the local `spark` fixture
# (master="local[1]") -- it is not a cluster-only concern. Regression-tested
# directly rather than left as an unverified docstring claim.
# ---------------------------------------------------------------------------


def test_udf_closing_over_opdi_symbol_fails_on_worker_local_spark(spark):
    """A Spark UDF whose closure references a symbol imported from
    ``opdi.utils.h3_helpers`` fails on the worker, even under the local
    ``spark`` fixture used by this whole suite (``master("local[1]")``).

    Root cause: this worktree has a top-level ``opdi.py`` script (the CLI
    entrypoint) that shadows the ``src/opdi`` package. The driver resolves
    ``opdi`` correctly because pytest's ``pythonpath = ["src"]`` (see
    pyproject.toml) prepends ``src/`` to *its* sys.path -- but a Spark
    worker is a separate Python subprocess that does not inherit that
    prepend (only PYSPARK_PYTHON is pinned; PYTHONPATH is not exported with
    ``src/`` in it -- see conftest.py). cloudpickle pickles a UDF's
    referenced globals *by reference* when they are (or resolve to) another
    named function or an imported symbol, which requires the worker to
    re-import that symbol's home module to reconstruct it. On this worker,
    that re-import finds ``opdi.py`` first and fails.

    This is why ``h3_airport_zones.py``'s UDFs (including
    ``_polyfill_geojson_udf``) are all self-contained around the
    third-party ``h3`` package only, instead of delegating to
    ``h3_helpers.polyfill_geojson`` as ``h3_airspaces.py`` does driver-side.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType

    from opdi.utils.h3_helpers import h3_distance as _opdi_h3_distance

    @F.udf(returnType=IntegerType())
    def _dist_udf(h1, h2):
        return _opdi_h3_distance(h1, h2)

    cell = h3.latlng_to_cell(50.9014, 4.4844, 9)
    neighbor = next(iter(h3.grid_ring(cell, 1)))
    df = spark.createDataFrame([(cell, neighbor)], ["h1", "h2"])

    with pytest.raises(Exception) as excinfo:
        df.withColumn("d", _dist_udf(F.col("h1"), F.col("h2"))).collect()

    msg = str(excinfo.value)
    assert "ModuleNotFoundError" in msg
    assert "opdi" in msg and "not a package" in msg


# ---------------------------------------------------------------------------
# tracks.py: the H3-index column expression (formerly h3_pyspark.geo_to_h3)
# ---------------------------------------------------------------------------


def _proc(spark, tmp_path):
    cfg = OPDIConfig()
    return TrackProcessor(spark, cfg, log_file_path=str(tmp_path / "log.parquet"))


def test_tracks_h3_index_matches_native_latlng_to_cell(spark, tmp_path):
    """The h3_res_7 / h3_res_12 columns tracks.py builds must be exactly the
    cells h3.latlng_to_cell produces for the same point -- cell identity is
    version-independent, so a correct port reproduces published tables
    byte-for-cell.
    """
    proc = _proc(spark, tmp_path)
    lat, lon = 50.9014, 4.4844  # EBBR
    df = make_track(spark, [{"t": 0, "lat": lat, "lon": lon}])

    out = proc._add_h3_encoding(df)
    row = out.collect()[0]

    assert row["h3_res_7"] == h3.latlng_to_cell(lat, lon, 7)
    assert row["h3_res_12"] == h3.latlng_to_cell(lat, lon, 12)


def test_tracks_h3_index_null_lat_lon_yields_null_cell(spark, tmp_path):
    """Matches h3_pyspark's ``handle_nulls`` contract: a null coordinate
    produces a null cell rather than raising.
    """
    proc = _proc(spark, tmp_path)
    df = make_track(spark, [{"t": 0, "lat": None, "lon": None}])

    out = proc._add_h3_encoding(df)
    row = out.collect()[0]

    assert row["h3_res_7"] is None
    assert row["h3_res_12"] is None


# ---------------------------------------------------------------------------
# h3_airport_zones.py (step 00a)
# ---------------------------------------------------------------------------


def test_hex_lat_lon_udfs_match_v4_cell_to_latlng(spark):
    cell = h3.latlng_to_cell(50.9014, 4.4844, 9)
    df = spark.createDataFrame([(cell,)], ["h"])
    out = df.withColumn("lat", Z._hex_lat_udf("h")).withColumn(
        "lon", Z._hex_lon_udf("h")
    ).collect()[0]

    exp_lat, exp_lon = h3.cell_to_latlng(cell)
    assert out["lat"] == pytest.approx(exp_lat, abs=1e-5)
    assert out["lon"] == pytest.approx(exp_lon, abs=1e-5)


def test_geo_to_h3_udf_matches_v4_latlng_to_cell(spark):
    df = spark.createDataFrame([(50.9014, 4.4844, 9)], ["lat", "lon", "res"])
    got = df.withColumn("h", Z._geo_to_h3_udf("lat", "lon", "res")).collect()[0]["h"]
    assert got == h3.latlng_to_cell(50.9014, 4.4844, 9)


def test_h3_distance_udf_matches_v4_grid_distance(spark):
    cell = h3.latlng_to_cell(50.9014, 4.4844, 9)
    neighbor = next(iter(h3.grid_ring(cell, 1)))
    df = spark.createDataFrame([(cell, neighbor)], ["h1", "h2"])
    got = df.withColumn("d", Z._h3_distance_udf("h1", "h2")).collect()[0]["d"]
    assert got == h3.grid_distance(cell, neighbor)


#: A ~0.2deg box around EBBR, GeoJSON-conformant [lon, lat] ring -- matching
#: what generate_circle_polygon produces (json.dumps of [lon, lat] points).
_ZONE_POLY = {
    "type": "Polygon",
    "coordinates": [[
        [4.4, 50.8], [4.5, 50.8], [4.5, 50.9], [4.4, 50.9], [4.4, 50.8],
    ]],
}


def test_polyfill_geojson_udf_matches_h3_helpers_polyfill_geojson(spark):
    """The replacement for h3_pyspark.polyfill(col, res, geo_json_conformant=True):
    parses the GeoJSON string column and delegates to h3_helpers.polyfill_geojson,
    the same primitive used by h3_airport_layouts and h3_airspaces (this is the
    third module to need polyfill-from-geojson, so it is consolidated there
    rather than re-implemented a third time).
    """
    from opdi.utils.h3_helpers import polyfill_geojson

    geojson_str = json.dumps(_ZONE_POLY)
    df = spark.createDataFrame([(geojson_str, 7)], ["poly", "res"])
    got = df.withColumn(
        "cells", Z._polyfill_geojson_udf("poly", "res")
    ).collect()[0]["cells"]

    expected = polyfill_geojson(_ZONE_POLY, 7, geo_json_conformant=True)
    assert set(got) == expected
    assert len(got) > 0


#: NOTE: AirportDetectionZoneGenerator.generate() itself is NOT exercised
#: end-to-end here. Its ``self._circle_udf = udf(generate_circle_polygon,
#: StringType())`` wraps a *named* top-level function from this module by
#: reference; cloudpickle needs to re-import ``opdi.reference.h3_airport_zones``
#: to reconstruct it on the worker, which fails under local pytest because
#: this worktree's top-level ``opdi.py`` shadows the ``src/opdi`` package on
#: a Spark worker's sys.path (workers don't inherit the driver's pytest
#: `pythonpath = ["src"]` prepend). This is PRE-EXISTING and unrelated to h3
#: v3->v4 or h3_pyspark -- confirmed by reproducing the same
#: ModuleNotFoundError with ``generate_circle_polygon`` wrapped exactly as
#: the class already did, on the pre-port code path. It only ever worked on
#: the cluster, where `opdi` is pip-installed in the image (no shadowing
#: opdi.py). Out of scope for this task; the unit tests above cover every
#: piece ``generate()`` calls (`_polyfill_geojson_udf`, and the plain-Python
#: ring math in `generate_circle_polygon`, which has no h3 dependency at all).


# ---------------------------------------------------------------------------
# h3_airspaces.py (step 00c)
# ---------------------------------------------------------------------------


#: A small square MultiPolygon WKT around EBBR (lon lat order, as WKT is).
_AIRSPACE_WKT = (
    "MULTIPOLYGON (((4.4 50.8, 4.5 50.8, 4.5 50.9, 4.4 50.9, 4.4 50.8)))"
)


def test_fill_geometry_matches_h3_helpers_polyfill_geojson():
    import shapely.wkt
    from opdi.utils.h3_helpers import polyfill_geojson

    got = A.fill_geometry(_AIRSPACE_WKT, res=7)
    assert len(got) == 1
    assert isinstance(got[0], set)
    assert len(got[0]) > 0

    geom = shapely.wkt.loads(_AIRSPACE_WKT).geoms.__iter__().__next__()
    import shapely as sh
    expected = polyfill_geojson(sh.geometry.mapping(geom), 7, geo_json_conformant=True)
    assert got[0] == expected


def test_fill_geometry_compact_is_smaller_or_equal_and_covers_same_area():
    full = A.fill_geometry(_AIRSPACE_WKT, res=8)[0]
    compact = A.fill_geometry_compact(_AIRSPACE_WKT, res=8)[0]
    assert len(compact) <= len(full)
    assert len(compact) > 0
    # Uncompacting must reproduce the full cell set exactly.
    from opdi.utils.h3_helpers import uncompact_h3_set
    assert uncompact_h3_set(compact, 8) == full


def test_get_coords_matches_v4_cell_to_latlng():
    cell = h3.latlng_to_cell(50.9014, 4.4844, 7)
    lat, lon = A.get_coords(cell)
    exp_lat, exp_lon = h3.cell_to_latlng(cell)
    assert (lat, lon) == (exp_lat, exp_lon)


def test_get_coords_handles_invalid_hex_gracefully():
    lat, lon = A.get_coords("not-a-real-hex")
    assert (lat, lon) == (0.0, 0.0)
