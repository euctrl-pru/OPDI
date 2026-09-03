"""Tests for opdi.reference.h3_runway_grid.

The pure-geometry tests (``TestRunwayCells``, ``TestStripCells``) use a
synthetic north-south runway so ``1 NM == 1/60 degree`` of latitude exactly,
per this task's brief -- the expected cells are hand-computed from that
approximation rather than derived through ``runway_cells``' own geodesic
implementation, so a bug in the implementation can't cancel out against the
same bug in the test.

The generator tests (``TestRunwayGridGenerator``) use a stub storage (no
Spark catalog / S3 needed) with a 1-2 row ``oa_runways`` fixture, checking the
produced schema, that both zones appear, and that ``strip_id`` is stable.
"""

import math

import h3
import pandas as pd
import pytest

from opdi.config import OPDIConfig
from opdi.reference.h3_runway_grid import (
    APPROACH_ZONE,
    DEFAULT_WIDTH_FT,
    RUNWAY_ZONE,
    NullGeometryError,
    RunwayGridGenerator,
    _strip_cells,
    runway_cells,
)

RES = 12
NM_PER_DEG_LAT = 60.0

# A synthetic 2 NM, north-south runway threshold at (50.0, 4.0). Chosen so
# ``1 NM == 1/60 degree`` of latitude exactly -- see module docstring.
THR_LAT, THR_LON = 50.0, 4.0
FAR_LAT, FAR_LON = 50.0 + 2.0 / NM_PER_DEG_LAT, 4.0
WIDTH_FT = 147.0  # ~44.8 m


def _cell(lat, lon):
    return h3.latlng_to_cell(lat, lon, RES)


# ---------------------------------------------------------------------------
# runway_cells -- exact geometry
# ---------------------------------------------------------------------------


class TestRunwayCells:
    def test_covers_both_zones(self):
        cells = runway_cells(
            THR_LAT, THR_LON, FAR_LAT, FAR_LON, WIDTH_FT, approach_nm=3.0, res=RES
        )
        kinds = {c.zone for c in cells}
        assert kinds == {RUNWAY_ZONE, APPROACH_ZONE}

    def test_midpoint_cell_is_runway(self):
        cells = {
            c.h3_id: c.zone
            for c in runway_cells(THR_LAT, THR_LON, FAR_LAT, FAR_LON, WIDTH_FT, approach_nm=3.0, res=RES)
        }
        mid_lat = THR_LAT + 1.0 / NM_PER_DEG_LAT  # 1 NM along a 2 NM strip
        mid_cell = _cell(mid_lat, THR_LON)
        assert mid_cell in cells
        assert cells[mid_cell] == RUNWAY_ZONE

    def test_one_nm_before_threshold_is_approach(self):
        cells = {
            c.h3_id: c.zone
            for c in runway_cells(THR_LAT, THR_LON, FAR_LAT, FAR_LON, WIDTH_FT, approach_nm=3.0, res=RES)
        }
        # 1 NM south of the threshold, along the reciprocal (away-from-runway)
        # bearing -- inside the 3 NM approach corridor, outside the runway
        # rectangle.
        before_lat = THR_LAT - 1.0 / NM_PER_DEG_LAT
        before_cell = _cell(before_lat, THR_LON)
        assert before_cell in cells
        assert cells[before_cell] == APPROACH_ZONE

    def test_one_nm_to_the_side_is_absent(self):
        cells = {
            c.h3_id
            for c in runway_cells(THR_LAT, THR_LON, FAR_LAT, FAR_LON, WIDTH_FT, approach_nm=3.0, res=RES)
        }
        # 1 NM east of the runway midpoint -- far outside the ~45 m-wide
        # rectangle, and nowhere near the approach corridor (which sits south
        # of the threshold, not abeam the strip).
        mid_lat = THR_LAT + 1.0 / NM_PER_DEG_LAT
        dlon = (1.0 / NM_PER_DEG_LAT) / math.cos(math.radians(mid_lat))
        side_cell = _cell(mid_lat, THR_LON + dlon)
        assert side_cell not in cells

    def test_null_width_defaults_to_documented_constant(self):
        with_none = {
            c.h3_id
            for c in runway_cells(THR_LAT, THR_LON, FAR_LAT, FAR_LON, None, approach_nm=3.0, res=RES)
        }
        with_default = {
            c.h3_id
            for c in runway_cells(
                THR_LAT, THR_LON, FAR_LAT, FAR_LON, DEFAULT_WIDTH_FT, approach_nm=3.0, res=RES
            )
        }
        assert with_none == with_default
        assert len(with_none) > 0

    def test_both_ends_get_their_own_approach(self):
        """Calling with the ends swapped moves the approach corridor to the
        other threshold -- both directions must be built by the caller to
        cover arrivals from either end of a physical runway."""
        from_le = {
            c.h3_id: c.zone
            for c in runway_cells(THR_LAT, THR_LON, FAR_LAT, FAR_LON, WIDTH_FT, approach_nm=3.0, res=RES)
        }
        from_he = {
            c.h3_id: c.zone
            for c in runway_cells(FAR_LAT, FAR_LON, THR_LAT, THR_LON, WIDTH_FT, approach_nm=3.0, res=RES)
        }
        before_thr_cell = _cell(THR_LAT - 1.0 / NM_PER_DEG_LAT, THR_LON)
        beyond_far_cell = _cell(FAR_LAT + 1.0 / NM_PER_DEG_LAT, FAR_LON)

        assert from_le.get(before_thr_cell) == APPROACH_ZONE
        assert before_thr_cell not in from_he

        assert from_he.get(beyond_far_cell) == APPROACH_ZONE
        assert beyond_far_cell not in from_le


# ---------------------------------------------------------------------------
# _strip_cells -- defaults, fallback, null-geometry skip
# ---------------------------------------------------------------------------


def _row(**overrides):
    base = {
        "id": 1,
        "le_ident": "07",
        "he_ident": "25",
        "le_latitude_deg": THR_LAT,
        "le_longitude_deg": THR_LON,
        "he_latitude_deg": FAR_LAT,
        "he_longitude_deg": FAR_LON,
        "le_heading_degT": 0.0,
        "he_heading_degT": 180.0,
        "length_ft": 12000.0,
        "width_ft": WIDTH_FT,
    }
    base.update(overrides)
    return pd.Series(base)


class TestStripCells:
    def test_both_thresholds_present_builds_both_zones(self):
        cells = _strip_cells(_row(), RES)
        assert set(cells.values()) == {RUNWAY_ZONE, APPROACH_ZONE}

    def test_missing_he_position_derives_from_heading_and_length(self):
        row = _row(he_latitude_deg=None, he_longitude_deg=None)
        cells = _strip_cells(row, RES)
        assert len(cells) > 0
        assert RUNWAY_ZONE in cells.values()

    def test_missing_both_positions_and_headings_raises(self):
        row = _row(
            le_latitude_deg=None, le_longitude_deg=None,
            he_latitude_deg=None, he_longitude_deg=None,
            le_heading_degT=None, he_heading_degT=None,
        )
        with pytest.raises(NullGeometryError):
            _strip_cells(row, RES)

    def test_null_length_defaults_when_deriving_missing_endpoint(self):
        row = _row(he_latitude_deg=None, he_longitude_deg=None, length_ft=None)
        cells = _strip_cells(row, RES)  # must not raise
        assert len(cells) > 0


# ---------------------------------------------------------------------------
# RunwayGridGenerator -- schema, both zones, stable strip_id, resumability
# ---------------------------------------------------------------------------


class StubStorage:
    def __init__(self, tables):
        self._tables = tables
        self.written = []

    def table_exists(self, name):
        return name in self._tables

    def read_table(self, name):
        return self._tables[name]

    def write_table(self, df, table_name, mode):
        self.written.append((table_name, mode, df))

    def create_table(self, sql):
        pass


RUNWAY_ROW_SCHEMA = (
    "id int, airport_ident string, le_ident string, le_latitude_deg double, "
    "le_longitude_deg double, le_heading_degT double, he_ident string, "
    "he_latitude_deg double, he_longitude_deg double, he_heading_degT double, "
    "length_ft double, width_ft double"
)


def _airports_df(spark):
    return spark.createDataFrame(
        [("TEST", THR_LAT, THR_LON, "large_airport")],
        "ident string, latitude_deg double, longitude_deg double, type string",
    )


class TestRunwayGridGenerator:
    def test_schema_both_zones_and_stable_strip_id(self, spark, tmp_path):
        runways = spark.createDataFrame(
            [(1, "TEST", "07", THR_LAT, THR_LON, 0.0, "25", FAR_LAT, FAR_LON, 180.0, 12000.0, WIDTH_FT)],
            RUNWAY_ROW_SCHEMA,
        )
        storage = StubStorage({"oa_airports": _airports_df(spark), "oa_runways": runways})
        gen = RunwayGridGenerator(spark, OPDIConfig(), log_dir=str(tmp_path), storage=storage)

        success, failed = gen.save_prepared_to_table()

        assert success == ["TEST"]
        assert failed == []
        assert len(storage.written) == 1
        table_name, mode, sdf = storage.written[0]
        assert table_name == "h3_runway_zones"
        assert mode == "overwrite"  # first (and only) write of a fresh run
        assert sdf.columns == ["h3_id", "apt_icao", "strip_id", "le_ident", "he_ident", "zone"]

        pdf = sdf.toPandas()
        assert set(pdf["zone"]) == {"runway", "approach"}
        assert set(pdf["apt_icao"]) == {"TEST"}
        # One physical runway -> exactly one strip_id, shared by every row.
        assert pdf["strip_id"].nunique() == 1
        assert pdf["strip_id"].iloc[0] == "1"
        assert set(pdf["le_ident"]) == {"07"}
        assert set(pdf["he_ident"]) == {"25"}

    def test_two_runways_get_distinct_strip_ids(self, spark, tmp_path):
        # A second, perpendicular-ish runway at the same airport so its
        # geometry doesn't overlap the first.
        far2_lat = THR_LAT
        far2_lon = THR_LON + 0.05 + 2.0 / NM_PER_DEG_LAT / math.cos(math.radians(THR_LAT))
        runways = spark.createDataFrame(
            [
                (1, "TEST", "07", THR_LAT, THR_LON, 0.0, "25", FAR_LAT, FAR_LON, 180.0, 12000.0, WIDTH_FT),
                (2, "TEST", "09", THR_LAT, THR_LON + 0.05, 90.0, "27", far2_lat, far2_lon, 270.0, 12000.0, WIDTH_FT),
            ],
            RUNWAY_ROW_SCHEMA,
        )
        storage = StubStorage({"oa_airports": _airports_df(spark), "oa_runways": runways})
        gen = RunwayGridGenerator(spark, OPDIConfig(), log_dir=str(tmp_path), storage=storage)

        gen.save_prepared_to_table()

        _, _, sdf = storage.written[0]
        pdf = sdf.toPandas()
        assert set(pdf["strip_id"]) == {"1", "2"}

    def test_resume_skips_previously_successful_airport(self, spark, tmp_path):
        runways = spark.createDataFrame(
            [(1, "TEST", "07", THR_LAT, THR_LON, 0.0, "25", FAR_LAT, FAR_LON, 180.0, 12000.0, WIDTH_FT)],
            RUNWAY_ROW_SCHEMA,
        )
        storage = StubStorage({"oa_airports": _airports_df(spark), "oa_runways": runways})
        cfg = OPDIConfig()

        gen1 = RunwayGridGenerator(spark, cfg, log_dir=str(tmp_path), storage=storage)
        gen1.save_prepared_to_table()
        assert len(storage.written) == 1

        # A second generator sharing the same log_dir sees TEST already
        # recorded successful and must not reprocess (or rewrite) it.
        gen2 = RunwayGridGenerator(spark, cfg, log_dir=str(tmp_path), storage=storage)
        success2, failed2 = gen2.save_prepared_to_table()
        assert success2 == ["TEST"]
        assert failed2 == []
        assert len(storage.written) == 1  # nothing new to process

    def test_all_strips_null_geometry_is_not_a_failure(self, spark, tmp_path):
        runways = spark.createDataFrame(
            [(1, "TEST", "07", None, None, None, "25", None, None, None, None, None)],
            RUNWAY_ROW_SCHEMA,
        )
        storage = StubStorage({"oa_airports": _airports_df(spark), "oa_runways": runways})
        gen = RunwayGridGenerator(spark, OPDIConfig(), log_dir=str(tmp_path), storage=storage)

        success, failed = gen.save_prepared_to_table()

        assert success == ["TEST"]
        assert failed == []
        assert storage.written == []  # nothing buildable -- not an error
