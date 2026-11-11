"""xing-fir: a change of FIR volume along a track, read from the compact H3 reference."""

import json
import random

import h3
from navigation_fixtures import fly, legs_to_samples, make_track, measured
from pyspark.sql import functions as F

from opdi.config import EventConfig
from opdi.pipeline.fir import (
    FIR_VERSION,
    attach_fir,
    calculate_fir_events,
    h3_parent_expr,
    load_fir_reference,
)
from opdi.reference.h3_airspaces import AIRSPACE_SCHEMA

LAT0, LAT1 = 50.0, 52.0


def _ref(spark, volumes, res=7):
    """``volumes = [(code, min_fl, max_fl, area_km2, lon_min, lon_max)]`` between LAT0 and LAT1,
    filled at ``res`` and compacted, stamped with cycle 524."""
    rows = []
    for code, lo, hi, area, x0, x1 in volumes:
        poly = h3.LatLngPoly([(LAT0, x0), (LAT1, x0), (LAT1, x1), (LAT0, x1)])
        for cell in h3.compact_cells(h3.polygon_to_cells(poly, res)):
            rows.append(("FIR", code, code, "524", "2024-11-28", "2024-12-26", lo, hi, float(area),
                         cell, h3.get_resolution(cell)))
    return spark.createDataFrame(rows, AIRSPACE_SCHEMA)


def _with_cells(sdf):
    cell = F.udf(lambda lat, lon: h3.latlng_to_cell(lat, lon, 7) if lat is not None else None)
    return sdf.withColumn("h3_res_7", cell("lat", "lon"))


def _track(spark, legs, **kw):
    return _with_cells(fly(spark, legs, **kw))


TWO = [("AAAA", 0, 999, 1e4, -2.0, 0.0), ("BBBB", 0, 999, 1e4, 0.0, 2.0)]


def test_h3_parent_expr_matches_the_library(spark):
    random.seed(1)
    cells = [h3.latlng_to_cell(random.uniform(-80, 80), random.uniform(-180, 180), 7) for _ in range(20)]
    df = spark.createDataFrame([(c,) for c in cells], "cell string")
    for r in range(0, 8):
        got = {row.cell: row.p for row in df.withColumn("p", h3_parent_expr(F.col("cell"), r)).collect()}
        assert got == {c: h3.cell_to_parent(c, r) for c in cells}, r


def test_a_straight_flight_across_the_boundary_gives_one_crossing(spark):
    # Eastbound at FL330 from 1.5 degrees west of the boundary to 1.5 east.
    sdf = _track(spark, [(1500, 0, 0)], lat0=51.0, lon0=-1.5, hdg0=90.0, alt_ft=33000.0)
    rows = calculate_fir_events(sdf, _ref(spark, TWO), EventConfig()).collect()
    assert len(rows) == 1
    row = rows[0]
    assert row.type == "xing-fir" and row.version == FIR_VERSION == "fir_v0.1.0"
    info = json.loads(row.info)
    assert info["fir_from"] == "AAAA" and info["fir_to"] == "BBBB"
    assert info["crossing_seq"] == 1
    assert info["airac_cfmu"] == "524"
    assert info["airac_validity_start"] == "2024-11-28" and info["airac_validity_end"] == "2024-12-26"
    assert abs(info["fl"] - 330) <= 1
    assert 0 < info["bracket_s"] <= 10
    assert abs(row.lon) < 0.05           # at the boundary
    assert row.cumulative_distance_nm > 0


def test_a_boundary_skim_shorter_than_the_dwell_is_not_a_crossing(spark):
    # Along the boundary on the west side, a 30 s excursion east, back west.
    samples = legs_to_samples([(600, 0, 0)], lat0=50.5, lon0=-0.05, hdg0=0.0)
    for s in samples:
        if 300 <= s["t"] < 330:
            s["lon"] = 0.05
    sdf = _with_cells(measured(make_track(spark, samples)))
    assert calculate_fir_events(sdf, _ref(spark, TWO), EventConfig()).count() == 0


def test_no_crossing_across_a_hole(spark):
    # The first piece ends 6 NM west of the boundary; after 40 min of silence
    # the second starts 7 NM east of it. The change of FIR falls in the hole.
    west = legs_to_samples([(300, 0, 0)], lat0=51.0, lon0=-0.7, hdg0=90.0)
    east = legs_to_samples([(300, 0, 0)], lat0=51.0, lon0=0.2, hdg0=90.0, t0=2700.0)
    sdf = _with_cells(measured(make_track(spark, west + east)))
    assert {r.piece for r in sdf.select("piece").distinct().collect()} == {0, 1}
    rows = calculate_fir_events(sdf, _ref(spark, TWO), EventConfig()).collect()
    assert rows == []


def test_a_climb_through_the_band_boundary_is_a_crossing(spark):
    layered = [("AAAAFIR", 0, 195, 1e4, -2.0, 2.0), ("AAAAUIR", 195, 999, 1e4, -2.0, 2.0)]
    # Climb from FL150 to FL250 at 2,000 ft/min, then level: 300 s of climb.
    sdf = _track(spark, [(300, 0, 2000.0), (300, 0, 0)], lat0=51.0, lon0=-1.0, hdg0=90.0, alt_ft=15000.0)
    rows = calculate_fir_events(sdf, _ref(spark, layered), EventConfig()).collect()
    assert len(rows) == 1
    info = json.loads(rows[0].info)
    assert (info["fir_from"], info["fir_to"]) == ("AAAAFIR", "AAAAUIR")


def test_a_double_match_keeps_the_narrower_band_once(spark):
    overlapping = [("PLAC", 0, 999, 1e6, -2.0, 2.0), ("AAAA", 0, 195, 1e4, -2.0, 2.0)]
    sdf = _track(spark, [(300, 0, 0)], lat0=51.0, lon0=-1.0, hdg0=90.0, alt_ft=10000.0)
    out = attach_fir(sdf, _ref(spark, overlapping), EventConfig())
    assert out.count() == sdf.count()
    assert {r.fir_code for r in out.select("fir_code").distinct().collect()} == {"AAAA"}


def test_a_sample_outside_every_volume_has_no_code(spark):
    sdf = _track(spark, [(60, 0, 0)], lat0=55.0, lon0=-1.0, hdg0=90.0)
    out = attach_fir(sdf, _ref(spark, TWO), EventConfig())
    assert {r.fir_code for r in out.collect()} == {None}


def test_missing_reference_table_skips_the_family(spark):
    sdf = _track(spark, [(60, 0, 0)])
    assert calculate_fir_events(sdf, None, EventConfig()) is None

    class NoTable:
        def table_exists(self, name):
            return False

    assert load_fir_reference(NoTable()) is None

    class OldTable:
        def table_exists(self, name):
            return True

        def read_table(self, name):
            return spark.createDataFrame([("FIR", "AAAA", "x")], "airspace_type string, code string, h3_res_7 string")

    assert load_fir_reference(OldTable()) is None


def test_off_flag_returns_none(spark):
    sdf = _track(spark, [(60, 0, 0)])
    assert calculate_fir_events(sdf, _ref(spark, TWO), EventConfig(emit_fir_crossings=False)) is None


def test_a_volume_stored_at_resolution_zero_is_found(spark):
    # A whole resolution-0 cell is stored as one row: a sample anywhere inside
    # it must find the volume (the lookup walks the ancestors down to 0).
    cell0 = h3.cell_to_parent(h3.latlng_to_cell(51.0, -1.0, 7), 0)
    rows = [("FIR", "ZZZZ", "ZZZZ", "524", "2024-11-28", "2024-12-26", 0, 999, 1e6, cell0, 0)]
    ref = spark.createDataFrame(rows, AIRSPACE_SCHEMA)
    sdf = _track(spark, [(60, 0, 0)], lat0=51.0, lon0=-1.0, hdg0=90.0)
    out = attach_fir(sdf, ref, EventConfig())
    assert {r.fir_code for r in out.collect()} == {"ZZZZ"}


def test_a_crossing_across_a_long_blank_is_still_a_crossing(spark):
    # Altitude masked for 90 s while the aircraft crosses: no volume matches
    # there, yet the flight went from AAAA to BBBB and the event must say so,
    # with bracket_s measured from the last sample that had a volume.
    samples = legs_to_samples([(1200, 0, 0)], lat0=51.0, lon0=-1.2, hdg0=90.0, alt_ft=33000.0)
    for s in samples:
        if 540 <= s["t"] < 630:
            s["baro_altitude"] = None
    sdf = _with_cells(measured(make_track(spark, samples)))
    rows = calculate_fir_events(sdf, _ref(spark, TWO), EventConfig()).collect()
    assert len(rows) == 1
    info = json.loads(rows[0].info)
    assert (info["fir_from"], info["fir_to"]) == ("AAAA", "BBBB")
    # The 20 s of AAAA after the blank keep their code (a short run inherits
    # the last *known* long run, which is AAAA itself), so the bracket is one
    # or two samples, not the whole blank.
    assert info["bracket_s"] <= 10
