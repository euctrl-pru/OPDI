"""Step 00c: a compact, worldwide, correct H3 airspace reference."""

import h3
import pandas as pd
import pytest
import shapely
from shapely.geometry import Polygon, box

from opdi.reference import h3_airspaces as ha


def _row(code, name, min_fl, max_fl, geom, airac=524):
    return {"airac_cfmu": airac, "icao": code[:2], "id": code, "code": code, "name": name,
            "min_fl": min_fl, "max_fl": max_fl, "airspace_type": "FIR",
            "geometry_wkt": shapely.to_wkt(geom)}


def test_an_antimeridian_polygon_fills_the_right_side():
    # A box from 170E across the dateline to 170W, written the way the NM
    # polygons are: longitudes jump from +170 to -170 inside one ring.
    ring = Polygon([(170, -5), (180, -5), (-180, -5), (-170, -5), (-170, 5), (-180, 5), (180, 5), (170, 5)])
    cells = ha.compact_cells(shapely.to_wkt(ring), 4)
    assert cells
    for cell, res in cells:
        lat, lon = h3.cell_to_latlng(cell)
        assert abs(lon) >= 169.0, (cell, lon)
        assert res <= 4


def test_split_antimeridian_drops_the_line_a_cut_can_leave():
    # A polygon with a spike that touches the dateline only along a line: the
    # eastern intersection is a LineString, which must not reach MultiPolygon.
    from shapely.geometry import GeometryCollection, LineString

    g = GeometryCollection([box(170, -5, 179, 5), LineString([(179, -5), (179, 5)])])
    out = ha.split_antimeridian(g)
    assert out.geom_type == "Polygon" and out.area > 0
    ring = Polygon([(170, -5), (180, -5), (-180, -5), (-170, -5), (-170, 5), (-180, 5), (180, 5), (170, 5),
                    (170, 0), (170, -5)])   # a vertex repeated on the western edge
    assert ha.split_antimeridian(ring).geom_type == "MultiPolygon"


def test_split_antimeridian_leaves_an_ordinary_polygon_alone():
    g = box(0, 50, 2, 52)
    assert ha.split_antimeridian(g).equals(g)


def test_placeholder_rows_are_dropped():
    df = pd.DataFrame([
        _row("XXXX", "ANYWHERE", 0, 999, box(0, 0, 1, 1)),
        _row("EGGX", "REROUTING EXTENSION", 0, 999, box(0, 0, 1, 1)),
        _row("EGGXFIR", "SHANWICK FIR", 0, 999, box(0, 0, 1, 1)),
    ])
    out = ha.dissolve_volumes(df)
    assert list(out["code"]) == ["EGGXFIR"]


def test_same_code_bands_are_merged():
    df = pd.DataFrame([
        _row("OBBBUIR", "BAHRAIN UIR", 150, 245, box(50, 25, 51, 26)),
        _row("OBBBUIR", "BAHRAIN UIR", 245, 999, box(50, 25, 51, 26)),
    ])
    out = ha.dissolve_volumes(df)
    assert len(out) == 1
    assert (int(out.iloc[0]["min_fl"]), int(out.iloc[0]["max_fl"])) == (150, 999)


def test_sub_volumes_with_one_code_dissolve():
    df = pd.DataFrame([
        _row("CZQXFIR", "GANDER NAT RVSM", 0, 999, box(-50, 45, -45, 50)),
        _row("CZQXFIR", "GANDER FIR", 0, 999, box(-45, 45, -40, 50)),
    ])
    out = ha.dissolve_volumes(df)
    assert len(out) == 1
    geom = shapely.from_wkt(out.iloc[0]["geometry_wkt"])
    assert geom.bounds == pytest.approx((-50, 45, -40, 50))
    assert out.iloc[0]["area_km2"] > 0


def test_separate_bands_of_one_code_stay_separate():
    df = pd.DataFrame([
        _row("AAAAFIR", "A LOWER", 0, 195, box(0, 50, 2, 52)),
        _row("AAAAFIR", "A UPPER", 245, 999, box(0, 50, 2, 52)),
    ])
    assert len(ha.dissolve_volumes(df)) == 2


def test_compact_cells_carry_their_resolution():
    cells = ha.compact_cells(shapely.to_wkt(box(0, 50, 2, 52)), 7)
    resolutions = {r for _, r in cells}
    assert 7 in resolutions and min(resolutions) < 7
    full = set(h3.uncompact_cells([c for c, _ in cells], 7))
    expected = h3.polygon_to_cells(h3.LatLngPoly([(50, 0), (52, 0), (52, 2), (50, 2)]), 7)
    assert full == set(expected)
    assert all(h3.get_resolution(c) == r for c, r in cells)


def test_overlap_report_names_the_pairs():
    df = pd.DataFrame([
        _row("PLACFIR", "PLACEHOLDER", 0, 999, box(0, 50, 1, 51)),
        _row("AAAAFIR", "A", 0, 195, box(0, 50, 1, 51)),
        _row("BBBBFIR", "B", 195, 999, box(0, 50, 1, 51)),
    ])
    volumes = ha.dissolve_volumes(df)
    cells = ha.fill_volumes(volumes, 5)
    report = ha.overlap_report(cells)
    pairs = set(map(tuple, report[["code_a", "code_b"]].values))
    assert pairs == {("AAAAFIR", "PLACFIR"), ("BBBBFIR", "PLACFIR")}
    assert (report["cells"] > 0).all()


class _StubStorage:
    def __init__(self, existing=None):
        self.writes = []
        self.existing = existing

    def table_exists(self, name):
        return self.existing is not None

    def read_table(self, name):
        return self.existing

    def write_table(self, df, table_name, mode="append", partition_by=None, partition_values=None):
        self.writes.append((table_name, mode, partition_by, df))

    def create_table(self, sql):
        pass


def test_the_layer_is_written_once(spark):
    from opdi.config import OPDIConfig

    gen = ha.AirspaceH3Generator(spark, OPDIConfig(), storage=_StubStorage())
    df = pd.DataFrame([
        _row("AAAAFIR", "A", 0, 195, box(0, 50, 1, 51)),
        _row("BBBBFIR", "B", 0, 195, box(1, 50, 2, 51)),
    ])
    gen.process_fir(frames=[df], res=5)
    fir_writes = [w for w in gen.storage.writes if w[0] == "opdi_h3_airspace_ref"]
    assert len(fir_writes) == 1
    _, mode, partition_by, written = fir_writes[0]
    assert mode == "overwrite" and list(partition_by) == ["airspace_type"]
    rows = written.collect()
    assert {r.code for r in rows} == {"AAAAFIR", "BBBBFIR"}
    assert set(written.columns) >= {"airspace_type", "code", "name", "airac_cfmu", "validity_start",
                                    "validity_end", "min_fl", "max_fl", "area_km2", "h3_cell", "h3_res"}
    assert all(r.airac_cfmu == "524" for r in rows)
    assert all(str(r.validity_start)[:10] == "2024-11-28" for r in rows)


def test_worldwide_runs_00c():
    from opdi.config import OPDIConfig
    from opdi.runner import reference_substeps_for_coverage

    world = reference_substeps_for_coverage(OPDIConfig.for_environment("opensky", worldwide=True))
    assert world["00c"] is True


def test_the_old_per_cell_table_is_refused_not_overwritten(spark):
    from opdi.config import OPDIConfig

    old = spark.createDataFrame([("FIR", "AAAA", "x", 50.0, 4.0)],
                                "airspace_type string, code string, h3_res_7 string, h3_res_7_lat double, h3_res_7_lon double")
    gen = ha.AirspaceH3Generator(spark, OPDIConfig(), storage=_StubStorage(existing=old))
    df = pd.DataFrame([_row("AAAAFIR", "A", 0, 195, box(0, 50, 1, 51))])
    with pytest.raises(RuntimeError, match="pre-2026-11"):
        gen.process_fir(frames=[df], res=5)
