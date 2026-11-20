"""Step 00c: a compact, worldwide, correct H3 airspace reference."""

import h3
import pandas as pd
import pytest
import shapely
from shapely.geometry import Polygon, box

from opdi.reference import h3_airspaces as ha


def _row(code, name, min_fl, max_fl, geom, airac=524):
    return {
        "airac_cfmu": airac,
        "icao": code[:2],
        "id": code,
        "code": code,
        "name": name,
        "min_fl": min_fl,
        "max_fl": max_fl,
        "airspace_type": "FIR",
        "geometry_wkt": shapely.to_wkt(geom),
    }


def test_an_antimeridian_polygon_fills_the_right_side():
    # A box from 170E across the dateline to 170W, written the way the NM
    # polygons are: longitudes jump from +170 to -170 inside one ring.
    ring = Polygon(
        [(170, -5), (180, -5), (-180, -5), (-170, -5), (-170, 5), (-180, 5), (180, 5), (170, 5)]
    )
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
    ring = Polygon(
        [
            (170, -5),
            (180, -5),
            (-180, -5),
            (-170, -5),
            (-170, 5),
            (-180, 5),
            (180, 5),
            (170, 5),
            (170, 0),
            (170, -5),
        ]
    )  # a vertex repeated on the western edge
    assert ha.split_antimeridian(ring).geom_type == "MultiPolygon"


def test_split_antimeridian_leaves_an_ordinary_polygon_alone():
    g = box(0, 50, 2, 52)
    assert ha.split_antimeridian(g).equals(g)


def test_placeholder_rows_are_dropped():
    df = pd.DataFrame(
        [
            _row("XXXX", "ANYWHERE", 0, 999, box(0, 0, 1, 1)),
            _row("EGGX", "REROUTING EXTENSION", 0, 999, box(0, 0, 1, 1)),
            _row("EGGXFIR", "SHANWICK FIR", 0, 999, box(0, 0, 1, 1)),
        ]
    )
    out = ha.dissolve_volumes(df)
    assert list(out["code"]) == ["EGGXFIR"]


def test_same_code_bands_are_merged():
    df = pd.DataFrame(
        [
            _row("OBBBUIR", "BAHRAIN UIR", 150, 245, box(50, 25, 51, 26)),
            _row("OBBBUIR", "BAHRAIN UIR", 245, 999, box(50, 25, 51, 26)),
        ]
    )
    out = ha.dissolve_volumes(df)
    assert len(out) == 1
    assert (int(out.iloc[0]["min_fl"]), int(out.iloc[0]["max_fl"])) == (150, 999)


def test_sub_volumes_with_one_code_dissolve():
    df = pd.DataFrame(
        [
            _row("CZQXFIR", "GANDER NAT RVSM", 0, 999, box(-50, 45, -45, 50)),
            _row("CZQXFIR", "GANDER FIR", 0, 999, box(-45, 45, -40, 50)),
        ]
    )
    out = ha.dissolve_volumes(df)
    assert len(out) == 1
    geom = shapely.from_wkt(out.iloc[0]["geometry_wkt"])
    assert geom.bounds == pytest.approx((-50, 45, -40, 50))
    assert out.iloc[0]["area_km2"] > 0


def test_separate_bands_of_one_code_stay_separate():
    df = pd.DataFrame(
        [
            _row("AAAAFIR", "A LOWER", 0, 195, box(0, 50, 2, 52)),
            _row("AAAAFIR", "A UPPER", 245, 999, box(0, 50, 2, 52)),
        ]
    )
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
    df = pd.DataFrame(
        [
            _row("PLACFIR", "PLACEHOLDER", 0, 999, box(0, 50, 1, 51)),
            _row("AAAAFIR", "A", 0, 195, box(0, 50, 1, 51)),
            _row("BBBBFIR", "B", 195, 999, box(0, 50, 1, 51)),
        ]
    )
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
    df = pd.DataFrame(
        [
            _row("AAAAFIR", "A", 0, 195, box(0, 50, 1, 51)),
            _row("BBBBFIR", "B", 0, 195, box(1, 50, 2, 51)),
        ]
    )
    gen.process_fir(frames=[df], res=5)
    fir_writes = [w for w in gen.storage.writes if w[0] == "opdi_h3_airspace_ref"]
    assert len(fir_writes) == 1
    _, mode, partition_by, written = fir_writes[0]
    assert mode == "overwrite" and list(partition_by) == ["airspace_type"]
    rows = written.collect()
    assert {r.code for r in rows} == {"AAAAFIR", "BBBBFIR"}
    assert set(written.columns) >= {
        "airspace_type",
        "code",
        "name",
        "airac_cfmu",
        "validity_start",
        "validity_end",
        "min_fl",
        "max_fl",
        "area_km2",
        "h3_cell",
        "h3_res",
    }
    assert all(r.airac_cfmu == "524" for r in rows)
    assert all(str(r.validity_start)[:10] == "2024-11-28" for r in rows)


def test_worldwide_runs_00c():
    from opdi.config import OPDIConfig
    from opdi.runner import reference_substeps_for_coverage

    world = reference_substeps_for_coverage(OPDIConfig.for_environment("opensky", worldwide=True))
    assert world["00c"] is True


def test_the_old_per_cell_table_is_refused_not_overwritten(spark):
    from opdi.config import OPDIConfig

    old = spark.createDataFrame(
        [("FIR", "AAAA", "x", 50.0, 4.0)],
        "airspace_type string, code string, h3_res_7 string, h3_res_7_lat double, "
        "h3_res_7_lon double",
    )
    gen = ha.AirspaceH3Generator(spark, OPDIConfig(), storage=_StubStorage(existing=old))
    df = pd.DataFrame([_row("AAAAFIR", "A", 0, 195, box(0, 50, 1, 51))])
    with pytest.raises(RuntimeError, match="pre-2026-11"):
        gen.process_fir(frames=[df], res=5)


# --- the overlap report against a reference that fills every volume ------------


def _overlap_report_by_filling(cells: pd.DataFrame) -> pd.DataFrame:
    """Pairs of codes that claim the same cell at overlapping flight levels.

    Columns ``code_a``, ``code_b`` and ``cells`` (the number of shared cells
    at full resolution).

    Computed at the finest resolution present by uncompacting every volume to
    integer cell arrays, so a coarse interior cell of one volume is compared
    against the fine boundary cells of another. Only cells claimed more than
    once are looked at further.
    """
    import h3
    import numpy as np
    from h3.api import numpy_int as h3n

    if cells.empty:
        return pd.DataFrame(columns=["code_a", "code_b", "cells"])
    res = int(cells["h3_res"].max())
    volumes = []
    arrays = []
    for i, ((code, lo, hi), g) in enumerate(cells.groupby(["code", "min_fl", "max_fl"])):
        ints = np.array([h3.str_to_int(c) for c in g["h3_cell"]], dtype=np.uint64)
        full = h3n.uncompact_cells(ints, res)
        arrays.append(np.stack([full, np.full(len(full), i, dtype=np.uint64)], axis=1))
        volumes.append((code, int(lo), int(hi)))
    both = np.concatenate(arrays)
    cell_ids, counts = np.unique(both[:, 0], return_counts=True)
    shared = cell_ids[counts > 1]
    if len(shared) == 0:
        return pd.DataFrame(columns=["code_a", "code_b", "cells"])
    hit = both[np.isin(both[:, 0], shared)]
    # pandas cannot factorise unsigned 64-bit keys; the same bits as signed do.
    df = pd.DataFrame({"cell": hit[:, 0].view(np.int64), "vol": hit[:, 1].astype(int)})
    joined = df.merge(df, on="cell", suffixes=("_a", "_b"))
    joined = joined[joined["vol_a"] < joined["vol_b"]]
    rows = []
    for (va, vb), g in joined.groupby(["vol_a", "vol_b"]):
        code_a, lo_a, hi_a = volumes[va]
        code_b, lo_b, hi_b = volumes[vb]
        if code_a == code_b or not (lo_a < hi_b and lo_b < hi_a):
            continue
        a, b = sorted((code_a, code_b))
        rows.append((a, b, int(len(g))))
    report = pd.DataFrame(rows, columns=["code_a", "code_b", "cells"])
    report = report.groupby(["code_a", "code_b"], as_index=False)["cells"].sum()
    return report.sort_values("cells", ascending=False).reset_index(drop=True)


def test_overlap_report_counts_as_filling_every_volume_would():
    """Volumes that overlap partly, wholly, at different sizes and not at all."""
    df = pd.DataFrame(
        [
            _row("AAAAFIR", "A", 0, 999, box(0, 50, 3, 52)),
            _row("BBBBFIR", "B", 0, 999, box(2, 51, 6, 54)),  # partly over A
            _row("CCCCFIR", "C", 0, 195, box(0.5, 50.5, 1, 51)),  # inside A
            _row("DDDDFIR", "D", 195, 999, box(0.5, 50.5, 1, 51)),  # same ground as C, above it
            _row("EEEEFIR", "E", 0, 999, box(20, 20, 22, 22)),  # far away
            _row("FFFFFIR", "F", 0, 999, box(-10, 40, 12, 60)),  # holds A, B, C and D
        ]
    )
    cells = ha.fill_volumes(ha.dissolve_volumes(df), 6)
    assert cells["h3_res"].nunique() > 1, "the volumes must compact, or nothing is tested"

    got = ha.overlap_report(cells)
    want = _overlap_report_by_filling(cells)

    assert len(want) >= 6
    key = ["code_a", "code_b"]
    pd.testing.assert_frame_equal(
        got.sort_values(key).reset_index(drop=True),
        want.sort_values(key).reset_index(drop=True),
        check_dtype=False,
    )


def test_overlap_report_of_volumes_that_do_not_meet_is_empty():
    df = pd.DataFrame(
        [
            _row("AAAAFIR", "A", 0, 999, box(0, 50, 1, 51)),
            _row("EEEEFIR", "E", 0, 999, box(20, 20, 21, 21)),
        ]
    )
    cells = ha.fill_volumes(ha.dissolve_volumes(df), 5)
    assert len(ha.overlap_report(cells)) == 0


# --- a polygon around a pole ---------------------------------------------------


def _polar_cap(edge_lat=-80.0):
    """A cap round the South Pole, drawn as map data draws Antarctica."""
    from shapely.geometry import Polygon

    coast = [(lon, edge_lat) for lon in range(-180, 181, 10)]
    return Polygon(coast + [(180, -90), (-180, -90)])


def test_a_polygon_round_a_pole_is_filled():
    """h3 fails on the whole cap; cut into sectors it fills, pole included."""
    import h3

    cells = ha.compact_cells(_polar_cap().wkt, 4)
    assert cells, "the cap gave no cells"
    held = {c for c, _ in cells}

    def inside(lat, lon):
        cell = h3.latlng_to_cell(lat, lon, 4)
        return any(h3.cell_to_parent(cell, r) in held for r in range(5))

    assert inside(-89.9, 0.0)
    assert inside(-85.0, 100.0) and inside(-85.0, -100.0) and inside(-85.0, 179.0)
    assert not inside(-70.0, 100.0)


def test_other_polygons_are_not_cut():
    geom = box(0, 50, 1, 51)
    assert ha.split_polar(geom).equals(geom)


def test_a_polygon_h3_cannot_fill_is_halved(monkeypatch):
    """A fill that fails for a wide polygon is done on its halves instead."""
    from h3.api import numpy_int as h3n

    real = h3n.polygon_to_cells
    widths = []

    def picky(shape, res):
        lons = [lon for _, lon in shape.outer]
        widths.append(max(lons) - min(lons))
        if widths[-1] > 1.5:
            raise RuntimeError("too wide for this h3")
        return real(shape, res)

    monkeypatch.setattr(h3n, "polygon_to_cells", picky)
    whole = box(0, 50, 2, 51)
    got = {c for c, _ in ha.compact_cells(whole.wkt, 5)}
    monkeypatch.setattr(h3n, "polygon_to_cells", real)
    want = {c for c, _ in ha.compact_cells(whole.wkt, 5)}

    assert max(widths) > 1.5 and len(widths) == 3
    assert got == want
