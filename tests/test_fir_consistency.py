"""The exact-polygon check behind the FIR consistency figures."""

import fir_consistency as fc
import pandas as pd
import shapely
from shapely.geometry import box

T0 = pd.Timestamp("2026-06-14 10:00:00")


def _volumes():
    return pd.DataFrame([
        {"code": "AAAA", "min_fl": 0, "max_fl": 999, "area_km2": 1e4, "geometry_wkt": shapely.to_wkt(box(-2, 50, 0, 52))},
        {"code": "BBBB", "min_fl": 0, "max_fl": 999, "area_km2": 1e4, "geometry_wkt": shapely.to_wkt(box(0, 50, 2, 52))},
        {"code": "PLAC", "min_fl": 0, "max_fl": 999, "area_km2": 1e6, "geometry_wkt": shapely.to_wkt(box(-2, 50, 2, 52))},
    ])


def test_exact_membership_resolves_overlaps_like_the_pipeline():
    s = pd.DataFrame({"lat": [51.0, 51.0, 55.0], "lon": [-1.0, 1.0, 1.0], "fl": [330.0, 330.0, 330.0]})
    assert list(fc.exact_membership(s, _volumes())) == ["AAAA", "BBBB", None]


def test_crossing_times_and_timing_errors():
    f = pd.DataFrame({
        "track_id": ["t"] * 4,
        "event_time": [T0 + pd.Timedelta(seconds=5 * i) for i in range(4)],
        "fir_code": ["AAAA", "AAAA", "BBBB", "BBBB"],
        "exact_code": ["AAAA", "AAAA", "AAAA", "BBBB"],
    })
    h3_x = fc.crossing_times(f, "fir_code")
    exact_x = fc.crossing_times(f, "exact_code")
    assert len(h3_x) == 1 and len(exact_x) == 1
    errors = fc.timing_errors(h3_x, exact_x)
    assert list(errors) == [5.0]
    s = fc.summarise(f, h3_x, exact_x, errors)
    assert s["agree_pct"] == 75.0 and s["matched_crossings"] == 1 and s["timing_error_max_s"] == 5.0
