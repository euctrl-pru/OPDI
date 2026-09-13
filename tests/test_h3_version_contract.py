"""The executors' h3 must be the h3 the code is written against.

`docker/Dockerfile` pinned `h3<4` while `pyproject.toml` asked only for
`h3>=3.7.0`, so the driver resolved 4.5.0 and the executors stayed on 3.7.7.
Every h3 name this codebase uses exists only in v4, so the first executor-side
call raised AttributeError.

It failed silently, which is what made it expensive: the UDFs caught the
exception and returned NULL, `explode` drops a NULL array, and step 00a
committed an empty `h3_airport_detection_zones` with a `_SUCCESS` marker. The
pipeline had no detection grid and would have reported no ADEP or ADES for any
flight -- a coverage result rather than a broken build.
"""
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

#: Names the codebase calls that exist only in h3 v4.
V4_ONLY = (
    "polygon_to_cells", "LatLngPoly", "latlng_to_cell",
    "grid_distance", "cell_to_latlng", "is_valid_cell",
)


def test_the_image_is_not_pinned_below_the_api_the_code_uses():
    """A `h3<4` pin here is the whole defect, expressed in one line."""
    text = (ROOT / "docker" / "Dockerfile").read_text()
    assert '"h3<4"' not in text, (
        "docker/Dockerfile pins h3 below 4, but the code calls the v4 API. "
        "Executors will raise AttributeError on the first h3 call."
    )
    assert re.search(r'"h3>=4', text), "the image must ask for h3 v4 explicitly"


def test_the_driver_has_the_api_the_code_calls():
    import h3

    missing = [n for n in V4_ONLY if not hasattr(h3, n)]
    assert not missing, f"h3 {h3.__version__} lacks {missing}"


def test_the_raised_error_names_the_cause_and_the_fix():
    """A bare AttributeError on a worker says nothing about why. The message
    has to name the version and the image, or the next person debugs the
    geometry instead of the container."""
    import inspect

    from opdi.reference import h3_airport_zones as z

    src = inspect.getsource(z._polyfill_geojson_udf.func)
    assert "h3 v4 API" in src
    assert "Dockerfile" in src
    assert "k8s_container_image" in src


@pytest.mark.parametrize("udf_name", [
    "_geo_to_h3_udf", "_h3_distance_udf", "_polyfill_geojson_udf",
])
def test_no_udf_swallows_an_attribute_error(udf_name):
    """Each of the three used `except Exception: return None`."""
    import inspect

    from opdi.reference import h3_airport_zones as z

    src = inspect.getsource(getattr(z, udf_name).func)
    assert "except AttributeError" in src, (
        f"{udf_name} must re-raise an h3 API mismatch rather than nulling it"
    )
