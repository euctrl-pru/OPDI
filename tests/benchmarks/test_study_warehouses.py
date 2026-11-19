"""Every study script names the warehouse it reads and writes.

``OPDIConfig.for_environment("opensky")`` takes the warehouse from the
``OPDI_WAREHOUSE`` environment variable, which on the cluster points at the
production warehouse. A study script must therefore name its warehouse
itself: as the ``warehouse=`` argument, or by setting
``project.warehouse_path`` right after.
"""

import ast
from pathlib import Path

import pytest

BENCHMARKS = Path(__file__).resolve().parents[2] / "benchmarks"


def _opensky_calls(tree):
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and getattr(node.func, "attr", None) == "for_environment"
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and node.args[0].value == "opensky"
        ):
            yield node


SCRIPTS = sorted(p for p in BENCHMARKS.glob("*.py") if 'for_environment("opensky"' in p.read_text())


@pytest.mark.parametrize("script", SCRIPTS, ids=lambda p: p.name)
def test_the_script_names_its_warehouse_and_coverage(script):
    source = script.read_text()
    tree = ast.parse(source)
    sets_path = "warehouse_path =" in source
    for call in _opensky_calls(tree):
        keywords = {k.arg for k in call.keywords}
        assert "warehouse" in keywords or sets_path, f"{script.name}:{call.lineno}"
        assert "worldwide" in keywords or sets_path, f"{script.name}:{call.lineno}"
