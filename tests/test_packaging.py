"""The package metadata in pyproject.toml matches the code."""

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent

try:  # Python 3.11 and later
    import tomllib
except ImportError:  # pragma: no cover - Python 3.10
    tomllib = pytest.importorskip("tomli")


@pytest.fixture(scope="module")
def project():
    return tomllib.loads((ROOT / "pyproject.toml").read_text())


def test_python_version_and_readme(project):
    meta = project["project"]
    assert meta["requires-python"] == ">=3.10"
    assert (ROOT / meta["readme"]).is_file()
    versions = [
        c for c in meta["classifiers"] if c.startswith("Programming Language :: Python :: 3.")
    ]
    assert versions == [f"Programming Language :: Python :: 3.{n}" for n in (10, 11, 12)]
    assert project["tool"]["ruff"]["target-version"] == "py310"
    assert project["tool"]["black"]["target-version"] == ["py310", "py311", "py312"]


def test_the_reference_build_dependencies_are_declared(project):
    names = [
        d.split(">")[0].split("=")[0].split("<")[0] for d in project["project"]["dependencies"]
    ]
    assert "geopandas" in names
    extras = project["project"]["optional-dependencies"]
    assert any(d.startswith("osmium") for d in extras["reference"])
