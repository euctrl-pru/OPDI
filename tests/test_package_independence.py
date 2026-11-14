"""The package holds the default and the v0.0.2 rules only; study variants live in benchmarks/.

``benchmarks/`` may import the package, never the other way round. A study
rule selected by name without first registering it fails with a message that
says where it lives.
"""

import re
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parent.parent / "src" / "opdi"

STUDY_ARMS = (
    "no_month_suffix",
    "traffic_style",
    "airframe_only",
    "ground_anchored",
    "airport_anchored",
    "vertical_profile",
    "recommended",
    "debounced",
)


def _sources():
    return sorted(SRC.rglob("*.py"))


def test_the_package_never_imports_the_study_scripts():
    pattern = re.compile(r"^\s*(from|import)\s+(benchmarks|study_variants)\b", re.M)
    offenders = [str(p) for p in _sources() if pattern.search(p.read_text())]
    assert offenders == []


def test_the_package_defines_no_study_rule():
    pattern = re.compile(r"^def (%s)\(" % "|".join(STUDY_ARMS), re.M)
    offenders = [str(p) for p in _sources() if pattern.search(p.read_text())]
    assert offenders == []


@pytest.mark.parametrize("name", ["recommended", "debounced", "airframe_only"])
def test_a_study_rule_is_refused_with_its_new_home(name, monkeypatch):
    from opdi.config import SegmentationConfig
    from opdi.pipeline.segmentation import methods

    monkeypatch.setattr(
        methods, "ARMS", {k: v for k, v in methods.ARMS.items() if k not in STUDY_ARMS}
    )
    with pytest.raises(ValueError, match="benchmarks/study_variants"):
        SegmentationConfig(method=name)


def test_an_old_import_path_names_the_new_home():
    with pytest.raises(ImportError, match="study_variants"):
        from opdi.pipeline.segmentation.methods import debounced  # noqa: F401


def test_registering_makes_the_study_rules_selectable(monkeypatch):
    from opdi.config import SegmentationConfig
    from opdi.pipeline.segmentation import methods

    monkeypatch.setattr(methods, "ARMS", dict(methods.ARMS))
    from study_variants.segmentation import register_study_arms

    register_study_arms()
    for name in STUDY_ARMS:
        assert name in methods.ARMS
        assert SegmentationConfig(method=name).method == name


def test_the_study_detection_options_are_not_in_the_package():
    from opdi.config import DetectionConfig
    from opdi.pipeline.flights import FlightListProcessor

    assert not hasattr(DetectionConfig(), "trend_ooa")
    assert "nearest" not in FlightListProcessor.DETECTION_MODES
