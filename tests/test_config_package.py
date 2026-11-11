"""The configuration package keeps the behaviour of the former single module."""

from pathlib import Path


def test_the_dotenv_file_is_looked_for_in_the_repository_root():
    from opdi.config import dotenv

    assert (dotenv.REPOSITORY_ROOT / "pyproject.toml").is_file()
    assert dotenv.REPOSITORY_ROOT == Path(__file__).resolve().parent.parent


def test_study_rule_registry_stays_reachable():
    from opdi.config import STUDY_SEGMENTATION_METHODS, SegmentationConfig
    from opdi.pipeline.segmentation import methods

    assert "debounced" in STUDY_SEGMENTATION_METHODS
    assert isinstance(methods.ARMS, dict)
    assert SegmentationConfig().method == "standard"
