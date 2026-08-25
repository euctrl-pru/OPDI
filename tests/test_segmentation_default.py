"""The shipped segmentation, asserted where it cannot be quietly reverted."""
from opdi.config import OPDIConfig
from opdi.pipeline.segmentation.methods import ARMS


def test_standard_is_the_default_segmentation():
    cfg = OPDIConfig.for_environment("opensky")
    assert cfg.segmentation.method == "standard"


def test_every_environment_ships_the_same_segmentation():
    """A default that varies by environment is a default nobody can reason about.

    dev and live would otherwise be able to publish track_ids that local runs
    cannot reproduce, and nothing in the data would say why.
    """
    for env in ("opensky", "local", "dev", "live"):
        assert OPDIConfig.for_environment(env).segmentation.method == "standard"


def test_legacy_is_still_reachable():
    """Reproducing a pre-release track_id must remain possible.

    Without this the old ids become unreproducible by any configuration, which
    is a stronger break than the release intends.
    """
    assert "legacy" in ARMS


def test_standard_resolves_to_the_recommended_rule():
    """`standard` is an alias. If it ever stops resolving to the arm the study
    measured, the shipped algorithm and the published evidence part company."""
    rule = ARMS["recommended"]()
    assert rule.group_cols == ["icao24"]
    assert rule.month_suffix is False
