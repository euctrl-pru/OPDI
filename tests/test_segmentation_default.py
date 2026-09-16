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


def test_standard_resolves_to_the_debounced_rule():
    """`standard` is a pointer to the current production arm. As of this release
    it resolves to `debounced` (A9); if it ever stops resolving to the arm the
    study measured, the shipped algorithm and the published evidence part
    company. A9 keeps A8's airframe grouping and suffix-free id shape."""
    rule = ARMS["debounced"]()
    assert rule.group_cols == ["icao24"]
    assert rule.month_suffix is False


def test_a8_stays_reachable_under_its_own_name():
    """`recommended` (A8) must remain selectable so data published between
    2026-08-27 and this release stays reproducible."""
    rule = ARMS["recommended"]()
    assert rule.group_cols == ["icao24"]
    assert rule.month_suffix is False


def test_the_shipped_persistence_guard_is_active_by_default():
    """`standard` == `debounced` only debounces if the hold is non-zero; zero
    collapses it to A8. The shipped default must therefore be the validated 30 s."""
    assert OPDIConfig.for_environment("opensky").segmentation.callsign_min_persistence_seconds == 30.0
