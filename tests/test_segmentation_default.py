"""The shipped segmentation, asserted where it cannot be quietly reverted."""

from opdi.config import OPDIConfig
from opdi.pipeline.segmentation.methods import ARMS


def test_standard_is_the_default_segmentation():
    cfg = OPDIConfig.for_environment("opensky")
    assert cfg.segmentation.method == "standard"


def test_every_environment_ships_the_same_segmentation():
    """A default that varies by environment is a default nobody can reason about.

    One environment would otherwise be able to publish track_ids that the other
    cannot reproduce, and nothing in the data would say why.
    """
    for env in ("opensky", "local"):
        assert OPDIConfig.for_environment(env).segmentation.method == "standard"


def test_legacy_is_still_reachable():
    """Reproducing a pre-release track_id must remain possible.

    Without this the old ids become unreproducible by any configuration, which
    is a stronger break than the release intends.
    """
    assert "legacy" in ARMS


def test_standard_resolves_to_cruise_linked():
    """`standard` is a pointer to the current production arm. It resolves to
    `cruise_linked` (A10); if it ever stops resolving to the arm the study
    measured, the shipped algorithm and the published evidence part company.
    A10 keeps A8's airframe grouping and suffix-free id shape."""
    from opdi.pipeline.tracks import _resolve_segmentation_method

    assert _resolve_segmentation_method("standard") == "cruise_linked"
    rule = ARMS["cruise_linked"]()
    assert rule.group_cols == ["icao24"]
    assert rule.month_suffix is False


def test_legacy_by_name_reads_3h_back():
    """The v0.0.2 rule does not link holes, so it reads the short lookback."""
    from opdi.pipeline.segmentation import SegmentationParams
    from opdi.pipeline.tracks import _resolve_segmentation_method, required_lookback_hours

    assert _resolve_segmentation_method("legacy") == "legacy"
    assert required_lookback_hours("legacy", SegmentationParams()) == 3


def test_the_default_run_reads_11_hours_back():
    from opdi.pipeline.segmentation import SegmentationParams
    from opdi.pipeline.tracks import required_lookback_hours

    p = SegmentationParams.from_config(OPDIConfig.for_environment("opensky").segmentation)
    assert required_lookback_hours("standard", p) == 11


def test_the_shipped_persistence_guard_is_active_by_default():
    """`standard` only debounces a callsign change if the hold is non-zero. The
    shipped default must therefore be the validated 30 s."""
    assert (
        OPDIConfig.for_environment("opensky").segmentation.callsign_min_persistence_seconds == 30.0
    )
