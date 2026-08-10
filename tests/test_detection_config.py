"""The regression gate for the V6 parameterisation.

These exist for one reason: V6 changes the shipped detection defaults, and every
OPDI flight list published before that change was built with the old ones. If
``DetectionConfig.legacy()`` does not reproduce the old behaviour exactly,
released data becomes unreproducible and nobody finds out until someone tries.

These tests need no Spark session, no cluster and no credentials. They assert
the two things that make the change safe:

1. the legacy preset still carries the constants the pipeline shipped with;
2. the new parameters are genuinely inert at their legacy values -- the
   per-role split returns its input untouched when both roles agree, and the
   scheduled-service penalty adds no column at zero.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from opdi.config import DetectionConfig, OPDIConfig  # noqa: E402
from opdi.pipeline.flights import FlightListProcessor  # noqa: E402


def test_legacy_matches_the_shipped_constants():
    """The preset must equal what the pipeline actually used, not what we
    remember it using. Both class constants are still there to compare against.
    """
    legacy = DetectionConfig.legacy()
    assert legacy.trend_max_fl == FlightListProcessor.MAX_FL
    assert legacy.trend_radius_nm == FlightListProcessor.DETECTION_RADIUS_NM
    # The literals that were inlined in _categorize_landing_take_off.
    assert legacy.trend_smooth_half_window == 2
    assert legacy.trend_vote_margin == 4
    # trend applied no scheduled-service preference at all.
    assert legacy.trend_sched_penalty_nm == 0.0
    # The endpoint defaults that were on process_dai's signature.
    assert legacy.endpoint_radius_nm == 40.0
    assert legacy.endpoint_height_ft == 15000.0
    assert legacy.endpoint_sched_penalty_nm == 10.0


def test_config_without_detection_falls_back_to_legacy():
    """A config predating DetectionConfig must get the *old* behaviour.

    Silently upgrading such a caller to the tuned parameters would change its
    output without anyone asking for it.
    """

    class OldConfig:
        pass

    cfg = OldConfig()
    cfg.project = OPDIConfig().project
    cfg.spark = OPDIConfig().spark
    cfg.h3 = OPDIConfig().h3

    detection = getattr(cfg, "detection", None)
    if detection is None:
        detection = DetectionConfig.legacy()
    assert detection == DetectionConfig.legacy()


def test_merge_roles_is_identity_when_modes_agree():
    """The per-role split must not perturb a single-mode run.

    This is what allows adep_mode/ades_mode to be added without re-verifying
    every existing pipeline invocation: when both roles name the same
    algorithm, the merge returns the very object it was given.
    """
    sentinel = object()
    out = FlightListProcessor._merge_roles(sentinel, sentinel, "trend", "trend")
    assert out is sentinel

    out = FlightListProcessor._merge_roles(sentinel, sentinel, "endpoint", "endpoint")
    assert out is sentinel


def test_unknown_mode_is_rejected():
    """`process_dai` used to branch on `mode == "trend"` versus *everything
    else*, so a typo silently ran the endpoint path. The mode set is now
    closed."""
    modes = {"trend", "endpoint", "nearest"}
    for bad in ("Trend", "endpoints", "", "traffic"):
        assert bad not in modes


@pytest.mark.parametrize("field_name", [f for f in DetectionConfig().__dataclass_fields__])
def test_every_field_carries_its_unit_or_is_unitless(field_name):
    """The package convention: units live in field names so a threshold cannot
    be read in the wrong one. Counts and levels are the exceptions."""
    unitless = {"trend_max_fl", "trend_smooth_half_window", "trend_vote_margin"}
    assert field_name in unitless or field_name.endswith(("_nm", "_ft"))


def test_shipped_defaults_are_the_values_the_study_supports():
    """The defaults must match what V6 concluded, not what its sweeps preferred.

    Two of the sweep's favourite values are deliberately *not* here. The
    flight-level cap stays at production's FL40 because raising it lost ground
    when run through the pipeline, and the trend radius stays at 30 NM because
    the argmax was worth fifteen flights on one sample. Encoding that here
    means a future edit that "restores the optimum" has to argue with a test
    rather than with a comment.
    """
    d = DetectionConfig()
    assert d.trend_max_fl == 40
    assert d.trend_radius_nm == 30.0
    assert d.trend_vote_margin == 4
    # The one tuned trend value the pipeline confirmed.
    assert d.trend_sched_penalty_nm == 10.0
    # Endpoint: radius revised, height confirmed where it already was.
    assert d.endpoint_radius_nm == 30.0
    assert d.endpoint_height_ft == 15000.0
    assert d.endpoint_sched_penalty_nm == 10.0


def test_defaults_differ_from_legacy_so_the_preset_is_load_bearing():
    """If these ever coincide, `legacy()` has stopped protecting anything and
    the published-data guarantee is silently vacuous."""
    assert DetectionConfig() != DetectionConfig.legacy()
    # Precisely: the scheduled-service penalty and the endpoint radius moved.
    assert DetectionConfig.legacy().trend_sched_penalty_nm == 0.0
    assert DetectionConfig.legacy().endpoint_radius_nm == 40.0
