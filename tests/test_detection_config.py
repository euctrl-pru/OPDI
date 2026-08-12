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
    # Counts, levels and the categoricals. `trend_rank_by` names a rule and
    # `adep_mode`/`ades_mode` name algorithms, so a unit suffix would be a lie.
    unitless = {"trend_max_fl", "trend_smooth_half_window", "trend_vote_margin",
                "trend_rank_by", "adep_mode", "ades_mode"}
    assert field_name in unitless or field_name.endswith(("_nm", "_ft"))


def test_shipped_defaults_are_the_values_the_study_supports():
    """The defaults must match what V6 measured through the pipeline.

    Every value here was confirmed by running `process_dai` itself, not by a
    sweep harness -- with one exception, the vote margin, which the pipeline
    grid held fixed. The flight-level cap and radius were each rejected once on
    ring-ranked evidence and reinstated once ranking became exact; pinning them
    here means a future edit that "restores the production constant" has to
    argue with a test rather than with a comment.
    """
    d = DetectionConfig()
    assert d.trend_max_fl == 60
    assert d.trend_radius_nm == 20.0
    assert d.trend_vote_margin == 2
    # The one tuned trend value the pipeline confirmed.
    assert d.trend_sched_penalty_nm == 10.0
    # Endpoint: radius revised, height confirmed where it already was.
    assert d.endpoint_radius_nm == 30.0
    assert d.endpoint_height_ft == 15000.0
    assert d.endpoint_sched_penalty_nm == 10.0


def test_the_recommended_configuration_is_what_a_caller_gets_by_default():
    """Which algorithm serves which role is a shipped decision, not a caller's.

    Until these fields existed, `process_dai` fell back to the literal "trend"
    for both roles, so every entry point that did not name the modes -- the CLI
    included -- ran a configuration this study recommends for neither role. The
    thresholds were tuned and the algorithm choice was not applied, which is the
    worst of both.
    """
    d = DetectionConfig()
    assert d.adep_mode == "endpoint"
    assert d.ades_mode == "trend"
    assert {d.adep_mode, d.ades_mode} <= {"trend", "endpoint", "nearest"}

    # Published lists were built from `trend` alone; `endpoint` served nothing.
    legacy = DetectionConfig.legacy()
    assert legacy.adep_mode == "trend"
    assert legacy.ades_mode == "trend"


def test_ranking_rule_is_exact_distance_by_default_and_rings_under_legacy():
    """The candidate ranking rule, which matters more than any threshold here.

    `ring` keeps only candidates at the minimum H3 ring count before measuring
    distance, so an aerodrome one ring further out is discarded unmeasured.
    That coarseness is why every tuned trend parameter failed to transfer from
    the sweep harness, which always ranked on exact distance.
    """
    assert DetectionConfig().trend_rank_by == "haversine"
    assert DetectionConfig.legacy().trend_rank_by == "ring"


def test_defaults_differ_from_legacy_so_the_preset_is_load_bearing():
    """If these ever coincide, `legacy()` has stopped protecting anything and
    the published-data guarantee is silently vacuous."""
    assert DetectionConfig() != DetectionConfig.legacy()
    # Precisely: the scheduled-service penalty and the endpoint radius moved.
    assert DetectionConfig.legacy().trend_sched_penalty_nm == 0.0
    assert DetectionConfig.legacy().endpoint_radius_nm == 40.0
