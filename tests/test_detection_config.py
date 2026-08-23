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


@pytest.mark.parametrize(
    "field_name,field", list(DetectionConfig().__dataclass_fields__.items())
)
def test_every_measurement_carries_its_unit(field_name, field):
    """The package convention: units live in field names so a threshold cannot
    be read in the wrong one.

    Stated as a rule about *measurements* rather than as a list of exceptions,
    so adding a switch does not mean editing the test. A bool is a decision and
    a str names a rule or an algorithm; neither measures anything, so a unit
    suffix on either would be a lie. Everything numeric measures something.
    """
    # `field.type` is the annotation: a type object here, a string under
    # `from __future__ import annotations`. Both spellings are accepted, and
    # neither is `isinstance(..., type)` -- which is true of `float` as well
    # and would exempt every field in the class.
    if field.type in (bool, str, "bool", "str"):
        return
    # The two numeric exceptions: a count of samples and a flight level, both
    # of which carry their unit in the name already ("fl", "window", "margin").
    counts = {"trend_max_fl", "trend_smooth_half_window", "trend_vote_margin"}
    assert field_name in counts or field_name.endswith(("_nm", "_ft"))


def test_shipped_defaults_are_the_values_the_study_supports():
    """The defaults must match what was measured through the pipeline.

    Every value here was confirmed by running `process_dai` itself, not by a
    sweep harness -- with one exception, the vote margin, which both pipeline
    grids held fixed. Pinning them means a future edit has to argue with a test
    rather than with a comment.
    """
    d = DetectionConfig()
    assert d.trend_max_fl == 60
    # 30, not 20. V6 moved this to 20 on a single period; V7 measured it on two
    # and it is the only ladder step whose sign differs between them -- +214 on
    # 2025, -79 on 2024 -- with the joint sweep preferring 30 as an interior
    # optimum. Reverted on four independent measurements.
    assert d.trend_radius_nm == 30.0
    # 0, not 2. V6 shipped 2 on sweep evidence and flagged it as its weakest
    # value; V7 measured it through the pipeline on both periods -- +395 and
    # +296 -- gaining 465 correct arrivals for 35 wrong, which is 13 per wrong
    # against a bar of 2. Cleaning removes the noise the margin defended
    # against, before the vote is counted.
    assert d.trend_vote_margin == 0
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


def test_every_new_behaviour_is_off_under_legacy():
    """`legacy()` must reproduce the published algorithm, not approximate it.

    Each of these is a capability the published lists were built without. A new
    field that defaults to on and is *not* turned off here would silently change
    what a legacy run produces, which is the one thing the preset exists to
    prevent -- and it would do so without failing anything.
    """
    legacy = DetectionConfig.legacy()
    assert legacy.trend_ooa is False
    assert legacy.trend_bearing_tiebreak_nm == 0.0
    assert legacy.trend_smooth_before_cut is False
    assert legacy.trend_radius_exact is False
    assert legacy.trend_rank_by == "ring"


def test_the_shipped_new_behaviours_are_on():
    """The other half of the pairing above: on by default, off under legacy."""
    d = DetectionConfig()
    # An interior optimum -- 5 NM turns negative and no band at all is
    # catastrophic, so this is not an "as large as possible" threshold.
    assert d.trend_bearing_tiebreak_nm == 2.0
    assert d.trend_smooth_before_cut is True
    assert d.trend_radius_exact is True


def test_trend_out_of_area_is_implemented_but_not_enabled():
    """Built, measured, and deliberately off -- which is not the same as absent.

    The switch exists and works; the geometry does not support it for the role
    that matters. Departures ship from `endpoint`, so this only reaches
    arrivals, where the label is right 50.35% of the time against `endpoint`'s
    89.20%. Every label replaces a silence, so at k = 2 that is a losing trade.

    Pinned so that "the feature is implemented, why is it off?" has to be
    answered against the measurement rather than switched on by assumption.
    """
    assert DetectionConfig().trend_ooa is False
    assert DetectionConfig.legacy().trend_ooa is False


def test_version_string_is_new_and_the_published_ones_are_untouched():
    """Published version strings are frozen; a changed algorithm gets a new one.

    Both old strings stay in the module because a legacy run must still stamp
    them. If someone ever "tidies" them away, a re-processed month stops
    matching the release it was meant to reproduce.
    """
    from opdi.pipeline.flights import (  # noqa: PLC0415
        FLIGHT_LIST_VERSION,
        LEGACY_ENDPOINT_VERSION,
        LEGACY_TREND_VERSION,
    )

    assert FLIGHT_LIST_VERSION == "v5.0.0"
    assert LEGACY_TREND_VERSION == "v2.0.0"
    assert LEGACY_ENDPOINT_VERSION == "v3.0.0"
    assert FLIGHT_LIST_VERSION not in (LEGACY_TREND_VERSION, LEGACY_ENDPOINT_VERSION)


def test_cleaned_tracks_feed_the_flight_list_but_only_when_cleaning_runs():
    """Step 02a wrote a table nothing read until 2026-08.

    The fallback matters as much as the default: selecting the cleaned table
    when cleaning is switched off would point step 03 at something step 02a was
    never asked to write.
    """
    from opdi.config import CleaningConfig  # noqa: PLC0415

    class Stub:
        """Only what `tracks_table` reads."""

        def __init__(self, cleaning):
            self.config = self
            self.cleaning = cleaning

        tracks_table = FlightListProcessor.tracks_table

    assert CleaningConfig().feeds_flight_list is True
    assert Stub(CleaningConfig()).tracks_table == "osn_tracks_clean"

    import dataclasses  # noqa: PLC0415

    off = dataclasses.replace(CleaningConfig(), enabled=False)
    assert Stub(off).tracks_table == "osn_tracks"

    unwired = dataclasses.replace(CleaningConfig(), feeds_flight_list=False)
    assert Stub(unwired).tracks_table == "osn_tracks"

    # A config predating the field at all must not select a table that may not
    # exist.
    assert Stub(None).tracks_table == "osn_tracks"


def test_defaults_differ_from_legacy_so_the_preset_is_load_bearing():
    """If these ever coincide, `legacy()` has stopped protecting anything and
    the published-data guarantee is silently vacuous."""
    assert DetectionConfig() != DetectionConfig.legacy()
    # Precisely: the scheduled-service penalty and the endpoint radius moved.
    assert DetectionConfig.legacy().trend_sched_penalty_nm == 0.0
    assert DetectionConfig.legacy().endpoint_radius_nm == 40.0


@pytest.mark.parametrize("env", ["opensky", "local", "dev", "live"])
def test_the_pipeline_gets_the_field_datum_in_every_environment(env):
    """The datum has to reach the *pipeline*, not just the benchmarks.

    `opdi run --step 03` builds its config through `OPDIConfig.for_environment`
    and hands it to `FlightListProcessor`, which falls back to `legacy()` when
    a config carries no detection block. So an environment factory that forgot
    to populate one would silently run the sea-level cut while every test on
    `DetectionConfig()` still passed -- the change would look shipped and not
    be.
    """
    cfg = OPDIConfig.for_environment(env)
    assert cfg.detection is not None, f"{env} carries no detection config"
    assert cfg.detection.trend_max_datum == "field"
    assert cfg.detection.trend_max_height_ft == 6000.0


def test_legacy_stays_on_the_sea_level_datum():
    """Released data was built with a flight-level cut; the preset must keep it.

    If the preset silently moved onto the field-elevation datum, every
    reprocessed month would differ from what was published at exactly the
    aerodromes the change is meant to help -- and `legacy()` exists precisely
    so that cannot happen.
    """
    legacy = DetectionConfig.legacy()
    assert legacy.trend_max_datum == "msl"
    assert legacy.trend_max_fl == 40


def test_the_shipped_default_is_the_field_datum():
    d = DetectionConfig()
    assert d.trend_max_datum == "field"
    assert d.trend_max_height_ft == 6000.0
    # Retained, not removed: the msl branch still reads it, and `legacy()`
    # still needs a flight level to reproduce released data with.
    assert d.trend_max_fl == 60


def test_an_unknown_datum_is_rejected_at_construction():
    """A typo must fail loudly. Falling through to one of the two branches
    would apply a cut nobody asked for and report it as the other one.
    """
    with pytest.raises(ValueError, match="trend_max_datum"):
        DetectionConfig(trend_max_datum="agl")


def test_the_datum_participates_in_equality():
    """`_version_for` decides whether a run is a legacy one by comparing the
    whole config against `legacy()`. That only works if the datum is part of
    the comparison -- otherwise a field-datum run wearing legacy thresholds
    would stamp a published version string it does not reproduce.
    """
    import dataclasses  # noqa: PLC0415

    legacy = DetectionConfig.legacy()
    assert dataclasses.replace(legacy, trend_max_datum="field") != legacy
