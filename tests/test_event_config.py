"""Conventions and defaults for :class:`EventConfig`.

Mirrors ``test_detection_config.py``. These are property tests over the
dataclass rather than tests of behaviour: they exist so that adding a field
cannot quietly break the two promises the configuration makes -- that a
threshold cannot be read in the wrong unit, and that ``legacy()`` still
reproduces what was published.
"""

import pytest

from opdi.config import EventConfig, OPDIConfig


def test_events_config_is_reachable_from_the_container():
    """A config nobody can get to tunes nothing."""
    assert isinstance(OPDIConfig().events, EventConfig)


@pytest.mark.parametrize(
    "field_name,field", list(EventConfig().__dataclass_fields__.items())
)
def test_every_measurement_carries_its_unit(field_name, field):
    """Units live in field names so a threshold cannot be read in the wrong one.

    Stated as a rule about *measurements*, as in ``test_detection_config``: a
    bool is a decision and a str names an algorithm or a version, so neither
    measures anything and a unit suffix on either would be a lie.

    This matters more here than anywhere else in the package. Every one of
    these numbers was an inline literal in ``pipeline/events.py``, mixing
    metres, feet, flight levels and feet per minute in one function, and the
    conversion factors were repeated rather than shared.
    """
    if field.type in (bool, str, "bool", "str"):
        return
    # Two numeric exceptions, both carrying their unit in the name already:
    # a set of flight levels and a flight level ceiling.
    counts = {"crossing_levels_fl", "airport_max_fl"}
    assert field_name in counts or field_name.endswith(
        ("_nm", "_ft", "_kt", "_ftmin", "_seconds", "_pct")
    )


def test_every_new_behaviour_is_off_under_legacy():
    """``legacy()`` must reproduce the published detector, not approximate it.

    Each of these is a capability no published event was built with. A new
    field defaulting to on and *not* turned off here would silently change what
    a legacy run produces -- without failing anything, which is exactly the
    failure mode the preset exists to prevent.
    """
    legacy = EventConfig.legacy()

    assert legacy.phase_twindow_seconds == 0.0
    assert legacy.phase_ground_above_field is False
    assert legacy.phase_require_complete_rules is False
    assert legacy.crossing_all_occurrences is False
    assert legacy.crossing_interpolate is False
    assert legacy.airport_events_ordered is False
    assert legacy.feeds_from_clean_tracks is False
    assert legacy.deterministic_event_ids is False
    assert legacy.enable_pandas_stage is False
    # Rings did not exist at all, so a legacy run must emit none.
    assert tuple(legacy.ring_radii_nm) == ()


def test_the_shipped_new_behaviours_are_on():
    """The other half of the pairing above."""
    e = EventConfig()

    assert e.phase_twindow_seconds == 60.0
    assert e.phase_require_complete_rules is True
    assert e.crossing_all_occurrences is True
    assert e.crossing_interpolate is True
    assert e.airport_events_ordered is True
    assert e.feeds_from_clean_tracks is True
    assert e.deterministic_event_ids is True
    # The escape hatch stays shut: it needs the fatter executor image, and
    # nothing in the current vocabulary requires it.
    assert e.enable_pandas_stage is False


def test_version_string_is_new_and_the_published_one_is_untouched():
    """Published version strings are frozen; a changed algorithm gets a new one.

    ``events_v0.0.2`` must remain reachable, because a legacy run has to stamp
    it for a re-processed month to match the release it reproduces.
    """
    assert EventConfig().events_version == "events_v0.1.0"
    assert EventConfig.legacy().events_version == "events_v0.0.2"


def test_the_published_flight_level_vocabulary_does_not_shift_by_accident():
    """Making the levels configurable must not change which ones ship."""
    assert tuple(EventConfig().crossing_levels_fl) == (50, 70, 100, 245)


def test_the_dead_bands_are_non_zero():
    """Hysteresis is what makes "every crossing" usable rather than absurd.

    At zero, an aircraft levelled at a threshold emits a crossing on every
    sample of noise. This pins the guard so a future edit that zeroes it has to
    argue with a test.
    """
    e = EventConfig()
    assert e.crossing_hysteresis_ft > 0
    assert e.ring_hysteresis_nm > 0


def test_unimplemented_behaviour_ships_inert():
    """A flag that defaults on and does nothing is worse than no flag.

    Each of these describes a behaviour whose detector does not exist yet.
    Shipping them on would mean anyone reading ``EventConfig`` -- or trusting
    this test file -- would conclude that ground membership is measured above
    field elevation and that ring events are emitted. Neither is true. Each
    default flips in the commit that implements it, which is also what makes
    that commit's diff say what it changed.
    """
    e = EventConfig()

    # D1: pipeline/events.py does not read this field.
    assert e.phase_ground_above_field is False
    # crossings.ring_crossings exists and is tested, but nothing builds the
    # (sample, aerodrome) distance frame it consumes.
    assert tuple(e.ring_radii_nm) == ()


def test_the_hysteresis_stays_live_while_the_radii_are_inert():
    """The dead band is the detector's parameter, not the caller's choice of
    rings, so it keeps its value while ``ring_radii_nm`` is empty. When the
    radii come back the band must already be right."""
    assert EventConfig().ring_hysteresis_nm > 0
    assert EventConfig.legacy().ring_hysteresis_nm > 0


def test_level_segment_parameters_are_icaos_published_values():
    """KPI17 and KPI19 are specified by ICAO with named parameters and example
    values (GANP KPI overview, ganpportal.icao.int/asbu/kpi).

    These are not ours to choose. Pinning them means a conformance claim in the
    paper is checkable, which is what makes it worth making -- no external
    source holds level-segment truth to score against.
    """
    e = EventConfig()
    assert e.level_analysis_radius_nm == 200.0
    assert e.level_vertical_speed_limit_ftmin == 300.0
    assert e.level_band_limit_ft == 200.0
    assert e.level_min_duration_seconds == 20.0
    assert e.level_exclusion_box_pct == 90.0
    assert e.level_exclusion_box_seconds == 300.0
    # Climb detection starts higher than descent detection stops, because an
    # aircraft on final is legitimately close to level.
    assert e.level_min_altitude_climb_ft == 3000.0
    assert e.level_min_altitude_descent_ft == 1800.0


def test_cruise_speed_membership_is_left_at_openaps_values():
    """A known limitation, deliberately not "fixed" without evidence.

    ``gaussmf(speed_kt, 600, 100)`` gives a 250 kt turboprop an activation of
    0.002, so ``rule_cruise`` never wins and the aircraft gets no top-of-climb
    or top-of-descent at all. But these are OpenAP's own published constants,
    so the port is faithful and this is inherited rather than an OPDI defect.
    Exposed as configuration so the affected population can be measured by
    typecode before anyone argues for deviating from the reference.
    """
    e = EventConfig()
    assert e.phase_cruise_speed_kt == 600.0
    assert e.phase_cruise_speed_sigma_kt == 100.0
    assert EventConfig.legacy().phase_cruise_speed_kt == 600.0
