"""The families' measurements are registered with step 04."""

from opdi.pipeline.events import EXTRA_MEASURES


def test_holding_measurements_are_registered():
    types = {(t, m) for t, _, m, _ in EXTRA_MEASURES}
    assert ("holding-end", "Holding duration (s)") in types
    assert ("holding-end", "Holding distance (NM)") in types
    assert ("holding-end", "Holding circuits") in types


def test_point_merge_measurements_are_registered():
    types = {(t, m) for t, _, m, _ in EXTRA_MEASURES}
    assert ("pms-leg-exit", "Point merge leg time (s)") in types
    assert ("pms-leg-exit", "Point merge leg distance (NM)") in types
    assert ("pms-leg-exit", "Point merge direct-to distance (NM)") in types


def test_every_entry_names_a_versioned_family():
    for event_type, field, measure, version in EXTRA_MEASURES:
        assert version.endswith("_v0.1.0") or version.startswith(("holding", "pms", "fir"))
        assert field and measure and event_type
