"""The pipeline-day runner writes only under its own prefix and flips only the three flags."""

import pytest
import run_airspace_events_day as r


def test_tables_resolve_to_the_right_prefixes():
    assert r.arm_path("on", "opdi_flight_events") == f"{r.PREFIX}/on/opdi_flight_events"
    assert r.arm_path("off", "opdi_h3_airspace_ref") == f"{r.PREFIX}/ref/opdi_h3_airspace_ref"
    assert r.arm_path("on", "osn_tracks_clean") == f"{r.INPUT_WAREHOUSE}/osn_tracks_clean"
    assert r.arm_path("on", "opdi_flight_list", "a10") == f"{r.A10_ARM}/opdi_flight_list"
    assert r.arm_path("on", "opdi_flight_list") == f"{r.PREFIX}/shared/opdi_flight_list"
    assert r.arm_path("off", "opdi_endpoint_candidates") == f"{r.PREFIX}/shared/opdi_endpoint_candidates"
    with pytest.raises(RuntimeError):
        r.arm_path("on", "osn_statevectors_v2")
    with pytest.raises(ValueError):
        r.arm_path("debounced", "opdi_flight_events")


def test_writes_outside_the_study_prefix_are_refused():
    r.guard_write_path(f"{r.PREFIX}/on/opdi_flight_events")
    for bad in (f"{r.INPUT_WAREHOUSE}/osn_tracks_clean", f"{r.A10_ARM}/opdi_flight_events",
                "s3a://eurocontrol/opdi-world/research/a10/cruise_linked/opdi_measurements"):
        with pytest.raises(AssertionError):
            r.guard_write_path(bad)


def test_the_arms_differ_in_the_three_flags_only():
    off, on = r.build_config("off"), r.build_config("on")
    for flag in r.NAVIGATION_FLAGS:
        assert getattr(off.events, flag) is False and getattr(on.events, flag) is True
    differing = {f for f in off.events.__dataclass_fields__
                 if getattr(off.events, f) != getattr(on.events, f)}
    assert differing == set(r.NAVIGATION_FLAGS)
    assert on.coverage.is_worldwide and on.project.warehouse_path == r.INPUT_WAREHOUSE
