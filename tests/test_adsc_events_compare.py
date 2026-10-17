"""The two-arm scorer's wiring. The arithmetic is events_score's own tests."""

import pytest

import adsc_events_compare as a


def test_arm_tables_point_at_the_arm_prefix():
    t = a.arm_tables("s3a://eurocontrol/opdi/research/adsc", "merged")
    assert t["events"] == (
        "s3a://eurocontrol/opdi/research/adsc/merged/opdi_flight_events")
    assert t["flight_list"] == (
        "s3a://eurocontrol/opdi/research/adsc/merged/opdi_flight_list")


def test_the_event_table_is_an_absolute_path():
    # `detected_events` branches on `table.startswith("s3a://")`: an absolute
    # path is read with spark.read.parquet, anything else as a catalog table.
    # A relative name here would be resolved against OPDI_WAREHOUSE, which
    # production exports -- so the scorer would silently read opdi-prod.
    for arm in a.ARMS:
        assert a.arm_tables(a.ARM_PREFIX, arm)["events"].startswith("s3a://")


def test_both_arms_are_scored():
    assert a.ARMS == ("base", "merged")


def test_the_period_is_the_v4_period():
    # Comparability with flight-events-v4 is the reason this period and not
    # another: 202606 is the only APDF extract covering all twenty study
    # aerodromes.
    import events_gt

    assert a.PERIOD == "2026"
    assert events_gt.PERIODS[a.PERIOD]["month"] == "202606"
    assert events_gt.PERIODS[a.PERIOD]["days"] == [
        "2026-06-05", "2026-06-06", "2026-06-07"]


def test_an_unknown_arm_is_refused():
    with pytest.raises(ValueError, match="unknown arm"):
        a.arm_tables(a.ARM_PREFIX, "mergd")


def test_the_two_arms_read_different_event_tables():
    # Scoring one table twice would produce a perfect null result and look
    # exactly like the expected outcome.
    base = a.arm_tables(a.ARM_PREFIX, "base")
    merged = a.arm_tables(a.ARM_PREFIX, "merged")
    assert base["events"] != merged["events"]
    assert base["flight_list"] != merged["flight_list"]


def test_one_truth_frame_scores_both_arms():
    # Read off the source: building the truth per arm is the easiest way to
    # make the arms incomparable, because the bridge is sampled and its reach
    # would differ between two builds.
    import inspect

    src = inspect.getsource(a.main)
    assert src.count("events_gt.build") == 1
    assert src.index("events_gt.build") < src.index("for arm in args.arms")
