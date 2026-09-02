"""The benchmark's write guard, and the ladder's plan assertions.

Both are cheap to test and expensive to get wrong: an unguarded run appends a
duplicate month to the *published* event table, and a ladder whose first rung
is not the published algorithm measures a baseline nobody asked about -- at
roughly two hours per rung to discover.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks"))

import pytest

import event_bench
from event_bench import (
    LADDER,
    detected_events,
    LADDER_V4,
    PERIOD_TRACKS,
    V4_BASE,
    V4_NOOP_EXEMPT,
    build_plan,
    flight_list_table,
    guard_writes,
    milestone_map,
    redirect_event_tables,
    runway_identity_types,
    verify_plan,
    verify_plan_v4,
)
from opdi.config import EventConfig


def test_the_first_rung_is_exactly_the_published_algorithm():
    plan = build_plan()

    assert plan["L00_legacy"] == EventConfig.legacy()


def test_the_last_rung_is_exactly_what_ships():
    """Otherwise the ladder's verdict is about a configuration nobody runs."""
    plan = build_plan()

    assert plan["L13_shipped"] == EventConfig()


def test_no_two_adjacent_rungs_are_identical():
    """A no-op rung costs a full run and reports a difference of zero, which
    reads as 'this change is worthless' rather than 'this rung did nothing'."""
    verify_plan(build_plan())


def test_every_rung_name_is_unique():
    names = [n for n, _ in LADDER]

    assert len(names) == len(set(names))


@pytest.fixture
def pristine_storage():
    """Snapshot and restore ``StorageManager``'s patchable surface.

    ``guard_writes`` patches the *class*, so without this the guard leaks into
    every test that runs afterwards in the same session -- which is exactly
    what happened: two unrelated storage tests started failing because they
    were calling the benchmark's wrapper rather than the real method. A
    module-level monkeypatch needs a teardown even when the function under test
    is idempotent.
    """
    from opdi.utils.storage import StorageManager

    saved = {
        name: getattr(StorageManager, name, None)
        for name in (
            "write_table", "_s3_path",
            "_events_guarded", "_events_orig_write", "_events_orig_path",
        )
    }
    yield StorageManager
    for name, value in saved.items():
        if value is None:
            if hasattr(StorageManager, name):
                delattr(StorageManager, name)
        else:
            setattr(StorageManager, name, value)


def test_the_guard_refuses_a_write_outside_research(spark, pristine_storage):
    from opdi.config import OPDIConfig

    storage = pristine_storage(spark, OPDIConfig.for_environment("local"))
    guard_writes()
    df = spark.createDataFrame([(1,)], "x int")

    with pytest.raises(RuntimeError, match="writes only under"):
        storage.write_table(df, "opdi_flight_events", mode="append")


def test_a_redirected_write_is_allowed_through(spark, pristine_storage):
    """The other half: the redirect is what makes the write legitimate, and the
    guard has to recognise it by path rather than by name."""
    from opdi.config import OPDIConfig

    storage = pristine_storage(spark, OPDIConfig.for_environment("local"))
    guard_writes()
    redirect_event_tables("research/events_test")

    landing = storage._s3_path("opdi_flight_events")

    assert "/research/events_test" in landing


# ---------------------------------------------------------------------------
# The V4 ladder
#
# It has no constructor for its baseline -- `EventConfig()` moved to v0.2.0 --
# so the reconstruction is the thing most likely to rot, and it rots silently:
# a base missing one field measures that field's effect inside whichever rung
# happens to follow it.


def test_the_v4_baseline_is_v0_1_0_and_nothing_else():
    """Exactly the eight reconstructed fields differ from what ships.

    A ninth difference means the baseline is not v0.1.0. An eighth missing one
    means a behaviour shipped with no rung measuring it.
    """
    base = build_plan(ladder="v4")["V00_v3_shipped"]
    shipped = EventConfig()

    differing = {
        f for f in EventConfig().__dataclass_fields__
        if getattr(base, f) != getattr(shipped, f)
    }

    assert differing == set(V4_BASE)
    assert len(V4_BASE) == 8


def test_the_last_v4_rung_is_exactly_what_ships():
    """Otherwise the ladder's verdict is about a configuration nobody runs."""
    assert build_plan(ladder="v4")["V07_shipped"] == EventConfig()


def test_the_v4_ladder_verifies():
    verify_plan_v4(build_plan(ladder="v4"))


def test_the_v3_ladder_still_verifies_under_its_own_verifier():
    """V3 is a published instrument; adding V4 must not move its assertions."""
    verify_plan(build_plan())
    assert build_plan()["L00_legacy"] == EventConfig.legacy()


def test_every_v4_rung_name_is_unique():
    names = [n for n, _ in LADDER_V4]

    assert len(names) == len(set(names))


def test_the_no_op_exemption_is_by_name_not_by_position():
    """V01 is deliberately config-identical to V00 -- the runway tie-break is a
    bug fix, not a flag. A *new* accidental no-op must still fail, so the
    exemption names the rung rather than allowing one repeat anywhere."""
    plan = build_plan(ladder="v4")
    assert V4_NOOP_EXEMPT == {"V01_runway_tiebreak"}

    # An accidental repeat elsewhere: V04 re-stating what V03 already set.
    accidental = dict(plan)
    accidental["V04_level_geometry"] = accidental["V03_pru_level"]

    with pytest.raises(AssertionError, match="identical"):
        verify_plan_v4(accidental)


# ---------------------------------------------------------------------------
# The milestone map
#
# `landing` is the subtle one: ground contact under v0.1.0, the ICAO T16
# threshold-plane crossing under v0.2.0. APDF records the first and has no
# column at all for the second.


def test_v0_1_0_landing_is_scored_against_aldt():
    mapping = milestone_map(build_plan(ladder="v4")["V00_v3_shipped"])

    assert mapping["landing"] == "ALDT"
    assert mapping["take-off"] == "ATOT"


def test_v0_2_0_landing_is_not_scored_at_all():
    """It is a threshold crossing; touchdown is that vocabulary's ALDT."""
    mapping = milestone_map(build_plan(ladder="v4")["V07_shipped"])

    assert "landing" not in mapping
    assert mapping["touchdown"] == "ALDT"
    assert mapping["airborne"] == "ATOT"


def test_verify_plan_v4_refuses_a_map_that_scores_v0_2_0_landing(monkeypatch):
    """The assertion has teeth: widen the map and the plan stops verifying."""
    monkeypatch.setattr(event_bench, "milestone_map", lambda cfg: {"landing": "ALDT"})

    with pytest.raises(AssertionError, match="threshold-plane"):
        verify_plan_v4(build_plan(ladder="v4"))


def test_runway_identity_types_never_mix_the_two_vocabularies():
    """Both name a runway, under different type strings. Comparing them
    together would align two detections onto one truth row."""
    plan = build_plan(ladder="v4")

    assert runway_identity_types(plan["V00_v3_shipped"]) == {"ATOT": "ATOT", "ALDT": "ALDT"}
    assert runway_identity_types(plan["V07_shipped"]) == {
        "airborne": "ATOT", "touchdown": "ALDT"}


# ---------------------------------------------------------------------------
# The 2026 flight list


def test_only_the_2026_period_overrides_the_flight_list():
    """2025 and 2024 must keep resolving identity exactly as they did."""
    assert PERIOD_TRACKS["2026"]["flight_list"] == "research/flight_list_2026"
    assert flight_list_table("2026") == "research/flight_list_2026"
    assert flight_list_table("2025") == "opdi_flight_list"
    assert flight_list_table("2024") == "opdi_flight_list"


def test_the_2026_redirect_sends_the_flight_list_read_to_the_research_copy(
    spark, pristine_storage
):
    """Step 04 asks for `opdi_flight_list` by name. Without this the 2026 run
    joins against a 2025-only table, finds no aerodrome for any track, and
    reports every aerodrome-anchored family as empty rather than as broken."""
    from opdi.config import OPDIConfig

    storage = pristine_storage(spark, OPDIConfig.for_environment("local"))
    redirect_event_tables("research/events_2026_V07_shipped",
                          flight_list=flight_list_table("2026"))

    assert "research/flight_list_2026" in str(storage._s3_path("opdi_flight_list"))
    assert "research/events_2026_V07_shipped" in str(
        storage._s3_path("opdi_flight_events"))


def test_a_period_without_an_override_leaves_the_flight_list_alone(
    spark, pristine_storage
):
    from opdi.config import OPDIConfig

    storage = pristine_storage(spark, OPDIConfig.for_environment("local"))
    redirect_event_tables("research/events_2025_L13_shipped",
                          flight_list=flight_list_table("2025"))

    assert str(storage._s3_path("opdi_flight_list")).endswith("opdi_flight_list")


def test_the_flight_list_redirect_does_not_relax_its_write_mode(
    spark, pristine_storage
):
    """The redirect exists for a *read*. Forcing overwrite on it too would be a
    loaded gun pointed at whatever table it happens to name."""
    from opdi.config import OPDIConfig

    storage = pristine_storage(spark, OPDIConfig.for_environment("local"))
    seen = {}

    def record(self, df, table_name, mode="append"):
        seen[table_name] = mode

    # Patched *before* the redirect, so the redirect wraps this and the mode it
    # decided is what arrives here.
    pristine_storage.write_table = record
    redirect_event_tables("research/events_2026_V07_shipped",
                          flight_list=flight_list_table("2026"))
    df = spark.createDataFrame([(1,)], "x int")

    storage.write_table(df, "opdi_flight_list", "append")
    storage.write_table(df, "opdi_flight_events", "append")

    assert seen["opdi_flight_list"] == "append"
    assert seen["opdi_flight_events"] == "overwrite"


# ---------------------------------------------------------------------------
# Reading the written event table


def _event_rows(spark):
    """Two arrivals' worth of v0.2.0 events, in the shape step 04 writes."""
    import datetime as dt

    t = dt.datetime(2026, 6, 5, 10, 0, 0)
    rows = [
        # The A-CDM family names its runway `rwy_ident`, not `runway`.
        ("trk-1", "touchdown", t, 50.9, 4.5,
         '{"rwy_ident": "25L", "apt_icao": "EBBR", "milestone": "T17"}'),
        # ICAO T16: the threshold plane, which APDF does not record.
        ("trk-1", "landing", t - dt.timedelta(seconds=20), 50.9, 4.4,
         '{"rwy_ident": "25L", "apt_icao": "EBBR", "milestone": "T16"}'),
        # v0.1.0's key, still readable so the baseline rung scores.
        ("trk-2", "ALDT", t, 50.9, 4.5, '{"runway": "07R", "apt_icao": "EBBR"}'),
    ]
    df = spark.createDataFrame(
        rows, "flight_id string, type string, event_time timestamp, "
              "latitude double, longitude double, info string")
    df.createOrReplaceTempView("v4_events_under_test")
    tracks = spark.createDataFrame(
        [("trk-1", "ABC123", "BEL123 ", t), ("trk-2", "DEF456", "DLH8  ", t)],
        "track_id string, icao24 string, callsign string, event_time timestamp")
    return tracks


def test_the_reader_finds_the_runway_under_both_info_keys(spark):
    """`runway` is v0.1.0's key and `rwy_ident` the A-CDM family's. Reading
    only the first left every v0.2.0 designator NULL, which reads as 'the
    detector named no runway' rather than as a mis-read column."""
    tracks = _event_rows(spark)

    got = {
        r["det_type"]: r["det_runway"]
        for r in detected_events(
            spark, "v4_events_under_test", tracks=tracks, identity="tracks"
        ).collect()
    }

    assert got["touchdown"] == "25L"
    assert got["ALDT"] == "07R"


def test_the_shipped_mapping_drops_landing_from_the_scored_frame(spark):
    tracks = _event_rows(spark)
    shipped = build_plan(ladder="v4")["V07_shipped"]

    got = detected_events(
        spark, "v4_events_under_test", tracks=tracks, identity="tracks",
        mapping=milestone_map(shipped),
    ).collect()

    assert {r["det_type"] for r in got} == {"touchdown", "ALDT"}
    assert {r["milestone"] for r in got} == {"ALDT"}
    assert {r["icao24"] for r in got} == {"abc123", "def456"}
    assert {r["callsign"] for r in got} == {"BEL123", "DLH8"}


def test_the_baseline_mapping_keeps_landing_on_the_same_frame(spark):
    """The other half, against the same rows: under v0.1.0 `landing` *is*
    ground contact, so dropping it there would delete the baseline rung's ALDT
    evidence rather than protect it. One frame, both rungs, so the difference
    is the configuration and nothing else."""
    tracks = _event_rows(spark)
    baseline = build_plan(ladder="v4")["V00_v3_shipped"]

    got = detected_events(
        spark, "v4_events_under_test", tracks=tracks, identity="tracks",
        mapping=milestone_map(baseline),
    ).collect()

    landing = [r for r in got if r["det_type"] == "landing"]
    assert len(landing) == 1
    assert landing[0]["milestone"] == "ALDT"
