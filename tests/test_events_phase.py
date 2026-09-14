"""Tests for the phase-classification fixes in ``pipeline/events.py``.

The published detector has never had a test. These cover the two changes that
alter which events come out -- the smoothing OpenAP applies and the port
dropped, and the NULL handling that let an incomplete fuzzy rule win -- plus
the crossing path that now routes through ``pipeline/crossings.py``.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from conftest import make_track

from opdi.config import EventConfig
from opdi.pipeline.events import (
    calculate_horizontal_segment_events,
    calculate_threshold_crossing_events,
    _smooth_phase,
)

FT_PER_M = 3.28084
FTMIN_PER_MPS = 196.850394
KT_PER_MPS = 1.94384


def _m(feet):
    return feet / FT_PER_M


def _measured(sdf):
    """Add what step 02 adds and ``TRACK_SCHEMA`` does not.

    The shared fixture is shaped for the cleaning tests, so it carries the raw
    ``baro_altitude``; every event detector reads ``baro_altitude_c``, the
    rolling-mean repair step 02 writes, plus the cumulative measures.
    """
    return (
        sdf.withColumn("baro_altitude_c", F.col("baro_altitude"))
        .withColumn("cumulative_distance_nm", F.lit(0.0))
        .withColumn("cumulative_time_s", F.lit(0).cast("long"))
    )


def _labelled(spark, labels, step_s=5):
    """A track carrying an explicit phase label per sample, in order."""
    df = make_track(spark, [{"t": i * step_s} for i in range(len(labels))])
    return df.withColumn(
        "flight_phase",
        F.element_at(
            F.array(*[F.lit(x) for x in labels]),
            F.row_number()
            .over(Window.partitionBy("track_id").orderBy("event_time"))
            .cast("int"),
        ),
    )


# ---------------------------------------------------------------------------
# D2 -- the smoothing OpenAP applies and the port dropped
# ---------------------------------------------------------------------------

def test_smoothing_removes_a_single_flickering_sample(spark):
    """One stray label is exactly what injects a spurious level-start/end pair."""
    df = _labelled(spark, ["CL", "CL", "CL", "CL", "LVL", "CL", "CL", "CL", "CL"])

    out = _smooth_phase(df, 60.0).orderBy("event_time").collect()

    assert [r.flight_phase for r in out] == ["CL"] * 9


def test_smoothing_keeps_a_real_sustained_transition(spark):
    """A de-flicker must not erase a genuine phase change."""
    df = _labelled(spark, ["CL"] * 6 + ["CR"] * 6)

    out = _smooth_phase(df, 30.0).orderBy("event_time").collect()
    got = [r.flight_phase for r in out]

    assert got[0] == "CL" and got[-1] == "CR"
    assert got.count("CL") + got.count("CR") == 12
    # Exactly one transition survives -- no oscillation reintroduced.
    assert sum(1 for a, b in zip(got, got[1:]) if a != b) == 1


def test_smoothing_is_off_when_the_window_is_zero(spark):
    """``legacy()`` sets the window to zero and must be a pass-through."""
    df = make_track(spark, [{"t": i * 5} for i in range(5)]).withColumn(
        "flight_phase", F.lit("CL")
    )

    assert _smooth_phase(df, 0.0).collect() == df.collect()
    assert EventConfig.legacy().phase_twindow_seconds == 0.0


# ---------------------------------------------------------------------------
# D4 -- a rule with a NULL input must abstain, not win
# ---------------------------------------------------------------------------

def test_a_rule_with_a_missing_input_does_not_win(spark):
    """`F.least` skips NULLs, so a two-of-three rule can out-score a complete
    one. With the fix the incomplete rule yields NULL and abstains."""
    # Cruise-like altitude and vertical rate, but no velocity at all. Under the
    # published behaviour rule_cruise becomes min(alt_hi, roc_zero) and can
    # win on two terms; with the fix it cannot compete.
    samples = [
        {"t": i * 5, "baro_altitude": _m(35000), "vert_rate": 0.0, "velocity": None}
        for i in range(4)
    ]
    sdf = _measured(make_track(spark, samples))

    strict = calculate_horizontal_segment_events(sdf, EventConfig())
    loose = calculate_horizontal_segment_events(sdf, EventConfig.legacy())

    # The published path finds cruise here and therefore a TOC/TOD; the fixed
    # path declines to name a phase from incomplete evidence.
    assert {r.type for r in loose.collect()} >= {"top-of-climb"}
    assert strict.count() == 0


def test_a_complete_rule_still_wins(spark):
    """The guard must not suppress phases where every input is present.

    Asserted on ``level-start`` rather than on a top: the shipped
    configuration publishes the PRU tops from ``vertical_pru`` and this
    function no longer emits the fuzzy pair at all. The phase it finds is what
    is under test, and a cruise phase is what produces a level segment.
    """
    samples = [
        {"t": i * 5, "baro_altitude": _m(35000), "vert_rate": 0.0,
         "velocity": 600 / KT_PER_MPS}
        for i in range(4)
    ]
    sdf = _measured(make_track(spark, samples))

    types = {r.type for r in calculate_horizontal_segment_events(sdf, EventConfig()).collect()}

    assert "level-start" in types
    assert "top-of-climb" not in types, (
        "the fuzzy tops belong to vertical_pru now; two definitions of a top of "
        "climb in one table is what this release removes"
    )


# ---------------------------------------------------------------------------
# D8 -- the crossing path, wired through the new detector
# ---------------------------------------------------------------------------

def test_crossing_events_carry_sequence_and_direction(spark):
    profile = [9000, 9500, 10500, 11000, 10500, 9500, 9000, 9500, 10500, 11000]
    sdf = _measured(
        make_track(spark, [{"t": i * 5, "baro_altitude": _m(a)} for i, a in enumerate(profile)])
    )

    rows = [
        r for r in calculate_threshold_crossing_events(sdf, EventConfig()).collect()
        if r.type == "xing-fl100"
    ]
    rows.sort(key=lambda r: r.event_time)

    assert len(rows) == 3
    assert all(r.altitude_ft == 10000.0 for r in rows)
    import json

    info = [json.loads(r.info) for r in rows]
    assert [i["crossing_seq"] for i in info] == [1, 2, 3]
    assert [i["direction"] for i in info] == ["up", "down", "up"]


def test_crossing_events_emit_the_published_type_shape(spark):
    """One type per level, per the agreed schema -- not per direction."""
    sdf = _measured(
        make_track(spark, [{"t": 0, "baro_altitude": _m(4000)},
                           {"t": 5, "baro_altitude": _m(11000)},
                           {"t": 10, "baro_altitude": _m(12000)}])
    )

    types = {r.type for r in calculate_threshold_crossing_events(sdf, EventConfig()).collect()}

    assert types == {"xing-fl50", "xing-fl70", "xing-fl100"}


# ---------------------------------------------------------------------------
# D1 -- ground membership measured above field elevation
# ---------------------------------------------------------------------------

#: The phase family with the A-CDM runway milestones switched off.
#:
#: ``take-off`` is the *ground-contact* reading of the runway, and under the
#: shipped ``EventConfig()`` it is retired: ``runway_ops`` publishes the same
#: physical event as ``airborne``, interpolated to the 15 ft crossing, and
#: emitting both would put two answers to one question in the table. These
#: tests are about ground *membership* -- whether a sample at a 5,000 ft field
#: can be classified GND at all -- so they need the arm on which that decision
#: is visible, which is the v0.1.0 baseline rung rather than the default.
GROUND_CONTACT = EventConfig(emit_runway_milestones=False)


def _departure(spark, field_elev_ft, with_elevation=True):
    """A departure from a field at `field_elev_ft`: a run on the ground, then a
    climb. GND -> CL is what `take-off` looks for.

    Both legs are 75 s, comfortably longer than the 60 s smoothing window. A
    shorter track is not a smaller version of this test -- it is a different
    one, because a window wider than the track makes the majority label swallow
    the transition and no take-off can exist regardless of elevation.
    """
    on_ground = [field_elev_ft] * 15
    climb = [field_elev_ft + 400 * (i + 1) for i in range(15)]
    sdf = _measured(
        make_track(
            spark,
            [
                {"t": i * 5, "baro_altitude": _m(a),
                 "vert_rate": 0.0 if i < len(on_ground) else 12.0,
                 "velocity": (20 if i < len(on_ground) else 250) / KT_PER_MPS}
                for i, a in enumerate(on_ground + climb)
            ],
        )
    )
    if with_elevation:
        sdf = sdf.withColumn("elev_adep_ft", F.lit(float(field_elev_ft))).withColumn(
            "elev_ades_ft", F.lit(None).cast("double")
        )
    return sdf


def test_a_high_elevation_departure_gets_no_takeoff_without_the_fix(spark):
    """The defect, demonstrated. At a field above the 200 ft ceiling the
    published detector can never classify a sample as GND, so `take-off`
    -- which requires prev_phase == GND -- is never emitted at all."""
    sdf = _departure(spark, 5000, with_elevation=False)

    types = {r.type for r in calculate_horizontal_segment_events(sdf, EventConfig.legacy()).collect()}

    assert "take-off" not in types


def test_the_same_departure_yields_a_takeoff_with_the_fix(spark):
    sdf = _departure(spark, 5000)

    types = {r.type for r in calculate_horizontal_segment_events(sdf, GROUND_CONTACT).collect()}

    assert "take-off" in types


def test_a_sea_level_departure_is_unaffected(spark):
    """The fix must not change what already worked."""
    with_fix = {
        r.type for r in calculate_horizontal_segment_events(_departure(spark, 0), GROUND_CONTACT).collect()
    }

    assert "take-off" in with_fix


def test_a_missing_elevation_degrades_to_the_published_behaviour(spark):
    """A flight whose aerodrome the flight list never named must keep the
    phases it had, not lose them."""
    sdf = _departure(spark, 0).withColumn("elev_adep_ft", F.lit(None).cast("double"))

    types = {r.type for r in calculate_horizontal_segment_events(sdf, GROUND_CONTACT).collect()}

    assert "take-off" in types


def test_the_arrival_end_elevation_also_counts(spark):
    """Both ends are attached because a track is on the ground at one of them;
    a landing at a high field must resolve against ADES, not ADEP."""
    sdf = _departure(spark, 5000).withColumn(
        "elev_adep_ft", F.lit(None).cast("double")
    ).withColumn("elev_ades_ft", F.lit(5000.0))

    types = {r.type for r in calculate_horizontal_segment_events(sdf, GROUND_CONTACT).collect()}

    assert "take-off" in types


# ---------------------------------------------------------------------------
# The level-segment defect


def _cruise_traj(n_cruise, step=60):
    """Climb, ``n_cruise`` samples of cruise, descend.

    The sample step is 60 s so a single cruise sample survives
    ``phase_twindow_seconds`` smoothing -- at 5 s it is erased as a flicker and
    the trajectory never reaches cruise at all, which is why an earlier version
    of these tests measured nothing. The altitudes and speeds are chosen to
    satisfy the fuzzy classifier's cruise rule outright: the detector recomputes
    ``flight_phase`` from ``baro_altitude_c``/``vert_rate``/``velocity`` and
    ignores any label attached to the frame, so the phase has to be driven
    through the physics rather than asserted.
    """
    samples, t = [], 0
    for alt in range(10000, 35000, 5000):
        samples.append({"t": t, "baro_altitude": _m(alt),
                        "vert_rate": _m(2000) / 60, "velocity": 300 / KT_PER_MPS})
        t += step
    for _ in range(n_cruise):
        samples.append({"t": t, "baro_altitude": _m(35000),
                        "vert_rate": 0.0, "velocity": 600 / KT_PER_MPS})
        t += step
    for alt in range(30000, 5000, -5000):
        samples.append({"t": t, "baro_altitude": _m(alt),
                        "vert_rate": -_m(2000) / 60, "velocity": 300 / KT_PER_MPS})
        t += step
    return samples


def _counts(spark, samples, config):
    out = calculate_horizontal_segment_events(
        _measured(make_track(spark, samples)), config
    )
    types = [r.type for r in out.collect()]
    return {t: types.count(t) for t in
            ("level-start", "level-end", "top-of-climb", "top-of-descent")}


def test_a_one_sample_cruise_emits_both_a_start_and_an_end(spark):
    """The defect, and the fix, in one trajectory.

    A cruise one sample long is simultaneously the start and the end of a level
    segment. The published chain was first-match-wins with four things to say in
    one slot, so this sample matched the "level-start, top-of-climb" branch and
    could never reach the "level-end, top-of-descent" one. Measured over
    2026-06-01 that cost 28,855 unmatched starts across 17,356 of 44,461
    flights, and only 22% of them fell near ``last_seen`` -- the rest were
    mid-flight, which is where one-sample segments live.
    """
    got = _counts(spark, _cruise_traj(1), EventConfig())

    assert got["level-start"] == got["level-end"] == 1


def test_the_published_chain_loses_that_end_and_its_top_of_descent(spark):
    """The old behaviour, pinned rather than assumed.

    Without this nothing distinguishes "fixed" from "never broken". Note what
    is lost: the precedence cost the ``level-end`` *and* the ``top-of-descent``
    together, because both lived in the same unreachable branch.
    """
    got = _counts(spark, _cruise_traj(1), EventConfig(emit_runway_milestones=False))

    assert got["level-start"] == 1
    assert got["level-end"] == 0
    assert got["top-of-climb"] == 1
    assert got["top-of-descent"] == 0


def test_a_longer_cruise_was_never_affected(spark):
    """Two cruise samples put the start and the end on different rows, so the
    precedence never bit. The fix must leave this case exactly as it was."""
    shipped = _counts(spark, _cruise_traj(2), EventConfig())
    published = _counts(spark, _cruise_traj(2), EventConfig(emit_runway_milestones=False))

    assert shipped["level-start"] == shipped["level-end"] == 1
    assert published["level-start"] == published["level-end"] == 1


def test_the_shipped_configuration_publishes_no_fuzzy_top(spark):
    """The tops come from ``vertical_pru`` now. Two definitions of a top of
    climb in one table, under one name, is what this release removes."""
    got = _counts(spark, _cruise_traj(2), EventConfig())

    assert got["top-of-climb"] == got["top-of-descent"] == 0
