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
    """The guard must not suppress phases where every input is present."""
    samples = [
        {"t": i * 5, "baro_altitude": _m(35000), "vert_rate": 0.0,
         "velocity": 600 / KT_PER_MPS}
        for i in range(4)
    ]
    sdf = _measured(make_track(spark, samples))

    types = {r.type for r in calculate_horizontal_segment_events(sdf, EventConfig()).collect()}

    assert "top-of-climb" in types


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
