"""Tests for the ground movement milestones (T04 off-block, T21 on-block)."""

from conftest import _EPOCH, make_track
from pyspark.sql import functions as F

from opdi.config import EventConfig
from opdi.pipeline.ground import block_times, movement_window

KT_PER_MPS = 1.94384


def _kt(kt):
    return kt / KT_PER_MPS


def _speeds(spark, speeds_kt, step_s=10):
    return make_track(
        spark, [{"t": i * step_s, "velocity": _kt(v)} for i, v in enumerate(speeds_kt)]
    )


def _stand_events(spark, types):
    rows = [("trk-1", t, _EPOCH) for t in types]
    return spark.createDataFrame(rows, "track_id string, type string, event_time timestamp")


NM_PER_DEG = 60.0


def _positions(spark, leg_speeds_kt, step_s=10, start_lat=50.0, lon=4.0):
    """A track moving north along a meridian, one leg per entry in
    ``leg_speeds_kt`` (knots). ``velocity`` is NULL throughout, so
    ``movement_window`` has nothing to read but the position-derived
    fallback.
    """
    lat = start_lat
    samples = [{"t": 0, "lat": lat, "lon": lon, "velocity": None}]
    for i, kt in enumerate(leg_speeds_kt, start=1):
        distance_nm = kt * step_s / 3600.0
        lat += distance_nm / NM_PER_DEG
        samples.append({"t": i * step_s, "lat": lat, "lon": lon, "velocity": None})
    return make_track(spark, samples)


def test_sustained_movement_is_detected(spark):
    sdf = _speeds(spark, [0, 0, 8, 10, 12, 10, 9, 0, 0])

    got = movement_window(sdf, EventConfig()).collect()

    assert len(got) == 1
    assert (got[0].moving_start - _EPOCH).total_seconds() == 20
    assert (got[0].moving_stop - _EPOCH).total_seconds() == 60


def test_a_brief_jitter_is_not_movement(spark):
    """One sample above the threshold is a wobble in the speed field, not a
    push. At 5-10 s sampling that is not a rare occurrence."""
    sdf = _speeds(spark, [0, 0, 8, 0, 0, 0, 0])

    assert movement_window(sdf, EventConfig()).collect() == []


def test_a_stationary_aircraft_yields_nothing(spark):
    assert movement_window(_speeds(spark, [0, 0, 1, 0, 1, 0]), EventConfig()).collect() == []


def test_off_block_requires_having_left_a_stand(spark):
    """Without the anchor, a track picked up mid-taxi would report its first
    movement as an off-block -- a different event, and one that would read as a
    very short taxi rather than as a miss."""
    movements = movement_window(_speeds(spark, [0, 0, 8, 10, 12, 10, 0]), EventConfig())

    anchored = block_times(movements, _stand_events(spark, ["exit-parking_position"])).collect()
    unanchored = block_times(movements, _stand_events(spark, ["entry-runway"])).collect()

    assert anchored[0].aobt is not None
    assert unanchored == []


def test_on_block_requires_having_entered_a_stand(spark):
    movements = movement_window(_speeds(spark, [0, 0, 8, 10, 12, 10, 0]), EventConfig())

    got = block_times(movements, _stand_events(spark, ["entry-parking_position"])).collect()

    assert got[0].aibt is not None
    assert got[0].aobt is None


def test_a_turnaround_gets_both(spark):
    movements = movement_window(_speeds(spark, [0, 0, 8, 10, 12, 10, 0]), EventConfig())

    got = block_times(
        movements,
        _stand_events(spark, ["exit-parking_position", "entry-parking_position"]),
    ).collect()

    assert got[0].aobt is not None and got[0].aibt is not None


# ---------------------------------------------------------------------------
# Position-derived groundspeed (velocity absent or NULLed)
# ---------------------------------------------------------------------------


def test_position_derived_groundspeed_recovers_movement_when_velocity_is_null(spark):
    """`velocity` is NULL throughout -- the case cleaning's stale-broadcast
    mask produces for a genuinely stationary-then-pushing aircraft. The
    position-derived fallback must recover the movement anyway."""
    leg_speeds_kt = [0, 0, 10, 10, 10, 10, 10, 10, 0, 0]
    sdf = _positions(spark, leg_speeds_kt, step_s=10)

    got = movement_window(sdf, EventConfig()).collect()

    assert len(got) == 1
    assert (got[0].moving_start - _EPOCH).total_seconds() == 30
    assert (got[0].moving_stop - _EPOCH).total_seconds() == 80


def test_position_derived_groundspeed_is_off_under_legacy(spark):
    """`EventConfig.legacy()` must reproduce today's exact behaviour: with
    `velocity` NULL throughout and no fallback, nothing looks like movement
    at all."""
    leg_speeds_kt = [0, 0, 10, 10, 10, 10, 10, 10, 0, 0]
    sdf = _positions(spark, leg_speeds_kt, step_s=10)

    assert EventConfig.legacy().ground_speed_derive_from_position is False
    assert movement_window(sdf, EventConfig.legacy()).collect() == []


def test_position_derived_groundspeed_does_not_override_real_velocity(spark):
    """`coalesce(velocity_kt, derived_kt)`, not a replacement: wherever
    `velocity` is present it must still win, so a stationary aircraft with a
    spurious position jump but real velocity=0 does not read as moving."""
    sdf = _speeds(spark, [0, 0, 0, 0])
    # Force a large position jump that would look like huge derived speed,
    # while velocity stays firmly at 0.
    sdf = sdf.withColumn(
        "lat",
        F.when(F.col("event_time") == F.lit(_EPOCH), F.col("lat")).otherwise(
            F.col("lat") + F.lit(1.0)
        ),
    )

    assert movement_window(sdf, EventConfig()).collect() == []


# ---------------------------------------------------------------------------
# Block times from the ground part of the right leg
# ---------------------------------------------------------------------------

import datetime as dt  # noqa: E402

from opdi.pipeline.ground import block_events  # noqa: E402

FT_PER_M = 3.28084

_REPORT_DDL = (
    "track_id string, event_time timestamp, lat double, lon double, velocity double, "
    "baro_altitude_c double, on_ground boolean"
)


def _at(s):
    return _EPOCH + dt.timedelta(seconds=s)


def _leg(start_s, end_s, kt, alt_ft=None, on_ground=True, step_s=10):
    """Reports every ``step_s`` seconds from ``start_s`` up to ``end_s``."""
    alt_m = None if alt_ft is None else alt_ft / FT_PER_M
    return [(s, kt, alt_m, on_ground) for s in range(start_s, end_s, step_s)]


def _reports(spark, legs, track="trk-1"):
    rows = [
        (track, _at(s), 50.0, 4.0, _kt(kt), alt_m, gnd)
        for leg in legs
        for (s, kt, alt_m, gnd) in leg
    ]
    return spark.createDataFrame(rows, _REPORT_DDL)


def _stands(spark, rows, track="trk-1"):
    """``(type, seconds, stand, airport)`` as layout events."""
    import json

    return spark.createDataFrame(
        [
            (track, t, _at(s), json.dumps({"osm_ref": ref, "osm_airport": apt}))
            for t, s, ref, apt in rows
        ],
        "track_id string, type string, event_time timestamp, info string",
    )


def _runway(spark, rows, track="trk-1"):
    """``(type, seconds)`` as take-off and landing events."""
    return spark.createDataFrame(
        [(track, t, _at(s)) for t, s in rows],
        "track_id string, type string, event_time timestamp",
    )


def _blocks(spark, reports, stands, runway):
    rows = block_events(reports, stands, runway, EventConfig()).collect()
    return sorted(
        [
            (r.block, (r.event_time - _EPOCH).total_seconds(), r.stand, r.stand_airport)
            for r in rows
        ],
        key=lambda x: x[1],
    )


#: A departure from stand A1 at EBBR: parked, push-back, a stop to start the
#: engines, taxi, take-off at 600 s, climb away.
DEPARTURE = [
    _leg(0, 60, 0),
    _leg(60, 120, 5),
    _leg(120, 180, 0),
    _leg(180, 600, 15),
    _leg(600, 700, 160, alt_ft=4000, on_ground=False),
]
DEPARTURE_STANDS = [
    ("entry-parking_position", 0, "A1", "EBBR"),
    ("exit-parking_position", 100, "A1", "EBBR"),
]

#: An arrival at stand B2 at EDDF: in flight, landing at 1,000 s, taxi, parked.
ARRIVAL = [
    _leg(700, 1000, 250, alt_ft=6000, on_ground=False),
    _leg(1000, 1300, 15),
    _leg(1300, 1420, 0),
]
ARRIVAL_STANDS = [
    ("entry-parking_position", 1250, "B2", "EDDF"),
    ("exit-parking_position", 1410, "B2", "EDDF"),
]


def test_a_departure_gets_its_off_block_and_no_in_block(spark):
    """The stand passage gives an entry as well as an exit. Before, that entry
    made the last movement of the track, high in the climb, an in-block time."""
    got = _blocks(
        spark,
        _reports(spark, DEPARTURE),
        _stands(spark, DEPARTURE_STANDS),
        _runway(spark, [("ATOT", 600)]),
    )

    assert got == [("off", 60.0, "A1", "EBBR")]


def test_the_off_block_time_of_a_departure_does_not_move(spark):
    """For the event that was right before, the time is the same: the start of
    the first sustained movement, the push-back."""
    reports = _reports(spark, DEPARTURE)
    before = movement_window(reports, EventConfig()).collect()[0].moving_start
    got = _blocks(spark, reports, _stands(spark, DEPARTURE_STANDS), _runway(spark, [("ATOT", 600)]))

    assert got[0][1] == (before - _EPOCH).total_seconds()


def test_an_arrival_gets_its_in_block_and_no_off_block(spark):
    """Before, the first movement of the track, in flight, was its off-block time."""
    got = _blocks(
        spark,
        _reports(spark, ARRIVAL),
        _stands(spark, ARRIVAL_STANDS),
        _runway(spark, [("ALDT", 1000)]),
    )

    assert got == [("on", 1290.0, "B2", "EDDF")]


def test_a_flight_gets_the_stand_of_each_of_its_two_airports(spark):
    """Stand to stand: the off-block time names the stand it left at EBBR, the
    in-block time the stand it reached at EDDF. The two times are those of
    the earlier rule."""
    reports = _reports(spark, DEPARTURE + ARRIVAL)
    before = movement_window(reports, EventConfig()).collect()[0]
    got = _blocks(
        spark,
        reports,
        _stands(spark, DEPARTURE_STANDS + ARRIVAL_STANDS),
        _runway(spark, [("ATOT", 600), ("ALDT", 1000)]),
    )

    assert got == [("off", 60.0, "A1", "EBBR"), ("on", 1290.0, "B2", "EDDF")]
    assert (before.moving_start - _EPOCH).total_seconds() == 60.0
    assert (before.moving_stop - _EPOCH).total_seconds() == 1290.0


def test_an_arrival_then_a_departure_gives_both_in_that_order(spark):
    """One track: landing at 1,000 s, taxi to stand B2, a 40-minute stop,
    push-back at 3,700 s, taxi, take-off at 4,100 s. The in-block time is the
    end of the taxi-in, the off-block time the start of the push-back."""
    legs = ARRIVAL + [
        _leg(1420, 3700, 0, step_s=60),
        _leg(3700, 3760, 5),
        _leg(3760, 3820, 0),
        _leg(3820, 4100, 15),
        _leg(4100, 4300, 160, alt_ft=4000, on_ground=False),
    ]
    stands = [
        ("entry-parking_position", 1250, "B2", "EDDF"),
        ("exit-parking_position", 3740, "B2", "EDDF"),
    ]
    got = _blocks(
        spark,
        _reports(spark, legs),
        _stands(spark, stands),
        _runway(spark, [("ALDT", 1000), ("ATOT", 4100)]),
    )

    assert got == [("on", 1290.0, "B2", "EDDF"), ("off", 3700.0, "B2", "EDDF")]


def test_a_touch_and_go_gives_no_block_time(spark):
    """Landing and take-off 30 s apart, no stand: nothing."""
    legs = [
        _leg(0, 300, 140, alt_ft=1500, on_ground=False),
        _leg(300, 330, 120),
        _leg(330, 600, 140, alt_ft=1500, on_ground=False),
    ]
    got = _blocks(
        spark,
        _reports(spark, legs),
        _stands(spark, []),
        _runway(spark, [("ALDT", 300), ("ATOT", 330)]),
    )

    assert got == []


def test_without_take_off_or_landing_the_ground_part_at_each_end_is_used(spark):
    """A track seen only on the ground, with no take-off or landing time:
    both times, as before (it cannot be told which of the two it is)."""
    reports = _reports(spark, [_leg(0, 60, 0), _leg(60, 300, 12), _leg(300, 360, 0)])
    stands = [
        ("entry-parking_position", 0, "C3", "LFPG"),
        ("exit-parking_position", 80, "C3", "LFPG"),
    ]
    got = _blocks(spark, reports, _stands(spark, stands), _runway(spark, []))

    assert got == [("off", 60.0, "C3", "LFPG"), ("on", 290.0, "C3", "LFPG")]


def test_without_take_off_time_the_flight_does_not_give_an_off_block(spark):
    """An arrival whose landing time was not found: the track starts in flight,
    so there is no ground part at its start, and no off-block time."""
    got = _blocks(
        spark, _reports(spark, ARRIVAL), _stands(spark, ARRIVAL_STANDS), _runway(spark, [])
    )

    assert got == [("on", 1290.0, "B2", "EDDF")]


def test_a_report_without_altitude_counts_as_possibly_on_the_ground(spark):
    """A surface report carries no altitude and is often not flagged on the
    ground; its movement still counts."""
    legs = [_leg(0, 60, 0, on_ground=False), _leg(60, 300, 12, on_ground=False)]
    stands = [
        ("entry-parking_position", 0, "C3", "LFPG"),
        ("exit-parking_position", 80, "C3", "LFPG"),
    ]
    got = _blocks(spark, _reports(spark, legs), _stands(spark, stands), _runway(spark, []))

    assert ("off", 60.0, "C3", "LFPG") in got


def test_the_v002_settings_keep_the_earlier_block_times(spark):
    assert EventConfig.legacy().block_times_on_ground_only is False
    assert EventConfig().block_times_on_ground_only is True
