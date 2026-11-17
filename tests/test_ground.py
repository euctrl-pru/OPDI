"""Tests for the ground movement milestones: off-block (``AOBT``) and in-block (``AIBT``) times."""

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


def _reports(spark, legs, track="trk-1", moving=False):
    """The reports of ``legs``, all at one position, or with ``moving`` at
    positions that advance north at each report's speed."""
    rows, lat, last = [], 50.0, None
    for leg in legs:
        for s, kt, alt_m, gnd in leg:
            if moving and last is not None:
                lat += kt * (s - last) / 3600.0 / NM_PER_DEG
            last = s
            rows.append((track, _at(s), lat, 4.0, _kt(kt), alt_m, gnd))
    return spark.createDataFrame(rows, _REPORT_DDL)


def _stands(spark, rows, track="trk-1"):
    """``(type, seconds, stand, airport)`` as layout events.

    The time in the stand's cells (``opdi_time_in_use_s``) is the time from
    the entry to the exit of the same stand where one of each is given, else
    the fifth value of the row, else 60 s.
    """
    import json

    times = {}
    for row in rows:
        times.setdefault((row[2], row[3]), {}).setdefault(row[0], []).append(row[1])

    def dwell(row):
        if len(row) > 4:
            return row[4]
        pair = times[(row[2], row[3])]
        entries = pair.get("entry-parking_position", [])
        exits = pair.get("exit-parking_position", [])
        if len(entries) == 1 and len(exits) == 1:
            return exits[0] - entries[0]
        return 60

    return spark.createDataFrame(
        [
            (
                track,
                row[0],
                _at(row[1]),
                json.dumps(
                    {"osm_ref": row[2], "osm_airport": row[3], "opdi_time_in_use_s": dwell(row)}
                ),
            )
            for row in rows
        ],
        "track_id string, type string, event_time timestamp, info string",
    )


def _runway(spark, rows, track="trk-1"):
    """``(type, seconds)`` as take-off and landing events."""
    return spark.createDataFrame(
        [(track, t, _at(s)) for t, s in rows],
        "track_id string, type string, event_time timestamp",
    )


def _blocks(spark, reports, stands, runway, config=None):
    rows = block_events(reports, stands, runway, config or EventConfig()).collect()
    return sorted(
        [
            (r.block, (r.event_time - _EPOCH).total_seconds(), r.stand, r.stand_airport)
            for r in rows
        ],
        key=lambda x: x[1],
    )


#: Settings that give block times to tracks that never leave the ground, for
#: the tests of the ground parts that use such tracks.
_GROUND_ONLY = EventConfig(block_time_needs_flight=False)

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
    """The stand passage gives an entry as well as an exit. That entry must
    not make the last movement of the track, high in the climb, an in-block
    time."""
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
    """The first movement of the track, in flight, is not an off-block time."""
    got = _blocks(
        spark,
        _reports(spark, ARRIVAL),
        _stands(spark, ARRIVAL_STANDS),
        _runway(spark, [("ALDT", 1000)]),
    )

    assert got == [("on", 1290.0, "B2", "EDDF")]


def test_a_flight_gets_the_stand_of_each_of_its_two_airports(spark):
    """Stand to stand: the off-block time names the stand it left at EBBR, the
    in-block time the stand it reached at EDDF. The two times are the first
    and last sustained movement of the whole track (``movement_window``)."""
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
    both times (it cannot be told which of the two it is), when such tracks
    are given block times (``block_time_needs_flight`` off)."""
    reports = _reports(spark, [_leg(0, 60, 0), _leg(60, 300, 12), _leg(300, 360, 0)])
    stands = [
        ("entry-parking_position", 0, "C3", "LFPG"),
        ("exit-parking_position", 80, "C3", "LFPG"),
        ("entry-parking_position", 290, "C4", "LFPG", 60),
    ]
    got = _blocks(spark, reports, _stands(spark, stands), _runway(spark, []), _GROUND_ONLY)

    assert got == [("off", 60.0, "C3", "LFPG"), ("on", 290.0, "C4", "LFPG")]


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
    got = _blocks(
        spark, _reports(spark, legs), _stands(spark, stands), _runway(spark, []), _GROUND_ONLY
    )

    assert ("off", 60.0, "C3", "LFPG") in got


def test_the_v002_settings_use_the_movement_of_the_whole_track(spark):
    assert EventConfig.legacy().block_times_on_ground_only is False
    assert EventConfig().block_times_on_ground_only is True


def test_a_landing_that_was_not_found_after_the_last_take_off_still_gives_the_in_block(
    spark,
):
    """Arrival at EDDF (landing found), departure (take-off found), then an
    arrival at a second airport whose landing time was not found: the ground
    part after the last report in flight gives the in-block time of that
    last arrival."""
    legs = ARRIVAL + [
        _leg(1420, 3700, 0, step_s=60),
        _leg(3700, 4100, 15),
        _leg(4100, 6000, 250, alt_ft=9000, on_ground=False),
        _leg(6000, 6300, 15),
        _leg(6300, 6400, 0),
    ]
    stands = ARRIVAL_STANDS + [("entry-parking_position", 6250, "C3", "LFPG")]
    got = _blocks(
        spark,
        _reports(spark, legs),
        _stands(spark, stands),
        _runway(spark, [("ALDT", 1000), ("ATOT", 4100)]),
    )

    assert ("on", 6290.0, "C3", "LFPG") in got


def _with_glitch(legs, at_s):
    """The report at ``at_s`` replaced by one at 16,000 ft, not on the ground:
    a garbled altitude in an airborne-format message."""
    out = []
    for leg in legs:
        out.append(
            [
                (s, kt, 16000 / FT_PER_M, False) if s == at_s else (s, kt, a, g)
                for s, kt, a, g in leg
            ]
        )
    return out


def test_a_garbled_altitude_on_the_ground_does_not_split_the_movement(spark):
    """Two reports at 16,000 ft in the middle of the push-back: the off-block
    time stays at the start of the push-back."""
    legs = _with_glitch(_with_glitch(DEPARTURE, 80), 90)
    got = _blocks(
        spark,
        _reports(spark, legs),
        _stands(spark, DEPARTURE_STANDS),
        _runway(spark, [("ATOT", 600)]),
    )

    assert got == [("off", 60.0, "A1", "EBBR")]


def test_a_garbled_altitude_on_the_ground_does_not_end_the_ground_part(spark):
    """A ground track without take-off or landing time, with a garbled report
    near its end: it still gets both times."""
    legs = _with_glitch([_leg(0, 60, 0), _leg(60, 300, 12), _leg(300, 360, 0)], 330)
    stands = [
        ("entry-parking_position", 0, "C3", "LFPG"),
        ("exit-parking_position", 80, "C3", "LFPG"),
        ("entry-parking_position", 290, "C4", "LFPG", 60),
    ]
    got = _blocks(
        spark, _reports(spark, legs), _stands(spark, stands), _runway(spark, []), _GROUND_ONLY
    )

    assert got == [("off", 60.0, "C3", "LFPG"), ("on", 290.0, "C4", "LFPG")]


def test_reports_without_position_do_not_count_as_flight(spark):
    """A parked aircraft whose last reports carry no position and a stale
    altitude of 15,000 ft: they do not end the ground part, so the in-block
    time of the arrival is kept."""
    legs = ARRIVAL + [[(s, None, 15000 / FT_PER_M, False) for s in range(1420, 1600, 5)]]
    rows = [
        (
            "trk-1",
            _at(s),
            None if s >= 1420 else 50.0,
            None if s >= 1420 else 4.0,
            None if kt is None else _kt(kt),
            a,
            g,
        )
        for leg in legs
        for (s, kt, a, g) in leg
    ]
    reports = spark.createDataFrame(rows, _REPORT_DDL)
    got = _blocks(spark, reports, _stands(spark, ARRIVAL_STANDS), _runway(spark, []))

    assert got == [("on", 1290.0, "B2", "EDDF")]


def _runway_with_method(spark, rows, track="trk-1"):
    """``(type, seconds, method)`` as take-off and landing events with ``info``."""
    import json

    return spark.createDataFrame(
        [(track, t, _at(s), json.dumps({"method": m})) for t, s, m in rows],
        "track_id string, type string, event_time timestamp, info string",
    )


def test_each_block_time_names_the_runway_time_of_its_own_leg(spark):
    """The off-block time carries the take-off after it, the in-block time the
    landing before it, each with the detector that found it; the
    plausibility checks compare the block time with these, not with the one
    take-off and one landing step 04 keeps per track."""
    import json

    from opdi.pipeline.events import calculate_block_events

    rows = calculate_block_events(
        _reports(spark, DEPARTURE + ARRIVAL),
        _stands(spark, DEPARTURE_STANDS + ARRIVAL_STANDS),
        EventConfig(),
        _runway_with_method(spark, [("ATOT", 600, "legacy"), ("ALDT", 1000, "acdm")]),
    ).collect()
    info = {r.type: json.loads(r.info) for r in rows}

    assert info["AOBT"]["take_off"] == "2024-06-01T12:10:00.000000Z"
    assert info["AOBT"]["take_off_method"] == "legacy"
    assert info["AIBT"]["landing"] == "2024-06-01T12:16:40.000000Z"
    assert info["AIBT"]["landing_method"] == "acdm"


def test_a_block_time_at_the_start_of_a_track_names_no_runway_time(spark):
    """A track seen only on the ground: its block times have no take-off or
    landing beside them."""
    import json

    from opdi.pipeline.events import calculate_block_events

    reports = _reports(spark, [_leg(0, 60, 0), _leg(60, 300, 12), _leg(300, 360, 0)])
    stands = [
        ("entry-parking_position", 0, "C3", "LFPG"),
        ("exit-parking_position", 80, "C3", "LFPG"),
        ("entry-parking_position", 290, "C4", "LFPG", 60),
    ]
    rows = calculate_block_events(
        reports, _stands(spark, stands), _GROUND_ONLY, _runway_with_method(spark, [])
    ).collect()

    assert len(rows) == 2
    for r in rows:
        assert not {"take_off", "landing"} & set(json.loads(r.info))


def test_a_landing_and_a_take_off_at_the_same_instant_are_ordered_landing_first(spark):
    """Two runway times at one instant sort the same way on every run, the
    landing first, as in a touch-and-go: the track then has no ground part
    that starts at a landing and ends at the track's end, and no ground part
    that starts at the track's start and ends at a take-off, so it gives no
    block time, whatever order the rows come in."""
    got = []
    for order in (
        [("ATOT", 1000, "legacy"), ("ALDT", 1000, "acdm")],
        [("ALDT", 1000, "acdm"), ("ATOT", 1000, "legacy")],
    ):
        rows = block_events(
            _reports(spark, ARRIVAL),
            _stands(spark, ARRIVAL_STANDS),
            _runway_with_method(spark, order),
            EventConfig(),
        ).collect()
        got.append(sorted((r.block, r.event_time) for r in rows))

    assert got == [[], []]


# ---------------------------------------------------------------------------
# A block time is the start (end) of a movement from (to) a standstill
# ---------------------------------------------------------------------------

#: The arrival of a flight whose stand at the destination is the only one seen.
_ARRIVAL_AT_B2 = [_leg(900, 1000, 140, alt_ft=6000, on_ground=False)] + ARRIVAL[1:]


def test_the_settings_of_the_standstill_condition():
    assert EventConfig().block_time_max_start_speed_kt == 30.0


def test_a_flight_first_received_after_lift_off_gives_no_off_block(spark):
    """First report at 300 ft and 150 kt, just after lift-off; the runway
    window gives the take-off at that report. The climb below 2,000 ft is a
    movement from the first report, but the aircraft was already moving fast
    when first received: its off-block time was not observed."""
    legs = [
        _leg(0, 60, 150, alt_ft=300, on_ground=False),
        _leg(60, 900, 250, alt_ft=20000, on_ground=False),
    ] + _ARRIVAL_AT_B2
    got = _blocks(
        spark,
        _reports(spark, legs, moving=True),
        _stands(spark, ARRIVAL_STANDS),
        _runway(spark, [("ATOT", 0), ("ALDT", 1000)]),
    )

    assert got == [("on", 1290.0, "B2", "EDDF")]


def test_a_flight_first_received_on_the_take_off_roll_gives_no_off_block(spark):
    """First reports on the runway at 90 kt, flagged on the ground and
    without altitude; lift-off 20 s later."""
    legs = [
        _leg(0, 20, 90, step_s=5),
        _leg(20, 60, 150, alt_ft=300, on_ground=False),
        _leg(60, 900, 250, alt_ft=20000, on_ground=False),
    ] + _ARRIVAL_AT_B2
    got = _blocks(
        spark,
        _reports(spark, legs, moving=True),
        _stands(spark, ARRIVAL_STANDS),
        _runway(spark, [("ATOT", 20), ("ALDT", 1000)]),
    )

    assert got == [("on", 1290.0, "B2", "EDDF")]


def test_an_arrival_first_received_on_final_gives_no_off_block(spark):
    """First report on final approach at 1,200 ft and 140 kt, landing at
    1,000 s: no off-block time in the air."""
    legs = [_leg(900, 1000, 140, alt_ft=1200, on_ground=False)] + ARRIVAL[1:]
    got = _blocks(
        spark,
        _reports(spark, legs, moving=True),
        _stands(spark, ARRIVAL_STANDS),
        _runway(spark, [("ALDT", 1000)]),
    )

    assert got == [("on", 1290.0, "B2", "EDDF")]


def test_a_departure_ending_in_the_climb_below_2000_ft_gives_no_in_block(spark):
    """The track ends at 1,200 ft and 160 kt, still climbing: the last
    movement ends at the last report while fast, so no in-block time."""
    legs = DEPARTURE[:-1] + [_leg(600, 700, 160, alt_ft=1200, on_ground=False)]
    got = _blocks(
        spark,
        _reports(spark, legs, moving=True),
        _stands(spark, DEPARTURE_STANDS),
        _runway(spark, [("ATOT", 600)]),
    )

    assert got == [("off", 60.0, "A1", "EBBR")]


def test_a_departure_ending_in_cruise_with_reports_without_position_gives_no_in_block(spark):
    """The track ends in cruise at 35,000 ft and 450 kt with 60 s of reports
    that carry an altitude but no position: they say nothing about the
    ground, so they give no movement on it."""
    legs = DEPARTURE[:-1] + [_leg(600, 1500, 450, alt_ft=35000, on_ground=False)]
    rows = [
        ("trk-1", _at(s), 50.0, 4.0, _kt(kt), a, g) for leg in legs for (s, kt, a, g) in leg
    ] + [
        ("trk-1", _at(s), None, None, _kt(450), 35000 / FT_PER_M, False)
        for s in range(1500, 1570, 10)
    ]
    got = _blocks(
        spark,
        spark.createDataFrame(rows, _REPORT_DDL),
        _stands(spark, DEPARTURE_STANDS),
        _runway(spark, [("ATOT", 600)]),
    )

    assert got == [("off", 60.0, "A1", "EBBR")]


def test_a_wait_at_the_holding_point_between_two_circuits_is_not_a_stand_stop(spark):
    """A training flight: leaves stand A1, take-off at 600 s, full-stop
    landing at 1,200 s, taxi back, a 200 s wait at the holding point, take-off
    at 1,700 s, landing at 2,300 s, parks at A1. No stand lies between the
    two flights, so the wait gives no in-block and no off-block time."""
    legs = [
        _leg(0, 60, 0),
        _leg(60, 600, 15),
        _leg(600, 1200, 100, alt_ft=3000, on_ground=False),
        _leg(1200, 1400, 15),
        _leg(1400, 1600, 0),
        _leg(1600, 1700, 15),
        _leg(1700, 2300, 100, alt_ft=3000, on_ground=False),
        _leg(2300, 2500, 15),
        _leg(2500, 2600, 0),
    ]
    stands = [
        ("entry-parking_position", 0, "A1", "EBBR"),
        ("exit-parking_position", 70, "A1", "EBBR"),
        ("entry-parking_position", 2490, "A1", "EBBR"),
    ]
    got = _blocks(
        spark,
        _reports(spark, legs),
        _stands(spark, stands),
        _runway(spark, [("ATOT", 600), ("ALDT", 1200), ("ATOT", 1700), ("ALDT", 2300)]),
    )

    assert got == [("off", 60.0, "A1", "EBBR"), ("on", 2490.0, "A1", "EBBR")]


def test_a_stale_reported_speed_on_the_stand_does_not_hide_the_in_block(spark):
    """The reported speed of the last reports repeats 100 kt while the
    positions stand still at the stand: the position-derived speed says the
    aircraft stopped, and the in-block time is kept."""
    legs = ARRIVAL[:-1]
    rows = [
        ("trk-1", _at(s), 50.0 + s * 1e-5, 4.0, _kt(kt), a, g)
        for leg in legs
        for (s, kt, a, g) in leg
    ]
    # At the stand: the positions do not move, the reported speed is stale.
    rows += [
        ("trk-1", _at(s), 50.0 + 1300 * 1e-5, 4.0, _kt(100), None, True)
        for s in range(1300, 1330, 10)
    ]
    got = _blocks(
        spark,
        spark.createDataFrame(rows, _REPORT_DDL),
        _stands(spark, ARRIVAL_STANDS),
        _runway(spark, [("ALDT", 1000)]),
    )

    assert [g[0] for g in got] == ["on"]


def _ground_flag(spark, alt_ft, d_dep_nm, d_arr_nm, lat=50.0):
    from opdi.pipeline.ground import on_or_near_ground

    df = spark.createDataFrame(
        [(lat, alt_ft / FT_PER_M, False, 100.0, 5000.0, d_dep_nm, d_arr_nm)],
        "lat double, baro_altitude_c double, on_ground boolean, elev_adep_ft double, "
        "elev_ades_ft double, dist_adep_nm double, dist_ades_nm double",
    )
    return df.select(on_or_near_ground(df, EventConfig()).alias("g")).first().g


def test_the_height_is_measured_above_the_airport_of_the_leg(spark):
    """Departure from a 100 ft airport to one at 5,000 ft. At 6,500 ft next to
    the departure airport the aircraft is 6,400 ft above it: in flight. The
    same altitude next to the 5,000 ft airport is 1,500 ft above it: possibly
    on the ground there."""
    assert _ground_flag(spark, 6500, 1.0, 60.0) is False
    assert _ground_flag(spark, 6500, 60.0, 1.0) is True


def test_a_report_without_position_counts_as_ground_only_when_flagged_or_without_altitude(spark):
    from opdi.pipeline.ground import on_or_near_ground

    df = spark.createDataFrame(
        [(None, 35000 / FT_PER_M, False), (None, 35000 / FT_PER_M, True), (None, None, False)],
        "lat double, baro_altitude_c double, on_ground boolean",
    )
    got = [r.g for r in df.select(on_or_near_ground(df, EventConfig()).alias("g")).collect()]
    assert got == [False, True, True]


def test_without_runway_times_no_block_time_names_one(spark):
    """The runway families not computed in the call (``runway_events`` None):
    the block times have no take-off or landing beside them, and the order
    checks then keep them."""
    rows = block_events(
        _reports(spark, DEPARTURE), _stands(spark, DEPARTURE_STANDS), None, EventConfig()
    ).collect()
    assert rows and all(r.bound_time is None and r.bound_method is None for r in rows)


# ---------------------------------------------------------------------------
# Movement across reports whose speed is not known (work package 3, item 1)
# ---------------------------------------------------------------------------


def _surface(start_s, end_s, kt, step_s=10, positions="all", reported=False, on_ground=True):
    """Surface reports every ``step_s`` seconds: ``positions`` says which carry
    a position ("all", "none", or "alternate": every second one, starting
    with one without), ``reported`` whether they carry the reported speed.
    No altitude, as on most surface reports."""
    out = []
    for i, s in enumerate(range(start_s, end_s, step_s)):
        has_pos = positions == "all" or (positions == "alternate" and i % 2 == 1)
        out.append((s, kt, has_pos, reported, on_ground, None))
    return out


def _airborne(start_s, end_s, kt, alt_ft, step_s=10):
    return [(s, kt, True, True, False, alt_ft / FT_PER_M) for s in range(start_s, end_s, step_s)]


def _sparse(spark, legs, track="trk-1"):
    """Reports advancing north at each report's speed; the position and the
    reported speed are left out where the leg says so."""
    rows, lat, last = [], 50.0, None
    for leg in legs:
        for s, kt, has_pos, reported, gnd, alt_m in leg:
            if last is not None:
                lat += kt * (s - last) / 3600.0 / NM_PER_DEG
            last = s
            rows.append(
                (
                    track,
                    _at(s),
                    lat if has_pos else None,
                    4.0 if has_pos else None,
                    _kt(kt) if reported else None,
                    alt_m,
                    gnd,
                )
            )
    return spark.createDataFrame(rows, _REPORT_DDL)


def _blocks_with(spark, reports, stands, runway, config):
    rows = block_events(reports, stands, runway, config).collect()
    return sorted(
        [(r.block, (r.event_time - _EPOCH).total_seconds()) for r in rows], key=lambda x: x[1]
    )


_NO_BRIDGE = EventConfig(ground_movement_through_reports_without_speed=False)


def test_the_setting_of_movement_through_reports_without_speed():
    assert EventConfig().ground_movement_through_reports_without_speed is True
    assert EventConfig().ground_speed_max_position_gap_seconds == 30.0
    assert EventConfig.legacy().ground_movement_through_reports_without_speed is False


def test_a_taxi_whose_positions_alternate_with_reports_without_position_is_movement(spark):
    """AOBT#14 (LEZL) and AIBT#17 (YMML): every second surface report has no
    position and none has a reported speed. The speed derived from the
    previous positioned report, 10 s earlier, shows the taxi; the off-block
    time is the first positioned report of the push, at 65 s."""
    legs = [
        _surface(0, 60, 0, step_s=5, positions="alternate"),
        _surface(60, 400, 15, step_s=5, positions="alternate"),
        _airborne(400, 600, 160, 3000),
    ]
    stands = _stands(spark, DEPARTURE_STANDS)
    runway = _runway(spark, [("ATOT", 400)])

    assert _blocks_with(spark, _sparse(spark, legs), stands, runway, EventConfig()) == [
        ("off", 65.0)
    ]
    assert _blocks_with(spark, _sparse(spark, legs), stands, runway, _NO_BRIDGE) == []


def test_a_silence_of_positions_during_the_taxi_does_not_cut_the_push_back_off(spark):
    """AOBT#14 (LEZL): a slow push seen on two positioned reports, then 70 s
    of reports without position, then the taxi. The push is shorter than the
    30 s a movement must last, so without the bridge the off-block time is
    the start of the taxi after the silence (150 s); with it, the start of
    the push (60 s)."""
    legs = [
        _surface(0, 60, 0),
        _surface(60, 80, 4),
        _surface(80, 150, 4, positions="none"),
        _surface(150, 400, 15),
        _airborne(400, 600, 160, 3000),
    ]
    stands = _stands(spark, DEPARTURE_STANDS)
    runway = _runway(spark, [("ATOT", 400)])

    assert _blocks_with(spark, _sparse(spark, legs), stands, runway, EventConfig()) == [
        ("off", 60.0)
    ]
    assert _blocks_with(spark, _sparse(spark, legs), stands, runway, _NO_BRIDGE) == [("off", 150.0)]


def test_an_in_block_is_the_end_of_the_taxi_and_not_of_the_landing_roll(spark):
    """AIBT#16 (EGCC): the landing roll carries reported speeds, the taxi-in
    only positions every second report. Without the bridge the roll is the
    last movement and its end the in-block time; with it, the end of the
    taxi at the stand (the last positioned report before the stand, 1,295 s,
    is already slow in the median of three)."""
    legs = [
        _airborne(700, 1000, 140, 1500),
        _surface(1000, 1040, 100, step_s=5, reported=True),
        _surface(1040, 1300, 15, step_s=5, positions="alternate"),
        _surface(1300, 1420, 0),
    ]
    stands = _stands(spark, ARRIVAL_STANDS)
    runway = _runway(spark, [("ALDT", 1000)])

    # Without the bridge, and without the rule that drops an in-block time
    # next to the landing.
    before = EventConfig(
        ground_movement_through_reports_without_speed=False, block_time_off_the_runway=False
    )
    new = _blocks_with(spark, _sparse(spark, legs), stands, runway, EventConfig())
    old = _blocks_with(spark, _sparse(spark, legs), stands, runway, before)

    assert new == [("on", 1285.0)]
    assert old == [("on", 1040.0)]


def test_a_stop_shown_by_positions_still_ends_the_movement(spark):
    """A real stop gives positioned reports that do not move: the movement
    ends there, so the in-block time is the end of the taxi, not the last
    report at the stand."""
    legs = [
        _airborne(700, 1000, 140, 1500),
        _surface(1000, 1300, 15),
        _surface(1300, 1420, 0),
    ]
    got = _blocks_with(
        spark,
        _sparse(spark, legs),
        _stands(spark, ARRIVAL_STANDS),
        _runway(spark, [("ALDT", 1000)]),
        EventConfig(),
    )

    assert got == [("on", 1290.0)]


def test_reports_without_position_at_the_stand_do_not_extend_the_movement(spark):
    """After the taxi-in, 20 minutes of reports without position at the stand
    (the aircraft is parked; nothing tells its speed): the in-block time is
    the last report that showed movement, not the last report of the track."""
    legs = [
        _airborne(700, 1000, 140, 1500),
        _surface(1000, 1300, 15),
        _surface(1300, 2500, 0, positions="none"),
    ]
    got = _blocks_with(
        spark,
        _sparse(spark, legs),
        _stands(spark, ARRIVAL_STANDS),
        _runway(spark, [("ALDT", 1000)]),
        EventConfig(),
    )

    assert got == [("on", 1290.0)]


# ---------------------------------------------------------------------------
# Block times off the runway, from the stand stop of their own part, naming
# the stand stood on (work package 3, items 2 to 7)
# ---------------------------------------------------------------------------


def _off(**fields):
    """The default settings with the named corrections changed."""
    return EventConfig(**fields)


def _named(spark, reports, stands, runway, config=None):
    rows = block_events(reports, stands, runway, config or EventConfig()).collect()
    return sorted(
        [(r.block, (r.event_time - _EPOCH).total_seconds(), r.stand) for r in rows],
        key=lambda x: x[1],
    )


def test_the_settings_of_the_block_time_corrections():
    new, old = EventConfig(), EventConfig.legacy()
    for name in (
        "block_time_off_the_runway",
        "block_time_stand_at_same_airport",
        "block_time_from_last_stand_stop",
        "block_time_stand_longest_dwell",
        "block_time_one_part_per_leg",
        "block_time_needs_flight",
        "layout_ref_missing_as_null",
    ):
        assert getattr(new, name) is True, name
        assert getattr(old, name) is False, name
    assert new.block_time_min_gap_to_runway_time_seconds == 60.0
    assert new.block_time_min_stand_dwell_seconds == 30.0
    assert new.block_time_long_stand_stop_seconds == 600.0


#: AIBT#16 (EGCC): the taxi-in was not seen. The landing roll slows from
#: 120 kt, flagged on the ground, and the reports stop at 100 kt; the
#: aircraft reaches stand 51 seven minutes later.
_ROLL_ONLY = [
    _leg(700, 1000, 140, alt_ft=1500, on_ground=False),
    _leg(1000, 1040, 100),
    _leg(1040, 1400, 0, on_ground=True),
]
_ROLL_ONLY_STANDS = [("entry-parking_position", 1300, "51", "EGCC", 100)]


def test_a_landing_roll_ending_fast_is_not_an_in_block(spark):
    """The roll ends at a report flagged on the ground but at 100 kt: on
    speed, that is no standstill."""
    reports = _reports(spark, [_ROLL_ONLY[0], _ROLL_ONLY[1], [(1040, 100, None, True)]])
    stands = _stands(spark, _ROLL_ONLY_STANDS)
    runway = _runway(spark, [("ALDT", 1000)])
    old = _off(block_time_off_the_runway=False, block_time_stand_at_same_airport=False)

    assert _named(spark, reports, stands, runway) == []
    assert [b[0] for b in _named(spark, reports, stands, runway, old)] == ["on"]


def test_an_in_block_less_than_60_s_after_the_landing_is_not_given(spark):
    """The roll slows to a standstill 40 s after the landing (the taxi was
    not seen): that end is not an in-block time."""
    legs = [
        _leg(700, 1000, 140, alt_ft=1500, on_ground=False),
        _leg(1000, 1040, 60),
        _leg(1040, 1400, 0),
    ]
    reports = _reports(spark, legs)
    runway = _runway(spark, [("ALDT", 1000)])
    stands = _stands(spark, [("entry-parking_position", 1040, "51", "EGCC", 360)])

    assert _named(spark, reports, stands, runway) == []
    assert _named(spark, reports, stands, runway, _off(block_time_off_the_runway=False)) == [
        ("on", 1030.0, "51")
    ]


def test_an_off_block_after_the_line_up_is_not_given(spark):
    """AOBT#20 (LFBO): the taxi was not seen; the aircraft lined up at 500 s,
    waited on the runway and started its roll at 560 s, 80 s before the
    take-off. The start of the roll is not an off-block time."""
    legs = [
        _leg(0, 120, 0),
        _leg(120, 560, 0, step_s=60),
        _leg(560, 640, 25),
        _leg(640, 700, 160, alt_ft=3000, on_ground=False),
    ]
    reports = _reports(spark, legs)
    stands = _stands(spark, [("exit-parking_position", 100, "U40", "LFBO", 100)])
    runway = _runway(spark, [("line-up", 500), ("ATOT", 640)])

    assert _named(spark, reports, stands, runway) == []
    assert _named(spark, reports, stands, runway, _off(block_time_off_the_runway=False)) == [
        ("off", 560.0, "U40")
    ]


def test_an_in_block_before_the_runway_is_vacated_is_not_given(spark):
    """The aircraft stops on the runway 70 s after landing and vacates it at
    200 s: a stop there is not the stand."""
    legs = [
        _leg(700, 1000, 140, alt_ft=1500, on_ground=False),
        _leg(1000, 1070, 20),
        _leg(1070, 1400, 0),
    ]
    reports = _reports(spark, legs)
    stands = _stands(spark, [("entry-parking_position", 1100, "51", "EGCC", 300)])
    runway = _runway(spark, [("ALDT", 1000), ("runway-vacated", 1200)])

    assert _named(spark, reports, stands, runway) == []
    assert _named(spark, reports, stands, runway, _off(block_time_off_the_runway=False)) == [
        ("on", 1060.0, "51")
    ]


def test_a_departure_whose_stand_was_seen_only_at_the_other_airport_names_no_stand(spark):
    """Stand to stand, the departure stand not received: the off-block time
    is kept (the stand at a leg's own airport is often not received), but it
    names no stand, and none of the destination."""
    reports = _reports(spark, DEPARTURE + ARRIVAL)
    stands = _stands(spark, ARRIVAL_STANDS)
    runway = _runway(spark, [("ATOT", 600), ("ALDT", 1000)])

    assert _named(spark, reports, stands, runway) == [("off", 60.0, None), ("on", 1290.0, "B2")]


def test_a_stand_passed_for_a_few_seconds_is_not_named(spark):
    """AOBT#26 (LMML): stand 13 crossed for 5 s on the way out; the off-block
    time names no stand rather than that one."""
    reports = _reports(spark, DEPARTURE)
    stands = _stands(spark, [("exit-parking_position", 300, "13", "LMML", 5)])
    runway = _runway(spark, [("ATOT", 600)])
    nearest = _off(block_time_stand_at_same_airport=False)

    assert _named(spark, reports, stands, runway) == [("off", 60.0, None)]
    assert _named(spark, reports, stands, runway, nearest) == [("off", 60.0, "13")]


#: The first (last) movement of the part, naming the stand passage nearest in
#: time.
_FIRST_MOVEMENT = EventConfig(
    block_time_from_last_stand_stop=False, block_time_stand_longest_dwell=False
)


def test_the_off_block_leaves_the_last_stand_stop_before_the_take_off(spark):
    """AOBT#42 (PANC): first received on stand A1, repositioned at 1,000 s to
    stand B7, where it stood for an hour; push-back at 5,000 s, take-off at
    5,400 s. The off-block time is the push from B7, not the reposition."""
    legs = [
        _leg(0, 1000, 0, step_s=60),
        _leg(1000, 1100, 10),
        _leg(1100, 5000, 0, step_s=60),
        _leg(5000, 5060, 5),
        _leg(5060, 5100, 0),
        _leg(5100, 5400, 15),
        _leg(5400, 5500, 160, alt_ft=4000, on_ground=False),
    ]
    stands = [
        ("entry-parking_position", 0, "A1", "PANC", 1030),
        ("exit-parking_position", 1030, "A1", "PANC", 1030),
        ("entry-parking_position", 1090, "B7", "PANC", 3950),
        ("exit-parking_position", 5040, "B7", "PANC", 3950),
    ]
    reports = _reports(spark, legs)
    runway = _runway(spark, [("ATOT", 5400)])

    assert _named(spark, reports, _stands(spark, stands), runway) == [("off", 5000.0, "B7")]
    assert _named(spark, reports, _stands(spark, stands), runway, _FIRST_MOVEMENT) == [
        ("off", 1000.0, "A1")
    ]


def test_the_in_block_reaches_the_first_stand_stop_after_the_landing(spark):
    """AIBT#41 (KMSP): landing, taxi to stand G8, parked for hours, towed to
    G8A the next morning. The in-block time is the arrival at G8."""
    legs = ARRIVAL[:-1] + [
        _leg(1300, 9000, 0, step_s=60),
        _leg(9000, 9200, 8),
        _leg(9200, 9400, 0),
    ]
    stands = [
        ("entry-parking_position", 1250, "G8", "KMSP", 7800),
        ("exit-parking_position", 9050, "G8", "KMSP", 7800),
        ("entry-parking_position", 9150, "G8A", "KMSP", 250),
    ]
    reports = _reports(spark, legs)
    runway = _runway(spark, [("ALDT", 1000)])

    assert _named(spark, reports, _stands(spark, stands), runway) == [("on", 1290.0, "G8")]
    assert _named(spark, reports, _stands(spark, stands), runway, _FIRST_MOVEMENT) == [
        ("on", 9190.0, "G8A")
    ]


def test_the_stand_named_is_the_one_stood_on(spark):
    """AOBT#48 (EDDF): the aircraft stood on V160 for 170 s and passed V155
    for 55 s on the way out; the off-block time names V160, not the passage
    nearest in time."""
    stands = [
        ("entry-parking_position", 0, "V160", "EDDF", 170),
        ("exit-parking_position", 170, "V160", "EDDF", 170),
        ("exit-parking_position", 160, "V155", "EDDF", 55),
    ]
    legs = [_leg(0, 150, 0), _leg(150, 600, 15), _leg(600, 700, 160, 4000, False)]
    reports = _reports(spark, legs)
    runway = _runway(spark, [("ATOT", 600)])
    nearest = _off(block_time_from_last_stand_stop=False, block_time_stand_longest_dwell=False)

    assert _named(spark, reports, _stands(spark, stands), runway) == [("off", 150.0, "V160")]
    assert _named(
        spark, reports, _stands(spark, stands), runway, _off(block_time_from_last_stand_stop=False)
    ) == [("off", 150.0, "V160")]
    assert _named(spark, reports, _stands(spark, stands), runway, nearest) == [
        ("off", 150.0, "V155")
    ]


def test_an_arrival_only_track_carries_its_landing(spark):
    """AIBT#02 and 16 others: a track with a landing and no take-off gives
    one in-block time, which carries that landing."""
    rows = block_events(
        _reports(spark, ARRIVAL),
        _stands(spark, ARRIVAL_STANDS),
        _runway(spark, [("ALDT", 1000)]),
        EventConfig(),
    ).collect()

    assert [(r.block, r.bound_time) for r in rows] == [("on", _at(1000))]


def test_a_departure_only_track_carries_its_take_off(spark):
    rows = block_events(
        _reports(spark, DEPARTURE),
        _stands(spark, DEPARTURE_STANDS),
        _runway(spark, [("ATOT", 600)]),
        EventConfig(),
    ).collect()

    assert [(r.block, r.bound_time) for r in rows] == [("off", _at(600))]


def test_a_stand_without_a_name_names_none(spark):
    """23% of the stands in the layout carry the text "None" for a missing
    name; the block time names no stand instead."""
    stands = [
        ("entry-parking_position", 0, "None", "LEZL", 100),
        ("exit-parking_position", 100, "None", "LEZL", 100),
    ]
    got = _named(
        spark, _reports(spark, DEPARTURE), _stands(spark, stands), _runway(spark, [("ATOT", 600)])
    )
    old = _named(
        spark,
        _reports(spark, DEPARTURE),
        _stands(spark, stands),
        _runway(spark, [("ATOT", 600)]),
        _off(layout_ref_missing_as_null=False),
    )

    assert got == [("off", 60.0, None)]
    assert old == [("off", 60.0, "None")]


def test_missing_names_become_null(spark):
    from opdi.pipeline.ground import missing_ref_as_null

    df = spark.createDataFrame(
        [("None",), ("nan",), ("",), (" ",), ("A12",), ("0",), (None,)], "ref string"
    )
    got = [r.r for r in df.select(missing_ref_as_null(F.col("ref")).alias("r")).collect()]
    assert got == [None, None, None, None, "A12", "0", None]


def test_missing_names_become_none_in_the_layout_table():
    import pandas as pd

    from opdi.reference.h3_airport_layouts import missing_ref_as_none

    got = missing_ref_as_none(pd.Series(["None", "nan", "", "12", "R226", None, float("nan")]))
    assert got.tolist()[:5] == [None, None, None, "12", "R226"]
    assert got.isna().tolist()[5:] == [True, True]


def test_a_track_that_never_leaves_the_ground_gets_no_block_time(spark):
    """AOBT#40 (EGKK, an aircraft towed at night) and vehicles such as TUG15
    (YPPH): no take-off, no landing, nothing in flight."""
    reports = _reports(spark, [_leg(0, 60, 0), _leg(60, 300, 12), _leg(300, 360, 0)])
    stands = _stands(
        spark,
        [
            ("entry-parking_position", 0, "C3", "LFPG", 80),
            ("exit-parking_position", 80, "C3", "LFPG", 80),
            ("entry-parking_position", 290, "C4", "LFPG", 60),
        ],
    )
    assert _named(spark, reports, stands, _runway(spark, [])) == []
    assert _named(
        spark, reports, stands, _runway(spark, []), _off(block_time_needs_flight=False)
    ) == [
        ("off", 60.0, "C3"),
        ("on", 290.0, "C4"),
    ]


def test_a_slow_light_aircraft_keeps_its_block_times(spark):
    """A light aircraft taxis at 6 kt for three minutes, takes off, flies a
    circuit, lands and taxis back to its stand."""
    legs = [
        _leg(0, 60, 0),
        _leg(60, 240, 6),
        _leg(240, 900, 80, alt_ft=1000, on_ground=False),
        _leg(900, 1080, 6),
        _leg(1080, 1200, 0),
    ]
    stands = [
        ("entry-parking_position", 0, "GA1", "EBAW", 70),
        ("exit-parking_position", 70, "GA1", "EBAW", 70),
        ("entry-parking_position", 1070, "GA1", "EBAW", 130),
    ]
    got = _named(
        spark,
        _reports(spark, legs),
        _stands(spark, stands),
        _runway(spark, [("ATOT", 240), ("ALDT", 900)]),
    )

    assert got == [("off", 60.0, "GA1"), ("on", 1070.0, "GA1")]


def test_a_short_push_back_seen_on_few_reports_is_the_off_block(spark):
    """Flight 034f151f92 (sample of 2026-06-14): the push-back moves at 3 kt
    on six positioned reports over 25 s, then one report shows it standing
    and nothing is received for two minutes before the taxi. The push
    lasted until that standing report, 35 s: it is the off-block time, not
    the start of the taxi."""
    legs = [
        _surface(0, 50, 0, step_s=5),
        _surface(50, 80, 3, step_s=5),
        _surface(80, 85, 0, step_s=5),
        _surface(85, 200, 0, step_s=5, positions="none"),
        _surface(200, 500, 15, step_s=5),
        _airborne(500, 600, 160, 3000),
    ]
    stands = _stands(spark, [("exit-parking_position", 75, "A1", "EBBR", 75)])
    got = _blocks_with(
        spark, _sparse(spark, legs), stands, _runway(spark, [("ATOT", 500)]), EventConfig()
    )

    assert got == [("off", 50.0)]


def test_a_single_moving_report_before_a_silence_is_not_a_movement(spark):
    """One position 20 m off while parked, then two minutes without position:
    not a push-back."""
    legs = [
        _surface(0, 50, 0, step_s=5),
        _surface(50, 55, 8, step_s=5),
        _surface(55, 200, 0, step_s=5, positions="none"),
        _surface(200, 260, 0, step_s=5),
        _surface(260, 500, 15, step_s=5),
        _airborne(500, 600, 160, 3000),
    ]
    stands = _stands(spark, [("exit-parking_position", 265, "A1", "EBBR", 265)])
    got = _blocks_with(
        spark, _sparse(spark, legs), stands, _runway(spark, [("ATOT", 500)]), EventConfig()
    )

    assert got == [("off", 260.0)]


def test_a_long_engine_start_after_the_push_back_does_not_move_the_off_block(spark):
    """Received 30 s before the push-back; the push lasts 40 s, the engines
    are started during a 200 s stop still inside the stand's cells, then the
    taxi. The off-block time is the push-back, although the stop after it is
    longer than the one before it."""
    legs = [
        _leg(0, 30, 0),
        _leg(30, 70, 4),
        _leg(70, 270, 0),
        _leg(270, 600, 15),
        _leg(600, 700, 160, alt_ft=4000, on_ground=False),
    ]
    stands = _stands(spark, [("exit-parking_position", 280, "A1", "EBBR", 280)])
    got = _named(spark, _reports(spark, legs), stands, _runway(spark, [("ATOT", 600)]))

    assert got == [("off", 30.0, "A1")]
