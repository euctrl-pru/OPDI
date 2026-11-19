"""The two measurements every event gets: distance flown and time passed.

The distance is computed once per report (:func:`add_distance_measure`) and
the events take it from their reports; :func:`measures_from_event_time` then
gives every event both values from its own time. The shapes come from the
spot checks of 2026-06-14 (findings d3), reduced to the reports that matter.
"""

import datetime as dt

import pytest
from conftest import _EPOCH, make_track

from opdi.config import EventConfig
from opdi.pipeline.events import (
    LAT_BETWEEN_POSITIONS,
    LON_BETWEEN_POSITIONS,
    add_distance_measure,
    measures_from_event_time,
)
from opdi.pipeline.holes import add_piece

DEFAULT = EventConfig()


def _distance(spark, samples, config=DEFAULT):
    sdf = add_piece(make_track(spark, samples), config.track_hole_seconds)
    out = add_distance_measure(
        sdf,
        config.distance_from_last_known_position,
        max_implied_speed_kt=config.distance_max_implied_speed_kt,
        between_positions=config.distance_between_positions,
        position_fill_seconds=config.crossing_value_max_gap_seconds,
        jump_allowance_nm=config.position_jump_allowance_nm,
    )
    return out.orderBy("event_time").collect()


def _seconds(row):
    return (row.event_time - _EPOCH).total_seconds()


# --- a step implying more than 1,000 kt -----------------------------------


def _parked_with_a_shifted_longitude():
    """Spot check m_distance_flown_nm#34 (SIS61 at KOAK): a parked aircraft
    whose longitude is 90 degrees off for 20 minutes, then right again.
    Cleaning keeps the shifted positions because they are steady among
    themselves."""
    samples = [{"t": 5 * k, "lat": 37.7213, "lon": -122.2100} for k in range(12)]
    samples += [{"t": 60 + 5 * k, "lat": 37.7213, "lon": -32.2100} for k in range(240)]
    samples += [{"t": 1260 + 5 * k, "lat": 37.7213, "lon": -122.2100} for k in range(12)]
    return samples


def test_a_jump_of_the_position_adds_no_distance(spark):
    rows = _distance(spark, _parked_with_a_shifted_longitude())

    assert rows[-1].cumulative_distance_nm == pytest.approx(0.0, abs=1e-6)


def test_without_the_limit_the_jump_is_counted_twice(spark):
    config = EventConfig(distance_max_implied_speed_kt=None)

    rows = _distance(spark, _parked_with_a_shifted_longitude(), config)

    # Out and back, about 4,260 NM each way at this latitude.
    assert rows[-1].cumulative_distance_nm > 8000
    assert EventConfig.legacy().distance_max_implied_speed_kt is None


def test_a_long_oceanic_gap_at_cruise_speed_is_still_bridged(spark):
    """WSSS to KEWR in the spot checks: 3,729 NM in 7 hours (533 kt) without
    reception. The straight line between the two sides is added."""
    samples = [{"t": 0, "lat": 0.0, "lon": 0.0}, {"t": 25200, "lat": 0.0, "lon": 3729 / 60}]

    rows = _distance(spark, samples)

    assert rows[-1].cumulative_distance_nm == pytest.approx(3729, rel=0.005)


# --- a report without a position ------------------------------------------


def _stretch_without_positions():
    """Spot check m_distance_flown_nm (VHHH, track 63a01728): cleaning removed
    20 minutes of positions in the climb; the reports in between carry an
    altitude only. Here 240 NM are flown at 480 kt over 30 minutes, with no
    position from t=300 s to t=1500 s."""
    samples = []
    for k in range(361):
        t = 5 * k
        lat = 22.0 + (480.0 * t / 3600.0) / 60.0
        has = t <= 300 or t >= 1500
        samples.append({"t": t, "lat": lat if has else None, "lon": 114.0 if has else None})
    return samples


def test_a_report_without_a_position_gets_the_distance_between_its_neighbours(spark):
    rows = _distance(spark, _stretch_without_positions())
    at = {_seconds(r): r.cumulative_distance_nm for r in rows}

    # Halfway through the stretch, the aircraft has flown 480 kt x 900 s.
    assert at[900.0] == pytest.approx(120.0, rel=0.005)
    values = [r.cumulative_distance_nm for r in rows]
    assert values == sorted(values)


def test_without_the_setting_the_distance_stays_flat_through_the_stretch(spark):
    config = EventConfig(distance_between_positions=False)

    rows = _distance(spark, _stretch_without_positions(), config)
    at = {_seconds(r): r.cumulative_distance_nm for r in rows}

    assert at[900.0] == pytest.approx(at[300.0])
    assert EventConfig.legacy().distance_between_positions is False


def test_the_distance_is_not_interpolated_across_a_hole(spark):
    """A report without a position just before a 40-minute silence keeps the
    distance of the last known position: nothing was received across the
    hole to say when the aircraft flew the straight line."""
    samples = [
        {"t": 0, "lat": 50.0, "lon": 4.0},
        {"t": 5, "lat": 50.0 + 0.6 / 60, "lon": 4.0},
        {"t": 10, "lat": None, "lon": None},
        {"t": 2410, "lat": 50.0 + 320 / 60, "lon": 4.0},
    ]

    rows = _distance(spark, samples)

    assert rows[2].cumulative_distance_nm == pytest.approx(rows[1].cumulative_distance_nm)
    assert rows[3].cumulative_distance_nm == pytest.approx(320.0, rel=0.005)


def test_the_distance_is_unknown_before_the_first_position(spark):
    """Spot check m_distance_flown_nm#09: a report without a position at the
    start of the track had distance 0, a value a user would trust."""
    samples = [
        {"t": 0, "lat": None, "lon": None},
        {"t": 5, "lat": 50.0, "lon": 4.0},
        {"t": 10, "lat": 50.0 + 0.6 / 60, "lon": 4.0},
    ]

    rows = _distance(spark, samples)

    assert rows[0].cumulative_distance_nm is None
    assert rows[1].cumulative_distance_nm == 0.0
    assert rows[2].cumulative_distance_nm == pytest.approx(0.6, rel=0.01)


def test_a_report_without_a_position_gets_one_only_between_close_reports(spark):
    """The position of a report without one, used to place a flight-level
    crossing, is interpolated only between reports at most 30 s away."""
    samples = [
        {"t": 0, "lat": 50.0, "lon": 179.99},
        {"t": 10, "lat": None, "lon": None},
        {"t": 20, "lat": 50.1, "lon": -179.99},
        {"t": 30, "lat": None, "lon": None},
        {"t": 80, "lat": 50.3, "lon": -179.9},
    ]

    rows = _distance(spark, samples)

    assert rows[1][LAT_BETWEEN_POSITIONS] == pytest.approx(50.05)
    # Across the 180th meridian the shorter way round.
    assert abs(rows[1][LON_BETWEEN_POSITIONS]) == pytest.approx(180.0, abs=1e-6)
    assert rows[3][LAT_BETWEEN_POSITIONS] is None
    assert rows[0][LAT_BETWEEN_POSITIONS] is None
    assert rows[1].lat is None


# --- every event its two measurements -------------------------------------


def _events(spark, rows):
    """``(type, seconds, distance, time)`` rows of one track."""
    return spark.createDataFrame(
        [("trk-1", type_, _EPOCH + dt.timedelta(seconds=s), d, t) for type_, s, d, t in rows],
        "track_id string, type string, event_time timestamp, "
        "cumulative_distance_nm double, cumulative_time_s double",
    )


def _measured(spark, rows, config=DEFAULT):
    out = measures_from_event_time(_events(spark, rows), config)
    return {r.type: r for r in out.collect()}


def test_an_event_without_a_distance_gets_it_between_its_neighbours(spark):
    """Level-offs, block times and the take-off and landing times of the
    runway window had no measurement at all (finding d3.G)."""
    got = _measured(
        spark,
        [
            ("first_seen", 0, 0.0, 0.0),
            ("xing-fl100", 600, 50.0, 600.0),
            ("level-off-climb-start", 700, None, None),
            ("xing-fl245", 1000, 100.0, 1000.0),
            ("last_seen", 2000, 300.0, 2000.0),
        ],
    )

    assert got["level-off-climb-start"].cumulative_distance_nm == pytest.approx(62.5)
    assert got["level-off-climb-start"].cumulative_time_s == pytest.approx(700.0)
    assert got["xing-fl100"].cumulative_distance_nm == 50.0


def test_an_event_before_any_known_distance_stays_without_one(spark):
    got = _measured(
        spark,
        [
            ("first_seen", 0, None, 0.0),
            ("AOBT", 60, None, None),
            ("take-off", 600, 1.0, 600.0),
            ("AIBT", 900, None, None),
        ],
    )

    assert got["AOBT"].cumulative_distance_nm is None
    # After the last known distance it stays at that value.
    assert got["AIBT"].cumulative_distance_nm == 1.0


def test_the_time_passed_counts_from_the_first_seen_event(spark):
    """Whatever ``first_seen`` means (work package 4 places it on the first
    report with a measured value), the time passed counts from it."""
    got = _measured(
        spark,
        [
            ("first_seen", 300, 0.0, 300.0),
            ("xing-fl100", 900, 50.0, 900.0),
        ],
    )

    assert got["first_seen"].cumulative_time_s == 0.0
    assert got["xing-fl100"].cumulative_time_s == pytest.approx(600.0)


def test_an_event_before_first_seen_has_no_time_passed(spark):
    """An event on a report before the first one with a position (a block
    time or a top on reports without a position) would get a negative time
    passed; it gets none instead, and no time-passed measurement."""
    got = _measured(
        spark,
        [
            ("AOBT", 100, None, 100.0),
            ("first_seen", 300, 0.0, 300.0),
            ("xing-fl100", 900, 50.0, 900.0),
        ],
    )

    assert got["AOBT"].cumulative_time_s is None
    assert got["first_seen"].cumulative_time_s == 0.0
    assert got["xing-fl100"].cumulative_time_s == pytest.approx(600.0)


def test_with_both_settings_off_nothing_changes(spark):
    config = EventConfig(measure_every_event=False, time_passed_from_first_seen=False)

    got = _measured(
        spark,
        [("first_seen", 300, 0.0, 300.0), ("level-off-climb-start", 700, None, None)],
        config,
    )

    assert got["first_seen"].cumulative_time_s == 300.0
    assert got["level-off-climb-start"].cumulative_distance_nm is None
    assert EventConfig.legacy().measure_every_event is False
    assert EventConfig.legacy().time_passed_from_first_seen is False


def test_events_at_the_same_time_get_the_same_distance(spark):
    got = measures_from_event_time(
        _events(
            spark,
            [
                ("first_seen", 0, 0.0, 0.0),
                ("level-off-climb-end", 500, None, None),
                ("level-end", 500, 40.0, 500.0),
                ("last_seen", 1000, 80.0, 1000.0),
            ],
        ),
        DEFAULT,
    ).collect()

    at_500 = {r.cumulative_distance_nm for r in got if _seconds(r) == 500.0}
    assert at_500 == {40.0}


def test_an_event_after_the_first_position_but_before_any_known_distance_gets_one(spark):
    """A take-off time of the runway window at the track's first report with a
    position, while ``first_seen`` sits on an earlier report without one: the
    distance at the first position is 0, which is known without an event."""
    events = _events(
        spark,
        [
            ("first_seen", 0, None, 0.0),
            ("ATOT", 60, None, None),
            ("xing-fl50", 260, 10.0, 260.0),
        ],
    )
    starts = spark.createDataFrame(
        [("trk-1", _EPOCH + dt.timedelta(seconds=60))],
        "track_id string, first_position_time timestamp",
    )

    got = {r.type: r for r in measures_from_event_time(events, DEFAULT, starts).collect()}

    assert got["ATOT"].cumulative_distance_nm == 0.0
    assert got["first_seen"].cumulative_distance_nm is None
