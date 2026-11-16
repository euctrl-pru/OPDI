"""Runway events: the corrections decided after the spot checks of 2026-06-14.

Each test copies the shape of a real example of that day (the example id and
the track are named), reduced to the reports that matter, laid out on the
strip 07/25 of ``test_runway_ops`` (07 at 50 N 4 E, 25 two miles up it). The
same pass is then read with the correction off, which must give the result
the example showed.

Rows are ``(t, along_nm, height_ft, kt, on_ground, heading)``, optionally
followed by the vertical rate in ft/min and the distance north of the centre
line in NM. ``along`` is measured from the
threshold of 07: the runway is 0 to 2 NM, the approach to 07 is below 0 and
the approach to 25 above 2. ``height_ft`` is above the airport's published
elevation, as an altimeter set to standard pressure reads it.
"""

import json

import pytest
from conftest import _EPOCH, make_track
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from test_runway_ops import _as_flown, _dest, _m, _mps, _prepared, _thresholds

from opdi.config import EventConfig
from opdi.pipeline.runway_ops import runway_milestones, runway_traversals

FTMIN_PER_MPS = 196.850394


def _pass(spark, rows, elev_ft=0.0, apt="EBBR", cells=None):
    """A track along strip 07/25 (see the module docstring)."""
    samples, cell_ids = [], []
    for row in rows:
        t, along, height_ft, kt, on_ground, heading = row[:6]
        fpm = row[6] if len(row) > 6 else 0.0
        cross = row[7] if len(row) > 7 else 0.0
        if along is None:
            lat, lon = None, None
        elif along >= 0:
            lat, lon = _dest(50.0, 4.0, 70.0, along)
        else:
            lat, lon = _dest(50.0, 4.0, 250.0, -along)
        if lat is not None and cross:
            lat, lon = _dest(lat, lon, 340.0, cross)
        samples.append(
            {
                "t": t,
                "lat": lat,
                "lon": lon,
                "baro_altitude": None if height_ft is None else _m(elev_ft + height_ft),
                "velocity": None if kt is None else _mps(kt),
                "vert_rate": None if fpm is None else fpm / FTMIN_PER_MPS,
                "heading": heading,
                "on_ground": on_ground,
            }
        )
        if along is None:
            cell_ids.append(None)
        else:
            cell_ids.append("cell-rwy" if 0.0 <= along <= 2.0 else "cell-air")
    sv = _prepared(make_track(spark, samples), elev_ft, elev_ft, apt, apt)
    return _as_flown(sv, cells=cells or cell_ids, apt=(apt,))


def _grid(spark, apt="EBBR"):
    return spark.createDataFrame(
        [
            ("cell-rwy", apt, apt + "-07/25", "07", "25", "runway"),
            ("cell-air", apt, apt + "-07/25", "07", "25", "approach"),
        ],
        "h3_id string, apt_icao string, strip_id string, "
        "le_ident string, he_ident string, zone string",
    )


def _family(spark, sv, config=None, apt="EBBR", grid=None, thresholds=None):
    """The traversals and the milestones of ``sv``, as plain lists."""
    config = config or EventConfig()
    traversals = runway_traversals(
        sv,
        grid if grid is not None else _grid(spark, apt),
        thresholds if thresholds is not None else _thresholds(spark, apt),
        config,
    )
    milestones = runway_milestones(sv, traversals, config)
    passes = sorted((r["rwy_ident"], r["class"]) for r in traversals.collect())
    events = [
        ((r["event_time"] - _EPOCH).total_seconds(), r["type"], json.loads(r["info"]), r)
        for r in milestones.orderBy("event_time", "type").collect()
    ]
    return passes, events


def _of(events, kind):
    return [e for e in events if e[1] == kind]


def _types(events):
    return [e[1] for e in events]


#: None of the corrections of work package 2.
_ALL_OFF = dict(
    runway_height_above_track_ground=False,
    runway_direction_by_motion=False,
    runway_events_on_runway_reports=False,
    runway_roll_speed_from_positions=False,
    runway_line_up_needs_ground_report=False,
    runway_threshold_crossing_last_only=False,
    goaround_excludes_landings=False,
    goaround_in_approach_corridor=False,
    runway_crossing_ground_without_altitude=False,
    runway_window_times_checked=False,
)


def _only(**on):
    """Every correction of work package 2 off except those named."""
    fields = dict(_ALL_OFF)
    fields.update(on)
    return EventConfig(**fields)


def test_every_correction_is_off_in_the_release_settings():
    legacy = EventConfig.legacy()
    default = EventConfig()
    for name in _ALL_OFF:
        assert getattr(legacy, name) is False, name
        assert getattr(default, name) is True, name


# ---------------------------------------------------------------------------
# 1. Height above the track's own ground altitude.
# ---------------------------------------------------------------------------

#: The take-off of ATOT#03 (track f2beb58ca27a92f2_1781476314, MAS134, YBBN,
#: elevation 13 ft): reports flagged on the ground without altitude while
#: lining up, then the roll read 375 to 400 ft below the elevation (a day of
#: high pressure). Measured from the elevation, "15 ft" is reached about
#: 430 ft above the runway, some 10 s after the lift-off.
_YBBN_ELEV_FT = 13.0
_HIGH_PRESSURE_TAKE_OFF = [
    (0, 0.02, None, None, True, None),
    (10, 0.03, None, None, True, None),
    (15, 0.08, -388.0, 110.0, False, 70.0, 64.0),
    (20, 0.17, -413.0, 127.0, False, 70.0, 0.0),
    (25, 0.27, -413.0, None, False, None, None),
    (35, 0.52, -388.0, 169.0, False, 70.0, 128.0),
    (40, 0.66, -188.0, 170.0, False, 70.0, 2000.0),
    (45, 0.80, 37.0, 167.0, False, 70.0, 2600.0),
    (50, 0.94, 237.0, 165.0, False, 70.0, 2600.0),
    (55, 1.08, 462.0, 163.0, False, 70.0, 2600.0),
    (60, 1.22, 687.0, 162.0, False, 70.0, 2600.0),
]


def test_a_take_off_on_a_day_of_high_pressure_lifts_off_above_the_roll_not_the_elevation(spark):
    sv = _pass(spark, _HIGH_PRESSURE_TAKE_OFF, elev_ft=_YBBN_ELEV_FT)
    passes, events = _family(spark, sv)

    assert passes == [("07", "departure")]
    (atot,) = _of(events, "ATOT")
    # The ground reads -388 ft: 15 ft above it is reached between 35 s
    # (0 ft) and 40 s (200 ft).
    assert atot[0] == pytest.approx(35.375, abs=0.01)
    assert atot[2]["height_reference"] == "track"

    _, before = _family(spark, sv, _only())
    (atot_before,) = _of(before, "ATOT")
    # Above the elevation: between -188 ft (40 s) and 37 ft (45 s).
    assert atot_before[0] == pytest.approx(44.51, abs=0.01)
    assert "height_reference" not in atot_before[2]


def test_a_lift_off_next_to_a_report_without_position_gets_a_position(spark):
    """The crossing of 15 ft is often bracketed by a report that has an
    altitude but no position. The crossing then takes the position the
    report would have between the positioned reports either side (within
    30 s), so the lift-off is not published without a position."""
    rows = list(_HIGH_PRESSURE_TAKE_OFF)
    t, _, h, kt, g, hdg, fpm = rows[6]
    rows[6] = (t, None, h, kt, g, hdg, fpm)
    sv = _pass(spark, rows, elev_ft=_YBBN_ELEV_FT)
    _, events = _family(spark, sv)
    (atot,) = _of(events, "ATOT")
    assert atot[0] == pytest.approx(35.375, abs=0.01)
    assert atot[3]["lat"] is not None and atot[3]["lon"] is not None
    # Between the positioned reports at 35 s and 45 s, at 37.5% of the way
    # from 35 s to 40 s: along the runway between 0.52 and 0.80 NM.
    lat35, lat45 = _dest(50.0, 4.0, 70.0, 0.52)[0], _dest(50.0, 4.0, 70.0, 0.80)[0]
    assert lat35 < atot[3]["lat"] < lat45

    _, before = _family(spark, sv, EventConfig(runway_times_position_between_reports=False))
    (atot_before,) = _of(before, "ATOT")
    assert atot_before[3]["lat"] is None


def test_a_lift_off_is_placed_between_reports_with_a_position(spark):
    """When the report without a position has no positioned neighbour within
    30 s, the crossing is placed between the reports that have both."""
    rows = list(_HIGH_PRESSURE_TAKE_OFF)
    for i in (6, 7):
        t, _, h, kt, g, hdg, fpm = rows[i]
        rows[i] = (t, None, h, kt, g, hdg, fpm)
    rows = rows[:8] + [(t + 40, *rest) for t, *rest in rows[8:]]
    sv = _pass(spark, rows, elev_ft=_YBBN_ELEV_FT)
    _, events = _family(spark, sv)
    (atot,) = _of(events, "ATOT")
    assert atot[3]["lat"] is not None
    # Between 35 s (0 ft above the ground, positioned) and 90 s (positioned).
    assert 35.0 < atot[0] < 90.0


#: The landing of ALDT#04 (track 60616737214fb211_1781432574, TAM3640, SBVT,
#: elevation 34 ft): the final approach reads 209 ft below the elevation on
#: the runway, then reports flagged on the ground without altitude. From the
#: elevation, the landing time was 0.42 NM before the threshold, about 220 ft
#: above it.
_SBVT_ELEV_FT = 34.0
_HIGH_PRESSURE_LANDING = [
    (0, -2.53, 691.0, 151.0, False, 70.0, -832.0),
    (10, -2.14, 566.0, 149.0, False, 70.0, -768.0),
    (20, -1.72, 441.0, 146.0, False, 70.0, -768.0),
    (30, -1.34, 316.0, 142.0, False, 70.0, -640.0),
    (35, -1.12, 266.0, 141.0, False, 70.0, -640.0),
    (40, -0.92, 216.0, 140.0, False, 70.0, -832.0),
    (45, -0.73, 141.0, 140.0, False, 70.0, -768.0),
    (50, -0.51, 66.0, 140.0, False, 70.0, -896.0),
    (55, -0.33, -34.0, 139.0, False, 70.0, -1024.0),
    (60, -0.13, -109.0, 136.0, False, 70.0, -896.0),
    (65, 0.06, -184.0, 135.0, False, 70.0, -896.0),
    (70, 0.21, -209.0, 131.0, False, 70.0, -64.0),
    (75, 0.41, None, 122.0, True, 70.0, -128.0),
    (80, 0.55, None, None, True, None, None),
    (85, 0.66, None, None, True, None, None),
    (95, 0.80, None, None, True, None, None),
]


def test_a_landing_on_a_day_of_high_pressure_touches_down_on_the_runway(spark):
    sv = _pass(spark, _HIGH_PRESSURE_LANDING, elev_ft=_SBVT_ELEV_FT)
    passes, events = _family(spark, sv)

    assert passes == [("07", "arrival")]
    (aldt,) = _of(events, "ALDT")
    # The ground reads -209 ft (the report at 70 s, levelled off; the one
    # at 65 s is still descending at 900 ft/min): 15 ft above it between
    # 65 s (25 ft) and 70 s (0 ft), past the threshold.
    assert aldt[0] == pytest.approx(67.0, abs=0.01)
    assert _of(events, "xing-runway-threshold"), "the threshold crossing was missing"

    _, before = _family(spark, sv, _only())
    (aldt_before,) = _of(before, "ALDT")
    # Above the elevation: between 66 ft (50 s) and -34 ft (55 s), 0.4 NM out.
    assert aldt_before[0] == pytest.approx(52.55, abs=0.01)
    assert not _of(before, "xing-runway-threshold")


#: The take-off of SAS4704 (track 00233761c240f13a_1781423769, LFMN 04R,
#: elevation 13 ft): flagged on the ground without altitude while lining up
#: and rolling, then -25 ft twice on the roll, a dip to -75 ft at rotation,
#: and the climb. Measured from the dip, the roll read 50 ft and the pass was
#: no take-off at all: line-up, take-off-roll and the take-off time were lost.
_DIP_AT_ROTATION = [
    (0, 0.02, None, None, True, None),
    (10, 0.05, None, None, True, None),
    (20, 0.15, None, None, True, None),
    (30, 0.35, None, None, True, None),
    (35, 0.60, -25.0, 119.0, False, 70.0, 64.0),
    (40, 0.85, -25.0, 141.0, False, 70.0, 64.0),
    (45, 1.10, -75.0, 160.0, False, 70.0, 448.0),
    (50, 1.40, 100.0, 166.0, False, 70.0, 2496.0),
    (55, 1.70, 325.0, 165.0, False, 70.0, 2944.0),
]


def test_a_dip_of_the_altitude_at_rotation_is_not_the_ground(spark):
    sv = _pass(spark, _DIP_AT_ROTATION, elev_ft=13.0)
    passes, events = _family(spark, sv)
    assert passes == [("07", "departure")]
    assert {"line-up", "take-off-roll", "ATOT"} <= set(_types(events))
    (atot,) = _of(events, "ATOT")
    # The ground reads -25 ft: 15 ft above it between -75 ft (45 s) and
    # 100 ft (50 s).
    assert atot[0] == pytest.approx(45.0 + 5.0 * 65.0 / 175.0, abs=0.01)


def test_without_a_report_on_the_ground_the_height_is_above_the_elevation(spark):
    """Reception ends on final, before the runway (runway-vacated#03, EGLL):
    no report shows the ground, so the elevation is the reference."""
    rows = [row for row in _HIGH_PRESSURE_LANDING if not row[4]]
    sv = _pass(spark, rows, elev_ft=_SBVT_ELEV_FT)
    _, events = _family(spark, sv)
    _, before = _family(spark, sv, _only())

    (aldt,) = _of(events, "ALDT")
    assert aldt[2]["height_reference"] == "elevation"
    assert aldt[0] == pytest.approx(_of(before, "ALDT")[0][0])


def _ground_altitude(spark, rows, elev_ft):
    """``_ground_altitude_ft`` of ``(t, alt_ft, kt, on_ground, fpm)`` reports."""
    import datetime as dt

    from opdi.pipeline.runway_ops import _ground_altitude_ft

    frame = spark.createDataFrame(
        [
            ("trk-1", "EBBR", _EPOCH + dt.timedelta(seconds=t), alt, kt, g, fpm)
            for t, alt, kt, g, fpm in rows
        ],
        "track_id string, apt_icao string, event_time timestamp, alt_ft double, "
        "kt double, _on_ground boolean, fpm double",
    )
    frame = frame.withColumn("velocity", F.col("kt") / F.lit(1.94384)).withColumn(
        "vert_rate", F.col("fpm") / F.lit(FTMIN_PER_MPS)
    )
    out = _ground_altitude_ft(frame, F.col("alt_ft"), F.lit(elev_ft))
    return {r["_g_ref"] for r in out.select("_g_ref").collect()}


def test_the_ground_altitude_is_the_upper_step_of_the_take_off_roll(spark):
    """ATOT#01 (KATL, elevation 1,026 ft, KLM248): flagged on the ground
    without altitude, then 950 ft three times on the roll, 1,000 and 1,050 ft
    after the lift-off; 950 ft is the ground."""
    rows = [
        (0, None, None, True, None),
        (10, 950.0, 113.0, False, 64.0),
        (15, 950.0, 138.0, False, 64.0),
        (20, 950.0, 153.0, False, 192.0),
        (25, 1000.0, 173.0, False, 640.0),
        (30, 1050.0, 181.0, False, 2112.0),
        (35, 1225.0, 181.0, False, 2624.0),
    ]
    assert _ground_altitude(spark, rows, 1026.0) == {950.0}


def test_a_track_without_a_report_on_the_ground_has_no_ground_altitude(spark):
    rows = [(5 * i, 900.0 - 50 * i, 140.0, False, -700.0) for i in range(10)]
    assert _ground_altitude(spark, rows, 100.0) == {None}


def test_a_ground_altitude_far_from_the_elevation_is_not_used(spark):
    """A ghost altitude on the ground (9,800 ft at a field at 100 ft) is not a
    pressure error, and no ground altitude is given."""
    rows = [(5 * i, 9800.0, 5.0, True, 0.0) for i in range(5)]
    assert _ground_altitude(spark, rows, 100.0) == {None}
    assert _ground_altitude(spark, [(t, 80.0, *r) for t, _, *r in rows], 100.0) == {80.0}


# ---------------------------------------------------------------------------
# 2. The direction by the motion; one runway per pass.
# ---------------------------------------------------------------------------

#: A take-off on 07 that leaves the ground only past the far end of the
#: runway, as at KATL (ATOT#01, 27R named 09L) and YBBN (ATOT#03): over the
#: runway every height is below 15 ft. The climb-out lies short of the
#: threshold of 25, so only the reading over 25 sees the lift-off.
_LIFT_OFF_PAST_THE_END = (
    [(5 * i, 0.02, 0.0, 10.0, True, 70.0) for i in range(3)]
    + [(15 + 5 * i, 0.05 + 0.27 * i, 0.0, 60.0 + 15 * i, True, 70.0) for i in range(7)]
    + [
        (50, 2.10, 40.0, 160.0, False, 70.0, 1500.0),
        (55, 2.40, 150.0, 162.0, False, 70.0, 1500.0),
        (60, 2.70, 300.0, 164.0, False, 70.0, 1500.0),
        (65, 3.00, 450.0, 166.0, False, 70.0, 1500.0),
    ]
)


def test_a_take_off_is_named_for_the_direction_it_rolls(spark):
    sv = _pass(spark, _LIFT_OFF_PAST_THE_END)
    passes, events = _family(spark, sv)

    assert passes == [("07", "departure")]
    for kind in ("line-up", "take-off-roll", "ATOT"):
        (event,) = _of(events, kind)
        assert event[2]["runway"] == "07", kind
        assert event[2]["runway_bearing_deg"] == pytest.approx(70.0, abs=0.01)

    passes_before, before = _family(spark, sv, _only())
    assert passes_before == [("25", "departure")]
    assert _of(before, "ATOT")[0][2]["runway"] == "25"


def test_the_motion_setting_alone_names_the_direction(spark):
    sv = _pass(spark, _LIFT_OFF_PAST_THE_END)
    passes, _ = _family(spark, sv, _only(runway_direction_by_motion=True))
    assert passes == [("07", "departure")]


#: An earlier visit to the approach area of 25 only (a taxi past the far
#: end), then ten minutes later a take-off on 07 that climbs out over 25's
#: approach. Numbered per direction, the take-off is pass 0 over 07 and pass 1
#: over 25, so both readings were kept: one event id on two rows (line-up,
#: take-off-roll, ATOT), as on KOKB and KVNY (a2.C8).
_EARLIER_VISIT_THEN_TAKE_OFF = [(0, 2.30, 0.0, 10.0, True, 250.0)] + [
    (700 + t, along, h, kt, g, hd, fpm)
    for t, along, h, kt, g, hd, fpm in [
        (0, 0.02, 0.0, 10.0, True, 70.0, 0.0),
        (5, 0.02, 0.0, 10.0, True, 70.0, 0.0),
        (20, 0.05, 0.0, 60.0, True, 70.0, 0.0),
        (25, 0.15, 0.0, 90.0, True, 70.0, 0.0),
        (30, 0.32, 0.0, 120.0, True, 70.0, 0.0),
        (35, 0.55, 0.0, 145.0, True, 70.0, 0.0),
        (40, 0.80, 100.0, 160.0, False, 70.0, 2000.0),
        (45, 1.05, 400.0, 165.0, False, 70.0, 2000.0),
        (50, 1.30, 700.0, 170.0, False, 70.0, 2000.0),
        (55, 1.60, 1000.0, 170.0, False, 70.0, 2000.0),
        (60, 1.90, 1300.0, 170.0, False, 70.0, 2000.0),
        (65, 2.20, 1600.0, 170.0, False, 70.0, 2000.0),
    ]
]


def test_one_take_off_gives_one_line_up(spark):
    sv = _pass(spark, _EARLIER_VISIT_THEN_TAKE_OFF)
    passes, events = _family(spark, sv)
    assert passes == [("07", "departure")]
    for kind in ("line-up", "take-off-roll", "ATOT"):
        assert len(_of(events, kind)) == 1, kind
        assert _of(events, kind)[0][2]["runway"] == "07"

    _, before = _family(spark, sv, _only())
    assert len(_of(before, "line-up")) == 2
    assert {e[2]["runway"] for e in _of(before, "line-up")} == {"07", "25"}


def _parallels(spark, apt="EBBR"):
    """Strip 07/25 and a parallel strip 07R/25L 0.15 NM to its north."""
    lat_b, lon_b = _dest(50.0, 4.0, 340.0, 0.15)
    lat_b25, lon_b25 = _dest(lat_b, lon_b, 70.0, 2.0)
    lat25, lon25 = _dest(50.0, 4.0, 70.0, 2.0)
    thresholds = spark.createDataFrame(
        [
            (apt, "07", 50.0, 4.0, 70.0, 2.0, 0.05),
            (apt, "25", lat25, lon25, 250.0, 2.0, 0.05),
            (apt, "07R", lat_b, lon_b, 70.0, 2.0, 0.05),
            (apt, "25L", lat_b25, lon_b25, 250.0, 2.0, 0.05),
        ],
        "apt_ident string, rwy_ident string, thr_lat double, thr_lon double, "
        "rwy_bearing double, rwy_length_nm double, rwy_half_width_nm double",
    )
    grid = spark.createDataFrame(
        [
            ("cell-rwy", apt, apt + "-07/25", "07", "25", "runway"),
            ("cell-air", apt, apt + "-07/25", "07", "25", "approach"),
            ("cell-rwy", apt, apt + "-07R/25L", "07R", "25L", "runway"),
            ("cell-air", apt, apt + "-07R/25L", "07R", "25L", "approach"),
        ],
        "h3_id string, apt_icao string, strip_id string, "
        "le_ident string, he_ident string, zone string",
    )
    return grid, thresholds


#: A landing on 07 whose last report before the threshold is 10 ft up, 0.15 NM
#: from the centre line of the parallel 07R as well: read over 07R alone (its
#: approach area holds the final), it also looks like a landing, with the
#: runway-vacated in the air short of 07R (PHNL 04L/04R, runway-vacated#38).
_LANDING_BESIDE_A_PARALLEL = [
    (0, -1.5, 500.0, 140.0, False, 70.0, -700.0),
    (10, -1.0, 340.0, 138.0, False, 70.0, -700.0),
    (20, -0.5, 180.0, 136.0, False, 70.0, -700.0),
    (30, -0.2, 60.0, 134.0, False, 70.0, -700.0),
    (35, -0.05, 10.0, 132.0, False, 70.0, -300.0),
    (40, 0.1, 0.0, 130.0, True, 70.0, 0.0),
    (50, 0.4, 0.0, 110.0, True, 70.0, 0.0),
    (60, 0.7, 0.0, 80.0, True, 70.0, 0.0),
    (70, 0.9, 0.0, 50.0, True, 70.0, 0.0),
    (80, 1.1, 0.0, 25.0, True, 70.0, 0.0),
]


def test_a_landing_beside_a_parallel_runway_names_one_runway(spark):
    grid, thresholds = _parallels(spark)
    sv = _pass(spark, _LANDING_BESIDE_A_PARALLEL)
    passes, events = _family(spark, sv, grid=grid, thresholds=thresholds)
    assert passes == [("07", "arrival")]
    assert {e[2]["runway"] for e in events} == {"07"}

    passes_before, _ = _family(spark, sv, _only(), grid=grid, thresholds=thresholds)
    assert passes_before == [("07", "arrival"), ("07R", "arrival")]


# ---------------------------------------------------------------------------
# 3. runway-vacated and the crossings from the reports on the runway.
# ---------------------------------------------------------------------------


def test_a_landing_whose_reception_ends_before_the_threshold_has_no_runway_vacated(spark):
    """runway-vacated#03 (EGLL 27L, BAW54): the last report is 0.22 NM short
    of the threshold, 134 kt, descending; it was published as
    ``runway-vacated``, 11 s before the airport's landing time."""
    rows = [row for row in _HIGH_PRESSURE_LANDING if row[1] < 0]
    sv = _pass(spark, rows, elev_ft=_SBVT_ELEV_FT)
    passes, events = _family(spark, sv)
    assert passes == [("07", "arrival")]
    assert "ALDT" in _types(events)
    assert "runway-vacated" not in _types(events)

    _, before = _family(spark, sv, _only())
    (vacated,) = _of(before, "runway-vacated")
    assert vacated[0] == 60.0


def test_runway_vacated_is_the_last_report_on_the_runway(spark):
    sv = _pass(spark, _HIGH_PRESSURE_LANDING, elev_ft=_SBVT_ELEV_FT)
    _, events = _family(spark, sv)
    (vacated,) = _of(events, "runway-vacated")
    assert vacated[0] == 95.0
    assert vacated[3]["lat"] is not None


#: YSTW (entry#44 and others): a taxi at 16 to 40 kt across the approach
#: area of runway 06, 0.2 to 0.3 NM short of its threshold: published as a
#: crossing of 06, though no report is on the runway.
_TAXI_SHORT_OF_THE_THRESHOLD = [
    (0, -0.25, 0.0, 18.0, True, 160.0, 0.0, 0.0),
    (5, -0.25, 0.0, 18.0, True, 160.0, 0.0, 0.01),
]


def test_a_taxi_short_of_the_threshold_is_not_a_runway_crossing(spark):
    sv = _pass(spark, _TAXI_SHORT_OF_THE_THRESHOLD)
    _, events = _family(spark, sv)
    assert events == []
    _, before = _family(spark, sv, _only())
    assert "runway-crossing-entry" in _types(before)


def test_a_crossing_of_the_runway_is_still_a_crossing(spark):
    rows = [(0, 0.5, 0.0, 15.0, True, 160.0, 0.0, 0.0), (5, 0.5, 0.0, 15.0, True, 160.0, 0.0, 0.0)]
    sv = _pass(spark, rows)
    passes, events = _family(spark, sv)
    assert passes == [("07", "crossing")] or passes == [("25", "crossing")]
    assert _types(events) == ["runway-crossing-entry", "runway-crossing-vacated"]


# ---------------------------------------------------------------------------
# 4. take-off-roll from the speed the positions give.
# ---------------------------------------------------------------------------

#: An airliner's take-off: the reports on the ground carry a position but no
#: speed (99 % of them on 2026-06-14), and the first report with a speed is
#: the first in the air. take-off-roll was then placed near the lift-off, at
#: about 100 kt (a2.C4): here at 45 s instead of 25 s.
_ROLL_WITHOUT_SPEED = [
    (5 * i, 0.02, None if i < 3 else 0.0, None, True, None) for i in range(4)
] + [
    (20, 0.05, None, None, True, None),
    (25, 0.12, None, None, True, None),
    (30, 0.22, None, None, True, None),
    (33, None, None, 90.0, False, 70.0),
    (35, 0.36, None, None, True, None),
    (40, 0.55, None, None, True, None),
    (45, 0.78, 100.0, 165.0, False, 70.0, 2000.0),
    (50, 1.02, 300.0, 166.0, False, 70.0, 2000.0),
    (55, 1.26, 500.0, 167.0, False, 70.0, 2000.0),
]


def test_take_off_roll_is_found_from_the_positions_on_the_ground(spark):
    sv = _pass(spark, _ROLL_WITHOUT_SPEED)
    _, events = _family(spark, sv)
    (roll,) = _of(events, "take-off-roll")
    # 0.07 NM in 5 s between 20 s and 25 s is 50.4 kt.
    assert roll[0] == 25.0
    assert roll[3]["lat"] is not None

    _, before = _family(spark, sv, _only())
    (roll_before,) = _of(before, "take-off-roll")
    # The report without position at 33 s carries the first reported speed,
    # but the reports without speed after it end that run at once.
    assert roll_before[0] == 45.0


#: take-off-roll#12 (KUAO, N33363): a report at 51 kt, then one with a
#: position and no speed, then 57 kt: the report without speed ended the run,
#: and take-off-roll came 11 s after the take-off time, 100 ft up.
_ROLL_BROKEN_BY_A_REPORT_WITHOUT_SPEED = [
    (0, 0.02, 0.0, 10.0, True, 70.0),
    (5, 0.02, 0.0, 10.0, True, 70.0),
    (10, 0.06, 0.0, 51.0, False, 70.0),
    (15, 0.13, 0.0, None, False, None),
    (20, 0.21, 0.0, 57.0, False, 70.0),
    (25, 0.29, 0.0, 58.0, False, 70.0),
    (30, 0.38, 60.0, 61.0, False, 70.0, 500.0),
    (35, 0.47, 120.0, 62.0, False, 70.0, 500.0),
    (40, 0.56, 180.0, 62.0, False, 70.0, 500.0),
]


def test_a_report_without_speed_does_not_end_the_take_off_roll(spark):
    sv = _pass(spark, _ROLL_BROKEN_BY_A_REPORT_WITHOUT_SPEED)
    _, events = _family(spark, sv)
    assert _of(events, "take-off-roll")[0][0] == 10.0
    _, before = _family(spark, sv, _only())
    assert _of(before, "take-off-roll")[0][0] == 20.0


# ---------------------------------------------------------------------------
# 5. line-up and take-off-roll only for a take-off that starts on the ground.
# ---------------------------------------------------------------------------

#: take-off-roll#01 (KCLM, N403BB) and #06 (EHGG): a touch-and-go. The final
#: approach, never slower than 77 kt, the roll on the runway, the climb-out.
#: line-up and take-off-roll were placed on the approach, descending.
_TOUCH_AND_GO = [
    (0, -1.00, 184.0, 91.0, False, 70.0, -900.0),
    (10, -0.60, 109.0, 84.0, False, 70.0, -500.0),
    (15, -0.45, 59.0, 84.0, False, 70.0, -700.0),
    (20, -0.30, 9.0, 85.0, False, 70.0, -450.0),
    (25, 0.02, 0.0, 83.0, False, 70.0, -300.0),
    (30, 0.14, 0.0, 80.0, False, 70.0, 0.0),
    (35, 0.25, 0.0, 78.0, False, 70.0, 0.0),
    (45, 0.40, 0.0, 70.0, False, 70.0, 0.0),
    (50, 0.52, 0.0, 84.0, False, 70.0, 128.0),
    (55, 0.65, 0.0, 90.0, False, 70.0, 400.0),
    (65, 0.90, 100.0, 90.0, False, 70.0, 900.0),
    (75, 1.15, 250.0, 88.0, False, 70.0, 830.0),
    (85, 1.40, 400.0, 94.0, False, 70.0, 400.0),
]


def test_a_touch_and_go_keeps_its_take_off_time_but_has_no_line_up(spark):
    sv = _pass(spark, _TOUCH_AND_GO)
    _, events = _family(spark, sv)
    assert "ATOT" in _types(events)
    assert "line-up" not in _types(events)
    assert "take-off-roll" not in _types(events)

    _, before = _family(spark, sv, _only())
    assert "line-up" in _types(before)


def test_a_take_off_from_a_stand_still_keeps_its_line_up(spark):
    rows = [(5 * i, 0.02, 0.0, 5.0, False, 70.0) for i in range(4)] + [
        (20 + t, along, h, kt, g, hd, *rest) for t, along, h, kt, g, hd, *rest in _TOUCH_AND_GO[5:]
    ]
    sv = _pass(spark, rows)
    _, events = _family(spark, sv)
    assert _types(events)[:2] == ["line-up", "take-off-roll"]


# ---------------------------------------------------------------------------
# 6. One threshold crossing per landing, low over the threshold.
# ---------------------------------------------------------------------------

#: xing-runway-threshold#18 (KPAO): a low pass over the threshold at about
#: 200 ft, a circuit out of sight of the runway zones, and the landing,
#: within one pass. Both crossings of the threshold were published, the
#: first 126 s before the landing.
_LOW_PASS_THEN_LANDING = [
    (0, -1.0, 450.0, 90.0, False, 70.0, -500.0),
    (10, -0.5, 300.0, 90.0, False, 70.0, -500.0),
    (20, -0.1, 210.0, 90.0, False, 70.0, -300.0),
    (25, 0.1, 200.0, 90.0, False, 70.0, 0.0),
    (35, 0.5, 350.0, 92.0, False, 70.0, 800.0),
    (45, 0.9, 500.0, 92.0, False, 70.0, 800.0),
    (200, -1.0, 330.0, 70.0, False, 70.0, -500.0),
    (210, -0.6, 200.0, 68.0, False, 70.0, -500.0),
    (220, -0.2, 80.0, 66.0, False, 70.0, -500.0),
    (225, 0.05, 30.0, 64.0, False, 70.0, -400.0),
    (230, 0.25, 0.0, 60.0, True, 70.0, 0.0),
    (240, 0.45, 0.0, 40.0, True, 70.0, 0.0),
    (250, 0.55, 0.0, 20.0, True, 70.0, 0.0),
]


def test_a_low_pass_before_the_landing_gives_no_threshold_crossing(spark):
    sv = _pass(spark, _LOW_PASS_THEN_LANDING)
    passes, events = _family(spark, sv)
    assert ("07", "arrival") in passes
    (xing,) = _of(events, "xing-runway-threshold")
    assert 220.0 < xing[0] < 225.0

    _, before = _family(spark, sv, _only())
    assert len(_of(before, "xing-runway-threshold")) == 2


def test_a_threshold_crossing_high_above_the_runway_is_not_given(spark):
    """An overflight at 1,200 ft over the threshold (skydiving aircraft at
    KOKB crossed at 9,800 to 13,000 ft), then a landing whose first report
    is already past the threshold: the only crossing is the overflight."""
    rows = [
        (0, -1.0, 1200.0, 140.0, False, 70.0, 0.0),
        (10, 0.2, 1200.0, 140.0, False, 70.0, 0.0),
        (150, 0.1, 40.0, 70.0, False, 70.0, -500.0),
        (155, 0.3, 0.0, 60.0, True, 70.0, 0.0),
        (165, 0.5, 0.0, 30.0, True, 70.0, 0.0),
    ]
    sv = _pass(spark, rows)
    passes, events = _family(spark, sv)
    assert ("07", "arrival") in passes
    assert not _of(events, "xing-runway-threshold")
    _, before = _family(spark, sv, _only())
    assert len(_of(before, "xing-runway-threshold")) == 1


# ---------------------------------------------------------------------------
# 7. Go-around: not a landing, not a data hole, on the approach.
# ---------------------------------------------------------------------------


def _go_arounds(spark, rows, config=None, thresholds=True):
    from opdi.pipeline.runway_ops import go_arounds

    sv = _pass(spark, rows).withColumn("ades_lat", F.lit(50.0)).withColumn("ades_lon", F.lit(4.0))
    thr = _thresholds(spark) if thresholds else None
    out = go_arounds(sv, config or EventConfig(), thr).collect()
    return [((r["event_time"] - _EPOCH).total_seconds(), json.loads(r["info"])) for r in out]


_APPROACH = [
    (0, -3.0, 900.0, 140.0, False, 70.0, -700.0),
    (20, -2.0, 600.0, 140.0, False, 70.0, -700.0),
    (40, -1.0, 350.0, 138.0, False, 70.0, -700.0),
    (50, -0.5, 200.0, 136.0, False, 70.0, -700.0),
]
_CLIMB_OUT = [
    (0, 0.1, 50.0, 140.0, False, 70.0, 2000.0),
    (10, 0.6, 400.0, 145.0, False, 70.0, 2000.0),
    (20, 1.1, 800.0, 150.0, False, 70.0, 2000.0),
    (30, 1.6, 1200.0, 155.0, False, 70.0, 2000.0),
    (40, 2.1, 1600.0, 160.0, False, 70.0, 2000.0),
    (50, 2.6, 2000.0, 160.0, False, 70.0, 2000.0),
]


def _later(rows, dt):
    return [(t + dt, *rest) for t, *rest in rows]


def test_a_landing_then_a_take_off_is_not_a_go_around(spark):
    """go-around#14 (CYRT): a landing, the last report in the air at 81 ft,
    20 min of reports on the ground without altitude, then a take-off."""
    rows = (
        _APPROACH
        + [(60, 0.0, 81.0, 130.0, False, 70.0, -500.0)]
        + [(90 + 30 * i, 0.5, None, None, True, None, None) for i in range(40)]
        + _later(_CLIMB_OUT, 1300)
    )
    assert _go_arounds(spark, rows) == []
    assert len(_go_arounds(spark, rows, _only())) == 1


def test_an_approach_across_a_data_hole_is_not_a_go_around(spark):
    """go-around#10 (KTOL): no report for two minutes between the descent
    and the climb."""
    rows = _APPROACH + [(60, 0.0, 120.0, 135.0, False, 70.0, -500.0)] + _later(_CLIMB_OUT, 180)
    assert _go_arounds(spark, rows) == []
    assert len(_go_arounds(spark, rows, _only())) == 1


_GO_AROUND = (
    _APPROACH
    + [(60, 0.0, 120.0, 135.0, False, 70.0, -500.0)]
    + [(t + 70, a, h + 100.0, *rest) for t, a, h, *rest in _CLIMB_OUT]
)


def test_a_go_around_on_the_approach_is_still_a_go_around(spark):
    (ga,) = _go_arounds(spark, _GO_AROUND)
    assert ga[0] == 60.0


def test_a_low_point_away_from_the_runways_is_not_a_go_around(spark):
    """go-around#24 (KMCC, 9 NM from KSMF) and others: low near another
    airfield within 10 NM of the destination."""
    rows = [(*r[:7], 6.0) for r in _GO_AROUND]
    assert _go_arounds(spark, rows) == []
    assert len(_go_arounds(spark, rows, _only())) == 1
    # Without the destination's runways, the go-around is kept.
    assert len(_go_arounds(spark, rows, thresholds=False)) == 1


# ---------------------------------------------------------------------------
# 8. Runway crossings: airliners without altitude; not the own runway.
# ---------------------------------------------------------------------------


def test_a_crossing_by_an_aircraft_without_altitude_on_the_ground_is_found(spark):
    """Airliners send no altitude on the ground, so no crossing of theirs
    was found (150 crossings worldwide on the day)."""
    rows = [(0, 0.5, None, 15.0, True, 160.0), (5, 0.5, None, 15.0, True, 160.0)]
    sv = _pass(spark, rows)
    _, events = _family(spark, sv)
    assert _types(events) == ["runway-crossing-entry", "runway-crossing-vacated"]
    _, before = _family(spark, sv, _only())
    assert before == []


#: entry#02 (CYXE): a landing, then the turn-off at 18 kt. The reports of the
#: landing roll carry no speed and no heading, so read in the opposite
#: direction the pass is slow and across the runway: a "crossing" of the
#: runway the aircraft had just landed on, and the landing itself was lost.
_LANDING_THEN_TURN_OFF = [
    (0, -1.5, 500.0, 140.0, False, 70.0, -700.0, 0.03),
    (10, -1.0, 340.0, 138.0, False, 70.0, -700.0, 0.03),
    (20, -0.5, 180.0, 136.0, False, 70.0, -700.0, 0.02),
    (30, -0.2, 60.0, 134.0, False, 70.0, -700.0, 0.01),
    (40, 0.1, 0.0, None, True, None, 0.0),
    (50, 0.4, 0.0, None, True, None, 0.0),
    (60, 0.6, 0.0, None, True, None, 0.0),
    (70, 0.7, 0.0, 18.0, True, 160.0, 0.0),
    (75, 0.71, 0.0, 18.0, True, 160.0, 0.0),
    (80, 0.72, 0.0, 18.0, True, 160.0, 0.0),
]


def test_the_turn_off_after_a_landing_is_not_a_crossing_of_that_runway(spark):
    sv = _pass(spark, _LANDING_THEN_TURN_OFF)
    passes, events = _family(spark, sv)
    assert passes == [("07", "arrival")]
    assert not [t for t in _types(events) if t.startswith("runway-crossing")]

    passes_before, before = _family(spark, sv, _only())
    assert passes_before == [("25", "crossing")]
    passes_x, _ = _family(spark, sv, _only(runway_crossing_ground_without_altitude=True))
    assert passes_x == [("07", "arrival")]


# ---------------------------------------------------------------------------
# 9. The take-off and landing times of the runway window method.
# ---------------------------------------------------------------------------

#: EBBR, elevation 184 ft, runway 07 at (50, 4), bearing 70 (``_thresholds``).
_EBBR_FT = 184.0


def _window_track(spark, rows):
    """``(t, along_nm, height_ft, kt, fpm, on_ground)`` along runway 07."""
    samples = []
    for t, along, height, kt, fpm, ground in rows:
        lat, lon = _dest(50.0, 4.0, 70.0 if along >= 0 else 250.0, abs(along))
        samples.append(
            {
                "t": t,
                "lat": lat,
                "lon": lon,
                "baro_altitude": None if height is None else _m(_EBBR_FT + height),
                "velocity": None if kt is None else _mps(kt),
                "vert_rate": fpm / FTMIN_PER_MPS,
                "heading": 70.0,
                "on_ground": ground,
            }
        )
    sdf = make_track(spark, samples).withColumn("baro_altitude_c", F.col("baro_altitude"))
    first = F.min("event_time").over(Window.partitionBy())
    return sdf.withColumn(
        "cumulative_time_s", F.col("event_time").cast("double") - first.cast("double")
    ).withColumn("cumulative_distance_nm", F.lit(1.0))


def _window(spark, rows, role, config=None, last_seen_s=None):
    import datetime as dt

    from opdi.pipeline.runways import detect_runway_movements

    sdf = _window_track(spark, rows)
    first = _EPOCH + dt.timedelta(seconds=rows[0][0])
    last = _EPOCH + dt.timedelta(seconds=last_seen_s if last_seen_s is not None else rows[-1][0])
    ends = spark.createDataFrame(
        [("trk-1", "EBBR", role, 50.0, 4.0, _EBBR_FT, first, last)],
        "track_id string, apt_ident string, role string, apt_lat double, apt_lon double, "
        "apt_elevation_ft double, first_seen timestamp, last_seen timestamp",
    )
    out = detect_runway_movements(sdf, ends, _thresholds(spark), config or EventConfig())
    return out.collect()


def _seconds(ts):
    return (ts - _EPOCH).total_seconds()


_TAKE_OFF = [(0, 0.0, 0.0, 10.0, 0.0, True)] + [
    (60 + 5 * i, 0.5 + 0.25 * i, 30.0 + 150.0 * i, 150.0, 1800.0, False) for i in range(8)
]


def test_a_window_take_off_time_carries_its_report(spark):
    (row,) = _window(spark, _TAKE_OFF, "departure")
    assert _seconds(row["first_time"]) == 60.0
    assert row["at_lat"] is not None
    assert row["at_altitude_ft"] == pytest.approx(_EBBR_FT + 30.0, abs=0.01)
    assert row["at_track_edge"] is False


def test_a_track_first_seen_high_in_the_climb_gives_no_window_take_off_time(spark):
    """The first report in the air more than 1,500 ft above the track's own
    ground (here 1,200 ft above the published elevation, on a day the ground
    reads 400 ft below it) is not a take-off time."""
    rows = [(0, 0.0, -400.0, 10.0, 0.0, True)] + [
        (t, a, h + 1170.0, kt, fpm, g) for t, a, h, kt, fpm, g in _TAKE_OFF[1:]
    ]
    assert _window(spark, rows, "departure") == []
    (before,) = _window(spark, rows, "departure", _only())
    assert _seconds(before["first_time"]) == 60.0


def test_a_track_first_seen_low_in_the_climb_keeps_its_window_take_off_time(spark):
    """ATOT#04 and others: the time is the track's first report, 345 to 730 ft
    above the airport. Within 1,500 ft it is kept, with its position, its
    altitude and the mark that it is the first report of the track (about
    1,000 such times on the day were within 60 s of the airport record)."""
    rows = [(t, a, h + 600.0, kt, fpm, g) for t, a, h, kt, fpm, g in _TAKE_OFF[1:]]
    (row,) = _window(spark, rows, "departure")
    assert _seconds(row["first_time"]) == 60.0
    assert row["at_lat"] is not None
    assert row["at_altitude_ft"] == pytest.approx(_EBBR_FT + 630.0, abs=0.01)
    assert row["at_track_edge"] is True


def test_a_take_off_at_the_first_report_is_marked_at_the_track_edge(spark):
    rows = _TAKE_OFF[1:]
    (row,) = _window(spark, rows, "departure")
    assert row["at_track_edge"] is True


def test_a_take_off_off_the_runway_centre_line_gives_no_window_time(spark):
    """ATOT#45: a C172 climbing out of Renton, 4.7 NM from KSEA, published as
    a KSEA take-off. Here 1 NM to the side of the runway."""
    import datetime as dt

    from opdi.pipeline.runways import detect_runway_movements

    sdf = _window_track(spark, _TAKE_OFF)
    sdf = sdf.withColumn("lat", F.col("lat") + F.lit(1.0 / 60.0))
    ends = spark.createDataFrame(
        [("trk-1", "EBBR", "departure", 50.0, 4.0, _EBBR_FT, _EPOCH, _EPOCH + dt.timedelta(1))],
        "track_id string, apt_ident string, role string, apt_lat double, apt_lon double, "
        "apt_elevation_ft double, first_seen timestamp, last_seen timestamp",
    )
    assert detect_runway_movements(sdf, ends, _thresholds(spark), EventConfig()).collect() == []


_FINAL = [(5 * i, -2.0 + 0.3 * i, 600.0 - 90.0 * i, 140.0, -800.0, False) for i in range(7)]


def test_a_window_landing_followed_by_the_ground_is_kept(spark):
    rows = _FINAL + [(40, 0.3, None, 40.0, 0.0, True), (60, 0.6, None, 15.0, 0.0, True)]
    (row,) = _window(spark, rows, "arrival", last_seen_s=3600)
    assert _seconds(row["last_time"]) == 30.0


def test_a_low_approach_that_flies_on_gives_no_window_landing_time(spark):
    """ALDT#19 (KCGF) and others: a practice approach, the track goes on in
    the air for minutes."""
    rows = _FINAL + [
        (60 + 10 * i, 1.0 + 0.5 * i, 300.0 + 200 * i, 140.0, 1500.0, False) for i in range(5)
    ]
    assert _window(spark, rows, "arrival", last_seen_s=900) == []
    assert len(_window(spark, rows, "arrival", _only(), last_seen_s=900)) == 1


def test_a_landing_whose_track_ends_on_final_is_kept_and_marked(spark):
    (row,) = _window(spark, _FINAL, "arrival")
    assert _seconds(row["last_time"]) == 30.0
    assert row["at_track_edge"] is True
