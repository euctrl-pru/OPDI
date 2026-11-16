"""Surface events: one element per passage, air reports, parked aircraft, vehicles.

The shapes come from the spot checks of 2026-06-14 (findings b2 and b3):
entry-taxiway#01 at VABB (four overlapping taxiway ways, entries and exits
chosen separately), entry-apron#04 at EYVI (an approach at 145 kt merged into
the ground passage), entry-threshold#33 at CYYZ (a transponder that never sets
the ground flag, taxiing at 2 to 9 kt), entry-hangar#43 at LOWW (a helicopter
at 350 ft over a hangar), entry-hangar#01 at CYYZ (an aircraft parked 19 hours
at a hangar, 42 entry/exit pairs), and the airport vehicles (type ``SERV``,
or no type, call signs like BLUE613, FM4, TUGG298).
"""

import datetime as dt
import json

from opdi.config import EventConfig
from opdi.pipeline.layout import calculate_airport_events

MONTH = dt.date(2026, 6, 1)
APT = "EYVI"
FIELD_ELEV_FT = 650.0
FT_PER_M = 3.28084
KT_PER_MS = 1.94384

SCHEMA = (
    "track_id string, icao24 string, callsign string, event_time timestamp, "
    "lat double, lon double, baro_altitude_c double, heading double, vert_rate double, "
    "velocity double, h3_res_12 string, cumulative_distance_nm double, "
    "cumulative_time_s double, on_ground boolean"
)


class Storage:
    def __init__(self, spark, layouts, typecode="B738", tracks=("trk-1",)):
        self._tables = {
            "opdi_flight_list": spark.createDataFrame(
                [(t, dt.datetime(2026, 6, 14), APT, "ZZZZ", None, None, typecode) for t in tracks],
                "id string, dof timestamp, adep string, ades string, adep_p string, "
                "ades_p string, typecode string",
            ),
            "hexaero_airport_layouts": spark.createDataFrame(
                layouts,
                "hexaero_h3_id string, hexaero_apt_icao string, hexaero_osm_id long, "
                "hexaero_aeroway string, hexaero_ref string",
            ),
            "oa_airports": spark.createDataFrame(
                [(APT, FIELD_ELEV_FT)], "ident string, elevation_ft double"
            ),
        }

    def table_exists(self, name):
        return name in self._tables

    def read_table(self, name):
        return self._tables[name]


def _t(h, m, s):
    return dt.datetime(2026, 6, 14, h, m, s)


def report(t, cell, lat=54.6, lon=25.3, on_ground=True, height_ft=None, kt=None, track="trk-1"):
    alt_m = None if height_ft is None else (FIELD_ELEV_FT + height_ft) / FT_PER_M
    vel = None if kt is None else kt / KT_PER_MS
    return (
        track,
        "abc123",
        "RYR4978",
        t,
        lat,
        lon,
        alt_m,
        90.0,
        0.0,
        vel,
        cell,
        0.0,
        0.0,
        on_ground,
    )


def run(spark, rows, layouts, config=None, **storage_kw):
    sv = spark.createDataFrame(rows, SCHEMA)
    out = calculate_airport_events(
        sv, MONTH, Storage(spark, layouts, **storage_kw), config or EventConfig()
    )
    events = []
    for r in out.collect():
        info = json.loads(r.info)
        events.append((r.type, r.event_time, info["osm_id"], info))
    return sorted(events, key=lambda e: (e[1], e[0], e[2]))


def _pairs(events):
    """Every entry has an exit of the same element, and the reverse."""
    entries = {
        (e[2], e[1] + dt.timedelta(seconds=e[3]["opdi_time_in_use_s"]))
        for e in events
        if e[0].startswith("entry")
    }
    exits = {(e[2], e[1]) for e in events if e[0].startswith("exit")}
    return entries == exits


# --- one element per passage (VABB, entry-taxiway#01) ----------------------

A, B, C, D = 59395088, 1554212673, 1554212672, 1554212671


def _vabb_layouts():
    return [
        ("c1", APT, A, "taxiway", None),
        ("c1", APT, D, "taxiway", None),
        ("c2", APT, A, "taxiway", None),
        ("c2", APT, D, "taxiway", None),
        ("c2", APT, B, "taxiway", None),
        ("c3", APT, A, "taxiway", None),
        ("c3", APT, B, "taxiway", None),
        ("c3", APT, C, "taxiway", None),
    ]


def _vabb_reports():
    return [
        report(_t(14, 21, 54), "c1"),
        report(_t(14, 22, 44), "c2"),
        report(_t(14, 22, 49), "c2"),
        report(_t(14, 23, 9), "c3"),
        report(_t(14, 23, 14), "c3"),
    ]


def test_entries_and_exits_choose_their_element_separately_without_the_rule(spark):
    """The defect: five events, three of them without a partner."""
    events = run(
        spark, _vabb_reports(), _vabb_layouts(), EventConfig(layout_element_per_passage=False)
    )
    assert len(events) == 5
    assert not _pairs(events)


def test_one_element_per_passage_every_entry_has_its_exit(spark):
    events = run(spark, _vabb_reports(), _vabb_layouts())
    assert [(e[0], e[1], e[2]) for e in events] == [
        ("entry-taxiway", _t(14, 21, 54), A),
        ("exit-taxiway", _t(14, 23, 14), A),
    ]
    assert events[0][3]["opdi_time_in_use_s"] == 80
    assert _pairs(events)


def test_of_two_stands_starting_together_the_one_left_first_is_kept(spark):
    """A stand drawn inside a larger stand polygon, entered at the same
    report: the aircraft leaves the stand itself at push-back and the larger
    area 40 s later. The exit of the stand is the one that marks the
    off-block time (the airport records confirmed it)."""
    layouts = [
        ("s1", APT, 10, "parking_position", "11"),
        ("s1", APT, 20, "parking_position", "12"),
        ("s2", APT, 20, "parking_position", "12"),
    ]
    rows = [
        report(_t(10, 0, 0), "s1"),
        report(_t(10, 0, 10), "s1"),
        report(_t(10, 0, 30), "s2"),
        report(_t(10, 0, 50), "s2"),
    ]
    events = run(spark, rows, layouts)
    assert [(e[0], e[1], e[2]) for e in events] == [
        ("entry-parking_position", _t(10, 0, 0), 10),
        ("exit-parking_position", _t(10, 0, 10), 10),
    ]


def test_two_elements_crossed_one_after_the_other_both_keep_their_events(spark):
    """Overlap at the joint of two taxiway ways: neither shares an entry or an
    exit, so both passages and all four events stay."""
    layouts = [
        ("j1", APT, 1, "taxiway", None),
        ("j2", APT, 1, "taxiway", None),
        ("j2", APT, 2, "taxiway", None),
        ("j3", APT, 2, "taxiway", None),
    ]
    rows = [
        report(_t(10, 0, 0), "j1"),
        report(_t(10, 0, 5), "j2"),
        report(_t(10, 0, 10), "j2"),
        report(_t(10, 0, 15), "j3"),
    ]
    events = run(spark, rows, layouts)
    assert len(events) == 4 and _pairs(events)


def test_v002_settings_switch_every_surface_correction_off():
    legacy = EventConfig.legacy()
    assert not (
        legacy.layout_element_per_passage
        or legacy.layout_air_report_must_be_slow
        or legacy.layout_merge_parked_passages
        or legacy.layout_mark_track_edges
        or legacy.layout_mark_ground_only
    )
    d = EventConfig()
    assert (
        d.layout_element_per_passage
        and d.layout_air_report_must_be_slow
        and d.layout_merge_parked_passages
        and d.layout_mark_track_edges
        and d.layout_mark_ground_only
    )


# --- reports not flagged on the ground --------------------------------------


def _eyvi_approach_then_ground():
    """RYR4978 at EYVI: two reports on final over apron 3536241 at 277 and
    240 ft and 145 kt, then the aircraft on the apron 60 s later."""
    return [
        report(_t(9, 0, 0), "ap", lat=54.6300, on_ground=False, height_ft=277.0, kt=145.0),
        report(_t(9, 0, 5), "ap", lat=54.6266, on_ground=False, height_ft=240.0, kt=145.0),
        report(_t(9, 1, 5), "ap", lat=54.6240, on_ground=True),
        report(_t(9, 1, 15), "ap", lat=54.62402, on_ground=True),
    ]


AP_LAYOUT = [("ap", APT, 3536241, "apron", None)]


def test_the_approach_is_not_an_apron_entry(spark):
    before = run(
        spark,
        _eyvi_approach_then_ground(),
        AP_LAYOUT,
        EventConfig(layout_air_report_must_be_slow=False),
    )
    assert before[0][1] == _t(9, 0, 0)
    events = run(spark, _eyvi_approach_then_ground(), AP_LAYOUT)
    assert [(e[0], e[1]) for e in events] == [
        ("entry-apron", _t(9, 1, 5)),
        ("exit-apron", _t(9, 1, 15)),
    ]


def test_speed_comes_from_positions_when_the_report_has_none(spark):
    """A climb-out at 160 kt with no reported speed: 410 m in 5 s."""
    rows = [
        report(_t(9, 0, 0), "ap", lat=54.6300, on_ground=False, height_ft=150.0),
        report(_t(9, 0, 5), "ap", lat=54.6337, on_ground=False, height_ft=250.0),
    ]
    assert run(spark, rows, AP_LAYOUT) == []


def test_a_transponder_that_never_sets_the_ground_flag_still_taxis(spark):
    """CYYZ entry-threshold#33: not flagged on the ground, 181 ft above the
    field (pressure), 9 and 8 kt by position and by report."""
    rows = [
        report(_t(12, 37, 9), "ap", lat=54.60000, on_ground=False, height_ft=181.0, kt=8.5),
        report(_t(12, 37, 19), "ap", lat=54.60004, on_ground=False, height_ft=181.0, kt=8.6),
        report(_t(12, 37, 29), "ap", lat=54.60008, on_ground=False, height_ft=181.0),
    ]
    events = run(spark, rows, AP_LAYOUT)
    assert [(e[0], e[1]) for e in events] == [
        ("entry-apron", _t(12, 37, 9)),
        ("exit-apron", _t(12, 37, 29)),
    ]


def test_a_helicopter_above_the_apron_is_not_on_it(spark):
    """LOWW entry-hangar#43: police EC35 at 350 ft above the field, slowing
    through 30 kt."""
    rows = [
        report(_t(18, 2, 19), "ap", lat=54.60000, on_ground=False, height_ft=350.0, kt=35.0),
        report(_t(18, 2, 24), "ap", lat=54.60008, on_ground=False, height_ft=350.0, kt=31.0),
    ]
    assert run(spark, rows, AP_LAYOUT) == []
    assert len(run(spark, rows, AP_LAYOUT, EventConfig(layout_air_report_must_be_slow=False))) == 2


def test_a_report_flagged_on_the_ground_is_admitted_whatever_its_speed(spark):
    """Surface reports carry garbled speeds (103 kt while the positions give
    3 kt); the ground flag decides."""
    rows = [
        report(_t(10, 0, 0), "ap", on_ground=True, kt=103.0),
        report(_t(10, 0, 10), "ap", on_ground=True, kt=103.0),
    ]
    assert len(run(spark, rows, AP_LAYOUT)) == 2


# --- parked aircraft ---------------------------------------------------------

HANGAR = [("h", APT, 777, "hangar", "H1"), ("t", APT, 888, "taxiway", "T")]


def _parked_at_hangar(n=4, step_s=600):
    """TR452 at CYYZ: inside the hangar, positions only every 10 minutes and
    a metre or two apart: every absence over 300 s starts a new passage."""
    rows = []
    for i in range(n):
        t0 = _t(2, 0, 0) + dt.timedelta(seconds=i * step_s)
        rows.append(report(t0, "h", lat=54.6 + i * 1e-5))
        rows.append(report(t0 + dt.timedelta(seconds=10), "h", lat=54.6 + i * 1e-5))
    return rows


def test_a_parked_aircraft_gives_one_entry_and_one_exit(spark):
    before = run(
        spark, _parked_at_hangar(), HANGAR, EventConfig(layout_merge_parked_passages=False)
    )
    assert len(before) == 8
    events = run(spark, _parked_at_hangar(), HANGAR)
    assert [(e[0], e[1]) for e in events] == [
        ("entry-hangar", _t(2, 0, 0)),
        ("exit-hangar", _t(2, 30, 10)),
    ]
    assert events[0][3]["opdi_time_in_use_s"] == 1810


def test_leaving_by_a_taxiway_and_coming_back_is_two_passages(spark):
    rows = _parked_at_hangar(n=2, step_s=900)
    rows.insert(2, report(_t(2, 7, 0), "t"))
    rows.insert(3, report(_t(2, 7, 10), "t"))
    events = run(spark, rows, HANGAR)
    assert [e[0] for e in events if "hangar" in e[0]] == [
        "entry-hangar",
        "exit-hangar",
        "entry-hangar",
        "exit-hangar",
    ]


def test_coming_back_100_m_away_is_two_passages(spark):
    rows = [
        report(_t(2, 0, 0), "h", lat=54.6),
        report(_t(2, 0, 10), "h", lat=54.6),
        report(_t(2, 10, 0), "h", lat=54.6009),
        report(_t(2, 10, 10), "h", lat=54.6009),
    ]
    assert len(run(spark, rows, HANGAR)) == 4


# --- edges of the track and vehicles ----------------------------------------


def test_entry_at_the_first_positioned_report_is_marked(spark):
    """The transponder is switched on at the stand: the entry is the start of
    reception. A first report without a position does not change that."""
    rows = [
        report(_t(5, 59, 50), "ap", lat=None, lon=None),
        report(_t(6, 0, 0), "ap"),
        report(_t(6, 0, 10), "ap"),
        report(_t(6, 5, 0), "t"),
        report(_t(6, 5, 10), "t"),
    ]
    events = {(e[0], e[1]): e[3] for e in run(spark, rows, AP_LAYOUT + HANGAR)}
    assert events[("entry-apron", _t(6, 0, 0))]["at_track_start"] is True
    assert "at_track_end" not in events[("exit-apron", _t(6, 0, 10))]
    assert events[("exit-taxiway", _t(6, 5, 10))]["at_track_end"] is True
    assert "at_track_start" not in events[("entry-taxiway", _t(6, 5, 0))]


def _ground_track():
    return [report(_t(8, 0, 0), "ap"), report(_t(8, 0, 10), "ap")]


def test_a_vehicle_that_never_flies_is_marked_ground_only(spark):
    for code in ("SERV", "GRND", "GND", "TWR", None, ""):
        events = run(spark, _ground_track(), AP_LAYOUT, typecode=code)
        assert len(events) == 2
        assert all(e[3].get("ground_only") is True for e in events), code


def test_a_towed_aircraft_of_known_type_is_not_marked(spark):
    events = run(spark, _ground_track(), AP_LAYOUT, typecode="A320")
    assert len(events) == 2 and not any("ground_only" in e[3] for e in events)


def test_a_flight_of_unknown_type_is_not_marked(spark):
    """One report in the air with an altitude is enough."""
    rows = _ground_track() + [
        report(_t(8, 10, 0), "zz", on_ground=False, height_ft=3000.0, kt=200.0)
    ]
    events = run(spark, rows, AP_LAYOUT, typecode=None)
    assert len(events) == 2 and not any("ground_only" in e[3] for e in events)


def test_reports_not_flagged_on_the_ground_without_altitude_do_not_count_as_flying(spark):
    """Vehicles send empty reports not flagged on the ground (no altitude, no
    speed, no position)."""
    rows = _ground_track() + [report(_t(8, 0, 5), "ap", lat=None, lon=None, on_ground=False)]
    events = run(spark, rows, AP_LAYOUT, typecode="SERV")
    assert all(e[3].get("ground_only") is True for e in events)


def test_of_two_stands_ending_together_the_inner_one_is_kept(spark):
    """04afa15fe6916be6 (shape): the aircraft enters a large stand polygon,
    then the stand drawn inside it 35 s later, where it stops (the in-block
    record is at that entry); it leaves both at one report. The inner
    stand is kept."""
    layouts = [
        ("o", APT, 405457947, "parking_position", "A1"),
        ("i", APT, 405457947, "parking_position", "A1"),
        ("i", APT, 4076567490, "parking_position", "A1a"),
    ]
    rows = [report(_t(21, 38, 29), "o"), report(_t(21, 39, 4), "i")]
    rows += [report(_t(21, 40 + i, 4), "i") for i in range(9)]
    rows += [report(_t(21, 49, 24), "i")]
    events = run(spark, rows, layouts)
    assert [(e[0], e[1], e[2]) for e in events] == [
        ("entry-parking_position", _t(21, 39, 4), 4076567490),
        ("exit-parking_position", _t(21, 49, 24), 4076567490),
    ]


def test_two_passages_of_one_stand_are_not_joined(spark):
    """e94ad338ee75ccf9: exit of stand 1452016753 at 07:09:39 (off-block
    record 07:10:00), no position during the push-back, then a position at
    the stand's edge again at 07:15:39. The exit at 07:09:39 stays."""
    layouts = [("s", APT, 1452016753, "parking_position", "12")]
    rows = [report(_t(7, 2 + i, 39), "s") for i in range(8)]
    rows += [report(_t(7, 15, 39), "s", lat=54.60005), report(_t(7, 16, 24), "s", lat=54.60005)]
    events = run(spark, rows, layouts)
    assert ("exit-parking_position", _t(7, 9, 39)) in [(e[0], e[1]) for e in events]
    assert len(events) == 4
