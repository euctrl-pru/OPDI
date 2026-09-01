"""Runway operation milestones.

Every trajectory here is built so the right answer is arithmetic. A departure
that lifts off exactly halfway between two samples must yield an `airborne`
time exactly halfway between their timestamps -- that is what makes
"interpolated" a checkable claim rather than a hopeful one.
"""
import datetime as dt
import json
import math

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

from conftest import _EPOCH, make_track

from opdi.config import EventConfig
from opdi.pipeline.runway_ops import (
    classify_traversal,
    go_arounds,
    runway_milestones,
    runway_traversals,
)

FT_PER_M = 3.28084
KT_PER_MPS = 1.94384


def _m(ft):
    return ft / FT_PER_M


def _mps(kt):
    return kt / KT_PER_MPS


# ---------------------------------------------------------------------------
# Fixtures.
#
# `make_track` carries only the raw TRACK_SCHEMA and silently drops unknown
# keys, so nothing here may pass `baro_altitude_c` or an elevation through it.
# `_prepared` is the adapter, the same shape as `test_events_phase._measured`.
# ---------------------------------------------------------------------------

def _prepared(sdf, elev_adep_ft=0.0, elev_ades_ft=0.0):
    """Add what step 02 and the flight-list join add and TRACK_SCHEMA does not.

    ``baro_altitude_c`` is the rolling-mean repair step 02 writes and every
    event detector reads; the cumulative measures are what the measurement
    helpers attach; the two elevations are what ``attach_field_elevation``
    attaches. All default to a sea-level field so a test only has to state the
    geometry it cares about.
    """
    return (
        sdf.withColumn("baro_altitude_c", F.col("baro_altitude"))
        .withColumn("cumulative_distance_nm", F.lit(0.0))
        .withColumn("cumulative_time_s", F.lit(0.0))
        .withColumn("elev_adep_ft", F.lit(elev_adep_ft))
        .withColumn("elev_ades_ft", F.lit(elev_ades_ft))
    )


_TRAVERSAL_SCHEMA = StructType(
    [
        StructField("track_id", StringType()),
        StructField("apt_ident", StringType()),
        StructField("rwy_ident", StringType()),
        StructField("rwy_bearing", DoubleType()),
        StructField("thr_lat", DoubleType()),
        StructField("thr_lon", DoubleType()),
        StructField("trace_id", StringType()),
        StructField("entry_time", TimestampType()),
        StructField("exit_time", TimestampType()),
        StructField("duration_seconds", DoubleType()),
        StructField("max_gs_kt", DoubleType()),
        StructField("median_track_deg", DoubleType()),
        StructField("align_deg", DoubleType()),
        StructField("entry_height_ft", DoubleType()),
        StructField("exit_height_ft", DoubleType()),
        StructField("class", StringType()),
        StructField("osn_flight_id", StringType()),
    ]
)


def _traversal(spark, cls, entry_s, exit_s, **over):
    """One classified traversal, stated rather than derived.

    `runway_milestones` takes traversals as an argument precisely so the
    timing tests do not have to depend on the classifier as well; stating the
    row keeps each test about one thing.
    """
    row = dict(
        track_id="trk-1",
        apt_ident="EBBR",
        rwy_ident="07",
        rwy_bearing=70.0,
        thr_lat=50.0,
        thr_lon=4.0,
        trace_id="0",
        entry_time=_EPOCH + dt.timedelta(seconds=entry_s),
        exit_time=_EPOCH + dt.timedelta(seconds=exit_s),
        duration_seconds=float(exit_s - entry_s),
        max_gs_kt=170.0,
        median_track_deg=70.0,
        align_deg=3.0,
        entry_height_ft=0.0,
        exit_height_ft=800.0,
        **{"class": cls},
    )
    row["osn_flight_id"] = over.pop("osn_flight_id", "TEST123")
    row.update(over)
    return spark.createDataFrame(
        [tuple(row[f.name] for f in _TRAVERSAL_SCHEMA.fields)],
        schema=_TRAVERSAL_SCHEMA,
    )


def _departure_traversal(spark):
    """A departure traversal spanning the whole of every departure track here.

    The window is deliberately wider than any of them: `runway_milestones`
    selects the samples inside `[entry_time, exit_time]`, so one traversal can
    serve tracks of different lengths without the window becoming a hidden
    parameter of each test.
    """
    return _traversal(spark, "departure", 0, 120)


def _crossing_traversal(spark):
    """A taxiing crossing: perpendicular, slow, on the deck, half a minute."""
    return _traversal(
        spark, "crossing", 0, 30,
        max_gs_kt=18.0, median_track_deg=160.0, align_deg=88.0,
        entry_height_ft=0.0, exit_height_ft=0.0,
    )


def _departure_track(spark):
    """Hold, roll, rotate -- with each stage long enough to be distinguishable.

    Twenty-five seconds at taxi speed put `line-up` strictly before the roll;
    the roll then holds above 50 kt for four sample intervals, so
    `runway_roll_min_seconds` (5 s) is reached at the *second* fast sample and
    not at the first. The last on-deck sample is at 0 ft and the first airborne
    one at 100 ft, which straddles the 15 ft threshold with both samples
    outside its 5 ft dead band -- without that the crossing is never confirmed
    and the test would be asserting on a detector that had abstained.
    """
    samples = [
        {"t": i * 5, "velocity": _mps(15), "baro_altitude": 0.0,
         "vert_rate": 0.0, "heading": 70.0}
        for i in range(5)
    ]
    samples += [
        {"t": 25 + i * 5, "velocity": _mps(kt), "baro_altitude": 0.0,
         "vert_rate": 0.0, "heading": 70.0}
        for i, kt in enumerate([60, 90, 120, 150, 170])
    ]
    samples += [
        {"t": 50 + i * 5, "velocity": _mps(180), "baro_altitude": _m(ft),
         "vert_rate": 12.0, "heading": 70.0}
        for i, ft in enumerate([100, 400, 800])
    ]
    return _prepared(make_track(spark, samples))


def _taxi_track_with_one_fast_sample(spark):
    """A high-speed turn-off: one 60 kt sample, never sustained.

    Everything else is taxi speed and the aircraft never leaves the deck, so
    the only milestone that could plausibly fire is `take-off-roll` -- which is
    exactly what `runway_roll_min_seconds` exists to prevent.
    """
    speeds = [15, 15, 60, 15, 15]
    return _prepared(make_track(spark, [
        {"t": i * 5, "velocity": _mps(kt), "baro_altitude": 0.0,
         "vert_rate": 0.0, "heading": 70.0}
        for i, kt in enumerate(speeds)
    ]))


def _crossing_track(spark):
    """Slow, on the deck, across the strip. Nothing here is a movement."""
    return _prepared(make_track(spark, [
        {"t": i * 5, "velocity": _mps(16), "baro_altitude": 0.0,
         "vert_rate": 0.0, "heading": 160.0}
        for i in range(7)
    ]))


def _dest(lat, lon, bearing, dist_nm):
    """Point ``dist_nm`` along ``bearing`` from (lat, lon).

    The arrival tests need positions whose along-track distance from the
    threshold is a *stated* number, because `landing` is asserted to be the
    interpolated instant that distance changes sign.
    """
    radius = 3440.065
    br = math.radians(bearing)
    d = dist_nm / radius
    p1, l1 = math.radians(lat), math.radians(lon)
    p2 = math.asin(math.sin(p1) * math.cos(d) + math.cos(p1) * math.sin(d) * math.cos(br))
    l2 = l1 + math.atan2(
        math.sin(br) * math.sin(d) * math.cos(p1),
        math.cos(d) - math.sin(p1) * math.sin(p2),
    )
    return math.degrees(p2), math.degrees(l2)


#: (seconds, along-track NM from the threshold, height ft, groundspeed kt).
#: Negative along-track is short of the threshold. The pair (-0.2, 60) ->
#: (+0.1, 20) straddles the threshold plane two thirds of the way through, and
#: (20 ft) -> (5 ft) straddles 15 ft one third of the way through -- both
#: chosen so the interpolated answer is a number a reader can check by hand.
_ARRIVAL = [
    (0, -1.5, 500.0, 140),
    (10, -1.0, 340.0, 138),
    (20, -0.5, 180.0, 136),
    (30, -0.2, 60.0, 134),
    (40, 0.1, 20.0, 130),
    (50, 0.4, 5.0, 110),
    (60, 0.7, 0.0, 80),
    (70, 0.9, 0.0, 50),
    (80, 1.1, 0.0, 25),
]


def _arrival_track(spark):
    """A landing on runway 07, laid out along its own centreline."""
    samples = []
    for t, along_nm, height_ft, kt in _ARRIVAL:
        bearing = 70.0 if along_nm >= 0 else 250.0
        lat, lon = _dest(50.0, 4.0, bearing, abs(along_nm))
        samples.append({
            "t": t, "lat": lat, "lon": lon, "baro_altitude": _m(height_ft),
            "velocity": _mps(kt), "vert_rate": -3.0, "heading": 70.0,
        })
    return _prepared(make_track(spark, samples))


def _arrival_traversal(spark):
    return _traversal(
        spark, "arrival", 0, 80,
        max_gs_kt=140.0, align_deg=2.0,
        entry_height_ft=500.0, exit_height_ft=0.0,
    )


def _approach_track(spark, low_point_ft):
    """A descent to ``low_point_ft`` at t=40 s, then a climb away.

    500 ft is crossed downward at t=25 and 1,500 ft upward at t=63.75, so the
    excursion window is unambiguous; whether it is a go-around then turns only
    on how low the aircraft got, which is the one thing each caller varies.
    """
    profile = [
        (0, 2000.0, -8.0), (10, 1200.0, -8.0), (20, 700.0, -8.0),
        (30, 300.0, -4.0), (40, low_point_ft, 0.0), (50, 600.0, 10.0),
        (60, 1200.0, 10.0), (70, 2000.0, 10.0), (80, 2500.0, 10.0),
    ]
    sdf = _prepared(make_track(spark, [
        {"t": t, "baro_altitude": _m(ft), "vert_rate": vr,
         "velocity": _mps(140), "lat": 50.0, "lon": 4.0}
        for t, ft, vr in profile
    ]))
    return (
        sdf.withColumn("ades_lat", F.lit(50.0))
        .withColumn("ades_lon", F.lit(4.0))
        .withColumn("ades", F.lit("EBBR"))
    )


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def test_an_aligned_high_speed_traversal_that_lifts_off_is_a_departure(spark):
    c = EventConfig()
    row = spark.range(1).select(
        classify_traversal(
            align_deg=F.lit(2.0), max_gs_kt=F.lit(140.0),
            entry_height_ft=F.lit(0.0), exit_height_ft=F.lit(120.0),
            duration_seconds=F.lit(45.0), config=c,
        ).alias("cls")
    ).collect()[0]
    assert row["cls"] == "departure"


def test_a_slow_perpendicular_traversal_on_the_ground_is_a_crossing(spark):
    c = EventConfig()
    row = spark.range(1).select(
        classify_traversal(
            align_deg=F.lit(88.0), max_gs_kt=F.lit(18.0),
            entry_height_ft=F.lit(0.0), exit_height_ft=F.lit(0.0),
            duration_seconds=F.lit(25.0), config=c,
        ).alias("cls")
    ).collect()[0]
    assert row["cls"] == "crossing"


def test_the_detector_abstains_in_the_band_between_aligned_and_crossing(spark):
    """37 degrees is neither. Naming a milestone there would corrupt a movement
    count; naming none is an explicit null."""
    c = EventConfig()
    row = spark.range(1).select(
        classify_traversal(
            align_deg=F.lit(37.0), max_gs_kt=F.lit(30.0),
            entry_height_ft=F.lit(0.0), exit_height_ft=F.lit(0.0),
            duration_seconds=F.lit(60.0), config=c,
        ).alias("cls")
    ).collect()[0]
    assert row["cls"] is None


# ---------------------------------------------------------------------------
# Milestones
# ---------------------------------------------------------------------------

def test_airborne_is_interpolated_to_the_height_threshold(spark):
    """Two samples 10 s apart straddle 15 ft AGL at 5 ft and 25 ft.

    15 ft is exactly halfway, so `airborne` must land exactly 5 s after the
    first. A detector reporting either sample's own timestamp fails this.

    `make_track` carries only the raw schema and silently drops unknown keys,
    so `baro_altitude` is set and `_prepared` aliases it to `baro_altitude_c`
    and attaches zero field elevations -- the same adapter shape as
    `test_events_phase._measured`.
    """
    sv = make_track(spark, [
        {"t": 0,  "baro_altitude": _m(5.0),  "velocity": _mps(150), "vert_rate": 10.0},
        {"t": 10, "baro_altitude": _m(25.0), "velocity": _mps(160), "vert_rate": 10.0},
    ])
    out = runway_milestones(_prepared(sv), _departure_traversal(spark), EventConfig())
    airborne = out.filter(F.col("type") == "airborne").collect()
    assert len(airborne) == 1
    assert airborne[0]["event_time"].second == 5


def test_a_departure_emits_line_up_then_roll_then_airborne_in_order(spark):
    out = runway_milestones(_departure_track(spark), _departure_traversal(spark),
                            EventConfig())
    seq = [r["type"] for r in
           out.orderBy("event_time").filter(F.col("type") != "go-around").collect()]
    assert seq == ["line-up", "take-off-roll", "airborne"]


def test_take_off_roll_ignores_a_single_fast_sample(spark):
    """One 60 kt sample during a high-speed turn-off is not a take-off roll:
    `runway_roll_min_seconds` requires the speed to persist."""
    out = runway_milestones(_taxi_track_with_one_fast_sample(spark),
                            _departure_traversal(spark), EventConfig())
    assert out.filter(F.col("type") == "take-off-roll").count() == 0


def test_a_crossing_emits_only_the_two_crossing_types(spark):
    out = runway_milestones(_crossing_track(spark), _crossing_traversal(spark),
                            EventConfig())
    assert sorted(r["type"] for r in out.collect()) == [
        "runway-crossing-entry", "runway-crossing-vacated"
    ]


def test_no_runway_milestone_is_emitted_under_the_legacy_configuration(spark):
    out = runway_milestones(_departure_track(spark), _departure_traversal(spark),
                            EventConfig.legacy())
    assert out.count() == 0


def test_an_arrival_emits_landing_then_touchdown_then_vacated(spark):
    """`landing` is the threshold plane, `touchdown` the 15 ft crossing.

    They are different instants and the arrival profile puts them 6.7 s apart,
    so a detector that collapsed the two -- as the retired ground-contact
    `landing` effectively did -- fails on the order as well as the count.
    """
    out = runway_milestones(_arrival_track(spark), _arrival_traversal(spark),
                            EventConfig())
    seq = [r["type"] for r in out.orderBy("event_time").collect()]
    assert seq == ["landing", "touchdown", "runway-vacated"]


def test_landing_is_interpolated_to_the_threshold_plane(spark):
    """-0.2 NM at t=30 and +0.1 NM at t=40 put the plane two thirds through,
    i.e. at t=36.67 s -- not at either sample."""
    out = runway_milestones(_arrival_track(spark), _arrival_traversal(spark),
                            EventConfig())
    landing = out.filter(F.col("type") == "landing").collect()
    assert len(landing) == 1
    offset = (landing[0]["event_time"] - _EPOCH).total_seconds()
    assert offset == pytest.approx(36.667, abs=0.1)


def test_every_milestone_carries_its_icao_number(spark):
    """The number is what lets a consumer tell the new threshold-crossing
    `landing` from the retired ground-contact one without parsing versions."""
    out = runway_milestones(_arrival_track(spark), _arrival_traversal(spark),
                            EventConfig())
    numbers = {r["type"]: json.loads(r["info"]).get("milestone")
               for r in out.collect()}
    assert numbers["landing"] == "T16"
    assert numbers["touchdown"] == "T17"
    assert numbers["runway-vacated"] == "T19"


def test_milestone_info_names_the_runway_and_the_classification(spark):
    out = runway_milestones(_arrival_track(spark), _arrival_traversal(spark),
                            EventConfig())
    info = json.loads(out.filter(F.col("type") == "touchdown").collect()[0]["info"])
    assert info["rwy_ident"] == "07"
    assert info["apt_icao"] == "EBBR"
    assert info["traversal_class"] == "arrival"
    assert info["osn_flight_id"] == "TEST123"


# ---------------------------------------------------------------------------
# Traversals, end to end
# ---------------------------------------------------------------------------

def _layouts(spark):
    """One runway polygon cell, plus a taxiway cell that must not be read.

    The H3 identifiers are opaque strings joined by equality, so no H3 library
    is needed to state which samples fall inside which polygon.
    """
    return spark.createDataFrame(
        [
            ("cell-rwy", "EBBR", "runway", "osm-1", "07/25"),
            ("cell-twy", "EBBR", "taxiway", "osm-2", "B"),
        ],
        "hexaero_h3_id string, hexaero_apt_icao string, hexaero_aeroway string, "
        "hexaero_osm_id string, hexaero_ref string",
    )


def _thresholds(spark):
    """Both directions of one strip: 07 at (50, 4) and 25 two miles up it."""
    lat25, lon25 = _dest(50.0, 4.0, 70.0, 2.0)
    return spark.createDataFrame(
        [("EBBR", "07", 50.0, 4.0, 70.0), ("EBBR", "25", lat25, lon25, 250.0)],
        "apt_ident string, rwy_ident string, thr_lat double, thr_lon double, "
        "rwy_bearing double",
    )


def _departure_on_25(spark):
    """The same departure profile, rolling *down* the strip on runway 25.

    Departing on 25 rather than 07 is what makes the direction check bite: an
    alphabetical tie-break would name 07, and the two directions share one
    centreline so cross-track distance cannot separate them either. Only the
    unfolded bearing error can.
    """
    sdf = _departure_track(spark).withColumn("heading", F.lit(250.0))
    positions = [_dest(50.0, 4.0, 70.0, 2.0 - 0.05 * i) for i in range(13)]
    lat_arr = F.array(*[F.lit(p[0]) for p in positions])
    lon_arr = F.array(*[F.lit(p[1]) for p in positions])
    idx = F.row_number().over(Window.partitionBy("track_id").orderBy("event_time"))
    sdf = sdf.withColumn("_i", idx)
    return (
        sdf.withColumn("lat", F.element_at(lat_arr, F.col("_i")))
        .withColumn("lon", F.element_at(lon_arr, F.col("_i")))
        .drop("_i")
        .withColumn("h3_res_12", F.lit("cell-rwy"))
        .withColumn("apt", F.array(F.lit("EBBR")))
        .withColumn("flight_id", F.lit("TEST123"))
    )


def test_runway_traversals_classifies_a_departure_and_names_its_direction(spark):
    out = runway_traversals(
        _departure_on_25(spark), _layouts(spark), _thresholds(spark), EventConfig()
    ).collect()

    assert len(out) == 1
    assert out[0]["class"] == "departure"
    assert out[0]["rwy_ident"] == "25"
    assert out[0]["apt_ident"] == "EBBR"
    assert out[0]["align_deg"] == pytest.approx(0.0, abs=0.5)


# ---------------------------------------------------------------------------
# Go-around
# ---------------------------------------------------------------------------

def test_a_go_around_is_stamped_at_the_lowest_point(spark):
    """The instant the approach was abandoned, not the instant it recovered."""
    out = go_arounds(_approach_track(spark, 200.0), EventConfig()).collect()
    assert len(out) == 1
    assert out[0]["type"] == "go-around"
    assert (out[0]["event_time"] - _EPOCH).total_seconds() == pytest.approx(40.0)


def test_an_excursion_that_reaches_the_deck_is_a_landing_not_a_go_around(spark):
    """A touchdown inside the window means the aircraft landed and departed
    again; publishing that as a go-around would invent an abandoned approach."""
    out = go_arounds(_approach_track(spark, 0.0), EventConfig())
    assert out.count() == 0


def test_no_go_around_is_emitted_under_the_legacy_configuration(spark):
    out = go_arounds(_approach_track(spark, 200.0), EventConfig.legacy())
    assert out.count() == 0
