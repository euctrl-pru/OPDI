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
    MILESTONE_SCHEMA,
    along_track_nm,
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

def _prepared(sdf, elev_adep_ft=0.0, elev_ades_ft=0.0, adep=None, ades=None):
    """Add what step 02 and the flight-list join add and TRACK_SCHEMA does not.

    ``baro_altitude_c`` is the rolling-mean repair step 02 writes and every
    event detector reads; the cumulative measures are what the measurement
    helpers attach; the two elevations are what ``attach_field_elevation``
    attaches. All default to a sea-level field so a test only has to state the
    geometry it cares about.

    ``adep``/``ades`` are what ``attach_aerodrome_geometry`` adds and what lets
    a height be measured against *one named* aerodrome. Omitted by default, so
    a test that does not care about the two-field distinction gets the
    permissive fallback and reads exactly as it did before this existed.
    """
    out = (
        sdf.withColumn("baro_altitude_c", F.col("baro_altitude"))
        .withColumn("cumulative_distance_nm", F.lit(0.0))
        .withColumn("cumulative_time_s", F.lit(0.0))
        .withColumn("elev_adep_ft", F.lit(elev_adep_ft))
        .withColumn("elev_ades_ft", F.lit(elev_ades_ft))
    )
    if adep is not None or ades is not None:
        out = out.withColumn("adep", F.lit(adep)).withColumn("ades", F.lit(ades))
    return out


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


def _departure_track(spark, elev_adep_ft=0.0, elev_ades_ft=0.0,
                     adep=None, ades=None):
    """Hold, roll, rotate -- with each stage long enough to be distinguishable.

    Altitudes are stated as height above the *departure* field and offset by
    ``elev_adep_ft`` here, because that is what a barometric altimeter reads:
    an aircraft holding at a 24 ft aerodrome broadcasts 24 ft, not 0.

    Twenty-five seconds at taxi speed put `line-up` strictly before the roll;
    the roll then holds above 50 kt for four sample intervals, so
    `runway_roll_min_seconds` (5 s) is reached at the *second* fast sample and
    not at the first. The last on-deck sample is at 0 ft and the first airborne
    one at 100 ft, which straddles the 15 ft threshold with both samples
    outside its 5 ft dead band -- without that the crossing is never confirmed
    and the test would be asserting on a detector that had abstained.
    """
    samples = [
        {"t": i * 5, "velocity": _mps(15), "baro_altitude": _m(elev_adep_ft),
         "vert_rate": 0.0, "heading": 70.0}
        for i in range(5)
    ]
    samples += [
        {"t": 25 + i * 5, "velocity": _mps(kt), "baro_altitude": _m(elev_adep_ft),
         "vert_rate": 0.0, "heading": 70.0}
        for i, kt in enumerate([60, 90, 120, 150, 170])
    ]
    samples += [
        {"t": 50 + i * 5, "velocity": _mps(180), "baro_altitude": _m(elev_adep_ft + ft),
         "vert_rate": 12.0, "heading": 70.0}
        for i, ft in enumerate([100, 400, 800])
    ]
    return _prepared(make_track(spark, samples), elev_adep_ft, elev_ades_ft,
                     adep, ades)


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


#: (seconds, along-track NM from the threshold, height ft above the arrival
#: field, groundspeed kt). Negative along-track is short of the threshold. The
#: pair (-0.2, 60) -> (+0.1, 30) straddles the threshold plane two thirds of
#: the way through (t = 36.67 s), and (30 ft) -> (5 ft) straddles 15 ft three
#: fifths of the way through (t = 46 s) -- both chosen so the interpolated
#: answer is a number a reader can check by hand.
#:
#: **Only the samples at or beyond the threshold are inside the runway
#: polygon.** That is the shape a real traversal has, and it is why the T16
#: crossing needs `ARRIVAL_LEAD_SECONDS`: the pair that straddles the plane has
#: one endpoint outside the polygon by definition.
_ARRIVAL = [
    (0, -1.5, 500.0, 140),
    (10, -1.0, 340.0, 138),
    (20, -0.5, 180.0, 136),
    (30, -0.2, 60.0, 134),
    (40, 0.1, 30.0, 130),
    (50, 0.4, 5.0, 110),
    (60, 0.7, 0.0, 80),
    (70, 0.9, 0.0, 50),
    (80, 1.1, 0.0, 25),
]

#: The first and last sample of `_ARRIVAL` that a runway polygon covers, i.e.
#: the entry and exit a real `runway_traversals` would derive. Named rather
#: than written twice, because the hand-built traversal and the end-to-end one
#: have to agree about it or the two tests are about different runways.
_ARRIVAL_ENTRY_S, _ARRIVAL_EXIT_S = 40, 80


def _arrival_samples(elev_ades_ft=0.0):
    """The arrival profile as raw samples, on runway 07's own centreline.

    Altitudes are offset by the arrival field's elevation, because that is what
    a barometric altimeter reads: an aircraft on the deck at a 24 ft aerodrome
    broadcasts 24 ft.
    """
    out = []
    for t, along_nm, height_ft, kt in _ARRIVAL:
        bearing = 70.0 if along_nm >= 0 else 250.0
        lat, lon = _dest(50.0, 4.0, bearing, abs(along_nm))
        out.append({
            "t": t, "lat": lat, "lon": lon,
            "baro_altitude": _m(elev_ades_ft + height_ft),
            "velocity": _mps(kt), "vert_rate": -3.0, "heading": 70.0,
        })
    return out


def _arrival_track(spark, elev_adep_ft=0.0, elev_ades_ft=0.0,
                   adep=None, ades=None):
    """A landing on runway 07, laid out along its own centreline."""
    return _prepared(make_track(spark, _arrival_samples(elev_ades_ft)),
                     elev_adep_ft, elev_ades_ft, adep, ades)


def _arrival_traversal(spark):
    """The traversal `runway_traversals` derives from `_arrival_track`.

    Bounded by the runway polygon, so it starts at the first sample *at* the
    threshold -- not 1.5 NM out on final. An earlier version of this fixture
    declared the wider window, and `landing` passed on a traversal shape the
    detector could never produce.
    """
    return _traversal(
        spark, "arrival", _ARRIVAL_ENTRY_S, _ARRIVAL_EXIT_S,
        max_gs_kt=130.0, align_deg=2.0,
        entry_height_ft=30.0, exit_height_ft=0.0,
    )


def _approach_track(spark, low_point_ft, elev_adep_ft=0.0, elev_ades_ft=0.0,
                    adep="LSZH", ades="EBBR"):
    """A descent to ``low_point_ft`` at t=40 s, then a climb away.

    500 ft is crossed downward at t=25 and 1,500 ft upward at t=63.75, so the
    excursion window is unambiguous; whether it is a go-around then turns only
    on how low the aircraft got, which is the one thing each caller varies.

    The profile is stated as height above the **arrival** field and offset by
    ``elev_ades_ft``, because that is the field a go-around is measured
    against: the aircraft is on final at its destination.
    """
    profile = [
        (0, 2000.0, -8.0), (10, 1200.0, -8.0), (20, 700.0, -8.0),
        (30, 300.0, -4.0), (40, low_point_ft, 0.0), (50, 600.0, 10.0),
        (60, 1200.0, 10.0), (70, 2000.0, 10.0), (80, 2500.0, 10.0),
    ]
    sdf = _prepared(make_track(spark, [
        {"t": t, "baro_altitude": _m(elev_ades_ft + ft), "vert_rate": vr,
         "velocity": _mps(140), "lat": 50.0, "lon": 4.0}
        for t, ft, vr in profile
    ]), elev_adep_ft, elev_ades_ft)
    return (
        sdf.withColumn("ades_lat", F.lit(50.0))
        .withColumn("ades_lon", F.lit(4.0))
        .withColumn("adep", F.lit(adep))
        .withColumn("ades", F.lit(ades))
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


def _two_aerodrome_crossing_track(spark):
    """One track that crosses a runway called 07 at each end of its flight.

    An hour apart, so nothing but the traversal key can tell the two apart.
    Duplicate runway designators between origin and destination are ordinary --
    07/25 and 09/27 are among the commonest in Europe -- so a key that cannot
    separate them silently merges a departure aerodrome's crossing with an
    arrival aerodrome's.
    """
    samples = [
        {"t": base + i * 5, "velocity": _mps(16), "baro_altitude": 0.0,
         "vert_rate": 0.0, "heading": 160.0}
        for base in (0, 3600) for i in range(7)
    ]
    return _prepared(make_track(spark, samples))


def _two_aerodrome_crossing_traversals(spark):
    """Two crossings of a runway named 07, at two different aerodromes.

    ``trace_id`` is 0 for both, which is not a fixture convenience but what the
    detector produces: it restarts per (track, OSM way) and so carries no
    information across aerodromes.
    """
    common = dict(
        max_gs_kt=18.0, median_track_deg=160.0, align_deg=88.0,
        entry_height_ft=0.0, exit_height_ft=0.0,
    )
    first = _traversal(spark, "crossing", 0, 30, apt_ident="EBBR", **common)
    second = _traversal(spark, "crossing", 3600, 3630, apt_ident="EHAM", **common)
    return first.unionByName(second)


def _roll_across_a_coverage_hole(spark):
    """Two fast samples 200 s apart and nothing in between.

    Both are above the roll speed, so a hold measured as wall-clock reads 200 s
    and confirms a take-off roll that was never observed. A traversal may
    legitimately span a gap of up to `airport_trace_gap_seconds` (300 s), so
    this is inside the shape of a real traversal, not a malformed one.
    """
    return _prepared(make_track(spark, [
        {"t": 0, "velocity": _mps(60), "baro_altitude": 0.0,
         "vert_rate": 0.0, "heading": 70.0},
        {"t": 200, "velocity": _mps(60), "baro_altitude": 0.0,
         "vert_rate": 0.0, "heading": 70.0},
    ]))


def test_take_off_roll_is_stamped_at_the_start_of_the_roll(spark):
    """The hold confirms the roll; it does not date it.

    `_departure_track` first exceeds 50 kt at t=25 s and holds it to the end,
    so `runway_roll_min_seconds` is satisfied at t=30. Reporting t=30 would put
    every T07 one hold-duration late, always in the same direction -- the bias
    this module exists to remove from `ATOT`, reintroduced under a new name.
    """
    out = runway_milestones(_departure_track(spark), _departure_traversal(spark),
                            EventConfig())
    roll = out.filter(F.col("type") == "take-off-roll").collect()
    assert len(roll) == 1
    assert (roll[0]["event_time"] - _EPOCH).total_seconds() == pytest.approx(25.0)


def test_take_off_roll_does_not_hold_across_a_coverage_hole(spark):
    """A 200 s gap is not a 200 s roll."""
    out = runway_milestones(_roll_across_a_coverage_hole(spark),
                            _traversal(spark, "departure", 0, 250), EventConfig())
    assert out.filter(F.col("type") == "take-off-roll").count() == 0


def test_two_crossings_of_a_same_named_runway_stay_two_crossings(spark):
    """One 07 at the origin, another 07 at the destination, an hour apart.

    With a traversal key blind to the aerodrome these collapse into a single
    pair whose entry is the origin's and whose exit is the destination's --
    one "crossing" straddling the entire flight, and two real ones lost.
    """
    out = runway_milestones(_two_aerodrome_crossing_track(spark),
                            _two_aerodrome_crossing_traversals(spark),
                            EventConfig()).collect()

    assert len(out) == 4
    seen = {(r["type"], json.loads(r["info"])["apt_icao"]) for r in out}
    assert seen == {
        ("runway-crossing-entry", "EBBR"), ("runway-crossing-vacated", "EBBR"),
        ("runway-crossing-entry", "EHAM"), ("runway-crossing-vacated", "EHAM"),
    }


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

def _grid(spark, apt="EBBR"):
    """The ``h3_runway_zones`` rows for one strip: a runway cell and an
    approach cell, both of strip ``07/25``.

    The H3 identifiers are opaque strings joined by equality, so no H3 library
    is needed to state which cell a sample falls in. ``cell-rwy`` is the
    occupancy cell (``zone = "runway"``); ``cell-air`` is the approach corridor
    (``zone = "approach"``), which the geometry test keeps out of the traversal
    but the arrival window still reaches through the state vectors.
    """
    return spark.createDataFrame(
        [
            ("cell-rwy", apt, apt + "-07/25", "07", "25", "runway"),
            ("cell-air", apt, apt + "-07/25", "07", "25", "approach"),
        ],
        "h3_id string, apt_icao string, strip_id string, "
        "le_ident string, he_ident string, zone string",
    )


def _thresholds(spark, apt="EBBR"):
    """Both directions of one strip: 07 at (50, 4) and 25 two miles up it.

    The strip is 2 NM long and carries a generous half-width, so a sample on
    the centreline anywhere between the two thresholds passes the geometric
    on-runway test.
    """
    lat25, lon25 = _dest(50.0, 4.0, 70.0, 2.0)
    return spark.createDataFrame(
        [(apt, "07", 50.0, 4.0, 70.0, 2.0, 0.05),
         (apt, "25", lat25, lon25, 250.0, 2.0, 0.05)],
        "apt_ident string, rwy_ident string, thr_lat double, thr_lon double, "
        "rwy_bearing double, rwy_length_nm double, rwy_half_width_nm double",
    )


def _as_flown(sdf, positions=None, cells="cell-rwy", apt=("EBBR",)):
    """What `_runway_traversal_family` adds before `runway_traversals` runs.

    ``positions`` is one (lat, lon) per sample in time order and ``cells`` one
    H3 identifier per sample -- or a single identifier for all of them. The
    flight-list aerodrome array and the resolved callsign complete the frame.

    Stated per sample rather than derived, because which samples a runway
    polygon covers is the very thing `ARRIVAL_LEAD_SECONDS` exists for: a
    fixture that puts the whole final approach inside the polygon tests a
    traversal shape the detector cannot produce.
    """
    idx = F.row_number().over(Window.partitionBy("track_id").orderBy("event_time"))
    out = sdf.withColumn("_i", idx)
    if positions is not None:
        lat_arr = F.array(*[F.lit(p[0]) for p in positions])
        lon_arr = F.array(*[F.lit(p[1]) for p in positions])
        out = (
            out.withColumn("lat", F.element_at(lat_arr, F.col("_i")))
            .withColumn("lon", F.element_at(lon_arr, F.col("_i")))
        )
    if isinstance(cells, str):
        out = out.withColumn("h3_res_12", F.lit(cells))
    else:
        cell_arr = F.array(*[F.lit(c) for c in cells])
        out = out.withColumn("h3_res_12", F.element_at(cell_arr, F.col("_i")))
    return (
        out.drop("_i")
        .withColumn("apt", F.array(*[F.lit(a) for a in apt]))
        .withColumn("flight_id", F.lit("TEST123"))
    )


def _departure_on_25(spark):
    """The same departure profile, rolling *down* the strip on runway 25.

    Departing on 25 rather than 07 is what makes the direction check bite: an
    alphabetical tie-break would name 07, and the two directions share one
    centreline so cross-track distance cannot separate them either. Only the
    unfolded bearing error can.
    """
    return _as_flown(
        _departure_track(spark).withColumn("heading", F.lit(250.0)),
        positions=[_dest(50.0, 4.0, 70.0, 2.0 - 0.05 * i) for i in range(13)],
    )


def test_runway_traversals_classifies_a_departure_and_names_its_direction(spark):
    out = runway_traversals(
        _departure_on_25(spark), _grid(spark), _thresholds(spark), EventConfig()
    ).collect()

    assert len(out) == 1
    assert out[0]["class"] == "departure"
    assert out[0]["rwy_ident"] == "25"
    assert out[0]["apt_ident"] == "EBBR"
    assert out[0]["align_deg"] == pytest.approx(0.0, abs=0.5)


def _arrival_on_07(spark, elev_adep_ft=0.0, elev_ades_ft=0.0,
                   adep="EBBR", ades="EBBR"):
    """The arrival profile with the H3 cells a real runway polygon gives it.

    The polygon starts at the threshold, so the four samples short of it are
    airborne cells and only the last five are runway cells. `runway_traversals`
    therefore derives `entry_time` = 40 s, which is exactly the shape that
    makes `landing` unreachable without the arrival lead.
    """
    return _as_flown(
        _arrival_track(spark, elev_adep_ft, elev_ades_ft, adep, ades),
        cells=["cell-air" if along < 0 else "cell-rwy"
               for _, along, _, _ in _ARRIVAL],
        apt=(adep, ades),
    )


def test_runway_traversals_derives_an_arrival_bounded_by_the_polygon(spark):
    """The entry is the threshold, not the point 1.5 NM out on final."""
    out = runway_traversals(
        _arrival_on_07(spark), _grid(spark), _thresholds(spark), EventConfig()
    ).collect()

    assert len(out) == 1
    assert out[0]["class"] == "arrival"
    assert out[0]["rwy_ident"] == "07"
    assert (out[0]["entry_time"] - _EPOCH).total_seconds() == _ARRIVAL_ENTRY_S
    assert (out[0]["exit_time"] - _EPOCH).total_seconds() == _ARRIVAL_EXIT_S


def test_landing_fires_on_a_traversal_the_detector_actually_produces(spark):
    """End to end: the polygon-bounded traversal, then T16 through the lead.

    The straddling pair is (-0.2 NM at t=30, +0.1 NM at t=40) and the first of
    those is *outside* the polygon. Without `ARRIVAL_LEAD_SECONDS` the sample
    is not in the traversal's frame, the Schmitt trigger never sees a negative
    along-track distance, and `landing` cannot fire at all -- which is what
    production did while a hand-built traversal made the unit test pass.
    """
    sv = _arrival_on_07(spark)
    traversals = runway_traversals(sv, _grid(spark), _thresholds(spark),
                                   EventConfig())
    out = runway_milestones(sv, traversals, EventConfig())

    landing = out.filter(F.col("type") == "landing").collect()
    assert len(landing) == 1
    assert (landing[0]["event_time"] - _EPOCH).total_seconds() == pytest.approx(
        36.667, abs=0.1
    )


def test_runway_traversals_classifies_a_taxiing_crossing(spark):
    """Perpendicular, slow, on the deck -- and not a movement."""
    out = runway_traversals(
        _as_flown(_crossing_track(spark)),
        _grid(spark), _thresholds(spark), EventConfig(),
    ).collect()

    assert len(out) == 1
    assert out[0]["class"] == "crossing"


def test_runway_traversals_abstains_in_the_band_between_the_two(spark):
    """37 degrees off the centreline is neither aligned nor crossing.

    An abstention leaves *no traversal at all*, so nothing downstream can
    invent a milestone for it. Asserted end to end rather than on
    `classify_traversal` alone, because the filter that drops the abstaining
    row lives in `runway_traversals`.
    """
    oblique = _as_flown(
        _crossing_track(spark).withColumn("heading", F.lit(107.0))
    )
    out = runway_traversals(oblique, _grid(spark), _thresholds(spark),
                            EventConfig())

    assert out.count() == 0


# ---------------------------------------------------------------------------
# Per-aerodrome height references.
#
# Every fixture above sits at a sea-level field, which is exactly the blind
# spot: with both elevations zero, "the height above the traversal's own
# aerodrome" and "the height above the more permissive of the flight's two
# fields" are the same number. These three state two *different* fields.
# ---------------------------------------------------------------------------

#: Ibiza and Zurich -- the pair `EventConfig.level_floors_above_field` names,
#: reused so the whole codebase argues about one concrete altitude difference.
_LOW_FIELD_FT = 24.0
_HIGH_FIELD_FT = 1416.0


def test_a_departure_from_the_lower_of_two_aerodromes_still_lifts_off(spark):
    """A 24 ft origin, a 1,416 ft destination, and a departure at the origin.

    Measured against the more permissive of the two fields -- the higher one,
    which is what `least()` selects -- every sample of this roll has a height
    around -1,400 ft. `classify_traversal` then finds no exit above 15 ft, sees
    no arrival and no crossing either, and abstains: `line-up`, `take-off-roll`
    and `airborne` are all silently absent for the whole flight.

    Against the traversal's own aerodrome the roll is at 0 ft and the first
    airborne sample at 100 ft, so 15 ft is crossed 15% of the way through the
    5 s interval ending at t=50 -- t=45.75 s.
    """
    sv = _as_flown(
        _departure_track(spark, elev_adep_ft=_LOW_FIELD_FT,
                         elev_ades_ft=_HIGH_FIELD_FT, adep="EBBR", ades="LSZH"),
        positions=[_dest(50.0, 4.0, 70.0, 0.05 * i) for i in range(13)],
        apt=("EBBR", "LSZH"),
    )
    traversals = runway_traversals(sv, _grid(spark), _thresholds(spark),
                                   EventConfig())

    rows = traversals.collect()
    assert len(rows) == 1, "the departure was classified away by the wrong field"
    assert rows[0]["class"] == "departure"

    airborne = runway_milestones(sv, traversals, EventConfig()).filter(
        F.col("type") == "airborne"
    ).collect()
    assert len(airborne) == 1
    assert (airborne[0]["event_time"] - _EPOCH).total_seconds() == pytest.approx(
        45.75, abs=0.01
    )


def test_an_arrival_at_the_lower_of_two_aerodromes_still_touches_down(spark):
    """The symmetric case: a 1,416 ft origin and a 24 ft destination.

    The permissive height is again about -1,400 ft throughout, so no sample
    enters above 15 ft and the arrival is never classified -- no `landing`, no
    `touchdown`, no `runway-vacated`. Against the arrival's own field the
    profile is the ordinary one, and 15 ft is crossed three fifths of the way
    between the 30 ft and 5 ft samples: t = 46 s.
    """
    sv = _arrival_on_07(spark, elev_adep_ft=_HIGH_FIELD_FT,
                        elev_ades_ft=_LOW_FIELD_FT, adep="LSZH", ades="EBBR")
    traversals = runway_traversals(sv, _grid(spark), _thresholds(spark),
                                   EventConfig())

    rows = traversals.collect()
    assert len(rows) == 1, "the arrival was classified away by the wrong field"
    assert rows[0]["class"] == "arrival"

    touchdown = runway_milestones(sv, traversals, EventConfig()).filter(
        F.col("type") == "touchdown"
    ).collect()
    assert len(touchdown) == 1
    assert (touchdown[0]["event_time"] - _EPOCH).total_seconds() == pytest.approx(
        46.0, abs=0.01
    )


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


def test_a_go_around_at_a_destination_below_the_origin_still_fires(spark):
    """A 1,416 ft origin and a 24 ft destination, abandoned at 200 ft AGL.

    ``go-around`` is arrival-anchored by definition, so its heights belong to
    the destination field. Measured against the permissive minimum -- the
    origin's, here -- the whole excursion sits about 1,400 ft low: it never
    reaches the 1,500 ft recovery height, so no excursion window is ever
    formed, and had one been, the 200 ft low point would read as below the deck
    and be discarded as a landing. Either way the go-around is suppressed at
    exactly the aerodromes where an approach is most often abandoned.
    """
    track = _approach_track(spark, 200.0, elev_adep_ft=_HIGH_FIELD_FT,
                            elev_ades_ft=_LOW_FIELD_FT)
    out = go_arounds(track, EventConfig()).collect()

    assert len(out) == 1
    assert (out[0]["event_time"] - _EPOCH).total_seconds() == pytest.approx(40.0)


def test_no_go_around_is_emitted_under_the_legacy_configuration(spark):
    out = go_arounds(_approach_track(spark, 200.0), EventConfig.legacy())
    assert out.count() == 0


# ---------------------------------------------------------------------------
# Grid + geometry, end to end.
#
# The detector now prunes on the ``h3_runway_zones`` grid and refines with the
# along/cross geometry rather than trusting a hexaero polygon, so these state a
# grid frame in place of the old layouts one. Field elevations are non-zero
# where the point is to catch a height measured against the wrong aerodrome --
# the flat 0 ft airports the first attempt used could never see it.
# ---------------------------------------------------------------------------

def test_along_track_nm_sign_and_magnitude(spark):
    """Signed distance along runway 07 (threshold (50, 4), bearing 070).

    A point 1 NM beyond the threshold on the runway bearing reads +1.0; 1 NM
    short of it on the reciprocal reads -1.0; a point 1 NM abeam reads ~0. The
    sign is what makes ``landing`` a zero crossing and the magnitude what the
    on-runway test bounds against the runway length.
    """
    past = _dest(50.0, 4.0, 70.0, 1.0)
    before = _dest(50.0, 4.0, 250.0, 1.0)
    abeam = _dest(50.0, 4.0, 160.0, 1.0)
    rows = spark.createDataFrame(
        [("past", past[0], past[1]),
         ("before", before[0], before[1]),
         ("abeam", abeam[0], abeam[1])],
        "kind string, lat double, lon double",
    ).select(
        F.col("kind"),
        along_track_nm(
            F.col("lat"), F.col("lon"), F.lit(50.0), F.lit(4.0), F.lit(70.0)
        ).alias("along"),
    )
    got = {r["kind"]: r["along"] for r in rows.collect()}
    assert got["past"] == pytest.approx(1.0, abs=0.01)
    assert got["before"] == pytest.approx(-1.0, abs=0.01)
    assert got["abeam"] == pytest.approx(0.0, abs=0.05)


def test_departure_at_a_grid_airport_with_high_field_emits_airborne(spark):
    """A departure from a 1,416 ft field (LSZH), pruned on the grid.

    Coverage first: under hexaero this aerodrome had no runway polygon and the
    whole departure -- ``line-up``, ``take-off-roll``, ``airborne`` -- was
    silently absent. The grid covers it.

    And the height reference second: the destination here is *higher* still
    (2,000 ft), so measuring against the permissive two-field minimum -- which
    subtracts the higher field, 2,000 ft -- puts every sample of the roll about
    -580 ft, no sample crosses 15 ft upward, ``classify_traversal`` sees no
    departure and abstains. Against the traversal's own 1,416 ft field the roll
    is at 0 ft AGL and 15 ft is crossed 15 % of the way through the interval
    ending at t=50, i.e. t=45.75 s. A detector that regressed to the permissive
    minimum emits nothing at all and fails here.
    """
    sv = _as_flown(
        _departure_track(spark, elev_adep_ft=1416.0, elev_ades_ft=2000.0,
                         adep="LSZH", ades="LEMD"),
        positions=[_dest(50.0, 4.0, 70.0, 0.05 * i) for i in range(13)],
        apt=("LSZH", "LEMD"),
    )
    traversals = runway_traversals(
        sv, _grid(spark, apt="LSZH"), _thresholds(spark, apt="LSZH"), EventConfig()
    )
    rows = traversals.collect()
    assert len(rows) == 1, "the high-field departure was classified away"
    assert rows[0]["class"] == "departure"

    out = runway_milestones(sv, traversals, EventConfig())
    seq = [r["type"] for r in
           out.orderBy("event_time").filter(F.col("type") != "go-around").collect()]
    assert seq == ["line-up", "take-off-roll", "airborne"]

    airborne = out.filter(F.col("type") == "airborne").collect()
    assert len(airborne) == 1
    assert (airborne[0]["event_time"] - _EPOCH).total_seconds() == pytest.approx(
        45.75, abs=0.01
    )


def test_arrival_from_final_approach_emits_touchdown(spark):
    """A descending final approach, pruned on the grid, all the way to rollout.

    The approach samples are ``approach``-zone cells short of the threshold,
    descending through the field; the geometry test keeps them out of the
    occupancy traversal, but the arrival window reaches back across them so the
    threshold-plane crossing has a sample with a negative along-track distance
    to interpolate from. All three arrival milestones fire, in order, and
    ``touchdown`` -- which emitted for essentially no arrival under hexaero --
    is among them. Remove the approach corridor (the arrival lead) and
    ``landing`` disappears, breaking the sequence.
    """
    sv = _arrival_on_07(spark)
    traversals = runway_traversals(sv, _grid(spark), _thresholds(spark), EventConfig())
    out = runway_milestones(sv, traversals, EventConfig())

    seq = [r["type"] for r in out.orderBy("event_time").collect()]
    assert seq == ["landing", "touchdown", "runway-vacated"]
    assert out.filter(F.col("type") == "touchdown").count() == 1


def test_a_crossing_emits_the_two_crossing_types_end_to_end(spark):
    """A slow perpendicular taxi across the strip, pruned on the grid, yields a
    crossing pair and nothing that could be counted as a movement."""
    sv = _as_flown(_crossing_track(spark))
    traversals = runway_traversals(sv, _grid(spark), _thresholds(spark), EventConfig())
    out = runway_milestones(sv, traversals, EventConfig())
    assert sorted(r["type"] for r in out.collect()) == [
        "runway-crossing-entry", "runway-crossing-vacated"
    ]


def test_an_oblique_traversal_emits_no_milestone(spark):
    """37 degrees off the centreline is neither aligned nor a crossing, so the
    traversal abstains and nothing downstream can invent a milestone for it."""
    oblique = _as_flown(
        _crossing_track(spark).withColumn("heading", F.lit(107.0))
    )
    traversals = runway_traversals(oblique, _grid(spark), _thresholds(spark),
                                   EventConfig())
    out = runway_milestones(oblique, traversals, EventConfig())
    assert out.count() == 0


def test_runway_milestones_is_an_empty_frame_when_the_grid_prunes_everything(spark):
    """No grid cell matches, so no traversal forms and the family returns the
    standard empty event frame rather than ``None`` or a differently shaped
    one -- the contract a caller unioning families depends on."""
    empty_grid = spark.createDataFrame(
        [],
        "h3_id string, apt_icao string, strip_id string, "
        "le_ident string, he_ident string, zone string",
    )
    traversals = runway_traversals(
        _departure_on_25(spark), empty_grid, _thresholds(spark), EventConfig()
    )
    assert traversals.count() == 0

    out = runway_milestones(_departure_on_25(spark), traversals, EventConfig())
    assert out.count() == 0
    assert [f.name for f in out.schema.fields] == [
        f.name for f in MILESTONE_SCHEMA.fields
    ]


def test_no_traversal_milestone_under_the_legacy_configuration_end_to_end(spark):
    """Legacy switches the whole family off: an empty, standard-shaped frame,
    whatever the grid says."""
    out = runway_milestones(
        _departure_on_25(spark),
        runway_traversals(_departure_on_25(spark), _grid(spark), _thresholds(spark),
                          EventConfig()),
        EventConfig.legacy(),
    )
    assert out.count() == 0
    assert [f.name for f in out.schema.fields] == [
        f.name for f in MILESTONE_SCHEMA.fields
    ]
