"""Unit tests for the detection logic added to ``pipeline/flights.py``.

Three behaviours are new and none of them can be checked by reading the code:
the bearing tie-break, the out-of-area label for ``trend``, and the exact
radius cut. Each is exercised here against a synthetic track small enough that
the right answer is obvious by inspection, on a local Spark session -- no
cluster, no credentials.

The geometry is deliberately the awkward case in every test. A tie-break that
only fires when the aligned aerodrome is also the nearest proves nothing; the
one below has to overrule distance to be right.
"""

import datetime as dt

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from opdi.pipeline.flights import (
    FlightListProcessor,
    OOA,
    angle_between,
    at_border,
    bearing_deg,
)

_EPOCH = dt.datetime(2025, 6, 5, 12, 0, 0)

#: What `_compute_flight_table` reads: one row per (sample, candidate
#: aerodrome), which is the shape `_categorize_landing_take_off` emits.
CLASSIFIED = StructType([
    StructField("icao24", StringType()),
    StructField("flight_id", StringType()),
    StructField("track_id", StringType()),
    StructField("apt_ident", StringType()),
    StructField("apt_latitude_deg", DoubleType()),
    StructField("apt_longitude_deg", DoubleType()),
    StructField("event_time", TimestampType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("status", StringType()),
    StructField("distance_from_center", IntegerType()),
    StructField("first_seen", TimestampType()),
    StructField("last_seen", TimestampType()),
    StructField("DOF", DateType()),
    StructField("take_off_count", IntegerType()),
    StructField("landing_count", IntegerType()),
])

#: A landing track running due west along 50 N, ending at 4.000 E.
#:
#: Two candidates sit near that final fix and the choice between them is the
#: whole point:
#:
#: * ``EBAL`` is due west at 4.5 NM -- straight ahead, alignment 0 degrees;
#: * ``EBNO`` is due north at 4.2 NM -- nearer, alignment 90 degrees.
#:
#: Distance alone picks EBNO. They are 0.3 NM apart, well inside the 2 NM
#: band, so the tie-break should overrule it and pick EBAL.
AERODROMES = {
    "EBAL": (50.0, 3.883),   # due west of the final fix
    "EBNO": (50.070, 4.0),   # due north of it
}


def _landing_track(spark, n=8, step_s=30, start_lon=4.5, end_lon=4.0):
    """A westbound landing, sampled evenly, seen near both aerodromes."""
    rows = []
    first_seen = _EPOCH
    last_seen = _EPOCH + dt.timedelta(seconds=(n - 1) * step_s)
    for i in range(n):
        t = _EPOCH + dt.timedelta(seconds=i * step_s)
        lon = start_lon + (end_lon - start_lon) * i / (n - 1)
        for apt, (alat, alon) in AERODROMES.items():
            rows.append((
                "abc123", "TEST01  ", "trk-1", apt, alat, alon,
                t, 50.0, lon, "landing", 1,
                first_seen, last_seen, _EPOCH.date(), 0, 1,
            ))
    return spark.createDataFrame(rows, schema=CLASSIFIED)


def _first(df, column):
    rows = df.collect()
    assert len(rows) == 1, f"expected one flight, got {len(rows)}"
    return rows[0][column]


# ---------------------------------------------------------------------------
# The bearing helpers
# ---------------------------------------------------------------------------


def test_angle_between_does_not_break_across_north(spark):
    """`%` keeps the sign of the dividend in Spark, so bearings straddling
    north came back as 340 degrees apart instead of 20 -- inverting the metric
    on every pair near the wrap. `pmod` is what makes this correct."""
    df = spark.createDataFrame([(10.0, 350.0), (350.0, 10.0), (0.0, 180.0),
                                (90.0, 270.0)], ["a", "b"])
    got = [r.d for r in df.select(angle_between(F.col("a"), F.col("b")).alias("d")).collect()]
    assert got == pytest.approx([20.0, 20.0, 180.0, 180.0], abs=1e-9)


def test_bearing_deg_reads_the_cardinal_directions(spark):
    df = spark.createDataFrame(
        [(50.0, 4.0, 51.0, 4.0), (50.0, 4.0, 50.0, 5.0), (50.0, 4.0, 49.0, 4.0)],
        ["y1", "x1", "y2", "x2"])
    got = [r.b for r in df.select(
        bearing_deg(F.col("y1"), F.col("x1"), F.col("y2"), F.col("x2")).alias("b")
    ).collect()]
    assert got[0] == pytest.approx(0.0, abs=0.5)     # north
    assert got[1] == pytest.approx(90.0, abs=0.5)    # east
    assert got[2] == pytest.approx(180.0, abs=0.5)   # south


# ---------------------------------------------------------------------------
# The course
# ---------------------------------------------------------------------------


def test_course_points_along_the_track_toward_the_endpoint(spark):
    """For a landing the window sits *before* the final fix, so far-to-near
    reads the direction of travel: due west here, 270 degrees."""
    crs = FlightListProcessor._endpoint_courses(_landing_track(spark)).collect()
    assert len(crs) == 1
    assert crs[0]["course"] == pytest.approx(270.0, abs=1.0)


def test_course_is_null_on_too_few_samples(spark):
    """Under five distinct fixes a bearing is position noise, and a null course
    is what lets the tie-break stand down rather than fire on nonsense."""
    crs = FlightListProcessor._endpoint_courses(_landing_track(spark, n=3)).collect()
    assert crs[0]["course"] is None


def test_course_is_null_on_too_short_a_baseline(spark):
    """Eight fixes one second apart is half a minute of nothing."""
    crs = FlightListProcessor._endpoint_courses(
        _landing_track(spark, n=8, step_s=1)).collect()
    assert crs[0]["course"] is None


def test_the_sample_count_guard_counts_fixes_not_rows(spark):
    """The zone join repeats each fix once per candidate aerodrome. Counting
    rows would pass the five-sample guard on a single fix seen near five
    aerodromes, which is exactly the case with no course at all."""
    # Four fixes, two aerodromes: eight rows, and it must still fail the guard.
    crs = FlightListProcessor._endpoint_courses(_landing_track(spark, n=4)).collect()
    assert crs[0]["course"] is None


# ---------------------------------------------------------------------------
# The tie-break
# ---------------------------------------------------------------------------


def test_distance_alone_picks_the_nearer_misaligned_aerodrome(spark):
    """The control. Without the tie-break, EBNO wins on distance."""
    out = FlightListProcessor._compute_flight_table(
        _landing_track(spark), rank_by="haversine", bearing_tiebreak_nm=0.0)
    assert _first(out, "ADES") == "EBNO"


def test_the_tie_break_overrules_distance_inside_the_band(spark):
    """The result the study measured: same flight answered, answered right."""
    out = FlightListProcessor._compute_flight_table(
        _landing_track(spark), rank_by="haversine", bearing_tiebreak_nm=2.0)
    assert _first(out, "ADES") == "EBAL"


def test_the_tie_break_does_not_fire_outside_the_band(spark):
    """A band of 0.1 NM cannot reach the 0.3 NM gap between the two, so
    distance decides alone and the answer returns to the control's."""
    out = FlightListProcessor._compute_flight_table(
        _landing_track(spark), rank_by="haversine", bearing_tiebreak_nm=0.1)
    assert _first(out, "ADES") == "EBNO"


def test_the_tie_break_stands_down_without_a_course(spark):
    """A null alignment must not win the ordering. With too few fixes to read a
    course, the answer has to be the distance answer -- not an arbitrary one."""
    out = FlightListProcessor._compute_flight_table(
        _landing_track(spark, n=3), rank_by="haversine", bearing_tiebreak_nm=2.0)
    assert _first(out, "ADES") == "EBNO"


def test_the_tie_break_leaves_coverage_alone(spark):
    """It re-ranks; it never abstains. The same flight is answered either way,
    which is why it costs no coverage."""
    args = dict(rank_by="haversine")
    off = FlightListProcessor._compute_flight_table(
        _landing_track(spark), bearing_tiebreak_nm=0.0, **args)
    on = FlightListProcessor._compute_flight_table(
        _landing_track(spark), bearing_tiebreak_nm=2.0, **args)
    assert off.count() == on.count() == 1
    assert _first(off, "id") == _first(on, "id")


# ---------------------------------------------------------------------------
# Out of area
# ---------------------------------------------------------------------------


def test_at_border_scales_the_longitude_margin_with_latitude(spark):
    """A degree of longitude shrinks toward the pole. An unscaled margin would
    be roughly twice as strict in northern Norway as in the Canaries."""
    # Same distance inside the western edge (-25.867), two latitudes.
    df = spark.createDataFrame([(30.0, -25.0), (69.0, -25.0)], ["lat", "lon"])
    got = [r.b for r in df.select(at_border(F.col("lat"), F.col("lon")).alias("b")).collect()]
    # At 69 N a degree of longitude is ~21 NM, so 0.867 degrees is inside the
    # 30 NM margin; at 30 N it is ~52 NM and outside it.
    assert got == [False, True]


def test_at_border_is_false_well_inside_the_area(spark):
    df = spark.createDataFrame([(50.0, 4.0)], ["lat", "lon"])
    assert df.select(at_border(F.col("lat"), F.col("lon")).alias("b")).collect()[0].b is False


def test_version_is_new_unless_the_run_is_a_legacy_one():
    """The stamp is derived from the configuration, so it cannot disagree with
    the algorithm that produced the row."""
    from opdi.config import DetectionConfig, OPDIConfig

    class Stub:
        _version_for = FlightListProcessor._version_for

        def __init__(self, detection, tracks):
            self.detection = detection
            self.tracks_table = tracks

    assert Stub(DetectionConfig(), "osn_tracks_clean")._version_for("trend") == "v5.0.0"
    assert Stub(DetectionConfig(), "osn_tracks_clean")._version_for("endpoint") == "v5.0.0"

    legacy = DetectionConfig.legacy()
    assert Stub(legacy, "osn_tracks")._version_for("trend") == "v2.0.0"
    assert Stub(legacy, "osn_tracks")._version_for("endpoint") == "v3.0.0"

    # Legacy thresholds over *cleaned* tracks is not a legacy run: the input
    # differs, so the output cannot be claimed to reproduce a release.
    assert Stub(legacy, "osn_tracks_clean")._version_for("trend") == "v5.0.0"
    assert OPDIConfig().detection == DetectionConfig()


def test_the_datum_alone_makes_a_run_non_legacy():
    """`_version_for` decides by comparing the whole config against `legacy()`,
    so the datum has to be part of that comparison.

    Everything here *except* the datum is the legacy preset. If the comparison
    ignored `trend_max_datum`, this run would stamp v2.0.0 -- promising a
    reproduction of released data while cutting altitude on a datum released
    data never used.
    """
    import dataclasses  # noqa: PLC0415

    from opdi.config import DetectionConfig  # noqa: PLC0415

    class Stub:
        _version_for = FlightListProcessor._version_for

        def __init__(self, detection, tracks):
            self.detection = detection
            self.tracks_table = tracks

    legacy = DetectionConfig.legacy()
    assert Stub(legacy, "osn_tracks")._version_for("trend") == "v2.0.0"

    on_field = dataclasses.replace(legacy, trend_max_datum="field")
    assert Stub(on_field, "osn_tracks")._version_for("trend") == "v5.0.0"


def test_ooa_marker_is_what_the_endpoint_path_already_uses():
    """One marker, defined once. Two spellings of "outside the area" in one
    flight list would be indistinguishable from a bug to any consumer."""
    assert OOA == "OOA"


def test_a_null_coordinate_gives_a_null_distance(spark):
    """The clamp must not invent a position.

    `F.least(a, 1.0)` skips NULLs, so a missing coordinate became
    `asin(1)` = 10,807 NM -- the antipodal distance -- instead of NULL. Benign
    in aerodrome ranking, where a point 10,807 NM away never wins, and
    catastrophic in the ring detector: a parked aircraft whose repeated
    positions the cleaner had nulled oscillated between 0.7 NM and 10,807 NM
    and crossed both rings twenty thousand times.
    """
    from opdi.pipeline.flights import haversine_nm

    df = spark.createDataFrame(
        [(50.0, 4.0, 50.9, 4.5), (None, None, 50.9, 4.5), (50.0, 4.0, None, None)],
        "lat1 double, lon1 double, lat2 double, lon2 double",
    ).withColumn("d", haversine_nm(F.col("lat1"), F.col("lon1"), F.col("lat2"), F.col("lon2")))
    got = [r.d for r in df.collect()]

    assert got[0] == pytest.approx(56, abs=3)   # a real distance
    assert got[1] is None
    assert got[2] is None


def test_the_distance_is_still_clamped_for_antipodal_points(spark):
    """The clamp exists because floating point can push the argument of asin
    just past 1; removing it would raise instead of returning half a
    circumference."""
    from opdi.pipeline.flights import haversine_nm

    df = spark.createDataFrame(
        [(0.0, 0.0, 0.0, 180.0)], "lat1 double, lon1 double, lat2 double, lon2 double"
    ).withColumn("d", haversine_nm(F.col("lat1"), F.col("lon1"), F.col("lat2"), F.col("lon2")))

    assert df.collect()[0].d == pytest.approx(10807, abs=5)


# ---------------------------------------------------------------------------
# The altitude cut, on both datums.
#
# `_fetch_and_label_sv` reads from storage and cannot be exercised directly, so
# the cut is a module-level expression and is tested as one -- the same shape
# as `angle_between`, `bearing_deg` and `at_border` above.
# ---------------------------------------------------------------------------

#: Shaped like the trend path just after its left join to the zone table:
#: altitude in metres (SI storage), elevation in feet, and both nullable.
_CUT_SCHEMA = StructType([
    StructField("baro_altitude", DoubleType()),
    StructField("apt_elevation_ft", DoubleType()),
    StructField("flight_level", IntegerType()),
])


def _cut_row(alt_ft, elev_ft):
    """A row at *alt_ft* pressure altitude near a field at *elev_ft*.

    `flight_level` is derived exactly as `_fetch_and_label_sv` derives it,
    including the integer cast -- which is the whole point of several of the
    tests below.
    """
    from opdi.pipeline.flights import FT_PER_M

    return (alt_ft / FT_PER_M, elev_ft, int(alt_ft / 100))


def _survives(spark, rows, detection):
    """Which rows survive the cut, in input order."""
    from opdi.pipeline.flights import trend_altitude_cut

    df = spark.createDataFrame(rows, schema=_CUT_SCHEMA)
    df = df.withColumn("_i", F.monotonically_increasing_id())
    keep = {r["_i"] for r in df.filter(trend_altitude_cut(detection)).select("_i").collect()}
    return [r["_i"] in keep for r in df.select("_i").collect()]


def _processor_with_airports(spark, rows):
    """A FlightListProcessor whose only storage table is `oa_airports`.

    The real StorageManager needs S3 and Kerberos; the two helpers under test
    read exactly one table, so a stub is honest here rather than a shortcut.
    """
    from opdi.config import DetectionConfig

    class _Storage:
        def table_exists(self, name):
            return name == "oa_airports"

        def read_table(self, name):
            assert name == "oa_airports"
            return spark.createDataFrame(rows, "ident string, elevation_ft double")

    proc = FlightListProcessor.__new__(FlightListProcessor)
    proc.storage = _Storage()
    proc.spark = spark
    proc.detection = DetectionConfig()
    proc._max_elev_ft = None
    return proc


def test_attach_field_elevation_keeps_unmatched_rows(spark):
    """The helper is shared with the endpoint path, and the trend path feeds it
    left-joined rows whose apt_ident is NULL. Those must come back with a NULL
    elevation, not be dropped -- an inner join here would silently delete every
    sample that matched no aerodrome, and with it every track that has none.
    """
    proc = _processor_with_airports(spark, [("EHAM", -11.0), ("LTCE", 5763.0)])
    cand = spark.createDataFrame(
        [("EHAM",), ("LTCE",), (None,), ("ZZZZ",)], "apt_ident string"
    )
    got = {r["apt_ident"]: r["apt_elevation_ft"] for r in proc._attach_field_elevation(cand).collect()}

    assert got["EHAM"] == pytest.approx(-11.0)
    assert got["LTCE"] == pytest.approx(5763.0)
    assert got[None] is None          # matched no aerodrome
    assert got["ZZZZ"] is None        # aerodrome absent from the reference
    assert len(got) == 4              # nothing dropped


def test_max_field_elevation_comes_from_the_zone_table_not_the_world(spark):
    """The pre-filter's width is set by the highest aerodrome that can actually
    match. Taking the max over all of OurAirports would drag in fields above
    14,000 ft that the bounding box excludes, widening the pre-filter by two
    thirds for aerodromes no sample can ever be joined to.
    """
    proc = _processor_with_airports(
        spark, [("EHAM", -11.0), ("LTCE", 5763.0), ("ZUDC", 14472.0)]
    )
    zones = spark.createDataFrame([("EHAM",), ("LTCE",)], "apt_ident string")
    assert proc._max_field_elevation_ft(zones) == pytest.approx(5763.0)


def test_max_field_elevation_is_zero_without_a_reference(spark):
    """No reference means no elevation to subtract, so the field datum must
    degrade to the sea-level cut rather than widening the pre-filter by a NULL.
    """
    proc = _processor_with_airports(spark, [("EHAM", -11.0)])
    proc.storage.table_exists = lambda name: False
    zones = spark.createDataFrame([("EHAM",)], "apt_ident string")
    assert proc._max_field_elevation_ft(zones) == 0.0


def test_height_above_field_subtracts_the_elevation(spark):
    from opdi.pipeline.flights import height_above_field

    df = spark.createDataFrame([(1828.8, 1000.0)], "alt double, elev double")
    got = df.select(height_above_field(F.col("alt"), F.col("elev")).alias("h")).first()["h"]
    # 1828.8 m is 6,000 ft; above a 1,000 ft field that is 5,000 ft.
    assert got == pytest.approx(5000.0, abs=0.5)


def test_a_null_elevation_falls_back_to_the_sea_level_height(spark):
    """Unknown elevation must degrade to the published behaviour, not vanish.

    Dropping the aerodrome instead would remove detection from exactly the
    fields the reference data is weakest about.
    """
    from opdi.pipeline.flights import height_above_field

    df = spark.createDataFrame([(1828.8, None)], "alt double, elev double")
    got = df.select(height_above_field(F.col("alt"), F.col("elev")).alias("h")).first()["h"]
    assert got == pytest.approx(6000.0, abs=0.5)


def test_the_field_datum_admits_a_high_aerodrome_that_msl_excludes(spark):
    """The whole point of the change, as one assertion.

    A flight 3,000 ft above Erzurum (field 5,763 ft) sits at 8,763 ft pressure
    altitude. On the sea-level datum it is far above FL60 and never votes; on
    the field datum it is well inside a 6,000 ft ceiling.
    """
    from opdi.config import DetectionConfig

    rows = [_cut_row(8763.0, 5763.0)]
    assert _survives(spark, rows, DetectionConfig(trend_max_datum="msl")) == [False]
    assert _survives(spark, rows, DetectionConfig(trend_max_datum="field")) == [True]


def test_the_field_datum_changes_nothing_at_a_sea_level_aerodrome(spark):
    """The control. At elevation 0 the two datums are the same test, so a
    difference here would mean the change is doing something else as well.
    """
    from opdi.config import DetectionConfig

    rows = [_cut_row(3000.0, 0.0), _cut_row(5900.0, 0.0), _cut_row(9000.0, 0.0)]
    assert _survives(spark, rows, DetectionConfig(trend_max_datum="msl")) == [True, True, False]
    assert _survives(spark, rows, DetectionConfig(trend_max_datum="field")) == [True, True, False]


def test_the_msl_branch_keeps_the_integer_flight_level_cut(spark):
    """FL60 admits everything below 6,100 ft, because `flight_level` is an int
    cast. Rewriting it as `alt_ft <= 6000` would move the published cut by up
    to 99 ft and nothing downstream would notice.
    """
    from opdi.config import DetectionConfig

    rows = [_cut_row(6050.0, 0.0)]
    assert _survives(spark, rows, DetectionConfig(trend_max_datum="msl")) == [True]
    # The field datum, being an honest height, cuts at 6,000 exactly.
    assert _survives(spark, rows, DetectionConfig(trend_max_datum="field")) == [False]


def test_a_sample_matching_no_aerodrome_is_cut_on_sea_level(spark):
    """The trend path left-joins the zone table, and unmatched rows must
    survive to keep otherwise-unnamed tracks in the flight list. A NULL
    elevation is what those rows carry, so they must behave exactly as before.
    """
    from opdi.config import DetectionConfig

    rows = [_cut_row(3000.0, None), _cut_row(9000.0, None)]
    assert _survives(spark, rows, DetectionConfig(trend_max_datum="field")) == [True, False]


def test_the_prefilter_ceiling_covers_the_highest_field():
    """The pre-filter is a performance guard and must drop nothing the exact
    cut would have kept. Its ceiling is therefore the union bound over every
    aerodrome: the cap plus the highest field elevation in the reference set.
    """
    from opdi.config import DetectionConfig
    from opdi.pipeline.flights import trend_prefilter_ceiling_ft

    d = DetectionConfig(trend_max_datum="field", trend_max_height_ft=6000.0)
    assert trend_prefilter_ceiling_ft(d, 5763.0) == pytest.approx(11763.0)


def test_the_prefilter_is_the_plain_cut_on_the_msl_datum():
    """With no elevation term there is nothing to widen for."""
    from opdi.config import DetectionConfig
    from opdi.pipeline.flights import trend_prefilter_ceiling_ft

    d = DetectionConfig(trend_max_datum="msl", trend_max_fl=60)
    assert trend_prefilter_ceiling_ft(d, 5763.0) == pytest.approx(6100.0)


def test_the_prefilter_never_cuts_below_the_exact_cut(spark):
    """The property that makes the pre-filter safe, asserted rather than
    argued: across a sweep of altitudes and elevations, every row the exact
    cut keeps is a row the pre-filter also keeps.
    """
    from opdi.config import DetectionConfig
    from opdi.pipeline.flights import FT_PER_M, trend_prefilter_ceiling_ft

    d = DetectionConfig(trend_max_datum="field", trend_max_height_ft=6000.0)
    max_elev = 9000.0
    ceiling = trend_prefilter_ceiling_ft(d, max_elev)

    rows = [
        _cut_row(alt, elev)
        for alt in (500.0, 3000.0, 6000.0, 9000.0, 12000.0, 14000.0, 16000.0)
        for elev in (0.0, 2000.0, 5763.0, max_elev)
    ]
    exact = _survives(spark, rows, d)
    prefiltered = [(r[0] * FT_PER_M) <= ceiling for r in rows]
    for kept_exact, kept_pre in zip(exact, prefiltered):
        assert not (kept_exact and not kept_pre)
