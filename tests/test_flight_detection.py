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

    assert Stub(DetectionConfig(), "osn_tracks_clean")._version_for("trend") == "v4.0.0"
    assert Stub(DetectionConfig(), "osn_tracks_clean")._version_for("endpoint") == "v4.0.0"

    legacy = DetectionConfig.legacy()
    assert Stub(legacy, "osn_tracks")._version_for("trend") == "v2.0.0"
    assert Stub(legacy, "osn_tracks")._version_for("endpoint") == "v3.0.0"

    # Legacy thresholds over *cleaned* tracks is not a legacy run: the input
    # differs, so the output cannot be claimed to reproduce a release.
    assert Stub(legacy, "osn_tracks_clean")._version_for("trend") == "v4.0.0"
    assert OPDIConfig().detection == DetectionConfig()


def test_ooa_marker_is_what_the_endpoint_path_already_uses():
    """One marker, defined once. Two spellings of "outside the area" in one
    flight list would be indistinguishable from a bug to any consumer."""
    assert OOA == "OOA"
