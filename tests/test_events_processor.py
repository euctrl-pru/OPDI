"""Tests for the step-04 processor's wiring.

The detector functions are tested elsewhere. These cover the plumbing that
decides *which* detector runs, what it stamps, and where it reads from -- the
part that turns a configuration into published data, and the part that was
carrying inline literals until now.
"""

import datetime as dt
import json
import tempfile

import pytest
from pyspark.sql import functions as F

from conftest import make_track
# The great-circle offset helper, imported rather than copied: the arrival
# geometry below has to be the same geometry ``test_runway_ops`` asserts the
# threshold-plane timing against, or the two would be testing different
# runways under the same name.
from test_runway_ops import _dest

from opdi.config import EventConfig, OPDIConfig
from opdi.pipeline.events import FlightEventProcessor


@pytest.fixture
def processor(spark, tmp_path):
    def build(events: EventConfig) -> FlightEventProcessor:
        config = OPDIConfig()
        config.events = events
        return FlightEventProcessor(spark, config, log_dir=str(tmp_path / "logs"))

    return build


def test_the_processor_reads_its_thresholds_from_the_config(processor):
    """Until this existed, every number in step 04 was an inline literal."""
    events = EventConfig()
    assert processor(events).events is events


def test_a_config_without_events_still_builds(processor, spark, tmp_path):
    """An OPDIConfig from an older pickle or a hand-built stub must not crash
    the step; it falls back to the shipped defaults."""
    config = OPDIConfig()
    del config.events

    proc = FlightEventProcessor(spark, config, log_dir=str(tmp_path / "l2"))

    assert isinstance(proc.events, EventConfig)


def test_the_version_stamped_is_the_configured_one(processor):
    assert processor(EventConfig()).events.events_version == "events_v0.2.0"
    assert processor(EventConfig.legacy()).events.events_version == "events_v0.0.2"


def _ids(spark, proc, rows):
    df = spark.createDataFrame(rows, "track_id string, type string, version string").withColumn(
        "event_time", F.to_timestamp(F.lit("2024-06-01 12:00:00"))
    )
    return [r.id for r in df.withColumn("id", proc._event_id("batch_")).collect()]


def test_event_ids_are_reproducible_across_runs(processor, spark):
    """``monotonically_increasing_id`` encodes the partition index, so the same
    event gets a different id every run and two runs of one month cannot be
    reconciled -- which matters because the write path appends."""
    proc = processor(EventConfig())
    rows = [("trk-1", "take-off", "events_v0.2.0")]

    assert _ids(spark, proc, rows) == _ids(spark, proc, rows)


def test_distinct_events_get_distinct_ids(processor, spark):
    proc = processor(EventConfig())
    ids = _ids(
        spark,
        proc,
        [
            ("trk-1", "take-off", "events_v0.2.0"),
            ("trk-1", "landing", "events_v0.2.0"),
            ("trk-2", "take-off", "events_v0.2.0"),
        ],
    )

    assert len(set(ids)) == 3


def test_the_version_participates_in_the_identity(processor, spark):
    """Two algorithm versions may both describe the same milestone of the same
    track. They must not collide."""
    proc = processor(EventConfig())
    ids = _ids(
        spark,
        proc,
        [("trk-1", "take-off", "events_v0.0.2"), ("trk-1", "take-off", "events_v0.2.0")],
    )

    assert ids[0] != ids[1]


def test_legacy_keeps_the_unreproducible_ids(processor, spark):
    """Not an improvement worth back-porting: legacy must reproduce what was
    published, warts included."""
    proc = processor(EventConfig.legacy())
    rows = [("trk-1", "take-off", "events_v0.0.2")]

    got = _ids(spark, proc, rows)

    assert got[0].startswith("batch_")


def test_measurement_ids_hang_off_their_milestone(processor, spark):
    proc = processor(EventConfig())
    df = (
        spark.createDataFrame([("abc",)], "id_tmp string")
        .withColumn("d", proc._measurement_id("batch_", "_d_"))
        .withColumn("t", proc._measurement_id("batch_", "_t_"))
    )
    row = df.collect()[0]

    assert row.d == "abc_d"
    assert row.t == "abc_t"
    assert row.d != row.t


# ===========================================================================
# The v0.2.0 vocabulary, end to end through the processor
#
# One synthetic flight, EBBR -> EDDF, driven through
# ``_etl_flight_events_and_measures`` twice: once under ``EventConfig()`` and
# once under ``EventConfig.legacy()``. These are the only tests in the suite
# that exercise the *whole* step, and they exist because the thing being tested
# is precisely the wiring -- which detector runs, under which flag, stamping
# which type string. A unit test of any single family cannot see a double
# emission or a retired type surviving.
# ===========================================================================

MONTH = dt.date(2024, 6, 1)
_T0 = dt.datetime(2024, 6, 1, 12, 0, 0)

#: Types the A-CDM vocabulary retires. Under ``EventConfig()`` not one of them
#: may appear: each is either superseded by a better-timed detector
#: (``ATOT``/``ALDT`` by the interpolated ``airborne``/``touchdown``,
#: ``take-off``/``landing`` likewise), renamed (``AOBT``/``AIBT`` ->
#: ``off-block``/``on-block``), or split into the four runway-occupancy types
#: (``entry-runway``/``exit-runway``).
V4_RETIRED = {"take-off", "ATOT", "ALDT", "AOBT", "AIBT",
              "entry-runway", "exit-runway"}

_RWY_BEARING = 70.0
_EBBR = (50.0, 4.0)   # runway 07 threshold, and the aerodrome reference point
_EDDF = (51.0, 6.0)

_TRACK_SCHEMA = (
    "track_id string, lat double, lon double, event_time timestamp, "
    "baro_altitude_c double, velocity double, vert_rate double, "
    "callsign string, icao24 string, heading double, h3_res_12 string"
)

#: (seconds, along-track NM from the 07 threshold, altitude ft, ROC ft/min,
#: groundspeed kt). The departure holds, rolls and rotates over the EBBR
#: runway polygon; the last sample is above the 15 ft airborne threshold, which
#: is what makes the traversal a *departure* rather than a crossing.
_DEPARTURE = [
    (0, -0.2, 0.0, 0.0, 5.0),
    (10, -0.2, 0.0, 0.0, 5.0),
    (20, -0.2, 0.0, 0.0, 5.0),
    (30, -0.2, 0.0, 0.0, 5.0),
    (40, -0.2, 0.0, 0.0, 5.0),
    (50, -0.1, 0.0, 0.0, 60.0),
    (60, 0.1, 0.0, 0.0, 100.0),
    # Rotation: still on the deck, already showing a rate of climb. That is
    # what makes the sample CL rather than LVL, and it has to be, because the
    # legacy ``take-off`` arm fires on a GND -> CL adjacency and sits *after*
    # the level-segment arms in the same ``when`` chain -- an LVL sample here
    # would be published as ``level-start`` instead.
    (70, 0.4, 0.0, 500.0, 140.0),
    (80, 0.9, 500.0, 2000.0, 160.0),
]

#: The same arrival profile ``test_runway_ops`` uses, at EDDF: it straddles the
#: threshold plane between -0.2 and +0.1 NM and the 15 ft height between 20 and
#: 5 ft, so ``landing`` (T16) and ``touchdown`` (T17) are both well defined.
_ARRIVAL = [
    (-1.5, 500.0, 140.0),
    (-1.0, 340.0, 138.0),
    (-0.5, 180.0, 136.0),
    (-0.2, 60.0, 134.0),
    (0.1, 20.0, 130.0),
    (0.4, 5.0, 110.0),
    (0.7, 0.0, 80.0),
    (0.9, 0.0, 50.0),
    (1.1, 0.0, 25.0),
]


def _along(origin, along_nm):
    """A point ``along_nm`` up runway 07's centreline from its threshold."""
    bearing = _RWY_BEARING if along_nm >= 0 else _RWY_BEARING + 180.0
    return _dest(origin[0], origin[1], bearing, abs(along_nm))


def _flight(spark):
    """One EBBR -> EDDF flight: roll, climb, cruise, descent, landing.

    Every phase is stated in aviation units and converted here, because that is
    how the thresholds are written and a profile a reader cannot check against
    them is not a fixture, it is a fixed point.
    """
    rows = []

    def add(t, lat, lon, alt_ft, roc_ftmin, gs_kt, h3):
        rows.append((
            "trk-1", lat, lon, _T0 + dt.timedelta(seconds=t),
            alt_ft / 3.28084, gs_kt / 1.94384, roc_ftmin / 196.850394,
            "TEST123", "abc123", _RWY_BEARING, h3,
        ))

    for t, along, alt_ft, roc, kt in _DEPARTURE:
        lat, lon = _along(_EBBR, along)
        add(t, lat, lon, alt_ft, roc, kt, "cell-ebbr-rwy")

    # En route: nine climb samples, twenty-three at cruise, ten descending.
    # Cruise is under ``level_exclusion_box_seconds`` so the PRU tops are not
    # relocated -- the relocation has its own tests in test_vertical_pru.
    enroute = (
        [(alt, 2000.0, 300.0) for alt in range(2000, 20000, 2000)]
        + [(35000, 0.0, 600.0)] * 23
        + [(alt, -2000.0, 300.0) for alt in
           (33000, 29000, 25000, 21000, 17000, 13000, 9000, 7000, 6000, 5000)]
    )
    start, end = _along(_EBBR, 0.9), _along(_EDDF, -1.5)
    for i, (alt_ft, roc, kt) in enumerate(enroute):
        f = (i + 1) / (len(enroute) + 1)
        lat = start[0] + (end[0] - start[0]) * f
        lon = start[1] + (end[1] - start[1]) * f
        add(90 + 10 * i, lat, lon, float(alt_ft), roc, kt, "cell-air")

    for i, (along, alt_ft, kt) in enumerate(_ARRIVAL):
        lat, lon = _along(_EDDF, along)
        add(510 + 10 * i, lat, lon, alt_ft, -590.0 if alt_ft > 0 else 0.0, kt,
            "cell-eddf-rwy")

    return spark.createDataFrame(rows, _TRACK_SCHEMA)


class _StubStorage:
    """Just enough ``StorageManager`` for the step, plus the written frames.

    Modelled on ``test_events_labelling._StubStorage`` -- same three read
    methods -- with ``write_table`` capturing instead of writing, because the
    step's output is what these tests are about.
    """

    def __init__(self, tables, tracks):
        self._tables = tables
        #: The state vectors the step is driven over. They live here because
        #: ``_run`` takes the storage and nothing else, and the tracks and the
        #: reference tables have to describe the same flight.
        self.tracks = tracks
        self.written = {}

    def table_exists(self, name):
        return name in self._tables

    def read_table(self, name):
        return self._tables[name]

    def write_table(self, df, name, mode="append"):
        self.written[name] = df


@pytest.fixture(scope="session")
def stub_storage(spark):
    """The four reference tables the step touches, for one flight.

    Session-scoped, because ``_run`` memoises its results against it: a full
    pass through the step is the most expensive thing in the suite and the two
    configurations are each run exactly once.
    """
    flight_list = spark.createDataFrame(
        [("trk-1", dt.datetime(2024, 6, 1, 12, 0), "EBBR", "EDDF", None, None)],
        "id string, dof timestamp, adep string, ades string, "
        "adep_p string, ades_p string",
    )
    layouts = spark.createDataFrame(
        [("cell-ebbr-rwy", "EBBR", "osm-ebbr", "runway", "07/25"),
         ("cell-eddf-rwy", "EDDF", "osm-eddf", "runway", "07/25")],
        "hexaero_h3_id string, hexaero_apt_icao string, hexaero_osm_id string, "
        "hexaero_aeroway string, hexaero_ref string",
    )
    airports = spark.createDataFrame(
        [("EBBR", _EBBR[0], _EBBR[1], 0.0), ("EDDF", _EDDF[0], _EDDF[1], 0.0)],
        "ident string, latitude_deg double, longitude_deg double, "
        "elevation_ft double",
    )
    runways = []
    for ident, thr in (("EBBR", _EBBR), ("EDDF", _EDDF)):
        far = _along(thr, 2.0)
        runways.append((ident, "07", thr[0], thr[1], "25", far[0], far[1], False))
    runways = spark.createDataFrame(
        runways,
        "airport_ident string, le_ident string, le_latitude_deg double, "
        "le_longitude_deg double, he_ident string, he_latitude_deg double, "
        "he_longitude_deg double, closed boolean",
    )
    return _StubStorage(
        {
            "opdi_flight_list": flight_list,
            "hexaero_airport_layouts": layouts,
            "oa_airports": airports,
            "oa_runways": runways,
        },
        tracks=_flight(spark),
    )


_RUN_CACHE = {}


def _run(spark, stub_storage, events: EventConfig):
    """Drive the whole step over the fixture flight and return its milestones.

    Memoised on the version string: both fixtures are session-scoped, so one
    pass per configuration serves every assertion below.
    """
    key = events.events_version
    if key not in _RUN_CACHE:
        config = OPDIConfig()
        config.events = events
        with tempfile.TemporaryDirectory() as log_dir:
            proc = FlightEventProcessor(spark, config, log_dir=log_dir)
            proc.storage = stub_storage
            proc._etl_flight_events_and_measures(
                stub_storage.tracks, batch_id="b_", month=MONTH
            )
        # Materialised into a plain frame of its own rows, and the session
        # cache dropped afterwards. A whole pass through step 04 leaves a query
        # plan and a dozen cached frames behind it; keeping either alive across
        # the rest of the suite exhausts the local driver heap, and the tests
        # that then fail are ones this change never touched. Production drops
        # them at the end of ``process_month`` for the same reason.
        written = stub_storage.written["opdi_flight_events"]
        out = spark.createDataFrame(written.collect(), written.schema)
        spark.catalog.clearCache()
        _RUN_CACHE[key] = out
    return _RUN_CACHE[key]


def _types(out):
    return {r["type"] for r in out.collect()}


def test_v4_emits_no_retired_type(spark, stub_storage):
    """The retirement, stated as the table a consumer would query."""
    assert _types(_run(spark, stub_storage, EventConfig())) & V4_RETIRED == set()


def test_v4_emits_the_a_cdm_runway_family(spark, stub_storage):
    """The other half of the trade: the retired types are gone *because* these
    arrived. Without this the previous test passes on a step that emits
    nothing at all."""
    got = _types(_run(spark, stub_storage, EventConfig()))
    assert {"line-up", "take-off-roll", "airborne", "landing", "touchdown",
            "runway-vacated"} <= got


def test_legacy_still_emits_exactly_what_it_always_did(spark, stub_storage):
    out = _run(spark, stub_storage, EventConfig.legacy())
    assert "take-off" in _types(out)
    assert out.select("version").distinct().collect()[0][0] == "events_v0.0.2"


def test_legacy_keeps_the_undifferentiated_runway_pair(spark, stub_storage):
    """``entry-runway`` is retired by the flag, not by the version string."""
    got = _types(_run(spark, stub_storage, EventConfig.legacy()))
    assert {"entry-runway", "exit-runway"} <= got
    assert got & {"line-up", "airborne", "touchdown"} == set()


def test_landing_carries_the_new_meaning_only_under_the_new_version(spark, stub_storage):
    """Same string, two definitions, distinguished by ``version`` -- which is
    what the version column is for, and which the breaking-changes chapter
    leads with. Under v0.2.0 ``landing`` is ICAO T16, the threshold plane;
    under v0.0.2 it is the phase change at ground contact and carries no
    milestone number at all."""
    v4 = _run(spark, stub_storage, EventConfig()).filter(F.col("type") == "landing")
    assert v4.count() > 0
    info = json.loads(v4.collect()[0]["info"])
    assert info["milestone"] == "T16"

    legacy = _run(spark, stub_storage, EventConfig.legacy()).filter(
        F.col("type") == "landing"
    )
    assert legacy.count() > 0
    assert legacy.collect()[0]["info"] == ""


def test_every_top_of_climb_records_which_algorithm_produced_it(spark, stub_storage):
    out = _run(spark, stub_storage, EventConfig()).filter(
        F.col("type").startswith("top-of-climb"))
    methods = {json.loads(r["info"])["method"] for r in out.collect()}
    assert methods == {"phase", "pru"}


def test_every_top_of_descent_records_it_too(spark, stub_storage):
    out = _run(spark, stub_storage, EventConfig()).filter(
        F.col("type").startswith("top-of-descent"))
    methods = {json.loads(r["info"])["method"] for r in out.collect()}
    assert methods == {"phase", "pru"}


def test_the_block_events_are_one_detector_under_two_names(spark):
    """A pure rename keyed on the same flag as the rest of the vocabulary --
    so the V4 ladder's baseline rung keeps the names it published."""
    from opdi.pipeline.events import calculate_block_events

    sv = spark.createDataFrame(
        [("trk-1", _T0 + dt.timedelta(seconds=i), 20.0) for i in range(0, 120, 10)],
        "track_id string, event_time timestamp, velocity double",
    )
    stand = spark.createDataFrame(
        [("trk-1", "exit-parking_position", _T0),
         ("trk-1", "entry-parking_position", _T0 + dt.timedelta(seconds=200))],
        "track_id string, type string, event_time timestamp",
    )

    v4 = {r["type"] for r in calculate_block_events(sv, stand, EventConfig()).collect()}
    legacy = {r["type"] for r in
              calculate_block_events(sv, stand, EventConfig.legacy()).collect()}

    assert v4 == {"off-block", "on-block"}
    assert legacy == {"AOBT", "AIBT"}
