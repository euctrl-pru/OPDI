"""Tests for the step-04 processor's wiring.

The detector functions are tested elsewhere. These cover the plumbing that
decides *which* detector runs, what it stamps, and where it reads from -- the
part that turns a configuration into published data, and the part that was
carrying inline literals until now.
"""

import datetime as dt
import json
import tempfile

# The ladder's own reconstruction of the v0.1.0 configuration, imported rather
# than restated: a test that hand-builds "v0.1.0" proves only that the hand-built
# thing behaves, not that the rung the paper reports does.
import event_bench
import pytest
from pyspark.sql import functions as F

# The great-circle offset helper, imported rather than copied: the arrival
# geometry below has to be the same geometry ``test_runway_ops`` asserts the
# threshold-plane timing against, or the two would be testing different
# runways under the same name.
from test_runway_ops import _dest

from opdi.config import EventConfig, OPDIConfig
from opdi.pipeline.events import FlightEventProcessor
from opdi.pipeline.runway_ops import RUNWAY_THRESHOLD_CROSSING


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
    assert processor(EventConfig()).events.events_version == "events_v0.3.0"
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
#: may appear: each is either superseded by a better detector
#: (``take-off``/``landing`` (ground contact) by the interpolated
#: ``airborne``/``touchdown``, which the benchmark confirmed beat them),
#: renamed (``AOBT``/``AIBT`` -> ``off-block``/``on-block``), or split into the
#: runway-occupancy types (``entry-runway``/``exit-runway``).
#:
#: ``ATOT``/``ALDT`` are NOT here: the flight-events-v4 benchmark found the
#: A-CDM ``airborne``/``touchdown`` coverage-limited (they anchor on ~0-15 ft
#: on-runway samples, seen densely only near Switzerland: ~4%/7% network vs
#: ATOT/ALDT ~92%/~100%), so ATOT/ALDT are RETAINED under v0.2.0 for coverage,
#: published alongside the A-CDM family for its accuracy where reception allows.
#: Types no configuration should still publish once the merge is on. ``AOBT``
#: and ``AIBT`` are deliberately absent: v0.2.0 retired them in favour of
#: ``off-block``/``on-block``, and v0.3.0 brings the names back because one
#: detector should have one name. The A-CDM strings are what is retired now.
V4_RETIRED = {
    "take-off",
    "airborne",
    "touchdown",
    "off-block",
    "on-block",
    "entry-runway",
    "exit-runway",
    "top-of-climb-cco",
    "top-of-descent-cdo",
}

_RWY_BEARING = 70.0
_EBBR = (50.0, 4.0)  # runway 07 threshold, and the aerodrome reference point
_EDDF = (51.0, 6.0)

_TRACK_SCHEMA = (
    "track_id string, lat double, lon double, event_time timestamp, "
    "baro_altitude_c double, velocity double, vert_rate double, "
    "callsign string, icao24 string, heading double, h3_res_12 string"
)

#: (seconds, along-track NM from the 07 threshold, altitude ft, ROC ft/min,
#: groundspeed kt, H3 cell). The aircraft starts on a stand, holds on the
#: runway, rolls and rotates; the last samples are above the 15 ft airborne
#: threshold, which is what makes the traversal a *departure* rather than a
#: crossing. The two 900/1,300 ft samples are there so the *published*
#: ``ATOT`` detector has its four surviving samples -- it wants climbing
#: samples under 1,500 ft above the field and within 5 NM -- because the
#: v0.1.0 rung has to be reconstructible through this same fixture.
_DEPARTURE = [
    (0, -0.2, 0.0, 0.0, 5.0, "cell-ebbr-stand"),
    (10, -0.2, 0.0, 0.0, 5.0, "cell-ebbr-stand"),
    (20, -0.2, 0.0, 0.0, 5.0, "cell-ebbr-stand"),
    (30, -0.2, 0.0, 0.0, 5.0, "cell-ebbr-rwy"),
    (40, -0.2, 0.0, 0.0, 5.0, "cell-ebbr-rwy"),
    (50, -0.1, 0.0, 0.0, 60.0, "cell-ebbr-rwy"),
    (60, 0.1, 0.0, 0.0, 100.0, "cell-ebbr-rwy"),
    # Rotation: still on the deck, already showing a rate of climb. That is
    # what makes the sample CL rather than LVL, and it has to be, because the
    # legacy ``take-off`` arm fires on a GND -> CL adjacency and sits *after*
    # the level-segment arms in the same ``when`` chain -- an LVL sample here
    # would be published as ``level-start`` instead.
    (70, 0.4, 0.0, 500.0, 140.0, "cell-ebbr-rwy"),
    (80, 0.9, 500.0, 2000.0, 160.0, "cell-ebbr-rwy"),
    (90, 1.5, 900.0, 2000.0, 180.0, "cell-air"),
    (100, 2.5, 1300.0, 2000.0, 200.0, "cell-air"),
]

#: The same arrival profile ``test_runway_ops`` uses, at EDDF: it straddles the
#: threshold plane between -0.2 and +0.1 NM and the 15 ft height between 30 and
#: 5 ft, so ``xing-runway-threshold`` (T16) and ``touchdown`` (T17) are both well defined.
_ARRIVAL = [
    (-1.5, 500.0, 140.0),
    (-1.0, 340.0, 138.0),
    (-0.5, 180.0, 136.0),
    (-0.2, 60.0, 134.0),
    (0.1, 30.0, 130.0),
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
        rows.append(
            (
                "trk-1",
                lat,
                lon,
                _T0 + dt.timedelta(seconds=t),
                alt_ft / 3.28084,
                gs_kt / 1.94384,
                roc_ftmin / 196.850394,
                "TEST123",
                "abc123",
                _RWY_BEARING,
                h3,
            )
        )

    for t, along, alt_ft, roc, kt, cell in _DEPARTURE:
        lat, lon = _along(_EBBR, along)
        add(t, lat, lon, alt_ft, roc, kt, cell)

    # En route: nine climb samples, twenty-three at cruise, ten descending.
    # Cruise is under ``level_exclusion_box_seconds`` so the PRU tops are not
    # relocated -- the relocation has its own tests in test_vertical_pru.
    enroute = (
        [(alt, 2000.0, 300.0) for alt in range(2000, 20000, 2000)]
        + [(35000, 0.0, 600.0)] * 21
        + [
            (alt, -2000.0, 300.0)
            for alt in (33000, 29000, 25000, 21000, 17000, 13000, 9000, 7000, 6000, 5000)
        ]
    )
    start, end = _along(_EBBR, 2.5), _along(_EDDF, -1.5)
    for i, (alt_ft, roc, kt) in enumerate(enroute):
        f = (i + 1) / (len(enroute) + 1)
        lat = start[0] + (end[0] - start[0]) * f
        lon = start[1] + (end[1] - start[1]) * f
        add(110 + 10 * i, lat, lon, float(alt_ft), roc, kt, "cell-air")

    for i, (along, alt_ft, kt) in enumerate(_ARRIVAL):
        lat, lon = _along(_EDDF, along)
        # The last two samples are on a stand: without an
        # ``entry-parking_position`` to anchor on there is no on-block, and the
        # block pair is half of what the flag renames.
        #
        # Everything short of the threshold is airborne, **not** a runway cell.
        # A runway polygon begins at the threshold, so the sample the T16
        # crossing interpolates from is outside the traversal by construction
        # and reaches the detector only through ``ARRIVAL_LEAD_SECONDS``. An
        # earlier version of this fixture painted the whole final approach as
        # runway cells, and ``xing-runway-threshold`` passed here on a traversal shape
        # ``runway_traversals`` cannot produce.
        if i >= len(_ARRIVAL) - 2:
            cell = "cell-eddf-stand"
        else:
            cell = "cell-eddf-rwy" if along >= 0 else "cell-air"
        add(510 + 10 * i, lat, lon, alt_ft, -590.0 if alt_ft > 0 else 0.0, kt, cell)

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
        #: ``(table, mode, partition_values)`` of every write, in order.
        self.calls = []

    def table_exists(self, name):
        return name in self._tables

    def read_table(self, name):
        return self._tables[name]

    def write_table(self, df, name, mode="append", partition_by=None, partition_values=None):
        # Computed here, as a real write computes the frame: the step frees
        # what it kept once its tables are written, so a frame kept unread
        # would no longer be readable.
        self.written[name] = df.sparkSession.createDataFrame(df.collect(), df.schema)
        self.calls.append((name, mode, partition_values))


@pytest.fixture(scope="session")
def stub_storage(spark):
    """The four reference tables the step touches, for one flight.

    Session-scoped, because ``_run`` memoises its results against it: a full
    pass through the step is the most expensive thing in the suite and the two
    configurations are each run exactly once.
    """
    flight_list = spark.createDataFrame(
        [("trk-1", dt.datetime(2024, 6, 1, 12, 0), "EBBR", "EDDF", None, None)],
        "id string, dof timestamp, adep string, ades string, " "adep_p string, ades_p string",
    )
    layouts = spark.createDataFrame(
        [
            ("cell-ebbr-rwy", "EBBR", "osm-ebbr", "runway", "07/25"),
            ("cell-eddf-rwy", "EDDF", "osm-eddf", "runway", "07/25"),
            ("cell-ebbr-stand", "EBBR", "osm-ebbr-a1", "parking_position", "A1"),
            ("cell-eddf-stand", "EDDF", "osm-eddf-b2", "parking_position", "B2"),
        ],
        "hexaero_h3_id string, hexaero_apt_icao string, hexaero_osm_id string, "
        "hexaero_aeroway string, hexaero_ref string",
    )
    airports = spark.createDataFrame(
        [("EBBR", _EBBR[0], _EBBR[1], 0.0), ("EDDF", _EDDF[0], _EDDF[1], 0.0)],
        "ident string, latitude_deg double, longitude_deg double, " "elevation_ft double",
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
    # The runway-traversal family now prunes on the res-12 runway grid rather
    # than the hexaero polygons (which stay for ``calculate_airport_events``).
    # One runway cell per aerodrome, tagged with the strip's two directions.
    runway_grid = spark.createDataFrame(
        [
            ("cell-ebbr-rwy", "EBBR", "EBBR-07/25", "07", "25", "runway"),
            ("cell-eddf-rwy", "EDDF", "EDDF-07/25", "07", "25", "runway"),
        ],
        "h3_id string, apt_icao string, strip_id string, "
        "le_ident string, he_ident string, zone string",
    )
    # The FIR layer of step 00c: two volumes, cell A below 20,000 ft (the
    # climb and the descent) and cell B at cruise, so the shipped run must
    # emit a crossing each way. The families that did not exist before 2026-11
    # are off under every reconstructed configuration, which the ladder's base
    # pins, so the older runs are unaffected.
    import h3

    from opdi.reference.h3_airspaces import AIRSPACE_SCHEMA

    cell_a = h3.latlng_to_cell(50.9, 4.48, 7)
    cell_b = h3.latlng_to_cell(50.0, 8.5, 7)
    fir = spark.createDataFrame(
        [
            ("FIR", code, code, "524", "2024-11-28", "2024-12-26", 0, 999, 1e4, cell, 7)
            for code, cell in (("AAAA", cell_a), ("BBBB", cell_b))
        ],
        AIRSPACE_SCHEMA,
    )
    tracks = _flight(spark).withColumn(
        "h3_res_7",
        F.when(F.col("baro_altitude_c") * 3.28084 >= 20000.0, F.lit(cell_b)).otherwise(
            F.lit(cell_a)
        ),
    )
    return _StubStorage(
        {
            "opdi_flight_list": flight_list,
            "hexaero_airport_layouts": layouts,
            "h3_runway_zones": runway_grid,
            "oa_airports": airports,
            "oa_runways": runways,
            "opdi_h3_airspace_ref": fir,
        },
        tracks=tracks,
    )


_RUN_CACHE = {}


def _run(spark, stub_storage, events: EventConfig):
    """Drive the whole step over the fixture flight and return its milestones.

    Memoised on the version string: both fixtures are session-scoped, so one
    pass per configuration serves every assertion below.
    """
    # Keyed on the whole configuration, not on the version string: three of
    # the configurations exercised here stamp ``events_v0.2.0`` and differ only
    # in their flags, and a version key would silently hand the second and
    # third the first one's output -- a memo that makes every test agree by
    # construction.
    key = repr(events)
    if key not in _RUN_CACHE:
        config = OPDIConfig()
        config.events = events
        with tempfile.TemporaryDirectory() as log_dir:
            proc = FlightEventProcessor(spark, config, log_dir=log_dir)
            proc.storage = stub_storage
            proc._etl_flight_events_and_measures(stub_storage.tracks, batch_id="b_", month=MONTH)
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


def test_v4_keeps_atot_aldt_alongside_the_acdm_family(spark, stub_storage):
    """The other half of the trade, after the benchmark.

    The A-CDM ``airborne``/``touchdown`` were meant to retire ``ATOT``/``ALDT``,
    but the flight-events-v4 benchmark found them coverage-limited: they anchor
    on ~0-15 ft on-runway / near-threshold samples, seen densely only near
    Switzerland (~4%/7% network vs ATOT/ALDT ~92%/~100%). So v0.2.0 keeps
    ``ATOT``/``ALDT`` for coverage and publishes the A-CDM family alongside for
    its interpolated-crossing accuracy where reception allows -- both in one
    table, told apart by type string and version."""
    types = _types(_run(spark, stub_storage, EventConfig(**event_bench.V020_BASE)))
    assert {
        "ATOT",
        "ALDT",
    } <= types, f"ATOT/ALDT must be retained under v0.2.0; got {sorted(types)}"
    assert {"airborne", "touchdown"} <= types


def test_v5_publishes_one_type_per_question(spark, stub_storage):
    """The merge, stated as an absence.

    v0.2.0 answered "when did it take off" twice -- once as ``ATOT``, once as
    ``airborne`` -- and left the consumer to decide which to trust, an answer
    that varies by aerodrome. Under v0.3.0 the A-CDM arm takes the ``ATOT``
    name and the two collapse. ``airborne`` and ``touchdown`` must therefore
    not appear at all: their surviving would mean the merge renamed nothing and
    simply added two more rows.
    """
    types = _types(_run(spark, stub_storage, EventConfig()))
    assert {"ATOT", "ALDT"} <= types
    assert (
        types & {"airborne", "touchdown"} == set()
    ), f"the merged names must replace the A-CDM ones, not join them: {sorted(types)}"
    assert types & {"off-block", "on-block"} == set()
    assert {"AOBT", "AIBT"} <= types


def test_v5_publishes_one_top_per_direction(spark, stub_storage):
    """The fuzzy arm is dropped and the PRU arm takes the plain names, so there
    is exactly one top-of-climb definition in the table rather than two under
    different suffixes."""
    types = _types(_run(spark, stub_storage, EventConfig()))
    assert {"top-of-climb", "top-of-descent"} <= types
    assert types & {"top-of-climb-cco", "top-of-descent-cdo"} == set()


def test_v4_emits_the_a_cdm_runway_family(spark, stub_storage):
    """The other half of the trade: the retired types are gone *because* these
    arrived. Without this the previous test passes on a step that emits
    nothing at all."""
    got = _types(_run(spark, stub_storage, EventConfig()))
    assert {
        "line-up",
        "take-off-roll",
        "ATOT",
        RUNWAY_THRESHOLD_CROSSING,
        "ALDT",
        "runway-vacated",
    } <= got


def test_legacy_still_emits_exactly_what_it_always_did(spark, stub_storage):
    out = _run(spark, stub_storage, EventConfig.legacy())
    assert "take-off" in _types(out)
    assert out.select("version").distinct().collect()[0][0] == "events_v0.0.2"


def test_the_v002_settings_still_emit_landing(spark, stub_storage):
    """The v0.0.2 settings keep their ``landing``: the report at which the
    flight phase changes from descent to ground, with an empty ``info``. The
    default settings call the runway threshold crossing
    ``xing-runway-threshold`` and emit no ``landing``."""
    legacy = _run(spark, stub_storage, EventConfig.legacy()).filter(F.col("type") == "landing")
    assert legacy.count() > 0
    assert {r["info"] for r in legacy.collect()} == {""}
    assert "landing" not in _types(_run(spark, stub_storage, EventConfig()))


def test_legacy_keeps_the_undifferentiated_runway_pair(spark, stub_storage):
    """``entry-runway`` is retired by the flag, not by the version string."""
    got = _types(_run(spark, stub_storage, EventConfig.legacy()))
    assert {"entry-runway", "exit-runway"} <= got
    assert got & {"line-up", "take-off-roll", "runway-vacated"} == set()


def test_the_v0_1_0_rung_is_reconstructible(spark, stub_storage):
    """The reason the gate is a flag and not the version string.

    Task 10's ladder measures every V4 change against a baseline rung that
    reconstructs v0.1.0 -- which stamped ``events_v0.2.0``'s predecessor and
    *did* publish ``take-off``, ``landing``, ``ATOT``, ``ALDT``, ``AOBT`` and
    ``AIBT``. A gate on the version string would have stripped them from the
    rung named after them, and the baseline would no longer be the thing it is
    named after. Nothing else in the suite drives this configuration through
    the step.
    """
    types = _types(_run(spark, stub_storage, EventConfig(**event_bench.V4_BASE)))

    assert {
        "take-off",
        "landing",
        "ATOT",
        "ALDT",
        "AOBT",
        "AIBT",
    } <= types, f"the v0.1.0 vocabulary is not reconstructible: {sorted(types)}"
    assert types & {"line-up", "airborne"} == set()


def test_legacy_tops_carry_the_empty_info_of_their_release(spark, stub_storage):
    """``info`` is free-form and nothing downstream compares it, so a stamp
    added to it under a released version would change published data with no
    error anywhere. ``events_v0.0.2`` published ``""`` on its tops."""
    out = _run(spark, stub_storage, EventConfig.legacy()).filter(
        F.col("type").isin("top-of-climb", "top-of-descent")
    )

    assert out.count() > 0
    assert {r["info"] for r in out.collect()} == {""}


def test_the_threshold_crossing_carries_its_milestone_number(spark, stub_storage):
    """Under the default settings ``xing-runway-threshold`` is ICAO milestone
    T16, the crossing of the threshold plane. The ``landing`` of the v0.0.2
    settings is a different event (the phase change at ground contact) and
    carries no milestone number at all."""
    xing = _run(spark, stub_storage, EventConfig()).filter(
        F.col("type") == RUNWAY_THRESHOLD_CROSSING
    )
    assert xing.count() > 0
    info = json.loads(xing.collect()[0]["info"])
    assert info["milestone"] == "T16"


def test_every_top_of_climb_records_which_algorithm_produced_it(spark, stub_storage):
    """One arm now, and it still says which.

    The fuzzy arm is dropped and PRU takes the plain name, so the stamp no
    longer disambiguates two live algorithms -- it records which one produced
    a published row, which is what makes a past table readable after the next
    change.
    """
    out = _run(spark, stub_storage, EventConfig()).filter(F.col("type").startswith("top-of-climb"))
    methods = {json.loads(r["info"])["method"] for r in out.collect()}
    assert methods == {"pru"}


def test_every_top_of_descent_records_it_too(spark, stub_storage):
    """One arm now, and it still says which.

    The fuzzy arm is dropped and PRU takes the plain name, so the stamp no
    longer disambiguates two live algorithms -- it records which one produced
    a published row, which is what makes a past table readable after the next
    change.
    """
    out = _run(spark, stub_storage, EventConfig()).filter(
        F.col("type").startswith("top-of-descent")
    )
    methods = {json.loads(r["info"])["method"] for r in out.collect()}
    assert methods == {"pru"}


def test_a_go_around_does_not_depend_on_the_runway_reference_tables(spark, tmp_path):
    """A go-around may never touch a runway polygon.

    An approach abandoned at 200 ft is a go-around and the aircraft was never
    over the strip, which is why ``go_arounds`` reads only the destination
    position and the field elevations. Routing it through the guard that skips
    the traversal family when a runway reference table is missing would put
    back exactly the dependency the detector exists to avoid -- and it would
    fail open: no go-arounds and no error.
    """
    profile = [
        (0, 2000.0, -1575.0),
        (10, 1200.0, -1575.0),
        (20, 700.0, -1575.0),
        (30, 300.0, -787.0),
        (40, 200.0, 0.0),
        (50, 600.0, 1969.0),
        (60, 1200.0, 1969.0),
        (70, 2000.0, 1969.0),
        (80, 2500.0, 1969.0),
    ]
    tracks = spark.createDataFrame(
        [
            (
                "trk-g",
                _EDDF[0],
                _EDDF[1],
                _T0 + dt.timedelta(seconds=t),
                ft / 3.28084,
                140 / 1.94384,
                roc / 196.850394,
                "TEST456",
                "abc124",
                70.0,
                "cell-air",
            )
            for t, ft, roc in profile
        ],
        _TRACK_SCHEMA,
    )
    # Layouts present, ``oa_runways`` absent: the traversal family cannot name
    # a runway and skips, which must not take the go-around with it.
    storage = _StubStorage(
        {
            "opdi_flight_list": spark.createDataFrame(
                [("trk-g", dt.datetime(2024, 6, 1, 12, 0), "EBBR", "EDDF", None, None)],
                "id string, dof timestamp, adep string, ades string, "
                "adep_p string, ades_p string",
            ),
            "hexaero_airport_layouts": spark.createDataFrame(
                [("cell-ebbr-rwy", "EBBR", "osm-ebbr", "runway", "07/25")],
                "hexaero_h3_id string, hexaero_apt_icao string, "
                "hexaero_osm_id string, hexaero_aeroway string, hexaero_ref string",
            ),
            "oa_airports": spark.createDataFrame(
                [("EBBR", _EBBR[0], _EBBR[1], 0.0), ("EDDF", _EDDF[0], _EDDF[1], 0.0)],
                "ident string, latitude_deg double, longitude_deg double, " "elevation_ft double",
            ),
        },
        tracks=tracks,
    )

    config = OPDIConfig()
    config.events = EventConfig()
    proc = FlightEventProcessor(spark, config, log_dir=str(tmp_path / "ga"))
    proc.storage = storage
    # Only the branch under test: the runway family and the go-arounds live in
    # the hexaero branch, and a whole-step pass here would add three more
    # families to the most expensive kind of test in the suite for nothing.
    proc._etl_flight_events_and_measures(
        tracks,
        batch_id="g_",
        month=MONTH,
        calc_vertical=False,
        calc_horizontal=False,
        calc_seen=False,
    )
    types = {r["type"] for r in storage.written["opdi_flight_events"].collect()}
    spark.catalog.clearCache()

    assert "go-around" in types, f"expected a go-around in {sorted(types)}"
    assert "line-up" not in types


def test_the_block_events_are_one_detector_under_two_names(spark):
    """A pure rename keyed on the same flag as the rest of the vocabulary --
    so the V4 ladder's baseline rung keeps the names it published."""
    from opdi.pipeline.events import calculate_block_events

    # A taxi at 5 m/s (about 10 kt) from the first report to the last.
    sv = spark.createDataFrame(
        [("trk-1", _T0 + dt.timedelta(seconds=i), 5.0) for i in range(0, 120, 10)],
        "track_id string, event_time timestamp, velocity double",
    )
    stand = spark.createDataFrame(
        [
            ("trk-1", "exit-parking_position", _T0),
            ("trk-1", "entry-parking_position", _T0 + dt.timedelta(seconds=100)),
        ],
        "track_id string, type string, event_time timestamp",
    )

    shipped = {r["type"] for r in calculate_block_events(sv, stand, EventConfig()).collect()}
    v020 = {
        r["type"]
        for r in calculate_block_events(
            sv, stand, EventConfig(merge_duplicate_milestones=False)
        ).collect()
    }
    legacy = {r["type"] for r in calculate_block_events(sv, stand, EventConfig.legacy()).collect()}

    # One detector, one name -- and the name is the one legacy already used.
    # Only v0.2.0, which published the A-CDM vocabulary without merging it,
    # spells them differently.
    assert shipped == {"AOBT", "AIBT"}
    assert v020 == {"off-block", "on-block"}
    assert legacy == {"AOBT", "AIBT"}


def test_a_block_time_names_its_stand_and_follows_the_landing(spark):
    """An arrival: in flight until the landing at 300 s, taxi, stand B2 at
    EDDF. Only the in-block time is given, and it names the stand."""
    import json as _json

    from opdi.pipeline.events import calculate_block_events

    rows = [
        ("trk-1", _T0 + dt.timedelta(seconds=s), 50.0, 4.0, 120.0, 900.0, False)
        for s in range(0, 300, 10)
    ] + [
        ("trk-1", _T0 + dt.timedelta(seconds=s), 50.0, 4.0, 7.0, None, True)
        for s in range(300, 500, 10)
    ]
    sv = spark.createDataFrame(
        rows,
        "track_id string, event_time timestamp, lat double, lon double, velocity double, "
        "baro_altitude_c double, on_ground boolean",
    )
    info = _json.dumps({"osm_ref": "B2", "osm_airport": "EDDF"})
    stand = spark.createDataFrame(
        [
            ("trk-1", "entry-parking_position", _T0 + dt.timedelta(seconds=450), info),
            ("trk-1", "exit-parking_position", _T0 + dt.timedelta(seconds=490), info),
        ],
        "track_id string, type string, event_time timestamp, info string",
    )
    runway = spark.createDataFrame(
        [("trk-1", "ALDT", _T0 + dt.timedelta(seconds=300))],
        "track_id string, type string, event_time timestamp",
    )

    got = calculate_block_events(sv, stand, EventConfig(), runway_events=runway).collect()

    assert [r["type"] for r in got] == ["AIBT"]
    assert got[0]["event_time"] == _T0 + dt.timedelta(seconds=490)
    detail = _json.loads(got[0]["info"])
    assert detail["stand"] == "B2" and detail["apt_icao"] == "EDDF"
    assert "stand_entry" in detail


def test_the_track_projection_carries_every_column_a_detector_feature_gates_on():
    """Step 04 must not project away a column a detector tests for by name.

    ``calculate_airport_events`` enables ``airport_admit_on_ground`` only when
    ``"on_ground" in sv_f.columns``. That guard exists so the family degrades
    on data lacking the field rather than erroring -- but it also means a
    projection that drops the column turns the whole feature into a **silent
    no-op**, with the gate falling back to ``baro_altitude_c.isNotNull()``.

    That is what happened. ``on_ground`` was absent from ``process_month``'s
    select, so the fix merged to admit surface samples never ran in production.
    Measured on 2026-06-05 with everything else held constant -- same tracks,
    same layout table, same V07_shipped config -- adding the one column moved
    ``exit-parking_position`` from **1,489 to 45,364** events network-wide, and
    EBBR from **1 to 1,947**.

    Surface position messages carry no altitude at all, so without ``on_ground``
    the gate keeps only the ~0.3% of in-stand samples that happen to carry a
    barometric reading. Coverage then tracks barometric availability rather
    than reception, which is why LSZH (12.8% of in-stand samples with ground
    altitude) reached 64% block-time coverage while EBBR (0.1%) reached 1.18%,
    despite both sitting at ~100% ground detection in the coverage ranking.

    Pinning the column list rather than the behaviour because the behavioural
    test already existed and passed: ``test_layout.py`` hands ``on_ground``
    straight to the function, so it proves the gate works while saying nothing
    about whether the caller supplies it.
    """
    from opdi.pipeline.events import TRACK_COLUMNS

    assert "on_ground" in TRACK_COLUMNS, (
        "process_month's projection dropped on_ground; airport_admit_on_ground "
        "is silently inert without it"
    )


def test_the_log_directory_is_created_by_the_constructor_and_written_at_the_end(spark, tmp_path):
    """`log_dir` is created on construction and written to as the last act of a run.

    Both halves matter together, and the pairing is what makes the *ordering*
    of any reset load-bearing. A caller that clears `log_dir` to force a
    recompute must do it **before** constructing the processor: doing it after
    does not reset anything, it removes the directory the live processor is
    still holding paths into, and the run then dies marking progress -- after
    every event table has already been written.

    That is not a hypothetical. `event_bench`'s `--reuse-table` support cleared
    the directory one line too late on its first outing, and a rung died on the
    final line of its final family with two hours of compute already spent and
    no scores to show for it.
    """
    import datetime as dt
    import shutil

    log_dir = tmp_path / "events_log"
    config = OPDIConfig()
    config.events = EventConfig()
    proc = FlightEventProcessor(spark, config, log_dir=str(log_dir))

    assert log_dir.is_dir(), "the constructor must create its own log directory"

    month = dt.date(2026, 6, 1)
    proc._mark_processed("horizontal", month)
    assert month in proc._load_processed("horizontal")

    # The failure mode itself: the directory removed after construction, which
    # is precisely what a mis-ordered reset does.
    shutil.rmtree(log_dir)
    with pytest.raises(OSError):
        proc._mark_processed("vertical", month)


# --- the per-day partition key ------------------------------------------


def test_events_and_measurements_carry_the_track_s_start_day(spark, stub_storage):
    """`dof` is the day the *track* starts, not the day the event happens.

    A departure at 22:50 produces events after midnight. Keying those on their
    own date would split one flight's events across two partitions, so a day's
    run would write into its neighbour's day and could not be re-run in
    isolation. Keyed on the track, a day owns every row its tracks produce.
    """

    _run(spark, stub_storage, EventConfig())
    events = stub_storage.written["opdi_flight_events"]
    measurements = stub_storage.written["opdi_measurements"]

    assert "dof" in events.columns
    assert "dof" in measurements.columns

    # The fixture's single track starts on 2024-06-01; every row must say so,
    # including any event whose own timestamp has run past midnight.
    days = {str(r["dof"]) for r in events.select("dof").distinct().collect()}
    assert days == {"2024-06-01"}, days

    mdays = {str(r["dof"]) for r in measurements.select("dof").distinct().collect()}
    assert mdays == {"2024-06-01"}, mdays


def test_a_measurement_inherits_the_day_from_its_parent_event(spark, stub_storage):
    """opdi_measurements has no instant of its own -- its schema was id,
    milestone_id, type, value, version. The day has to come from the event the
    measurement hangs off, which is why it is joined rather than derived."""
    from pyspark.sql import functions as SF

    _run(spark, stub_storage, EventConfig())
    events = stub_storage.written["opdi_flight_events"].select(
        SF.col("id").alias("_ev_id"), SF.col("dof").alias("_ev_dof")
    )
    measurements = stub_storage.written["opdi_measurements"].select(
        SF.col("milestone_id").alias("_ms_id"), SF.col("dof").alias("_ms_dof")
    )

    joined = measurements.join(events, SF.col("_ms_id") == SF.col("_ev_id"), "inner")
    assert joined.count() > 0, "no measurement joined its event; the test proves nothing"
    assert joined.filter(SF.col("_ms_dof") != SF.col("_ev_dof")).count() == 0


def test_force_recomputes_every_family(spark, stub_storage, monkeypatch):
    """`skip_if_processed=False` must mean recompute, not just "don't return".

    The four family flags decide whether anything is computed. Read from the
    progress log unconditionally, a forced re-run produced a step that ran,
    wrote nothing, and reported success in 35 seconds -- a timing that looks
    like a 231x speedup and is actually a no-op.
    """
    import inspect

    from opdi.pipeline.events import FlightEventProcessor

    src = inspect.getsource(FlightEventProcessor.process_month)
    assert "_redo = not skip_if_processed" in src
    for family in ("horizontal", "vertical", "hexaero", "seen"):
        assert f'_redo or _batch not in self._load_processed("{family}")' in src, family


# --- per-family versions, info-backed measurements, the navigation group ------


def test_a_family_s_own_version_survives_stamping(spark, stub_storage):
    """A detector that stamps its own ``version`` keeps it; the published
    types keep ``events_version``."""
    out = _run(spark, stub_storage, EventConfig())
    versions = {r["type"]: r["version"] for r in out.collect()}
    assert versions["xing-fir"] == "fir_v0.1.0"
    assert versions["level-start"] == EventConfig().events_version


def test_a_full_run_emits_the_fir_family(spark, stub_storage):
    out = _run(spark, stub_storage, EventConfig())
    xings = out.filter(F.col("type") == "xing-fir").collect()
    assert len(xings) == 2
    pairs = sorted(
        (json.loads(r["info"])["fir_from"], json.loads(r["info"])["fir_to"]) for r in xings
    )
    assert pairs == [("AAAA", "BBBB"), ("BBBB", "AAAA")]


def test_families_off_emit_none_of_the_new_types(spark, stub_storage):
    """The ladder's base turns the three families off; its run has none of
    their types."""
    types = _types(_run(spark, stub_storage, EventConfig(**event_bench.V4_BASE)))
    assert not {t for t in types if t.startswith(("holding-", "pms-", "xing-fir"))}


def test_an_info_field_listed_in_extra_measures_becomes_a_measurement(spark, monkeypatch):
    import opdi.pipeline.events as ev

    monkeypatch.setattr(
        ev,
        "EXTRA_MEASURES",
        [
            ("holding-end", "duration_s", "Holding duration (s)", "holding_v9"),
            ("holding-end", "absent_field", "Test absent", "holding_v9"),
        ],
    )
    events = spark.createDataFrame(
        [
            ("e1", "holding-end", '{"duration_s": 42.0}', dt.date(2024, 6, 1)),
            ("e2", "holding-start", '{"duration_s": 42.0}', dt.date(2024, 6, 1)),
            ("e3", "holding-end", '{"other": 1}', dt.date(2024, 6, 1)),
        ],
        "id_tmp string, type string, info string, dof date",
    )
    rows = ev.extra_measurement_rows(events).collect()
    assert len(rows) == 1
    row = rows[0]
    assert (
        row["id"],
        row["milestone_id"],
        row["type"],
        row["value"],
        row["version"],
        str(row["dof"]),
    ) == (
        "e1" + ev.measurement_suffix("Holding duration (s)"),
        "e1",
        "Holding duration (s)",
        42.0,
        "holding_v9",
        "2024-06-01",
    )
    assert row["id"].startswith("e1_m") and len(row["id"]) == len("e1_m") + 6


def test_measurement_suffixes_are_stable_and_distinct():
    from opdi.pipeline.events import EXTRA_MEASURES, measurement_suffix

    suffixes = [measurement_suffix(m) for _, _, m, _ in EXTRA_MEASURES]
    assert len(set(suffixes)) == len(suffixes)
    assert measurement_suffix("Holding duration (s)") == measurement_suffix("Holding duration (s)")


def test_a_navigation_only_top_up_still_computes_go_arounds(spark, stub_storage, monkeypatch):
    """The holding family drops episodes next to a go-around; a run that
    computes only the navigation group must still have them."""
    import opdi.pipeline.events as ev

    seen = {}

    def capture(geo, config, go_arounds=None):
        seen["go_arounds"] = go_arounds
        return None

    monkeypatch.setattr(ev, "calculate_holding_events", capture)
    monkeypatch.setattr(ev, "calculate_point_merge_events", lambda *a, **k: None)
    monkeypatch.setattr(ev, "calculate_fir_events", lambda *a, **k: None)
    config = OPDIConfig()
    with tempfile.TemporaryDirectory() as log_dir:
        proc = FlightEventProcessor(spark, config, log_dir=log_dir)
        proc.storage = stub_storage
        proc._etl_flight_events_and_measures(
            stub_storage.tracks,
            batch_id="b_",
            month=MONTH,
            calc_vertical=False,
            calc_horizontal=False,
            calc_hexaero=False,
            calc_seen=True,
            calc_navigation=True,
        )
    assert seen["go_arounds"] is not None
    assert "event_time" in seen["go_arounds"].columns


def test_no_extra_measures_means_no_frame(spark, monkeypatch):
    import opdi.pipeline.events as ev

    monkeypatch.setattr(ev, "EXTRA_MEASURES", [])
    events = spark.createDataFrame(
        [("e1", "x", "{}", dt.date(2024, 6, 1))],
        "id_tmp string, type string, info string, dof date",
    )
    assert ev.extra_measurement_rows(events) is None


def test_track_columns_include_h3_res_7():
    from opdi.pipeline.events import TRACK_COLUMNS

    assert "h3_res_7" in TRACK_COLUMNS


def test_the_navigation_group_is_tracked_in_the_progress_log(spark, tmp_path):
    config = OPDIConfig()
    proc = FlightEventProcessor(spark, config, log_dir=str(tmp_path))
    assert "navigation" in proc._log_paths
    proc._mark_processed("navigation", dt.date(2026, 6, 14))
    assert dt.date(2026, 6, 14) in [
        d.date() if hasattr(d, "date") else d for d in proc._load_processed("navigation")
    ]


def test_a_removed_event_leaves_no_measurement_and_no_flight_list_column(
    spark, stub_storage, monkeypatch
):
    """An off-block time placed at cruise (35,000 ft, 600 kt) is removed by the
    plausibility checks. It is written to the working table of removed events,
    not to the events; it has no measurement; and step 04b, which reads the
    events, leaves the flight list's AOBT column empty."""
    from opdi.pipeline import events as events_module
    from opdi.pipeline.flight_list_milestones import enrich_flight_list
    from opdi.pipeline.plausibility import REJECTED_TABLE

    cruise = _T0 + dt.timedelta(seconds=300)

    def in_flight_block(sdf, airport_events, config=None, runway_events=None, checkpoints=None):
        return sdf.limit(1).select(
            F.lit("trk-1").alias("track_id"),
            F.lit("AOBT").alias("type"),
            F.lit(cruise).alias("event_time"),
            F.lit(None).cast("double").alias("lon"),
            F.lit(None).cast("double").alias("lat"),
            F.lit(None).cast("double").alias("altitude_ft"),
            F.lit(None).cast("double").alias("cumulative_distance_nm"),
            F.lit(None).cast("long").alias("cumulative_time_s"),
            F.lit("{}").alias("info"),
        )

    monkeypatch.setattr(events_module, "calculate_block_events", in_flight_block)
    storage = _StubStorage(
        stub_storage._tables,
        stub_storage.tracks.withColumn(
            "on_ground", F.col("baro_altitude_c") * 3.28084 < F.lit(1.0)
        ),
    )
    config = OPDIConfig()
    config.events = EventConfig()
    with tempfile.TemporaryDirectory() as log_dir:
        proc = FlightEventProcessor(spark, config, log_dir=log_dir)
        proc.storage = storage
        proc._etl_flight_events_and_measures(storage.tracks, batch_id="b_", month=MONTH)

    events = spark.createDataFrame(
        storage.written["opdi_flight_events"].collect(),
        storage.written["opdi_flight_events"].schema,
    )
    rejected = storage.written[REJECTED_TABLE].collect()
    measurements = storage.written["opdi_measurements"].collect()
    spark.catalog.clearCache()

    assert [(r["type"], r["event_time"], r["reject_rule"]) for r in rejected] == [
        ("AOBT", cruise, "airborne_ground_event")
    ]
    assert events.filter(F.col("type") == "AOBT").count() == 0
    assert rejected[0]["id"] not in {m["milestone_id"] for m in measurements}

    flight_list = spark.createDataFrame(
        [("trk-1", "abc123", "TEST123", _T0, "EBBR", "EDDF", "v", _T0, _T0)],
        "id string, ICAO24 string, FLT_ID string, DOF timestamp, ADEP string, ADES string, "
        "VERSION string, FIRST_SEEN timestamp, LAST_SEEN timestamp",
    )
    row = enrich_flight_list(flight_list, events, EventConfig()).collect()[0]
    assert row["AOBT"] is None


# ---------------------------------------------------------------------------
# The table of removed events
# ---------------------------------------------------------------------------


def _rejected_rows(spark, ids, day):
    from opdi.pipeline.events import REJECTED_SCHEMA

    return spark.createDataFrame(
        [
            (i, "trk-1", "AOBT", _T0, None, None, None, "OSN", "v", "{}", "rule", "{}", day)
            for i in ids
        ],
        REJECTED_SCHEMA,
    )


def _rejected_processor(spark, tables, tmp_path):
    from opdi.pipeline.events import FlightEventProcessor

    storage = _StubStorage(tables, None)
    proc = FlightEventProcessor(spark, OPDIConfig(), log_dir=str(tmp_path))
    proc.storage = storage
    return proc, storage


def test_a_full_rebuild_with_nothing_removed_empties_the_day(spark, tmp_path):
    """The checks off (or nothing removed) in a full rebuild of a day whose
    removed events exist from an earlier run: the day is replaced with
    nothing, for a day-scoped and for a month-scoped run."""
    from opdi.pipeline.plausibility import REJECTED_TABLE

    day = dt.date(2024, 6, 1)
    tables = {REJECTED_TABLE: _rejected_rows(spark, ["old"], day)}
    track_day = spark.createDataFrame([("trk-1", day)], "track_id string, dof date")
    for day_arg in (day, None):
        proc, storage = _rejected_processor(spark, tables, tmp_path)
        proc._write_rejected(None, track_day, full_rebuild=True, day=day_arg)
        assert storage.calls == [(REJECTED_TABLE, "overwrite", [{"dof": day}])]
        assert storage.written[REJECTED_TABLE].count() == 0


def test_without_the_checks_and_without_the_table_nothing_is_written(spark, tmp_path):
    proc, storage = _rejected_processor(spark, {}, tmp_path)
    track_day = spark.createDataFrame([("trk-1", dt.date(2024, 6, 1))], "track_id string, dof date")
    proc._write_rejected(None, track_day, full_rebuild=True, day=None)
    assert storage.calls == []


def test_a_call_that_adds_families_does_not_add_a_removed_event_twice(spark, tmp_path):
    from opdi.pipeline.plausibility import REJECTED_TABLE

    day = dt.date(2024, 6, 1)
    proc, storage = _rejected_processor(
        spark, {REJECTED_TABLE: _rejected_rows(spark, ["a"], day)}, tmp_path
    )
    track_day = spark.createDataFrame([("trk-1", day)], "track_id string, dof date")
    proc._write_rejected(
        _rejected_rows(spark, ["a", "b"], day), track_day, full_rebuild=False, day=day
    )
    assert storage.calls == [(REJECTED_TABLE, "append", None)]
    assert [r["id"] for r in storage.written[REJECTED_TABLE].collect()] == ["b"]
