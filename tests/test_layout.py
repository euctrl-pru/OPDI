"""Task 4: airport layout events, gated on height above field elevation.

``calculate_airport_events`` used to gate its H3 layout match on
``flight_level <= airport_max_fl`` (FL20) against **uncorrected pressure
altitude** -- the 1013.25 hPa datum, not the ground. At any aerodrome whose
own elevation is a meaningful fraction of that 2,000 ft margin, the published
gate stops tracking the ground it is meant to be near: the same defect class
``phase_ground_above_field`` already fixes for the phase family. This file
checks the fix (the first test) and that moving the function did not change
anything else it publishes (the other two): the runway family (Task 3) now
owns ``entry-/exit-runway`` outright, and taxiway/apron matching is
untouched.
"""

import datetime as dt

from pyspark.sql import functions as F

from opdi.config import EventConfig
from opdi.pipeline.layout import calculate_airport_events

MONTH = dt.date(2024, 6, 1)

#: Two distinct resolution-12 cells at the same aerodrome, one per aeroway
#: type, so ``_track_over_a_runway``/``_track_over_a_taxiway`` can share one
#: ``StubStorage`` and differ only in which hexagon their samples sit in.
H3_CELL_RUNWAY = "8c1f05a3280a1ff"
H3_CELL_TAXIWAY = "8c1f05a3280a2ff"

#: The aerodrome every default fixture binds a flight to, and the one
#: ``StubStorage``'s ``oa_airports`` reports an elevation for.
ADEP = "EKCH"

FIELD_ELEV_FT = 3000.0
"""Deliberately not Zurich's real 1,416 ft. A sample 500 ft above Zurich sits
at pressure altitude 1,916 ft = FL19.2, which happens to also clear the
legacy FL20 gate -- so a test built on Zurich's real elevation could not tell
a fixed gate from a still-broken one. 3,000 ft is high enough that the two
gates disagree, which is the whole point of the first test below."""


class StubStorage:
    """Just enough of ``StorageManager`` for ``calculate_airport_events``.

    Same shape as ``tests/test_events_labelling.py``'s ``_StubStorage``
    (``table_exists``/``read_table`` over a dict), with an ``oa_airports``
    table added: Task 4's above-field gate is the first thing in this module
    that needs a field elevation, via ``elevation.attach_field_elevation``.
    """

    def __init__(self, spark, *, flight_list=None, layouts=None, airports=None):
        self._tables = {
            "opdi_flight_list": (
                flight_list if flight_list is not None else _default_flight_list(spark)
            ),
            "hexaero_airport_layouts": (
                layouts if layouts is not None else _default_layouts(spark)
            ),
            "oa_airports": (
                airports if airports is not None else _default_airports(spark)
            ),
        }

    def table_exists(self, name):
        return name in self._tables

    def read_table(self, name):
        return self._tables[name]


def _default_flight_list(spark):
    """One flight, ``trk-1``, from ``EKCH`` to ``ZZZZ``.

    ``ZZZZ`` deliberately has no ``oa_airports`` row: ``height_above_field_ft``
    takes the *smaller* of the two field-relative heights, so a NULL-elevation
    ``ZZZZ`` end (coalesced to 0, i.e. the datum) must lose to the ``EKCH`` end
    for these tests to be about ``EKCH``'s elevation rather than an accident of
    which end ``least()`` picked.
    """
    return spark.createDataFrame(
        [("trk-1", dt.datetime(2024, 6, 1, 12, 0), ADEP, "ZZZZ", None, None)],
        "id string, dof timestamp, adep string, ades string, "
        "adep_p string, ades_p string",
    )


def _default_layouts(spark):
    """One runway hexagon and one taxiway hexagon, both at ``EKCH``, each at
    its own cell -- so a track over one is never accidentally over the other.
    """
    return spark.createDataFrame(
        [
            (H3_CELL_RUNWAY, ADEP, "osm-rwy", "runway", "04L"),
            (H3_CELL_TAXIWAY, ADEP, "osm-twy", "taxiway", "TWY-A"),
        ],
        "hexaero_h3_id string, hexaero_apt_icao string, hexaero_osm_id string, "
        "hexaero_aeroway string, hexaero_ref string",
    )


def _default_airports(spark):
    """``EKCH``'s elevation, ``FIELD_ELEV_FT``. No row for ``ZZZZ``."""
    return spark.createDataFrame(
        [(ADEP, FIELD_ELEV_FT)],
        "ident string, elevation_ft double",
    )


_SAMPLE_SCHEMA = (
    "track_id string, icao24 string, callsign string, event_time timestamp, "
    "lat double, lon double, baro_altitude_c double, heading double, "
    "vert_rate double, h3_res_12 string, cumulative_distance_nm double, "
    "cumulative_time_s double"
)

FT_PER_M = 3.28084


def _sample_at(spark, height_above_field_ft, field_elev_ft, h3_cell=H3_CELL_TAXIWAY):
    """Two samples of track ``trk-1``, ten seconds apart, both at *h3_cell*
    and at a pressure altitude of ``field_elev_ft + height_above_field_ft``.

    Two samples rather than one: ``calculate_airport_events`` drops
    zero-duration crossings (``time_in_use_seconds != 0``), so a single point
    never survives to be counted either way -- entry and exit have to differ.

    ``field_elev_ft`` is a parameter, not read from ``FIELD_ELEV_FT``
    directly, only so a test that calls this states the two numbers behind
    its assertion together. For the result to mean what the caller intends,
    it must equal the elevation the ``StubStorage`` in play reports for
    ``EKCH`` (``FIELD_ELEV_FT`` under the default one).

    ``baro_altitude_c`` is metres, matching the OSN storage schema every
    other detector reads -- the conversion the other way (``* FT_PER_M``)
    lives in ``elevation.height_above_field_ft`` and in ``events.py``'s own
    ``altitude_ft`` column, so it is undone here to build the raw sample.
    """
    altitude_ft = field_elev_ft + height_above_field_ft
    baro_altitude_c = altitude_ft / FT_PER_M
    rows = [
        (
            "trk-1", "abc123", "SAS123",
            dt.datetime(2024, 6, 1, 12, 0, i * 10),
            55.618, 12.656, baro_altitude_c, 90.0, 0.0,
            h3_cell, float(i) * 0.01, float(i * 10),
        )
        for i in range(2)
    ]
    return spark.createDataFrame(rows, _SAMPLE_SCHEMA)


def _passes_gate(sv, config):
    """Whether *sv* clears the low-altitude gate.

    The default ``StubStorage`` always has a matching taxiway hexagon under
    ``H3_CELL_TAXIWAY`` (where ``_sample_at`` places its samples by default),
    so any row surviving to the output means the gate let the sample through
    to be layout-matched; an empty result means the gate rejected it before
    the layout join ever ran. Deliberately not the runway cell: under the
    default config the runway family's retirement (see the second test below)
    would drop the row for an unrelated reason and give a false negative.
    """
    out = calculate_airport_events(sv, MONTH, StubStorage(sv.sparkSession), config)
    return out.count() > 0


def _track_over_a_runway(spark, field_elev_ft=FIELD_ELEV_FT):
    """A track sitting on the runway hexagon -- on the ground, so it clears
    either gate regardless of which one is in force, *provided* the raw
    pressure altitude that ``field_elev_ft`` produces (via ``_sample_at``,
    with ``height_above_field_ft=0.0``) also clears the legacy FL20 arm.
    ``FIELD_ELEV_FT`` (3,000 ft) does not -- it exists to demonstrate the
    fix, not to survive the config it fixes -- so a legacy-config caller
    must pass a low ``field_elev_ft`` (e.g. ``0.0``) explicitly."""
    return _sample_at(spark, height_above_field_ft=0.0, field_elev_ft=field_elev_ft,
                       h3_cell=H3_CELL_RUNWAY)


def _track_over_a_taxiway(spark):
    """Same geometry as ``_track_over_a_runway``, on the taxiway hexagon
    instead -- the only variable between the two fixtures is aeroway type."""
    return _sample_at(spark, height_above_field_ft=0.0, field_elev_ft=FIELD_ELEV_FT,
                       h3_cell=H3_CELL_TAXIWAY)


def test_the_gate_is_measured_above_the_field_not_above_the_datum(spark):
    """An aircraft 500 ft above a 1,416 ft field (Zurich) is inside the gate.

    Its pressure altitude is 1,916 ft = FL19.2, which happens to pass the
    published FL20 gate too -- so the test uses a *higher* field where the two
    answers differ, which is the whole point: the published gate moves relative
    to the ground it is meant to be near.
    """
    sv = _sample_at(spark, height_above_field_ft=500.0, field_elev_ft=3000.0)
    assert _passes_gate(sv, EventConfig()) is True
    assert _passes_gate(sv, EventConfig.legacy()) is False


def test_runway_features_are_not_emitted_as_entry_runway_in_v4(spark):
    """`entry-runway`/`exit-runway` are retired: the runway family replaces them
    with line-up, runway-vacated and the two crossing types."""
    out = calculate_airport_events(_track_over_a_runway(spark), MONTH,
                                   StubStorage(spark), EventConfig())
    assert out.filter(F.col("type").rlike("runway")).count() == 0


def test_taxiway_and_stand_events_are_unchanged(spark):
    out = calculate_airport_events(_track_over_a_taxiway(spark), MONTH,
                                   StubStorage(spark), EventConfig())
    assert sorted({r["type"] for r in out.collect()}) == ["entry-taxiway", "exit-taxiway"]


def test_a_legacy_run_still_emits_entry_and_exit_runway(spark):
    """The runway family's retirement (the test above) is conditional on
    ``config.emit_runway_milestones``, which ``EventConfig.legacy()`` turns
    off. A run stamping ``events_v0.0.2`` has to reproduce the release it
    names, and that release published ``entry-runway``/``exit-runway`` from
    exactly this H3 layout match -- there was no runway family to retire
    yet. If a future edit to the ``emit_runway_milestones`` gate ever made
    the drop unconditional, this is the test that would catch it; nothing
    else in this file or in ``test_events_labelling.py`` runs a runway-typed
    layout row under ``EventConfig.legacy()`` any more (see the Task 4
    report's "Fix round 1" for why ``test_events_labelling.py`` moved to a
    taxiway fixture instead of using ``legacy()``).

    Built on ``field_elev_ft=0.0``, not ``FIELD_ELEV_FT`` (3,000 ft): the
    legacy config's gate is FL20 against *uncorrected* pressure altitude
    (``airport_gate_above_field=False``), and 3,000 ft clears FL20's
    2,000 ft ceiling -- the same reason the first test above needed a high
    field to make the two gates disagree. A sea-level field puts the track
    at pressure altitude 0 ft = FL0, unambiguously under FL20, so this test
    is about the runway-retirement condition alone, not the gate.
    """
    out = calculate_airport_events(
        _track_over_a_runway(spark, field_elev_ft=0.0), MONTH,
        StubStorage(spark), EventConfig.legacy(),
    )
    types = {r["type"] for r in out.collect()}
    assert {"entry-runway", "exit-runway"} <= types, (
        f"expected entry-runway and exit-runway in {sorted(types)}"
    )
