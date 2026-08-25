"""Step 04 must not emit one event per callsign a track happened to broadcast.

``calculate_airport_events`` groups on ``flight_id`` without aggregating it.
That is sound only while a track carries exactly one callsign -- which legacy
segmentation guarantees by construction, because callsign is part of the
track's group key, and which the ``standard`` segmentation does not, because it
groups on the airframe alone.

The consequence is not internal: line 1003 publishes ``flight_id`` as
``osn_flight_id`` inside the event's ``info`` JSON, so one aircraft crossing one
runway once becomes two ``entry-runway`` milestones in the table OPDI actually
ships. Both look entirely plausible in isolation, which is why this needs a test
rather than an inspection.
"""

import datetime as dt
import json

import pytest
from pyspark.sql import functions as F

from opdi.config import EventConfig
from opdi.pipeline.events import calculate_airport_events, resolve_flight_id

MONTH = dt.date(2024, 6, 1)
#: One resolution-12 cell, shared by every sample and by the layout row, so the
#: layout join is a given and the test is about the grouping alone.
H3_CELL = "8c1f05a3280a1ff"


# --- the invariant, at the level of the key itself --------------------------

def test_a_track_with_two_callsigns_yields_one_group_per_zone(spark):
    """The fan-out, stated as the thing a reader would notice in the data.

    One aircraft entering one runway once must be one event. Grouping on an
    unresolved ``flight_id`` makes it two.
    """
    df = spark.createDataFrame(
        [("t1", "abc123", "SAS123", "EKCH", "rwy04L"),
         ("t1", "abc123", "", "EKCH", "rwy04L"),
         ("t1", "abc123", "SAS123", "EKCH", "rwy04L")],
        "track_id string, icao24 string, flight_id string, "
        "hexaero_apt_icao string, hexaero_ref string",
    )
    grouped = (
        resolve_flight_id(df)
        .groupBy("track_id", "icao24", "flight_id",
                 "hexaero_apt_icao", "hexaero_ref")
        .agg(F.count(F.lit(1)).alias("n"))
    )
    rows = grouped.collect()
    assert len(rows) == 1, f"expected one group, got {len(rows)}"
    assert rows[0]["flight_id"] == "SAS123"
    assert rows[0]["n"] == 3


def test_events_imports_the_helper_rather_than_owning_a_copy(spark):
    """Two copies of this rule is how production and the benchmark drifted.

    ``events.py`` re-exports the name it imports; asserting the identity keeps a
    later "small local tweak" from quietly forking the resolution rule between
    step 03 and step 04.
    """
    from opdi.pipeline import flights

    assert resolve_flight_id is flights.resolve_flight_id


# --- the same invariant, through the function that publishes the events -----

class _StubStorage:
    """Just enough StorageManager for the airport detector."""

    def __init__(self, tables):
        self._tables = tables

    def table_exists(self, name):
        return name in self._tables

    def read_table(self, name):
        return self._tables[name]


@pytest.fixture
def storage(spark):
    """One flight bound for EKCH, and one EKCH runway hexagon."""
    flight_list = spark.createDataFrame(
        [("trk-1", dt.datetime(2024, 6, 1, 12, 0), "EKCH", "EDDF", None, None)],
        "id string, dof timestamp, adep string, ades string, "
        "adep_p string, ades_p string",
    )
    layouts = spark.createDataFrame(
        [(H3_CELL, "EKCH", "osm-1", "runway", "04L")],
        "hexaero_h3_id string, hexaero_apt_icao string, hexaero_osm_id string, "
        "hexaero_aeroway string, hexaero_ref string",
    )
    return _StubStorage({
        "opdi_flight_list": flight_list,
        "hexaero_airport_layouts": layouts,
    })


def _crossing(spark, samples):
    """One track rolling along one runway, broadcasting *samples* in order.

    A sample is a callsign, or a ``(callsign, positional)`` pair. Positional
    samples sit in the same H3 cell, one second apart, on the ground -- so the
    only thing that can split this into more than one event group is the
    callsign. A non-positional sample is a velocity-only broadcast: it carries
    its callsign and nothing else, exactly as ADS-B delivers it, and the
    detector's ``dropna`` removes it before any event is formed.
    """
    rows = []
    for i, sample in enumerate(samples):
        callsign, positional = sample if isinstance(sample, tuple) else (sample, True)
        rows.append(
            (
                "trk-1",
                "abc123",
                callsign,
                dt.datetime(2024, 6, 1, 12, 0, i),
                55.618 if positional else None,
                12.656 if positional else None,
                0.0 if positional else None,
                90.0,
                0.0,
                H3_CELL if positional else None,
                float(i) * 0.01,
                float(i),
            )
        )
    return spark.createDataFrame(
        rows,
        "track_id string, icao24 string, callsign string, event_time timestamp, "
        "lat double, lon double, baro_altitude_c double, heading double, "
        "vert_rate double, h3_res_12 string, cumulative_distance_nm double, "
        "cumulative_time_s double",
    )


def _events(spark, storage, samples, config=None):
    return calculate_airport_events(
        _crossing(spark, samples), MONTH, storage, config or EventConfig()
    ).collect()


def _published_callsigns(rows):
    return {json.loads(r["info"])["osn_flight_id"] for r in rows}


def test_one_runway_crossing_is_one_event_not_one_per_callsign(spark, storage):
    """The defect end to end, in the milestone rows the step actually returns.

    Five samples, one runway, one crossing. Three of them carry SAS123 and two
    are blank -- and each sub-group spans more than a second, so each survives
    the zero-duration filter and becomes its own entry/exit pair. Unresolved
    this returns four milestones for one crossing; resolved it returns two.
    """
    rows = _events(spark, storage, ["SAS123", "SAS123", "SAS123", "", ""])
    assert sorted(r["type"] for r in rows) == ["entry-runway", "exit-runway"], (
        f"expected one entry/exit pair, got {[r['type'] for r in rows]}"
    )
    assert _published_callsigns(rows) == {"SAS123"}


def test_the_published_callsign_is_the_one_the_track_flew(spark, storage):
    """``osn_flight_id`` reaches the released table, so its value is not a
    detail. The blank the first samples carry must not become the label."""
    rows = _events(spark, storage, ["", "", "SAS123", "SAS123", "SAS123"])
    assert _published_callsigns(rows) == {"SAS123"}


def test_a_single_callsign_track_is_untouched(spark, storage):
    """Every track produced by legacy segmentation looks like this. The fix has
    to be a no-op on them or it is a regression wearing a fix's clothes."""
    rows = _events(spark, storage, ["SAS123"] * 4)
    assert sorted(r["type"] for r in rows) == ["entry-runway", "exit-runway"]
    assert _published_callsigns(rows) == {"SAS123"}


def test_a_track_that_never_broadcast_a_callsign_stays_in_the_events(spark, storage):
    """It is unlabelled, not absent. An inner join in the resolution would drop
    the crossing entirely, which is a worse bug than the one being fixed."""
    rows = _events(spark, storage, ["", "", ""])
    assert sorted(r["type"] for r in rows) == ["entry-runway", "exit-runway"]
    assert _published_callsigns(rows) == {""}


def test_the_vote_counts_samples_the_dropna_would_later_remove(spark, storage):
    """Resolution runs before the dropna, so step 04 votes over what step 03 did.

    The population a mode is taken over *is* the rule. Step 03 resolves on the
    whole month of the track table; if step 04 resolved after its own
    ``dropna(lat, lon, baro_altitude_c)`` it would be taking a different mode
    under the same name, and the two steps would name the same track
    differently -- SAS123 in ``opdi_flight_list``, "" or AAA111 in that track's
    own ``info.osn_flight_id``.

    That is not hypothetical: ADS-B sends position and velocity in separate
    message types, so a velocity-only sample carries a callsign and no
    position, and step 02a's cleaning NULLs the barometric altitude it rejects.
    Here SAS123 is the majority only if those three position-less samples
    count. If they do not, AAA111 wins on 2 to 0 and the assertion fails --
    which is what makes this test sensitive to the ordering rather than merely
    compatible with it.
    """
    rows = _events(spark, storage, [
        ("SAS123", False),
        ("SAS123", False),
        ("SAS123", False),
        ("AAA111", True),
        ("AAA111", True),
    ])
    assert sorted(r["type"] for r in rows) == ["entry-runway", "exit-runway"]
    assert _published_callsigns(rows) == {"SAS123"}


# --- the exemption for runs that reproduce a release ------------------------

def test_a_legacy_run_publishes_the_padded_blank_its_release_published(spark, storage):
    """``events_v0.0.2`` is a released string; a run stamping it must reproduce.

    Resolution would be a no-op on legacy tracks -- callsign is in their group
    key, so they are homogeneous -- with one exception it must not make. A
    callsign of eight spaces is blank to ``dominant_flight_id`` (which trims)
    but is not blank to ``fillna``, so resolution rewrites it to "" while the
    released events carry the spaces. OpenSky pads every callsign to eight
    characters, so this is the ordinary case for an aircraft that set none, not
    a corner one.

    Hence the exemption is on the version stamp rather than on an argument
    about homogeneity.
    """
    rows = _events(spark, storage, ["        "] * 4, config=EventConfig.legacy())
    assert _published_callsigns(rows) == {"        "}


def test_a_current_run_resolves_that_same_padded_blank_to_empty(spark, storage):
    """The other branch, so the conditional is not a code path nothing runs."""
    rows = _events(spark, storage, ["        "] * 4)
    assert _published_callsigns(rows) == {""}


def test_the_exempt_version_is_the_one_the_legacy_preset_stamps(spark):
    """Two spellings of "the released version" are one rename from disagreeing,
    and the disagreement would be silent: the guard would simply stop firing."""
    from opdi.pipeline.events import LEGACY_EVENTS_VERSION

    assert LEGACY_EVENTS_VERSION == EventConfig.legacy().events_version
