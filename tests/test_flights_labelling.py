"""The flight list must label a track with the callsign it actually flew."""
from opdi.pipeline.flights import dominant_flight_id


def _label(df):
    """The one-row-per-track label frame, as a dict, for readable assertions."""
    return {r["track_id"]: r["_dominant_flight_id"]
            for r in dominant_flight_id(df).collect()}


def test_blank_samples_do_not_win_the_label(spark):
    """F.min returns "" here. That is the bug, in one test."""
    df = spark.createDataFrame(
        [("t1", ""), ("t1", ""), ("t1", "SAS123"), ("t1", "SAS123")],
        "track_id string, flight_id string",
    )
    assert _label(df) == {"t1": "SAS123"}


def test_the_most_frequent_callsign_wins_not_the_smallest(spark):
    """Two real callsigns in one track: frequency decides, not the alphabet.

    AAA111 sorts first and would win under any smallest-real rule. It must lose.
    """
    df = spark.createDataFrame(
        [("t1", "ZZZ999"), ("t1", "ZZZ999"), ("t1", "ZZZ999"), ("t1", "AAA111")],
        "track_id string, flight_id string",
    )
    assert _label(df) == {"t1": "ZZZ999"}


def test_ties_break_deterministically_on_the_callsign(spark):
    """Equal counts must not depend on partitioning, or two runs disagree."""
    df = spark.createDataFrame(
        [("t1", "BBB222"), ("t1", "AAA111")],
        "track_id string, flight_id string",
    )
    assert _label(df) == {"t1": "AAA111"}


def test_a_track_that_never_broadcast_a_callsign_has_no_row(spark):
    """It drops out of the label frame, and the caller's left join restores "".

    Asserted here so the contract is explicit: this function does not invent a
    blank, the join does. A caller using an inner join would silently lose the
    flight, which is why Step 4 specifies a left join and a coalesce.
    """
    df = spark.createDataFrame(
        [("t1", ""), ("t1", "")], "track_id string, flight_id string")
    assert _label(df) == {}


def test_a_callsign_homogeneous_track_is_unchanged(spark):
    """Legacy tracks are homogeneous by construction; the fix must be a no-op.

    If this fails, the change is not backward compatible for legacy data and the
    version bump is hiding a regression rather than describing an improvement.
    """
    df = spark.createDataFrame(
        [("t1", "SAS123"), ("t1", "SAS123")],
        "track_id string, flight_id string",
    )
    assert _label(df) == {"t1": "SAS123"}


def test_two_tracks_are_labelled_independently(spark):
    """The window partitions by track. A leak across tracks is the same class of
    bug as the unbounded lookback that cost 31 points of fragmentation in V1."""
    df = spark.createDataFrame(
        [("t1", "SAS123"), ("t1", ""), ("t2", "KLM456"), ("t2", "")],
        "track_id string, flight_id string",
    )
    assert _label(df) == {"t1": "SAS123", "t2": "KLM456"}


# --- resolve_flight_id: the invariant the rest of the module depends on ------

def test_resolution_gives_every_sample_of_a_track_one_callsign(spark):
    """flight_id is a grouping key at ten sites and is never aggregated.

    One value per track is the invariant those sites were written on. This is
    the test that says so.
    """
    from opdi.pipeline.flights import resolve_flight_id

    df = spark.createDataFrame(
        [("t1", "SAS123"), ("t1", ""), ("t1", "SAS123"), ("t1", "")],
        "track_id string, flight_id string",
    )
    out = resolve_flight_id(df)
    assert {r["flight_id"] for r in out.collect()} == {"SAS123"}


def test_resolution_does_not_add_or_drop_samples(spark):
    """A left join that fans out is the bug wearing the fix's clothes.

    The failure this whole task addresses is a track becoming several rows at a
    grouping key. A resolution step that duplicates rows would cause exactly
    that, one stage earlier, and every downstream count would be wrong in a way
    no label assertion catches.
    """
    from opdi.pipeline.flights import resolve_flight_id

    df = spark.createDataFrame(
        [("t1", "SAS123"), ("t1", ""), ("t2", "KLM456"), ("t2", "KLM456"),
         ("t3", ""), ("t3", "")],
        "track_id string, flight_id string",
    )
    assert resolve_flight_id(df).count() == df.count() == 6


def test_resolution_leaves_an_unlabelled_track_blank_not_null(spark):
    """Downstream code fillna's to "" and compares against it. NULL would slip
    past those comparisons and reappear as a different bug."""
    from opdi.pipeline.flights import resolve_flight_id

    df = spark.createDataFrame(
        [("t1", ""), ("t1", "")], "track_id string, flight_id string")
    assert [r["flight_id"] for r in resolve_flight_id(df).collect()] == ["", ""]


def test_resolution_is_a_no_op_on_a_legacy_style_track(spark):
    """Legacy tracks are callsign-homogeneous by construction.

    If this fails, the change is not backward compatible and the version bump
    describes a regression rather than a capability.
    """
    from opdi.pipeline.flights import resolve_flight_id

    df = spark.createDataFrame(
        [("t1", "SAS123"), ("t1", "SAS123")],
        "track_id string, flight_id string",
    )
    out = [r["flight_id"] for r in resolve_flight_id(df).collect()]
    assert out == ["SAS123", "SAS123"]


# --- the overflight path writes into the same published table ---------------

def _overflights(spark, sv_rows, fl_ids=()):
    """Run ``_fetch_overflights`` over a hand-built track table.

    Stubbed the way ``test_version_is_new_unless_the_run_is_a_legacy_one``
    stubs ``_version_for``: the method is bound to a bare object carrying only
    the attributes it actually touches, so the path can be exercised without a
    StorageManager, a catalogue or a month of data.
    """
    from datetime import date

    from opdi.pipeline.flights import FlightListProcessor

    sv = spark.createDataFrame(
        sv_rows, "track_id string, icao24 string, callsign string, event_time timestamp"
    )
    fl = spark.createDataFrame([(i,) for i in fl_ids], "id string")

    class Stub:
        _fetch_overflights = FlightListProcessor._fetch_overflights

        def __init__(self):
            self.tracks_table = "osn_tracks"

        def _get_data_within_timeframe(self, table, month, time_col="event_time"):
            return sv if table == self.tracks_table else fl

        def _version_for(self, role):
            return "v5.0.0"

    return Stub()._fetch_overflights(date(2025, 6, 1)).collect()


def _sv_row(track, cs, minute):
    from datetime import datetime

    return (track, "abc123", cs, datetime(2025, 6, 1, 12, minute, 0))


def test_an_overflight_is_named_by_the_callsign_its_track_flew(spark):
    """The first sample decides FLT_ID on this path, and it is routinely blank.

    ``_fetch_overflights`` keeps only each track's earliest row, so before the
    fix the published FLT_ID was whatever the airframe happened to be
    broadcasting at that instant -- often nothing, because the callsign is blank
    until the crew sets it. These rows land in ``opdi_flight_list`` next to the
    detected flights, so a blank here is a blank in the published table.
    """
    rows = _overflights(spark, [
        _sv_row("t1", None, 0),
        _sv_row("t1", "", 2),
        _sv_row("t1", "RYR456", 4),
        _sv_row("t1", "RYR456", 6),
        _sv_row("t1", "RYR456", 8),
    ])
    assert [r["FLT_ID"] for r in rows] == ["RYR456"]


def test_an_overflight_track_still_yields_exactly_one_row(spark):
    """This path cannot fan out -- it reduces to one row per track before the
    rename -- and resolution must not be what introduces a fan-out."""
    rows = _overflights(spark, [
        _sv_row("t1", "", 0),
        _sv_row("t1", "RYR456", 5),
        _sv_row("t1", "EZY789", 6),
        _sv_row("t1", "RYR456", 7),
        _sv_row("t2", "KLM111", 0),
        _sv_row("t2", "KLM111", 9),
    ])
    assert sorted((r["id"], r["FLT_ID"]) for r in rows) == [
        ("t1", "RYR456"), ("t2", "KLM111"),
    ]


def test_an_overflight_that_never_broadcast_a_callsign_is_blank_not_null(spark):
    """It stays in the list, unlabelled.

    This is a change to the published column: the overflight path used to emit
    NULL here while the three detection paths emitted "". One published table
    should not carry two spellings of "no callsign", and FLIGHT_LIST_VERSION
    v5.0.0 is what says the rule changed.
    """
    rows = _overflights(spark, [_sv_row("t1", None, 0), _sv_row("t1", None, 6)])
    assert [r["FLT_ID"] for r in rows] == [""]


def test_an_overflight_already_in_the_flight_list_is_still_excluded(spark):
    """The anti-join is the point of the method; resolution must not disturb it."""
    rows = _overflights(
        spark,
        [_sv_row("t1", "RYR456", 0), _sv_row("t1", "RYR456", 6),
         _sv_row("t2", "KLM111", 0), _sv_row("t2", "KLM111", 6)],
        fl_ids=("t1",),
    )
    assert [r["id"] for r in rows] == ["t2"]


def test_a_short_overflight_is_still_dropped(spark):
    """Under five minutes of reception is not a flight. Unchanged by the fix."""
    rows = _overflights(spark, [_sv_row("t1", "RYR456", 0), _sv_row("t1", "RYR456", 2)])
    assert rows == []
