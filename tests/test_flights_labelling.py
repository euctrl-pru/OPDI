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
