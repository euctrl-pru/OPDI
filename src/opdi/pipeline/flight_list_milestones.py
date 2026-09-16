"""Step 04b: milestones, runways, stands and ring crossings on the flight list.

``opdi_flight_events`` is long-form -- one row per (flight, milestone) -- while
APDF, the ground truth OPDI benchmarks against, is flight-shaped. Every
comparison therefore began by reshaping OPDI's output: three joins and a pivot
to answer "when did this flight take off, from which runway, off which stand".
This step does that reshaping once, at write time, and publishes the answer as
columns beside the flight it belongs to.

**Additive only.** Every column the flight list already had keeps its name, its
type and its meaning, so current consumers are unaffected. What is added is
:data:`ADDED_COLUMNS`, all nullable, all null for a flight whose events are
missing -- which is most flights for most of them, because the ground milestones
need surface reception.

**The column set does not move with the configuration.** The six radii in
:data:`FLIGHT_LIST_RING_RADII_NM` are always published, whether or not
``EventConfig.ring_radii_nm`` asks for them. A schema that changes shape with a
config flag is a trap for a consumer: a missing column raises where a null
column reads as "not detected", and only one of those is true.

The step reads and writes nothing itself: it takes two DataFrames and returns
one. The caller wires storage.
"""

from typing import Optional

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from ..config import EventConfig

#: The radii the flight list publishes, as ``C{NM}_ARR`` / ``C{NM}_DEP``.
#:
#: Fixed here rather than read from ``EventConfig.ring_radii_nm`` on purpose --
#: see the module docstring. A radius the config does not emit yields a null
#: column, not a missing one.
FLIGHT_LIST_RING_RADII_NM = (40, 50, 60, 100, 110, 120)

#: Ring crossing events are typed ``xing-40nm``, ``xing-100nm`` and so on --
#: see ``events.calculate_ring_crossing_events``, which formats the radius as
#: ``threshold.cast("int")``.
_RING_TYPE = "xing-{nm}nm"

#: The four merged milestone types of ``events_v0.3.0``. Under that vocabulary
#: there is one event type per operational question: ``ATOT`` is the merge of
#: the A-CDM ``airborne`` and the legacy ``ATOT``, ``ALDT`` of ``touchdown``
#: and the legacy ``ALDT``, and ``AOBT``/``AIBT`` are the renamed
#: ``off-block``/``on-block``. The merge, and the ``info.method`` that records
#: which arm produced each value, belong to the event table; the flight list is
#: the convenience view and carries the value alone. Against an older
#: vocabulary these four columns come out null -- deliberately, rather than
#: reimplementing the merge here, where the two would be free to disagree.
_ATOT, _ALDT, _AOBT, _AIBT = "ATOT", "ALDT", "AOBT", "AIBT"

#: Parking-position types from ``pipeline.layout``, which builds them as
#: ``concat_ws("-", "entry"|"exit", osm_aeroway)``.
_STAND_EXIT, _STAND_ENTRY = "exit-parking_position", "entry-parking_position"

#: Columns this step adds, in the order it adds them.
#: Columns that say whether a row is a movement, rather than what happened
#: during one. They exist because the flight list counts more movements than
#: APDF does, and the excess is not spread evenly: it is departures (1.23x
#: against arrivals' 1.03x, measured 2026-06-01..03).
#:
#: ``MOVEMENT_DEP``/``MOVEMENT_ARR`` are the ones to filter on: they answer
#: "does this row count as a departure / an arrival", which is the only
#: question most consumers have. ``SUPERSEDED_*`` remain beside them to say
#: *why* a row does not count, which is what makes a surprising total
#: diagnosable rather than merely surprising.
#:
#: ``TRACK_DURATION_MIN`` is published rather than used as a filter here. A
#: duration threshold is a proxy for "is this a real flight" and leaks in both
#: directions -- at 30 minutes it discards 4,364 departures that demonstrably
#: took off while still admitting hour-long circuits -- so the honest thing is
#: to hand consumers the number and let them choose, not to choose for them.
MOVEMENT_COLUMNS = [
    "TRACK_DURATION_MIN",
    "SUPERSEDED_DEP", "SUPERSEDED_ARR",   # why a row does not count
    "MOVEMENT_DEP", "MOVEMENT_ARR",       # whether it counts -- filter on these
]

ADDED_COLUMNS = (
    [
        "ATOT", "ALDT", "AOBT", "AIBT",
        "RWY_DEP", "RWY_DEP_BEARING_DEG", "RWY_ARR", "RWY_ARR_BEARING_DEG",
        "STND_DEP", "STND_ARR",
    ]
    + [f"C{nm}_ARR" for nm in FLIGHT_LIST_RING_RADII_NM]
    + [f"C{nm}_DEP" for nm in FLIGHT_LIST_RING_RADII_NM]
    + MOVEMENT_COLUMNS
)


def _info(field: str) -> Column:
    """One field out of an event's ``info`` JSON.

    ``get_json_object`` rather than ``from_json`` with a schema: ``info`` is
    free-form by design and its keys differ per producer -- the legacy ``ATOT``
    carries ``runway``, the A-CDM ``airborne`` carries ``rwy_ident`` -- so a
    single declared schema would have to be a superset invented here and would
    silently return nulls the day a producer renamed a key.
    """
    return F.get_json_object(F.col("info"), f"$.{field}")


def _runway() -> Column:
    """The runway an ``ATOT``/``ALDT`` event used.

    Two spellings because the two arms that can produce the merged event spell
    it differently: ``events.calculate_runway_events`` writes ``runway``,
    ``runway_ops._info`` writes ``rwy_ident``. Coalescing reads whichever arm
    won for that flight without this module having to know which.
    """
    return F.coalesce(_info("runway"), _info("rwy_ident"))


def _runway_bearing() -> Column:
    """The runway centreline's true bearing, in degrees.

    A property of the pavement, derived from the threshold geometry -- not the
    aircraft's heading over it. Null is ordinary: the runway may not have been
    named, and any event written before the key existed carries no value for
    it, which reads the same way.
    """
    return _info("runway_bearing_deg").cast("double")


def _first(cond: Column, value: Column) -> Column:
    """The *value* of the earliest matching event, or NULL if none match."""
    return F.min(F.when(cond, F.struct(F.col("event_time").alias("t"), value.alias("v")))).getField("v")


def _last(cond: Column, value: Column) -> Column:
    """The *value* of the latest matching event, or NULL if none match."""
    return F.max(F.when(cond, F.struct(F.col("event_time").alias("t"), value.alias("v")))).getField("v")


def _null_ts() -> Column:
    return F.lit(None).cast("timestamp")


#: What the flight key is called, most-published name first. ``events.py``
#: writes ``track_id`` out as ``flight_id``, so a table read back from storage
#: and a frame taken straight from a detector disagree about the name of the
#: same column. Resolving it here beats making every caller know which side of
#: the write it is on.
FLIGHT_KEY_NAMES = ("flight_id", "track_id")


#: Runway events that belong to a *specific leg*. A take-off is this flight's
#: departure and a landing is its arrival, so the aerodrome such an event names
#: is the aerodrome of that leg -- not merely somewhere the aircraft once was.
#:
#: Stand, taxiway and apron events are deliberately **not** here. They prove
#: presence, not which leg: a track routinely covers the previous arrival's
#: taxi-in and a turnaround, so the aircraft parks at its *departure* field
#: before pushing back. Measured on 2026-06-01, where both kinds of evidence
#: exist they disagree on 19.2% of departures and 21.2% of arrivals -- and the
#: runway evidence is the one that is leg-specific. Runway-only also costs
#: nothing in reach: 20,879 departures and 23,521 arrivals against the ground
#: family's 23,103 of each.
DEPARTURE_RUNWAY_TYPES = ("ATOT", "line-up", "take-off-roll")
ARRIVAL_RUNWAY_TYPES = ("ALDT", "touchdown", "runway-vacated")


def correct_aerodromes(flight_list: DataFrame, events: DataFrame) -> DataFrame:
    """Let a runway event overrule the flight list's inferred aerodrome.

    ``ADEP``/``ADES`` come from trajectory geometry relative to aerodrome
    zones, which is inference. A runway event is not: the aircraft was matched
    to a named aerodrome's own pavement *while taking off or landing*. Where
    the two disagree, the pavement wins.

    No first/last heuristic and no guard against inventing a landing: a
    departure aerodrome is where the take-off happened and an arrival aerodrome
    is where the landing did. A track with no landing gets no ``ADES`` from
    here, which is the right answer rather than a case to special-case.

    ``ADEP_SOURCE``/``ADES_SOURCE`` become ``"runway_event"`` where the value
    came from here, so a correction is never silent and the evidence can be
    told from the inference it replaced.
    """
    key = _flight_key(events)
    apt = F.coalesce(_info("apt_icao"), _info("osm_airport"))

    runway = events.select(
        F.col(key).alias("_g_id"), "type", "event_time", apt.alias("_apt")
    ).filter(F.col("_apt").isNotNull())

    dep = F.col("type").isin(*DEPARTURE_RUNWAY_TYPES)
    arr = F.col("type").isin(*ARRIVAL_RUNWAY_TYPES)
    per_track = runway.groupBy("_g_id").agg(
        # Earliest departure evidence and latest arrival evidence, matching how
        # the milestone columns pick their own events: where a track holds more
        # than one movement, the flight list's departure describes how it began
        # and its arrival how it ended.
        _first(dep, F.col("_apt")).alias("_dep_apt"),
        _last(arr, F.col("_apt")).alias("_arr_apt"),
    )

    out = flight_list.join(per_track, flight_list["id"] == F.col("_g_id"), "left")
    new_adep, new_ades = F.col("_dep_apt"), F.col("_arr_apt")

    # Written back under the frame's *own* spelling. Spark resolves column
    # names case-insensitively but ``withColumn`` writes the name it is given,
    # so passing "ADEP" to a frame holding "adep" silently renames a column the
    # flight list publishes.
    def _as_named(name: str):
        for actual in out.columns:
            if actual.upper() == name:
                return actual
        return None

    for name, value in (("ADEP_SOURCE", new_adep), ("ADES_SOURCE", new_ades)):
        actual = _as_named(name)
        if actual is not None:
            out = out.withColumn(
                actual,
                F.when(value.isNotNull(), F.lit("runway_event")).otherwise(F.col(actual)),
            )
    for name, value in (("ADEP", new_adep), ("ADES", new_ades)):
        actual = _as_named(name)
        if actual is not None:
            out = out.withColumn(actual, F.coalesce(value, F.col(actual)))

    return out.drop("_g_id", "_dep_apt", "_arr_apt")


def add_movement_columns(
    flight_list: DataFrame, config: Optional[EventConfig] = None
) -> DataFrame:
    """Mark the rows that are half of a movement already counted.

    A real departure is routinely cut into two tracks -- a ground fragment that
    never leaves the stand, then the flight -- and both begin at the aerodrome,
    so both are given an ADEP and both count. The fragment is identifiable
    without guessing: it has **no take-off**, and another departure of the
    **same aircraft** from the **same aerodrome** follows it within
    ``supersede_window_seconds``.

    Arrivals get the mirror rule, reversed in time: the *later* of the pair is
    the fragment, because an arrival's flight ends at the aerodrome and the
    ground remnant follows it.

    Marked, never dropped. A row that is a superseded departure may still be a
    perfectly good arrival, and a consumer counting one leg must not lose the
    other. Counting movements is then::

        departures = ADEP is not null and not SUPERSEDED_DEP
        arrivals   = ADES is not null and not SUPERSEDED_ARR

    Measured over three days against APDF: departures fall from 1.23x to 1.11x
    and arrivals from 1.03x to 1.02x, while **no flight with a detected
    take-off or landing is marked** -- zero of 57,990 and zero of 58,000.
    """
    config = config or EventConfig()
    window_s = float(config.supersede_window_seconds)

    first_s = F.col("FIRST_SEEN").cast("long")
    last_s = F.col("LAST_SEEN").cast("long")

    # ``lead``/``lag`` at offset 1, not an unbounded frame: the question is
    # only about the adjacent movement of that aircraft at that aerodrome, so
    # this stays a single-row lookup rather than a scan.
    dep_order = Window.partitionBy("ADEP", "ICAO24").orderBy(first_s)
    arr_order = Window.partitionBy("ADES", "ICAO24").orderBy(last_s)

    next_dep = F.lead(first_s).over(dep_order)
    prev_arr = F.lag(last_s).over(arr_order)

    # The rule reads "this track has no take-off" as evidence. On a day whose
    # events have not been extracted yet, *every* track has no take-off, and
    # the absence means "not computed" rather than "did not fly" -- so the rule
    # would mark real movements. Measured: day 2026-06-04 sat in the table with
    # step 03 done and step 04 unfinished, and 7,610 of its departures were
    # flagged on no evidence at all.
    #
    # A day is only judged once something in it has flown. This is per-day
    # rather than per-row because that is the granularity at which step 04
    # completes: the table is legitimately half-processed during a campaign,
    # and a flag that is wrong until the next run is a flag a dashboard can
    # read in the meantime.
    day = Window.partitionBy("DOF")
    day_processed = (
        F.sum(F.when(F.col("ATOT").isNotNull() | F.col("ALDT").isNotNull(), 1)
              .otherwise(0)).over(day) > 0
    )

    superseded_dep = (
        day_processed
        & F.col("ATOT").isNull()
        & next_dep.isNotNull()
        & ((next_dep - first_s) <= F.lit(window_s))
    )
    superseded_arr = (
        day_processed
        & F.col("ALDT").isNull()
        & prev_arr.isNotNull()
        & ((last_s - prev_arr) <= F.lit(window_s))
    )

    # A "ghost": the row claims a leg, but nothing was seen to fly and no other
    # end was named. That is not evidence of a movement, only of an aircraft
    # near an aerodrome. They are the whole of the residual over-count -- at
    # EBBR, 338 of a 342-departure excess -- and the superseded rule cannot
    # reach them, because a ghost has no later real departure to be superseded
    # by.
    #
    # Guarded by ``day_processed`` for the same reason as above and more
    # sharply: before step 04 runs, *every* row has a null ATOT, so every
    # departure without a destination would be condemned as a ghost.
    # ``left_dep``/``came_arr``: any ring crossing at all, which is positive
    # evidence the aircraft actually went somewhere. It is the part that makes
    # this rule survive poor coverage -- a take-off can be missed while the
    # same flight is still seen at 40 NM, and at the edge of the network that
    # is the common case, not the exception.
    #
    # Coalesced across every ring rather than pinned to C40, so a configuration
    # that does not emit 40 NM does not silently condemn every departure.
    left_dep = F.coalesce(*[F.col(f"C{nm}_DEP") for nm in FLIGHT_LIST_RING_RADII_NM])
    came_arr = F.coalesce(*[F.col(f"C{nm}_ARR") for nm in FLIGHT_LIST_RING_RADII_NM])

    # Without the ring term this rule is a coverage detector wearing a movement
    # detector's clothes. Measured: dropping rows on "no take-off and no other
    # end" alone fixed EBBR (1.80x to 1.00x) and destroyed LTFM, which fell
    # from 0.97x to **0.39x** -- because at the edge of coverage a real
    # departure has no take-off and no destination either, and looks exactly
    # like a ramp ghost. The network aggregate still read 0.96x, flattered by
    # one aerodrome's under-count cancelling another's over-count.
    ghost_dep = (
        day_processed
        & F.col("ATOT").isNull() & F.col("ADES").isNull() & left_dep.isNull()
    )
    ghost_arr = (
        day_processed
        & F.col("ALDT").isNull() & F.col("ADEP").isNull() & came_arr.isNull()
    )

    # The single question a consumer actually has. Composed here so that
    # counting movements is one predicate rather than three, and so the
    # definition lives in one place when it changes again.
    movement_dep = F.col("ADEP").isNotNull() & ~superseded_dep & ~ghost_dep
    movement_arr = F.col("ADES").isNotNull() & ~superseded_arr & ~ghost_arr

    return (
        flight_list
        .withColumn("TRACK_DURATION_MIN", (last_s - first_s) / F.lit(60.0))
        # ``coalesce`` to False: a row with no ADEP has no next departure to be
        # superseded by, and NULL there would read as "unknown" when the answer
        # is simply no.
        .withColumn("SUPERSEDED_DEP", F.coalesce(superseded_dep, F.lit(False)))
        .withColumn("SUPERSEDED_ARR", F.coalesce(superseded_arr, F.lit(False)))
        .withColumn("MOVEMENT_DEP", F.coalesce(movement_dep, F.lit(False)))
        .withColumn("MOVEMENT_ARR", F.coalesce(movement_arr, F.lit(False)))
    )


def _flight_key(events: DataFrame) -> str:
    """The name the events frame uses for the flight key."""
    for name in FLIGHT_KEY_NAMES:
        if name in events.columns:
            return name
    raise ValueError(
        "the events frame has no flight key: expected one of "
        f"{', '.join(FLIGHT_KEY_NAMES)}, found {sorted(events.columns)}. "
        "opdi_flight_events names it flight_id; a detector frame names it "
        "track_id."
    )


def enrich_flight_list(
    flight_list: DataFrame,
    events: DataFrame,
    config: Optional[EventConfig] = None,
) -> DataFrame:
    """Add the milestone, runway, stand and ring columns to a flight list.

    Args:
        flight_list: the flight list, keyed on ``id`` (the ``track_id``) and
            carrying ``adep``/``ades``. Returned unchanged but for the added
            columns -- same names, same types, same order, same row count.
        events: ``opdi_flight_events``-shaped: ``type``, ``event_time``,
            ``info`` and the flight key. The **published** table names that key
            ``flight_id`` -- ``events.py`` writes ``track_id`` out under that
            alias -- while every detector frame upstream of the write still
            calls it ``track_id``. Either is accepted, ``flight_id`` first, so
            this works on a table read back from storage and on a frame handed
            straight from a detector. Other columns are ignored, and no filter
            on ``version`` is applied: the caller decides which vocabulary it
            is enriching from.
        config: supplies ``ring_radii_nm``. A radius it does not list still
            gets its column, filled with nulls.

    Returns:
        The flight list plus :data:`ADDED_COLUMNS`.

    Raises:
        ValueError: if the flight list already carries one of the added names,
            which would otherwise produce two columns of the same name and an
            ambiguous reference downstream.
    """
    config = config or EventConfig()

    # Before anything else reads adep/ades. The ring columns filter on them, the
    # movement rules test them, and the milestone columns are chosen relative to
    # them -- correcting afterwards would leave every one of those computed
    # against the aerodrome the evidence disagrees with.
    flight_list = correct_aerodromes(flight_list, events)

    # Re-enriching is normal, not an error. Step 04b runs once per campaign day
    # and rewrites every partition, so the second day necessarily meets a table
    # the first day already enriched; a re-run after an events fix meets one
    # too. Refusing made the step non-idempotent and stopped a campaign dead on
    # its first retry.
    #
    # Every added column is *derived* -- recomputable from the events in full
    # -- so dropping the previous answer and recomputing is exactly right, and
    # is what makes a day whose events arrived late pick them up later. Matched
    # case-insensitively because the flight list mixes cases and Spark resolves
    # either way; dropped by their real names so the drop actually finds them.
    stale = [c for c in flight_list.columns if c.upper() in {a.upper() for a in ADDED_COLUMNS}]
    if stale:
        flight_list = flight_list.drop(*stale)

    radii = {float(r) for r in config.ring_radii_nm}
    ring_types = [
        _RING_TYPE.format(nm=nm)
        for nm in FLIGHT_LIST_RING_RADII_NM
        if float(nm) in radii
    ]
    wanted = [_ATOT, _ALDT, _AOBT, _AIBT, _STAND_EXIT, _STAND_ENTRY] + ring_types

    # The ring events name the aerodrome they belong to (``info.apt_icao``) and
    # the sense of the crossing (``info.direction``: "inbound"/"outbound"), but
    # not which *leg* that is -- an event cannot know, since the same aerodrome
    # can be a flight's origin and its destination. The leg comes from the
    # flight list, so a narrow three-column projection of it joins the events
    # before the pivot. It is projected rather than joined in place so that the
    # wide flight list is touched exactly once, by the LEFT join at the end.
    keys = flight_list.select(
        F.col("id").alias("_track_id"),
        F.col("adep").alias("_adep"),
        F.col("ades").alias("_ades"),
    )
    narrowed = (
        events.select(
            F.col(_flight_key(events)).alias("_event_key"),
            "type", "event_time", "info",
        )
        .filter(F.col("type").isin(wanted))
        .join(keys, F.col("_event_key") == F.col("_track_id"), "inner")
    )

    apt = _info("apt_icao")
    direction = _info("direction")

    def ring(nm: int, arrival: bool) -> Column:
        """One ring column. Null throughout if the config does not emit it."""
        type_ = _RING_TYPE.format(nm=nm)
        if type_ not in ring_types:
            return _null_ts()
        if arrival:
            cond = (
                (F.col("type") == type_)
                & (direction == F.lit("inbound"))
                & (apt == F.col("_ades"))
            )
            return _last(cond, F.col("event_time"))
        cond = (
            (F.col("type") == type_)
            & (direction == F.lit("outbound"))
            & (apt == F.col("_adep"))
        )
        return _first(cond, F.col("event_time"))

    # Departure milestones take the *earliest* matching event and arrival
    # milestones the *latest*. A track is not guaranteed to hold exactly one
    # movement -- a turnaround that never changed callsign stays one track, a
    # go-around puts a second touchdown on one -- and where it holds more than
    # one, the flight list's departure describes how the track began and its
    # arrival how it ended. Picking by the extreme of ``event_time`` also makes
    # the answer independent of row order, so a re-run reproduces it; the
    # struct comparison breaks a tie on the value itself for the same reason.
    # Each milestone must come from an event at the aerodrome of *its own leg*.
    # A track routinely covers the previous arrival's taxi-in, a turnaround and
    # this departure, so an ``entry-parking_position`` on it is as likely to be
    # the stand the aircraft arrived on *last* night as this flight's. Without
    # the check, 52% of published STND_ARR values were the stand at a different
    # aerodrome; correcting ADEP/ADES from ground evidence only moved that to
    # 48%, because the disagreement is real rather than an error -- the event
    # belongs to a leg this row does not describe.
    #
    # The ring columns have always applied this test. The milestones did not,
    # and the inconsistency was the bug.
    # Filters what is demonstrably wrong, not what is merely unverifiable. An
    # event naming no aerodrome cannot be checked against the leg, and
    # discarding it would lose coverage to no purpose -- a null here means the
    # detector did not record where, not that it recorded somewhere else.
    at = F.coalesce(_info("osm_airport"), apt)
    _matches = lambda leg: at.isNull() | (at == F.col(leg))  # noqa: E731
    is_dep = lambda t: (F.col("type") == F.lit(t)) & _matches("_adep")  # noqa: E731
    is_arr = lambda t: (F.col("type") == F.lit(t)) & _matches("_ades")  # noqa: E731
    is_ = lambda t: F.col("type") == F.lit(t)  # noqa: E731
    agg = narrowed.groupBy("_track_id").agg(
        _first(is_dep(_ATOT), F.col("event_time")).alias("ATOT"),
        _last(is_arr(_ALDT), F.col("event_time")).alias("ALDT"),
        _first(is_(_AOBT), F.col("event_time")).alias("AOBT"),
        _last(is_(_AIBT), F.col("event_time")).alias("AIBT"),
        # The runway of the very event the time was taken from, not of some
        # other ATOT on the same track: same predicate, same extreme, so the
        # pair cannot disagree.
        _first(is_dep(_ATOT), _runway()).alias("RWY_DEP"),
        _first(is_dep(_ATOT), _runway_bearing()).alias("RWY_DEP_BEARING_DEG"),
        _last(is_arr(_ALDT), _runway()).alias("RWY_ARR"),
        _last(is_arr(_ALDT), _runway_bearing()).alias("RWY_ARR_BEARING_DEG"),
        _first(is_dep(_STAND_EXIT), _info("osm_ref")).alias("STND_DEP"),
        _last(is_arr(_STAND_ENTRY), _info("osm_ref")).alias("STND_ARR"),
        *[ring(nm, arrival=True).alias(f"C{nm}_ARR") for nm in FLIGHT_LIST_RING_RADII_NM],
        *[ring(nm, arrival=False).alias(f"C{nm}_DEP") for nm in FLIGHT_LIST_RING_RADII_NM],
    )

    # LEFT, flight list on the left: a flight with no events at all must
    # survive with nulls rather than vanish, and the row count is part of the
    # contract this step is checked against.
    # The movement columns read ATOT/ALDT, so they are computed *after* the
    # milestones land rather than beside them.
    # The milestone columns first, then the movement columns on top: the latter
    # read ATOT/ALDT, so they cannot be selected before those exist. Appending
    # in this order reproduces ADDED_COLUMNS exactly, which is the published
    # column order.
    milestones = [c for c in ADDED_COLUMNS if c not in MOVEMENT_COLUMNS]
    return add_movement_columns(
        flight_list.join(agg, flight_list["id"] == agg["_track_id"], "left")
        .drop("_track_id")
        .select(*flight_list.columns, *milestones),
        config,
    )
