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

from pyspark.sql import Column, DataFrame
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
ADDED_COLUMNS = (
    [
        "ATOT", "ALDT", "AOBT", "AIBT",
        "RWY_DEP", "RWY_DEP_BEARING_DEG", "RWY_ARR", "RWY_ARR_BEARING_DEG",
        "STND_DEP", "STND_ARR",
    ]
    + [f"C{nm}_ARR" for nm in FLIGHT_LIST_RING_RADII_NM]
    + [f"C{nm}_DEP" for nm in FLIGHT_LIST_RING_RADII_NM]
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

    existing = {c.upper() for c in flight_list.columns}
    clashes = [c for c in ADDED_COLUMNS if c.upper() in existing]
    if clashes:
        raise ValueError(
            f"flight_list already carries {clashes}; it looks enriched already"
        )

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
    is_ = lambda t: F.col("type") == F.lit(t)  # noqa: E731
    agg = narrowed.groupBy("_track_id").agg(
        _first(is_(_ATOT), F.col("event_time")).alias("ATOT"),
        _last(is_(_ALDT), F.col("event_time")).alias("ALDT"),
        _first(is_(_AOBT), F.col("event_time")).alias("AOBT"),
        _last(is_(_AIBT), F.col("event_time")).alias("AIBT"),
        # The runway of the very event the time was taken from, not of some
        # other ATOT on the same track: same predicate, same extreme, so the
        # pair cannot disagree.
        _first(is_(_ATOT), _runway()).alias("RWY_DEP"),
        _first(is_(_ATOT), _runway_bearing()).alias("RWY_DEP_BEARING_DEG"),
        _last(is_(_ALDT), _runway()).alias("RWY_ARR"),
        _last(is_(_ALDT), _runway_bearing()).alias("RWY_ARR_BEARING_DEG"),
        _first(is_(_STAND_EXIT), _info("osm_ref")).alias("STND_DEP"),
        _last(is_(_STAND_ENTRY), _info("osm_ref")).alias("STND_ARR"),
        *[ring(nm, arrival=True).alias(f"C{nm}_ARR") for nm in FLIGHT_LIST_RING_RADII_NM],
        *[ring(nm, arrival=False).alias(f"C{nm}_DEP") for nm in FLIGHT_LIST_RING_RADII_NM],
    )

    # LEFT, flight list on the left: a flight with no events at all must
    # survive with nulls rather than vanish, and the row count is part of the
    # contract this step is checked against.
    return (
        flight_list.join(agg, flight_list["id"] == agg["_track_id"], "left")
        .drop("_track_id")
        .select(*flight_list.columns, *ADDED_COLUMNS)
    )
