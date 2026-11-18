"""Step 04b: the main milestones, runways, stands and circle crossings on the flight list.

The flight events have one row per (flight, event). Most users want, for each
flight, when it took off and landed, from which runway and stand, and when it
crossed the distance circles around its airports. This step reads the events
and adds those values as columns of the flight list, and corrects the
departure and destination airports where a runway event names them.

Every column the flight list already had keeps its name, type and meaning.
The added columns are :data:`ADDED_COLUMNS`, all nullable, and NULL for a
flight whose events are missing, which for the ground milestones is common,
because they need reception on the airport surface.

**The column set does not move with the configuration.** The six radii in
:data:`FLIGHT_LIST_RING_RADII_NM` are always published, whether or not
``EventConfig.ring_radii_nm`` asks for them. A schema that changes shape with a
config flag is a trap for a consumer: a missing column raises where a null
column reads as "not detected", and only one of those is true.

The step reads and writes nothing itself: it takes two DataFrames and returns
one. The caller wires storage.
"""

from typing import Optional, Tuple

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from ..config import EventConfig
from ..utils.geometry import haversine_nm
from ..versions import FLIGHT_LIST_LEGS_VERSION, FLIGHT_LIST_VERSION
from .flight_legs import LEG_COLUMNS, event_in_leg, flight_legs, flight_stops, leg_id

#: The radii the flight list publishes, as ``C{NM}_ARR`` / ``C{NM}_DEP``.
#:
#: Fixed here rather than read from ``EventConfig.ring_radii_nm``, so the
#: columns are the same whatever the configuration. A radius the config does not emit yields a null
#: column, not a missing one.
FLIGHT_LIST_RING_RADII_NM = (40, 50, 60, 100, 110, 120)

#: Ring crossing events are typed ``xing-40nm``, ``xing-100nm`` and so on --
#: see ``events.calculate_ring_crossing_events``, which formats the radius as
#: ``threshold.cast("int")``.
_RING_TYPE = "xing-{nm}nm"

#: The four milestone types of ``events_v0.3.0``, one event type per
#: milestone: ``ATOT`` (the runway milestone ``airborne``, else the runway
#: window take-off time), ``ALDT`` (``touchdown``, else the runway window
#: landing time), ``AOBT`` and ``AIBT``. Which detector gave each value is in
#: ``info.method`` of the event; the flight list carries the value alone. With
#: the event types of release v0.0.2 these four columns are NULL.
_ATOT, _ALDT, _AOBT, _AIBT = "ATOT", "ALDT", "AOBT", "AIBT"

#: Parking-position types from ``pipeline.layout``, which builds them as
#: ``concat_ws("-", "entry"|"exit", osm_aeroway)``.
_STAND_EXIT, _STAND_ENTRY = "exit-parking_position", "entry-parking_position"

#: Columns this step adds, in the order it adds them.
#: Columns that say whether a row counts as a movement (a departure or an
#: arrival), rather than what happened during one. A flight list built from
#: tracks holds rows that are not movements: a departure is often split into
#: a short track that never leaves the stand and the flight itself.
#:
#: ``MOVEMENT_DEP``/``MOVEMENT_ARR`` are the ones to filter on: they answer
#: "does this row count as a departure / an arrival", which is the only
#: question most consumers have. ``SUPERSEDED_*`` remain beside them to say
#: *why* a row does not count, which is what makes a surprising total
#: diagnosable rather than merely surprising.
#:
#: ``TRACK_DURATION_MIN`` is published rather than used as a filter: a
#: duration limit would drop real short flights and keep long circuits, so
#: users can choose their own.
MOVEMENT_COLUMNS = [
    "TRACK_DURATION_MIN",
    "SUPERSEDED_DEP",
    "SUPERSEDED_ARR",  # why a row does not count
    "MOVEMENT_DEP",
    "MOVEMENT_ARR",  # whether it counts -- filter on these
]

ADDED_COLUMNS = (
    [
        "ATOT",
        "ALDT",
        "AOBT",
        "AIBT",
        "RWY_DEP",
        "RWY_DEP_BEARING_DEG",
        "RWY_ARR",
        "RWY_ARR_BEARING_DEG",
        "STND_DEP",
        "STND_ARR",
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

    Two spellings because the two detectors that can produce the merged event spell
    it differently: ``events.calculate_runway_events`` writes ``runway``,
    ``runway_ops._info`` writes ``rwy_ident``. Coalescing reads whichever one
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
    return F.min(F.when(cond, F.struct(F.col("event_time").alias("t"), value.alias("v")))).getField(
        "v"
    )


def _last(cond: Column, value: Column) -> Column:
    """The *value* of the latest matching event, or NULL if none match."""
    return F.max(F.when(cond, F.struct(F.col("event_time").alias("t"), value.alias("v")))).getField(
        "v"
    )


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
#: Stand, taxiway and apron events are not here. They show that the aircraft
#: was at an airport, not on which leg: a track often covers the previous
#: arrival's taxi-in and a turnaround before the departure. Runway events are
#: specific to the leg.
DEPARTURE_RUNWAY_TYPES = ("ATOT", "line-up", "take-off-roll")
ARRIVAL_RUNWAY_TYPES = ("ALDT", "touchdown", "runway-vacated")


# -- what the track shows (work package 4) ------------------------------------

#: Events whose altitude shows how high the track flew: the tops and the
#: flight-level crossings. Read for :attr:`EventConfig.movement_flags_need_track_evidence`.
_HEIGHT_TYPES = (
    "top-of-climb",
    "top-of-descent",
    "xing-fl50",
    "xing-fl70",
    "xing-fl100",
    "xing-fl245",
)

#: Prefix of the per-track columns :func:`_track_evidence` adds to the flight
#: list for the time of step 04b; all are dropped before the frame is returned.
_EV = "_ev_"


def _event_column(events: DataFrame, *names: str) -> Column:
    """The first of ``names`` the events frame has, or NULL.

    The stored events table names the position ``longitude``/``latitude``
    and the altitude ``altitude``; a detector frame names them ``lon``/``lat``
    and ``altitude_ft``.
    """
    for name in names:
        if name in events.columns:
            return F.col(name).cast("double")
    return F.lit(None).cast("double")


def _track_evidence(events: DataFrame) -> DataFrame:
    """Per track, what its events show about where it began and ended.

    One row per flight key, with:

    * ``_ev_dep_apt`` / ``_ev_dep_method``: airport and ``info.method`` of the
      earliest departure runway event (:data:`DEPARTURE_RUNWAY_TYPES`) that
      names an airport;
    * ``_ev_arr_apt`` / ``_ev_arr_method`` / ``_ev_arr_t``: the same for the
      latest arrival runway event, and its time;
    * ``_ev_last_dep_t``: the time of the latest departure runway event;
    * ``_ev_first_lat`` / ``_ev_first_lon`` and ``_ev_last_lat`` /
      ``_ev_last_lon``: the position of the ``first_seen`` and ``last_seen``
      events (the first and last report with a position, see
      :attr:`EventConfig.seen_from_measured_reports`);
    * ``_ev_max_alt_ft``: the highest altitude among the tops and flight-level
      crossings (:data:`_HEIGHT_TYPES`), in feet.

    One aggregation over the events, keyed like the milestone columns.
    """
    key = _flight_key(events)
    apt = F.coalesce(_info("apt_icao"), _info("osm_airport"))
    lat = _event_column(events, "latitude", "lat")
    lon = _event_column(events, "longitude", "lon")
    alt = _event_column(events, "altitude", "altitude_ft")
    named = apt.isNotNull()
    dep = F.col("type").isin(*DEPARTURE_RUNWAY_TYPES) & named
    arr = F.col("type").isin(*ARRIVAL_RUNWAY_TYPES) & named
    positioned = lat.isNotNull() & lon.isNotNull()
    first = (F.col("type") == "first_seen") & positioned
    last = (F.col("type") == "last_seen") & positioned
    method = _info("method")
    agg = events.groupBy(F.col(key).alias(f"{_EV}id")).agg(
        _first(dep, F.struct(apt.alias("a"), method.alias("m"))).alias("_dep"),
        _last(
            arr, F.struct(apt.alias("a"), method.alias("m"), F.col("event_time").alias("t"))
        ).alias("_arr"),
        F.max(F.when(dep, F.col("event_time"))).alias(f"{_EV}last_dep_t"),
        _first(first, F.struct(lat.alias("y"), lon.alias("x"))).alias("_fs"),
        _last(last, F.struct(lat.alias("y"), lon.alias("x"))).alias("_ls"),
        F.max(F.when(F.col("type").isin(*_HEIGHT_TYPES), alt)).alias(f"{_EV}max_alt_ft"),
    )
    return agg.select(
        f"{_EV}id",
        F.col("_dep.a").alias(f"{_EV}dep_apt"),
        F.col("_dep.m").alias(f"{_EV}dep_method"),
        F.col("_arr.a").alias(f"{_EV}arr_apt"),
        F.col("_arr.m").alias(f"{_EV}arr_method"),
        F.col("_arr.t").alias(f"{_EV}arr_t"),
        f"{_EV}last_dep_t",
        F.col("_fs.y").alias(f"{_EV}first_lat"),
        F.col("_fs.x").alias(f"{_EV}first_lon"),
        F.col("_ls.y").alias(f"{_EV}last_lat"),
        F.col("_ls.x").alias(f"{_EV}last_lon"),
        f"{_EV}max_alt_ft",
    )


def _with_airport(
    frame: DataFrame, airports: Optional[DataFrame], ident: Column, prefix: str
) -> DataFrame:
    """Add ``{prefix}lat``, ``{prefix}lon`` and ``{prefix}elev_ft`` of the airport ``ident``.

    ``airports`` is ``oa_airports`` (``ident``, ``latitude_deg``,
    ``longitude_deg``, ``elevation_ft``), sent to every executor. Without it
    the three columns are NULL, and the tests that need them read as unknown.
    """
    if airports is None:
        return (
            frame.withColumn(f"{prefix}lat", F.lit(None).cast("double"))
            .withColumn(f"{prefix}lon", F.lit(None).cast("double"))
            .withColumn(f"{prefix}elev_ft", F.lit(None).cast("double"))
        )
    ref = airports.select(
        F.col("ident").alias(f"{prefix}ident"),
        F.col("latitude_deg").cast("double").alias(f"{prefix}lat"),
        F.col("longitude_deg").cast("double").alias(f"{prefix}lon"),
        F.col("elevation_ft").cast("double").alias(f"{prefix}elev_ft"),
    ).dropDuplicates([f"{prefix}ident"])
    return frame.join(F.broadcast(ref), ident == F.col(f"{prefix}ident"), "left").drop(
        f"{prefix}ident"
    )


def _named(frame: DataFrame, name: str) -> Optional[str]:
    """The spelling ``frame`` uses for the column ``name`` (any case), or None."""
    for actual in frame.columns:
        if actual.upper() == name.upper():
            return actual
    return None


def correct_aerodromes(
    flight_list: DataFrame,
    events: DataFrame,
    config: Optional[EventConfig] = None,
    airports: Optional[DataFrame] = None,
) -> DataFrame:
    """Let a runway event overrule the flight list's inferred aerodrome.

    ``ADEP``/``ADES`` come from trajectory geometry relative to aerodrome
    zones, which is inference. A runway event is not: the aircraft was matched
    to a named aerodrome's own pavement *while taking off or landing*. Where
    the two disagree, the pavement wins.

    No first/last heuristic and no check against inventing a landing: a
    departure aerodrome is where the take-off happened and an arrival aerodrome
    is where the landing did. A track with no landing gets no ``ADES`` from
    here, which is the right answer rather than a case to special-case.

    ``ADEP_SOURCE``/``ADES_SOURCE`` become ``"runway_event"`` where the value
    came from here, so a correction is never silent and the evidence can be
    told from the inference it replaced.

    With :attr:`EventConfig.aerodrome_correction_checks_track`, the runway
    event must also fit the track: the flight list must already carry the
    columns of :func:`_track_evidence` (``_ev_*``), and ``airports`` gives
    the airports' positions. A distance that cannot be measured (no airport
    table, no position) does not stop the event.
    """
    if config is not None and config.aerodrome_correction_checks_track:
        return _correct_aerodromes_on_track(flight_list, config, airports)

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
    return _apply_aerodromes(out)


def _correct_aerodromes_on_track(
    flight_list: DataFrame, config: EventConfig, airports: Optional[DataFrame]
) -> DataFrame:
    """:func:`correct_aerodromes` with the checks against the track.

    Reads the ``_ev_*`` columns of :func:`_track_evidence` on ``flight_list``
    and keeps them. See :attr:`EventConfig.aerodrome_correction_checks_track`.
    """
    radius = float(config.aerodrome_correction_radius_nm)
    out = _with_airport(flight_list, airports, F.col(f"{_EV}dep_apt"), "_rd_")
    out = _with_airport(out, airports, F.col(f"{_EV}arr_apt"), "_ra_")

    def not_legacy(method: str) -> Column:
        return ~F.coalesce(F.col(method) == F.lit("legacy"), F.lit(False))

    def near(lat: str, lon: str, prefix: str) -> Column:
        # Unknown (no position, no airport) counts as near.
        d = haversine_nm(F.col(lat), F.col(lon), F.col(f"{prefix}lat"), F.col(f"{prefix}lon"))
        return F.coalesce(d <= F.lit(radius), F.lit(True))

    dep_ok = (
        F.col(f"{_EV}dep_apt").isNotNull()
        & not_legacy(f"{_EV}dep_method")
        & near(f"{_EV}first_lat", f"{_EV}first_lon", "_rd_")
    )
    after_departures = F.col(f"{_EV}last_dep_t").isNull() | (
        F.col(f"{_EV}arr_t") >= F.col(f"{_EV}last_dep_t")
    )
    arr_ok = (
        F.col(f"{_EV}arr_apt").isNotNull()
        & not_legacy(f"{_EV}arr_method")
        & after_departures
        & near(f"{_EV}last_lat", f"{_EV}last_lon", "_ra_")
    )
    out = out.withColumn("_dep_apt", F.when(dep_ok, F.col(f"{_EV}dep_apt"))).withColumn(
        "_arr_apt", F.when(arr_ok, F.col(f"{_EV}arr_apt"))
    )
    out = out.drop(*[c for c in out.columns if c.startswith("_rd_") or c.startswith("_ra_")])
    return _apply_aerodromes(out)


def _apply_aerodromes(out: DataFrame) -> DataFrame:
    """Write ``_dep_apt`` / ``_arr_apt`` into ADEP / ADES, their sources and lists.

    The second half of :func:`correct_aerodromes`: where ``_dep_apt``
    (``_arr_apt``) is not NULL it replaces the airport.
    """
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
        if actual is None:
            continue
        # The alternatives list names the aerodromes that were not chosen, so it
        # never names the row's own answer. A correction moves the chosen one:
        # the new aerodrome leaves the list and the overruled one joins it. The
        # removal is done on every pass, because this step rewrites every
        # partition each run and reads lists it corrected before.
        alternatives = _as_named(f"{name}_P")
        if alternatives is not None:
            final = F.coalesce(value, F.col(actual))
            moved = value.isNotNull() & ~value.eqNullSafe(F.col(actual))
            listed = F.filter(
                F.split(F.coalesce(F.col(alternatives), F.lit("")), ", "),
                # Built at once inside this iteration, so `final` is current.
                lambda x: (x != "") & ~x.eqNullSafe(final),  # noqa: B023
            )
            with_old = F.when(
                moved & F.col(actual).isNotNull(), F.concat(listed, F.array(F.col(actual)))
            ).otherwise(listed)
            cleaned = F.array_join(F.array_distinct(with_old), ", ")
            # A NULL list stays NULL unless something moved: it is the published
            # value on overflight rows and on the empty side of the flight table's
            # join, and a row this step does not touch must not change.
            out = out.withColumn(
                alternatives,
                F.when(F.col(alternatives).isNull() & ~moved, F.col(alternatives)).otherwise(
                    cleaned
                ),
            )
        out = out.withColumn(actual, F.coalesce(value, F.col(actual)))

    return out.drop(*[c for c in ("_g_id", "_dep_apt", "_arr_apt") if c in out.columns])


def add_movement_columns(
    flight_list: DataFrame,
    config: Optional[EventConfig] = None,
    airports: Optional[DataFrame] = None,
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

    A flight with a detected take-off or landing is never marked.
    See https://www.opdi.aero/content/methods/airports.html.

    With :attr:`EventConfig.movement_flags_need_track_evidence` and the
    ``_ev_*`` columns of :func:`_track_evidence` on ``flight_list``, the
    flags also read what the track shows (``airports`` gives the airports'
    positions and elevations).
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
    # events have not been computed yet every track has no take-off, and the
    # rule would mark real movements. So a day is judged only once some
    # flight in it has a take-off; per day, because step 04 completes a day
    # at a time.
    day = Window.partitionBy("DOF")
    day_processed = (
        F.sum(F.when(F.col("ATOT").isNotNull() | F.col("ALDT").isNotNull(), 1).otherwise(0)).over(
            day
        )
        > 0
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

    # A row that names a departure (or arrival) but where nothing was seen to
    # fly and no other end was named shows only an aircraft near an airport,
    # not a movement. The superseded rule cannot reach these rows, as no later
    # departure follows them.
    #
    # Applied only on days with events (``day_processed``), for the reason
    # above: before step 04 every row has a NULL ATOT.
    # ``left_dep``/``came_arr``: any ring crossing at all, which is positive
    # evidence the aircraft actually went somewhere. It is the part that makes
    # this rule survive poor coverage -- a take-off can be missed while the
    # same flight is still seen at 40 NM, and at the edge of the network that
    # is the common case, not the exception.
    #
    # Coalesced across every ring rather than tied to C40, so a configuration
    # that does not emit 40 NM does not silently condemn every departure.
    left_dep = F.coalesce(*[F.col(f"C{nm}_DEP") for nm in FLIGHT_LIST_RING_RADII_NM])
    came_arr = F.coalesce(*[F.col(f"C{nm}_ARR") for nm in FLIGHT_LIST_RING_RADII_NM])

    # Without the circle crossings this rule would remove real departures at
    # the edge of coverage, which often have neither a take-off nor a
    # destination either.
    left_dep = left_dep.isNotNull()
    came_arr = came_arr.isNotNull()

    evidence = (
        getattr(config, "movement_flags_need_track_evidence", False)
        and f"{_EV}max_alt_ft" in flight_list.columns
    )
    if evidence:
        # What the track shows (EventConfig.movement_flags_need_track_evidence):
        # how high it flew above each airport, and how far its other end lies.
        flight_list = _with_airport(flight_list, airports, F.col("ADEP"), "_md_")
        flight_list = _with_airport(flight_list, airports, F.col("ADES"), "_ma_")
        height = float(config.movement_airborne_height_ft)
        distance = float(config.movement_left_distance_nm)

        def flew_above(prefix: str) -> Column:
            above = F.col(f"{_EV}max_alt_ft") - F.coalesce(F.col(f"{prefix}elev_ft"), F.lit(0.0))
            return F.coalesce(above > F.lit(height), F.lit(False))

        def far_from(lat: str, lon: str, prefix: str) -> Column:
            d = haversine_nm(F.col(lat), F.col(lon), F.col(f"{prefix}lat"), F.col(f"{prefix}lon"))
            return F.coalesce(d > F.lit(distance), F.lit(False))

        flew_dep, flew_arr = flew_above("_md_"), flew_above("_ma_")
        # A superseded row is a ground fragment at a named airport.
        superseded_dep = F.col("ADEP").isNotNull() & superseded_dep & ~flew_dep
        superseded_arr = F.col("ADES").isNotNull() & superseded_arr & ~flew_arr
        # Climbing away from the airport, or ending far from it, is leaving it.
        left_dep = left_dep | flew_dep | far_from(f"{_EV}last_lat", f"{_EV}last_lon", "_md_")
        came_arr = came_arr | flew_arr | far_from(f"{_EV}first_lat", f"{_EV}first_lon", "_ma_")

    ghost_dep = day_processed & F.col("ATOT").isNull() & F.col("ADES").isNull() & ~left_dep
    ghost_arr = day_processed & F.col("ALDT").isNull() & F.col("ADEP").isNull() & ~came_arr

    # The single question a consumer actually has. Composed here so that
    # counting movements is one predicate rather than three, and so the
    # definition lives in one place when it changes again.
    movement_dep = F.col("ADEP").isNotNull() & ~superseded_dep & ~ghost_dep
    movement_arr = F.col("ADES").isNotNull() & ~superseded_arr & ~ghost_arr

    out = (
        flight_list.withColumn("TRACK_DURATION_MIN", (last_s - first_s) / F.lit(60.0))
        # ``coalesce`` to False: a row with no ADEP has no next departure to be
        # superseded by, and NULL there would read as "unknown" when the answer
        # is simply no.
        .withColumn("SUPERSEDED_DEP", F.coalesce(superseded_dep, F.lit(False)))
        .withColumn("SUPERSEDED_ARR", F.coalesce(superseded_arr, F.lit(False)))
        .withColumn("MOVEMENT_DEP", F.coalesce(movement_dep, F.lit(False)))
        .withColumn("MOVEMENT_ARR", F.coalesce(movement_arr, F.lit(False)))
    )
    if evidence:
        out = out.drop(*[c for c in out.columns if c.startswith("_md_") or c.startswith("_ma_")])
    return out


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


def _with_track_evidence(
    flight_list: DataFrame, events: DataFrame, config: EventConfig
) -> DataFrame:
    """The flight list with the columns of :func:`_track_evidence` joined on.

    Only when a check of work package 4 reads them
    (:attr:`EventConfig.aerodrome_correction_checks_track`,
    :attr:`EventConfig.movement_flags_need_track_evidence`); otherwise the
    flight list is returned as it is. One aggregation of the events, joined
    once.
    """
    if not (config.aerodrome_correction_checks_track or config.movement_flags_need_track_evidence):
        return flight_list
    evidence = _track_evidence(events)
    return flight_list.join(evidence, flight_list["id"] == evidence[f"{_EV}id"], "left").drop(
        f"{_EV}id"
    )


def corrected_aerodromes(
    flight_list: DataFrame,
    events: DataFrame,
    config: Optional[EventConfig] = None,
    airports: Optional[DataFrame] = None,
) -> DataFrame:
    """The flight list's ``id``, ``adep`` and ``ades`` after the runway-event correction.

    The same correction step 04b applies (:func:`enrich_flight_list` calls
    :func:`correct_aerodromes` with the same arguments), for a reader that
    needs the airports the flight list will publish before step 04b runs:
    the distance circles of step 04 are drawn around them
    (:attr:`EventConfig.rings_around_flight_list_airports`).
    """
    config = config or EventConfig()
    out = correct_aerodromes(
        _with_track_evidence(flight_list, events, config), events, config, airports
    )
    return out.select("id", "adep", "ades")


def enrich_flight_list(
    flight_list: DataFrame,
    events: DataFrame,
    config: Optional[EventConfig] = None,
    airports: Optional[DataFrame] = None,
) -> DataFrame:
    """Add the milestone, runway, stand and ring columns to a flight list.

    The track rows of :func:`enrich_flight_list_and_flights`, which documents
    the arguments; the flight rows of tracks that hold several flights are
    not computed.
    """
    return _enrich(flight_list, events, config or EventConfig(), airports, with_legs=False)[0]


def enrich_flight_list_and_flights(
    flight_list: DataFrame,
    events: DataFrame,
    config: Optional[EventConfig] = None,
    airports: Optional[DataFrame] = None,
) -> Tuple[DataFrame, Optional[DataFrame]]:
    """The enriched flight list, and the rows of the flights of tracks that hold several.

    The first frame is the flight list with one row per track, as
    :func:`enrich_flight_list` returns it. The second, with
    :attr:`EventConfig.flight_list_one_row_per_flight`, holds one row per
    flight of each track that holds more than one (see
    :mod:`opdi.pipeline.flight_legs`), each with its own airports, first
    and last report times, and the milestone, runway, stand, ring and
    movement columns computed from that flight's events only; ``None``
    without that setting. Both carry the version of the flight list with
    one row per flight, :data:`FLIGHT_LIST_LEGS_VERSION`, when the setting
    is on.
    """
    config = config or EventConfig()
    return _enrich(
        flight_list,
        events,
        config,
        airports,
        with_legs=bool(getattr(config, "flight_list_one_row_per_flight", False)),
    )


def _enrich(
    flight_list: DataFrame,
    events: DataFrame,
    config: Optional[EventConfig] = None,
    airports: Optional[DataFrame] = None,
    with_legs: bool = False,
) -> Tuple[DataFrame, Optional[DataFrame]]:
    """Add the milestone, runway, stand and ring columns to a flight list.

    With ``with_legs``, also the rows of the flights of tracks that hold
    several (see :func:`enrich_flight_list_and_flights`).

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
        airports: ``oa_airports`` (``ident``, ``latitude_deg``,
            ``longitude_deg``, ``elevation_ft``), read by the checks of
            :attr:`EventConfig.aerodrome_correction_checks_track` and
            :attr:`EventConfig.movement_flags_need_track_evidence`. Without
            it those checks treat every distance and height as unknown.
        with_legs: also compute the rows of the flights of tracks that hold
            several flights.

    Returns:
        The flight list plus :data:`ADDED_COLUMNS`, and the flight rows (or
        ``None`` without ``with_legs``).

    Raises:
        ValueError: if the flight list already carries one of the added names,
            which would otherwise produce two columns of the same name and an
            ambiguous reference downstream.
    """
    config = config or EventConfig()

    # Before anything else reads adep/ades. The ring columns filter on them, the
    # movement rules test them, and the milestone columns are chosen relative to
    # them -- correcting afterwards would leave every one of those computed
    # against the aerodrome the evidence disagrees with. The columns of the
    # track evidence (_ev_*) are dropped before the return.
    flight_list = _with_track_evidence(flight_list, events, config)
    if with_legs:
        # The airports before the correction, for a first flight without a
        # take-off and a last flight without a landing: the correction takes
        # the track's earliest take-off and latest landing, which then lie at
        # a stop (dropped with the other _ev_ columns).
        for name in _END_COLUMNS:
            actual = _named(flight_list, name)
            if actual is not None:
                flight_list = flight_list.withColumn(f"{_EV}o_{name}", F.col(actual))
    flight_list = correct_aerodromes(flight_list, events, config, airports)

    # Enriching a flight list that was enriched before is normal: step 04b runs
    # every day, and a day can be processed again.
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
        _RING_TYPE.format(nm=nm) for nm in FLIGHT_LIST_RING_RADII_NM if float(nm) in radii
    ]
    wanted = [_ATOT, _ALDT, _AOBT, _AIBT, _STAND_EXIT, _STAND_ENTRY] + ring_types

    # The ring events name the aerodrome they belong to (``info.apt_icao``) and
    # the sense of the crossing (``info.direction``: "inbound"/"outbound"), but
    # not which *leg* that is -- an event cannot know, since the same aerodrome
    # can be a flight's origin and its destination. The leg comes from the
    # flight list, so a narrow three-column projection of it joins the events
    # before the pivot. It is projected rather than joined in place so that the
    # wide flight list is touched exactly once, by the LEFT join at the end.
    # ``_track_id`` names the row the milestones are for and ``_trk`` the
    # track whose events it reads. They are the same on a track's own row; a
    # flight row of a track that holds several flights reads its track's
    # events between its two division times ``_lo`` and ``_hi``
    # (opdi.pipeline.flight_legs).
    tracks = flight_list.withColumn("_trk", F.col("id"))
    tracks = tracks.withColumn("_lo", _null_ts()).withColumn("_hi", _null_ts())
    rows = tracks
    leg_rows = None
    if with_legs:
        # The corrected flight list and the flight rows are read by both
        # outputs; kept so that the events are not read again for each.
        flight_list = flight_list.cache()
        tracks = flight_list.withColumn("_trk", F.col("id"))
        tracks = tracks.withColumn("_lo", _null_ts()).withColumn("_hi", _null_ts())
        rows = tracks
        leg_rows = _leg_rows(flight_list, flight_legs(flight_stops(events, config))).cache()
        rows = tracks.unionByName(leg_rows.drop(*LEG_COLUMNS))
    keys = rows.select(
        F.col("id").alias("_track_id"),
        "_trk",
        "_lo",
        "_hi",
        F.col("adep").alias("_adep"),
        F.col("ades").alias("_ades"),
    )
    narrowed = (
        events.select(
            F.col(_flight_key(events)).alias("_event_key"),
            "type",
            "event_time",
            "info",
        )
        .filter(F.col("type").isin(wanted))
        .join(
            keys,
            (F.col("_event_key") == F.col("_trk"))
            & event_in_leg(F.col("event_time"), F.col("_lo"), F.col("_hi")),
            "inner",
        )
    )
    if config.layout_nested_stands_both_published:
        # The stand columns name the inner one of two nested stands, as the
        # block times do.
        narrowed = narrowed.filter(_info("encloses_stand").isNull())

    apt = _info("apt_icao")
    direction = _info("direction")

    def ring(nm: int, arrival: bool) -> Column:
        """One ring column. Null throughout if the config does not emit it."""
        type_ = _RING_TYPE.format(nm=nm)
        if type_ not in ring_types:
            return _null_ts()
        if arrival:
            cond = (
                (F.col("type") == type_) & (direction == F.lit("inbound")) & (apt == F.col("_ades"))
            )
            return _last(cond, F.col("event_time"))
        cond = (F.col("type") == type_) & (direction == F.lit("outbound")) & (apt == F.col("_adep"))
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
    # the stand the aircraft arrived on the night before as this flight's. The
    # circle crossing columns apply the same test.
    # Filters what is demonstrably wrong, not what is merely unverifiable. An
    # event naming no aerodrome cannot be checked against the leg, and
    # discarding it would lose coverage to no purpose -- a null here means the
    # detector did not record where, not that it recorded somewhere else.
    at = F.coalesce(_info("osm_airport"), apt)
    _matches = lambda leg: at.isNull() | (at == F.col(leg))  # noqa: E731
    is_dep = lambda t: (F.col("type") == F.lit(t)) & _matches("_adep")  # noqa: E731
    is_arr = lambda t: (F.col("type") == F.lit(t)) & _matches("_ades")  # noqa: E731
    is_ = lambda t: F.col("type") == F.lit(t)  # noqa: E731
    if config.milestones_on_own_leg:
        agg = _aggregate_on_own_leg(narrowed, is_dep, is_arr, ring_types)
    else:
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
            _first(is_dep(_STAND_EXIT), _info("osm_ref")).alias("_stnd_dep_layout"),
            _last(is_arr(_STAND_ENTRY), _info("osm_ref")).alias("_stnd_arr_layout"),
            # The stand the block time itself was read from (``info.stand``), and
            # its airport. Block times written without it give NULL here.
            _first(is_(_AOBT), _info("stand")).alias("_aobt_stand"),
            _first(is_(_AOBT), _info("apt_icao")).alias("_aobt_apt"),
            _last(is_(_AIBT), _info("stand")).alias("_aibt_stand"),
            _last(is_(_AIBT), _info("apt_icao")).alias("_aibt_apt"),
            F.first("_adep").alias("_k_adep"),
            F.first("_ades").alias("_k_ades"),
            *[ring(nm, arrival=True).alias(f"C{nm}_ARR") for nm in FLIGHT_LIST_RING_RADII_NM],
            *[ring(nm, arrival=False).alias(f"C{nm}_DEP") for nm in FLIGHT_LIST_RING_RADII_NM],
        )

    # STND_DEP and STND_ARR are the stands of the AOBT and AIBT columns when
    # those events name a stand at the flight's own airport, so the time and
    # the stand of a row come from the same stand passage. Otherwise they are
    # the first stand exit at ADEP and the last stand entry at ADES.
    def _own_stand(stand: str, airport: str, leg: str, layout: str) -> Column:
        at_leg = F.col(airport).isNull() | (F.col(airport) == F.col(leg))
        if config.milestones_on_own_leg:
            # The block time of a row without that airport (see
            # _aggregate_on_own_leg) keeps the stand it was read at.
            at_leg = at_leg | F.col(leg).isNull()
        return F.when(F.col(stand).isNotNull() & at_leg, F.col(stand)).otherwise(F.col(layout))

    agg = agg.withColumn(
        "STND_DEP", _own_stand("_aobt_stand", "_aobt_apt", "_k_adep", "_stnd_dep_layout")
    ).withColumn("STND_ARR", _own_stand("_aibt_stand", "_aibt_apt", "_k_ades", "_stnd_arr_layout"))

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
    if with_legs:
        # Read twice below, by the track rows and by the flight rows.
        agg = agg.cache()
    out = add_movement_columns(
        flight_list.join(agg, flight_list["id"] == agg["_track_id"], "left")
        .drop("_track_id")
        .select(*flight_list.columns, *milestones),
        config,
        airports,
    )
    out = _without_internal(out)
    if not with_legs:
        return out, None
    # The movement flags of a flight row compare it with the other rows of
    # its aircraft as the published flight list holds them: the flight rows
    # and the rows of tracks with one flight.
    single = tracks.join(
        leg_rows.select(F.col("TRACK_ID").alias("_l_track")).distinct(),
        F.col("id") == F.col("_l_track"),
        "left_anti",
    ).withColumn("_is_leg", F.lit(False))
    for name, dtype in zip(LEG_COLUMNS, ("string", "int", "int", "timestamp", "timestamp")):
        single = single.withColumn(name, F.lit(None).cast(dtype))
    flights = single.unionByName(
        leg_rows.withColumn("_is_leg", F.lit(True)), allowMissingColumns=False
    )
    flights = add_movement_columns(
        flights.join(agg, flights["id"] == agg["_track_id"], "left")
        .drop("_track_id")
        .select(*flight_list.columns, *milestones, *LEG_COLUMNS, "_is_leg"),
        config,
        airports,
    )
    flights = _without_internal(flights.filter(F.col("_is_leg")).drop("_is_leg"))
    return _with_legs_version(out), _with_legs_version(flights)


def _without_internal(frame: DataFrame) -> DataFrame:
    """``frame`` without the columns of the track evidence (``_ev_*``)."""
    internal = [c for c in frame.columns if c.startswith(_EV)]
    return frame.drop(*internal) if internal else frame


def _with_legs_version(frame: DataFrame) -> DataFrame:
    """The flight-list version of a list with one row per flight.

    A row of the flight list of release v2.0.0 (:data:`FLIGHT_LIST_VERSION`)
    gets :data:`FLIGHT_LIST_LEGS_VERSION`; any other version is kept.
    """
    name = _named(frame, "VERSION")
    if name is None:
        return frame
    current = F.col(name) == F.lit(FLIGHT_LIST_VERSION)
    return frame.withColumn(
        name, F.when(current, F.lit(FLIGHT_LIST_LEGS_VERSION)).otherwise(F.col(name))
    )


#: The airport columns of each end of a flight-list row.
_END_COLUMNS = ("ADEP", "ADEP_P", "ADEP_SOURCE", "ADES", "ADES_P", "ADES_SOURCE")


def _leg_rows(flight_list: DataFrame, legs: DataFrame) -> DataFrame:
    """One row per flight of the tracks in ``legs``, built from the track's row.

    The rules are those of :mod:`opdi.pipeline.flight_legs`: the identifier
    is ``<track_id>-L<k>``; the first flight keeps the track's departure
    airport (with its alternatives and source) and first report time, the
    last flight its destination airport and last report time; an airport at
    a stop is the airport of the landing or the take-off there, with the
    source ``runway_event`` and no alternatives, and the first and last
    report times at a stop are the division time. Every other column is the
    track's.

    Returns:
        The columns of ``flight_list``, then ``_trk``, ``_lo``, ``_hi`` (the
        track and the division times the flight's events are read between)
        and :data:`~opdi.pipeline.flight_legs.LEG_COLUMNS`.
    """
    leg = legs.select(
        F.col("track_id").alias("_l_track"),
        "LEG",
        "LEGS",
        "LEG_START",
        "LEG_END",
        "leg_adep",
        "leg_ades",
        "leg_takes_off",
        "leg_lands",
    )
    j = flight_list.join(leg, F.col("id") == F.col("_l_track"), "inner")
    first = F.col("LEG") == F.lit(1)
    last = F.col("LEG") == F.col("LEGS")
    from_stop = F.lit("runway_event")
    # Per column: the end of the track it describes (first or last flight)
    # and the value a flight takes at a stop.
    at_stops = {
        "ADEP": (first, F.col("leg_adep")),
        "ADEP_P": (first, F.lit(None)),
        "ADEP_SOURCE": (first, F.when(F.col("leg_adep").isNotNull(), from_stop)),
        "FIRST_SEEN": (first, F.col("LEG_START")),
        "ADES": (last, F.col("leg_ades")),
        "ADES_P": (last, F.lit(None)),
        "ADES_SOURCE": (last, F.when(F.col("leg_ades").isNotNull(), from_stop)),
        "LAST_SEEN": (last, F.col("LEG_END")),
    }
    types = dict(flight_list.dtypes)
    cols = []
    for name in flight_list.columns:
        upper = name.upper()
        if upper == "ID":
            cols.append(leg_id(F.col(name), F.col("LEG")).alias(name))
        elif upper in at_stops:
            own_end, value = at_stops[upper]
            own = F.col(name)
            before = f"{_EV}o_{upper}"
            if before in flight_list.columns:
                # The first flight's departure is the corrected one only when
                # it took off within the track; the last flight's destination
                # only when it landed within it. Otherwise it is the airport
                # found from the reports, unless that is the airport of the
                # stop itself: a flight that arrives at an airport did not
                # come from it, and one that leaves it did not go to it.
                side = upper[:4]
                seen = F.col("leg_takes_off") if side == "ADEP" else F.col("leg_lands")
                stop = F.col("leg_ades") if side == "ADEP" else F.col("leg_adep")
                at_stop = F.col(f"{_EV}o_{side}").eqNullSafe(stop)
                if upper.endswith("_SOURCE"):
                    unknown = F.lit("undetermined")
                elif upper.endswith("_P"):
                    unknown = F.col(before)
                else:
                    unknown = F.lit(None)
                found = F.when(at_stop, unknown).otherwise(F.col(before))
                own = F.when(seen, own).otherwise(found)
            cols.append(F.when(own_end, own).otherwise(value).cast(types[name]).alias(name))
        else:
            cols.append(F.col(name))
    return j.select(
        *cols,
        F.col("id").alias("_trk"),
        F.col("LEG_START").alias("_lo"),
        F.col("LEG_END").alias("_hi"),
        F.col("id").alias("TRACK_ID"),
        F.col("LEG").cast("int").alias("LEG"),
        F.col("LEGS").cast("int").alias("LEGS"),
        "LEG_START",
        "LEG_END",
    )


def _aggregate_on_own_leg(narrowed: DataFrame, is_dep, is_arr, ring_types) -> DataFrame:
    """The milestone columns, each taken on the right side of the take-off or landing.

    The aggregation of :func:`enrich_flight_list` with
    :attr:`EventConfig.milestones_on_own_leg`: the take-off time ``ATOT``
    and landing time ``ALDT`` of the row are found first (a window over each
    flight's events, no second pass over the data), then

    * ``AOBT``: the latest off-block at ADEP not after ``ATOT``;
    * ``AIBT``: the earliest in-block at ADES not before ``ALDT``;
    * the stands: from that same block event, else the last stand exit at
      ADEP not after ``ATOT`` and the first stand entry at ADES not before
      ``ALDT``;
    * ``C{NM}_DEP``: the first outbound crossing at ADEP not before
      ``ATOT``; ``C{NM}_ARR``: the last inbound crossing at ADES not after
      ``ALDT``.

    Without a take-off (landing) time, the departure (arrival) columns take
    the earliest (latest) matching event at the airport, as before.
    """
    t = F.col("event_time")
    per_flight = Window.partitionBy("_track_id")
    narrowed = narrowed.withColumn(
        "_atot_t", F.min(F.when(is_dep(_ATOT), t)).over(per_flight)
    ).withColumn("_aldt_t", F.max(F.when(is_arr(_ALDT), t)).over(per_flight))
    atot, aldt = F.col("_atot_t"), F.col("_aldt_t")
    known_atot = F.max(atot).isNotNull()
    known_aldt = F.max(aldt).isNotNull()

    def before_takeoff(cond: Column, value: Column) -> Column:
        """Latest matching event not after ATOT, or the earliest without ATOT."""
        return F.when(known_atot, _last(cond & (t <= atot), value)).otherwise(_first(cond, value))

    def after_landing(cond: Column, value: Column) -> Column:
        """Earliest matching event not before ALDT, or the latest without ALDT."""
        return F.when(known_aldt, _first(cond & (t >= aldt), value)).otherwise(_last(cond, value))

    apt = _info("apt_icao")
    direction = _info("direction")

    def ring(nm: int, arrival: bool) -> Column:
        type_ = _RING_TYPE.format(nm=nm)
        if type_ not in ring_types:
            return _null_ts()
        if arrival:
            cond = (
                (F.col("type") == type_)
                & (direction == F.lit("inbound"))
                & (apt == F.col("_ades"))
                & (aldt.isNull() | (t <= aldt))
            )
            return _last(cond, t)
        cond = (
            (F.col("type") == type_)
            & (direction == F.lit("outbound"))
            & (apt == F.col("_adep"))
            & (atot.isNull() | (t >= atot))
        )
        return _first(cond, t)

    # A block time read at a named stand stays on a row whose airport at that
    # end is unknown: the stand passage it was read from is its evidence, and
    # the row has no airport to contradict it. Taken on the side of the
    # take-off or landing as above, so it is of the row's own leg when the
    # track holds several.
    stand = _info("stand").isNotNull()
    aobt = is_dep(_AOBT) | ((F.col("type") == F.lit(_AOBT)) & F.col("_adep").isNull() & stand)
    aibt = is_arr(_AIBT) | ((F.col("type") == F.lit(_AIBT)) & F.col("_ades").isNull() & stand)
    return narrowed.groupBy("_track_id").agg(
        _first(is_dep(_ATOT), t).alias("ATOT"),
        _last(is_arr(_ALDT), t).alias("ALDT"),
        before_takeoff(aobt, t).alias("AOBT"),
        after_landing(aibt, t).alias("AIBT"),
        _first(is_dep(_ATOT), _runway()).alias("RWY_DEP"),
        _first(is_dep(_ATOT), _runway_bearing()).alias("RWY_DEP_BEARING_DEG"),
        _last(is_arr(_ALDT), _runway()).alias("RWY_ARR"),
        _last(is_arr(_ALDT), _runway_bearing()).alias("RWY_ARR_BEARING_DEG"),
        before_takeoff(is_dep(_STAND_EXIT), _info("osm_ref")).alias("_stnd_dep_layout"),
        after_landing(is_arr(_STAND_ENTRY), _info("osm_ref")).alias("_stnd_arr_layout"),
        before_takeoff(aobt, _info("stand")).alias("_aobt_stand"),
        before_takeoff(aobt, apt).alias("_aobt_apt"),
        after_landing(aibt, _info("stand")).alias("_aibt_stand"),
        after_landing(aibt, apt).alias("_aibt_apt"),
        F.first("_adep").alias("_k_adep"),
        F.first("_ades").alias("_k_ades"),
        *[ring(nm, arrival=True).alias(f"C{nm}_ARR") for nm in FLIGHT_LIST_RING_RADII_NM],
        *[ring(nm, arrival=False).alias(f"C{nm}_DEP") for nm in FLIGHT_LIST_RING_RADII_NM],
    )
