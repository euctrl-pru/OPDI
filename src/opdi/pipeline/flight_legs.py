"""The flights of a track: one flight-list row for each flight a track holds.

A **track** is one continuous series of position reports of one aircraft
under one call sign. Most tracks hold one flight. Some hold more: an
aircraft that flies Zurich - Hamburg - Zurich under one call sign, a
turnaround without a change of call sign, or a training aircraft that lands
and takes off again several times. This module divides such a track into its
**flights** (also called legs), so that the flight list has one row for each.

**What separates two flights.** A landing of the track followed by a
take-off of the same track, with the aircraft on the ground in between for
at least :attr:`~opdi.config.EventConfig.flight_stop_min_ground_s` seconds
(the time from the landing time ``ALDT`` to the next take-off time
``ATOT``). Such a pair is a **stop**. The two airports may be the same: an
out-and-back, or a training circuit that ends with a full stop, makes a stop
at the airport it left from. What does not separate two flights:

* a touch-and-go, a stop-and-go or a landing roll followed by an immediate
  take-off: the take-off follows the landing within less than the minimum
  ground time;
* a go-around: no landing is detected, or the climb away follows the low
  pass within less than the minimum ground time;
* a rejected take-off: no take-off is detected.

The landings and take-offs are the ``ATOT`` and ``ALDT`` events of step 04,
which keeps one of each per track and airport.

**Where one flight ends and the next begins** (the *division time*). The
reports and events up to and including the in-block time (``AIBT``) after
the landing belong to the arriving flight, and those after it to the
departing flight: the taxi-in is part of the first flight, the time at the
stand and the taxi-out part of the next. When no in-block time was detected
between the landing and the take-off, the division is at the midpoint
between the two, or one second before the off-block time (``AOBT``) when
that comes earlier, so that an off-block always belongs to the flight it
starts.

**The flights of a track with stops.** A track with ``n`` stops holds
``n + 1`` flights, numbered from 1 in time order. Flight ``k`` holds the
events after the division time of stop ``k - 1`` up to and including that of
stop ``k``; the first flight has no lower limit and the last no upper limit,
so every event of the track belongs to exactly one flight. Its departure
airport is the take-off airport of stop ``k - 1`` (for the first flight, the
track's departure airport), and its destination the landing airport of stop
``k`` (for the last flight, the track's destination). A track that starts in
the air has a first flight without a take-off; one that ends in the air a
last flight without a landing. A track with no stop (and so every track
without both a landing and a later take-off, such as a track seen only on
the ground) keeps its single row as it is.

**Identifiers.** The flights of a track with stops are named
``<track_id>-L<k>`` (``...-L1``, ``...-L2``, ...). A track with one flight
keeps ``track_id`` as its flight identifier. The flight rows are written to
the working table :data:`LEGS_TABLE` beside the track's own row in
``opdi_flight_list``, which stays as it is so that the steps that read the
flight list by track are not changed; the published flight list takes the
flight rows in place of the track's row (:func:`with_flight_rows`).

Native Spark only: one window over the take-offs and landings of each track,
and one join with the block times of the few tracks that have a stop.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

if TYPE_CHECKING:  # pragma: no cover
    from opdi.config import EventConfig

__all__ = [
    "LEGS_TABLE",
    "LEG_COLUMNS",
    "flight_stops",
    "flight_legs",
    "leg_id",
    "with_flight_rows",
    "event_in_leg",
]

#: The working table of the flight rows of tracks that hold several flights:
#: the columns of ``opdi_flight_list`` plus :data:`LEG_COLUMNS`, partitioned
#: by ``DOF`` like it. Written by step 04b, read by step 09.
LEGS_TABLE = "opdi_flight_legs"

#: The columns a flight row has beside those of the flight list:
#: ``TRACK_ID`` (the track the flight belongs to), ``LEG`` (its number in
#: the track, from 1), ``LEGS`` (the number of flights of the track), and
#: ``LEG_START`` / ``LEG_END``, the division times before and after it
#: (NULL for the first and the last flight).
LEG_COLUMNS = ("TRACK_ID", "LEG", "LEGS", "LEG_START", "LEG_END")

_KEYS = ("flight_id", "track_id")


def _key(events: DataFrame) -> str:
    for name in _KEYS:
        if name in events.columns:
            return name
    raise ValueError(
        f"the events frame has no flight key: expected one of {', '.join(_KEYS)}, "
        f"found {sorted(events.columns)}."
    )


def _info(field: str) -> Column:
    return F.get_json_object(F.col("info"), f"$.{field}")


def leg_id(track_id: Column, leg: Column) -> Column:
    """The flight identifier of flight ``leg`` of a track that holds several."""
    return F.concat(track_id, F.lit("-L"), leg.cast("string"))


def flight_stops(events: DataFrame, config: EventConfig) -> DataFrame:
    """The stops of each track: a landing followed by a take-off after a ground stop.

    Args:
        events: Events with ``type``, ``event_time``, ``info`` and the track
            key (``flight_id`` as stored, or ``track_id`` as in step 04).
        config: Gives :attr:`~opdi.config.EventConfig.flight_stop_min_ground_s`.

    Returns:
        One row per stop: ``track_id``, ``stop`` (its number in the track,
        from 1), ``landing_time`` and ``landing_airport`` (of the ``ALDT``),
        ``take_off_time`` and ``take_off_airport`` (of the next ``ATOT``),
        ``division_time`` (see the module's description), and the track's
        ``first_take_off_time`` and ``last_landing_time``. Tracks without a
        stop have no row.
    """
    key = _key(events)
    t = F.col("event_time")
    runway = events.filter(F.col("type").isin("ATOT", "ALDT") & t.isNotNull()).select(
        F.col(key).alias("track_id"),
        "type",
        t.alias("t"),
        _info("apt_icao").alias("apt"),
    )
    # In time order; at the same instant the landing ("ALDT") sorts first.
    per_track = Window.partitionBy("track_id")
    order = per_track.orderBy("t", "type")
    minimum = float(config.flight_stop_min_ground_s)
    stops = (
        runway.withColumn(
            "first_take_off_time",
            F.min(F.when(F.col("type") == "ATOT", F.col("t"))).over(per_track),
        )
        .withColumn(
            "last_landing_time", F.max(F.when(F.col("type") == "ALDT", F.col("t"))).over(per_track)
        )
        .withColumn("_prev_type", F.lag("type").over(order))
        .withColumn("_prev_t", F.lag("t").over(order))
        .withColumn("_prev_apt", F.lag("apt").over(order))
        .filter(
            (F.col("type") == "ATOT")
            & (F.col("_prev_type") == "ALDT")
            & ((F.col("t").cast("double") - F.col("_prev_t").cast("double")) >= F.lit(minimum))
        )
        .select(
            "track_id",
            F.col("_prev_t").alias("landing_time"),
            F.col("_prev_apt").alias("landing_airport"),
            F.col("t").alias("take_off_time"),
            F.col("apt").alias("take_off_airport"),
            "first_take_off_time",
            "last_landing_time",
        )
    )

    # The block times between the landing and the take-off: the first
    # in-block and the last off-block. Only the tracks with a stop are read.
    blocks = (
        events.filter(F.col("type").isin("AIBT", "AOBT") & t.isNotNull())
        .select(F.col(key).alias("_b_track"), F.col("type").alias("_b_type"), t.alias("_b_t"))
        .join(stops.select(F.col("track_id").alias("_b_track")).distinct(), "_b_track", "left_semi")
    )
    between = (
        stops.join(
            blocks,
            (F.col("track_id") == F.col("_b_track"))
            & (F.col("_b_t") > F.col("landing_time"))
            & (F.col("_b_t") < F.col("take_off_time")),
            "left",
        )
        .groupBy(
            "track_id",
            "landing_time",
            "landing_airport",
            "take_off_time",
            "take_off_airport",
            "first_take_off_time",
            "last_landing_time",
        )
        .agg(
            F.min(F.when(F.col("_b_type") == "AIBT", F.col("_b_t"))).alias("_in_block"),
            F.max(F.when(F.col("_b_type") == "AOBT", F.col("_b_t"))).alias("_off_block"),
        )
    )
    landing_s = F.col("landing_time").cast("double")
    midpoint = (landing_s + F.col("take_off_time").cast("double")) / F.lit(2.0)
    before_off_block = F.col("_off_block").cast("double") - F.lit(1.0)
    without_in_block = F.when(
        F.col("_off_block").isNotNull() & (before_off_block < midpoint), before_off_block
    ).otherwise(midpoint)
    # Never at or before the landing itself, which belongs to the arriving flight.
    division_s = F.greatest(
        F.coalesce(F.col("_in_block").cast("double"), without_in_block), landing_s
    )
    return between.select(
        "track_id",
        F.row_number().over(Window.partitionBy("track_id").orderBy("landing_time")).alias("stop"),
        "landing_time",
        "landing_airport",
        "take_off_time",
        "take_off_airport",
        F.timestamp_seconds(division_s).alias("division_time"),
        "first_take_off_time",
        "last_landing_time",
    )


def flight_legs(stops: DataFrame) -> DataFrame:
    """The flights of the tracks that have stops.

    Args:
        stops: The rows of :func:`flight_stops`.

    Returns:
        One row per flight of a track with at least one stop: ``track_id``,
        ``LEG`` (from 1), ``LEGS`` (the number of flights of the track),
        ``LEG_START`` and ``LEG_END`` (the division times before and after
        the flight; NULL for the first and the last), ``leg_adep`` (the
        take-off airport of the stop before; NULL for the first flight),
        ``leg_ades`` (the landing airport of the stop after; NULL for the
        last flight), ``leg_takes_off`` (False for a first flight of a
        track that starts in the air) and ``leg_lands`` (False for a last
        flight of a track that ends in the air).
    """
    per_track = Window.partitionBy("track_id")
    order = per_track.orderBy("stop")
    s = stops.withColumn("_n", F.count(F.lit(1)).over(per_track))
    first = s.filter(F.col("stop") == 1).select(
        "track_id",
        F.lit(1).alias("LEG"),
        (F.col("_n") + 1).alias("LEGS"),
        F.lit(None).cast("timestamp").alias("LEG_START"),
        F.col("division_time").alias("LEG_END"),
        F.lit(None).cast("string").alias("leg_adep"),
        F.col("landing_airport").alias("leg_ades"),
        # Whether the first flight took off within the track (else the
        # track starts in the air).
        F.coalesce(F.col("first_take_off_time") < F.col("landing_time"), F.lit(False)).alias(
            "leg_takes_off"
        ),
        F.lit(True).alias("leg_lands"),
    )
    later = s.select(
        "track_id",
        (F.col("stop") + 1).alias("LEG"),
        (F.col("_n") + 1).alias("LEGS"),
        F.col("division_time").alias("LEG_START"),
        F.lead("division_time").over(order).alias("LEG_END"),
        F.col("take_off_airport").alias("leg_adep"),
        F.lead("landing_airport").over(order).alias("leg_ades"),
        F.lit(True).alias("leg_takes_off"),
        # Whether the last flight landed within the track (else the track
        # ends in the air).
        F.when(F.col("stop") < F.col("_n"), F.lit(True))
        .otherwise(F.coalesce(F.col("last_landing_time") > F.col("take_off_time"), F.lit(False)))
        .alias("leg_lands"),
    )
    return first.unionByName(later)


def event_in_leg(event_time: Column, start: Column, end: Column) -> Column:
    """Whether an event at ``event_time`` belongs to the flight between ``start`` and ``end``.

    After ``start`` and up to and including ``end``; a NULL limit is no
    limit, so a row without division times (a track with one flight) takes
    every event. An event without a time belongs to the first flight.
    """
    after = start.isNull() | (event_time.isNotNull() & (event_time > start))
    before = end.isNull() | event_time.isNull() | (event_time <= end)
    return after & before


def with_flight_rows(flight_list: DataFrame, legs: Optional[DataFrame]) -> DataFrame:
    """The flight list with one row per flight: the flight rows in place of their track's row.

    Args:
        flight_list: Rows of ``opdi_flight_list`` (one per track).
        legs: Rows of :data:`LEGS_TABLE` for the same days, or None.

    A flight row is used only when its version equals that of its track's
    row, which step 04b writes with it; rows a run with other settings left
    behind are ignored.

    Returns:
        The rows of ``flight_list`` whose track has no flight rows, and the
        flight rows, with the columns of ``flight_list`` and
        :data:`LEG_COLUMNS` (NULL on the rows of tracks with one flight;
        ``TRACK_ID`` is then the row's ``ID``, ``LEG`` and ``LEGS`` are 1).
        Without ``legs``, ``flight_list`` with those columns added.
    """
    single = (
        flight_list.withColumn("TRACK_ID", F.col("ID"))
        .withColumn("LEG", F.lit(1))
        .withColumn("LEGS", F.lit(1))
        .withColumn("LEG_START", F.lit(None).cast("timestamp"))
        .withColumn("LEG_END", F.lit(None).cast("timestamp"))
    )
    if legs is None:
        return single
    # Only flight rows written with the track's row: step 04b gives both the
    # same version, so rows left from a run with other settings are not used.
    current = flight_list.select(
        F.col("ID").alias("_l_track"), F.col("VERSION").alias("_l_version")
    )
    legs = legs.join(
        current,
        (F.col("TRACK_ID") == F.col("_l_track")) & (F.col("VERSION") == F.col("_l_version")),
        "left_semi",
    )
    tracks = legs.select(F.col("TRACK_ID").alias("_l_track")).distinct()
    single = single.join(tracks, F.col("ID") == F.col("_l_track"), "left_anti")
    return single.unionByName(legs.select(*single.columns))
