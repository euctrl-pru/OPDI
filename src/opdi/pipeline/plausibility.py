"""Plausibility checks: flight events that the aircraft's own track contradicts.

An event says that something happened at a time. An off-block time
(``AOBT``) says the aircraft started to move from its stand; an entry into a
taxiway says it rolled onto that taxiway. The position reports of the same
track say what the aircraft was doing at that time: its height above the
airport, its ground speed, whether its transponder reported it on the ground.
Where the two clearly disagree, the event cannot be true, and step 04 does
not publish it.

**Only clear evidence removes an event.** Each check fires only when every
fact it needs is known and contradicts the event. An event with no position
report near its time, or at an airport whose elevation is unknown, is kept.

**The checks** (:data:`RULES`), in the order they are applied; an event is
removed by the first one that fires:

* ``missing_time``: the event has no time.
* ``airborne_ground_event``: an off-block or in-block time, or an entry or
  exit of a part of the airport layout (stand, apron, taxiway, ...), while
  the aircraft flies: more than 1,500 ft above the airport (more than
  15,000 ft barometric altitude when the airport or its elevation is
  unknown), faster than 80 kt, and no position report within 60 s flagged
  on the ground. All three must hold.
* ``runway_time_too_high``: a take-off or landing time (``ATOT``, ``ALDT``,
  and ``airborne``, ``touchdown`` under the other naming) more than 1,500 ft
  above the ground at its airport. The altitude is the event's own when it
  has one, else interpolated between the nearest reports before and after
  it within 10 s that have an altitude and a position. The ground level is
  the same track's barometric altitude on the ground at that airport (the
  median of its reports flagged on the ground within 30 minutes and 5 NM),
  which cancels the error of barometric altitude on a day of low or high
  pressure (about 27 ft per hPa); without such reports it is the airport's
  elevation and the limit is raised by 1,000 ft for that error. A time
  close to the ground, though not on it, is kept; an unknown altitude or
  airport keeps the event.
* ``block_after_takeoff``: an off-block time that is not earlier than the
  take-off of its own leg.
* ``block_before_landing``: an in-block time that is not later than the
  landing of its own leg.

  The block time detector (:func:`opdi.pipeline.ground.block_events`)
  writes that take-off or landing into the block time's ``info``
  (``take_off`` or ``landing``, with the detector that found it in
  ``take_off_method`` or ``landing_method``). It reads every take-off and
  landing of the track, before step 04 keeps one ``ATOT`` and one ``ALDT``
  per track, so a track with two legs is compared leg by leg. A block time
  without such a time (no take-off or landing found in its leg, or the
  runway events not computed in the same call) is not judged by these two
  checks.

The other runway milestones (line-up, take-off roll, runway vacated, runway
crossings, ``landing``) are not checked against the track height: their
detector already tests the height on the runway, so a report that
contradicts them points at an altitude error rather than a wrong event.

The thresholds come from a study of one worldwide day (2026-06-14) against
the times airport operators report for about ninety European airports; each
is set where it removes none of the events those records confirm within
60 s, and the website gives the measured numbers.
See ``https://www.opdi.aero/content/methods/plausibility-checks.html``.

Removed events are kept, with the check that removed them and the facts it
used, in the working table :data:`REJECTED_TABLE`; they are never published.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from opdi.pipeline.ground import LANDING_TYPES, TAKE_OFF_TYPES
from opdi.utils.geometry import haversine_nm

if TYPE_CHECKING:  # pragma: no cover - avoids an import cycle
    from opdi.config import EventConfig

__all__ = [
    "ARRIVAL_SIDE",
    "DEPARTURE_SIDE",
    "GROUND_EVENT_TYPES",
    "REJECTED_TABLE",
    "RULES",
    "Rule",
    "check_events",
    "event_context",
    "is_ground_event",
    "label_events",
    "split_labelled",
]

#: The working table of the removed events (event columns, ``reject_rule``,
#: ``reject_detail``), one folder per day (``dof``).
REJECTED_TABLE = "opdi_flight_events_rejected"

#: Storage is SI; these are the conversions ``events.py`` uses.
FT_PER_M = 3.28084
KT_PER_MPS = 1.94384
FTMIN_PER_MPS = 196.850394

#: Events that happen at the departure airport. Their height is measured above
#: the flight's departure airport unless the event names an airport itself.
DEPARTURE_SIDE = ("AOBT", "off-block", "line-up", "take-off-roll", "ATOT", "airborne", "take-off")
#: Events that happen at the destination airport.
ARRIVAL_SIDE = (
    "AIBT",
    "on-block",
    "ALDT",
    "touchdown",
    "landing",
    "runway-vacated",
    "go-around",
)

#: Off-block and in-block times, under both namings.
OFF_BLOCK_TYPES = ("AOBT", "off-block")
ON_BLOCK_TYPES = ("AIBT", "on-block")

#: Take-off and landing times, checked against :attr:`plausibility_max_runway_time_height_ft`.
RUNWAY_TIME_TYPES = TAKE_OFF_TYPES + LANDING_TYPES

#: Event types checked against the track height, besides every
#: ``entry-<part>`` and ``exit-<part>`` of the airport layout.
GROUND_EVENT_TYPES = OFF_BLOCK_TYPES + ON_BLOCK_TYPES


def is_ground_event(type_col: Column) -> Column:
    """True for an off-block or in-block time and for every layout entry or exit."""
    return type_col.isin(*GROUND_EVENT_TYPES) | type_col.rlike(r"^(entry|exit)-")


@dataclass(frozen=True)
class Rule:
    """One plausibility check.

    Attributes:
        name: The value of ``reject_rule`` for an event this check removes.
        applies_to: The event types it reads. ``("*",)`` means every type;
            ``"entry-*"`` and ``"exit-*"`` mean every layout entry and exit.
        description: What the check removes, in one sentence.
    """

    name: str
    applies_to: Tuple[str, ...]
    description: str


#: The checks, in the order they are applied.
RULES: Tuple[Rule, ...] = (
    Rule("missing_time", ("*",), "The event has no time."),
    Rule(
        "airborne_ground_event",
        GROUND_EVENT_TYPES + ("entry-*", "exit-*"),
        "A ground event while the aircraft flies: high above the airport, fast, "
        "and no report near it flagged on the ground.",
    ),
    Rule(
        "runway_time_too_high",
        RUNWAY_TIME_TYPES,
        "A take-off or landing time high above its airport.",
    ),
    Rule(
        "block_after_takeoff",
        OFF_BLOCK_TYPES,
        "An off-block time not earlier than the take-off of its own leg.",
    ),
    Rule(
        "block_before_landing",
        ON_BLOCK_TYPES,
        "An in-block time not later than the landing of its own leg.",
    ),
)


# ---------------------------------------------------------------------------
# What the aircraft was doing at the event's time
# ---------------------------------------------------------------------------


def event_context(
    events: DataFrame,
    tracks: DataFrame,
    airports: Optional[DataFrame],
    window_s: float = 60.0,
    interpolation_window_s: Optional[float] = None,
) -> DataFrame:
    """Add to each event the state of the aircraft at the event's time.

    Args:
        events: one row per event with ``track_id``, ``type`` and
            ``event_time``, and optionally ``apt_icao`` (the airport the
            event names), ``adep`` and ``ades`` (the flight's departure and
            destination airports). Other columns are kept.
        tracks: position reports with ``track_id``, ``event_time``, ``lat``,
            ``lon``, ``baro_altitude_c`` (m), ``velocity`` (m/s), ``vert_rate``
            (m/s) and optionally ``on_ground``.
        airports: ``ident``, ``latitude_deg``, ``longitude_deg``,
            ``elevation_ft`` (the OurAirports table), or None when unknown.
        window_s: only reports within this many seconds of the event are read.
        interpolation_window_s: when given, also add
            ``ctx_interpolated_altitude_ft`` (see below); at most ``window_s``.

    Returns:
        ``events`` with these columns added, each NULL when unknown:

        * ``ctx_gap_s`` -- seconds to the nearest report of the track;
        * ``ctx_altitude_ft`` -- barometric altitude of the nearest report that
          has one;
        * ``ctx_height_ft`` -- that altitude minus the elevation of the event's
          airport (``ctx_airport``);
        * ``ctx_speed_kt``, ``ctx_vertical_rate_ft_min`` -- ground speed and
          vertical rate of the nearest report that has them;
        * ``ctx_on_ground`` -- share of the reports in the window flagged on the
          ground (0 to 1);
        * ``ctx_lat``, ``ctx_lon`` -- position of the nearest report with one;
        * ``ctx_airport`` -- the airport the event belongs to: the one it names,
          else the departure airport for a departure-side type
          (:data:`DEPARTURE_SIDE`), the destination for an arrival-side type
          (:data:`ARRIVAL_SIDE`), and for every other type the nearer of the
          two to the aircraft;
        * ``ctx_airport_elevation_ft``, ``ctx_airport_distance_nm``;
        * with ``interpolation_window_s``: ``ctx_interpolated_altitude_ft``,
          the barometric altitude at the event's time, interpolated in time
          between the nearest report before it and the nearest report after
          it within ``interpolation_window_s`` that have both an altitude
          and a position (a report at the event's own time gives its
          altitude). NULL without such a report on each side.

    Events of one track at the same time share their context. When two
    reports are equally near, the earlier one is used.
    """
    w = float(window_s)
    ev = events
    for c in ("apt_icao", "adep", "ades"):
        if c not in ev.columns:
            ev = ev.withColumn(c, F.lit(None).cast("string"))

    # Reports are joined through time buckets one window wide: an event meets
    # the reports of its own bucket and of the two next to it, which holds
    # every report within the window and few others.
    probe = (
        ev.filter(F.col("event_time").isNotNull())
        .select(
            F.col("track_id").alias("_etrack"),
            F.col("event_time").alias("_etime"),
        )
        .distinct()
        .withColumn("_et", F.col("_etime").cast("double"))
        .withColumn("_eb0", F.floor(F.col("_et") / F.lit(w)))
    )
    probe = probe.withColumn(
        "_eb", F.explode(F.array(F.col("_eb0") - 1, F.col("_eb0"), F.col("_eb0") + 1))
    ).drop("_eb0")

    on_ground = (
        F.col("on_ground").cast("double")
        if "on_ground" in tracks.columns
        else F.lit(None).cast("double")
    )
    rep = tracks.select(
        F.col("track_id").alias("_rtrack"),
        F.col("event_time").cast("double").alias("_rt"),
        F.col("lat").alias("_lat"),
        F.col("lon").alias("_lon"),
        F.col("baro_altitude_c").alias("_alt_m"),
        F.col("velocity").alias("_gs_ms"),
        F.col("vert_rate").alias("_vr_ms"),
        on_ground.alias("_gnd"),
    ).withColumn("_rb", F.floor(F.col("_rt") / F.lit(w)))

    pairs = probe.join(
        rep, (probe["_etrack"] == rep["_rtrack"]) & (probe["_eb"] == rep["_rb"]), "inner"
    )
    pairs = pairs.withColumn("_dt", F.col("_rt") - F.col("_et")).filter(
        F.abs(F.col("_dt")) <= F.lit(w)
    )
    # Nearest first; a report after the event loses a tie to one before it.
    pairs = pairs.withColumn(
        "_key", F.abs(F.col("_dt")) + F.when(F.col("_dt") > 0, F.lit(1e-3)).otherwise(F.lit(0.0))
    )

    def nearest(value, *needed):
        known = F.lit(True)
        for c in needed:
            known = known & F.col(c).isNotNull()
        return F.min_by(value, F.when(known, F.col("_key")))

    aggregates = [
        F.min(F.abs(F.col("_dt"))).alias("ctx_gap_s"),
        nearest(F.col("_alt_m"), "_alt_m").alias("_alt_m"),
        nearest(F.col("_gs_ms"), "_gs_ms").alias("_gs_ms"),
        nearest(F.col("_vr_ms"), "_vr_ms").alias("_vr_ms"),
        nearest(F.struct(F.col("_lat"), F.col("_lon")), "_lat", "_lon").alias("_pos"),
        F.avg(F.col("_gnd")).alias("ctx_on_ground"),
    ]
    if interpolation_window_s is not None:
        iw = F.lit(min(float(interpolation_window_s), w))
        usable = F.col("_alt_m").isNotNull() & F.col("_lat").isNotNull() & F.col("_lon").isNotNull()
        point = F.struct(F.col("_dt").alias("dt"), F.col("_alt_m").alias("alt"))
        before = usable & (F.col("_dt") <= 0) & (F.col("_dt") >= -iw)
        after = usable & (F.col("_dt") >= 0) & (F.col("_dt") <= iw)
        aggregates += [
            F.max_by(point, F.when(before, F.col("_dt"))).alias("_ib"),
            F.min_by(point, F.when(after, F.col("_dt"))).alias("_ia"),
        ]
    agg = pairs.groupBy("_etrack", "_etime").agg(*aggregates)

    out = ev.join(
        agg,
        (ev["track_id"] == agg["_etrack"]) & (ev["event_time"] == agg["_etime"]),
        "left",
    ).drop("_etrack", "_etime")
    out = (
        out.withColumn("ctx_altitude_ft", F.col("_alt_m") * F.lit(FT_PER_M))
        .withColumn("ctx_speed_kt", F.col("_gs_ms") * F.lit(KT_PER_MPS))
        .withColumn("ctx_vertical_rate_ft_min", F.col("_vr_ms") * F.lit(FTMIN_PER_MPS))
        .withColumn("ctx_lat", F.col("_pos._lat"))
        .withColumn("ctx_lon", F.col("_pos._lon"))
        .drop("_alt_m", "_gs_ms", "_vr_ms", "_pos")
    )
    if interpolation_window_s is not None:
        b, a = F.col("_ib"), F.col("_ia")
        at_time = (
            F.when(b.getField("dt") == 0, b.getField("alt"))
            .when(a.getField("dt") == 0, a.getField("alt"))
            .when(
                b.isNotNull() & a.isNotNull(),
                b.getField("alt")
                + (a.getField("alt") - b.getField("alt"))
                * (-b.getField("dt"))
                / (a.getField("dt") - b.getField("dt")),
            )
        )
        out = out.withColumn("ctx_interpolated_altitude_ft", at_time * F.lit(FT_PER_M)).drop(
            "_ib", "_ia"
        )

    if airports is None:
        apt = ev.sparkSession.createDataFrame(
            [], "ident string, _alat double, _alon double, _aelev double"
        )
    else:
        # One row per airport: a repeated ident would repeat the events.
        apt = (
            airports.filter(F.col("ident").isNotNull())
            .dropDuplicates(["ident"])
            .select(
                F.col("ident"),
                F.col("latitude_deg").cast("double").alias("_alat"),
                F.col("longitude_deg").cast("double").alias("_alon"),
                F.col("elevation_ft").cast("double").alias("_aelev"),
            )
        )

    def airport(prefix):
        return F.broadcast(
            apt.select(
                F.col("ident").alias(f"{prefix}_ident"),
                F.col("_alat").alias(f"{prefix}_lat"),
                F.col("_alon").alias(f"{prefix}_lon"),
            )
        )

    out = out.join(airport("_dep"), F.col("adep") == F.col("_dep_ident"), "left")
    out = out.join(airport("_arr"), F.col("ades") == F.col("_arr_ident"), "left")
    d_dep = haversine_nm(F.col("ctx_lat"), F.col("ctx_lon"), F.col("_dep_lat"), F.col("_dep_lon"))
    d_arr = haversine_nm(F.col("ctx_lat"), F.col("ctx_lon"), F.col("_arr_lat"), F.col("_arr_lon"))
    nearer = (
        F.when(d_dep.isNull(), F.when(d_arr.isNotNull(), F.col("ades")))
        .when(d_arr.isNull(), F.col("adep"))
        .when(d_dep <= d_arr, F.col("adep"))
        .otherwise(F.col("ades"))
    )
    out = out.withColumn(
        "ctx_airport",
        F.coalesce(
            F.col("apt_icao"),
            F.when(F.col("type").isin(*DEPARTURE_SIDE), F.col("adep"))
            .when(F.col("type").isin(*ARRIVAL_SIDE), F.col("ades"))
            .otherwise(nearer),
        ),
    ).drop("_dep_ident", "_dep_lat", "_dep_lon", "_arr_ident", "_arr_lat", "_arr_lon")

    own = F.broadcast(
        apt.select(
            F.col("ident").alias("_own_ident"),
            F.col("_alat").alias("_own_lat"),
            F.col("_alon").alias("_own_lon"),
            F.col("_aelev").alias("ctx_airport_elevation_ft"),
        )
    )
    out = out.join(own, F.col("ctx_airport") == F.col("_own_ident"), "left")
    out = out.withColumn(
        "ctx_height_ft", F.col("ctx_altitude_ft") - F.col("ctx_airport_elevation_ft")
    ).withColumn(
        "ctx_airport_distance_nm",
        haversine_nm(F.col("ctx_lat"), F.col("ctx_lon"), F.col("_own_lat"), F.col("_own_lon")),
    )
    return out.drop("_own_ident", "_own_lat", "_own_lon")


def ground_reference(
    events: DataFrame,
    tracks: DataFrame,
    airports: Optional[DataFrame],
    window_s: float,
    radius_nm: float,
) -> Optional[DataFrame]:
    """The track's own barometric altitude on the ground at the event's airport.

    Args:
        events: ``track_id``, ``event_time`` and ``ctx_airport`` (the airport
            the event belongs to, from :func:`event_context`).
        tracks: position reports with ``track_id``, ``event_time``, ``lat``,
            ``lon``, ``baro_altitude_c`` (m) and ``on_ground``.
        airports: the OurAirports table, or None.
        window_s: reports within this many seconds of the event are read.
        radius_nm: reports within this distance of the airport are read.

    Returns:
        One row per ``(track_id, event_time, ctx_airport)`` that has such
        reports: ``ground_altitude_ft``, the median altitude of the track's
        reports flagged on the ground, with an altitude, within the window
        and the radius, and ``ground_reports``, their number. None when the
        tracks have no on-ground flag or the airports are unknown.

    Barometric altitude is measured from standard pressure, so on a day of
    low pressure it reads high by the same amount on the ground and just
    above it; a height measured from this altitude rather than from the
    airport's elevation cancels that error.
    """
    if airports is None or "on_ground" not in tracks.columns:
        return None
    w = float(window_s)
    apt = F.broadcast(
        airports.dropDuplicates(["ident"]).select(
            F.col("ident").alias("_g_ident"),
            F.col("latitude_deg").cast("double").alias("_g_alat"),
            F.col("longitude_deg").cast("double").alias("_g_alon"),
        )
    )
    probe = (
        events.filter(
            F.col("event_time").isNotNull()
            & F.col("track_id").isNotNull()
            & F.col("ctx_airport").isNotNull()
        )
        .select(
            F.col("track_id").alias("_g_track"),
            F.col("event_time").alias("_g_time"),
            F.col("ctx_airport").alias("_g_airport"),
        )
        .distinct()
        .join(apt, F.col("_g_airport") == F.col("_g_ident"), "inner")
        .drop("_g_ident")
        .withColumn("_g_et", F.col("_g_time").cast("double"))
        .withColumn("_g_b0", F.floor(F.col("_g_et") / F.lit(w)))
    )
    probe = probe.withColumn(
        "_g_b", F.explode(F.array(F.col("_g_b0") - 1, F.col("_g_b0"), F.col("_g_b0") + 1))
    ).drop("_g_b0")
    rep = (
        tracks.filter(
            F.coalesce(F.col("on_ground"), F.lit(False))
            & F.col("baro_altitude_c").isNotNull()
            & F.col("lat").isNotNull()
            & F.col("lon").isNotNull()
        )
        .select(
            F.col("track_id").alias("_r_track"),
            F.col("event_time").cast("double").alias("_r_t"),
            F.col("lat").alias("_r_lat"),
            F.col("lon").alias("_r_lon"),
            F.col("baro_altitude_c").alias("_r_alt_m"),
        )
        .withColumn("_r_b", F.floor(F.col("_r_t") / F.lit(w)))
    )
    pairs = probe.join(
        rep, (F.col("_g_track") == F.col("_r_track")) & (F.col("_g_b") == F.col("_r_b")), "inner"
    ).filter(
        (F.abs(F.col("_r_t") - F.col("_g_et")) <= F.lit(w))
        & (
            haversine_nm(F.col("_r_lat"), F.col("_r_lon"), F.col("_g_alat"), F.col("_g_alon"))
            <= F.lit(float(radius_nm))
        )
    )
    return pairs.groupBy(
        F.col("_g_track").alias("track_id"),
        F.col("_g_time").alias("event_time"),
        F.col("_g_airport").alias("ctx_airport"),
    ).agg(
        (F.median(F.col("_r_alt_m")) * F.lit(FT_PER_M)).alias("ground_altitude_ft"),
        F.count(F.lit(1)).alias("ground_reports"),
    )


# ---------------------------------------------------------------------------
# The checks
# ---------------------------------------------------------------------------


def _info(field: str) -> Column:
    return F.get_json_object(F.col("info"), f"$.{field}")


def label_events(
    events: DataFrame,
    tracks: DataFrame,
    airports: Optional[DataFrame],
    config: "EventConfig",
    flights: Optional[DataFrame] = None,
) -> DataFrame:
    """``events`` with ``reject_rule`` and ``reject_detail`` added.

    ``reject_rule`` is the first check of :data:`RULES` that fires, or NULL
    for an event that is kept; ``reject_detail`` is a JSON object with the
    facts that check used (NULL for a kept event).

    Args:
        events: the events of step 04 (``track_id``, ``type``,
            ``event_time``, ``info``, any other columns).
        tracks: the position reports the events were found in.
        airports: the OurAirports table, or None.
        config: :class:`~opdi.config.EventConfig`; its ``plausibility_*``
            fields are the thresholds.
        flights: ``id``, ``adep``, ``ades`` of the flight list, used to measure
            the height of an event that names no airport. None leaves that
            height unknown.
    """
    columns = list(events.columns)
    timed = events.filter(F.col("event_time").isNotNull())
    no_time = events.filter(F.col("event_time").isNull()).select(
        *columns,
        F.lit("missing_time").alias("reject_rule"),
        F.lit("{}").alias("reject_detail"),
    )

    # Never NULL: an event without a type is not checked, and is kept.
    ground_row = F.coalesce(
        is_ground_event(F.col("type")) | F.col("type").isin(*RUNWAY_TIME_TYPES), F.lit(False)
    )
    others = timed.filter(~ground_row).select(
        *columns,
        F.lit(None).cast("string").alias("reject_rule"),
        F.lit(None).cast("string").alias("reject_detail"),
    )

    # The checked events carry all their columns through the context, so no
    # row identifier is needed to join the verdict back.
    probe = timed.filter(ground_row)
    probe = probe.withColumn("apt_icao", F.coalesce(_info("apt_icao"), _info("osm_airport")))
    if flights is not None:
        # One row per flight: a flight list naming a flight twice would
        # otherwise repeat its events.
        fl = flights.dropDuplicates(["id"]).select(
            F.col("id").alias("_fl_id"), F.col("adep").alias("adep"), F.col("ades").alias("ades")
        )
        probe = probe.join(fl, probe["track_id"] == fl["_fl_id"], "left").drop("_fl_id")
    probe = event_context(
        probe,
        tracks,
        airports,
        config.plausibility_context_window_s,
        interpolation_window_s=config.plausibility_runway_time_window_s,
    )
    reference = ground_reference(
        probe.filter(F.col("type").isin(*RUNWAY_TIME_TYPES)),
        tracks,
        airports,
        config.plausibility_ground_reference_window_s,
        config.plausibility_ground_reference_radius_nm,
    )
    if reference is None:
        probe = probe.withColumn("ground_altitude_ft", F.lit(None).cast("double")).withColumn(
            "ground_reports", F.lit(None).cast("long")
        )
    else:
        probe = probe.join(reference, ["track_id", "event_time", "ctx_airport"], "left")

    # The take-off or landing that bounds each block time, written by the block
    # time detector into ``info`` from the runway times of the same leg, found
    # before step 04 keeps one take-off and one landing per track.
    bound_take_off = F.to_timestamp(_info("take_off"))
    bound_landing = F.to_timestamp(_info("landing"))

    height = F.col("ctx_height_ft")
    high = (height > F.lit(float(config.plausibility_max_ground_event_height_ft))) | (
        height.isNull()
        & (
            F.col("ctx_altitude_ft")
            > F.lit(float(config.plausibility_max_ground_event_altitude_ft))
        )
    )
    fast = F.col("ctx_speed_kt") > F.lit(float(config.plausibility_max_ground_event_speed_kt))
    never_on_ground = F.col("ctx_on_ground") <= F.lit(0.0)
    # Every comparison with an unknown value is NULL, which keeps the event.
    airborne = F.coalesce(
        is_ground_event(F.col("type")) & high & fast & never_on_ground, F.lit(False)
    )
    # A take-off or landing time: its own altitude, else the altitude
    # interpolated between the reports just before and just after it. The
    # height is measured above the track's own altitude on the ground at the
    # airport, which cancels the pressure error of barometric altitude, else
    # above the airport's elevation with an allowance for that error.
    own_ft = F.col("altitude_ft") if "altitude_ft" in columns else F.lit(None).cast("double")
    runway_altitude = F.coalesce(own_ft, F.col("ctx_interpolated_altitude_ft"))
    on_ground_ref = F.col("ground_altitude_ft").isNotNull()
    runway_height = F.when(on_ground_ref, runway_altitude - F.col("ground_altitude_ft")).otherwise(
        runway_altitude - F.col("ctx_airport_elevation_ft")
    )
    max_height = float(config.plausibility_max_runway_time_height_ft)
    runway_limit = F.when(on_ground_ref, F.lit(max_height)).otherwise(
        F.lit(max_height + float(config.plausibility_pressure_allowance_ft))
    )
    too_high = F.coalesce(
        F.col("type").isin(*RUNWAY_TIME_TYPES) & (runway_height > runway_limit),
        F.lit(False),
    )
    # A block time without such a bound is not judged by these two checks.
    after_take_off = F.coalesce(
        F.col("type").isin(*OFF_BLOCK_TYPES) & (F.col("event_time") >= bound_take_off),
        F.lit(False),
    )
    before_landing = F.coalesce(
        F.col("type").isin(*ON_BLOCK_TYPES) & (F.col("event_time") <= bound_landing),
        F.lit(False),
    )

    airborne_detail = F.to_json(
        F.struct(
            F.col("ctx_airport").alias("airport"),
            F.col("ctx_airport_elevation_ft").alias("airport_elevation_ft"),
            F.round(F.col("ctx_altitude_ft"), 1).alias("altitude_ft"),
            F.round(height, 1).alias("height_ft"),
            F.round(F.col("ctx_speed_kt"), 1).alias("speed_kt"),
            F.col("ctx_on_ground").alias("on_ground_share"),
            F.col("ctx_gap_s").alias("nearest_report_s"),
            F.lit(float(config.plausibility_context_window_s)).alias("window_s"),
        )
    )
    checked = probe.select(
        *columns,
        F.when(airborne, F.lit("airborne_ground_event"))
        .when(too_high, F.lit("runway_time_too_high"))
        .when(after_take_off, F.lit("block_after_takeoff"))
        .when(before_landing, F.lit("block_before_landing"))
        .alias("reject_rule"),
        F.when(airborne, airborne_detail)
        .when(
            too_high,
            F.to_json(
                F.struct(
                    F.col("ctx_airport").alias("airport"),
                    F.col("ctx_airport_elevation_ft").alias("airport_elevation_ft"),
                    own_ft.alias("event_altitude_ft"),
                    F.round(F.col("ctx_interpolated_altitude_ft"), 1).alias(
                        "interpolated_altitude_ft"
                    ),
                    F.round(F.col("ground_altitude_ft"), 1).alias("ground_altitude_ft"),
                    F.col("ground_reports").alias("ground_reports"),
                    F.when(on_ground_ref, F.lit("ground_reports"))
                    .otherwise(F.lit("airport_elevation"))
                    .alias("reference"),
                    F.round(runway_height, 1).alias("height_ft"),
                    runway_limit.alias("limit_ft"),
                )
            ),
        )
        .when(
            after_take_off,
            F.to_json(
                F.struct(
                    _info("take_off").alias("take_off"),
                    _info("take_off_method").alias("take_off_method"),
                )
            ),
        )
        .when(
            before_landing,
            F.to_json(
                F.struct(
                    _info("landing").alias("landing"),
                    _info("landing_method").alias("landing_method"),
                )
            ),
        )
        .alias("reject_detail"),
    )
    return others.unionByName(checked).unionByName(no_time)


def split_labelled(labelled: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """(kept, rejected) from the output of :func:`label_events`.

    The kept events have the columns of the events given to
    :func:`label_events`; the rejected ones have ``reject_rule`` and
    ``reject_detail`` besides.
    """
    columns = [c for c in labelled.columns if c not in ("reject_rule", "reject_detail")]
    kept = labelled.filter(F.col("reject_rule").isNull()).select(*columns)
    rejected = labelled.filter(F.col("reject_rule").isNotNull())
    return kept, rejected


def check_events(
    events: DataFrame,
    tracks: DataFrame,
    airports: Optional[DataFrame],
    config: "EventConfig",
    flights: Optional[DataFrame] = None,
) -> Tuple[DataFrame, DataFrame]:
    """Split ``events`` into the events kept and the events removed.

    With ``config.plausibility_checks`` off, every event is kept and the
    removed frame is empty. See :func:`label_events` for the arguments.

    Returns:
        ``(kept, rejected)``: ``kept`` has the columns of ``events``;
        ``rejected`` has them plus ``reject_rule`` (the first check of
        :data:`RULES` that fired) and ``reject_detail`` (JSON of the facts it
        used).
    """
    if not config.plausibility_checks:
        rejected = events.limit(0).select(
            *events.columns,
            F.lit(None).cast("string").alias("reject_rule"),
            F.lit(None).cast("string").alias("reject_detail"),
        )
        return events, rejected
    return split_labelled(label_events(events, tracks, airports, config, flights))
