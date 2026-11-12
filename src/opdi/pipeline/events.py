"""Step 04: flight events and their measurements.

Reads the tracks (the cleaned ``osn_tracks_clean`` by default) and the
flight list, and writes the flight events (``opdi_flight_events``) and their
measurements (``opdi_measurements``). An event is a milestone of a flight
with its time, position and altitude, a ``type``, a ``source`` and the
``version`` of the method that found it; a measurement is a value attached
to an event, such as the distance flown up to it.

The event families, each described on the OPDI website under
``https://www.opdi.aero/content/methods/``:

* flight phases by fuzzy logic, and the events read from their changes:
  take-off, landing, top of climb, top of descent, level-start and level-end
  (``vertical-events``, ``takeoff-landing``);
* crossings of flight levels and of distance circles around the airports
  (``vertical-events``, ``distance-rings``);
* level-offs in climb and descent and the vertical profile tops
  (``vertical-events``);
* runway milestones, take-off and landing times and go-arounds
  (``runway-milestones``, ``takeoff-landing``);
* off-block and in-block times (``block-times``);
* entry and exit of runways, taxiways, aprons and stands
  (``surface-events``);
* holding, point merge legs and FIR crossings (``holding``, ``point-merge``,
  ``fir-crossings``);
* first and last seen (``first-last-seen``).

The measurements are the distance flown and the time passed since the
start of the track, and the holding and point merge values
(``measurements``). The thresholds are those of
:class:`opdi.config.EventConfig`; the per-family detectors live in their own
modules (``layout``, ``runway_ops``, ``ground``, ``level_segments``,
``vertical_pru``, ``holding``, ``point_merge``, ``fir``) and this module
combines them in :class:`FlightEventProcessor`.
"""

import os
from datetime import date, datetime
from typing import List, Optional, Tuple

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    abs as f_abs,
)
from pyspark.sql.functions import (
    col,
    concat,
    concat_ws,
    explode,
    lag,
    lead,
    lit,
    monotonically_increasing_id,
    row_number,
    struct,
    to_json,
    to_timestamp,
    when,
)
from pyspark.sql.functions import (
    max as f_max,
)
from pyspark.sql.functions import (
    min as f_min,
)
from pyspark.sql.functions import (
    sum as f_sum,
)
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

from opdi.config import EventConfig, OPDIConfig
from opdi.pipeline.crossings import flight_level_crossings, ring_crossings
from opdi.pipeline.fir import calculate_fir_events, load_fir_reference
from opdi.pipeline.flights import resolve_flight_id
from opdi.pipeline.ground import block_events, block_times, movement_window
from opdi.pipeline.holding import calculate_holding_events
from opdi.pipeline.holes import PIECE, add_piece

# The airport layout events are detected in ``layout.py``. These names are
# also importable from this module.
from opdi.pipeline.layout import (  # noqa: E402
    LEGACY_EVENTS_VERSION,  # noqa: F401  (imported from here by callers)
    calculate_airport_events,
    flight_aerodrome_sets,
    height_above_field_ft,  # noqa: F401  (imported from here by callers)
)
from opdi.pipeline.level_segments import LEVEL_ARMS, classify_level_offs
from opdi.pipeline.point_merge import calculate_point_merge_events
from opdi.pipeline.runway_ops import go_arounds, runway_milestones, runway_traversals
from opdi.pipeline.runways import detect_runway_movements, runway_thresholds
from opdi.pipeline.vertical_pru import (
    aerodrome_distances,
    attach_aerodrome_geometry,
    pru_top_events,
    pru_tops,
)
from opdi.reference.point_merge import point_merge_systems_for_day
from opdi.utils.datetime_helpers import generate_months, get_start_end_of_month
from opdi.utils.geometry import bearing_deg, haversine_nm
from opdi.utils.storage import StorageManager
from opdi.versions import DISTANCE_VERSION, LEGACY_DISTANCE_VERSION, TIME_VERSION

#: Columns step 04 reads from the track table.
#:
#: Some detectors run only when a column they need is present (``if "x" in
#: sdf.columns``), so a column missing from this list silently turns that
#: detector off. ``on_ground``, for example, lets the layout events admit
#: surface reports, which carry no altitude. Before removing a column, check
#: whether a detector tests for it; ``tests/test_events_processor.py`` checks
#: the list.
TRACK_COLUMNS = [
    "track_id",
    "lat",
    "lon",
    "event_time",
    "baro_altitude_c",
    "velocity",
    "vert_rate",
    "callsign",
    "icao24",
    "heading",
    "h3_res_12",
    "on_ground",
    # The FIR crossings join the resolution-7 cell with the airspace table.
    "h3_res_7",
]


# ======================================================================
# Fuzzy membership functions for flight phase classification
# ======================================================================


def zmf(column, a, b):
    """Z-shaped fuzzy membership: 1 below ``a``, 0 above ``b``, smooth between."""
    return F.when(column <= a, 1).when(column >= b, 0).otherwise((b - column) / (b - a))


def gaussmf(column, mean, sigma):
    """Bell-shaped fuzzy membership: 1 at ``mean``, width set by ``sigma``."""
    return F.exp(-((column - mean) ** 2) / (2 * sigma**2))


def smf(column, a, b):
    """S-shaped fuzzy membership: 0 below ``a``, 1 above ``b``, smooth between."""
    return F.when(column <= a, 0).when(column >= b, 1).otherwise((column - a) / (b - a))


# ======================================================================
# Event calculation functions
# ======================================================================


def _smooth_phase(df: DataFrame, twindow_seconds: float) -> DataFrame:
    """Smooth the phase labels by majority vote over a time window.

    Each report takes the most frequent phase label of the reports within
    ``twindow_seconds`` around it, as OpenAP does (``phaselabel(twindow=60)``).
    One misclassified report then no longer creates a spurious
    level-start/level-end pair or breaks the ground-to-climb change a
    take-off is read from. On a tie the report keeps its own label, so a
    label changes only when the window disagrees with it.
    """
    if not twindow_seconds or twindow_seconds <= 0:
        return df

    half = int(twindow_seconds // 2)
    window = (
        Window.partitionBy("track_id")
        .orderBy(col("event_time").cast("long"))
        .rangeBetween(-half, half)
    )

    phases = ["GND", "CL", "DE", "CR", "LVL"]
    for phase in phases:
        df = df.withColumn(
            f"_n_{phase}",
            f_sum(when(col("flight_phase") == phase, 1).otherwise(0)).over(window),
        )
    df = df.withColumn("_n_max", F.greatest(*[col(f"_n_{p}") for p in phases]))

    incumbent_wins = F.coalesce(
        F.when(col("flight_phase") == "GND", col("_n_GND") == col("_n_max"))
        .when(col("flight_phase") == "CL", col("_n_CL") == col("_n_max"))
        .when(col("flight_phase") == "DE", col("_n_DE") == col("_n_max"))
        .when(col("flight_phase") == "CR", col("_n_CR") == col("_n_max"))
        .when(col("flight_phase") == "LVL", col("_n_LVL") == col("_n_max")),
        lit(False),
    )
    majority = (
        F.when(col("_n_GND") == col("_n_max"), "GND")
        .when(col("_n_CL") == col("_n_max"), "CL")
        .when(col("_n_DE") == col("_n_max"), "DE")
        .when(col("_n_CR") == col("_n_max"), "CR")
        .when(col("_n_LVL") == col("_n_max"), "LVL")
    )
    df = df.withColumn(
        "flight_phase",
        F.when(col("flight_phase").isNull(), col("flight_phase"))
        .when(incumbent_wins, col("flight_phase"))
        .otherwise(majority),
    )
    return df.drop(*[f"_n_{p}" for p in phases], "_n_max")


def _not_across_a_hole(crossings: DataFrame, config: EventConfig) -> DataFrame:
    """Drop a crossing placed between the two sides of a gap in reception.

    A crossing is placed between the two reports on either side of the
    level, ``bracket_seconds`` apart. When those are the two sides of a hole
    in the track, the aircraft did cross, but its time and position would be
    invented. Within a piece of track no two reports are further apart than
    the hole threshold, so a track without a hole loses nothing.

    ``crossing_seq`` is numbered before this and keeps its gap: the dropped
    crossing happened, it just cannot be placed.
    """
    limit_s = float(config.track_hole_seconds)
    return crossings.filter(F.coalesce(col("bracket_seconds") <= lit(limit_s), lit(True)))


def calculate_threshold_crossing_events(
    sdf_input: DataFrame, config: Optional[EventConfig] = None
) -> DataFrame:
    """Flight-level crossings with a dead band around each level.

    The release v0.0.2 method, :func:`calculate_vertical_crossing_events`, is
    kept beside this one and is used by ``EventConfig.legacy()``.
    See :attr:`opdi.config.EventConfig.crossing_hysteresis_ft`.

    Args:
        sdf_input: Track reports with ``baro_altitude_c``, ``track_id``,
            ``event_time`` and the cumulative measures.
        config: Event settings; the defaults when omitted.

    Returns:
        One row per crossing: ``first-xing-fl<level>``/``last-xing-fl<level>``
        events (or every crossing, with ``crossing_all_occurrences``).
    """
    config = config or EventConfig()

    src = sdf_input.select(
        "track_id",
        "lat",
        "lon",
        "event_time",
        "baro_altitude_c",
        "cumulative_distance_nm",
        "cumulative_time_s",
    )
    # The distance flown and time passed are interpolated to the crossing as
    # well, so the measurements match their event.
    crossings = flight_level_crossings(
        src.withColumn(
            "cumulative_distance_nm", col("cumulative_distance_nm").cast("double")
        ).withColumn("cumulative_time_s", col("cumulative_time_s").cast("double")),
        config,
        interpolate_cols=("lat", "lon", "cumulative_distance_nm", "cumulative_time_s"),
    )

    crossings = _not_across_a_hole(crossings, config)

    fl = col("threshold").cast("int").cast("string")
    return crossings.select(
        col("track_id"),
        concat(lit("xing-fl"), fl).alias("type"),
        col("event_time"),
        col("lon"),
        col("lat"),
        (col("threshold") * 100).alias("altitude_ft"),
        col("cumulative_distance_nm"),
        col("cumulative_time_s"),
        to_json(
            struct(
                col("crossing_seq").alias("crossing_seq"),
                col("direction").alias("direction"),
                col("bracket_seconds").alias("bracket_s"),
            )
        ).alias("info"),
    )


def calculate_level_off_events(
    sdf_input: DataFrame,
    horizontal_events: DataFrame,
    config: Optional[EventConfig] = None,
    *,
    segments: Optional[DataFrame] = None,
    tops: Optional[DataFrame] = None,
) -> Optional[DataFrame]:
    """Level-offs in climb and in descent.

    Published as their own family beside ``level-start``/``level-end``.
    Those come from the fuzzy phases and say where flight looks level; the
    level-offs follow the ICAO and EUROCONTROL definitions of level flight in
    climb and descent, used by the vertical flight efficiency indicators. The
    two are not interchangeable.

    Two settings reach this function:

    * ``level_method`` chooses the segment method of :data:`LEVEL_ARMS`: an
      altitude band anchored at the start of each segment (``icao``) or a
      rolling window (``pru``). ``segments`` and ``tops`` pass segments and
      tops the caller has already computed, so they are not computed twice.
    * ``level_anchor`` chooses the tops of climb and descent that bound the
      cruise exclusion box: those of the phase method (``"phase"``) or of the
      vertical profile method (``"pru"``).

    ``sdf_input`` should carry ``elev_adep_ft``/``elev_ades_ft`` and
    ``dist_adep_nm``/``dist_ades_nm``: the height floors and the 200 NM radius
    read them, and :func:`classify_level_offs` skips the radius when the
    distance columns are absent. :class:`FlightEventProcessor` adds them.

    Returns:
        The level-off events, or None when the family is off.
    """
    config = config or EventConfig()

    segs = segments if segments is not None else LEVEL_ARMS[config.level_method](sdf_input, config)

    if config.level_anchor == "pru":
        pru = tops if tops is not None else pru_tops(sdf_input, segs, config)
        anchors = pru.select(
            col("track_id").alias("_top_id"),
            col("toc_cco_time").alias("toc_time"),
            col("toc_cco_alt_ft").alias("toc_alt"),
            col("tod_cdo_time").alias("tod_time"),
            col("tod_cdo_alt_ft").alias("tod_alt"),
        )
        segs = segs.join(anchors, segs.track_id == col("_top_id"), "left").drop("_top_id")
    else:
        phase_tops = horizontal_events.filter(col("type").isin("top-of-climb", "top-of-descent"))
        toc = phase_tops.filter(col("type") == "top-of-climb").select(
            col("track_id").alias("_toc_id"),
            col("event_time").alias("toc_time"),
            col("altitude_ft").alias("toc_alt"),
        )
        tod = phase_tops.filter(col("type") == "top-of-descent").select(
            col("track_id").alias("_tod_id"),
            col("event_time").alias("tod_time"),
            col("altitude_ft").alias("tod_alt"),
        )
        segs = segs.join(toc, segs.track_id == col("_toc_id"), "left").drop("_toc_id")
        segs = segs.join(tod, segs.track_id == col("_tod_id"), "left").drop("_tod_id")

    # A flight with no cruise has no climb or descent to attribute a level-off
    # to; the exclusion box is defined against the top of climb altitude.
    segs = segs.filter(col("toc_time").isNotNull() & col("tod_time").isNotNull())

    labelled = classify_level_offs(
        segs,
        config,
        toc_time=col("toc_time"),
        tod_time=col("tod_time"),
        toc_altitude_ft=col("toc_alt"),
        tod_altitude_ft=col("tod_alt"),
    )

    leg = F.when(col("kpi") == "KPI17", lit("climb")).otherwise(lit("descent"))
    info = to_json(
        struct(
            col("kpi").alias("kpi"),
            col("duration_seconds").alias("duration_s"),
            col("distance_nm").alias("distance_nm"),
            col("level_ft").alias("level_ft"),
        )
    )
    common = [
        col("track_id"),
        lit(None).cast("double").alias("lon"),
        lit(None).cast("double").alias("lat"),
        col("level_ft").alias("altitude_ft"),
        lit(None).cast("double").alias("cumulative_distance_nm"),
        lit(None).cast("long").alias("cumulative_time_s"),
        info.alias("info"),
    ]
    starts = labelled.select(
        concat(lit("level-off-"), leg, lit("-start")).alias("type"),
        col("start_time").alias("event_time"),
        *common,
    )
    ends = labelled.select(
        concat(lit("level-off-"), leg, lit("-end")).alias("type"),
        col("end_time").alias("event_time"),
        *common,
    )
    return starts.unionByName(ends)


def calculate_block_events(
    sdf_input: DataFrame,
    airport_events: DataFrame,
    config: Optional[EventConfig] = None,
    runway_events: Optional[DataFrame] = None,
) -> Optional[DataFrame]:
    """Off-block and in-block times (A-CDM milestones T04 and T21).

    Published as ``AOBT``/``AIBT``, or as ``off-block``/``on-block`` when the
    runway milestones are on without :attr:`merge_duplicate_milestones`. One
    detector, two names.

    Found from the movement on the ground next to the stand events
    (``exit-parking_position``, ``entry-parking_position``) that step 04
    already finds from the airport layouts. With
    :attr:`~opdi.config.EventConfig.block_times_on_ground_only` (the default)
    each time comes from the ground part of its own leg, bounded by the
    take-off and landing times in ``runway_events``
    (:func:`opdi.pipeline.ground.block_events`), and ``info`` names the stand
    (``stand``) and its airport (``apt_icao``). Without it, the first and last
    sustained movement of the whole track are used. Coverage is the main
    limit: an aircraft on a stand is often not received at all.
    See https://www.opdi.aero/content/methods/block-times.html.
    """
    config = config or EventConfig()
    common = [
        lit(None).cast("double").alias("lon"),
        lit(None).cast("double").alias("lat"),
        lit(None).cast("double").alias("altitude_ft"),
        lit(None).cast("double").alias("cumulative_distance_nm"),
        lit(None).cast("long").alias("cumulative_time_s"),
    ]
    # ``AOBT``/``AIBT`` with the merge or with the v0.0.2 settings;
    # ``off-block``/``on-block`` with the runway milestones but no merge.
    _acdm_names = config.emit_runway_milestones and not config.merge_duplicate_milestones
    off_type = "off-block" if _acdm_names else "AOBT"
    on_type = "on-block" if _acdm_names else "AIBT"

    if config.block_times_on_ground_only:
        found = block_events(sdf_input, airport_events, runway_events, config)
        stand = [col("stand").alias("stand"), col("stand_airport").alias("apt_icao")]
        aobt = found.filter(col("block") == "off").select(
            col("track_id"),
            lit(off_type).alias("type"),
            col("event_time"),
            *common,
            to_json(struct(col("stand_time").alias("stand_exit"), *stand)).alias("info"),
        )
        aibt = found.filter(col("block") == "on").select(
            col("track_id"),
            lit(on_type).alias("type"),
            col("event_time"),
            *common,
            to_json(struct(col("stand_time").alias("stand_entry"), *stand)).alias("info"),
        )
        return aobt.unionByName(aibt)

    movements = movement_window(sdf_input, config)
    blocks = block_times(movements, airport_events)
    if blocks is None:
        return None
    aobt = blocks.filter(col("aobt").isNotNull()).select(
        col("track_id"),
        lit(off_type).alias("type"),
        col("aobt").alias("event_time"),
        *common,
        to_json(struct(col("stand_exit").alias("stand_exit"))).alias("info"),
    )
    aibt = blocks.filter(col("aibt").isNotNull()).select(
        col("track_id"),
        lit(on_type).alias("type"),
        col("aibt").alias("event_time"),
        *common,
        to_json(struct(col("stand_entry").alias("stand_entry"))).alias("info"),
    )
    return aobt.unionByName(aibt)


def calculate_runway_events(
    sdf_input: DataFrame,
    month: date,
    storage: "StorageManager",
    config: Optional[EventConfig] = None,
) -> Optional[DataFrame]:
    """Take-off and landing times (``ATOT``, ``ALDT``) and their runway.

    These are the A-CDM milestones T08 and T17. They are separate from the
    ``take-off`` and ``landing`` events of the phase method: those say where
    the flight phase changed, these which runway was used and when the aircraft
    left or met it.

    Each time is the first (departure) or last (arrival) report in the
    runway window. When the runway milestones are on, ``airborne`` and
    ``touchdown`` give better times (interpolated at a set height), and
    :class:`FlightEventProcessor` uses this function only for the flights
    those do not cover.
    See https://www.opdi.aero/content/methods/takeoff-landing.html.
    """
    config = config or EventConfig()
    thresholds = runway_thresholds(storage)
    if thresholds is None or not storage.table_exists("opdi_flight_list"):
        return None
    if not storage.table_exists("oa_airports"):
        return None

    start_ts, end_ts = get_start_end_of_month(month)
    fl = (
        storage.read_table("opdi_flight_list")
        .filter(
            (col("dof") >= to_timestamp(lit(start_ts))) & (col("dof") < to_timestamp(lit(end_ts)))
        )
        .select(col("id").alias("track_id"), col("adep"), col("ades"))
    )
    ends = (
        fl.select("track_id", col("adep").alias("apt_ident"), lit("departure").alias("role"))
        .unionByName(
            fl.select("track_id", col("ades").alias("apt_ident"), lit("arrival").alias("role"))
        )
        .filter(col("apt_ident").isNotNull() & (col("apt_ident") != ""))
    )

    apt = storage.read_table("oa_airports").select(
        col("ident").alias("_ident"),
        col("latitude_deg").cast("double").alias("apt_lat"),
        col("longitude_deg").cast("double").alias("apt_lon"),
        col("elevation_ft").cast("double").alias("apt_elevation_ft"),
    )
    ends = ends.join(F.broadcast(apt), ends.apt_ident == col("_ident"), "inner").drop("_ident")

    moves = detect_runway_movements(sdf_input, ends, thresholds, config)

    # The departure's time is its first report in the window and the
    # arrival's its last: stand-ins for lift-off and touchdown.
    event_time = F.when(col("role") == "departure", col("first_time")).otherwise(col("last_time"))
    type_ = F.when(col("role") == "departure", lit("ATOT")).otherwise(lit("ALDT"))

    return moves.select(
        col("track_id"),
        type_.alias("type"),
        event_time.alias("event_time"),
        lit(None).cast("double").alias("lon"),
        lit(None).cast("double").alias("lat"),
        lit(None).cast("double").alias("altitude_ft"),
        lit(None).cast("double").alias("cumulative_distance_nm"),
        lit(None).cast("long").alias("cumulative_time_s"),
        to_json(
            struct(
                col("rwy_ident").alias("runway"),
                # The same info key as the runway milestones, so the runway is
                # read the same way whichever detector gave the row.
                col("rwy_bearing").cast("double").alias("runway_bearing_deg"),
                col("apt_ident").alias("apt_icao"),
                col("role").alias("role"),
                col("bearing_error").alias("bearing_error_deg"),
                col("n_samples").alias("n_samples"),
                # Which detector gave this time. With the merge, these rows
                # share the ``ATOT``/``ALDT`` type with the runway milestones,
                # and the two differ in accuracy. NULL without the merge.
                (
                    lit("legacy") if config.merge_duplicate_milestones else lit(None).cast("string")
                ).alias("method"),
            )
        ).alias("info"),
    )


#: Event types that two detectors can give; each row of them names its
#: detector in ``info.method``.
MERGED_TYPES = ("ATOT", "ALDT")

#: Which detector is kept where both gave a time: the runway milestones
#: (``acdm``, interpolated at ``runway_airborne_height_ft``) before the
#: runway window method (``legacy``). Lower sorts first.
_METHOD_RANK = {"acdm": 0, "legacy": 1}


def merge_milestone_duplicates(
    df_events: DataFrame, config: Optional[EventConfig] = None
) -> DataFrame:
    """Keep one ``ATOT`` and one ``ALDT`` per flight.

    With :attr:`merge_duplicate_milestones`, both detectors write the same
    type, and this keeps one row per ``(track_id, type)``::

        ATOT = runway milestone airborne if found, else runway window time
        ALDT = runway milestone touchdown if found, else runway window time

    A flight keeps a time whenever either detector found one, and a row
    without a time is kept only when no row of that flight and type has one.
    Every other
    event type, and rows without ``info.method``, pass through unchanged.
    """
    config = config or EventConfig()
    if not config.merge_duplicate_milestones:
        return df_events

    merged = col("type").isin(*MERGED_TYPES)
    rank = F.coalesce(
        F.create_map(*[x for k, v in _METHOD_RANK.items() for x in (lit(k), lit(v))])[
            F.get_json_object(col("info"), "$.method")
        ],
        # An unknown or missing method sorts last: it may fill a gap but never
        # replaces a known one.
        lit(len(_METHOD_RANK)),
    )
    # A row without a time is never kept where a row with one exists, whatever
    # its method. ``event_time`` then breaks a tie between two rows of one
    # method, so the choice does not depend on how Spark split the data.
    best = Window.partitionBy("track_id", "type").orderBy(
        col("event_time").isNull().asc(), rank.asc(), col("event_time").asc()
    )
    kept = (
        df_events.filter(merged)
        .withColumn("_rank", F.row_number().over(best))
        .filter(col("_rank") == 1)
        .drop("_rank")
    )
    return df_events.filter(~merged | col("type").isNull()).unionByName(kept)


def calculate_ring_crossing_events(
    sdf_input: DataFrame,
    month: date,
    storage: "StorageManager",
    config: Optional[EventConfig] = None,
) -> Optional[DataFrame]:
    """Crossings of the distance circles around the flight's own airports.

    The circles are drawn around the flight's departure and destination
    airports from the flight list, as the ICAO indicators of the arrival
    sequencing area (ASMA, 40 NM around the destination) and of the
    reference area around both airports define them. Each sample is
    compared with at most two airports.
    See https://www.opdi.aero/content/methods/distance-rings.html.

    Returns:
        The crossing events, or None when no radius is configured.
    """
    config = config or EventConfig()
    radii = list(config.ring_radii_nm)
    if not radii:
        return None
    if not (storage.table_exists("opdi_flight_list") and storage.table_exists("oa_airports")):
        return None

    start_ts, end_ts = get_start_end_of_month(month)
    fl = (
        storage.read_table("opdi_flight_list")
        .filter(
            (col("dof") >= to_timestamp(lit(start_ts))) & (col("dof") < to_timestamp(lit(end_ts)))
        )
        .select(col("id").alias("_fl_id"), col("adep"), col("ades"))
    )
    ends = (
        fl.select("_fl_id", col("adep").alias("apt_ident"))
        .unionByName(fl.select("_fl_id", col("ades").alias("apt_ident")))
        .filter(col("apt_ident").isNotNull() & (col("apt_ident") != ""))
        .distinct()
    )

    apt = storage.read_table("oa_airports").select(
        col("ident").alias("_apt_ident"),
        col("latitude_deg").cast("double").alias("apt_lat"),
        col("longitude_deg").cast("double").alias("apt_lon"),
    )
    ends = ends.join(F.broadcast(apt), ends.apt_ident == col("_apt_ident"), "inner").drop(
        "_apt_ident"
    )

    work = (
        sdf_input.select(
            "track_id",
            "lat",
            "lon",
            "event_time",
            "baro_altitude_c",
            "cumulative_distance_nm",
            "cumulative_time_s",
        )
        .join(F.broadcast(ends), col("track_id") == col("_fl_id"), "inner")
        .drop("_fl_id")
    )

    work = (
        work.withColumn(
            "distance_nm", haversine_nm(col("lat"), col("lon"), col("apt_lat"), col("apt_lon"))
        )
        .withColumn("flight_level", col("baro_altitude_c") * 3.28084 / 100.0)
        .withColumn("cumulative_distance_nm", col("cumulative_distance_nm").cast("double"))
        .withColumn("cumulative_time_s", col("cumulative_time_s").cast("double"))
    )

    crossings = ring_crossings(
        work,
        config,
        partition_cols=["track_id", "apt_ident", "apt_lat", "apt_lon"],
        interpolate_cols=(
            "lat",
            "lon",
            "flight_level",
            "cumulative_distance_nm",
            "cumulative_time_s",
        ),
    )

    # The bearing is computed from the interpolated crossing position, not
    # interpolated itself: averaging the bearings 359 and 1 would give 180.
    # Measured from the airport outwards.
    crossings = _not_across_a_hole(crossings, config)
    crossings = crossings.withColumn(
        "bearing_deg",
        bearing_deg(col("apt_lat"), col("apt_lon"), col("lat"), col("lon")),
    )

    ring = col("threshold").cast("int").cast("string")
    return crossings.select(
        col("track_id"),
        concat(lit("xing-"), ring, lit("nm")).alias("type"),
        col("event_time"),
        col("lon"),
        col("lat"),
        (col("flight_level") * 100).alias("altitude_ft"),
        col("cumulative_distance_nm"),
        col("cumulative_time_s"),
        to_json(
            struct(
                col("crossing_seq").alias("crossing_seq"),
                col("direction").alias("direction"),
                col("apt_ident").alias("apt_icao"),
                col("bearing_deg").alias("bearing"),
                col("flight_level").alias("flight_level"),
                col("bracket_seconds").alias("bracket_s"),
            )
        ).alias("info"),
    )


def calculate_horizontal_segment_events(
    sdf_input: DataFrame, config: Optional[EventConfig] = None
) -> DataFrame:
    """Flight phases by fuzzy logic, and the events read from their changes.

    Each report is classified into a phase (ground ``GND``, climb ``CL``,
    descent ``DE``, cruise ``CR``, level ``LVL``) by fuzzy membership
    functions of altitude, rate of climb and speed (the constants of
    OpenAP). The changes of phase give the events ``level-start``,
    ``level-end``, ``top-of-climb``, ``top-of-descent``, and, with the v0.0.2
    settings, ``take-off`` and ``landing``.
    See https://www.opdi.aero/content/methods/vertical-events.html.

    Args:
        sdf_input: Track reports with ``baro_altitude_c``, ``vert_rate`` and
            ``velocity``.
        config: Event settings; the defaults when omitted.

    Returns:
        The events with the columns ``track_id``, ``type``, ``event_time``,
        ``lon``, ``lat``, ``altitude_ft``, ``cumulative_distance_nm``,
        ``cumulative_time_s`` and ``info``.
    """
    config = config or EventConfig()

    wanted = [
        "track_id",
        "lat",
        "lon",
        "event_time",
        "baro_altitude_c",
        "vert_rate",
        "velocity",
        "cumulative_distance_nm",
        "cumulative_time_s",
    ]
    # Carried only when the caller attached them, so the detector stays a pure
    # column-in/column-out function and can be tested without storage.
    elevations = [c for c in ("elev_adep_ft", "elev_ades_ft") if c in sdf_input.columns]
    # The piece of the track, where the caller added one (see
    # ``pipeline/holes.py``): a level segment then ends at the hole it meets.
    # Without it the results are those of a track without holes.
    pieces = [PIECE] if PIECE in sdf_input.columns else []
    df = sdf_input.select(*wanted, *elevations, *pieces)
    use_agl = config.phase_ground_above_field and len(elevations) == 2

    # Convert units: meters -> feet, m/s -> ft/min, m/s -> knots
    df = df.withColumn("alt", col("baro_altitude_c") * 3.28084)
    df = df.withColumn("roc", col("vert_rate") * 196.850394)
    df = df.withColumn("spd", col("velocity") * 1.94384)

    # Fuzzy membership functions, with the constants of OpenAP; the ones that
    # can be set are in EventConfig.
    if use_agl:
        # Height above the airport at whichever end the aircraft is. An unknown
        # elevation counts as 0 ft, so the flight keeps its phases.
        ceiling = config.phase_ground_ceiling_ft
        df = df.withColumn(
            "alt_gnd",
            F.greatest(
                zmf(col("alt") - F.coalesce(col("elev_adep_ft"), lit(0.0)), 0, ceiling),
                zmf(col("alt") - F.coalesce(col("elev_ades_ft"), lit(0.0)), 0, ceiling),
            ),
        )
    else:
        df = df.withColumn("alt_gnd", zmf(col("alt"), 0, config.phase_ground_ceiling_ft))
    df = df.withColumn("alt_lo", gaussmf(col("alt"), 10000, 10000))
    df = df.withColumn("alt_hi", gaussmf(col("alt"), 35000, 20000))
    df = df.withColumn("roc_zero", gaussmf(col("roc"), 0, 100))
    df = df.withColumn("roc_plus", smf(col("roc"), 10, 1000))
    df = df.withColumn("roc_minus", zmf(col("roc"), -1000, -10))
    df = df.withColumn(
        "spd_hi",
        gaussmf(col("spd"), config.phase_cruise_speed_kt, config.phase_cruise_speed_sigma_kt),
    )
    df = df.withColumn("spd_md", gaussmf(col("spd"), 300, 100))
    df = df.withColumn("spd_lo", gaussmf(col("spd"), 0, 50))

    df.cache()

    def rule(*members):
        """The smallest membership of a rule's inputs (its activation).

        ``F.least`` skips NULLs, so a rule with a missing input would be
        judged on fewer inputs and score higher. With
        ``phase_require_complete_rules`` a rule with a NULL input gives NULL,
        which ``F.greatest`` then ignores.
        """
        activation = F.least(*members)
        if not config.phase_require_complete_rules:
            return activation
        complete = members[0].isNotNull()
        for m in members[1:]:
            complete = complete & m.isNotNull()
        return F.when(complete, activation)

    # Fuzzy logic rules
    df = df.withColumn("rule_ground", rule(col("alt_gnd"), col("roc_zero"), col("spd_lo")))
    df = df.withColumn("rule_climb", rule(col("alt_lo"), col("roc_plus"), col("spd_md")))
    df = df.withColumn("rule_descent", rule(col("alt_lo"), col("roc_minus"), col("spd_md")))
    df = df.withColumn("rule_cruise", rule(col("alt_hi"), col("roc_zero"), col("spd_hi")))
    df = df.withColumn("rule_level", rule(col("alt_lo"), col("roc_zero"), col("spd_md")))

    # Determine phase by maximum rule activation
    df = df.withColumn(
        "aggregated",
        F.greatest(
            col("rule_ground"),
            col("rule_climb"),
            col("rule_descent"),
            col("rule_cruise"),
            col("rule_level"),
        ),
    )

    df = df.withColumn(
        "flight_phase",
        F.when(col("aggregated") == col("rule_ground"), "GND")
        .when(col("aggregated") == col("rule_climb"), "CL")
        .when(col("aggregated") == col("rule_descent"), "DE")
        .when(col("aggregated") == col("rule_cruise"), "CR")
        .when(col("aggregated") == col("rule_level"), "LVL"),
    )

    df = _smooth_phase(df, config.phase_twindow_seconds)

    df = (
        df.withColumnRenamed("alt", "altitude_ft")
        .withColumnRenamed("roc", "roc_ft_min")
        .withColumnRenamed("spd", "speed_kt")
    )

    df = df.select(
        "track_id",
        "lat",
        "lon",
        "event_time",
        "cumulative_distance_nm",
        "cumulative_time_s",
        "altitude_ft",
        "roc_ft_min",
        "speed_kt",
        "flight_phase",
        *pieces,
    )

    df.cache()

    # Phase changes. The neighbouring phase is looked for inside the report's
    # own piece of track: across a hole there is no neighbour, so a segment
    # ends at the last report before a hole and a new one starts after it.
    # The first and last cruise report stay per track.
    window_phase = Window.partitionBy("track_id", *pieces).orderBy("event_time")
    window_cumulative = (
        Window.partitionBy("track_id")
        .orderBy("event_time")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    if config.merge_duplicate_milestones:
        # The nearest known phase on either side, not the immediate neighbour.
        #
        # The classifier gives NULL where a rule's inputs are incomplete, and
        # such reports are scattered. Compared with the immediate neighbour, a
        # level segment next to a NULL would get no boundary there. Counting
        # a NULL as a different phase would split real segments at every
        # scattered NULL. Skipping the NULLs keeps a segment interrupted by
        # them as one segment and puts boundaries only at real phase changes.
        #
        # The reversed order turns a frame that shrinks for every row (and is
        # rescanned each time) into one that grows and is scanned once.
        known = when(col("flight_phase").isNotNull(), col("flight_phase"))
        back = window_phase.rowsBetween(Window.unboundedPreceding, -1)
        forward = (
            Window.partitionBy("track_id", *pieces)
            .orderBy(col("event_time").desc())
            .rowsBetween(Window.unboundedPreceding, -1)
        )
        # ``"None"`` marks the edge of a track, as in release v0.0.2, so a track
        # that begins or ends in level flight gets a boundary at both ends.
        df = df.withColumn(
            "prev_phase",
            F.coalesce(F.last(known, ignorenulls=True).over(back), lit("None")),
        )
        df = df.withColumn(
            "next_phase",
            F.coalesce(F.last(known, ignorenulls=True).over(forward), lit("None")),
        )
    else:
        df = df.withColumn("prev_phase", lag("flight_phase", 1, "None").over(window_phase))
        df = df.withColumn("next_phase", lead("flight_phase", 1, "None").over(window_phase))

    df.cache()

    # Identify TOC and TOD (first/last cruise points)
    df = df.withColumn(
        "first_cr_time",
        f_min(when(col("flight_phase") == "CR", col("event_time"))).over(window_cumulative),
    )
    df = df.withColumn(
        "last_cr_time",
        f_max(when(col("flight_phase") == "CR", col("event_time"))).over(window_cumulative),
    )

    start_of_segment = (col("flight_phase").isin("CR", "LVL")) & (
        col("prev_phase") != col("flight_phase")
    )
    df = df.withColumn("start_of_segment", start_of_segment)
    df = df.withColumn(
        "segment_count",
        f_sum(when(col("start_of_segment"), 1).otherwise(0)).over(window_cumulative),
    )

    # Create event type arrays
    is_level_start = start_of_segment | (col("event_time") == col("first_cr_time"))
    is_level_end = (
        (col("flight_phase").isin("CR", "LVL")) & (col("next_phase") != col("flight_phase"))
    ) | (col("event_time") == col("last_cr_time"))

    if config.merge_duplicate_milestones:
        # Two independent tests: a report that both starts and ends a level
        # segment (a segment one report long) gives both events.
        milestone_types = F.filter(
            F.array(
                F.when(is_level_start, lit("level-start")),
                F.when(is_level_end, lit("level-end")),
            ),
            lambda x: x.isNotNull(),
        )
    else:
        # The v0.0.2 settings: the first matching type wins and the phase
        # method's tops share the same slot, as that release published.
        milestone_types = (
            F.when(
                col("event_time") == col("first_cr_time"),
                F.array(lit("level-start"), lit("top-of-climb")),
            )
            .when(
                col("event_time") == col("last_cr_time"),
                F.array(lit("level-end"), lit("top-of-descent")),
            )
            .when(start_of_segment, F.array(lit("level-start")))
            .when(
                (col("flight_phase").isin("CR", "LVL"))
                & (col("next_phase") != col("flight_phase")),
                F.array(lit("level-end")),
            )
        )

    # ``take-off`` and ``landing`` here are the reports at which the phase
    # changed to or from ground. The runway milestones give the same moments
    # as ``airborne``/``touchdown``, interpolated at a set height, and their
    # ``landing`` is the threshold crossing (A-CDM T16). So this pair is
    # published only when the runway milestones are off.
    if not config.emit_runway_milestones:
        # Only with the runway milestones off, which also means no merge.
        milestone_types = milestone_types.when(
            (col("prev_phase") == "GND") & (col("next_phase") == "CL"),
            F.array(lit("take-off")),
        ).when(
            (col("prev_phase") == "DE") & (col("flight_phase") == "GND"),
            F.array(lit("landing")),
        )

    if not config.merge_duplicate_milestones:
        milestone_types = milestone_types.otherwise(F.array())
    df = df.withColumn("milestone_types", milestone_types)

    # Explode to one row per event
    df_exploded = df.select("*", explode(col("milestone_types")).alias("type"))
    df_exploded = df_exploded.filter("type IS NOT NULL")

    df_events = df_exploded.select(
        col("track_id"),
        col("type"),
        col("event_time"),
        col("lon"),
        col("lat"),
        col("altitude_ft"),
        col("cumulative_distance_nm"),
        col("cumulative_time_s"),
    )

    df_events = df_events.dropDuplicates(["track_id", "type", "event_time"])

    # Each top names its method in ``info`` only where a second method's tops
    # are published too (``emit_pru_tops``). With the v0.0.2 settings ``info``
    # stays empty, as that release published it.
    if config.emit_pru_tops:
        info = when(
            col("type").isin("top-of-climb", "top-of-descent"),
            to_json(struct(lit("phase").alias("method"))),
        ).otherwise(lit(""))
    else:
        info = lit("")
    df_events = df_events.withColumn("info", info)

    return df_events


def calculate_vertical_crossing_events(sdf_input: DataFrame) -> DataFrame:
    """First and last crossing of each flight level, as in release v0.0.2.

    Args:
        sdf_input: Track reports with ``baro_altitude_c``.

    Returns:
        The crossing events with the columns ``track_id``, ``type``,
        ``event_time``, ``lon``, ``lat``, ``altitude_ft``,
        ``cumulative_distance_nm``, ``cumulative_time_s`` and ``info``.
    """
    df = sdf_input.select(
        "track_id",
        "lat",
        "lon",
        "event_time",
        "baro_altitude_c",
        "vert_rate",
        "velocity",
        "cumulative_distance_nm",
        "cumulative_time_s",
    )

    df = df.withColumn("altitude_ft", col("baro_altitude_c") * 3.28084)
    df = df.withColumn("FL", (col("baro_altitude_c") * 3.28084 / 100).cast(DoubleType()))

    window_spec = Window.partitionBy("track_id").orderBy("event_time")
    df = df.withColumn("next_FL", F.lead("FL").over(window_spec)).cache()

    # Define crossing conditions for each FL
    crossing_conditions = [
        ((col("FL") < 50) & (col("next_FL") >= 50), 50),
        ((col("FL") >= 50) & (col("next_FL") < 50), 50),
        ((col("FL") < 70) & (col("next_FL") >= 70), 70),
        ((col("FL") >= 70) & (col("next_FL") < 70), 70),
        ((col("FL") < 100) & (col("next_FL") >= 100), 100),
        ((col("FL") >= 100) & (col("next_FL") < 100), 100),
        ((col("FL") < 245) & (col("next_FL") >= 245), 245),
        ((col("FL") >= 245) & (col("next_FL") < 245), 245),
    ]

    crossing_column = F.when(crossing_conditions[0][0], crossing_conditions[0][1])
    for cond, fl_value in crossing_conditions[1:]:
        crossing_column = crossing_column.when(cond, fl_value)

    df = df.withColumn("crossing", crossing_column)
    crossing_points = df.filter(col("crossing").isNotNull())
    crossing_points = crossing_points.filter(f_abs(col("crossing") - col("FL")) < 10).cache()

    # First and last crossings per FL
    window_asc = Window.partitionBy("track_id", "crossing").orderBy("event_time")
    window_desc = Window.partitionBy("track_id", "crossing").orderBy(col("event_time").desc())

    crossing_points = crossing_points.withColumn("row_asc", row_number().over(window_asc))
    crossing_points = crossing_points.withColumn("row_desc", row_number().over(window_desc))
    crossing_points.cache()

    first_crossings = (
        crossing_points.filter(col("row_asc") == 1)
        .drop("row_asc", "row_desc")
        .withColumn("type", concat(lit("first-xing-fl"), col("crossing").cast("string")))
    )
    last_crossings = (
        crossing_points.filter(col("row_desc") == 1)
        .drop("row_asc", "row_desc")
        .withColumn("type", concat(lit("last-xing-fl"), col("crossing").cast("string")))
    )

    all_crossings = first_crossings.unionByName(last_crossings)

    df_events = all_crossings.select(
        "track_id",
        "type",
        "event_time",
        "lon",
        "lat",
        "altitude_ft",
        "cumulative_distance_nm",
        "cumulative_time_s",
    )
    df_events = df_events.dropDuplicates(
        [
            "track_id",
            "type",
            "event_time",
            "lon",
            "lat",
            "altitude_ft",
            "cumulative_distance_nm",
            "cumulative_time_s",
        ]
    )
    df_events = df_events.withColumn("info", lit(""))

    return df_events


def calculate_firstseen_lastseen_events(sdf_input: DataFrame) -> DataFrame:
    """The ``first_seen`` and ``last_seen`` events: a track's first and last report.

    Args:
        sdf_input: Track reports.

    Returns:
        Two events per track.
    """
    df = sdf_input.select(
        "track_id",
        "lat",
        "lon",
        "event_time",
        "baro_altitude_c",
        "vert_rate",
        "velocity",
        "cumulative_distance_nm",
        "cumulative_time_s",
    )

    df = df.withColumn("altitude_ft", col("baro_altitude_c") * 3.28084)

    window_asc = Window.partitionBy("track_id").orderBy("event_time")
    window_desc = Window.partitionBy("track_id").orderBy(col("event_time").desc())

    df = df.withColumn("row_asc", row_number().over(window_asc))
    df = df.withColumn("row_desc", row_number().over(window_desc))

    first_seen = (
        df.filter(col("row_asc") == 1)
        .drop("row_asc", "row_desc")
        .withColumn("type", lit("first_seen"))
    )
    last_seen = (
        df.filter(col("row_desc") == 1)
        .drop("row_asc", "row_desc")
        .withColumn("type", lit("last_seen"))
    )

    all_events = first_seen.unionByName(last_seen)

    df_events = all_events.select(
        "track_id",
        "type",
        "event_time",
        "lon",
        "lat",
        "altitude_ft",
        "cumulative_distance_nm",
        "cumulative_time_s",
    )
    df_events = df_events.dropDuplicates(
        [
            "track_id",
            "type",
            "event_time",
            "lon",
            "lat",
            "altitude_ft",
            "cumulative_distance_nm",
            "cumulative_time_s",
        ]
    )
    df_events = df_events.withColumn("info", lit(""))

    return df_events


# ======================================================================
# Airport layout events are in layout.py (imported at the top).
# ======================================================================


# ======================================================================
# Measurement helper functions
# ======================================================================

#: Measurements read off an event's ``info`` JSON, beside the two every event
#: gets (distance flown, time passed). One entry is
#: ``(event type, info field, measurement type, measurement version)``: where the
#: field is present in a row of that type, one measurement row is written with
#: the field's value cast to double. This is how a family publishes a
#: duration or a count as a measurement without its own write path. Families
#: append to it when they are imported (see ``holding.py``, ``point_merge.py``).
EXTRA_MEASURES: List[Tuple[str, str, str, str]] = []


def _register_family_measures() -> None:
    """Collect the measurement entries of the event families.

    Imported when first needed, so the family modules, which import the
    configuration, do not import this module when they load.
    """
    from opdi.pipeline.holding import HOLDING_MEASURES
    from opdi.pipeline.point_merge import PMS_MEASURES

    for entry in HOLDING_MEASURES + PMS_MEASURES:
        if entry not in EXTRA_MEASURES:
            EXTRA_MEASURES.append(entry)


_register_family_measures()


def measurement_suffix(measurement_type: str) -> str:
    """The id suffix of an extra measurement.

    ``_m`` plus six hexadecimal characters of the hash of the measurement
    type. It depends on the type only, so adding or reordering entries of
    ``EXTRA_MEASURES`` never changes a published id.
    """
    import hashlib

    return "_m" + hashlib.sha256(measurement_type.encode("utf-8")).hexdigest()[:6]


def extra_measurement_rows(df_events: DataFrame) -> Optional[DataFrame]:
    """Measurement rows for every ``EXTRA_MEASURES`` entry, or None if none apply.

    ``df_events`` holds the events with their ids (``id_tmp``, ``type``,
    ``info``, ``dof``). Each measurement id is the event id plus
    :func:`measurement_suffix` of its type, distinct from the ``_d``/``_t``
    ids of distance and time. The families that use this run only with
    event ids built from the event's content.

    All entries are read in one projection and exploded, which keeps the
    Spark plan small.
    """
    if not EXTRA_MEASURES:
        return None
    values = F.array(
        *[
            F.when(
                col("type") == event_type,
                F.get_json_object(col("info"), f"$.{field}").cast("double"),
            )
            for event_type, field, _, _ in EXTRA_MEASURES
        ]
    )
    types = F.array(*[lit(m) for _, _, m, _ in EXTRA_MEASURES])
    versions = F.array(*[lit(v) for _, _, _, v in EXTRA_MEASURES])
    suffixes = F.array(*[lit(measurement_suffix(m)) for _, _, m, _ in EXTRA_MEASURES])
    exploded = df_events.select(
        col("id_tmp"), col("dof"), F.posexplode(values).alias("_pos", "value")
    ).filter(col("value").isNotNull())
    return exploded.select(
        F.concat(col("id_tmp"), F.element_at(suffixes, col("_pos") + 1)).alias("id"),
        col("id_tmp").alias("milestone_id"),
        F.element_at(types, col("_pos") + 1).alias("type"),
        col("value"),
        F.element_at(versions, col("_pos") + 1).alias("version"),
        col("dof"),
    )


def add_time_measure(sdf_input: DataFrame) -> DataFrame:
    """Add ``cumulative_time_s``: seconds since the track's first report."""
    window_spec = Window.partitionBy("track_id").orderBy("event_time")
    sdf_input = sdf_input.withColumn("min_event_time", f_min("event_time").over(window_spec))
    sdf_input = sdf_input.withColumn(
        "cumulative_time_s",
        col("event_time").cast("long") - col("min_event_time").cast("long"),
    )
    return sdf_input.drop("min_event_time")


def distance_version(config: EventConfig) -> str:
    """The ``version`` of the distance measurement under ``config``."""
    if config.distance_from_last_known_position:
        return DISTANCE_VERSION
    return LEGACY_DISTANCE_VERSION


def add_distance_measure(df: DataFrame, last_known_position: bool = True) -> DataFrame:
    """Add the distance from the previous report and the distance flown, in NM.

    Great-circle (haversine) distances. With ``last_known_position`` (the
    default), each report is measured from the last report that has a
    position: a report without a position adds nothing, and the next one
    with a position covers the whole stretch, across a hole the straight line
    between its sides. Without it (release v0.0.2), each report is measured
    from the previous row, so a missing position loses the distance on both
    sides of it.

    Args:
        df: Reports with ``lat``, ``lon``, ``track_id`` and ``event_time``.
        last_known_position: Measure from the last known position.

    Returns:
        The reports with ``segment_distance_nm`` and
        ``cumulative_distance_nm``.
    """
    window_lag = Window.partitionBy("track_id").orderBy("event_time")
    if last_known_position:
        return _distance_from_last_known_position(df, window_lag)
    window_cumsum = (
        Window.partitionBy("track_id")
        .orderBy("event_time")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    df = df.withColumn("lat_rad", F.radians(col("lat")))
    df = df.withColumn("lon_rad", F.radians(col("lon")))
    df = df.withColumn("prev_lat_rad", lag("lat_rad").over(window_lag))
    df = df.withColumn("prev_lon_rad", lag("lon_rad").over(window_lag))

    df = df.withColumn(
        "a",
        F.sin((col("lat_rad") - col("prev_lat_rad")) / 2) ** 2
        + F.cos(col("prev_lat_rad"))
        * F.cos(col("lat_rad"))
        * F.sin((col("lon_rad") - col("prev_lon_rad")) / 2) ** 2,
    )
    df = df.withColumn("c", 2 * F.atan2(F.sqrt(col("a")), F.sqrt(1 - col("a"))))
    df = df.withColumn("distance_km", 6371 * col("c"))
    df = df.withColumn(
        "segment_distance_nm",
        when(col("distance_km").isNull(), 0).otherwise(col("distance_km") / 1.852),
    )
    df = df.withColumn("cumulative_distance_nm", f_sum("segment_distance_nm").over(window_cumsum))
    df = df.drop("lat_rad", "lon_rad", "prev_lat_rad", "prev_lon_rad", "a", "c", "distance_km")

    return df


def _distance_from_last_known_position(df: DataFrame, ordered) -> DataFrame:
    """:func:`add_distance_measure` from the last report holding a position.

    Both coordinates come from the same report.
    """
    known = when(col("lat").isNotNull() & col("lon").isNotNull(), struct("lat", "lon"))
    before = ordered.rowsBetween(Window.unboundedPreceding, -1)
    running = ordered.rowsBetween(Window.unboundedPreceding, 0)
    df = df.withColumn("_prev_pos", F.last(known, ignorenulls=True).over(before))

    lat, lon = F.radians(col("lat")), F.radians(col("lon"))
    prev_lat = F.radians(col("_prev_pos.lat"))
    prev_lon = F.radians(col("_prev_pos.lon"))
    a = (
        F.sin((lat - prev_lat) / 2) ** 2
        + F.cos(prev_lat) * F.cos(lat) * F.sin((lon - prev_lon) / 2) ** 2
    )
    distance_km = 6371 * 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
    # No position here, or none yet before it: nothing is added. The same
    # formula as the v0.0.2 measure, so a track with every position known
    # gives the same result.
    df = df.withColumn("segment_distance_nm", F.coalesce(distance_km / 1.852, lit(0.0)))
    df = df.withColumn("cumulative_distance_nm", f_sum("segment_distance_nm").over(running))
    return df.drop("_prev_pos")


# ======================================================================
# Main orchestrator class
# ======================================================================


class FlightEventProcessor:
    """Step 04: detect the flight events and measurements of a day.

    Combines every event family (see the module description), gives each
    event its id and version, and writes ``opdi_flight_events`` and
    ``opdi_measurements``, partitioned by the day the track started.

    Args:
        spark: Active Spark session.
        config: OPDI configuration.
        log_dir: Folder of the progress logs.

    Example:
        >>> processor = FlightEventProcessor(spark, config)
        >>> processor.process_day(date(2024, 6, 12))
    """

    def __init__(
        self,
        spark: SparkSession,
        config: OPDIConfig,
        log_dir: str = "OPDI_live/logs",
    ):
        """Prepare the processor; see the class description for the arguments."""
        self.spark = spark
        self.config = config
        # All thresholds of the step come from here.
        self.events = getattr(config, "events", None) or EventConfig()
        self.storage = StorageManager(spark, config)
        self.project = config.project.project_name
        self.log_dir = log_dir

        self._log_paths = {
            "horizontal": os.path.join(log_dir, "04_osn-flight_event-horizontal-etl-log.parquet"),
            "vertical": os.path.join(log_dir, "04_osn-flight_event-vertical-etl-log.parquet"),
            "hexaero": os.path.join(log_dir, "04_osn-flight_event-hexaero_airport-log.parquet"),
            "seen": os.path.join(log_dir, "04_osn-flight_event-first_seen_last_seen-log.parquet"),
            "navigation": os.path.join(log_dir, "04_osn-flight_event-navigation-log.parquet"),
        }

        os.makedirs(log_dir, exist_ok=True)

    def _load_processed(self, key: str) -> List[date]:
        """The batches the progress log ``key`` records as processed."""
        path = self._log_paths[key]
        if os.path.isfile(path):
            return pd.read_parquet(path).months.to_list()
        return []

    def _mark_processed(self, key: str, month: date) -> None:
        """Record ``month`` as processed in the progress log ``key``."""
        processed = self._load_processed(key)
        if month not in processed:
            processed.append(month)
            pd.DataFrame({"months": processed}).to_parquet(self._log_paths[key])

    def _get_data_within_timeframe(
        self,
        table_name: str,
        month: date,
        time_col: str = "event_time",
        day: date = None,
    ) -> DataFrame:
        """The rows of ``table_name`` for one batch.

        ``day`` selects the tracks that started that day, whole; ``month``
        selects a time window. See :func:`opdi.utils.batching.filter_batch`.
        """
        from opdi.utils.batching import filter_batch

        df = self.storage.read_table(table_name)
        if day is not None:
            return filter_batch(df, day=day)
        return filter_batch(df, month=month, time_col=time_col)

    def _event_id(self, batch_id: str):
        """Identifier of an event row.

        With ``deterministic_event_ids`` (the default) the id is built from
        the event's own content (track, type, time), so processing a day
        again gives the same ids. Otherwise (release v0.0.2) it is the batch
        id followed by ``monotonically_increasing_id()``, which depends on how
        Spark split the data.
        """
        if not self.events.deterministic_event_ids:
            return concat(lit(batch_id), monotonically_increasing_id().cast("string"))
        return F.sha2(
            concat_ws(
                "|",
                col("track_id"),
                col("type"),
                col("event_time").cast("string"),
                col("version"),
            ),
            256,
        ).substr(1, 32)

    def _measurement_id(self, batch_id: str, kind: str):
        """Identifier of a measurement row: its event's id plus ``kind``."""
        if not self.events.deterministic_event_ids:
            return concat(lit(batch_id + kind), monotonically_increasing_id().cast("string"))
        return concat(col("id_tmp"), lit(kind.rstrip("_")))

    def _with_aerodrome_geometry(self, sdf: DataFrame, month: date) -> DataFrame:
        """Add the flight's airports and each report's distance to them.

        Done once for the whole step: the airports' positions and elevations, and the
        distance of every report to each. Every family that needs them reads them from
        here: the phase floors (``elev_adep_ft``/``elev_ades_ft``), the runway passages,
        the go-arounds (``ades_lat``/``ades_lon``) and the level-offs
        (``dist_adep_nm``/``dist_ades_nm``), so they all use the same airports.
        :func:`~opdi.pipeline.level_segments.classify_level_offs` skips the 200 NM
        radius when the distance columns are absent.

        Returns the frame unchanged when the flight list is missing.
        """
        geo = attach_aerodrome_geometry(sdf, month, self.storage)
        return aerodrome_distances(geo)

    def _with_flight_id(self, sv: DataFrame) -> DataFrame:
        """Add the track's call sign, published by the runway milestones.

        The runway milestones publish it as ``osn_flight_id``. It uses the same rule as
        step 03 (:func:`resolve_flight_id`). A frame without call signs is returned
        unchanged, and ``osn_flight_id`` is then NULL.
        """
        if "flight_id" not in sv.columns:
            if "callsign" not in sv.columns:
                return sv
            sv = sv.withColumnRenamed("callsign", "flight_id")
        return resolve_flight_id(sv.fillna({"flight_id": ""}))

    def _runway_traversal_family(self, sv: DataFrame, month: date) -> Optional[DataFrame]:
        """The runway milestones derived from runway passages.

        ``sv`` holds the reports with the airport geometry and the call sign.
        The flight list's airports are added here, built by
        :func:`~opdi.pipeline.layout.flight_aerodrome_sets` as for the layout
        events.

        Returns ``None`` when a reference table it needs is missing, and the
        caller skips the family. The go-arounds are not part of it: they need
        none of these tables, as an approach abandoned in the air may never
        cross a runway.
        """
        if not (
            self.storage.table_exists("opdi_flight_list")
            and self.storage.table_exists("h3_runway_zones")
        ):
            return None
        thresholds = runway_thresholds(self.storage)
        if thresholds is None:
            return None

        apt_sets = flight_aerodrome_sets(month, self.storage)
        sv = sv.join(F.broadcast(apt_sets), sv.track_id == apt_sets.id, "inner").drop("id")
        sv.cache()

        grid = self.storage.read_table("h3_runway_zones")
        # ``h3_runway_zones`` covers every large and medium airport. Only the
        # airports this batch names can match, so the grid is cut to those
        # before it is broadcast to the executors.
        apt_present = sv.select(F.explode("apt").alias("_apt")).distinct()
        grid = grid.join(F.broadcast(apt_present), grid.apt_icao == apt_present._apt, "left_semi")
        traversals = runway_traversals(sv, grid, thresholds, self.events)
        return runway_milestones(sv, traversals, self.events)

    def _etl_flight_events_and_measures(
        self,
        sdf_input: DataFrame,
        batch_id: str,
        month: date,
        day: date = None,
        calc_vertical: bool = True,
        calc_horizontal: bool = True,
        calc_hexaero: bool = True,
        calc_seen: bool = True,
        calc_navigation: bool = True,
    ) -> None:
        """Detect the events and measurements of one batch and write them.

        Args:
            sdf_input: Track reports of the batch.
            batch_id: Prefix of the ids (with non-deterministic ids).
            month: Month (or day) being processed.
            calc_vertical: Detect the flight-level crossings.
            calc_horizontal: Detect the phase events and the families built
                on them.
            calc_hexaero: Detect the airport layout events.
            calc_seen: Detect first seen and last seen.
            calc_navigation: Detect holding, point merge and FIR crossings
                (each also subject to its ``emit_*`` setting).
            day: The day, when a single day is processed.
        """
        sdf_input = add_distance_measure(sdf_input, self.events.distance_from_last_known_position)
        sdf_input = add_time_measure(sdf_input)
        # The piece of its track each report belongs to (0 for a track without
        # holes), so that no detector pairs or interpolates reports across a
        # hole.
        sdf_input = add_piece(sdf_input, self.events.track_hole_seconds)
        sdf_input.cache()

        # One airport join for the whole step; see _with_aerodrome_geometry.
        #
        # Not cached: ``sdf_input`` is, and caching both would hold two copies
        # of the batch. This frame is cheap to derive again (a broadcast join
        # and two distances); ``_runway_traversal_family`` caches its own
        # smaller copy for the time it uses it.
        geo = self._with_aerodrome_geometry(sdf_input, month)

        # The level segments are detected once and shared by the level-offs
        # and the vertical profile tops, which are defined on the same
        # segments; the tops are likewise computed once.
        segments = None
        tops = None
        if calc_vertical and (self.events.emit_level_offs or self.events.emit_pru_tops):
            # Per piece: a level segment cannot run through a hole. The piece
            # column is dropped afterwards because tops and level-offs join on
            # the track.
            segments = LEVEL_ARMS[self.events.level_method](
                geo, self.events, partition_cols=("track_id", PIECE)
            ).drop(PIECE)
            segments.cache()
            if self.events.emit_pru_tops or (
                self.events.emit_level_offs and self.events.level_anchor == "pru"
            ):
                tops = pru_tops(geo, segments, self.events)
                tops.cache()

        df_events = None
        #: The go-around rows, if the runway family computed them: the holding
        #: family drops an episode next to one.
        df_go_arounds = None
        #: The take-off and landing times found so far, which bound the ground
        #: parts the block times are read from.
        runway_frames: List[DataFrame] = []

        def keep_runway(frame: Optional[DataFrame]) -> Optional[DataFrame]:
            # Cached because the block family reads these rows and the write
            # reads them again; both read the same rows.
            if frame is None or not self.events.block_times_on_ground_only:
                return frame
            frame = frame.cache()
            runway_frames.append(frame)
            return frame

        def add(frame: Optional[DataFrame]) -> None:
            nonlocal df_events
            if frame is None:
                return
            # Some families carry their own ``version`` column (holding, point
            # merge, FIR); for the others it is added as NULL, and the shared
            # version is filled in below.
            df_events = (
                frame
                if df_events is None
                else df_events.unionByName(frame, allowMissingColumns=True)
            )

        if calc_horizontal:
            print(f"Calculating horizontal events (phase) for batch: {batch_id}")
            add(calculate_horizontal_segment_events(geo, self.events))

        if calc_vertical:
            print(f"Calculating vertical events (FL crossings) for batch: {batch_id}")
            # The v0.0.2 settings run the release v0.0.2 crossing method.
            if self.events.crossing_all_occurrences:
                add(calculate_threshold_crossing_events(sdf_input, self.events))
            else:
                add(calculate_vertical_crossing_events(sdf_input))

            if df_events is not None and self.events.emit_level_offs:
                add(
                    calculate_level_off_events(
                        geo, df_events, self.events, segments=segments, tops=tops
                    )
                )

            if self.events.emit_pru_tops:
                add(pru_top_events(geo, segments, self.events, tops=tops))

            # ATOT/ALDT from the runway window and the runway milestones
            # airborne/touchdown give the same two moments by different means.
            # The runway milestones need reports on or just above the runway,
            # which few receivers see, so they cover few airports; the runway
            # window reads the initial climb and final descent, seen almost
            # everywhere. Both are kept; with the merge, ATOT/ALDT take the
            # runway milestone where there is one. Off with the v0.0.2
            # settings (emit_runway_events).
            if self.events.emit_runway_events:
                add(
                    keep_runway(
                        calculate_runway_events(sdf_input, month, self.storage, self.events)
                    )
                )

            add(calculate_ring_crossing_events(sdf_input, month, self.storage, self.events))

        if calc_hexaero:
            print(f"Calculating airport events for batch: {batch_id}")
            df_hexaero = calculate_airport_events(sdf_input, month, self.storage, self.events)
            add(df_hexaero)

            if self.events.emit_runway_milestones:
                print(f"Calculating runway milestones for batch: {batch_id}")
                rwy_sv = self._with_flight_id(geo)
                add(keep_runway(self._runway_traversal_family(rwy_sv, month)))
                # Independent of the runway passages and of the layout table: a
                # go-around may never touch a runway, so it needs only the
                # destination airport.
                if "ades_lat" in rwy_sv.columns:
                    df_go_arounds = go_arounds(rwy_sv, self.events)
                    add(df_go_arounds)

            if self.events.emit_block_events:
                if self.events.block_times_on_ground_only:
                    # ``geo`` carries the airport elevations of the ground test.
                    runway = None
                    for frame in runway_frames:
                        part = frame.select("track_id", "type", "event_time")
                        runway = part if runway is None else runway.unionByName(part)
                    add(calculate_block_events(geo, df_hexaero, self.events, runway))
                else:
                    add(calculate_block_events(sdf_input, df_hexaero, self.events))

        if (
            calc_navigation
            and not calc_hexaero
            and df_go_arounds is None
            and self.events.emit_holding
            and self.events.emit_runway_milestones
        ):
            # A run that adds only the holding, point merge and FIR families
            # still needs the go-arounds, which the holding family checks
            # against.
            rwy_sv = self._with_flight_id(geo)
            if "ades_lat" in rwy_sv.columns:
                df_go_arounds = go_arounds(rwy_sv, self.events)

        if calc_seen:
            print(f"Calculating first_seen/last_seen events for batch: {batch_id}")
            add(calculate_firstseen_lastseen_events(sdf_input))

        if calc_navigation:
            ev = self.events
            if ev.emit_holding:
                print(f"Calculating holding events for batch: {batch_id}")
                add(calculate_holding_events(geo, ev, go_arounds=df_go_arounds))
            if ev.emit_point_merge:
                print(f"Calculating point merge events for batch: {batch_id}")
                pms_ref = point_merge_systems_for_day(self.spark, day or month)
                add(calculate_point_merge_events(geo, pms_ref, ev))
            if ev.emit_fir_crossings:
                print(f"Calculating FIR crossings for batch: {batch_id}")
                add(calculate_fir_events(sdf_input, load_fir_reference(self.storage), ev))

        if df_events is None:
            return

        df_events.cache()

        # One event per milestone (merge_duplicate_milestones), before the ids
        # are made: the id includes the type. Done on the cached frame, which
        # the merge reads twice.
        df_events = merge_milestone_duplicates(df_events, self.events)
        df_events = df_events.withColumn("source", lit("OSN"))
        # Versions: a family that writes its own ``version`` keeps it; all other
        # events get the shared events version (``events_v0.3.0`` by default).
        if "version" not in df_events.columns:
            df_events = df_events.withColumn("version", lit(None).cast("string"))
        df_events = df_events.withColumn(
            "version", F.coalesce(col("version"), lit(self.events.events_version))
        )

        df_events = df_events.withColumn("id_tmp", self._event_id(batch_id)).select(
            col("id_tmp"),
            col("track_id"),
            col("type"),
            col("event_time"),
            col("lon"),
            col("lat"),
            col("altitude_ft"),
            col("source"),
            col("version"),
            col("info"),
            col("cumulative_distance_nm"),
            col("cumulative_time_s"),
        )

        # The day each track starts, from the tracks themselves.
        #
        # It is the day folder of the events and measurements written below,
        # not the date of each event: a flight departing at 22:50 has events
        # after midnight, and they belong to the day that processed its
        # track, so each day can be processed again on its own.
        track_day = sdf_input.groupBy("track_id").agg(F.to_date(F.min("event_time")).alias("dof"))
        df_events = df_events.join(track_day, on="track_id", how="left")

        # Whether this call builds the whole day or adds families to it.
        # ``process_month`` computes only the families its progress log lists
        # as missing; overwriting the day with those would delete the others,
        # so a partial call appends.
        full_rebuild = all(
            [calc_vertical, calc_horizontal, calc_hexaero, calc_seen, calc_navigation]
        )
        # A day-scoped batch writes exactly one partition, and it is the day.
        _partition_values = [{"dof": day}] if day is not None else None
        write_mode = "overwrite" if full_rebuild else "append"

        # Write milestones (flight events)
        df_milestones = df_events.select(
            col("id_tmp").alias("id"),
            col("track_id").alias("flight_id"),
            col("type"),
            col("event_time"),
            col("lon").alias("longitude"),
            col("lat").alias("latitude"),
            col("altitude_ft").alias("altitude"),
            col("source"),
            col("version"),
            col("info"),
            col("dof"),
        )
        df_milestones = df_milestones.repartition("dof").orderBy("type", "version")
        self.storage.write_table(
            df_milestones,
            "opdi_flight_events",
            mode=write_mode,
            partition_by=["dof"],
            # Known when a single day is being processed; None falls back to
            # probing, which a month-scoped run still needs.
            partition_values=_partition_values,
        )

        # Write measurements (distance + time)
        df_dist = (
            df_events.withColumn("type", lit("Distance flown (NM)"))
            .withColumn("version", lit(distance_version(self.events)))
            .withColumn("id", self._measurement_id(batch_id, "_d_"))
            .select(
                col("id"),
                col("id_tmp").alias("milestone_id"),
                col("type"),
                col("cumulative_distance_nm").alias("value"),
                col("version"),
                col("dof"),
            )
        )

        df_time = (
            df_events.withColumn("type", lit("Time Passed (s)"))
            .withColumn("version", lit(TIME_VERSION))
            .withColumn("id", self._measurement_id(batch_id, "_t_"))
            .select(
                col("id"),
                col("id_tmp").alias("milestone_id"),
                col("type"),
                col("cumulative_time_s").alias("value"),
                col("version"),
                col("dof"),
            )
        )

        df_measurements = df_dist.unionByName(df_time)
        extra = extra_measurement_rows(df_events)
        if extra is not None:
            df_measurements = df_measurements.unionByName(extra)
        df_measurements = df_measurements.repartition("dof").orderBy("type", "version")
        # Measurements have no time of their own, so their day folder is the
        # ``dof`` of their event's track.
        self.storage.write_table(
            df_measurements,
            "opdi_measurements",
            mode=write_mode,
            partition_by=["dof"],
            # Known when a single day is being processed; None falls back to
            # probing, which a month-scoped run still needs.
            partition_values=_partition_values,
        )

    def process_day(self, day: date, skip_if_processed: bool = True) -> None:
        """Detect the events of the tracks that started on ``day``.

        Step 02 writes each track in the day folder of its first report, so
        this reads whole tracks, including the part of a late departure after
        midnight.
        """
        self.process_month(day, skip_if_processed=skip_if_processed, day=day)

    def process_month(self, month: date, skip_if_processed: bool = True, day: date = None) -> None:
        """Detect the events of a month, or of one day when ``day`` is given.

        Args:
            month: Month to process.
            skip_if_processed: Skip the event families the log records as
                processed; ``False`` computes every family again.
            day: Process this day only.
        """
        print(f"Processing flight events for month: {month}")

        _batch = day if day is not None else month

        # ``skip_if_processed=False`` means compute again: every family flag is
        # set, whatever the progress log says.
        _redo = not skip_if_processed
        calc_horizontal = _redo or _batch not in self._load_processed("horizontal")
        calc_vertical = _redo or _batch not in self._load_processed("vertical")
        calc_hexaero = _redo or _batch not in self._load_processed("hexaero")
        calc_seen = _redo or _batch not in self._load_processed("seen")
        calc_navigation = _redo or _batch not in self._load_processed("navigation")

        if not any([calc_horizontal, calc_vertical, calc_hexaero, calc_seen, calc_navigation]):
            if skip_if_processed:
                print("All events for this month already processed.")
                return

        # Read the cleaned tracks (step 02a) when configured; without them,
        # fall back to the uncleaned tracks, as cleaning can be off.
        source = "osn_tracks"
        if self.events.feeds_from_clean_tracks and self.storage.table_exists("osn_tracks_clean"):
            source = "osn_tracks_clean"
        elif self.events.feeds_from_clean_tracks:
            print("osn_tracks_clean not found; falling back to raw osn_tracks.")
        print(f"Reading tracks from: {source}")

        sdf_input = (
            self._get_data_within_timeframe(source, month, day=day).select(*TRACK_COLUMNS).cache()
        )

        batch_id = month.strftime("%Y%m%d") + "_"

        self._etl_flight_events_and_measures(
            sdf_input,
            batch_id=batch_id,
            month=month,
            day=day,
            calc_vertical=calc_vertical,
            calc_horizontal=calc_horizontal,
            calc_hexaero=calc_hexaero,
            calc_seen=calc_seen,
            calc_navigation=calc_navigation,
        )

        # Update progress logs
        # Keyed on the batch (the day, when one day is processed), so that one
        # day does not mark its whole month as done.
        batch = day if day is not None else month
        if calc_horizontal:
            self._mark_processed("horizontal", batch)
        if calc_vertical:
            self._mark_processed("vertical", batch)
        if calc_hexaero:
            self._mark_processed("hexaero", batch)
        if calc_seen:
            self._mark_processed("seen", batch)
        if calc_navigation:
            self._mark_processed("navigation", batch)

        self.spark.catalog.clearCache()

    def process_date_range(
        self,
        start_month: date,
        end_month: date,
        skip_if_processed: bool = True,
    ) -> None:
        """Detect the events of a range of months.

        Args:
            start_month: First month to process.
            end_month: Last month to process.
            skip_if_processed: Skip months and families already processed.

        Example:
            >>> processor = FlightEventProcessor(spark, config)
            >>> processor.process_date_range(date(2024, 1, 1), date(2024, 6, 1))
        """
        months = generate_months(start_month, end_month)
        print(f"Processing flight events for {len(months)} months...")

        for month in months:
            self.process_month(month, skip_if_processed)

        print(f"Flight event processing complete for {start_month} to {end_month}.")

    def create_tables_if_not_exist(self) -> None:
        """Create the Iceberg tables of the events and measurements if missing."""
        today = datetime.today().strftime("%d %B %Y")

        events_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.project}`.`opdi_flight_events` (
            id STRING COMMENT 'Unique event identifier',
            flight_id STRING COMMENT 'Track/flight identifier',
            type STRING COMMENT 'Event type (take-off, landing, level-start, etc.)',
            event_time TIMESTAMP COMMENT 'Event timestamp',
            longitude DOUBLE COMMENT 'Event longitude',
            latitude DOUBLE COMMENT 'Event latitude',
            altitude DOUBLE COMMENT 'Event altitude in feet',
            source STRING COMMENT 'Data source (OSN)',
            version STRING COMMENT 'Processing version',
            info STRING COMMENT 'Additional info (JSON for airport events)'
        )
        USING iceberg
        PARTITIONED BY (type, version)
        COMMENT 'OPDI flight events (milestones). Last updated: {today}.'
        """

        measurements_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.project}`.`opdi_measurements` (
            id STRING COMMENT 'Unique measurement identifier',
            milestone_id STRING COMMENT 'Associated event identifier',
            type STRING COMMENT 'Measurement type (Distance flown, Time Passed)',
            value DOUBLE COMMENT 'Measurement value',
            version STRING COMMENT 'Processing version'
        )
        USING iceberg
        PARTITIONED BY (type, version)
        COMMENT 'OPDI measurements linked to flight events. Last updated: {today}.'
        """

        self.storage.create_table(events_sql)
        print(f"Table {self.project}.opdi_flight_events created/verified.")

        self.storage.create_table(measurements_sql)
        print(f"Table {self.project}.opdi_measurements created/verified.")
