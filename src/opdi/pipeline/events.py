"""
Flight events and measurements ETL module.

Detects and records flight events (milestones) from track data.

Event types:

* Horizontal segment events -- flight phases (GND, CL, DE, CR, LVL),
  top-of-climb, top-of-descent, take-off, landing using fuzzy logic
* Vertical crossing events -- flight level crossings (FL50, FL70, FL100, FL245)
* Airport events -- entry/exit of runway, taxiway, apron via H3 layout matching
* First/last seen events per track

Also produces measurement records (distance flown, time passed) linked
to each event.

Ported from ``OPDI-live/python/v2.0.0/04_opdi_flight_events_etl.py``.
"""

import os
from datetime import date, datetime
from typing import List, Optional

import pandas as pd

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    abs as f_abs,
    avg,
    col,
    concat,
    concat_ws,
    explode,
    filter,
    lag,
    lead,
    lit,
    max as f_max,
    min as f_min,
    monotonically_increasing_id,
    row_number,
    split,
    struct,
    sum as f_sum,
    to_json,
    to_timestamp,
    when,
)
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

from opdi.config import EventConfig, OPDIConfig
from opdi.pipeline.crossings import flight_level_crossings, ring_crossings
from opdi.pipeline.flights import bearing_deg, haversine_nm, resolve_flight_id
from opdi.pipeline.level_segments import LEVEL_ARMS, classify_level_offs
from opdi.pipeline.ground import block_times, movement_window
from opdi.pipeline.runway_ops import go_arounds, runway_milestones, runway_traversals
from opdi.pipeline.runways import detect_runway_movements, runway_thresholds
from opdi.pipeline.vertical_pru import (
    aerodrome_distances,
    attach_aerodrome_geometry,
    pru_top_events,
    pru_tops,
)
from opdi.utils.datetime_helpers import generate_months, get_start_end_of_month
from opdi.utils.storage import StorageManager

# ``calculate_airport_events`` (with its ``LEGACY_EVENTS_VERSION`` guard and
# the ``height_above_field_ft`` helper it uses) moved to ``layout.py``
# (Task 4): the gate it applies is now height above field elevation rather
# than uncorrected pressure altitude, sharing the helpers ``elevation.py``
# (Task 2) provides rather than owning a second copy of that arithmetic.
# Imported here, rather than left only in ``layout.py``, so
# ``FlightEventProcessor`` and this module's existing callers/tests keep
# importing these names from ``opdi.pipeline.events``.
from opdi.pipeline.layout import (  # noqa: E402
    LEGACY_EVENTS_VERSION,
    calculate_airport_events,
    flight_aerodrome_sets,
    height_above_field_ft,
)


# ======================================================================
# Fuzzy membership functions for flight phase classification
# ======================================================================

def zmf(column, a, b):
    """Zero-order membership function (Z-shaped). Returns 1 below a, 0 above b."""
    return F.when(column <= a, 1).when(column >= b, 0).otherwise((b - column) / (b - a))


def gaussmf(column, mean, sigma):
    """Gaussian membership function. Peak at mean, width controlled by sigma."""
    return F.exp(-((column - mean) ** 2) / (2 * sigma ** 2))


def smf(column, a, b):
    """S-shaped membership function. Returns 0 below a, 1 above b."""
    return F.when(column <= a, 0).when(column >= b, 1).otherwise((column - a) / (b - a))


# ======================================================================
# Event calculation functions
# ======================================================================

def _smooth_phase(df: DataFrame, twindow_seconds: float) -> DataFrame:
    """Majority-smooth the per-sample phase label over a time window.

    OpenAP labels phases per sample and then calls ``phaselabel(twindow=60)``,
    which takes the majority label over a 60 second window. The OPDI port
    carried the membership functions across faithfully and dropped the
    smoothing, so phases were decided per state vector at 5 s spacing with no
    temporal aggregation whatever -- one misclassified sample is enough to
    inject a spurious ``level-start``/``level-end`` pair, or to destroy a
    ``take-off`` by breaking the GND->CL adjacency the detector looks for.

    Ties keep the unsmoothed label. Preferring the incumbent is what makes this
    a de-flicker rather than a relabel: a sample only moves when the window
    actually disagrees with it.
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


def calculate_threshold_crossing_events(
    sdf_input: DataFrame, config: Optional[EventConfig] = None
) -> DataFrame:
    """Flight level crossings via the hysteresis detector.

    The published :func:`calculate_vertical_crossing_events` is left untouched
    beside this one, and ``EventConfig.legacy()`` still routes to it, so a
    re-processed month reproduces ``events_v0.0.2`` exactly rather than
    approximately.
    """
    config = config or EventConfig()

    src = sdf_input.select(
        "track_id", "lat", "lon", "event_time", "baro_altitude_c",
        "cumulative_distance_nm", "cumulative_time_s",
    )
    # The cumulative measures are interpolated to the crossing alongside the
    # position: a measurement attached to an interpolated instant but read off
    # a neighbouring sample would disagree with its own event.
    crossings = flight_level_crossings(
        src.withColumn("cumulative_distance_nm", col("cumulative_distance_nm").cast("double"))
           .withColumn("cumulative_time_s", col("cumulative_time_s").cast("double")),
        config,
        interpolate_cols=("lat", "lon", "cumulative_distance_nm", "cumulative_time_s"),
    )

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
    """ICAO level-offs in climb (KPI17) and descent (KPI19).

    Emitted as a *separate* family from ``level-start``/``level-end``. Those
    come from the fuzzy phase classifier and answer "does this look like level
    flight"; ICAO asks a geometric question about a band anchored at the
    segment's own start. Both are published and they are not interchangeable --
    the paper has to say so.

    Two configuration choices reach this function:

    * ``level_method`` selects which arm of :data:`LEVEL_ARMS` detects the
      segments -- the anchored band (``icao``) or PRU's rolling window
      (``pru``). Passing ``segments`` reuses a frame the caller already built,
      which is how ``FlightEventProcessor`` avoids detecting the same segments
      twice for this family and for the PRU tops. ``tops`` does the same for
      the PRU tops themselves, which are otherwise computed once here and once
      in ``pru_top_events`` -- the same windowed pass over the month's state
      vectors, twice per rung.
    * ``level_anchor`` selects the tops the classification hangs from:
      ``"phase"`` takes them from the horizontal detector's own output, so the
      two families cannot disagree about where cruise began (v0.1.0's
      behaviour); ``"pru"`` takes ToC-CCO and ToD-CDO, which is the anchor
      PRU's own definition of a level-off is written against.

    ``sdf_input`` should already carry ``elev_adep_ft``/``elev_ades_ft`` and
    ``dist_adep_nm``/``dist_ades_nm`` -- the floors and the 200 NM radius bind
    to those columns, and :func:`classify_level_offs` treats an *absent*
    distance column as "filter not applicable", so an un-enriched frame gets
    the unrestricted classification silently. The processor attaches them once
    for the whole step.
    """
    config = config or EventConfig()

    segs = segments if segments is not None else LEVEL_ARMS[config.level_method](
        sdf_input, config
    )

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
        phase_tops = horizontal_events.filter(
            col("type").isin("top-of-climb", "top-of-descent")
        )
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

    # A flight with no cruise has no climb or descent phase to attribute a
    # level-off to; ICAO's exclusion box is defined against the TOC altitude.
    segs = segs.filter(col("toc_time").isNotNull() & col("tod_time").isNotNull())

    labelled = classify_level_offs(
        segs, config,
        toc_time=col("toc_time"), tod_time=col("tod_time"),
        toc_altitude_ft=col("toc_alt"), tod_altitude_ft=col("tod_alt"),
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
        col("start_time").alias("event_time"), *common,
    )
    ends = labelled.select(
        concat(lit("level-off-"), leg, lit("-end")).alias("type"),
        col("end_time").alias("event_time"), *common,
    )
    return starts.unionByName(ends)


def calculate_block_events(
    sdf_input: DataFrame,
    airport_events: DataFrame,
    config: Optional[EventConfig] = None,
) -> Optional[DataFrame]:
    """Off-block (T04) and on-block (T21).

    Published as ``off-block``/``on-block`` under the A-CDM vocabulary and as
    ``AOBT``/``AIBT`` without it. One detector, two names: the flag renames the
    output, it does not change what was detected. Keyed on
    ``emit_runway_milestones`` rather than on the version string so that the
    V4 ladder's baseline rung, which reconstructs v0.1.0, keeps the names that
    rung published.

    Anchored on the ``exit-parking_position``/``entry-parking_position`` events
    step 04 already emits from ``hexaero_airport_layouts``, so no OSM query is
    needed at runtime -- the reason traffic's own parking-position path cannot
    be used on an offline executor.

    Expect coverage, not accuracy, to be the limitation: an aircraft on a stand
    is often not received at all.
    """
    config = config or EventConfig()
    movements = movement_window(sdf_input, config)
    blocks = block_times(movements, airport_events)
    if blocks is None:
        return None

    common = [
        lit(None).cast("double").alias("lon"),
        lit(None).cast("double").alias("lat"),
        lit(None).cast("double").alias("altitude_ft"),
        lit(None).cast("double").alias("cumulative_distance_nm"),
        lit(None).cast("long").alias("cumulative_time_s"),
    ]
    off_type = "off-block" if config.emit_runway_milestones else "AOBT"
    on_type = "on-block" if config.emit_runway_milestones else "AIBT"
    aobt = blocks.filter(col("aobt").isNotNull()).select(
        col("track_id"), lit(off_type).alias("type"), col("aobt").alias("event_time"),
        *common,
        to_json(struct(col("stand_exit").alias("stand_exit"))).alias("info"),
    )
    aibt = blocks.filter(col("aibt").isNotNull()).select(
        col("track_id"), lit(on_type).alias("type"), col("aibt").alias("event_time"),
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
    """ATOT (T08) and ALDT (T17), with the runway that served them.

    ``take-off`` and ``landing`` keep their published type strings and their
    fuzzy-phase derivation; these are new types alongside them. The two answer
    different questions -- the published pair asks where the phase changed, this
    one asks which runway the movement used and when it left or met it -- and a
    consumer needs to be able to tell them apart rather than find one silently
    replaced.

    **Superseded when ``emit_runway_milestones`` is on.** These times are the
    extreme *sample* of a detection window, which is what gave ``ATOT`` a +19 s
    median bias; ``runway_ops`` answers the same question by interpolating the
    15 ft crossing. The processor therefore calls this only when the A-CDM
    family is off, so ``airborne``/``touchdown`` have exactly one source per
    configuration.
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
        .filter((col("dof") >= to_timestamp(lit(start_ts))) & (col("dof") < to_timestamp(lit(end_ts))))
        .select(col("id").alias("track_id"), col("adep"), col("ades"))
    )
    ends = fl.select(
        "track_id", col("adep").alias("apt_ident"), lit("departure").alias("role")
    ).unionByName(
        fl.select("track_id", col("ades").alias("apt_ident"), lit("arrival").alias("role"))
    ).filter(col("apt_ident").isNotNull() & (col("apt_ident") != ""))

    apt = storage.read_table("oa_airports").select(
        col("ident").alias("_ident"),
        col("latitude_deg").cast("double").alias("apt_lat"),
        col("longitude_deg").cast("double").alias("apt_lon"),
        col("elevation_ft").cast("double").alias("apt_elevation_ft"),
    )
    ends = ends.join(F.broadcast(apt), ends.apt_ident == col("_ident"), "inner").drop("_ident")

    moves = detect_runway_movements(sdf_input, ends, thresholds, config)

    # The departure's time is its first surviving sample and the arrival's its
    # last: a lift-off and a touchdown proxy respectively.
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
                col("apt_ident").alias("apt_icao"),
                col("role").alias("role"),
                col("bearing_error").alias("bearing_error_deg"),
                col("n_samples").alias("n_samples"),
            )
        ).alias("info"),
    )


def calculate_ring_crossing_events(
    sdf_input: DataFrame,
    month: date,
    storage: "StorageManager",
    config: Optional[EventConfig] = None,
) -> Optional[DataFrame]:
    """Crossings of the ASMA rings around a flight's own ADEP and ADES.

    ICAO defines both indicators these serve against the flight's *own*
    aerodromes -- KPI08's ASMA is a cylinder around the destination, KPI05's
    reference area a cylinder around origin and destination -- so the rings are
    built from the flight list rather than from ``h3_airport_detection_zones``.
    That is also what APDF's ``C40_``/``C100_`` columns record: one crossing per
    movement, not one per aerodrome overflown.

    It is the cheaper construction as well. The zone table would multiply every
    sample by every aerodrome within 110 NM and then need its 30 NM
    ``max_radius_nm`` ceiling raised in two places; this multiplies each sample
    by at most two, and needs no reference-table change at all.

    Returns None when no radii are configured, so the caller can skip the union.
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
        .filter((col("dof") >= to_timestamp(lit(start_ts))) & (col("dof") < to_timestamp(lit(end_ts))))
        .select(col("id").alias("_fl_id"), col("adep"), col("ades"))
    )
    ends = fl.select("_fl_id", col("adep").alias("apt_ident")).unionByName(
        fl.select("_fl_id", col("ades").alias("apt_ident"))
    ).filter(col("apt_ident").isNotNull() & (col("apt_ident") != "")).distinct()

    apt = storage.read_table("oa_airports").select(
        col("ident").alias("_apt_ident"),
        col("latitude_deg").cast("double").alias("apt_lat"),
        col("longitude_deg").cast("double").alias("apt_lon"),
    )
    ends = ends.join(
        F.broadcast(apt), ends.apt_ident == col("_apt_ident"), "inner"
    ).drop("_apt_ident")

    work = sdf_input.select(
        "track_id", "lat", "lon", "event_time", "baro_altitude_c",
        "cumulative_distance_nm", "cumulative_time_s",
    ).join(F.broadcast(ends), col("track_id") == col("_fl_id"), "inner").drop("_fl_id")

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
            "lat", "lon", "flight_level",
            "cumulative_distance_nm", "cumulative_time_s",
        ),
    )

    # Bearing is computed *from* the interpolated crossing position, not
    # interpolated alongside it: a bearing is circular, and averaging 359 and 1
    # gives 180. From the aerodrome outwards, matching APDF's C40_BEARING.
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
    """
    Detect flight phase transitions and horizontal segment events.

    Uses fuzzy logic membership functions to classify each state vector
    into flight phases (GND, CL, DE, CR, LVL) based on altitude,
    rate of climb, and speed. Detects phase transitions to produce
    events: level-start, level-end, top-of-climb, top-of-descent,
    take-off, landing.

    Args:
        sdf_input: Input DataFrame with track data including
            baro_altitude_c, vert_rate, velocity.

    Returns:
        DataFrame of detected events with columns: track_id, type,
        event_time, lon, lat, altitude_ft, cumulative_distance_nm,
        cumulative_time_s, info.
    """
    config = config or EventConfig()

    wanted = [
        "track_id", "lat", "lon", "event_time", "baro_altitude_c",
        "vert_rate", "velocity", "cumulative_distance_nm", "cumulative_time_s",
    ]
    # Carried only when the caller attached them, so the detector stays a pure
    # column-in/column-out function and can be tested without storage.
    elevations = [c for c in ("elev_adep_ft", "elev_ades_ft") if c in sdf_input.columns]
    df = sdf_input.select(*wanted, *elevations)
    use_agl = config.phase_ground_above_field and len(elevations) == 2

    # Convert units: meters -> feet, m/s -> ft/min, m/s -> knots
    df = df.withColumn("alt", col("baro_altitude_c") * 3.28084)
    df = df.withColumn("roc", col("vert_rate") * 196.850394)
    df = df.withColumn("spd", col("velocity") * 1.94384)

    # Apply fuzzy membership functions. The constants are OpenAP's published
    # values; the two reachable from config are the ones with a measured or
    # suspected problem behind them (see EventConfig).
    if use_agl:
        # Height above field, at whichever end the aircraft is actually at.
        # A missing elevation coalesces to zero, i.e. to today's behaviour,
        # rather than removing the flight's phases altogether.
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
        """Minimum t-norm over a rule's membership functions.

        ``F.least`` *skips* NULLs, so a rule with a missing input silently
        becomes a minimum over fewer terms -- which can only raise its
        activation, letting an incomplete rule out-compete a complete one.
        Under ``phase_require_complete_rules`` a rule with any NULL input
        yields NULL instead, and ``F.greatest`` then ignores it: it abstains
        rather than winning on a technicality.
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
            col("rule_ground"), col("rule_climb"), col("rule_descent"),
            col("rule_cruise"), col("rule_level"),
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
        "track_id", "lat", "lon", "event_time", "cumulative_distance_nm",
        "cumulative_time_s", "altitude_ft", "roc_ft_min", "speed_kt", "flight_phase",
    )

    df.cache()

    # Detect phase transitions
    window_phase = Window.partitionBy("track_id").orderBy("event_time")
    window_cumulative = (
        Window.partitionBy("track_id")
        .orderBy("event_time")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

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
            (col("flight_phase").isin("CR", "LVL")) & (col("next_phase") != col("flight_phase")),
            F.array(lit("level-end")),
        )
    )

    # ``take-off`` and ``landing`` here are the *ground-contact* reading: the
    # sample at which the phase changed. ``runway_ops`` publishes the same two
    # physical events as ``airborne``/``touchdown``, interpolated to the 15 ft
    # crossing, and its ``landing`` is a third thing again -- the threshold
    # plane, ICAO T16. Emitting both families would put two answers to one
    # question in the table, and ``airborne`` would have two sources.
    #
    # Gated on the flag rather than on the version string because the V4
    # ladder's baseline rung reconstructs v0.1.0, which *did* publish this
    # pair; a version gate would strip them from the rung named after them.
    if not config.emit_runway_milestones:
        milestone_types = milestone_types.when(
            (col("prev_phase") == "GND") & (col("next_phase") == "CL"),
            F.array(lit("take-off")),
        ).when(
            (col("prev_phase") == "DE") & (col("flight_phase") == "GND"),
            F.array(lit("landing")),
        )

    df = df.withColumn("milestone_types", milestone_types.otherwise(F.array()))

    # Explode to one row per event
    df_exploded = df.select("*", explode(col("milestone_types")).alias("type"))
    df_exploded = df_exploded.filter("type IS NOT NULL")

    df_events = df_exploded.select(
        col("track_id"), col("type"), col("event_time"),
        col("lon"), col("lat"), col("altitude_ft"),
        col("cumulative_distance_nm"), col("cumulative_time_s"),
    )

    df_events = df_events.dropDuplicates(["track_id", "type", "event_time"])

    # Every top names the algorithm that produced it -- but only where there is
    # another algorithm to be told apart from. The stamp exists because
    # ``vertical_pru`` publishes a second pair of tops carrying
    # ``method: "pru"``; with that family switched off there is one definition
    # of "top of climb" in the table and nothing to disambiguate.
    #
    # Gated for a harder reason than tidiness. ``events_v0.0.2`` published
    # ``info = ""`` on these rows, and ``EventConfig.legacy()`` exists so that
    # re-processing a past month reproduces the release. An unconditional stamp
    # would have made every legacy top differ from the row it is supposed to
    # reproduce -- silently, because ``info`` is free-form and nothing compares
    # it. ``emit_pru_tops`` is off under ``legacy()`` and on by default, so the
    # gate is exactly the condition "is there a PRU pair to disambiguate from".
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
    """
    Detect flight level crossing events (FL50, FL70, FL100, FL245).

    For each track, identifies the first and last time the aircraft
    crosses each monitored flight level boundary.

    Args:
        sdf_input: Input DataFrame with baro_altitude_c.

    Returns:
        DataFrame of crossing events with columns: track_id, type,
        event_time, lon, lat, altitude_ft, cumulative_distance_nm,
        cumulative_time_s, info.
    """
    df = sdf_input.select(
        "track_id", "lat", "lon", "event_time", "baro_altitude_c",
        "vert_rate", "velocity", "cumulative_distance_nm", "cumulative_time_s",
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
        "track_id", "type", "event_time", "lon", "lat",
        "altitude_ft", "cumulative_distance_nm", "cumulative_time_s",
    )
    df_events = df_events.dropDuplicates([
        "track_id", "type", "event_time", "lon", "lat",
        "altitude_ft", "cumulative_distance_nm", "cumulative_time_s",
    ])
    df_events = df_events.withColumn("info", lit(""))

    return df_events


def calculate_firstseen_lastseen_events(sdf_input: DataFrame) -> DataFrame:
    """
    Calculate first_seen and last_seen events for each track.

    Args:
        sdf_input: Input DataFrame with track data.

    Returns:
        DataFrame with first/last seen events.
    """
    df = sdf_input.select(
        "track_id", "lat", "lon", "event_time", "baro_altitude_c",
        "vert_rate", "velocity", "cumulative_distance_nm", "cumulative_time_s",
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
        "track_id", "type", "event_time", "lon", "lat",
        "altitude_ft", "cumulative_distance_nm", "cumulative_time_s",
    )
    df_events = df_events.dropDuplicates([
        "track_id", "type", "event_time", "lon", "lat",
        "altitude_ft", "cumulative_distance_nm", "cumulative_time_s",
    ])
    df_events = df_events.withColumn("info", lit(""))

    return df_events


# ======================================================================
# Airport layout events -- moved to layout.py (Task 4); see the import at
# the top of this module for calculate_airport_events, height_above_field_ft
# and LEGACY_EVENTS_VERSION.
# ======================================================================


# ======================================================================
# Measurement helper functions
# ======================================================================

def add_time_measure(sdf_input: DataFrame) -> DataFrame:
    """Add cumulative time (seconds from track start) to each state vector."""
    window_spec = Window.partitionBy("track_id").orderBy("event_time")
    sdf_input = sdf_input.withColumn("min_event_time", f_min("event_time").over(window_spec))
    sdf_input = sdf_input.withColumn(
        "cumulative_time_s",
        col("event_time").cast("long") - col("min_event_time").cast("long"),
    )
    return sdf_input.drop("min_event_time")


def add_distance_measure(df: DataFrame) -> DataFrame:
    """
    Calculate segment and cumulative distance (NM) using Haversine formula.

    Uses native PySpark functions for distributed computation.

    Args:
        df: DataFrame with lat, lon, track_id, event_time columns.

    Returns:
        DataFrame with segment_distance_nm and cumulative_distance_nm.
    """
    window_lag = Window.partitionBy("track_id").orderBy("event_time")
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


# ======================================================================
# Main orchestrator class
# ======================================================================

class FlightEventProcessor:
    """
    Orchestrates the extraction of flight events and measurements.

    Combines horizontal (phase), vertical (FL crossing), airport, and
    first/last-seen event detection into a single processing pipeline.
    Writes results to opdi_flight_events and opdi_measurements tables.

    Args:
        spark: Active SparkSession.
        config: OPDI configuration object.
        log_dir: Directory for processing progress logs.

    Example:
        >>> processor = FlightEventProcessor(spark, config)
        >>> processor.process_date_range(date(2024, 1, 1), date(2024, 6, 1))
    """

    def __init__(
        self,
        spark: SparkSession,
        config: OPDIConfig,
        log_dir: str = "OPDI_live/logs",
    ):
        self.spark = spark
        self.config = config
        # One place the whole step reads its thresholds from. Before this,
        # every number in this module was an inline literal.
        self.events = getattr(config, "events", None) or EventConfig()
        self.storage = StorageManager(spark, config)
        self.project = config.project.project_name
        self.log_dir = log_dir

        self._log_paths = {
            "horizontal": os.path.join(log_dir, "04_osn-flight_event-horizontal-etl-log.parquet"),
            "vertical": os.path.join(log_dir, "04_osn-flight_event-vertical-etl-log.parquet"),
            "hexaero": os.path.join(log_dir, "04_osn-flight_event-hexaero_airport-log.parquet"),
            "seen": os.path.join(log_dir, "04_osn-flight_event-first_seen_last_seen-log.parquet"),
        }

        os.makedirs(log_dir, exist_ok=True)

    def _load_processed(self, key: str) -> List[date]:
        """Load processed months for a specific event type."""
        path = self._log_paths[key]
        if os.path.isfile(path):
            return pd.read_parquet(path).months.to_list()
        return []

    def _mark_processed(self, key: str, month: date) -> None:
        """Mark a month as processed for a specific event type."""
        processed = self._load_processed(key)
        if month not in processed:
            processed.append(month)
            pd.DataFrame({"months": processed}).to_parquet(self._log_paths[key])

    def _get_data_within_timeframe(
        self, table_name: str, month: date, time_col: str = "event_time"
    ) -> DataFrame:
        """Retrieve records within a monthly timeframe."""
        start_ts, end_ts = get_start_end_of_month(month)
        start_lit = to_timestamp(lit(start_ts))
        end_lit = to_timestamp(lit(end_ts))
        df = self.storage.read_table(table_name)
        return df.filter((col(time_col) >= start_lit) & (col(time_col) < end_lit))

    def _event_id(self, batch_id: str):
        """Identifier for an event row.

        ``monotonically_increasing_id()`` encodes the partition index, so the
        same event gets a different id on every run and two runs of one month
        cannot be reconciled. Worse, ``StorageManager.write_table`` defaults to
        append: re-processing a month adds a second copy of every event rather
        than replacing it, and nothing errors -- the table simply weighs each
        event twice.

        Deriving the id from the event's own identity makes a re-run
        idempotent in content, so duplicates are at least detectable and
        removable. It does not make the *write* idempotent; that needs
        overwrite semantics on the month's partition, which is a separate
        change to the write path.
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
        """Identifier for a measurement row, derived from its own milestone."""
        if not self.events.deterministic_event_ids:
            return concat(lit(batch_id + kind), monotonically_increasing_id().cast("string"))
        return concat(col("id_tmp"), lit(kind.rstrip("_")))

    def _with_aerodrome_geometry(self, sdf: DataFrame, month: date) -> DataFrame:
        """Attach the flight's own aerodromes, their positions and elevations,
        and the per-sample distance to each -- once, for the whole step.

        Every family that needs geometry reads it from here: the phase floors
        (``elev_adep_ft``/``elev_ades_ft``), the runway traversals (the same two
        plus ``apt``, added separately), ``go_arounds`` (``ades_lat``/
        ``ades_lon``), and the level machinery (``dist_adep_nm``/
        ``dist_ades_nm``). Attaching per family would mean four joins to the
        same two tables and four opportunities for them to disagree about which
        aerodrome a flight used.

        The distances matter more than they look:
        :func:`~opdi.pipeline.level_segments.classify_level_offs` treats an
        *absent* distance column as "radius not applicable", so a frame that
        never got them yields level-offs with the 200 NM bound silently
        unenforced rather than an error.

        Returns the frame unchanged when the flight list is missing --
        ``attach_aerodrome_geometry``'s own contract -- so a run without it
        degrades to the un-enriched behaviour instead of failing.
        """
        geo = attach_aerodrome_geometry(sdf, month, self.storage)
        return aerodrome_distances(geo)

    def _with_flight_id(self, sv: DataFrame) -> DataFrame:
        """The resolved callsign the runway family publishes as ``osn_flight_id``.

        Resolved by the same helper step 03 uses, for the reason
        ``calculate_airport_events`` documents at length: a second copy of this
        rule is how the production flight list and its benchmark came to
        disagree about it. A frame with no callsign at all is returned
        unchanged -- ``runway_ops`` reads the column through ``_col_or_null``
        and leaves ``osn_flight_id`` null rather than losing the family.
        """
        if "flight_id" not in sv.columns:
            if "callsign" not in sv.columns:
                return sv
            sv = sv.withColumnRenamed("callsign", "flight_id")
        return resolve_flight_id(sv.fillna({"flight_id": ""}))

    def _runway_traversal_family(
        self, sv: DataFrame, month: date
    ) -> Optional[DataFrame]:
        """The eight traversal-derived A-CDM milestones.

        ``sv`` is the geometry-enriched frame with ``flight_id`` already
        resolved. What is added here and nowhere else is the flight list's
        aerodrome array, built by the shared
        :func:`~opdi.pipeline.layout.flight_aerodrome_sets` so it is the
        identical array the layout family matches on.

        Returns ``None`` when a reference table this family cannot work without
        is absent, so the caller skips it rather than failing the step.
        **``go_arounds`` is deliberately not here**: it needs none of these
        tables, and routing it through this guard would make an approach
        abandoned at 400 ft depend on a runway polygon the aircraft never
        crossed -- exactly the dependency that detector exists to avoid.
        """
        if not (
            self.storage.table_exists("opdi_flight_list")
            and self.storage.table_exists("hexaero_airport_layouts")
        ):
            return None
        thresholds = runway_thresholds(self.storage)
        if thresholds is None:
            return None

        apt_sets = flight_aerodrome_sets(month, self.storage)
        sv = sv.join(
            F.broadcast(apt_sets), sv.track_id == apt_sets.id, "inner"
        ).drop("id")
        sv.cache()

        layouts = self.storage.read_table("hexaero_airport_layouts")
        traversals = runway_traversals(sv, layouts, thresholds, self.events)
        return runway_milestones(sv, traversals, self.events)

    def _etl_flight_events_and_measures(
        self,
        sdf_input: DataFrame,
        batch_id: str,
        month: date,
        calc_vertical: bool = True,
        calc_horizontal: bool = True,
        calc_hexaero: bool = True,
        calc_seen: bool = True,
    ) -> None:
        """
        Core ETL: calculate events and measurements, write to tables.

        Args:
            sdf_input: Input tracks DataFrame.
            batch_id: Batch identifier prefix for generated IDs.
            month: Month being processed.
            calc_vertical: Whether to calculate FL crossing events.
            calc_horizontal: Whether to calculate phase events.
            calc_hexaero: Whether to calculate airport events.
            calc_seen: Whether to calculate first/last seen events.
        """
        sdf_input = add_distance_measure(sdf_input)
        sdf_input = add_time_measure(sdf_input)
        sdf_input.cache()

        # One aerodrome join for the whole step; see _with_aerodrome_geometry.
        #
        # **Not cached here, deliberately.** ``sdf_input`` is, and caching both
        # would hold two copies of the month's state vectors -- the second
        # differing only by eight broadcast-joined columns. Of the two, this is
        # the one worth rederiving: it is a broadcast join plus two haversines
        # over an already-cached frame, all narrow and shuffle-free, whereas
        # ``sdf_input`` is two window functions and rebuilding *it* per
        # consumer would cost a shuffle each time.
        #
        # ``_runway_traversal_family`` does cache its own copy, and that is not
        # a contradiction of this: it caches the frame *after* the aerodrome
        # array join, which it consumes twice in quick succession -- once for
        # the traversals and once for the milestones -- and drops out of scope
        # with the step. What is avoided here is holding the wide frame alive
        # across every family in the step.
        geo = self._with_aerodrome_geometry(sdf_input, month)

        # The level segments are detected once and shared by the two families
        # that consume them -- the level-offs and the PRU tops, whose
        # relocation is defined against the very same segments. Detecting them
        # twice would be the same work done twice and, worse, would let the
        # relocation and the classification disagree.
        segments = None
        # The PRU tops, likewise: with ``level_anchor = "pru"`` the level-off
        # classification and the published ``top-of-climb-cco`` pair hang from
        # the same tops, and each was computing them independently -- one
        # windowed pass over the month's state vectors, done twice per rung.
        tops = None
        if calc_vertical and (self.events.emit_level_offs or self.events.emit_pru_tops):
            segments = LEVEL_ARMS[self.events.level_method](geo, self.events)
            segments.cache()
            if self.events.emit_pru_tops or (
                self.events.emit_level_offs and self.events.level_anchor == "pru"
            ):
                tops = pru_tops(geo, segments, self.events)
                tops.cache()

        df_events = None

        def add(frame: Optional[DataFrame]) -> None:
            nonlocal df_events
            if frame is None:
                return
            df_events = frame if df_events is None else df_events.unionByName(frame)

        if calc_horizontal:
            print(f"Calculating horizontal events (phase) for batch: {batch_id}")
            add(calculate_horizontal_segment_events(geo, self.events))

        if calc_vertical:
            print(f"Calculating vertical events (FL crossings) for batch: {batch_id}")
            # The published detector is kept, not adapted: under legacy() the
            # literal old code runs, so a re-processed month reproduces
            # events_v0.0.2 exactly rather than approximately.
            if self.events.crossing_all_occurrences:
                add(calculate_threshold_crossing_events(sdf_input, self.events))
            else:
                add(calculate_vertical_crossing_events(sdf_input))

            if df_events is not None and self.events.emit_level_offs:
                add(calculate_level_off_events(
                    geo, df_events, self.events, segments=segments, tops=tops
                ))

            if self.events.emit_pru_tops:
                add(pru_top_events(geo, segments, self.events, tops=tops))

            # ATOT/ALDT are the extreme sample of a detection window; the A-CDM
            # family interpolates the same two instants and publishes them as
            # airborne/touchdown. Exactly one of the two runs, so no
            # configuration reports a lift-off twice.
            if self.events.emit_runway_events and not self.events.emit_runway_milestones:
                add(calculate_runway_events(
                    sdf_input, month, self.storage, self.events
                ))

            add(calculate_ring_crossing_events(
                sdf_input, month, self.storage, self.events
            ))

        if calc_hexaero:
            print(f"Calculating airport events for batch: {batch_id}")
            df_hexaero = calculate_airport_events(
                sdf_input, month, self.storage, self.events
            )
            add(df_hexaero)

            if self.events.emit_runway_milestones:
                print(f"Calculating runway milestones for batch: {batch_id}")
                rwy_sv = self._with_flight_id(geo)
                add(self._runway_traversal_family(rwy_sv, month))
                # Independent of the traversals *and* of the layout table: a
                # go-around may never touch a runway polygon, so it is gated on
                # the destination geometry it actually reads and on nothing
                # else.
                if "ades_lat" in rwy_sv.columns:
                    add(go_arounds(rwy_sv, self.events))

            if self.events.emit_block_events:
                add(calculate_block_events(sdf_input, df_hexaero, self.events))

        if calc_seen:
            print(f"Calculating first_seen/last_seen events for batch: {batch_id}")
            add(calculate_firstseen_lastseen_events(sdf_input))

        if df_events is None:
            return

        df_events.cache()
        df_events = df_events.withColumn("source", lit("OSN"))
        df_events = df_events.withColumn("version", lit(self.events.events_version))

        df_events = df_events.withColumn("id_tmp", self._event_id(batch_id)).select(
            col("id_tmp"), col("track_id"), col("type"), col("event_time"),
            col("lon"), col("lat"), col("altitude_ft"), col("source"),
            col("version"), col("info"), col("cumulative_distance_nm"),
            col("cumulative_time_s"),
        )

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
        )
        df_milestones = df_milestones.repartition("type", "version").orderBy("type", "version")
        self.storage.write_table(df_milestones, "opdi_flight_events", mode="append")

        # Write measurements (distance + time)
        df_dist = (
            df_events.withColumn("type", lit("Distance flown (NM)"))
            .withColumn("version", lit("distance_v0.0.2"))
            .withColumn("id", self._measurement_id(batch_id, "_d_"))
            .select(
                col("id"),
                col("id_tmp").alias("milestone_id"),
                col("type"),
                col("cumulative_distance_nm").alias("value"),
                col("version"),
            )
        )

        df_time = (
            df_events.withColumn("type", lit("Time Passed (s)"))
            .withColumn("version", lit("time_v0.0.1"))
            .withColumn("id", self._measurement_id(batch_id, "_t_"))
            .select(
                col("id"),
                col("id_tmp").alias("milestone_id"),
                col("type"),
                col("cumulative_time_s").alias("value"),
                col("version"),
            )
        )

        df_measurements = df_dist.unionByName(df_time)
        df_measurements = df_measurements.repartition("type", "version").orderBy("type", "version")
        self.storage.write_table(df_measurements, "opdi_measurements", mode="append")

    def process_month(self, month: date, skip_if_processed: bool = True) -> None:
        """
        Process all flight events for a single month.

        Args:
            month: Month to process.
            skip_if_processed: Skip event types already processed.
        """
        print(f"Processing flight events for month: {month}")

        calc_horizontal = month not in self._load_processed("horizontal")
        calc_vertical = month not in self._load_processed("vertical")
        calc_hexaero = month not in self._load_processed("hexaero")
        calc_seen = month not in self._load_processed("seen")

        if not any([calc_horizontal, calc_vertical, calc_hexaero, calc_seen]):
            if skip_if_processed:
                print("All events for this month already processed.")
                return

        # Step 02a writes osn_tracks_clean, and until now only the flight list
        # (step 03) read it -- so no published event had ever been derived from
        # a cleaned trajectory. Fall back if the table is absent, because
        # cleaning is itself optional and a missing table should degrade to the
        # old behaviour rather than fail the step.
        source = "osn_tracks"
        if self.events.feeds_from_clean_tracks and self.storage.table_exists("osn_tracks_clean"):
            source = "osn_tracks_clean"
        elif self.events.feeds_from_clean_tracks:
            print("osn_tracks_clean not found; falling back to raw osn_tracks.")
        print(f"Reading tracks from: {source}")

        sdf_input = (
            self._get_data_within_timeframe(source, month)
            .select(
                "track_id", "lat", "lon", "event_time", "baro_altitude_c",
                "velocity", "vert_rate", "callsign", "icao24", "heading", "h3_res_12",
            )
            .cache()
        )

        batch_id = month.strftime("%Y%m%d") + "_"

        self._etl_flight_events_and_measures(
            sdf_input,
            batch_id=batch_id,
            month=month,
            calc_vertical=calc_vertical,
            calc_horizontal=calc_horizontal,
            calc_hexaero=calc_hexaero,
            calc_seen=calc_seen,
        )

        # Update progress logs
        if calc_horizontal:
            self._mark_processed("horizontal", month)
        if calc_vertical:
            self._mark_processed("vertical", month)
        if calc_hexaero:
            self._mark_processed("hexaero", month)
        if calc_seen:
            self._mark_processed("seen", month)

        self.spark.catalog.clearCache()

    def process_date_range(
        self,
        start_month: date,
        end_month: date,
        skip_if_processed: bool = True,
    ) -> None:
        """
        Process flight events for a range of months.

        Args:
            start_month: First month to process.
            end_month: Last month to process.
            skip_if_processed: Skip already processed months/event types.

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
        """Create opdi_flight_events and opdi_measurements Iceberg tables."""
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
