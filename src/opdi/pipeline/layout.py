"""Airport layout events: entry and exit of runways, taxiways, aprons and stands.

Part of step 04. Low reports of a flight are matched to the H3 cells
(resolution 12, about 20 m) of the layouts of its airports
(``hexaero_airport_layouts``, built from OpenStreetMap by step 00b), and
each passage through a part of the airport gives an ``entry-<part>`` and an
``exit-<part>`` event (for example ``entry-parking_position``). A report is
low enough when it is within a set height of the airport's elevation, or
flagged on the ground.
See https://www.opdi.aero/content/methods/surface-events.html.
"""

from datetime import date
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    concat_ws,
    lag,
    lit,
    split,
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
from pyspark.sql.window import Window

from opdi.config import EventConfig
from opdi.pipeline.elevation import attach_field_elevation, height_above_field_ft
from opdi.pipeline.flights import resolve_flight_id
from opdi.utils.datetime_helpers import get_start_end_of_month
from opdi.utils.storage import StorageManager

# The events version of release v0.0.2. A run that writes it reproduces that
# release, so the call sign of the track is not resolved for it (see
# below). ``EventConfig.legacy()`` sets it; ``tests/test_events_labelling.py``
# checks that the two agree.
from opdi.versions import LEGACY_EVENTS_VERSION


def flight_aerodrome_sets(month: date, storage: "StorageManager") -> DataFrame:
    """``id`` and ``apt``: the airports each flight of ``month`` may be at.

    The array holds ``adep``, ``ades`` and the two lists of other candidate
    airports, without the empty strings a missing airport leaves, so that a
    flight with an unknown ADES does not match every layout row with a blank
    airport code. Shared with the runway milestones, so both use the same
    airports for a flight.

    A missing flight list raises an error; the caller decides what that
    means.
    """
    start_ts, end_ts = get_start_end_of_month(month)
    flight_list = (
        storage.read_table("opdi_flight_list")
        .filter(
            (col("dof") >= to_timestamp(lit(start_ts))) & (col("dof") < to_timestamp(lit(end_ts)))
        )
        .select("id", "adep", "ades", "adep_p", "ades_p")
    )

    for c in ["adep", "ades", "adep_p", "ades_p"]:
        flight_list = flight_list.withColumn(c, when(col(c).isNull(), lit("")).otherwise(col(c)))

    return (
        flight_list.withColumn(
            "apt",
            F.concat(
                F.array(col("adep"), col("ades")),
                split(col("adep_p"), ", "),
                split(col("ades_p"), ", "),
            ),
        )
        .withColumn("apt", F.array_remove(col("apt"), ""))
        .select("id", "apt")
    )


def calculate_airport_events(
    sv: DataFrame,
    month: date,
    storage: "StorageManager",
    config: Optional[EventConfig] = None,
) -> DataFrame:
    """Entry and exit events of the parts of an airport, by H3 layout matching.

    Args:
        sv: Track reports.
        month: Month (or day) being processed, to read the flight list.
        storage: :class:`~opdi.utils.storage.StorageManager`.
        config: Event settings; the defaults when omitted.

    Returns:
        The entry and exit events.
    """
    config = config or EventConfig()

    # The same airports as the runway milestones use.
    flight_list = flight_aerodrome_sets(month, storage)

    sv_f = sv.withColumnRenamed("callsign", "flight_id")
    sv_f = sv_f.fillna({"flight_id": ""})
    # One call sign per track, before flight_id is used as a grouping key: a
    # track that broadcast two call signs while crossing one runway would
    # otherwise give two events for one crossing. The same rule as step 03
    # (``resolve_flight_id``), applied before reports are dropped below, so
    # both steps give a track the same call sign.
    #
    # Not applied when the run reproduces release v0.0.2: its tracks carry one
    # call sign each, and its events keep call signs padded with spaces, which
    # the resolution would trim.
    if config.events_version != LEGACY_EVENTS_VERSION:
        sv_f = resolve_flight_id(sv_f)
    # A position is required; an altitude is not when the report says the
    # aircraft is on the ground. A surface position report carries no
    # altitude at all, so requiring one would remove almost every report at a
    # stand. ``altitude_ft`` and ``flight_level`` are then NULL for those rows:
    # the altitude at the stand is unknown, not zero.
    keep_altitude = col("baro_altitude_c").isNotNull()
    if config.airport_admit_on_ground and "on_ground" in sv_f.columns:
        keep_altitude = keep_altitude | col("on_ground")
    sv_f = sv_f.dropna(subset=["lat", "lon"]).filter(keep_altitude)
    sv_f = sv_f.withColumn("altitude_ft", col("baro_altitude_c") * 3.28084)
    sv_f = sv_f.withColumn("flight_level", col("altitude_ft") / 100)

    # The height test is measured above the airport, not from standard
    # pressure, except with the v0.0.2 settings. It runs before the select
    # below, which drops the columns it needs.
    if config.airport_gate_above_field:
        sv_f = attach_field_elevation(sv_f, month, storage)
        gate = height_above_field_ft(sv_f) <= F.lit(config.airport_max_height_agl_ft)
    else:
        gate = col("flight_level") <= lit(config.airport_max_fl)
    # A surface position report has no altitude, so the height test is NULL
    # for it and a filter would drop it. The on-ground flag answers the same
    # question ("near the ground, or only over the airport?") directly.
    if config.airport_admit_on_ground and "on_ground" in sv_f.columns:
        gate = col("on_ground") | gate
    sv_f = sv_f.filter(gate)

    columns = [
        "track_id",
        "icao24",
        "flight_id",
        "event_time",
        "lat",
        "lon",
        "altitude_ft",
        "flight_level",
        "heading",
        "vert_rate",
        "h3_res_12",
        "cumulative_distance_nm",
        "cumulative_time_s",
    ]
    sv_f = sv_f.select(columns)

    sv_low_alt = sv_f.cache()
    sv_nearby_apt = sv_low_alt.join(flight_list, sv.track_id == flight_list.id, "inner")

    apt_sdf = storage.read_table("hexaero_airport_layouts")

    # Keep only the layout rows of airports this batch's flights name, before
    # the join on the H3 cell. The join requires the airport anyway, so no
    # result changes; the rows are only dropped before the shuffle instead of
    # after it. The list of airports is small, so it is broadcast.
    batch_apts = flight_list.select(F.explode("apt").alias("_batch_apt")).distinct()
    apt_sdf = apt_sdf.join(
        F.broadcast(batch_apts),
        apt_sdf.hexaero_apt_icao == batch_apts._batch_apt,
        "left_semi",
    )

    df_labelled = sv_nearby_apt.join(
        apt_sdf,
        (sv_nearby_apt.h3_res_12 == apt_sdf.hexaero_h3_id)
        & F.array_contains(sv_nearby_apt.apt, apt_sdf.hexaero_apt_icao),
        "inner",
    )

    # The runway milestones (runway_ops.py) publish line-up, runway-vacated
    # and the runway crossings from the same runway occupancy, so the runway
    # rows are dropped here, and one passage is not published twice.
    if config.emit_runway_milestones:
        df_labelled = df_labelled.filter(F.col("hexaero_aeroway") != "runway")

    # A gap of more than ``airport_trace_gap_seconds`` starts a new passage.
    window_spec = Window.partitionBy("track_id", "hexaero_osm_id").orderBy("event_time")
    df_labelled = df_labelled.withColumn(
        "time_diff",
        col("event_time").cast("long") - lag(col("event_time").cast("long"), 1).over(window_spec),
    )
    df_labelled = df_labelled.withColumn(
        "new_trace",
        when(col("time_diff") > config.airport_trace_gap_seconds, lit(1)).otherwise(lit(0)),
    )
    df_labelled = df_labelled.withColumn("trace_id", f_sum(col("new_trace")).over(window_spec))
    df_labelled = df_labelled.drop("time_diff", "new_trace")

    # Entry and exit. ``F.first``/``F.last`` would take the rows in the order
    # Spark reads them, not in time order; ``min_by``/``max_by`` take the
    # values of the report at the entry and exit time (with
    # ``airport_events_ordered``).
    if config.airport_events_ordered:

        def at_entry(c):
            return F.min_by(c, "event_time")

        def at_exit(c):
            return F.max_by(c, "event_time")

    else:
        at_entry, at_exit = F.first, F.last

    result = df_labelled.groupBy(
        "track_id",
        "icao24",
        "flight_id",
        "hexaero_apt_icao",
        "hexaero_osm_id",
        "hexaero_aeroway",
        "hexaero_ref",
        "trace_id",
    ).agg(
        f_min("event_time").alias("entry_time"),
        f_max("event_time").alias("exit_time"),
        at_entry("lat").alias("entry_lat"),
        at_exit("lat").alias("exit_lat"),
        at_entry("lon").alias("entry_lon"),
        at_exit("lon").alias("exit_lon"),
        at_entry("altitude_ft").alias("entry_altitude_ft"),
        at_exit("altitude_ft").alias("exit_altitude_ft"),
        at_entry("cumulative_distance_nm").alias("entry_cumulative_distance_nm"),
        at_exit("cumulative_distance_nm").alias("exit_cumulative_distance_nm"),
        at_entry("cumulative_time_s").alias("entry_cumulative_time_s"),
        at_exit("cumulative_time_s").alias("exit_cumulative_time_s"),
    )

    result = result.withColumn(
        "time_in_use_seconds",
        col("exit_time").cast("long") - col("entry_time").cast("long"),
    )
    result = result.filter(col("time_in_use_seconds") != 0)

    result = (
        result.withColumnRenamed("hexaero_osm_id", "osm_id")
        .withColumnRenamed("hexaero_aeroway", "osm_aeroway")
        .withColumnRenamed("hexaero_ref", "osm_ref")
        .withColumnRenamed("hexaero_apt_icao", "osm_airport")
    )

    result = result.withColumn(
        "info",
        to_json(
            struct(
                col("osm_id"),
                col("osm_aeroway"),
                col("osm_ref"),
                col("osm_airport"),
                col("time_in_use_seconds").alias("opdi_time_in_use_s"),
                col("icao24").alias("osn_icao24"),
                col("flight_id").alias("osn_flight_id"),
            )
        ),
    )

    result = result.withColumn("entry_type", concat_ws("-", lit("entry"), col("osm_aeroway")))
    result = result.withColumn("exit_type", concat_ws("-", lit("exit"), col("osm_aeroway")))
    result.cache()

    entry_events = result.select(
        col("track_id"),
        col("entry_time").alias("event_time"),
        col("entry_lon").alias("lon"),
        col("entry_lat").alias("lat"),
        col("entry_altitude_ft").alias("altitude_ft"),
        col("entry_cumulative_distance_nm").alias("cumulative_distance_nm"),
        col("entry_cumulative_time_s").alias("cumulative_time_s"),
        col("entry_type").alias("type"),
        col("info"),
        col("osm_id").alias("_osm_id"),
    )
    exit_events = result.select(
        col("track_id"),
        col("exit_time").alias("event_time"),
        col("exit_lon").alias("lon"),
        col("exit_lat").alias("lat"),
        col("exit_altitude_ft").alias("altitude_ft"),
        col("exit_cumulative_distance_nm").alias("cumulative_distance_nm"),
        col("exit_cumulative_time_s").alias("cumulative_time_s"),
        col("exit_type").alias("type"),
        col("info"),
        col("osm_id").alias("_osm_id"),
    )

    apt_events = entry_events.unionByName(exit_events)
    apt_events = apt_events.select(
        "track_id",
        "type",
        "event_time",
        "lon",
        "lat",
        "altitude_ft",
        "cumulative_distance_nm",
        "cumulative_time_s",
        "info",
        "_osm_id",
    )
    return one_element_per_passage(apt_events, config)


#: The columns that make two layout events the same passage; ``info`` names
#: the element.
_PASSAGE_KEY = [
    "track_id",
    "type",
    "event_time",
    "lon",
    "lat",
    "altitude_ft",
    "cumulative_distance_nm",
    "cumulative_time_s",
]


def one_element_per_passage(events: DataFrame, config: EventConfig) -> DataFrame:
    """One event per passage when overlapping layout elements give several.

    Elements of one kind can share an H3 cell (a taxiway crossing an apron's
    edge, stands drawn over an apron), and a passage through the cell then
    gives the same event once per element, differing only in ``info``. With
    :attr:`~opdi.config.EventConfig.layout_deterministic_element` the event
    kept names the element with the smallest identifier (``_osm_id``), then
    the first in the order of ``info``; otherwise any one is kept.

    Args:
        events: Layout events with an ``_osm_id`` column, dropped here.
        config: Event settings.
    """
    if not config.layout_deterministic_element:
        return events.dropDuplicates(_PASSAGE_KEY).drop("_osm_id")
    w = Window.partitionBy(*_PASSAGE_KEY).orderBy(
        col("_osm_id").asc_nulls_last(), col("info").asc_nulls_last()
    )
    return (
        events.withColumn("_rank", F.row_number().over(w))
        .filter(col("_rank") == 1)
        .drop("_rank", "_osm_id")
    )
