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
from opdi.utils.geometry import haversine_nm
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
        if config.layout_air_report_must_be_slow:
            # A report not flagged on the ground must also be close to the
            # ground; its speed is tested after the layout join, below.
            height = (
                height_above_field_ft(sv_f)
                if config.airport_gate_above_field
                else col("altitude_ft")
            )
            gate = gate & (height <= F.lit(config.layout_air_report_max_height_ft))
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
    slow_test = config.layout_air_report_must_be_slow and "on_ground" in sv_f.columns
    if slow_test:
        columns = columns + ["on_ground"]
        columns = columns + (["velocity"] if "velocity" in sv_f.columns else [])
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
    if slow_test:
        df_labelled = _keep_slow_reports(df_labelled, window_spec, config)
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

    if config.layout_element_per_passage or config.layout_merge_parked_passages:
        # Every step below works on the passages of one track; one exchange
        # by track serves them all.
        result = result.repartition("track_id")
    if config.layout_element_per_passage:
        result = _one_element_per_shared_report(result)
    if config.layout_merge_parked_passages:
        result = _merge_parked_passages(result, config)
        result = result.withColumn(
            "time_in_use_seconds",
            col("exit_time").cast("long") - col("entry_time").cast("long"),
        )

    info_fields = [
        col("osm_id"),
        col("osm_aeroway"),
        col("osm_ref"),
        col("osm_airport"),
        col("time_in_use_seconds").alias("opdi_time_in_use_s"),
        col("icao24").alias("osn_icao24"),
        col("flight_id").alias("osn_flight_id"),
    ]
    result = result.withColumn("info", to_json(struct(*info_fields)))
    # The facts that differ between the entry and the exit of one passage
    # (``at_track_start``, ``at_track_end``) give each its own ``info``. A
    # fact that is false is left out of ``info`` (``to_json`` drops NULLs).
    entry_info = exit_info = col("info")
    if config.layout_mark_track_edges or config.layout_mark_ground_only:
        result = _with_track_facts(result, sv, month, storage, config)
        common = []
        if config.layout_mark_ground_only:
            common.append(F.when(col("_ground_only"), lit(True)).alias("ground_only"))
        entry_extra, exit_extra = list(common), list(common)
        if config.layout_mark_track_edges:
            entry_extra.append(
                F.when(col("entry_time") == col("_first_pos_time"), lit(True)).alias(
                    "at_track_start"
                )
            )
            exit_extra.append(
                F.when(col("exit_time") == col("_last_pos_time"), lit(True)).alias("at_track_end")
            )
        entry_info = to_json(struct(*info_fields, *entry_extra))
        exit_info = to_json(struct(*info_fields, *exit_extra))

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
        entry_info.alias("info"),
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
        exit_info.alias("info"),
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
    if config.layout_element_per_passage:
        # The element was chosen per passage above: at one report there is
        # at most one entry and one exit of each kind.
        return apt_events.drop("_osm_id")
    return one_element_per_passage(apt_events, config)


#: Knots per metre per second, and feet per nautical mile.
KT_PER_MS = 1.94384
FT_PER_NM = 6076.12


def _keep_slow_reports(labelled: DataFrame, window_spec, config: EventConfig) -> DataFrame:
    """Drop the reports not flagged on the ground that move faster than taxiing.

    See :attr:`~opdi.config.EventConfig.layout_air_report_must_be_slow`. The
    speed between two reports comes from their positions, as reports on the
    surface rarely carry a speed. It is read between consecutive reports in
    the same element (the window of the passages), which costs no further
    exchange of the data.
    """
    t = col("event_time").cast("double")
    max_gap = float(config.layout_position_speed_max_gap_seconds)

    def speed_to(offset_fn):
        dt = F.abs(offset_fn(t) - t)
        dist_nm = haversine_nm(col("lat"), col("lon"), offset_fn(col("lat")), offset_fn(col("lon")))
        return F.when((dt > 0) & (dt <= max_gap), dist_nm / dt * 3600.0)

    prev_kt = speed_to(lambda c: lag(c, 1).over(window_spec))
    next_kt = speed_to(lambda c: F.lead(c, 1).over(window_spec))
    reported_kt = (
        col("velocity") * KT_PER_MS if "velocity" in labelled.columns else lit(None).cast("double")
    )
    speed_kt = F.greatest(prev_kt, next_kt, reported_kt)
    slow = speed_kt.isNull() | (speed_kt <= F.lit(config.layout_air_report_max_speed_kt))
    on_ground = F.coalesce(col("on_ground"), lit(False))
    return labelled.withColumn("_slow", on_ground | slow).filter(col("_slow")).drop("_slow")


def _one_element_per_shared_report(passages: DataFrame) -> DataFrame:
    """Of the passages of one kind that share a report, keep the longest.

    See :attr:`~opdi.config.EventConfig.layout_element_per_passage`. First,
    of the passages of one track and one kind (``osm_aeroway``) that start at
    the same report, the one that ends last is kept; then, of those left that
    end at the same report, the one that starts first. The kept passage
    contains the dropped ones. Between passages of equal length the element
    with the smallest identifier is kept, then the first in the order of the
    reference, so the choice is the same on every run.
    """
    by_track = Window.partitionBy("track_id")
    tie = [col("osm_id").asc_nulls_last(), col("osm_ref").asc_nulls_last()]

    def first_of_group(df, group_col, order):
        w = by_track.orderBy(col("osm_aeroway"), col(group_col), *order, *tie)
        same = (lag("osm_aeroway", 1).over(w) == col("osm_aeroway")) & (
            lag(group_col, 1).over(w) == col(group_col)
        )
        return (
            df.withColumn("_same", F.coalesce(same, lit(False))).filter(~col("_same")).drop("_same")
        )

    passages = first_of_group(passages, "entry_time", [col("exit_time").desc()])
    return first_of_group(passages, "exit_time", [col("entry_time").asc()])


def _merge_parked_passages(passages: DataFrame, config: EventConfig) -> DataFrame:
    """Join consecutive passages of one element of an aircraft that stayed put.

    See :attr:`~opdi.config.EventConfig.layout_merge_parked_passages`. A
    passage is joined to the previous passage of the same element when the
    distance between that one's exit and this one's entry is under
    :attr:`~opdi.config.EventConfig.layout_merge_max_move_ft` and no passage
    of another element of the track started in between.
    """
    by_element = Window.partitionBy("track_id", "osm_id").orderBy("entry_time")
    by_entry = Window.partitionBy("track_id").orderBy("entry_time", "osm_id")
    passages = passages.withColumn(
        "_last_entry_before",
        F.max("entry_time").over(by_entry.rowsBetween(Window.unboundedPreceding, -1)),
    )
    prev_exit = lag("exit_time", 1).over(by_element)
    moved_ft = (
        haversine_nm(
            lag("exit_lat", 1).over(by_element),
            lag("exit_lon", 1).over(by_element),
            col("entry_lat"),
            col("entry_lon"),
        )
        * FT_PER_NM
    )
    # ``_last_entry_before`` is the latest entry of any passage before this
    # one; the previous passage of this element started before its own exit,
    # so a later entry is that of another element.
    joined = (moved_ft < F.lit(config.layout_merge_max_move_ft)) & (
        col("_last_entry_before") <= prev_exit
    )
    passages = passages.withColumn("_new_stay", ~F.coalesce(joined, lit(False)))
    passages = passages.withColumn(
        "_stay",
        f_sum(col("_new_stay").cast("int")).over(
            by_element.rowsBetween(Window.unboundedPreceding, 0)
        ),
    )
    keys = [
        "track_id",
        "icao24",
        "flight_id",
        "osm_airport",
        "osm_id",
        "osm_aeroway",
        "osm_ref",
        "_stay",
    ]
    entry_cols = [c for c in passages.columns if c.startswith("entry_")]
    exit_cols = [c for c in passages.columns if c.startswith("exit_")]
    return (
        passages.groupBy(*keys)
        .agg(
            *[F.min_by(c, "entry_time").alias(c) for c in entry_cols],
            *[F.max_by(c, "exit_time").alias(c) for c in exit_cols],
            f_min("trace_id").alias("trace_id"),
        )
        .drop("_stay")
    )


def _with_track_facts(
    passages: DataFrame,
    sv: DataFrame,
    month: date,
    storage: "StorageManager",
    config: EventConfig,
) -> DataFrame:
    """Add the facts of each passage's track used in the events' ``info``.

    ``_first_pos_time`` and ``_last_pos_time``: the first and last report of
    the track that has a position. ``_ground_only``: the track has no report
    that is not flagged on the ground and has an altitude, and the flight
    list gives it no aircraft type or a vehicle type
    (:attr:`~opdi.config.EventConfig.layout_vehicle_typecodes`). One pass
    over the reports, grouped by track as they are already split.
    """
    has_pos = col("lat").isNotNull() & col("lon").isNotNull()
    aggs = [
        f_min(when(has_pos, col("event_time"))).alias("_first_pos_time"),
        f_max(when(has_pos, col("event_time"))).alias("_last_pos_time"),
    ]
    if "on_ground" in sv.columns:
        airborne = (~F.coalesce(col("on_ground"), lit(False))) & col("baro_altitude_c").isNotNull()
        aggs.append(f_max(airborne.cast("int")).alias("_any_air"))
    else:
        aggs.append(f_max(col("baro_altitude_c").isNotNull().cast("int")).alias("_any_air"))
    facts = sv.groupBy("track_id").agg(*aggs)
    passages = passages.join(facts, on="track_id", how="left")

    ground_only = lit(False)
    if config.layout_mark_ground_only and storage.table_exists("opdi_flight_list"):
        start_ts, end_ts = get_start_end_of_month(month)
        fl = storage.read_table("opdi_flight_list").filter(
            (col("dof") >= to_timestamp(lit(start_ts))) & (col("dof") < to_timestamp(lit(end_ts)))
        )
        if "typecode" in [c.lower() for c in fl.columns]:
            code = F.upper(F.trim(col("typecode")))
            vehicle = (
                code.isNull()
                | (code == "")
                | code.isin(*[t.upper() for t in config.layout_vehicle_typecodes])
            )
            types = (
                fl.select(col("id").alias("track_id"), vehicle.alias("_vehicle_type"))
                .groupBy("track_id")
                .agg(f_max(col("_vehicle_type").cast("int")).alias("_vehicle_type"))
            )
            passages = passages.join(F.broadcast(types), on="track_id", how="left")
            ground_only = (F.coalesce(col("_any_air"), lit(0)) == 0) & (
                F.coalesce(col("_vehicle_type"), lit(1)) == 1
            )
    return passages.withColumn("_ground_only", ground_only)


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
