"""Airport layout events -- entry/exit of runway, taxiway, apron via H3
matching against ``hexaero_airport_layouts``.

Moved out of ``events.py`` (Task 4) so the gate that decides which samples are
"low enough to be near the ground" can be expressed against height above field
elevation rather than uncorrected pressure altitude, using the shared helpers
in ``elevation.py`` (Task 2) instead of a second copy of that arithmetic.
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
    max as f_max,
    min as f_min,
    split,
    struct,
    sum as f_sum,
    to_json,
    to_timestamp,
    when,
)
from pyspark.sql.window import Window

from opdi.config import EventConfig
from opdi.pipeline.elevation import attach_field_elevation, height_above_field_ft
from opdi.pipeline.flights import resolve_flight_id
from opdi.utils.datetime_helpers import get_start_end_of_month
from opdi.utils.storage import StorageManager

#: The events version every published dataset up to 2026-08 carries.
#:
#: Named here because this module has to *test* for it -- a run stamping a
#: released string has to reproduce that release, so the callsign-resolution
#: behaviour change below is skipped for it. ``EventConfig.legacy()`` sets it;
#: the two are asserted equal in ``tests/test_events_labelling.py`` so a rename
#: cannot make the guard quietly stop firing.
LEGACY_EVENTS_VERSION = "events_v0.0.2"


def flight_aerodrome_sets(month: date, storage: "StorageManager") -> DataFrame:
    """``id`` and ``apt``: the aerodromes each flight of *month* may be at.

    The array is ``adep``, ``ades`` and the two proximity lists, with the empty
    strings a missing aerodrome leaves removed -- because that is what the
    layout join tests with ``array_contains``, and a flight whose ADES is
    unknown must not match every aerodrome whose ``hexaero_apt_icao`` happens
    to be blank.

    Extracted from :func:`calculate_airport_events` rather than copied into the
    runway family: the two must agree about which aerodromes a flight is
    allowed to be at, or one crossing gets published by one family and dropped
    by the other, and nothing in the output would say which rule each used.

    No ``table_exists`` guard: the caller decides what a missing flight list
    means -- fatal here, "skip the family" for the runway milestones -- and a
    guard returning ``None`` would turn the first case into an
    ``AttributeError`` several frames away from its cause.
    """
    start_ts, end_ts = get_start_end_of_month(month)
    flight_list = (
        storage.read_table("opdi_flight_list")
        .filter(
            (col("dof") >= to_timestamp(lit(start_ts)))
            & (col("dof") < to_timestamp(lit(end_ts)))
        )
        .select("id", "adep", "ades", "adep_p", "ades_p")
    )

    for c in ["adep", "ades", "adep_p", "ades_p"]:
        flight_list = flight_list.withColumn(
            c, when(col(c).isNull(), lit("")).otherwise(col(c))
        )

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
    sv: DataFrame, month: date, storage: "StorageManager",
    config: Optional[EventConfig] = None,
) -> DataFrame:
    """
    Detect airport infrastructure entry/exit events using H3 layout matching.

    Matches low-altitude track points against airport layout H3 hexagons
    (resolution 12) to detect when aircraft enter and exit runways,
    taxiways, aprons, and other ground infrastructure.

    Args:
        sv: Input tracks DataFrame.
        month: Month being processed (for flight list lookup).
        storage: StorageManager instance.

    Returns:
        DataFrame of airport entry/exit events.
    """
    config = config or EventConfig()

    # Built by the shared helper, because the runway family needs the identical
    # array and two constructions of it would be two definitions of "which
    # aerodromes may this flight be at".
    flight_list = flight_aerodrome_sets(month, storage)

    sv_f = sv.withColumnRenamed("callsign", "flight_id")
    sv_f = sv_f.fillna({"flight_id": ""})
    # One callsign per track, before flight_id becomes a grouping key below.
    #
    # The groupBy that aggregates entry and exit times carries flight_id
    # without aggregating it. That is sound only while a track holds one
    # callsign, which legacy segmentation guarantees by construction -- the
    # callsign is part of the track's group key -- and which a segmentation
    # grouping on the airframe alone does not. Without this, a track that
    # broadcast two callsigns while crossing one runway becomes two groups and
    # emits two entry-runway events for one crossing, and the ``info`` JSON
    # below publishes flight_id as ``osn_flight_id``, so the duplication
    # reaches the milestone table rather than staying an internal artefact.
    #
    # Same helper as step 03, imported rather than copied: two copies of this
    # rule is how the production flight list and its benchmark came to disagree
    # about it in the first place.
    #
    # **Before the dropna, deliberately -- the vote is over the unfiltered
    # frame.** ``resolve_flight_id`` documents that as its contract, and it is
    # the contract because the population a mode is taken over *is* the rule: a
    # different population is a different rule wearing the same name. Step 03
    # votes over the whole month of the track table. If step 04 voted only over
    # samples carrying position and barometric altitude, the two would diverge
    # on exactly the tracks that matter -- ADS-B sends position and velocity in
    # separate message types, so a velocity-only sample carries a callsign and
    # no position, and step 02a's cleaning NULLs the baro_altitude_c it
    # rejects. A track whose real callsign appears mostly in such samples would
    # be named SAS123 in opdi_flight_list and "" in the same track's
    # info.osn_flight_id, from one aircraft on one day. That is the divergence
    # step 03 refactored away within itself; it must not reappear across steps.
    #
    # **Not applied when the run is reproducing a release.** ``events_v0.0.2``
    # exists so that re-processing a past month reproduces it, and changing
    # what a run publishes while it stamps an old version is the one thing a
    # version string must prevent. Resolution would be a no-op on legacy tracks
    # anyway -- they are callsign-homogeneous by construction -- with one
    # exception it must not make: OpenSky pads callsigns to eight characters,
    # so an aircraft that set none broadcasts spaces, which ``fillna`` keeps
    # verbatim but ``dominant_flight_id`` trims and reads as blank. The
    # released events carry the spaces. Hence the test is on the version stamp
    # rather than on an argument about homogeneity.
    if config.events_version != LEGACY_EVENTS_VERSION:
        sv_f = resolve_flight_id(sv_f)
    # Position is required; altitude is not, when the message says the aircraft
    # is on the ground. Dropping every null ``baro_altitude_c`` here deleted the
    # surface samples before the gate below ever saw them -- a surface position
    # report carries no altitude at all, so this one line discarded 99.9% of the
    # state vectors inside a stand polygon at EBBR and is why ``off-block`` and
    # ``on-block`` answered ~1% of movements at every aerodrome but one.
    # ``altitude_ft`` and ``flight_level`` are then null for those rows, which is
    # the honest reading: the aircraft's altitude at the stand is unknown, not
    # zero, and the events built from them carry that null through.
    keep_altitude = col("baro_altitude_c").isNotNull()
    if config.airport_admit_on_ground and "on_ground" in sv_f.columns:
        keep_altitude = keep_altitude | col("on_ground")
    sv_f = sv_f.dropna(subset=["lat", "lon"]).filter(keep_altitude)
    sv_f = sv_f.withColumn("altitude_ft", col("baro_altitude_c") * 3.28084)
    sv_f = sv_f.withColumn("flight_level", col("altitude_ft") / 100)

    # The gate is measured above the field, not above the 1013.25 hPa datum,
    # unless the caller asks for the legacy behaviour. It has to run before
    # the projection below narrows the columns: ``height_above_field_ft``
    # needs ``baro_altitude_c`` and the two ``elev_*_ft`` columns
    # ``attach_field_elevation`` joins on, none of which survive the select.
    if config.airport_gate_above_field:
        sv_f = attach_field_elevation(sv_f, month, storage)
        gate = height_above_field_ft(sv_f) <= F.lit(config.airport_max_height_agl_ft)
    else:
        gate = col("flight_level") <= lit(config.airport_max_fl)
    # A surface position message carries no altitude -- neither barometric nor
    # geometric -- so the gate above is NULL for it, and `NULL <= x` is dropped
    # by a filter. That silently discarded the samples this family is *for*:
    # at EBBR 99.9% of the state vectors inside a stand polygon have a null
    # altitude, and the gate passed 57 aircraft out of 2,013. `on_ground` is
    # the message's own statement that the aircraft is on the surface, so it
    # answers the gate's question -- "is this near the ground, or merely over
    # the airport" -- without going through an altitude that is not there.
    if config.airport_admit_on_ground and "on_ground" in sv_f.columns:
        gate = col("on_ground") | gate
    sv_f = sv_f.filter(gate)

    columns = [
        "track_id", "icao24", "flight_id", "event_time", "lat", "lon",
        "altitude_ft", "flight_level", "heading", "vert_rate",
        "h3_res_12", "cumulative_distance_nm", "cumulative_time_s",
    ]
    sv_f = sv_f.select(columns)

    sv_low_alt = sv_f.cache()
    sv_nearby_apt = sv_low_alt.join(flight_list, sv.track_id == flight_list.id, "inner")

    apt_sdf = storage.read_table("hexaero_airport_layouts")

    # Restrict the layout grid to the aerodromes this month's flights name,
    # before joining on the H3 cell.
    #
    # This changes no result. The join below already requires
    # ``array_contains(apt, hexaero_apt_icao)``, so a reference row for an
    # aerodrome no flight names cannot survive it. The semi-join only moves
    # that elimination earlier, so the unmatched remainder is dropped before
    # the shuffle on ``h3_res_12`` rather than carried through it.
    #
    # Taken from ``flight_list`` rather than from ``sv_nearby_apt``: the
    # aerodrome set lives on the flight list, which is already filtered to the
    # month, and reading it from the joined frame would recompute that join
    # just to learn something the smaller side already knows.
    #
    # **How much this saves depends on the batch, and it is not uniform.** The
    # 2026-09-07 PBF rebuild took this table from 15 aerodromes to 1,036 and
    # from ~140k rows to 2,900,230. A study-scoped run touching twenty
    # aerodromes now discards ~98% of it before the shuffle. A network-wide
    # month touches most of Europe's large and medium fields, so it discards
    # little -- the pruning is real there but modest, and the reason to keep
    # it is that the cost is one broadcast of a few thousand short strings
    # either way.
    #
    # Broadcasting *this* side is safe precisely because it is small.
    # Broadcasting the grid is not: doing that to the comparable
    # ``h3_runway_zones`` OOM'd the driver JVM outright (see runway_ops.py).
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

    # The runway family (runway_ops.py) publishes line-up, runway-vacated and
    # the two crossing types from the same physical runway occupancy this
    # layout match would otherwise report as entry-runway/exit-runway. Drop
    # the runway rows here so one crossing is not published under two
    # vocabularies.
    if config.emit_runway_milestones:
        df_labelled = df_labelled.filter(F.col("hexaero_aeroway") != "runway")

    # Detect separate traces (gap > 5 min = new trace)
    window_spec = Window.partitionBy("track_id", "hexaero_osm_id").orderBy("event_time")
    df_labelled = df_labelled.withColumn(
        "time_diff",
        col("event_time").cast("long") - lag(col("event_time").cast("long"), 1).over(window_spec),
    )
    df_labelled = df_labelled.withColumn(
        "new_trace", when(col("time_diff") > config.airport_trace_gap_seconds, lit(1)).otherwise(lit(0))
    )
    df_labelled = df_labelled.withColumn(
        "trace_id", f_sum(col("new_trace")).over(window_spec)
    )
    df_labelled = df_labelled.drop("time_diff", "new_trace")

    # Aggregate entry/exit times.
    #
    # ``F.first``/``F.last`` inside a groupBy take *partition* order, not
    # event_time order, so the reported entry position, altitude and cumulative
    # measures were not guaranteed to be the values at the reported entry time
    # -- a row could describe one instant and be stamped with another, with
    # nothing to indicate it. ``min_by``/``max_by`` pick the value at the
    # extreme of an explicit ordering column, which is what was meant.
    if config.airport_events_ordered:
        def at_entry(c):
            return F.min_by(c, "event_time")

        def at_exit(c):
            return F.max_by(c, "event_time")
    else:
        at_entry, at_exit = F.first, F.last

    result = df_labelled.groupBy(
        "track_id", "icao24", "flight_id",
        "hexaero_apt_icao", "hexaero_osm_id", "hexaero_aeroway", "hexaero_ref", "trace_id",
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
        result
        .withColumnRenamed("hexaero_osm_id", "osm_id")
        .withColumnRenamed("hexaero_aeroway", "osm_aeroway")
        .withColumnRenamed("hexaero_ref", "osm_ref")
        .withColumnRenamed("hexaero_apt_icao", "osm_airport")
    )

    result = result.withColumn("info", to_json(struct(
        col("osm_id"), col("osm_aeroway"), col("osm_ref"), col("osm_airport"),
        col("time_in_use_seconds").alias("opdi_time_in_use_s"),
        col("icao24").alias("osn_icao24"),
        col("flight_id").alias("osn_flight_id"),
    )))

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
    )

    apt_events = entry_events.unionByName(exit_events)
    apt_events = apt_events.select(
        "track_id", "type", "event_time", "lon", "lat",
        "altitude_ft", "cumulative_distance_nm", "cumulative_time_s", "info",
    )
    apt_events = apt_events.dropDuplicates([
        "track_id", "type", "event_time", "lon", "lat",
        "altitude_ft", "cumulative_distance_nm", "cumulative_time_s",
    ])

    return apt_events
