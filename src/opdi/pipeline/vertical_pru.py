"""PRU's vertical profile: the tops of climb and descent, and where they move to.

EUROCONTROL's Performance Review Unit defines the vertical-efficiency tops
differently from OPDI's published fuzzy phase classifier, and the difference is
not a tuning choice:

* The analysis is **bounded by a 200 NM radius** around the flight's own
  aerodromes. ``D200`` is the first crossing of that radius outbound, ``A200``
  the last inbound. A step climb 250 NM out is cruise-altitude optimisation and
  is not a top of climb; the radius exists precisely to exclude it.
* **ToC-D200** is the first 4D point of highest altitude between take-off and
  D200 inclusive; **ToD-A200** the last 4D point at which the aircraft leaves
  the highest altitude between A200 and touchdown. First and last respectively,
  not either end of a plateau -- a climb is interrupted from below and a
  descent from above.
* A long level *inside the exclusion box* -- from the top down to 90% of it --
  is cruise rather than a level-off, and the top **moves**: **ToC-CCO** to the
  start of the first such segment, **ToD-CDO** to the end of the last. With no
  such segment the CCO top is the D200 top.

Both definitions are published. No reference data records a top of climb --
APDF has none and neither does anything else OPDI can reach -- so neither can
be shown better than the other, and the honest publication is both, each saying
which algorithm produced it. That is what the ``method`` key in ``info`` is
for: the PRU pair stamps ``"pru"`` here, the fuzzy pair stamps ``"phase"``
where it is built.

Only :func:`attach_aerodrome_geometry` reads storage. :func:`pru_tops` and
:func:`pru_top_events` take DataFrames, so the algorithm is testable on a
laptop against trajectories whose geometry is known by construction.
"""

from datetime import date
from typing import TYPE_CHECKING, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, to_timestamp
from pyspark.sql.window import Window

from opdi.pipeline.flights import haversine_nm
from opdi.pipeline.runway_ops import MILESTONE_SCHEMA
from opdi.utils.datetime_helpers import get_start_end_of_month

if TYPE_CHECKING:  # pragma: no cover - import cycle guard
    from opdi.config import EventConfig
    from opdi.utils.storage import StorageManager

FT_PER_M = 3.28084

#: Everything :func:`attach_aerodrome_geometry` attaches, and therefore
#: everything it drops first so that attaching twice -- or attaching after
#: :func:`~opdi.pipeline.elevation.attach_field_elevation`, which shares four of
#: these -- cannot leave two columns of the same name behind.
ATTACHED_GEOMETRY_COLS = (
    "adep",
    "ades",
    "adep_lat",
    "adep_lon",
    "elev_adep_ft",
    "ades_lat",
    "ades_lon",
    "elev_ades_ft",
)


def attach_aerodrome_geometry(
    sdf: DataFrame, month: date, storage: "StorageManager"
) -> DataFrame:
    """Attach each track's ADEP and ADES with their position and elevation.

    The same join as :func:`opdi.pipeline.elevation.attach_field_elevation`,
    widened to carry ``latitude_deg``/``longitude_deg`` as well as
    ``elevation_ft``. PRU's analysis radius is a distance from the aerodrome,
    so the elevation alone is not enough, and doing the two joins twice would
    put two versions of the same aerodrome lookup in the pipeline.

    Adds ``adep``, ``adep_lat``, ``adep_lon``, ``elev_adep_ft`` and the ``ades``
    equivalents. Columns are left NULL when the flight list names no aerodrome
    or OurAirports has no record for it; a NULL distance then fails the radius
    test rather than passing it, because a segment that cannot be placed
    relative to an aerodrome is outside the methodology, not inside it by
    default.

    **Idempotent, and safe after** :func:`~opdi.pipeline.elevation.attach_field_elevation`.
    That function attaches ``adep``, ``ades`` and both elevations for the phase
    family, so a frame arriving here may already carry four of the eight columns
    this adds. Joining on top of them would produce duplicates and an
    ``AMBIGUOUS_REFERENCE`` at the first reference -- in production, where the
    two are wired in sequence, and not in any test that calls one of them alone.
    They are dropped and reattached instead: both read the same two tables
    through the same join, so the reattached values are the values that were
    dropped.
    """
    if not storage.table_exists("opdi_flight_list"):
        return sdf

    sdf = sdf.drop(*[c for c in ATTACHED_GEOMETRY_COLS if c in sdf.columns])

    start_ts, end_ts = get_start_end_of_month(month)
    fl = (
        storage.read_table("opdi_flight_list")
        .filter(
            (col("dof") >= to_timestamp(lit(start_ts)))
            & (col("dof") < to_timestamp(lit(end_ts)))
        )
        .select(col("id").alias("_fl_id"), col("adep"), col("ades"))
    )
    _assert_unique_flight_ids(fl, month)

    if storage.table_exists("oa_airports"):
        apt = storage.read_table("oa_airports").select(
            col("ident").alias("_ident"),
            col("latitude_deg").cast("double").alias("_lat"),
            col("longitude_deg").cast("double").alias("_lon"),
            col("elevation_ft").cast("double").alias("_elev"),
        )
        for end, prefix in (("adep", "adep"), ("ades", "ades")):
            fl = (
                fl.join(F.broadcast(apt), fl[end] == col("_ident"), "left")
                .withColumnRenamed("_lat", f"{prefix}_lat")
                .withColumnRenamed("_lon", f"{prefix}_lon")
                .withColumnRenamed("_elev", f"elev_{prefix}_ft")
                .drop("_ident")
            )
    else:
        for prefix in ("adep", "ades"):
            fl = (
                fl.withColumn(f"{prefix}_lat", lit(None).cast("double"))
                .withColumn(f"{prefix}_lon", lit(None).cast("double"))
                .withColumn(f"elev_{prefix}_ft", lit(None).cast("double"))
            )

    return sdf.join(F.broadcast(fl), sdf.track_id == col("_fl_id"), "left").drop("_fl_id")


def _assert_unique_flight_ids(fl: DataFrame, month: date) -> None:
    """Fail loudly if the month's flight list has a repeated ``id``.

    This join is a *left* join onto the state vectors, so a duplicated flight
    list id does not error: it multiplies every state vector of that track by
    the number of rows carrying the id. Nothing downstream would say so. The
    level segments would be detected over a trajectory sampled twice, the
    runway traversals would emit two of every milestone, and the event counts
    the ladder reports would be inflated for exactly the flights whose flight
    list entry was ambiguous.

    Cheap, and paid once per step: one aggregation over the month's flight
    list, which is a few hundred thousand rows and is about to be broadcast
    anyway. The alternative is a silent fan-out that only shows up as a
    benchmark that will not reconcile.
    """
    n, distinct = fl.select(
        F.count(col("_fl_id")), F.countDistinct(col("_fl_id"))
    ).first()
    if n != distinct:
        raise ValueError(
            f"the flight list for {month:%Y-%m} has {n} rows but only "
            f"{distinct} distinct ids. This join is a left join onto the state "
            f"vectors, so a repeated id does not fail -- it silently "
            f"multiplies every sample of that track, and every event derived "
            f"from it, by the number of duplicates. Deduplicate the flight "
            f"list before running step 04."
        )


def aerodrome_distances(sdf: DataFrame) -> DataFrame:
    """Per-sample haversine distances to the flight's own ADEP and ADES.

    Idempotent, and a no-op when the caller has already attached them: the
    level arms and :func:`pru_tops` both need these columns, and computing them
    once at the top of the family is cheaper than twice and cannot produce two
    answers.
    """
    for prefix, out in (("adep", "dist_adep_nm"), ("ades", "dist_ades_nm")):
        if out in sdf.columns:
            continue
        if f"{prefix}_lat" not in sdf.columns:
            sdf = sdf.withColumn(out, lit(None).cast("double"))
            continue
        sdf = sdf.withColumn(
            out,
            haversine_nm(
                col("lat"), col("lon"), col(f"{prefix}_lat"), col(f"{prefix}_lon")
            ),
        )
    return sdf


def pru_tops(
    sdf: DataFrame,
    segments: DataFrame,
    config: "EventConfig",
    *,
    altitude_col: str = "baro_altitude_c",
    time_col: str = "event_time",
) -> DataFrame:
    """The PRU tops, one row per ``track_id``.

    ``sdf`` is the state-vector frame carrying either ``dist_adep_nm`` /
    ``dist_ades_nm`` or the aerodrome coordinates to compute them from;
    ``segments`` is the output of either level arm, and is consulted only for
    the relocation. Returns ``toc_d200_time``, ``toc_d200_alt_ft``,
    ``toc_cco_time``, ``toc_cco_alt_ft``, ``toc_relocated`` and the four
    ``tod_`` equivalents.

    A track that never reaches the radius keeps its whole trajectory in the
    analysis window -- the fallback is the last sample outbound and the first
    inbound. A short flight is a flight PRU still has tops for; abstaining
    would drop every sector under 400 NM, which is most of them.
    """
    work = aerodrome_distances(sdf).select(
        "track_id",
        F.col(time_col).alias("_t"),
        (F.col(altitude_col) * F.lit(FT_PER_M)).alias("_alt_ft"),
        "dist_adep_nm",
        "dist_ades_nm",
    )

    radius = F.lit(config.level_analysis_radius_nm)
    whole = Window.partitionBy("track_id")

    # D200: the first time the distance from the departure aerodrome reaches
    # the radius. A200: the last time the distance to the arrival aerodrome
    # still exceeds it -- the last moment before the aircraft is finally inside
    # the ring, which is where PRU starts looking for the top of descent.
    work = work.withColumn(
        "_t_d200",
        F.coalesce(
            F.min(F.when(F.col("dist_adep_nm") >= radius, F.col("_t"))).over(whole),
            F.max("_t").over(whole),
        ),
    ).withColumn(
        "_t_a200",
        F.coalesce(
            F.max(F.when(F.col("dist_ades_nm") > radius, F.col("_t"))).over(whole),
            F.min("_t").over(whole),
        ),
    )

    in_climb = F.col("_t") <= F.col("_t_d200")
    in_descent = F.col("_t") >= F.col("_t_a200")
    work = work.withColumn(
        "_toc_alt", F.max(F.when(in_climb, F.col("_alt_ft"))).over(whole)
    ).withColumn(
        "_tod_alt", F.max(F.when(in_descent, F.col("_alt_ft"))).over(whole)
    )

    # First and last: a plateau at the top of the climb is entered once and
    # left once, and PRU names the entry in climb and the exit in descent.
    tops = work.groupBy("track_id").agg(
        F.first("_toc_alt").alias("toc_d200_alt_ft"),
        F.first("_tod_alt").alias("tod_a200_alt_ft"),
        F.min(
            F.when(in_climb & (F.col("_alt_ft") == F.col("_toc_alt")), F.col("_t"))
        ).alias("toc_d200_time"),
        F.max(
            F.when(in_descent & (F.col("_alt_ft") == F.col("_tod_alt")), F.col("_t"))
        ).alias("tod_a200_time"),
    )

    return _relocate(tops, segments, config)


def _relocate(tops: DataFrame, segments: DataFrame, config: "EventConfig") -> DataFrame:
    """Move each top to the far side of the cruise that hides it.

    The exclusion box runs from the top's own altitude down to
    ``level_exclusion_box_pct`` of it. A level segment inside the box lasting
    longer than ``level_exclusion_box_seconds`` is cruise, and a top found on
    the far side of it is the top of the cruise rather than of the climb.

    ``F.min``/``F.max`` over a struct ordered by time, rather than
    ``min_by``/``max_by``: the altitude reported has to be the one belonging to
    the segment whose time was chosen, and a struct makes that a single
    comparison instead of two aggregations that could disagree.
    """
    pct = F.lit(config.level_exclusion_box_pct / 100.0)
    long_hold = F.col("duration_seconds") > F.lit(config.level_exclusion_box_seconds)

    keys = [
        "track_id",
        "toc_d200_time",
        "toc_d200_alt_ft",
        "tod_a200_time",
        "tod_a200_alt_ft",
    ]
    seg = segments.select("track_id", "start_time", "end_time", "duration_seconds", "level_ft")
    joined = tops.join(seg, "track_id", "left")

    # The box is a climb box before the top and a descent box after it. Without
    # the time bound a cruise segment after the top of climb would move the top
    # *forwards*, which is the opposite of what relocation is for.
    climb_hold = (
        (F.col("level_ft") >= pct * F.col("toc_d200_alt_ft"))
        & long_hold
        & (F.col("start_time") <= F.col("toc_d200_time"))
    )
    descent_hold = (
        (F.col("level_ft") >= pct * F.col("tod_a200_alt_ft"))
        & long_hold
        & (F.col("end_time") >= F.col("tod_a200_time"))
    )

    moved = joined.groupBy(*keys).agg(
        F.min(F.when(climb_hold, F.struct("start_time", "level_ft"))).alias("_cco"),
        F.max(F.when(descent_hold, F.struct("end_time", "level_ft"))).alias("_cdo"),
    )

    return moved.select(
        "track_id",
        "toc_d200_time",
        "toc_d200_alt_ft",
        F.coalesce(F.col("_cco.start_time"), F.col("toc_d200_time")).alias("toc_cco_time"),
        F.coalesce(F.col("_cco.level_ft"), F.col("toc_d200_alt_ft")).alias("toc_cco_alt_ft"),
        F.col("_cco").isNotNull().alias("toc_relocated"),
        "tod_a200_time",
        "tod_a200_alt_ft",
        F.coalesce(F.col("_cdo.end_time"), F.col("tod_a200_time")).alias("tod_cdo_time"),
        F.coalesce(F.col("_cdo.level_ft"), F.col("tod_a200_alt_ft")).alias("tod_cdo_alt_ft"),
        F.col("_cdo").isNotNull().alias("tod_relocated"),
    )


def pru_top_events(
    sdf: DataFrame,
    segments: DataFrame,
    config: "EventConfig",
    *,
    tops: Optional[DataFrame] = None,
    altitude_col: str = "baro_altitude_c",
    time_col: str = "event_time",
) -> DataFrame:
    """``top-of-climb-cco`` and ``top-of-descent-cdo``, on the standard event frame.

    Emitted *alongside* the published fuzzy ``top-of-climb``/``top-of-descent``,
    which are unchanged. ``info`` carries the D200/A200 point the top was
    relocated from, the radius the analysis was bounded by, and ``method:
    "pru"`` -- so a consumer can tell the two families apart, and can recover
    the unrelocated top without re-running anything.

    Returns an empty frame of the standard shape when ``emit_pru_tops`` is off,
    never ``None``, so that unioning the families never branches on nullity.

    ``tops`` reuses a frame the caller already built. With ``level_anchor =
    "pru"`` both this family and the level-off classification hang from the
    same tops, and computing them twice is the same windowed pass over the
    month's state vectors done twice -- and would let the two disagree if
    anything about the computation were ever non-deterministic.
    """
    session = sdf.sparkSession
    if not config.emit_pru_tops:
        return session.createDataFrame([], MILESTONE_SCHEMA)

    if tops is None:
        tops = pru_tops(
            sdf, segments, config, altitude_col=altitude_col, time_col=time_col
        )

    climb = tops.select(
        "track_id",
        lit("top-of-climb-cco").alias("type"),
        col("toc_cco_time").alias("event_time"),
        col("toc_cco_alt_ft").alias("altitude_ft"),
        col("toc_d200_time").alias("_d200_time"),
        col("toc_d200_alt_ft").alias("_d200_alt"),
        col("toc_relocated").alias("_relocated"),
    )
    descent = tops.select(
        "track_id",
        lit("top-of-descent-cdo").alias("type"),
        col("tod_cdo_time").alias("event_time"),
        col("tod_cdo_alt_ft").alias("altitude_ft"),
        col("tod_a200_time").alias("_d200_time"),
        col("tod_a200_alt_ft").alias("_d200_alt"),
        col("tod_relocated").alias("_relocated"),
    )
    events = climb.unionByName(descent).filter(col("event_time").isNotNull())

    events = _attach_position(events, sdf, time_col)
    return events.select(
        col("track_id"),
        col("type"),
        col("event_time"),
        col("lon").cast("double").alias("lon"),
        col("lat").cast("double").alias("lat"),
        col("altitude_ft").cast("double").alias("altitude_ft"),
        col("cumulative_distance_nm").cast("double").alias("cumulative_distance_nm"),
        col("cumulative_time_s").cast("double").alias("cumulative_time_s"),
        F.to_json(
            F.struct(
                col("_d200_time").alias("d200_time"),
                col("_d200_alt").alias("d200_altitude_ft"),
                col("_relocated").alias("relocated"),
                lit(config.level_analysis_radius_nm).alias("analysis_radius_nm"),
                lit("pru").alias("method"),
            )
        ).alias("info"),
    )


def _attach_position(events: DataFrame, sdf: DataFrame, time_col: str) -> DataFrame:
    """Place each top at the last sample at or before its time.

    Not an equality join: a relocated top sits at a level segment's boundary,
    which the PRU arm reports on an interpolated grid and so need not be an
    instant the track was sampled at. An equality join would silently null the
    position of exactly the events the relocation moved.
    """
    carried = [c for c in ("cumulative_distance_nm", "cumulative_time_s") if c in sdf.columns]
    samples = sdf.select(
        col("track_id").alias("_sid"),
        col(time_col).alias("_st"),
        col("lon").cast("double").alias("lon"),
        col("lat").cast("double").alias("lat"),
        *[col(c).cast("double").alias(c) for c in carried],
    )
    for missing in ("cumulative_distance_nm", "cumulative_time_s"):
        if missing not in carried:
            samples = samples.withColumn(missing, lit(None).cast("double"))

    joined = events.join(
        samples,
        (events.track_id == col("_sid")) & (col("_st") <= events.event_time),
        "left",
    )
    picked = joined.groupBy("track_id", "type").agg(
        F.max(
            F.struct(
                col("_st"),
                col("lon"),
                col("lat"),
                col("cumulative_distance_nm"),
                col("cumulative_time_s"),
            )
        ).alias("_s")
    )
    return events.join(picked, ["track_id", "type"], "left").select(
        *[col(c) for c in events.columns],
        col("_s.lon").alias("lon"),
        col("_s.lat").alias("lat"),
        col("_s.cumulative_distance_nm").alias("cumulative_distance_nm"),
        col("_s.cumulative_time_s").alias("cumulative_time_s"),
    )
