"""Ground movement milestones: off-block (T04) and on-block (T21).

A native port of the signal in ``traffic``'s ``StartMoving``
(``traffic/src/traffic/algorithms/ground/movement.py:13-63``): the aircraft is
moving once its groundspeed stays above a threshold for a sustained period, and
the sustained part is what separates a push from a jitter in the speed field.

``traffic``'s parking-position path is **not** ported. It resolves stands
through ``airport.parking_position``, which issues a live OpenStreetMap
Overpass query whose ``lru_cache`` is stripped on pickling
(``traffic/core/structure.py:104-137``) -- so it cannot run on an executor with
no network, which is the case OPDI has to assume. OPDI's own
``hexaero_airport_layouts`` already carries the same OSM ``parking_position``
geometry as a committed H3 res-12 table, and step 04 already emits
``entry-parking_position``/``exit-parking_position`` from it. This anchors on
those events rather than re-deriving the geometry.

**Coverage, not accuracy, is the expected limitation.** ADS-B ground reception
is sparse -- an aircraft on a stand is often not received at all -- so these
milestones will be missing for a large share of flights. That number is a
finding to report, not something to tune away.
"""

from typing import TYPE_CHECKING, Optional, Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from opdi.pipeline.flights import haversine_nm

if TYPE_CHECKING:  # pragma: no cover - import cycle guard
    from opdi.config import EventConfig

KT_PER_MPS = 1.94384


def _derived_groundspeed_kt(ordered: Window, time_col: str) -> Column:
    """Position-derived groundspeed in knots, 3-sample median filtered.

    Ported from traffic's ``StartMoving``, which does not trust the broadcast
    ``velocity`` field at all: ``resampled.cumulative_distance().filter(
    compute_gs=3).query("compute_gs > threshold")``
    (``traffic/src/traffic/algorithms/ground/movement.py:37-46``). Native
    Spark equivalent -- no UDF: haversine between consecutive samples over the
    time delta, then a centred 3-sample rolling median (``filter(compute_gs=3)``
    is itself a rolling median filter), because position-derived speed is
    spiky and traffic filters it for exactly that reason before thresholding.
    """
    prev_lat = F.lag(F.col("lat")).over(ordered)
    prev_lon = F.lag(F.col("lon")).over(ordered)
    prev_t = F.lag(F.col(time_col)).over(ordered)

    dt_s = F.col(time_col).cast("double") - prev_t.cast("double")
    dist_nm = haversine_nm(prev_lat, prev_lon, F.col("lat"), F.col("lon"))
    raw_kt = F.when(dt_s > 0, dist_nm / (dt_s / 3600.0))

    # `F.median` cannot be windowed with an explicit frame in this Spark
    # version ([INVALID_WINDOW_SPEC_FOR_AGGREGATION_FUNC]); `percentile_approx`
    # at 0.5 is Spark's windowable median and, over a 3-row frame, exact for
    # any practical purpose (default accuracy 10000 >> 3 samples).
    return F.percentile_approx(raw_kt, 0.5).over(ordered.rowsBetween(-1, 1))


def movement_window(
    sdf: DataFrame,
    config: "EventConfig",
    *,
    partition_cols: Sequence[str] = ("track_id",),
    time_col: str = "event_time",
) -> DataFrame:
    """First and last instant of sustained ground movement, per track.

    Returns one row per partition with ``moving_start`` and ``moving_stop``,
    or no row where nothing sustained was seen.
    """
    ordered = Window.partitionBy(*partition_cols).orderBy(time_col)

    gs_kt = F.col("velocity") * KT_PER_MPS
    if config.ground_speed_derive_from_position and "lat" in sdf.columns and "lon" in sdf.columns:
        # Fill the gap, don't replace: keeps today's behaviour wherever
        # `velocity` exists and adds signal only where it is missing -- see
        # EventConfig.ground_speed_derive_from_position. Guarded on column
        # presence, not just NULL values: a caller that never carried lat/lon
        # at all (some tests, and any minimal projection) must get exactly
        # today's velocity-only behaviour rather than an unresolved-column
        # analysis error.
        gs_kt = F.coalesce(gs_kt, _derived_groundspeed_kt(ordered, time_col))
    work = sdf.withColumn("_moving", gs_kt > F.lit(config.ground_speed_threshold_kt))

    # Sessionise runs of movement, then keep runs long enough to be a push
    # rather than a wobble in the speed field.
    work = work.withColumn("_prev", F.lag("_moving").over(ordered))
    work = work.withColumn(
        "_new_run",
        F.when(F.col("_moving") & ~F.coalesce(F.col("_prev"), F.lit(False)), 1).otherwise(0),
    )
    work = work.withColumn(
        "_run",
        F.sum("_new_run").over(ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
    )

    runs = (
        work.filter(F.col("_moving"))
        .groupBy(*partition_cols, "_run")
        .agg(F.min(time_col).alias("_from"), F.max(time_col).alias("_to"))
    )
    runs = runs.withColumn(
        "_seconds", F.col("_to").cast("double") - F.col("_from").cast("double")
    ).filter(F.col("_seconds") >= F.lit(config.ground_move_min_seconds))

    return runs.groupBy(*partition_cols).agg(
        F.min("_from").alias("moving_start"), F.max("_to").alias("moving_stop")
    )


def block_times(
    movements: DataFrame,
    airport_events: DataFrame,
) -> Optional[DataFrame]:
    """Off-block and on-block, anchored on the parking-position events.

    Off-block is the start of sustained movement, but only for a track that was
    seen leaving a stand -- without that anchor the first movement of a track
    picked up mid-taxi would be reported as its off-block, which is not the
    same event and would look like a very short taxi rather than a miss.
    """
    exits = airport_events.filter(F.col("type") == "exit-parking_position").groupBy(
        "track_id"
    ).agg(F.min("event_time").alias("stand_exit"))
    entries = airport_events.filter(F.col("type") == "entry-parking_position").groupBy(
        "track_id"
    ).agg(F.max("event_time").alias("stand_entry"))

    out = movements.join(exits, "track_id", "left").join(entries, "track_id", "left")
    return out.select(
        "track_id",
        F.when(F.col("stand_exit").isNotNull(), F.col("moving_start")).alias("aobt"),
        F.when(F.col("stand_entry").isNotNull(), F.col("moving_stop")).alias("aibt"),
        F.col("stand_exit"),
        F.col("stand_entry"),
    ).filter(F.col("aobt").isNotNull() | F.col("aibt").isNotNull())
