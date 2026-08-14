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

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

if TYPE_CHECKING:  # pragma: no cover - import cycle guard
    from opdi.config import EventConfig

KT_PER_MPS = 1.94384


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
