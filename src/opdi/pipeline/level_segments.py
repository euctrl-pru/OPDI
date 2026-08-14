"""ICAO level-segment detection, for KPI17 and KPI19.

KPI17 (level-off during climb) and KPI19 (level-off during descent) are the two
vertical-efficiency indicators ADS-B can reach, and ICAO specifies them with a
complete algorithm and named parameters rather than leaving the detection to
the implementer::

    a data point starts a level segment when the altitude difference with the
    next data point is <= the level band limit and the vertical speed towards
    it is <= the vertical speed limit. The segment ends when the altitude
    differs from the segment's starting altitude by more than the level band
    limit, or the vertical speed between two consecutive points exceeds the
    limit.

**This is not what OPDI's ``level-start``/``level-end`` events compute.** Those
come from the fuzzy phase classifier, which asks whether a sample looks like
level flight; ICAO asks a geometric question about a specific altitude band
anchored at the segment's own start. The two are not interchangeable and both
are published, under different type names.

Why a separate detector is worth the code: it makes the claim checkable. No
data source holds level-segment truth -- APDF has none, and neither does
anything else OPDI can reach -- so "accurate" is not a claim this family can
support. "Conformant to ICAO's published algorithm" is, and a specification can
be tested against on synthetic trajectories whose geometry is known.

The anchoring is the part a window function cannot express directly: a
segment's end is defined against *its own start*, which is a running value that
resets. That is a sessionisation, done here in two passes -- mark where a
segment cannot continue, then forward-fill the anchor within the resulting
groups -- rather than with a UDF.
"""

from typing import Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

FT_PER_M = 3.28084
FTMIN_PER_MPS = 196.850394


def level_segments(
    sdf: DataFrame,
    config,
    *,
    partition_cols: Sequence[str] = ("track_id",),
    altitude_col: str = "baro_altitude_c",
    vert_rate_col: str = "vert_rate",
    time_col: str = "event_time",
) -> DataFrame:
    """Detect ICAO level segments.

    ``altitude_col`` is metres and ``vert_rate_col`` is m/s -- the storage layer
    is SI -- and both are scaled into aviation units to meet the thresholds,
    which are expressed as ICAO expresses them.

    Returns one row per segment: the partition columns plus ``start_time``,
    ``end_time``, ``duration_seconds``, ``level_ft`` (the anchor altitude) and
    ``distance_nm`` when a cumulative distance is available.
    """
    ordered = Window.partitionBy(*partition_cols).orderBy(time_col)
    running = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    alt_ft = F.col(altitude_col) * FT_PER_M
    roc_ftmin = F.abs(F.col(vert_rate_col) * FTMIN_PER_MPS)

    work = sdf.withColumn("_alt_ft", alt_ft).withColumn("_roc", roc_ftmin)
    work = work.withColumn("_alt_next", F.lead("_alt_ft").over(ordered))

    # A sample is "flat" when the step to the next one stays inside the band and
    # the vertical speed towards it is within the limit -- ICAO's start
    # condition, evaluated at every sample.
    flat = (
        (F.abs(F.col("_alt_next") - F.col("_alt_ft")) <= F.lit(config.level_band_limit_ft))
        & (F.col("_roc") <= F.lit(config.level_vertical_speed_limit_ftmin))
    )
    work = work.withColumn("_flat", F.coalesce(flat, F.lit(False)))

    # Sessionise: a new group starts wherever flatness begins. Everything not
    # flat is discarded afterwards, so non-flat runs simply form groups of
    # their own.
    work = work.withColumn("_prev_flat", F.lag("_flat").over(ordered))
    work = work.withColumn(
        "_new_group",
        F.when(F.col("_flat") & ~F.coalesce(F.col("_prev_flat"), F.lit(False)), 1).otherwise(0),
    )
    work = work.withColumn("_group", F.sum("_new_group").over(running))

    # The anchor: the altitude at which this run of flat samples began. ICAO
    # ends a segment when the altitude leaves the band *around the anchor*, not
    # around the previous sample -- which is what stops a slow drift being read
    # as one long level segment.
    group_window = Window.partitionBy(*partition_cols, "_group").orderBy(time_col)
    work = work.withColumn("_anchor", F.first("_alt_ft").over(group_window))

    # Membership is evaluated at the sample itself, not on its step to the
    # next one. The forward step only ever *starts* a segment; ICAO ends one
    # at the point where the condition breaks, so the last level sample --
    # whose next step is the climb away -- still belongs to the segment. Using
    # the forward step for membership drops it and reports every level-off one
    # sample interval short, always in the same direction.
    work = work.withColumn(
        "_member",
        (F.abs(F.col("_alt_ft") - F.col("_anchor")) <= F.lit(config.level_band_limit_ft))
        & (F.col("_roc") <= F.lit(config.level_vertical_speed_limit_ftmin)),
    )

    # Truncate each group at its first failure rather than filtering failures
    # out, so a trajectory that leaves the band and drifts back does not get
    # stitched into one segment across the excursion.
    group_running = group_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    work = work.withColumn(
        "_failures",
        F.sum(F.when(F.col("_member"), 0).otherwise(1)).over(group_running),
    )

    seg = work.filter(F.col("_member") & (F.col("_failures") == 0))

    agg = [
        F.min(time_col).alias("start_time"),
        F.max(time_col).alias("end_time"),
        F.first("_anchor").alias("level_ft"),
    ]
    if "cumulative_distance_nm" in sdf.columns:
        agg += [
            F.min("cumulative_distance_nm").alias("_d0"),
            F.max("cumulative_distance_nm").alias("_d1"),
        ]

    out = seg.groupBy(*partition_cols, "_group").agg(*agg)
    out = out.withColumn(
        "duration_seconds",
        F.col("end_time").cast("double") - F.col("start_time").cast("double"),
    )
    if "cumulative_distance_nm" in sdf.columns:
        out = out.withColumn("distance_nm", F.col("_d1") - F.col("_d0")).drop("_d0", "_d1")
    else:
        out = out.withColumn("distance_nm", F.lit(None).cast("double"))

    return out.filter(
        F.col("duration_seconds") >= F.lit(config.level_min_duration_seconds)
    ).drop("_group")


def classify_level_offs(
    segments: DataFrame,
    config,
    *,
    toc_time,
    tod_time,
    toc_altitude_ft,
    tod_altitude_ft,
) -> DataFrame:
    """Split segments into KPI17 (climb) and KPI19 (descent) level-offs.

    Applies ICAO's two exclusions:

    * the **minimum altitude**, below which the trajectory is not analysed --
      3,000 ft in climb, 1,800 ft in descent, the difference being that an
      aircraft on final is legitimately close to level;
    * the **exclusion box**, which removes a segment sitting above
      ``level_exclusion_box_pct`` of the top-of-climb (or top-of-descent)
      altitude and lasting longer than ``level_exclusion_box_seconds``. That is
      cruise, not a level-off, and without the box every cruise would be
      counted as the largest level-off in the flight.
    """
    before_toc = F.col("end_time") <= toc_time
    after_tod = F.col("start_time") >= tod_time

    climb_floor = F.col("level_ft") >= F.lit(config.level_min_altitude_climb_ft)
    descent_floor = F.col("level_ft") >= F.lit(config.level_min_altitude_descent_ft)

    pct = F.lit(config.level_exclusion_box_pct / 100.0)
    long_enough_to_be_cruise = F.col("duration_seconds") > F.lit(
        config.level_exclusion_box_seconds
    )
    in_climb_box = (F.col("level_ft") >= pct * toc_altitude_ft) & long_enough_to_be_cruise
    in_descent_box = (F.col("level_ft") >= pct * tod_altitude_ft) & long_enough_to_be_cruise

    return segments.withColumn(
        "kpi",
        F.when(before_toc & climb_floor & ~in_climb_box, F.lit("KPI17"))
        .when(after_tod & descent_floor & ~in_descent_box, F.lit("KPI19")),
    ).filter(F.col("kpi").isNotNull())
