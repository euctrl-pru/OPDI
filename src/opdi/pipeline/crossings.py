"""Threshold crossing detection.

One algorithm serves two milestone families, because they are the same problem:
a quantity attached to a trajectory passes a fixed threshold.

* **Flight level crossings** -- the quantity is flight level, the thresholds are
  FL 50/70/100/245, and the partition is the track.
* **Ring crossings** -- the quantity is distance to an aerodrome, the thresholds
  are the 40 NM and 100 NM cylinders ICAO uses for KPI05 and KPI08, and the
  partition is (track, aerodrome).

Three things distinguish this from the detector it replaces
(``events.calculate_vertical_crossing_events``):

**Every crossing is reported, not only the first and last.** The published
detector keeps ``row_number`` 1 ascending and 1 descending per
``(track_id, level)``, so a flight that levels off and re-climbs through FL100
loses the crossings in between -- which are exactly the ones a vertical
efficiency indicator is about.

**Chatter is suppressed by hysteresis rather than by discarding data.** Naively
emitting every crossing is unusable: an aircraft cruising *at* FL100 oscillates
across the bare boundary on barometric noise and would produce hundreds of
meaningless events. A crossing is therefore registered only once the aircraft
has passed from below ``L - hysteresis`` to above ``L + hysteresis`` (or the
reverse). Inside the dead band nothing is emitted. The cost is that a flight
which levels off *exactly* at a threshold never clears the far edge and so
registers no crossing there -- that case is described by the level segment and
top-of-climb events instead.

**The instant is interpolated to the exact threshold.** The published detector
reports the bracketing sample, which biases every crossing by up to one sample
interval (5 s) always in the same direction. Note the confirming sample and the
bracketing sample are *different* rows: confirmation happens when the far edge
of the dead band is cleared, while the true crossing sits at the consecutive
pair that straddles the threshold, which is earlier. This module finds the
straddling pair and interpolates within it.

Cost. Thresholds are evaluated as parallel columns over one window
specification rather than by exploding the trajectory once per threshold, so N
thresholds cost one shuffle rather than N, and the trajectory is never
multiplied. Only the handful of rows where a crossing actually fired are
exploded into events. Evaluating each threshold independently also fixes a
latent fault in the published detector, whose first-match ``when`` chain
reported only one level when a single sample interval spanned two.
"""

from typing import TYPE_CHECKING, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

if TYPE_CHECKING:  # pragma: no cover - avoids an import cycle
    from opdi.config import EventConfig


def _ordered(partition_cols: Sequence[str], time_col: str) -> Window:
    return Window.partitionBy(*partition_cols).orderBy(time_col)


def _running(partition_cols: Sequence[str], time_col: str) -> Window:
    return _ordered(partition_cols, time_col).rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )


def _slug(threshold: float) -> str:
    """A column-name-safe token for a threshold value."""
    return str(threshold).replace(".", "p").replace("-", "m")


def threshold_crossings(
    sdf: DataFrame,
    value_col: str,
    thresholds: Sequence[float],
    hysteresis: float,
    partition_cols: Sequence[str],
    *,
    time_col: str = "event_time",
    interpolate_cols: Sequence[str] = ("lat", "lon"),
    up_label: str = "up",
    down_label: str = "down",
    interpolate: bool = True,
    all_occurrences: bool = True,
    over_missing_values: bool = True,
    max_bracket_seconds: Optional[float] = None,
) -> DataFrame:
    """Detect every hysteresis-confirmed crossing of ``thresholds``.

    Parameters
    ----------
    sdf
        Trajectory samples. Must carry ``value_col``, ``time_col``, every
        column in ``partition_cols`` and every column in ``interpolate_cols``.
    value_col
        The quantity being compared against the thresholds -- flight level for
        vertical crossings, distance in NM for ring crossings.
    thresholds
        Threshold values, in the units of ``value_col``.
    hysteresis
        Half-width of the dead band, in the units of ``value_col``. A crossing
        is confirmed only once the far edge is cleared. Zero disables the dead
        band, which restores the chatter this exists to prevent.
    partition_cols
        What a trajectory is. ``["track_id"]`` for flight levels;
        ``["track_id", "apt_ident"]`` for rings. Include ``segment_id`` to stop
        crossings being inferred across a coverage hole.
    interpolate_cols
        Numeric columns interpolated to the crossing instant alongside the
        time. Their names are preserved on the output.
    up_label, down_label
        Values of the emitted ``direction`` column. Rings invert the sense of
        the words -- an increasing distance is *outbound* -- so the caller
        names them.
    interpolate
        When False, report the confirming sample as-is. This is the published
        behaviour and exists so a legacy run stays reproducible.
        ``over_missing_values`` and ``max_bracket_seconds`` then have no
        effect.
    all_occurrences
        When False, keep only the first and last crossing of each threshold,
        as the published detector did.
    over_missing_values
        Compare each report with the last earlier report that has a value,
        skipping reports where ``value_col`` is NULL. When False, each report
        is compared with the report just before it, even when that one has no
        value; a crossing over missing values then has no time, or the time of
        an earlier crossing of the same threshold. False only to reproduce
        that behaviour (``EventConfig.interpolate_crossings_over_missing_values``).
    max_bracket_seconds
        With ``over_missing_values``, a crossing whose two known values are
        more than this many seconds apart is not returned: its time and
        position would be invented. ``crossing_seq`` still counts it, so the
        crossings returned can skip a number (1, 3, 4 when the second could
        not be placed): the number says which crossing of the threshold it
        is along the track. None keeps every crossing.

    Returns:
    -------
    One row per crossing, carrying ``partition_cols`` plus ``threshold``,
    ``direction``, ``crossing_seq`` (1 for the first crossing of a threshold
    in the partition, 2 for the second, ...; see ``max_bracket_seconds`` for
    the numbers that can be missing), the (possibly interpolated) ``time_col``
    and ``interpolate_cols``, and ``bracket_seconds`` -- the spacing of the two
    samples the crossing was interpolated between, so a consumer can tell an
    instant measured across a 5 s gap from one inferred across a 4 minute hole.
    """
    if not thresholds:
        return _empty_result(sdf, partition_cols, time_col, interpolate_cols)
    # Skipping reports without a value only changes where a crossing is
    # interpolated; without interpolation the confirming report is reported
    # as it is, and the comparison with the report just before it is kept.
    if over_missing_values and interpolate:
        return _crossings_over_missing_values(
            sdf,
            value_col,
            thresholds,
            hysteresis,
            partition_cols,
            time_col=time_col,
            interpolate_cols=interpolate_cols,
            up_label=up_label,
            down_label=down_label,
            interpolate=interpolate,
            all_occurrences=all_occurrences,
            max_bracket_seconds=max_bracket_seconds,
        )

    ordered = _ordered(partition_cols, time_col)
    running = _running(partition_cols, time_col)

    t_secs = F.col(time_col).cast("double")
    work = sdf.withColumn("_t", t_secs).withColumn("_t_prev", F.lag("_t").over(ordered))
    work = work.withColumn("_v_prev", F.lag(value_col).over(ordered))
    for c in interpolate_cols:
        work = work.withColumn(f"_{c}_prev", F.lag(c).over(ordered))

    v = F.col(value_col)
    v_prev = F.col("_v_prev")

    # Stage 1 -- which side of each dead band the sample sits on, and where a
    # consecutive pair straddles the bare threshold.
    for th in thresholds:
        s = _slug(th)
        work = work.withColumn(
            f"_side_{s}",
            F.when(v > F.lit(th) + F.lit(hysteresis), F.lit(1)).when(
                v < F.lit(th) - F.lit(hysteresis), F.lit(-1)
            ),
        )
        # Equality is attributed to the upward case so a sample landing exactly
        # on the threshold is counted once, not twice or never.
        up = (v_prev < F.lit(th)) & (v >= F.lit(th))
        down = (v_prev > F.lit(th)) & (v <= F.lit(th))
        straddles = up | down
        # Safe: in both branches one endpoint is strictly beyond the threshold
        # and the other is not, so the denominator cannot be zero.
        frac = (F.lit(th) - v_prev) / (v - v_prev)
        work = work.withColumn(
            f"_xt_{s}",
            F.when(straddles, F.col("_t_prev") + frac * (F.col("_t") - F.col("_t_prev"))),
        )
        work = work.withColumn(f"_gap_{s}", F.when(straddles, F.col("_t") - F.col("_t_prev")))
        for c in interpolate_cols:
            work = work.withColumn(
                f"_x_{c}_{s}",
                F.when(
                    straddles,
                    F.col(f"_{c}_prev") + frac * (F.col(c) - F.col(f"_{c}_prev")),
                ),
            )

    # Stage 2 -- carry the last resolved side, and the last straddle, forward.
    # A window function cannot be nested inside another, so these are separate
    # columns; they share one window specification and therefore one shuffle.
    for th in thresholds:
        s = _slug(th)
        work = work.withColumn(
            f"_side_ff_{s}", F.last(f"_side_{s}", ignorenulls=True).over(running)
        )
        work = work.withColumn(f"_xt_ff_{s}", F.last(f"_xt_{s}", ignorenulls=True).over(running))
        work = work.withColumn(f"_gap_ff_{s}", F.last(f"_gap_{s}", ignorenulls=True).over(running))
        for c in interpolate_cols:
            work = work.withColumn(
                f"_x_ff_{c}_{s}",
                F.last(f"_x_{c}_{s}", ignorenulls=True).over(running),
            )

    # Stage 3 -- confirmation: the forward-filled side flipped.
    for th in thresholds:
        s = _slug(th)
        work = work.withColumn(f"_side_was_{s}", F.lag(f"_side_ff_{s}").over(ordered))

    fired = []
    for th in thresholds:
        s = _slug(th)
        now, was = F.col(f"_side_ff_{s}"), F.col(f"_side_was_{s}")
        confirmed = now.isNotNull() & was.isNotNull() & (now != was)
        fields = [
            F.lit(float(th)).alias("threshold"),
            F.when(now == 1, F.lit(up_label)).otherwise(F.lit(down_label)).alias("direction"),
            (F.timestamp_seconds(F.col(f"_xt_ff_{s}")) if interpolate else F.col(time_col)).alias(
                time_col
            ),
            (F.col(f"_gap_ff_{s}") if interpolate else F.lit(None).cast("double")).alias(
                "bracket_seconds"
            ),
        ]
        for c in interpolate_cols:
            fields.append((F.col(f"_x_ff_{c}_{s}") if interpolate else F.col(c)).alias(c))
        fired.append(F.when(confirmed, F.struct(*fields)))

    # Only rows where something fired survive; the trajectory is never
    # multiplied by the threshold count.
    work = work.withColumn("_fired", F.array_compact(F.array(*fired)))
    work = work.filter(F.size("_fired") > 0)
    out = work.select(*partition_cols, F.explode("_fired").alias("_x"))
    out = out.select(
        *partition_cols,
        F.col("_x.threshold").alias("threshold"),
        F.col("_x.direction").alias("direction"),
        F.col(f"_x.{time_col}").alias(time_col),
        F.col("_x.bracket_seconds").alias("bracket_seconds"),
        *[F.col(f"_x.{c}").alias(c) for c in interpolate_cols],
    )

    seq_window = Window.partitionBy(*partition_cols, "threshold").orderBy(time_col)
    out = out.withColumn("crossing_seq", F.row_number().over(seq_window))

    if not all_occurrences:
        total = Window.partitionBy(*partition_cols, "threshold")
        out = out.withColumn("_n", F.max("crossing_seq").over(total))
        out = out.filter(
            (F.col("crossing_seq") == 1) | (F.col("crossing_seq") == F.col("_n"))
        ).drop("_n")

    return out


def _crossings_over_missing_values(
    sdf: DataFrame,
    value_col: str,
    thresholds: Sequence[float],
    hysteresis: float,
    partition_cols: Sequence[str],
    *,
    time_col: str,
    interpolate_cols: Sequence[str],
    up_label: str,
    down_label: str,
    interpolate: bool,
    all_occurrences: bool,
    max_bracket_seconds: Optional[float],
) -> DataFrame:
    """:func:`threshold_crossings` with reports without a value skipped.

    The same three stages. The difference is the pair of reports a crossing is
    placed between: the current report and the last earlier report that has a
    value, rather than the report just before it. A report without a value
    takes part in nothing: it cannot straddle a threshold, and it does not
    change the side the track is on.

    Each straddle is carried forward as one record (time, gap, positions). A
    straddle whose known values are too far apart keeps its record with no
    time, so the confirmation that follows it finds that record, and not the
    straddle of an earlier crossing of the same threshold.

    Only called with ``interpolate`` (see :func:`threshold_crossings`).
    """
    assert interpolate, "the crossings over missing values are always interpolated"
    ordered = _ordered(partition_cols, time_col)
    running = _running(partition_cols, time_col)
    before = ordered.rowsBetween(Window.unboundedPreceding, -1)

    v = F.col(value_col)
    work = sdf.withColumn("_t", F.col(time_col).cast("double"))
    # The last earlier report with a value: its time, value and positions,
    # taken together from one report.
    known = F.when(
        v.isNotNull(),
        F.struct(
            F.col("_t").alias("t"),
            v.cast("double").alias("v"),
            *[F.col(c).cast("double").alias(c) for c in interpolate_cols],
        ),
    )
    work = work.withColumn("_known_prev", F.last(known, ignorenulls=True).over(before))
    t_prev = F.col("_known_prev.t")
    v_prev = F.col("_known_prev.v")
    gap = F.col("_t") - t_prev
    placeable = (
        F.lit(True) if max_bracket_seconds is None else gap <= F.lit(float(max_bracket_seconds))
    )

    for th in thresholds:
        s = _slug(th)
        work = work.withColumn(
            f"_side_{s}",
            F.when(v > F.lit(th) + F.lit(hysteresis), F.lit(1)).when(
                v < F.lit(th) - F.lit(hysteresis), F.lit(-1)
            ),
        )
        up = (v_prev < F.lit(th)) & (v >= F.lit(th))
        down = (v_prev > F.lit(th)) & (v <= F.lit(th))
        straddles = F.coalesce(up | down, F.lit(False))
        frac = (F.lit(th) - v_prev) / (v - v_prev)
        fields = [
            F.when(placeable, t_prev + frac * gap).alias("xt"),
            gap.alias("gap"),
        ]
        for c in interpolate_cols:
            prev_c = F.col(f"_known_prev.{c}")
            fields.append(
                F.when(placeable, prev_c + frac * (F.col(c) - prev_c)).alias(c),
            )
        work = work.withColumn(f"_st_{s}", F.when(straddles, F.struct(*fields)))

    for th in thresholds:
        s = _slug(th)
        work = work.withColumn(
            f"_side_ff_{s}", F.last(f"_side_{s}", ignorenulls=True).over(running)
        )
        work = work.withColumn(f"_st_ff_{s}", F.last(f"_st_{s}", ignorenulls=True).over(running))

    for th in thresholds:
        s = _slug(th)
        work = work.withColumn(f"_side_was_{s}", F.lag(f"_side_ff_{s}").over(ordered))

    fired = []
    for th in thresholds:
        s = _slug(th)
        now, was = F.col(f"_side_ff_{s}"), F.col(f"_side_was_{s}")
        # A report without a value has the side of the last one with a value,
        # so only a report with a value can confirm a change of side.
        confirmed = v.isNotNull() & now.isNotNull() & was.isNotNull() & (now != was)
        st = F.col(f"_st_ff_{s}")
        fields = [
            F.lit(float(th)).alias("threshold"),
            F.when(now == 1, F.lit(up_label)).otherwise(F.lit(down_label)).alias("direction"),
            (F.timestamp_seconds(st.getField("xt")) if interpolate else F.col(time_col)).alias(
                time_col
            ),
            (st.getField("gap") if interpolate else F.lit(None).cast("double")).alias(
                "bracket_seconds"
            ),
            # The order of the crossings, also for one that cannot be placed.
            F.col("_t").alias("_confirmed_t"),
        ]
        for c in interpolate_cols:
            fields.append((st.getField(c) if interpolate else F.col(c)).alias(c))
        fired.append(F.when(confirmed, F.struct(*fields)))

    work = work.withColumn("_fired", F.array_compact(F.array(*fired)))
    work = work.filter(F.size("_fired") > 0)
    out = work.select(*partition_cols, F.explode("_fired").alias("_x"))
    out = out.select(
        *partition_cols,
        F.col("_x.threshold").alias("threshold"),
        F.col("_x.direction").alias("direction"),
        F.col(f"_x.{time_col}").alias(time_col),
        F.col("_x.bracket_seconds").alias("bracket_seconds"),
        F.col("_x._confirmed_t").alias("_confirmed_t"),
        *[F.col(f"_x.{c}").alias(c) for c in interpolate_cols],
    )

    seq_window = Window.partitionBy(*partition_cols, "threshold").orderBy("_confirmed_t")
    out = out.withColumn("crossing_seq", F.row_number().over(seq_window))

    if not all_occurrences:
        total = Window.partitionBy(*partition_cols, "threshold")
        out = out.withColumn("_n", F.max("crossing_seq").over(total))
        out = out.filter(
            (F.col("crossing_seq") == 1) | (F.col("crossing_seq") == F.col("_n"))
        ).drop("_n")

    # A crossing that cannot be placed is counted in ``crossing_seq`` and then
    # left out.
    return out.filter(F.col(time_col).isNotNull()).drop("_confirmed_t")


def _empty_result(
    sdf: DataFrame,
    partition_cols: Sequence[str],
    time_col: str,
    interpolate_cols: Sequence[str],
) -> DataFrame:
    """An empty frame with the columns of a crossing frame.

    A caller configured with no thresholds (``EventConfig.legacy()`` has no
    distance circles) can still union it with the other events.
    """
    cols = (
        [F.col(c) for c in partition_cols]
        + [
            F.lit(None).cast("double").alias("threshold"),
            F.lit(None).cast("string").alias("direction"),
            F.col(time_col),
            F.lit(None).cast("double").alias("bracket_seconds"),
        ]
        + [F.col(c) for c in interpolate_cols]
        + [F.lit(None).cast("int").alias("crossing_seq")]
    )
    return sdf.select(*cols).limit(0)


def flight_level_crossings(
    sdf: DataFrame,
    config: "EventConfig",
    *,
    partition_cols: Optional[Sequence[str]] = None,
    altitude_col: str = "baro_altitude_c",
    interpolate_cols: Sequence[str] = ("lat", "lon"),
) -> DataFrame:
    """Flight level crossings, in the vocabulary of :class:`EventConfig`.

    ``altitude_col`` is metres (the storage layer is SI); the comparison is
    scaled into flight levels, which is where the thresholds are expressed.
    """
    work = sdf.withColumn("flight_level", F.col(altitude_col) * 3.28084 / 100.0)
    return threshold_crossings(
        work,
        value_col="flight_level",
        thresholds=list(config.crossing_levels_fl),
        hysteresis=config.crossing_hysteresis_ft / 100.0,
        partition_cols=list(partition_cols or ["track_id"]),
        interpolate_cols=tuple(interpolate_cols),
        up_label="up",
        down_label="down",
        interpolate=config.crossing_interpolate,
        all_occurrences=config.crossing_all_occurrences,
        **missing_value_options(config),
    )


def missing_value_options(config: "EventConfig") -> dict:
    """The two missing-value arguments of :func:`threshold_crossings` from ``config``.

    A crossing is placed between known values when
    ``interpolate_crossings_over_missing_values`` is set, and is not given when
    those values are more than ``track_hole_seconds`` apart.
    """
    on = bool(getattr(config, "interpolate_crossings_over_missing_values", True))
    return {
        "over_missing_values": on,
        "max_bracket_seconds": float(config.track_hole_seconds) if on else None,
    }


def ring_crossings(
    sdf: DataFrame,
    config: "EventConfig",
    *,
    distance_col: str = "distance_nm",
    partition_cols: Optional[Sequence[str]] = None,
    interpolate_cols: Sequence[str] = ("lat", "lon", "flight_level"),
) -> DataFrame:
    """Aerodrome ring crossings.

    ``sdf`` must already carry the distance from each sample to the aerodrome
    it is being tested against, and be partitioned by that aerodrome -- one row
    per (sample, aerodrome) pair.
    """
    return threshold_crossings(
        sdf,
        value_col=distance_col,
        thresholds=list(config.ring_radii_nm),
        hysteresis=config.ring_hysteresis_nm,
        partition_cols=list(partition_cols or ["track_id", "apt_ident"]),
        interpolate_cols=tuple(interpolate_cols),
        # Distance grows as the aircraft leaves, so the sense of the words is
        # the opposite of the vertical case.
        up_label="outbound",
        down_label="inbound",
        interpolate=config.crossing_interpolate,
        all_occurrences=config.crossing_all_occurrences,
        **missing_value_options(config),
    )
