"""The engine that splits position reports into tracks.

A track rule (:class:`BreakRule`) says how the reports are grouped (for
example by aircraft address) and at which report a new track starts. The
engine, :func:`assign_track_id`, sorts each group by time, marks the reports
at which the rule starts a new track, numbers the tracks and builds their
``track_id``. The rules themselves are in :mod:`opdi.pipeline.segmentation.methods`.

A rule's break expression may use the names the engine computes before
calling it:

* ``_grp``: the hashed group key, built from ``BreakRule.group_cols``;
* ``_ts``: ``event_time`` as a timestamp, the order of the reports;
* the accessors :func:`gap_minutes`, :func:`altitude_ft` and :func:`speed_kt`;
* :func:`segment_window`, the engine's window over one group in time order.

A rule that needs a window must use :func:`segment_window`, so that it
groups the reports exactly as the engine does.

Units: the thresholds are in aviation units (minutes, feet, knots); the
stored reports are in SI units (metres, metres per second). The stored
column is scaled to the threshold's unit for the comparison and never
written back. Because the conversion factor is rounded, the comparison
``altitude * 3.28084 < 5000`` differs from ``altitude < 1524`` only for
altitudes between 1524 m and 1524.00005 m.
"""

from dataclasses import dataclass, field, fields
from typing import Callable, List

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.window import WindowSpec

__all__ = [
    "BreakRule",
    "SegmentationParams",
    "assign_track_id",
    "segment_window",
    "w_partition_cols",
    "gap_minutes",
    "altitude_ft",
    "speed_kt",
    "lookback_minutes",
    "FT_PER_M",
    "KT_PER_MPS",
    "FPM_PER_MPS",
]

#: The pipeline's conversion factors, shared so they cannot differ.
FT_PER_M = 3.28084
KT_PER_MPS = 1.94384
FPM_PER_MPS = 196.850394

#: Gap in minutes from the previous sample of the same group. Available to every
#: break expression; the window is applied by :func:`assign_track_id`.
_GAP_MIN = "_gap_minutes"
#: Barometric altitude in feet. Scaled for comparison only; never written.
_ALT_FT = "_alt_ft"
#: Ground speed in knots. Scaled for comparison only; never written.
_SPD_KT = "_spd_kt"
#: The hashed group key; see the module description.
_GRP = "_grp"
#: ``event_time`` as a timestamp, the order of the reports in a group.
_TS = "_ts"

_TEMP_COLS = [_GAP_MIN, _ALT_FT, _SPD_KT, _GRP, "_brk", "_offset", _TS]


@dataclass(frozen=True)
class SegmentationParams:
    """Thresholds of the track rules, in aviation units.

    The fields and defaults are those of
    :class:`opdi.config.SegmentationConfig`, where each is described; build
    an instance with :meth:`from_config`. The first three are the gap
    thresholds also used by the rule of release v0.0.2 (30 minutes, and 15
    minutes below 5,000 ft, which is 1524 m).
    """

    gap_minutes: float = 30.0
    low_alt_gap_minutes: float = 15.0
    low_alt_ft: float = 5000.0
    #: How far back, in minutes, the last call sign is looked for when deciding
    #: whether the call sign changed. ``None`` uses ``gap_minutes``.
    callsign_lookback_minutes: float | None = None
    #: Seconds a new call sign must be kept before the change starts a new
    #: track; a call sign garbled for a few seconds then does not split a
    #: flight. ``0.0`` splits at once.
    callsign_min_persistence_seconds: float = 30.0
    # Thresholds that decide whether a gap in reception is linked into one
    # track (the "standard" rule only). Each is one test of ``linkable`` in
    # ``methods.py``.
    #: Longest gap that can be linked: 10 hours covers the longest oceanic gaps.
    link_max_gap_minutes: float = 600.0
    #: Altitude above which aircraft are en route rather than near an airport.
    link_cruise_floor_ft: float = 18000.0
    link_edge_vert_rate_fpm: float = 500.0
    link_callsign_lookahead_minutes: float = 10.0
    #: How old the call sign before a gap may be and still count. Step 02
    #: reads this far back, so every day's run sees the same reports: see
    #: ``tracks.required_lookback_hours``.
    link_callsign_lookback_minutes: float = 60.0
    #: How far from each side of a gap to skip reports without a position,
    #: because the position often arrives in a later message than altitude
    #: and speed. No position within it means split. The side's altitude,
    #: vertical rate and speed are read from its nearest ``LINK_EDGE_SAMPLES``
    #: values within the same span. Step 02 reads at least this far back.
    link_edge_position_minutes: float = 5.0
    #: Lowest implied speed across a gap; a slower crossing suggests a landing
    #: in between.
    link_min_implied_speed_kt: float = 400.0
    link_max_implied_speed_kt: float = 750.0
    #: ``0.0`` turns this test off.
    link_min_speed_ratio: float = 0.0
    #: ``0.0`` turns this test off.
    link_min_stop_minutes: float = 0.0
    link_vfast_kt: float = 650.0
    #: Let the old side of a gap below the cruise floor be linked when it is
    #: climbing: an aircraft lost while still climbing is outbound.
    link_old_edge_climb: bool = True
    #: Let the new side of a gap below the cruise floor be linked when it is
    #: descending; the implied speed test still leaves out hidden landings.
    link_new_edge_descent: bool = True

    @classmethod
    def from_config(cls, config) -> "SegmentationParams":
        """The thresholds of a configuration.

        Args:
            config: An :class:`opdi.config.OPDIConfig` or its
                :class:`opdi.config.SegmentationConfig`.

        Returns:
            The parameters, with every field taken from the configuration.
        """
        seg = getattr(config, "segmentation", config)
        missing = [f.name for f in fields(cls) if not hasattr(seg, f.name)]
        if missing:
            raise TypeError(f"{type(seg).__name__} is missing segmentation fields: {missing}")
        return cls(**{f.name: getattr(seg, f.name) for f in fields(cls)})


@dataclass(frozen=True)
class BreakRule:
    """A track rule: how to group the reports and where a new track starts.

    Attributes:
        name: Name of the rule.
        break_expr: Function of :class:`SegmentationParams` that returns a
            Boolean column, true at each report that starts a new track.
        group_cols: Columns whose values group the reports, hashed into the
            group key.
        month_suffix: End each ``track_id`` with ``_{year}_{month}``, as in
            release v0.0.2.
        id_from_start_time: Number each track by the time of its first
            report; see below.
    """

    name: str
    break_expr: Callable[["SegmentationParams"], Column]
    group_cols: List[str] = field(default_factory=lambda: ["icao24", "callsign"])
    month_suffix: bool = True
    id_from_start_time: bool = False
    """Number each track by the second of its first report rather than by
    counting tracks from the start of the data read.

    A running count depends on how much data was read before the track, so
    the same track could get different ids in two runs. The time of its first
    report does not. The rule of release v0.0.2 keeps the running count and
    the month suffix, which reproduces its published ids.
    """

    def __post_init__(self):
        """Refuse a break expression that cannot be called."""
        if not callable(self.break_expr):
            raise TypeError(
                f"BreakRule({self.name!r}): break_expr must be callable, got "
                f"{type(self.break_expr).__name__}"
            )


def assign_track_id(df: DataFrame, rule: BreakRule, params: SegmentationParams) -> DataFrame:
    """Add ``track_id`` to state vectors according to ``rule``.

    Args:
        df: State vectors with at least ``event_time`` and the columns of
            ``rule.group_cols``.
        rule: The track rule.
        params: Thresholds read by the rule's break expression.

    Returns:
        The state vectors with a ``track_id`` column. The id is the first 16
        hexadecimal characters of the SHA-256 hash of the group key, then
        ``_`` and the track's number (or start second), and for the v0.0.2
        rule ``_{year}_{month}``.
    """
    out = (
        df.withColumn(_TS, F.to_timestamp("event_time"))
        .withColumn(_GRP, F.substring(F.sha2(F.concat_ws("", *rule.group_cols), 256), 1, 16))
        .withColumn(_ALT_FT, F.col("baro_altitude") * FT_PER_M)
        .withColumn(_SPD_KT, F.col("velocity") * KT_PER_MPS)
    )

    w = segment_window()
    out = out.withColumn(
        _GAP_MIN,
        (F.unix_timestamp(_TS) - F.unix_timestamp(F.lag(_TS).over(w))) / 60.0,
    )

    out = out.withColumn("_brk", F.when(rule.break_expr(params), 1).otherwise(0))

    running = w.rowsBetween(Window.unboundedPreceding, 0)
    out = out.withColumn("_offset", F.sum("_brk").over(running))

    if rule.id_from_start_time:
        # The second of the track's first report, which does not depend on how
        # much data was read. ``_offset`` still tells the tracks apart; only
        # the label changes. Two tracks of one group cannot start in the same
        # second.
        start = F.min(_TS).over(Window.partitionBy(_GRP, "_offset"))
        out = out.withColumn(
            "track_id",
            F.concat(F.col(_GRP), F.lit("_"), F.unix_timestamp(start).cast("string")),
        )
    else:
        parts = [F.col(_GRP), F.lit("_"), F.col("_offset")]
        if rule.month_suffix:
            parts += [
                F.lit("_"),
                F.year(_TS).cast("string"),
                F.lit("_"),
                F.month(_TS).cast("string"),
            ]
        out = out.withColumn("track_id", F.concat(*parts))

    return out.drop(*_TEMP_COLS)


def segment_window() -> WindowSpec:
    """The engine's window over one group in time order.

    It is ``partitionBy(_grp).orderBy(_ts)``. Rules that need a window use
    this one, so that they group the reports exactly as the engine does.
    """
    return Window.partitionBy(_GRP).orderBy(_TS)


def w_partition_cols() -> list:
    """The columns :func:`segment_window` is partitioned by.

    For a rule that needs the same window in reverse order.
    """
    return [F.col(_GRP)]


def gap_minutes() -> Column:
    """The previous-sample gap, in minutes. NULL at a group's first sample."""
    return F.col(_GAP_MIN)


def altitude_ft() -> Column:
    """Barometric altitude in feet, for comparison only."""
    return F.col(_ALT_FT)


def speed_kt() -> Column:
    """Ground speed in knots, for comparison only."""
    return F.col(_SPD_KT)


def lookback_minutes(p: "SegmentationParams") -> float:
    """How far back, in minutes, the last call sign is looked for.

    The explicit ``callsign_lookback_minutes``, or ``gap_minutes`` when that
    is ``None``. ``0.0`` is a valid value, which turns the call sign test off.
    """
    if p.callsign_lookback_minutes is None:
        return p.gap_minutes
    return p.callsign_lookback_minutes
