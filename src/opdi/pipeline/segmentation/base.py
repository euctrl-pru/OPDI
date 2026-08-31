"""The generic track-segmentation engine.

Every arm of the track-construction study is this one function plus a break
expression. The engine groups an airframe's samples, marks the samples at which a
break occurs, numbers the runs between breaks, and builds an id.

``tracks.py:_add_track_id`` is frozen and is not touched by this module. What
guarantees the two agree is ``tests/test_segmentation_base.py``, which asserts
the engine reproduces the frozen algorithm's *partition* under production
parameters. That test is the contract; the code below is only its implementation.

Units: parameters are aviation (minutes, feet, knots); ``osn_tracks`` is SI
(metres, m/s). The comparison is scaled, never the stored column -- the same rule
``cleaning/native.py`` follows, and for the same reason: a threshold 3.28x too
large simply never fires.

The **arm contract**. A break expression is handed a frame on which the engine
has already computed everything an arm is allowed to depend on, and those names
are part of the contract, not engine internals:

* ``_grp`` -- the hashed group key, whatever ``BreakRule.group_cols`` said
* ``_ts``  -- ``event_time`` as a timestamp, the engine's ordering column
* the three accessors :func:`gap_minutes`, :func:`altitude_ft`, :func:`speed_kt`
* :func:`segment_window` -- the engine's own ``partitionBy(_grp).orderBy(_ts)``

An arm that needs a window **must** call :func:`segment_window` rather than
rebuild one. A hand-built ``Window.partitionBy("icao24", "callsign")`` would
silently disagree with the engine's grouping for any arm whose ``group_cols``
are not those two, and nothing in the output would show it.

Unit epsilon: the frozen algorithm compares ``baro_altitude < 1524.0`` (metres);
the engine compares ``baro_altitude * FT_PER_M < 5000.0`` (feet). These are not
bit-identical -- they differ on the interval [1524.0, 1524.00005) metres, a band
about 0.05 mm wide, because ``FT_PER_M`` is a rounded constant. This is accepted
deliberately: the project's standing rule is aviation units with the comparison
scaled, never the stored value, and no real ADS-B sample lands in a 0.05 mm
band. Do not "fix" this by comparing in metres.
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
    "gap_minutes",
    "altitude_ft",
    "speed_kt",
    "lookback_minutes",
    "FT_PER_M",
    "KT_PER_MPS",
]

#: Reused from the pipeline's own conversions so the two can never drift.
FT_PER_M = 3.28084
KT_PER_MPS = 1.94384

#: Gap in minutes from the previous sample of the same group. Available to every
#: break expression; the window is applied by :func:`assign_track_id`.
_GAP_MIN = "_gap_minutes"
#: Barometric altitude in feet. Scaled for comparison only; never written.
_ALT_FT = "_alt_ft"
#: Ground speed in knots. Scaled for comparison only; never written.
_SPD_KT = "_spd_kt"
#: The hashed group key. Part of the arm contract -- see the module docstring.
_GRP = "_grp"
#: ``event_time`` as a timestamp; the engine's ordering column. Arm contract.
_TS = "_ts"

_TEMP_COLS = [_GAP_MIN, _ALT_FT, _SPD_KT, _GRP, "_brk", "_offset", _TS]


@dataclass(frozen=True)
class SegmentationParams:
    """Thresholds for the gap family. Aviation units, unit in every name.

    Defaults reproduce production: ``track_gap_threshold_minutes = 30``,
    ``track_gap_low_altitude_minutes = 15``, ``track_gap_low_altitude_meters =
    1524.0`` -- the last expressed here as 5,000 ft, which is what 1524 m is and
    what the frozen code's own comment calls it.
    """

    gap_minutes: float = 30.0
    low_alt_gap_minutes: float = 15.0
    low_alt_ft: float = 5000.0
    #: Bound on A8's callsign lookback, in minutes. ``None`` means "follow
    #: ``gap_minutes``", which is what the rule did when the bound was written
    #: and is therefore the only default that reproduces published behaviour.
    #:
    #: Separate from ``gap_minutes`` because the two answer different questions.
    #: ``gap_minutes`` asks how long a reception hole must be before it is a new
    #: flight. This asks how long a *callsign* stays valid for comparison across
    #: blank samples. Nothing says one number is right for both; they were the
    #: same number because one was to hand when the other was needed.
    callsign_lookback_minutes: float | None = None
    # A5-A7 knobs; unused by the gap family.
    ground_dwell_minutes: float = 5.0
    turnaround_max_height_ft: float = 1000.0
    turnaround_max_speed_kt: float = 40.0
    descent_floor_ft: float = 1500.0

    @classmethod
    def from_config(cls, config) -> "SegmentationParams":
        """Build the engine's parameters from ``OPDIConfig``.

        ``config.SegmentationConfig`` carries the same seven fields with the same
        defaults, and this is the only thing that reads it. Without this method
        the two were a coincidence rather than a link: someone setting
        ``OPDIConfig().segmentation.low_alt_ft`` would have observed no effect and
        no error. ``tests/test_segmentation_base.py`` asserts the two default
        sets are identical field by field, so they cannot drift apart again.

        Accepts either an ``OPDIConfig`` or a ``SegmentationConfig`` directly.
        """
        seg = getattr(config, "segmentation", config)
        missing = [f.name for f in fields(cls) if not hasattr(seg, f.name)]
        if missing:
            raise TypeError(
                f"{type(seg).__name__} is missing segmentation fields: {missing}"
            )
        return cls(**{f.name: getattr(seg, f.name) for f in fields(cls)})


@dataclass(frozen=True)
class BreakRule:
    """One arm: how to group, when to break, and whether to suffix the month.

    ``break_expr`` is required and comes second, ahead of the two fields that
    have defaults. It used to be typed as required while defaulting to ``None``,
    so a ``BreakRule`` built without one constructed happily and then died inside
    :func:`assign_track_id` with ``TypeError: 'NoneType' object is not
    callable`` -- a message that names neither the rule nor the missing field.
    Every construction in this package is by keyword, so the order change is not
    a breaking one.
    """

    name: str
    break_expr: Callable[["SegmentationParams"], Column]
    group_cols: List[str] = field(default_factory=lambda: ["icao24", "callsign"])
    month_suffix: bool = True

    def __post_init__(self):
        if not callable(self.break_expr):
            raise TypeError(
                f"BreakRule({self.name!r}): break_expr must be callable, got "
                f"{type(self.break_expr).__name__}"
            )


def assign_track_id(
    df: DataFrame, rule: BreakRule, params: SegmentationParams
) -> DataFrame:
    """Add ``track_id`` to state vectors according to *rule*.

    The frozen algorithm hashes its group key; the engine does too, so ids are
    the same shape and the same length wherever an arm groups on the same
    columns. Ids are never compared across arms -- partitions are.
    """
    out = (
        df.withColumn(_TS, F.to_timestamp("event_time"))
        .withColumn(
            _GRP, F.substring(F.sha2(F.concat_ws("", *rule.group_cols), 256), 1, 16)
        )
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

    parts = [F.col(_GRP), F.lit("_"), F.col("_offset")]
    if rule.month_suffix:
        parts += [
            F.lit("_"), F.year(_TS).cast("string"),
            F.lit("_"), F.month(_TS).cast("string"),
        ]
    out = out.withColumn("track_id", F.concat(*parts))

    return out.drop(*_TEMP_COLS)


def segment_window() -> WindowSpec:
    """The engine's own ordering window: ``partitionBy(_grp).orderBy(_ts)``.

    Part of the arm contract. Four arms need a window and every one of them used
    to rebuild it from the private column names by copy-paste; an arm that
    guessed ``Window.partitionBy("icao24", "callsign")`` would have got a
    *different* partition from the engine's for any arm grouping on something
    else, with nothing in the output to show it.
    """
    return Window.partitionBy(_GRP).orderBy(_TS)


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
    """A8's lookback bound: the explicit value, or ``gap_minutes`` when unset.

    ``is None`` rather than ``or``: ``0.0`` is a meaningful setting -- it
    disables the callsign-change break, which is the grid's ``airframe_only``
    corner -- and ``or`` would read it as unset.
    """
    if p.callsign_lookback_minutes is None:
        return p.gap_minutes
    return p.callsign_lookback_minutes
