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

Unit epsilon: the frozen algorithm compares ``baro_altitude < 1524.0`` (metres);
the engine compares ``baro_altitude * FT_PER_M < 5000.0`` (feet). These are not
bit-identical -- they differ on the interval [1524.0, 1524.00005) metres, a band
about 0.05 mm wide, because ``FT_PER_M`` is a rounded constant. This is accepted
deliberately: the project's standing rule is aviation units with the comparison
scaled, never the stored value, and no real ADS-B sample lands in a 0.05 mm
band. Do not "fix" this by comparing in metres.
"""

from dataclasses import dataclass, field
from typing import Callable, List

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

__all__ = ["BreakRule", "SegmentationParams", "assign_track_id", "FT_PER_M", "KT_PER_MPS"]

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

_TEMP_COLS = [_GAP_MIN, _ALT_FT, _SPD_KT, "_grp", "_brk", "_offset", "_ts"]


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
    # A5-A7 knobs; unused by the gap family.
    ground_dwell_minutes: float = 5.0
    turnaround_max_height_ft: float = 1000.0
    turnaround_max_speed_kt: float = 40.0
    descent_floor_ft: float = 1500.0


@dataclass(frozen=True)
class BreakRule:
    """One arm: how to group, when to break, and whether to suffix the month."""

    name: str
    group_cols: List[str] = field(default_factory=lambda: ["icao24", "callsign"])
    break_expr: Callable[[SegmentationParams], Column] = None
    month_suffix: bool = True


def assign_track_id(
    df: DataFrame, rule: BreakRule, params: SegmentationParams
) -> DataFrame:
    """Add ``track_id`` to state vectors according to *rule*.

    The frozen algorithm hashes its group key; the engine does too, so ids are
    the same shape and the same length wherever an arm groups on the same
    columns. Ids are never compared across arms -- partitions are.
    """
    out = (
        df.withColumn("_ts", F.to_timestamp("event_time"))
        .withColumn(
            "_grp", F.substring(F.sha2(F.concat_ws("", *rule.group_cols), 256), 1, 16)
        )
        .withColumn(_ALT_FT, F.col("baro_altitude") * FT_PER_M)
        .withColumn(_SPD_KT, F.col("velocity") * KT_PER_MPS)
    )

    w = Window.partitionBy("_grp").orderBy("_ts")
    out = out.withColumn(
        _GAP_MIN,
        (F.unix_timestamp("_ts") - F.unix_timestamp(F.lag("_ts").over(w))) / 60.0,
    )

    out = out.withColumn("_brk", F.when(rule.break_expr(params), 1).otherwise(0))

    running = w.rowsBetween(Window.unboundedPreceding, 0)
    out = out.withColumn("_offset", F.sum("_brk").over(running))

    parts = [F.col("_grp"), F.lit("_"), F.col("_offset")]
    if rule.month_suffix:
        parts += [
            F.lit("_"), F.year("_ts").cast("string"),
            F.lit("_"), F.month("_ts").cast("string"),
        ]
    out = out.withColumn("track_id", F.concat(*parts))

    return out.drop(*_TEMP_COLS)


def gap_minutes() -> Column:
    """The previous-sample gap, in minutes. NULL at a group's first sample."""
    return F.col(_GAP_MIN)


def altitude_ft() -> Column:
    """Barometric altitude in feet, for comparison only."""
    return F.col(_ALT_FT)


def speed_kt() -> Column:
    """Ground speed in knots, for comparison only."""
    return F.col(_SPD_KT)
