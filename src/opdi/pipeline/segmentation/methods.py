"""One function per arm of the track-construction study.

Each returns a :class:`~opdi.pipeline.segmentation.base.BreakRule`. No Spark
session logic lives here -- an arm is a grouping, a predicate and a flag.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from opdi.pipeline.segmentation.base import (
    _ALT_FT,
    _SPD_KT,
    BreakRule,
    altitude_ft,
    gap_minutes,
)

__all__ = [
    "legacy",
    "no_month_suffix",
    "traffic_style",
    "airframe_only",
    "ground_anchored",
    "airport_anchored",
    "vertical_profile",
    "recommended",
    "ARMS",
]


def legacy() -> BreakRule:
    """A0 -- the production algorithm, as parameters of the engine.

    Split when the gap exceeds ``gap_minutes``, or exceeds
    ``low_alt_gap_minutes`` below ``low_alt_ft``. The month suffix is kept: it is
    what production does, and A2 exists to measure removing it.
    """
    return BreakRule(
        name="legacy",
        group_cols=["icao24", "callsign"],
        break_expr=lambda p: (gap_minutes() > p.gap_minutes)
        | ((gap_minutes() > p.low_alt_gap_minutes) & (altitude_ft() < p.low_alt_ft)),
        month_suffix=True,
    )


def no_month_suffix() -> BreakRule:
    """A2 -- legacy without the ``_{year}_{month}`` id suffix.

    The suffix splits any flight airborne at midnight on the 1st of a month, by
    construction and regardless of the data. That is a correctness defect, not a
    threshold, which is why this arm is separate from the A1 sweep: sweeping
    parameters cannot reach it.
    """
    rule = legacy()
    return BreakRule(
        name="no_month_suffix",
        group_cols=rule.group_cols,
        break_expr=rule.break_expr,
        month_suffix=False,
    )


def traffic_style() -> BreakRule:
    """A3 -- ``traffic``'s ``Flight.split`` semantics.

    A single gap threshold (traffic's default is 10 minutes) with a *predicate*
    deciding whether a candidate gap is really a break, rather than a second
    hard-coded threshold. traffic's own documented example is "do not split below
    5,000 ft"; the aviation-sensible inverse is used here -- do not split when
    both sides of the gap are above ``low_alt_ft``, because a reception hole in
    the cruise is a coverage gap, not a landing.
    """

    def expr(p):
        w = Window.partitionBy("_grp").orderBy("_ts")
        prev_alt = F.lag(F.col(_ALT_FT)).over(w)
        both_airborne = (F.col(_ALT_FT) >= p.low_alt_ft) & (prev_alt >= p.low_alt_ft)
        return (gap_minutes() > TRAFFIC_DEFAULT_GAP_MINUTES) & ~both_airborne

    return BreakRule(
        name="traffic_style",
        group_cols=["icao24", "callsign"],
        break_expr=expr,
        month_suffix=False,
    )


#: ``traffic``'s documented default for ``Flight.split``.
TRAFFIC_DEFAULT_GAP_MINUTES = 10.0


def airframe_only() -> BreakRule:
    """A4 -- group on ``icao24`` alone; callsign becomes an attribute.

    A callsign change mid-flight splits a track under legacy, and a missing
    callsign collapses an airframe's segments into one group. Identity belongs to
    the airframe; the callsign is something the airframe was broadcasting at the
    time.
    """
    rule = legacy()
    return BreakRule(
        name="airframe_only",
        group_cols=["icao24"],
        break_expr=rule.break_expr,
        month_suffix=False,
    )


def ground_anchored() -> BreakRule:
    """A5 -- a break is a ground contact long enough to be a turnaround.

    ``on_ground`` is in the OSN schema and step 02 reads none of it. A break is
    declared at the *first airborne sample after* a run of on-ground samples
    lasting at least ``ground_dwell_minutes`` -- so the departing leg starts at
    the rotation, not at the arrival's last airborne point.

    The dwell test is what separates a turnaround from a touch-and-go. The legacy
    gap rules are kept as a fallback, because an aircraft that stops broadcasting
    entirely still has to be split somehow.
    """

    def expr(p):
        w = Window.partitionBy("_grp").orderBy("_ts")
        # A ground run's length: time from the first to the last of the
        # consecutive on-ground samples immediately preceding this one.
        ground_start = F.last(
            F.when(~F.col("on_ground"), F.col("_ts")), ignorenulls=True
        ).over(w.rowsBetween(Window.unboundedPreceding, -1))
        prev_ts = F.lag("_ts").over(w)
        prev_ground = F.lag("on_ground").over(w)
        dwell_min = (F.unix_timestamp(prev_ts) - F.unix_timestamp(ground_start)) / 60.0
        turnaround = (
            ~F.col("on_ground")
            & prev_ground
            & (dwell_min >= p.ground_dwell_minutes)
        )
        return turnaround | legacy().break_expr(p)

    return BreakRule(
        name="ground_anchored",
        group_cols=["icao24"],
        break_expr=expr,
        month_suffix=False,
    )


def airport_anchored() -> BreakRule:
    """A6 -- a break must happen *at an aerodrome*.

    Requires ``near_airport`` and ``field_elev_ft`` on the input; Task 6's runner
    joins them from ``h3_airport_detection_zones`` and ``oa_airports`` before
    calling the engine.

    Two things this fixes that no altitude threshold can. An aircraft parked at a
    6,000 ft aerodrome never satisfies ``baro_altitude < 1524 m``, so legacy
    misses the turnaround and merges two flights -- the case
    ``track_quality.py`` names. And a long, slow, low dwell in the middle of
    nowhere satisfies legacy's low-altitude rule and is split, when it is not a
    turnaround at all.

    Height above field, not barometric altitude, is the test.
    """

    def expr(p):
        w = Window.partitionBy("_grp").orderBy("_ts")
        height_ft = F.col(_ALT_FT) - F.col("field_elev_ft")
        stationary = (
            F.col("near_airport")
            & (height_ft < p.turnaround_max_height_ft)
            & (F.coalesce(F.col(_SPD_KT), F.lit(0.0)) < p.turnaround_max_speed_kt)
        )
        prev_stationary = F.lag(stationary).over(w)
        dwell_min = gap_minutes()
        # A break where the previous sample was stationary at an aerodrome and
        # enough time passed for a turnaround -- whether that time was a gap in
        # reception or a run of parked samples.
        return prev_stationary & (dwell_min >= p.ground_dwell_minutes)

    return BreakRule(
        name="airport_anchored",
        group_cols=["icao24"],
        break_expr=expr,
        month_suffix=False,
    )


def vertical_profile() -> BreakRule:
    """A7 -- a flight is one climb-cruise-descent cycle.

    A descent reaching ``descent_floor_ft`` followed by a climb away from it is a
    new sortie, whether or not there was a gap and whether or not ground contact
    was broadcast. This catches merges that no gap rule can reach, and it is the
    only arm that works when the transponder never reports ``on_ground``.

    The floor test is what keeps a cruise step-descent (FL350 -> FL310) from
    counting: that never approaches the floor.
    """

    def expr(p):
        w = Window.partitionBy("_grp").orderBy("_ts")
        below_floor = F.col(_ALT_FT) <= p.descent_floor_ft
        # The most recent sample at or below the floor, before this one.
        was_below = F.max(F.when(below_floor, 1).otherwise(0)).over(
            w.rowsBetween(Window.unboundedPreceding, -1)
        )
        climbing_away = (F.col(_ALT_FT) > p.descent_floor_ft) & (
            F.lag(F.col(_ALT_FT)).over(w) <= p.descent_floor_ft
        )
        return (was_below == 1) & climbing_away

    return BreakRule(
        name="vertical_profile",
        group_cols=["icao24"],
        break_expr=expr,
        month_suffix=False,
    )


def recommended() -> BreakRule:
    """A8 -- the combination the study recommends.

    Placeholder until the ladder has run: currently ground-anchored grouping with
    the airport test as a guard. **Task 9 replaces this body with whatever the
    measured results support, and the paper states the evidence.** Do not ship a
    v2 on this default.
    """

    def expr(p):
        return ground_anchored().break_expr(p) | airport_anchored().break_expr(p)

    return BreakRule(
        name="recommended",
        group_cols=["icao24"],
        break_expr=lambda p: F.coalesce(expr(p), F.lit(False)),
        month_suffix=False,
    )


ARMS = {
    "legacy": legacy,
    "no_month_suffix": no_month_suffix,
    "traffic_style": traffic_style,
    "airframe_only": airframe_only,
    "ground_anchored": ground_anchored,
    "airport_anchored": airport_anchored,
    "vertical_profile": vertical_profile,
    "recommended": recommended,
}
