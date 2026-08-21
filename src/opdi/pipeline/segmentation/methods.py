"""One function per arm of the track-construction study.

Each returns a :class:`~opdi.pipeline.segmentation.base.BreakRule`. No Spark
session logic lives here -- an arm is a grouping, a predicate and a flag.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from opdi.pipeline.segmentation.base import _ALT_FT, BreakRule, altitude_ft, gap_minutes

__all__ = ["legacy", "no_month_suffix", "traffic_style", "airframe_only", "ARMS"]


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


ARMS = {
    "legacy": legacy,
    "no_month_suffix": no_month_suffix,
    "traffic_style": traffic_style,
    "airframe_only": airframe_only,
}
