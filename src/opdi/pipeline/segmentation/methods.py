"""One function per arm of the track-construction study.

Each returns a :class:`~opdi.pipeline.segmentation.base.BreakRule`. No Spark
session logic lives here -- an arm is a grouping, a predicate and a flag.
"""

from opdi.pipeline.segmentation.base import BreakRule, altitude_ft, gap_minutes

__all__ = ["legacy", "ARMS"]


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


ARMS = {"legacy": legacy}
