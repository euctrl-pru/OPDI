"""One function per arm of the track-construction study.

Each returns a :class:`~opdi.pipeline.segmentation.base.BreakRule`. No Spark
session logic lives here -- an arm is a grouping, a predicate and a flag.

**Every domain arm (A5-A7) ORs the legacy gap rule in as a floor.** Two reasons,
and they are the standing rule for any arm added later:

1. *The ladder measures one change at a time.* Each domain arm is "legacy plus
   one idea", so its delta from A0 is attributable to that idea alone. An arm
   that also silently *removed* legacy's splits would report a delta that mixes
   two changes, and the study could not say which one moved the number.
2. *An arm whose inputs are missing must degrade to legacy, not to one track a
   month.* A6 depends on ``near_airport`` and ``field_elev_ft``, which Task 6
   supplies by a left join -- so every sample away from a covered aerodrome gets
   NULL. With no floor, an airframe that never gets a reception gap at a covered
   aerodrome would have its entire month assigned to a single track, and A6
   would score as a catastrophic merger for reasons having nothing to do with
   its idea.

The cost of the floor is stated where it bites: A6 can only ever *add* splits to
legacy's, so it cannot suppress legacy's spurious low-altitude split away from
any aerodrome. See :func:`airport_anchored`.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from opdi.pipeline.segmentation.base import (
    BreakRule,
    altitude_ft,
    gap_minutes,
    segment_window,
    speed_kt,
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
    "TRAFFIC_DEFAULT_GAP_MINUTES",
    "ARMS",
]

#: ``traffic``'s documented default for :meth:`Flight.split`. Defined here, above
#: its only use, because it is the one number an A3 sweep would want to vary.
TRAFFIC_DEFAULT_GAP_MINUTES = 10.0


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
        w = segment_window()
        prev_alt = F.lag(altitude_ft()).over(w)
        both_airborne = (altitude_ft() >= p.low_alt_ft) & (prev_alt >= p.low_alt_ft)
        return (gap_minutes() > TRAFFIC_DEFAULT_GAP_MINUTES) & ~both_airborne

    return BreakRule(
        name="traffic_style",
        group_cols=["icao24", "callsign"],
        break_expr=expr,
        month_suffix=False,
    )


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
        w = segment_window()
        # A ground run's length: time from the first to the last of the
        # consecutive on-ground samples immediately preceding this one.
        ground_start = F.last(
            F.when(~F.col("on_ground"), F.col("_ts")), ignorenulls=True
        ).over(w.rowsBetween(Window.unboundedPreceding, -1))
        prev_ts = F.lag("_ts").over(w)
        prev_ground = F.lag("on_ground").over(w)
        ground_run_min = (
            F.unix_timestamp(prev_ts) - F.unix_timestamp(ground_start)
        ) / 60.0
        turnaround = (
            ~F.col("on_ground")
            & prev_ground
            & (ground_run_min >= p.ground_dwell_minutes)
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

    The case this fixes that no altitude threshold can: an aircraft parked at a
    6,000 ft aerodrome never satisfies ``baro_altitude < 1524 m``, so legacy
    misses the turnaround and merges two flights -- the case ``track_quality.py``
    names. **Height above field, not barometric altitude, is the test.**

    A turnaround is recognised two ways, and the arm fires on either:

    * *a reception gap* -- the previous sample was stationary at an aerodrome and
      at least ``ground_dwell_minutes`` passed before the next one arrived;
    * *a run of parked samples* -- a continuous stationary-at-aerodrome run
      lasting at least ``ground_dwell_minutes``, broken at the first sample that
      moves again. This is the same accumulation :func:`ground_anchored` does,
      and it is what makes the arm comparable with A5. Before it existed the arm
      used the previous-sample gap as its dwell, which for a run of parked
      samples is the *sampling period* -- seconds -- so A6 fired only on
      reception gaps, and an A5-vs-A6 comparison conflated "on-ground flag versus
      airport geometry" with "continuous coverage versus gap-only".

    **Missing inputs.** ``near_airport`` is coalesced to ``False`` and
    ``field_elev_ft`` to ``0.0``, so the predicate is a real boolean rather than
    NULL wherever the join found nothing. A sample with no ``near_airport`` can
    never trigger A6's own rule, so the arm degrades to the legacy floor there. A
    sample that *is* near a known aerodrome but whose elevation is unknown is
    tested at sea level -- i.e. on barometric altitude, exactly as legacy would,
    which is the conservative direction: it can miss a high-field turnaround, it
    cannot invent a low-field one.

    **The legacy floor's cost, stated.** Because the legacy gap rule is ORed in
    (see the module docstring), A6 can only ever *add* splits to legacy's. The
    inverse case an earlier draft claimed -- a long, slow, low dwell in the
    middle of nowhere that satisfies legacy's low-altitude rule and is split when
    it is not a turnaround at all -- is therefore **not** fixed by this arm. That
    is a deliberate consequence of the one-change-at-a-time ladder, not an
    oversight; an arm that suppressed legacy splits would need to be a separate
    rung.
    """

    def expr(p):
        w = segment_window()
        height_ft = altitude_ft() - F.coalesce(F.col("field_elev_ft"), F.lit(0.0))
        stationary = (
            F.coalesce(F.col("near_airport"), F.lit(False))
            & (height_ft < p.turnaround_max_height_ft)
            & (F.coalesce(speed_kt(), F.lit(0.0)) < p.turnaround_max_speed_kt)
        )
        prev_stationary = F.lag(stationary).over(w)
        prev_ts = F.lag("_ts").over(w)
        # Start of the stationary run: the last sample before this one at which
        # the aircraft was *not* stationary at an aerodrome. NULL when the group
        # has never moved, in which case the group's own first sample anchors it.
        run_start = F.coalesce(
            F.last(
                F.when(~stationary, F.col("_ts")), ignorenulls=True
            ).over(w.rowsBetween(Window.unboundedPreceding, -1)),
            F.first(F.col("_ts")).over(w.rowsBetween(Window.unboundedPreceding, 0)),
        )
        parked_run_min = (
            F.unix_timestamp(prev_ts) - F.unix_timestamp(run_start)
        ) / 60.0

        # Gap arm: fires once, at the sample that ends the silence.
        gap_turnaround = prev_stationary & (gap_minutes() >= p.ground_dwell_minutes)
        # Parked-run arm: fires once, at the first sample that moves again.
        parked_turnaround = (
            ~stationary
            & prev_stationary
            & (parked_run_min >= p.ground_dwell_minutes)
        )
        return gap_turnaround | parked_turnaround | legacy().break_expr(p)

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

    The legacy gap rule is ORed in as a floor, as for A5 and A6 -- see the module
    docstring for why.

    An earlier draft also carried a ``was_below`` term: a running max over
    ``rowsBetween(unboundedPreceding, -1)`` asserting some earlier sample had
    been at or below the floor. It was unreachable dead weight, and the most
    expensive expression in the arm. ``climbing_away`` already requires the *lag*
    row to be at or below the floor, and that lag row is inside ``was_below``'s
    own window, so ``climbing_away`` implies ``was_below == 1`` for every row.
    Removed rather than replaced: the condition the docstring implies -- "a
    descent reached the floor" -- is exactly what the lag row being at or below
    the floor already says.
    """

    def expr(p):
        w = segment_window()
        climbing_away = (altitude_ft() > p.descent_floor_ft) & (
            F.lag(altitude_ft()).over(w) <= p.descent_floor_ft
        )
        return climbing_away | legacy().break_expr(p)

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
