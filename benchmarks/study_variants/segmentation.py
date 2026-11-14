"""Track segmentation rules that the package no longer ships.

The package builds ``track_id`` with two rules only: the default rule
(``standard``, also called ``cruise_linked``) and ``legacy``, which reproduces
the v0.0.2 release. The rules below were compared with them in the track
construction studies and stay here, unchanged, so the study scripts can still
run them.

A study script makes them available to the pipeline with one call::

    from study_variants.segmentation import register_study_arms

    register_study_arms()
    config.segmentation.method = "debounced"   # now accepted

The rules are:

* ``no_month_suffix``: ``legacy`` without the ``_{year}_{month}`` id suffix.
* ``traffic_style``: one 10 minute gap rule, ignored when both sides are at
  altitude (the rule of the ``traffic`` library).
* ``airframe_only``: ``legacy`` grouped on the airframe address alone.
* ``ground_anchored``: also breaks after a long enough run of on-ground samples.
* ``airport_anchored``: breaks only at an aerodrome, by height above the field.
* ``vertical_profile``: also breaks when a descent to near the field is followed
  by a climb.
* ``recommended``: grouped on the airframe, breaks when the callsign changes.
* ``debounced``: ``recommended``, but a new callsign must last 30 s first.

``ground_anchored``, ``airport_anchored`` and ``vertical_profile`` read four
thresholds the package does not have. They come from
:class:`StudySegmentationParams`, with the values the studies used.
"""

from dataclasses import dataclass, fields

from pyspark.sql import Window
from pyspark.sql import functions as F

from opdi.pipeline.segmentation import methods
from opdi.pipeline.segmentation.base import (
    BreakRule,
    SegmentationParams,
    altitude_ft,
    gap_minutes,
    lookback_minutes,
    segment_window,
    speed_kt,
)
from opdi.pipeline.segmentation.methods import (
    debounced_callsign_change,
    legacy,
    legacy_general_gap,
)

__all__ = [
    "StudySegmentationParams",
    "study_params",
    "TRAFFIC_DEFAULT_GAP_MINUTES",
    "no_month_suffix",
    "traffic_style",
    "airframe_only",
    "ground_anchored",
    "airport_anchored",
    "vertical_profile",
    "recommended",
    "debounced",
    "STUDY_ARMS",
    "register_study_arms",
]

#: The default gap of ``traffic``'s ``Flight.split``, in minutes.
TRAFFIC_DEFAULT_GAP_MINUTES = 10.0


@dataclass(frozen=True)
class StudySegmentationParams(SegmentationParams):
    """The package's segmentation thresholds plus four used only by study rules."""

    #: On-ground dwell above which a ground contact is a turnaround (minutes).
    ground_dwell_minutes: float = 5.0
    #: Height above field elevation below which a turnaround can occur (feet).
    turnaround_max_height_ft: float = 1000.0
    #: Ground speed below which an aircraft counts as stationary (knots).
    turnaround_max_speed_kt: float = 40.0
    #: Height above the field a descent must reach before a following climb is
    #: a new flight (feet).
    descent_floor_ft: float = 1500.0


def study_params(p: SegmentationParams) -> StudySegmentationParams:
    """``p`` with the study thresholds added, at their study values if ``p`` has none.

    The pipeline builds plain :class:`SegmentationParams` from the configuration,
    so a study rule run through the pipeline gets the study values here.
    """
    if isinstance(p, StudySegmentationParams):
        return p
    return StudySegmentationParams(**{f.name: getattr(p, f.name) for f in fields(p)})


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
        p = study_params(p)
        w = segment_window()
        # A ground run's length: time from the first to the last of the
        # consecutive on-ground samples immediately preceding this one.
        ground_start = F.last(F.when(~F.col("on_ground"), F.col("_ts")), ignorenulls=True).over(
            w.rowsBetween(Window.unboundedPreceding, -1)
        )
        prev_ts = F.lag("_ts").over(w)
        prev_ground = F.lag("on_ground").over(w)
        ground_run_min = (F.unix_timestamp(prev_ts) - F.unix_timestamp(ground_start)) / 60.0
        turnaround = ~F.col("on_ground") & prev_ground & (ground_run_min >= p.ground_dwell_minutes)
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

    **Two things this fixes that no altitude threshold can**, and the arm is
    measured on both:

    1. *A split legacy misses.* An aircraft parked at a 6,000 ft aerodrome never
       satisfies ``baro_altitude < 1524 m``, so legacy misses the turnaround and
       merges two flights -- the case ``track_quality.py`` names. Height above
       field, not barometric altitude, is the test.
    2. *A split legacy invents.* A long, slow, low dwell in the middle of nowhere
       satisfies legacy's low-altitude rule and is split, when it is not a
       turnaround at all. A6 does not split it, because it is not at an
       aerodrome.

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

    **Its floor is deliberately only half of legacy.** A6 ORs in
    :func:`legacy_general_gap` -- any gap over ``gap_minutes`` -- and pointedly
    **not** :func:`legacy_low_altitude_gap`. Every other domain arm takes both.

    The reason is that A6's thesis *is* that legacy's ``altitude < 5,000 ft`` test
    is a crude proxy for "at an aerodrome" and that real airport geometry is
    better. An arm cannot measure the replacement of a rule while also inheriting
    that rule as its own floor: property 2 above would be unreachable by
    construction, because A6 would then only ever *add* splits to legacy's. The
    general gap rule carries no such claim -- "a long enough silence is a new
    track" is not what this arm disputes -- so keeping it costs property 2
    nothing while still guaranteeing that an airframe whose ``near_airport`` is
    NULL for a whole month cannot end up in a single track.
    """

    def expr(p):
        p = study_params(p)
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
            F.last(F.when(~stationary, F.col("_ts")), ignorenulls=True).over(
                w.rowsBetween(Window.unboundedPreceding, -1)
            ),
            F.first(F.col("_ts")).over(w.rowsBetween(Window.unboundedPreceding, 0)),
        )
        parked_run_min = (F.unix_timestamp(prev_ts) - F.unix_timestamp(run_start)) / 60.0

        # Gap arm: fires once, at the sample that ends the silence.
        gap_turnaround = prev_stationary & (gap_minutes() >= p.ground_dwell_minutes)
        # Parked-run arm: fires once, at the first sample that moves again.
        parked_turnaround = (
            ~stationary & prev_stationary & (parked_run_min >= p.ground_dwell_minutes)
        )
        # Floor: the general gap rule only. NOT legacy_low_altitude_gap -- that
        # is the rule this arm exists to replace, and an arm cannot measure a
        # replacement while flooring itself on the thing replaced. See the
        # module docstring and this function's own.
        return gap_turnaround | parked_turnaround | legacy_general_gap(p)

    return BreakRule(
        name="airport_anchored",
        group_cols=["icao24"],
        break_expr=expr,
        month_suffix=False,
    )


def vertical_profile() -> BreakRule:
    """A7 -- a flight is one climb-cruise-descent cycle.

    A descent reaching ``descent_floor_ft`` **above the field**, followed by a
    climb back through it, is a new sortie -- whether or not there was a gap and
    whether or not ground contact was broadcast. This catches merges that no gap
    rule can reach, and it is the only arm that works when the transponder never
    reports ``on_ground``.

    Both halves of that sentence are conditions, and the arm is worthless
    without either:

    * *the descent* -- some earlier sample crossed the floor **downwards**
      (previous sample above it, this one at or below). Without this the rule
      fires on any upward crossing of the floor, including the very first
      departure of a cold track, which is not a new sortie at all and is the
      opposite of what the arm claims to detect. A track split there also
      carries the *next* leg's ``ATOT -> floor`` samples, so it scores as a
      merger for a reason that has nothing to do with the arm's thesis.
    * *the climb back through it* -- this row is above the floor and the
      previous one was not. This is where the break is placed, so the departing
      leg begins at the crossing.

    The downward-crossing flag is a running max over
    ``rowsBetween(unboundedPreceding, -1)``, so it is monotone within a group:
    once the aircraft has come down through the floor, every later climb through
    it splits. That is the intended reading -- any upward crossing after a
    descent *is* preceded by a descent -- and it costs one more window
    aggregation than the incorrect version did.

    **Height above field, not barometric MSL.** ``field_elev_ft`` is joined onto
    the frame by Task 6's runner, exactly as for :func:`airport_anchored`, and
    is coalesced to 0.0 where the join found nothing (sea level -- the same
    conservative direction A6 documents). Comparing MSL made the arm degenerate
    to bare legacy at any aerodrome above 1,500 ft elevation: a parked aircraft
    there already reads above the floor, the lag row is never at or below it,
    and no crossing is ever seen. That is precisely the blindness A6's docstring
    argues legacy suffers from, and there is no reason for A7 to inherit it.

    Per this repo's unit rule the comparison is scaled, never the stored column:
    ``altitude_ft()`` is the engine's aviation-unit accessor over the SI
    ``baro_altitude``, and ``descent_floor_ft`` is aviation with the unit in its
    name. See ``base.py``'s module docstring.

    The floor test is what keeps a cruise step-descent (FL350 -> FL310) from
    counting: that never approaches the floor.

    The legacy gap rule is ORed in as a floor, as for A5 and A6 -- see the module
    docstring for why.
    """

    def expr(p):
        p = study_params(p)
        w = segment_window()
        height_ft = altitude_ft() - F.coalesce(F.col("field_elev_ft"), F.lit(0.0))
        prev_height_ft = F.lag(height_ft).over(w)
        # The descent half: a downward crossing of the floor, anywhere earlier
        # in the group. NULL-safe -- the group's first row has no lag, so the
        # when/otherwise makes it a 0 rather than propagating a NULL through
        # the running max.
        descended_to_floor = (height_ft <= p.descent_floor_ft) & (
            prev_height_ft > p.descent_floor_ft
        )
        reached_floor_before = (
            F.max(F.when(descended_to_floor, 1).otherwise(0)).over(
                w.rowsBetween(Window.unboundedPreceding, -1)
            )
            == 1
        )
        # The climb half: back up through the floor. This is where the break
        # goes, so the departing leg starts at the crossing.
        climbing_away = (height_ft > p.descent_floor_ft) & (prev_height_ft <= p.descent_floor_ft)
        return (climbing_away & reached_floor_before) | legacy().break_expr(p)

    return BreakRule(
        name="vertical_profile",
        group_cols=["icao24"],
        break_expr=expr,
        month_suffix=False,
    )


def recommended() -> BreakRule:
    """A8 -- the combination the study recommends.

    Group on the airframe; split when the callsign really changes. Derived from
    the measured ladder, not chosen in advance. The evidence, both periods
    agreeing throughout:

    * **Dropping callsign from the group key is the single largest gain in the
      study.** ``airframe_only`` takes clean matching from 49.64% to 88.55%
      (2025) and 52.33% to 89.79% (2024), cutting fragmentation from ~50% to
      ~7%. Nothing else is close: the whole A1 parameter sweep -- 262 cells,
      every axis extended until it turned over -- spans 0.95 points and beats
      production by 0.21.
    * **But it is not free.** 4,314 of ``airframe_only``'s 131,498 tracks (3.3%)
      contain more than one *real* callsign: two flights of one airframe joined
      into a single track.
    * **Legacy fragments because of blanks, not because of callsign changes.**
      With callsign in the group key, samples whose callsign is null form tracks
      of their own -- 42.4% of legacy's tracks are blank-labelled. That is the
      fragmentation, and it is an artefact of the key rather than of the flight.

    The two failures have different causes, so they can be fixed independently:
    group on ``icao24`` alone, so a blank or flickering callsign no longer starts
    a track, and break when the last *non-blank* callsign differs from the
    current one, so two genuinely different flights still separate.

    ``F.last(..., ignorenulls=True)`` carries the last real callsign across the
    blanks, which is precisely the property the group key used to supply and that
    ``airframe_only`` gave up. Comparing raw ``callsign`` would break on every
    blank sample and reproduce legacy's fragmentation.

    **Shipping this changes ``track_id`` for all data from that release forward**,
    in shape as well as value -- there is no ``_{year}_{month}`` suffix. The study
    recommends; it does not schedule.
    """

    def expr(p):
        w = segment_window()
        back = w.rowsBetween(Window.unboundedPreceding, -1)
        # Trimmed for comparison: ADS-B pads callsigns to eight characters, so
        # "ABC123  " and "ABC123" are the same callsign and must not read as a
        # change.
        real = F.when(
            F.trim(F.coalesce(F.col("callsign"), F.lit(""))) != "",
            F.trim(F.col("callsign")),
        )
        prev_real = F.last(real, ignorenulls=True).over(back)
        prev_real_ts = F.last(F.when(real.isNotNull(), F.col("_ts")), ignorenulls=True).over(back)

        # **The lookback must be bounded, and that is the whole subtlety here.**
        # `break_expr` is evaluated over the group window, which spans every
        # sample of the airframe -- it has no notion of the track boundaries it
        # is in the middle of deciding. An unbounded `F.last` therefore reaches
        # back past a gap break into the *previous* flight, so a track whose
        # callsign starts blank and resolves later gets compared against the
        # callsign before the gap and splits in its own middle. Measured, that
        # bug cost 31 points of fragmentation (37.75% against airframe_only's
        # 6.92%) while only 3.2% of tracks contain a real-to-real transition at
        # all -- the mismatch between those two numbers is what exposed it.
        #
        # Requiring the previous real callsign to be no older than the general
        # gap threshold keeps the comparison inside one track: a longer silence
        # is itself a break, so there is nothing to carry across.
        recent = (
            F.unix_timestamp(F.col("_ts")) - F.unix_timestamp(prev_real_ts)
        ) / 60.0 < lookback_minutes(p)

        callsign_change = real.isNotNull() & prev_real.isNotNull() & recent & (real != prev_real)
        return F.coalesce(callsign_change, F.lit(False)) | legacy().break_expr(p)

    return BreakRule(
        name="recommended",
        group_cols=["icao24"],
        break_expr=expr,
        month_suffix=False,
        # Dropping the month suffix left this arm with no disambiguator across
        # batches, and production batches (month today, day under the day-by-day
        # runner). Keying on the track's own start restores uniqueness without
        # reintroducing a suffix that encodes how the data happened to be
        # sliced.
        id_from_start_time=True,
    )


def debounced() -> BreakRule:
    """A9 -- A8, but a callsign must persist before it breaks a track.

    `recommended` breaks on the first sample whose real callsign differs from
    the previous real one. A value that appears once and reverts therefore cuts
    a flight in half. Measured on 2026-06-01: of 1,765 track splits with a
    sub-minute gap -- which neither gap rule can produce, so all of them are
    callsign breaks -- **1,457 had the same resolved callsign on both halves**,
    at cruise, with a median implied speed of 421 kt across the gap. One flight,
    cut in two.

    The extra condition is that the new value is still in force
    ``callsign_min_persistence_seconds`` later. A flicker fails it because the
    value has already reverted; a genuine change passes because the new
    callsign is still there.

    ``callsign_min_persistence_seconds = 0`` reproduces `recommended` exactly,
    which is what makes this safe to add beside a published arm.
    """

    def expr(p):
        return debounced_callsign_change(p) | legacy().break_expr(p)

    return BreakRule(
        name="debounced",
        group_cols=["icao24"],
        break_expr=expr,
        month_suffix=False,
        id_from_start_time=True,
    )


#: Every rule of this module, by the name a configuration uses.
STUDY_ARMS = {
    "no_month_suffix": no_month_suffix,
    "traffic_style": traffic_style,
    "airframe_only": airframe_only,
    "ground_anchored": ground_anchored,
    "airport_anchored": airport_anchored,
    "vertical_profile": vertical_profile,
    "recommended": recommended,
    "debounced": debounced,
}


def register_study_arms() -> None:
    """Make the rules of this module selectable through ``config.segmentation.method``.

    Adds them to the package's rule table (``methods.ARMS``). Calling it twice
    does nothing more.
    """
    for name, rule in STUDY_ARMS.items():
        methods.register_arm(name, rule)
