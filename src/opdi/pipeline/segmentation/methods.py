"""One function per arm of the track-construction study.

Each returns a :class:`~opdi.pipeline.segmentation.base.BreakRule`. No Spark
session logic lives here -- an arm is a grouping, a predicate and a flag.

**Every domain arm (A5-A7) ORs a legacy gap rule in as a floor.** Two reasons,
and they are the standing rule for any arm added later, with the one exception
recorded at the end of this docstring:

1. *The ladder measures one change at a time.* Each domain arm is "legacy plus
   one idea", so its delta from A0 is attributable to that idea alone. An arm
   that also silently *removed* legacy's splits would report a delta that mixes
   two changes, and the study could not say which one moved the number.
2. *An arm whose inputs are missing must not degrade to one track a month.* A6
   depends on ``near_airport`` and ``field_elev_ft``, which Task 6 supplies by a
   left join -- so every sample away from a covered aerodrome gets NULL. With no
   floor, an airframe that never gets a reception gap at a covered aerodrome
   would have its entire month assigned to a single track, and A6 would score as
   a catastrophic merger for reasons having nothing to do with its idea.

**Which floor: the part of legacy the arm is not replacing.** Legacy is two
rules, and this module names them separately -- :func:`legacy_general_gap` (any
gap over ``gap_minutes``, at any altitude) and :func:`legacy_low_altitude_gap` (a
shorter gap, below ``low_alt_ft``). A5 and A7 take **both**, because neither
replaces anything: A5 adds the continuous-turnaround split, A7 adds the
descent-climb split, and a full floor costs a purely additive arm nothing.

A6 takes **only the general gap rule**, and that is the one exception. A6's whole
thesis is that legacy's ``altitude < 5,000 ft`` test is a *crude proxy* for "at an
aerodrome" and that real airport geometry is better -- so inheriting the
low-altitude rule would make the arm floor itself on the very rule it exists to
replace, and reason 1 above would forbid measuring the replacement at all. Reason
2 is still satisfied without it: an airframe with NULL inputs still splits on any
gap over 30 minutes, so no month-long track can form. See
:func:`airport_anchored`, which among A5-A7 is the only arm claiming to *remove* a
legacy split and therefore the only one there where a full floor conflicts.

**A10 is a conditional exception.** :func:`cruise_linked` builds on A9 rather than
on legacy, and keeps :func:`legacy_low_altitude_gap` whole, but it makes
:func:`legacy_general_gap` conditional: a long gap does not split when the two
edges can be one flight. That is the arm's whole idea, so a full floor would make
it unmeasurable, as it would for A6. Reason 1 survives, because the arm is A9 plus
this one change. Reason 2 survives for two reasons of its own: a NULL in any input
:func:`linkable` requires fails closed, so an airframe with missing inputs splits
on every long gap, as it would under the floor; and a gap over
``link_max_gap_minutes`` always breaks, whatever the edges look like.
"""

from functools import reduce
from operator import and_
from typing import Callable, Dict, Mapping

from pyspark.sql import Column, Window
from pyspark.sql import functions as F
from pyspark.sql.window import WindowSpec

from opdi.pipeline.flights import haversine_nm
from opdi.pipeline.segmentation.base import (
    FPM_PER_MPS,
    BreakRule,
    altitude_ft,
    gap_minutes,
    lookback_minutes,
    segment_window,
    speed_kt,
)

__all__ = [
    "legacy",
    "legacy_general_gap",
    "legacy_low_altitude_gap",
    "no_month_suffix",
    "traffic_style",
    "airframe_only",
    "ground_anchored",
    "airport_anchored",
    "vertical_profile",
    "recommended",
    "debounced",
    "stable_callsign",
    "debounced_callsign_change",
    "link_feature_columns",
    "link_checks",
    "linkable",
    "cruise_linked",
    "LINK_EDGE_SAMPLES",
    "PLACEHOLDER_ICAO24",
    "TRAFFIC_DEFAULT_GAP_MINUTES",
    "ARMS",
]

#: ``traffic``'s documented default for :meth:`Flight.split`. Defined here, above
#: its only use, because it is the one number an A3 sweep would want to vary.
TRAFFIC_DEFAULT_GAP_MINUTES = 10.0

#: Samples on each side of a hole that :func:`link_feature_columns` summarises.
LINK_EDGE_SAMPLES = 5

#: ``icao24`` values that are not an airframe, so two edges carrying one say
#: nothing about being one flight.
PLACEHOLDER_ICAO24 = ("000000", "000001", "ffffff")


def legacy_general_gap(p):
    """Legacy's altitude-independent half: any gap over ``gap_minutes`` splits.

    Named separately from :func:`legacy_low_altitude_gap` because the two halves
    are not interchangeable as an arm's floor -- see the module docstring. This
    half asserts only "a long enough silence is a new track", which no arm in the
    study disputes, so it is safe for every arm to inherit.
    """
    return gap_minutes() > p.gap_minutes


def legacy_low_altitude_gap(p):
    """Legacy's other half: a *shorter* gap counts, when below ``low_alt_ft``.

    This is production's proxy for "the aircraft is on the ground somewhere" --
    it uses barometric altitude because that is all it has. It is exactly the
    rule :func:`airport_anchored` exists to replace with real airport geometry,
    which is why A6 is the one arm that does not inherit it as a floor.
    """
    return (gap_minutes() > p.low_alt_gap_minutes) & (altitude_ft() < p.low_alt_ft)


def legacy() -> BreakRule:
    """A0 -- the production algorithm, as parameters of the engine.

    Split when the gap exceeds ``gap_minutes``, or exceeds
    ``low_alt_gap_minutes`` below ``low_alt_ft``. The month suffix is kept: it is
    what production does, and A2 exists to measure removing it.

    The two disjuncts are :func:`legacy_general_gap` and
    :func:`legacy_low_altitude_gap`; A0's behaviour is their OR and is unchanged
    by having been given names.
    """
    return BreakRule(
        name="legacy",
        group_cols=["icao24", "callsign"],
        break_expr=lambda p: legacy_general_gap(p) | legacy_low_altitude_gap(p),
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
        climbing_away = (height_ft > p.descent_floor_ft) & (
            prev_height_ft <= p.descent_floor_ft
        )
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
        prev_real_ts = F.last(
            F.when(real.isNotNull(), F.col("_ts")), ignorenulls=True
        ).over(back)

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

        callsign_change = (
            real.isNotNull()
            & prev_real.isNotNull()
            & recent
            & (real != prev_real)
        )
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


def _real_callsign() -> Column:
    """The sample's callsign, trimmed, or NULL if it is blank."""
    return F.when(
        F.trim(F.coalesce(F.col("callsign"), F.lit(""))) != "",
        F.trim(F.col("callsign")),
    )


def stable_callsign(p, window: WindowSpec | None = None) -> Column:
    """The debounced callsign: the last value that persisted, carried forward.

    NULL until a group's first real callsign. ``window`` defaults to the engine's
    :func:`~opdi.pipeline.segmentation.base.segment_window`; it exists so a caller
    scoring synthetic holes gets the same value per control, and its partition is
    kept while its ordering is replaced for the reversed frame below.
    """
    w = segment_window() if window is None else window
    back = w.rowsBetween(Window.unboundedPreceding, -1)
    real = _real_callsign()
    # The previous real callsign. Used here to find where a new run of a real
    # value *starts* -- it is deliberately not used as the comparison value
    # itself; see the block in `debounced_callsign_change`.
    prev_real = F.last(real, ignorenulls=True).over(back)

    hold = float(p.callsign_min_persistence_seconds)
    if hold <= 0.0:
        return real

    # A run is a maximal stretch of consecutive samples carrying the
    # same real callsign, blanks bridged over exactly as `recommended`
    # bridges them. `is_new_value` marks the one row that starts a
    # run: its real callsign differs from the last real one seen (or
    # there is no previous real value at all -- the track's very
    # first run, which is never checked, because "does it persist"
    # presupposes something it might be reverting *from*).
    is_new_value = real.isNotNull() & (
        prev_real.isNull() | (real != prev_real)
    )

    # How long this run lasts before the *next* run starts --
    # measured as a timestamp difference, not a row count, so an
    # irregular sampling rate cannot skew it. `next_transition_ts` is
    # the nearest future row where a new run begins, found by the
    # same reversed-growing-frame technique `cleaning/native.py`
    # documents (there for `_next_valid`): `F.first(...)` over
    # `rowsBetween(1, unboundedFollowing)` is a shrinking frame,
    # re-scanned per row and quadratic in track length; `F.last(...,
    # ignorenulls=True)` over the reversed ordering's `rowsBetween(
    # unboundedPreceding, -1)` asks the same question of a frame that
    # grows, which Spark maintains incrementally. This search is
    # genuinely unbounded -- unlike a fixed persistence horizon, the
    # next transition could be arbitrarily far away -- so this is
    # the one place in this arm where that transformation actually
    # earns its keep.
    next_transition_ts = F.last(
        F.when(is_new_value, F.col("_ts")), ignorenulls=True
    ).over(
        w.orderBy(F.col("_ts").desc()).rowsBetween(Window.unboundedPreceding, -1)
    )

    # Persisted if: this is the track's first run (nothing to revert
    # from, so nothing to check); or there is no next run at all (the
    # track ends inside this one, and a value with no later data has
    # not been shown to revert); or the run lasted at least `hold`
    # seconds before it was replaced. The boundary is inclusive --
    # exactly `hold` seconds counts.
    #
    # This is a run-level verdict, not a per-row one, and that
    # distinction is load-bearing. An earlier version asked whether
    # *this row's own* forward horizon still showed its own value --
    # which nulled every sample within `hold` seconds *before* a
    # genuine change, because a row deep inside an established run
    # can have a real, later transition inside its own horizon. With
    # every sample ahead of a genuine change nulled, the arm had no
    # earlier stable value left to compare the change against, and
    # reported no break at all. Asking instead "how long did *the
    # run this row belongs to* last" is answered once per run and
    # inherited by every row in it, so a run's own impending genuine
    # replacement cannot retroactively undermine it.
    persisted_run = (
        prev_real.isNull()
        | next_transition_ts.isNull()
        | (
            (
                F.unix_timestamp(next_transition_ts)
                - F.unix_timestamp(F.col("_ts"))
            )
            >= hold
        )
    )

    # The verdict is planted only at the row that starts a run --
    # NULL everywhere else -- then carried forward across every
    # continuation sample of that run by the same ignorenulls
    # forward-fill `recommended` uses to carry a real callsign
    # across blanks. A run whose start failed the persistence check
    # leaves no seed, so the forward-fill keeps supplying the
    # *previous* successful run's value straight through it --
    # "a flicker is invisible", applied per run instead of per row.
    run_seed = F.when(is_new_value & persisted_run, real)
    return F.last(run_seed, ignorenulls=True).over(
        w.rowsBetween(Window.unboundedPreceding, 0)
    )


def debounced_callsign_change(p) -> Column:
    """A9's callsign break: the stable callsign changed, recently. Never NULL."""
    w = segment_window()
    back = w.rowsBetween(Window.unboundedPreceding, -1)
    real = _real_callsign()
    # The previous real callsign's timestamp, for the lookback bound.
    prev_real_ts = F.last(
        F.when(real.isNotNull(), F.col("_ts")), ignorenulls=True
    ).over(back)
    recent = (
        F.unix_timestamp(F.col("_ts")) - F.unix_timestamp(prev_real_ts)
    ) / 60.0 < lookback_minutes(p)

    stable = stable_callsign(p)

    # **Persisted against persisted, not persisted against previous-real.**
    # A flicker trips the rule twice -- once on the X->Y excursion and again
    # on the Y->X revert -- and measured on 2026-06-01 both legs are there:
    # 5,241 revert boundaries and 5,000 excursion boundaries, 23.1% of all
    # 44,406 boundaries between them.
    #
    # Comparing the new value against the previous *real* one suppresses
    # only the excursion: at the first X after the flicker, the previous
    # real callsign is Y, the current is X, and X does persist -- so it
    # breaks. The flight still splits, in two instead of three, and the
    # fragmentation measurement would show a real but halved improvement,
    # plausible enough to be accepted.
    #
    # Carrying the last *stable* callsign forward instead leaves the
    # transient invisible: stable is X,X,X,X,NULL,X,X,X,X across a flicker,
    # so every comparison is X against X and nothing breaks. A genuine
    # change gives X,X,X,X,Y,Y,Y,Y and breaks once, at the first Y.
    prev_stable = F.last(stable, ignorenulls=True).over(back)
    callsign_change = (
        stable.isNotNull()
        & prev_stable.isNotNull()
        & recent
        & (stable != prev_stable)
    )
    return F.coalesce(callsign_change, F.lit(False))


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


def _once(value: Column, fn: Callable[[Column], Column]) -> Column:
    """``fn(value)``, with *value* evaluated once however often ``fn`` refers to it."""
    return F.element_at(F.transform(F.array(value), fn), 1)


def link_feature_columns(p, window: WindowSpec | None = None) -> Dict[str, Column]:
    """What :func:`linkable` sees of a hole, evaluated at the first sample after it.

    Every value is computed on every row, gap or no gap, and carries no verdict:
    the study applies many parameter sets to one cached frame, so nothing here may
    read a threshold the grid varies. The only parameters reached are the callsign
    lookahead and lookback, the edge position window and, through
    :func:`stable_callsign`, the debounce it shares with A9.

    ``gap_min`` is the silence between samples, which is what the 30 minute split
    sees. ``position_gap_min`` and ``gc_nm`` are the time and distance between the
    two edge positions, which can be further apart: see ``position`` below.

    ``window`` defaults to the engine's :func:`segment_window`. Whichever is used,
    the frame needs the arm contract's ``_ts`` and the ``_alt_ft`` and ``_spd_kt``
    behind the accessors. A frame near a group's edge just holds fewer rows, and
    the edge is decided from those.

    Raises:
        ValueError: ``link_edge_position_minutes`` is not below
            ``low_alt_gap_minutes``. The read through blank samples could then cross a
            gap the low-altitude rule splits at, and take a position from another flight.
    """
    if p.link_edge_position_minutes >= p.low_alt_gap_minutes:
        raise ValueError(
            f"link_edge_position_minutes={p.link_edge_position_minutes} must be below "
            f"low_alt_gap_minutes={p.low_alt_gap_minutes}: the read through blank samples "
            f"could cross a low-altitude split into another flight"
        )
    w = segment_window() if window is None else window
    before = w.rowsBetween(-LINK_EDGE_SAMPLES, -1)
    after = w.rowsBetween(0, LINK_EDGE_SAMPLES - 1)
    # Same partition, ordered by a number, because a range frame needs one.
    seconds = w.orderBy(F.unix_timestamp(F.col("_ts")))
    lookahead = seconds.rangeBetween(0, int(p.link_callsign_lookahead_minutes * 60))
    vert_rate_fpm = F.col("vert_rate") * FPM_PER_MPS

    # The callsign before the hole, and whether it is recent enough to count. The
    # bound is measured back from the old edge, and `process_day` reads at least
    # that far behind it, so two days' reads of one hole see the same evidence.
    # Unbounded, one read could see an old callsign and split where the next day's
    # read could not and link, and the piece after the hole would be published by
    # neither day.
    back = w.rowsBetween(Window.unboundedPreceding, -1)
    real = _real_callsign()
    old_edge_ts = F.lag(F.col("_ts")).over(w)
    last_seen_ts = F.last(F.when(real.isNotNull(), F.col("_ts")), ignorenulls=True).over(back)
    seen_recently = (
        F.unix_timestamp(old_edge_ts) - F.unix_timestamp(last_seen_ts)
    ) / 60.0 <= p.link_callsign_lookback_minutes

    # The position at each edge, read through blank samples. A position message
    # often arrives after altitude and speed, so the first sample after a hole has
    # none in about one cruise hole in seven, and that alone must not read as a stop.
    # The struct makes both coordinates and the time come from one sample: a row with
    # a lat and no lon is not a position, and coalescing each one apart could pair
    # coordinates from two samples seconds apart. The old edge is the row before this
    # one, which a range frame cannot start from, so it reuses the growing `back`
    # frame and bounds the age afterwards, as the callsign does. The new edge is a
    # bounded forward frame because a shrinking one would be quadratic.
    #
    # Spark evaluates every copy of a window expression, and the two features below
    # read these positions many times over. So the frames are read once into one
    # struct, and a lambda over a one-element array is what lets the rest refer to it.
    position = F.when(
        F.col("lat").isNotNull() & F.col("lon").isNotNull(),
        F.struct(
            F.col("_ts").alias("ts"), F.col("lat").alias("lat"), F.col("lon").alias("lon")
        ),
    )
    span_s = int(p.link_edge_position_minutes * 60)
    edges = F.struct(
        old_edge_ts.alias("old_edge_ts"),
        F.last(position, ignorenulls=True).over(back).alias("old"),
        F.first(position, ignorenulls=True).over(seconds.rangeBetween(0, span_s)).alias("new"),
    )

    def between_positions(e: Column) -> Column:
        last_position, new = e.getField("old"), e.getField("new")
        old = F.when(
            F.unix_timestamp(e.getField("old_edge_ts"))
            - F.unix_timestamp(last_position.getField("ts"))
            <= span_s,
            last_position,
        )
        return F.struct(
            (
                (F.unix_timestamp(new.getField("ts")) - F.unix_timestamp(old.getField("ts")))
                / 60.0
            ).alias("position_gap_min"),
            haversine_nm(
                old.getField("lat"), old.getField("lon"), new.getField("lat"), new.getField("lon")
            ).alias("gc_nm"),
        )

    positions = _once(edges, between_positions)

    return {
        # Computed here rather than read from `gap_minutes()`, which is the
        # engine's gap and so wrong under a caller's window.
        "gap_min": (
            F.unix_timestamp(F.col("_ts")) - F.unix_timestamp(F.lag(F.col("_ts")).over(w))
        )
        / 60.0,
        "position_gap_min": positions.getField("position_gap_min"),
        "gc_nm": positions.getField("gc_nm"),
        "alt_before_ft": F.min(altitude_ft()).over(before),
        "alt_after_ft": F.min(altitude_ft()).over(after),
        # What `legacy_low_altitude_gap` reads at this sample, which the minimum above
        # cannot stand in for: only with direction on can a link reach that low.
        "alt_new_edge_ft": altitude_ft(),
        "vr_before_fpm": F.avg(vert_rate_fpm).over(before),
        "vr_after_fpm": F.avg(vert_rate_fpm).over(after),
        "spd_before_kt": F.avg(speed_kt()).over(before),
        "cs_before": F.when(
            seen_recently,
            F.last(stable_callsign(p, w), ignorenulls=True).over(back),
        ),
        "cs_after": F.first(real, ignorenulls=True).over(lookahead),
        "icao24": F.col("icao24"),
    }


def link_checks(f: Mapping[str, Column], p) -> Dict[str, Column]:
    """One named verdict per test of the rule, in the spec's order. Never NULL.

    A pure function of the features and the thresholds, so the study can score a
    parameter set without re-segmenting, and can say which test stopped a hole.
    A wrongly glued flight is worse than a split one, so a NULL in a required input
    fails its test; the ``coalesce`` on every entry is what enforces that. So a hole
    with no position inside the window on either side splits.

    Speed and time budget divide the distance by ``position_gap_min``, the time
    between the samples the positions came from: using the sample gap would credit
    the aircraft with distance it flew while its position was blank. The length limit
    reads ``gap_min``, since that is the silence the 30 minute split sees and blank
    positions at an edge do not make a hole longer.

    Three tests are neutral on NULL rather than failing closed, each for a reason:
    vertical rate arrives in separate ADS-B messages, so its absence says nothing;
    a blank callsign agrees with anything; and the edge speed and time-budget
    tests pass outright at their ``0.0`` off setting, so a missing speed cannot
    fail a test that is switched off.

    With ``link_directional_edges``, flights that reappear on approach to their
    destination, or left coverage still climbing, can pass the two edge tests below
    the floor. The direction is the evidence there, so a NULL vertical rate does not
    pass, unlike in the descend-then-climb test. A new edge the low-altitude gap breaks
    at does not pass either: the rule splits there whatever this says, and the study,
    which reads this verdict instead of re-segmenting, must not count it as a link.
    """
    # Guarded because two samples can share a second, and a Spark 4 session raises
    # on a zero divisor. A plain division here would fail the whole job on them.
    implied_kt = F.when(
        f["position_gap_min"] > 0.0, f["gc_nm"] / (f["position_gap_min"] / 60.0)
    )

    descend_then_climb = (f["vr_before_fpm"] < -p.link_edge_vert_rate_fpm) & (
        f["vr_after_fpm"] > p.link_edge_vert_rate_fpm
    )
    callsign_differs = f["cs_before"] != f["cs_after"]

    old_edge_passes = f["alt_before_ft"] >= p.link_cruise_floor_ft
    new_edge_passes = f["alt_after_ft"] >= p.link_cruise_floor_ft
    if p.link_directional_edges:
        # Spelt "below", not left to the OR: a NULL altitude must stay a failure
        # whatever the vertical rate says.
        old_edge_passes = old_edge_passes | (
            (f["alt_before_ft"] < p.link_cruise_floor_ft)
            & (f["vr_before_fpm"] > p.link_edge_vert_rate_fpm)
        )
        low_altitude_gap_breaks = F.coalesce(f["alt_new_edge_ft"] < p.low_alt_ft, F.lit(False))
        new_edge_passes = new_edge_passes | (
            (f["alt_after_ft"] < p.link_cruise_floor_ft)
            & (f["vr_after_fpm"] < -p.link_edge_vert_rate_fpm)
            & ~low_altitude_gap_breaks
        )

    tests = {
        "gap_not_too_long": f["gap_min"] <= p.link_max_gap_minutes,
        "cruise_at_old_edge": old_edge_passes,
        "cruise_at_new_edge": new_edge_passes,
        "not_descend_then_climb": ~F.coalesce(descend_then_climb, F.lit(False)),
        "callsign_agrees": ~F.coalesce(callsign_differs, F.lit(False)),
        "implied_speed_not_too_slow": implied_kt >= p.link_min_implied_speed_kt,
        "implied_speed_not_impossible": implied_kt <= p.link_max_implied_speed_kt,
        "not_slow_for_this_flight": (
            implied_kt >= p.link_min_speed_ratio * f["spd_before_kt"]
            if p.link_min_speed_ratio > 0.0
            else F.lit(True)
        ),
        "no_time_for_a_stop": (
            f["position_gap_min"] - f["gc_nm"] / p.link_vfast_kt * 60.0
            < p.link_min_stop_minutes
            if p.link_min_stop_minutes > 0.0
            else F.lit(True)
        ),
        "real_address": ~F.lower(f["icao24"]).isin(*PLACEHOLDER_ICAO24),
    }
    return {name: F.coalesce(test, F.lit(False)) for name, test in tests.items()}


def linkable(f: Mapping[str, Column], p) -> Column:
    """Whether a hole's two edges can be one flight: every :func:`link_checks` holds."""
    return reduce(and_, link_checks(f, p).values())


def cruise_linked() -> BreakRule:
    """A10 -- A9, but a gap whose two edges can be one flight does not break.

    A flight crossing the Atlantic loses ADS-B reception for hours, and A9 turns
    it into two tracks, neither with both ends. Measured on 2026-06-05, among
    airframes with ADS-C: 1,204 of 1,334 flights crossing the European box have a
    gap over 30 min, median 191 min. Only the general gap rule changes. A short
    gap at low altitude still breaks, and a callsign change still breaks.
    ``link_directional_edges`` widens what counts as an edge: see :func:`link_checks`.

    Research only. Making this ``standard`` would be another silent ``track_id``
    boundary. ``process_day`` reads further back under this arm, or a piece after
    midnight would be claimed twice: see ``tracks.required_lookback_hours``.
    """

    def expr(p):
        # `linkable` refers to some features several times, so through `_once` each
        # window expression is evaluated once for the whole verdict.
        features = link_feature_columns(p)
        linked = _once(
            F.struct(*[column.alias(name) for name, column in features.items()]),
            lambda row: linkable({name: row.getField(name) for name in features}, p),
        )
        return F.coalesce(
            debounced_callsign_change(p)
            | legacy_low_altitude_gap(p)
            | (legacy_general_gap(p) & ~linked),
            F.lit(False),
        )

    return BreakRule(
        name="cruise_linked",
        group_cols=["icao24"],
        break_expr=expr,
        month_suffix=False,
        id_from_start_time=True,
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
    "debounced": debounced,
    "cruise_linked": cruise_linked,
}
