"""The track segmentation rules of the package.

Each rule is a function that returns a
:class:`~opdi.pipeline.segmentation.base.BreakRule`: how samples are grouped,
when a new track starts, and how the id is built. There are two:

* :func:`cruise_linked`, the default (``method="standard"``): grouped on the
  airframe, a new track at a lasting callsign change, at a short gap at low
  altitude, and at a long gap unless its two edges fit one continuous flight.
* :func:`legacy`, the rule of the v0.0.2 release: grouped on airframe and
  callsign, a new track at a gap over 30 min, or over 15 min below 5,000 ft.

:data:`ARMS` maps a configuration's ``segmentation.method`` to its rule. The
other rules of the track construction studies live with the study scripts
(``benchmarks/study_variants/segmentation.py``), which add them to
:data:`ARMS` with :func:`register_arm`.
"""

from functools import reduce
from operator import and_
from typing import Callable, Dict, Mapping, Sequence

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.window import WindowSpec

from opdi.config import STUDY_SEGMENTATION_METHODS
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
    "callsign_memory_minutes",
    "stable_callsign",
    "debounced_callsign_change",
    "link_feature_columns",
    "select_link_features",
    "link_checks",
    "linkable",
    "cruise_linked",
    "LINK_EDGE_SAMPLES",
    "LINK_DIRECTION_MIN_RATES",
    "PLACEHOLDER_ICAO24",
    "ARMS",
    "register_arm",
]

#: Values of each field on each side of a hole that :func:`link_feature_columns`
#: summarises, at most: the nearest that carry one, within ``link_edge_position_minutes``.
LINK_EDGE_SAMPLES = 5

#: Vertical rates an edge must hold before its direction counts as evidence: without
#: this one message would carry the verdict. Reading past blank samples does not change
#: that, since a rate can still be the only one in the window.
LINK_DIRECTION_MIN_RATES = 3

#: ``icao24`` values that are not an airframe, so two edges carrying one say
#: nothing about being one flight.
PLACEHOLDER_ICAO24 = ("000000", "000001", "ffffff")


def legacy_general_gap(p):
    """Legacy's altitude-independent half: any gap over ``gap_minutes`` splits.

    Named separately from :func:`legacy_low_altitude_gap` because the default
    rule keeps only the other half whole: it does not break at a long gap whose
    two edges fit one flight.
    """
    return gap_minutes() > p.gap_minutes


def legacy_low_altitude_gap(p):
    """Legacy's other half: a *shorter* gap counts, when below ``low_alt_ft``.

    This is production's proxy for "the aircraft is on the ground somewhere" --
    it uses barometric altitude because that is all it has. It is exactly the
    rule one of the study rules replaces with real airport geometry.
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


def _real_callsign() -> Column:
    """The sample's callsign, trimmed, or NULL if it is blank."""
    return F.when(
        F.trim(F.coalesce(F.col("callsign"), F.lit(""))) != "",
        F.trim(F.col("callsign")),
    )


def callsign_memory_minutes(p) -> float:
    """How long A10 remembers a callsign it has not heard again.

    A9 remembers one without limit, and that is a read-window dependence: a day's
    run reads a bounded stretch before the day, so one day can still hold a
    callsign the next day's read begins after, and the two then judge the same
    flicker differently -- a piece written twice, or by no day. Under A9 that
    takes three hours of samples with no callsign; under A10 a linked hole hides
    the callsign for hours, so A10 forgets a callsign older than the longest hole
    it can link. Every day reads at least that far back plus the callsign
    lookback (``tracks.required_lookback_hours``), so both reads hold whatever
    counts. ``debounced`` by name keeps its published behaviour.
    """
    return float(p.link_max_gap_minutes)


def stable_callsign(p, memory_minutes: float | None = None) -> Column:
    """The debounced callsign: the last value that persisted, carried forward.

    NULL until a group's first real callsign. With ``memory_minutes``, a previous
    callsign heard longer ago than that counts as none, so the next one is a
    group's first run again: see :func:`callsign_memory_minutes`.
    """
    w = segment_window()
    back = w.rowsBetween(Window.unboundedPreceding, -1)
    real = _real_callsign()
    # The previous real callsign. Used here to find where a new run of a real
    # value *starts* -- it is deliberately not used as the comparison value
    # itself; see the block in `debounced_callsign_change`.
    prev_real = F.last(real, ignorenulls=True).over(back)
    if memory_minutes is not None:
        prev_real_ts = F.last(F.when(real.isNotNull(), F.col("_ts")), ignorenulls=True).over(back)
        remembered = (
            F.unix_timestamp(F.col("_ts")) - F.unix_timestamp(prev_real_ts)
        ) / 60.0 <= float(memory_minutes)
        prev_real = F.when(remembered, prev_real)

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
    is_new_value = real.isNotNull() & (prev_real.isNull() | (real != prev_real))

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
    next_transition_ts = F.last(F.when(is_new_value, F.col("_ts")), ignorenulls=True).over(
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
        | ((F.unix_timestamp(next_transition_ts) - F.unix_timestamp(F.col("_ts"))) >= hold)
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
    return F.last(run_seed, ignorenulls=True).over(w.rowsBetween(Window.unboundedPreceding, 0))


def debounced_callsign_change(p, memory_minutes: float | None = None) -> Column:
    """A9's callsign break: the stable callsign changed, recently. Never NULL.

    ``memory_minutes`` is passed on to :func:`stable_callsign`; A9 passes none.
    """
    w = segment_window()
    back = w.rowsBetween(Window.unboundedPreceding, -1)
    real = _real_callsign()
    # The previous real callsign's timestamp, for the lookback bound.
    prev_real_ts = F.last(F.when(real.isNotNull(), F.col("_ts")), ignorenulls=True).over(back)
    recent = (
        F.unix_timestamp(F.col("_ts")) - F.unix_timestamp(prev_real_ts)
    ) / 60.0 < lookback_minutes(p)

    stable = stable_callsign(p, memory_minutes)

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
        stable.isNotNull() & prev_stable.isNotNull() & recent & (stable != prev_stable)
    )
    return F.coalesce(callsign_change, F.lit(False))


def _once(value: Column, fn: Callable[[Column], Column]) -> Column:
    """``fn(value)``, with *value* evaluated once however often ``fn`` refers to it."""
    return F.element_at(F.transform(F.array(value), fn), 1)


def _mean(values: Column) -> Column:
    """The mean of an array with no NULL in it, NULL when empty, as ``avg`` would give."""
    return F.when(
        F.size(values) > 0,
        F.aggregate(values, F.lit(0.0), lambda total, value: total + value) / F.size(values),
    )


def _edge_summary(frame: Column, latest_first: bool) -> Column:
    """What one edge's samples say: each field's nearest ``LINK_EDGE_SAMPLES`` values.

    *frame* is one window read of an array of samples, which the frame has bounded in
    time. Taking every field from it keeps this to one read rather than one each.
    Altitude and vertical rate arrive in separate messages, so a sample without one
    says nothing and each field looks past those to its own nearest values.

    Sorted on the whole sample, the instant at full resolution first, so the nearest
    samples are the nearest in time and two days' reads keep the same five of them.
    ``latest_first`` is for the old edge, where the nearest sample is the latest.

    NaN is no value, as NULL is not. Spark orders NaN above every number, so it passes
    ``>=`` and ``>``, and one in a mean makes the mean NaN.

    ``alt_read_past_blank`` says no altitude in the edge's own second was there to read
    and one further off was, which :func:`link_checks` does not look at and the study
    counts. By the second because samples in one second have no order, so that a blank
    twin of a good sample does not count.
    """

    def has_value(value: Column) -> Column:
        return value.isNotNull() & ~F.isnan(value)

    def over_sorted(samples: Column) -> Column:
        def values(field: str) -> Column:
            read = F.transform(samples, lambda sample: sample.getField(field))
            present = F.filter(read, has_value)
            return F.slice(present, 1, LINK_EDGE_SAMPLES).alias(field)

        edge_second = F.element_at(samples, 1).getField("s")
        altitude_at_the_edge = F.exists(
            samples,
            lambda sample: (sample.getField("s") == edge_second)
            & has_value(sample.getField("alt_ft")),
        ).alias("altitude_at_the_edge")

        def over_values(v: Column) -> Column:
            return F.struct(
                F.array_min(v.getField("alt_ft")).alias("min_alt_ft"),
                _mean(v.getField("vr_fpm")).alias("mean_vr_fpm"),
                F.size(v.getField("vr_fpm")).cast("long").alias("vr_samples"),
                _mean(v.getField("spd_kt")).alias("mean_spd_kt"),
                ((F.size(v.getField("alt_ft")) > 0) & ~v.getField("altitude_at_the_edge")).alias(
                    "alt_read_past_blank"
                ),
            )

        return _once(
            F.struct(values("alt_ft"), values("vr_fpm"), values("spd_kt"), altitude_at_the_edge),
            over_values,
        )

    return _once(F.sort_array(frame, asc=not latest_first), over_sorted)


def link_feature_columns(p, window: WindowSpec | None = None) -> Dict[str, Column]:
    """What :func:`linkable` sees of a hole, evaluated at the first sample after it.

    Every value is computed on every row, gap or no gap, and carries no verdict:
    the study applies many parameter sets to one cached frame, so nothing here may
    read a threshold the grid varies. The only parameters reached are the callsign
    lookahead and lookback, the edge position window and, through
    :func:`stable_callsign`, the debounce it shares with A9.

    ``gap_min`` is the silence between samples, which is what the 30 minute split
    sees. ``position_gap_min`` and ``gc_nm`` are the time and distance between the
    two edge positions, which can be further apart: see ``position`` in
    :func:`_link_reads`.

    The altitude, vertical rate, speed and rate-count features read each field's
    nearest :data:`LINK_EDGE_SAMPLES` values within ``link_edge_position_minutes`` of
    the edge, past any blank samples. A row count alone has no bound in time, so at a
    sparse edge it reaches across an earlier break into another flight, and a row can
    fall inside one day's read and outside the next day's. The bound makes two days'
    reads of a hole agree: on the lookback side this is prevented, since
    ``process_day`` reads at least that far behind the old edge. Past the new edge it
    is detected, not prevented: see the truncation warning in ``process_day``.

    ``window`` defaults to the engine's :func:`segment_window`. Whichever is used,
    the frame needs the arm contract's ``_ts`` and the ``_alt_ft`` and ``_spd_kt``
    behind the accessors. A frame near a group's edge just holds fewer rows, and
    the edge is decided from those.

    Raises:
        ValueError: ``link_edge_position_minutes`` is not below
            ``low_alt_gap_minutes``. The read through blank samples could then cross a
            gap the low-altitude rule splits at, and take a position from another flight.
    """
    return _features_from(_link_reads(p, window))


def select_link_features(
    frame: DataFrame, p, window: WindowSpec | None = None, keep: Sequence = ()
) -> DataFrame:
    """*frame* reduced to *keep* and :func:`link_feature_columns`, with each read done once.

    The same values, at about a third of the cost on a local run: selected column by
    column, every feature taken from an edge's read is that read again. A study that keeps
    the features of a day wants this.

    Raises:
        ValueError: see :func:`link_feature_columns`.
    """
    reads = _link_reads(p, window)
    read = "_link_read"
    selected = frame.select(
        *keep, F.struct(*[column.alias(name) for name, column in reads.items()]).alias(read)
    )
    features = _features_from({name: F.col(read).getField(name) for name in reads})
    return selected.select(
        *[column for column in selected.columns if column != read],
        *[column.alias(name) for name, column in features.items()],
    )


def _link_reads(p, window: WindowSpec | None = None) -> Dict[str, Column]:
    """The window reads behind :func:`link_feature_columns`, each a separate column.

    Spark evaluates every copy of a window expression, so several features taken from
    one read cost several reads. Selecting a feature each is what a caller of
    :func:`link_feature_columns` wants; :func:`cruise_linked`, which runs on every
    sample of every day, and :func:`select_link_features` put these in one struct to read
    each just once.

    Raises:
        ValueError: see :func:`link_feature_columns`.
    """
    if p.link_edge_position_minutes >= p.low_alt_gap_minutes:
        raise ValueError(
            f"link_edge_position_minutes={p.link_edge_position_minutes} must be below "
            f"low_alt_gap_minutes={p.low_alt_gap_minutes}: the read through blank samples "
            f"could cross a low-altitude split into another flight"
        )
    w = segment_window() if window is None else window
    # Same partition, ordered by a number, because a range frame needs one.
    seconds = w.orderBy(F.unix_timestamp(F.col("_ts")))
    lookahead = seconds.rangeBetween(0, int(p.link_callsign_lookahead_minutes * 60))
    vert_rate_fpm = F.col("vert_rate") * FPM_PER_MPS
    span_s = int(p.link_edge_position_minutes * 60)

    # The callsign before the hole, and whether it is recent enough to count. The
    # bound is measured back from the old edge, and `process_day` reads at least
    # that far behind it, so two days' reads of one hole see the same evidence behind
    # it. Unbounded, one read could see an old callsign and split where the next day's
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
        F.col("lat").isNotNull()
        & F.col("lon").isNotNull()
        & ~F.isnan(F.col("lat"))
        & ~F.isnan(F.col("lon")),
        F.struct(F.col("_ts").alias("ts"), F.col("lat").alias("lat"), F.col("lon").alias("lon")),
    )
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
                (F.unix_timestamp(new.getField("ts")) - F.unix_timestamp(old.getField("ts"))) / 60.0
            ).alias("position_gap_min"),
            haversine_nm(
                old.getField("lat"), old.getField("lon"), new.getField("lat"), new.getField("lon")
            ).alias("gc_nm"),
        )

    positions = _once(edges, between_positions)

    # The instant at full resolution comes first, so the nearest samples are the
    # nearest in time, as for every `lag` in the arm. Ordered on `s` alone, two
    # samples inside one second -- ADS-C can report so -- would be ordered by the
    # altitude being judged.
    edge_sample = F.struct(
        F.unix_micros(F.col("_ts")).alias("us"),
        F.unix_timestamp(F.col("_ts")).alias("s"),
        altitude_ft().cast("double").alias("alt_ft"),
        vert_rate_fpm.cast("double").alias("vr_fpm"),
        speed_kt().cast("double").alias("spd_kt"),
    )
    # Each edge reads a time-bounded frame, once. The old edge is the row before this
    # one, which a range frame cannot start from, so every row summarises the span up to
    # itself and the next row reads that with `lag`. The summary is what is carried on,
    # rather than the array, because a frame holds about 60 samples at 5 s cadence and
    # up to 300 at 1 Hz, and every window operator above this one would sort them.
    before_edge = F.lag(
        _edge_summary(
            F.collect_list(edge_sample).over(seconds.rangeBetween(-span_s, 0)),
            latest_first=True,
        )
    ).over(w)
    after_edge = _edge_summary(
        F.collect_list(edge_sample).over(seconds.rangeBetween(0, span_s)),
        latest_first=False,
    )

    return {
        # Computed here rather than read from `gap_minutes()`, which is the
        # engine's gap and so wrong under a caller's window.
        "gap_min": (F.unix_timestamp(F.col("_ts")) - F.unix_timestamp(old_edge_ts)) / 60.0,
        "positions": positions,
        "before_edge": before_edge,
        "after_edge": after_edge,
        # The last real callsign, not the debounced one: a run that ends at a hole
        # always persists, since its next transition lies beyond the hole, so at a
        # hole the two are the same value. A garbled last sample before a hole is
        # therefore the callsign before it, and the hole splits.
        "cs_before": F.when(seen_recently, F.last(real, ignorenulls=True).over(back)),
        "cs_after": F.first(real, ignorenulls=True).over(lookahead),
        "icao24": F.col("icao24"),
    }


def _features_from(reads: Mapping[str, Column]) -> Dict[str, Column]:
    """The named features of :func:`link_feature_columns`, out of :func:`_link_reads`."""
    positions, before, after = reads["positions"], reads["before_edge"], reads["after_edge"]
    return {
        "gap_min": reads["gap_min"],
        "position_gap_min": positions.getField("position_gap_min"),
        "gc_nm": positions.getField("gc_nm"),
        "alt_before_ft": before.getField("min_alt_ft"),
        "alt_after_ft": after.getField("min_alt_ft"),
        # Reported by the study. Whether the altitude above is older than the edge.
        "alt_before_read_past_blank": before.getField("alt_read_past_blank"),
        "alt_after_read_past_blank": after.getField("alt_read_past_blank"),
        "vr_before_fpm": before.getField("mean_vr_fpm"),
        "vr_after_fpm": after.getField("mean_vr_fpm"),
        # How many rates those two means rest on. Only the direction test reads these.
        "vr_before_samples": before.getField("vr_samples"),
        "vr_after_samples": after.getField("vr_samples"),
        "spd_before_kt": before.getField("mean_spd_kt"),
        "cs_before": reads["cs_before"],
        "cs_after": reads["cs_after"],
        "icao24": reads["icao24"],
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

    With ``link_old_edge_climb``, a flight that left coverage still climbing can pass
    the old edge's test below the floor, and with ``link_new_edge_descent`` one that
    reappears on approach to its destination can pass the new edge's. They are separate
    settings because the study decides them separately. The direction is the evidence
    there, so a NULL vertical rate does not pass, unlike in the descend-then-climb test,
    and the mean must rest on at least :data:`LINK_DIRECTION_MIN_RATES` rates in that
    edge's values. A new edge the low-altitude gap can break at does not pass either: the
    rule splits there whatever this says, and the study, which reads this verdict
    instead of re-segmenting, must not count it as a link. That reads the lowest of the
    edge's values, not the first sample's, so a blank first sample cannot slip past a low
    value right behind it: the rule then splits too, on this test.

    ``link_min_speed_ratio`` is uninformative at a climb edge: the speed before the gap
    is low there, which makes the ratio easy, and it says nothing of the speed in
    cruise. So where the old edge passed only by direction the ratio is not read, and
    ``not_slow_for_this_flight`` falls back to ``link_min_implied_speed_kt`` alone. That
    adds no evidence, since ``implied_speed_not_too_slow`` requires the same: such a
    hole has no per-flight speed test at all, and the study should read it so.
    """
    # Guarded because two samples can share a second, and a Spark 4 session raises
    # on a zero divisor. A plain division here would fail the whole job on them.
    implied_kt = F.when(f["position_gap_min"] > 0.0, f["gc_nm"] / (f["position_gap_min"] / 60.0))

    descend_then_climb = (f["vr_before_fpm"] < -p.link_edge_vert_rate_fpm) & (
        f["vr_after_fpm"] > p.link_edge_vert_rate_fpm
    )
    callsign_differs = f["cs_before"] != f["cs_after"]

    old_edge_passes = f["alt_before_ft"] >= p.link_cruise_floor_ft
    new_edge_passes = f["alt_after_ft"] >= p.link_cruise_floor_ft
    slow_for_this_flight_passes = (
        implied_kt >= p.link_min_speed_ratio * f["spd_before_kt"]
        if p.link_min_speed_ratio > 0.0
        else F.lit(True)
    )
    # Spelt "below", not left to the OR: a NULL altitude must stay a failure whatever
    # the vertical rate says.
    if p.link_old_edge_climb:
        old_edge_by_direction = (
            (f["alt_before_ft"] < p.link_cruise_floor_ft)
            & (f["vr_before_fpm"] > p.link_edge_vert_rate_fpm)
            & (f["vr_before_samples"] >= LINK_DIRECTION_MIN_RATES)
        )
        old_edge_passes = old_edge_passes | old_edge_by_direction
        if p.link_min_speed_ratio > 0.0:
            slow_for_this_flight_passes = F.when(
                F.coalesce(old_edge_by_direction, F.lit(False)),
                implied_kt >= p.link_min_implied_speed_kt,
            ).otherwise(slow_for_this_flight_passes)
    if p.link_new_edge_descent:
        new_edge_passes = new_edge_passes | (
            (f["alt_after_ft"] < p.link_cruise_floor_ft)
            & (f["alt_after_ft"] >= p.low_alt_ft)
            & (f["vr_after_fpm"] < -p.link_edge_vert_rate_fpm)
            & (f["vr_after_samples"] >= LINK_DIRECTION_MIN_RATES)
        )

    tests = {
        "gap_not_too_long": f["gap_min"] <= p.link_max_gap_minutes,
        "cruise_at_old_edge": old_edge_passes,
        "cruise_at_new_edge": new_edge_passes,
        "not_descend_then_climb": ~F.coalesce(descend_then_climb, F.lit(False)),
        "callsign_agrees": ~F.coalesce(callsign_differs, F.lit(False)),
        "implied_speed_not_too_slow": implied_kt >= p.link_min_implied_speed_kt,
        "implied_speed_not_impossible": implied_kt <= p.link_max_implied_speed_kt,
        "not_slow_for_this_flight": slow_for_this_flight_passes,
        "no_time_for_a_stop": (
            f["position_gap_min"] - f["gc_nm"] / p.link_vfast_kt * 60.0 < p.link_min_stop_minutes
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
    ``link_old_edge_climb`` and ``link_new_edge_descent`` widen what counts as an edge:
    see :func:`link_checks`.

    The default since the track-construction-v5 study: ``standard`` resolves to
    it. That is a ``track_id`` boundary like A8's and A9's before it, silent to a
    joiner. ``process_day`` reads further back under this arm, or a piece after
    midnight would be claimed twice: see ``tracks.required_lookback_hours``. The
    callsign guard it shares with A9 forgets a callsign older than the longest
    linkable hole, for the same reason: see :func:`callsign_memory_minutes`.
    """

    def expr(p):
        # `linkable` refers to some features several times, and several features come
        # from one read, so through `_once` each window expression is evaluated once for
        # the whole verdict.
        reads = _link_reads(p)
        linked = _once(
            F.struct(*[column.alias(name) for name, column in reads.items()]),
            lambda row: linkable(_features_from({name: row.getField(name) for name in reads}), p),
        )
        return F.coalesce(
            debounced_callsign_change(p, callsign_memory_minutes(p))
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


#: Names of the rules that moved to the study scripts, for the messages below.
STUDY_ARMS = STUDY_SEGMENTATION_METHODS

#: Where the study rules live, named in every error about one.
STUDY_HOME = (
    "benchmarks/study_variants/segmentation.py; a study script calls "
    "study_variants.segmentation.register_study_arms() to select one"
)


class _Rules(dict):
    """The rule table: a ``dict`` whose missing study rules say where they went."""

    def __missing__(self, name):
        if name in STUDY_ARMS:
            raise KeyError(f"segmentation rule {name!r} is a study rule; it lives in {STUDY_HOME}")
        raise KeyError(name)


#: The segmentation rules the pipeline can select, by name. ``standard`` is not
#: in it: it is the name of the default rule, ``cruise_linked``.
ARMS = _Rules(
    legacy=legacy,
    cruise_linked=cruise_linked,
)


def register_arm(name: str, rule: Callable[[], BreakRule]) -> None:
    """Add a rule to :data:`ARMS`, so a configuration can select it by name.

    Used by the study scripts to bring back the rules of the studies.
    """
    ARMS[name] = rule


def __getattr__(name):
    if name in STUDY_ARMS or name == "TRAFFIC_DEFAULT_GAP_MINUTES":
        raise ImportError(f"{name!r} is no longer part of opdi; it lives in {STUDY_HOME}")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
