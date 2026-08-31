"""Diagnostics about V1's own method, rather than about a segmentation arm.

Each exists because a reviewer asked a question the study could only answer with
an argument, and an argument is not a measurement.

**The containment census** (``--job containment``). V1 scores only ground-truth
flights whose whole airborne interval lies inside the sampled window. The
defence of that restriction is sound -- a flight observed for its last twenty
minutes has no samples for the rest of itself, so it scores as truncated however
well the segmentation performed, and including it would measure where the window
fell rather than what the algorithm did. But a sound defence of a restriction is
not a measurement of its price. This job counts what goes and how much of each
excluded flight was actually visible, so the paper can state the cost.

**The boundary-error histogram** (``--job boundary-hist``). V1 reports p10/p50/p90
of the signed offset between a track's ends and the flight's ATOT/ALDT.
Percentiles cannot distinguish a symmetric spread from a bimodal mixture, and
the two mean different things: a spread is noise to tune against, two modes are
two populations, one of which is probably a different failure wearing the same
number.

**The traffic-fill census** (``--job traffic-fill``). A3 runs ``traffic``'s
``Flight.split()`` rule against a raw OSN frame, but the rule was designed
against a filled one -- idiomatic ``traffic`` use bfills/ffills or interpolates
before splitting. ``gap_boundary_nulls`` counts how often the predicate's own
altitude comparison lands on a NULL, and separately counts "no-gap turnarounds"
-- a continuous broadcast through a stand -- which is the failure mode no amount
of filling could fix, because traffic's single gap-length threshold never sees
a gap at all.

    python benchmarks/track_diagnostics.py --job containment --period 2025 \\
        --results-dir ../opdi-portal/papers/track-construction-v1/data
    python benchmarks/track_diagnostics.py --job boundary-hist --period 2025 \\
        --results-dir ../opdi-portal/papers/track-construction-v1/data
    python benchmarks/track_diagnostics.py --job traffic-fill --period 2025 \\
        --results-dir ../opdi-portal/papers/track-construction-v1/data

The census reads only Network Manager/APDF reference parquet -- no state
vectors, no S3 writes. The histogram builds one assignment table per arm and
deletes it again by *calling* ``track_methods.run_arm`` -- its streaming, its
free-space gate and its scoped single-object delete, not a second copy of them
-- passing its own scoring callable and a ``diag_``-prefixed arm directory, so
that a concurrent ``track_methods`` run and this one can never delete each
other's tables. The traffic-fill census reads the same cleaned track table the
arms jobs read, filtered to the period's sampled days, and writes nothing else.

One Spark job at a time -- ``spark.driver.port`` is pinned, so a second
concurrent job kills both.
"""

import argparse
import csv
import datetime as dt
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

import osn_sample  # noqa: E402
import provenance  # noqa: E402
import track_methods  # noqa: E402
import track_truth  # noqa: E402
from osn_sample import build_spark, load_dotenv  # noqa: E402
from pyspark.sql import DataFrame, SparkSession, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from track_score import boundary_offsets  # noqa: E402

from opdi.config import OPDIConfig  # noqa: E402
from opdi.pipeline.segmentation import SegmentationParams  # noqa: E402
from opdi.pipeline.segmentation.base import FT_PER_M  # noqa: E402

__all__ = ["inside_window", "containment_census", "census_ground_truth",
           "null_rates", "gap_boundary_nulls",
           "boundary_histogram"]

#: Days added on each side of the sampled window when loading ground truth for
#: the census. One is enough for any flight in this data: the longest ECAC leg
#: is a few hours, so a flight airborne at either edge of the window departed at
#: most one calendar day before it.
CENSUS_PAD_DAYS = 1

#: Arms the histogram is drawn for by default. Not all eight: an assignment
#: table is a cluster job each, and these three are the arms whose boundary
#: numbers the paper actually discusses -- the shipped default, the algorithm it
#: replaces, and the arm with the tightest signed error of the ladder.
DEFAULT_HIST_ARMS = ["recommended", "legacy", "ground_anchored"]

#: Full width of the histogram, each side of zero. 1800 s rather than the 900 s
#: first proposed: ``land_err_p50_s`` is 374 s and that is a *median*, so half
#: the arrival distribution lies above it and a 900 s span would clamp a large
#: share of it into the end bin -- hiding exactly the shape the figure exists to
#: show.
SPAN_SECONDS = 1800
BIN_SECONDS = 30

#: Column order of each CSV, fixed here so the file the paper reads and the
#: interface the plan declares cannot drift apart.
CONTAINMENT_FIELDS = [
    "period", "n_gt_flights", "n_wholly_inside", "n_clipped_start",
    "n_clipped_end", "pct_kept", "median_observed_fraction_clipped",
]
HIST_FIELDS = ["arm", "edge", "bin_lower_s", "bin_upper_s", "n"]
TRAFFIC_FILL_FIELDS = [
    "period", "n_gaps", "n_null_either_side", "null_pct", "n_no_gap_turnarounds",
]

#: A3 splits on ``traffic``'s own fixed 10-minute gap threshold, not on OPDI's
#: configurable ``gap_minutes`` -- the two can differ, and this diagnostic
#: exists to explain *A3's* behaviour, so it must use A3's number rather than
#: whatever the pipeline happens to be tuned to today.
TRAFFIC_GAP_MINUTES = 10.0


def inside_window(window_start, window_end):
    """V1's containment predicate: the whole airborne interval inside the window.

    One definition, because it is applied in two places -- the census itself and
    the cross-check that proves the census reproduces
    ``load_flight_intervals``'s rule. A second copy would let the check pass
    while both copies were wrong together, which is the one failure the check
    exists to rule out.

    ``t_land < we``, not ``<=``: ``we`` is the *exclusive* midnight after the
    last sampled day and ``track_truth.load_flight_intervals`` keeps a flight on
    exactly ``t_off >= ws AND t_land < we``. A census that used ``<=`` would be
    measuring a rule V1 does not apply.

    Both bounds are cast to timestamp, so the function takes the strings
    ``track_truth._sample_window`` returns as readily as datetimes. That is not
    cosmetic. ``F.lit("2025-06-05 00:00:00").cast("long")`` does not mean "epoch
    second of that instant": it is a string-to-BIGINT cast, which under ANSI
    mode raises ``CAST_INVALID_INPUT`` and without it returns NULL -- so casting
    the raw literal per use either kills the job or blanks a quantile with
    nothing anywhere saying why. Going via timestamp makes the seconds cast mean
    what it reads as.
    """
    return (F.col("t_off") >= F.lit(window_start).cast("timestamp")) & (
        F.col("t_land") < F.lit(window_end).cast("timestamp")
    )


def containment_census(gt: DataFrame, window_start, window_end) -> dict:
    """What the wholly-inside-the-window restriction excludes, and how much.

    V1 scores only ground-truth flights whose entire airborne interval lies
    inside the sample window. The reason is not tidiness: a flight observed for
    its last twenty minutes has no samples for the rest of itself, so it scores
    as truncated however well the segmentation performed. Including it would
    measure where the window fell, not what the algorithm did.

    That argument justifies the restriction; it does not measure its price.
    This does -- how many flights go, and how much of each one was actually
    visible -- so the paper can state the cost rather than assert there is none.

    ``gt`` must be the *overlapping* ground truth, not
    ``load_flight_intervals``'s output: that function already applies the
    restriction being measured here, so passing it would report 100 % kept and
    answer nothing. :func:`census_ground_truth` builds the right frame.
    """
    # t_off / t_land, NOT t_start / t_end. `load_flight_intervals` returns the
    # ground-truth *flight* interval as (t_off, t_land, t_source, day);
    # t_start/t_end are the *track* extents from `track_score.track_extents`,
    # a different frame entirely. Mixing the two silently compares a track
    # against a window it was never measured against.
    ws = F.lit(window_start).cast("timestamp")
    we = F.lit(window_end).cast("timestamp")

    inside = inside_window(window_start, window_end)
    clipped_start = F.col("t_off") < ws
    clipped_end = F.col("t_land") >= we

    # Seconds of this flight that fall inside the window, over its whole
    # airborne duration. Reported only over the excluded flights: for a kept one
    # it is 1.0 by construction and would just dilute the median.
    observed = F.least(F.col("t_land").cast("long"), we.cast("long")) - F.greatest(
        F.col("t_off").cast("long"), ws.cast("long")
    )
    total = F.col("t_land").cast("long") - F.col("t_off").cast("long")

    agg = gt.select(
        F.count(F.lit(1)).alias("n"),
        F.sum(F.when(inside, 1).otherwise(0)).alias("n_in"),
        F.sum(F.when(clipped_start, 1).otherwise(0)).alias("n_cs"),
        F.sum(F.when(clipped_end, 1).otherwise(0)).alias("n_ce"),
    ).collect()[0]

    # `F.when(total > 0, total)` with no otherwise yields NULL for a
    # zero-duration flight, so the division is NULL rather than a divide error;
    # approxQuantile ignores NULLs. load_flight_intervals already filters
    # `t_land > t_off`, so this is belt and braces.
    frac = (
        gt.filter(~inside)
        .select((observed / F.when(total > 0, total)).alias("f"))
        .approxQuantile("f", [0.5], 0.01)
    )

    return {
        "n_gt_flights": agg["n"],
        "n_wholly_inside": agg["n_in"],
        "n_clipped_start": agg["n_cs"],
        "n_clipped_end": agg["n_ce"],
        "pct_kept": round(100.0 * agg["n_in"] / agg["n"], 2) if agg["n"] else 0.0,
        "median_observed_fraction_clipped": round(frac[0], 3) if frac else None,
    }


def census_ground_truth(
    spark: SparkSession,
    months: list,
    days: list,
    pad_days: int = CENSUS_PAD_DAYS,
    reference_base: str = track_truth.REFERENCE_BASE,
) -> tuple:
    """Every ground-truth flight with *any* airborne time inside the window.

    Returns ``(gt, window_start, window_end)``.

    This is the denominator the census needs, and it cannot come from a plain
    ``load_flight_intervals(spark, months, days)``: that call already applies
    the containment restriction this job exists to measure, so counting its
    output against the window reports 100 % kept and answers nothing.

    Two corrections, in order.

    **The day list is padded** by ``pad_days`` on each side. Ground truth is
    keyed on the *departure* day, so a flight that left the day before the
    window and lands inside it is not loaded at all without the pad, and
    ``n_clipped_start`` would come back structurally zero rather than measured
    -- a confident, wrong answer to the reviewer's question. The window
    ``load_flight_intervals`` derives internally widens with the pad and
    therefore stops excluding anything, which is the point: the *real* window is
    applied afterwards, by :func:`containment_census`.

    **The padded frame is then cut back to flights that overlap the real
    window** (``t_land > window_start AND t_off < window_end``). Padding alone
    leaves a denominator full of flights that never touched the sampled window
    at all, which would deflate ``pct_kept`` with flights V1 was never asked
    about. "Has some airborne time in the window" is the honest denominator for
    "what does requiring *all* of it cost".

    The window itself comes from ``track_truth._sample_window`` -- a private
    function, deliberately reused rather than reimplemented. Its own docstring
    argues that deriving the bounds from the caller's day list is what stops
    them drifting from the filter applied beside them; a second copy here would
    reintroduce exactly that drift, and the census would then measure a window
    the study does not use.
    """
    parsed = sorted(dt.date.fromisoformat(str(d)) for d in days)
    pad = [parsed[0] - dt.timedelta(days=k) for k in range(pad_days, 0, -1)]
    pad += [parsed[-1] + dt.timedelta(days=k) for k in range(1, pad_days + 1)]
    wide = sorted(set(parsed) | set(pad))

    # A padded day in a month whose reference parquet was not requested would
    # silently contribute no flights, which looks exactly like "nothing was
    # clipped". Fail instead: the caller must widen --months.
    unknown = sorted({d.strftime("%Y%m") for d in wide} - set(months))
    if unknown:
        raise SystemExit(
            f"census_ground_truth: padding {days} by {pad_days} day(s) reaches "
            f"month(s) {unknown}, which are not in months={months}. Pass them, "
            "or the census would report zero clipped flights at that edge "
            "because the data for them was never loaded."
        )

    window = track_truth._sample_window(days)
    if window is None:
        raise SystemExit("census_ground_truth: needs an explicit day list")
    ws, we = window

    gt = track_truth.load_flight_intervals(
        spark, months, [str(d) for d in wide], reference_base=reference_base
    )
    overlaps = (F.col("t_land") > F.lit(ws).cast("timestamp")) & (
        F.col("t_off") < F.lit(we).cast("timestamp")
    )
    return gt.filter(overlaps), ws, we


def boundary_histogram(
    matched: DataFrame,
    extents: DataFrame,
    bin_seconds: int = BIN_SECONDS,
    span_seconds: int = SPAN_SECONDS,
) -> list:
    """The signed boundary offsets as a distribution, not three percentiles.

    p10/p50/p90 cannot distinguish a symmetric spread from a bimodal one, and
    the two mean different things: a spread is noise to tune against, two modes
    are two populations, one of which is probably a different failure wearing
    the same number. ``boundary_error``'s own docstring makes exactly this
    argument about ``abs()``; this is the same argument one level further out.

    Sign convention is ``boundary_error``'s, unchanged: ``off = trk_start -
    t_off``, so **negative ``off`` means the track starts before take-off**, and
    ``land = trk_end - t_land``, so **positive ``land`` means it ends after
    landing**. Both of those are the normal case -- an OPDI track includes
    ground movement by design, while ground truth's interval is airborne only.
    A histogram that loses this convention inverts the reader's diagnosis.

    Restricted to ``t_source == "apdf"``, as ``boundary_error`` is. That
    restriction, the dominant-track pick and the subtraction itself are not
    repeated here: they come from ``track_score.boundary_offsets``, which
    ``boundary_error`` also calls, so the histogram and the percentiles printed
    beside it cannot describe different populations.

    Bins are clamped to +/- ``span_seconds`` so the tails do not stretch the
    axis into uselessness. Clamped counts stay in the end bins rather than
    being dropped, so the histogram sums to the sample and an end bin reads
    honestly as "this many, at least this far out".

    Returns one dict per (``edge``, bin): ``edge``, ``bin_lower_s``,
    ``bin_upper_s``, ``n``. Every bin in the span is present, including empty
    ones, so two arms' histograms lie on the same grid and a plot does not have
    to guess at the gaps.
    """
    if span_seconds % bin_seconds:
        raise ValueError(
            f"span_seconds={span_seconds} must be a whole number of "
            f"bin_seconds={bin_seconds} bins, or the end bin is a different "
            "width from the rest and the figure lies about its own shape."
        )
    half = span_seconds // bin_seconds

    off = boundary_offsets(matched, extents)
    try:
        # Long form: one row per (flight, edge). Both edges share a grid, so a
        # reader can put the departure and arrival panels side by side.
        both = off.select(
            F.lit("off").alias("edge"), F.col("off_s").alias("d")
        ).unionByName(off.select(F.lit("land").alias("edge"), F.col("land_s").alias("d")))
        clamped = F.greatest(F.least(F.col("d"), F.lit(span_seconds)), F.lit(-span_seconds))
        # floor() of the clamped value, then folded back by one at the top:
        # a value of exactly +span_seconds floors to `half`, one bin past the
        # last, which would be a zero-width bin of its own. Folding it into
        # [span-bin, span] is what makes the end bin mean "at least this far".
        idx = F.least(F.floor(clamped / F.lit(bin_seconds)).cast("int"), F.lit(half - 1))
        counted = (
            both.select("edge", idx.alias("b"))
            .groupBy("edge", "b")
            .agg(F.count(F.lit(1)).alias("n"))
        )
        counts = {(r["edge"], r["b"]): r["n"] for r in counted.collect()}
        n_rows = both.count()
    finally:
        # boundary_offsets caches; its docstring makes unpersisting the caller's
        # job, and it must happen on the failure path too or a driver holding
        # this study's other arms keeps the block.
        off.unpersist()

    rows = []
    for edge in ("off", "land"):
        for b in range(-half, half):
            rows.append({
                "edge": edge,
                "bin_lower_s": b * bin_seconds,
                "bin_upper_s": (b + 1) * bin_seconds,
                "n": counts.get((edge, b), 0),
            })

    # The docstring promises the histogram sums to the sample. A NULL offset
    # would break that silently -- it lands in no bin at all -- so it is
    # asserted rather than trusted. boundary_offsets already raises on a missing
    # extent, which is the only way one could arise.
    total = sum(r["n"] for r in rows)
    if total != n_rows:
        raise ValueError(
            f"boundary_histogram: bins hold {total} of {n_rows} offsets. "
            "Some offset fell in no bin, which means it was NULL."
        )
    return rows


def run_arm_histogram(spark, s3, arm_name, period, sv, gt, params, args):
    """One arm's boundary histogram, over ``track_methods``'s streamed runner.

    The orchestration -- write the assignment table, take extents from the
    *full* table, overlap-join it to ground truth, release the table on the
    failure path as well as the success path -- is
    :func:`track_methods.run_arm`, called rather than copied. Only two things
    differ from the ladder's own use of it: what is computed from
    ``(matched, extents)``, and the ``diag_`` path prefix.

    That prefix matters. ``track_methods`` writes the bare arm name, and a
    delete here that matched its prefix would be this study removing a table
    another process was mid-run on. One job at a time is the rule; a distinct
    prefix is what makes a broken rule survivable.

    The ``meta`` half of the runner's return -- object count and byte size --
    is dropped: this job's CSV is a bin grid, and the assignment table's size
    is a property of the arm that ``arms_<period>.csv`` already records.
    """
    rows, _meta = track_methods.run_arm(
        spark, s3, arm_name, period, sv, gt, params, args.keep_assignments,
        score=lambda matched, extents, assign: boundary_histogram(
            matched, extents, args.bin_seconds, args.span_seconds
        ),
        path_arm=f"diag_{arm_name}",
    )
    for r in rows:
        r["arm"] = arm_name
    return rows


#: Short names for the columns ``cleaning/native.py`` may mask to NULL. The
#: report reads better as ``null_baro_pct`` than ``null_baro_altitude_pct``,
#: and the mapping is stated once here rather than being guessed at each call.
NULL_SHORT_NAMES = {"baro_altitude": "baro", "geo_altitude": "geo"}


def null_rates(df: DataFrame, columns=None) -> dict:
    """NULL share per cleaned column, as ``null_<short>_pct`` percentages.

    Step 02a masks bad values to NULL and keeps the row -- the 2024-challenge
    design, chosen so that "no data" and "interpolated data" stay
    distinguishable -- so a cleaned table's null rate *is* the cleaning's
    output volume, and it belongs beside the scores rather than in a separate
    study. A segmentation change should barely move it: cleaning is
    track-local, so a different partition of the same samples shifts it a
    little, but an arm whose null rate jumps has changed what the cleaner sees,
    and that is a finding rather than a rounding difference.

    Delegates the counting to :func:`opdi.cleaning.native.null_rate_report` --
    one Spark action over the columns that module declares it may mask -- so
    that adding a maskable column there adds it here too, rather than leaving
    this list quietly behind. Rates are converted to percentages, because every
    other rate in this study's CSVs is a percentage and a lone fraction in the
    same row is a units bug waiting to be read as one.

    No sample count is returned: the caller has it from the extents export at
    no extra scan, and recomputing it here would cost a second pass over the
    table for a number that must then agree with one already in the row.
    """
    from opdi.cleaning.native import null_rate_report

    rates = null_rate_report(df) if columns is None else null_rate_report(df, columns)
    return {
        f"null_{NULL_SHORT_NAMES.get(c, c)}_pct": 100.0 * v
        for c, v in rates.items()
    }


def gap_boundary_nulls(sv: DataFrame, gap_minutes: float = TRAFFIC_GAP_MINUTES) -> dict:
    """How often A3's split predicate cannot see the altitude it needs, and the
    failure mode that has nothing to do with NULLs at all.

    ``traffic``'s ``Flight.split()`` cuts on a raw timestamp gap alone -- see
    this module's docstring -- but the rule that decides whether to *keep*
    A3's split or reclassify it (and every legacy-style low-altitude rule
    ported alongside it) reads ``baro_altitude_ft`` on both sides of the
    candidate gap. On a raw OSN frame that value is NULL about a fifth of the
    time, and a comparison against NULL is not a decision -- it is the absence
    of one. Counted here as **undecidable**: a candidate gap where the current
    row's or the previous row's altitude is NULL.

    Two counts, over one pass:

    * ``n_gaps`` / ``n_null_either_side`` / ``null_pct`` -- of every candidate
      gap (``icao24``-partitioned, time-ordered, more than ``gap_minutes``
      since the previous sample), how many are undecidable because an
      altitude either side is missing. This is the part a fill would help.
    * ``n_no_gap_turnarounds`` -- a maximal run of ``on_ground = true``
      samples, per ``icao24``, spanning more than ``gap_minutes`` start to
      end with **no** internal gap above the threshold. This is an aircraft
      broadcasting continuously through a stand: there is no gap for any
      threshold to catch, on a raw frame or a filled one, so it is the part a
      fill could never help. Legacy's second rule -- a shorter gap below
      5,000 ft -- catches this shape; ``traffic``'s single 10-minute threshold
      structurally cannot.

    ``gap_minutes`` defaults to :data:`TRAFFIC_GAP_MINUTES` -- ``traffic``'s
    own fixed threshold, not OPDI's configurable ``gap_minutes`` -- because
    this diagnostic explains *A3's* behaviour specifically.
    """
    w = Window.partitionBy("icao24").orderBy("event_time")
    prev_time = F.lag("event_time").over(w)
    prev_alt = F.lag("baro_altitude_ft").over(w)
    prev_on_ground = F.lag("on_ground").over(w)

    gap_min = (F.col("event_time").cast("long") - prev_time.cast("long")) / 60.0
    is_gap = gap_min > F.lit(gap_minutes)
    null_either_side = F.col("baro_altitude_ft").isNull() | prev_alt.isNull()

    annotated = sv.select(
        "icao24", "event_time", "on_ground",
        gap_min.alias("gap_min"),
        is_gap.alias("is_gap"),
        null_either_side.alias("null_either_side"),
        prev_on_ground.alias("prev_on_ground"),
    ).cache()

    try:
        counts = annotated.select(
            F.sum(F.when(F.col("is_gap"), 1).otherwise(0)).alias("n_gaps"),
            F.sum(
                F.when(F.col("is_gap") & F.col("null_either_side"), 1).otherwise(0)
            ).alias("n_null_either_side"),
        ).collect()[0]
        n_gaps = counts["n_gaps"] or 0
        n_null = counts["n_null_either_side"] or 0

        # A run of on_ground samples starts where the previous row (in the
        # full, un-filtered time order) was not on_ground -- or was the first
        # row of the icao24 partition. A cumulative sum of that indicator,
        # over every row regardless of on_ground, gives every row in the same
        # airborne-free stretch a shared id; rows outside any such stretch get
        # an id too, but they are dropped below and never grouped on.
        is_run_start = F.col("on_ground") & (
            F.col("prev_on_ground").isNull() | (~F.col("prev_on_ground"))
        )
        run_id = F.sum(F.when(is_run_start, 1).otherwise(0)).over(
            w.rowsBetween(Window.unboundedPreceding, 0)
        )
        runs = annotated.select(
            "icao24", "event_time", "on_ground", "gap_min",
            is_run_start.alias("is_run_start"),
            run_id.alias("run_id"),
        ).filter(F.col("on_ground"))

        run_stats = runs.groupBy("icao24", "run_id").agg(
            F.min("event_time").alias("run_start"),
            F.max("event_time").alias("run_end"),
            # Only gaps *within* the run count as internal: the run's first
            # row's `gap_min` is the gap leading *into* the stretch, from
            # whatever (on_ground or not) preceded it, not a gap inside it.
            F.max(
                F.when(~F.col("is_run_start"), F.col("gap_min"))
            ).alias("max_internal_gap_min"),
        )
        span_min = (
            F.col("run_end").cast("long") - F.col("run_start").cast("long")
        ) / 60.0
        no_gap_turnaround = (span_min > F.lit(gap_minutes)) & (
            F.col("max_internal_gap_min").isNull()
            | (F.col("max_internal_gap_min") <= F.lit(gap_minutes))
        )
        n_no_gap = run_stats.filter(no_gap_turnaround).count()
    finally:
        annotated.unpersist()

    return {
        "n_gaps": n_gaps,
        "n_null_either_side": n_null,
        "null_pct": round(100.0 * n_null / n_gaps, 2) if n_gaps else 0.0,
        "n_no_gap_turnarounds": n_no_gap,
    }


def _write_csv(path: Path, fieldnames: list, rows: list) -> None:
    with path.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r[k] for k in fieldnames})


def run_containment(spark, args, period_cfg, days, out_path) -> dict:
    gt, ws, we = census_ground_truth(spark, period_cfg["months"], days)
    gt = gt.cache()
    n_overlapping = gt.count()
    print(f"window [{ws}, {we}) -- {n_overlapping:,} ground-truth flights overlap it")
    row = containment_census(gt, ws, we)
    row["period"] = args.period
    inside = gt.filter(inside_window(ws, we)).select("flight_key").cache()

    # The census re-derives V1's containment rule, so it is checked against the
    # rule itself rather than trusted: `load_flight_intervals(months, days)` is
    # the exact call every arm makes. Two reference-parquet reads; no cluster
    # work worth the name, against a number that would otherwise be wrong in
    # precisely the way nobody can see on the page.
    #
    # **The check is one-sided.** Every flight V1 keeps must be counted inside;
    # if one is not, the census is narrower than the rule it claims to describe
    # and the job stops. The other direction -- flights wholly inside that V1
    # does not load -- is reported as `surplus` rather than fatal, and it is how
    # this census found the defect fixed in Task 4b: `load_flight_intervals`
    # used to filter on `day = to_date(AOBT_3)`, the *off-block* day, before
    # applying its own `t_off`-based window, so a flight that went off-block at
    # 23:5x and got airborne after midnight was dropped by the day key even
    # though its whole airborne interval lay inside the window. Measured on the
    # committed reference data at the time: 53 flights of 93,026 for 2025
    # (0.06 %) and 55 of 90,867 for 2024, every one of them departure-day 06-04
    # with `t_off` a few seconds past midnight, and zero in the other direction.
    #
    # `track_truth` now widens that pre-filter past the window and lets
    # `t_off`/`t_land` decide, so `surplus` is expected to be zero and a
    # non-zero value is news again -- either a fresh way for the day key to
    # decide membership, or a sample abutting a month edge, where the widened
    # day reaches into a month `months` does not load. It is printed below and
    # returned as its own input count either way.
    v1 = track_truth.load_flight_intervals(spark, period_cfg["months"], days)
    n_kept = v1.count()
    missed = v1.join(inside, "flight_key", "left_anti").count()
    inside.unpersist()
    gt.unpersist()
    if missed:
        raise SystemExit(
            f"containment census is narrower than the rule it measures: "
            f"{missed:,} flight(s) that load_flight_intervals keeps are not "
            "counted as wholly inside. The census predicate has drifted from "
            "track_truth's."
        )
    surplus = row["n_wholly_inside"] - n_kept
    print(f"cross-check: V1 loads {n_kept:,}, all of them counted inside")
    if surplus:
        print(
            f"  note: {surplus:,} further flight(s) lie wholly inside the window "
            "but V1 never loads them. Since Task 4b this should be zero -- check "
            "whether the sample abuts a month edge (pass the adjacent month in "
            "`months`) before treating it as a new defect."
        )

    for k in CONTAINMENT_FIELDS:
        print(f"  {k:34} {row[k]}")
    _write_csv(out_path, CONTAINMENT_FIELDS, [row])
    return {
        "gt_flights_overlapping_window": n_overlapping,
        "gt_flights_kept_by_v1": n_kept,
        "gt_flights_inside_but_not_loaded_by_v1": surplus,
    }


def run_boundary_hist(spark, s3, args, period_cfg, days, out_path) -> dict:
    """Histogram each arm, writing its bins out the moment they are computed.

    Appended and flushed per arm, the pattern ``track_methods`` and
    ``track_sweep`` both use and for the same reason: this arm's assignment
    table has already been deleted by the time the next one starts, so a
    failure on the last arm would otherwise discard the earlier arms' numbers
    with no way back to them short of a full cluster re-run.
    """
    sv = spark.read.parquet(period_cfg["tracks"]).filter(
        F.to_date("event_time").isin(days)
    )
    sv = track_methods.attach_airport_context(spark, sv).cache()
    gt = track_truth.load_flight_intervals(
        spark, period_cfg["months"], days
    ).cache()
    n_sv, n_gt = sv.count(), gt.count()
    print(f"{n_sv:,} samples, {n_gt:,} ground-truth flights")

    cfg = OPDIConfig().segmentation
    params = SegmentationParams(
        gap_minutes=cfg.gap_minutes,
        low_alt_gap_minutes=cfg.low_alt_gap_minutes,
        low_alt_ft=cfg.low_alt_ft,
        ground_dwell_minutes=cfg.ground_dwell_minutes,
        turnaround_max_height_ft=cfg.turnaround_max_height_ft,
        turnaround_max_speed_kt=cfg.turnaround_max_speed_kt,
        descent_floor_ft=cfg.descent_floor_ft,
    )

    fh = out_path.open("w", newline="")
    writer = csv.DictWriter(fh, fieldnames=HIST_FIELDS)
    writer.writeheader()
    try:
        for arm in args.arms:
            print(f"\n=== {arm} ({args.period}) ===")
            track_methods.require_headroom(s3, f"arm {arm}")
            arm_rows = run_arm_histogram(
                spark, s3, arm, args.period, sv, gt, params, args
            )
            for r in arm_rows:
                writer.writerow({k: r[k] for k in HIST_FIELDS})
            fh.flush()
            for edge in ("off", "land"):
                n = sum(r["n"] for r in arm_rows if r["edge"] == edge)
                print(f"  {edge:5} {n:,} flights binned")
    finally:
        fh.close()
    sv.unpersist()
    gt.unpersist()
    return {"samples": n_sv, "gt_flights": n_gt}


def run_traffic_fill(spark, args, period_cfg, days, out_path) -> dict:
    """What A3's split predicate cannot see, over the period's raw cleaned track.

    Reads the same cleaned track table the arms jobs read -- ``period_cfg
    ["tracks"]``, filtered to the sampled days -- and nothing else: no ground
    truth, no segmentation, no assignment table. ``gap_boundary_nulls`` uses
    ``traffic``'s own fixed 10-minute threshold by default, not the pipeline's
    configurable ``gap_minutes``, because this measures A3's behaviour
    specifically.

    ``osn_tracks_clean``/``tracks_clean`` store ``baro_altitude`` in metres --
    storage is SI, ``cleaning/native.py`` and ``segmentation/base.py`` both
    convert only at the point of use -- but ``gap_boundary_nulls`` reads
    ``baro_altitude_ft``, the aviation-unit name its own field names promise.
    Converting here, once, at the point of use is that same rule; ``FT_PER_M``
    is imported from ``segmentation/base.py`` rather than retyped, so the
    pipeline's conversion and this one can never drift apart. No other column
    ``gap_boundary_nulls`` reads (``icao24``, ``event_time``, ``on_ground``) is
    unit-bearing, so this is the only conversion this job needs.
    """
    sv = spark.read.parquet(period_cfg["tracks"]).filter(
        F.to_date("event_time").isin(days)
    ).withColumn("baro_altitude_ft", F.col("baro_altitude") * FT_PER_M).cache()
    n_sv = sv.count()
    print(f"{n_sv:,} samples")

    row = gap_boundary_nulls(sv)
    sv.unpersist()
    row["period"] = args.period

    for k in TRAFFIC_FILL_FIELDS:
        print(f"  {k:22} {row[k]}")
    _write_csv(out_path, TRAFFIC_FILL_FIELDS, [row])
    return {"samples": n_sv}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--job", choices=["containment", "boundary-hist", "traffic-fill"],
        required=True,
    )
    ap.add_argument("--period", choices=sorted(track_methods.PERIODS), default="2025")
    ap.add_argument("--results-dir", type=Path, required=True)
    ap.add_argument("--out-name", default=None,
                    help="default: <job>_<period>.csv")
    ap.add_argument("--arms", nargs="+", default=list(DEFAULT_HIST_ARMS),
                    help="boundary-hist only")
    ap.add_argument("--bin-seconds", type=int, default=BIN_SECONDS)
    ap.add_argument("--span-seconds", type=int, default=SPAN_SECONDS)
    ap.add_argument("--executors", type=int, default=10)
    ap.add_argument("--ui-port", type=int, default=4059)
    ap.add_argument("--days", nargs="+", default=None,
                    help="override the period's day list")
    ap.add_argument("--keep-assignments", action="store_true",
                    help="do not delete each arm's assignment table (default: delete)")
    args = ap.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
    load_dotenv()
    s3 = track_methods.s3_client()
    track_methods.require_headroom(s3, "startup")

    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(6, "9g")
    spark.sparkContext.setLogLevel("ERROR")

    period_cfg = track_methods.PERIODS[args.period]
    days = args.days or period_cfg["days"]

    out_name = args.out_name or f"{args.job.replace('-', '_')}_{args.period}.csv"
    args.results_dir.mkdir(parents=True, exist_ok=True)
    out_path = args.results_dir / out_name

    if args.job == "containment":
        inputs = run_containment(spark, args, period_cfg, days, out_path)
        tables = []
        code = ["benchmarks/track_diagnostics.py", "benchmarks/track_truth.py"]
    elif args.job == "traffic-fill":
        inputs = run_traffic_fill(spark, args, period_cfg, days, out_path)
        tables = [period_cfg["tracks"]]
        code = ["benchmarks/track_diagnostics.py"]
    else:
        inputs = run_boundary_hist(spark, s3, args, period_cfg, days, out_path)
        tables = [period_cfg["tracks"]]
        code = ["benchmarks/track_diagnostics.py", "benchmarks/track_truth.py",
                "benchmarks/track_score.py",
                "src/opdi/pipeline/segmentation/base.py",
                "src/opdi/pipeline/segmentation/methods.py"]

    provenance.record(
        args.results_dir, out_name,
        script="benchmarks/track_diagnostics.py",
        argv=sys.argv[1:],
        code_paths=code,
        inputs=inputs,
        input_tables=tables,
        notes=f"job={args.job}. days={days}.",
    )
    print(f"\n-> {out_path}")
    print(
        f"bucket: {track_methods.bucket_total_gb(s3):.2f} GB used of "
        f"~{track_methods.BUCKET_QUOTA_GB:.0f} GB after this run"
    )
    spark.stop()


if __name__ == "__main__":
    main()
