#!/usr/bin/env python
"""Scoring for the flight-event benchmark.

``adep_ades.score()`` does not transfer. That one is categorical -- an
aerodrome is right, wrong, or absent -- and its exchange rate (``correct −
k·wrong``) exists because ratios cannot settle a coverage/accuracy trade-off. A
milestone is different: it is a *time*, and the question is not whether it is
right but how far off it is and in which direction. A detector that is
consistently eight seconds early is far more useful than one scattered
symmetrically about zero, and a categorical scorer cannot tell them apart.

So the metrics here are:

* **coverage** -- reference milestones with any detection at all. The
  denominator is always ground truth, so a flight never seen counts as a miss
  rather than vanishing from the sample.
* **bias** -- *median* signed error. Median rather than mean throughout,
  because a single detection that landed on the wrong flight contributes an
  error of hours and would drag a mean anywhere.
* **spread** -- MAD and p90 |Δt|.
* **hit rates** at ±30 s and ±60 s, swept rather than fixed.
* **runway exact-match** against ``AP_C_RWY``.
* **position error** against ``C40_CROSS_LAT/LON`` for the rings.

On tolerances: ``C40_CROSS_TIME_CTFM`` and the shipped ``C40_CROSS_TIME`` are
two EUROCONTROL derivations of the same crossing and differ by p10 −9 s /
p90 +11 s. That spread is the floor on what "agreement" can mean here, and it
is a better yardstick than a round number picked for looking reasonable.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

#: Reported hit-rate thresholds, in seconds. Swept rather than fixed so the
#: shape of the error distribution is visible, not just one point on it.
TOLERANCES_S = (10, 30, 60, 120, 300)


def align(truth: DataFrame, detected: DataFrame) -> DataFrame:
    """One row per reference milestone, with the detection attached.

    Ground truth is the LEFT side throughout: a milestone the pipeline never
    produced has to count against it, and an inner join would silently drop
    exactly the failures being measured.

    Where a flight has several detections of the same milestone -- a go-around
    gives two ALDT candidates -- the nearest in time is kept. That is generous
    to the detector, and is stated rather than hidden: the alternative, taking
    the first, would score the detector on an arbitrary choice.
    """
    d = detected.select(
        F.col("icao24"),
        F.col("callsign"),
        F.col("day"),
        F.col("milestone"),
        F.col("event_time").alias("det_time"),
        *[c for c in ("det_runway", "det_lat", "det_lon") if c in detected.columns],
    )
    j = truth.join(d, ["icao24", "callsign", "day", "milestone"], "left")
    j = j.withColumn(
        "error_s",
        F.col("det_time").cast("double") - F.col("gt_time").cast("double"),
    )
    nearest = Window.partitionBy(
        "icao24", "callsign", "day", "milestone"
    ).orderBy(F.abs(F.col("error_s")).asc_nulls_last())
    return j.withColumn("_r", F.row_number().over(nearest)).filter(F.col("_r") == 1).drop("_r")


def align_by_detector(truth: DataFrame, detected: DataFrame) -> DataFrame:
    """:func:`align`, but keeping each detector's own best match.

    `align` drops ``det_type`` and partitions the nearest-detection window on
    the truth key alone, so where two detectors answer one milestone -- which
    is what ``events_v0.2.0`` does, publishing ``ATOT`` beside ``airborne`` and
    ``ALDT`` beside ``touchdown`` -- the pair is **pooled**: the detection
    nearest in time wins and the other is discarded. That is the right frame
    for "what did OPDI publish for this movement", and it is what the
    per-aerodrome annex reports. It cannot answer "how did *this* detector do",
    because the losing detector's row is gone before the scoring sees it, and a
    pooled bias is therefore a best-of-both figure rather than either
    detector's own.

    Here ``det_type`` is carried through and joined into the window partition,
    so each detector keeps its own nearest detection against the same truth row
    and scores on its own merits. Truth is still the LEFT side, so a detector
    that answered nothing still counts every reference movement against itself:
    the ``n_truth`` in each detector's rows is the same denominator, which is
    what makes the two comparable.
    """
    cols = ["icao24", "callsign", "day", "milestone"]
    d = detected.select(
        *[F.col(c) for c in cols],
        F.col("det_type"),
        F.col("event_time").alias("det_time"),
        *[F.col(c) for c in ("det_runway", "det_lat", "det_lon")
          if c in detected.columns],
    )
    # The detector list has to come from the data: a left join cannot invent a
    # row for a detector that produced nothing for this truth movement, and
    # without one such a detector would simply be absent rather than scored at
    # 0% coverage. Cross-joining truth against the detectors present keeps the
    # denominator identical for every detector.
    dets = d.select("milestone", "det_type").distinct()
    base = truth.join(F.broadcast(dets), on="milestone", how="inner")
    j = base.join(d, cols + ["det_type"], "left")
    j = j.withColumn(
        "error_s",
        F.col("det_time").cast("double") - F.col("gt_time").cast("double"),
    )
    nearest = Window.partitionBy(
        "icao24", "callsign", "day", "milestone", "det_type"
    ).orderBy(F.abs(F.col("error_s")).asc_nulls_last())
    return (
        j.withColumn("_r", F.row_number().over(nearest))
        .filter(F.col("_r") == 1)
        .drop("_r")
    )


def align_by_priority(truth: DataFrame, detected: DataFrame,
                      priority: "list[str]") -> DataFrame:
    """:func:`align`, resolving ties by a **fixed detector preference**.

    :func:`align` keeps whichever detection is nearest the reference. That is
    the right way to ask what the pair is *capable* of, but it is not a rule
    anybody can ship: choosing the nearest requires already knowing the answer.
    It is an oracle, and its bias is a lower bound no published table can
    reproduce.

    A shippable rule is a *priority*: take the preferred detector's answer when
    it has one, fall back to the next otherwise. Coverage is identical to the
    oracle's -- a movement is answered if any detector answered it, whatever the
    order -- but the bias is not, because each movement now carries the bias of
    whichever detector actually supplied it rather than of whichever happened to
    be luckier.

    ``priority`` is detector names best-first. A detector absent from the list
    sorts after every named one, so an unlisted detector is a last resort rather
    than being dropped.
    """
    cols = ["icao24", "callsign", "day", "milestone"]
    d = detected.select(
        *[F.col(c) for c in cols],
        F.col("det_type"),
        F.col("event_time").alias("det_time"),
        *[F.col(c) for c in ("det_runway", "det_lat", "det_lon")
          if c in detected.columns],
    )
    rank = F.lit(len(priority))
    for i, name in enumerate(priority):
        rank = F.when(F.col("det_type") == F.lit(name), F.lit(i)).otherwise(rank)
    d = d.withColumn("_pref", rank)

    j = truth.join(d, cols, "left")
    j = j.withColumn(
        "error_s",
        F.col("det_time").cast("double") - F.col("gt_time").cast("double"),
    )
    # Preference first, and only then nearest *within* the chosen detector --
    # so a second answer from the preferred detector still beats a closer one
    # from the fallback, which is what makes this a rule and not the oracle.
    chosen = Window.partitionBy(*cols).orderBy(
        F.col("_pref").asc_nulls_last(), F.abs(F.col("error_s")).asc_nulls_last()
    )
    return (
        j.withColumn("_r", F.row_number().over(chosen))
        .filter(F.col("_r") == 1)
        .drop("_r")
    )


def score(aligned: DataFrame, group_cols=("milestone",)) -> DataFrame:
    """Coverage, bias and spread per milestone."""
    err = F.col("error_s")
    aggs = [
        F.count(F.lit(1)).alias("n_truth"),
        F.sum(F.when(err.isNotNull(), 1).otherwise(0)).alias("n_detected"),
        F.expr("percentile_approx(error_s, 0.5)").alias("bias_s"),
        F.expr("percentile_approx(abs(error_s), 0.5)").alias("mad_s"),
        F.expr("percentile_approx(abs(error_s), 0.9)").alias("p90_abs_s"),
    ]
    for t in TOLERANCES_S:
        aggs.append(
            F.sum(F.when(F.abs(err) <= t, 1).otherwise(0)).alias(f"within_{t}s")
        )

    out = aligned.groupBy(*group_cols).agg(*aggs)
    out = out.withColumn(
        "coverage_pct", F.round(100.0 * F.col("n_detected") / F.col("n_truth"), 2)
    )
    for t in TOLERANCES_S:
        out = out.withColumn(
            f"within_{t}s_pct",
            F.round(100.0 * F.col(f"within_{t}s") / F.col("n_truth"), 2),
        )
    return out


#: Below this many detections a percentile is noise wearing a number's clothes.
#: Taken from `oac.aggregate.MIN_N`, which uses the same floor for the same
#: reason on the same aerodromes.
MIN_DETECTED = 20


def score_by_airport(aligned: DataFrame) -> DataFrame:
    """Coverage, bias and spread per (aerodrome, milestone).

    `score()` has taken `group_cols` since it was written and no caller has
    ever passed anything but the default. This is that caller.

    Thin cells are **marked, not dropped**: an aerodrome removed from the table
    is indistinguishable from one that was never in the study, and the twenty
    aerodromes here differ in size by a factor of twenty -- UGKO has 45
    departures over the sample where EBBR has 843.
    """
    out = score(aligned, group_cols=("gt_airport", "milestone"))
    return out.withColumn("reportable", F.col("n_detected") >= F.lit(MIN_DETECTED))


def score_by_truth_resolution(
    aligned: DataFrame, group_cols=("milestone", "gt_subminute")
) -> DataFrame:
    """The same scores, split by whether the airport reports to the second.

    Without this split the ATOT/ALDT error distribution mostly measures APDF's
    own quantisation: 64% of movement times land on a whole minute, so a
    perfect detector would still show a spread of +/-30 s against them. The
    sub-minute subset is the only place a seconds-level claim can be made.

    ``group_cols`` defaults to the network-level split, so every existing
    caller is unchanged. V4's annex passes
    ``("gt_airport", "milestone", "gt_subminute")``: reporting resolution is a
    property of the *aerodrome's* reporting system, not of the network, so
    "64% land on a whole minute" is an average over aerodromes that are
    individually at 0% or 100% -- and a reader who wants to know whether a
    given aerodrome's error figure is dominated by quantisation cannot get
    that from the pooled row. ``gt_subminute`` must stay in the grouping
    whatever else is added, or this stops being the resolution split.
    """
    group_cols = tuple(group_cols)
    if "gt_subminute" not in group_cols:
        raise ValueError(
            f"score_by_truth_resolution must group on 'gt_subminute'; got "
            f"{group_cols}. Without it this is `score()` under another name, "
            f"and the caller believes it is reading a resolution split."
        )
    return score(aligned, group_cols=group_cols)


def score_runways(aligned: DataFrame) -> DataFrame:
    """Exact-match rate against ``AP_C_RWY`` -- the one categorical metric.

    Designators are compared case-insensitively with whitespace stripped;
    beyond that they are compared literally, because '07R' and '07L' are
    different runways and a fuzzy match would hide precisely the error worth
    finding.
    """
    if "det_runway" not in aligned.columns:
        return None
    a = aligned.filter(F.col("gt_runway").isNotNull())
    norm = lambda c: F.upper(F.trim(F.col(c)))  # noqa: E731
    return a.groupBy("milestone").agg(
        F.count(F.lit(1)).alias("n_truth"),
        F.sum(F.when(F.col("det_runway").isNotNull(), 1).otherwise(0)).alias("n_named"),
        F.sum(
            F.when(norm("det_runway") == norm("gt_runway"), 1).otherwise(0)
        ).alias("n_exact"),
    ).withColumn(
        "exact_pct_of_named",
        F.round(100.0 * F.col("n_exact") / F.nullif(F.col("n_named"), F.lit(0)), 2),
    ).withColumn(
        "named_pct", F.round(100.0 * F.col("n_named") / F.col("n_truth"), 2)
    )


def score_positions(aligned: DataFrame) -> DataFrame:
    """Great-circle error against the reference crossing position, in NM."""
    if "det_lat" not in aligned.columns:
        return None
    from opdi.pipeline.flights import haversine_nm

    a = aligned.filter(F.col("det_lat").isNotNull() & F.col("gt_lat").isNotNull())
    a = a.withColumn(
        "pos_error_nm",
        haversine_nm(F.col("det_lat"), F.col("det_lon"), F.col("gt_lat"), F.col("gt_lon")),
    )
    return a.groupBy("milestone").agg(
        F.count(F.lit(1)).alias("n_compared"),
        F.expr("percentile_approx(pos_error_nm, 0.5)").alias("median_pos_error_nm"),
        F.expr("percentile_approx(pos_error_nm, 0.9)").alias("p90_pos_error_nm"),
    )


def inter_source_floor(ring_truth: DataFrame) -> DataFrame:
    """How far apart two EUROCONTROL derivations of the same crossing are.

    Not a score of OPDI at all -- it is the yardstick. A detector landing
    inside this spread is as close to the reference as the reference is to
    itself, and no tolerance tighter than this can be claimed to mean anything.
    """
    a = ring_truth.filter(F.col("gt_time_ctfm").isNotNull())
    a = a.withColumn(
        "src_delta_s",
        F.col("gt_time_ctfm").cast("double") - F.col("gt_time").cast("double"),
    )
    return a.groupBy("milestone").agg(
        F.count(F.lit(1)).alias("n"),
        F.expr("percentile_approx(src_delta_s, 0.1)").alias("p10_s"),
        F.expr("percentile_approx(src_delta_s, 0.5)").alias("median_s"),
        F.expr("percentile_approx(src_delta_s, 0.9)").alias("p90_s"),
    )


def guard_not_all_zero(scored: DataFrame) -> None:
    """Refuse to report a table of zeros as a result.

    Version 6 shipped a CSV of zeros because the callsigns were space-padded
    and the join matched nothing; the run exited 0 and the failure was found
    only when someone read the numbers. Zero coverage on *every* milestone
    means the join is broken, not that detection failed.
    """
    rows = scored.collect()
    if rows and all((r["n_detected"] or 0) == 0 for r in rows):
        raise SystemExit(
            "Every milestone scored zero coverage. That is an identity-join "
            "failure, not a detection result -- check the callsign trim and the "
            "day filter before believing this."
        )
