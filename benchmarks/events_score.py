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


def score_by_truth_resolution(aligned: DataFrame) -> DataFrame:
    """The same scores, split by whether the airport reports to the second.

    Without this split the ATOT/ALDT error distribution mostly measures APDF's
    own quantisation: 64% of movement times land on a whole minute, so a
    perfect detector would still show a spread of +/-30 s against them. The
    sub-minute subset is the only place a seconds-level claim can be made.
    """
    return score(aligned, group_cols=("milestone", "gt_subminute"))


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
