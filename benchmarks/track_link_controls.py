"""The evidence for A10's link decision: real holes, and holes made on purpose.

A10 ``cruise_linked`` keeps one ``track_id`` across a reception hole when the two
edges can be one flight, and no published method says how often that is wrong. This
module builds what OPDI measures it with. Every row carries the features of
``methods.link_feature_columns``, so the study applies ``methods.linkable`` for each
parameter set to one frame and never re-segments.

**Real holes** are every gap over ``gap_minutes`` in an airframe's samples. Network
Manager labels them by airframe and time and never by callsign: the rule has a
callsign test, and scoring it through a callsign join would be circular. Many stay
``unknown``, because truth is European and holds only flights inside the sampled
days, so the share of unknowns is a result to report and not a defect.

**Negative controls** hide a real landing in a hole. For consecutive truth flights
of one airframe, the samples from a cut in the first to a cut in the second are
removed, and a link is a false join. The base cut uses the last sample at or above
the cruise floor in the first flight and the first in the second. That alone is too
easy: both edges are near one airport, so the implied speed rejects nearly all of
it. Deep cuts move further into cruise by the same time on each side, which gives
the case that matters: A to B then B to C, with cruise edges and a plausible speed.

**Positive controls** remove the middle of one truth flight's cruise, and no link is
a missed link. Removal never reaches beyond the requested length, since a hole a
sample longer than ``link_max_gap_minutes`` would count against the rule at the
longest gap for a reason that has nothing to do with the flight. A control whose
cut meets a real hole, and so cannot have that length, is dropped.

**A control is scored by the rule's own expressions.** Generators only pick samples
around a cut, then call ``link_feature_columns`` on a window partitioned by
``control_id``. They keep what those expressions can read: ``LINK_EDGE_SAMPLES`` rows
each side, the callsign lookback before the cut, and the lookahead after it.

``same_callsign`` compares the callsigns the rule itself sees, not Network Manager's,
and is true wherever the callsign test cannot reject, including a blank side. So the
same-callsign stratum is the one the other tests must decide alone. Controls of
``gap_minutes`` or less are excluded, as they are never candidates. Controls over
360 min keep a stratum of their own: they exceed every ``link_max_gap_minutes`` in
the study's grid, so they read as correct rejections and must not hide among the rest.

This module reads no storage. Callers hand it frames: samples with the raw
``osn_tracks`` columns (``icao24``, ``event_time``, ``lat``, ``lon``,
``baro_altitude``, ``velocity``, ``vert_rate``, ``callsign``), and truth from
``track_truth.load_flight_intervals``.
"""
import math
from typing import Iterable, Optional

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from opdi.pipeline.segmentation import SegmentationParams
from opdi.pipeline.segmentation.base import FT_PER_M, KT_PER_MPS
from opdi.pipeline.segmentation.methods import LINK_EDGE_SAMPLES, link_feature_columns

__all__ = [
    "real_hole_candidates",
    "label_real_holes",
    "negative_controls",
    "positive_controls",
    "rule_of_three_upper",
]

#: How far a positive control's hole may fall short of the requested length, in
#: minutes. A hole ends on a real sample, so it is short by up to one sample
#: interval; anything more means a real hole sits at the cut.
POSITIVE_GAP_TOLERANCE_MIN = 1.0

_DEFAULT_PARAMS = SegmentationParams()


def _with_engine_columns(samples: DataFrame) -> DataFrame:
    """Add the columns the rule's expressions read, as ``assign_track_id`` does."""
    return (
        samples.withColumn("_ts", F.to_timestamp("event_time"))
        .withColumn("_alt_ft", F.col("baro_altitude") * FT_PER_M)
        .withColumn("_spd_kt", F.col("velocity") * KT_PER_MPS)
    )


def real_hole_candidates(tracks: DataFrame, p) -> DataFrame:
    """One row per gap over ``p.gap_minutes``, with the features and both edge times.

    Gaps are per airframe and ignore any ``track_id`` the frame carries: a hole is
    the same hole whichever rule cut there. A sample duplicated at the new edge sits
    zero minutes after its twin, so the hole is still one row.
    """
    window = Window.partitionBy("icao24").orderBy("_ts")
    features = link_feature_columns(p, window)
    return (
        _with_engine_columns(tracks)
        .select(
            F.lag(F.col("_ts")).over(window).alias("t_old_edge"),
            F.col("_ts").alias("t_new_edge"),
            *[c.alias(name) for name, c in features.items()],
        )
        .filter(F.col("gap_min") > p.gap_minutes)
    )


def label_real_holes(holes: DataFrame, truth: DataFrame) -> DataFrame:
    """Add ``label``: ``true_link``, ``false_join`` or ``unknown``. One row per hole.

    ``true_link`` means the hole lies within one flight's ``[t_off, t_land]``, and
    ``false_join`` that a truth landing and a later take-off of that airframe fall
    inside it. Anything else is ``unknown``, including a landing alone, since truth
    may not hold what came next. Where overlapping truth satisfies both, a false
    join wins, because hiding one is the worse error. Matched on ``icao24`` and time
    only.

    A landing is followed by a take-off exactly when the earliest landing in the
    hole precedes the latest take-off in it, so no join of flights to flights is
    needed.
    """
    key = ["icao24", "t_old_edge", "t_new_edge"]
    old, new = F.col("t_old_edge"), F.col("t_new_edge")
    t_off, t_land = F.col("t_off"), F.col("t_land")

    labels = (
        holes.select(*key)
        .distinct()
        .join(truth.select("icao24", "t_off", "t_land"), on="icao24", how="left")
        .groupBy(*key)
        .agg(
            F.max((t_off <= old) & (new <= t_land)).alias("_inside_one"),
            F.min(F.when(t_land.between(old, new), t_land)).alias("_first_landing"),
            F.max(F.when(t_off.between(old, new), t_off)).alias("_last_takeoff"),
        )
        .withColumn(
            "label",
            F.when(F.col("_first_landing") < F.col("_last_takeoff"), "false_join")
            .when(F.col("_inside_one"), "true_link")
            .otherwise("unknown"),
        )
        .select(*key, "label")
    )
    return holes.join(labels, on=key, how="inner")


def _flight_samples(tracks: DataFrame, truth: DataFrame) -> DataFrame:
    """Each sample inside a truth flight's interval, under that flight's key.

    Airframe and time, as ``track_truth.overlap_join``, which cannot be used here
    because its fixed output drops the columns the features need.
    """
    flights = truth.select(
        F.col("flight_key"),
        F.col("icao24").alias("_truth_icao24"),
        F.col("t_off"),
        F.col("t_land"),
    ).distinct()
    samples = _with_engine_columns(tracks).select(
        "icao24", "lat", "lon", "vert_rate", "callsign", "_ts", "_alt_ft", "_spd_kt"
    )
    return samples.join(
        flights,
        (F.col("icao24") == F.col("_truth_icao24"))
        & F.col("_ts").between(F.col("t_off"), F.col("t_land")),
        "inner",
    ).drop("_truth_icao24", "t_off", "t_land")


def _seconds(column: str) -> Column:
    return F.unix_timestamp(F.col(column))


def _edges_of_cruise(samples: DataFrame, floor_ft: float) -> DataFrame:
    """Each sample with its flight's first and last sample at or above the floor."""
    flight = Window.partitionBy("flight_key")
    at_floor = F.col("_alt_ft") >= floor_ft
    return samples.withColumn(
        "_first_cruise", F.min(F.when(at_floor, F.col("_ts"))).over(flight)
    ).withColumn("_last_cruise", F.max(F.when(at_floor, F.col("_ts"))).over(flight))


def _deep_cuts(samples: DataFrame, floor_ft: float, depths: DataFrame) -> tuple:
    """Each flight's ``(old_cut, new_cut)`` per depth, none where it would leave cruise.

    A cruise here is the unbroken run of samples at or above the floor that ends at
    the last such sample, or starts at the first. A dip below the floor ends it, and
    a sample with no altitude does not, since ``linkable``'s altitude test ignores a
    NULL too. A flight without enough cruise for a depth has no cut at that depth.
    """
    flight = Window.partitionBy("flight_key")
    below = F.col("_alt_ft") < floor_ft
    with_runs = (
        _edges_of_cruise(samples.filter(F.col("_alt_ft").isNotNull()), floor_ft)
        .withColumn(
            "_dip_before_last",
            F.max(F.when(below & (F.col("_ts") < F.col("_last_cruise")), F.col("_ts"))).over(flight),
        )
        .withColumn(
            "_dip_after_first",
            F.min(F.when(below & (F.col("_ts") > F.col("_first_cruise")), F.col("_ts"))).over(flight),
        )
        .filter(~below)
        .crossJoin(depths)
    )
    depth_s = F.col("depth_min") * 60
    old_cut = with_runs.filter(
        (_seconds("_ts") <= _seconds("_last_cruise") - depth_s)
        & (F.col("_dip_before_last").isNull() | (F.col("_ts") > F.col("_dip_before_last")))
    ).groupBy("flight_key", "depth_min").agg(F.max("_ts").alias("t_old_edge"))
    new_cut = with_runs.filter(
        (_seconds("_ts") >= _seconds("_first_cruise") + depth_s)
        & (F.col("_dip_after_first").isNull() | (F.col("_ts") < F.col("_dip_after_first")))
    ).groupBy("flight_key", "depth_min").agg(F.min("_ts").alias("t_new_edge"))
    return old_cut, new_cut


def _gap_stratum(gap_min: Column) -> Column:
    """The spec's strata. Upper edges belong to the lower one, as ``gap <= max`` does."""
    return (
        F.when(gap_min <= 120.0, "30-120")
        .when(gap_min <= 240.0, "120-240")
        .when(gap_min <= 360.0, "240-360")
        .otherwise("over-360")
    )


def _score_cuts(samples: DataFrame, cuts: DataFrame, p, expected: str) -> DataFrame:
    """One row per cut: the samples around it, scored by the rule's own expressions.

    ``cuts`` has ``control_id``, ``depth_min``, ``requested_gap_min``, the two edge
    times, and the flights on each side as ``flight_before`` and ``flight_after``.
    """
    newest_first = Window.partitionBy("control_id").orderBy(F.col("_ts").desc())
    oldest_first = Window.partitionBy("control_id").orderBy(F.col("_ts"))
    lookback_s = p.link_callsign_lookback_minutes * 60
    lookahead_s = p.link_callsign_lookahead_minutes * 60

    before = (
        samples.join(
            cuts,
            (F.col("flight_key") == F.col("flight_before"))
            & (F.col("_ts") <= F.col("t_old_edge")),
        )
        .withColumn("_rank", F.row_number().over(newest_first))
        .filter(
            (F.col("_rank") <= LINK_EDGE_SAMPLES)
            | (_seconds("_ts") >= _seconds("t_old_edge") - lookback_s)
        )
    )
    after = (
        samples.join(
            cuts,
            (F.col("flight_key") == F.col("flight_after"))
            & (F.col("_ts") >= F.col("t_new_edge")),
        )
        .withColumn("_rank", F.row_number().over(oldest_first))
        .filter(
            (F.col("_rank") <= LINK_EDGE_SAMPLES)
            | (_seconds("_ts") <= _seconds("t_new_edge") + lookahead_s)
        )
    )

    features = link_feature_columns(p, Window.partitionBy("control_id").orderBy("_ts"))
    return (
        before.unionByName(after)
        .select(
            "control_id", "depth_min", "requested_gap_min", "t_old_edge", "t_new_edge", "_ts",
            *[c.alias(name) for name, c in features.items()],
        )
        # A sample duplicated at the new edge is zero minutes after its twin.
        .filter((F.col("_ts") == F.col("t_new_edge")) & (F.col("gap_min") > p.gap_minutes))
        .drop("_ts")
        .withColumn("gap_stratum", _gap_stratum(F.col("gap_min")))
        .withColumn(
            "same_callsign",
            ~F.coalesce(F.col("cs_before") != F.col("cs_after"), F.lit(False)),
        )
        .withColumn("expected", F.lit(expected))
    )


def _minutes_frame(samples: DataFrame, values: Iterable[float], name: str) -> DataFrame:
    """A one-column frame of the given minutes, in the session of *samples*."""
    spark = samples.sparkSession
    return spark.createDataFrame([(float(v),) for v in values], [name])


def negative_controls(
    tracks: DataFrame,
    truth: DataFrame,
    floor_ft: float,
    depths_min=(0, 30, 60, 120),
    p: SegmentationParams = _DEFAULT_PARAMS,
) -> DataFrame:
    """A real landing hidden in a hole, one row per consecutive flight pair and depth.

    ``expected`` is ``split``. A pair whose truth intervals overlap is not a landing
    followed by a take-off, so it gives no control. ``p`` supplies the callsign
    settings the features read; pass the same one given to
    :func:`real_hole_candidates`.
    """
    samples = _flight_samples(tracks, truth)
    old_cut, new_cut = _deep_cuts(samples, floor_ft, _minutes_frame(samples, depths_min, "depth_min"))

    by_airframe = Window.partitionBy("icao24").orderBy("t_off", "t_land")
    pairs = (
        truth.select("flight_key", "icao24", "t_off", "t_land")
        .distinct()
        .withColumn("flight_after", F.lead("flight_key").over(by_airframe))
        .withColumn("_next_off", F.lead("t_off").over(by_airframe))
        .filter(F.col("t_land") <= F.col("_next_off"))
        .select(F.col("flight_key").alias("flight_before"), "flight_after")
    )
    cuts = (
        pairs.join(old_cut.withColumnRenamed("flight_key", "flight_before"), on="flight_before")
        .join(
            new_cut.withColumnRenamed("flight_key", "flight_after"),
            on=["flight_after", "depth_min"],
        )
        .withColumn(
            "control_id",
            F.concat_ws("|", F.lit("negative"), "flight_before", "flight_after", "depth_min"),
        )
        .withColumn("requested_gap_min", F.lit(None).cast("double"))
    )
    return _score_cuts(samples, cuts, p, expected="split")


def positive_controls(
    tracks: DataFrame,
    truth: DataFrame,
    floor_ft: float,
    gaps_min=(60, 120, 180, 360),
    p: SegmentationParams = _DEFAULT_PARAMS,
) -> DataFrame:
    """The middle of one flight's cruise removed, one row per flight and length.

    ``expected`` is ``link``. The hole is centred between the flight's first and last
    samples at or above the floor, and starts at the last such sample at or before
    where it should. ``requested_gap_min`` says which length was asked for, as
    ``gap_stratum`` merges 60 and 120, and ``depth_min`` is NULL. A flight whose
    cruise is shorter than the hole has no control at that length. ``p`` is as in
    :func:`negative_controls`.
    """
    samples = _flight_samples(tracks, truth)
    gaps = _minutes_frame(samples, gaps_min, "requested_gap_min")

    centre_s = (_seconds("_first_cruise") + _seconds("_last_cruise")) / 2.0
    old_cut = (
        _edges_of_cruise(samples, floor_ft)
        .filter(F.col("_alt_ft") >= floor_ft)
        .crossJoin(gaps)
        .filter(_seconds("_ts") <= centre_s - F.col("requested_gap_min") * 30.0)
        .groupBy("flight_key", "requested_gap_min")
        .agg(F.max("_ts").alias("t_old_edge"))
    )
    new_cut = (
        samples.select("flight_key", "_ts")
        .join(old_cut, on="flight_key")
        .filter(
            (F.col("_ts") > F.col("t_old_edge"))
            & (_seconds("_ts") <= _seconds("t_old_edge") + F.col("requested_gap_min") * 60.0)
        )
        .groupBy("flight_key", "requested_gap_min", "t_old_edge")
        .agg(F.max("_ts").alias("t_new_edge"))
    )
    short_by_min = (
        F.col("requested_gap_min") - (_seconds("t_new_edge") - _seconds("t_old_edge")) / 60.0
    )
    cuts = (
        new_cut.filter(short_by_min <= POSITIVE_GAP_TOLERANCE_MIN)
        .withColumnRenamed("flight_key", "flight_before")
        .withColumn("flight_after", F.col("flight_before"))
        .withColumn(
            "control_id",
            F.concat_ws("|", F.lit("positive"), "flight_before", "requested_gap_min"),
        )
        .withColumn("depth_min", F.lit(None).cast("double"))
    )
    return _score_cuts(samples, cuts, p, expected="link")


def _binomial_cdf(k: int, n: int, prob: float) -> float:
    """``P(X <= k)`` for ``X ~ Bin(n, prob)``, in logs so a large *n* cannot underflow."""
    log_p, log_q = math.log(prob), math.log1p(-prob)
    return sum(
        math.exp(
            math.lgamma(n + 1) - math.lgamma(i + 1) - math.lgamma(n - i + 1)
            + i * log_p + (n - i) * log_q
        )
        for i in range(k + 1)
    )


def rule_of_three_upper(n_events: int, n: int) -> Optional[float]:
    """Clopper-Pearson one-sided 95% upper bound on a rate, or ``None`` when ``n == 0``.

    Zero of ``n`` bounds the rate at about ``3 / n``, hence the name, and it is what
    the study reports because zero false joins alone would say nothing about ``n``.
    The bound is the probability at which seeing ``n_events`` or fewer has a 5%
    chance, found by bisection because the tail falls as the probability rises.
    """
    if n_events < 0 or n_events > n:
        raise ValueError(f"need 0 <= n_events <= n, got {n_events} of {n}")
    if n == 0:
        return None
    if n_events == n:
        return 1.0
    low, high = 0.0, 1.0
    for _ in range(60):
        mid = (low + high) / 2.0
        if _binomial_cdf(n_events, n, mid) > 0.05:
            low = mid
        else:
            high = mid
    return (low + high) / 2.0
