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
A real reception hole under a cut moves the edge beyond the depth asked for, so each
control records the depth it reached, one that overshoots by more than
``DEPTH_TOLERANCE_MIN`` is dropped, and one hole is counted once per pair. Otherwise
one hole at several depths would tighten the upper bound without new evidence.

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

Every reason a control does not exist is counted by :func:`control_funnel`, because
a rate over the controls that survived says nothing about the ones that did not.

This module reads no storage. Callers hand it frames: samples with the raw
``osn_tracks`` columns (``icao24``, ``event_time``, ``lat``, ``lon``,
``baro_altitude``, ``velocity``, ``vert_rate``, ``callsign``), and truth from
``track_truth.load_flight_intervals``.
"""
import math
from typing import Dict, Iterable, Optional

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from opdi.pipeline.segmentation import SegmentationParams
from opdi.pipeline.segmentation.base import FT_PER_M, KT_PER_MPS
from opdi.pipeline.segmentation.methods import LINK_EDGE_SAMPLES, link_feature_columns

__all__ = [
    "flight_samples",
    "real_hole_candidates",
    "label_real_holes",
    "negative_controls",
    "positive_controls",
    "control_funnel",
    "rule_of_three_upper",
]

#: How far a positive control's hole may fall short of the requested length, in
#: minutes. A hole ends on a real sample, so it is short by up to one sample
#: interval; anything more means a real hole sits at the cut.
POSITIVE_GAP_TOLERANCE_MIN = 1.0

#: How far beyond its requested depth a negative control's cut may land, in minutes.
#: A cut takes the nearest sample further into cruise, so a real reception hole there
#: pushes it on; past this it is no longer the depth it would be reported under.
DEPTH_TOLERANCE_MIN = 5.0

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

    ``false_join`` means a truth landing or take-off of that airframe falls inside the
    hole. Either one proves a stop, given what is being labelled: a linked hole has
    both edges at cruise, so an aircraft cannot have touched the ground between them
    and still be one flight. ``true_link`` means the hole lies within one flight's
    ``[t_off, t_land]`` with no such event inside. Anything else is ``unknown``.
    Matched on ``icao24`` and time only.

    Inside is strict: a hole whose edge is exactly a flight's take-off or landing lies
    within that flight, and the event is not hidden by it.
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
            F.max((t_off <= old) & (new <= t_land)).alias("_within_one"),
            F.max(
                ((old < t_off) & (t_off < new)) | ((old < t_land) & (t_land < new))
            ).alias("_event_inside"),
        )
        .withColumn(
            "label",
            F.when(F.col("_event_inside"), "false_join")
            .when(F.col("_within_one"), "true_link")
            .otherwise("unknown"),
        )
        .select(*key, "label")
    )
    return holes.join(labels, on=key, how="inner")


def flight_samples(tracks: DataFrame, truth: DataFrame) -> DataFrame:
    """Each sample inside a truth flight's interval, under that flight's key.

    Airframe and time, as ``track_truth.overlap_join``, which cannot be used here
    because its fixed output drops the columns the features need. Every generator
    starts from this, so a caller making several on a real day caches it once and
    hands it to them as ``samples``.
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
    Each cut carries the depth it reached, which a real hole can put beyond the one
    asked for.
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
    ).groupBy("flight_key", "depth_min", "_last_cruise").agg(F.max("_ts").alias("t_old_edge"))
    new_cut = with_runs.filter(
        (_seconds("_ts") >= _seconds("_first_cruise") + depth_s)
        & (F.col("_dip_after_first").isNull() | (F.col("_ts") < F.col("_dip_after_first")))
    ).groupBy("flight_key", "depth_min", "_first_cruise").agg(F.min("_ts").alias("t_new_edge"))
    old_cut = old_cut.withColumn(
        "old_depth_min", (_seconds("_last_cruise") - _seconds("t_old_edge")) / 60.0
    ).drop("_last_cruise")
    new_cut = new_cut.withColumn(
        "new_depth_min", (_seconds("t_new_edge") - _seconds("_first_cruise")) / 60.0
    ).drop("_first_cruise")
    return old_cut, new_cut


_CUT_COLUMNS = (
    "control_id", "pair_id", "flight_before", "flight_after", "t_old_edge", "t_new_edge",
    "depth_min", "old_depth_min", "new_depth_min", "requested_gap_min",
)


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

    ``cuts`` has the :data:`_CUT_COLUMNS`: the flights on each side, the two edge
    times, and the columns that say what kind of control it is.
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
            *[c for c in _CUT_COLUMNS if c not in ("flight_before", "flight_after")], "_ts",
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
    """A one-column frame of the given minutes, in the session of *samples*.

    Broadcast because it gets cross-joined with every sample at the floor, and a frame
    built from a list has no size estimate, so Spark would plan a ``CartesianProduct``.
    """
    spark = samples.sparkSession
    return F.broadcast(spark.createDataFrame([(float(v),) for v in values], [name]))


def _hole_min() -> Column:
    return (_seconds("t_new_edge") - _seconds("t_old_edge")) / 60.0


def _flights_in_cruise(samples: DataFrame, floor_ft: float) -> DataFrame:
    """``flight_key`` of each flight with a sample at or above the floor."""
    return samples.filter(F.col("_alt_ft") >= floor_ft).select("flight_key").distinct()


def _flight_pairs(samples: DataFrame, truth: DataFrame, floor_ft: float) -> DataFrame:
    """Consecutive truth flights of one airframe, each with a ``pair_fate``.

    ``overlap`` means the intervals overlap, so this is not a landing followed by a
    take-off. ``no_cruise_sample`` means a flight has no sample at the floor to cut at.
    """
    by_airframe = Window.partitionBy("icao24").orderBy("t_off", "t_land")
    cruising = _flights_in_cruise(samples, floor_ft).withColumn("_cruises", F.lit(True))
    pairs = (
        truth.select("flight_key", "icao24", "t_off", "t_land")
        .distinct()
        .withColumn("flight_after", F.lead("flight_key").over(by_airframe))
        .withColumn("_next_off", F.lead("t_off").over(by_airframe))
        .filter(F.col("flight_after").isNotNull())
        .withColumnRenamed("flight_key", "flight_before")
    )
    return (
        pairs.join(
            cruising.select(
                F.col("flight_key").alias("flight_before"), F.col("_cruises").alias("_cruises_1")
            ),
            on="flight_before",
            how="left",
        )
        .join(
            cruising.select(
                F.col("flight_key").alias("flight_after"), F.col("_cruises").alias("_cruises_2")
            ),
            on="flight_after",
            how="left",
        )
        .withColumn("pair_id", F.concat_ws("|", "flight_before", "flight_after"))
        .withColumn(
            "pair_fate",
            F.when(F.col("t_land") > F.col("_next_off"), "overlap")
            .when(F.col("_cruises_1").isNull() | F.col("_cruises_2").isNull(), "no_cruise_sample")
            .otherwise("usable"),
        )
        .select("pair_id", "flight_before", "flight_after", "pair_fate")
    )


def _negative_attempts(samples: DataFrame, pairs: DataFrame, floor_ft: float, depths_min, p) -> DataFrame:
    """Each usable pair at each depth, with the ``fate`` of that attempt.

    In order: ``outside_cruise``, a side has no cut at this depth; ``depth_missed``, a
    side overshot the depth by more than :data:`DEPTH_TOLERANCE_MIN`; ``repeated_hole``,
    a shallower depth of this pair already gives these two edges; ``too_short``, not
    over ``gap_minutes``; otherwise ``control``.
    """
    depths = _minutes_frame(samples, depths_min, "depth_min")
    old_cut, new_cut = _deep_cuts(samples, floor_ft, depths)
    overshoot = (F.col("old_depth_min") - F.col("depth_min") > DEPTH_TOLERANCE_MIN) | (
        F.col("new_depth_min") - F.col("depth_min") > DEPTH_TOLERANCE_MIN
    )
    same_hole = Window.partitionBy("pair_id", "t_old_edge", "t_new_edge", "_reached").orderBy("depth_min")
    return (
        pairs.filter(F.col("pair_fate") == "usable")
        .crossJoin(depths)
        .join(
            old_cut.withColumnRenamed("flight_key", "flight_before"),
            on=["flight_before", "depth_min"],
            how="left",
        )
        .join(
            new_cut.withColumnRenamed("flight_key", "flight_after"),
            on=["flight_after", "depth_min"],
            how="left",
        )
        .withColumn(
            "_reached",
            F.when(F.col("t_old_edge").isNull() | F.col("t_new_edge").isNull(), "outside_cruise")
            .when(overshoot, "depth_missed")
            .otherwise("cut"),
        )
        .withColumn("_repeat", F.row_number().over(same_hole))
        .withColumn(
            "fate",
            F.when(F.col("_reached") != "cut", F.col("_reached"))
            .when(F.col("_repeat") > 1, "repeated_hole")
            .when(_hole_min() <= p.gap_minutes, "too_short")
            .otherwise("control"),
        )
        .withColumn("control_id", F.concat_ws("|", F.lit("negative"), "pair_id", "depth_min"))
        .withColumn("requested_gap_min", F.lit(None).cast("double"))
        .select(*_CUT_COLUMNS, "fate")
    )


def _positive_attempts(samples: DataFrame, truth: DataFrame, floor_ft: float, gaps_min, p) -> DataFrame:
    """Each truth flight at each requested length, with the ``fate`` of that attempt.

    In order: ``no_cruise_sample``; ``outside_cruise``, the cruise is shorter than the
    hole; ``gap_tolerance``, a real hole at the cut leaves it over
    :data:`POSITIVE_GAP_TOLERANCE_MIN` short; ``too_short``; otherwise ``control``.
    """
    gaps = _minutes_frame(samples, gaps_min, "requested_gap_min")
    cruising = _flights_in_cruise(samples, floor_ft).withColumn("_cruises", F.lit(True))

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
        .groupBy("flight_key", "requested_gap_min")
        .agg(F.max("_ts").alias("t_new_edge"))
    )
    return (
        truth.select("flight_key")
        .distinct()
        .crossJoin(gaps)
        .join(cruising, on="flight_key", how="left")
        .join(old_cut, on=["flight_key", "requested_gap_min"], how="left")
        .join(new_cut, on=["flight_key", "requested_gap_min"], how="left")
        .withColumn(
            "fate",
            F.when(F.col("_cruises").isNull(), "no_cruise_sample")
            .when(F.col("t_old_edge").isNull(), "outside_cruise")
            .when(
                F.col("t_new_edge").isNull()
                | (F.col("requested_gap_min") - _hole_min() > POSITIVE_GAP_TOLERANCE_MIN),
                "gap_tolerance",
            )
            .when(_hole_min() <= p.gap_minutes, "too_short")
            .otherwise("control"),
        )
        .withColumn("flight_before", F.col("flight_key"))
        .withColumn("flight_after", F.col("flight_key"))
        .withColumn(
            "control_id", F.concat_ws("|", F.lit("positive"), "flight_key", "requested_gap_min")
        )
        .withColumn("pair_id", F.lit(None).cast("string"))
        .withColumn("depth_min", F.lit(None).cast("double"))
        .withColumn("old_depth_min", F.lit(None).cast("double"))
        .withColumn("new_depth_min", F.lit(None).cast("double"))
        .select(*_CUT_COLUMNS, "fate")
    )


def negative_controls(
    tracks: DataFrame,
    truth: DataFrame,
    floor_ft: float,
    depths_min=(0, 30, 60, 120),
    p: SegmentationParams = _DEFAULT_PARAMS,
    samples: Optional[DataFrame] = None,
) -> DataFrame:
    """A real landing hidden in a hole, at most one row per pair and distinct hole.

    ``expected`` is ``split``. ``depth_min`` is the depth asked for, ``old_depth_min``
    and ``new_depth_min`` the depths reached, and ``pair_id`` names the flight pair, so
    a caller can count per pair as well as per control. ``p`` supplies the callsign
    settings the features read; pass the same one given to
    :func:`real_hole_candidates`. ``samples`` is :func:`flight_samples` already made,
    in which case ``tracks`` is not read.
    """
    samples = flight_samples(tracks, truth) if samples is None else samples
    pairs = _flight_pairs(samples, truth, floor_ft)
    attempts = _negative_attempts(samples, pairs, floor_ft, depths_min, p)
    return _score_cuts(samples, attempts.filter(F.col("fate") == "control"), p, expected="split")


def positive_controls(
    tracks: DataFrame,
    truth: DataFrame,
    floor_ft: float,
    gaps_min=(60, 120, 180, 360),
    p: SegmentationParams = _DEFAULT_PARAMS,
    samples: Optional[DataFrame] = None,
) -> DataFrame:
    """The middle of one flight's cruise removed, one row per flight and length.

    ``expected`` is ``link``. The hole is centred between the flight's first and last
    samples at or above the floor, and starts at the last such sample at or before
    where it should. ``requested_gap_min`` says which length was asked for, as
    ``gap_stratum`` merges 60 and 120. The columns of the negative controls' depth and
    pair are NULL. A flight whose cruise is shorter than the hole has no control at
    that length. ``p`` and ``samples`` are as in :func:`negative_controls`.
    """
    samples = flight_samples(tracks, truth) if samples is None else samples
    attempts = _positive_attempts(samples, truth, floor_ft, gaps_min, p)
    return _score_cuts(samples, attempts.filter(F.col("fate") == "control"), p, expected="link")


def _counts(frame: DataFrame, column: str) -> Dict[str, int]:
    return {r[column]: r["count"] for r in frame.groupBy(column).count().collect()}


def control_funnel(
    tracks: DataFrame,
    truth: DataFrame,
    floor_ft: float,
    depths_min=(0, 30, 60, 120),
    gaps_min=(60, 120, 180, 360),
    p: SegmentationParams = _DEFAULT_PARAMS,
    samples: Optional[DataFrame] = None,
) -> Dict[str, int]:
    """Where every candidate control went, from the same frames the generators use.

    Pair counts are per flight pair; attempts are a usable pair at one depth, or a
    flight at one length. Two identities hold, and the study can assert them:
    ``considered = overlap + no cruise + usable pairs``, and each ``attempts`` count is
    the sum of the counts beneath it. Costs several passes over the input, so a caller
    with a real day caches ``truth`` and passes ``samples``, as in
    :func:`negative_controls`.
    """
    samples = flight_samples(tracks, truth) if samples is None else samples
    pairs = _flight_pairs(samples, truth, floor_ft)
    pair_fates = _counts(pairs, "pair_fate")
    negative = _counts(_negative_attempts(samples, pairs, floor_ft, depths_min, p), "fate")
    positive = _counts(_positive_attempts(samples, truth, floor_ft, gaps_min, p), "fate")
    return {
        "negative_pairs_considered": sum(pair_fates.values()),
        "negative_pairs_lost_to_overlap": pair_fates.get("overlap", 0),
        "negative_pairs_lost_to_no_cruise_sample": pair_fates.get("no_cruise_sample", 0),
        "negative_pairs_usable": pair_fates.get("usable", 0),
        "negative_attempts": sum(negative.values()),
        "negative_cuts_dropped_outside_cruise": negative.get("outside_cruise", 0),
        "negative_cuts_dropped_for_depth": negative.get("depth_missed", 0),
        "negative_repeated_holes_dropped": negative.get("repeated_hole", 0),
        "negative_excluded_too_short": negative.get("too_short", 0),
        "negative_controls": negative.get("control", 0),
        "positive_attempts": sum(positive.values()),
        "positive_dropped_no_cruise_sample": positive.get("no_cruise_sample", 0),
        "positive_dropped_outside_cruise": positive.get("outside_cruise", 0),
        "positive_dropped_by_gap_tolerance": positive.get("gap_tolerance", 0),
        "positive_excluded_too_short": positive.get("too_short", 0),
        "positive_controls": positive.get("control", 0),
    }


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
