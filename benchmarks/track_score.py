"""Scoring a segmentation against ground-truth flight intervals.

A segmentation is a partition of an airframe's samples; ground truth is another
partition of the same samples. Comparing two partitions is a solved problem, and
the two standard scalars land exactly on the two failure modes
``track_quality.py`` names:

* **completeness** -- one flight scattered across tracks -> fragmentation
* **homogeneity** -- one track carrying several flights -> merging
* **V-measure** -- their harmonic mean; the smooth objective a sweep can optimise

Everything here is a contingency table plus an entropy sum, so it is a groupBy.
No UDF, no pandas.

The **headline** number is not V-measure -- nobody has an intuition for it. It is
``clean_match_pct``: the share of ground-truth flights that occupy exactly one
track, that track carrying no other flight.

Not measured here, deliberately: tracks with no ground-truth overlap. GT covers
NM flights only, so a track that merged an NM flight with a military or GA flight
looks clean. The paper must state this as a floor on the merge rate, not a
measurement of it.
"""

import math

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

__all__ = [
    "contingency", "vmeasure", "match_rates", "track_extents", "boundary_offsets",
    "boundary_error", "score_arm", "score_arm_gated",
]


def track_extents(assign: DataFrame) -> DataFrame:
    """One row per track: its true first and last sample time.

    Computed from the **full assignment table** -- ``(icao24, event_time,
    track_id)``, before ``overlap_join`` restricts anything to a ground-truth
    interval -- not from ``matched``. See :func:`boundary_error`'s docstring
    for why the distinction is load-bearing rather than cosmetic.
    """
    return assign.groupBy("track_id").agg(
        F.min("event_time").alias("trk_start"),
        F.max("event_time").alias("trk_end"),
    )


def contingency(matched: DataFrame) -> DataFrame:
    """Sample counts per (track, ground-truth flight) pair."""
    return matched.groupBy("track_id", "flight_key").agg(F.count(F.lit(1)).alias("n"))


def _entropy(counts, total):
    h = 0.0
    for c in counts:
        if c > 0:
            p = c / total
            h -= p * math.log(p)
    return h


def vmeasure(matched: DataFrame) -> dict:
    """Homogeneity, completeness and their harmonic mean.

    The contingency table is small -- tracks times flights per airframe, not
    samples -- so it is collected to the driver and the entropies summed in
    Python. Spark computes the table; the driver computes three logs.
    """
    rows = contingency(matched).collect()
    total = sum(r["n"] for r in rows)
    if total == 0:
        return {"homogeneity": 0.0, "completeness": 0.0, "v_measure": 0.0}

    by_track, by_flight = {}, {}
    joint = {}
    for r in rows:
        by_track[r["track_id"]] = by_track.get(r["track_id"], 0) + r["n"]
        by_flight[r["flight_key"]] = by_flight.get(r["flight_key"], 0) + r["n"]
        joint[(r["track_id"], r["flight_key"])] = r["n"]

    h_flight = _entropy(by_flight.values(), total)
    h_track = _entropy(by_track.values(), total)

    # H(flight | track) and H(track | flight)
    h_f_given_t = 0.0
    h_t_given_f = 0.0
    for (t, f), n in joint.items():
        p = n / total
        h_f_given_t -= p * math.log(n / by_track[t])
        h_t_given_f -= p * math.log(n / by_flight[f])

    homogeneity = 1.0 if h_flight == 0 else 1.0 - h_f_given_t / h_flight
    completeness = 1.0 if h_track == 0 else 1.0 - h_t_given_f / h_track
    denom = homogeneity + completeness
    v = 0.0 if denom == 0 else 2 * homogeneity * completeness / denom
    return {"homogeneity": homogeneity, "completeness": completeness, "v_measure": v}


def match_rates(matched: DataFrame) -> dict:
    """The headline: what fraction of real flights became exactly one track.

    A flight is **cleanly matched** when it occupies exactly one track and that
    track carries exactly one flight. The three outcomes are mutually exclusive
    and sum to 100%:

    * **clean** -- one track, and that track is only this flight
    * **merged** -- the flight's dominant track also carries another flight
    * **fragmented** -- the flight spans several tracks, none of them merged

    A flight that is both merged and fragmented counts as **merged**, because a
    merge is the worse failure for ADEP/ADES: a fragment loses an endpoint, a
    merge invents one.

    This definition is deliberately strict -- a single stray sample in a second
    track costs a flight its clean match. The tolerant alternative (a dominant
    track covering some fraction of the flight) was rejected because the fraction
    would be a free parameter chosen after seeing the results, and because a
    stray fragment is exactly what fragments ADEP/ADES detection in production.
    The strict rate is a lower bound and is reported as one.
    """
    c = contingency(matched)
    per_flight = c.groupBy("flight_key").agg(
        F.sum("n").alias("f_n"),
        F.max("n").alias("best_n"),
        F.count(F.lit(1)).alias("n_tracks_for_flight"),
    )
    per_track = c.groupBy("track_id").agg(
        F.count(F.lit(1)).alias("n_flights_for_track")
    )
    # One row per flight: its dominant track, and how many flights that track holds.
    # A flight can be tied between several tracks with equal sample counts (e.g. an
    # exact 50/50 split). The tie-break must be deterministic -- a metric that can
    # change label between runs without the code changing defeats this repo's
    # provenance discipline -- and must favour a merged track over a pure one,
    # since that matches the documented "merge is the worse failure" priority.
    # per_track is joined in *before* breaking the tie, so the ordering can see
    # n_flights_for_track; track_id gives a total order for the remaining case of
    # a tie between two tracks that are equally (non-)merged.
    tie_break = Window.partitionBy("flight_key").orderBy(
        F.col("n_flights_for_track").desc(), F.col("track_id").asc()
    )
    best = (
        c.join(per_flight, "flight_key")
        .filter(F.col("n") == F.col("best_n"))
        .join(per_track, "track_id")
        .withColumn("_rank", F.row_number().over(tie_break))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    is_merged = F.col("n_flights_for_track") > 1
    is_fragmented = ~is_merged & (F.col("n_tracks_for_flight") > 1)
    is_clean = ~is_merged & (F.col("n_tracks_for_flight") == 1)

    agg = best.select(
        F.count(F.lit(1)).alias("n_flights"),
        F.sum(F.when(is_clean, 1).otherwise(0)).alias("clean"),
        F.sum(F.when(is_merged, 1).otherwise(0)).alias("merged"),
        F.sum(F.when(is_fragmented, 1).otherwise(0)).alias("fragmented"),
    ).first()

    n = agg["n_flights"] or 0
    pct = lambda x: 0.0 if n == 0 else 100.0 * (x or 0) / n  # noqa: E731
    return {
        "n_flights": n,
        "clean_match_pct": pct(agg["clean"]),
        "fragmented_pct": pct(agg["fragmented"]),
        "merged_pct": pct(agg["merged"]),
    }


def boundary_offsets(matched: DataFrame, extents: DataFrame) -> DataFrame:
    """One row per APDF-sourced ground-truth flight, with its signed offsets.

    The shared half of :func:`boundary_error` and
    ``benchmarks/track_diagnostics.py``'s ``boundary_histogram``: the sample
    selection, the dominant-track pick, the ``extents`` join and the signed
    subtraction -- everything except the aggregation on top.

    Extracted rather than copied. The percentiles and the histogram must not be
    able to disagree about which flights are in the sample, which track each
    one is measured against, or which way the sign points; a histogram built
    from a second implementation of this would look entirely plausible while
    describing a different population from the percentiles printed beside it,
    and nothing in either output would say so.

    Returns ``flight_key``, ``track_id``, ``trk_start``, ``off_s``, ``land_s``.
    Seconds, signed as ``trk_start - t_off`` and ``trk_end - t_land``: a
    *negative* ``off_s`` means the track starts **before** take-off and a
    *positive* ``land_s`` means it ends **after** landing. Both are the normal
    case -- :func:`boundary_error` argues that convention at length, and this
    docstring must not be read as restating it differently.

    ``trk_start`` is carried through so the caller can see an unmatched track
    directly rather than inferring it from a NULL offset.

    **The returned frame is cached and the caller must ``unpersist()`` it.**
    Both consumers scan it more than once; caching in each caller instead is
    precisely the duplication this function exists to remove.

    Raises ``ValueError`` when ``extents`` misses any ``track_id`` present in
    ``matched`` -- which can only mean the two came from different assignment
    tables.
    """
    apdf = matched.filter(F.col("t_source") == "apdf")
    # F.first on t_off/t_land assumes both are constant within a flight_key.
    # For t_off that is structural: flight_key is a hash *over* t_off, so two
    # rows with the same key cannot disagree about it. For t_land it is an
    # assumption, not a guarantee -- it rests on (aircraft, callsign, day, ADEP,
    # ADES, second-precision t_off) identifying at most one physical leg, which
    # is a statement about the world rather than about the hash. It is believed
    # safe and is not enforced here; if it were ever violated, F.first would pick
    # one t_land arbitrarily rather than fail.
    ends = apdf.groupBy("flight_key", "track_id").agg(
        F.first("t_off").alias("t_off"),
        F.first("t_land").alias("t_land"),
        F.count(F.lit(1)).alias("n"),
    )
    # One row per flight: its dominant track. Same tie-break as match_rates and
    # for the same reason -- an exact sample-count tie across two tracks must not
    # pick arbitrarily (and, before this fix, an unbroken tie on `n` kept *both*
    # rows, double-counting the flight into n_apdf_flights and every percentile).
    # There is no merge/pure distinction to prefer here, so the ordering is just
    # dominant sample count, then track_id for a total order.
    tie_break = Window.partitionBy("flight_key").orderBy(
        F.col("n").desc(), F.col("track_id").asc()
    )
    best = (
        ends.withColumn("_rank", F.row_number().over(tie_break))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
        # Left, deliberately, even though a miss is impossible in a correct
        # call: every track_id reaching here came from `matched`, which came
        # from an `assign` table -- and `extents` is built from that same
        # `assign` table (see track_methods.py:run_arm), so it must cover every
        # track_id here. A miss would mean extents were computed from something
        # other than this run's own assignment, and that must fail loudly.
        # An *inner* join is precisely the silent drop this comment used to
        # disclaim: the flight would vanish from n_apdf_flights and from every
        # percentile, leaving a plausible-looking CSV row and no error at all.
        # Left join plus an explicit check makes the behaviour match the claim.
        .join(extents, "track_id", "left")
        .select(
            "flight_key",
            "track_id",
            "trk_start",
            # Signed, per the convention in the docstring: trk_start - t_off
            # and trk_end - t_land. Not abs() -- the sign is the diagnostic.
            # abs() is applied by boundary_error alone, on top of this.
            (F.unix_timestamp("trk_start") - F.unix_timestamp("t_off")).alias("off_s"),
            (F.unix_timestamp("trk_end") - F.unix_timestamp("t_land")).alias("land_s"),
        )
    ).cache()

    missing = (
        best.filter(F.col("trk_start").isNull())
        .select("track_id")
        .distinct()
    )
    n_missing = missing.count()
    if n_missing:
        sample = [r["track_id"] for r in missing.limit(5).collect()]
        best.unpersist()
        raise ValueError(
            f"boundary_offsets: `extents` is missing {n_missing} track_id(s) that "
            f"`matched` contains, e.g. {sample}. extents must be computed from "
            "the same assignment table as matched -- see track_methods.run_arm."
        )
    return best


def boundary_error(matched: DataFrame, extents: DataFrame) -> dict:
    """Seconds between a track's *true* ends and the flight's ATOT/ALDT.

    Only over flights whose ``t_source`` is ``"apdf"``.

    **Both an absolute and a signed view are reported, and the signed one is
    the diagnostic.** The sign convention is ``trk_start - t_off`` for the
    departure side and ``trk_end - t_land`` for the arrival side, so a
    *negative* ``off`` means the track starts **before** take-off and a
    *positive* ``land`` means it ends **after** landing. Both of those are the
    normal, correct case: an OPDI track includes ground movement by design --
    the pipeline publishes ``entry-taxiway`` and ``entry-parking_position``
    events off these same tracks -- while ground truth's ``[t_off, t_land]``
    is airborne only, so a stand-to-stand track legitimately overhangs the
    interval on both sides.

    ``abs()`` destroys exactly the distinction that matters. A track starting
    20 min before ATOT (taxi-out, correct) and one starting 20 min after it
    (the departure was lost to some other track -- broken) produce the
    identical absolute number. On the 2025 sample the measured
    ``off_err_p50_s`` is 109 s: *not* the taxi-out inflation one might expect,
    because departure ADS-B coverage typically begins near the runway rather
    than at the stand -- but at 109 s absolute there is no way to tell which
    of the two populations above that median is made of, or in what mixture.
    The arrival side does carry real taxi-in (``land_err_p50_s`` = 374 s, a
    textbook European taxi-in).

    ``p10`` is reported alongside ``p50``/``p90`` because, once signed, the
    interesting tail is on *both* sides: a merge shows as a large negative
    ``off`` or a large positive ``land``, a lost departure as a large positive
    ``off``. The four absolute fields are kept exactly as they were so the
    already-published runs stay comparable -- the signed fields are an
    addition, not a replacement.

    **That restriction is deliberate conservatism, not a settled fact, and this
    module and ``track_truth.py`` must not be read as disagreeing.** The original
    reason was that NM-inferred endpoints carry the taxi-time inference's own
    error, and reporting a boundary accuracy that is mostly that error would
    measure the wrong thing. That error has since been measured (Task 4 Step 1;
    see ``benchmarks/DATASETS.md``, "Ground truth semantics"): ``AOBT_3 +
    TAXI_TIME_3`` matches real APDF ATOT with **median 0 s, IQR 17 s**, and
    ``ARVT_3`` matches real APDF ALDT with **median 0 s, IQR 25 s** -- roughly an
    order of magnitude under the ~120 s that would have forced the restriction.

    So the filter is kept only because *narrowing a claim after seeing the
    numbers is safe and widening it is not*. Widening it to every ``t_source``
    would raise the denominator from PRU-covered aerodromes to all of ECAC, and
    on the measured evidence that is probably right -- but it is a claim-scope
    decision for the study's author, and it is **open**. ``t_source`` travels
    with every row precisely so it can be reopened per airport.

    Do not "reconcile" the two modules by deleting either statement: the
    measurement in ``track_truth.py`` and the restriction here are both true.

    **``extents`` -- not ``matched`` -- supplies ``trk_start``/``trk_end``, and
    that is load-bearing, not a style choice.** An earlier version computed them
    as ``F.min``/``F.max("event_time")`` over ``matched`` itself. But every row
    in ``matched`` already satisfies ``t_off <= event_time <= t_land`` --
    ``track_truth.overlap_join`` filters on exactly that -- so ``trk_start`` and
    ``trk_end`` could *only* land inside ``[t_off, t_land]`` by construction.
    Three consequences, all silent: the error was one-sided (a track starting
    before ATOT or ending after ALDT was invisible); what it actually measured
    was the gap from the nearest in-interval sample to the edge, i.e. sampling
    cadence, not boundary accuracy; and worst, a track merging two flights --
    the failure this whole study exists to catch -- starts before one flight's
    ATOT and/or ends after another's ALDT, and would have scored near-zero error
    for it. ``extents`` is built by :func:`track_extents` from the *unfiltered*
    assignment table, so a track's real first/last sample can fall on either
    side of the interval, and a merge shows up as a large error rather than
    none. See ``tests/test_track_score.py`` for the fixture that makes this
    concrete: it is asserted to report ~5 s of error under the old computation
    (indistinguishable from noise) and the true ~1800 s under this one.

    **The sample selection, the dominant-track pick, the ``extents`` join and
    the signed subtraction all live in :func:`boundary_offsets`**, which
    ``benchmarks/track_diagnostics.py``'s ``boundary_histogram`` also calls.
    What remains here is the aggregation, and the fields it returns are
    unchanged by that extraction -- V1's published tables quote these numbers,
    so a change to any of them would be a change to a published result rather
    than a refactor.
    """
    best = boundary_offsets(matched, extents)

    def _q(delta: str, q: float, alias: str):
        return F.expr(f"percentile_approx({delta}, {q})").alias(alias)

    e = best.select(
        F.count(F.lit(1)).alias("n_apdf_flights"),
        _q("abs(off_s)", 0.5, "off_p50"),
        _q("abs(off_s)", 0.9, "off_p90"),
        _q("abs(land_s)", 0.5, "land_p50"),
        _q("abs(land_s)", 0.9, "land_p90"),
        _q("off_s", 0.1, "off_s_p10"),
        _q("off_s", 0.5, "off_s_p50"),
        _q("off_s", 0.9, "off_s_p90"),
        _q("land_s", 0.1, "land_s_p10"),
        _q("land_s", 0.5, "land_s_p50"),
        _q("land_s", 0.9, "land_s_p90"),
    ).first()
    best.unpersist()

    n = e["n_apdf_flights"] or 0
    # With no APDF-sourced rows every percentile is NULL. Coercing that to 0.0 --
    # which this did -- reports the *best possible* boundary error, so an arm
    # with no APDF coverage at all outscored every arm that was actually
    # measured. vmeasure and match_rates degrade to 0.0 meaning "bad" and are
    # safe; this one inverts, so it must return None and leave the CSV cell
    # blank rather than answer a question it has no data for.
    def _sec(v):
        return None if n == 0 or v is None else float(v)

    return {
        "n_apdf_flights": n,
        "off_err_p50_s": _sec(e["off_p50"]),
        "off_err_p90_s": _sec(e["off_p90"]),
        "land_err_p50_s": _sec(e["land_p50"]),
        "land_err_p90_s": _sec(e["land_p90"]),
        "off_signed_p10_s": _sec(e["off_s_p10"]),
        "off_signed_p50_s": _sec(e["off_s_p50"]),
        "off_signed_p90_s": _sec(e["off_s_p90"]),
        "land_signed_p10_s": _sec(e["land_s_p10"]),
        "land_signed_p50_s": _sec(e["land_s_p50"]),
        "land_signed_p90_s": _sec(e["land_s_p90"]),
    }


def score_arm(matched: DataFrame, extents: DataFrame, assign: DataFrame = None) -> dict:
    """Every metric for one arm, as one flat row ready for a CSV.

    ``extents`` is one row per track_id (``trk_start``, ``trk_end``) from
    :func:`track_extents`, over the *full* assignment table -- see
    :func:`boundary_error` for why it cannot be derived from ``matched``.

    ``assign`` is accepted and ignored. ``track_methods.run_arm`` passes the
    unfiltered assignment table to every scorer because some measurements are
    impossible without it; this one needs only what ``extents`` already
    carries, and takes the argument so the hook has one signature rather than
    two.

    Every value is a number except the four ``*_err_p*_s`` and six
    ``*_signed_p*_s`` fields, which are ``None`` when the arm's sample contains
    no APDF-sourced flight -- a blank cell, not a fictitious perfect score. See
    :func:`boundary_error`, which also states the signed fields' sign
    convention (negative ``off`` = track starts before take-off).
    """
    matched = matched.cache()
    row = {"n_tracks": matched.select("track_id").distinct().count()}
    row.update(vmeasure(matched))
    row.update(match_rates(matched))
    row.update(boundary_error(matched, extents))
    matched.unpersist()
    return row


def score_arm_gated(matched: DataFrame, extents: DataFrame,
                    matched_gate: DataFrame) -> dict:
    """The airborne row, plus every gate-to-gate rate under a ``gate_`` prefix.

    ``matched`` is ``track_truth.overlap_join``'s default output -- containment
    in ``[t_off, t_land]`` -- and ``matched_gate`` is that same join taken over
    ``("t_off_block", "t_in_block")``. Both are reported because they answer
    different questions and the paper needs both. The airborne metrics are what
    every V1 number was computed over and must stay comparable to; the gate
    metrics say whether the aircraft's time at the stand and on the taxiways
    ended up in the right track -- which the airborne interval cannot say at
    all, because it drops those samples before any metric sees them.

    Composition, not a second metric definition. The airborne half is
    :func:`score_arm`'s dict verbatim, so an arm scored through this function
    and an arm scored through ``score_arm`` carry the same numbers under the
    same names, and a CSV gains columns rather than changing them.

    Boundary error is computed once, from the airborne match. It is defined
    against ``t_off``/``t_land`` -- which ``overlap_join`` emits whichever
    interval decided membership -- so computing it a second time would produce
    two columns with the same name and different meanings. Only
    :func:`match_rates` is repeated, because only it is a statement about which
    samples landed in which track.
    """
    row = score_arm(matched, extents)
    row.update({f"gate_{k}": v for k, v in match_rates(matched_gate).items()})
    return row
