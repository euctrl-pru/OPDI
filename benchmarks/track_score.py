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

__all__ = ["contingency", "vmeasure", "match_rates", "boundary_error", "score_arm"]


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


def boundary_error(matched: DataFrame) -> dict:
    """Seconds between a track's ends and the flight's ATOT/ALDT.

    Only over flights whose ``t_source`` is ``"apdf"``. NM-inferred endpoints
    carry the taxi-time inference's own error, and reporting a boundary accuracy
    that is mostly that error would be a measurement of the wrong thing.
    """
    apdf = matched.filter(F.col("t_source") == "apdf")
    ends = apdf.groupBy("flight_key", "track_id").agg(
        F.min("event_time").alias("trk_start"),
        F.max("event_time").alias("trk_end"),
        F.first("t_off").alias("t_off"),
        F.first("t_land").alias("t_land"),
        F.count(F.lit(1)).alias("n"),
    )
    w = ends.groupBy("flight_key").agg(F.max("n").alias("best_n"))
    best = ends.join(w, "flight_key").filter(F.col("n") == F.col("best_n"))

    e = best.select(
        F.count(F.lit(1)).alias("n_apdf_flights"),
        F.expr(
            "percentile_approx(abs(unix_timestamp(trk_start) - unix_timestamp(t_off)), 0.5)"
        ).alias("off_p50"),
        F.expr(
            "percentile_approx(abs(unix_timestamp(trk_start) - unix_timestamp(t_off)), 0.9)"
        ).alias("off_p90"),
        F.expr(
            "percentile_approx(abs(unix_timestamp(trk_end) - unix_timestamp(t_land)), 0.5)"
        ).alias("land_p50"),
        F.expr(
            "percentile_approx(abs(unix_timestamp(trk_end) - unix_timestamp(t_land)), 0.9)"
        ).alias("land_p90"),
    ).first()

    return {
        "n_apdf_flights": e["n_apdf_flights"] or 0,
        "off_err_p50_s": float(e["off_p50"] or 0),
        "off_err_p90_s": float(e["off_p90"] or 0),
        "land_err_p50_s": float(e["land_p50"] or 0),
        "land_err_p90_s": float(e["land_p90"] or 0),
    }


def score_arm(matched: DataFrame) -> dict:
    """Every metric for one arm, as one flat row ready for a CSV."""
    matched = matched.cache()
    row = {"n_tracks": matched.select("track_id").distinct().count()}
    row.update(vmeasure(matched))
    row.update(match_rates(matched))
    row.update(boundary_error(matched))
    matched.unpersist()
    return row
