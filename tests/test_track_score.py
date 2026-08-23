"""Metric tests against cases whose answers are computable by hand.

A perfect segmentation scores 1. A totally merged one has completeness 1 and
homogeneity 0. A totally fragmented one has the reverse. If a metric does not do
that, it is not measuring what its name says.
"""

import datetime as dt

import pytest
from pyspark.sql import Row
from track_score import boundary_error, match_rates, score_arm, track_extents, vmeasure

_T0 = dt.datetime(2025, 6, 5, 8, 0, 0)


def _matched(spark, pairs, t_source="nm_inferred", t_off=_T0, t_land=_T0 + dt.timedelta(hours=1)):
    """pairs: list of (track_id, flight_key, samples).

    ``samples`` is either an int n -- n samples 10 s apart, starting at ``_T0``,
    the shape every original test uses -- or an explicit list of second-offsets
    from ``_T0``, for tests that need to pin exact ``event_time`` values (e.g.
    ``boundary_error``, where the gap to ``t_off``/``t_land`` is the thing under
    test). ``t_source``, ``t_off`` and ``t_land`` are shared across every row
    produced by one call; tests that need two flights with different truth
    windows or sources make two calls and ``union`` the results.
    """
    rows = []
    for track_id, flight_key, samples in pairs:
        offsets = range(0, samples * 10, 10) if isinstance(samples, int) else samples
        for off in offsets:
            rows.append(Row(
                icao24="abc123",
                event_time=_T0 + dt.timedelta(seconds=off),
                track_id=track_id,
                flight_key=flight_key,
                t_off=t_off,
                t_land=t_land,
                t_source=t_source,
            ))
    return spark.createDataFrame(rows)


def _extents_from(matched):
    """``track_extents`` over ``matched``'s own rows.

    For every test in this file except the boundary-fix ones below, ``matched``
    already contains every sample a track has -- there is no wider assignment
    it was clipped from -- so extents computed from it reproduce exactly what
    the pre-fix code computed inline from ``matched`` itself. Passing this is
    how the other tests stay numerically unchanged by the ``extents`` parameter.
    """
    return track_extents(matched.select("track_id", "event_time"))


def test_perfect_segmentation_scores_one(spark):
    m = _matched(spark, [("T1", "F1", 10), ("T2", "F2", 10)])
    v = vmeasure(m)
    assert v["homogeneity"] == pytest.approx(1.0)
    assert v["completeness"] == pytest.approx(1.0)
    assert v["v_measure"] == pytest.approx(1.0)


def test_total_merge_has_completeness_one_and_low_homogeneity(spark):
    """One track holding both flights: nothing is scattered, everything is impure."""
    m = _matched(spark, [("T1", "F1", 10), ("T1", "F2", 10)])
    v = vmeasure(m)
    assert v["completeness"] == pytest.approx(1.0)
    assert v["homogeneity"] == pytest.approx(0.0, abs=1e-9)


def test_total_fragmentation_has_homogeneity_one_and_low_completeness(spark):
    """One flight split across two tracks: every track is pure, nothing is whole."""
    m = _matched(spark, [("T1", "F1", 10), ("T2", "F1", 10)])
    v = vmeasure(m)
    assert v["homogeneity"] == pytest.approx(1.0)
    assert v["completeness"] == pytest.approx(0.0, abs=1e-9)


def test_clean_match_counts_only_one_to_one_flights(spark):
    m = _matched(spark, [
        ("T1", "F1", 10),                      # clean
        ("T2", "F2", 6), ("T3", "F2", 4),      # fragmented
        ("T4", "F3", 5), ("T4", "F4", 5),      # merged (two flights, one track)
    ])
    r = match_rates(m)
    assert r["n_flights"] == 4
    assert r["clean_match_pct"] == pytest.approx(25.0)
    assert r["fragmented_pct"] == pytest.approx(25.0)
    assert r["merged_pct"] == pytest.approx(50.0)


def test_a_dominant_track_with_a_stray_fragment_is_not_clean(spark):
    """The strict definition: one stray sample costs the clean match.

    T1 holds 9 of 10 samples, which any tolerant threshold would accept. It is
    still fragmented, because the stray sample is a second track and that is what
    breaks endpoint-based ADEP/ADES in production.
    """
    m = _matched(spark, [("T1", "F1", 9), ("T2", "F1", 1)])
    r = match_rates(m)
    assert r["clean_match_pct"] == pytest.approx(0.0)
    assert r["fragmented_pct"] == pytest.approx(100.0)


# ``test_the_three_outcomes_are_mutually_exclusive_and_sum_to_100`` used to live
# here. It was deleted: it ran the same fixture as
# ``test_clean_match_counts_only_one_to_one_flights``, which already asserts
# 25/25/50 exactly, so summing those three to 100 was arithmetic on numbers the
# neighbouring test had already pinned. The case it was *meant* to catch -- a
# flight that is both merged and fragmented, which an unguarded implementation
# counts twice -- is covered for real by
# ``test_a_flight_both_merged_and_fragmented_counts_as_merged``.


BOUNDARY_FIELDS = (
    "off_err_p50_s", "off_err_p90_s", "land_err_p50_s", "land_err_p90_s",
    "off_signed_p10_s", "off_signed_p50_s", "off_signed_p90_s",
    "land_signed_p10_s", "land_signed_p50_s", "land_signed_p90_s",
)


def test_score_arm_returns_a_flat_row_of_scalars(spark):
    """Every field is a number -- except the boundary errors with no APDF data.

    This fixture is ``t_source="nm_inferred"``, which ``boundary_error`` filters
    out entirely, so it has no APDF flight at all. The four boundary fields must
    therefore be ``None``: a blank CSV cell, not a number. An earlier version of
    this assertion (``isinstance(v, (int, float))`` over every value) walked this
    exact path and blessed the bug it should have caught.
    """
    m = _matched(spark, [("T1", "F1", 10)])
    row = score_arm(m, _extents_from(m))
    assert set(row) >= {"v_measure", "homogeneity", "completeness",
                        "clean_match_pct", "fragmented_pct", "merged_pct",
                        "n_flights", "n_tracks"}
    assert row["n_apdf_flights"] == 0
    for f in BOUNDARY_FIELDS:
        assert row[f] is None, f
    numeric = {k: v for k, v in row.items() if k not in BOUNDARY_FIELDS}
    assert all(isinstance(v, (int, float)) for v in numeric.values())


def test_boundary_error_on_an_empty_apdf_sample_is_none_not_zero(spark):
    """A missing measurement must not report the best possible score.

    ``float(e["off_p50"] or 0)`` coerced a NULL percentile to ``0.0`` -- and for
    boundary error, 0.0 seconds is *perfect*. An arm with no APDF coverage at all
    therefore outscored every arm that had actually been measured, which is the
    one direction in which a degenerate default is dangerous. ``vmeasure`` and
    ``match_rates`` degrade to 0.0 meaning "bad", so they are safe; this one
    inverts, so it returns None.
    """
    m = _matched(spark, [("T1", "F1", 10)], t_source="nm_inferred")
    e = boundary_error(m, _extents_from(m))
    assert e["n_apdf_flights"] == 0
    for f in BOUNDARY_FIELDS:
        assert e[f] is None, f


def test_boundary_error_measures_the_gap_to_apdf_truth(spark):
    """The actual percentile_approx arithmetic, exercised for real.

    Every other test uses ``t_source="nm_inferred"``, which ``boundary_error``
    filters out entirely -- so nothing else in this file touches the gap
    computation. Two flights, offsets pinned by hand:

    * F1 on T1: starts 30 s after t_off, ends 45 s before t_land.
    * F2 on T2: starts 90 s after t_off, ends 100 s before t_land.

    Spark's ``percentile_approx`` is nearest-rank, not interpolated: verified
    directly against a 2-element frame, p50 returns the smaller value and p90
    the larger (rank = ceil(p * n), so rank 1 and rank 2 of 2). That makes
    both quantiles hand-computable here as the min/max of the two flights'
    errors, not an average.
    """
    m = _matched(
        spark,
        [("T1", "F1", [30, 3555]), ("T2", "F2", [90, 3500])],
        t_source="apdf",
    )
    e = boundary_error(m, _extents_from(m))
    assert e["n_apdf_flights"] == 2
    assert e["off_err_p50_s"] == pytest.approx(30.0)
    assert e["off_err_p90_s"] == pytest.approx(90.0)
    assert e["land_err_p50_s"] == pytest.approx(45.0)
    assert e["land_err_p90_s"] == pytest.approx(100.0)


def test_boundary_error_reports_the_sign_of_each_overhang(spark):
    """The signed fields, and the direction they point.

    ``abs()`` cannot tell a track that starts *before* take-off -- the normal
    case, an OPDI track includes taxi -- from one that starts *after* it, which
    means the departure was lost to another track. The signed convention is
    ``trk_start - t_off`` and ``trk_end - t_land``, so the normal case is
    negative ``off`` and positive ``land``.

    T1 is the normal shape: it starts 300 s before ATOT and ends 600 s after
    ALDT (taxi-out, taxi-in). T2 is the broken shape: it starts 120 s *after*
    ATOT and ends 200 s *before* ALDT. Both report the same kind of absolute
    number; only the sign separates them, and with p10/p50/p90 over the two
    flights the two populations sit at opposite ends of the distribution.
    """
    m = _matched(spark, [("T1", "F1", [5]), ("T2", "F2", [5])], t_source="apdf")
    extents = spark.createDataFrame([
        Row(track_id="T1",
            trk_start=_T0 - dt.timedelta(seconds=300),
            trk_end=_T0 + dt.timedelta(hours=1) + dt.timedelta(seconds=600)),
        Row(track_id="T2",
            trk_start=_T0 + dt.timedelta(seconds=120),
            trk_end=_T0 + dt.timedelta(hours=1) - dt.timedelta(seconds=200)),
    ])
    e = boundary_error(m, extents)
    assert e["n_apdf_flights"] == 2
    # nearest-rank percentile_approx over two values: p10/p50 -> smaller,
    # p90 -> larger.
    assert e["off_signed_p10_s"] == pytest.approx(-300.0)
    assert e["off_signed_p50_s"] == pytest.approx(-300.0)
    assert e["off_signed_p90_s"] == pytest.approx(120.0)
    assert e["land_signed_p10_s"] == pytest.approx(-200.0)
    assert e["land_signed_p50_s"] == pytest.approx(-200.0)
    assert e["land_signed_p90_s"] == pytest.approx(600.0)
    # The absolute fields are unchanged in meaning, and cannot see the sign:
    # both flights' departure overhangs are positive numbers there.
    assert e["off_err_p90_s"] == pytest.approx(300.0)


def test_boundary_error_raises_when_extents_misses_a_track(spark):
    """The fail-loud contract the join's own comment states.

    An inner join here would silently drop the flight from ``n_apdf_flights``
    and from every percentile, producing a plausible CSV row and no error --
    exactly what the comment claimed to prevent. ``extents`` covering only T1
    while ``matched`` also holds T2 must raise, and the message must say how
    many track_ids were missing.
    """
    m = _matched(spark, [("T1", "F1", 5), ("T2", "F2", 5)], t_source="apdf")
    partial = _extents_from(m).filter("track_id = 'T1'")
    with pytest.raises(ValueError, match="missing 1 track_id"):
        boundary_error(m, partial)


def test_boundary_error_breaks_an_exact_track_tie_deterministically(spark):
    """An APDF flight split exactly evenly across two tracks must be counted once.

    F1 has 2 samples on each of two tracks -- an exact tie on ``n``, the count
    ``boundary_error`` uses to pick a flight's dominant track. Before the fix,
    ``best = ends.join(w, "flight_key").filter(F.col("n") == F.col("best_n"))``
    had no tie-break at all: with two rows tied for ``best_n``, both survived
    the filter, so the flight was double-counted into ``n_apdf_flights`` and
    both tracks' errors leaked into every percentile.

    Track ids are chosen so the (correct) tie-break -- dominant ``n``, then
    ``track_id`` ascending -- picks ``Ta``. ``Ta``'s and ``Tb``'s errors are
    chosen apart on purpose (200 s/400 s vs 50 s/50 s) so that, if both rows
    survived, the percentiles would land on ``Tb``'s smaller values instead --
    not just double the count, but silently answer with the wrong track.
    """
    m = _matched(
        spark,
        [("Ta", "F1", [200, 3200]), ("Tb", "F1", [50, 3550])],
        t_source="apdf",
    )
    e = boundary_error(m, _extents_from(m))
    assert e["n_apdf_flights"] == 1
    assert e["off_err_p50_s"] == pytest.approx(200.0)
    assert e["off_err_p90_s"] == pytest.approx(200.0)
    assert e["land_err_p50_s"] == pytest.approx(400.0)
    assert e["land_err_p90_s"] == pytest.approx(400.0)


def test_boundary_error_sees_a_merge_that_extends_past_the_interval(spark):
    """The bug this module's ``extents`` parameter exists to fix.

    ``overlap_join`` restricts ``matched`` to samples already inside
    ``[t_off, t_land]`` -- that is what an inner join on containment means. A
    track's *real* first/last sample is not so restricted: a track merging two
    flights starts before one flight's ATOT and/or ends after another's ALDT,
    and that overhang is exactly what a boundary-error metric must be able to
    see, because merging is the failure this whole study exists to catch.

    ``matched`` here holds only two in-interval samples (5 s inside each edge)
    -- as ``overlap_join`` would actually produce for a merged track, since it
    physically cannot pass through samples outside the interval. Measured with
    ``_extents_from(m)`` -- the pre-fix computation, ``trk_start``/``trk_end``
    taken from ``matched`` itself -- the error is ~5 s: indistinguishable from
    ordinary sampling noise, and this assertion is run first specifically to
    prove the old computation really did produce that number here, not assumed.
    Measured with ``true_extents`` -- the track's real span, built the way
    ``track_methods.py`` builds it in production, from the *full* assignment
    table rather than from ``matched`` -- the same track reports the real
    ~1800 s overhang on both sides.
    """
    m = _matched(
        spark,
        [("TMERGED", "F1", [5, 3595])],
        t_source="apdf",
    )

    pre_fix = boundary_error(m, _extents_from(m))
    assert pre_fix["off_err_p50_s"] == pytest.approx(5.0)
    assert pre_fix["land_err_p50_s"] == pytest.approx(5.0)

    true_extents = spark.createDataFrame([
        Row(track_id="TMERGED",
            trk_start=_T0 - dt.timedelta(seconds=1800),
            trk_end=_T0 + dt.timedelta(hours=1) + dt.timedelta(seconds=1800)),
    ])
    fixed = boundary_error(m, true_extents)
    assert fixed["n_apdf_flights"] == 1
    assert fixed["off_err_p50_s"] == pytest.approx(1800.0)
    assert fixed["off_err_p90_s"] == pytest.approx(1800.0)
    assert fixed["land_err_p50_s"] == pytest.approx(1800.0)
    assert fixed["land_err_p90_s"] == pytest.approx(1800.0)


def test_a_flight_both_merged_and_fragmented_counts_as_merged(spark):
    """The priority rule the module's docstring exists to state.

    F5 genuinely spans two tracks (7 samples on T5a, 3 on T5b) -- fragmented by
    itself. But its dominant track, T5a, also carries F6 -- merged. The rule is
    that merged wins: a fragment loses an endpoint, a merge invents one.

    Without the ``~is_merged`` guard on ``is_fragmented``, F5 would be counted
    in *both* the merged and the fragmented sums, and the three percentages
    would sum to more than 100 -- exactly the failure mode
    ``test_the_three_outcomes_are_mutually_exclusive_and_sum_to_100`` was
    meant to catch, except that test's fixture never actually builds a flight
    satisfying both conditions at once.
    """
    m = _matched(spark, [
        ("T5a", "F5", 7), ("T5b", "F5", 3),   # F5: fragmented AND (via T5a) merged
        ("T5a", "F6", 5),                      # F6: shares T5a with F5 -- merged
        ("Tc", "Fc", 5),                       # control: clean
    ])
    r = match_rates(m)
    assert r["n_flights"] == 3
    assert r["merged_pct"] == pytest.approx(200.0 / 3.0)
    assert r["fragmented_pct"] == pytest.approx(0.0)
    assert r["clean_match_pct"] == pytest.approx(100.0 / 3.0)
    total = r["clean_match_pct"] + r["fragmented_pct"] + r["merged_pct"]
    assert total == pytest.approx(100.0)


def test_tied_dominant_tracks_break_deterministically_toward_merged(spark):
    """A flight split exactly 50/50 must not have its label depend on row order.

    F7 has 5 samples on each of two tracks -- an exact tie for "dominant
    track". One tied track (T9zulu) also carries F8, so it is merged; the
    other (T9alpha) carries only F7, so it is pure. T9alpha sorts first
    alphabetically, so a tie-break on track_id alone would (wrongly) pick the
    pure track and call F7 clean or fragmented. The tie-break must prefer the
    merged track first and use track_id only to order the remainder, so F7 is
    merged, deterministically, on every run.

    Row order matters for the bug this guards against: a naive tie-break
    (``dropDuplicates`` with no explicit ordering) picks whichever row its
    internal hash aggregate happens to build first, which tracks *insertion*
    order, not track_id or merge status -- feeding T9zulu's rows before
    T9alpha's picks the merged track by accident even without a real
    ordering rule; feeding T9alpha's F7 rows last (as below) flips that same
    unordered code to the wrong track. The fixed implementation must get
    this right regardless of row order, because production data has no
    controllable order at all.
    """
    m = _matched(spark, [
        ("T9zulu", "F7", 5),
        ("T9zulu", "F8", 5),
        ("T9alpha", "F7", 5),
    ])
    r1 = match_rates(m)
    r2 = match_rates(m)
    assert r1 == r2
    assert r1["n_flights"] == 2
    assert r1["merged_pct"] == pytest.approx(100.0)
    assert r1["fragmented_pct"] == pytest.approx(0.0)
    assert r1["clean_match_pct"] == pytest.approx(0.0)
