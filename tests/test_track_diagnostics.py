"""Tests for the two V1 diagnostics: the containment census and the histogram.

Both answer a reviewer with a number, so both are tested against cases whose
answer is computable by hand -- a diagnostic that is merely plausible is worth
nothing, because there is no second source to check it against.

The histogram tests pin ``trk_start``/``trk_end`` explicitly rather than deriving
them from sample times: the offset to ``t_off``/``t_land`` is the thing under
test, and a fixture that computes it the same way the code does would pass
whatever the code did.
"""

import datetime as dt

import pytest
from pyspark.sql import Row
from track_diagnostics import boundary_histogram, census_ground_truth, containment_census
from track_score import boundary_error

#: A three-day window, the shape both real periods have. Exclusive upper bound
#: at the midnight after the last sampled day, exactly as
#: ``track_truth._sample_window`` returns it.
WS = "2025-06-05 00:00:00"
WE = "2025-06-08 00:00:00"

_T0 = dt.datetime(2025, 6, 5, 8, 0, 0)


def _gt(spark, flights):
    """flights: list of (flight_key, t_off, t_land[, t_source])."""
    rows = []
    for f in flights:
        key, t_off, t_land = f[0], f[1], f[2]
        rows.append(Row(
            flight_key=key,
            icao24="abc123",
            callsign="TST1",
            gt_adep="EBBR",
            gt_ades="LFPG",
            t_off=t_off,
            t_land=t_land,
            t_source=f[3] if len(f) > 3 else "apdf",
            day=t_off.date(),
        ))
    return spark.createDataFrame(rows)


def _hour(day, h, m=0):
    return dt.datetime(2025, 6, day, h, m, 0)


# --------------------------------------------------------------------------
# containment census
# --------------------------------------------------------------------------

def test_census_splits_kept_from_clipped_at_each_edge(spark):
    """One flight of each kind, so every counter is checkable by hand."""
    gt = _gt(spark, [
        ("F_in", _hour(5, 8), _hour(5, 10)),        # wholly inside
        ("F_cs", _hour(4, 23), _hour(5, 1)),        # departed before the window
        ("F_ce", _hour(7, 23), _hour(8, 1)),        # lands after the window
    ])
    c = containment_census(gt, WS, WE)
    assert c["n_gt_flights"] == 3
    assert c["n_wholly_inside"] == 1
    assert c["n_clipped_start"] == 1
    assert c["n_clipped_end"] == 1
    assert c["pct_kept"] == pytest.approx(33.33)
    # Both clipped flights are two hours long with one hour inside the window.
    assert c["median_observed_fraction_clipped"] == pytest.approx(0.5)


def test_census_reads_string_and_datetime_bounds_identically(spark):
    """The bounds arrive as strings from ``_sample_window``; datetimes must work too.

    Not a style point. Casting each literal at point of use -- ``F.lit(ws).cast(
    "long")`` -- is a string-to-BIGINT cast, which raises under ANSI mode and
    returns NULL without it: the job either dies or reports a blank
    observed-fraction with nothing saying why.
    """
    gt = _gt(spark, [
        ("F_in", _hour(5, 8), _hour(5, 10)),
        ("F_cs", _hour(4, 23), _hour(5, 1)),
    ])
    as_str = containment_census(gt, WS, WE)
    as_dt = containment_census(
        gt, dt.datetime(2025, 6, 5), dt.datetime(2025, 6, 8)
    )
    assert as_str == as_dt
    assert as_str["median_observed_fraction_clipped"] == pytest.approx(0.5)


def test_census_treats_the_upper_bound_as_exclusive(spark):
    """A flight landing exactly at the closing midnight is clipped, not kept.

    ``load_flight_intervals`` keeps a flight on ``t_land < we``. The census has
    to apply the identical predicate or it reports on a rule the study does not
    use.
    """
    gt = _gt(spark, [("F_edge", _hour(7, 22), _hour(8, 0))])
    c = containment_census(gt, WS, WE)
    assert c["n_wholly_inside"] == 0
    assert c["n_clipped_end"] == 1


def test_census_on_an_all_inside_sample_keeps_everything(spark):
    gt = _gt(spark, [
        ("F1", _hour(5, 8), _hour(5, 10)),
        ("F2", _hour(6, 8), _hour(6, 12)),
    ])
    c = containment_census(gt, WS, WE)
    assert c["pct_kept"] == pytest.approx(100.0)
    assert c["n_clipped_start"] == c["n_clipped_end"] == 0
    # No excluded flight, so there is no fraction to report -- a blank cell,
    # not a fabricated 1.0.
    assert c["median_observed_fraction_clipped"] is None


def test_census_ground_truth_refuses_to_pad_into_an_unloaded_month():
    """Padding across a month edge without its parquet would report zero clipped.

    The failure is silent by construction -- flights that were never loaded
    cannot be counted as excluded -- so it has to be an error rather than a
    number.
    """
    with pytest.raises(SystemExit, match="202505"):
        census_ground_truth(None, ["202506"], ["2025-06-01", "2025-06-02"])


# --------------------------------------------------------------------------
# boundary histogram
# --------------------------------------------------------------------------

def _matched_and_extents(spark, flights):
    """flights: list of (flight_key, track_id, off_s, land_s[, t_source]).

    ``off_s``/``land_s`` are the signed offsets the fixture wants to see come
    back out, applied to ``trk_start``/``trk_end`` directly.
    """
    m_rows, e_rows = [], []
    for f in flights:
        key, tid, off_s, land_s = f[0], f[1], f[2], f[3]
        src = f[4] if len(f) > 4 else "apdf"
        t_off, t_land = _T0, _T0 + dt.timedelta(hours=2)
        for i in range(3):
            m_rows.append(Row(
                icao24="abc123",
                event_time=t_off + dt.timedelta(minutes=10 * i + 1),
                track_id=tid,
                flight_key=key,
                t_off=t_off,
                t_land=t_land,
                t_source=src,
            ))
        e_rows.append(Row(
            track_id=tid,
            trk_start=t_off + dt.timedelta(seconds=off_s),
            trk_end=t_land + dt.timedelta(seconds=land_s),
        ))
    return spark.createDataFrame(m_rows), spark.createDataFrame(e_rows)


def _bin_of(rows, edge, value):
    """The single bin holding *value*, from the returned grid."""
    hit = [r for r in rows if r["edge"] == edge and r["n"]]
    assert len(hit) == 1, f"expected one non-empty {edge} bin, got {hit}"
    r = hit[0]
    assert r["bin_lower_s"] <= value <= r["bin_upper_s"]
    return r


def test_histogram_keeps_the_sign_of_each_overhang(spark):
    """A track starting before take-off must land in a NEGATIVE bin.

    This is the whole reason the histogram exists rather than an absolute one:
    a track starting 90 s before wheels-off is a correct track with taxi-out in
    it, and one starting 90 s after has lost part of its own flight. Inverting
    the convention inverts the reader's diagnosis.
    """
    m, e = _matched_and_extents(spark, [("F1", "T1", -90, 400)])
    rows = boundary_histogram(m, e)
    off = _bin_of(rows, "off", -90)
    assert (off["bin_lower_s"], off["bin_upper_s"]) == (-90, -60)
    land = _bin_of(rows, "land", 400)
    assert (land["bin_lower_s"], land["bin_upper_s"]) == (390, 420)


def test_histogram_clamps_a_far_tail_into_the_end_bin(spark):
    """Beyond the span, counts stay in the end bin rather than disappearing.

    Dropping them would make the histogram sum to less than the sample it
    claims to describe, and the two failures -- a thin tail and a truncated one
    -- look identical on the page.
    """
    m, e = _matched_and_extents(spark, [("F1", "T1", -100_000, 99_999)])
    rows = boundary_histogram(m, e)
    off = _bin_of(rows, "off", -1800)
    assert (off["bin_lower_s"], off["bin_upper_s"]) == (-1800, -1770)
    land = _bin_of(rows, "land", 1800)
    assert (land["bin_lower_s"], land["bin_upper_s"]) == (1770, 1800)
    for edge in ("off", "land"):
        assert sum(r["n"] for r in rows if r["edge"] == edge) == 1


def test_histogram_returns_a_complete_grid_including_empty_bins(spark):
    m, e = _matched_and_extents(spark, [("F1", "T1", 0, 0)])
    rows = boundary_histogram(m, e, bin_seconds=30, span_seconds=1800)
    assert len(rows) == 2 * 2 * 1800 // 30
    for edge in ("off", "land"):
        lowers = [r["bin_lower_s"] for r in rows if r["edge"] == edge]
        assert lowers == sorted(lowers)
        assert len(set(lowers)) == len(lowers)
        assert min(lowers) == -1800 and max(lowers) == 1770


def test_histogram_covers_the_same_sample_as_the_percentiles(spark):
    """The histogram and ``boundary_error`` must describe one population.

    They share ``boundary_offsets`` precisely so this holds; the test is the
    regression that catches the two drifting apart -- including the APDF
    restriction and the dominant-track pick, both of which change the
    denominator without changing the shape.
    """
    m, e = _matched_and_extents(spark, [
        ("F1", "T1", -90, 400),
        ("F2", "T2", 30, 120),
        ("F3", "T3", 10, 10, "nm_inferred"),   # excluded from both
    ])
    rows = boundary_histogram(m, e)
    n = boundary_error(m, e)["n_apdf_flights"]
    assert n == 2
    for edge in ("off", "land"):
        assert sum(r["n"] for r in rows if r["edge"] == edge) == n


def test_histogram_refuses_a_span_that_is_not_a_whole_number_of_bins(spark):
    m, e = _matched_and_extents(spark, [("F1", "T1", 0, 0)])
    with pytest.raises(ValueError, match="whole number of"):
        boundary_histogram(m, e, bin_seconds=7, span_seconds=1800)
