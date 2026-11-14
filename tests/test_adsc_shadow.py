"""ADS-C rows an ADS-B sample already covers are dropped."""

import datetime as dt

import pytest
from pyspark.sql import Row

from opdi.ingestion.osn_adsc import shadow_filter

T0 = dt.datetime(2026, 6, 5, 12, 0, 0)


def _adsb(spark, rows):
    return spark.createDataFrame([Row(icao24=i, event_time=t) for i, t in rows])


def _adsc(spark, rows):
    return spark.createDataFrame([Row(icao24=i, event_time=t, lat=52.0, lon=-5.0) for i, t in rows])


def test_a_row_inside_the_window_is_dropped(spark):
    kept = shadow_filter(
        _adsc(spark, [("4acb41", T0 + dt.timedelta(seconds=10))]),
        _adsb(spark, [("4acb41", T0)]),
        60,
        60,
    )
    assert kept.count() == 0


def test_a_row_outside_the_window_is_kept(spark):
    kept = shadow_filter(
        _adsc(spark, [("4acb41", T0 + dt.timedelta(seconds=600))]),
        _adsb(spark, [("4acb41", T0)]),
        60,
        60,
    )
    assert kept.count() == 1


def test_the_window_is_exact_at_both_edges(spark):
    # Exactly the half-window away is covered; one second beyond is not. An
    # off-by-a-bin implementation passes the two tests above and fails here.
    inside = shadow_filter(
        _adsc(spark, [("4acb41", T0 + dt.timedelta(seconds=60))]),
        _adsb(spark, [("4acb41", T0)]),
        60,
        60,
    )
    outside = shadow_filter(
        _adsc(spark, [("4acb41", T0 + dt.timedelta(seconds=61))]),
        _adsb(spark, [("4acb41", T0)]),
        60,
        60,
    )
    assert inside.count() == 0
    assert outside.count() == 1


def test_the_window_is_symmetric_by_default(spark):
    # An ADS-C report can precede the ADS-B sample that covers it: the
    # satellite link's latency is not a constant.
    kept = shadow_filter(
        _adsc(spark, [("4acb41", T0 - dt.timedelta(seconds=30))]),
        _adsb(spark, [("4acb41", T0)]),
        60,
        60,
    )
    assert kept.count() == 0


def test_another_airframe_does_not_shadow_this_one(spark):
    kept = shadow_filter(_adsc(spark, [("4acb41", T0)]), _adsb(spark, [("3c6568", T0)]), 60, 60)
    assert kept.count() == 1


def test_no_adsb_at_all_keeps_everything(spark):
    # The whole point of the merge: an airframe with no reception keeps every
    # ADS-C row it has.
    empty = _adsb(spark, [("x", T0)]).where("icao24 = 'nothing'")
    kept = shadow_filter(_adsc(spark, [("4acb41", T0)]), empty, 60, 60)
    assert kept.count() == 1


def test_the_window_can_be_asymmetric(spark):
    # The reason the two halves are separate parameters: an airframe whose
    # ADS-C clock runs 12.9 s slow has its true partner 12.9 s away on one
    # side only, and a window centred on zero either misses it or has to be
    # widened on both sides -- which discards ADS-C that is genuinely alone.
    #
    # ADS-B 30 s *behind* the report is covered by `before`, not by `after`.
    assert (
        shadow_filter(
            _adsc(spark, [("4acb41", T0 + dt.timedelta(seconds=30))]),
            _adsb(spark, [("4acb41", T0)]),
            60,
            5,
        ).count()
        == 0
    )
    assert (
        shadow_filter(
            _adsc(spark, [("4acb41", T0 + dt.timedelta(seconds=30))]),
            _adsb(spark, [("4acb41", T0)]),
            5,
            60,
        ).count()
        == 1
    )
    # And the mirror image.
    assert (
        shadow_filter(
            _adsc(spark, [("4acb41", T0 - dt.timedelta(seconds=30))]),
            _adsb(spark, [("4acb41", T0)]),
            60,
            5,
        ).count()
        == 1
    )
    assert (
        shadow_filter(
            _adsc(spark, [("4acb41", T0 - dt.timedelta(seconds=30))]),
            _adsb(spark, [("4acb41", T0)]),
            5,
            60,
        ).count()
        == 0
    )


def test_a_half_window_of_zero_is_refused(spark):
    with pytest.raises(ValueError, match="positive"):
        shadow_filter(_adsc(spark, [("4acb41", T0)]), _adsb(spark, [("4acb41", T0)]), 60, 0)


def test_output_columns_are_unchanged(spark):
    adsc = _adsc(spark, [("4acb41", T0 + dt.timedelta(seconds=600))])
    assert shadow_filter(adsc, _adsb(spark, [("4acb41", T0)]), 60, 60).columns == adsc.columns


def test_duplicate_adsb_rows_do_not_duplicate_the_output(spark):
    # An anti-join against a non-distinct key set is the classic way to turn a
    # filter into a fan-out.
    adsc = _adsc(spark, [("4acb41", T0 + dt.timedelta(seconds=600))])
    adsb = _adsb(spark, [("4acb41", T0), ("4acb41", T0), ("4acb41", T0)])
    assert shadow_filter(adsc, adsb, 60, 60).count() == 1


def test_a_partner_one_bin_away_is_still_found(spark):
    # The three-bin prefilter is what makes the exact test reachable. A report
    # at 59 s past a bin boundary and an ADS-B sample 2 s after it sit in
    # different bins, and a one-bin implementation would keep the report.
    base = dt.datetime(2026, 6, 5, 12, 0, 59)
    assert (
        shadow_filter(
            _adsc(spark, [("4acb41", base)]),
            _adsb(spark, [("4acb41", base + dt.timedelta(seconds=2))]),
            60,
            60,
        ).count()
        == 0
    )
