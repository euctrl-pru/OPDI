"""A10 `cruise_linked`: one track_id across an en-route reception hole.

A10 is A9 `debounced` with one change: a gap over ``gap_minutes`` breaks only when
the two edges cannot be one flight. A wrongly glued flight is worse than a split
one, so unclear evidence means split. Later study stages score the link decision
directly, which is why it is a pure function of named edge features and why some
tests below drive ``linkable`` with literals.
"""

import dataclasses
import math
import re

import pytest
from conftest import make_track
from pyspark.sql import Window
from pyspark.sql import functions as F

# The study rules the default rule is compared with live with the study scripts.
from study_variants.segmentation import debounced, recommended  # noqa: E402

from opdi.config import SegmentationConfig
from opdi.pipeline.flights import EARTH_R_NM, haversine_nm
from opdi.pipeline.segmentation import SegmentationParams, assign_track_id
from opdi.pipeline.segmentation.base import (
    FPM_PER_MPS,
    FT_PER_M,
    KT_PER_MPS,
    segment_window,
)
from opdi.pipeline.segmentation.methods import (
    ARMS,
    LINK_EDGE_SAMPLES,
    PLACEHOLDER_ICAO24,
    cruise_linked,
    debounced_callsign_change,
    link_checks,
    link_feature_columns,
    linkable,
    select_link_features,
    stable_callsign,
)

#: The rule's behaviour tests below were written at the pre-study settings, so they
#: pass those explicitly; ``test_defaults_are_on_both_config_classes`` pins the shipped
#: ones. A test that means the shipped defaults uses ``SHIPPED``.
P = SegmentationParams(
    link_cruise_floor_ft=25000.0,
    link_min_implied_speed_kt=315.0,
    link_max_gap_minutes=360.0,
    link_old_edge_climb=False,
    link_new_edge_descent=False,
)
SHIPPED = SegmentationParams()
_BOTH_DIRECTIONS = {"link_old_edge_climb": True, "link_new_edge_descent": True}
_DIRECTIONAL = dataclasses.replace(P, **_BOTH_DIRECTIONS)
_CLIMB_ONLY = dataclasses.replace(P, link_old_edge_climb=True)
_DESCENT_ONLY = dataclasses.replace(P, link_new_edge_descent=True)

CRUISE_M = 11000.0
EDGE_MPS = 480.0 / KT_PER_MPS


def _lon(nm):
    """Longitude span on the equator covering *nm* nautical miles."""
    return math.degrees(nm / EARTH_R_NM)


def _piece(start_s, lon, n=10, step_s=5, **overrides):
    """A level cruise piece: *n* samples from *start_s*, at one position."""
    base = {
        "lat": 0.0,
        "lon": lon,
        "baro_altitude": CRUISE_M,
        "velocity": EDGE_MPS,
        "vert_rate": 0.0,
        "callsign": "DLH1",
    }
    return [{**base, **overrides, "t": start_s + i * step_s} for i in range(n)]


def _hole_samples(gap_min=191.0, nm=1530.0, before=None, after=None, n_before=10, n_after=10):
    """Two pieces of one airframe with a hole between their edges at t=0 and t=gap."""
    return _piece(-(n_before - 1) * 5, 0.0, n_before, **(before or {})) + _piece(
        gap_min * 60, _lon(nm), n_after, **(after or {})
    )


def _hole(spark, **kwargs):
    return make_track(spark, _hole_samples(**kwargs))


def n_tracks(df, p=P):
    out = assign_track_id(df, cruise_linked(), p)
    return out.select("track_id").distinct().count()


def _partition(df, rule, p=P):
    """Which samples share a track, by ``last_contact`` -- make_track's sample offset."""
    rows = (
        assign_track_id(df, rule, p)
        .groupBy("track_id")
        .agg(F.collect_set("last_contact").alias("s"))
        .collect()
    )
    return {frozenset(r["s"]) for r in rows}


def _engine_frame(df):
    """The columns ``assign_track_id`` hands a break expression (the arm contract)."""
    return (
        df.withColumn("_ts", F.to_timestamp("event_time"))
        .withColumn("_grp", F.col("icao24"))
        .withColumn("_alt_ft", F.col("baro_altitude") * FT_PER_M)
        .withColumn("_spd_kt", F.col("velocity") * KT_PER_MPS)
        .withColumn(
            "_gap_minutes",
            (
                F.unix_timestamp("_ts")
                - F.unix_timestamp(F.lag("_ts").over(Window.partitionBy("_grp").orderBy("_ts")))
            )
            / 60.0,
        )
    )


def _with_features(df, **kwargs):
    frame = _engine_frame(df)
    features = link_feature_columns(P, **kwargs)
    return frame.select("event_time", *[c.alias(name) for name, c in features.items()])


# --- the rule, on tracks ----------------------------------------------------


def test_a_transatlantic_hole_is_one_track(spark):
    df = _hole(spark)
    assert n_tracks(df) == 1
    assert assign_track_id(df, debounced(), P).select("track_id").distinct().count() == 2


def test_the_second_hole_is_judged_on_its_own(spark):
    """Three pieces. The first hole links; the second is too slow, and splits."""
    samples = _hole_samples()
    gap2_start = 191 * 60 + 45 + 100 * 60
    samples += _piece(gap2_start, _lon(1530.0 + 100.0))  # 100 NM in 100 min: 60 kt
    df = make_track(spark, samples)

    partition = _partition(df, cruise_linked())
    assert len(partition) == 2
    assert {len(s) for s in partition} == {20, 10}


def test_a_low_edge_splits(spark):
    assert n_tracks(_hole(spark, after={"baro_altitude": 3000.0 / FT_PER_M})) == 2


def test_an_edge_below_the_cruise_floor_splits_though_it_is_not_low(spark):
    """20,000 ft clears `low_altitude_gap`, so only the cruise floor can split it."""
    fl200 = 20000.0 / FT_PER_M
    assert n_tracks(_hole(spark, before={"baro_altitude": fl200})) == 2
    assert n_tracks(_hole(spark, after={"baro_altitude": fl200})) == 2


def test_the_cruise_floor_looks_at_exactly_five_samples_each_side(spark):
    fl200 = 20000.0 / FT_PER_M

    def with_one_low(index):
        samples = _hole_samples()
        samples[index]["baro_altitude"] = fl200
        return make_track(spark, samples)

    # Samples 0-9 are the old piece (edge at 9), samples 10-19 the new (edge at 10).
    assert LINK_EDGE_SAMPLES == 5
    assert n_tracks(with_one_low(5)) == 2  # fifth before the gap
    assert n_tracks(with_one_low(4)) == 1  # sixth before the gap
    assert n_tracks(with_one_low(14)) == 2  # fifth from the gap on
    assert n_tracks(with_one_low(15)) == 1  # sixth from the gap on


def test_descent_then_climb_splits(spark):
    fl300 = 30000.0 / FT_PER_M
    down = {"baro_altitude": fl300, "vert_rate": -1500.0 / FPM_PER_MPS}
    up = {"baro_altitude": fl300, "vert_rate": 1500.0 / FPM_PER_MPS}
    level = {"baro_altitude": fl300, "vert_rate": 0.0}

    assert n_tracks(_hole(spark, before=down, after=up)) == 2
    # One side alone is a step, not a stop: the test needs both.
    assert n_tracks(_hole(spark, before=down, after=level)) == 1
    assert n_tracks(_hole(spark, before=level, after=up)) == 1


def test_null_vert_rate_is_neutral(spark):
    """ADS-B sends vertical rate in separate messages, so absence says nothing."""
    assert n_tracks(_hole(spark, before={"vert_rate": None}, after={"vert_rate": None})) == 1


def test_different_callsigns_split(spark):
    assert n_tracks(_hole(spark, after={"callsign": "BAW2"})) == 2


def test_padded_callsigns_agree(spark):
    """ADS-B pads to eight characters; that is not a different flight."""
    assert n_tracks(_hole(spark, after={"callsign": "DLH1    "})) == 1


def test_blank_then_different_callsign_splits(spark):
    """X, hole, blank, Y. The blank agrees with anything, and A9's break does not
    fire at Y because X is hours old, so only the lookahead can catch this."""
    samples = _hole_samples(n_after=0)
    gap_s = 191 * 60
    samples += _piece(gap_s, _lon(1530.0), n=5, step_s=30, callsign="")
    samples += _piece(gap_s + 150, _lon(1530.0), n=10, callsign="BAW2")
    assert n_tracks(make_track(spark, samples)) == 2


def test_a_callsign_past_the_lookahead_slips_through(spark):
    """The rule's documented limit: the real-hole measurement catches this case."""
    samples = _hole_samples(n_after=0)
    gap_s = 191 * 60
    samples += _piece(gap_s, _lon(1530.0), n=5, step_s=150, callsign="")
    samples += _piece(gap_s + 11 * 60, _lon(1530.0), n=10, callsign="BAW2")
    assert n_tracks(make_track(spark, samples)) == 1


def _old_callsign_then_blank(spark, seen_min_before_edge):
    """DLH1 last seen that long before the old edge, blank from there to the edge,
    a hole, then BAW2."""
    seen_s = -seen_min_before_edge * 60
    samples = _piece(seen_s - 45, 0.0, callsign="DLH1")
    for t in [*range(seen_s + 300, 0, 300), 0]:
        samples += _piece(t, 0.0, n=1, callsign="")
    samples += _piece(191 * 60, _lon(1530.0), callsign="BAW2")
    return make_track(spark, samples)


def test_an_old_callsign_within_the_lookback_still_counts(spark):
    """It ended well before the hole, and is still evidence against BAW2. Inclusive."""
    assert n_tracks(_old_callsign_then_blank(spark, 37)) == 2
    assert n_tracks(_old_callsign_then_blank(spark, 60)) == 2


def test_an_old_callsign_past_the_lookback_counts_as_blank(spark):
    """Otherwise the answer would depend on how far back this day's read reaches."""
    assert n_tracks(_old_callsign_then_blank(spark, 61)) == 1


def test_the_lookback_is_a_parameter(spark):
    wider = dataclasses.replace(P, link_callsign_lookback_minutes=90.0)
    assert n_tracks(_old_callsign_then_blank(spark, 61), wider) == 2


def test_blank_throughout_is_neutral(spark):
    assert n_tracks(_hole(spark, before={"callsign": ""}, after={"callsign": ""})) == 1
    assert n_tracks(_hole(spark, before={"callsign": None}, after={"callsign": None})) == 1


def test_too_slow_splits(spark):
    assert n_tracks(_hole(spark, nm=200.0 * 191 / 60)) == 2


def test_impossible_speed_splits(spark):
    """Above the cap is a duplicate address, not wind."""
    assert n_tracks(_hole(spark, nm=900.0 * 191 / 60)) == 2


def test_a_hole_past_the_maximum_splits(spark):
    assert n_tracks(_hole(spark, gap_min=360.0, nm=480.0 * 6)) == 1
    assert n_tracks(_hole(spark, gap_min=361.0, nm=480.0 * 361 / 60)) == 2


def test_time_budget_when_enabled(spark):
    """Off, the 1,530 NM hole links. On, 191 min at 650 kt leaves 50 min, room for a stop."""
    on = dataclasses.replace(P, link_min_stop_minutes=35.0)
    assert n_tracks(_hole(spark, nm=1530.0), on) == 2
    assert n_tracks(_hole(spark, nm=1950.0), on) == 1
    assert n_tracks(_hole(spark, nm=1530.0), P) == 1


def test_speed_ratio_when_enabled(spark):
    """Implied 367 kt passes the absolute floor but not 0.85 x the 480 kt edge speed."""
    on = dataclasses.replace(P, link_min_speed_ratio=0.85)
    nm = 367.0 * 191 / 60
    assert n_tracks(_hole(spark, nm=nm), on) == 2
    assert n_tracks(_hole(spark, nm=nm), P) == 1


def test_placeholder_address_splits(spark):
    samples = _hole_samples()
    for sample in samples:
        sample["icao24"] = "000001"
    assert n_tracks(make_track(spark, samples)) == 2


def test_no_position_within_the_window_fails_closed(spark):
    samples = _hole_samples()
    for sample in samples[:10]:  # the whole old piece, 45 s of it
        sample["lat"] = sample["lon"] = None
    df = make_track(spark, samples)
    assert n_tracks(df) == 2

    frame = _engine_frame(df)
    rows = (
        frame.withColumn("_brk", cruise_linked().break_expr(P))
        .orderBy("_ts")
        .select("_brk")
        .collect()
    )
    assert all(r["_brk"] is not None for r in rows)
    assert rows[10]["_brk"] is True
    assert rows[0]["_brk"] is False  # the group's first sample has no gap at all


# --- edge positions, read through blank samples --------------------------------


def _after_hole(blank_min, gap_min=60.0, nm=330.0):
    """Cruise, then a hole, then a sample a minute whose first *blank_min* have no position."""
    samples = _piece(-45, 0.0)
    samples += _piece(gap_min * 60, _lon(nm), n=12, step_s=60)
    for sample in samples[10 : 10 + blank_min]:
        sample["lat"] = sample["lon"] = None
    return samples


def _before_hole(blank_min, gap_min=60.0, nm=330.0):
    """The same at the old edge: a sample a minute, its last *blank_min* with no position."""
    samples = _piece(-11 * 60, 0.0, n=12, step_s=60)
    for sample in samples[12 - blank_min :]:
        sample["lat"] = sample["lon"] = None
    return samples + _piece(gap_min * 60, _lon(nm))


def _hole_row(spark, samples):
    """The features at the first sample after the only hole."""
    rows = _with_features(make_track(spark, samples)).orderBy("event_time").collect()
    return max(rows, key=lambda r: r["gap_min"] or 0.0)


@pytest.mark.parametrize("build", [_after_hole, _before_hole])
def test_a_blank_edge_sample_reads_the_next_position(spark, build):
    """330 NM over 60 min of samples: the speed must read the times of the positions."""
    samples = build(1)
    assert n_tracks(make_track(spark, samples)) == 1

    row = _hole_row(spark, samples)
    assert row["gap_min"] == pytest.approx(60.0)
    assert row["position_gap_min"] == pytest.approx(61.0)
    assert row["gc_nm"] == pytest.approx(330.0, abs=0.01)


@pytest.mark.parametrize("build", [_after_hole, _before_hole])
def test_speed_uses_the_time_of_the_sample_the_position_came_from(spark, build):
    """Over the 60 min sample gap this is 330 kt and links. Over the 65 min between
    the positions it is 305 kt, under the 315 kt floor, and splits."""
    samples = build(5)
    assert n_tracks(make_track(spark, samples)) == 2
    assert _hole_row(spark, samples)["position_gap_min"] == pytest.approx(65.0)


@pytest.mark.parametrize("build", [_after_hole, _before_hole])
def test_a_position_past_the_window_does_not_count(spark, build):
    """Five minutes is inclusive, and the sixth minute's position is not seen."""
    fast = dict(gap_min=60.0, nm=480.0)
    assert n_tracks(make_track(spark, build(5, **fast))) == 1
    assert n_tracks(make_track(spark, build(6, **fast))) == 2

    row = _hole_row(spark, build(6, **fast))
    assert row["gc_nm"] is None
    assert row["position_gap_min"] is None


def test_the_window_is_a_parameter_and_changes_the_features(spark):
    wider = dataclasses.replace(P, link_edge_position_minutes=7.0)
    df = make_track(spark, _after_hole(6, nm=480.0))
    assert n_tracks(df) == 2
    assert n_tracks(df, wider) == 1


@pytest.mark.parametrize("missing", ["lat", "lon"])
def test_half_a_position_does_not_count(spark, missing):
    """The next sample's full pair is read, not the coordinate this one has."""
    samples = _after_hole(0)
    samples[10][missing] = None
    assert _hole_row(spark, samples)["position_gap_min"] == pytest.approx(61.0)


def test_both_coordinates_come_from_one_sample(spark):
    """Samples with only a lat and only a lon must never pair into a position."""
    samples = _after_hole(0)
    for i, sample in enumerate(samples[10:]):
        sample["lat" if i % 2 else "lon"] = None
    assert n_tracks(make_track(spark, samples)) == 2
    assert _hole_row(spark, samples)["gc_nm"] is None


def test_the_length_limit_reads_the_sample_gap_not_the_position_gap(spark):
    """The 30 min split sees samples, so the limit does too. Blank edges cannot lengthen it."""
    samples = _piece(-45, 0.0)
    samples += _piece(360 * 60, _lon(480.0 * 6), n=12, step_s=60)
    samples[10]["lat"] = samples[10]["lon"] = None
    row = _hole_row(spark, samples)
    assert row["gap_min"] == pytest.approx(360.0)
    assert row["position_gap_min"] == pytest.approx(361.0)
    assert n_tracks(make_track(spark, samples)) == 1


def test_the_antimeridian_is_still_crossed_the_short_way_through_blank_edges(spark):
    fl350 = 35000.0 / FT_PER_M
    samples = _piece(-45, 178.0, baro_altitude=fl350)
    samples += _piece(31 * 60, -178.0, baro_altitude=fl350)
    samples[9]["lat"] = samples[9]["lon"] = None
    samples[10]["lat"] = samples[10]["lon"] = None
    assert n_tracks(make_track(spark, samples)) == 1


@pytest.mark.parametrize("missing", ["lat", "lon"])
def test_half_a_position_at_the_old_edge_does_not_count(spark, missing):
    """The sample before it holds the full pair, and that is the one read."""
    samples = _before_hole(0)
    samples[11][missing] = None
    assert _hole_row(spark, samples)["position_gap_min"] == pytest.approx(61.0)


def test_too_few_edge_samples_still_decides(spark):
    assert n_tracks(_hole(spark, n_before=1)) == 1
    assert n_tracks(_hole(spark, n_before=1, before={"baro_altitude": 3000.0})) == 2


def test_two_samples_in_one_second_do_not_fail_the_job(spark):
    """The features are computed on every row, so a zero gap reaches the division."""
    samples = _hole_samples()
    samples.insert(1, dict(samples[0]))
    assert n_tracks(make_track(spark, samples)) == 1


def test_the_antimeridian_is_crossed_the_short_way(spark):
    """Lon 178 to -178 is 240 NM. The long way would read as impossible."""
    fl350 = 35000.0 / FT_PER_M
    samples = _piece(-45, 178.0, baro_altitude=fl350)
    samples += _piece(31 * 60, -178.0, baro_altitude=fl350)
    assert n_tracks(make_track(spark, samples)) == 1


def test_short_gaps_behave_as_debounced(spark):
    """Everything but a long gap is A9's, verbatim."""
    samples = _piece(0, 0.0, n=8)
    samples += _piece(40, 0.0, n=5, step_s=2, callsign="XXX9")  # flicker, 8 s long
    samples += _piece(50, 0.0, n=8)
    samples += _piece(20 * 60, 0.1, n=8)  # 20 min at cruise
    samples += _piece(40 * 60, 0.2, n=8, baro_altitude=1000.0)  # 20 min, low
    samples += _piece(50 * 60, 0.2, n=8, callsign="BAW2", baro_altitude=1000.0)
    df = make_track(spark, samples)
    assert _partition(df, cruise_linked()) == _partition(df, debounced())
    assert len(_partition(df, cruise_linked())) == 3


def test_debounced_partition_is_unchanged(spark):
    """The extraction, pinned by hand as well as by the unmodified debounce suite."""
    samples = _piece(0, 0.0, n=8, step_s=5)
    samples += _piece(40, 0.0, n=1, callsign="XXX9")  # flicker
    samples += _piece(45, 0.0, n=8, step_s=5)
    samples += _piece(85, 0.0, n=8, step_s=5, callsign="BAW2")  # genuine change
    samples += _piece(40 * 60, 1.0, n=8, step_s=5, callsign="BAW2")  # long gap
    df = make_track(spark, samples)

    t = [float(s["t"]) for s in samples]
    expected = {frozenset(t[:17]), frozenset(t[17:25]), frozenset(t[25:])}
    assert _partition(df, debounced()) == expected

    zero = dataclasses.replace(P, callsign_min_persistence_seconds=0.0)
    assert _partition(df, debounced(), zero) == _partition(df, recommended(), zero)
    assert _partition(df, debounced(), zero) != expected


def test_the_extracted_expressions_never_null(spark):
    df = _engine_frame(_hole(spark, before={"callsign": None}, after={"callsign": None}))
    rows = df.select(
        stable_callsign(P).alias("stable"),
        debounced_callsign_change(P).alias("change"),
    ).collect()
    assert all(r["stable"] is None for r in rows)
    assert all(r["change"] is False for r in rows)


def test_defaults_are_on_both_config_classes():
    expected = {
        "link_max_gap_minutes": 600.0,
        "link_cruise_floor_ft": 18000.0,
        "link_edge_vert_rate_fpm": 500.0,
        "link_callsign_lookahead_minutes": 10.0,
        "link_callsign_lookback_minutes": 60.0,
        "link_edge_position_minutes": 5.0,
        "link_min_implied_speed_kt": 400.0,
        "link_max_implied_speed_kt": 750.0,
        "link_min_speed_ratio": 0.0,
        "link_min_stop_minutes": 0.0,
        "link_vfast_kt": 650.0,
    }
    for cls in (SegmentationParams, SegmentationConfig):
        defaults = {f.name: f.default for f in dataclasses.fields(cls)}
        assert {k: defaults[k] for k in expected} == expected, cls.__name__
    assert LINK_EDGE_SAMPLES == 5
    assert PLACEHOLDER_ICAO24 == ("000000", "000001", "ffffff")


def test_the_arm_is_registered_as_a9_is():
    rule = ARMS["cruise_linked"]()
    a9 = debounced()
    assert rule.name == "cruise_linked"
    assert rule.group_cols == a9.group_cols == ["icao24"]
    assert rule.month_suffix is a9.month_suffix is False
    assert rule.id_from_start_time is a9.id_from_start_time is True


# --- direction at an edge below the floor -------------------------------------


FL200 = 20000.0 / FT_PER_M
CLIMBING = {"baro_altitude": FL200, "vert_rate": 1500.0 / FPM_PER_MPS}
DESCENDING = {"baro_altitude": FL200, "vert_rate": -1500.0 / FPM_PER_MPS}


def test_both_edge_allowances_are_on_by_default():
    """Climb since version 4; descent since version 5, which found the three false
    joins version 4 counted against it were estimated arrival times outside Europe."""
    for cls in (SegmentationParams, SegmentationConfig):
        defaults = {f.name: f.default for f in dataclasses.fields(cls)}
        assert defaults["link_old_edge_climb"] is True, cls.__name__
        assert defaults["link_new_edge_descent"] is True, cls.__name__
        assert "link_directional_edges" not in defaults, cls.__name__


def test_a_climbing_old_edge_links_by_default(spark):
    df = _hole(spark, before=CLIMBING)
    assert (
        n_tracks(
            df,
            dataclasses.replace(
                SHIPPED,
                link_cruise_floor_ft=25000.0,
                link_min_implied_speed_kt=315.0,
                link_max_gap_minutes=360.0,
            ),
        )
        == 1
    )


def test_a_descending_new_edge_links_by_default(spark):
    df = _hole(spark, after=DESCENDING)
    assert (
        n_tracks(
            df,
            dataclasses.replace(
                SHIPPED,
                link_cruise_floor_ft=25000.0,
                link_min_implied_speed_kt=315.0,
                link_max_gap_minutes=360.0,
            ),
        )
        == 1
    )


def test_descent_off_by_name_reproduces_the_version_4_rule(spark):
    """The allowance is a setting, so the rule version 4 measured stays reachable."""
    no_descent = dataclasses.replace(SHIPPED, link_new_edge_descent=False)
    at_12000 = {"baro_altitude": 12000.0 / FT_PER_M, "vert_rate": -1000.0 / FPM_PER_MPS}
    assert n_tracks(_hole(spark, after=at_12000), SHIPPED) == 1
    assert n_tracks(_hole(spark, after=at_12000), no_descent) == 2
    assert _partition(_hole(spark), cruise_linked(), SHIPPED) == _partition(
        _hole(spark), cruise_linked(), no_descent
    )


def test_a_descending_new_edge_below_the_low_altitude_limit_still_splits(spark):
    low = {"baro_altitude": 4000.0 / FT_PER_M, "vert_rate": -1000.0 / FPM_PER_MPS}
    assert n_tracks(_hole(spark, after=low), SHIPPED) == 2


def test_two_vertical_rates_do_not_make_a_descent_by_default(spark):
    samples = _hole_samples(
        after={"baro_altitude": 12000.0 / FT_PER_M, "vert_rate": -1000.0 / FPM_PER_MPS}
    )
    for s in samples[12:]:
        s["vert_rate"] = None
    assert n_tracks(make_track(spark, samples), SHIPPED) == 2


def test_a_flight_reappearing_on_approach_links_only_with_direction_on(spark):
    df = _hole(spark, after=DESCENDING)
    assert n_tracks(df) == 2
    assert n_tracks(df, _DIRECTIONAL) == 1


def test_a_flight_leaving_coverage_in_its_climb_links_only_with_direction_on(spark):
    df = _hole(spark, before=CLIMBING)
    assert n_tracks(df) == 2
    assert n_tracks(df, _DIRECTIONAL) == 1


def test_climb_at_the_old_edge_and_descent_at_the_new_one_link_together(spark):
    assert n_tracks(_hole(spark, before=CLIMBING, after=DESCENDING), _DIRECTIONAL) == 1


@pytest.mark.parametrize("side", ["before", "after"])
def test_a_null_vertical_rate_does_not_pass_the_alternative(spark, side):
    """Neutral at cruise, but below the floor it was the only evidence."""
    low = {"baro_altitude": FL200, "vert_rate": None}
    assert n_tracks(_hole(spark, **{side: low}), _DIRECTIONAL) == 2


def test_a_climb_at_the_new_edge_splits(spark):
    assert n_tracks(_hole(spark, after=CLIMBING), _DIRECTIONAL) == 2


def test_a_descent_at_the_old_edge_splits(spark):
    assert n_tracks(_hole(spark, before=DESCENDING), _DIRECTIONAL) == 2


def test_descend_then_climb_still_splits_with_direction_on(spark):
    fl300 = 30000.0 / FT_PER_M
    down = {"baro_altitude": fl300, "vert_rate": -1500.0 / FPM_PER_MPS}
    up = {"baro_altitude": fl300, "vert_rate": 1500.0 / FPM_PER_MPS}
    assert n_tracks(_hole(spark, before=down, after=up), _DIRECTIONAL) == 2


def test_linkable_agrees_with_the_rule_when_the_new_edge_is_low(spark):
    """The low-altitude gap keeps breaking. If `linkable` said yes here, the study would
    count a link that the rule does not make."""
    low_descent = {"baro_altitude": 3000.0 / FT_PER_M, "vert_rate": -1500.0 / FPM_PER_MPS}
    samples = _hole_samples(after=low_descent)
    assert n_tracks(make_track(spark, samples), _DIRECTIONAL) == 2

    assert _hole_row(spark, samples)["alt_after_ft"] == pytest.approx(3000.0)
    assert _decide(spark, _DIRECTIONAL, alt_after_ft=3000.0, vr_after_fpm=-1500.0) is False


def test_the_descent_guard_reads_past_a_blank_first_sample(spark):
    """The low-altitude gap sees nothing on a blank sample, so the guard has to read on."""
    low = {"baro_altitude": 3000.0 / FT_PER_M, "vert_rate": -1500.0 / FPM_PER_MPS}
    samples = _piece(-45, 0.0)
    samples += _piece(191 * 60, _lon(1530.0), n=1, baro_altitude=None, vert_rate=low["vert_rate"])
    samples += _piece(191 * 60 + 5, _lon(1530.0), n=9, **low)
    assert n_tracks(make_track(spark, samples), _DESCENT_ONLY) == 2
    assert _hole_row(spark, samples)["alt_after_ft"] == pytest.approx(3000.0)


def _with_rates(rates, samples):
    """Set the vertical rates of *samples*, nearest the hole last on either side."""
    for sample, rate in zip(samples, rates):
        sample["vert_rate"] = rate
    return samples


@pytest.mark.parametrize("n_rates, tracks", [(1, 2), (2, 2), (3, 1), (5, 1)])
def test_a_direction_needs_three_rates_at_the_old_edge(spark, n_rates, tracks):
    """`avg` skips a NULL, so without the count one message would carry the verdict."""
    up = 1500.0 / FPM_PER_MPS
    samples = _hole_samples(before={"baro_altitude": FL200, "vert_rate": None})
    _with_rates([up] * n_rates, samples[5:10])
    assert n_tracks(make_track(spark, samples), _DIRECTIONAL) == tracks
    assert _hole_row(spark, samples)["vr_before_samples"] == n_rates


@pytest.mark.parametrize("n_rates, tracks", [(1, 2), (2, 2), (3, 1), (5, 1)])
def test_a_direction_needs_three_rates_at_the_new_edge(spark, n_rates, tracks):
    down = -1500.0 / FPM_PER_MPS
    samples = _hole_samples(after={"baro_altitude": FL200, "vert_rate": None})
    _with_rates([down] * n_rates, samples[10:15])
    assert n_tracks(make_track(spark, samples), _DIRECTIONAL) == tracks
    assert _hole_row(spark, samples)["vr_after_samples"] == n_rates


def test_rates_further_than_the_nearest_five_do_not_count(spark):
    """Level at the five nearest, climbing before them: the edge is not climbing."""
    up = 1500.0 / FPM_PER_MPS
    samples = _hole_samples(before={"baro_altitude": FL200})
    _with_rates([up] * 5, samples[0:5])
    assert n_tracks(make_track(spark, samples), _DIRECTIONAL) == 2
    row = _hole_row(spark, samples)
    assert row["vr_before_fpm"] == 0.0
    assert row["vr_before_samples"] == 5


def test_too_few_samples_at_a_low_edge_splits_though_they_all_agree(spark):
    assert n_tracks(_hole(spark, n_before=2, before=CLIMBING), _DIRECTIONAL) == 2
    assert n_tracks(_hole(spark, n_before=3, before=CLIMBING), _DIRECTIONAL) == 1


def test_the_speed_ratio_is_not_read_at_a_climb_edge(spark):
    """On real tracks: 367 kt implied is under 0.85 x a 480 kt edge, and the climb edge passes."""
    nm = 367.0 * 191 / 60
    on = _DIRECTIONAL_RATIO_ON
    assert n_tracks(_hole(spark, nm=nm, before=CLIMBING), on) == 1
    assert n_tracks(_hole(spark, nm=nm), on) == 2


def test_short_gaps_behave_as_debounced_with_direction_on(spark):
    samples = _piece(0, 0.0, n=8)
    samples += _piece(20 * 60, 0.1, n=8)
    samples += _piece(40 * 60, 0.2, n=8, baro_altitude=1000.0, vert_rate=-5.0)
    df = make_track(spark, samples)
    assert _partition(df, cruise_linked(), _DIRECTIONAL) == _partition(df, debounced())


# --- each edge position is read once ------------------------------------------


def _unshared_position_features(p):
    """The two position features as they were written before the single read."""
    w = segment_window()
    back = w.rowsBetween(Window.unboundedPreceding, -1)
    seconds = w.orderBy(F.unix_timestamp(F.col("_ts")))
    position = F.when(
        F.col("lat").isNotNull() & F.col("lon").isNotNull(),
        F.struct(F.col("_ts").alias("ts"), F.col("lat").alias("lat"), F.col("lon").alias("lon")),
    )
    span_s = int(p.link_edge_position_minutes * 60)
    old_edge_ts = F.lag(F.col("_ts")).over(w)
    last_position = F.last(position, ignorenulls=True).over(back)
    old_position = F.when(
        F.unix_timestamp(old_edge_ts) - F.unix_timestamp(last_position.getField("ts")) <= span_s,
        last_position,
    )
    new_position = F.first(position, ignorenulls=True).over(seconds.rangeBetween(0, span_s))
    return {
        "position_gap_min": (
            F.unix_timestamp(new_position.getField("ts"))
            - F.unix_timestamp(old_position.getField("ts"))
        )
        / 60.0,
        "gc_nm": haversine_nm(
            old_position.getField("lat"),
            old_position.getField("lon"),
            new_position.getField("lat"),
            new_position.getField("lon"),
        ),
    }


def _single_read_fixtures():
    half = _after_hole(0)
    half[10]["lat"] = None
    old_half = _before_hole(0)
    old_half[11]["lon"] = None
    twin = _hole_samples()
    twin.insert(1, dict(twin[0]))
    return {
        "plain": _hole_samples(),
        "after, read through": _after_hole(3),
        "before, read through": _before_hole(3),
        "after, too far": _after_hole(6, nm=480.0),
        "before, too far": _before_hole(6, nm=480.0),
        "half after": half,
        "half before": old_half,
        "no position at all": [{**s, "lat": None, "lon": None} for s in _hole_samples()],
        "two in one second": twin,
    }


@pytest.mark.parametrize("name", list(_single_read_fixtures()))
def test_the_single_read_gives_the_values_of_the_separate_reads(spark, name):
    frame = _engine_frame(make_track(spark, _single_read_fixtures()[name]))
    new = link_feature_columns(P)
    old = _unshared_position_features(P)
    rows = (
        frame.select(
            "event_time",
            *[new[k].alias(f"new_{k}") for k in old],
            *[old[k].alias(f"old_{k}") for k in old],
        )
        .orderBy("event_time")
        .collect()
    )
    assert any(r["old_position_gap_min"] is not None for r in rows) or name == "no position at all"
    for r in rows:
        assert r["new_position_gap_min"] == r["old_position_gap_min"]
        assert r["new_gc_nm"] == r["old_gc_nm"]


def _plan(df):
    return df._jdf.queryExecution().executedPlan().toString()


def test_the_rule_reads_each_edge_position_once(spark):
    """Spark evaluates each copy of a window expression, so this is the cost of the rule.

    ``first`` is the count to pin because only the new edge's position multiplies it.
    It was 17: here it is that one read, the callsign after the hole, and the debounce.
    """
    frame = _engine_frame(_hole(spark))
    plan = _plan(frame.withColumn("_brk", cruise_linked().break_expr(P)))
    assert len(re.findall(r"\bfirst\(", plan)) <= 3, plan


@pytest.mark.parametrize("name", list(_single_read_fixtures()))
def test_selecting_the_features_together_gives_the_separate_columns(spark, name):
    frame = _engine_frame(make_track(spark, _single_read_fixtures()[name]))
    apart = frame.select(
        "event_time", *[c.alias(k) for k, c in link_feature_columns(P).items()]
    ).orderBy("event_time")
    together = select_link_features(frame, P, keep=["event_time"])
    assert together.columns == apart.columns
    assert together.orderBy("event_time").collect() == apart.collect()


def test_selecting_the_features_together_reads_each_edge_once(spark):
    """Column by column it is seven, which made the study 3.2x slower on a local run."""
    frame = _engine_frame(_hole(spark))
    plan = _plan(select_link_features(frame, P))
    assert len(re.findall(r"\bcollect_list\(", plan)) == 2, plan


def test_the_features_read_each_edge_position_once_per_column(spark):
    """It was 8: here it is one read for each of the two position columns, and the callsign."""
    frame = _engine_frame(_hole(spark))
    features = link_feature_columns(P)
    plan = _plan(frame.select(*[c.alias(k) for k, c in features.items()]))
    assert len(re.findall(r"\bfirst\(", plan)) <= 3, plan


# --- the edge frames are bounded in time as well as in rows ---------------------

#: A sample that would fail both edge tests and move every other edge feature.
_ODD_SAMPLE = {"baro_altitude": 914.4, "velocity": 50.0, "vert_rate": -20.0}


def _with_odd_sample_before(seconds_before_old_edge):
    samples = _piece(-4 * 30, 0.0, n=5, step_s=30)[1:]
    return (
        _piece(-seconds_before_old_edge, 0.0, n=1, **_ODD_SAMPLE)
        + samples
        + _piece(60 * 60, _lon(480.0), n=10)
    )


def _with_odd_sample_after(seconds_after_new_edge):
    new_edge = 60 * 60
    return (
        _piece(-45, 0.0)
        + _piece(new_edge, _lon(480.0), n=4, step_s=30)
        + _piece(new_edge + seconds_after_new_edge, _lon(480.0 + 1.0), n=1, **_ODD_SAMPLE)
    )


def test_a_low_sample_older_than_the_window_does_not_fail_the_old_edge(spark):
    """Five rows can reach back past an earlier break, into another flight."""
    samples = _with_odd_sample_before(5 * 60 + 1)
    row = _hole_row(spark, samples)
    assert row["alt_before_ft"] == pytest.approx(CRUISE_M * FT_PER_M)
    assert row["vr_before_fpm"] == 0.0
    assert row["vr_before_samples"] == 4
    assert row["spd_before_kt"] == pytest.approx(480.0)
    assert n_tracks(make_track(spark, samples), SHIPPED) == 1


def test_a_low_sample_inside_the_window_still_fails_the_old_edge(spark):
    """The bound is inclusive, as the position's is. Nothing younger gets away."""
    samples = _with_odd_sample_before(5 * 60)
    row = _hole_row(spark, samples)
    assert row["alt_before_ft"] == pytest.approx(3000.0, abs=0.01)
    assert row["vr_before_samples"] == 5
    assert row["vr_before_fpm"] == pytest.approx(-20.0 * FPM_PER_MPS / 5)
    assert row["spd_before_kt"] < 480.0
    assert n_tracks(make_track(spark, samples), SHIPPED) == 2


def test_a_low_sample_past_the_window_does_not_fail_the_new_edge(spark):
    samples = _with_odd_sample_after(5 * 60 + 1)
    row = _hole_row(spark, samples)
    assert row["alt_after_ft"] == pytest.approx(CRUISE_M * FT_PER_M)
    assert row["vr_after_fpm"] == 0.0
    assert row["vr_after_samples"] == 4
    assert n_tracks(make_track(spark, samples), SHIPPED) == 1


def test_a_low_sample_inside_the_window_still_fails_the_new_edge(spark):
    samples = _with_odd_sample_after(5 * 60)
    row = _hole_row(spark, samples)
    assert row["alt_after_ft"] == pytest.approx(3000.0, abs=0.01)
    assert row["vr_after_samples"] == 5
    assert n_tracks(make_track(spark, samples), SHIPPED) == 2


def test_the_window_follows_the_setting(spark):
    """Widened, the same sample is inside it again, so nothing is hard-wired to 5 min."""
    samples = _with_odd_sample_before(6 * 60)
    frame = _engine_frame(make_track(spark, samples))
    for minutes, expected in ((5.0, CRUISE_M * FT_PER_M), (7.0, 3000.0)):
        p = dataclasses.replace(SHIPPED, link_edge_position_minutes=minutes)
        features = link_feature_columns(p)
        rows = frame.select(
            features["gap_min"].alias("gap_min"), features["alt_before_ft"].alias("alt")
        ).collect()
        row = max(rows, key=lambda r: r["gap_min"] or 0.0)
        assert row["alt"] == pytest.approx(expected, abs=0.01)


def test_the_edge_samples_are_read_once_per_frame(spark):
    """Spark evaluates each copy of a window expression, and these run on every row."""
    frame = _engine_frame(_hole(spark))
    plan = _plan(frame.withColumn("_brk", cruise_linked().break_expr(P)))
    assert len(re.findall(r"\bcollect_list\(", plan)) <= 2, plan


# --- settings that cannot be honoured -------------------------------------------


@pytest.mark.parametrize("minutes", [15.0, 20.0])
def test_a_position_window_reaching_the_low_altitude_gap_raises(minutes):
    """It could read through a break the low-altitude rule makes, into another flight."""
    p = dataclasses.replace(P, link_edge_position_minutes=minutes)
    with pytest.raises(ValueError, match="link_edge_position_minutes"):
        link_feature_columns(p)
    with pytest.raises(ValueError, match="link_edge_position_minutes"):
        cruise_linked().break_expr(p)


def test_a_position_window_just_under_the_low_altitude_gap_is_taken(spark):
    p = dataclasses.replace(P, link_edge_position_minutes=14.0)
    assert n_tracks(_hole(spark), p) == 1


# --- the features -----------------------------------------------------------


def test_features_at_the_first_sample_after_the_hole(spark):
    df = _hole(
        spark,
        before={"vert_rate": -500.0 / FPM_PER_MPS},
        after={"vert_rate": 1000.0 / FPM_PER_MPS, "callsign": "BAW2"},
    )
    row = _with_features(df).orderBy("event_time").collect()[10]

    assert row["gap_min"] == pytest.approx(191.0)
    assert row["position_gap_min"] == pytest.approx(191.0)
    assert row["gc_nm"] == pytest.approx(1530.0, abs=0.01)
    assert row["alt_before_ft"] == pytest.approx(CRUISE_M * FT_PER_M)
    assert row["alt_after_ft"] == pytest.approx(CRUISE_M * FT_PER_M)
    assert row["vr_before_fpm"] == pytest.approx(-500.0)
    assert row["vr_after_fpm"] == pytest.approx(1000.0)
    assert row["spd_before_kt"] == pytest.approx(480.0)
    assert row["cs_before"] == "DLH1"
    assert row["cs_after"] == "BAW2"
    assert row["icao24"] == "abc123"


def test_features_ignore_every_parameter_the_grid_varies(spark):
    """The grid applies `linkable` to one cached feature frame, 54 times over."""
    df = _hole(spark)
    frame = _engine_frame(df)
    varied = dataclasses.replace(
        P,
        link_min_implied_speed_kt=250.0,
        link_min_speed_ratio=0.85,
        link_min_stop_minutes=35.0,
        link_max_gap_minutes=120.0,
    )

    def collect(params):
        f = link_feature_columns(params)
        return (
            frame.select("event_time", *[c.alias(k) for k, c in f.items()])
            .orderBy("event_time")
            .collect()
        )

    assert collect(P) == collect(varied)


def test_a_caller_window_scopes_every_feature(spark):
    """A control in a shared frame reads exactly as it would alone.

    Two controls of one airframe, interleaved 2-3 s apart, so they share `_grp`. Any
    feature computed over the engine's window instead of the caller's reads the other
    control's samples. In particular a's AFR7 run holds for 50 s, but b turns up a new
    value 18 s into it, so over the airframe AFR7 would read as a flicker.
    """
    a_samples = _piece(-100, 0.0, n=10) + _piece(-50, 0.0, n=11, callsign="AFR7")
    a_samples += _piece(191 * 60, _lon(1530.0), callsign="")

    b_samples = _piece(-102, 0.0, n=20, callsign="BAW2", baro_altitude=9000.0)
    b_samples[14]["callsign"] = "KLM8"  # t = -32
    b_samples += _piece(3, 0.0, n=1, callsign="BAW2", baro_altitude=9000.0)
    b_samples += _piece(191 * 60 + 4, _lon(1600.0), callsign="BAW2")

    names = list(link_feature_columns(P))

    def alone(samples):
        rows = _with_features(make_track(spark, samples)).orderBy("event_time")
        return rows.select(*names).collect()

    def shared(control):
        both = (
            make_track(spark, a_samples)
            .withColumn("control_id", F.lit("a"))
            .unionByName(make_track(spark, b_samples).withColumn("control_id", F.lit("b")))
        )
        features = link_feature_columns(P, Window.partitionBy("control_id").orderBy("_ts"))
        rows = _engine_frame(both).select(
            "control_id", "event_time", *[c.alias(k) for k, c in features.items()]
        )
        return (
            rows.filter(F.col("control_id") == control)
            .orderBy("event_time")
            .select(*names)
            .collect()
        )

    assert shared("a") == alone(a_samples)
    assert shared("b") == alone(b_samples)
    edge = max(shared("a"), key=lambda r: r["gap_min"] or 0.0)
    assert (edge["cs_before"], edge["cs_after"]) == ("AFR7", None)


# --- edge values read past blank samples --------------------------------------


def _one_hz_after_hole(n_blank, then, **blank):
    """Cruise, a hole, then *n_blank* samples a second with *blank* set, then *then* a second."""
    samples = _piece(-45, 0.0)
    samples += _piece(3600, _lon(480.0), n=n_blank, step_s=1, **blank)
    samples += _piece(3600 + n_blank, _lon(480.0), step_s=1, **then)
    return samples


def test_altitude_is_read_past_blank_samples(spark):
    """Altitude arrives in a later message, as position does. Five rows hold only blanks."""
    samples = _one_hz_after_hole(8, {}, baro_altitude=None)
    assert n_tracks(make_track(spark, samples)) == 1
    assert _hole_row(spark, samples)["alt_after_ft"] == pytest.approx(CRUISE_M * FT_PER_M)


def test_altitude_is_read_past_blank_samples_at_the_old_edge(spark):
    samples = _piece(-69, 0.0, n=10) + _piece(-20, 0.0, n=21, step_s=1, baro_altitude=None)
    samples += _piece(3600, _lon(480.0))
    assert n_tracks(make_track(spark, samples)) == 1
    assert _hole_row(spark, samples)["alt_before_ft"] == pytest.approx(CRUISE_M * FT_PER_M)


def test_a_low_value_after_blanks_still_counts(spark):
    """It is the nearest value there is, so blanks ahead of it must not hide it."""
    samples = _one_hz_after_hole(8, {"baro_altitude": 3000.0 / FT_PER_M}, baro_altitude=None)
    assert n_tracks(make_track(spark, samples)) == 2
    assert _hole_row(spark, samples)["alt_after_ft"] == pytest.approx(3000.0)


@pytest.mark.parametrize("blank_s, tracks", [(300, 1), (305, 2)])
def test_no_altitude_in_the_window_splits(spark, blank_s, tracks):
    """Blank until *blank_s* after the new edge, at 5 s. The bound is inclusive."""
    new_edge = 3600
    samples = _piece(-45, 0.0)
    samples += _piece(new_edge, _lon(480.0), n=blank_s // 5, baro_altitude=None)
    samples += _piece(new_edge + blank_s, _lon(480.0), n=5)
    assert n_tracks(make_track(spark, samples)) == tracks
    row = _hole_row(spark, samples)
    if tracks == 2:
        assert row["alt_after_ft"] is None


@pytest.mark.parametrize("blank_s, tracks", [(300, 1), (305, 2)])
def test_no_altitude_in_the_window_splits_at_the_old_edge(spark, blank_s, tracks):
    samples = _piece(-blank_s - 20, 0.0, n=5)
    samples += _piece(-blank_s + 5, 0.0, n=blank_s // 5, baro_altitude=None)
    samples += _piece(3600, _lon(480.0))
    assert n_tracks(make_track(spark, samples)) == tracks
    row = _hole_row(spark, samples)
    if tracks == 2:
        assert row["alt_before_ft"] is None


def test_no_rate_in_the_window_stays_neutral(spark):
    samples = _hole_samples(before={"vert_rate": None}, after={"vert_rate": None})
    assert n_tracks(make_track(spark, samples)) == 1
    row = _hole_row(spark, samples)
    assert row["vr_before_fpm"] is None
    assert row["vr_before_samples"] == 0
    assert row["vr_after_samples"] == 0


def test_a_rate_past_the_window_is_not_read(spark):
    """Five minutes on each side. Read, these two would be a descent then a climb, and split."""
    down, up = -1500.0 / FPM_PER_MPS, 1500.0 / FPM_PER_MPS
    new_edge = 191 * 60
    samples = _piece(-320, 0.0, n=3, vert_rate=down)
    samples += _piece(-300, 0.0, n=61, vert_rate=None)
    samples += _piece(new_edge, _lon(1530.0), n=61, vert_rate=None)
    samples += _piece(new_edge + 305, _lon(1530.0), n=3, vert_rate=up)
    assert n_tracks(make_track(spark, samples)) == 1
    row = _hole_row(spark, samples)
    assert row["vr_before_samples"] == 0
    assert row["vr_after_samples"] == 0


def test_an_old_edge_blank_for_285_s_with_cruise_before_it_links(spark):
    """Deliberate: inside the window altitude can be nearly five minutes old and still count.

    The report counts these as ``linked_altitude_read_past_blank_old``.
    """
    samples = _piece(-345, 0.0, n=12) + _piece(-285, 0.0, n=58, baro_altitude=None)
    samples += _piece(191 * 60, _lon(1530.0))
    assert n_tracks(make_track(spark, samples)) == 1
    row = _hole_row(spark, samples)
    assert row["alt_before_ft"] == pytest.approx(CRUISE_M * FT_PER_M)
    assert row["alt_before_read_past_blank"] is True
    assert row["alt_after_read_past_blank"] is False


NAN = float("nan")


def test_nan_altitude_everywhere_splits(spark):
    """NaN passes ``>=`` in Spark, so counted as a value it would link on no altitude at all."""
    samples = _hole_samples(after={"baro_altitude": NAN})
    assert n_tracks(make_track(spark, samples)) == 2
    assert _hole_row(spark, samples)["alt_after_ft"] is None


def test_nan_altitudes_do_not_hide_a_low_value(spark):
    samples = _one_hz_after_hole(8, {"baro_altitude": 3000.0 / FT_PER_M}, baro_altitude=NAN)
    assert n_tracks(make_track(spark, samples)) == 2
    row = _hole_row(spark, samples)
    assert row["alt_after_ft"] == pytest.approx(3000.0)
    assert row["alt_after_read_past_blank"] is True


def test_nan_rates_do_not_pass_a_climb(spark):
    """NaN passes ``>`` too, and five of them would meet the three-rate requirement."""
    samples = _hole_samples(before={"baro_altitude": FL200, "vert_rate": NAN})
    assert n_tracks(make_track(spark, samples), _DIRECTIONAL) == 2
    row = _hole_row(spark, samples)
    assert row["vr_before_fpm"] is None
    assert row["vr_before_samples"] == 0


def test_nan_speeds_are_read_past(spark):
    """One NaN in a mean makes the mean NaN, so it must not take the place of a value."""
    samples = _piece(-45, 0.0, n=5) + _piece(-20, 0.0, n=5, velocity=NAN)
    samples += _piece(191 * 60, _lon(1530.0))
    assert _hole_row(spark, samples)["spd_before_kt"] == pytest.approx(480.0)


@pytest.mark.parametrize(
    "first, expected",
    [
        ({}, False),
        ({"baro_altitude": None}, True),
        ({"baro_altitude": NAN}, True),
    ],
    ids=["value at the edge", "blank at the edge", "nan at the edge"],
)
def test_the_new_edge_says_whether_its_altitude_is_the_edge_sample(spark, first, expected):
    samples = _piece(-45, 0.0) + _piece(3600, _lon(480.0), n=1, **first)
    samples += _piece(3605, _lon(480.0), n=9)
    assert _hole_row(spark, samples)["alt_after_read_past_blank"] is expected


def test_no_altitude_at_all_is_not_a_read_past_a_blank(spark):
    """Nothing was read. Such an edge fails its test, so it can never count as a link."""
    row = _hole_row(spark, _hole_samples(after={"baro_altitude": None}))
    assert row["alt_after_ft"] is None
    assert row["alt_after_read_past_blank"] is False


def test_a_value_in_the_edges_own_second_is_the_edge_sample(spark):
    """Two samples in one second have no order, so a blank twin is not a read past a blank."""
    samples = _piece(-45, 0.0) + _piece(3600, _lon(480.0), n=1, baro_altitude=None)
    samples += _piece(3600, _lon(480.0), n=10)
    assert _hole_row(spark, samples)["alt_after_read_past_blank"] is False


def test_each_field_finds_its_own_samples(spark):
    """On either side, no sample carries both, so a shared row bound would lose one of them."""
    up = 1500.0 / FPM_PER_MPS
    alt_ft = 30000.0
    only_rate = {"baro_altitude": None, "vert_rate": up, "velocity": None}
    only_alt = {"baro_altitude": alt_ft / FT_PER_M, "vert_rate": None, "velocity": None}
    only_speed = {"baro_altitude": None, "vert_rate": None}

    def side(order):
        return [dict(fields) for fields in order for _ in range(5)]

    near_first = side([only_rate, only_alt, only_speed])
    new_edge = 3600
    after = [
        {**s, "t": new_edge + i, "lat": 0.0, "lon": _lon(480.0), "callsign": "DLH1"}
        for i, s in enumerate(near_first)
    ]
    for s in after:
        s.setdefault("velocity", EDGE_MPS)
    before = [
        {**s, "t": -i, "lat": 0.0, "lon": 0.0, "callsign": "DLH1"} for i, s in enumerate(near_first)
    ]
    for s in before:
        s.setdefault("velocity", EDGE_MPS)
    samples = sorted(before, key=lambda s: s["t"]) + after
    row = _hole_row(spark, samples)
    assert row["alt_before_ft"] == pytest.approx(alt_ft)
    assert row["alt_after_ft"] == pytest.approx(alt_ft)
    assert row["vr_before_fpm"] == pytest.approx(1500.0)
    assert row["vr_before_samples"] == 5
    assert row["vr_after_fpm"] == pytest.approx(1500.0)
    assert row["vr_after_samples"] == 5
    assert row["spd_before_kt"] == pytest.approx(480.0)
    assert n_tracks(make_track(spark, samples)) == 1


def test_only_the_nearest_five_values_of_a_field_count(spark):
    """Blanks, five at cruise, then one low: the low one is the sixth value, not the sixth row."""
    samples = _piece(-45, 0.0)
    samples += _piece(3600, _lon(480.0), n=8, step_s=1, baro_altitude=None)
    samples += _piece(3608, _lon(480.0), n=5, step_s=1)
    samples += _piece(3613, _lon(480.0), n=1, baro_altitude=3000.0 / FT_PER_M)
    assert n_tracks(make_track(spark, samples)) == 1
    assert _hole_row(spark, samples)["alt_after_ft"] == pytest.approx(CRUISE_M * FT_PER_M)

    samples[10 + 12]["baro_altitude"] = 3000.0 / FT_PER_M  # now the fifth value
    assert n_tracks(make_track(spark, samples)) == 2


def test_samples_in_one_second_are_ordered_by_value_not_by_arrival(spark):
    """Six values in the old edge's last second, in two arrival orders, read the same."""
    low = 20000.0 / FT_PER_M
    tied = [{"baro_altitude": CRUISE_M}] * 5 + [{"baro_altitude": low}]
    base = _piece(0, 0.0, n=1)[0]
    new_piece = _piece(3600, _lon(480.0))
    forward = [{**base, **t} for t in tied] + new_piece
    backward = [{**base, **t} for t in reversed(tied)] + new_piece
    # The highest five, in any arrival order: a slice in arrival order would read the low one.
    assert _hole_row(spark, forward)["alt_before_ft"] == pytest.approx(CRUISE_M * FT_PER_M)
    assert _hole_row(spark, backward)["alt_before_ft"] == pytest.approx(CRUISE_M * FT_PER_M)


# --- climb and descent are separate settings ------------------------------------

FT_9000 = 9000.0 / FT_PER_M
LOW_CLIMB = {"baro_altitude": FT_9000, "vert_rate": 1500.0 / FPM_PER_MPS}
LOW_DESCENT = {"baro_altitude": FT_9000, "vert_rate": -1500.0 / FPM_PER_MPS}


def test_climb_edge_links_only_when_on(spark):
    df = _hole(spark, before=LOW_CLIMB)
    assert n_tracks(df) == 2
    assert n_tracks(df, _DESCENT_ONLY) == 2
    assert n_tracks(df, _CLIMB_ONLY) == 1


def test_descent_edge_links_only_when_on(spark):
    df = _hole(spark, after=LOW_DESCENT)
    assert n_tracks(df) == 2
    assert n_tracks(df, _CLIMB_ONLY) == 2
    assert n_tracks(df, _DESCENT_ONLY) == 1


def test_climb_on_does_not_allow_a_descent_edge(spark):
    assert n_tracks(_hole(spark, before=LOW_CLIMB, after=LOW_DESCENT), _CLIMB_ONLY) == 2
    assert n_tracks(_hole(spark, before=LOW_CLIMB, after=LOW_DESCENT), _DESCENT_ONLY) == 2
    assert n_tracks(_hole(spark, before=LOW_CLIMB, after=LOW_DESCENT), _DIRECTIONAL) == 1


@pytest.mark.parametrize("p", [_CLIMB_ONLY, _DIRECTIONAL], ids=["climb", "both"])
def test_a_low_new_edge_still_splits_with_climb_on(spark, p):
    """Below 5,000 ft after the hole: the unchanged low-altitude rule breaks there."""
    low = {"baro_altitude": 3000.0 / FT_PER_M, "vert_rate": -1500.0 / FPM_PER_MPS}
    assert n_tracks(_hole(spark, before=LOW_CLIMB, after=low), p) == 2


# --- `linkable`, as a pure function of the features ---------------------------


_DOUBLES = {
    "gap_min": 191.0,
    "position_gap_min": 191.0,
    "gc_nm": 1530.0,
    "alt_before_ft": 36000.0,
    "alt_after_ft": 36000.0,
    "vr_before_fpm": 0.0,
    "vr_after_fpm": 0.0,
    "vr_before_samples": 5.0,
    "vr_after_samples": 5.0,
    "spd_before_kt": 480.0,
}
_STRINGS = {"cs_before": "DLH1", "cs_after": "DLH1", "icao24": "abc123"}


def _decide(spark, p=P, **overrides):
    f = {
        **{k: F.lit(overrides.get(k, v)).cast("double") for k, v in _DOUBLES.items()},
        **{k: F.lit(overrides.get(k, v)).cast("string") for k, v in _STRINGS.items()},
    }
    return spark.range(1).select(linkable(f, p).alias("x")).first()["x"]


@pytest.mark.parametrize(
    "overrides, expected",
    [
        ({}, True),
        # Gap and implied speed, gap 60 min so implied speed is gc_nm. Inclusive.
        ({"gap_min": 360.0, "position_gap_min": 360.0, "gc_nm": 2880.0}, True),
        ({"gap_min": 360.5, "position_gap_min": 360.5, "gc_nm": 2880.0}, False),
        ({"gap_min": 60.0, "position_gap_min": 60.0, "gc_nm": 315.0}, True),
        ({"gap_min": 60.0, "position_gap_min": 60.0, "gc_nm": 314.9}, False),
        ({"gap_min": 60.0, "position_gap_min": 60.0, "gc_nm": 750.0}, True),
        ({"gap_min": 60.0, "position_gap_min": 60.0, "gc_nm": 750.1}, False),
        # Cruise floor, inclusive.
        ({"alt_before_ft": 25000.0}, True),
        ({"alt_before_ft": 24999.0}, False),
        ({"alt_after_ft": 25000.0}, True),
        ({"alt_after_ft": 24999.0}, False),
        # Descend then climb: strict, both sides, and only in that order.
        ({"vr_before_fpm": -500.0, "vr_after_fpm": 500.0}, True),
        ({"vr_before_fpm": -501.0, "vr_after_fpm": 501.0}, False),
        ({"vr_before_fpm": 501.0, "vr_after_fpm": -501.0}, True),
        ({"vr_before_fpm": -501.0, "vr_after_fpm": 0.0}, True),
        # Callsigns.
        ({"cs_after": "BAW2"}, False),
        ({"icao24": "000000"}, False),
        ({"icao24": "000001"}, False),
        ({"icao24": "ffffff"}, False),
        ({"icao24": "FFFFFF"}, False),
        # A gap of zero cannot imply a speed, and must not read as linkable.
        ({"position_gap_min": 0.0}, False),
    ],
)
def test_linkable_on_literal_features(spark, overrides, expected):
    assert _decide(spark, **overrides) is expected


@pytest.mark.parametrize(
    "name", ["gap_min", "position_gap_min", "gc_nm", "alt_before_ft", "alt_after_ft", "icao24"]
)
def test_a_null_required_input_is_not_linkable(spark, name):
    assert _decide(spark, **{name: None}) is False


@pytest.mark.parametrize(
    "overrides",
    [
        {"vr_before_fpm": None},
        {"vr_after_fpm": None},
        {"vr_before_fpm": None, "vr_after_fpm": None},
        {"cs_before": None},
        {"cs_after": None},
        {"cs_before": None, "cs_after": None},
        {"spd_before_kt": None},
    ],
)
def test_a_null_optional_input_is_neutral(spark, overrides):
    assert _decide(spark, **overrides) is True


def test_edge_speed_is_required_once_the_ratio_is_on(spark):
    on = dataclasses.replace(P, link_min_speed_ratio=0.85)
    assert _decide(spark, on) is True
    assert _decide(spark, on, spd_before_kt=None) is False


# --- link_checks: one named verdict per test of the rule --------------------------


_CHECK_NAMES = [
    "gap_not_too_long",
    "cruise_at_old_edge",
    "cruise_at_new_edge",
    "not_descend_then_climb",
    "callsign_agrees",
    "implied_speed_not_too_slow",
    "implied_speed_not_impossible",
    "not_slow_for_this_flight",
    "no_time_for_a_stop",
    "real_address",
]

#: A hole every test accepts at the defaults: 191 min at 480 kt.
_GOOD_HOLE = {
    "gap_min": 191.0,
    "position_gap_min": 191.0,
    "gc_nm": 1528.0,
    "alt_before_ft": 36000.0,
    "alt_after_ft": 36000.0,
    "vr_before_fpm": 0.0,
    "vr_after_fpm": 0.0,
    "vr_before_samples": 5.0,
    "vr_after_samples": 5.0,
    "spd_before_kt": 480.0,
    "cs_before": "DLH1",
    "cs_after": "DLH1",
    "icao24": "abc123",
}

_RATIO_ON = dataclasses.replace(P, link_min_speed_ratio=0.85)
_STOP_ON = dataclasses.replace(P, link_min_stop_minutes=20.0)
_DIRECTIONAL_RATIO_ON = dataclasses.replace(_RATIO_ON, **_BOTH_DIRECTIONS)
_CLIMB_EDGE = {"alt_before_ft": 20000.0, "vr_before_fpm": 1500.0}

#: ``(settings, what differs from the good hole, the tests that must fail)``.
_CHECK_CASES = [
    (P, {}, []),
    (P, {"gap_min": 400.0, "position_gap_min": 400.0, "gc_nm": 3200.0}, ["gap_not_too_long"]),
    # The length limit reads the sample gap, and only that.
    (P, {"gap_min": 360.0, "position_gap_min": 365.0, "gc_nm": 2920.0}, []),
    (P, {"gap_min": 361.0, "position_gap_min": 361.0, "gc_nm": 2888.0}, ["gap_not_too_long"]),
    (P, {"gap_min": None}, ["gap_not_too_long"]),
    # The speed tests read the position gap, and only that: 1,528 NM in 382 min is 240 kt.
    (P, {"position_gap_min": 382.0}, ["implied_speed_not_too_slow"]),
    (P, {"alt_before_ft": 20000.0}, ["cruise_at_old_edge"]),
    (P, {"alt_before_ft": None}, ["cruise_at_old_edge"]),
    (P, {"alt_after_ft": 20000.0}, ["cruise_at_new_edge"]),
    (P, {"alt_after_ft": None}, ["cruise_at_new_edge"]),
    (P, {"vr_before_fpm": -1000.0, "vr_after_fpm": 1000.0}, ["not_descend_then_climb"]),
    (P, {"vr_before_fpm": None, "vr_after_fpm": 1000.0}, []),
    (P, {"vr_before_fpm": -1000.0, "vr_after_fpm": None}, []),
    (P, {"cs_after": "BAW2"}, ["callsign_agrees"]),
    (P, {"cs_before": None}, []),
    (P, {"cs_before": None, "cs_after": None}, []),
    (P, {"gc_nm": 100.0}, ["implied_speed_not_too_slow"]),
    (P, {"gc_nm": 5000.0}, ["implied_speed_not_impossible"]),
    (P, {"gc_nm": None}, ["implied_speed_not_too_slow", "implied_speed_not_impossible"]),
    # Two samples in one second: no speed can be implied, and the division is guarded.
    (P, {"position_gap_min": 0.0}, ["implied_speed_not_too_slow", "implied_speed_not_impossible"]),
    (P, {"position_gap_min": None}, ["implied_speed_not_too_slow", "implied_speed_not_impossible"]),
    (P, {"icao24": "000000"}, ["real_address"]),
    (P, {"icao24": "FFFFFF"}, ["real_address"]),
    (P, {"icao24": None}, ["real_address"]),
    # Off at 0.0, so a missing or high speed cannot fail them.
    (P, {"spd_before_kt": None}, []),
    (P, {"spd_before_kt": 900.0}, []),
    (_RATIO_ON, {}, []),
    (_RATIO_ON, {"spd_before_kt": 900.0}, ["not_slow_for_this_flight"]),
    (_RATIO_ON, {"spd_before_kt": None}, ["not_slow_for_this_flight"]),
    # 1,528 NM in 230 min is 399 kt: over the floor, under 0.85 x 480 kt.
    (_RATIO_ON, {"position_gap_min": 230.0}, ["not_slow_for_this_flight"]),
    # 191 min at 480 kt takes 141 min at 650 kt, which leaves 50 min for a stop.
    (_STOP_ON, {}, ["no_time_for_a_stop"]),
    (_STOP_ON, {"gc_nm": 1960.0}, []),
    # The same distance with 215 min between positions leaves 34 min, room for a stop.
    (_STOP_ON, {"gc_nm": 1960.0, "position_gap_min": 215.0}, ["no_time_for_a_stop"]),
    (
        _STOP_ON,
        {"gc_nm": None},
        [
            "implied_speed_not_too_slow",
            "implied_speed_not_impossible",
            "no_time_for_a_stop",
        ],
    ),
    (
        _STOP_ON,
        {"gc_nm": 1960.0, "position_gap_min": None},
        [
            "implied_speed_not_too_slow",
            "implied_speed_not_impossible",
            "no_time_for_a_stop",
        ],
    ),
    # Several at once: every failing test says so.
    (P, {"alt_after_ft": 20000.0, "cs_after": "BAW2"}, ["cruise_at_new_edge", "callsign_agrees"]),
    # Off, direction buys nothing, and the edge feature it would read is not needed.
    (P, {"alt_before_ft": 20000.0, "vr_before_fpm": 1500.0}, ["cruise_at_old_edge"]),
    (P, {"alt_after_ft": 20000.0, "vr_after_fpm": -1500.0}, ["cruise_at_new_edge"]),
    # On, an edge below the floor passes only by the direction that fits one flight.
    (_DIRECTIONAL, {}, []),
    (_DIRECTIONAL, {"alt_before_ft": 20000.0, "vr_before_fpm": 1500.0}, []),
    (_DIRECTIONAL, {"alt_after_ft": 20000.0, "vr_after_fpm": -1500.0}, []),
    (_DIRECTIONAL, {"alt_before_ft": 20000.0, "vr_before_fpm": 500.0}, ["cruise_at_old_edge"]),
    (_DIRECTIONAL, {"alt_after_ft": 20000.0, "vr_after_fpm": -500.0}, ["cruise_at_new_edge"]),
    (_DIRECTIONAL, {"alt_before_ft": 20000.0, "vr_before_fpm": -1500.0}, ["cruise_at_old_edge"]),
    (_DIRECTIONAL, {"alt_after_ft": 20000.0, "vr_after_fpm": 1500.0}, ["cruise_at_new_edge"]),
    # The direction is the evidence here, so its absence is not neutral.
    (_DIRECTIONAL, {"alt_before_ft": 20000.0, "vr_before_fpm": None}, ["cruise_at_old_edge"]),
    (_DIRECTIONAL, {"alt_after_ft": 20000.0, "vr_after_fpm": None}, ["cruise_at_new_edge"]),
    # At cruise it stays neutral, as it always was.
    (_DIRECTIONAL, {"vr_before_fpm": None, "vr_after_fpm": None}, []),
    # A missing altitude is not below anything.
    (_DIRECTIONAL, {"alt_before_ft": None, "vr_before_fpm": 1500.0}, ["cruise_at_old_edge"]),
    (_DIRECTIONAL, {"alt_after_ft": None, "vr_after_fpm": -1500.0}, ["cruise_at_new_edge"]),
    # The low-altitude gap can break here whatever the verdict, so it cannot read as
    # linked. The limit is read on the lowest of the edge's values, and is strict.
    (_DIRECTIONAL, {"alt_after_ft": 4000.0, "vr_after_fpm": -1500.0}, ["cruise_at_new_edge"]),
    (_DIRECTIONAL, {"alt_after_ft": 5000.0, "vr_after_fpm": -1500.0}, []),
    # A climb at the old edge and a descent at the new one, which is the whole flight's
    # shape, and descend-then-climb still applies unchanged.
    (
        _DIRECTIONAL,
        {
            "alt_before_ft": 9000.0,
            "vr_before_fpm": 1500.0,
            "alt_after_ft": 9000.0,
            "vr_after_fpm": -1500.0,
        },
        [],
    ),
    (_DIRECTIONAL, {"vr_before_fpm": -1500.0, "vr_after_fpm": 1500.0}, ["not_descend_then_climb"]),
    # One rate of five cannot carry a direction: three are needed, on that edge's frame.
    (_DIRECTIONAL, {**_CLIMB_EDGE, "vr_before_samples": 3.0}, []),
    (_DIRECTIONAL, {**_CLIMB_EDGE, "vr_before_samples": 2.0}, ["cruise_at_old_edge"]),
    (_DIRECTIONAL, {"alt_after_ft": 20000.0, "vr_after_fpm": -1500.0, "vr_after_samples": 3.0}, []),
    (
        _DIRECTIONAL,
        {"alt_after_ft": 20000.0, "vr_after_fpm": -1500.0, "vr_after_samples": 2.0},
        ["cruise_at_new_edge"],
    ),
    (_DIRECTIONAL, {**_CLIMB_EDGE, "vr_after_samples": 0.0}, []),
    # At cruise the count does not matter: rates stay neutral there.
    (_DIRECTIONAL, {"vr_before_samples": 0.0, "vr_after_samples": 0.0}, []),
    # A climb's speed says nothing about cruise, so the ratio is not read at a climb edge
    # and the speed floor stands in. 900 kt would fail 0.85 x edge speed at any implied speed.
    (_DIRECTIONAL_RATIO_ON, {**_CLIMB_EDGE, "spd_before_kt": 900.0}, []),
    (_DIRECTIONAL_RATIO_ON, {**_CLIMB_EDGE, "spd_before_kt": None}, []),
    (
        _DIRECTIONAL_RATIO_ON,
        {**_CLIMB_EDGE, "gc_nm": 100.0},
        ["implied_speed_not_too_slow", "not_slow_for_this_flight"],
    ),
    # An old edge at cruise is still read against its own speed, direction on or not.
    (_DIRECTIONAL_RATIO_ON, {"spd_before_kt": 900.0}, ["not_slow_for_this_flight"]),
    (
        _DIRECTIONAL_RATIO_ON,
        {"alt_after_ft": 20000.0, "vr_after_fpm": -1500.0, "spd_before_kt": 900.0},
        ["not_slow_for_this_flight"],
    ),
    # And with direction off, a slow edge below the floor gets no such allowance.
    (
        _RATIO_ON,
        {**_CLIMB_EDGE, "spd_before_kt": 900.0},
        ["cruise_at_old_edge", "not_slow_for_this_flight"],
    ),
]


def _feature_literals(hole):
    types = {"cs_before": "string", "cs_after": "string", "icao24": "string"}
    return {name: F.lit(value).cast(types.get(name, "double")) for name, value in hole.items()}


@pytest.mark.parametrize("p, changes, failing", _CHECK_CASES)
def test_link_checks_name_each_failing_test_and_linkable_is_their_and(spark, p, changes, failing):
    f = _feature_literals({**_GOOD_HOLE, **changes})
    checks = link_checks(f, p)
    row = (
        spark.range(1)
        .select(*[c.alias(name) for name, c in checks.items()], linkable(f, p).alias("linked"))
        .first()
    )
    assert list(checks) == _CHECK_NAMES
    verdicts = {name: row[name] for name in _CHECK_NAMES}
    assert all(v is True or v is False for v in verdicts.values()), verdicts
    assert [name for name, ok in verdicts.items() if not ok] == failing
    assert row["linked"] is (not failing)


# --- the shipped defaults (chosen by the track-construction-v4 study) -------


def test_shipped_defaults_link_what_the_pre_study_settings_split(spark):
    """A 20,000 ft edge clears the 18,000 ft floor; an 8 h hole is inside 10 h."""
    fl200 = 20000.0 / FT_PER_M
    df = _hole(spark, before={"baro_altitude": fl200})
    assert n_tracks(df, P) == 2
    assert n_tracks(df, SHIPPED) == 1
    long_hole = _hole(spark, gap_min=480.0, nm=480.0 * 8)
    assert n_tracks(long_hole, P) == 2
    assert n_tracks(long_hole, SHIPPED) == 1


def test_shipped_default_speed_floor_is_400_kt(spark):
    """350 kt across the hole linked at 315 kt; it is a split at 400 kt."""
    df = _hole(spark, nm=350.0 * 191 / 60)
    assert n_tracks(df, P) == 1
    assert n_tracks(df, SHIPPED) == 2


# --- what the audit of the rule found ----------------------------------------------


def test_edge_samples_within_one_second_are_taken_in_time_order(spark):
    """Two samples 0.6 s apart; the later one is at 17,000 ft, under the floor.

    The five nearest the old edge are those at 0, -1, -2 and -3 s and the *later* of the
    pair, so the edge reads 17,000 ft and the hole splits. The edge used to order the
    pair by altitude and keep the earlier, higher one. Sub-second instants exist only
    with ADS-C merged.
    """
    high = 19000.0 / FT_PER_M
    one = _piece(0, 0.0, n=1)[0]
    old = [
        {**one, "t": -3.8, "baro_altitude": high},
        {**one, "t": -3.2, "baro_altitude": 17000.0 / FT_PER_M},
    ] + [{**one, "t": float(t), "baro_altitude": high} for t in (-3, -2, -1, 0)]
    earlier_low = [dict(s) for s in old]
    earlier_low[0]["baro_altitude"], earlier_low[1]["baro_altitude"] = (
        earlier_low[1]["baro_altitude"],
        earlier_low[0]["baro_altitude"],
    )
    after = _piece(191 * 60, _lon(1530.0))
    no_climb = dataclasses.replace(SHIPPED, link_old_edge_climb=False)

    assert n_tracks(make_track(spark, old + after), no_climb) == 2
    # The low sample now the sixth nearest: it is not among the five, and the hole links.
    assert n_tracks(make_track(spark, earlier_low + after), no_climb) == 1


@pytest.mark.parametrize("coordinate", ["lat", "lon"])
def test_a_nan_position_is_read_past_like_a_blank_one(spark, coordinate):
    """NaN is no value for altitude, rate and speed, and is none for a position either."""
    samples = _hole_samples()
    samples[10][coordinate] = float("nan")
    assert n_tracks(make_track(spark, samples), SHIPPED) == 1
    samples[9][coordinate] = float("nan")
    assert n_tracks(make_track(spark, samples), SHIPPED) == 1
