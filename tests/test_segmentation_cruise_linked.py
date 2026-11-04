"""A10 `cruise_linked`: one track_id across an en-route reception hole.

A10 is A9 `debounced` with one change: a gap over ``gap_minutes`` breaks only when
the two edges cannot be one flight. A wrongly glued flight is worse than a split
one, so unclear evidence means split. Later study stages score the link decision
directly, which is why it is a pure function of named edge features and why some
tests below drive ``linkable`` with literals.
"""
import dataclasses
import math

import pytest
from conftest import make_track
from pyspark.sql import Window
from pyspark.sql import functions as F

from opdi.config import SegmentationConfig
from opdi.pipeline.flights import EARTH_R_NM
from opdi.pipeline.segmentation import SegmentationParams, assign_track_id
from opdi.pipeline.segmentation.base import FPM_PER_MPS, FT_PER_M, KT_PER_MPS
from opdi.pipeline.segmentation.methods import (
    ARMS,
    LINK_EDGE_SAMPLES,
    PLACEHOLDER_ICAO24,
    cruise_linked,
    debounced,
    debounced_callsign_change,
    link_feature_columns,
    linkable,
    recommended,
    stable_callsign,
)

P = SegmentationParams()

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
    return [
        {**base, **overrides, "t": start_s + i * step_s} for i in range(n)
    ]


def _hole_samples(gap_min=191.0, nm=1530.0, before=None, after=None,
                  n_before=10, n_after=10):
    """Two pieces of one airframe with a hole between their edges at t=0 and t=gap."""
    return (
        _piece(-(n_before - 1) * 5, 0.0, n_before, **(before or {}))
        + _piece(gap_min * 60, _lon(nm), n_after, **(after or {}))
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
                - F.unix_timestamp(
                    F.lag("_ts").over(Window.partitionBy("_grp").orderBy("_ts"))
                )
            )
            / 60.0,
        )
    )


def _with_features(df, **kwargs):
    frame = _engine_frame(df)
    features = link_feature_columns(P, **kwargs)
    return frame.select(
        "event_time", *[c.alias(name) for name, c in features.items()]
    )


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
    assert n_tracks(with_one_low(5)) == 2    # fifth before the gap
    assert n_tracks(with_one_low(4)) == 1    # sixth before the gap
    assert n_tracks(with_one_low(14)) == 2   # fifth from the gap on
    assert n_tracks(with_one_low(15)) == 1   # sixth from the gap on


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


def test_null_edge_position_fails_closed(spark):
    samples = _hole_samples()
    samples[9]["lat"] = None     # the old edge
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
    assert rows[0]["_brk"] is False     # the group's first sample has no gap at all


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
    samples += _piece(40, 0.0, n=5, step_s=2, callsign="XXX9")     # flicker, 8 s long
    samples += _piece(50, 0.0, n=8)
    samples += _piece(20 * 60, 0.1, n=8)                           # 20 min at cruise
    samples += _piece(40 * 60, 0.2, n=8, baro_altitude=1000.0)     # 20 min, low
    samples += _piece(50 * 60, 0.2, n=8, callsign="BAW2", baro_altitude=1000.0)
    df = make_track(spark, samples)
    assert _partition(df, cruise_linked()) == _partition(df, debounced())
    assert len(_partition(df, cruise_linked())) == 3


def test_debounced_partition_is_unchanged(spark):
    """The extraction, pinned by hand as well as by the unmodified debounce suite."""
    samples = _piece(0, 0.0, n=8, step_s=5)
    samples += _piece(40, 0.0, n=1, callsign="XXX9")                # flicker
    samples += _piece(45, 0.0, n=8, step_s=5)
    samples += _piece(85, 0.0, n=8, step_s=5, callsign="BAW2")      # genuine change
    samples += _piece(40 * 60, 1.0, n=8, step_s=5, callsign="BAW2")   # long gap
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
        "link_max_gap_minutes": 360.0,
        "link_cruise_floor_ft": 25000.0,
        "link_edge_vert_rate_fpm": 500.0,
        "link_callsign_lookahead_minutes": 10.0,
        "link_callsign_lookback_minutes": 60.0,
        "link_min_implied_speed_kt": 315.0,
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


# --- the features -----------------------------------------------------------


def test_features_at_the_first_sample_after_the_hole(spark):
    df = _hole(
        spark,
        before={"vert_rate": -500.0 / FPM_PER_MPS},
        after={"vert_rate": 1000.0 / FPM_PER_MPS, "callsign": "BAW2"},
    )
    row = _with_features(df).orderBy("event_time").collect()[10]

    assert row["gap_min"] == pytest.approx(191.0)
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
        return frame.select(
            "event_time", *[c.alias(k) for k, c in f.items()]
        ).orderBy("event_time").collect()

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
    b_samples[14]["callsign"] = "KLM8"     # t = -32
    b_samples += _piece(3, 0.0, n=1, callsign="BAW2", baro_altitude=9000.0)
    b_samples += _piece(191 * 60 + 4, _lon(1600.0), callsign="BAW2")

    names = list(link_feature_columns(P))

    def alone(samples):
        rows = _with_features(make_track(spark, samples)).orderBy("event_time")
        return rows.select(*names).collect()

    def shared(control):
        both = make_track(spark, a_samples).withColumn("control_id", F.lit("a")).unionByName(
            make_track(spark, b_samples).withColumn("control_id", F.lit("b"))
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


# --- `linkable`, as a pure function of the features ---------------------------


_DOUBLES = {
    "gap_min": 191.0,
    "gc_nm": 1530.0,
    "alt_before_ft": 36000.0,
    "alt_after_ft": 36000.0,
    "vr_before_fpm": 0.0,
    "vr_after_fpm": 0.0,
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
        ({"gap_min": 360.0, "gc_nm": 2880.0}, True),
        ({"gap_min": 360.5, "gc_nm": 2880.0}, False),
        ({"gap_min": 60.0, "gc_nm": 315.0}, True),
        ({"gap_min": 60.0, "gc_nm": 314.9}, False),
        ({"gap_min": 60.0, "gc_nm": 750.0}, True),
        ({"gap_min": 60.0, "gc_nm": 750.1}, False),
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
        ({"gap_min": 0.0}, False),
    ],
)
def test_linkable_on_literal_features(spark, overrides, expected):
    assert _decide(spark, **overrides) is expected


@pytest.mark.parametrize("name", ["gap_min", "gc_nm", "alt_before_ft", "alt_after_ft", "icao24"])
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
