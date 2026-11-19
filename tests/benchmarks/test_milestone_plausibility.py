"""What the aircraft was doing at each event's time, read from its own track."""

import datetime as dt

import milestone_plausibility as mp
import pytest
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

T0 = dt.datetime(2026, 6, 14, 10, 0, 0)

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("track_id", StringType()),
        StructField("type", StringType()),
        StructField("event_time", TimestampType()),
        StructField("apt_icao", StringType()),
        StructField("adep", StringType()),
        StructField("ades", StringType()),
    ]
)

TRACK_SCHEMA = StructType(
    [
        StructField("track_id", StringType()),
        StructField("event_time", TimestampType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("baro_altitude_c", DoubleType()),
        StructField("velocity", DoubleType()),
        StructField("vert_rate", DoubleType()),
        StructField("on_ground", BooleanType()),
    ]
)

AIRPORT_SCHEMA = StructType(
    [
        StructField("ident", StringType()),
        StructField("latitude_deg", DoubleType()),
        StructField("longitude_deg", DoubleType()),
        StructField("elevation_ft", IntegerType()),
    ]
)

#: A low field and a high field one degree of latitude (60 NM) apart.
AIRPORTS = [("LOW", 50.0, 4.0, 100), ("HIGH", 51.0, 4.0, 5000)]


def _t(seconds):
    return T0 + dt.timedelta(seconds=seconds)


def _event(spark, type_="AOBT", at=0, apt=None, adep="LOW", ades="HIGH", track="t1"):
    return spark.createDataFrame([("e1", track, type_, _t(at), apt, adep, ades)], EVENT_SCHEMA)


def _tracks(spark, rows):
    return spark.createDataFrame(rows, TRACK_SCHEMA)


def _airports(spark):
    return spark.createDataFrame(AIRPORTS, AIRPORT_SCHEMA)


def _report(at, alt_m, lat=50.0, gs_ms=0.0, vr_ms=0.0, on_ground=False, track="t1"):
    return (track, _t(at), lat, 4.0, alt_m, gs_ms, vr_ms, on_ground)


def _context(spark, events, tracks, window_s=60):
    rows = mp.event_context(events, _tracks(spark, tracks), _airports(spark), window_s).collect()
    assert len(rows) == 1
    return rows[0]


def test_the_nearer_report_gives_the_context(spark):
    # The event is 10 s after the first report and 20 s before the second.
    row = _context(
        spark,
        _event(spark, at=10),
        [_report(0, 100.0, gs_ms=5.0), _report(30, 1000.0, gs_ms=100.0)],
    )
    assert row.ctx_gap_s == pytest.approx(10.0)
    assert row.ctx_height_ft == pytest.approx(100.0 * mp.FT_PER_M - 100.0)
    assert row.ctx_speed_kt == pytest.approx(5.0 * mp.KT_PER_MPS)


def test_units_are_feet_knots_and_feet_per_minute(spark):
    row = _context(spark, _event(spark), [_report(0, 1000.0, gs_ms=50.0, vr_ms=5.0)])
    assert row.ctx_altitude_ft == pytest.approx(1000.0 * 3.28084)
    assert row.ctx_speed_kt == pytest.approx(50.0 * 1.94384)
    assert row.ctx_vertical_rate_ft_min == pytest.approx(5.0 * 196.850394)


def test_no_report_within_the_window_gives_null_context(spark):
    row = _context(spark, _event(spark, at=0), [_report(200, 1000.0)], window_s=60)
    assert row.event_id == "e1"
    assert row.ctx_gap_s is None
    assert row.ctx_height_ft is None
    assert row.ctx_speed_kt is None
    assert row.ctx_on_ground is None


def test_a_report_of_another_track_is_not_used(spark):
    row = _context(spark, _event(spark), [_report(0, 1000.0, track="other")])
    assert row.ctx_gap_s is None


def test_a_report_without_altitude_still_gives_speed_and_on_ground(spark):
    # A surface report carries no altitude: the height is unknown, not zero.
    row = _context(spark, _event(spark), [_report(0, None, gs_ms=3.0, on_ground=True)])
    assert row.ctx_height_ft is None
    assert row.ctx_speed_kt == pytest.approx(3.0 * mp.KT_PER_MPS)
    assert row.ctx_on_ground == pytest.approx(1.0)


def test_height_falls_back_to_the_nearest_report_that_has_an_altitude(spark):
    row = _context(
        spark,
        _event(spark, at=0),
        [_report(0, None, on_ground=True), _report(20, 50.0)],
    )
    assert row.ctx_gap_s == pytest.approx(0.0)
    assert row.ctx_height_ft == pytest.approx(50.0 * mp.FT_PER_M - 100.0)


def test_on_ground_is_the_share_of_reports_flagged_in_the_window(spark):
    row = _context(
        spark,
        _event(spark, at=0),
        [
            _report(-20, 10.0, on_ground=True),
            _report(0, 10.0, on_ground=True),
            _report(20, 10.0, on_ground=False),
            _report(40, 10.0, on_ground=False),
        ],
    )
    assert row.ctx_on_ground == pytest.approx(0.5)


def test_the_airport_elevation_is_subtracted(spark):
    # 1,000 m is 3,281 ft: 5,000 ft fields lie above it.
    row = _context(spark, _event(spark, type_="AIBT", at=0), [_report(0, 1000.0, lat=51.0)])
    assert row.ctx_airport == "HIGH"
    assert row.ctx_height_ft == pytest.approx(1000.0 * mp.FT_PER_M - 5000.0)
    assert row.ctx_airport_distance_nm == pytest.approx(0.0, abs=1e-6)


def test_departure_side_types_use_the_departure_airport(spark):
    for type_ in ("AOBT", "ATOT", "take-off-roll", "line-up"):
        row = _context(spark, _event(spark, type_=type_), [_report(0, 1000.0, lat=51.0)])
        assert row.ctx_airport == "LOW", type_
        assert row.ctx_airport_distance_nm == pytest.approx(60.0, rel=0.01)


def test_arrival_side_types_use_the_destination_airport(spark):
    # ``landing`` is the threshold crossing in event tables written before it
    # was named ``xing-runway-threshold``; both names are on the arrival side.
    for type_ in ("AIBT", "ALDT", "xing-runway-threshold", "landing", "runway-vacated"):
        row = _context(spark, _event(spark, type_=type_), [_report(0, 1000.0, lat=50.0)])
        assert row.ctx_airport == "HIGH", type_


def test_the_airport_named_by_the_event_comes_first(spark):
    row = _context(spark, _event(spark, type_="ATOT", apt="HIGH"), [_report(0, 1000.0)])
    assert row.ctx_airport == "HIGH"


def test_a_surface_event_uses_the_nearer_of_the_two_airports(spark):
    near_high = _context(
        spark, _event(spark, type_="entry-taxiway"), [_report(0, 1600.0, lat=50.9)]
    )
    assert near_high.ctx_airport == "HIGH"
    near_low = _context(spark, _event(spark, type_="entry-taxiway"), [_report(0, 40.0, lat=50.1)])
    assert near_low.ctx_airport == "LOW"


def test_an_unknown_airport_leaves_the_height_unknown(spark):
    row = _context(spark, _event(spark, type_="AOBT", adep=None, ades=None), [_report(0, 1000.0)])
    assert row.ctx_airport is None
    assert row.ctx_height_ft is None
    assert row.ctx_altitude_ft == pytest.approx(1000.0 * mp.FT_PER_M)
    assert row.ctx_airport_distance_nm is None


def test_every_event_is_kept_once(spark):
    events = spark.createDataFrame(
        [
            ("e1", "t1", "AOBT", _t(0), None, "LOW", "HIGH"),
            ("e2", "t1", "ATOT", _t(5), None, "LOW", "HIGH"),
            ("e3", "t2", "AIBT", _t(0), None, "LOW", "HIGH"),
        ],
        EVENT_SCHEMA,
    )
    tracks = [_report(s, 10.0) for s in range(-60, 61, 5)]
    out = mp.event_context(events, _tracks(spark, tracks), _airports(spark), 60)
    ids = sorted(r.event_id for r in out.collect())
    assert ids == ["e1", "e2", "e3"]


# ---------------------------------------------------------------------------
# The candidate checks (pandas, no Spark)
# ---------------------------------------------------------------------------


def _frame(rows):
    import pandas as pd

    base = {
        "track_id": "t1",
        "type": "AOBT",
        "event_time": pd.Timestamp("2026-06-14 10:00:00"),
        "ctx_height_ft": None,
        "ctx_altitude_ft": None,
        "ctx_speed_kt": None,
        "ctx_on_ground": None,
        "ev_airport_distance_nm": None,
        "error_s": None,
        "trk_first_time": pd.Timestamp("2026-06-14 09:00:00"),
        "trk_last_time": pd.Timestamp("2026-06-14 12:00:00"),
    }
    df = pd.DataFrame([{**base, **r} for r in rows])
    for c in ("ctx_height_ft", "ctx_altitude_ft", "ctx_speed_kt", "ctx_on_ground", "error_s"):
        df[c] = df[c].astype(float)
    return mp.prepare(df)


AIRBORNE = mp.CHOSEN["airborne_ground_event"]


def test_a_stored_landing_is_read_as_the_threshold_crossing():
    """Tables written before the rename call the threshold crossing ``landing``;
    the study reads it under its current name, so the sequence checks find it."""
    import pandas as pd

    df = _frame(
        [
            {"type": "landing", "event_time": pd.Timestamp("2026-06-14 11:00:00")},
            {"type": "ALDT", "event_time": pd.Timestamp("2026-06-14 11:00:07")},
        ]
    )
    assert set(df["type"]) == {"xing-runway-threshold", "ALDT"}
    assert df["trk_xing-runway-threshold_s"].notna().all()


def test_a_stored_landing_of_the_v002_settings_keeps_its_name():
    """With the v0.0.2 settings ``landing`` is the end of the descent, stamped
    with the v0.0.2 events version; only the default settings' ``landing`` (and
    a context written without the version, read from the default-settings
    study tables) is the threshold crossing."""
    import pandas as pd

    from opdi.versions import EVENTS_VERSION, LEGACY_EVENTS_VERSION

    t = pd.Timestamp("2026-06-14 11:00:00")
    df = _frame(
        [
            {"type": "landing", "event_time": t, "version": LEGACY_EVENTS_VERSION},
            {"type": "landing", "event_time": t, "version": EVENTS_VERSION},
            {"type": "ALDT", "event_time": t, "version": EVENTS_VERSION},
        ]
    )
    assert list(df["type"]) == ["landing", "xing-runway-threshold", "ALDT"]


def test_an_off_block_time_in_flight_is_removed_and_its_neighbour_kept():
    df = _frame(
        [
            {"ctx_height_ft": 1501.0, "ctx_speed_kt": 81.0, "ctx_on_ground": 0.0},
            {"ctx_height_ft": 1499.0, "ctx_speed_kt": 81.0, "ctx_on_ground": 0.0},
            {"ctx_height_ft": 1501.0, "ctx_speed_kt": 79.0, "ctx_on_ground": 0.0},
        ]
    )
    assert mp.rule_masks(df, *AIRBORNE).tolist() == [True, False, False]


def test_unknown_context_keeps_the_event():
    df = _frame([{}, {"ctx_speed_kt": 300.0, "ctx_on_ground": 0.0}])
    assert not mp.rule_masks(df, *AIRBORNE).any()


def test_a_report_flagged_on_the_ground_keeps_the_event():
    # A garbled report at a stand: high and fast, but the same window says ground.
    df = _frame([{"ctx_height_ft": 3700.0, "ctx_speed_kt": 208.0, "ctx_on_ground": 0.7}])
    assert not mp.rule_masks(df, *AIRBORNE).any()


def test_an_unknown_airport_needs_an_altitude_above_every_airport():
    df = _frame(
        [
            {"ctx_altitude_ft": 30000.0, "ctx_speed_kt": 450.0, "ctx_on_ground": 0.0},
            {"ctx_altitude_ft": 9000.0, "ctx_speed_kt": 250.0, "ctx_on_ground": 0.0},
        ]
    )
    assert mp.rule_masks(df, *AIRBORNE).tolist() == [True, False]


def test_the_runway_family_is_not_checked_against_the_track_height():
    df = _frame(
        [{"type": "line-up", "ctx_height_ft": 33000.0, "ctx_speed_kt": 150.0, "ctx_on_ground": 0.0}]
    )
    assert not mp.rule_masks(df, *AIRBORNE).any()


def test_block_times_on_the_wrong_side_of_the_runway_times_are_removed():
    import pandas as pd

    t = pd.Timestamp("2026-06-14 10:00:00")
    df = _frame(
        [
            {"type": "ATOT", "event_time": t},
            {"type": "AOBT", "event_time": t + pd.Timedelta(seconds=60)},
            {"type": "ALDT", "event_time": t + pd.Timedelta(hours=1)},
            {"type": "AIBT", "event_time": t + pd.Timedelta(minutes=59)},
            {"type": "AOBT", "event_time": t - pd.Timedelta(minutes=10), "track_id": "t2"},
        ]
    )
    after = mp.rule_masks(df, *mp.CHOSEN["block_after_takeoff"])
    before = mp.rule_masks(df, *mp.CHOSEN["block_before_landing"])
    assert after.tolist() == [False, True, False, False, False]
    assert before.tolist() == [False, False, False, True, False]


def test_an_event_without_a_time_is_removed():
    import pandas as pd

    df = _frame([{"type": "xing-40nm", "event_time": pd.NaT}, {"type": "xing-40nm"}])
    assert mp.rule_masks(df, *mp.CHOSEN["missing_time"]).tolist() == [True, False]
