"""Ground truth is windowed on the flight, not on the day it pushed back.

These tests drive the real ``load_flight_intervals`` over synthetic
``flights_*``/``apdf_*`` parquet written to ``tmp_path``, the same device
``test_track_truth.py`` already uses for its ``flight_key`` and containment
cases. The alternative -- exercising the day-filter expression on its own --
was rejected: the defect is an *ordering* between two filters (the ``day``
pre-filter runs before the ``t_off``/``t_land`` window), and a test that
evaluates either filter in isolation cannot see an ordering. Synthetic parquet
also lets a flight be placed to the minute either side of midnight, which the
committed ``reference/`` extracts cannot be asked to contain on demand.

Each flight below differs from its neighbour only in its times, so nothing but
window membership can explain one surviving and another not.
"""

import datetime as dt

from pyspark.sql import Row
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from track_truth import load_flight_intervals

_FLIGHTS_SCHEMA = StructType(
    [
        StructField("AIRCRAFT_ADDRESS", StringType(), True),
        StructField("AIRCRAFT_ID", StringType(), True),
        StructField("ADEP", StringType(), True),
        StructField("ADES", StringType(), True),
        StructField("AOBT_3", TimestampType(), True),
        StructField("ARVT_3", TimestampType(), True),
        StructField("TAXI_TIME_3", IntegerType(), True),
    ]
)

_APDF_SCHEMA = StructType(
    [
        StructField("AP_C_FLTID", StringType(), True),
        StructField("ADEP_ICAO", StringType(), True),
        StructField("ADES_ICAO", StringType(), True),
        StructField("SRC_PHASE", StringType(), True),
        StructField("MVT_TIME_UTC", TimestampType(), True),
        # The block time: AOBT on a DEP row, AIBT on an ARR row. None
        # throughout this file -- no test here exercises the ground phase --
        # but the column must exist, because `load_apdf_times` selects it and
        # the real extract always carries it.
        StructField("BLOCK_TIME_UTC", TimestampType(), True),
    ]
)


def _leg(icao24, callsign, aobt, arvt, taxi_min=10):
    return Row(
        AIRCRAFT_ADDRESS=icao24, AIRCRAFT_ID=callsign, ADEP="EBBR", ADES="BIKF",
        AOBT_3=aobt, ARVT_3=arvt, TAXI_TIME_3=taxi_min,
    )


def _write_month(spark, base, month, legs):
    """Stage one month of NM flights, with APDF present but empty.

    Empty APDF is deliberate: it puts every leg on the NM-inferred branch, so
    ``t_off`` is exactly ``AOBT_3 + TAXI_TIME_3`` and the assertions below are
    about the window and nothing else.
    """
    spark.createDataFrame(legs, schema=_FLIGHTS_SCHEMA).write.parquet(
        str(base / f"flights_{month}.parquet")
    )
    spark.createDataFrame([], schema=_APDF_SCHEMA).write.parquet(
        str(base / f"apdf_{month}.parquet")
    )


def _callsigns(gt):
    return sorted(r["callsign"] for r in gt.collect())


def test_a_flight_off_block_before_midnight_is_kept(spark, tmp_path):
    """The defect, in one case.

    Off-block 23:52 on the day before the sample; airborne 00:14 and landed
    04:30 inside it. The interval lies wholly within the window, so the flight
    belongs in the sample -- but a filter keyed on the off-block day drops it.
    """
    _write_month(spark, tmp_path, "300001", [
        # off-block 2025-06-04 23:52, airborne 2025-06-05 00:14, down 04:30
        _leg("mid01", "MIDNIGHT", dt.datetime(2025, 6, 4, 23, 52, 0),
             dt.datetime(2025, 6, 5, 4, 30, 0), taxi_min=22),
        # an ordinary leg on the sampled day, as a control
        _leg("day01", "DAYTIME", dt.datetime(2025, 6, 5, 8, 0, 0),
             dt.datetime(2025, 6, 5, 10, 0, 0)),
    ])

    gt = load_flight_intervals(
        spark, months=["300001"], days=["2025-06-05"], reference_base=str(tmp_path)
    )
    assert _callsigns(gt) == ["DAYTIME", "MIDNIGHT"]

    row = gt.filter(gt.callsign == "MIDNIGHT").collect()[0]
    assert row["t_off"] == dt.datetime(2025, 6, 5, 0, 14, 0)
    assert row["t_land"] == dt.datetime(2025, 6, 5, 4, 30, 0)
    # The returned `day` column keeps its documented meaning -- the off-block
    # day -- even for a flight the window admits on the strength of t_off.
    assert row["day"] == dt.date(2025, 6, 4)


def test_a_flight_genuinely_outside_the_window_is_still_dropped(spark, tmp_path):
    """Widening the pre-filter must not widen the window itself.

    The point of the fix is that the pre-filter stops deciding membership, not
    that membership gets looser. A flight airborne before the window opens stays
    out.

    ``EARLY`` pushes back two minutes before the ``MIDNIGHT`` leg of the test
    above and is airborne at 23:58, two minutes before the window opens; it
    lands inside the window, so only ``t_off`` can exclude it. ``LATE`` is the
    mirror at the closing edge: airborne inside the sample, down at 00:30 the
    next day, which is the fabricated-overhang case the containment rule
    already existed for.
    """
    _write_month(spark, tmp_path, "300002", [
        _leg("erl01", "EARLY", dt.datetime(2025, 6, 4, 23, 50, 0),
             dt.datetime(2025, 6, 5, 3, 0, 0), taxi_min=8),
        _leg("lat01", "LATE", dt.datetime(2025, 6, 5, 23, 40, 0),
             dt.datetime(2025, 6, 6, 0, 30, 0)),
        _leg("day01", "DAYTIME", dt.datetime(2025, 6, 5, 8, 0, 0),
             dt.datetime(2025, 6, 5, 10, 0, 0)),
    ])

    gt = load_flight_intervals(
        spark, months=["300002"], days=["2025-06-05"], reference_base=str(tmp_path)
    )
    assert _callsigns(gt) == ["DAYTIME"]


def test_the_pre_filter_still_prunes_days_the_window_cannot_admit(spark, tmp_path):
    """Widening is one day either side, not the whole month.

    The ``day`` filter is a pruning aid: dropping it altogether would be
    correct but would read every day NM holds. A leg two days before the sample
    cannot reach the window under any plausible taxi time, so it must not
    survive the pre-filter -- and a leg two days after cannot either.
    """
    _write_month(spark, tmp_path, "300003", [
        _leg("far01", "FARBEFORE", dt.datetime(2025, 6, 3, 8, 0, 0),
             dt.datetime(2025, 6, 3, 10, 0, 0)),
        _leg("far02", "FARAFTER", dt.datetime(2025, 6, 7, 8, 0, 0),
             dt.datetime(2025, 6, 7, 10, 0, 0)),
        _leg("day01", "DAYTIME", dt.datetime(2025, 6, 5, 8, 0, 0),
             dt.datetime(2025, 6, 5, 10, 0, 0)),
    ])

    gt = load_flight_intervals(
        spark, months=["300003"], days=["2025-06-05"], reference_base=str(tmp_path)
    )
    assert _callsigns(gt) == ["DAYTIME"]


def test_at_a_month_edge_the_crosser_is_recovered_only_if_its_month_is_loaded(
    spark, tmp_path
):
    """What the widened pre-filter does -- and does not do -- on the 1st.

    The widening reaches an off-block day in the *previous month*, and that day
    lives in the previous month's file. ``months`` decides which files are read
    at all, so the recovery works when the caller passes both months and cannot
    work when it passes one: the row is not in the frame for any filter to
    admit. Stated as a test rather than a footnote, because the failure mode is
    silent -- a sample starting on the 1st simply keeps the old, slightly
    lossy, behaviour.
    """
    _write_month(spark, tmp_path, "300004", [  # "May"
        _leg("mid02", "CROSSER", dt.datetime(2025, 5, 31, 23, 52, 0),
             dt.datetime(2025, 6, 1, 4, 30, 0), taxi_min=22),
    ])
    _write_month(spark, tmp_path, "300005", [  # "June"
        _leg("day02", "DAYTIME", dt.datetime(2025, 6, 1, 8, 0, 0),
             dt.datetime(2025, 6, 1, 10, 0, 0)),
    ])

    both = load_flight_intervals(
        spark, months=["300004", "300005"], days=["2025-06-01"],
        reference_base=str(tmp_path),
    )
    assert _callsigns(both) == ["CROSSER", "DAYTIME"]

    june_only = load_flight_intervals(
        spark, months=["300005"], days=["2025-06-01"], reference_base=str(tmp_path)
    )
    assert _callsigns(june_only) == ["DAYTIME"]
