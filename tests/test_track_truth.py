"""Tests for ground-truth flight intervals and the interval-overlap join.

The join deliberately does NOT use callsign. Arm A4 drops callsign from track
identity; joining ground truth on it would score A4 against a key it does not
have, and would do so invisibly.
"""

import datetime as dt

from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType
from track_truth import load_apdf_times, load_flight_intervals, overlap_join

#: The git-lfs reference parquets committed under reference/ -- real production
#: data, not fixtures. Tests against this exercise the actual column names,
#: casing, and the two-sided asymmetric APDF join with no cluster and no
#: credentials.
_LOCAL_REFERENCE = "reference"

_T0 = dt.datetime(2025, 6, 5, 8, 0, 0)


def _gt(spark, rows):
    return spark.createDataFrame([Row(**r) for r in rows])


def _assign(spark, rows):
    return spark.createDataFrame([Row(**r) for r in rows])


def test_overlap_join_matches_a_sample_inside_the_interval(spark):
    gt = _gt(spark, [{"flight_key": "F1", "icao24": "abc123",
                      "gt_adep": "EBBR", "gt_ades": "BIKF", "t_source": "apdf",
                      "t_off": _T0, "t_land": _T0 + dt.timedelta(hours=2)}])
    a = _assign(spark, [{"icao24": "abc123", "event_time": _T0 + dt.timedelta(minutes=30),
                         "track_id": "T1"}])
    out = overlap_join(a, gt).collect()
    assert len(out) == 1 and out[0]["flight_key"] == "F1"


def test_overlap_join_excludes_a_sample_outside_every_interval(spark):
    gt = _gt(spark, [{"flight_key": "F1", "icao24": "abc123",
                      "gt_adep": "EBBR", "gt_ades": "BIKF", "t_source": "apdf",
                      "t_off": _T0, "t_land": _T0 + dt.timedelta(hours=2)}])
    a = _assign(spark, [{"icao24": "abc123", "event_time": _T0 - dt.timedelta(hours=1),
                         "track_id": "T1"}])
    assert overlap_join(a, gt).count() == 0


def test_overlap_join_ignores_callsign_entirely(spark):
    """A4's whole premise. If this test ever needs callsign, the join is wrong."""
    gt = _gt(spark, [{"flight_key": "F1", "icao24": "abc123", "callsign": "BEL123",
                      "gt_adep": "EBBR", "gt_ades": "BIKF", "t_source": "apdf",
                      "t_off": _T0, "t_land": _T0 + dt.timedelta(hours=2)}])
    a = _assign(spark, [{"icao24": "abc123", "callsign": "TOTALLY_DIFFERENT",
                         "event_time": _T0 + dt.timedelta(minutes=30), "track_id": "T1"}])
    assert overlap_join(a, gt).count() == 1


def test_overlap_join_does_not_cross_airframes(spark):
    gt = _gt(spark, [{"flight_key": "F1", "icao24": "abc123",
                      "gt_adep": "EBBR", "gt_ades": "BIKF", "t_source": "apdf",
                      "t_off": _T0, "t_land": _T0 + dt.timedelta(hours=2)}])
    a = _assign(spark, [{"icao24": "zzz999", "event_time": _T0 + dt.timedelta(minutes=30),
                         "track_id": "T1"}])
    assert overlap_join(a, gt).count() == 0


def test_overlap_join_keeps_duplicate_state_vectors(spark):
    """Two identical raw samples must stay two rows, not collapse into one.

    The "pick the earlier interval" window used to partition on
    ``(a.icao24, a.event_time)`` -- the *sample* key, not a row identity -- so
    any duplicate state vector was silently dropped by the ``_r == 1`` filter.
    Raw OSN state vectors can contain duplicates: dedup on
    ``(track_id, timestamp)`` is listed in the project's evidence base as a
    cleaning step not yet implemented, so this harness cannot assume it. Dropping
    them undercounts the contingency table every metric in ``track_score.py`` is
    computed from.
    """
    gt = _gt(spark, [{"flight_key": "F1", "icao24": "abc123",
                      "gt_adep": "EBBR", "gt_ades": "BIKF", "t_source": "apdf",
                      "t_off": _T0, "t_land": _T0 + dt.timedelta(hours=2)}])
    dup = {"icao24": "abc123", "event_time": _T0 + dt.timedelta(minutes=30),
           "track_id": "T1"}
    assert overlap_join(_assign(spark, [dup, dict(dup)]), gt).count() == 2


def test_overlap_join_still_deduplicates_across_touching_intervals(spark):
    """The duplicate fix must not reopen the boundary-sample double count.

    One sample, two ground-truth intervals that touch at its instant: still one
    output row, assigned to the earlier leg. This is the property the old
    sample-keyed window got right for the wrong reason.
    """
    gt = _gt(spark, [
        {"flight_key": "F1", "icao24": "abc123",
         "gt_adep": "EBBR", "gt_ades": "BIKF", "t_source": "apdf",
         "t_off": _T0, "t_land": _T0 + dt.timedelta(hours=1)},
        {"flight_key": "F2", "icao24": "abc123",
         "gt_adep": "BIKF", "gt_ades": "EBBR", "t_source": "apdf",
         "t_off": _T0 + dt.timedelta(hours=1), "t_land": _T0 + dt.timedelta(hours=2)},
    ])
    a = _assign(spark, [{"icao24": "abc123", "event_time": _T0 + dt.timedelta(hours=1),
                         "track_id": "T1"}])
    out = overlap_join(a, gt).collect()
    assert len(out) == 1 and out[0]["flight_key"] == "F1"


def test_overlap_join_assigns_a_sample_to_only_one_flight_when_intervals_touch(spark):
    """Back-to-back legs must not double-count the sample at the boundary."""
    gt = _gt(spark, [
        {"flight_key": "F1", "icao24": "abc123",
         "gt_adep": "EBBR", "gt_ades": "BIKF", "t_source": "apdf",
         "t_off": _T0, "t_land": _T0 + dt.timedelta(hours=1)},
        {"flight_key": "F2", "icao24": "abc123",
         "gt_adep": "BIKF", "gt_ades": "EBBR", "t_source": "apdf",
         "t_off": _T0 + dt.timedelta(hours=1), "t_land": _T0 + dt.timedelta(hours=2)},
    ])
    a = _assign(spark, [{"icao24": "abc123", "event_time": _T0 + dt.timedelta(hours=1),
                         "track_id": "T1"}])
    assert overlap_join(a, gt).count() == 1


# -- load_apdf_times / load_flight_intervals, against the real committed
# reference/*.parquet. The brief scoped tests to overlap_join only; these
# were added after review found the two-sided APDF join, the t_source
# branching, and flight_key construction had zero coverage despite being the
# riskiest logic in the file and never having been run end-to-end.

def test_load_apdf_times_resolves_real_columns_and_returns_dep_and_arr_unjoined(spark):
    dep, arr = load_apdf_times(spark, months=["202506"], reference_base=_LOCAL_REFERENCE)
    # Keyed on the Samad id; the callsign and day columns the old join needed
    # are gone with it.
    assert set(dep.columns) == {"sam_id", "apdf_adep", "atot", "aobt"}
    assert set(arr.columns) == {"sam_id", "apdf_ades", "aldt", "aibt"}
    assert dep.count() > 0
    assert arr.count() > 0


def test_block_times_are_populated_and_bracket_the_movement(spark):
    """AOBT precedes ATOT and AIBT follows ALDT, in the real extract.

    Both come from one column, `BLOCK_TIME_UTC`, discriminated only by
    `SRC_PHASE` -- so getting the discrimination backwards would produce a
    fully populated, entirely wrong pair, with an off-block *after* take-off
    and an in-block *before* landing. Asserting the order is what catches that;
    asserting non-nullness alone would not.
    """
    dep, arr = load_apdf_times(spark, months=["202506"], reference_base=_LOCAL_REFERENCE)

    d = dep.filter(dep.aobt.isNotNull() & dep.atot.isNotNull())
    n_dep = dep.count()
    assert d.count() / n_dep > 0.95, "AOBT is the capture denominator"
    assert d.filter(d.aobt < d.atot).count() / d.count() > 0.98

    a = arr.filter(arr.aibt.isNotNull() & arr.aldt.isNotNull())
    n_arr = arr.count()
    assert a.count() / n_arr > 0.95, "AIBT is the capture denominator"
    assert a.filter(a.aibt > a.aldt).count() / a.count() > 0.98


def test_load_flight_intervals_marks_t_source_apdf_when_both_ends_are_measured(spark):
    """icao24 4cc577 / callsign ICE73P, EBBR->BIKF on 2025-06-05: real APDF ATOT
    and ALDT both exist for this leg (checked against the committed parquet)."""
    gt = load_flight_intervals(
        spark, months=["202506"], days=["2025-06-05"], reference_base=_LOCAL_REFERENCE
    )
    row = gt.filter((gt.icao24 == "4cc577") & (gt.callsign == "ICE73P")).collect()
    assert len(row) == 1
    assert row[0]["t_source"] == "apdf"
    assert row[0]["gt_adep"] == "EBBR"
    assert row[0]["gt_ades"] == "BIKF"


def test_load_flight_intervals_marks_t_source_nm_inferred_when_apdf_is_missing(spark):
    """icao24 4cc2aa / callsign FNA501, BIAR->BGCO on 2025-06-05: neither
    aerodrome is APDF-covered, so this leg has no real ATOT or ALDT."""
    gt = load_flight_intervals(
        spark, months=["202506"], days=["2025-06-05"], reference_base=_LOCAL_REFERENCE
    )
    row = gt.filter((gt.icao24 == "4cc2aa") & (gt.callsign == "FNA501")).collect()
    assert len(row) == 1
    assert row[0]["t_source"] == "nm_inferred"
    assert row[0]["gt_adep"] == "BIAR"
    assert row[0]["gt_ades"] == "BGCO"


def test_load_flight_intervals_attaches_apdf_dep_time_to_the_correct_leg(spark):
    """icao24 4cae87 / callsign ABR1CE flies two legs on 2025-06-05: LFMN->LFLL
    in the morning and LFLL->LFPG at night. Each leg's ADEP must pick up its
    own APDF departure milestone, not the other leg's -- this is what the join
    keying on aerodrome (not just callsign+day) is for. Also regression cover
    for a real bug found while writing this test: the arrival-side join had no
    day key, so a recurring route fanned out across every day APDF had a match
    for that callsign+ADES, corrupting t_land with dates from elsewhere in the
    month."""
    gt = load_flight_intervals(
        spark, months=["202506"], days=["2025-06-05"], reference_base=_LOCAL_REFERENCE
    )
    legs = (
        gt.filter((gt.icao24 == "4cae87") & (gt.callsign == "ABR1CE"))
        .orderBy("t_off")
        .collect()
    )
    assert len(legs) == 2
    assert legs[0]["gt_adep"] == "LFMN" and legs[0]["t_off"].hour < 12
    assert legs[1]["gt_adep"] == "LFLL" and legs[1]["t_off"].hour >= 18
    assert legs[0]["day"] == legs[1]["day"] == dt.date(2025, 6, 5)


# -- flight_key must not collapse two distinct legs of the same rotation.

_SYNTH_FLIGHTS_SCHEMA = StructType(
    [
        # The Samad flight id -- the key APDF is joined on.
        StructField("ID", IntegerType(), True),
        StructField("AIRCRAFT_ADDRESS", StringType(), True),
        StructField("AIRCRAFT_ID", StringType(), True),
        StructField("ADEP", StringType(), True),
        StructField("ADES", StringType(), True),
        StructField("AOBT_3", TimestampType(), True),
        StructField("ARVT_3", TimestampType(), True),
        StructField("TAXI_TIME_3", IntegerType(), True),
    ]
)

_SYNTH_APDF_SCHEMA = StructType(
    [
        StructField("ID", IntegerType(), True),
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

def _nm_row(sam_id, icao24, callsign, aobt, arvt, taxi=10, adep="EBBR",
            ades="BIKF"):
    return Row(ID=sam_id, AIRCRAFT_ADDRESS=icao24, AIRCRAFT_ID=callsign,
               ADEP=adep, ADES=ades, AOBT_3=aobt, ARVT_3=arvt,
               TAXI_TIME_3=taxi)


def _apdf_row(sam_id, phase, mvt, callsign="X", adep="EBBR", ades="BIKF",
              block=None):
    return Row(ID=sam_id, AP_C_FLTID=callsign, ADEP_ICAO=adep, ADES_ICAO=ades,
               SRC_PHASE=phase, MVT_TIME_UTC=mvt, BLOCK_TIME_UTC=block)


def _write(spark, tmp_path, nm_rows, apdf_rows, tag="200002",
           days=("2025-06-05",)):
    spark.createDataFrame(nm_rows, schema=_SYNTH_FLIGHTS_SCHEMA).write.parquet(
        str(tmp_path / f"flights_{tag}.parquet")
    )
    spark.createDataFrame(apdf_rows, schema=_SYNTH_APDF_SCHEMA).write.parquet(
        str(tmp_path / f"apdf_{tag}.parquet")
    )
    return load_flight_intervals(
        spark, months=[tag], days=list(days), reference_base=str(tmp_path)
    )


def test_apdf_is_attached_by_samad_id(spark, tmp_path):
    """The join key is NM's ``ID``, which APDF carries as the same value."""
    gt = _write(
        spark, tmp_path,
        [_nm_row(1, "aaa111", "SYN1", dt.datetime(2025, 6, 5, 8, 0),
                 dt.datetime(2025, 6, 5, 10, 0))],
        [_apdf_row(1, "DEP", dt.datetime(2025, 6, 5, 8, 12),
                   block=dt.datetime(2025, 6, 5, 8, 0)),
         _apdf_row(1, "ARR", dt.datetime(2025, 6, 5, 9, 55),
                   block=dt.datetime(2025, 6, 5, 10, 3))],
    ).collect()
    assert len(gt) == 1
    r = gt[0]
    assert r["t_source"] == "apdf"
    assert r["t_off"] == dt.datetime(2025, 6, 5, 8, 12), "measured ATOT, not the estimate"
    assert r["t_land"] == dt.datetime(2025, 6, 5, 9, 55)
    assert r["aibt"] == dt.datetime(2025, 6, 5, 10, 3)


def test_the_id_join_survives_a_callsign_that_does_not_match(spark, tmp_path):
    """The regression this key exists for.

    ``AP_C_FLTID`` is whatever the airport system holds, and at some large
    aerodromes that is not the ATC callsign at all: Frankfurt reports IATA
    flight numbers, Zurich zero-pads. Matching on callsign found 48 of 963
    Frankfurt departures and 175 of 561 at Zurich, so those aerodromes were
    reported as having no measured milestones and dropped out of every metric
    that needs them.
    """
    gt = _write(
        spark, tmp_path,
        [_nm_row(7, "bbb222", "DLH400", dt.datetime(2025, 6, 5, 8, 0),
                 dt.datetime(2025, 6, 5, 10, 0))],
        # APDF's own identifier for the same flight, entirely different.
        [_apdf_row(7, "DEP", dt.datetime(2025, 6, 5, 8, 12), callsign="4Y002"),
         _apdf_row(7, "ARR", dt.datetime(2025, 6, 5, 9, 55), callsign="4Y002",
                   block=dt.datetime(2025, 6, 5, 10, 3))],
    ).collect()
    assert gt[0]["t_source"] == "apdf"
    assert gt[0]["t_off"] == dt.datetime(2025, 6, 5, 8, 12)


def test_the_id_join_survives_a_taxi_crossing_midnight(spark, tmp_path):
    """A day-keyed join dropped these to inferred times, or attached the wrong
    movement entirely -- once producing a 26.9-hour ground-truth interval. An
    equality join on a unique id cannot express the problem."""
    gt = _write(
        spark, tmp_path,
        [_nm_row(9, "ccc333", "SYN9", dt.datetime(2025, 6, 5, 23, 52),
                 dt.datetime(2025, 6, 6, 3, 16), taxi=10)],
        [_apdf_row(9, "DEP", dt.datetime(2025, 6, 6, 0, 2, 15)),
         # A movement by the same callsign the night before, which the
         # off-block-day key used to prefer.
         _apdf_row(99, "DEP", dt.datetime(2025, 6, 5, 0, 20, 3))],
        # Both days: the flight lands after midnight, and the window filter
        # requires the whole interval inside the sample -- which is a separate,
        # correct rule from the join being tested here.
        days=("2025-06-05", "2025-06-06"),
    ).collect()
    assert len(gt) == 1
    assert gt[0]["t_off"] == dt.datetime(2025, 6, 6, 0, 2, 15)
    assert (gt[0]["t_land"] - gt[0]["t_off"]).total_seconds() < 4 * 3600


def test_an_apdf_row_for_another_flight_is_not_attached(spark, tmp_path):
    gt = _write(
        spark, tmp_path,
        [_nm_row(3, "ddd444", "SYN3", dt.datetime(2025, 6, 5, 8, 0),
                 dt.datetime(2025, 6, 5, 10, 0))],
        [_apdf_row(4, "DEP", dt.datetime(2025, 6, 5, 8, 12))],
    ).collect()
    assert gt[0]["t_source"] == "nm_inferred"
    # Falls back to the estimate: off-block plus the predicted taxi.
    assert gt[0]["t_off"] == dt.datetime(2025, 6, 5, 8, 10)


def test_flight_key_distinguishes_same_day_same_route_legs(spark, tmp_path):
    """Two legs by one airframe, same callsign, same route, same day.

    They must get distinct flight_keys; without the take-off time in the hash
    they collapse into one, and a study measuring merging is handed a flight
    that cannot be merged with itself.
    """
    gt = _write(
        spark, tmp_path,
        [_nm_row(11, "eee555", "SYN2", dt.datetime(2025, 6, 5, 8, 0),
                 dt.datetime(2025, 6, 5, 10, 0)),
         _nm_row(12, "eee555", "SYN2", dt.datetime(2025, 6, 5, 14, 0),
                 dt.datetime(2025, 6, 5, 16, 0))],
        [_apdf_row(11, "DEP", dt.datetime(2025, 6, 5, 8, 5)),
         _apdf_row(11, "ARR", dt.datetime(2025, 6, 5, 10, 5),
                   block=dt.datetime(2025, 6, 5, 10, 12)),
         _apdf_row(12, "DEP", dt.datetime(2025, 6, 5, 14, 7)),
         _apdf_row(12, "ARR", dt.datetime(2025, 6, 5, 16, 5),
                   block=dt.datetime(2025, 6, 5, 16, 12))],
    )
    rows = gt.orderBy("t_off").collect()
    assert len(rows) == 2
    assert rows[0]["flight_key"] != rows[1]["flight_key"]
    # Each leg took its own APDF movement, not the other's.
    assert rows[0]["t_off"] == dt.datetime(2025, 6, 5, 8, 5)
    assert rows[1]["t_off"] == dt.datetime(2025, 6, 5, 14, 7)
    assert all(r["t_source"] == "apdf" for r in rows)


def test_endpoint_provenance_is_finer_than_t_source(spark):
    """A measured arrival at an uncovered departure aerodrome is still measured.

    `t_source` is "apdf" only when both ends are measured. On the real 2025
    extract 44,841 flights carry a genuine APDF AIBT while only 22,588 are
    labelled "apdf" -- so a consumer cutting the data by *aerodrome* rather
    than by flight-pair needs the endpoint flags, and reading `t_source`
    instead mis-classifies aerodromes whose arrivals are fully measured.
    """
    gt = load_flight_intervals(
        spark, months=["202506"], days=["2025-06-05"], reference_base=_LOCAL_REFERENCE
    ).cache()

    assert {"dep_measured", "arr_measured"} <= set(gt.columns)

    # Both flags true is a *subset* of what t_source calls "apdf", and
    # deliberately so: `arr_measured` also demands AIBT, because a capture
    # fraction without the in-block time has no denominator. APDF leaves
    # BLOCK_TIME_UTC null on a small fraction of arrival rows, and those rows
    # are "apdf" to t_source and not measured to this study.
    both = gt.filter(F.col("dep_measured") & F.col("arr_measured"))
    apdf = gt.filter(F.col("t_source") == "apdf")
    assert both.count() <= apdf.count()
    # The whole of the difference is missing AIBT -- nothing else.
    assert apdf.filter(
        ~(F.col("dep_measured") & F.col("arr_measured"))
        & F.col("aibt").isNotNull()
    ).count() == 0

    # And the flags are strictly finer: some arrivals are measured on rows
    # t_source calls inferred. If this ever reaches zero the split has gone
    # away and the extra columns are dead weight -- which would itself be worth
    # knowing.
    finer = gt.filter(F.col("arr_measured") & (F.col("t_source") != "apdf"))
    assert finer.count() > 0

    # arr_measured must imply a usable arrival ground phase.
    assert gt.filter(F.col("arr_measured") & F.col("aibt").isNull()).count() == 0
    gt.unpersist()


def test_arvt3_is_derived_from_aobt_not_observed(spark):
    """NM holds no measured runway time, and `ARVT_3` is not an exception.

    Its column comment reads "actual as calculated from AOBT", and it
    reproduces as `AOBT_3 + TAXI_TIME_3 + FLT_DUR_3`. This matters because the
    whole tier split rests on it: if `ARVT_3` were an observed landing, the
    estimated-times aerodromes would have a real arrival boundary and their
    ground coverage could be measured. It is not, and they do not.

    Asserted against the committed extract, not a fixture, because the claim is
    about the source data rather than about this module.
    """
    nm = spark.read.parquet(f"{_LOCAL_REFERENCE}/flights_202506.parquet").select(
        "AOBT_3", "ARVT_3", "TAXI_TIME_3", "FLT_DUR_3"
    ).filter(
        F.col("AOBT_3").isNotNull() & F.col("ARVT_3").isNotNull()
        & F.col("TAXI_TIME_3").isNotNull() & F.col("FLT_DUR_3").isNotNull()
    )
    predicted = (
        F.unix_timestamp("AOBT_3")
        + F.col("TAXI_TIME_3") * 60
        + F.col("FLT_DUR_3") * 60
    )
    resid = F.abs(F.unix_timestamp("ARVT_3") - predicted)
    row = nm.select(
        F.count(F.lit(1)).alias("n"),
        F.expr("percentile_approx(%s, 0.5)" % "abs(unix_timestamp(ARVT_3) - "
               "(unix_timestamp(AOBT_3) + TAXI_TIME_3*60 + FLT_DUR_3*60))")
        .alias("p50"),
        F.max(resid).alias("worst"),
    ).first()

    assert row["n"] > 100_000
    # Minute-rounding, not independent measurement.
    assert row["p50"] <= 30, f"median residual {row['p50']}s"
    assert row["worst"] <= 120, f"worst residual {row['worst']}s"
