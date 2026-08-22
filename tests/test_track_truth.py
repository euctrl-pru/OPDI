"""Tests for ground-truth flight intervals and the interval-overlap join.

The join deliberately does NOT use callsign. Arm A4 drops callsign from track
identity; joining ground truth on it would score A4 against a key it does not
have, and would do so invisibly.
"""

import datetime as dt

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
    assert set(dep.columns) == {"callsign", "mvt_day", "apdf_adep", "atot"}
    assert set(arr.columns) == {"callsign", "mvt_day", "apdf_ades", "aldt"}
    assert dep.count() > 0
    assert arr.count() > 0


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
        StructField("AP_C_FLTID", StringType(), True),
        StructField("ADEP_ICAO", StringType(), True),
        StructField("ADES_ICAO", StringType(), True),
        StructField("SRC_PHASE", StringType(), True),
        StructField("MVT_TIME_UTC", TimestampType(), True),
    ]
)


def test_flight_key_distinguishes_same_day_same_route_legs_by_off_block_time(spark, tmp_path):
    """Two legs by the same aircraft, same callsign, same route, same day, and
    differing only in off-block time must get distinct flight_keys. Without
    t_off in the hash, this collapses -- and since this study measures
    merging, a collision makes a segmentation look better than it is, in
    exactly the statistic the paper reports."""
    flights_rows = [
        Row(AIRCRAFT_ADDRESS="synth01", AIRCRAFT_ID="SYN1", ADEP="EBBR", ADES="BIKF",
            AOBT_3=dt.datetime(2025, 6, 5, 8, 0, 0), ARVT_3=dt.datetime(2025, 6, 5, 10, 0, 0),
            TAXI_TIME_3=10),
        Row(AIRCRAFT_ADDRESS="synth01", AIRCRAFT_ID="SYN1", ADEP="EBBR", ADES="BIKF",
            AOBT_3=dt.datetime(2025, 6, 5, 14, 0, 0), ARVT_3=dt.datetime(2025, 6, 5, 16, 0, 0),
            TAXI_TIME_3=10),
    ]
    spark.createDataFrame(flights_rows, schema=_SYNTH_FLIGHTS_SCHEMA).write.parquet(
        str(tmp_path / "flights_200001.parquet")
    )
    spark.createDataFrame([], schema=_SYNTH_APDF_SCHEMA).write.parquet(
        str(tmp_path / "apdf_200001.parquet")
    )

    gt = load_flight_intervals(
        spark, months=["200001"], days=["2025-06-05"], reference_base=str(tmp_path)
    )
    rows = gt.filter(gt.icao24 == "synth01").orderBy("t_off").collect()
    assert len(rows) == 2
    assert rows[0]["t_off"] != rows[1]["t_off"]
    assert rows[0]["flight_key"] != rows[1]["flight_key"]


def test_load_flight_intervals_disambiguates_competing_apdf_candidates_by_proximity(
    spark, tmp_path
):
    """Two competing APDF departure candidates, both structurally eligible for
    both of two same-day same-route legs (same callsign, day, ADEP) before
    disambiguation runs. Existing tests exercise the disambiguation pathway
    but never its actual decision: the empty-APDF case has nothing to choose
    between, and real multi-leg rotations key on different aerodromes so each
    partition only ever sees one real candidate. This is the case round 2's
    proximity window exists for -- given two genuinely competing candidates,
    does each NM row get the nearer one, not just *a* one?"""
    flights_rows = [
        Row(AIRCRAFT_ADDRESS="synth02", AIRCRAFT_ID="SYN2", ADEP="EBBR", ADES="BIKF",
            AOBT_3=dt.datetime(2025, 6, 5, 8, 0, 0), ARVT_3=dt.datetime(2025, 6, 5, 10, 0, 0),
            TAXI_TIME_3=10),
        Row(AIRCRAFT_ADDRESS="synth02", AIRCRAFT_ID="SYN2", ADEP="EBBR", ADES="BIKF",
            AOBT_3=dt.datetime(2025, 6, 5, 14, 0, 0), ARVT_3=dt.datetime(2025, 6, 5, 16, 0, 0),
            TAXI_TIME_3=10),
    ]
    apdf_rows = [
        # Two DEP candidates sharing (callsign, day, ADEP) -- both eligible
        # for both NM rows on the equality join alone. 08:05 sits close to
        # the 08:00 leg's own estimate (08:00 + 10 min taxi = 08:10) and far
        # from the 14:00 leg's; 14:07 is the mirror.
        Row(AP_C_FLTID="SYN2", ADEP_ICAO="EBBR", ADES_ICAO="BIKF", SRC_PHASE="DEP",
            MVT_TIME_UTC=dt.datetime(2025, 6, 5, 8, 5, 0)),
        Row(AP_C_FLTID="SYN2", ADEP_ICAO="EBBR", ADES_ICAO="BIKF", SRC_PHASE="DEP",
            MVT_TIME_UTC=dt.datetime(2025, 6, 5, 14, 7, 0)),
        # One unambiguous ARR candidate per leg, so both legs actually reach
        # t_source == "apdf" instead of the assertion testing a t_source that
        # can never be anything but "nm_inferred".
        Row(AP_C_FLTID="SYN2", ADEP_ICAO="EBBR", ADES_ICAO="BIKF", SRC_PHASE="ARR",
            MVT_TIME_UTC=dt.datetime(2025, 6, 5, 10, 5, 0)),
        Row(AP_C_FLTID="SYN2", ADEP_ICAO="EBBR", ADES_ICAO="BIKF", SRC_PHASE="ARR",
            MVT_TIME_UTC=dt.datetime(2025, 6, 5, 16, 5, 0)),
    ]
    spark.createDataFrame(flights_rows, schema=_SYNTH_FLIGHTS_SCHEMA).write.parquet(
        str(tmp_path / "flights_200002.parquet")
    )
    spark.createDataFrame(apdf_rows, schema=_SYNTH_APDF_SCHEMA).write.parquet(
        str(tmp_path / "apdf_200002.parquet")
    )

    gt = load_flight_intervals(
        spark, months=["200002"], days=["2025-06-05"], reference_base=str(tmp_path)
    )
    rows = gt.filter(gt.icao24 == "synth02").orderBy("t_off").collect()

    assert len(rows) == 2  # no row lost, no fan-out

    morning, afternoon = rows
    assert morning["t_off"] == dt.datetime(2025, 6, 5, 8, 5, 0)  # ~08:05, not ~14:07
    assert afternoon["t_off"] == dt.datetime(2025, 6, 5, 14, 7, 0)  # ~14:07, not ~08:05
    assert morning["t_source"] == "apdf"
    assert afternoon["t_source"] == "apdf"
