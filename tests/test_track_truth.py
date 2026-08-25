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


def test_load_flight_intervals_excludes_a_flight_landing_after_the_sample_window(
    spark, tmp_path
):
    """A midnight-crosser's t_land is real; its samples are not.

    ``track_methods``/``track_sweep`` filter state vectors on
    ``to_date(event_time).isin(days)``, but ground truth is keyed on the
    *departure* day. A flight leaving 2025-06-05 22:00 and landing 06-06 00:30
    therefore keeps its true ``t_land`` while the samples available to score it
    stop at 23:59:59 -- so its track's ``trk_end`` is a clipping artefact and
    ``boundary_error`` reads ~30 minutes of fabricated overhang for a track
    that may be perfectly correct.

    The old boundary computation was structurally immune to this, because it
    took ``trk_end`` from ``matched``, which is clipped to ``[t_off, t_land]``
    by construction. Moving to real track extents removed that immunity, so
    the guard has to live here instead: ground truth is restricted to flights
    whose entire ``[t_off, t_land]`` lies inside the sampled window.

    The two legs differ only in their times, so nothing but window membership
    can explain one surviving and the other not.
    """
    flights_rows = [
        # wholly inside 2025-06-05: kept
        Row(AIRCRAFT_ADDRESS="synth03", AIRCRAFT_ID="SYN3", ADEP="EBBR", ADES="BIKF",
            AOBT_3=dt.datetime(2025, 6, 5, 8, 0, 0),
            ARVT_3=dt.datetime(2025, 6, 5, 10, 0, 0), TAXI_TIME_3=10),
        # departs on 2025-06-05, lands on 06-06: excluded
        Row(AIRCRAFT_ADDRESS="synth03", AIRCRAFT_ID="SYN3", ADEP="EBBR", ADES="BIKF",
            AOBT_3=dt.datetime(2025, 6, 5, 22, 0, 0),
            ARVT_3=dt.datetime(2025, 6, 6, 0, 30, 0), TAXI_TIME_3=10),
    ]
    spark.createDataFrame(flights_rows, schema=_SYNTH_FLIGHTS_SCHEMA).write.parquet(
        str(tmp_path / "flights_200003.parquet")
    )
    spark.createDataFrame([], schema=_SYNTH_APDF_SCHEMA).write.parquet(
        str(tmp_path / "apdf_200003.parquet")
    )

    gt = load_flight_intervals(
        spark, months=["200003"], days=["2025-06-05"], reference_base=str(tmp_path)
    )
    rows = gt.filter(gt.icao24 == "synth03").collect()
    assert len(rows) == 1
    assert rows[0]["t_off"] == dt.datetime(2025, 6, 5, 8, 10, 0)
    assert rows[0]["t_land"] == dt.datetime(2025, 6, 5, 10, 0, 0)


# -- the APDF departure join must key on the take-off day, not the off-block
# day. A taxi that crosses midnight separates the two: 2,272 of 957,396 NM
# flights in 202506 (0.237 %) and 2,102 of 935,887 in 202406 (0.225 %),
# measured over the committed reference extracts.


def test_apdf_departure_is_attached_when_the_taxi_crosses_midnight(spark, tmp_path):
    """Off-block 23:52, airborne 00:16: the milestone is real and must be used.

    ``day`` is ``to_date(AOBT_3)`` and APDF's ``mvt_day`` is the take-off day,
    so keying the departure join on ``day`` misses this leg entirely and drops
    it to ``t_source = "nm_inferred"`` -- lossy rather than wrong, but
    systematically lossy: ``boundary_error`` is APDF-only, so the flights it
    never sees would be exactly the midnight-taxi ones, in a study about taxi
    behaviour. The key is ``to_date(AOBT_3 + TAXI_TIME_3)``, which ``CLAUDE.md``
    records as matching real APDF ATOT to a median of 0 s (IQR 17 s).
    """
    flights_rows = [
        Row(AIRCRAFT_ADDRESS="synth04", AIRCRAFT_ID="SYN4", ADEP="EBBR", ADES="BIKF",
            AOBT_3=dt.datetime(2025, 6, 4, 23, 52, 0),
            ARVT_3=dt.datetime(2025, 6, 5, 4, 30, 0), TAXI_TIME_3=22),
    ]
    apdf_rows = [
        # real ATOT, two minutes after the NM estimate of 00:14, on 06-05
        Row(AP_C_FLTID="SYN4", ADEP_ICAO="EBBR", ADES_ICAO="BIKF", SRC_PHASE="DEP",
            MVT_TIME_UTC=dt.datetime(2025, 6, 5, 0, 16, 0)),
        Row(AP_C_FLTID="SYN4", ADEP_ICAO="EBBR", ADES_ICAO="BIKF", SRC_PHASE="ARR",
            MVT_TIME_UTC=dt.datetime(2025, 6, 5, 4, 32, 0)),
    ]
    spark.createDataFrame(flights_rows, schema=_SYNTH_FLIGHTS_SCHEMA).write.parquet(
        str(tmp_path / "flights_200004.parquet")
    )
    spark.createDataFrame(apdf_rows, schema=_SYNTH_APDF_SCHEMA).write.parquet(
        str(tmp_path / "apdf_200004.parquet")
    )

    gt = load_flight_intervals(
        spark, months=["200004"], days=["2025-06-05"], reference_base=str(tmp_path)
    )
    rows = gt.filter(gt.icao24 == "synth04").collect()
    assert len(rows) == 1
    assert rows[0]["t_source"] == "apdf"
    assert rows[0]["t_off"] == dt.datetime(2025, 6, 5, 0, 16, 0)
    assert rows[0]["t_land"] == dt.datetime(2025, 6, 5, 4, 32, 0)
    # `day` keeps its documented meaning: the off-block day, not the key the
    # departure join now uses.
    assert rows[0]["day"] == dt.date(2025, 6, 4)


def test_apdf_departure_does_not_attach_a_movement_from_the_off_block_day(
    spark, tmp_path
):
    """The old key's silently-wrong half, not just its lossy half.

    The same callsign departed the same aerodrome at 20:00 on the off-block
    day. Keyed on ``day``, that movement is the *only* eligible candidate for a
    leg that pushed back at 23:52 -- so the join attaches a take-off nearly four
    hours before push-back, and ``t_source`` still reads "apdf". Keyed on the
    take-off day, only the 00:16 movement is eligible.
    """
    flights_rows = [
        Row(AIRCRAFT_ADDRESS="synth05", AIRCRAFT_ID="SYN5", ADEP="EBBR", ADES="BIKF",
            AOBT_3=dt.datetime(2025, 6, 4, 23, 52, 0),
            ARVT_3=dt.datetime(2025, 6, 5, 4, 30, 0), TAXI_TIME_3=22),
    ]
    apdf_rows = [
        # an earlier movement of the same callsign, on the off-block day
        Row(AP_C_FLTID="SYN5", ADEP_ICAO="EBBR", ADES_ICAO="BIKF", SRC_PHASE="DEP",
            MVT_TIME_UTC=dt.datetime(2025, 6, 4, 20, 0, 0)),
        # this leg's own take-off
        Row(AP_C_FLTID="SYN5", ADEP_ICAO="EBBR", ADES_ICAO="BIKF", SRC_PHASE="DEP",
            MVT_TIME_UTC=dt.datetime(2025, 6, 5, 0, 16, 0)),
        Row(AP_C_FLTID="SYN5", ADEP_ICAO="EBBR", ADES_ICAO="BIKF", SRC_PHASE="ARR",
            MVT_TIME_UTC=dt.datetime(2025, 6, 5, 4, 32, 0)),
    ]
    spark.createDataFrame(flights_rows, schema=_SYNTH_FLIGHTS_SCHEMA).write.parquet(
        str(tmp_path / "flights_200005.parquet")
    )
    spark.createDataFrame(apdf_rows, schema=_SYNTH_APDF_SCHEMA).write.parquet(
        str(tmp_path / "apdf_200005.parquet")
    )

    gt = load_flight_intervals(
        spark, months=["200005"], days=["2025-06-05"], reference_base=str(tmp_path)
    )
    rows = gt.filter(gt.icao24 == "synth05").collect()
    assert len(rows) == 1
    assert rows[0]["t_off"] == dt.datetime(2025, 6, 5, 0, 16, 0)


def test_apdf_departure_still_matches_an_ordinary_same_day_leg(spark, tmp_path):
    """Moving the key must not lose the 99.8 % of legs whose taxi stays put.

    Off-block and take-off on the same day is the normal case, and it has to go
    on matching exactly as before -- including the proximity choice between two
    candidates that share the new key.
    """
    flights_rows = [
        Row(AIRCRAFT_ADDRESS="synth06", AIRCRAFT_ID="SYN6", ADEP="EBBR", ADES="BIKF",
            AOBT_3=dt.datetime(2025, 6, 5, 8, 0, 0),
            ARVT_3=dt.datetime(2025, 6, 5, 10, 0, 0), TAXI_TIME_3=10),
        Row(AIRCRAFT_ADDRESS="synth06", AIRCRAFT_ID="SYN6", ADEP="EBBR", ADES="BIKF",
            AOBT_3=dt.datetime(2025, 6, 5, 14, 0, 0),
            ARVT_3=dt.datetime(2025, 6, 5, 16, 0, 0), TAXI_TIME_3=10),
    ]
    apdf_rows = [
        Row(AP_C_FLTID="SYN6", ADEP_ICAO="EBBR", ADES_ICAO="BIKF", SRC_PHASE="DEP",
            MVT_TIME_UTC=dt.datetime(2025, 6, 5, 8, 5, 0)),
        Row(AP_C_FLTID="SYN6", ADEP_ICAO="EBBR", ADES_ICAO="BIKF", SRC_PHASE="DEP",
            MVT_TIME_UTC=dt.datetime(2025, 6, 5, 14, 7, 0)),
        Row(AP_C_FLTID="SYN6", ADEP_ICAO="EBBR", ADES_ICAO="BIKF", SRC_PHASE="ARR",
            MVT_TIME_UTC=dt.datetime(2025, 6, 5, 10, 5, 0)),
        Row(AP_C_FLTID="SYN6", ADEP_ICAO="EBBR", ADES_ICAO="BIKF", SRC_PHASE="ARR",
            MVT_TIME_UTC=dt.datetime(2025, 6, 5, 16, 5, 0)),
    ]
    spark.createDataFrame(flights_rows, schema=_SYNTH_FLIGHTS_SCHEMA).write.parquet(
        str(tmp_path / "flights_200006.parquet")
    )
    spark.createDataFrame(apdf_rows, schema=_SYNTH_APDF_SCHEMA).write.parquet(
        str(tmp_path / "apdf_200006.parquet")
    )

    gt = load_flight_intervals(
        spark, months=["200006"], days=["2025-06-05"], reference_base=str(tmp_path)
    )
    rows = gt.filter(gt.icao24 == "synth06").orderBy("t_off").collect()
    assert len(rows) == 2
    assert rows[0]["t_off"] == dt.datetime(2025, 6, 5, 8, 5, 0)
    assert rows[1]["t_off"] == dt.datetime(2025, 6, 5, 14, 7, 0)
    assert rows[0]["t_source"] == rows[1]["t_source"] == "apdf"


def test_apdf_departure_does_not_inflate_a_ground_truth_interval(spark, tmp_path):
    """The consequence that reaches the metrics, in the shape it really had.

    A nightly EDDV->LTAI service, reproduced from the real case: icao24 4bce13,
    SXS9ZZ, off-block 2025-06-05 23:52 + 10 min taxi, down 06-06 03:16. APDF
    holds this leg's take-off at 06-06 00:02:15 and *yesterday's rotation* at
    06-05 00:20:03. Keyed on the off-block day, only yesterday's is eligible,
    and pairing it with this leg's own ARVT_3 gives a 26.9-hour ground-truth
    interval.

    That is not a labelling nicety: ``overlap_join`` assigns by containment, so
    an interval that long swallows a whole day of the airframe's samples --
    including its neighbouring legs' -- into one flight, in a study whose
    headline statistic is how often a segmentation merges flights. Nor does
    ``t_source`` flag it: LTAI is not APDF-covered, so ``aldt`` is null and the
    row reads "nm_inferred" while carrying an APDF take-off from the wrong day.
    """
    flights_rows = [
        Row(AIRCRAFT_ADDRESS="synth07", AIRCRAFT_ID="SYN7", ADEP="EDDV", ADES="LTAI",
            AOBT_3=dt.datetime(2025, 6, 5, 23, 52, 0),
            ARVT_3=dt.datetime(2025, 6, 6, 3, 16, 0), TAXI_TIME_3=10),
    ]
    apdf_rows = [
        # yesterday's rotation of the same nightly service
        Row(AP_C_FLTID="SYN7", ADEP_ICAO="EDDV", ADES_ICAO="LTAI", SRC_PHASE="DEP",
            MVT_TIME_UTC=dt.datetime(2025, 6, 5, 0, 20, 3)),
        # this leg's own take-off, 15 s from NM's estimate
        Row(AP_C_FLTID="SYN7", ADEP_ICAO="EDDV", ADES_ICAO="LTAI", SRC_PHASE="DEP",
            MVT_TIME_UTC=dt.datetime(2025, 6, 6, 0, 2, 15)),
        # no ARR row: LTAI is not APDF-covered, so t_source stays nm_inferred
    ]
    spark.createDataFrame(flights_rows, schema=_SYNTH_FLIGHTS_SCHEMA).write.parquet(
        str(tmp_path / "flights_200007.parquet")
    )
    spark.createDataFrame(apdf_rows, schema=_SYNTH_APDF_SCHEMA).write.parquet(
        str(tmp_path / "apdf_200007.parquet")
    )

    # The real study's three-day window, not a single day. With a one-day
    # window the mis-attached 06-05 00:20 take-off falls outside it and the row
    # is merely dropped -- a different failure, and one that hides this one.
    # Widened, the inflated interval survives into the output, as it did in the
    # 2025 sample.
    gt = load_flight_intervals(
        spark, months=["200007"],
        days=["2025-06-05", "2025-06-06", "2025-06-07"],
        reference_base=str(tmp_path),
    )
    rows = gt.filter(gt.icao24 == "synth07").collect()
    assert len(rows) == 1
    assert rows[0]["t_off"] == dt.datetime(2025, 6, 6, 0, 2, 15)
    duration_h = (rows[0]["t_land"] - rows[0]["t_off"]).total_seconds() / 3600.0
    assert duration_h < 4, f"ground-truth interval inflated to {duration_h:.1f} h"
    # The label is unchanged by this fix, and says less than it appears to:
    # a measured ATOT with no measured ALDT still reads "nm_inferred".
    assert rows[0]["t_source"] == "nm_inferred"
