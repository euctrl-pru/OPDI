"""Ground-truth flight intervals, and the join that scores a segmentation.

A segmentation is scored against what a flight *is*: an airframe and a span of
time. Network Manager ``flights_*.parquet`` gives both, across all of ECAC, at
957k flights a month. APDF gives truer times -- real ATOT and ALDT rather than
off-block and arrival -- but only at PRU-covered aerodromes, so it is the
calibration, not the denominator.

``TAXI_TIME_3`` semantics were measured against APDF in Task 4 Step 1 (see
``DATASETS.md`` under "Ground truth semantics" for the full numbers): it is
**taxi-out time only** (off-block ``AOBT_3`` to actual take-off), not total
(out + in) taxi time. ``AOBT_3 + TAXI_TIME_3`` predicts real APDF ATOT with
median error 0 s and IQR 17 s -- well under the ~120 s threshold that would
have forced boundary error to be reported only at APDF airports. ``ARVT_3``
is itself already a landing time (ALDT-like, not gate-arrival), matching real
APDF ALDT with median error 0 s and IQR 25 s. NM-inferred times are therefore
precise enough *in principle* for both matching and boundary error, not matching
alone -- ``t_source`` still travels with every row so a later study can revisit
this per airport if some subgroup turns out not to hold.

**What consumes them today is narrower, deliberately.**
``track_score.py:boundary_error`` still restricts itself to ``t_source ==
"apdf"``. That is conservatism, not a contradiction of the paragraph above:
narrowing a claim after seeing the numbers is safe, widening it is a
claim-scope decision, and it is recorded as **open** rather than settled. The
measurement here says widening would probably be justified; nothing here says
it has been done. Read the two files together, not either one alone.

**The join does not use callsign.** ``adep_ades.py:load_ground_truth`` joins on
``(icao24, callsign, day)``, which is right for the flight-list studies and wrong
here: this study's arm A4 removes callsign from track identity, and joining on it
would score A4 against a key it deliberately does not have. Matching is on
``icao24`` plus interval containment.

``REFERENCE_BASE`` defaults to the cluster's S3 path but every loader accepts a
``reference_base`` keyword that overrides it -- this module's own tests pass the
committed ``reference/`` directory so they run against real production data with
no cluster and no credentials.
"""

import datetime as dt

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

REFERENCE_BASE = "s3a://eurocontrol/opdi/research/reference"
#: overridable per call via the ``reference_base`` keyword -- see module docstring.

__all__ = ["load_flight_intervals", "load_apdf_times", "overlap_join"]


def _sample_window(days: list):
    """``(first_midnight, next_midnight_after_the_last_day)`` for *days*.

    The upper bound is exclusive and sits at the midnight *after* the last
    sampled day, because a sample at 23:59:59 is in the window and one at
    00:00:00 the next day is not. Derived from ``min``/``max`` of the caller's
    own day list; ``None`` when no days were given, in which case no window
    restriction applies. Accepts ``str`` or ``datetime.date`` entries, as the
    day filter does.
    """
    if not days:
        return None
    parsed = sorted(dt.date.fromisoformat(str(d)) for d in days)
    return (
        f"{parsed[0]} 00:00:00",
        f"{parsed[-1] + dt.timedelta(days=1)} 00:00:00",
    )


def load_apdf_times(
    spark: SparkSession, months: list, reference_base: str = REFERENCE_BASE
) -> tuple:
    """Real ATOT and ALDT, returned as two frames -- departures and arrivals.

    APDF has no literal ATOT column: departures and arrivals are separate rows
    discriminated by ``SRC_PHASE``, and the milestone is ``MVT_TIME_UTC`` in both.

    **The two are deliberately not joined to each other here.** Joining them on
    callsign would be a cross join across the month -- a callsign recurs daily,
    so every departure would match every arrival that ever carried it. They are
    joined to NM separately instead, each on ``(callsign, day, aerodrome)``,
    where NM supplies the ADEP and ADES that disambiguate the leg.

    Returns ``(dep, arr)``.
    """
    frames = [spark.read.parquet(f"{reference_base}/apdf_{m}.parquet") for m in months]
    ap = frames[0]
    for f_ in frames[1:]:
        ap = ap.unionByName(f_)

    ap = ap.select(
        F.trim(F.col("AP_C_FLTID")).alias("callsign"),
        F.col("SRC_PHASE"),
        F.col("MVT_TIME_UTC"),
        F.upper(F.trim(F.col("ADEP_ICAO"))).alias("apdf_adep"),
        F.upper(F.trim(F.col("ADES_ICAO"))).alias("apdf_ades"),
    ).withColumn("mvt_day", F.to_date("MVT_TIME_UTC"))

    # No dropDuplicates on (callsign, mvt_day, aerodrome): that key is not
    # unique -- same-day, same-route, same-callsign legs (16,174 of 462,676
    # keys in flights_202506, per Task 4 Step 1) share it, and deduplicating
    # to one arbitrary row per key would hand both legs the same candidate,
    # which is exactly the collision flight_key's t_off component exists to
    # prevent. Every candidate is kept; load_flight_intervals disambiguates
    # per NM row by proximity to that row's own time estimate.
    dep = ap.filter(F.col("SRC_PHASE") == "DEP").select(
        "callsign", "mvt_day", "apdf_adep", F.col("MVT_TIME_UTC").alias("atot")
    )
    arr = ap.filter(F.col("SRC_PHASE") == "ARR").select(
        "callsign", "mvt_day", "apdf_ades", F.col("MVT_TIME_UTC").alias("aldt")
    )
    return dep, arr


def load_flight_intervals(
    spark: SparkSession, months: list, days: list, reference_base: str = REFERENCE_BASE
) -> DataFrame:
    """One row per ground-truth flight: an airframe and an airborne interval.

    ``t_source`` records whether the interval's endpoints are APDF-measured or
    NM-inferred, so the paper can report boundary error only where it is real.

    **Ground truth is restricted to flights that fit entirely inside the
    sampled window**, i.e. ``t_off >= 00:00 of the first day in *days*`` and
    ``t_land < 00:00 of the day after the last``. That is not tidiness; it is
    what keeps a scored boundary error real. A caller filters state vectors on
    ``to_date(event_time).isin(days)`` while this function keys ground truth on
    the *departure* day, and the two windows do not close at the same instant:
    a flight leaving 2025-06-07 22:00 and landing 06-08 00:30 keeps its true
    ``t_land`` while its samples stop at 23:59:59, so the track's ``trk_end``
    is a clipping artefact and ``track_score.boundary_error`` reports ~30 min
    of fabricated overhang for a track that may be perfectly correct. The
    mirror case at the leading edge is the opposite and equally unwanted: a
    track continuing from the previous day has its ``trk_start`` clipped up to
    midnight, *understating* an overhang.

    The old boundary computation was structurally immune -- it took
    ``trk_end`` from ``matched``, which ``overlap_join`` clips to
    ``[t_off, t_land]`` by construction -- so this guard only became necessary
    when real track extents replaced it. The window is derived from
    ``min``/``max`` of ``days`` rather than taken as a parameter, so it cannot
    drift from the day list the same call already filters on.
    """
    frames = []
    for m in months:
        frames.append(
            spark.read.parquet(f"{reference_base}/flights_{m}.parquet").select(
                F.lower(F.col("AIRCRAFT_ADDRESS")).alias("icao24"),
                F.trim(F.col("AIRCRAFT_ID")).alias("callsign"),
                F.col("ADEP").alias("gt_adep"),
                F.col("ADES").alias("gt_ades"),
                F.col("AOBT_3").alias("aobt"),
                F.col("ARVT_3").alias("arvt"),
                F.col("TAXI_TIME_3").alias("taxi_min"),
            )
        )
    nm = frames[0]
    for f_ in frames[1:]:
        nm = nm.unionByName(f_)

    nm = nm.filter(F.col("icao24").isNotNull()).withColumn("day", F.to_date("aobt"))
    if days:
        nm = nm.filter(F.col("day").isin([str(d) for d in days]))
    # Whole-interval containment in the sampled window -- see the docstring.
    # Expressed on t_off/t_land, not on `day`: `day` is the departure day,
    # which is exactly the thing that fails to bound a midnight-crosser's
    # arrival. F.lit(True) when no days were given, so the chain below is the
    # same shape either way.
    window = _sample_window(days)
    in_window = (
        F.lit(True)
        if window is None
        else (F.col("t_off") >= F.lit(window[0]).cast("timestamp"))
        & (F.col("t_land") < F.lit(window[1]).cast("timestamp"))
    )

    # A synthetic per-row id. APDF is no longer deduplicated to one row per
    # (callsign, day, aerodrome) -- see load_apdf_times -- so a same-day
    # same-route same-callsign leg can now join against more than one real
    # candidate, and the natural key (callsign, day, ADEP/ADES) is exactly
    # what collides for those legs, so it cannot be the disambiguation
    # partition. A synthetic id keyed to the physical NM row can.
    nm = nm.withColumn("_nm_id", F.monotonically_increasing_id())

    # Each APDF side joins on its own aerodrome, so a leg cannot pick up
    # another leg's milestone. Left joins: APDF covers only PRU aerodromes,
    # and a flight it does not cover must still appear, with NM-inferred
    # times.
    #
    # Both joins finish with an explicit select, not a drop of the losing
    # side's duplicate-named columns. nm, dep and arr all carry a `callsign`
    # column; dropping the loser and re-aliasing still leaves Spark's resolver
    # able to reach the survivor through more than one qualifier path once a
    # second join follows, and a later bare `F.col("callsign")` fails with
    # AMBIGUOUS_REFERENCE. An explicit select collapses each join to a single,
    # uniquely-named schema, so nothing downstream can be ambiguous.
    #
    # Both joins can now fan out to several APDF candidates per NM row (the
    # dedup that used to prevent this is gone -- see load_apdf_times). Each
    # is resolved with row_number() over a window partitioned by the NM row's
    # own synthetic id and ordered by proximity to that row's own estimate:
    # AOBT_3 + TAXI_TIME_3 for departure, ARVT_3 for arrival. This is the same
    # pattern adep_ades.py:align_to_ground_truth uses ("ties are broken on
    # proximity ... rather than left to chance"), applied per NM row instead
    # of per natural key so that colliding legs are resolved independently
    # rather than collapsed into one.
    dep, arr = load_apdf_times(spark, months, reference_base=reference_base)
    jdep = nm.join(
        dep,
        (nm.callsign == dep.callsign)
        & (nm.day == dep.mvt_day)
        & (nm.gt_adep == dep.apdf_adep),
        "left",
    )
    # Ordered on proximity alone, no secondary key: exactly-equidistant
    # candidates tie non-deterministically (rare -- sub-second exact ties).
    w_dep = Window.partitionBy(nm._nm_id).orderBy(
        F.abs(
            F.unix_timestamp(dep.atot)
            - (F.unix_timestamp(nm.aobt) + F.coalesce(nm.taxi_min, F.lit(0.0)) * 60)
        ).asc_nulls_last()
    )
    jdep = (
        jdep.withColumn("_rdep", F.row_number().over(w_dep))
        .filter(F.col("_rdep") == 1)
        .select(
            nm._nm_id, nm.icao24, nm.callsign, nm.gt_adep, nm.gt_ades,
            nm.aobt, nm.arvt, nm.taxi_min, nm.day, dep.atot,
        )
    )

    # The arrival join keys on the arrival day, not the departure day. j.day
    # (as it was before this fix) is derived from AOBT_3 -- the departure
    # day -- and using it here was wrong: the dep-side day match is safe
    # because both its sides anchor to the same physical event (departure),
    # but ARVT_3 and APDF ARR's MVT_TIME_UTC both anchor to arrival, which
    # is not the same calendar day as departure for a flight that crosses
    # midnight. Keying on the departure day there was doubly wrong: it
    # dropped every midnight-crosser to nm_inferred (safe but lossy), and for
    # a same-day-same-route callsign collision where one leg crosses
    # midnight, it could let a departure day coincide with a *different*
    # leg's real arrival day and attach the wrong leg's ALDT while t_source
    # still read "apdf" -- silently wrong, not safely absent. Keying on
    # to_date(arvt) instead matches what this module's own DATASETS.md
    # calibration script already does (`nm["ARVT_3"].dt.date`), and recovers
    # midnight-crossing APDF matches instead of trading them away.
    j = jdep.join(
        arr,
        (jdep.callsign == arr.callsign)
        & (jdep.gt_ades == arr.apdf_ades)
        & (F.to_date(jdep.arvt) == arr.mvt_day),
        "left",
    )
    w_arr = Window.partitionBy(jdep._nm_id).orderBy(
        F.abs(F.unix_timestamp(arr.aldt) - F.unix_timestamp(jdep.arvt)).asc_nulls_last()
    )
    j = (
        j.withColumn("_rarr", F.row_number().over(w_arr))
        .filter(F.col("_rarr") == 1)
        .select(
            jdep.icao24, jdep.callsign, jdep.gt_adep, jdep.gt_ades,
            jdep.aobt, jdep.arvt, jdep.taxi_min, jdep.day, jdep.atot, arr.aldt,
        )
    )

    # APDF where it exists, NM inference otherwise. The inference is stated
    # rather than hidden: t_source travels with every row.
    #
    # Timestamp arithmetic goes through unix seconds. Spark has no way to add a
    # *column* of minutes as an interval -- `INTERVAL` literals are parsed at
    # plan time and cannot take a column operand.
    #
    # coalesce(taxi_min, 0.0) reads a missing TAXI_TIME_3 as "departed
    # instantly." Measured against flights_202506.parquet (957,396 rows):
    # TAXI_TIME_3 is null in 0 of them -- 0.0%. Negligible, so the coalesce is
    # kept as a formality rather than given a distinct t_source; re-measure if
    # this module is ever pointed at a month where that rate is not zero.
    t_off = F.coalesce(
        F.col("atot"),
        (
            F.unix_timestamp("aobt") + F.coalesce(F.col("taxi_min"), F.lit(0.0)) * 60
        ).cast("timestamp"),
    )
    t_land = F.coalesce(F.col("aldt"), F.col("arvt"))

    return (
        j.withColumn("t_off", t_off)
        .withColumn("t_land", t_land)
        .withColumn(
            "t_source",
            F.when(F.col("atot").isNotNull() & F.col("aldt").isNotNull(), "apdf")
            .otherwise("nm_inferred"),
        )
        .withColumn(
            "flight_key",
            # t_off is included: without it, two same-day same-route legs by
            # the same aircraft and callsign collapse into one flight_key.
            # Measured in flights_202506, 16,174 of 462,676 callsign+day keys
            # had more than one match -- exactly this collision -- and this
            # study measures merging, so a collision makes a segmentation
            # look better than it is in the statistic the paper reports.
            F.sha2(
                F.concat_ws(
                    "|", "icao24", "callsign", "day", "gt_adep", "gt_ades",
                    F.col("t_off").cast("string"),
                ),
                256,
            ),
        )
        .filter(F.col("t_off").isNotNull() & F.col("t_land").isNotNull())
        .filter(F.col("t_land") > F.col("t_off"))
        .filter(in_window)
        .select("flight_key", "icao24", "callsign", "gt_adep", "gt_ades",
                "t_off", "t_land", "t_source", "day")
    )


def overlap_join(assign: DataFrame, gt: DataFrame) -> DataFrame:
    """Attach each state vector to the ground-truth flight whose interval holds it.

    Airframe plus containment -- no callsign. A sample landing in two touching
    intervals is assigned to the earlier one, so back-to-back legs cannot
    double-count the boundary sample and inflate every merge statistic.

    **The "pick the earlier interval" window partitions on the assignment row,
    not on ``(icao24, event_time)``.** That distinction is not cosmetic: raw OSN
    state vectors can contain duplicate rows for the same airframe and instant
    (dedup on ``(track_id, timestamp)`` is listed in the project's evidence base
    as not yet implemented), and a window partitioned on the *sample* key would
    have collapsed every such duplicate to a single row -- silently undercounting
    the contingency table that every metric in ``track_score.py`` is computed
    from. A synthetic per-row id keeps duplicates as duplicates, so a
    segmentation is scored over the samples it was actually given. This is the
    same device ``load_flight_intervals`` uses for ``_nm_id``, and for the same
    reason: the natural key is not a row identity.

    The output select is an explicit, fixed list -- not a pass-through of
    whatever ``gt`` happens to carry. A generic pass-through was tried and
    reverted: against production ``gt`` (``load_flight_intervals``'s output)
    it silently leaked ``callsign`` and ``day`` into the result, in a module
    whose entire stated purpose is keeping callsign out of the scoring path
    for arm A4. Naming the columns here means ground truth missing one of
    them fails fast at the join, which is what a benchmark harness should do.
    """
    assign = assign.withColumn("_a_row", F.monotonically_increasing_id())
    j = assign.alias("a").join(
        gt.alias("g"),
        (F.col("a.icao24") == F.col("g.icao24"))
        & (F.col("a.event_time") >= F.col("g.t_off"))
        & (F.col("a.event_time") <= F.col("g.t_land")),
        "inner",
    )
    # Ordered on t_off alone: an exact tie there means two ground-truth flights
    # claiming the identical take-off instant for one airframe, which cannot
    # happen for physically distinct legs.
    w = Window.partitionBy("a._a_row").orderBy(F.col("g.t_off").asc())
    return (
        j.withColumn("_r", F.row_number().over(w))
        .filter(F.col("_r") == 1)
        .select(
            F.col("a.icao24").alias("icao24"),
            F.col("a.event_time").alias("event_time"),
            F.col("a.track_id").alias("track_id"),
            F.col("g.flight_key").alias("flight_key"),
            F.col("g.gt_adep").alias("gt_adep"),
            F.col("g.gt_ades").alias("gt_ades"),
            F.col("g.t_off").alias("t_off"),
            F.col("g.t_land").alias("t_land"),
            F.col("g.t_source").alias("t_source"),
        )
    )
