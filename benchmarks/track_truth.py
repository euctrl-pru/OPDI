"""Ground-truth flight intervals, and the join that scores a segmentation.

A segmentation is scored against what a flight *is*: an airframe and a span of
time. Network Manager ``flights_*.parquet`` gives both, across all of ECAC, at
957k flights a month. APDF gives truer times -- real ATOT and ALDT rather than
off-block and arrival -- but only at PRU-covered aerodromes, so it is the
calibration, not the denominator.

APDF also carries the two **block** times, ``AOBT`` and ``AIBT``, both of them
``BLOCK_TIME_UTC`` discriminated by ``SRC_PHASE`` exactly as the movement times
are. They bound the flight's ground phase, which is what lets a boundary error
be read as a *fraction* of taxi rather than as an absolute number of seconds.
``aibt`` is APDF-only and has no NM fallback -- and neither, in truth, is
``ARVT_3`` a measured landing. Its own column comment says "actual as
calculated from AOBT", and it reproduces as
``AOBT_3 + TAXI_TIME_3 + FLT_DUR_3`` to within 7 s at the median and 30 s at
the maximum over 96,146 flights (2026-06-05/07): the residual is
minute-rounding. It is a model output, accurate but not observed, and NM
carries no runway timestamp of any kind.

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


def _prefilter_days(days: list):
    """``(first_day - 1, last_day + 1)`` -- the off-block days worth reading.

    A *pruning* bound, deliberately wider than ``_sample_window`` and never the
    thing that decides membership. ``day`` is ``to_date(AOBT_3)``, the off-block
    day, while the window is expressed on ``t_off``/``t_land``: a flight that
    pushes back at 23:5x and gets airborne after midnight carries the previous
    day's key and belongs in the sample, so the read has to reach one day back.
    The trailing day is symmetry rather than necessity -- ``t_off >= aobt``
    under any non-negative taxi time -- and it costs one day of NM rows.

    ``None`` when no days were given, matching ``_sample_window``.
    """
    if not days:
        return None
    parsed = sorted(dt.date.fromisoformat(str(d)) for d in days)
    return (
        str(parsed[0] - dt.timedelta(days=1)),
        str(parsed[-1] + dt.timedelta(days=1)),
    )


def _estimated_atot(aobt, taxi_min):
    """NM's own take-off estimate: ``AOBT_3 + TAXI_TIME_3``, as a timestamp.

    ``CLAUDE.md`` records this as matching real APDF ATOT with median error 0 s
    and IQR 17 s, measured in Task 4 Step 1 -- which is why it is trusted both
    to stand in for a missing milestone and to say which day a take-off falls
    on.

    One definition, three users: the departure join's day key, that join's
    proximity ordering, and ``t_off``'s fallback where APDF has no milestone at
    all. Those were three copies of the same arithmetic, and the day key is the
    one that must not drift from the others -- a key computed one way and an
    estimate ranked another would match candidates on one clock and choose
    between them on a different one.

    Arithmetic goes through unix seconds because Spark cannot add a *column* of
    minutes as an interval: ``INTERVAL`` literals are parsed at plan time and
    cannot take a column operand. ``coalesce(taxi_min, 0.0)`` reads a missing
    ``TAXI_TIME_3`` as "departed instantly"; measured against
    ``flights_202506.parquet`` (957,396 rows) it is null in 0 of them, so the
    coalesce is a formality -- re-measure if this module is ever pointed at a
    month where that rate is not zero.
    """
    return (F.unix_timestamp(aobt) + F.coalesce(taxi_min, F.lit(0.0)) * 60).cast(
        "timestamp"
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
        F.col("BLOCK_TIME_UTC"),
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
        "callsign", "mvt_day", "apdf_adep",
        F.col("MVT_TIME_UTC").alias("atot"),
        # Off-block. APDF has no literal AOBT column either: BLOCK_TIME_UTC is
        # off-block on a DEP row and in-block on an ARR row, which is the same
        # SRC_PHASE discrimination already applied to MVT_TIME_UTC above.
        #
        # Carried so a consumer can normalise boundary error by the flight's
        # own ground phase. An absolute number of seconds is not comparable
        # between aerodromes: a track beginning 180 s before take-off saw all of
        # a regional field's three-minute taxi and about a seventh of a major
        # hub's twenty-minute one, and the two are indistinguishable once the
        # denominator is dropped.
        F.col("BLOCK_TIME_UTC").alias("aobt"),
    )
    arr = ap.filter(F.col("SRC_PHASE") == "ARR").select(
        "callsign", "mvt_day", "apdf_ades",
        F.col("MVT_TIME_UTC").alias("aldt"),
        F.col("BLOCK_TIME_UTC").alias("aibt"),  # in-block; see `aobt` above
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

    **The ``day`` pre-filter is a pruning aid, not the rule.** ``day`` is
    ``to_date(AOBT_3)`` -- the off-block day -- and it is filtered to
    ``[first day - 1, last day + 1]`` (``_prefilter_days``) so that the
    ``t_off``/``t_land`` comparison above is what actually decides membership.
    It was previously filtered to ``days`` exactly, which made the off-block
    day the rule and contradicted the paragraph above: a flight pushing back at
    23:5x and airborne after midnight was dropped although its whole interval
    lay inside the window. Task 4's containment census measured the loss at 53
    flights of 93,026 (2025) and 55 of 90,867 (2024) -- 0.06 %, all in the same
    direction, so a small systematic bias rather than noise.

    One residual edge, stated because it is silent: ``months`` decides which
    files are read at all, so when the sample starts on the 1st the widened day
    reaches into the *previous month* and finds nothing unless the caller also
    passes that month. ``CLAUDE.md`` makes the same point about ``apdf_tidy()``
    covering one month at a time. Both V1 study samples are mid-June, so
    neither is affected; a sample abutting a month edge should pass the
    adjacent month in ``months``.
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
        # Pruning only, one day wider than the window on each side -- see
        # _prefilter_days. This used to be `day.isin(days)`, which made the
        # off-block day decide membership and dropped every flight that pushed
        # back before midnight and got airborne after it. Cast the bounds to
        # date explicitly: `day` is a DateType and a bare string literal leaves
        # the comparison's casting to the analyser.
        lo, hi = _prefilter_days(days)
        nm = nm.filter(
            F.col("day").between(F.lit(lo).cast("date"), F.lit(hi).cast("date"))
        )
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

    # **The departure join keys on the take-off day, not the off-block day.**
    # ``day`` is ``to_date(AOBT_3)`` (off-block) while ``dep.mvt_day`` is
    # ``to_date(MVT_TIME_UTC)`` (take-off), and a taxi that crosses midnight
    # puts the two on different days -- 2,272 of 957,396 NM flights in 202506
    # (0.237 %) and 2,102 of 935,887 in 202406 (0.225 %), measured over the
    # committed reference extracts. Keying on ``day`` was wrong in both of the
    # ways the arrival side was, and is fixed the same way:
    #
    #   * it dropped every midnight-taxi leg to ``t_source = "nm_inferred"``,
    #     which is lossy rather than wrong -- but not harmlessly so.
    #     ``track_score.boundary_error`` restricts itself to APDF rows, so the
    #     flights it never saw were systematically the midnight-taxi ones. A
    #     lossy fallback that loses a random 0.2 % is noise; one that loses a
    #     0.2 % correlated with taxi behaviour, in a study about taxi
    #     behaviour, is a bias;
    #   * and where the same callsign had an earlier movement from the same
    #     aerodrome on the off-block day -- which for a nightly service means
    #     *yesterday's rotation* -- that movement was the only candidate the key
    #     admitted, so the join attached a take-off hours before push-back.
    #     Measured, not hypothesised. icao24 4bce13, SXS9ZZ, EDDV->LTAI,
    #     AOBT_3 2025-06-05 23:52 (+10 min taxi): APDF holds a DEP at
    #     2025-06-06 00:02:15, 15 s from NM's own estimate, and another at
    #     2025-06-05 00:20:03 -- the night before. The off-block-day key
    #     admitted only the latter, so this leg's t_off became 2025-06-05
    #     00:20:03 against its own ARVT_3 of 2025-06-06 03:16: a **26.9-hour
    #     ground-truth interval**. 10 such intervals over 20 h in the 2025
    #     sample and 9 in 2024. After this fix: **none in 2025, and two in
    #     2024** -- the longest 27.9 h, icao24 4bb194, THY801, LTFM->SKBO.
    #
    #     Both post-fix counts are stated because only one of them is zero.
    #     Quoting the 2025 zero alone would read as "the class of defect is
    #     eliminated", and it is not. The two survivors are **not** this
    #     defect: they carry no APDF candidate at either end, so their interval
    #     is NM's own `AOBT_3`/`ARVT_3` pairing and no join key of ours
    #     produced it. They are **undiagnosed**, and deliberately not filtered
    #     -- a sanity bound on interval length would move the denominator of
    #     every metric in the study, which is a scope decision rather than a
    #     cleanup. Written down here so the residual outlives the report that
    #     found it.
    #
    #     `overlap_join` assigns by containment, so an interval that long
    #     swallows a whole day of that airframe's samples -- including its
    #     neighbouring legs' -- into `match_rates`. Note *how*, because it is
    #     not the inflated merge rate one first assumes. Traced through
    #     `match_rates`: a neighbouring leg that loses all its samples drops
    #     out of the `n_flights` **denominator** rather than counting as
    #     merged; the over-long flight itself now spans several tracks and
    #     scores **fragmented**; and a genuine merge whose samples all carry
    #     this one `flight_key` is **masked**. Contaminated in three
    #     directions, none of them the obvious one -- which is why the earlier
    #     wording here, "straight into the merge statistic", was wrong and is
    #     corrected rather than softened.
    #
    #     Note what did *not* protect against it: `t_source` read
    #     "nm_inferred" for that row, because LTAI is not APDF-covered so
    #     `aldt` was null, and t_source is "apdf" only when both ends are
    #     measured. `t_off` takes `atot` whenever it exists, independently of
    #     the label. So `boundary_error`'s APDF-only restriction was no guard
    #     here at all. See the t_source computation below.
    #
    # The key is ``to_date(AOBT_3 + TAXI_TIME_3)`` -- the same estimate the
    # proximity ordering below ranks on and ``t_off`` falls back to, computed
    # once in ``_estimated_atot`` so the three cannot drift apart.
    est_atot = _estimated_atot(nm.aobt, nm.taxi_min)
    jdep = nm.join(
        dep,
        (nm.callsign == dep.callsign)
        & (F.to_date(est_atot) == dep.mvt_day)
        & (nm.gt_adep == dep.apdf_adep),
        "left",
    )
    # Ordered on proximity alone, no secondary key: exactly-equidistant
    # candidates tie non-deterministically (rare -- sub-second exact ties).
    w_dep = Window.partitionBy(nm._nm_id).orderBy(
        F.abs(F.unix_timestamp(dep.atot) - F.unix_timestamp(est_atot)).asc_nulls_last()
    )
    jdep = (
        jdep.withColumn("_rdep", F.row_number().over(w_dep))
        .filter(F.col("_rdep") == 1)
        .select(
            nm._nm_id, nm.icao24, nm.callsign, nm.gt_adep, nm.gt_ades,
            # nm.aobt is NM's own off-block (AOBT_3); dep.aobt is APDF's
            # measured one. Both are carried and they are not the same column --
            # hence the alias, without which the later select is ambiguous.
            nm.aobt, nm.arvt, nm.taxi_min, nm.day, dep.atot,
            dep.aobt.alias("apdf_aobt"),
        )
    )

    # The arrival join keys on the arrival day, not the departure day. j.day
    # (as it was before this fix) is derived from AOBT_3 -- the departure
    # day -- and using it here was wrong: the dep-side day match is at least
    # anchored near departure on both sides (off-block and take-off, minutes
    # apart -- though see the note above the dep join, where a midnight taxi
    # separates even those), but ARVT_3 and APDF ARR's MVT_TIME_UTC both
    # anchor to arrival, which is hours away and not the same calendar day as
    # departure for a flight that crosses midnight. Keying on the departure day
    # there was doubly wrong: it dropped every midnight-crosser to nm_inferred
    # (safe but lossy), and for a same-day-same-route callsign collision where
    # one leg crosses midnight, it could let a departure day coincide with a
    # *different* leg's real arrival day and attach the wrong leg's ALDT while
    # t_source still read "apdf" -- silently wrong, not safely absent. Keying on
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
            jdep.aobt, jdep.arvt, jdep.taxi_min, jdep.day, jdep.atot,
            F.col("apdf_aobt"), arr.aldt, arr.aibt,
        )
    )

    # APDF where it exists, NM inference otherwise. The inference is stated
    # rather than hidden: t_source travels with every row. The estimate is
    # _estimated_atot -- the same one the departure join keys and ranks on, so
    # a flight cannot be matched against one take-off time and then have
    # another written into t_off.
    t_off = F.coalesce(
        F.col("atot"), _estimated_atot(F.col("aobt"), F.col("taxi_min"))
    )
    t_land = F.coalesce(F.col("aldt"), F.col("arvt"))

    return (
        j.withColumn("t_off", t_off)
        .withColumn("t_land", t_land)
        .withColumn(
            # **"nm_inferred" does not mean "no APDF touched this row."** It
            # means not *both* ends were measured, which is the right rule for
            # boundary_error (it needs both) but is weaker than it looks: t_off
            # above takes `atot` whenever it exists, so a row with a real ATOT
            # and no ALDT -- an APDF-covered departure to an uncovered
            # aerodrome -- carries a measured take-off under the "nm_inferred"
            # label. It is also what let the departure-join defect above reach
            # the output unguarded.
            #
            # `t_source` itself is deliberately unchanged, because changing it
            # would move which rows `boundary_error` counts and therefore which
            # flights every published V1 number covers. The per-endpoint
            # answer is carried *beside* it instead, as `dep_measured` and
            # `arr_measured` -- see below.
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
        # Block times, exposed under the names a consumer expects.
        #
        # `aobt` prefers APDF's measured off-block and falls back to NM's
        # AOBT_3, mirroring how `t_off` prefers `atot`. `aibt` has **no NM
        # fallback and cannot have one**: ARVT_3 is not a gate arrival, and is
        # not a measured landing either -- it reproduces as
        # AOBT_3 + TAXI_TIME_3 + FLT_DUR_3 to within 7 s at the median, so it
        # carries nothing the off-block time and the two model durations do
        # not already say. There is no in-block anywhere outside APDF. It is
        # therefore NULL at every aerodrome APDF does not cover, and a consumer
        # computing an arrival ground phase must treat NULL as "unmeasurable
        # here" rather than as zero.
        #
        # Neither is part of `flight_key`. Adding them would change every key
        # this module has ever produced.
        .withColumn("aobt", F.coalesce(F.col("apdf_aobt"), F.col("aobt")))
        # **Per-endpoint provenance.** `t_source` is "apdf" only when *both*
        # ends are measured, which is the right rule for a metric needing both
        # -- and wrong for anything computed one end at a time.
        #
        # Measured on 2025-06-05/07: 44,841 flights carry a real APDF AIBT, but
        # only 22,588 are labelled "apdf", because the other 22,254 departed
        # from an aerodrome APDF does not cover. Reading the flight-level label
        # as an aerodrome's provenance therefore mis-classifies **26 aerodromes
        # with 20+ movements whose arrivals are 99-100% measured** -- Helsinki,
        # Stuttgart, Keflavik, Charleroi among them -- because most of *their*
        # traffic comes from uncovered aerodromes. A consumer cutting by
        # aerodrome needs the endpoint, not the pair.
        #
        # This is the split the docstring below records as observed and left
        # alone. It is no longer left alone: the flags are added beside
        # `t_source` rather than changing it, so nothing that reads `t_source`
        # moves.
        .withColumn("dep_measured", F.col("atot").isNotNull())
        .withColumn(
            "arr_measured",
            F.col("aldt").isNotNull() & F.col("aibt").isNotNull(),
        )
        .filter(F.col("t_off").isNotNull() & F.col("t_land").isNotNull())
        .filter(F.col("t_land") > F.col("t_off"))
        .filter(in_window)
        .select("flight_key", "icao24", "callsign", "gt_adep", "gt_ades",
                "t_off", "t_land", "aobt", "aibt", "t_source",
                "dep_measured", "arr_measured", "day")
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
