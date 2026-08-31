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

__all__ = [
    "load_flight_intervals", "load_apdf_times", "overlap_join",
    "gate_buffers", "attach_gate_interval",
]


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
        # The Samad flight id -- the same identifier NM carries in its own
        # ``ID`` column. This is the join key; see load_flight_intervals.
        F.col("ID").cast("long").alias("sam_id"),
        F.trim(F.col("AP_C_FLTID")).alias("callsign"),
        F.col("SRC_PHASE"),
        F.col("MVT_TIME_UTC"),
        F.col("BLOCK_TIME_UTC"),
        F.upper(F.trim(F.col("ADEP_ICAO"))).alias("apdf_adep"),
        F.upper(F.trim(F.col("ADES_ICAO"))).alias("apdf_ades"),
    ).filter(F.col("sam_id").isNotNull())

    # ``(sam_id, SRC_PHASE)`` is unique -- verified over apdf_202606, where it
    # holds for every row. So each frame below is one row per flight, and the
    # joins in load_flight_intervals are one-to-one with no disambiguation
    # needed.
    dep = ap.filter(F.col("SRC_PHASE") == "DEP").select(
        "sam_id", "apdf_adep",
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
        "sam_id", "apdf_ades",
        F.col("MVT_TIME_UTC").alias("aldt"),
        F.col("BLOCK_TIME_UTC").alias("aibt"),  # in-block; see `aobt` above
    )
    return dep, arr


def gate_buffers(gt: DataFrame) -> tuple:
    """Median taxi-out and taxi-in, in seconds, over the flights that measured them.

    These are the fallback where APDF has no block time. Measured rather than
    chosen: ``aobt`` covers ~100% of flights through NM's ``AOBT_3`` fallback,
    but ``aibt`` is APDF-only and covers about half, so roughly half the
    arrival intervals are modelled. Taking the modelled half's duration from
    the measured half is the honest version of a buffer; picking ten minutes
    because it sounds like a taxi is not.

    Returns ``(b_dep_s, b_arr_s)``. A period with no measured flight on one
    side falls back to zero there, degrading the gate interval to the airborne
    one rather than inventing a duration.

    **The median is the exact one, ``F.median``, not ``percentile_approx``.**
    ``track_score`` uses ``percentile_approx`` throughout and this deliberately
    does not, for two reasons. The aggregation here is a single scalar over a
    column of whole seconds -- taxi durations span a few thousand distinct
    values, not millions -- so the exact aggregate's value/count map is bounded
    and the approximation buys nothing. And ``percentile_approx`` does not
    interpolate: over two flights it returns the lower of the two taxi times
    rather than their midpoint, which makes the quantity untestable at fixture
    scale and, more to the point, not the median this docstring names. The
    difference is invisible on a real sample and decisive on a small one.
    """
    row = gt.select(
        F.median(
            F.when(
                F.col("dep_measured"),
                F.unix_timestamp("t_off") - F.unix_timestamp("aobt"),
            )
        ).alias("b_dep"),
        F.median(
            F.when(
                F.col("arr_measured"),
                F.unix_timestamp("aibt") - F.unix_timestamp("t_land"),
            )
        ).alias("b_arr"),
    ).collect()[0]
    return (float(row["b_dep"] or 0.0), float(row["b_arr"] or 0.0))


def attach_gate_interval(gt: DataFrame, b_dep_s: float, b_arr_s: float) -> DataFrame:
    """Add the gate-to-gate interval beside the airborne one.

    ``least``/``greatest`` are not defensive padding. APDF is operational data
    and carries rows whose block time falls the wrong side of its own movement
    time; without the clamp such a row yields a gate interval *narrower* than
    the airborne interval, and gate matching would drop samples airborne
    matching kept. The clamp makes the gate interval a guaranteed superset, so
    the two metrics differ only by the samples the wider one adds.

    ``gate_dep_measured``/``gate_arr_measured`` say whether the *block* time was
    observed, which is not what ``dep_measured``/``arr_measured`` say: those are
    about the movement times ATOT/ALDT. The two travel together because a gate
    interval can be measured at one end and modelled at the other, and a
    consumer cutting by aerodrome needs to know which.
    """
    return (
        gt.withColumn("gate_dep_measured", F.col("aobt").isNotNull())
        .withColumn("gate_arr_measured", F.col("aibt").isNotNull())
        .withColumn(
            "t_off_block",
            F.least(
                F.coalesce(
                    F.col("aobt"),
                    (F.unix_timestamp("t_off") - F.lit(b_dep_s)).cast("timestamp"),
                ),
                F.col("t_off"),
            ),
        )
        .withColumn(
            "t_in_block",
            F.greatest(
                F.coalesce(
                    F.col("aibt"),
                    (F.unix_timestamp("t_land") + F.lit(b_arr_s)).cast("timestamp"),
                ),
                F.col("t_land"),
            ),
        )
    )


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
                F.col("ID").cast("long").alias("sam_id"),
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

    # **Both APDF sides join on the Samad flight id.**
    #
    # NM's ``ID`` column is documented as the "Internal unique Samad Id", and
    # APDF carries the same identifier. It is an exact key, and it replaces the
    # whole apparatus that used to stand in for one: matching on callsign, a
    # day key, the aerodrome, and then a proximity tie-break to choose between
    # candidates.
    #
    # That apparatus was not merely complex, it was wrong at some of the
    # largest aerodromes, because ``AP_C_FLTID`` is whatever the airport system
    # holds and that is not always the ATC callsign. Measured over
    # 2026-06-05/07, flights matched out of NM's total:
    #
    #   * Frankfurt -- 48 of 963 on callsign (APDF carries IATA numbers there,
    #     "4Y002" against NM's ICAO callsigns); **1,992 of 2,003 on id**.
    #   * Amsterdam -- 27 of 985; **2,068 of 2,086**.
    #   * Zurich -- 175 of 561 (APDF zero-pads: "AAL093" against "AAL93");
    #     **1,187 of 1,198**.
    #   * Brussels -- 483 of 483, which is why the defect went unnoticed:
    #     the aerodromes that worked worked perfectly.
    #
    # Frankfurt, Amsterdam and Zurich were consequently reported as having no
    # measured milestones and dropped out of every metric that needs them.
    #
    # The id key also removes three problems the day key created and that the
    # comments here used to document at length: a taxi crossing midnight put
    # off-block and take-off on different days; a nightly rotation could attach
    # yesterday's movement, once producing a 26.9-hour ground-truth interval;
    # and same-day same-route legs by one airframe collided on the natural key.
    # None of them can arise from an equality join on a unique id.
    #
    # ``sam_id`` is null on 2.7% of APDF rows; those movements simply do not
    # match, which is the same outcome the old key gave them and better than
    # matching them to the wrong flight.
    jdep = nm.join(dep, nm.sam_id == dep.sam_id, "left").select(
        nm._nm_id, nm.icao24, nm.callsign, nm.gt_adep, nm.gt_ades,
        nm.aobt, nm.arvt, nm.taxi_min, nm.day, nm.sam_id,
        dep.atot, dep.aobt.alias("apdf_aobt"),
    )

    j = jdep.join(arr, jdep.sam_id == arr.sam_id, "left").select(
        jdep.icao24, jdep.callsign, jdep.gt_adep, jdep.gt_ades,
        jdep.aobt, jdep.arvt, jdep.taxi_min, jdep.day, jdep.atot,
        F.col("apdf_aobt"), arr.aldt, arr.aibt,
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

    flights = (
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
    )

    # The gate-to-gate interval, beside the airborne one -- see
    # :func:`attach_gate_interval`. Attached *after* the filters, so the
    # buffers are medians over the flights this call actually returns rather
    # than over everything the month's files happened to contain.
    #
    # This costs one extra pass over ground truth: `gate_buffers` collects, so
    # the plan above is evaluated once for the medians and once for whatever
    # the caller does with the result. Ground truth is ~1M rows a month against
    # billions of state vectors, and the alternative -- caching here and
    # unpersisting somewhere the caller cannot see -- trades a cheap re-read
    # for a lifetime this function does not own. Callers that read the frame
    # more than once cache it themselves (see track_methods.main).
    #
    # **Nothing above this line moved.** `flight_key`, `t_source`, the filters
    # and the airborne `t_off`/`t_land` are untouched, and the four new columns
    # are added to the select rather than replacing anything, so every metric
    # computed over `[t_off, t_land]` is bit-identical to what it was.
    b_dep_s, b_arr_s = gate_buffers(flights)
    return attach_gate_interval(flights, b_dep_s, b_arr_s).select(
        "flight_key", "icao24", "callsign", "gt_adep", "gt_ades",
        "t_off", "t_land", "aobt", "aibt", "t_source",
        "dep_measured", "arr_measured", "day",
        "t_off_block", "t_in_block", "gate_dep_measured", "gate_arr_measured",
    )


def overlap_join(assign: DataFrame, gt: DataFrame,
                 bounds=("t_off", "t_land")) -> DataFrame:
    """Attach each state vector to the ground-truth flight whose interval holds it.

    Airframe plus containment -- no callsign. A sample landing in two touching
    intervals is assigned to the earlier one, so back-to-back legs cannot
    double-count the boundary sample and inflate every merge statistic.

    ``bounds`` selects the interval. The default is the airborne
    ``[t_off, t_land]``. Passing ``("t_off_block", "t_in_block")`` matches over
    the gate-to-gate interval instead, which is what includes taxi-out, taxi-in
    and stand samples. The emitted ``t_off``/``t_land`` are unchanged either
    way: they are the airborne boundaries, and boundary error is defined against
    them regardless of which interval decided membership.

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
    lo, hi = bounds
    assign = assign.withColumn("_a_row", F.monotonically_increasing_id())
    j = assign.alias("a").join(
        gt.alias("g"),
        (F.col("a.icao24") == F.col("g.icao24"))
        & (F.col("a.event_time") >= F.col(f"g.{lo}"))
        & (F.col("a.event_time") <= F.col(f"g.{hi}")),
        "inner",
    )
    # Ordered on the interval's own lower bound alone: an exact tie there means
    # two ground-truth flights claiming the identical take-off -- or, under the
    # gate bounds, the identical off-block -- instant for one airframe, which
    # cannot happen for physically distinct legs.
    w = Window.partitionBy("a._a_row").orderBy(F.col(f"g.{lo}").asc())
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
