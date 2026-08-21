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
trusted here for both matching *and* boundary error, not matching alone --
``t_source`` still travels with every row so a later study can revisit this
per airport if some subgroup turns out not to hold.

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

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

REFERENCE_BASE = "s3a://eurocontrol/opdi/research/reference"
#: overridable per call via the ``reference_base`` keyword -- see module docstring.

__all__ = ["load_flight_intervals", "load_apdf_times", "overlap_join"]


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

    dep = (
        ap.filter(F.col("SRC_PHASE") == "DEP")
        .select("callsign", "mvt_day", "apdf_adep", F.col("MVT_TIME_UTC").alias("atot"))
        .dropDuplicates(["callsign", "mvt_day", "apdf_adep"])
    )
    arr = (
        ap.filter(F.col("SRC_PHASE") == "ARR")
        .select("callsign", "mvt_day", "apdf_ades", F.col("MVT_TIME_UTC").alias("aldt"))
        .dropDuplicates(["callsign", "mvt_day", "apdf_ades"])
    )
    return dep, arr


def load_flight_intervals(
    spark: SparkSession, months: list, days: list, reference_base: str = REFERENCE_BASE
) -> DataFrame:
    """One row per ground-truth flight: an airframe and an airborne interval.

    ``t_source`` records whether the interval's endpoints are APDF-measured or
    NM-inferred, so the paper can report boundary error only where it is real.
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

    # Each APDF side joins on its own aerodrome, so a leg cannot pick up another
    # leg's milestone. Left joins: APDF covers only PRU aerodromes, and a flight
    # it does not cover must still appear, with NM-inferred times.
    #
    # Both joins finish with an explicit select, not a drop of the losing
    # side's duplicate-named columns. nm, dep and arr all carry a `callsign`
    # column; dropping the loser and re-aliasing still leaves Spark's resolver
    # able to reach the survivor through more than one qualifier path once a
    # second join follows, and a later bare `F.col("callsign")` fails with
    # AMBIGUOUS_REFERENCE. An explicit select collapses each join to a single,
    # uniquely-named schema, so nothing downstream can be ambiguous.
    dep, arr = load_apdf_times(spark, months, reference_base=reference_base)
    j = nm.join(
        dep,
        (nm.callsign == dep.callsign)
        & (nm.day == dep.mvt_day)
        & (nm.gt_adep == dep.apdf_adep),
        "left",
    ).select(
        nm.icao24, nm.callsign, nm.gt_adep, nm.gt_ades,
        nm.aobt, nm.arvt, nm.taxi_min, nm.day, dep.atot,
    )
    # The arrival join also needs a day match (mirroring the dep side above).
    # Missing that here previously caused a real fan-out bug: with only
    # callsign+ADES as the key, a recurring route matched every ARR row APDF
    # had for that callsign+ADES pair across the whole month, producing
    # several rows per flight_key with wildly different (wrong) t_land values.
    j = j.join(
        arr,
        (j.callsign == arr.callsign)
        & (j.gt_ades == arr.apdf_ades)
        & (j.day == arr.mvt_day),
        "left",
    ).select(
        j.icao24, j.callsign, j.gt_adep, j.gt_ades,
        j.aobt, j.arvt, j.taxi_min, j.day, j.atot, arr.aldt,
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
        .select("flight_key", "icao24", "callsign", "gt_adep", "gt_ades",
                "t_off", "t_land", "t_source", "day")
    )


def overlap_join(assign: DataFrame, gt: DataFrame) -> DataFrame:
    """Attach each state vector to the ground-truth flight whose interval holds it.

    Airframe plus containment -- no callsign. A sample landing in two touching
    intervals is assigned to the earlier one, so back-to-back legs cannot
    double-count the boundary sample and inflate every merge statistic.

    The output select is an explicit, fixed list -- not a pass-through of
    whatever ``gt`` happens to carry. A generic pass-through was tried and
    reverted: against production ``gt`` (``load_flight_intervals``'s output)
    it silently leaked ``callsign`` and ``day`` into the result, in a module
    whose entire stated purpose is keeping callsign out of the scoring path
    for arm A4. Naming the columns here means ground truth missing one of
    them fails fast at the join, which is what a benchmark harness should do.
    """
    j = assign.alias("a").join(
        gt.alias("g"),
        (F.col("a.icao24") == F.col("g.icao24"))
        & (F.col("a.event_time") >= F.col("g.t_off"))
        & (F.col("a.event_time") <= F.col("g.t_land")),
        "inner",
    )
    w = Window.partitionBy("a.icao24", "a.event_time").orderBy(F.col("g.t_off").asc())
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
