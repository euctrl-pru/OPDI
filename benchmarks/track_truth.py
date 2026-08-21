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
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

REFERENCE_BASE = "s3a://eurocontrol/opdi/research/reference"

__all__ = ["load_flight_intervals", "load_apdf_times", "overlap_join"]


def load_apdf_times(spark: SparkSession, months: list) -> tuple:
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
    frames = [spark.read.parquet(f"{REFERENCE_BASE}/apdf_{m}.parquet") for m in months]
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


def load_flight_intervals(spark: SparkSession, months: list, days: list) -> DataFrame:
    """One row per ground-truth flight: an airframe and an airborne interval.

    ``t_source`` records whether the interval's endpoints are APDF-measured or
    NM-inferred, so the paper can report boundary error only where it is real.
    """
    frames = []
    for m in months:
        frames.append(
            spark.read.parquet(f"{REFERENCE_BASE}/flights_{m}.parquet").select(
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
    dep, arr = load_apdf_times(spark, months)
    j = (
        nm.join(
            dep,
            (nm.callsign == dep.callsign)
            & (nm.day == dep.mvt_day)
            & (nm.gt_adep == dep.apdf_adep),
            "left",
        )
        .drop(dep.callsign, dep.mvt_day, dep.apdf_adep)
        .alias("d")
    )
    j = (
        j.join(
            arr,
            (F.col("d.callsign") == arr.callsign)
            & (F.col("d.gt_ades") == arr.apdf_ades),
            "left",
        )
        .drop(arr.callsign, arr.mvt_day, arr.apdf_ades)
    )

    # APDF where it exists, NM inference otherwise. The inference is stated
    # rather than hidden: t_source travels with every row.
    #
    # Timestamp arithmetic goes through unix seconds. Spark has no way to add a
    # *column* of minutes as an interval -- `INTERVAL` literals are parsed at
    # plan time and cannot take a column operand.
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
            F.sha2(F.concat_ws("|", "icao24", "callsign", "day", "gt_adep", "gt_ades"), 256),
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

    Output columns are ``assign``'s ``icao24``/``event_time``/``track_id`` plus
    every column ``gt`` carries other than its own ``icao24`` (duplicate key).
    This is deliberately generic rather than a fixed list of ``gt``'s
    production columns (``flight_key``, ``gt_adep``, ``gt_ades``, ``t_off``,
    ``t_land``, ``t_source``, ...): a hardcoded select would break on any ``gt``
    frame -- including the minimal ones this module's own tests build -- that
    does not carry every one of those columns.
    """
    j = assign.alias("a").join(
        gt.alias("g"),
        (F.col("a.icao24") == F.col("g.icao24"))
        & (F.col("a.event_time") >= F.col("g.t_off"))
        & (F.col("a.event_time") <= F.col("g.t_land")),
        "inner",
    )
    w = Window.partitionBy("a.icao24", "a.event_time").orderBy(F.col("g.t_off").asc())
    gt_cols = [c for c in gt.columns if c != "icao24"]
    return (
        j.withColumn("_r", F.row_number().over(w))
        .filter(F.col("_r") == 1)
        .select(
            F.col("a.icao24").alias("icao24"),
            F.col("a.event_time").alias("event_time"),
            F.col("a.track_id").alias("track_id"),
            *[F.col(f"g.{c}").alias(c) for c in gt_cols],
        )
    )
