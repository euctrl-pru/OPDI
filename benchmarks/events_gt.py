#!/usr/bin/env python
"""EUROCONTROL ground truth for the flight-event benchmark.

APDF is in long/movement form -- there is no literal ATOT or ALDT column, and
which milestone a row carries depends on ``SRC_PHASE``. It also has **no
aircraft address**, so it cannot be joined to ADS-B directly; the route to
``icao24`` runs through ``flights_<month>.parquet`` on ``apdf.ID = flights.ID``
(= ``IM_SAMAD_ID``). That bridge is measured before anything else, because it
caps every coverage figure downstream: a milestone we cannot reach is
indistinguishable from one we failed to detect unless the ceiling is known.

Three traps are closed here rather than in the caller, because each of them
fails silently and each has cost this project a run before:

**Timezone.** Every APDF and flights timestamp is tagged ``Europe/Paris``
despite the ``_UTC`` suffix. The *instants* are correct, so converting is right
and stripping the zone is a two-hour error. Spark's session timezone is set to
UTC only in the local builder (``osn_sample.py``), never in the distributed
path, so this module asserts it rather than assuming it. Harmless at day
granularity; fatal for a metric whose whole output is a seconds-level error.

**Callsign vocabulary.** ``AP_C_FLTID`` and ``flights.AIRCRAFT_ID`` disagree on
20.4% of rows, and the reason is systematic rather than dirty data: APDF
largely carries the IATA commercial number (``LH416``) while ``flights``
carries the ICAO callsign (``DLH416``), which is what ADS-B broadcasts. Joining
on APDF's would quietly discard a fifth of the sample and read as a detection
failure.

**Padding.** ADS-B callsigns are space-padded to eight characters. An untrimmed
join matches nothing and shows up as every method scoring 0.00%, not as an
error -- version 6 shipped a CSV of zeros exactly this way.
"""

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

#: Mirrored to S3 because executors cannot read the driver's filesystem.
REFERENCE_BASE = os.environ.get(
    "OPDI_REFERENCE_BASE", "s3a://eurocontrol/opdi/research/reference"
)

PERIODS = {
    "2025": {"month": "202506", "days": ["2025-06-05", "2025-06-06", "2025-06-07"]},
    "2024": {"month": "202406", "days": ["2024-06-05", "2024-06-06", "2024-06-07"]},
}

#: Milestones APDF can score, and how to recover each from the long form.
#: SRC_PHASE is the discriminator: there is no ATOT column.
MILESTONES = {
    "ATOT": ("MVT_TIME_UTC", "DEP"),
    "ALDT": ("MVT_TIME_UTC", "ARR"),
    "AOBT": ("BLOCK_TIME_UTC", "DEP"),
    "AIBT": ("BLOCK_TIME_UTC", "ARR"),
}


def assert_utc_session(spark: SparkSession) -> None:
    """Fail loudly if the session timezone is not UTC.

    An assertion rather than a fix-up: silently setting it would hide the fact
    that the distributed builder does not, and the next benchmark would have to
    rediscover it.
    """
    tz = spark.conf.get("spark.sql.session.timeZone")
    if tz != "UTC":
        raise RuntimeError(
            f"spark.sql.session.timeZone is {tz!r}, not 'UTC'. Reference "
            "timestamps are tagged Europe/Paris; reading them in another zone "
            "shifts every milestone by the offset and the error is invisible "
            "in the output. Set it before building the ground truth."
        )


def load_apdf(spark: SparkSession, month: str, full: bool = True) -> DataFrame:
    """APDF movements for one month.

    ``full`` reads ``apdf_full_<month>.parquet``, the re-extraction that keeps
    the ``_CPF``, ``_CTFM`` and ``TRANSIT`` families ``apdf_tidy()`` drops.
    Falls back to the original extract when it is absent, so the benchmark runs
    on a checkout that has not pulled the newer file.
    """
    name = f"apdf_full_{month}.parquet" if full else f"apdf_{month}.parquet"
    try:
        return spark.read.parquet(f"{REFERENCE_BASE}/{name}")
    except Exception:
        if not full:
            raise
        print(f"  {name} not found; falling back to apdf_{month}.parquet")
        return spark.read.parquet(f"{REFERENCE_BASE}/apdf_{month}.parquet")


def load_flights(spark: SparkSession, month: str) -> DataFrame:
    return spark.read.parquet(f"{REFERENCE_BASE}/flights_{month}.parquet")


def bridge(apdf: DataFrame, flights: DataFrame) -> DataFrame:
    """Attach ``icao24`` and the ICAO callsign to each APDF movement.

    ``apdf.ID`` is ``IM_SAMAD_ID``, the internal key linking the two extracts.
    It is the clean join -- not a fuzzy match on callsign and registration --
    and it is ~97% populated on the APDF side.
    """
    fl = flights.select(
        F.col("ID").alias("_fl_id"),
        F.lower(F.col("AIRCRAFT_ADDRESS")).alias("icao24"),
        # The ICAO callsign, which is what ADS-B broadcasts. Not AP_C_FLTID.
        F.trim(F.col("AIRCRAFT_ID")).alias("callsign"),
        F.col("REGISTRATION").alias("registration"),
        F.col("ADEP").alias("gt_adep"),
        F.col("ADES").alias("gt_ades"),
        F.col("AOBT_3").alias("gt_aobt"),
    ).filter(F.col("_fl_id").isNotNull())

    return apdf.join(F.broadcast(fl), apdf.ID == F.col("_fl_id"), "left").drop("_fl_id")


def bridge_report(bridged: DataFrame) -> dict:
    """The gate. Everything downstream is capped by this."""
    agg = bridged.agg(
        F.count(F.lit(1)).alias("movements"),
        F.sum(F.when(F.col("ID").isNull(), 1).otherwise(0)).alias("null_id"),
        F.sum(F.when(F.col("icao24").isNotNull(), 1).otherwise(0)).alias("reached"),
    ).collect()[0]
    n = agg["movements"]
    return {
        "movements": n,
        "reached_icao24": agg["reached"],
        "reach_pct": round(100.0 * agg["reached"] / n, 2) if n else 0.0,
        "null_apdf_id": agg["null_id"],
    }


def milestones(bridged: DataFrame, days) -> DataFrame:
    """One row per (flight, milestone), long form.

    Restricting to the benchmark days is load-bearing: scoring three days of
    tracks against a whole month of ground truth divides every coverage figure
    by roughly ten and looks like a catastrophic detector rather than a
    mismatched denominator.
    """
    b = bridged.filter(F.col("icao24").isNotNull())
    b = b.withColumn("day", F.to_date(F.from_utc_timestamp(F.col("MVT_TIME_UTC"), "UTC")))
    if days:
        b = b.filter(F.col("day").isin(list(days)))

    parts = []
    for name, (column, phase) in MILESTONES.items():
        parts.append(
            b.filter(F.col("SRC_PHASE") == phase)
            .filter(F.col(column).isNotNull())
            .select(
                "icao24", "callsign", "day", "gt_aobt", "gt_adep", "gt_ades",
                F.lit(name).alias("milestone"),
                F.col(column).alias("gt_time"),
                F.when(F.col("SRC_PHASE") == "DEP", F.col("ADEP_ICAO"))
                .otherwise(F.col("ADES_ICAO")).alias("gt_airport"),
                F.col("AP_C_RWY").alias("gt_runway"),
                # Whether this airport reports to the second. 64% of
                # MVT_TIME_UTC values land on a whole minute, so an unstratified
                # error distribution is dominated by the truth's own
                # quantisation rather than by the detector.
                (F.second(F.col(column)) != 0).alias("gt_subminute"),
            )
        )
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out


def ring_truth(bridged: DataFrame, days) -> DataFrame:
    """C40/C100 crossings -- the *precise* target.

    Only 1.7% of these land on a whole minute, against 64% of the movement
    times, so the ring comparison carries the headline accuracy claim and
    ATOT/ALDT the coarse one. Where the re-extraction is present, ``_CTFM``
    comes too: it is a second EUROCONTROL derivation of the same crossing, and
    the spread between them is the floor on what agreement can mean.
    """
    b = bridged.filter(F.col("icao24").isNotNull()).filter(F.col("SRC_PHASE") == "ARR")
    b = b.withColumn("day", F.to_date(F.from_utc_timestamp(F.col("MVT_TIME_UTC"), "UTC")))
    if days:
        b = b.filter(F.col("day").isin(list(days)))

    parts = []
    for ring in (40, 100):
        cols = [
            F.lit(f"xing-{ring}nm").alias("milestone"),
            F.col(f"C{ring}_CROSS_TIME").alias("gt_time"),
            F.col(f"C{ring}_CROSS_LAT").alias("gt_lat"),
            F.col(f"C{ring}_CROSS_LON").alias("gt_lon"),
            F.col(f"C{ring}_CROSS_FL").alias("gt_fl"),
            F.col(f"C{ring}_BEARING").alias("gt_bearing"),
            F.col("ADES_ICAO").alias("gt_airport"),
        ]
        ctfm = f"C{ring}_CROSS_TIME_CTFM"
        cols.append(
            (F.col(ctfm) if ctfm in b.columns else F.lit(None).cast("timestamp")).alias(
                "gt_time_ctfm"
            )
        )
        parts.append(
            b.filter(F.col(f"C{ring}_CROSS_TIME").isNotNull()).select(
                "icao24", "callsign", "day", "gt_aobt", "gt_adep", "gt_ades", *cols
            )
        )
    return parts[0].unionByName(parts[1])


def build(spark: SparkSession, period: str, full: bool = True):
    """Ground truth for one period, plus the bridge report."""
    assert_utc_session(spark)
    spec = PERIODS[period]

    apdf = load_apdf(spark, spec["month"], full=full)
    flights = load_flights(spark, spec["month"])
    bridged = bridge(apdf, flights).cache()

    report = bridge_report(bridged)
    print(f"  bridge: {report['reached_icao24']:,} of {report['movements']:,} "
          f"movements reach icao24 ({report['reach_pct']}%)")

    return milestones(bridged, spec["days"]), ring_truth(bridged, spec["days"]), report


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--period", choices=sorted(PERIODS), required=True)
    ap.add_argument("--results-dir", default=None)
    ap.add_argument("--executors", type=int, default=10)
    ap.add_argument("--ui-port", type=int, default=4058)
    ap.add_argument("--cores", type=int, default=4)
    ap.add_argument("--driver-memory", default="8g")
    args = ap.parse_args()

    import osn_sample
    from osn_sample import build_spark, load_dotenv

    load_dotenv()
    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(args.cores, args.driver_memory, distributed=True)
    spark.sparkContext.setLogLevel("ERROR")
    # The distributed builder does not set this; the local one does. See the
    # module docstring -- reading Europe/Paris timestamps in another zone
    # shifts every milestone silently.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", "96")

    ms, rings, report = build(spark, args.period)
    print(f"  milestones: {ms.count():,}   ring crossings: {rings.count():,}")

    if args.results_dir:
        import json

        out = Path(args.results_dir)
        out.mkdir(parents=True, exist_ok=True)
        (out / f"bridge_{args.period}.json").write_text(json.dumps(report, indent=2))
        print(f"  wrote {out / f'bridge_{args.period}.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
