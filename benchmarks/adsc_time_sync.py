#!/usr/bin/env python
"""How far apart the ADS-B and ADS-C clocks are, measured rather than assumed.

Neither feed states a position age that can be used. ADS-B's
``last_pos_update`` is real but tiny (median 0.22 s, max 0.92 s on the sample
measured). ADS-C's ``seen``/``seen_pos`` are constant 0 -- the feed *asserts*
zero age for every report -- and the readsb ``Aircraft`` record does not expose
the ADS-C report's own time-of-position, so there is nothing in band to correct
from.

**The instrument.** For each ADS-C report, take the nearest ADS-B sample of the
same airframe on each side within ``max_gap_s``, interpolate the ADS-B position
linearly to the ADS-C timestamp, and decompose the offset ``p_adsc - p_adsb``
into along-track and cross-track components using the direction of the ADS-B
pair itself. Then:

* **cross-track** is the position-disagreement floor -- what the two feeds
  disagree about that no timing explanation can absorb;
* **along-track / ground speed** is the implied time offset.

A position error is isotropic, so it lands equally in both. A timing error
lands only along track. The ratio between them is therefore the discriminator,
and it is what makes the conclusion falsifiable rather than a story:
``test_a_cross_track_error_does_not_read_as_a_lag`` pins the maths down on a
synthetic track with a known injected offset.

**What the prototype found** (2026-06-01 11:00-14:00 UTC, 46 bracketed pairs,
25 airframes, full 1 Hz ADS-B): median \\|cross\\| 9 m against median
\\|along\\| 377 m -- a 43x ratio -- and an implied offset of +1.48 s with an
IQR of +0.32 to +1.96 s. ADS-B's own staleness cannot explain it (correlation
0.23, and the age never exceeds 0.92 s). And the offset is **per airframe**:
within one it holds to sigma 0.10-0.31 s, between them it runs +0.01 s to
+2.09 s, with one airframe persistently at -12.9 s. That is why
``AdscConfig.time_offset_seconds`` ships at zero -- a constant correction would
be wrong for nearly every aircraft and would damage the synchronised ones.

**Why the sample is small, and why that is not a defect.** The measurement
needs an airframe heard by *both* feeds at the same minute, and ADS-C exists
precisely where ADS-B cannot hear the aircraft. 46 of 1,088 reports bracketed
on the prototype day. More days widen it; nothing widens it a lot.

    python benchmarks/adsc_time_sync.py --days 2026-06-05 2026-06-06 \
        2026-06-07 --results-dir /tmp/sync
"""

import argparse
import csv
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

INPUT_PREFIX = "s3a://eurocontrol/opdi/research/adsc/input"

#: Earth radius for the local tangent-plane projection, metres. The offsets
#: being measured are hundreds of metres over baselines of tens of kilometres,
#: so a spherical tangent plane is three orders of magnitude more accurate
#: than it needs to be and a geodesic solver would be ceremony.
R_EARTH = 6371000.0

#: Ground speeds below this are not usable: the implied offset divides by
#: speed, so a taxiing or stationary aircraft turns a 50 m position
#: disagreement into minutes of apparent skew. ADS-C is a cruise feed, so this
#: discards almost nothing.
MIN_SPEED_MS = 50.0


def _east_north(lat, lon, lat0, lon0):
    """Local east/north metres of (lat, lon) relative to (lat0, lon0)."""
    east = F.radians(lon - lon0) * F.lit(R_EARTH) * F.cos(F.radians(lat0))
    north = F.radians(lat - lat0) * F.lit(R_EARTH)
    return east, north


def bracket(adsc: DataFrame, adsb: DataFrame, max_gap_s: int = 60) -> DataFrame:
    """ADS-C reports with the nearest ADS-B sample on **each** side.

    Both sides, never one: extrapolating from a single sample would measure the
    instrument's own straight line rather than the aircraft, and the sign of
    the result would depend on which side happened to exist.

    The join is the shadow filter's shape -- bin to ``max_gap_s``, expand the
    ADS-C side to three candidate bins, broadcast it, then test exactly -- for
    the same reason: ADS-C is a few thousand rows a day against hundreds of
    millions, so this is one broadcast hash join rather than a shuffle of the
    state vectors. ``max(struct(...))`` then picks the nearest sample on each
    side in a single hash aggregate, the idiom
    ``StateVectorIngestion._bucket_decimate`` already uses.
    """
    w = int(max_gap_s)
    if w <= 0:
        raise ValueError(f"max_gap_s must be positive, got {w!r}")

    tc = F.unix_timestamp("event_time").cast("double")
    bin_c = F.floor(tc / F.lit(w)).cast("long")
    keys = adsc.select(
        F.col("icao24"),
        F.col("event_time").alias("t_c_ts"),
        tc.alias("t_c"),
        F.col("lat").alias("lat_c"),
        F.col("lon").alias("lon_c"),
        F.explode(F.array(bin_c - 1, bin_c, bin_c + 1)).alias("_bin"),
    )

    tb = F.unix_timestamp("event_time").cast("double")
    samples = adsb.select(
        F.col("icao24").alias("_icao24"),
        tb.alias("t_b"),
        F.col("lat").alias("lat_b"),
        F.col("lon").alias("lon_b"),
        F.col("velocity").alias("v_b"),
        F.col("last_pos_update").cast("double").alias("lpu_b"),
        F.floor(tb / F.lit(w)).cast("long").alias("_bin"),
    ).where(F.col("lat_b").isNotNull() & F.col("lon_b").isNotNull())

    j = (
        samples.join(
            F.broadcast(keys),
            (F.col("_icao24") == F.col("icao24")) & (samples["_bin"] == keys["_bin"]),
            "inner",
        )
        .withColumn("_dt", F.col("t_b") - F.col("t_c"))
        .where(F.abs(F.col("_dt")) <= F.lit(float(w)))
    )

    # `max` over a struct compares field by field, so leading with the signed
    # gap picks the nearest sample on each side: the largest negative gap
    # before, the smallest positive gap after.
    def _side(pred, agg):
        return agg(
            F.when(
                pred,
                F.struct(
                    F.col("_dt").alias("dt"),
                    F.col("t_b"),
                    F.col("lat_b"),
                    F.col("lon_b"),
                    F.col("v_b"),
                    F.col("lpu_b"),
                ),
            )
        )

    before = _side(F.col("_dt") <= 0, F.max)
    after = _side(F.col("_dt") >= 0, F.min)

    out = (
        j.groupBy("icao24", "t_c_ts", "t_c", "lat_c", "lon_c")
        .agg(before.alias("_p"), after.alias("_n"))
        # A report with only one side is dropped here rather than filled: see
        # the docstring. `max`/`min` of an all-NULL group is NULL, so this is
        # the whole test.
        .where(F.col("_p").isNotNull() & F.col("_n").isNotNull())
        .select(
            "icao24",
            "t_c_ts",
            "t_c",
            "lat_c",
            "lon_c",
            F.col("_p.t_b").alias("t_prev"),
            F.col("_p.lat_b").alias("lat_prev"),
            F.col("_p.lon_b").alias("lon_prev"),
            F.col("_p.v_b").alias("v_prev"),
            F.col("_p.lpu_b").alias("lpu_prev"),
            F.col("_n.t_b").alias("t_next"),
            F.col("_n.lat_b").alias("lat_next"),
            F.col("_n.lon_b").alias("lon_next"),
            F.col("_n.v_b").alias("v_next"),
        )
        .withColumn("bracket_s", F.col("t_next") - F.col("t_prev"))
    )
    # A zero-length bracket cannot be interpolated and a long one interpolates
    # across more than the instrument can justify: at 250 m/s a 120 s bracket
    # is 30 km of assumed straight line, against the hundreds of metres being
    # measured.
    return out.where((F.col("bracket_s") > 0) & (F.col("bracket_s") <= F.lit(2.0 * w)))


def decompose(bracketed: DataFrame) -> DataFrame:
    """Add the along/cross decomposition and the implied time offset."""
    w = (F.col("t_c") - F.col("t_prev")) / (F.col("t_next") - F.col("t_prev"))
    lat_i = F.col("lat_prev") + w * (F.col("lat_next") - F.col("lat_prev"))
    lon_i = F.col("lon_prev") + w * (F.col("lon_next") - F.col("lon_prev"))

    df = bracketed.withColumn("_w", w).withColumn("lat_i", lat_i).withColumn("lon_i", lon_i)

    # Track direction from the ADS-B pair itself, not from the broadcast
    # `heading`: heading is what the aircraft says it is pointing at, which in
    # wind is not where it is going, and the decomposition needs the direction
    # it actually moved.
    dx, dy = _east_north(F.col("lat_next"), F.col("lon_next"), F.col("lat_prev"), F.col("lon_prev"))
    df = df.withColumn("_dx", dx).withColumn("_dy", dy)
    norm = F.sqrt(F.col("_dx") * F.col("_dx") + F.col("_dy") * F.col("_dy"))
    df = df.withColumn("_norm", norm).where(F.col("_norm") > F.lit(1.0))
    df = df.withColumn("_ux", F.col("_dx") / F.col("_norm")).withColumn(
        "_uy", F.col("_dy") / F.col("_norm")
    )

    ox, oy = _east_north(F.col("lat_c"), F.col("lon_c"), F.col("lat_i"), F.col("lon_i"))
    df = df.withColumn("_ox", ox).withColumn("_oy", oy)

    df = (
        df.withColumn("along_m", F.col("_ox") * F.col("_ux") + F.col("_oy") * F.col("_uy"))
        .withColumn("cross_m", -F.col("_ox") * F.col("_uy") + F.col("_oy") * F.col("_ux"))
        .withColumn("dist_m", F.sqrt(F.col("_ox") * F.col("_ox") + F.col("_oy") * F.col("_oy")))
        .withColumn("v_ms", (F.col("v_prev") + F.col("v_next")) / F.lit(2.0))
        # ADS-B's own position age at the bracketing sample. Reported because
        # the archive carries positions forward, which would bias the result
        # in exactly the direction measured -- so the alternative explanation
        # has to be checkable from the output rather than argued away.
        .withColumn("adsb_pos_age_s", F.col("t_prev") - F.col("lpu_prev"))
        .where(F.col("v_ms") >= F.lit(MIN_SPEED_MS))
        .withColumn("implied_dt_s", F.col("along_m") / F.col("v_ms"))
    )

    return df.select(
        "icao24",
        F.col("t_c_ts").alias("event_time"),
        "along_m",
        "cross_m",
        "dist_m",
        "implied_dt_s",
        "v_ms",
        "bracket_s",
        "adsb_pos_age_s",
    )


def per_airframe(decomposed: DataFrame) -> DataFrame:
    """Per-airframe summary -- the table the per-airframe conclusion rests on.

    Reported per airframe and not only in aggregate because that is where the
    finding is: the network median says +1.48 s, and an airframe-level view
    says one aircraft is at +0.01 s and another at -12.9 s. A single number
    would have hidden the only thing that decides whether a correction is
    possible.
    """
    return (
        decomposed.groupBy("icao24")
        .agg(
            F.count(F.lit(1)).alias("n"),
            F.expr("percentile_approx(implied_dt_s, 0.5)").alias("median_implied_dt_s"),
            F.stddev("implied_dt_s").alias("std_implied_dt_s"),
            F.expr("percentile_approx(abs(cross_m), 0.5)").alias("median_abs_cross_m"),
            F.expr("percentile_approx(abs(along_m), 0.5)").alias("median_abs_along_m"),
        )
        .orderBy("icao24")
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", nargs="+", required=True)
    ap.add_argument("--prefix", default=INPUT_PREFIX)
    ap.add_argument("--max-gap-s", type=int, default=60)
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--pairs-name", default="adsc_time_sync_pairs_2026.csv")
    ap.add_argument("--out-name", default="adsc_time_sync_2026.csv")
    ap.add_argument("--cores", type=int, default=4)
    ap.add_argument("--driver-memory", default="8g")
    ap.add_argument("--executors", type=int, default=8)
    args = ap.parse_args()

    import osn_sample
    from osn_sample import build_spark, load_dotenv

    load_dotenv()
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(args.cores, args.driver_memory, distributed=True)
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    days = F.array(*[F.lit(d) for d in args.days])
    try:
        adsc = spark.read.parquet(f"{args.prefix}/osn_adsc_statevectors").where(
            F.array_contains(days, F.col("event_time_day").cast("string"))
        )
        adsb = spark.read.parquet(f"{args.prefix}/osn_statevectors_v2").where(
            F.array_contains(days, F.col("event_time_day").cast("string"))
        )
        n_adsc = adsc.count()
        pairs = decompose(bracket(adsc, adsb, args.max_gap_s)).cache()
        pair_rows = [r.asDict() for r in pairs.collect()]
        print(
            f"bracketed {len(pair_rows)} of {n_adsc} ADS-C reports "
            f"({len({r['icao24'] for r in pair_rows})} airframes)",
            flush=True,
        )
        summary = [r.asDict() for r in per_airframe(pairs).collect()]
        overall = (
            pairs.agg(
                F.count(F.lit(1)).alias("n"),
                F.expr("percentile_approx(implied_dt_s, 0.5)").alias("median_implied_dt_s"),
                F.stddev("implied_dt_s").alias("std_implied_dt_s"),
                F.expr("percentile_approx(abs(cross_m), 0.5)").alias("median_abs_cross_m"),
                F.expr("percentile_approx(abs(along_m), 0.5)").alias("median_abs_along_m"),
            )
            .collect()[0]
            .asDict()
        )
        overall["icao24"] = "ALL"
        summary.append(overall)
    finally:
        spark.stop()

    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)

    def write(name, rows, required):
        if not rows and required:
            raise SystemExit(
                f"{name}: no bracketed pairs at all. That is not a null "
                f"result -- it means the two tables share no airframe-minute, "
                f"so either the ADS-C ingest or the state-vector window is "
                f"wrong for these days."
            )
        fields = sorted({k for r in rows for k in r}) or ["icao24"]
        with open(out / name, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)
        print(f"  wrote {out / name} ({len(rows)} rows)")

    write(args.pairs_name, pair_rows, required=True)
    write(args.out_name, summary, required=True)

    import provenance

    for name in (args.pairs_name, args.out_name):
        provenance.record(
            out,
            name,
            "benchmarks/adsc_time_sync.py",
            sys.argv[1:],
            ["benchmarks/adsc_time_sync.py", "src/opdi/ingestion/osn_adsc.py"],
            inputs={"days": args.days, "max_gap_s": args.max_gap_s, "min_speed_ms": MIN_SPEED_MS},
            input_tables=[
                f"{args.prefix}/osn_adsc_statevectors",
                f"{args.prefix}/osn_statevectors_v2",
            ],
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
