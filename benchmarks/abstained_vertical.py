"""
Can vertical motion separate the flights the endpoint rule stays silent on?

Version 4.5 measured that ~15% of in-area arrivals have a usable candidate that
the abstention rejects, and that guessing on them with the nearest rule scores
about 4 correct to 11 wrong. Position information is exhausted on that block.

This asks whether *motion* is not. For each abstained flight it computes the
mean vertical rate over the 5, 10 and 20 minutes before the arrival endpoint
(after the departure startpoint), and splits the distribution by whether the
nearest-aerodrome guess would have been right. If aircraft genuinely descending
toward their candidate are the ones the guess gets right, a vertical gate
recovers flights at a better ratio than the 0.39:1 that guessing blindly gives.

Two vertical measures are reported, because they fail differently:

* ``vert_rate`` as broadcast, averaged. Carried forward when nothing is heard,
  so it is diluted by stale rows.
* the altitude *slope* across the window, (last - first) / elapsed. Immune to
  stale repeats, but blind to what happened between the two samples.

Two horizontal measures come along at almost no extra cost, because they are
the other half of the same argument:

* **closing rate** -- nautical miles per minute of approach toward the
  candidate. Informative but scale-dependent: a fast aircraft far away closes
  quickly without necessarily going there.
* **bearing alignment** -- the angle between the direction the aircraft
  actually travelled and the direction of the candidate aerodrome, seen from
  the far end of the window. Zero degrees means the trajectory points straight
  at the aerodrome. This is scale-free, and it involves no extrapolated
  position: it is an angle between two observed bearings, not a projected
  point, so the documented failure of trajectory extrapolation does not apply.

    python benchmarks/abstained_vertical.py --results-dir <dir>
"""

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

import osn_sample
from tables import table, fixed
from osn_sample import build_spark, load_dotenv
from adep_ades import (
    airport_locations, label_ground_truth, load_ground_truth, align_to_ground_truth,
    AIRPORTS,
)
from benchmark_modes import identities_from_candidates

CANDIDATES = table("opdi_endpoint_candidates")
TRACKS = table("osn_tracks")

#: The version 4.5 operating point, which defines what "abstained" means.
OP_RADIUS_NM, OP_HEIGHT_FT, OP_PENALTY_NM = 30.0, 15000.0, 10.0

WINDOWS_MIN = (2, 3, 5, 7, 10, 15, 20, 30)

#: Bearing-alignment gates, degrees. The angle between the direction travelled
#: and the direction of the candidate aerodrome.
#: Sub-degree gates are included to find where the measure stops being
#: physical. An aerodrome is not a point: a runway complex spans 2-4 km, which
#: from 40 NM subtends roughly 2-3 degrees. Below that the gate is tighter than
#: the target, so it selects for pointing at the exact reference coordinate
#: OurAirports happens to publish rather than at the airport.
ANGLES_DEG = (0.1, 0.25, 0.5, 0.75, 1, 2, 3, 4, 5, 7, 10, 15, 20, 25, 30, 40, 50, 60, 90)

M_S_TO_FT_MIN = 196.850394
M_TO_FT = 3.28084
EARTH_R_NM = 3440.065


def best_candidate(cand: DataFrame) -> DataFrame:
    """One row per (track, role): the candidate the ranking would pick, plus
    whether the abstention accepts it."""
    penalty = F.when(F.col("apt_scheduled") == "yes", F.lit(0.0)).otherwise(
        F.lit(OP_PENALTY_NM))
    c = cand.withColumn("_eff", F.col("dist_nm") + penalty)
    w = Window.partitionBy("track_id", "role").orderBy(F.col("_eff").asc_nulls_last())
    best = c.withColumn("_r", F.row_number().over(w)).filter(F.col("_r") == 1)
    passes = (F.col("dist_nm") <= OP_RADIUS_NM) & (
        F.col("on_ground")
        | (F.col("elev_known") & (F.col("agl_ft") <= OP_HEIGHT_FT)))
    return (best
            .withColumn("gate_ok", passes)
            # abstained: a candidate existed, the gate refused it, and the
            # out-of-area branch did not fire either.
            .withColumn("abstained", ~passes & ~F.col("at_border")))


def bearing_deg(lat1, lon1, lat2, lon2):
    """Initial great-circle bearing from point 1 to point 2, degrees."""
    d_lon = F.radians(lon2 - lon1)
    y = F.sin(d_lon) * F.cos(F.radians(lat2))
    x = (F.cos(F.radians(lat1)) * F.sin(F.radians(lat2))
         - F.sin(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.cos(d_lon))
    return (F.degrees(F.atan2(y, x)) + 360.0) % 360.0


def angle_between(a, b):
    """Smallest absolute angle between two bearings, 0-180 degrees.

    ``pmod``, not ``%``: Spark's remainder operator keeps the sign of the
    dividend, so ``(-350) % 360`` is ``-350`` and not ``10``. With ``%`` this
    returned 340 degrees for bearings 20 degrees apart whenever the pair
    straddled north -- inverting the metric on a large share of cases.
    """
    return F.abs(F.pmod(a - b + 180.0, F.lit(360.0)) - 180.0)


def haversine_nm(lat1, lon1, lat2, lon2):
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = (F.sin(dlat / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2)
    return F.lit(2 * EARTH_R_NM) * F.asin(F.sqrt(F.least(a, F.lit(1.0))))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--months", nargs="+", default=["202506"])
    ap.add_argument("--days", nargs="+",
                    default=["2025-06-05", "2025-06-06", "2025-06-07"])
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--executors", type=int, default=8)
    ap.add_argument("--ui-port", type=int, default=4041)
    args = ap.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
    load_dotenv()
    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(6, "8g", distributed=True)
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)

    cand = spark.read.parquet(CANDIDATES)
    best = best_candidate(cand).cache()
    ident = identities_from_candidates(cand)

    gt = load_ground_truth(spark, args.months, args.days)
    gt = label_ground_truth(gt, airport_locations(spark)).cache()

    # Attach ground truth to each track, so "would the guess have been right"
    # is answerable per flight.
    pred = (best.groupBy("track_id").pivot("role", ["adep", "ades"])
            .agg(F.first("apt_ident")))
    j = align_to_ground_truth(pred, ident, gt).select(
        "track_id", "gt_adep", "gt_ades")

    rows, all_sweeps = [], []
    for role, side in (("ades", "arrival"), ("adep", "departure")):
        b = best.filter(F.col("role") == role).join(j, "track_id", "inner")
        b = (b.withColumn("truth", F.col(f"gt_{role}"))
             .filter(F.col("truth").isNotNull() & (F.col("truth") != "OOA"))
             .withColumn("guess_right", F.col("apt_ident") == F.col("truth")))
        # The candidate cache carries the *endpoint's* lat/lon, not the
        # aerodrome's, so the aerodrome position has to come from OurAirports.
        apt_pos = (spark.read.parquet(AIRPORTS)
                   .select(F.col("ident").alias("apt_ident"),
                           F.col("latitude_deg").cast("double").alias("apt_lat_x"),
                           F.col("longitude_deg").cast("double").alias("apt_lon_x"))
                   .dropDuplicates(["apt_ident"]))
        blk = (b.filter("abstained")
               .select("track_id", "apt_ident", "truth", "guess_right", "dist_nm",
                       "agl_ft", F.col("event_time").alias("t_end"))
               .join(F.broadcast(apt_pos), "apt_ident", "left")).cache()
        n_blk = blk.count()
        n_right = blk.filter("guess_right").count()
        print(f"\n=== {side}s: abstained block {n_blk:,} flights, "
              f"guess would be right for {n_right:,} ({n_right/max(n_blk,1):.1%}) ===")

        # Altitude edges and elapsed time must come from the *same* rows.
        # min_by/max_by skip null altitudes while min/max over _off do not, so
        # mixing them divided an altitude change by a time span that did not
        # correspond to it -- which made the slope disagree with vert_rate on
        # 12% of flights. Filtering nulls up front keeps the two consistent.
        # baro_altitude_c is the pipeline's own de-spiked altitude.
        tr_all = spark.read.parquet(TRACKS)
        alt_col = ("baro_altitude_c" if "baro_altitude_c" in tr_all.columns
                   else "baro_altitude")
        tr = (tr_all
              .withColumn("_alt", F.col(alt_col))
              .filter(F.col("_alt").isNotNull() & F.col("lat").isNotNull()
                      & F.col("lon").isNotNull())
              .select("track_id", "event_time", "lat", "lon",
                      F.col("_alt").alias("baro_altitude"), "vert_rate")
              .join(F.broadcast(blk.select("track_id", "t_end", "apt_lat_x",
                                           "apt_lon_x", "guess_right")),
                    "track_id", "inner"))
        # Seconds between this sample and the endpoint. Positive = before the
        # arrival endpoint; for departures the startpoint is the reference and
        # the sign flips, so take the absolute offset either way.
        tr = tr.withColumn(
            "_off", F.abs(F.unix_timestamp("t_end") - F.unix_timestamp("event_time")))

        for mins in WINDOWS_MIN:
            w = tr.filter(F.col("_off") <= mins * 60)
            # min_by / max_by, not first / last: a groupBy has no inherent
            # order, so first() would pick an arbitrary row. _off is the
            # distance in seconds from the endpoint, so min_by is the sample
            # at the endpoint and max_by the one at the far edge of the window.
            agg = (w.groupBy("track_id", "guess_right")
                   .agg(F.count(F.lit(1)).alias("n_samples"),
                        F.avg(F.col("vert_rate") * M_S_TO_FT_MIN).alias("vr_ft_min"),
                        F.min("_off").alias("off_min"),
                        F.max("_off").alias("off_max"),
                        F.min_by("baro_altitude", "_off").alias("_a_near"),
                        F.max_by("baro_altitude", "_off").alias("_a_far"),
                        F.sum(F.when(F.col("vert_rate").isNotNull(), 1)
                              .otherwise(0)).alias("n_vr"),
                        F.min_by("lat", "_off").alias("_y_near"),
                        F.min_by("lon", "_off").alias("_x_near"),
                        F.max_by("lat", "_off").alias("_y_far"),
                        F.max_by("lon", "_off").alias("_x_far"),
                        F.min("apt_lat_x").alias("_ay"),
                        F.min("apt_lon_x").alias("_ax"))
                   .filter(F.col("n_samples") >= 3)
                   .withColumn("elapsed_s", F.col("off_max") - F.col("off_min"))
                   .filter(F.col("elapsed_s") >= 60))
            # For an arrival the endpoint is the *end* of the window, so the
            # near sample is later in time; for a departure the startpoint is
            # the beginning, so the near sample is earlier. sgn puts both on
            # the same footing: slope negative = descending as time runs on,
            # closing positive = getting nearer the candidate as time runs on.
            sgn = F.lit(1.0 if role == "ades" else -1.0)
            per_min = F.col("elapsed_s") / 60.0
            agg = agg.withColumn(
                "slope_ft_min",
                sgn * (F.col("_a_near") - F.col("_a_far")) * M_TO_FT / per_min)
            agg = agg.withColumn(
                "closing_nm_min",
                sgn * (haversine_nm(F.col("_y_far"), F.col("_x_far"), F.col("_ay"), F.col("_ax"))
                       - haversine_nm(F.col("_y_near"), F.col("_x_near"), F.col("_ay"), F.col("_ax")))
                / per_min)
            # Does the trajectory point at the aerodrome? Seen from the far end
            # of the window, compare the direction the aircraft actually
            # travelled with the direction of the candidate. Zero means the
            # track runs straight at it.
            #
            # No sgn here, unlike slope and closing rate above, and that is not
            # an oversight. Those are signed rates along the time axis, so the
            # role genuinely flips them. This is an angle between two bearings
            # from a common origin -- and the origin already flips with the
            # role, because _off is measured from the arrival's *last* fix and
            # the departure's *first*. So "far" is earlier in time for an
            # arrival and later for a departure, which makes bearing(far->near)
            # the course for one and the reverse course for the other. Both
            # read 0 degrees for a correct match; a 180 degree correction would
            # break departures rather than fix them. Confirmed empirically:
            # correctly identified departures sit at 2-9 degrees, not 171-178.
            agg = agg.withColumn(
                "align_deg",
                angle_between(
                    bearing_deg(F.col("_y_far"), F.col("_x_far"),
                                F.col("_y_near"), F.col("_x_near")),
                    bearing_deg(F.col("_y_far"), F.col("_x_far"),
                                F.col("_ay"), F.col("_ax"))))
            agg.cache()
            n = agg.count()
            # Sanity check the repaired slope: it must now agree in sign with
            # the broadcast vertical rate far more often than it did.
            chk = agg.filter(F.col("n_vr") > 0).agg(
                F.avg(F.when(F.signum("slope_ft_min") == F.signum("vr_ft_min"), 1.0)
                      .otherwise(0.0)).alias("agree")).first()
            stats = (agg.groupBy("guess_right")
                     .agg(F.count(F.lit(1)).alias("flights"),
                          F.expr("percentile_approx(vr_ft_min, 0.5)").alias("med_vr"),
                          F.expr("percentile_approx(slope_ft_min, 0.5)").alias("med_slope"),
                          F.expr("percentile_approx(align_deg, 0.5)").alias("med_align"))
                     .orderBy("guess_right").collect())
            print(f"\n  -- {side}s, {mins} min window, {n:,} flights, "
                  f"slope/vert_rate sign agreement {chk['agree']:.1%} --")
            for r in stats:
                lab = "right" if r["guess_right"] else "wrong"
                print(f"     {lab:>5}: {r['flights']:>6,} flights   "
                      f"med vert_rate {r['med_vr']:>7.0f} ft/min   "
                      f"med slope {r['med_slope']:>7.0f} ft/min   "
                      f"med align {r['med_align']:>4.0f} deg")
                rows.append({"side": side, "window_min": mins,
                             "guess_right": bool(r["guess_right"]),
                             "flights": r["flights"],
                             "median_vr_ft_min": r["med_vr"],
                             "median_slope_ft_min": r["med_slope"],
                             "median_align_deg": r["med_align"],
                             "slope_vr_sign_agreement": chk["agree"]})

            sweep = []
            base_n = n
            base_ok = agg.filter("guess_right").count()
            print(f"     baseline on this block: {base_ok:,}/{base_n:,} "
                  f"= {base_ok/max(base_n,1):.1%}  ({base_ok/max(base_n-base_ok,1):.2f}:1)")
            print(f"     {'angle':>7} {'answered':>9} {'correct':>8} {'wrong':>8} "
                  f"{'accuracy':>9} {'ratio':>7} {'recall':>7}")
            for deg in ANGLES_DEG:
                t = agg.filter(F.col("align_deg") <= deg).agg(
                    F.count(F.lit(1)).alias("n"),
                    F.sum(F.when(F.col("guess_right"), 1).otherwise(0)).alias("ok")).first()
                nn, ok = t["n"] or 0, t["ok"] or 0
                sweep.append({"side": side, "window_min": mins, "angle_deg": deg,
                              "answered": nn, "correct": ok, "wrong": nn - ok,
                              "accuracy": ok / nn if nn else None,
                              "ratio": ok / (nn - ok) if nn - ok else None,
                              "recall_of_recoverable": ok / base_ok if base_ok else None,
                              "block_flights": base_n, "block_correct": base_ok})
                if nn:
                    print(f"     {deg:>6}d {nn:>9,} {ok:>8,} {nn-ok:>8,} "
                          f"{ok/nn:>9.1%} {ok/max(nn-ok,1):>6.2f}:1 {ok/max(base_ok,1):>6.1%}")
            all_sweeps.extend(sweep)
            agg.unpersist()

    spark.createDataFrame(rows).toPandas().to_csv(out / "motion_profile.csv", index=False)
    spark.createDataFrame(all_sweeps).toPandas().to_csv(out / "angle_sweep.csv", index=False)
    print(f"\nwritten to {out}")
    spark.stop()


if __name__ == "__main__":
    main()
