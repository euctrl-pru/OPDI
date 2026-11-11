#!/usr/bin/env python
"""Why does ADS-C join so few of the track pieces of one flight?

A new track starts at any gap over ``gap_minutes`` (30), or over 15 minutes below
5,000 ft. So ADS-C joins two pieces only if, with its samples added, no single
remaining gap in the hole exceeds that threshold. This takes every hole in the
ADS-B of a Network Manager flight and asks how close ADS-C came.

    python benchmarks/adsc_bridging_diagnosis.py --day 2026-06-05 \
        --results-dir benchmarks/out/adsc_worldwide_subset
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

from adsc_flight_list_truth import OVERLAP_SLACK, PREFIX, label_far_end, load_truth  # noqa: E402

GAP_MINUTES = 30.0
LOW_ALT_GAP_MINUTES = 15.0
LOW_ALT_FT = 5000.0
FT_PER_M = 3.28084


def largest_remaining_gap_min(start, end, samples) -> float:
    """The longest silence left in [start, end] once ``samples`` are added, minutes."""
    pts = sorted([start, end] + [s for s in samples if start < s < end])
    return max((b - a).total_seconds() for a, b in zip(pts, pts[1:])) / 60.0


def classify_hole(n_adsc: int, residual_min: float, end_alt_ft, joined: bool) -> str:
    """Why this hole did or did not close."""
    if joined:
        return "joined"
    if n_adsc == 0:
        return "no_adsc_in_hole"
    if residual_min > GAP_MINUTES:
        return "adsc_too_sparse"
    if (
        residual_min > LOW_ALT_GAP_MINUTES
        and end_alt_ft is not None
        and not pd.isna(end_alt_ft)
        and end_alt_ft < LOW_ALT_FT
    ):
        return "low_altitude_rule"
    return "dense_enough_but_split"


def holes_in_flights(holes: pd.DataFrame, truth: pd.DataFrame) -> pd.DataFrame:
    """Holes wholly inside one flight's block window, with that flight's fields."""
    j = holes.merge(truth, on="icao24")
    return j[
        (j["hole_start"] >= j["t_off"] - OVERLAP_SLACK)
        & (j["hole_end"] <= j["t_on"] + OVERLAP_SLACK)
    ]


def diagnose(holes: pd.DataFrame, adsc_raw: pd.DataFrame, adsc_clean: pd.DataFrame) -> pd.DataFrame:
    """One row per hole: ADS-C inside it, what gap is left, and the verdict."""
    raw = adsc_raw.groupby("icao24")["event_time"].apply(list).to_dict()
    clean = adsc_clean.groupby("icao24")["event_time"].apply(list).to_dict()
    rows = []
    for h in holes.itertuples(index=False):
        s_raw = [t for t in raw.get(h.icao24, []) if h.hole_start < t < h.hole_end]
        s_cl = [t for t in clean.get(h.icao24, []) if h.hole_start < t < h.hole_end]
        res_raw = largest_remaining_gap_min(h.hole_start, h.hole_end, s_raw)
        res_cl = largest_remaining_gap_min(h.hole_start, h.hole_end, s_cl)
        rows.append(
            {
                "flight_key": h.flight_key,
                "icao24": h.icao24,
                "flow": h.flow,
                "hole_min": (h.hole_end - h.hole_start).total_seconds() / 60.0,
                "adsc_raw": len(s_raw),
                "adsc_clean": len(s_cl),
                "residual_raw_min": res_raw,
                "residual_clean_min": res_cl,
                "start_alt_ft": h.start_alt_ft,
                "end_alt_ft": h.end_alt_ft,
                "joined": bool(h.joined),
                "why": classify_hole(len(s_cl), res_cl, h.end_alt_ft, bool(h.joined)),
            }
        )
    return pd.DataFrame(rows)


def summarise(d: pd.DataFrame) -> pd.DataFrame:
    """Counts per verdict, and how the residual gap sits against other thresholds."""
    rows = []
    for why, g in d.groupby("why"):
        rows.append({"metric": f"holes_{why}", "value": len(g)})
    rows += [
        {"metric": "holes", "value": len(d)},
        {"metric": "flights_with_a_hole", "value": d["flight_key"].nunique()},
        {"metric": "hole_median_min", "value": d["hole_min"].median()},
        {"metric": "hole_p90_min", "value": d["hole_min"].quantile(0.9)},
        {"metric": "holes_with_any_adsc", "value": int((d["adsc_clean"] > 0).sum())},
        {
            "metric": "adsc_per_hour_in_hole_median",
            "value": (d["adsc_clean"] / (d["hole_min"] / 60)).median(),
        },
    ]
    has = d[d["adsc_clean"] > 0]
    if len(has):
        rows += [
            {
                "metric": "residual_median_min_when_adsc",
                "value": has["residual_clean_min"].median(),
            },
            {
                "metric": "share_of_hole_left_median_when_adsc",
                "value": (has["residual_clean_min"] / has["hole_min"]).median(),
            },
        ]
    for limit in (30, 45, 60, 90, 120, 180):
        rows.append(
            {
                "metric": f"holes_residual_le_{limit}_min",
                "value": int((d["residual_clean_min"] <= limit).sum()),
            }
        )
    rows.append(
        {
            "metric": "holes_raw_would_close_clean_does_not",
            "value": int(
                (
                    (d["residual_raw_min"] <= GAP_MINUTES) & (d["residual_clean_min"] > GAP_MINUTES)
                ).sum()
            ),
        }
    )
    return pd.DataFrame(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--day", required=True)
    ap.add_argument("--prefix", default=PREFIX)
    ap.add_argument("--truth", default=str(REPO / "reference/flights_202606.parquet"))
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--executors", type=int, default=6)
    args = ap.parse_args()

    import osn_sample
    from osn_sample import build_spark, load_dotenv
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from opdi.coverage import EUROPE_BBOX

    load_dotenv()
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(4, "8g", distributed=True)
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        merged = (
            spark.read.parquet(f"{args.prefix}/merged/osn_tracks_clean")
            .where(F.col("dof").cast("string") == args.day)
            .withColumn("icao24", F.lower("icao24"))
        )
        adsb = merged.where(F.col("source") == "ADS-B").select(
            "icao24", "track_id", "event_time", "baro_altitude"
        )
        w = Window.partitionBy("icao24").orderBy("event_time")
        holes = (
            adsb.withColumn("hole_start", F.lag("event_time").over(w))
            .withColumn("prev_track", F.lag("track_id").over(w))
            .withColumn("prev_alt", F.lag("baro_altitude").over(w))
            .where(
                F.col("event_time").cast("long") - F.col("hole_start").cast("long")
                > GAP_MINUTES * 60
            )
            .select(
                "icao24",
                "hole_start",
                F.col("event_time").alias("hole_end"),
                (F.col("prev_track") == F.col("track_id")).alias("joined"),
                (F.col("prev_alt") * FT_PER_M).alias("start_alt_ft"),
                (F.col("baro_altitude") * FT_PER_M).alias("end_alt_ft"),
            )
            .toPandas()
        )
        airframes = [r[0] for r in merged.select("icao24").distinct().collect()]
        adsc_clean = (
            merged.where(F.col("source") == "ADS-C").select("icao24", "event_time").toPandas()
        )
        adsc_raw = (
            spark.read.parquet(f"{args.prefix}/input/osn_adsc_statevectors")
            .select(F.lower("icao24").alias("icao24"), "event_time")
            .toPandas()
        )
        min_lon, min_lat, max_lon, max_lat = EUROPE_BBOX
        apt = spark.read.parquet("s3a://eurocontrol/opdi-world/oa_airports")
        in_box = {
            r[0]
            for r in apt.where(
                (F.col("longitude_deg") >= min_lon)
                & (F.col("longitude_deg") <= max_lon)
                & (F.col("latitude_deg") >= min_lat)
                & (F.col("latitude_deg") <= max_lat)
            )
            .select("ident")
            .collect()
        }
    finally:
        spark.stop()

    truth = label_far_end(load_truth(args.truth, args.day, airframes), in_box)
    # Tracks starting after the day were not built, so only these have both pieces.
    truth = truth[truth["t_on"].dt.strftime("%Y-%m-%d") == args.day]
    truth = truth[truth["flow"].isin(["outside_to_europe", "europe_to_outside"])]
    d = diagnose(holes_in_flights(holes, truth), adsc_raw, adsc_clean)
    s = summarise(d)
    s = pd.concat(
        [pd.DataFrame([{"metric": "boundary_flights_landing_same_day", "value": len(truth)}]), s],
        ignore_index=True,
    )
    print(s.to_string(index=False))

    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)
    s.to_csv(out / "adsc_bridging_summary.csv", index=False)
    d.to_csv(out / "adsc_bridging_holes.csv", index=False)

    import provenance
    from regenerate_adsc_v1 import CORE

    for n in ("adsc_bridging_summary.csv", "adsc_bridging_holes.csv"):
        provenance.record(
            out,
            n,
            "benchmarks/adsc_bridging_diagnosis.py",
            sys.argv[1:],
            [
                "benchmarks/adsc_bridging_diagnosis.py",
                "benchmarks/adsc_flight_list_truth.py",
                *CORE,
            ],
            inputs={"day": args.day, "flights": len(truth)},
            notes="Holes over 30 min in the ADS-B of NM flights crossing the "
            "European box and landing on the day.",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
