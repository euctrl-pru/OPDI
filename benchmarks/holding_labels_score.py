"""Score the holding rule against the Olive et al. (2022) hand-corrected labels.

The label set (xoolive/holding_patterns, ``hp_labels.parquet.gz``) marks every
six-minute window (stepped two minutes) of 97,700 arrivals at EGLL, EGLC, EIDW
and LFPG between 1 October and 30 November 2019 as holding or not. The
trajectories are the 4TU archive (doi:10.4121/20411868), one parquet per
airport, resampled to 1 s in aviation units. This script adapts one day of
them to the shape step 04 hands a detector, runs ``calculate_holding_events``
with the local Spark session, and scores the episodes the way the paper's own
``06_metrics.py`` does: time-weighted per flight, precision = mean
intersection / mean detected duration, recall = mean intersection / mean
truth duration, IoU per flight. The bar is the neural detector trained on
these labels: precision 0.84, recall 0.70, F1 0.76 across airports.

Usage::

    python benchmarks/holding_labels_score.py --day 2019-10-01 --airports EGLL EIDW \\
        --data-dir $CLAUDE_JOB_DIR/tmp/research/holding_data --results-dir <paper>/data/holding

Writes ``holding_score_<day>.csv`` (shipped thresholds, per airport and
overall), ``holding_sweep_<day>.csv`` (the four-threshold grid) and
``holding_episodes_<day>.csv`` (detected episodes, for inspection), each with a
provenance entry. The trajectories are CC BY-NC-SA: scores are committed,
extracts are not.
"""

import argparse
import dataclasses
import itertools
import sys
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

import provenance  # noqa: E402

from opdi.config import EventConfig  # noqa: E402
from opdi.pipeline.events import add_distance_measure, add_time_measure  # noqa: E402
from opdi.pipeline.holding import calculate_holding_events  # noqa: E402
from opdi.pipeline.holes import add_piece  # noqa: E402

_CODE_PATHS = [
    REPO / "benchmarks" / "holding_labels_score.py",
    REPO / "src" / "opdi" / "pipeline" / "holding.py",
    REPO / "src" / "opdi" / "config.py",
]

#: Aerodrome elevations (ft) for the labelled airports, from OurAirports.
ELEVATION_FT = {"EGLL": 83.0, "EIDW": 242.0, "EGLC": 19.0, "LFPG": 392.0}

#: The four load-bearing thresholds and the values swept.
SWEEP = {
    "holding_net_turn_min_deg": (270.0, 300.0, 330.0),
    "holding_max_displacement_nm": (6.0, 8.0, 10.0),
    "holding_min_duration_seconds": (180.0, 240.0, 360.0),
}


# ---------------------------------------------------------------------------
# Adapting the trajectories
# ---------------------------------------------------------------------------

def as_track_frame(spark, traj: pd.DataFrame, elevation_ft: float, step_s: float = 5.0):
    """The step 04 input shape from a traffic-format frame, thinned to one
    sample every ``step_s`` seconds per flight."""
    from pyspark.sql import functions as F

    df = traj.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    secs = (df["timestamp"] - pd.Timestamp("1970-01-01", tz="UTC")).dt.total_seconds()
    df = df[(secs % step_s) < 1.0]
    out = pd.DataFrame({
        "track_id": df["flight_id"].astype(str),
        "icao24": df["icao24"].astype(str),
        "callsign": df["callsign"].astype(str),
        "event_time": df["timestamp"].dt.tz_convert(None),
        "lat": df["latitude"].astype(float),
        "lon": df["longitude"].astype(float),
        "baro_altitude_c": df["altitude"].astype(float) / 3.28084,
        "velocity": df["groundspeed"].astype(float) / 1.94384,
        "heading": df["track"].astype(float),
        "vert_rate": df["vertical_rate"].astype(float) / 196.850394,
    })
    sdf = spark.createDataFrame(out)
    sdf = add_distance_measure(sdf)
    sdf = add_time_measure(sdf)
    sdf = add_piece(sdf, EventConfig().track_hole_seconds)
    return sdf.withColumn("elev_ades_ft", F.lit(float(elevation_ft))).withColumn(
        "elev_adep_ft", F.lit(None).cast("double"))


def flights_of(traj: pd.DataFrame) -> pd.DataFrame:
    """``flight_id, callsign, start, stop`` per trajectory."""
    g = traj.groupby("flight_id")
    out = g.agg(callsign=("callsign", "first"), start=("timestamp", "min"), stop=("timestamp", "max"))
    return out.reset_index()


def match_labels(labels: pd.DataFrame, traj_flights: pd.DataFrame) -> pd.DataFrame:
    """Label rows with a ``track_id``: the trajectory of the same callsign
    whose span contains the window. The label ``flight_id`` suffix does not
    match the trajectory's, so the join is on callsign and time."""
    lab = labels.copy()
    lab["callsign"] = lab["flight_id"].str.rsplit("_", n=1).str[0]
    cand = lab.merge(traj_flights.rename(columns={"flight_id": "track_id", "start": "t_start",
                                                  "stop": "t_stop"}), on="callsign", how="inner")
    inside = (cand["start"] >= cand["t_start"] - pd.Timedelta(minutes=5)) & (
        cand["stop"] <= cand["t_stop"] + pd.Timedelta(minutes=5))
    cand = cand[inside].drop(columns=["t_start", "t_stop"])
    return cand.drop_duplicates(["flight_id", "start"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Scoring (06_metrics.py, reimplemented)
# ---------------------------------------------------------------------------

def episodes_from_windows(windows: pd.DataFrame, flag: str) -> pd.DataFrame:
    """Consecutive positive windows of one flight form one episode."""
    rows = []
    for flight_id, f in windows.sort_values("start").groupby("flight_id"):
        previous, first, last = False, None, None
        for start, stop, label in zip(f["start"], f["stop"], f[flag]):
            if label and first is None:
                first, last = start, stop
            if not label and previous:
                rows.append((flight_id, first, last))
                first, last = None, None
            if label and previous:
                last = stop
            previous = label
        if previous and first is not None:
            rows.append((flight_id, first, last))
    return pd.DataFrame(rows, columns=["flight_id", "start", "stop"])


#: A label window is six minutes; it counts as detected when at least half of
#: it lies inside a detected episode. The labels mark a window as holding when
#: the aircraft is holding *in* it, not when a hold merely touches it, and
#: "touches" would stretch every detected episode by up to a window at each
#: end before the time-weighted scoring sees it.
WINDOW_MIN_OVERLAP_S = 180.0


def windows_from_episodes(labels: pd.DataFrame, episodes: pd.DataFrame,
                          min_overlap_s: float = WINDOW_MIN_OVERLAP_S) -> pd.DataFrame:
    """Add ``hp_c``: at least ``min_overlap_s`` of the window lies inside a
    detected episode of its track (``0`` means any overlap)."""
    out = labels.copy()
    out["hp_c"] = False
    if episodes.empty:
        return out
    by_track = {k: g for k, g in episodes.groupby("track_id")}
    flags = []
    for track_id, start, stop in zip(out["track_id"], out["start"], out["stop"]):
        eps = by_track.get(track_id)
        if eps is None:
            flags.append(False)
            continue
        overlap = (eps["stop"].clip(upper=stop) - eps["start"].clip(lower=start)).dt.total_seconds()
        covered = float(overlap.clip(lower=0).sum())
        flags.append(covered >= min_overlap_s if min_overlap_s > 0 else covered > 0)
    out["hp_c"] = flags
    return out


def score(windows: pd.DataFrame) -> Dict[str, float]:
    """Precision, recall, F1 and IoU the way the label paper computes them."""
    truth = episodes_from_windows(windows, "hp").assign(label="t")
    det = episodes_from_windows(windows, "hp_c").assign(label="c")
    table = pd.concat([truth, det], ignore_index=True)
    zero = pd.Timedelta(0)
    rows = []
    for flight_id, hp in table.groupby("flight_id"):
        hp_t, hp_c = hp[hp.label == "t"], hp[hp.label == "c"]
        d_t = (hp_t.stop - hp_t.start).sum() if len(hp_t) else zero
        d_c = (hp_c.stop - hp_c.start).sum() if len(hp_c) else zero
        inter, union = zero, zero
        if not d_t:
            union = d_c
        elif not d_c:
            union = d_t
        else:
            for _, t in hp_t.iterrows():
                for _, c in hp_c.iterrows():
                    inter += max(zero, min(t.stop, c.stop) - max(t.start, c.start))
                    union += max(t.stop, c.stop) - min(t.start, c.start)
        rows.append((flight_id, d_t, d_c, inter, union, inter / union if union else 0.0))
    m = pd.DataFrame(rows, columns=["flight_id", "d_t", "d_c", "intersection", "union", "iou"])
    if m.empty:
        return {"precision": float("nan"), "recall": float("nan"), "f1": float("nan"), "iou": float("nan"),
                "n_flights": 0, "n_windows": len(windows), "n_positive": int(windows["hp"].sum()),
                "n_detected_windows": int(windows["hp_c"].sum())}
    inter = m["intersection"].mean()
    d_t, d_c = m["d_t"].mean(), m["d_c"].mean()
    precision = inter / d_c if d_c else float("nan")
    recall = inter / d_t if d_t else float("nan")
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {"precision": float(precision), "recall": float(recall), "f1": float(f1),
            "iou": float(m["iou"].mean()), "n_flights": int(len(m)), "n_windows": int(len(windows)),
            "n_positive": int(windows["hp"].sum()), "n_detected_windows": int(windows["hp_c"].sum())}


# ---------------------------------------------------------------------------
# Driving the detector
# ---------------------------------------------------------------------------

def detect(spark, sdf, config: EventConfig) -> pd.DataFrame:
    """Detected episodes ``track_id, start, stop`` plus the info fields."""
    import json

    rows = calculate_holding_events(sdf, config).collect()
    by = {}
    for r in rows:
        info = json.loads(r.info)
        key = (r.track_id, info["holding_seq"])
        by.setdefault(key, {}).update({r.type: r.event_time, **info, "track_id": r.track_id})
    out = []
    for (track_id, _), v in by.items():
        if "holding-start" in v and "holding-end" in v:
            out.append({"track_id": track_id,
                        "start": pd.Timestamp(v["holding-start"], tz="UTC"),
                        "stop": pd.Timestamp(v["holding-end"], tz="UTC"),
                        **{k: v[k] for k in ("pattern", "direction", "circuits", "duration_s",
                                             "distance_nm", "altitude_start_ft", "straight_pct")}})
    return pd.DataFrame(out, columns=["track_id", "start", "stop", "pattern", "direction", "circuits",
                                      "duration_s", "distance_nm", "altitude_start_ft", "straight_pct"])


def load_day(data_dir: Path, airport: str, day: pd.Timestamp) -> pd.DataFrame:
    traj = pd.read_parquet(data_dir / f"{airport}_dataset.parquet.gz",
                           columns=["timestamp", "icao24", "callsign", "latitude", "longitude",
                                    "altitude", "groundspeed", "track", "vertical_rate", "flight_id"])
    traj["timestamp"] = pd.to_datetime(traj["timestamp"], utc=True)
    span = traj.groupby("flight_id")["timestamp"].agg(["min", "max"]).reset_index()
    keep = span[(span["min"] < day + pd.Timedelta(days=1)) & (span["max"] >= day)]["flight_id"]
    return traj[traj["flight_id"].isin(set(keep))].sort_values(["flight_id", "timestamp"]).reset_index(drop=True)


def load_labels(data_dir: Path, airport: str, day: pd.Timestamp) -> pd.DataFrame:
    labels = pd.read_parquet(data_dir / "hp_labels.parquet.gz")
    labels = labels[labels["airport"] == airport.lower()].copy()
    labels["start"] = pd.to_datetime(labels["start"], utc=True)
    labels["stop"] = pd.to_datetime(labels["stop"], utc=True)
    return labels[(labels["start"] >= day) & (labels["start"] < day + pd.Timedelta(days=1))].reset_index(drop=True)


def run(spark, data_dir: Path, airports: List[str], day: pd.Timestamp, sweep: bool = True):
    scores, sweep_rows, episodes_all = [], [], []
    shipped = EventConfig()
    per_airport_windows = {}
    for apt in airports:
        traj = load_day(data_dir, apt, day)
        labels = load_labels(data_dir, apt, day)
        matched = match_labels(labels, flights_of(traj))
        print(f"{apt}: {traj.flight_id.nunique()} flights, {len(labels)} label windows, "
              f"{len(matched)} matched, {int(matched.hp.sum())} positive", flush=True)
        sdf = as_track_frame(spark, traj, ELEVATION_FT[apt]).cache()
        eps = detect(spark, sdf, shipped)
        windows = windows_from_episodes(matched, eps)
        per_airport_windows[apt] = windows
        s = score(windows)
        touch = score(windows_from_episodes(matched, eps, min_overlap_s=0.0))
        scores.append({"airport": apt, "day": str(day.date()), **s,
                       "n_episodes": len(eps), "n_flights_detected": eps.track_id.nunique(),
                       "precision_any_overlap": touch["precision"], "recall_any_overlap": touch["recall"],
                       "f1_any_overlap": touch["f1"]})
        episodes_all.append(eps.assign(airport=apt))
        if sweep:
            keys = list(SWEEP)
            for values in itertools.product(*[SWEEP[k] for k in keys]):
                cfg = dataclasses.replace(shipped, **dict(zip(keys, values)))
                s2 = score(windows_from_episodes(matched, detect(spark, sdf, cfg)))
                sweep_rows.append({"airport": apt, **dict(zip(keys, values)), **s2})
        sdf.unpersist()
    all_windows = pd.concat(per_airport_windows.values(), ignore_index=True)
    touch_all = score(pd.concat([windows_from_episodes(w.drop(columns=["hp_c"]), e, 0.0)
                                 for w, e in zip(per_airport_windows.values(), episodes_all)]))
    scores.append({"airport": "ALL", "day": str(day.date()), **score(all_windows),
                   "n_episodes": sum(len(e) for e in episodes_all),
                   "n_flights_detected": sum(e.track_id.nunique() for e in episodes_all),
                   "precision_any_overlap": touch_all["precision"], "recall_any_overlap": touch_all["recall"],
                   "f1_any_overlap": touch_all["f1"]})
    return pd.DataFrame(scores), pd.DataFrame(sweep_rows), pd.concat(episodes_all, ignore_index=True)


def write(results_dir: Path, name: str, df: pd.DataFrame, argv: List[str], notes: str = "") -> None:
    results_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(results_dir / name, index=False)
    provenance.record(results_dir, name, "benchmarks/holding_labels_score.py", argv, _CODE_PATHS,
                      inputs={"rows": len(df)}, notes=notes)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--day", default="2019-10-01")
    ap.add_argument("--airports", nargs="+", default=["EGLL", "EIDW"])
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--no-sweep", action="store_true")
    args = ap.parse_args(argv)
    argv = list(sys.argv[1:] if argv is None else argv)

    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.master("local[4]").appName("holding labels")
             .config("spark.driver.memory", "8g").config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false")
             .getOrCreate())
    try:
        day = pd.Timestamp(args.day, tz="UTC")
        scores, sweep, episodes = run(spark, Path(args.data_dir), args.airports, day, sweep=not args.no_sweep)
    finally:
        spark.stop()
    print(scores.to_string(index=False))
    out = Path(args.results_dir)
    write(out, f"holding_score_{args.day}.csv", scores, argv,
          notes="Shipped thresholds against the Olive et al. 2019 labels; 06_metrics.py scoring.")
    if not args.no_sweep:
        write(out, f"holding_sweep_{args.day}.csv", sweep, argv, notes="Three-threshold grid, 27 points per airport.")
    write(out, f"holding_episodes_{args.day}.csv", episodes, argv, notes="Detected episodes (shipped thresholds).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
