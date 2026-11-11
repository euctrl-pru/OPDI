"""How well the H3 lookup agrees with the exact polygons, and what the crossings look like.

    python benchmarks/fir_consistency.py --day 2026-06-14 --results-dir <paper>/data/fir \\
        [--input a10] [--tracks 2000]

Two things, both written with provenance:

``fir_consistency_<day>.csv``: for a sample of tracks with at least one
``xing-fir``, every sample's volume from the compact H3 lookup
(``attach_fir``) against exact point-in-polygon membership in the same
AIRAC 524 polygons (shapely, driver side, FL band applied). Rows: share of
samples agreeing, share with no volume on either side, the timing error at
each crossing (seconds between the H3 crossing and the exact one), and the
share of samples that matched more than one volume (the overlap rule in use).

``fir_crossings_<day>.csv``: crossings per flight and per FIR pair over the
whole day, the share of samples with no FIR, and the most crossed boundaries.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

import provenance  # noqa: E402

_CODE_PATHS = ["benchmarks/fir_consistency.py", "src/opdi/pipeline/fir.py",
               "src/opdi/reference/h3_airspaces.py"]


def exact_membership(samples: pd.DataFrame, volumes: pd.DataFrame) -> pd.Series:
    """The exact volume code per sample (``lat, lon, fl``) from WKT polygons
    (``code, min_fl, max_fl, geometry_wkt, area_km2``), ties resolved like
    the pipeline: narrowest band, smallest area, lowest code. NULL outside."""
    import shapely
    from shapely.strtree import STRtree

    geoms = [shapely.from_wkt(w) for w in volumes["geometry_wkt"]]
    tree = STRtree(geoms)
    vol = volumes.reset_index(drop=True)
    vol["band"] = vol["max_fl"] - vol["min_fl"]
    pts = shapely.points(samples["lon"].values, samples["lat"].values)
    hits = tree.query(pts, predicate="within")
    best = {}
    for i, j in zip(*hits):
        fl = samples["fl"].iat[i]
        r = vol.iloc[j]
        if not (r["min_fl"] <= fl < r["max_fl"]):
            continue
        key = (r["band"], r["area_km2"], r["code"])
        if i not in best or key < best[i]:
            best[i] = key
    return pd.Series([best[i][2] if i in best else None for i in range(len(samples))], index=samples.index)


def crossing_times(frame: pd.DataFrame, code_col: str) -> pd.DataFrame:
    """``track_id, fir_from, fir_to, t`` for every change of ``code_col`` between
    consecutive non-null samples of a track (no debounce)."""
    rows = []
    for tid, g in frame.sort_values(["track_id", "event_time"]).groupby("track_id"):
        prev_code = None
        for code, t in zip(g[code_col], g["event_time"]):
            if code is None or (isinstance(code, float) and pd.isna(code)):
                continue
            if prev_code is not None and code != prev_code:
                rows.append({"track_id": tid, "fir_from": prev_code, "fir_to": code, "t": t})
            prev_code = code
    return pd.DataFrame(rows, columns=["track_id", "fir_from", "fir_to", "t"])


def timing_errors(h3_x: pd.DataFrame, exact_x: pd.DataFrame) -> pd.Series:
    """Seconds between each H3 crossing and the nearest exact crossing of the
    same track and pair (NaN when there is none)."""
    out = []
    for _, r in h3_x.iterrows():
        cand = exact_x[(exact_x.track_id == r.track_id) & (exact_x.fir_from == r.fir_from) & (exact_x.fir_to == r.fir_to)]
        if cand.empty:
            out.append(float("nan"))
        else:
            out.append(float((cand["t"] - r["t"]).dt.total_seconds().abs().min()))
    return pd.Series(out)


def summarise(frame: pd.DataFrame, h3_x: pd.DataFrame, exact_x: pd.DataFrame, errors: pd.Series) -> dict:
    both = frame["fir_code"].notna() & frame["exact_code"].notna()
    return {
        "samples": int(len(frame)),
        "tracks": int(frame["track_id"].nunique()),
        "agree_pct": round(100.0 * float((frame["fir_code"].fillna("-") == frame["exact_code"].fillna("-")).mean()), 3),
        "agree_where_both_pct": round(100.0 * float((frame.loc[both, "fir_code"] == frame.loc[both, "exact_code"]).mean()), 3) if both.any() else None,
        "h3_null_pct": round(100.0 * float(frame["fir_code"].isna().mean()), 3),
        "exact_null_pct": round(100.0 * float(frame["exact_code"].isna().mean()), 3),
        "h3_crossings": int(len(h3_x)), "exact_crossings": int(len(exact_x)),
        "matched_crossings": int(errors.notna().sum()),
        "timing_error_p50_s": float(errors.quantile(0.5)) if errors.notna().any() else None,
        "timing_error_p90_s": float(errors.quantile(0.9)) if errors.notna().any() else None,
        "timing_error_max_s": float(errors.max()) if errors.notna().any() else None,
    }


def main(argv: Optional[List[str]] = None) -> int:
    from pyspark.sql import functions as F
    from run_airspace_events_day import A10_ARM, INPUT_WAREHOUSE, PREFIX, study_session

    from opdi.config import EventConfig
    from opdi.pipeline.fir import attach_fir
    from opdi.pipeline.holes import add_piece
    from opdi.reference.h3_airspaces import DEFAULT_FIR_URLS, dissolve_volumes

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--day", required=True)
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--input", choices=("world", "a10"), default="world")
    ap.add_argument("--tracks", type=int, default=2000)
    args = ap.parse_args(argv)
    argv = list(sys.argv[1:] if argv is None else argv)
    day = pd.Timestamp(args.day).date()
    inputs = A10_ARM if args.input == "a10" else INPUT_WAREHOUSE
    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)

    spark = study_session("fir consistency")
    events = spark.read.parquet(f"{PREFIX}/on/opdi_flight_events").filter(
        (F.col("dof").cast("date") == F.lit(day).cast("date")) & (F.col("type") == "xing-fir"))
    events.cache()

    # --- the whole day's crossings -------------------------------------------
    ev = events.select("flight_id", "event_time", "info").toPandas()
    info = pd.DataFrame(list(ev["info"].map(json.loads)))
    ev = pd.concat([ev, info], axis=1)
    per_flight = ev.groupby("flight_id").size()
    n_tracks_day = spark.read.parquet(f"{inputs}/osn_tracks_clean").filter(
        F.col("dof").cast("date") == F.lit(day).cast("date")).select("track_id").distinct().count()
    pairs = ev.groupby(["fir_from", "fir_to"]).size().sort_values(ascending=False).reset_index(name="crossings")
    day_rows = [{"metric": "tracks_on_day", "value": n_tracks_day},
                {"metric": "tracks_with_a_crossing", "value": int(per_flight.size)},
                {"metric": "crossings", "value": int(len(ev))},
                {"metric": "crossings_per_crossing_track_p50", "value": float(per_flight.median()) if len(per_flight) else None},
                {"metric": "crossings_per_crossing_track_p90", "value": float(per_flight.quantile(0.9)) if len(per_flight) else None},
                {"metric": "crossings_per_crossing_track_max", "value": int(per_flight.max()) if len(per_flight) else None},
                {"metric": "vertical_crossings_same_prefix", "value": int((ev["fir_from"].str[:4] == ev["fir_to"].str[:4]).sum()) if len(ev) else 0},
                {"metric": "bracket_s_p50", "value": float(ev["bracket_s"].median()) if len(ev) else None},
                {"metric": "bracket_s_p90", "value": float(ev["bracket_s"].quantile(0.9)) if len(ev) else None},
                {"metric": "airac_cfmu", "value": ",".join(sorted(ev["airac_cfmu"].astype(str).unique())) if len(ev) else None}]
    for _, r in pairs.head(15).iterrows():
        day_rows.append({"metric": f"pair {r.fir_from}->{r.fir_to}", "value": int(r.crossings)})
    name = f"fir_crossings_{args.day}.csv"
    pd.DataFrame(day_rows).to_csv(out / name, index=False)
    provenance.record(out, name, "benchmarks/fir_consistency.py", argv, [REPO / p for p in _CODE_PATHS],
                      inputs={"crossings": len(ev)}, input_tables=[f"{PREFIX}/on/opdi_flight_events"])

    # --- H3 against the exact polygons on a sample of crossing tracks --------
    sample_ids = [r.flight_id for r in events.select("flight_id").distinct().limit(args.tracks).collect()]
    tracks = (spark.read.parquet(f"{inputs}/osn_tracks_clean")
              .filter(F.col("dof").cast("date") == F.lit(day).cast("date"))
              .filter(F.col("track_id").isin(sample_ids))
              .select("track_id", "event_time", "lat", "lon", "baro_altitude_c", "h3_res_7")
              .filter(F.col("lat").isNotNull() & F.col("h3_res_7").isNotNull()))
    cfg = EventConfig()
    tracks = add_piece(tracks, cfg.track_hole_seconds)
    fir_ref = spark.read.parquet(f"{PREFIX}/ref/opdi_h3_airspace_ref").filter("airspace_type = 'FIR'")
    n_in = tracks.count()
    withfir = attach_fir(tracks, fir_ref, cfg)
    frame = withfir.select("track_id", "event_time", "lat", "lon", "_fl", "fir_code").toPandas()
    frame = frame.rename(columns={"_fl": "fl"})
    volumes = pd.concat([dissolve_volumes(pd.read_parquet(u)) for u in DEFAULT_FIR_URLS], ignore_index=True)
    frame["exact_code"] = exact_membership(frame, volumes)
    h3_x = crossing_times(frame, "fir_code")
    exact_x = crossing_times(frame, "exact_code")
    errors = timing_errors(h3_x, exact_x)
    summary = summarise(frame, h3_x, exact_x, errors)
    summary["double_match_rows_dropped"] = int(n_in - len(frame))
    name = f"fir_consistency_{args.day}.csv"
    pd.DataFrame([{"metric": k, "value": v} for k, v in summary.items()]).to_csv(out / name, index=False)
    provenance.record(out, name, "benchmarks/fir_consistency.py", argv, [REPO / p for p in _CODE_PATHS],
                      inputs={"samples": summary["samples"], "tracks": summary["tracks"]},
                      input_tables=[f"{inputs}/osn_tracks_clean", f"{PREFIX}/ref/opdi_h3_airspace_ref"])
    print(pd.DataFrame(day_rows).to_string(index=False))
    print(json.dumps(summary, indent=1))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
