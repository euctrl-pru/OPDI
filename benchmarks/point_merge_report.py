"""Point merge usage per airport on a pipeline day, with plots for a visual check.

    python benchmarks/point_merge_report.py --day 2026-06-14 --results-dir <paper>/data/point_merge \\
        --plot EIDW --n 20

Reads the ``on`` arm's ``opdi_flight_events`` and the day's ``opdi_flight_list``
from the study prefix (``run_airspace_events_day.py``) and writes
``point_merge_usage_<day>.csv``: per covered airport, arrivals, arrivals with
at least one leg pair, the share, and the median leg distance, level and
duration. With ``--plot`` it also draws ``n`` detected and ``n`` undetected
arrivals at one airport (tracks within 60 NM of the airport, merge points
marked) to PNGs under the results directory, for the inspection the paper
records in ``eidw_visual_check.csv``.
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

_CODE_PATHS = [REPO / "benchmarks" / "point_merge_report.py", REPO / "src" / "opdi" / "pipeline" / "point_merge.py"]


def usage_table(events: pd.DataFrame, flight_list: pd.DataFrame, covered: List[str]) -> pd.DataFrame:
    """Per airport in ``covered``: arrivals, arrivals with a leg pair, share,
    medians of the leg's distance, level and duration, and merge-point
    overflights without a leg.

    ``events`` has ``flight_id, type, info`` (``info`` JSON string);
    ``flight_list`` has ``id, ades``.
    """
    arrivals = flight_list[flight_list["ades"].isin(covered)].groupby("ades")["id"].nunique()
    ev = events[events["type"].isin(["pms-leg-exit", "pms-merge-point"])].copy()
    if len(ev):
        info = pd.DataFrame(list(ev["info"].map(json.loads)))
        ev = pd.concat([ev.reset_index(drop=True), info], axis=1)
    rows = []
    for apt in covered:
        e = ev[ev.get("apt_icao", pd.Series(dtype=str)) == apt] if len(ev) else ev
        legs = e[e["type"] == "pms-leg-exit"] if len(e) else e
        mps = e[e["type"] == "pms-merge-point"] if len(e) else e
        n_arr = int(arrivals.get(apt, 0))
        n_leg = int(legs["flight_id"].nunique()) if len(legs) else 0
        n_mp = int(mps["flight_id"].nunique()) if len(mps) else 0
        n_direct_only = int(mps[~mps["via_leg"].astype(bool)]["flight_id"].nunique()) if len(mps) else 0
        rows.append({
            "apt_icao": apt, "arrivals": n_arr, "arrivals_with_leg": n_leg,
            "leg_share_pct": round(100.0 * n_leg / n_arr, 1) if n_arr else None,
            "arrivals_at_merge_point": n_mp, "merge_point_without_leg": n_direct_only,
            "median_leg_distance_nm": round(float(legs["leg_distance_nm"].median()), 2) if n_leg else None,
            "median_leg_level_ft": round(float(legs["leg_level_ft"].median())) if n_leg else None,
            "median_leg_duration_s": round(float(legs["duration_s"].median())) if n_leg else None,
            "legs": int(len(legs)) if len(legs) else 0,
        })
    return pd.DataFrame(rows)


def _read(spark, prefix: str, table: str, day):
    from pyspark.sql import functions as F

    df = spark.read.parquet(f"{prefix}/{table}")
    if "dof" in df.columns:
        df = df.filter(F.col("dof").cast("date") == F.lit(day).cast("date"))
    return df


def plot_arrivals(spark, tracks_path: str, flight_list: pd.DataFrame, events: pd.DataFrame,
                  pms_ref: pd.DataFrame, apt: str, n: int, out_dir: Path, day) -> List[dict]:
    """Draw ``n`` arrivals with a leg pair and ``n`` without; return the rows
    of the visual-check template (``track_id, detected, judged_real_leg``)."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from pyspark.sql import functions as F

    arr = flight_list[flight_list["ades"] == apt]["id"].drop_duplicates()
    with_leg = set(events[(events["type"] == "pms-leg-exit")
                          & events["info"].str.contains(f'"apt_icao":"{apt}"')]["flight_id"])
    detected = [t for t in arr if t in with_leg][:n]
    undetected = [t for t in arr if t not in with_leg][:n]
    ids = detected + undetected
    tracks = (spark.read.parquet(tracks_path)
              .filter(F.col("dof").cast("date") == F.lit(day).cast("date"))
              .filter(F.col("track_id").isin(ids))
              .select("track_id", "event_time", "lat", "lon", "baro_altitude_c").toPandas())
    ref = pms_ref[pms_ref["apt_icao"] == apt]
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for kind, group in (("detected", detected), ("undetected", undetected)):
        for i, tid in enumerate(group):
            t = tracks[tracks["track_id"] == tid].sort_values("event_time")
            fig, ax = plt.subplots(figsize=(6, 6))
            ax.plot(t["lon"], t["lat"], "-", lw=0.8)
            ax.scatter(ref["lon"], ref["lat"], marker="x", c="red")
            for _, r in ref.iterrows():
                ax.annotate(r["merge_point"], (r["lon"], r["lat"]), fontsize=7)
            legs = events[(events["flight_id"] == tid) & events["type"].str.startswith("pms-leg")]
            for _, e in legs.iterrows():
                ax.scatter([e["longitude"]], [e["latitude"]], marker="o", c="green", s=30)
            lat0, lon0 = ref["lat"].mean(), ref["lon"].mean()
            ax.set_xlim(lon0 - 1.6, lon0 + 1.6)
            ax.set_ylim(lat0 - 1.0, lat0 + 1.0)
            ax.set_title(f"{apt} {kind} {tid}", fontsize=8)
            fig.savefig(out_dir / f"{apt}_{kind}_{i:02d}.png", dpi=80)
            plt.close(fig)
            rows.append({"track_id": tid, "detected": kind == "detected", "judged_real_leg": ""})
    return rows


def main(argv: Optional[List[str]] = None) -> int:
    from run_airspace_events_day import A10_ARM, INPUT_WAREHOUSE, PREFIX, study_session

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--day", required=True)
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--input", choices=("world", "a10"), default="world")
    ap.add_argument("--plot", default=None, help="airport to draw")
    ap.add_argument("--n", type=int, default=20)
    args = ap.parse_args(argv)
    argv = list(sys.argv[1:] if argv is None else argv)

    from pyspark.sql import SparkSession

    from opdi.reference.point_merge import point_merge_systems_for_day

    spark = study_session("point merge report")
    day = pd.Timestamp(args.day).date()
    inputs = A10_ARM if args.input == "a10" else INPUT_WAREHOUSE
    events = _read(spark, f"{PREFIX}/on", "opdi_flight_events", day).filter("type like 'pms-%'").toPandas()
    flight_list = _read(spark, inputs, "opdi_flight_list", day).select("id", "ades").toPandas()
    pms_ref = point_merge_systems_for_day(spark, day).toPandas()
    covered = sorted(pms_ref["apt_icao"].unique())
    table = usage_table(events, flight_list, covered)
    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)
    name = f"point_merge_usage_{args.day}.csv"
    table.to_csv(out / name, index=False)
    provenance.record(out, name, "benchmarks/point_merge_report.py", argv, _CODE_PATHS,
                      inputs={"airports": len(table), "pms_events": len(events)},
                      input_tables=[f"{PREFIX}/on/opdi_flight_events", f"{inputs}/opdi_flight_list"])
    print(table.to_string(index=False))
    if args.plot:
        rows = plot_arrivals(spark, f"{inputs}/osn_tracks_clean", flight_list, events, pms_ref,
                             args.plot, args.n, out / "plots", day)
        pd.DataFrame(rows).to_csv(out / f"{args.plot.lower()}_visual_check_template.csv", index=False)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
