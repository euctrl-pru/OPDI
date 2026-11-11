"""Point merge usage per airport on a pipeline day, with plots for a visual check.

    python benchmarks/point_merge_report.py --day 2026-06-14 --results-dir <paper>/data/point_merge \\
        --plot EIDW --n 20

Reads the ``on`` arm's ``opdi_flight_events`` and the day's ``opdi_flight_list``
from the study prefix (``run_airspace_events_day.py``) and writes
``point_merge_usage_<day>.csv``: per covered airport, arrivals, arrivals with
at least one leg pair, the share, and the median leg distance, level and
duration. With ``--plot`` it also exports the tracks of ``n`` detected and ``n``
undetected arrivals at one airport (CSV under ``plots/``), which
``plot_point_merge.py`` draws with matplotlib from any environment that has
it, for the inspection the paper records in ``eidw_visual_check.csv``.
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

_CODE_PATHS = ["benchmarks/point_merge_report.py", "src/opdi/pipeline/point_merge.py"]


def usage_table(events: pd.DataFrame, flight_list: pd.DataFrame, covered: List[str]) -> pd.DataFrame:
    """Per airport in ``covered``: arrivals, arrivals with a leg pair, share,
    medians of the leg's distance, level and duration, and merge-point
    overflights without a leg.

    An *arrival* at an airport is a flight-list row naming it as ``ades``
    **and** an inbound ``xing-40nm`` crossing of that airport's ring: the
    flight list also names a destination for flights that never landed there
    (the trend guess for a track that ends in the air), and those would
    inflate the denominator tenfold.

    ``events`` has ``flight_id, type, info`` (``info`` JSON string) for the
    ``pms-*`` types and ``xing-40nm``; ``flight_list`` has ``id, ades``.
    """
    ev = events[events["type"].isin(["pms-leg-exit", "pms-merge-point", "xing-40nm"])].copy()
    if len(ev):
        info = pd.DataFrame(list(ev["info"].map(json.loads)))
        ev = pd.concat([ev.reset_index(drop=True), info], axis=1)
    rows = []
    for apt in covered:
        named = set(flight_list[flight_list["ades"] == apt]["id"])
        e = ev[ev.get("apt_icao", pd.Series(dtype=str)) == apt] if len(ev) else ev
        rings = e[(e["type"] == "xing-40nm") & (e.get("direction", pd.Series(dtype=str)) == "inbound")] if len(e) else e
        arrived = (set(rings["flight_id"]) & named) if len(rings) else set()
        legs = e[(e["type"] == "pms-leg-exit") & e["flight_id"].isin(arrived)] if len(e) else e
        mps = e[(e["type"] == "pms-merge-point") & e["flight_id"].isin(arrived)] if len(e) else e
        n_arr = len(arrived)
        n_leg = int(legs["flight_id"].nunique()) if len(legs) else 0
        n_mp = int(mps["flight_id"].nunique()) if len(mps) else 0
        n_direct_only = int(mps[~mps["via_leg"].astype(bool)]["flight_id"].nunique()) if len(mps) else 0
        rows.append({
            "apt_icao": apt, "named_as_destination": len(named), "arrivals": n_arr,
            "arrivals_with_leg": n_leg,
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


def export_arrivals(spark, tracks_path: str, flight_list: pd.DataFrame, events: pd.DataFrame,
                    pms_ref: pd.DataFrame, apt: str, n: int, out_dir: Path, day) -> List[dict]:
    """Write what ``plot_point_merge.py`` draws: the tracks of ``n`` arrivals
    with a leg pair and ``n`` without (``tracks_<apt>.csv``), their point-merge
    events (``events_<apt>.csv``) and the merge points (``merge_points_<apt>.csv``).
    Returns the rows of the visual-check template."""
    from pyspark.sql import functions as F

    arr = flight_list[flight_list["ades"] == apt]["id"].drop_duplicates()
    with_leg = set(events[(events["type"] == "pms-leg-exit")
                          & events["info"].str.contains(f'"apt_icao":"{apt}"')]["flight_id"])
    at_mp = set(events[(events["type"] == "pms-merge-point")
                       & events["info"].str.contains(f'"apt_icao":"{apt}"')]["flight_id"])
    detected = [t for t in arr if t in with_leg][:n]
    # Undetected: arrivals that reached the merge point without a leg, so the
    # plot shows the approach rather than a flight that never came near.
    undetected = [t for t in arr if t in at_mp and t not in with_leg][:n]
    ids = detected + undetected
    tracks = (spark.read.parquet(tracks_path)
              .filter(F.col("dof").cast("date") == F.lit(day).cast("date"))
              .filter(F.col("track_id").isin(ids))
              .select("track_id", "event_time", "lat", "lon", "baro_altitude_c").toPandas())
    kind = {t: "detected" for t in detected}
    kind.update({t: "undetected" for t in undetected})
    tracks["kind"] = tracks["track_id"].map(kind)
    out_dir.mkdir(parents=True, exist_ok=True)
    tracks.to_csv(out_dir / f"tracks_{apt}.csv", index=False)
    events[events["flight_id"].isin(ids)].to_csv(out_dir / f"events_{apt}.csv", index=False)
    pms_ref[pms_ref["apt_icao"] == apt].to_csv(out_dir / f"merge_points_{apt}.csv", index=False)
    return [{"track_id": t, "detected": t in with_leg, "judged_real_leg": ""} for t in ids]


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


    from opdi.reference.point_merge import point_merge_systems_for_day

    spark = study_session("point merge report")
    day = pd.Timestamp(args.day).date()
    inputs = A10_ARM if args.input == "a10" else INPUT_WAREHOUSE
    events = _read(spark, f"{PREFIX}/on", "opdi_flight_events", day).filter(
        "type like 'pms-%' or type = 'xing-40nm'").toPandas()
    flight_list = _read(spark, inputs, "opdi_flight_list", day).select("id", "ades").toPandas()
    pms_ref = point_merge_systems_for_day(spark, day).toPandas()
    covered = sorted(pms_ref["apt_icao"].unique())
    table = usage_table(events, flight_list, covered)
    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)
    name = f"point_merge_usage_{args.day}.csv"
    table.to_csv(out / name, index=False)
    provenance.record(out, name, "benchmarks/point_merge_report.py", argv, [REPO / p for p in _CODE_PATHS],
                      inputs={"airports": len(table), "pms_events": len(events)},
                      input_tables=[f"{PREFIX}/on/opdi_flight_events", f"{inputs}/opdi_flight_list"])
    print(table.to_string(index=False))
    if args.plot:
        rows = export_arrivals(spark, f"{inputs}/osn_tracks_clean", flight_list, events, pms_ref,
                               args.plot, args.n, out / "plots", day)
        pd.DataFrame(rows).to_csv(out / f"{args.plot.lower()}_visual_check_template.csv", index=False)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
