"""Score ``xing-fir`` against NM airspace profiles (``reference/extract_airspace_profiles.R``).

    python benchmarks/fir_truth_score.py --day 2026-06-14 --truth reference/airspace_profiles_2026-06-14_CPF.parquet \\
        --results-dir <paper>/data/fir

A truth crossing is the boundary between consecutive FIR volumes of one NM
flight (``SEQ_ID`` order): ``(AIRCRAFT_ADDRESS, CALLSIGN, from, to, EXIT_TIME
of the first)``. An OPDI crossing matches when the flight (icao24 and callsign,
the track's ``flight_id`` resolved through the flight list), the volume pair
and the time within ``--tolerance`` seconds agree. Reported: matched share of
truth crossings (recall), matched share of OPDI crossings on flights that have
truth (precision), and the time offset quantiles. Nothing here runs without
the laptop extract; the scoring is unit-tested on synthetic frames.
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


def truth_crossings(profiles: pd.DataFrame) -> pd.DataFrame:
    """``icao24, callsign, fir_from, fir_to, t`` from the per-volume profile rows."""
    rows = []
    cols = ["ID", "SEQ_ID", "AIRSPACE_ID", "EXIT_TIME", "AIRCRAFT_ADDRESS", "CALLSIGN"]
    p = profiles[cols].sort_values(["ID", "SEQ_ID"])
    for _, g in p.groupby("ID"):
        prev = None
        for r in g.itertuples(index=False):
            if prev is not None and r.AIRSPACE_ID != prev.AIRSPACE_ID:
                rows.append({"icao24": str(prev.AIRCRAFT_ADDRESS).lower(), "callsign": str(prev.CALLSIGN).strip(),
                             "fir_from": prev.AIRSPACE_ID, "fir_to": r.AIRSPACE_ID, "t": pd.Timestamp(prev.EXIT_TIME)})
            prev = r
    return pd.DataFrame(rows, columns=["icao24", "callsign", "fir_from", "fir_to", "t"])


def opdi_crossings(events: pd.DataFrame) -> pd.DataFrame:
    """``icao24, callsign, fir_from, fir_to, t`` from ``xing-fir`` rows that carry
    ``icao24`` and ``callsign`` (joined from the tracks by the caller)."""
    info = pd.DataFrame(list(events["info"].map(json.loads))) if len(events) else pd.DataFrame()
    out = pd.DataFrame({
        "icao24": events["icao24"].str.lower().values, "callsign": events["callsign"].str.strip().values,
        "fir_from": info.get("fir_from", pd.Series(dtype=str)).values if len(info) else [],
        "fir_to": info.get("fir_to", pd.Series(dtype=str)).values if len(info) else [],
        "t": pd.to_datetime(events["event_time"]).values,
    })
    return out


def score_crossings(opdi: pd.DataFrame, truth: pd.DataFrame, tolerance_s: float = 120.0) -> dict:
    """Match on flight and volume pair, nearest in time within the tolerance."""
    key = ["icao24", "callsign", "fir_from", "fir_to"]
    flights_with_truth = set(map(tuple, truth[["icao24", "callsign"]].drop_duplicates().values))
    cand = opdi.merge(truth, on=key, suffixes=("_o", "_t"))
    cand["dt"] = (cand["t_o"] - cand["t_t"]).dt.total_seconds()
    cand = cand[cand["dt"].abs() <= tolerance_s].sort_values("dt", key=abs)
    matched = cand.drop_duplicates(subset=key + ["t_t"]).drop_duplicates(subset=key + ["t_o"])
    on_truth_flights = opdi[[tuple(x) in flights_with_truth for x in opdi[["icao24", "callsign"]].values]]
    return {
        "truth_crossings": int(len(truth)), "opdi_crossings": int(len(opdi)),
        "opdi_crossings_on_truth_flights": int(len(on_truth_flights)),
        "matched": int(len(matched)),
        "recall": float(len(matched) / len(truth)) if len(truth) else float("nan"),
        "precision": float(len(matched) / len(on_truth_flights)) if len(on_truth_flights) else float("nan"),
        "offset_p50_s": float(matched["dt"].abs().median()) if len(matched) else None,
        "offset_p90_s": float(matched["dt"].abs().quantile(0.9)) if len(matched) else None,
        "tolerance_s": tolerance_s,
    }


_CODE_PATHS = ["benchmarks/fir_truth_score.py"]


def main(argv: Optional[List[str]] = None) -> int:
    import provenance
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from run_airspace_events_day import INPUT_WAREHOUSE, PREFIX, study_session

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--day", required=True)
    ap.add_argument("--truth", required=True)
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--tolerance", type=float, default=120.0)
    args = ap.parse_args(argv)
    argv = list(sys.argv[1:] if argv is None else argv)
    day = pd.Timestamp(args.day).date()
    spark = study_session("fir truth")
    events = spark.read.parquet(f"{PREFIX}/on/opdi_flight_events").filter(
        (F.col("dof").cast("date") == F.lit(day).cast("date")) & (F.col("type") == "xing-fir"))
    ids = spark.read.parquet(f"{INPUT_WAREHOUSE}/osn_tracks_clean").filter(
        F.col("dof").cast("date") == F.lit(day).cast("date")).groupBy("track_id").agg(
        F.first("icao24", ignorenulls=True).alias("icao24"), F.first("callsign", ignorenulls=True).alias("callsign"))
    ev = events.join(ids, events.flight_id == ids.track_id).select("icao24", "callsign", "event_time", "info").toPandas()
    truth = truth_crossings(pd.read_parquet(args.truth))
    result = score_crossings(opdi_crossings(ev), truth, args.tolerance)
    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)
    name = f"fir_truth_score_{args.day}.csv"
    pd.DataFrame([{"metric": k, "value": v} for k, v in result.items()]).to_csv(out / name, index=False)
    provenance.record(out, name, "benchmarks/fir_truth_score.py", argv,
                      [REPO / p for p in _CODE_PATHS], inputs=result)
    print(json.dumps(result, indent=1))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
