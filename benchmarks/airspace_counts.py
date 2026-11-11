"""Row counts of the new event and measurement types on the pipeline day.

    python benchmarks/airspace_counts.py --day 2026-06-14 --results-dir <paper>/data/pipeline_day

Writes ``counts.csv`` (``kind, name, count``) from the ``on`` arm of the study
prefix: every event type that starts with ``holding-``, ``pms-`` or ``xing-fir``,
every measurement type the families add, and the number of tracks the day had.
"""

import argparse
import sys
from pathlib import Path
from typing import List, Optional

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

import provenance  # noqa: E402

_CODE_PATHS = ["benchmarks/airspace_counts.py"]

NEW_TYPES = ("holding-", "pms-", "xing-fir")
NEW_MEASURES = ("Holding ", "Point merge ")


def main(argv: Optional[List[str]] = None) -> int:
    from pyspark.sql import functions as F
    from run_airspace_events_day import A10_ARM, INPUT_WAREHOUSE, PREFIX, study_session

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--day", required=True)
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--input", choices=("world", "a10"), default="world")
    args = ap.parse_args(argv)
    argv = list(sys.argv[1:] if argv is None else argv)
    day = pd.Timestamp(args.day).date()
    inputs = A10_ARM if args.input == "a10" else INPUT_WAREHOUSE
    spark = study_session("airspace counts")
    dof = lambda df: df.filter(F.col("dof").cast("date") == F.lit(day).cast("date"))  # noqa: E731
    ev = dof(spark.read.parquet(f"{PREFIX}/on/opdi_flight_events"))
    ms = dof(spark.read.parquet(f"{PREFIX}/on/opdi_measurements"))
    rows = []
    for r in ev.groupBy("type").count().collect():
        if r["type"].startswith(NEW_TYPES):
            rows.append({"kind": "event", "name": r["type"], "count": r["count"]})
    for r in ms.groupBy("type").count().collect():
        if r["type"].startswith(NEW_MEASURES):
            rows.append({"kind": "measurement", "name": r["type"], "count": r["count"]})
    n_tracks = dof(spark.read.parquet(f"{inputs}/osn_tracks_clean")).select("track_id").distinct().count()
    rows.append({"kind": "input", "name": "tracks", "count": n_tracks})
    rows.append({"kind": "input", "name": "events_all_types", "count": ev.count()})
    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows).sort_values(["kind", "name"])
    df.to_csv(out / "counts.csv", index=False)
    provenance.record(out, "counts.csv", "benchmarks/airspace_counts.py", argv,
                      [REPO / p for p in _CODE_PATHS], inputs={"rows": len(df)},
                      input_tables=[f"{PREFIX}/on/opdi_flight_events", f"{PREFIX}/on/opdi_measurements"])
    print(df.to_string(index=False))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
