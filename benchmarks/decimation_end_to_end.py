"""
Run the ADEP/ADES detection end to end on bucket-decimated state vectors.

The decimation study measured that the bucket rule keeps 1.002x the rows of the
modulo rule, concentrated at low altitude. That predicts a negligible effect on
detection -- but predicting is not measuring, and the rows it rescues sit
exactly where the ADEP/ADES evidence is. This runs the whole chain on
bucket-sampled input and scores it against the same ground truth.

Everything is written under ``opdi/research/`` with a ``_bucket`` suffix, so no
production prefix is touched. The modulo arm is not re-run: its candidate cache
already exists, and is simply scored on the same days at the same parameters.

    python benchmarks/decimation_end_to_end.py --days 2025-06-05 --results-dir <dir>
"""

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import functions as F

import osn_sample
from osn_sample import build_spark, load_dotenv
from adep_ades import (
    airport_locations, airport_types, label_ground_truth, load_ground_truth,
    score, per_airport_counts, per_type_counts,
)
from benchmark_modes import predictions_from_candidates, identities_from_candidates

#: Where the bucket arm's intermediate tables go. Suffixed, under research/,
#: so a production prefix can never be the target of this experiment.
REDIRECT = {
    "osn_statevectors_v2": "research/sv_bucket",
    "osn_tracks": "research/tracks_bucket",
    "opdi_endpoint_candidates": "research/cand_bucket",
}

#: The operating point recommended by version 4.5, applied to both roles.
OP_RADIUS_NM = 30.0
OP_HEIGHT_FT = 15000.0
OP_PENALTY_NM = 10.0

MODULO_CANDIDATES = "s3a://eurocontrol/opdi/opdi_endpoint_candidates"
BUCKET_CANDIDATES = "s3a://eurocontrol/opdi/research/cand_bucket"


def redirect_storage():
    """Point the pipeline's table names at the research copies.

    Wrapping StorageManager rather than editing the pipeline keeps the
    experiment out of the production code path entirely: nothing in
    ``src/opdi`` knows this ran.
    """
    from opdi.utils.storage import StorageManager

    for name in ("read_table", "write_table", "table_ref", "table_exists"):
        original = getattr(StorageManager, name)
        if getattr(original, "_redirected", False):
            continue

        def make(orig):
            def wrapper(self, table_name, *a, **kw):
                return orig(self, REDIRECT.get(table_name, table_name), *a, **kw)
            wrapper._redirected = True
            return wrapper

        setattr(StorageManager, name, make(original))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", nargs="+", default=["2025-06-05"])
    ap.add_argument("--month", default="202506")
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--executors", type=int, default=8)
    ap.add_argument("--ui-port", type=int, default=4041)
    ap.add_argument("--skip-build", action="store_true",
                    help="score only; assumes the bucket tables already exist")
    args = ap.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
    load_dotenv()
    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(6, "8g", distributed=True)
    spark.sparkContext.setLogLevel("ERROR")
    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)

    days = [datetime.strptime(d, "%Y-%m-%d").date() for d in args.days]
    month = date(days[0].year, days[0].month, 1)

    if not args.skip_build:
        redirect_storage()
        from opdi.config import OPDIConfig
        from opdi.ingestion.osn_statevectors import StateVectorIngestion, DECIMATION_BUCKET
        from opdi.pipeline.tracks import TrackProcessor
        from opdi.pipeline.flights import FlightListProcessor

        cfg = OPDIConfig.for_environment("opensky")

        print(f"\n=== 01 ingest, decimation=bucket, {days[0]} .. {days[-1]} ===")
        ing = StateVectorIngestion(spark, cfg, decimation=DECIMATION_BUCKET)
        n = ing.ingest_from_s3(days[0], date.fromordinal(days[-1].toordinal() + 1))
        print(f"  rows ingested: {n:,}")

        print("\n=== 02 tracks ===")
        TrackProcessor(spark, cfg).process_month(month, skip_if_processed=False)

        print("\n=== 03 endpoint candidates ===")
        FlightListProcessor(spark, cfg).build_endpoint_candidates(
            month, max_radius_nm=110.0, rebuild=True)

    # -- score both arms on identical ground truth --------------------------
    gt = load_ground_truth(spark, [args.month], args.days)
    gt = label_ground_truth(gt, airport_locations(spark)).cache()
    print(f"\nground-truth flights on {args.days}: {gt.count():,}")

    rows, per_type_frames = [], []
    for arm, path in (("modulo (published)", MODULO_CANDIDATES),
                      ("bucket (proposed)", BUCKET_CANDIDATES)):
        cand = spark.read.parquet(path)
        # The modulo cache covers three days; restrict it to the same days.
        cand = cand.filter(F.to_date("event_time").isin(args.days)).cache()
        n_cand = cand.count()
        ident = identities_from_candidates(cand)
        pred = predictions_from_candidates(
            cand, OP_RADIUS_NM, OP_HEIGHT_FT, OP_PENALTY_NM)
        m = score(pred, ident, gt)
        m.update(arm=arm, candidates=n_cand, radius_nm=OP_RADIUS_NM,
                 height_ft=OP_HEIGHT_FT, penalty_nm=OP_PENALTY_NM)
        rows.append(m)
        print(f"  {arm:20} cand {n_cand:>10,}  "
              f"ADEP {m['adep_coverage']:6.2%}/{m['adep_accuracy']:6.2%}  "
              f"ADES {m['ades_coverage']:6.2%}/{m['ades_accuracy']:6.2%}")

        pt = per_type_counts(pred, ident, gt, airport_types(spark)).toPandas()
        pt["arm"] = arm
        per_type_frames.append(pt)

    spark.createDataFrame(rows).toPandas().to_csv(out / "arm_comparison.csv", index=False)
    import pandas as pd
    pd.concat(per_type_frames).to_csv(out / "arm_per_type.csv", index=False)
    print(f"\nwritten to {out}")
    spark.stop()


if __name__ == "__main__":
    main()
