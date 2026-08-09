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

    ``write_table`` takes the DataFrame first and the table name second, so it
    needs its own wrapper -- a generic one that assumes the name is the first
    argument silently fails to redirect *writes only*, which is the one case
    where failing silently destroys data. Hence the guard: any write whose
    target is not under ``research/`` raises rather than proceeding.
    """
    from opdi.utils.storage import StorageManager

    if getattr(StorageManager, "_redirected", False):
        return
    orig_read, orig_write = StorageManager.read_table, StorageManager.write_table
    orig_ref, orig_exists = StorageManager.table_ref, StorageManager.table_exists

    def read_table(self, table_name, *a, **kw):
        return orig_read(self, REDIRECT.get(table_name, table_name), *a, **kw)

    def table_ref(self, table_name, *a, **kw):
        # table_ref registers a temp view *named after the table*, and a
        # research path contains a slash, which is not a legal view name. So
        # read from the redirected path but register under a sanitised name.
        target = REDIRECT.get(table_name, table_name)
        if target == table_name or not self.use_s3:
            return orig_ref(self, target, *a, **kw)
        view = target.replace("/", "__").replace("-", "_")
        if view not in self._registered_views:
            self.spark.read.parquet(self._s3_path(target)).createOrReplaceTempView(view)
            self._registered_views.add(view)
        return view

    def table_exists(self, table_name, *a, **kw):
        return orig_exists(self, REDIRECT.get(table_name, table_name), *a, **kw)

    def write_table(self, df, table_name, *a, **kw):
        target = REDIRECT.get(table_name, table_name)
        if not target.startswith("research/"):
            raise RuntimeError(
                f"refusing to write to {target!r}. This experiment writes only "
                f"under research/; add {table_name!r} to REDIRECT."
            )
        print(f"  -> writing {target}")
        return orig_write(self, df, target, *a, **kw)

    StorageManager.read_table = read_table
    StorageManager.table_ref = table_ref
    StorageManager.table_exists = table_exists
    StorageManager.write_table = write_table
    StorageManager._redirected = True


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", nargs="+", default=["2025-06-05"])
    ap.add_argument("--month", default="202506")
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--executors", type=int, default=8)
    ap.add_argument("--ui-port", type=int, default=4041)
    ap.add_argument("--skip-build", action="store_true",
                    help="score only; assumes the bucket tables already exist")
    ap.add_argument("--skip-ingest", action="store_true",
                    help="reuse an existing research/sv_bucket from a previous run")
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
        # Separate processed-month logs. Sharing production's would let this
        # experiment mark 2025-06 as done and make a later production run skip
        # a month it never actually built.
        logs = Path("OPDI_live/logs/decimation_bucket")
        logs.mkdir(parents=True, exist_ok=True)

        if args.skip_ingest:
            print("\n=== 01 ingest skipped, reusing research/sv_bucket ===")
        else:
            print(f"\n=== 01 ingest, decimation=bucket, {days[0]} .. {days[-1]} ===")
            ing = StateVectorIngestion(
                spark, cfg, decimation=DECIMATION_BUCKET,
                log_file_path=str(logs / "01_statevectors.log"))
            n = ing.ingest_from_s3(
                days[0], date.fromordinal(days[-1].toordinal() + 1))
            print(f"  rows ingested: {n:,}")

        print("\n=== 02 tracks ===")
        TrackProcessor(
            spark, cfg, log_file_path=str(logs / "02_tracks.parquet")
        ).process_month(month, skip_if_processed=False)

        print("\n=== 03 endpoint candidates ===")
        FlightListProcessor(
            spark, cfg, log_dir=str(logs)
        ).build_endpoint_candidates(month, max_radius_nm=110.0, rebuild=True)

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
