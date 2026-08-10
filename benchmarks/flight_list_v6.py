"""
Build flight lists with the *actual pipeline* at the tuned parameters, and
score them.

Everything else in this directory sweeps a cached artefact: the trend sweep
re-implements the vote rule over a cached vote table, and the endpoint sweeps
filter and re-rank a cached candidate table. That is what makes hundreds of
parameter cells affordable, but it also means no swept number has ever been
produced by ``src/opdi`` itself. A recommendation to change production defaults
cannot rest on a reimplementation, so this module closes the loop: it calls
``FlightListProcessor.process_dai`` and scores what the pipeline wrote.

Two things follow from that, and both are the point of the exercise:

* **The tuned parameters are read out of the sweep CSVs, not typed in here.**
  The argmax is recomputed from the committed sweep output, so the flight list
  is built at provably the same setting the sweep recommended. A transcription
  error becomes impossible rather than unlikely.
* **The residual gap is expected to be non-zero.** The benchmark selects an
  aerodrome by exact haversine and filters zones by exact radius; the pipeline
  selects by H3 ring count (``distance_from_center``, an integer ~5.2 km step
  at resolution 7) and filters zones by radius *band*. The benchmark also
  smooths altitude across the flight-level boundary and the pipeline does not.
  The gap those produce is the fidelity of every tuned number in the report, so
  it is measured and stated rather than absorbed.

Writes go to ``research/flight_list_v6_<run>``. A guard refuses any write
outside ``research/``, because ``process_dai``'s default table name is the
production flight list and a mistyped argument would otherwise overwrite it.

    python benchmarks/flight_list_v6.py --months 202506 \\
        --days 2025-06-05 2025-06-06 2025-06-07 \\
        --trend-sweep <dir>/trend_sweep.csv \\
        --endpoint-sweep <dir>/sweep_radius_height.csv \\
        --results-dir <dir>
"""

import argparse
import csv
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

#: Where the pipeline's flight lists land. Suffixed and under research/, so the
#: published flight list is never the target.
OUT_TMPL = "research/flight_list_v6_{run}"

#: Modes to build. `legacy` is the control: the pre-V6 constants, which is what
#: every published OPDI flight list was built with. Without it the report would
#: have no measured baseline of its own to compare the tuned settings against.
RUNS = ("legacy", "trend", "endpoint", "nearest", "combined")


def counts(row: dict, side: str) -> tuple:
    """(correct, wrong) from a sweep row.

    Older sweep CSVs carry ratios only. The counts are exactly recoverable --
    coverage * n is the number answered and overall * n the number correct,
    both integers -- so this works whether or not the row has the columns.
    """
    if f"{side}_correct" in row and row[f"{side}_correct"] != "":
        return int(float(row[f"{side}_correct"])), int(float(row[f"{side}_wrong"]))
    n = int(float(row["n_ground_truth"]))
    correct = round(float(row[f"{side}_overall"]) * n)
    return correct, round(float(row[f"{side}_coverage"]) * n) - correct


def argmax(path: Path, side: str, k: float) -> dict:
    """The best-scoring row of a sweep for one role, at penalty k."""
    rows = list(csv.DictReader(open(path)))
    best = max(rows, key=lambda r: (lambda c: c[0] - k * c[1])(counts(r, side)))
    correct, wrong = counts(best, side)
    best["_correct"], best["_wrong"] = correct, wrong
    best["_score"] = correct - k * wrong
    return best


def guard_writes(allowed_prefix: str = "research/") -> None:
    """Refuse any write outside research/.

    ``process_dai(table_name=...)`` defaults to the published flight list, so a
    dropped argument writes over production. This makes that failure loud.
    """
    from opdi.utils.storage import StorageManager

    if getattr(StorageManager, "_v6_guarded", False):
        return
    orig_write = StorageManager.write_table

    def write_table(self, df, table_name, *a, **kw):
        if not str(table_name).startswith(allowed_prefix):
            raise RuntimeError(
                f"refusing to write {table_name!r}: this benchmark writes only "
                f"under {allowed_prefix!r}"
            )
        print(f"  -> writing {table_name}")
        return orig_write(self, df, table_name, *a, **kw)

    StorageManager.write_table = write_table
    StorageManager._v6_guarded = True


def build(spark, cfg, month, run, detection, adep_mode, ades_mode, log_dir):
    """Run step 03 for one configuration and return the table name."""
    from opdi.pipeline.flights import FlightListProcessor

    cfg.detection = detection
    table = OUT_TMPL.format(run=run)
    print(f"\n=== {run}: ADEP={adep_mode} ADES={ades_mode} ===")
    print(f"    {detection}")

    proc = FlightListProcessor(spark, cfg, log_dir=str(log_dir))
    proc.process_dai(
        month=month,
        skip_if_processed=False,
        adep_mode=adep_mode,
        ades_mode=ades_mode,
        abstention_radius_nm=detection.endpoint_radius_nm,
        abstention_height_ft=detection.endpoint_height_ft,
        sched_penalty_nm=detection.endpoint_sched_penalty_nm,
        table_name=table,
        write_mode="overwrite",
    )
    return table


def load_predictions(spark, table):
    """(predictions, identities) from a flight list this run just wrote."""
    fl = spark.read.parquet(f"s3a://eurocontrol/opdi/{table}")
    pred = fl.select(
        F.col("ID").alias("track_id"),
        F.col("ADEP").alias("adep"),
        F.col("ADES").alias("ades"),
    )
    ident = fl.select(
        F.col("ID").alias("track_id"),
        F.lower(F.col("ICAO24")).alias("icao24"),
        F.trim(F.col("FLT_ID")).alias("callsign"),
        F.to_date(F.col("FIRST_SEEN")).alias("day"),
        F.col("FIRST_SEEN").alias("t_start"),
    )
    return pred, ident


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--months", nargs="+", default=["202506"])
    ap.add_argument("--days", nargs="+",
                    default=["2025-06-05", "2025-06-06", "2025-06-07"])
    ap.add_argument("--trend-sweep", type=Path, required=True)
    ap.add_argument("--endpoint-sweep", type=Path, required=True)
    ap.add_argument("--results-dir", type=Path, required=True)
    ap.add_argument("--runs", nargs="+", default=list(RUNS))
    ap.add_argument("--k", type=float, default=2.0,
                    help="wrong-answer penalty used to pick the argmax")
    ap.add_argument("--pipeline-logs", type=Path,
                    default=REPO / "OPDI_live" / "logs",
                    help="where the pipeline keeps its progress logs; the "
                         "endpoint candidate log is read from here so the "
                         "existing cache is reused rather than rebuilt")
    ap.add_argument("--executors", type=int, default=10)
    ap.add_argument("--ui-port", type=int, default=4044)
    args = ap.parse_args()

    out = args.results_dir
    out.mkdir(parents=True, exist_ok=True)

    # The pipeline decides whether to rebuild the endpoint candidate cache from
    # a *local* progress log. A fresh log directory therefore means "not built"
    # and triggers a rebuild -- which the write guard would refuse, since the
    # cache does not live under research/. Copying that one log in makes the
    # existing cache visible; the DAI log is deliberately left out, so this run
    # records its own progress rather than appending to the pipeline's.
    log_dir = out / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    cand_log = "03_osn-endpoint_candidates-etl-log.parquet"
    src_log = args.pipeline_logs / cand_log
    if src_log.exists() and not (log_dir / cand_log).exists():
        import shutil

        shutil.copytree(src_log, log_dir / cand_log) if src_log.is_dir() else \
            shutil.copy2(src_log, log_dir / cand_log)
        print(f"reusing endpoint candidate cache per {src_log}")
    elif not src_log.exists():
        print(f"WARNING: no candidate log at {src_log}; endpoint modes will try "
              f"to rebuild the cache and the write guard will refuse")

    from opdi.config import DetectionConfig

    # ---- the tuned parameters, read out of the sweeps ---------------------
    t_adep = argmax(args.trend_sweep, "adep", args.k)
    t_ades = argmax(args.trend_sweep, "ades", args.k)
    e_adep = argmax(args.endpoint_sweep, "adep", args.k)
    e_ades = argmax(args.endpoint_sweep, "ades", args.k)

    print(f"=== tuned parameters, argmax at k={args.k:g} ===")
    for name, r, kind in (("trend ADEP", t_adep, "t"), ("trend ADES", t_ades, "t"),
                          ("endpoint ADEP", e_adep, "e"), ("endpoint ADES", e_ades, "e")):
        if kind == "t":
            desc = (f"FL{r['fl_cap']} margin {r['margin']} "
                    f"radius {float(r['radius_nm']):g} NM pen {float(r['penalty_nm']):g} NM")
        else:
            desc = (f"radius {float(r['radius_nm']):g} NM "
                    f"height {float(r['height_ft']):g} ft pen {float(r['penalty_nm']):g} NM")
        print(f"  {name:<14} {desc:<58} "
              f"correct={r['_correct']:>7,} wrong={r['_wrong']:>6,} score={r['_score']:>9,.0f}")

    # Whichever method scores higher takes that role in the combined run. This
    # is the report's headline recommendation, and it is decided by the sweeps
    # rather than asserted here.
    adep_method = "endpoint" if e_adep["_score"] >= t_adep["_score"] else "trend"
    ades_method = "endpoint" if e_ades["_score"] >= t_ades["_score"] else "trend"
    print(f"\n  combined: ADEP={adep_method}, ADES={ades_method}")

    def tuned() -> "DetectionConfig":
        return DetectionConfig(
            trend_max_fl=int(t_ades["fl_cap"]),
            trend_radius_nm=float(t_ades["radius_nm"]),
            trend_smooth_half_window=DetectionConfig().trend_smooth_half_window,
            trend_vote_margin=int(t_ades["margin"]),
            trend_sched_penalty_nm=float(t_ades["penalty_nm"]),
            endpoint_radius_nm=float(e_adep["radius_nm"]),
            endpoint_height_ft=float(e_adep["height_ft"]),
            endpoint_sched_penalty_nm=float(e_adep["penalty_nm"]),
            endpoint_candidate_radius_nm=DetectionConfig().endpoint_candidate_radius_nm,
        )

    # A single-mode run is scored at *its own* role optimum, so each method is
    # seen at its best rather than at the other role's setting.
    def single(mode):
        d = tuned()
        if mode == "trend":
            d.trend_max_fl = int(t_adep["fl_cap"])
            d.trend_radius_nm = float(t_adep["radius_nm"])
            d.trend_vote_margin = int(t_adep["margin"])
            d.trend_sched_penalty_nm = float(t_adep["penalty_nm"])
        return d

    plan = {
        "legacy": (DetectionConfig.legacy(), "trend", "trend"),
        "trend": (single("trend"), "trend", "trend"),
        "endpoint": (tuned(), "endpoint", "endpoint"),
        "nearest": (tuned(), "nearest", "nearest"),
        "combined": (tuned(), adep_method, ades_method),
    }

    load_dotenv()
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(args.ui_port)
    guard_writes()

    from opdi.config import OPDIConfig

    cfg = OPDIConfig()
    cfg.project.environment = "opensky"
    month = datetime.strptime(args.months[0], "%Y%m").date().replace(day=1)

    days = [datetime.strptime(d, "%Y-%m-%d").date() for d in args.days]
    gt = load_ground_truth(spark, args.months)
    apt = airport_locations(spark)
    gt = label_ground_truth(gt, apt)
    types = airport_types(spark)

    rows, per_apt, per_type = [], [], []
    for run in args.runs:
        detection, adep_mode, ades_mode = plan[run]
        table = build(spark, cfg, month, run, detection, adep_mode, ades_mode, log_dir)

        pred, ident = load_predictions(spark, table)
        m = score(pred, ident, gt, k=args.k)
        m.update(run=run, adep_mode=adep_mode, ades_mode=ades_mode,
                 trend_max_fl=detection.trend_max_fl,
                 trend_radius_nm=detection.trend_radius_nm,
                 trend_vote_margin=detection.trend_vote_margin,
                 trend_sched_penalty_nm=detection.trend_sched_penalty_nm,
                 endpoint_radius_nm=detection.endpoint_radius_nm,
                 endpoint_height_ft=detection.endpoint_height_ft,
                 endpoint_sched_penalty_nm=detection.endpoint_sched_penalty_nm)
        rows.append(m)
        print(f"  ADEP cov={m['adep_coverage']:.2%} acc={m['adep_accuracy']:.2%} "
              f"correct={m['adep_correct']:,} wrong={m['adep_wrong']:,} "
              f"score={m['adep_score']:,.0f}")
        print(f"  ADES cov={m['ades_coverage']:.2%} acc={m['ades_accuracy']:.2%} "
              f"correct={m['ades_correct']:,} wrong={m['ades_wrong']:,} "
              f"score={m['ades_score']:,.0f}")

        for r in per_airport_counts(pred, ident, gt).collect():
            per_apt.append({**r.asDict(), "run": run})
        for r in per_type_counts(pred, ident, gt, types).collect():
            per_type.append({**r.asDict(), "run": run})

    import pandas as pd

    pd.DataFrame(rows).to_csv(out / "mode_comparison_v6.csv", index=False)
    pd.DataFrame(per_apt).to_csv(out / "per_airport_v6.csv", index=False)
    pd.DataFrame(per_type).to_csv(out / "per_type_v6.csv", index=False)
    print(f"\nwritten to {out}")
    spark.stop()


if __name__ == "__main__":
    main()
