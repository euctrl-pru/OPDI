"""
Build flight lists with the *actual pipeline* on each altitude datum, and score
them. v6.1's fork of flight_list_v6.py.

Forked rather than parameterised. `flight_list_v6.py` is fingerprinted by every
pipeline job in `regenerate_v6.py`, so adding a flag to it would mark a
published paper's figures stale for a change that paper never made -- the same
reason `flight_list_v7.py` exists beside it.

Trimmed as well as forked. V6's version reads its parameters out of sweep CSVs
by argmax, because V6's question was "what is the best setting". v6.1's question
is narrower -- "does the datum matter, and where" -- so the runs here are
stated, not derived, and the sweep arguments are gone. Nothing is transcribed
from a sweep, so nothing can be mistranscribed.

The runs:

``datum_msl`` / ``datum_field``
    Arm A. The shipped configuration on each datum, at the *same ceiling*:
    FL60 against 6,100 ft above field. 6,100 rather than 6,000 because
    `flight_level` is an integer cast, so FL60 admits everything below
    6,100 ft -- comparing against 6,000 would move the ceiling and the datum
    together and the arm is meant to move one thing.

``height_{cap}``
    Arm B through the pipeline itself: the above-field ceiling swept over the
    values the cached sweep nominates, so the shipped number is one the
    pipeline produced rather than one the harness modelled.

``legacy``
    The control that every other number is read against: pre-V6 constants on
    the sea-level datum, which is what published lists were built with.

Writes go to ``research/flight_list_v61_<run>``. A guard refuses any write
outside ``research/``, because ``process_dai``'s default table name is the
production flight list and a mistyped argument would otherwise overwrite it.

    python benchmarks/flight_list_v61.py --months 202506 \\
        --days 2025-06-05 2025-06-06 2025-06-07 \\
        --runs datum_msl datum_field --results-dir <dir>
"""

import argparse
import sys
from datetime import datetime
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
OUT_TMPL = "research/flight_list_v61_{run}"

#: The ceiling the datum arm holds constant across both arms, in feet.
#: Deliberately 6,100 and not 6,000 -- see the module docstring.
DATUM_ARM_CEILING_FT = 6100.0

#: Above-field ceilings to walk through the pipeline for Arm B.
DEFAULT_HEIGHT_GRID = [3000, 4000, 6100, 8000, 10000, 12000]


def guard_writes(allowed_prefix: str = "research/") -> None:
    """Refuse any write outside research/.

    ``process_dai(table_name=...)`` defaults to the published flight list, so a
    dropped argument writes over production. This makes that failure loud.
    """
    from opdi.utils.storage import StorageManager

    if getattr(StorageManager, "_v61_guarded", False):
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
    StorageManager._v61_guarded = True


def build(spark, cfg, month, run, detection, adep_mode, ades_mode, log_dir,
          tracks_table=None):
    """Run step 03 for one configuration and return the table name."""
    from opdi.pipeline.flights import FlightListProcessor

    cfg.detection = detection
    # Grid cells are scored and discarded, so they share one table rather than
    # leaving a prefix behind for every ceiling tried.
    table = OUT_TMPL.format(run="grid" if run.startswith("height_") else run)
    print(f"\n=== {run}: ADEP={adep_mode} ADES={ades_mode} ===")
    print(f"    datum={detection.trend_max_datum} "
          f"height_ft={detection.trend_max_height_ft:g} fl={detection.trend_max_fl}")

    proc = FlightListProcessor(spark, cfg, log_dir=str(log_dir),
                               tracks_table=tracks_table)
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


def make_plan(args):
    """The runs this study scores, as {name: (config, adep_mode, ades_mode)}.

    Every configuration starts from ``DetectionConfig()`` -- the shipped
    defaults -- and changes only the datum and the ceiling. Assembling one field
    by field is how V6 once produced a row labelled "recommended" that silently
    kept an old ranking rule; starting from the shipped object makes that
    impossible.
    """
    from opdi.config import DetectionConfig
    import dataclasses

    shipped = DetectionConfig()
    plan = {
        # Arm A: one variable. Same ceiling, same everything else, two datums.
        "datum_msl": (
            dataclasses.replace(shipped, trend_max_datum="msl", trend_max_fl=60),
            shipped.adep_mode, shipped.ades_mode,
        ),
        "datum_field": (
            dataclasses.replace(shipped, trend_max_datum="field",
                                trend_max_height_ft=DATUM_ARM_CEILING_FT),
            shipped.adep_mode, shipped.ades_mode,
        ),
        # The control: what published lists were built with.
        "legacy": (DetectionConfig.legacy(), "trend", "trend"),
        # Literally the shipped defaults, whatever they currently are.
        "recommended": (shipped, shipped.adep_mode, shipped.ades_mode),
    }

    # Arm B through the pipeline.
    for cap in args.height_grid:
        plan[f"height_{cap}"] = (
            dataclasses.replace(shipped, trend_max_datum="field",
                                trend_max_height_ft=float(cap)),
            shipped.adep_mode, shipped.ades_mode,
        )
    return plan


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--months", nargs="+", default=["202506"])
    ap.add_argument("--days", nargs="+",
                    default=["2025-06-05", "2025-06-06", "2025-06-07"])
    ap.add_argument("--results-dir", type=Path, required=True)
    ap.add_argument("--runs", nargs="+", default=["datum_msl", "datum_field"])
    ap.add_argument("--k", type=float, default=2.0,
                    help="wrong-answer penalty used in the score")
    ap.add_argument("--height-grid", nargs="+", type=int,
                    default=DEFAULT_HEIGHT_GRID,
                    help="above-field ceilings in feet for the height_* runs")
    ap.add_argument("--tracks", default=None,
                    help="track table to read; use research/tracks for 2024")
    ap.add_argument("--pipeline-logs", type=Path,
                    default=REPO / "OPDI_live" / "logs",
                    help="where the pipeline keeps its progress logs; the "
                         "endpoint candidate log is read from here so the "
                         "existing cache is reused rather than rebuilt")
    ap.add_argument("--executors", type=int, default=10)
    ap.add_argument("--cores", type=int, default=6)
    ap.add_argument("--driver-memory", default="8g")
    ap.add_argument("--ui-port", type=int, default=4045)
    ap.add_argument("--out-name", default="datum_comparison.csv")
    args = ap.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
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

    plan = make_plan(args)
    unknown = set(args.runs) - set(plan)
    if unknown:
        raise SystemExit(
            f"unknown run(s): {sorted(unknown)}. Known: {sorted(plan)}"
        )

    load_dotenv()
    osn_sample.RESEARCH_EXECUTORS = args.executors
    osn_sample.UI_PORT = args.ui_port
    spark = build_spark(args.cores, args.driver_memory, distributed=True)
    guard_writes()

    from opdi.config import OPDIConfig

    # The factory, not OPDIConfig() plus an attribute: the environment decides
    # the storage backend (plain parquet over S3A, no Hive, no Iceberg), and
    # setting the field afterwards leaves the StorageManager unwired, so every
    # table looks absent.
    cfg = OPDIConfig.for_environment("opensky")
    month = datetime.strptime(args.months[0], "%Y%m").date().replace(day=1)

    # Restricted to the days actually ingested. Without this the ground truth
    # spans the whole month while the flight lists cover three days, so every
    # flight on the other twenty-seven counts as an abstention and coverage
    # comes out roughly ten times too low.
    gt = label_ground_truth(
        load_ground_truth(spark, args.months, args.days), airport_locations(spark)
    )
    types = airport_types(spark)

    rows, per_apt, per_type = [], [], []
    for run in args.runs:
        detection, adep_mode, ades_mode = plan[run]
        table = build(spark, cfg, month, run, detection, adep_mode, ades_mode,
                      log_dir, tracks_table=args.tracks)

        pred, ident = load_predictions(spark, table)
        m = score(pred, ident, gt, k=args.k)
        m.update(run=run, adep_mode=adep_mode, ades_mode=ades_mode,
                 period=args.months[0],
                 trend_max_datum=detection.trend_max_datum,
                 trend_max_height_ft=detection.trend_max_height_ft,
                 trend_max_fl=detection.trend_max_fl,
                 trend_radius_nm=detection.trend_radius_nm,
                 trend_vote_margin=detection.trend_vote_margin,
                 trend_sched_penalty_nm=detection.trend_sched_penalty_nm,
                 trend_rank_by=getattr(detection, "trend_rank_by", "ring"),
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
            per_apt.append({**r.asDict(), "run": run, "period": args.months[0]})
        for r in per_type_counts(pred, ident, gt, types).collect():
            per_type.append({**r.asDict(), "run": run, "period": args.months[0]})

    import pandas as pd

    pd.DataFrame(rows).to_csv(out / args.out_name, index=False)
    pd.DataFrame(per_apt).to_csv(out / "per_airport_v61.csv", index=False)
    pd.DataFrame(per_type).to_csv(out / "per_type_v61.csv", index=False)
    print(f"\nwritten to {out}")
    spark.stop()


if __name__ == "__main__":
    main()
