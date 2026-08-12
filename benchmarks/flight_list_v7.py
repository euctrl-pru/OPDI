"""
Build flight lists with the actual pipeline, and score them. Version 7.

Every number in the V7 report that describes what OPDI *produces* comes from
here: this calls ``FlightListProcessor.process_dai`` -- the code path that
writes the published flight list -- and scores what it wrote against EUROCONTROL
Network Manager movement data.

Three things distinguish it from the V6 version it replaces.

**Configurations are named in full, never inherited.** V6 built its
"recommended" run by starting from ``legacy()`` and overriding the fields the
author remembered, and forgot the ranking rule -- so the row labelled
*recommended* silently kept the old behaviour and scored below the baseline it
was meant to beat. Here every run states every switch it turns on, and
``verify_plan`` asserts that the run named ``shipped`` is field-for-field
``DetectionConfig()``. A configuration assembled by hand can disagree with the
one that ships; this one cannot.

**Both periods run through the pipeline, not just the sweep.** V6 measured its
second period in the research harness only. ``--period 2024`` points the same
detection code at the second period's tracks through ``tracks_table``, so
"does it hold on another sample" is answered by the thing that ships rather
than by a model of it.

**The ladder is cumulative and says so.** Each ``L*`` run adds one change to
the one above and keeps it, so the marginal worth of each change is the
difference between adjacent rows. That is a different kind of table from the
mode comparison, where each row is a whole alternative, and V7 never draws them
the same way.

    python benchmarks/flight_list_v7.py --period 2025 --runs ladder \\
        --results-dir <dir>
"""

import argparse
import dataclasses
import sys
from datetime import date, datetime
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))
sys.path.insert(0, str(REPO / "src"))

from pyspark.sql import functions as F

import osn_sample
from osn_sample import build_spark, load_dotenv
from adep_ades import (
    airport_locations, airport_types, label_ground_truth, load_ground_truth,
    score, per_airport_counts, per_type_counts,
)

#: Where flight lists land. Under ``research/`` and suffixed, so the published
#: flight list is never the target -- and ``guard_writes`` enforces it rather
#: than trusting the template.
OUT_TMPL = "research/flight_list_v7_{run}"

#: The two samples. Each names its own tracks, so a run cannot silently mix a
#: period's ground truth with another period's data -- which is how the V6
#: decimation comparison came to report a sampler collapsing coverage by fifty
#: points when the two arms had drifted onto different periods.
PERIODS = {
    "2025": {
        "months": ["202506"],
        "days": ["2025-06-05", "2025-06-06", "2025-06-07"],
        # None means "resolve from the config" -- which is `osn_tracks_clean`
        # when cleaning feeds the flight list, and the point of V7.
        "tracks": None,
        "raw_tracks": "osn_tracks",
    },
    "2024": {
        "months": ["202406"],
        "days": ["2024-06-05", "2024-06-06", "2024-06-07"],
        "tracks": "research/tracks_clean",
        "raw_tracks": "research/tracks",
    },
}


def guard_writes(allowed_prefix: str = "research/") -> None:
    """Refuse any write outside ``research/``.

    ``process_dai``'s default table name is the published flight list, so a
    dropped argument overwrites production. This makes that failure loud
    instead of silent and irreversible.
    """
    from opdi.utils.storage import StorageManager

    if getattr(StorageManager, "_v7_guarded", False):
        return
    orig_write = StorageManager.write_table

    def write_table(self, df, table_name, *a, **kw):
        if not str(table_name).startswith(allowed_prefix):
            raise RuntimeError(
                f"refusing to write {table_name!r}: this benchmark writes only "
                f"under {allowed_prefix!r}")
        print(f"  -> writing {table_name}")
        return orig_write(self, df, table_name, *a, **kw)

    StorageManager.write_table = write_table
    StorageManager._v7_guarded = True


# ---------------------------------------------------------------------------
# The plan
# ---------------------------------------------------------------------------


def build_plan(args) -> dict:
    """Every run this module can build: name -> (detection, adep, ades).

    Written as one function returning one dict so the whole experiment is
    readable in one place. Nothing here reads a sweep CSV: V6 derived its
    "tuned" configuration from an argmax at run time, which made the run
    depend on a file that could be regenerated underneath it. The values these
    runs use are the ones in ``DetectionConfig``, and the sweeps' job is to
    justify those rather than to supply them.
    """
    from opdi.config import DetectionConfig

    L = DetectionConfig.legacy
    S = DetectionConfig

    def frm(base, **kw):
        """A config differing from *base* in exactly the named fields."""
        return dataclasses.replace(base(), **kw)

    # --- the cumulative ladder ------------------------------------------
    # Order is causal, not alphabetical. The three geometry fixes come first
    # because they decide what the thresholds are even measuring: under ring
    # selection a tuned flight-level cap loses ground, and under exact distance
    # the same value gains it. Tuning first and fixing the geometry afterwards
    # would attribute the whole difference to the last step applied.
    steps = [
        ("L0_legacy", {}),
        ("L1_exact_rank", dict(trend_rank_by="haversine")),
        ("L2_exact_radius", dict(trend_radius_exact=True)),
        ("L3_smooth_first", dict(trend_smooth_before_cut=True)),
        ("L4_penalty", dict(trend_sched_penalty_nm=S().trend_sched_penalty_nm)),
        ("L5_flcap", dict(trend_max_fl=S().trend_max_fl)),
        ("L6_margin", dict(trend_vote_margin=S().trend_vote_margin)),
        ("L7_radius", dict(trend_radius_nm=S().trend_radius_nm)),
        ("L8_bearing",
         dict(trend_bearing_tiebreak_nm=S().trend_bearing_tiebreak_nm)),
        ("L9_ooa", dict(trend_ooa=True)),
    ]
    ladder, acc = {}, {}
    for name, delta in steps:
        acc = {**acc, **delta}
        # Both roles from `trend`: this ladder is about the trend algorithm,
        # and mixing in `endpoint` would make each row's change unattributable.
        ladder[name] = (frm(L, **acc), "trend", "trend")

    # --- whole configurations, to choose between -------------------------
    modes = {
        "legacy": (L(), L().adep_mode, L().ades_mode),
        "trend": (S(), "trend", "trend"),
        "endpoint": (S(), "endpoint", "endpoint"),
        "nearest": (S(), "nearest", "nearest"),
        "shipped": (S(), S().adep_mode, S().ades_mode),
    }

    # --- what cleaning costs and buys ------------------------------------
    # The same configuration over the two track tables. It is a *run* rather
    # than a flag because the comparison is the measurement: cleaning masks
    # implausible values to NULL and the detection path drops samples with no
    # barometric altitude, so it can only remove candidate samples.
    cleaning = {
        "clean_tracks": (S(), S().adep_mode, S().ades_mode),
        "raw_tracks": (S(), S().adep_mode, S().ades_mode),
    }

    # --- the flight-level grid -------------------------------------------
    # Closed at the top this time. V6 stopped at FL120 with departures still
    # rising, which read as "higher is always better" when the curve had simply
    # not been followed far enough.
    grid = {}
    for fl in args.grid_fl:
        for rd in args.grid_radius:
            grid[f"grid_fl{fl}_r{rd:g}"] = (
                frm(S, trend_max_fl=int(fl), trend_radius_nm=float(rd)),
                "trend", "trend")

    return {**ladder, **modes, **cleaning, **grid}


def verify_plan(plan: dict) -> None:
    """Assert the plan says what it means, before anything expensive runs.

    Two checks, both of which would have caught a defect this study actually
    shipped:

    * ``shipped`` must be exactly ``DetectionConfig()``. V6's equivalent was
      assembled by hand, missed one field, and scored below the baseline it
      claimed to beat -- and nothing said so.
    * every ladder rung must differ from the one below it. A rung that changes
      nothing scores identically to its predecessor, which reads as "this
      parameter does not matter" and means "this parameter is not reaching the
      code". That is exactly how the inert scheduled-service penalty hid.
    """
    from opdi.config import DetectionConfig

    shipped = plan["shipped"][0]
    if shipped != DetectionConfig():
        diff = [f.name for f in dataclasses.fields(DetectionConfig)
                if getattr(shipped, f.name) != getattr(DetectionConfig(), f.name)]
        raise SystemExit(
            f"the run named 'shipped' is not DetectionConfig(): {diff}. "
            f"Every figure attributed to the shipped configuration would "
            f"describe something else.")

    rungs = sorted(k for k in plan if k.startswith("L") and k[1:2].isdigit())
    for lo, hi in zip(rungs, rungs[1:]):
        if plan[lo][0] == plan[hi][0]:
            raise SystemExit(
                f"ladder rungs {lo} and {hi} are the same configuration. A "
                f"step that changes nothing is not a measurement of that step.")
    print(f"plan verified: {len(plan)} runs, {len(rungs)} ladder rungs")


# ---------------------------------------------------------------------------
# Running and scoring
# ---------------------------------------------------------------------------


def build(spark, cfg, month, run, detection, adep_mode, ades_mode, log_dir,
          tracks_table):
    """Run step 03 for one configuration and return the table it wrote."""
    from opdi.pipeline.flights import FlightListProcessor

    cfg.detection = detection
    # Grid cells are scored and discarded, so they share one table rather than
    # leaving a prefix behind for each of twenty-eight combinations.
    table = OUT_TMPL.format(run="grid" if run.startswith("grid_") else run)

    proc = FlightListProcessor(spark, cfg, log_dir=str(log_dir),
                               tracks_table=tracks_table)
    print(f"\n=== {run}: ADEP={adep_mode} ADES={ades_mode} "
          f"tracks={proc.tracks_table} ===")
    print(f"    {detection}")
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
        # Trimmed: ADS-B callsigns are space-padded to eight characters and the
        # reference callsign is not, so an untrimmed join matches nothing --
        # which shows up as every run scoring 0.00%, not as an error.
        F.trim(F.col("FLT_ID")).alias("callsign"),
        F.to_date(F.col("FIRST_SEEN")).alias("day"),
        F.col("FIRST_SEEN").alias("t_start"),
    )
    return pred, ident


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--period", choices=sorted(PERIODS), default="2025")
    ap.add_argument("--runs", nargs="+", required=True,
                    help="run names, or the groups 'ladder', 'modes', "
                         "'cleaning', 'grid', 'all'")
    ap.add_argument("--results-dir", type=Path, required=True)
    ap.add_argument("--out-name", default=None,
                    help="CSV basename; defaults to mode_comparison_v7.csv")
    ap.add_argument("--k", type=float, default=2.0,
                    help="wrong-answer weight in score = correct - k * wrong")
    ap.add_argument("--per-airport", action="store_true",
                    help="also emit per-aerodrome and per-type counts")
    ap.add_argument("--pipeline-logs", type=Path,
                    default=REPO / "OPDI_live" / "logs")
    # FL25..FL300 in steps of 25, plus the legacy 40 and the shipped 60 so both
    # stay on the grid rather than being interpolated between neighbours.
    ap.add_argument("--grid-fl", nargs="+", type=int,
                    default=sorted({40, 60} | set(range(25, 301, 25))))
    ap.add_argument("--grid-radius", nargs="+", type=float, default=[20.0, 30.0])
    ap.add_argument("--executors", type=int, default=10)
    ap.add_argument("--cores", type=int, default=6)
    ap.add_argument("--driver-memory", default="8g")
    ap.add_argument("--ui-port", type=int, default=4057)
    ap.add_argument("--dry-run", action="store_true",
                    help="print the plan and exit; no cluster needed")
    args = ap.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
    period = PERIODS[args.period]

    plan = build_plan(args)
    verify_plan(plan)

    groups = {
        "ladder": [k for k in plan if k.startswith("L") and k[1:2].isdigit()],
        "modes": ["legacy", "trend", "endpoint", "nearest", "shipped"],
        "cleaning": ["clean_tracks", "raw_tracks"],
        "grid": [k for k in plan if k.startswith("grid_")],
    }
    groups["all"] = groups["ladder"] + groups["modes"] + groups["cleaning"]

    runs, seen = [], set()
    for name in args.runs:
        for r in groups.get(name, [name]):
            if r not in seen:
                seen.add(r)
                runs.append(r)
    unknown = [r for r in runs if r not in plan]
    if unknown:
        raise SystemExit(f"unknown run(s): {unknown}")

    print(f"period {args.period}: {period['days'][0]} .. {period['days'][-1]}")
    print(f"runs ({len(runs)}): {', '.join(runs)}")
    if args.dry_run:
        for r in runs:
            d, a, s = plan[r]
            print(f"  {r:<20} ADEP={a:<9} ADES={s:<9} FL{d.trend_max_fl} "
                  f"r{d.trend_radius_nm:g} m{d.trend_vote_margin} "
                  f"pen{d.trend_sched_penalty_nm:g} rank={d.trend_rank_by} "
                  f"exact_r={d.trend_radius_exact} smooth1st="
                  f"{d.trend_smooth_before_cut} tie={d.trend_bearing_tiebreak_nm:g} "
                  f"ooa={d.trend_ooa}")
        return

    out = args.results_dir
    out.mkdir(parents=True, exist_ok=True)

    # The pipeline decides whether to rebuild the endpoint candidate cache from
    # a *local* progress log, so a fresh log directory reads as "not built" and
    # triggers a rebuild the write guard would refuse. Copying that one log in
    # makes the existing cache visible; the DAI log is deliberately left out so
    # this run records its own progress rather than appending to production's.
    log_dir = out / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    cand_log = "03_osn-endpoint_candidates-etl-log.parquet"
    src_log = args.pipeline_logs / cand_log
    if src_log.exists() and not (log_dir / cand_log).exists():
        import shutil
        (shutil.copytree if src_log.is_dir() else shutil.copy2)(
            src_log, log_dir / cand_log)
        print(f"reusing endpoint candidate cache per {src_log}")

    load_dotenv()
    osn_sample.RESEARCH_EXECUTORS = args.executors
    osn_sample.UI_PORT = args.ui_port
    spark = build_spark(args.cores, args.driver_memory, distributed=True)
    spark.sparkContext.setLogLevel("ERROR")
    guard_writes()

    from opdi.config import OPDIConfig

    # The factory, not OPDIConfig() plus an attribute: the environment decides
    # the storage backend, and setting the field afterwards leaves the
    # StorageManager unwired, so every table looks absent.
    cfg = OPDIConfig.for_environment("opensky")
    month = datetime.strptime(period["months"][0], "%Y%m").date().replace(day=1)

    # Restricted to the days actually ingested. Without this the reference
    # spans the whole month while the flight lists cover three days, so every
    # flight on the other twenty-seven counts as an abstention and coverage
    # comes out about ten times too low.
    gt = label_ground_truth(
        load_ground_truth(spark, period["months"], period["days"]),
        airport_locations(spark))
    n_gt = gt.count()
    print(f"ground-truth flights: {n_gt:,}")
    if n_gt == 0:
        raise SystemExit(
            "no ground-truth flights for this period. Every run would score "
            "zero coverage, which is not a result -- check the reference data "
            "covers these days.")
    types = airport_types(spark) if args.per_airport else None

    rows, per_apt, per_type = [], [], []
    for run in runs:
        detection, adep_mode, ades_mode = plan[run]
        tracks = period["raw_tracks"] if run == "raw_tracks" else period["tracks"]
        table = build(spark, cfg, month, run, detection, adep_mode, ades_mode,
                      log_dir, tracks)

        pred, ident = load_predictions(spark, table)
        m = score(pred, ident, gt, k=args.k)
        if m["adep_coverage"] == 0 and m["ades_coverage"] == 0:
            raise SystemExit(
                f"{run!r} scored zero coverage on both roles. With "
                f"{n_gt:,} reference flights that means the identity join "
                f"matched nothing, not that detection failed. Check icao24 "
                f"case and callsign padding before reading anything into it.")
        m.update(run=run, period=args.period, adep_mode=adep_mode,
                 ades_mode=ades_mode, tracks_table=tracks or "config",
                 **{f.name: getattr(detection, f.name)
                    for f in dataclasses.fields(detection)})
        rows.append(m)
        print(f"  ADEP cov={m['adep_coverage']:.2%} acc={m['adep_accuracy']:.2%} "
              f"correct={m['adep_correct']:,} wrong={m['adep_wrong']:,} "
              f"score={m['adep_score']:,.0f}")
        print(f"  ADES cov={m['ades_coverage']:.2%} acc={m['ades_accuracy']:.2%} "
              f"correct={m['ades_correct']:,} wrong={m['ades_wrong']:,} "
              f"score={m['ades_score']:,.0f}")

        if args.per_airport:
            for r in per_airport_counts(pred, ident, gt).collect():
                per_apt.append({**r.asDict(), "run": run, "period": args.period})
            for r in per_type_counts(pred, ident, gt, types).collect():
                per_type.append({**r.asDict(), "run": run, "period": args.period})

    import pandas as pd

    name = args.out_name or "mode_comparison_v7.csv"
    pd.DataFrame(rows).to_csv(out / name, index=False)
    if args.per_airport:
        pd.DataFrame(per_apt).to_csv(out / "per_airport_v7.csv", index=False)
        pd.DataFrame(per_type).to_csv(out / "per_type_v7.csv", index=False)
    print(f"\nwritten to {out}")
    spark.stop()


if __name__ == "__main__":
    main()
