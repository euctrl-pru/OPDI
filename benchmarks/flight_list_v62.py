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

V6.2 differs from V6 in one respect: the trend altitude cut is a **height above
the aerodrome's own field elevation**, which is what ships. Two consequences run
through this module:

* **The ceiling is read from the sweep row's own datum**, never assumed to be a
  flight level. `trend_ceiling_kwargs` returns only the keys that apply, so a
  config cannot carry an above-field ceiling and a stale flight level at once.
* **The path walk keeps its lower rungs at sea level.** Rungs 0-4 are tuned
  from ``--trend-sweep-msl`` and reproduce V6's ladder; rung 5 moves the datum
  and nothing else, at the exact height equivalent of rung 4's flight level. If
  the walk were tuned from the above-field sweep the datum would move at rung
  2, and rung 5 would report zero -- a null that looks like a finding.

Writes go to ``research/flight_list_v62_<run>``. A guard refuses any write
outside ``research/``, because ``process_dai``'s default table name is the
production flight list and a mistyped argument would otherwise overwrite it.

    python benchmarks/flight_list_v62.py --months 202506 \\
        --days 2025-06-05 2025-06-06 2025-06-07 \\
        --trend-sweep <dir>/height_sweep_2025.csv \\
        --trend-sweep-msl <dir>/fl_sweep_2025.csv \\
        --endpoint-sweep <dir>/sweep_radius_height_2025.csv \\
        --results-dir <dir>
"""

import argparse
import csv
import dataclasses
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
#: Namespaced away from V6's `flight_list_v6_*`. V6 is frozen but its tables
#: are still what it is reproducible against, and two studies writing the same
#: prefix would leave whichever ran last.
OUT_TMPL = "research/flight_list_v62_{run}"

#: Modes to build. `legacy` is the control: the pre-V6 constants, which is what
#: every published OPDI flight list was built with. Without it the report would
#: have no measured baseline of its own to compare the tuned settings against.
RUNS = ("legacy", "trend", "endpoint", "nearest", "combined", "recommended")

def msl_equivalent_height_ft(fl: int) -> float:
    """The above-field ceiling admitting the same band as ``flight_level <= fl``.

    ``flight_level`` is an integer cast, so the sea-level cut admits everything
    *below* ``(fl + 1) * 100`` ft -- FL60 reaches 6,100, not 6,000. Matching
    that exactly is what makes a datum comparison a one-variable change: same
    band, different reference surface. Comparing FL60 against a field-datum
    6,000 would move the ceiling and the datum together and measure neither.
    """
    return float((fl + 1) * 100)


#: The ceiling the datum arms hold constant, in feet: the height equivalent of
#: FL60, which is the sea-level optimum this study compares against.
DATUM_ARM_CEILING_FT = msl_equivalent_height_ft(60)

#: Ceilings walked through `process_dai` itself, in feet above field elevation.
#:
#: Carries both 6000 and 6100 deliberately. 6000 is what ships; 6100 is what the
#: sweep called optimal -- on a grid that never contained 6000. Until both are
#: run through the pipeline, "the ceiling is tuned" is a claim about a grid, not
#: about the shipped value.
GRID_HEIGHT_CAPS = (3000, 4000, 6000, 6100, 8000, 10000)


def trend_ceiling_kwargs(row: dict) -> dict:
    """Config keywords for the altitude cut a sweep row selects.

    Both sweeps carry the cap in `fl_cap`; `datum` says what the number means.
    On the field datum it is feet above field elevation, on MSL a flight level.

    Returns only the keys that apply, so a config can never carry two ceilings
    at once. That matters because `trend_altitude_cut` reads exactly one of
    them: a leftover `trend_max_fl` beside a field ceiling is inert on one code
    path and authoritative on the other, and nothing in the output says which
    ran.

    A row with no `datum` column is MSL -- that is V6's own CSV format, and
    reinterpreting FL60 as 60 ft above field would abstain on everything.
    """
    datum = row.get("datum") or "msl"
    if datum == "field":
        return {"trend_max_datum": "field",
                "trend_max_height_ft": float(row["fl_cap"])}
    if datum == "msl":
        return {"trend_max_datum": "msl", "trend_max_fl": int(row["fl_cap"])}
    raise ValueError(
        f"unknown datum {datum!r} in sweep row; expected 'field' or 'msl'. "
        f"Defaulting here would apply one altitude cut while reporting the other."
    )


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
    # Grid cells are scored and discarded, so they share one table rather than
    # leaving a prefix behind for every combination tried.
    table = OUT_TMPL.format(run="grid" if run.startswith("grid_") else run)
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
    ap.add_argument("--trend-sweep", type=Path, required=True,
                    help="the sweep the tuned arms are built from. On the "
                         "shipped datum this is the above-field sweep")
    ap.add_argument("--trend-sweep-msl", type=Path, default=None,
                    help="the sea-level sweep the path walk's rungs 0-4 use, "
                         "so the ladder reproduces V6's and the datum moves "
                         "only at rung 5. Defaults to --trend-sweep, which "
                         "then has to be a sea-level sweep itself")
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
    ap.add_argument("--grid-height", nargs="+", type=int,
                    default=list(GRID_HEIGHT_CAPS),
                    help="trend ceilings to walk through process_dai, in feet "
                         "above field elevation")
    ap.add_argument("--grid-radius", nargs="+", type=float, default=[20.0, 30.0])
    ap.add_argument("--grid-margin", nargs="+", type=int, default=[2])
    ap.add_argument("--grid-penalty", type=float, default=10.0)
    ap.add_argument("--trend-rank-by", default="haversine",
                    choices=["haversine", "ring"],
                    help="how trend chooses among candidate aerodromes")
    ap.add_argument("--cores", type=int, default=6)
    ap.add_argument("--driver-memory", default="8g")
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
            **trend_ceiling_kwargs(t_ades),
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
            # setattr rather than assignment: which ceiling field applies
            # depends on the sweep's datum, and naming one here would reinstate
            # exactly the coupling `trend_ceiling_kwargs` exists to remove.
            for k_, v_ in trend_ceiling_kwargs(t_adep).items():
                setattr(d, k_, v_)
            d.trend_radius_nm = float(t_adep["radius_nm"])
            d.trend_vote_margin = int(t_adep["margin"])
            d.trend_sched_penalty_nm = float(t_adep["penalty_nm"])
        return d

    # The harness path decomposition, re-walked through the pipeline itself.
    #
    # This exists because the two do not agree. `endpoint` reproduces exactly --
    # the sweep filters the same candidate cache the pipeline reads, so it is
    # the same computation -- but `trend` does not. The pipeline picks the
    # aerodrome at the minimum H3 *ring count* across all candidates before the
    # haversine tie-break runs, so a scheduled aerodrome one ring further out is
    # eliminated before the scheduled-service penalty can rescue it. A penalty
    # tuned against exact haversine therefore does something weaker, and
    # possibly something different, in production.
    #
    # So each step of the path is run through process_dai and scored, which
    # answers directly which tuned values survive contact with the pipeline.
    def path_cfg(**kw):
        d = DetectionConfig.legacy()
        # The ranking rule is the base the path is walked on, not a step in it.
        # Walking the thresholds on `ring` measures how far a tuned value can
        # get while the selection rule throws candidates away unmeasured; on
        # `haversine` it measures what the value is actually worth.
        d.trend_rank_by = args.trend_rank_by
        for k_, v_ in kw.items():
            setattr(d, k_, v_)
        return d

    # The ladder walks the SEA-LEVEL tuning for rungs 0-4 and moves the datum
    # only at rung 5.
    #
    # This is why the MSL sweep is read here rather than `args.trend_sweep`:
    # the pipeline arms are tuned against the *field* sweep, so passing that
    # through `trend_ceiling_kwargs` would set `trend_max_datum="field"` at
    # rung 2. The datum would then already have moved four rungs before the
    # rung that claims to move it, and rung 5's delta would be zero -- a null
    # result that looks like evidence and is an artefact.
    #
    # Reading MSL here has a second payoff: rungs 0-4 are then the same walk
    # V6 published, so they are directly comparable to it.
    m_ades = argmax(args.trend_sweep_msl or args.trend_sweep, "ades", args.k)
    m_ceiling = trend_ceiling_kwargs(m_ades)
    if m_ceiling["trend_max_datum"] != "msl":
        raise SystemExit(
            "the path walk needs a sea-level sweep for rungs 0-4; pass "
            "--trend-sweep-msl. Walking it on the field datum would move the "
            "datum at rung 2 and leave rung 5 measuring nothing."
        )
    m_fl = m_ceiling["trend_max_fl"]
    m_mg = int(m_ades["margin"])
    m_rd, m_pn = float(m_ades["radius_nm"]), float(m_ades["penalty_nm"])
    path = {
        "path0_legacy":  path_cfg(),
        "path1_penalty": path_cfg(trend_sched_penalty_nm=m_pn),
        "path2_ceiling": path_cfg(trend_sched_penalty_nm=m_pn, **m_ceiling),
        "path3_margin":  path_cfg(trend_sched_penalty_nm=m_pn, **m_ceiling,
                                  trend_vote_margin=m_mg),
        "path4_radius":  path_cfg(trend_sched_penalty_nm=m_pn, **m_ceiling,
                                  trend_vote_margin=m_mg, trend_radius_nm=m_rd),
        # The fifth rung: rung 4 with the reference surface moved and nothing
        # else. The ceiling is the exact height equivalent of rung 4's flight
        # level, so the band admitted is identical and the only variable is
        # what it is measured from.
        "path5_datum":   path_cfg(trend_sched_penalty_nm=m_pn,
                                  trend_vote_margin=m_mg, trend_radius_nm=m_rd,
                                  trend_max_datum="field",
                                  trend_max_height_ft=msl_equivalent_height_ft(m_fl)),
    }

    # What the study actually recommends, as one flight list.
    #
    # Departures take `endpoint` at the swept optimum, which the pipeline
    # reproduces exactly. Arrivals take `trend` at the *production* geometry
    # plus the scheduled-service penalty -- because the pipeline path
    # decomposition shows the penalty is the only tuned step that survives
    # contact with the ring-count ranking, and the flight-level cap that the
    # harness liked most actively loses ground here.
    # Literally the shipped defaults. Built from DetectionConfig() rather than
    # assembled field by field, because the previous version started from
    # legacy() and overrode the values it remembered to override -- and forgot
    # trend_rank_by, so the row labelled "recommended" silently kept the old
    # ring-count selection and scored worse than production. A config assembled
    # by hand can disagree with the one that ships; this one cannot.
    recommended = DetectionConfig()

    # The datum pair: one variable, at the shipped geometry.
    #
    # Distinct from the path walk's rung 5, and both are wanted. Rung 5 moves
    # the datum at the *swept-tuned* geometry, which says what the datum is
    # worth to a configuration tuned around it; this pair moves it at the
    # *shipped* geometry, which says what the change did to production. They
    # can disagree, and a study that reported only one would not know.
    #
    # Both start from `DetectionConfig()` and change only the datum and the
    # ceiling, at the same admitted band -- FL60 reaches 6,100 ft, so 6,100 is
    # the matching above-field ceiling. This pair also produces the per-airport
    # counts the elevation banding is read from, so the banded numbers cannot
    # disagree with the headline ones.
    datum = {
        "datum_msl": dataclasses.replace(
            recommended, trend_max_datum="msl", trend_max_fl=60),
        "datum_field": dataclasses.replace(
            recommended, trend_max_datum="field",
            trend_max_height_ft=DATUM_ARM_CEILING_FT),
    }

    # A grid of trend settings, run through the pipeline rather than the sweep
    # harness. This exists because the harness's optimum was found against a
    # ranking rule the pipeline did not have; now that it does, the harness is
    # a credible model again -- but "credible" is not "verified", and the
    # cheapest way to verify is to run the thing itself.
    # Swept in feet above field elevation, because that is the datum that
    # ships. The grid brackets the shipped 6,000 ft with the 6,100 the sweep
    # preferred, so the paper can say which the pipeline itself wants rather
    # than quoting a grid that never contained the shipped value.
    grid = {}
    for cap in args.grid_height:
        for rd in args.grid_radius:
            for mg in args.grid_margin:
                grid[f"grid_h{cap}_r{rd:g}_m{mg}"] = path_cfg(
                    trend_max_datum="field", trend_max_height_ft=float(cap),
                    trend_radius_nm=float(rd),
                    trend_vote_margin=mg, trend_sched_penalty_nm=args.grid_penalty,
                )

    plan = {
        **{k_: (v_, "trend", "trend") for k_, v_ in grid.items()},
        **{k_: (v_, "trend", "trend") for k_, v_ in path.items()},
        # At the shipped modes, not forced to trend/trend: the pair is meant to
        # measure what the datum did to production, and production takes
        # departures from endpoint.
        **{k_: (v_, recommended.adep_mode, recommended.ades_mode)
           for k_, v_ in datum.items()},
        "recommended": (recommended, "endpoint", "trend"),
        "legacy": (DetectionConfig.legacy(), "trend", "trend"),
        "trend": (single("trend"), "trend", "trend"),
        "endpoint": (tuned(), "endpoint", "endpoint"),
        "nearest": (tuned(), "nearest", "nearest"),
        "combined": (tuned(), adep_method, ades_method),
    }

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
    gt = load_ground_truth(spark, args.months, args.days)
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
                 trend_rank_by=getattr(detection, "trend_rank_by", "ring"),
                 trend_max_fl_v=detection.trend_max_fl,
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
