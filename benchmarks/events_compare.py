#!/usr/bin/env python
"""The three comparisons that were coded and never run.

``events_score.py`` has carried ``score_runways``, ``score_positions`` and
``inter_source_floor`` since it was written, and the ladder never called any of
them. So three questions the reference data can answer were left unanswered:

* **Do the ring crossings agree with APDF's?** This is the *precise*
  comparison in the whole study. Only 1.7% of ``C40_CROSS_TIME`` values land on
  a whole minute, against 64% of the movement times, so it is the one place a
  seconds-level claim about OPDI is defensible rather than dominated by the
  reference's own quantisation.
* **Is the runway identity right?** ``AP_C_RWY`` names the runway a movement
  used, so the detector's designator can be checked exactly.
* **How far apart are two EUROCONTROL derivations of the same crossing?** Not a
  score of OPDI at all -- the yardstick. A detector inside that spread is as
  close to the reference as the reference is to itself.

Two more were added for V4, and both are splits of the same aligned frame the
runway check builds rather than new measurements:

* **Where does it work?** ``score_by_airport`` -- the network figure is an
  average over twenty aerodromes that differ in size by a factor of twenty, and
  an average hides which of them the detector fails at.
* **Against a truth that can be read to the second?** ``score_by_truth_resolution``
  -- 64% of APDF movement times land on a whole minute, so an unstratified
  error distribution mostly measures the reference's quantisation.

This reads event tables that **already exist** on S3 and re-runs no detector.
That is the point: the ladder cost hours, its output is still there, and these
questions only ever needed a different query over it.
"""

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import functions as F

import events_gt
import events_score
from event_bench import (
    LADDERS,
    PERIOD_TRACKS,
    build_plan,
    flight_list_table,
    index_on_read,
    milestone_map,
    redirect_tracks,
    runway_identity_types,
)

#: Ring event types, and the truth milestone each corresponds to. Kept here
#: rather than added to `event_bench.TYPE_TO_MILESTONE`: that module's outputs
#: are already staged with provenance, and widening its map would add empty
#: rows to a ladder whose truth frame holds no ring milestones. A comparison
#: script should not perturb the harness it is comparing.
RING_TYPES = {"xing-40nm": "xing-40nm", "xing-100nm": "xing-100nm"}

# The types whose runway designator can be checked against AP_C_RWY used to be
# a constant here. They are not one: which types name a runway depends on the
# rung's vocabulary, so they are asked for per run --
# `event_bench.runway_identity_types`.


def _identity(spark, storage, period, tracks_table):
    """(track_id -> icao24, callsign, day), from whichever source the period has.

    Same rule as the ladder: the production flight list covers 2025, and 2024
    has none, so its identity comes from the tracks. Stated per period rather
    than inferred, because the flight list is not *empty* for 2024, it is the
    wrong period, and a row count cannot tell those apart.
    """
    if PERIOD_TRACKS[period]["identity"] == "flight_list":
        # Which flight list is a property of the period: 2025's is the
        # published table, 2026's is the research copy built for it. The same
        # `.get` the ladder uses, so the two cannot resolve identity through
        # different tables and disagree about which flights exist.
        return storage.read_table(flight_list_table(period)).select(
            F.col("ID").alias("_id"),
            F.lower(F.col("ICAO24")).alias("icao24"),
            F.trim(F.col("FLT_ID")).alias("callsign"),
            F.to_date(F.col("FIRST_SEEN")).alias("day"),
        )
    return (
        storage.read_table(tracks_table)
        .groupBy("track_id")
        .agg(
            F.lower(F.first("icao24", ignorenulls=True)).alias("icao24"),
            F.trim(F.first("callsign", ignorenulls=True)).alias("callsign"),
            F.to_date(F.min("event_time")).alias("day"),
        )
        .withColumnRenamed("track_id", "_id")
    )


def detected(spark, table, identity, types):
    """Event rows of the given types, carrying identity and the truth key."""
    ev = spark.read.parquet(table)
    # `runway` is v0.1.0's key (events.calculate_runway_events); `rwy_ident` is
    # the A-CDM family's (runway_ops._info). Reading one leaves the other's
    # designator NULL, which reads as an unnamed runway rather than as a
    # mis-read column.
    info = F.from_json(
        F.col("info"),
        "runway string, rwy_ident string, apt_icao string, direction string",
    )
    mapping = F.create_map(*[F.lit(x) for kv in types.items() for x in kv])
    ev = (
        ev.withColumn("_i", info)
        .filter(F.col("type").isin(list(types)))
        .select(
            F.col("flight_id").alias("_track_id"),
            F.col("type").alias("det_type"),
            mapping[F.col("type")].alias("milestone"),
            F.col("event_time"),
            F.col("latitude").alias("det_lat"),
            F.col("longitude").alias("det_lon"),
            F.coalesce(F.col("_i.runway"), F.col("_i.rwy_ident")).alias("det_runway"),
        )
    )
    return ev.join(
        F.broadcast(identity), ev._track_id == F.col("_id"), "inner"
    ).drop("_id", "_track_id")


def write_csv(rows, path):
    if not rows:
        print(f"  (nothing to write for {path.name})")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=sorted(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"  wrote {path} ({len(rows)} rows)")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--period", choices=sorted(events_gt.PERIODS), required=True)
    ap.add_argument(
        "--ladder", choices=sorted(LADDERS), default="v3",
        help="which ladder wrote the event table named by --rung. It decides "
             "the rung's configuration, and the configuration decides which "
             "event types mean which milestone.",
    )
    ap.add_argument("--rung", default="L13_shipped")
    ap.add_argument(
        "--airports", choices=("study", "all"), default="all",
        help="restrict the ground truth to events_gt.STUDY_AIRPORTS, matching "
             "the ladder run this compares.",
    )
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--executors", type=int, default=6)
    ap.add_argument("--ui-port", type=int, default=4065)
    ap.add_argument("--cores", type=int, default=4)
    ap.add_argument("--driver-memory", default="8g")
    args = ap.parse_args()

    import osn_sample
    from osn_sample import build_spark, load_dotenv

    load_dotenv()
    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(args.cores, args.driver_memory, distributed=True)
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", "96")

    redirect_tracks(args.period)
    index_on_read(PERIOD_TRACKS[args.period]["index_on_read"])

    from opdi.config import OPDIConfig
    from opdi.utils.storage import StorageManager

    storage = StorageManager(spark, OPDIConfig.for_environment("opensky"))
    truth, rings, _ = events_gt.build(spark, args.period, airports=args.airports)
    truth.cache()
    rings.cache()

    # The rung's own configuration, because `landing` and the runway families
    # mean different things either side of `emit_runway_milestones`. A rung
    # name that is not on the ladder is a typo worth failing on rather than
    # quietly scoring the wrong vocabulary.
    plan = build_plan(ladder=args.ladder)
    if args.rung not in plan:
        raise SystemExit(
            f"unknown rung {args.rung!r} on ladder {args.ladder!r}: "
            f"choose from {', '.join(plan)}"
        )
    cfg = plan[args.rung]

    out = Path(args.results_dir)
    table = f"s3a://eurocontrol/opdi/research/events_{args.period}_{args.rung}"
    ident = _identity(spark, storage, args.period, "osn_tracks_clean")

    # -- the yardstick, first: it sets what "close" can mean below -----------
    floor = [
        {"period": args.period, **r.asDict()} for r in
        events_score.inter_source_floor(rings).collect()
    ]
    for r in floor:
        print(f"  inter-source floor {r['milestone']}: "
              f"p10 {r['p10_s']:+.0f}s  median {r['median_s']:+.0f}s  p90 {r['p90_s']:+.0f}s")
    write_csv(floor, out / f"floor_{args.period}.csv")

    # -- rings: time error and position error --------------------------------
    ring_det = detected(spark, table, ident, RING_TYPES)
    ring_aligned = events_score.align(rings, ring_det)
    ring_rows = [
        {"period": args.period, "rung": args.rung, **r.asDict()}
        for r in events_score.score(ring_aligned).collect()
    ]
    pos = events_score.score_positions(ring_aligned)
    pos_by = {r["milestone"]: r.asDict() for r in pos.collect()} if pos is not None else {}
    for r in ring_rows:
        r.update({k: v for k, v in pos_by.get(r["milestone"], {}).items()
                  if k != "milestone"})
    write_csv(ring_rows, out / f"rings_{args.period}.csv")

    # -- runway identity against AP_C_RWY ------------------------------------
    rwy_types = runway_identity_types(cfg)
    rwy_det = detected(spark, table, ident, rwy_types)
    rwy_aligned = events_score.align(truth, rwy_det)
    rwy = events_score.score_runways(rwy_aligned)
    # `milestone` is the stable label -- ATOT/ALDT on both sides of the
    # vocabulary change, because `runway_identity_types` maps airborne -> ATOT
    # and touchdown -> ALDT before the scoring ever sees a type. `det_type`
    # records which detector produced it, so V4's runway_2026.csv can be read
    # column-to-column against V3's runway_2025.csv *and* still say that the
    # two rows came from different code. Without it the CSV cannot distinguish
    # a v0.1.0 ATOT from an A-CDM airborne at all.
    det_type_for = {milestone: type_ for type_, milestone in rwy_types.items()}
    rwy_rows = (
        [{"period": args.period, "rung": args.rung,
          "det_type": det_type_for.get(r["milestone"], ""), **r.asDict()}
         for r in rwy.collect()]
        if rwy is not None else []
    )
    write_csv(rwy_rows, out / f"runway_{args.period}.csv")

    # -- the milestone frame, split two ways ---------------------------------
    # One alignment, two tables. Both answer "where is the network figure
    # coming from" rather than adding a measurement: per aerodrome, and per
    # whether the aerodrome's APDF times are readable to the second.
    #
    # `milestone_map` is what keeps this honest under v0.2.0, where `landing`
    # is the threshold plane rather than the touchdown -- see its docstring.
    ms_det = detected(spark, table, ident, milestone_map(cfg))
    ms_aligned = events_score.align(truth, ms_det).cache()

    per_airport = [
        {"period": args.period, "rung": args.rung, **r.asDict()}
        for r in events_score.score_by_airport(ms_aligned).collect()
    ]
    write_csv(per_airport, out / f"per_airport_{args.period}.csv")

    # Per aerodrome as well as per milestone: whether the truth is readable to
    # the second is a property of the aerodrome's reporting system, so the
    # network-level "64% land on a whole minute" is an average over aerodromes
    # that are individually at 0% or 100%. Annex A needs the per-aerodrome
    # column to say whether a given aerodrome's error figure is dominated by
    # quantisation; the pooled row cannot answer that for any of them.
    resolution = [
        {"period": args.period, "rung": args.rung, **r.asDict()}
        for r in events_score.score_by_truth_resolution(
            ms_aligned, group_cols=("gt_airport", "milestone", "gt_subminute")
        ).collect()
    ]
    write_csv(resolution, out / f"resolution_{args.period}.csv")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
