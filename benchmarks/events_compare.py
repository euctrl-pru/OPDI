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
from event_bench import PERIOD_TRACKS, index_on_read, redirect_tracks

#: Ring event types, and the truth milestone each corresponds to. Kept here
#: rather than added to `event_bench.TYPE_TO_MILESTONE`: that module's outputs
#: are already staged with provenance, and widening its map would add empty
#: rows to a ladder whose truth frame holds no ring milestones. A comparison
#: script should not perturb the harness it is comparing.
RING_TYPES = {"xing-40nm": "xing-40nm", "xing-100nm": "xing-100nm"}

#: Types whose runway designator can be checked against AP_C_RWY.
RUNWAY_TYPES = {"ATOT": "ATOT", "ALDT": "ALDT"}


def _identity(spark, storage, period, tracks_table):
    """(track_id -> icao24, callsign, day), from whichever source the period has.

    Same rule as the ladder: the production flight list covers 2025, and 2024
    has none, so its identity comes from the tracks. Stated per period rather
    than inferred, because the flight list is not *empty* for 2024, it is the
    wrong period, and a row count cannot tell those apart.
    """
    if PERIOD_TRACKS[period]["identity"] == "flight_list":
        return storage.read_table("opdi_flight_list").select(
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
    info = F.from_json(F.col("info"), "runway string, apt_icao string, direction string")
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
            F.col("_i.runway").alias("det_runway"),
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
    ap.add_argument("--rung", default="L13_shipped")
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
    truth, rings, _ = events_gt.build(spark, args.period)
    rings.cache()

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
    rwy_det = detected(spark, table, ident, RUNWAY_TYPES)
    rwy_aligned = events_score.align(truth, rwy_det)
    rwy = events_score.score_runways(rwy_aligned)
    rwy_rows = (
        [{"period": args.period, "rung": args.rung, **r.asDict()} for r in rwy.collect()]
        if rwy is not None else []
    )
    write_csv(rwy_rows, out / f"runway_{args.period}.csv")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
