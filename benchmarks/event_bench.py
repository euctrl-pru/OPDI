#!/usr/bin/env python
"""Run step 04 through its real code path and score what it produced.

The point of running ``FlightEventProcessor`` rather than reimplementing the
detectors is that a benchmark of a reimplementation measures the
reimplementation. Version 6 learned this the expensive way on the flight list;
this follows ``flight_list_v7.py``'s shape for the same reason.

**The write guard is not optional.** Step 04 writes ``opdi_flight_events`` and
``opdi_measurements`` by name, and until recently did so in append mode by
default. An unguarded benchmark run would therefore add a duplicate copy of a
month to the *published* tables -- silently, since nothing errors and the rows
are merely counted twice afterwards. Every run here refuses any write outside
``research/``.

The ladder is cumulative in the V7 style: rung 0 is ``EventConfig.legacy()``,
which reproduces published ``events_v0.0.2``, and each subsequent rung turns on
one more change. What that buys over scoring only the endpoints is
attribution -- if the shipped configuration is better, the ladder says which
change made it so, and if a change is worth nothing it is visible rather than
carried along inside a net gain.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import functions as F

import events_gt
import events_score
from opdi.config import EventConfig, OPDIConfig

#: Where a benchmark run may write. Anything else is a bug, loudly.
RESEARCH_PREFIX = "research/"

#: The ladder. Each rung is (name, {field: value}) applied cumulatively on top
#: of `EventConfig.legacy()`, so rung 0 is the published algorithm and the last
#: rung must equal the shipped configuration -- `verify_plan` asserts both.
LADDER = [
    ("L00_legacy", {}),
    ("L01_clean_tracks", {"feeds_from_clean_tracks": True}),
    ("L02_smoothing", {"phase_twindow_seconds": 60.0}),
    ("L03_complete_rules", {"phase_require_complete_rules": True}),
    ("L04_field_elevation", {"phase_ground_above_field": True}),
    ("L05_ordered_airport", {"airport_events_ordered": True}),
    ("L06_deterministic_ids", {"deterministic_event_ids": True}),
    ("L07_all_crossings", {"crossing_all_occurrences": True}),
    ("L08_interpolated", {"crossing_interpolate": True}),
    ("L09_rings", {"ring_radii_nm": (40.0, 100.0)}),
    ("L10_shipped", {"events_version": "events_v0.1.0"}),
]


def guard_writes(allowed_prefix: str = RESEARCH_PREFIX) -> None:
    """Refuse any write outside ``research/``.

    Copied from ``flight_list_v7.py`` deliberately rather than imported: that
    module carries a plan and a CLI this one does not want, and a benchmark's
    safety rail should not depend on another benchmark staying importable.
    """
    from opdi.utils.storage import StorageManager

    if getattr(StorageManager, "_events_guarded", False):
        return
    orig_write = StorageManager.write_table

    def write_table(self, df, table_name, *a, **kw):
        # Check the resolved *path*, not the name. `redirect_event_tables`
        # sends `opdi_flight_events` to a research location by patching
        # `_s3_path`, so a name-based guard would reject exactly the writes the
        # redirect has already made safe -- and, worse, would pass a write whose
        # name looked safe but whose path had been redirected somewhere else.
        landing = str(self._s3_path(table_name))
        if f"/{allowed_prefix}" not in landing:
            raise RuntimeError(
                f"refusing to write {table_name!r} -> {landing!r}: this "
                f"benchmark writes only under {allowed_prefix!r}. Step 04 "
                f"writes opdi_flight_events by name, so without this an "
                f"unguarded run would append a duplicate month to the "
                f"published table."
            )
        print(f"  -> writing {table_name} -> {landing}")
        return orig_write(self, df, table_name, *a, **kw)

    StorageManager.write_table = write_table
    StorageManager._events_guarded = True


def redirect_event_tables(target: str) -> None:
    """Send step 04's two output tables to a per-rung research location.

    Patches ``_s3_path`` rather than the table *name*, following
    ``flight_list_v7.redirect_candidates``: ``table_ref`` registers a temp view
    named after the table, and ``research/events_2025_L00`` is not a legal SQL
    identifier. Also forces overwrite, because a re-run of one rung must
    replace its own output rather than append to it -- the same append trap the
    published tables have, in a place where it would quietly double a score.
    """
    from opdi.utils.storage import StorageManager

    redirected = {"opdi_flight_events": target, "opdi_measurements": target + "_meas"}
    orig_path = getattr(StorageManager, "_events_orig_path", StorageManager._s3_path)
    StorageManager._events_orig_path = orig_path

    def _s3_path(self, table_name):
        return orig_path(self, redirected.get(table_name, table_name))

    StorageManager._s3_path = _s3_path

    orig_write = getattr(StorageManager, "_events_orig_write", None)
    if orig_write is None:
        orig_write = StorageManager.write_table
        StorageManager._events_orig_write = orig_write

    def write_table(self, df, table_name, mode="append"):
        if table_name in redirected:
            mode = "overwrite"
        return orig_write(self, df, table_name, mode)

    StorageManager.write_table = write_table


def build_plan(only=None) -> dict:
    """name -> EventConfig, cumulative from legacy()."""
    plan, current = {}, EventConfig.legacy()
    for name, delta in LADDER:
        from dataclasses import replace

        current = replace(current, **delta) if delta else current
        plan[name] = current
    if only:
        missing = [n for n in only if n not in plan]
        if missing:
            raise SystemExit(f"unknown rung(s): {', '.join(missing)}")
        plan = {n: plan[n] for n in only}
    return plan


def verify_plan(plan: dict) -> None:
    """Four assertions before anything expensive runs.

    Every one of these has a V6/V7 precedent: a ladder whose first rung was not
    actually the published algorithm, or whose last was not what ships, measures
    something nobody asked about -- and costs two hours per rung to discover.
    """
    names = list(plan)
    if names and names[0] == "L00_legacy":
        assert plan["L00_legacy"] == EventConfig.legacy(), (
            "rung 0 must be exactly the published algorithm, or the baseline "
            "every gain is measured against is not the baseline"
        )
    if names and names[-1] == "L10_shipped":
        shipped = EventConfig()
        got = plan["L10_shipped"]
        differing = [
            f
            for f in EventConfig().__dataclass_fields__
            if getattr(got, f) != getattr(shipped, f)
        ]
        assert not differing, (
            f"the last rung must equal the shipped configuration; differs on "
            f"{', '.join(differing)}"
        )
    for a, b in zip(names, names[1:]):
        assert plan[a] != plan[b], f"rungs {a} and {b} are identical -- one is a no-op"


def detected_events(spark, table: str):
    """Reshape the written event table into what the scorer expects."""
    ev = spark.read.parquet(table) if table.startswith("s3a://") else spark.table(table)
    info = F.from_json(
        F.col("info"), "runway string, apt_icao string, crossing_seq int, direction string"
    )
    return (
        ev.withColumn("_i", info)
        .select(
            F.col("flight_id").alias("track_id"),
            F.col("type").alias("milestone"),
            F.col("event_time"),
            F.col("latitude").alias("det_lat"),
            F.col("longitude").alias("det_lon"),
            F.col("_i.runway").alias("det_runway"),
        )
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--period", choices=sorted(events_gt.PERIODS), required=True)
    ap.add_argument("--runs", nargs="*", default=None, help="ladder rung names")
    ap.add_argument("--results-dir", default=None)
    ap.add_argument("--out-name", default=None)
    ap.add_argument("--executors", type=int, default=10)
    ap.add_argument("--ui-port", type=int, default=4059)
    ap.add_argument("--cores", type=int, default=4)
    ap.add_argument("--driver-memory", default="8g")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    plan = build_plan(args.runs)
    verify_plan(plan)
    print(f"plan verified: {len(plan)} rung(s) -- {', '.join(plan)}")
    if args.dry_run:
        return 0

    import osn_sample
    from osn_sample import build_spark, load_dotenv

    load_dotenv()
    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(args.cores, args.driver_memory, distributed=True)
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", "96")
    guard_writes()

    truth, rings, report = events_gt.build(spark, args.period)
    truth.cache()

    import datetime as dt

    from opdi.pipeline.events import FlightEventProcessor

    month = dt.datetime.strptime(events_gt.PERIODS[args.period]["month"], "%Y%m").date()
    scored = []
    for name, cfg in plan.items():
        print(f"\n=== {name} ===")
        target = f"research/events_{args.period}_{name}"
        redirect_event_tables(target)

        config = OPDIConfig.for_environment("opensky")
        config.events = cfg
        proc = FlightEventProcessor(spark, config, log_dir=f"logs/events_{name}")
        proc.process_month(month, skip_if_processed=False)

        detected = detected_events(spark, f"s3a://eurocontrol/opdi/{target}")
        aligned = events_score.align(truth, detected)
        s = events_score.score(aligned)
        events_score.guard_not_all_zero(s)
        for row in s.collect():
            scored.append({"rung": name, **row.asDict()})
        print(f"  scored {len(scored)} rows so far")

    if args.results_dir and scored:
        import csv

        out = Path(args.results_dir)
        out.mkdir(parents=True, exist_ok=True)
        path = out / (args.out_name or f"ladder_{args.period}.csv")
        with open(path, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=sorted(scored[0]))
            w.writeheader()
            w.writerows(scored)
        print(f"\nwrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
