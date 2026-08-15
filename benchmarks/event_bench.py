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
import shutil
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

#: Which track tables each period actually lives in. The 2025 sample is in the
#: production tables; the 2024 sample was never ingested there and lives under
#: research/ -- so a run that does not redirect reads 2025 tracks, filters them
#: to June 2024, finds nothing, and reports every rung as empty. That happened.
#: `flight_list_v7.PERIODS` carries the same mapping for the same reason.
PERIOD_TRACKS = {
    "2025": {"raw": "osn_tracks", "clean": "osn_tracks_clean", "index_on_read": []},
    "2024": {
        "raw": "research/tracks",
        "clean": "research/tracks_clean",
        # The raw 2024 tracks pre-date step 02's H3 indexing, and the early
        # rungs read them deliberately. Computing the index on read costs one
        # column expression per scan; materialising a second 12 GB copy to add
        # a derived column would cost the bucket a sixth of its free space.
        "index_on_read": ["research/tracks", "research/tracks_clean"],
    },
}

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
    ("L10_runway", {"emit_runway_events": True}),
    ("L11_blocks", {"emit_block_events": True}),
    ("L12_level_offs", {"emit_level_offs": True}),
    ("L13_shipped", {"events_version": "events_v0.1.0"}),
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


def redirect_tracks(period: str) -> None:
    """Point the track reads at the period's own tables.

    Patches ``_s3_path`` rather than the table name, as
    ``redirect_event_tables`` does and for the same reason: ``table_ref``
    registers a temp view named after the table, and ``research/tracks_clean``
    is not a legal SQL identifier.
    """
    spec = PERIOD_TRACKS[period]
    if spec["raw"] == "osn_tracks" and spec["clean"] == "osn_tracks_clean":
        return
    from opdi.utils.storage import StorageManager

    mapping = {"osn_tracks": spec["raw"], "osn_tracks_clean": spec["clean"]}
    orig = getattr(StorageManager, "_events_track_path", None)
    if orig is None:
        orig = StorageManager._s3_path
        StorageManager._events_track_path = orig

    def _s3_path(self, table_name):
        return orig(self, mapping.get(table_name, table_name))

    StorageManager._s3_path = _s3_path


def index_on_read(tables) -> None:
    """Backfill the columns step 02 adds, for tables written before it did.

    Two are needed and both are guarded on absence, so this is a no-op the day
    the research copies are replaced by step 02's own output.

    ``h3_res_12`` -- step 04's airport events join the layout table on it, so a
    track table without it cannot be read by the real detector at all.

    ``baro_altitude_c`` -- step 02's rolling-mean altitude repair. The 2024
    research tracks predate it, and every event detector reads it. Falling back
    to the raw ``baro_altitude`` is the honest substitute rather than a
    silently different one: it is what the column *was* before the repair
    existed, so a 2024 legacy rung reading it is closer to what
    ``events_v0.0.2`` actually saw in 2024 than a repaired column would be.
    The repair itself is not reproduced here -- doing so would mean running
    step 02 over the period, which is a different experiment.
    """
    if not tables:
        return
    from opdi.utils.storage import StorageManager

    if getattr(StorageManager, "_events_h3_on_read", False):
        return
    orig_read = StorageManager.read_table
    wanted = set(tables)

    def read_table(self, table_name):
        df = orig_read(self, table_name)
        resolved = getattr(self, "_s3_path")(table_name)
        if any(w in str(resolved) for w in wanted):
            if "h3_res_12" not in df.columns:
                import h3_pyspark

                df = df.withColumn(
                    "h3_res_12", h3_pyspark.geo_to_h3("lat", "lon", F.lit(12))
                )
            if "baro_altitude_c" not in df.columns and "baro_altitude" in df.columns:
                df = df.withColumn("baro_altitude_c", F.col("baro_altitude"))
        return df

    StorageManager.read_table = read_table
    StorageManager._events_h3_on_read = True


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
    if names and names[-1] == "L13_shipped":
        shipped = EventConfig()
        got = plan["L13_shipped"]
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


#: Which OPDI event type claims to be which APDF milestone. `take-off` and
#: `landing` are the published fuzzy-phase pair; ATOT and ALDT are the new
#: runway-anchored ones. Both are scored, separately, against the same truth --
#: that comparison *is* the question the ladder exists to answer, and collapsing
#: them would hide it.
TYPE_TO_MILESTONE = {
    "take-off": "ATOT",
    "landing": "ALDT",
    "ATOT": "ATOT",
    "ALDT": "ALDT",
    "AOBT": "AOBT",
    "AIBT": "AIBT",
}


def detected_events(spark, table: str, storage=None):
    """Reshape the written event table into what the scorer expects.

    The event table keys on ``flight_id``, which is the ``track_id``; the
    ground truth keys on ``(icao24, callsign, day)``. Those are different
    identity spaces and the join between them has to be made explicitly --
    the flight list is what holds both, so it is the bridge.

    This was missed on the first run and cost an hour: the scorer's unit tests
    fed it synthetic frames that already carried ``icao24``, so they validated
    the scoring arithmetic and never the identity resolution. A test that
    constructs its own inputs cannot catch a mismatch between two real
    schemas.

    ``callsign`` is trimmed because ADS-B pads to eight characters, and
    ``icao24`` lowered because the reference carries it uppercase; both are the
    same traps the ground-truth loader closes on its side.
    """
    ev = spark.read.parquet(table) if table.startswith("s3a://") else spark.table(table)
    info = F.from_json(
        F.col("info"), "runway string, apt_icao string, crossing_seq int, direction string"
    )
    mapping = F.create_map(*[F.lit(x) for kv in TYPE_TO_MILESTONE.items() for x in kv])
    ev = ev.withColumn("_i", info).select(
        F.col("flight_id").alias("_track_id"),
        F.col("type").alias("det_type"),
        mapping[F.col("type")].alias("milestone"),
        F.col("event_time"),
        F.col("latitude").alias("det_lat"),
        F.col("longitude").alias("det_lon"),
        F.col("_i.runway").alias("det_runway"),
    )

    fl = storage.read_table("opdi_flight_list").select(
        F.col("ID").alias("_fl_id"),
        F.lower(F.col("ICAO24")).alias("icao24"),
        F.trim(F.col("FLT_ID")).alias("callsign"),
        F.to_date(F.col("FIRST_SEEN")).alias("day"),
    )
    return (
        ev.filter(F.col("milestone").isNotNull())
        .join(F.broadcast(fl), ev._track_id == F.col("_fl_id"), "inner")
        .drop("_fl_id", "_track_id")
    )


def extraction_counts(spark, table: str, rung: str):
    """What step 04 actually emitted, by event type.

    The scorer only ever sees the milestones APDF can reach, which is four
    types out of roughly twenty. Everything else -- the level-offs, the
    top-of-climb and top-of-descent, the airport entry and exit events, the
    first and last seen -- is invisible in a scored table, so a family that
    stopped being emitted entirely would not show up as a regression. It would
    show up as nothing at all, which is worse.

    This is also the number consumers need: it says how much bigger the
    published event table becomes, per type, which is what they are being asked
    to ingest.
    """
    ev = spark.read.parquet(table) if table.startswith("s3a://") else spark.table(table)
    return [
        {"rung": rung, "type": r["type"], "n_events": r["n_events"]}
        for r in ev.groupBy("type")
        .agg(F.count(F.lit(1)).alias("n_events"))
        .orderBy("type")
        .collect()
    ]


def write_csv(rows, path):
    import csv

    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=sorted(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"  wrote {path}")


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
    redirect_tracks(args.period)
    index_on_read(PERIOD_TRACKS[args.period]["index_on_read"])
    guard_writes()

    truth, rings, report = events_gt.build(spark, args.period)
    truth.cache()

    # Fail before the expensive part, not an hour into it. The first run of
    # this harness spent 68 minutes computing events and then died on the
    # scorer's join because the detected side had no `icao24` -- a check that
    # costs nothing here would have caught it in seconds.
    required = {"icao24", "callsign", "day", "milestone", "gt_time"}
    missing = required - set(truth.columns)
    if missing:
        raise SystemExit(f"ground truth is missing join keys: {sorted(missing)}")

    import datetime as dt

    from opdi.pipeline.events import FlightEventProcessor

    month = dt.datetime.strptime(events_gt.PERIODS[args.period]["month"], "%Y%m").date()
    scored, inventory = [], []
    for name, cfg in plan.items():
        print(f"\n=== {name} ===")
        target = f"research/events_{args.period}_{name}"
        redirect_event_tables(target)

        config = OPDIConfig.for_environment("opensky")
        config.events = cfg
        # Clear the per-family progress log first. `process_month` decides what
        # to compute from those logs *independently* of skip_if_processed, so a
        # rung whose name was used by an earlier run computes nothing, writes
        # nothing, and the scorer then reads the earlier run's table and reports
        # it as this rung's result. That happened: L00_legacy scored the
        # previous run's contaminated output and looked entirely plausible.
        log_dir = f"logs/events_{args.period}_{name}"
        shutil.rmtree(log_dir, ignore_errors=True)
        proc = FlightEventProcessor(spark, config, log_dir=log_dir)
        proc.process_month(month, skip_if_processed=False)

        table = f"s3a://eurocontrol/opdi/{target}"
        inventory.extend(extraction_counts(spark, table, name))
        detected = detected_events(spark, table, proc.storage).cache()
        types = [r.det_type for r in detected.select("det_type").distinct().collect()]
        rung_rows = []
        for det_type in sorted(types):
            aligned = events_score.align(
                truth, detected.filter(F.col("det_type") == det_type)
            )
            for row in events_score.score(aligned).collect():
                rung_rows.append({"rung": name, "det_type": det_type, **row.asDict()})
        if not rung_rows:
            raise SystemExit(
                f"{name}: emitted no scorable milestone type at all. A rung "
                f"that produces nothing is a configuration or input fault, not "
                f"a result -- on 2024 this meant the run was reading the 2025 "
                f"track tables and filtering them to a month they do not "
                f"contain."
            )
        else:
            events_score.guard_not_all_zero(
                spark.createDataFrame(rung_rows).select("n_detected")
            )
        scored.extend(rung_rows)
        print(f"  scored {len(scored)} rows so far")

    if args.results_dir:
        out = Path(args.results_dir)
        write_csv(scored, out / (args.out_name or f"ladder_{args.period}.csv"))
        write_csv(inventory, out / f"inventory_{args.period}.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
