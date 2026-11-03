#!/usr/bin/env python
"""Run one day through the real per-day pipeline, twice: ADS-B only, and
ADS-B + ADS-C. Into research prefixes, never production.

    export PYSPARK_DRIVER_PYTHON=$PWD/.venv310/bin/python
    unset PYSPARK_PYTHON    # executors must use the opdi-spark image's own
                            # python -- it has h3, and this venv does not
                            # exist in the executor container, which kills
                            # step 02's h3_res_12 UDF with "Cannot run
                            # program .../.venv310/bin/python: Exec failed".
    .venv310/bin/python -u benchmarks/run_adsc_day.py --day 2026-06-05 --arm both

**Why both arms are rebuilt.** The published 2026 tables
(``research/tracks_2026``) were built by the A8 ``recommended`` segmentation
arm; the current default is A9 ``debounced``. Using them as the ADS-B-only
baseline would put two changes in one delta and the study could not say which
moved the number -- the mistake the track-construction V1 study documents.

**Why the arms share their inputs.** ``osn_statevectors_v2`` and
``osn_adsc_statevectors`` live under ``research/adsc/input/``, written once and
read by both arms. The claim "the arms differ in exactly one setting" is then
structurally true rather than asserted, and the ADS-B ingest is paid once.

**How the redirect works.** ``StorageManager`` resolves every table against one
``warehouse_path``, so a run cannot read production reference data and write
research output without help. The help is a patched ``_s3_path``, the idiom
``event_bench.redirect_event_tables`` and ``flight_list_v7.redirect_candidates``
already use -- patching the resolver rather than the table name, because
``table_ref`` registers a temp view named after the table and
``research/adsc/merged/osn_tracks`` is not a legal SQL identifier.

Three disjoint sets, and a table in none of them raises. That is the whole
lesson of the A9 run, which failed twice because ``opdi_endpoint_candidates``
was written to the research prefix and read back from production: a
classification that defaults is a classification that is wrong silently.
"""

import argparse
import sys
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

from opdi.config import OPDIConfig  # noqa: E402
from opdi.periodrun import run_day_step  # noqa: E402
from opdi.utils.spark_helpers import SparkSessionManager  # noqa: E402
from opdi.utils.storage import StorageManager  # noqa: E402

#: Root of every table this script writes, per coverage. Appears in
#: :func:`guard_write_path`, so a typo here cannot send a write to production
#: -- it fails the guard.
#:
#: Worldwide writes under ``opdi-world``, not under the European prefix. That
#: is the pipeline's own rule (``config._worldwide_warehouse``) and it exists
#: because the two coverages are not comparable: a worldwide track reaches
#: aerodromes the European reference tables never described, and mixing them
#: in one prefix would make "which coverage produced this row" unanswerable.
ARM_PREFIX_BY_COVERAGE = {
    "europe": "s3a://eurocontrol/opdi/research/adsc",
    "worldwide": "s3a://eurocontrol/opdi-world/research/adsc",
}

#: Where the read-only reference tables live, per coverage. The worldwide H3
#: zones, runway grid and HexAero layouts are built for global aerodromes;
#: pointing a worldwide run at the European ones would find no aerodrome for
#: any track outside the box and report every aerodrome-anchored family empty
#: rather than broken.
REFERENCE_BY_COVERAGE = {
    "europe": "s3a://eurocontrol/opdi-prod",
    "worldwide": "s3a://eurocontrol/opdi-world",
}

#: Set by :func:`set_coverage` before any path is resolved.
ARM_PREFIX = ARM_PREFIX_BY_COVERAGE["europe"]
COVERAGE = "europe"

#: Restrict the ADS-B input to airframes present in the ADS-C table. Sound for
#: the comparison because segmentation partitions by airframe: an airframe with
#: no ADS-C is identical in both arms, so dropping it changes no difference
#: between them. It changes the totals, so a percentage needs the full feed's
#: denominator. Exists because a full worldwide day (about 21.5 GB per arm,
#: measured on opdi-world) does not fit the bucket.
ADSC_AIRFRAMES_ONLY = False


def set_coverage(label: str, adsc_airframes_only: bool = False) -> None:
    """Switch which coverage's prefixes this run reads and writes.

    Must be called before anything resolves a path. Rebinding the module
    globals is sound because every consumer reads them at call time.
    """
    global ARM_PREFIX, COVERAGE, ADSC_AIRFRAMES_ONLY
    if label not in ARM_PREFIX_BY_COVERAGE:
        raise ValueError(
            f"unknown coverage {label!r}; expected one of "
            f"{tuple(ARM_PREFIX_BY_COVERAGE)}")
    ARM_PREFIX = ARM_PREFIX_BY_COVERAGE[label]
    COVERAGE = label
    ADSC_AIRFRAMES_ONLY = adsc_airframes_only
    if adsc_airframes_only:
        # Beside the full-feed prefix, never inside it. Its row and track
        # counts describe a selected population and must not be read as the
        # feed's.
        ARM_PREFIX = f"{ARM_PREFIX}_airframes"


def check_subset_steps(steps) -> None:
    """Refuse a subset run that would ingest ADS-B before the airframes exist.

    Step 01 filters on the table step 01b writes. A run with neither reuses the
    inputs an earlier arm already wrote.
    """
    steps = list(steps)
    if "01" not in steps:
        return
    if "01b" not in steps or steps.index("01b") > steps.index("01"):
        raise ValueError(
            "--adsc-airframes-only needs step 01b before step 01: "
            f"got {' '.join(steps)}")


def is_subset_target(table_name: str) -> bool:
    """The one table the subset filters. Everything else derives from it."""
    return table_name == "osn_statevectors_v2"


def keep_adsc_airframes(adsb, adsc):
    """ADS-B rows of the airframes that appear in the ADS-C table."""
    return adsb.join(adsc.select("icao24").distinct(), "icao24", "left_semi")

#: Reference data, read and never written. Resolved against the run's own
#: ``warehouse_path`` (production), because re-deriving an H3 airport layout
#: for a two-arm comparison would cost more than the comparison.
PASSTHROUGH = (
    "h3_airport_detection_zones",
    "h3_runway_zones",
    "hexaero_airport_layouts",
    "oa_airports",
    "oa_runways",
    "osn_aircraft_db",
    "opdi_h3_airspace_ref",
    "coverage_marker",
)

#: Written once, read by both arms.
SHARED_INPUTS = (
    "osn_statevectors_v2",
    "osn_adsc_statevectors",
)

#: Everything the arms produce. Per-arm, because these are the comparison.
DERIVED = (
    "osn_tracks",
    "osn_tracks_clean",
    "opdi_flight_list",
    "opdi_endpoint_candidates",
    "opdi_flight_list_milestones_staging",
    "opdi_flight_events",
    "opdi_measurements",
)

ALL_TABLES = tuple(sorted(PASSTHROUGH + SHARED_INPUTS + DERIVED))

ARMS = ("base", "merged")
STEPS = ("01", "01b", "02", "02a", "03", "04", "04b")

_ORIG_S3_PATH = None
_ORIG_WRITE = None


def arm_path(arm: str, table: str) -> str:
    """Absolute S3 path for *table* in *arm*.

    Shared inputs ignore *arm*: that is what makes them shared.
    """
    if arm not in ARMS:
        raise ValueError(f"unknown arm {arm!r}; expected one of {ARMS}")
    if table in SHARED_INPUTS:
        return f"{ARM_PREFIX}/input/{table}"
    return f"{ARM_PREFIX}/{arm}/{table}"


def guard_write_path(path: str) -> None:
    """Refuse a write outside the research prefix.

    Not paranoia: this script runs steps 01 through 04b, which between them
    replace seven production tables, and the difference between the right and
    the wrong prefix is one string.
    """
    assert path.startswith(ARM_PREFIX), (
        f"refusing to write {path!r}: every table this script writes must sit "
        f"under {ARM_PREFIX!r}. A passthrough table is read-only and must "
        f"never be written by an arm run."
    )


def redirect_arm(arm: str, reference_warehouse: str) -> None:
    """Resolve derived and shared tables to research; pass reference through."""
    global _ORIG_S3_PATH, _ORIG_WRITE
    if _ORIG_S3_PATH is None:
        _ORIG_S3_PATH = StorageManager._s3_path
    if _ORIG_WRITE is None:
        _ORIG_WRITE = StorageManager.write_table
    orig_path, orig_write = _ORIG_S3_PATH, _ORIG_WRITE

    def _s3_path(self, table_name):
        if table_name in PASSTHROUGH:
            return orig_path(self, table_name)
        if table_name in SHARED_INPUTS or table_name in DERIVED:
            return arm_path(arm, table_name)
        raise RuntimeError(
            f"unclassified table {table_name!r}: add it to PASSTHROUGH, "
            f"SHARED_INPUTS or DERIVED in benchmarks/run_adsc_day.py. "
            f"Defaulting it to either side is how the A9 run produced a wrong "
            f"answer twice."
        )

    def write_table(self, df, table_name, mode="append", partition_by=None,
                    partition_values=None):
        guard_write_path(_s3_path(self, table_name))
        if ADSC_AIRFRAMES_ONLY and is_subset_target(table_name):
            df = keep_adsc_airframes(
                df,
                self.spark.read.parquet(arm_path(arm, "osn_adsc_statevectors")))
        return orig_write(self, df, table_name, mode, partition_by,
                          partition_values)

    StorageManager._s3_path = _s3_path
    StorageManager.write_table = write_table


def restore() -> None:
    """Undo the patch. Idempotent, so a test can call it in a finally block."""
    global _ORIG_S3_PATH, _ORIG_WRITE
    if _ORIG_S3_PATH is not None:
        StorageManager._s3_path = _ORIG_S3_PATH
        _ORIG_S3_PATH = None
    if _ORIG_WRITE is not None:
        StorageManager.write_table = _ORIG_WRITE
        _ORIG_WRITE = None


def run_arm(arm: str, day: date, executors: int, reference_warehouse: str,
            steps=STEPS, worldwide: bool = False) -> None:
    """One arm, one day, through the real per-day entry points."""
    cfg = OPDIConfig.for_environment("opensky", worldwide=worldwide)
    # The reference warehouse, not the coverage's own: `for_environment`
    # already routed `warehouse_path` to opdi-world under worldwide, and the
    # redirect below overrides it for everything except PASSTHROUGH anyway.
    # Setting it explicitly keeps one source of truth for where reference data
    # is read from.
    cfg.project.warehouse_path = reference_warehouse
    cfg.adsc.enabled = (arm == "merged")
    cfg.spark.executor_instances = str(executors)

    print("=" * 72)
    print(f"  arm        : {arm}  (adsc.enabled={cfg.adsc.enabled})")
    print(f"  day        : {day}")
    print(f"  reference  : {reference_warehouse}  (read-only)")
    print(f"  inputs     : {ARM_PREFIX}/input")
    print(f"  outputs    : {ARM_PREFIX}/{arm}")
    print(f"  segment'n  : {cfg.segmentation.method}")
    print(f"  coverage   : {cfg.coverage.label}  (bbox={cfg.coverage.bbox})")
    print(f"  steps      : {' '.join(steps)}")
    print("=" * 72, flush=True)

    redirect_arm(arm, reference_warehouse)
    spark = SparkSessionManager.create_session(
        app_name=f"adsc {arm} {day}", config=cfg, distributed=True)
    try:
        kwargs = {
            "airports_hex_path": str(
                REPO / "data" / "airport_hex" / "zones_res7_processed.parquet"),
            # Per-arm, and not optional.
            #
            # The ETL progress logs live on the driver's local disk, and one of
            # them gates a *remote* write: `build_endpoint_candidates` is
            # deliberately not keyed to `skip_if_processed`, so it skips on its
            # own day log and step 03 then reads the table it did not write.
            # The first attempt at this run died exactly there -- a production
            # campaign over 2026-06-01..14 had logged all fourteen days on this
            # pod, so the arm skipped the build and hit PATH_NOT_FOUND against
            # its own research prefix. Redirecting the tables is not enough;
            # the local state that claims they exist has to move too.
            #
            # Per *arm* rather than per run, because the two arms write
            # different tables under DERIVED and each has to build its own.
            "log_dir": str(REPO / "OPDI_live" / "logs" / f"adsc-{arm}"),
        }
        for step in steps:
            print(f"\n{'-' * 72}\n>>> arm {arm}  step {step}  day {day}\n"
                  f"{'-' * 72}", flush=True)
            run_day_step(spark, cfg, step, day, kwargs)
    finally:
        spark.stop()
        restore()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--day", required=True, help="YYYY-MM-DD")
    ap.add_argument("--arm", choices=(*ARMS, "both"), default="both")
    ap.add_argument("--steps", nargs="+", default=list(STEPS),
                    help="subset of the per-day steps, in order")
    ap.add_argument("--executors", type=int, default=8)
    ap.add_argument(
        "--worldwide", action="store_true", default=False,
        help="Drop the coverage bounding box and run over the whole feed. Off "
             "by default. ADS-C is a satellite feed and the European box "
             "excludes 81%% of it, so this is the arm that measures what ADS-C "
             "actually adds -- but APDF cannot score outside Europe, so a "
             "worldwide run is a coverage study with no scored arm.")
    ap.add_argument(
        "--adsc-airframes-only", action="store_true", default=False,
        help="Keep only the ADS-B of airframes present in the ADS-C table, "
             "under a separate prefix. The between-arm differences are "
             "unchanged and the totals are not the feed's. Build the merged "
             "arm first with --steps 01b 01 02 02a, then the base arm with "
             "--steps 02 02a: 01b only runs with ADS-C enabled.")
    ap.add_argument(
        "--reference-warehouse", default=None,
        help="where the read-only reference tables live. Defaults to "
             "opdi-prod for Europe and opdi-world for worldwide. opdi-prod, "
             "not opdi: the bare `opdi` prefix holds an unpartitioned scratch "
             "copy of the pipeline tables.")
    args = ap.parse_args()

    # Before anything resolves a path. A run that wrote worldwide rows into the
    # European research prefix while reporting itself worldwide is worse than
    # one that failed to start.
    coverage = "worldwide" if args.worldwide else "europe"
    set_coverage(coverage, adsc_airframes_only=args.adsc_airframes_only)
    if args.adsc_airframes_only:
        check_subset_steps(args.steps)
    reference = args.reference_warehouse or REFERENCE_BY_COVERAGE[coverage]

    day = date.fromisoformat(args.day)
    arms = ARMS if args.arm == "both" else (args.arm,)
    for arm in arms:
        run_arm(arm, day, args.executors, reference, args.steps,
                worldwide=args.worldwide)
    print(f"\nDone. {', '.join(arms)} written for {day} under {ARM_PREFIX} "
          f"(coverage: {coverage})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
