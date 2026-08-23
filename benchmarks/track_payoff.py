"""ADEP/ADES accuracy per segmentation arm, at V7's own recommended parameters.

The flight-list configuration is held fixed at what V7 recommends and only the
track assignment varies, so a change in accuracy is attributable to the
segmentation. Re-tuning V7's grid per arm would find a better joint optimum but
would multiply cluster time by the number of arms, and one job runs at a time.

If one arm wins clearly, re-tune V7's grid on that arm alone afterwards to check
the flight-list optimum has not moved -- one extra grid run, not eight.

**Staged, not persisted.** ``flight_list_v7.py`` reads a track table by name
through ``StorageManager.read_table``; an arm's output is only
``(icao24, event_time, track_id)``, so there is nothing on S3 for it to read
until this module writes one. ``track_methods.py`` deletes every assignment
table immediately after scoring it -- the bucket has single-digit GB of free
space and is shared with another project and a colleague's live study -- so
nothing is left there for this module to reuse either. This module therefore
stages every requested arm's assignment table itself, runs the flight list
against each in turn, and deletes all of them in a ``finally`` so a crash
midway cannot orphan a table.

**One Spark job at a time.** The staging session reads the shared track table
once, applies ``attach_airport_context`` once, and writes one assignment table
per arm -- ~0.3 GB each, ~2.4 GB for all eight -- then calls ``spark.stop()``
before the first ``flight_list_v7.py`` subprocess starts. The driver port is
pinned by a Kubernetes Service (see ``osn_sample.py``'s ``DRIVER_PORT``), so
two drivers cannot coexist, and each subprocess is run with
``subprocess.run(..., check=True)`` strictly sequentially -- never in
parallel, and never overlapping the staging session.

    python benchmarks/track_payoff.py --period 2025 --arms all \\
        --results-dir ../opdi-portal/papers/track-construction-v1/data
"""

import argparse
import csv
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

import osn_sample  # noqa: E402
import provenance  # noqa: E402
from osn_sample import build_spark, load_dotenv  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from track_methods import (  # noqa: E402
    ASSIGN_BASE,
    PERIODS,
    attach_airport_context,
    delete_assignment,
    s3_client,
)

from opdi.config import OPDIConfig  # noqa: E402
from opdi.pipeline.segmentation import SegmentationParams, assign_track_id  # noqa: E402
from opdi.pipeline.segmentation.methods import ARMS  # noqa: E402

#: The run name in flight_list_v7.py whose parameters V7 recommends: the
#: *shipped* configuration, ``(S(), S().adep_mode, S().ades_mode, clean)``
#: with ``S = DetectionConfig``. Two independent pieces of evidence make it
#: the right control:
#:
#: * ``src/opdi/config.py``'s ``DetectionConfig.trend_radius_nm`` defaults to
#:   30.0 NM -- the value V7's own sweep recommends. ``V7_RUN_LOG.md`` records
#:   that, ranked over both periods together, the research sweep
#:   "independently prefers 30 NM as an interior optimum" over the 20 NM V6
#:   previously shipped.
#: * ``V7_RUN_LOG.md:566`` records "the shipped configuration reproduced
#:   exactly. L12 -- the last rung, verified" -- i.e. ``shipped`` is not a
#:   guess at V7's recommendation, it is the ladder's own verified endpoint.
#:
#: There is **no** run called "recommended" in ``flight_list_v7.py``'s plan.
#: "recommended" is a *segmentation arm* name in
#: ``opdi.pipeline.segmentation.methods.ARMS`` -- a different namespace
#: entirely -- and using it here would silently benchmark an arm's own track
#: build against itself rather than against V7's recommended parameters.
V7_RECOMMENDED_RUN = "shipped"


def stage_assignments(period: str, arms: list, days: list, executors: int,
                       ui_port: int) -> None:
    """Write one (icao24, event_time, track_id) table per arm, then stop Spark.

    Reads the shared track table and applies ``attach_airport_context`` once,
    reused by every arm -- so an ADEP/ADES difference between arms cannot be
    an artefact of a different track build. That guarantee, and the
    ``PERIODS``/``attach_airport_context`` reuse, are exactly
    ``track_methods.py``'s streaming design; the write step below is the same
    one ``track_methods.run_arm`` performs, without the scoring or the
    immediate delete that follows it there -- payoff wants the assignment to
    survive until ``flight_list_v7.py`` has read it.

    ``spark.stop()`` runs in a ``finally`` here, not just on the success path:
    the K8s Service pins the driver port, so a session left open by a staging
    failure would block the very first subprocess main() is about to start.
    """
    osn_sample.UI_PORT = ui_port
    osn_sample.RESEARCH_EXECUTORS = executors
    spark = build_spark(6, "9g")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        p = PERIODS[period]
        use_days = days or p["days"]
        sv = spark.read.parquet(p["tracks"]).filter(F.to_date("event_time").isin(use_days))
        sv = attach_airport_context(spark, sv).cache()
        print(f"{sv.count():,} samples staged from {p['tracks']}")

        # SegmentationParams.from_config() rather than hand-listing the seven
        # fields the way track_methods.main() does: it is the one place the
        # engine's parameters are built from OPDIConfig, and
        # tests/test_segmentation_base.py already pins it against
        # SegmentationConfig's defaults -- reusing it means this module has no
        # copy of that field list to drift out of sync with either.
        params = SegmentationParams.from_config(OPDIConfig())

        for arm in arms:
            rule = ARMS[arm]()
            assigned = assign_track_id(sv, rule, params)
            out = f"{ASSIGN_BASE}/{arm}/{period}"
            (
                assigned.select("icao24", "event_time", "track_id")
                .write.mode("overwrite")
                .parquet(out)
            )
            print(f"  -> staged {out}")
    finally:
        spark.stop()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--period", choices=sorted(PERIODS), default="2025")
    ap.add_argument("--arms", nargs="+", default=["all"])
    ap.add_argument("--results-dir", type=Path, required=True)
    ap.add_argument("--executors", type=int, default=10)
    ap.add_argument(
        "--days", nargs="+", default=None,
        help="override the period's day list, e.g. a single day for a smoke test",
    )
    ap.add_argument("--ui-port", type=int, default=4058,
                     help="Spark UI port for the staging session (the "
                          "flight_list_v7.py subprocess picks its own)")
    args = ap.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
    arms = sorted(ARMS) if args.arms == ["all"] else args.arms
    unknown = [a for a in arms if a not in ARMS]
    if unknown:
        raise SystemExit(f"unknown arm(s): {unknown}")
    args.results_dir.mkdir(parents=True, exist_ok=True)

    load_dotenv()
    s3 = s3_client()
    rows = []
    try:
        # Step 1: stage every arm's assignment table. One Spark session, one
        # read of the shared tracks, stopped before any subprocess starts.
        stage_assignments(args.period, arms, args.days, args.executors, args.ui_port)

        # Step 2: run V7's own recommended parameters against each arm's
        # assignment, strictly sequentially -- never in parallel, and never
        # overlapping a Spark session of this module's own.
        for arm in arms:
            assign = f"{ASSIGN_BASE}/{arm}/{args.period}"
            out = args.results_dir / f"_payoff_{arm}_{args.period}.csv"
            cmd = [
                sys.executable, str(REPO / "benchmarks" / "flight_list_v7.py"),
                "--period", args.period,
                "--runs", V7_RECOMMENDED_RUN,
                "--track-assign", assign,
                "--results-dir", str(args.results_dir),
                "--out-name", out.name,
                "--executors", str(args.executors),
            ]
            print(f"\n=== {arm} ===\n  {' '.join(cmd)}")
            subprocess.run(cmd, check=True)  # sequential: one Spark job at a time

            with out.open() as fh:
                for r in csv.DictReader(fh):
                    r["arm"] = arm
                    r["period"] = args.period
                    rows.append(r)
    finally:
        # Every staged assignment is deleted here, regardless of how far the
        # loop above got: a crash on arm 5 must not leave arms 1-8's tables
        # (~0.3 GB each) parked on a bucket with single-digit GB of headroom
        # that is shared with another project. delete_assignment on an arm
        # that was never staged (e.g. staging itself failed first) finds
        # nothing and is a no-op.
        for arm in arms:
            n_del, freed = delete_assignment(s3, arm, args.period)
            print(f"  -- deleted {n_del} objects ({freed / 1e9:.3f} GB) "
                  f"for {arm}/{args.period}")

    if not rows:
        return

    final = args.results_dir / f"payoff_{args.period}.csv"
    with final.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=sorted(rows[0]))
        w.writeheader()
        w.writerows(rows)

    provenance.record(
        args.results_dir, final.name,
        script="benchmarks/track_payoff.py", argv=sys.argv[1:],
        code_paths=["benchmarks/track_payoff.py", "benchmarks/flight_list_v7.py",
                    "src/opdi/pipeline/flights.py",
                    "src/opdi/pipeline/segmentation/methods.py"],
        inputs={"arms": len(arms)},
        notes=f"V7 run '{V7_RECOMMENDED_RUN}' held fixed; only track_id varies.",
    )
    print(f"\n-> {final}")


if __name__ == "__main__":
    main()
