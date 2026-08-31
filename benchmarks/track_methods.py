"""Run one segmentation arm over one period and score it.

Each arm writes an **assignment table** -- ``(icao24, event_time, track_id)`` --
and nothing else. A materialised track table is ~10 GB per 3-day sample per
variant, so writing one per arm per period was never on the table.

**This runner streams: one arm at a time, deleted once it is scored.** The bucket
was measured at 96.89 GB of a ~100 GB quota -- 3.11 GB of headroom -- not the
~30 GB an earlier draft assumed (see ``DATASETS.md``, corrected 2026-08-23). A
write that exceeds quota fails *after* the job prints its results and before
anything persists, so leaving all sixteen assignment tables (8 arms x 2 periods)
on S3 at once was not a safe design even at "a few hundred MB each": for each
(arm, period) this
1. writes the assignment table,
2. reads it back, computes each track's true extent from the full table
   (``track_score.track_extents``), joins it to ground truth, scores it, and
   keeps only the metric row in memory,
3. deletes the assignment table's objects before moving to the next arm.

Peak S3 footprint is one assignment table, not sixteen. Pass
``--keep-assignments`` to skip the deletion, for a future run with headroom to
spare -- default is to delete.

**The abort gates on free space, not on the total**, and it is re-checked
before every arm (:func:`require_headroom`). Because the run streams, the
absolute total is the wrong quantity: what kills a run is a write that has
nowhere to go, and that depends only on what is free. The total also moves --
89.47 GB when this check was added, against the 96.89 GB measured on
2026-08-23 -- both because arms write and delete as they go and because the
bucket is shared, so a threshold on the total would be stale by the next run.

That streaming is not only a space saving. It means every arm is scored against
byte-identical input samples, so a difference between two arms cannot be an
artefact of a different track build -- which, with a ~10 GB non-deterministic
upstream, it otherwise could silently be.

**Deletion** loops single-object ``delete_object`` calls -- the OpenSky S3
endpoint rejects batch ``DeleteObjects`` with ``MissingContentMD5`` (DATASETS.md,
"Deleting objects") -- and every key is checked to start with this run's own
``track_assign/<arm>/<period>/`` prefix before it is deleted, so a bug here
cannot reach anything else in a bucket that also holds another project's
42.81 GB and a colleague's live study.

**Both halves of that streaming survive a crash.** The delete runs on the
failure path as well as the success path, so an exception in scoring cannot
orphan an assignment table on a bucket with single-digit GB of headroom; and
each arm's metric row is appended to the results CSV and flushed as soon as it
is scored, so a failure on arm 8 does not discard arms 1-7 -- whose tables are
already deleted, and whose numbers would otherwise cost a full cluster re-run.
``track_sweep.py`` writes its cells the same way.

    python benchmarks/track_methods.py --period 2025 --arms all \\
        --results-dir ../opdi-portal/papers/track-construction-v1/data

One Spark job at a time. Give a slow run more --executors; do not start a second.
"""

import argparse
import csv
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

import osn_sample  # noqa: E402
import provenance  # noqa: E402
import track_truth  # noqa: E402
from osn_sample import build_spark, load_dotenv  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from track_score import score_arm, track_extents  # noqa: E402

from opdi.config import OPDIConfig  # noqa: E402
from opdi.pipeline.segmentation import SegmentationParams, assign_track_id  # noqa: E402
from opdi.pipeline.segmentation.methods import ARMS  # noqa: E402

BUCKET = "eurocontrol"
S3_ENDPOINT = "https://s3.opensky-network.org"
ASSIGN_BASE = "s3a://eurocontrol/opdi/research/track_assign"
#: bucket-relative form of ASSIGN_BASE, used to scope every delete.
ASSIGN_PREFIX = "opdi/research/track_assign"

#: Same two periods as V6/V7, so a parameter chosen on one is validated on the
#: other and the ADEP/ADES deltas are comparable to published figures.
#:
#: Neither ``osn_tracks_clean`` nor ``research/tracks_clean`` is day-partitioned
#: on disk (measured 2026-08-23: both are a flat list of ``part-*.parquet``
#: files with no ``day=`` prefix) -- unlike ``research/tracks/``, which is.
#: ``main()`` reads the whole table and filters on ``to_date(event_time)``
#: instead of globbing a partition path that does not exist.
PERIODS = {
    "2026": {
        "months": ["202606"],
        "days": ["2026-06-05", "2026-06-06", "2026-06-07"],
        "tracks": "s3a://eurocontrol/opdi/research/tracks_clean_2026",
    },
    "2025": {
        "months": ["202506"],
        "days": ["2025-06-05", "2025-06-06", "2025-06-07"],
        "tracks": "s3a://eurocontrol/opdi/osn_tracks_clean",
    },
    "2024": {
        "months": ["202406"],
        "days": ["2024-06-05", "2024-06-06", "2024-06-07"],
        "tracks": "s3a://eurocontrol/opdi/research/tracks_clean",
    },
}

ZONES = "s3a://eurocontrol/opdi/h3_airport_detection_zones"
AIRPORTS = "s3a://eurocontrol/opdi/oa_airports"

#: Nominal quota on the OpenSky bucket. Not measured -- there is no API for it
#: -- so it is stated here as the one assumption the free-space check rests on.
#:
#: **Raised from 100 to 200 on 2026-08-27**, on the bucket owner's word. The
#: old figure was not merely conservative: `require_headroom` computes free
#: space as this constant minus the measured total, so an understated quota
#: aborts a run that had ample room, and it does so *before* the run starts --
#: which reads as a capacity problem rather than as a stale constant.
BUCKET_QUOTA_GB = 200.0
#: Refuse to start an arm with less than this free. One assignment table is
#: ~0.31 GB, so 2 GB is roughly six arms' worth of margin against a quota
#: failure that would otherwise strike after the job printed its results.
MIN_FREE_GB = 2.0

#: Radius within which a sample counts as "at an aerodrome" for arm A6. 5 NM is
#: the innermost band the zone table provides and is roughly an airport boundary.
NEAR_AIRPORT_NM = 5.0


def attach_airport_context(spark, sv):
    """Add ``near_airport`` and ``field_elev_ft`` for arm A6.

    The join lives here rather than in ``methods.py`` so that an arm stays a
    predicate over columns. Height above field elevation, not barometric
    altitude, is what makes A6 catch the aerodromes above 5,000 ft that legacy's
    fixed altitude threshold cannot see.

    The zone table's real (measured 2026-08-23) columns are ``apt_hex_id``,
    ``apt_ident`` and ``apt_max_c_radius_nm`` -- not ``h3_res_7``, ``aerodrome``
    and ``max_c_radius_nm`` as an earlier draft of this function assumed. They
    are renamed on the way in so the join key matches ``osn_tracks_clean``'s own
    ``h3_res_7`` column.
    """
    zones = (
        spark.read.parquet(ZONES)
        .filter(F.col("apt_max_c_radius_nm") <= NEAR_AIRPORT_NM)
        .select(
            F.col("apt_hex_id").alias("h3_res_7"),
            F.col("apt_ident").alias("_apt"),
        )
        .dropDuplicates(["h3_res_7"])
    )
    apts = spark.read.parquet(AIRPORTS).select(
        F.col("ident").alias("_apt"), F.col("elevation_ft").cast("double").alias("_elev")
    )
    return (
        sv.join(zones, "h3_res_7", "left")
        .join(apts, "_apt", "left")
        .withColumn("near_airport", F.col("_apt").isNotNull())
        .withColumn("field_elev_ft", F.coalesce(F.col("_elev"), F.lit(0.0)))
        .drop("_apt", "_elev")
    )


def s3_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def assign_prefix(arm_name: str, period: str) -> str:
    """Bucket-relative prefix for one (arm, period)'s assignment table."""
    return f"{ASSIGN_PREFIX}/{arm_name}/{period}/"


def prefix_size(s3, prefix: str) -> tuple:
    """(object_count, total_bytes) under *prefix*. Cheap: a listing, not a read."""
    n = total = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            n += 1
            total += o["Size"]
    return n, total


def bucket_total_gb(s3) -> float:
    """Whole-bucket usage, for the headroom check every run starts with."""
    _, total = prefix_size(s3, "")
    return total / 1e9


def require_headroom(s3, label: str) -> float:
    """Abort unless at least ``MIN_FREE_GB`` of quota is free. Returns free GB.

    **The gate is free space, not the absolute total.** The brief originally
    said "stop above 85 GB", written when the bucket was believed to be
    ~67 GB; it is 89.47 GB today, so a literal 85 GB abort would refuse to run
    at all -- and it would be gating on the wrong quantity anyway. This runner
    streams: peak footprint is *one* assignment table, measured at ~0.31 GB,
    not the sixteen an earlier draft assumed. What can actually kill a run is
    a write that exceeds quota, which fails after the job has printed its
    results and before anything persists, and that depends only on what is
    free.

    Checked **before every arm**, not once at startup: the total moves during a
    run -- each arm writes ~0.31 GB and gets it back on delete, ``--keep-assignments``
    never gets it back, and the bucket is shared with another project and a
    colleague's live study, so it can move for reasons this process does not
    control.
    """
    used = bucket_total_gb(s3)
    free = BUCKET_QUOTA_GB - used
    print(
        f"bucket ({label}): {used:.2f} GB used of ~{BUCKET_QUOTA_GB:.0f} GB, "
        f"{free:.2f} GB free"
    )
    if free < MIN_FREE_GB:
        raise SystemExit(
            f"aborting before {label}: only {free:.2f} GB free of the "
            f"~{BUCKET_QUOTA_GB:.0f} GB quota ({used:.2f} GB used), below the "
            f"{MIN_FREE_GB:.2f} GB this runner requires. One assignment table "
            "is ~0.31 GB and a write that exceeds quota fails after the job "
            "has printed its results. Free space, or pass a smaller --days."
        )
    return free


def delete_assignment(s3, arm_name: str, period: str) -> tuple:
    """Delete every object under this (arm, period)'s assignment prefix.

    Single-object ``delete_object`` calls, not batch ``DeleteObjects`` -- the
    OpenSky endpoint rejects the batch call with ``MissingContentMD5`` because
    botocore does not send the header it wants (DATASETS.md, "Deleting
    objects"). Every key is asserted to start with this run's own prefix before
    it is deleted: the bucket also holds another project's 42.81 GB and a
    colleague's live study, and there is no undo.
    """
    prefix = assign_prefix(arm_name, period)
    n = freed = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            key = o["Key"]
            assert key.startswith(prefix), f"refusing to delete outside {prefix}: {key}"
            s3.delete_object(Bucket=BUCKET, Key=key)
            n += 1
            freed += o["Size"]
    return n, freed


def release_assignment(s3, arm_name, period, out, keep_assignments, failed=False):
    """Delete this arm's assignment table -- on the failure path too.

    Called from both branches of :func:`run_arm`, because the table must not
    outlive the arm either way: the whole point of the streamed design is that
    peak S3 footprint is one assignment table, and an orphan left behind by a
    crash silently breaks that against a bucket with single-digit GB of
    headroom.

    ``failed`` says an exception is already propagating. In that case a *second*
    failure, in the delete itself, is reported and swallowed rather than allowed
    to replace the real traceback with an S3 error -- the prefix is printed so
    the orphan can be removed by hand. On the success path a delete failure is a
    genuine problem and propagates.
    """
    if keep_assignments:
        print(f"  -- keeping {out} (--keep-assignments)")
        return
    try:
        n_del, freed = delete_assignment(s3, arm_name, period)
        print(f"  -- deleted {n_del} objects ({freed / 1e9:.3f} GB) from {out}")
    except Exception as exc:  # noqa: BLE001 -- reported, never hidden
        if not failed:
            raise
        print(f"  !! ORPHAN LEFT at {assign_prefix(arm_name, period)}: {exc}")


def run_arm(spark, s3, arm_name, period, sv, gt, params, keep_assignments,
            score=score_arm, path_arm=None):
    """Build one arm's assignment table, derive something from it, delete it.

    **The orchestration lives here once**, and the two optional parameters are
    what keeps it that way. What varies between callers is a single line --
    what gets computed from ``(matched, extents)``. What does *not* vary is the
    part that is easy to get subtly wrong and expensive to have wrong in two
    places: the streamed write, the size accounting, and the release on the
    failure path as well as the success path.

    ``benchmarks/track_diagnostics.py``'s boundary histogram was briefly a
    verbatim copy of this function with one call swapped. That is the same
    defect class the histogram's own scoring half exists to avoid -- a
    duplicated invariant drifts, and here the copy that would be missed by a
    future fix to the orphan handling is the one that is *not* the paper's
    headline path.

    ``score`` is applied to ``(matched, extents, assign)`` and whatever it
    returns is passed straight back. It defaults to
    :func:`track_score.score_arm`, which yields one flat metric row and ignores
    the third argument.

    ``assign`` is handed over because some measurements cannot be made from
    ``matched`` at all. ``matched`` is ``overlap_join``'s output, restricted to
    the *airborne* interval ``[t_off, t_land]``, so anything about the aircraft
    on the ground -- taxi-out reception, for instance -- is structurally absent
    from it. A scorer needing that has to see the unfiltered table, and reading
    it a second time from S3 inside the scorer would both cost a re-read and
    risk pointing at a different table than the one just written.

    ``path_arm`` names the directory under ``ASSIGN_BASE``, defaulting to
    ``arm_name``. ``track_diagnostics`` passes ``diag_<arm>`` so that its
    deletes can never match a prefix this runner is mid-write on: one job at a
    time is the rule, and a distinct prefix is what makes a broken rule
    survivable.

    Returns ``(result, meta)``. ``meta`` carries the arm, the period and the
    assignment table's object count and byte size. It is returned beside the
    result rather than merged into it because ``result`` is only a dict for the
    default ``score`` -- the histogram's is a list of bins.
    """
    path_arm = path_arm or arm_name
    rule = ARMS[arm_name]()
    assigned = assign_track_id(sv, rule, params)
    out = f"{ASSIGN_BASE}/{path_arm}/{period}"
    (
        assigned.select("icao24", "event_time", "track_id")
        .write.mode("overwrite")
        .parquet(out)
    )
    n_obj, n_bytes = prefix_size(s3, assign_prefix(path_arm, period))
    print(f"  -> {out} ({n_obj} objects, {n_bytes / 1e9:.3f} GB)")

    # The delete used to be on the success path only. Scoring is the part that
    # can raise -- `homogeneity_completeness` collects the whole contingency
    # table to the driver -- so the one failure mode that matters left the
    # table behind on S3 exactly when nothing was going to come back for it.
    try:
        assign = spark.read.parquet(out)
        # Track extents come from the full assignment table, not from `matched`:
        # `overlap_join` restricts `matched` to samples already inside some
        # ground-truth flight's [t_off, t_land], so a track's real first/last
        # sample -- the thing that shows a merge extending past the interval --
        # would otherwise be structurally invisible. See
        # track_score.boundary_error's docstring.
        extents = track_extents(assign)
        matched = track_truth.overlap_join(assign, gt)
        result = score(matched, extents, assign)
    except BaseException:
        release_assignment(s3, path_arm, period, out, keep_assignments, failed=True)
        raise

    release_assignment(s3, path_arm, period, out, keep_assignments)
    return result, {
        "arm": arm_name, "period": period,
        "assign_objects": n_obj, "assign_bytes": n_bytes,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--period", choices=sorted(PERIODS), default="2025")
    ap.add_argument("--arms", nargs="+", default=["all"])
    ap.add_argument("--results-dir", type=Path, required=True)
    ap.add_argument("--out-name", default="arms.csv")
    ap.add_argument("--executors", type=int, default=10)
    ap.add_argument("--ui-port", type=int, default=4058)
    ap.add_argument(
        "--days", nargs="+", default=None,
        help="override the period's day list, e.g. a single day for a smoke test",
    )
    ap.add_argument(
        "--keep-assignments", action="store_true",
        help="do not delete each arm's assignment table after scoring it "
             "(default: delete -- see module docstring)",
    )
    args = ap.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
    load_dotenv()
    s3 = s3_client()
    # Once here, before Spark starts, so a bucket with no room costs a second
    # rather than a session; and again before each arm, in the loop below.
    require_headroom(s3, "startup")

    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(6, "9g")
    spark.sparkContext.setLogLevel("ERROR")

    p = PERIODS[args.period]
    days = args.days or p["days"]
    arms = sorted(ARMS) if args.arms == ["all"] else args.arms

    # Neither tracks table is day-partitioned on disk (see PERIODS' comment) --
    # read the whole table and filter on the timestamp column instead of
    # globbing a `day=` path that does not exist.
    sv = spark.read.parquet(p["tracks"]).filter(F.to_date("event_time").isin(days))
    sv = attach_airport_context(spark, sv).cache()
    gt = track_truth.load_flight_intervals(spark, p["months"], days).cache()
    print(f"{sv.count():,} samples, {gt.count():,} ground-truth flights")

    cfg = OPDIConfig().segmentation
    params = SegmentationParams(
        gap_minutes=cfg.gap_minutes,
        low_alt_gap_minutes=cfg.low_alt_gap_minutes,
        low_alt_ft=cfg.low_alt_ft,
        ground_dwell_minutes=cfg.ground_dwell_minutes,
        turnaround_max_height_ft=cfg.turnaround_max_height_ft,
        turnaround_max_speed_kt=cfg.turnaround_max_speed_kt,
        descent_floor_ft=cfg.descent_floor_ft,
    )

    args.results_dir.mkdir(parents=True, exist_ok=True)
    out = args.results_dir / args.out_name

    # Each arm's row is appended and flushed the moment it is scored -- the
    # same pattern track_sweep.py uses, so the two files read alike. Writing
    # the CSV after the loop meant a crash on arm 8 discarded arms 1-7, whose
    # assignment tables had already been deleted: the work was not recoverable
    # without re-running the whole ladder on the cluster.
    rows = []
    fh = out.open("w", newline="")
    writer = None
    try:
        for arm in arms:
            print(f"\n=== {arm} ({args.period}) ===")
            require_headroom(s3, f"arm {arm}")
            row, meta = run_arm(
                spark, s3, arm, args.period, sv, gt, params, args.keep_assignments
            )
            row.update(meta)
            rows.append(row)
            if writer is None:
                writer = csv.DictWriter(fh, fieldnames=sorted(row))
                writer.writeheader()
            writer.writerow(row)
            fh.flush()
            for k, v in row.items():
                print(f"  {k:20} {v}")
    finally:
        fh.close()

    provenance.record(
        args.results_dir,
        args.out_name,
        script="benchmarks/track_methods.py",
        argv=sys.argv[1:],
        code_paths=[
            "benchmarks/track_methods.py",
            "benchmarks/track_truth.py",
            "benchmarks/track_score.py",
            "src/opdi/pipeline/segmentation/base.py",
            "src/opdi/pipeline/segmentation/methods.py",
        ],
        inputs={"samples": sv.count(), "gt_flights": gt.count()},
        input_tables=[p["tracks"]],
        notes=f"near_airport radius for arm A6: {NEAR_AIRPORT_NM} NM. days={days}.",
    )
    print(f"\n-> {out}")
    print(
        f"bucket: {bucket_total_gb(s3):.2f} GB used of ~{BUCKET_QUOTA_GB:.0f} GB "
        "after this run"
    )
    spark.stop()


if __name__ == "__main__":
    main()
