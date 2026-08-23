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


def run_arm(spark, s3, arm_name, period, sv, gt, params, keep_assignments):
    rule = ARMS[arm_name]()
    assigned = assign_track_id(sv, rule, params)
    out = f"{ASSIGN_BASE}/{arm_name}/{period}"
    (
        assigned.select("icao24", "event_time", "track_id")
        .write.mode("overwrite")
        .parquet(out)
    )
    n_obj, n_bytes = prefix_size(s3, assign_prefix(arm_name, period))
    print(f"  -> {out} ({n_obj} objects, {n_bytes / 1e9:.3f} GB)")

    assign = spark.read.parquet(out)
    # Track extents come from the full assignment table, not from `matched`:
    # `overlap_join` restricts `matched` to samples already inside some
    # ground-truth flight's [t_off, t_land], so a track's real first/last
    # sample -- the thing that shows a merge extending past the interval --
    # would otherwise be structurally invisible. See
    # track_score.boundary_error's docstring.
    extents = track_extents(assign)
    matched = track_truth.overlap_join(assign, gt)
    row = score_arm(matched, extents)
    row.update({
        "arm": arm_name, "period": period,
        "assign_objects": n_obj, "assign_bytes": n_bytes,
    })

    if keep_assignments:
        print(f"  -- keeping {out} (--keep-assignments)")
    else:
        n_del, freed = delete_assignment(s3, arm_name, period)
        print(f"  -- deleted {n_del} objects ({freed / 1e9:.3f} GB) from {out}")

    return row


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
    print(f"bucket: {bucket_total_gb(s3):.2f} GB used of ~100 GB before this run")

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

    rows = []
    for arm in arms:
        print(f"\n=== {arm} ({args.period}) ===")
        rows.append(run_arm(spark, s3, arm, args.period, sv, gt, params, args.keep_assignments))
        for k, v in rows[-1].items():
            print(f"  {k:20} {v}")

    args.results_dir.mkdir(parents=True, exist_ok=True)
    out = args.results_dir / args.out_name

    with out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=sorted(rows[0]))
        w.writeheader()
        w.writerows(rows)

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
    print(f"bucket: {bucket_total_gb(s3):.2f} GB used of ~100 GB after this run")
    spark.stop()


if __name__ == "__main__":
    main()
