#!/usr/bin/env python
"""Mirror committed ground-truth parquet to S3, where executors can read it.

The ground truth lives in ``opdi/reference/`` under git-lfs, which is the right
home for it: versioned, reviewable, and pulled with the repo. But a remote
Spark executor cannot read the driver's local filesystem, so every benchmark
reads the same files from ``s3a://eurocontrol/opdi/research/reference/``
instead (``adep_ades.py:45-48``). Something has to put them there.

That something used to be a hand-typed ``aws s3 cp``, which is not available on
every machine that can run the extraction, leaves no record of what was
uploaded when, and is exactly the kind of step ``build_candidates.py`` argues
against in its own docstring: an inline one-liner beyond the reach of every
check.

Deliberately **not** a ``runner.py`` step. Reference data changes when someone
re-extracts it from PRISME -- a handful of times a year -- not once per
pipeline run, and a full ``opdi run`` re-uploading a quarter of a gigabyte of
unchanged parquet every month would be waste, not reproducibility.

Usage::

    python benchmarks/mirror_reference.py --dry-run
    python benchmarks/mirror_reference.py
    python benchmarks/mirror_reference.py --include 'apdf_full_*.parquet'
"""

import argparse
import fnmatch
import os
import sys
from pathlib import Path

# Importing the config loads .env, which is where the S3 credentials live.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import opdi.config  # noqa: F401  -- imported for its .env side effect
import provenance

REPO = Path(__file__).resolve().parent.parent
LOCAL_DIR = REPO / "reference"
BUCKET = "eurocontrol"
PREFIX = "opdi/research/reference"
ENDPOINT = "https://s3.opensky-network.org"

#: The bucket is shared and has a hard quota. Refuse to start an upload that
#: would obviously overrun it rather than discovering the failure at the end,
#: which is how S3A reports it -- after the work, before the persist.
QUOTA_GB = 100.0
HEADROOM_GB = 2.0

#: A git-lfs pointer is ~130 bytes of text. Uploading one produces an object
#: that exists, has a plausible name, and reads as an empty or corrupt table --
#: the worst failure mode available, because nothing errors.
LFS_MAGIC = b"version https://git-lfs.github.com/spec/v1"


def _client():
    import boto3

    missing = [k for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY") if not os.environ.get(k)]
    if missing:
        sys.exit(f"Missing credentials: {', '.join(missing)}. Expected in {REPO / '.env'}.")
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def is_lfs_pointer(path: Path) -> bool:
    with open(path, "rb") as fh:
        return fh.read(len(LFS_MAGIC)) == LFS_MAGIC


def remote_sizes(client) -> dict:
    """Object key -> size for everything already under the prefix."""
    sizes = {}
    token = None
    while True:
        kwargs = {"Bucket": BUCKET, "Prefix": PREFIX + "/"}
        if token:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        for obj in page.get("Contents", []):
            sizes[obj["Key"]] = obj["Size"]
        if not page.get("IsTruncated"):
            return sizes
        token = page["NextContinuationToken"]


def bucket_bytes(client) -> int:
    total, token = 0, None
    while True:
        kwargs = {"Bucket": BUCKET}
        if token:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        for obj in page.get("Contents", []):
            total += obj["Size"]
        if not page.get("IsTruncated"):
            return total
        token = page["NextContinuationToken"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--include", default="*.parquet",
                    help="glob over reference/ filenames (default: *.parquet)")
    ap.add_argument("--dry-run", action="store_true",
                    help="say what would be uploaded and stop")
    ap.add_argument("--force", action="store_true",
                    help="re-upload even when the remote size already matches")
    args = ap.parse_args()

    local = sorted(p for p in LOCAL_DIR.iterdir()
                   if p.is_file() and fnmatch.fnmatch(p.name, args.include))
    if not local:
        sys.exit(f"Nothing in {LOCAL_DIR} matches {args.include!r}.")

    # Check every file before uploading any: a half-mirrored set is worse than
    # an un-mirrored one, because it looks complete.
    pointers = [p for p in local if is_lfs_pointer(p)]
    if pointers:
        sys.exit(
            "These are git-lfs pointers, not data:\n  "
            + "\n  ".join(p.name for p in pointers)
            + f"\n\nRun:  git lfs pull --include='reference/{args.include}'"
        )

    client = _client()
    remote = remote_sizes(client)
    used = bucket_bytes(client)
    print(f"Bucket {BUCKET}: {used / 2**30:.2f} GB used of ~{QUOTA_GB:.0f} GB "
          f"(shared -- other projects' data is in this figure).")

    todo, skipped = [], []
    for p in local:
        key = f"{PREFIX}/{p.name}"
        if not args.force and remote.get(key) == p.stat().st_size:
            skipped.append(p)
        else:
            todo.append((p, key))

    for p in skipped:
        print(f"  current   {p.name}")
    if not todo:
        print("\nEverything is already mirrored.")
        return 0

    adding = sum(p.stat().st_size for p, _ in todo
                 if f"{PREFIX}/{p.name}" not in remote)
    print()
    for p, key in todo:
        verb = "replace " if f"{PREFIX}/{p.name}" in remote else "upload  "
        print(f"  {verb}  {p.name}  ({p.stat().st_size / 2**20:.1f} MB)  -> s3://{BUCKET}/{key}")

    projected = (used + adding) / 2**30
    if projected > QUOTA_GB - HEADROOM_GB:
        sys.exit(f"\nWould take the bucket to {projected:.2f} GB, inside the "
                 f"{HEADROOM_GB:.0f} GB headroom below the ~{QUOTA_GB:.0f} GB quota. "
                 "Free space first -- an over-quota write fails at the end of a job, "
                 "after the work and before the persist.")

    if args.dry_run:
        print(f"\nDry run. Projected bucket: {projected:.2f} GB.")
        return 0

    for p, key in todo:
        print(f"\nuploading {p.name} ...")
        client.upload_file(str(p), BUCKET, key)
        got = client.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
        want = p.stat().st_size
        if got != want:
            sys.exit(f"  size mismatch after upload: {got} != {want}")
        print(f"  ok, {got:,} bytes verified")

    ident = provenance.s3_identity(f"s3a://{BUCKET}/{PREFIX}")
    print(f"\n{PREFIX}: {ident}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
