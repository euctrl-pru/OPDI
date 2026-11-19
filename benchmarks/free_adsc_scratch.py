#!/usr/bin/env python
"""Delete the ADS-C study's tables under `opdi/research/adsc/`.

The `eurocontrol` bucket has a MinIO quota, and at the quota every write is
refused -- including the benchmarker's cache build.

**What it deletes:** by default everything under `opdi/research/adsc/`: the
shared inputs, both arms' derived tables and the benchmarker cache. `--prefix`
narrows that to anything beneath it.

**What is lost:** the staged CSVs under
`opdi-portal/papers/adsc-fusion-v1/data/` survive, but re-deriving them means
re-running the arms (about 5 h for three days) and `regenerate_adsc_v1.py
--check` cannot tell that the tables are gone. To free only the re-fetchable
state vectors, pass `--prefix opdi/research/adsc/input/osn_statevectors_v2/`.

**What it does NOT touch:** anything outside `opdi/research/adsc/`. Two checks
enforce that, one on the prefix argument and one on every key before it is
deleted. They raise rather than assert, so `python -O` cannot remove them.

    python benchmarks/free_adsc_scratch.py          # dry run: list and total
    python benchmarks/free_adsc_scratch.py --yes    # actually delete
"""

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

#: The only prefix family this script may ever touch. A --prefix argument may
#: narrow it but never escape it.
ROOT = "opdi/research/adsc/"
PREFIX = ROOT
BUCKET = "eurocontrol"
ENDPOINT = "https://s3.opensky-network.org"


def validate_prefix(prefix: str) -> str:
    """The prefix, if it sits under :data:`ROOT`. Raises rather than asserts."""
    if not prefix.startswith(ROOT):
        raise SystemExit(f"refusing to run: {prefix!r} is not under {ROOT!r}")
    return prefix


def assert_in_scope(key: str, prefix: str) -> None:
    """Per key: a server ignoring the prefix would otherwise hand back the bucket."""
    if not key.startswith(prefix):
        raise SystemExit(f"refusing to delete stray key {key!r}")


def _credentials() -> dict:
    env_file = REPO / ".env"
    if not env_file.is_file():
        sys.exit(f"no .env at {env_file}; S3 credentials are required")
    out = {}
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            out[key.strip()] = value.strip()
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--yes", action="store_true", help="actually delete; without it this only reports"
    )
    ap.add_argument("--prefix", default=PREFIX, help=f"narrow the target; must sit under {ROOT}")
    args = ap.parse_args()

    prefix = validate_prefix(args.prefix)

    import boto3

    creds = _credentials()
    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=creds["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=creds["AWS_SECRET_ACCESS_KEY"],
    )

    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix)
    keys, total, count = [], 0, 0
    for page in pages:
        objs = page.get("Contents", [])
        if not objs:
            continue
        for o in objs:
            assert_in_scope(o["Key"], prefix)
        keys += [o["Key"] for o in objs]
        total += sum(o["Size"] for o in objs)
        count += len(objs)

    print(f"prefix : s3://{BUCKET}/{prefix}")
    print(f"objects: {count:,}")
    print(f"size   : {total / 1e9:.2f} GB")

    if not count:
        print("\nnothing to delete -- already freed")
        return 0
    if not args.yes:
        print("\nDRY RUN. Re-run with --yes to delete.")
        print(
            "Re-create with: for d in 2026-06-05 2026-06-06 2026-06-07; do "
            "python benchmarks/run_adsc_day.py --day $d --arm both; "
            "done  (about 5 h)"
        )
        return 0

    # One key per request, not the batch `delete_objects` API.
    #
    # MinIO requires a Content-MD5 header on multi-object DeleteObjects, and
    # current botocore sends `x-amz-sdk-checksum-algorithm` in its place, so
    # that call fails against this endpoint with `MissingContentMD5` before
    # deleting anything. Single-object DeleteObject carries no such
    # requirement. Six hundred sequential requests is a few seconds, and the
    # per-key loop also means a failure part-way leaves a partially freed
    # prefix rather than an ambiguous batch result.
    for i, key in enumerate(keys, 1):
        assert_in_scope(key, prefix)
        s3.delete_object(Bucket=BUCKET, Key=key)
        if i % 100 == 0 or i == count:
            print(f"  deleted {i:,}/{count:,}", flush=True)
    print(f"\ndeleted {count:,} objects, freed {total / 1e9:.2f} GB")

    try:
        probe = "opdi/research/adsc/_quota_probe"
        s3.put_object(Bucket=BUCKET, Key=probe, Body=b"x" * 1024)
        s3.delete_object(Bucket=BUCKET, Key=probe)
        print("write probe: OK -- the bucket accepts writes again")
    except Exception as exc:  # noqa: BLE001
        print(f"write probe STILL FAILING: {type(exc).__name__}: {exc}")
        print("More space is needed than this prefix held.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
