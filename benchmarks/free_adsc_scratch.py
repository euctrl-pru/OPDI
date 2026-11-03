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

**What it does NOT touch:** anything outside `opdi/research/adsc/`. Two
assertions enforce that -- one on the prefix argument, one on every key before
it is deleted.

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

#: Prefixes outside the ADS-C study that must never be reachable, asserted
#: rather than assumed. The study's own tables are deliberately NOT listed:
#: they are the thing this script exists to remove, and every number derived
#: from them is already staged as a committed CSV under
#: ``opdi-portal/papers/adsc-fusion-v1/data/`` with provenance. What is lost by
#: deleting them is the ability to re-derive those numbers without re-running
#: the arms (~5 h for three days), not the numbers themselves.
PROTECTED = (
    "opdi-prod/",
    "opdi-world/",
    "opdi/research/reference/",
    "opdi/research/benchmarker/",
)


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
    ap.add_argument("--yes", action="store_true",
                    help="actually delete; without it this only reports")
    ap.add_argument("--prefix", default=PREFIX,
                    help=f"narrow the target; must sit under {ROOT}")
    args = ap.parse_args()

    prefix = args.prefix
    assert prefix.startswith(ROOT), (
        f"refusing to run: {prefix!r} is not under the ADS-C prefix {ROOT!r}")
    for protected in PROTECTED:
        assert not protected.startswith(prefix) and not prefix.startswith(protected), (
            f"refusing to run: {prefix!r} and {protected!r} overlap")

    import boto3

    creds = _credentials()
    s3 = boto3.client(
        "s3", endpoint_url=ENDPOINT,
        aws_access_key_id=creds["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=creds["AWS_SECRET_ACCESS_KEY"])

    pages = s3.get_paginator("list_objects_v2").paginate(
        Bucket=BUCKET, Prefix=prefix)
    keys, total, count = [], 0, 0
    for page in pages:
        objs = page.get("Contents", [])
        if not objs:
            continue
        for o in objs:
            # Every key, not just the prefix constant: a server that ignored
            # the prefix would otherwise hand back the whole bucket.
            assert o["Key"].startswith(prefix), f"stray key {o['Key']!r}"
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
        print("Re-create with: for d in 2026-06-05 2026-06-06 2026-06-07; do "
              "python benchmarks/run_adsc_day.py --day $d --arm merged "
              "--steps 01 01b; done")
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
        assert key.startswith(prefix), f"stray key {key!r}"
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
