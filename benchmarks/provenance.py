"""
Stamp every analysis output with what produced it.

A CSV sitting in a paper's ``data/`` directory is, on its own, an assertion
with no evidence behind it. This module attaches the evidence: which script,
which arguments, which commit, which input tables and how many rows they held.

That is not paperwork. Earlier in this study three staged CSVs turned out to be
derived from tables written three days before, by code with different
parameters -- they looked freshly computed because they had a recent mtime and
nothing recorded what they were actually built from. A manifest makes that
failure loud instead of invisible.

Two things are recorded per output:

* **provenance** -- script, argv, git SHA, dirty flag, UTC timestamp, and the
  inputs the job declared, each with a row count.
* **a code fingerprint** -- a hash over the source files the job actually
  depends on. This is what lets :mod:`regenerate_v6` decide whether an output
  is stale: not "is the file old", which says nothing, but "was it produced by
  the code that is checked out right now".
"""

import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

#: Written beside the CSVs and committed with them.
MANIFEST_NAME = "_manifest.json"


def git_sha(short: bool = True) -> str:
    try:
        out = subprocess.run(
            ["git", "-C", str(REPO), "rev-parse", "--short" if short else "HEAD", "HEAD"],
            capture_output=True, text=True, timeout=10,
        )
        return out.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def git_dirty() -> bool:
    """True when tracked files differ from HEAD.

    Recorded rather than prevented: research is done on dirty trees, and a
    number produced from uncommitted code is still a number -- it just must not
    be mistaken for one that can be reproduced from a commit.
    """
    try:
        out = subprocess.run(
            ["git", "-C", str(REPO), "status", "--porcelain", "--untracked-files=no"],
            capture_output=True, text=True, timeout=10,
        )
        return bool(out.stdout.strip())
    except Exception:
        return False


def fingerprint(paths) -> str:
    """A hash over the source files an output depends on.

    Deliberately explicit rather than walking imports: the point is to state
    which code a result depends on, and a dependency worth re-running for is
    worth naming.
    """
    h = hashlib.sha256()
    for p in sorted(str(x) for x in paths):
        f = REPO / p
        h.update(p.encode())
        h.update(f.read_bytes() if f.is_file() else b"<missing>")
    return h.hexdigest()[:16]


def file_hash(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()[:16]


def s3_identity(prefix: str) -> dict:
    """A cheap identity for an S3 prefix: object count, bytes, newest mtime.

    This is what lets a *data* dependency participate in staleness. A
    fingerprint over source files answers "was this produced by the current
    code"; it cannot answer "was this produced from the current data", and the
    two failures look identical in the output. A job whose input table has
    grown, shrunk or been rewritten since its result was recorded is stale in
    exactly the way that matters.

    Deliberately not a checksum of the contents: these prefixes hold tens of
    gigabytes, and reading them to decide whether to read them defeats the
    purpose.
    """
    import os

    try:
        import boto3
    except ImportError:
        return {"error": "boto3 unavailable"}

    env = REPO / ".env"
    if env.is_file():
        for line in env.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())
    if "AWS_ACCESS_KEY_ID" not in os.environ:
        return {"error": "no credentials"}

    path = prefix.replace("s3a://", "").replace("s3://", "")
    bucket, _, key = path.partition("/")
    try:
        s3 = boto3.client(
            "s3", endpoint_url=os.environ.get(
                "OPDI_S3_ENDPOINT", "https://s3.opensky-network.org"),
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )
        n = total = 0
        newest = ""
        for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=key.rstrip("/") + "/"):
            for o in page.get("Contents", []):
                n += 1
                total += o["Size"]
                ts = o["LastModified"].isoformat()
                if ts > newest:
                    newest = ts
        return {"objects": n, "bytes": total, "newest": newest}
    except Exception as exc:  # noqa: BLE001
        return {"error": f"{type(exc).__name__}: {exc}"}


def load_manifest(data_dir: Path) -> dict:
    p = Path(data_dir) / MANIFEST_NAME
    if not p.is_file():
        return {}
    try:
        return json.loads(p.read_text())
    except json.JSONDecodeError:
        return {}


def save_manifest(data_dir: Path, manifest: dict) -> None:
    p = Path(data_dir) / MANIFEST_NAME
    p.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")


def record(
    data_dir: Path,
    output: str,
    script: str,
    argv: list,
    code_paths: list,
    inputs: dict = None,
    notes: str = "",
    input_tables: list = None,
) -> dict:
    """Add or replace one output's entry in the manifest.

    ``inputs`` maps a human-readable name to a row count (or any scalar worth
    pinning). It is what makes "this was computed against three days" checkable
    rather than assumed.
    """
    data_dir = Path(data_dir)
    manifest = load_manifest(data_dir)
    out_path = data_dir / output
    entry = {
        "script": script,
        "argv": list(argv),
        "git_sha": git_sha(),
        "git_dirty": git_dirty(),
        "produced_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "code_fingerprint": fingerprint(code_paths),
        "code_paths": sorted(str(p) for p in code_paths),
        "inputs": inputs or {},
        "input_tables": {p: s3_identity(p) for p in (input_tables or [])},
        "notes": notes,
    }
    if out_path.is_file():
        entry["sha256_16"] = file_hash(out_path)
        entry["bytes"] = out_path.stat().st_size
    manifest[output] = entry
    save_manifest(data_dir, manifest)
    return entry


def inputs_changed(entry: dict, inputs: list) -> str:
    """Reason string if a declared input table differs from when recorded."""
    if not inputs:
        return ""
    recorded = (entry or {}).get("input_tables") or {}
    for prefix in inputs:
        was = recorded.get(prefix)
        if was is None:
            return f"input {prefix} was not recorded"
        if was.get("error") or "error" in (now := s3_identity(prefix)):
            # Cannot tell without credentials. Silence here would be a lie in
            # the safe-looking direction, so it is reported rather than passed.
            return ""
        if now.get("objects") != was.get("objects") or now.get("bytes") != was.get("bytes"):
            return (f"input {prefix} changed "
                    f"({was.get('objects')} objects -> {now.get('objects')})")
    return ""


def is_stale(data_dir: Path, output: str, code_paths: list, inputs: list = None) -> tuple:
    """(stale, reason) for one output.

    Stale means "not produced by the code currently checked out", not "old".
    A file's age says nothing; its fingerprint says everything.
    """
    data_dir = Path(data_dir)
    out_path = data_dir / output
    if not out_path.is_file():
        return True, "output missing"
    manifest = load_manifest(data_dir)
    entry = manifest.get(output)
    if entry is None:
        return True, "no manifest entry -- provenance unknown"
    if entry.get("code_fingerprint") != fingerprint(code_paths):
        return True, "code changed since this was produced"
    if entry.get("sha256_16") and entry["sha256_16"] != file_hash(out_path):
        return True, "file modified after it was recorded"
    why = inputs_changed(entry, inputs or [])
    if why:
        return True, why
    return False, "current"
