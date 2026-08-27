"""How far this study's inputs have drifted since version 6 recorded them.

Version 6.2 sets out to recompute version 6's study with one variable changed:
the datum. That claim is only true if nothing *else* moved underneath, and
something did — the endpoint candidate table was rebuilt after version 6 was
published.

This job measures the drift rather than asserting its absence. It compares each
input table's identity as recorded in version 6's manifest against the table as
it stands now, so the report can state plainly which of its numbers are
comparable to version 6's and which are not.

The distinction that matters:

* an object count falling by exactly one with **byte-identical** contents is
  the zero-byte directory marker that ``provenance.s3_identity`` stopped
  counting. Bookkeeping, no consequence.
* a changed byte count or a newer timestamp is a **rebuilt table**. Every
  figure derived from it moved, and no amount of reasoning about the datum
  explains the difference.

Needs credentials to list S3, but no cluster.

    python benchmarks/input_drift.py --results-dir <dir> \\
        --baseline <v6 paper>/data/_manifest.json
"""

import argparse
import csv
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

import provenance  # noqa: E402


def baseline_identities(manifest: Path) -> dict:
    """Each input table's identity, as the baseline study recorded it.

    Taken from the first entry that names the table: every entry recorded the
    same listing, because they were produced in one run against one bucket.
    """
    m = json.loads(manifest.read_text())
    out = {}
    for entry in m.values():
        if not isinstance(entry, dict):
            continue
        for prefix, ident in (entry.get("input_tables") or {}).items():
            if isinstance(ident, dict) and prefix not in out:
                out[prefix] = ident
    return out


def classify(old: dict, new: dict) -> str:
    if new.get("error"):
        return f"cannot check ({new['error']})"
    if not new.get("objects"):
        return "table now absent or empty"
    same_bytes = old.get("bytes") == new.get("bytes")
    same_newest = str(old.get("newest", ""))[:19] == str(new.get("newest", ""))[:19]
    if same_bytes and same_newest:
        d = new["objects"] - old["objects"]
        if d == 0:
            return "unchanged"
        # Byte-identical and same mtime, one fewer object: the directory marker.
        return "marker only" if d == -1 else f"object count {d:+d}, bytes identical"
    return "REBUILT"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results-dir", type=Path, required=True)
    ap.add_argument("--out-name", default="input_drift.csv")
    ap.add_argument("--baseline", type=Path, required=True,
                    help="the baseline study's _manifest.json")
    args = ap.parse_args()

    base = baseline_identities(args.baseline)
    if not base:
        raise SystemExit(f"no input_tables recorded in {args.baseline}")

    rows = []
    for prefix in sorted(base):
        old = base[prefix]
        new = provenance.s3_identity(prefix)
        verdict = classify(old, new)
        rows.append({
            "table": prefix.replace("s3a://eurocontrol/opdi/", ""),
            "baseline_objects": old.get("objects"),
            "now_objects": new.get("objects"),
            "baseline_bytes": old.get("bytes"),
            "now_bytes": new.get("bytes"),
            "baseline_newest": str(old.get("newest", ""))[:19],
            "now_newest": str(new.get("newest", ""))[:19],
            "verdict": verdict,
        })
        print(f"  {rows[-1]['table']:38s} {verdict}")

    args.results_dir.mkdir(parents=True, exist_ok=True)
    dest = args.results_dir / args.out_name
    with open(dest, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {dest}")


if __name__ == "__main__":
    main()
