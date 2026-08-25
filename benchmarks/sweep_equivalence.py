"""Check that the sea-level arm of the paired sweep reproduces version 6's.

V6.2 retires V6's two ``trend_sweep`` jobs and serves their numbers from
``fl_sweep_*`` instead, on the grounds that ``trend_sweep_agl.py`` is a strict
superset of ``trend_sweep.py`` -- same FL_CAPS, MARGINS, RADII_NM,
PENALTIES_NM, and a vote cache carrying both datums' counts.

"On the grounds that" is not evidence. This job is the evidence: it joins the
two frames cell by cell and reports how many differ. If the answer is ever
anything but zero, the retirement was wrong and the paper says so, rather than
quoting a sweep it did not run against a claim it did not check.

Needs no cluster and no credentials -- it reads two committed CSVs.

    python benchmarks/sweep_equivalence.py --results-dir <dir> \\
        --pairs v6=<path>/trend_sweep_2025.csv,new=<path>/fl_sweep_2025.csv
"""

import argparse
import csv
from pathlib import Path

#: The grid coordinates that identify a cell. `datum` is deliberately absent:
#: it is the column the new frame adds, and the whole point is to compare a
#: frame that has it against one that does not.
KEY = ("stage", "stage2_role", "fl_cap", "radius_nm", "penalty_nm",
       "margin", "k", "legacy")

#: Compared as floats where possible, so 0.5 and 0.50 agree. The tolerance is
#: relative and tiny: these are the same computation, not two estimates of it.
TOL = 1e-9


def load(path: Path) -> dict:
    with open(path) as fh:
        rows = list(csv.DictReader(fh))
    missing = [k for k in KEY if rows and k not in rows[0]]
    if missing:
        raise SystemExit(f"{path} lacks key column(s): {', '.join(missing)}")
    return {tuple(r[k] for k in KEY): r for r in rows}


def compare(old_path: Path, new_path: Path) -> dict:
    old, new = load(old_path), load(new_path)
    shared = set(old) & set(new)
    differing = 0
    example = ""
    for key in sorted(shared):
        for col, a in old[key].items():
            if col in KEY:
                continue
            b = new[key].get(col)
            if b is None:
                continue
            try:
                x, y = float(a), float(b)
                same = abs(x - y) <= TOL * max(1.0, abs(x))
            except (TypeError, ValueError):
                same = a == b
            if not same:
                differing += 1
                if not example:
                    example = f"{col} at {dict(zip(KEY, key))}: {a} vs {b}"
    return {
        "old_rows": len(old),
        "new_rows": len(new),
        "shared_cells": len(shared),
        "only_in_old": len(set(old) - set(new)),
        "only_in_new": len(set(new) - set(old)),
        "differing_values": differing,
        "example": example,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results-dir", type=Path, required=True)
    ap.add_argument("--out-name", default="sweep_equivalence.csv")
    ap.add_argument("--pairs", nargs="+", required=True,
                    help="one per period, as "
                         "label=<old csv>,<new csv>")
    args = ap.parse_args()

    out = []
    for spec in args.pairs:
        label, _, paths = spec.partition("=")
        old_s, _, new_s = paths.partition(",")
        if not old_s or not new_s:
            raise SystemExit(f"malformed --pairs entry: {spec!r}")
        row = {"period": label}
        row.update(compare(Path(old_s), Path(new_s)))
        out.append(row)
        print(f"{label}: {row['shared_cells']} shared cells, "
              f"{row['differing_values']} differing values"
              + (f" -- e.g. {row['example']}" if row["example"] else ""))

    args.results_dir.mkdir(parents=True, exist_ok=True)
    dest = args.results_dir / args.out_name
    with open(dest, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(out[0]))
        w.writeheader()
        w.writerows(out)
    print(f"wrote {dest}")


if __name__ == "__main__":
    main()
