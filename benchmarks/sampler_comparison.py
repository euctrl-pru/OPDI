"""
What the bin-based sampler is worth, measured across two full runs.

The end-to-end decimation harness cannot answer this any more. It compared two
candidate caches, one per sampler -- and once the bucket rule became the
production default there was no modulo-sampled cache left to compare against.
Worse, its two arms had drifted apart: the "modulo" arm was reading the table
this study had just rebuilt *with the bucket rule* on three days, while the
"bucket" arm still pointed at a one-day cache from the original decimation
study. Same ground truth, a third of the flights, and a table that reported the
bucket rule collapsing arrival coverage from 80.96% to 27.65%.

None of that was the sampler. It was two arms covering different periods.

This compares instead what the two samplers actually produced: the committed
outputs of the whole study under the modulo rule, frozen in
``data/modulo_baseline/``, against the current outputs under the bucket rule.
Same code, same grids, same three days, same ground truth -- the only
difference is the sampler. That is a cleaner A/B than the harness ever was,
because it spans every parameter cell rather than one operating point.

One caveat it cannot remove: rebuilding the sample changes ``track_id``, since
the rescued rows sit at track boundaries. So the benchmark's track-to-flight
alignment differs slightly between the arms, and part of any small delta is
that rather than the sampler.

    python benchmarks/sampler_comparison.py --results-dir <dir>
"""

import argparse
import csv
import statistics
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DATA = REPO.parent / "opdi-portal" / "papers" / "adep-ades-detection-v6" / "data"
BASELINE = DATA / "modulo_baseline"


def counts(row, side):
    n = int(float(row["n_ground_truth"]))
    correct = round(float(row[f"{side}_overall"]) * n)
    answered = round(float(row[f"{side}_coverage"]) * n)
    return correct, answered - correct


def load(path, key_cols):
    out = {}
    for r in csv.DictReader(open(path)):
        out[tuple(r.get(c, "") for c in key_cols)] = r
    return out


def compare(name, filename, key_cols, k=2.0):
    old_p, new_p = BASELINE / filename, DATA / filename
    if not (old_p.is_file() and new_p.is_file()):
        return []
    old, new = load(old_p, key_cols), load(new_p, key_cols)
    keys = set(old) & set(new)
    rows = []
    for side in ("adep", "ades"):
        ds, dc = [], []
        for key in keys:
            co, wo = counts(old[key], side)
            cn, wn = counts(new[key], side)
            ds.append((cn - k * wn) - (co - k * wo))
            dc.append(float(new[key][f"{side}_coverage"])
                      - float(old[key][f"{side}_coverage"]))
        rows.append({
            "comparison": name, "role": side, "cells": len(keys),
            "median_dscore": statistics.median(ds),
            "mean_dscore": round(statistics.mean(ds), 1),
            "min_dscore": min(ds), "max_dscore": max(ds),
            "median_dcoverage_pp": round(100 * statistics.median(dc), 4),
            "cells_improved": sum(1 for d in ds if d > 0),
            "cells_worsened": sum(1 for d in ds if d < 0),
        })
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results-dir", required=True)
    args = ap.parse_args()
    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)

    rows = []
    rows += compare("trend sweep", "trend_sweep_2025.csv",
                    ["stage", "fl_cap", "margin", "radius_nm", "penalty_nm",
                     "stage2_role"])
    rows += compare("endpoint grid", "sweep_radius_height_2025.csv",
                    ["radius_nm", "height_ft"])
    rows += compare("pipeline modes", "mode_comparison_v6.csv", ["run"])

    if not rows:
        raise SystemExit(
            "no comparable outputs found. The modulo baseline lives in "
            f"{BASELINE} and is frozen from the last run before the sampler "
            "changed; without it there is nothing to compare against.")

    with open(out / "sampler_comparison.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    print(f"{'comparison':<18}{'role':<6}{'cells':>7}{'med dscore':>12}"
          f"{'med dcov pp':>13}{'improved':>10}{'worsened':>10}")
    for r in rows:
        print(f"{r['comparison']:<18}{r['role']:<6}{r['cells']:>7}"
              f"{r['median_dscore']:>12,.0f}{r['median_dcoverage_pp']:>13.4f}"
              f"{r['cells_improved']:>10}{r['cells_worsened']:>10}")
    print(f"\nwritten to {out}")


if __name__ == "__main__":
    main()
