"""Arm A1 -- the production algorithm at swept parameters.

A1 is not new code. It is ``legacy()`` at parameters other than production's, so
the sweep measures the *shipped algorithm's* tuning headroom and nothing else. If
A1's best cell is close to A0, the algorithm is already well tuned and the study's
value lies entirely in the structural arms -- which is a finding, not a failure.

The grid is deliberately wider than plausible on the low side: V6 stopped a sweep
with the curve still rising and read the result as "higher is always better" when
it had simply not been followed far enough.

The objective is ``clean_match_pct``, not ``v_measure``. Across the eight arms
already benchmarked (Task 6), v_measure spans only 0.995952-0.997642 -- a range
of 0.0017 -- while clean_match_pct spans 39.84-88.48, and worse, v_measure
*misranks*: it places the best arm second-from-bottom. v_measure is still
recorded per cell for reference; it is never the thing selected on.

**This grid is 235 cells and runs for hours.** Each cell is scored and appended
to the output CSV immediately, with a flush after every row, so a crash or a
killed job loses at most the cell in flight. ``--resume`` reads whatever the
output file already has and skips any (gap_minutes, low_alt_gap_minutes,
low_alt_ft) triple already present, so a resumed run picks up where the last one
stopped rather than repeating cells that already cost cluster time. The
provenance record and the final ``best:`` line are computed from every row in the
file, including ones written by an earlier, interrupted run -- not only the rows
this invocation computed.

    python benchmarks/track_sweep.py --period 2025 \\
        --results-dir ../opdi-portal/papers/track-construction-v1/data
"""

import argparse
import csv
import itertools
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
from track_methods import PERIODS, attach_airport_context  # noqa: E402
from track_score import score_arm, track_extents  # noqa: E402

from opdi.pipeline.segmentation import SegmentationParams, assign_track_id  # noqa: E402
from opdi.pipeline.segmentation.methods import ARMS  # noqa: E402

#: Production is 30. Swept from well below to well above so the optimum is
#: interior to the grid rather than at its edge.
GAP_MIN = [10, 15, 20, 25, 30, 40, 50, 60, 90]
#: Production is 15.
LOW_ALT_GAP_MIN = [3, 5, 10, 15, 20, 30]
#: Production is 1524 m = 5,000 ft. Aviation units here, per the unit rule.
LOW_ALT_FT = [1000, 2500, 5000, 7500, 10000]

#: The four-column key identifying a grid cell, used for both the CSV
#: fieldnames' identity and the resume skip-set.
CELL_KEYS = ("gap_minutes", "low_alt_gap_minutes", "low_alt_ft",
             "callsign_lookback_minutes")


def cell_key(row: dict) -> tuple:
    """The grid-cell identity, tolerant of CSVs written before the 4th axis.

    A row from a three-axis sweep has no `callsign_lookback_minutes` at all and
    must read as the unset cell -- otherwise `--resume` against any committed V1
    sweep file raises KeyError instead of skipping.
    """
    out = []
    for k in CELL_KEYS:
        v = row.get(k, "")
        out.append(None if v in ("", None, "None") else float(v))
    return tuple(out)


def load_existing_rows(out: Path) -> list:
    """Rows already written to *out*, or ``[]`` if it does not exist yet."""
    if not out.is_file():
        return []
    with out.open(newline="") as fh:
        return list(csv.DictReader(fh))


def done_cells(rows: list) -> set:
    """The set of grid-cell keys already present in *rows*."""
    return {cell_key(r) for r in rows}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--period", choices=sorted(PERIODS), default="2025")
    ap.add_argument("--results-dir", type=Path, required=True)
    ap.add_argument("--out-name", default=None)
    ap.add_argument("--executors", type=int, default=10)
    ap.add_argument("--ui-port", type=int, default=4059)
    ap.add_argument(
        "--days", nargs="+", default=None,
        help="override the period's day list, e.g. a single day to locate the "
             "optimum before committing to the full sweep",
    )
    ap.add_argument(
        "--resume", action="store_true",
        help="skip grid cells already present in an existing output CSV, and "
             "append the new cells to it instead of overwriting it",
    )
    # Grid overrides. Step 3 requires extending the grid whenever a winning value
    # lands on an edge -- "an optimum at an edge is not an optimum; it is where
    # you stopped looking". Extending it by editing the constants above would
    # leave the committed defaults disagreeing with the run that produced the
    # results, and nothing in the CSV would say which grid it came from. As
    # arguments, the extension lands in the provenance argv instead.
    ap.add_argument("--grid-gap", nargs="+", type=int, default=None,
                    help=f"override GAP_MIN (default {GAP_MIN})")
    ap.add_argument("--grid-low-alt-gap", nargs="+", type=int, default=None,
                    help=f"override LOW_ALT_GAP_MIN (default {LOW_ALT_GAP_MIN})")
    ap.add_argument("--grid-low-alt-ft", nargs="+", type=int, default=None,
                    help=f"override LOW_ALT_FT (default {LOW_ALT_FT})")
    ap.add_argument("--method", default="legacy", choices=sorted(ARMS),
                    help="which arm to sweep; the grid is the same for all of "
                         "them, but only `recommended` reads --grid-lookback")
    ap.add_argument("--grid-lookback", nargs="+", type=float, default=None,
                    help="callsign_lookback_minutes values. Omit to leave it "
                         "unset, i.e. following gap_minutes -- which is what "
                         "every cell of the legacy sweep did.")
    args = ap.parse_args()
    out_name = args.out_name or f"sweep_{args.period}.csv"

    sys.stdout.reconfigure(line_buffering=True)
    load_dotenv()

    args.results_dir.mkdir(parents=True, exist_ok=True)
    out = args.results_dir / out_name

    existing_rows = load_existing_rows(out) if args.resume else []
    skip = done_cells(existing_rows)
    if skip:
        print(f"--resume: {len(skip)} cells already in {out}, will be skipped")

    n_gt = None  # populated only when this run actually loads ground truth

    gap_grid = args.grid_gap or GAP_MIN
    low_gap_grid = args.grid_low_alt_gap or LOW_ALT_GAP_MIN
    low_ft_grid = args.grid_low_alt_ft or LOW_ALT_FT
    lookback_grid = args.grid_lookback if args.grid_lookback else [None]
    grid = list(itertools.product(gap_grid, low_gap_grid, low_ft_grid, lookback_grid))
    # a low-altitude rule looser than the general one is inert
    grid = [c for c in grid if c[1] <= c[0]]
    todo = [cell for cell in grid if cell not in skip]
    print(f"{len(grid)} valid cells, {len(todo)} to compute")

    if not todo:
        print("nothing to do")
        rows = existing_rows
    else:
        osn_sample.UI_PORT = args.ui_port
        osn_sample.RESEARCH_EXECUTORS = args.executors
        spark = build_spark(6, "9g")
        spark.sparkContext.setLogLevel("ERROR")

        p = PERIODS[args.period]
        days = args.days or p["days"]
        sv = spark.read.parquet(p["tracks"]).filter(F.to_date("event_time").isin(days))
        sv = attach_airport_context(spark, sv).cache()
        gt = track_truth.load_flight_intervals(spark, p["months"], days).cache()
        n_sv = sv.count()
        n_gt = gt.count()
        print(f"{n_sv:,} samples, {n_gt:,} ground-truth flights")

        rule = ARMS[args.method]()

        # Header fieldnames are fixed up front from the first computed row plus
        # the cell-key columns, so every append uses the same schema regardless
        # of whether the file already existed.
        fieldnames = None
        if existing_rows:
            fieldnames = list(existing_rows[0].keys())
        write_header = not (args.resume and out.is_file())

        mode = "a" if (args.resume and out.is_file()) else "w"
        fh = out.open(mode, newline="")
        writer = None
        try:
            for i, (g, lg, lft, lb) in enumerate(todo, 1):
                params = SegmentationParams(
                    gap_minutes=float(g), low_alt_gap_minutes=float(lg),
                    low_alt_ft=float(lft),
                    callsign_lookback_minutes=lb,
                )
                assigned = (
                    assign_track_id(sv, rule, params)
                    .select("icao24", "event_time", "track_id")
                )
                extents = track_extents(assigned)
                matched = track_truth.overlap_join(assigned, gt)
                row = score_arm(matched, extents)
                row.update({
                    "gap_minutes": g, "low_alt_gap_minutes": lg, "low_alt_ft": lft,
                    "callsign_lookback_minutes": lb,
                    "period": args.period,
                })

                if writer is None:
                    fieldnames = fieldnames or sorted(row)
                    writer = csv.DictWriter(fh, fieldnames=fieldnames)
                    if write_header:
                        writer.writeheader()
                writer.writerow(row)
                fh.flush()

                print(
                    f"  [{i}/{len(todo)}] gap={g} lowgap={lg} lowalt={lft}ft "
                    f"lookback={lb}  clean={row['clean_match_pct']:.2f}%"
                )
        finally:
            fh.close()

        spark.stop()
        rows = load_existing_rows(out)

    if not rows:
        print("no rows in output -- nothing to report")
        return

    inputs = {"cells": len(rows)}
    if n_gt is not None:
        inputs["gt_flights"] = n_gt
    provenance.record(
        args.results_dir, out_name,
        script="benchmarks/track_sweep.py", argv=sys.argv[1:],
        code_paths=["benchmarks/track_sweep.py", "benchmarks/track_truth.py",
                    "benchmarks/track_score.py",
                    "src/opdi/pipeline/segmentation/base.py",
                    "src/opdi/pipeline/segmentation/methods.py"],
        inputs=inputs,
        input_tables=[PERIODS[args.period]["tracks"]],
    )

    best = max(rows, key=lambda r: float(r["clean_match_pct"]))
    print(
        f"\nbest: gap={best['gap_minutes']} lowgap={best['low_alt_gap_minutes']} "
        f"lowalt={best['low_alt_ft']}ft  clean={float(best['clean_match_pct']):.2f}%  "
        f"v={float(best['v_measure']):.4f}"
    )
    print(f"-> {out}")


if __name__ == "__main__":
    main()
