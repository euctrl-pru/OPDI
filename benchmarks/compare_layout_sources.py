"""Does the local extract produce the same layout grid as Overpass did?

Section A compares H3 cell sets per aeroway type for airports whose
Overpass-built grid was saved during the flight-events-v4 campaign. Cell
identity is exact, so the comparison is a set operation rather than a
tolerance.

Section B builds the grid for study aerodromes that have **no** Overpass
baseline -- because the Overpass path never produced one for them, which is
the premise of this whole plan. There is nothing to diff those against, so
Section B reports what the PBF path produces (per-family cell counts,
assignment method) rather than a pass/fail. It is evidence of coverage, not
of correctness -- the correctness claim rests entirely on Section A.

Run:
    .venv310/bin/python benchmarks/compare_layout_sources.py \
        --pbf /home/jupyter/work/osm/europe-latest.osm.pbf \
        --parts /home/jupyter/work/osm/overpass_baseline \
        --airports EBBR LSZH EICK EGGD \
        --build-only EDDP LPPT EGNT EGCC UGKO
"""
import argparse
import os
import sys
import time

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pbf", required=True)
    ap.add_argument("--parts", required=True,
                    help="directory of <ICAO>.parquet built from Overpass")
    ap.add_argument("--airports", nargs="+", required=True,
                    help="airports to run the graded Section A comparison on "
                         "(must have a baseline parquet under --parts)")
    ap.add_argument("--build-only", nargs="*", default=[],
                    help="airports with no Overpass baseline: Section B, "
                         "build-only, no pass/fail")
    ap.add_argument("--out", default=None,
                    help="also write the comparison table to this file")
    args = ap.parse_args()

    import osn_sample
    osn_sample.load_dotenv()
    spark = osn_sample.build_spark(2, "8g", distributed=False)
    spark.sparkContext.setLogLevel("ERROR")

    from opdi.config import OPDIConfig
    from opdi.reference.h3_airport_layouts import hexagonify_airport
    from opdi.reference.pbf_source import PbfLayoutSource
    from opdi.utils.storage import StorageManager

    storage = StorageManager(spark, OPDIConfig.for_environment("opensky"))
    src = PbfLayoutSource(args.pbf, storage)

    lines = []

    def emit(s: str = "") -> None:
        print(s, flush=True)
        lines.append(s)

    first_call_elapsed = None
    per_airport_elapsed = []

    def timed_build(apt: str) -> pd.DataFrame:
        nonlocal first_call_elapsed
        t0 = time.monotonic()
        df = hexagonify_airport(apt, resolution=12, source=src)
        elapsed = time.monotonic() - t0
        if first_call_elapsed is None:
            first_call_elapsed = elapsed
        else:
            per_airport_elapsed.append(elapsed)
        return df

    # ---------------------------------------------------------------------
    # Section A: graded comparison against a saved Overpass baseline.
    # ---------------------------------------------------------------------
    emit("=" * 100)
    emit("SECTION A -- graded comparison against Overpass baselines "
         f"({len(args.airports)} aerodromes)")
    emit("=" * 100)
    header = (f"{'airport':8s} {'aeroway':18s} {'overpass':>9s} {'pbf':>9s} "
              f"{'shared':>9s} {'only_op':>8s} {'only_pbf':>8s}")
    emit(header)

    for apt in args.airports:
        part = os.path.join(args.parts, f"{apt}.parquet")
        if not os.path.exists(part):
            emit(f"{apt}: no Overpass baseline, skipped")
            continue
        old = pd.read_parquet(part)
        new = timed_build(apt)
        for way in sorted(set(old["hexaero_aeroway"]) | set(new["hexaero_aeroway"])):
            a = set(old.loc[old["hexaero_aeroway"] == way, "hexaero_h3_id"])
            b = set(new.loc[new["hexaero_aeroway"] == way, "hexaero_h3_id"])
            emit(f"{apt:8s} {way:18s} {len(a):9d} {len(b):9d} "
                 f"{len(a & b):9d} {len(a - b):8d} {len(b - a):8d}")

    # ---------------------------------------------------------------------
    # Section B: build-only, no baseline exists -- coverage, not correctness.
    # ---------------------------------------------------------------------
    if args.build_only:
        emit("")
        emit("=" * 100)
        emit(f"SECTION B -- build-only, no Overpass baseline exists "
             f"({len(args.build_only)} aerodromes)")
        emit("No pass/fail is reported here: there is nothing to diff "
             "against. This is evidence of COVERAGE (geometry now exists "
             "where the Overpass path produced none), not of CORRECTNESS. "
             "The correctness claim rests entirely on Section A.")
        emit("=" * 100)
        b_header = f"{'airport':8s} {'aeroway':18s} {'pbf_cells':>9s}"
        emit(b_header)
        for apt in args.build_only:
            try:
                new = timed_build(apt)
            except Exception as exc:
                emit(f"{apt:8s} BUILD FAILED: {exc!r}")
                continue
            total = 0
            families = sorted(set(new["hexaero_aeroway"]))
            for way in families:
                n = int((new["hexaero_aeroway"] == way).sum())
                total += n
                emit(f"{apt:8s} {way:18s} {n:9d}")
            emit(f"{apt:8s} {'TOTAL':18s} {total:9d}")
            has_parking = "parking_position" in families
            emit(f"{apt:8s} has parking_position cells: {has_parking}")
            emit("")

    # ---------------------------------------------------------------------
    # Timing -- Task 6 runtime estimate.
    # ---------------------------------------------------------------------
    emit("")
    emit("=" * 100)
    emit("TIMING")
    emit("=" * 100)
    if first_call_elapsed is not None:
        emit(f"first features_for() call (includes the one-time extract read): "
             f"{first_call_elapsed:.1f}s")
    if per_airport_elapsed:
        avg = sum(per_airport_elapsed) / len(per_airport_elapsed)
        emit(f"subsequent hexagonify_airport() calls: n={len(per_airport_elapsed)}, "
             f"avg={avg*1000:.1f}ms, max={max(per_airport_elapsed)*1000:.1f}ms")

    # ---------------------------------------------------------------------
    # Assignment method -- which airports used the box fallback.
    # ---------------------------------------------------------------------
    all_airports = list(args.airports) + list(args.build_only)
    report = src.assignment_report()
    in_scope = report[report["icao"].isin(all_airports)]
    box_rows = in_scope[in_scope["method"] != "polygon"]
    emit("")
    emit("=" * 100)
    emit("ASSIGNMENT METHOD per airport in scope")
    emit("=" * 100)
    for _, r in in_scope.sort_values("icao").iterrows():
        emit(f"  {r['icao']:8s} {r['method']:8s} n_features={r['n_features']}")
    if len(box_rows):
        emit(f"NOTE: {len(box_rows)} airport(s) used the box fallback rather "
             f"than a polygon: {', '.join(sorted(box_rows['icao']))}")
    else:
        emit("All in-scope airports were assigned by polygon containment "
             "(no box fallback used).")

    if args.out:
        with open(args.out, "w") as fh:
            fh.write("\n".join(lines) + "\n")
        print(f"\nwrote comparison table to {args.out}", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
