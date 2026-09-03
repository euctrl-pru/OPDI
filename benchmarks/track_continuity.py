"""Does a segmentation change break the ``track_id`` a consumer already holds?

The scoring in ``track_pipeline_v2`` answers a question about *partitions*: how
well does each arm's grouping of samples line up with the flights that really
flew. That question is deliberately blind to what the groups are called --
:mod:`track_score` compares partitions, never ids, because comparing ids across
arms would report a difference wherever two correct answers happened to be
spelled differently.

This module asks the opposite question, and it is the one a release note has to
answer. Everything published so far carries a ``track_id`` built by ``legacy``,
which suffixes ``_{year}_{month}``; ``airframe_only`` and ``standard`` do not.
So two arms can agree perfectly about where every flight begins and ends and
still share not one id between them -- and for a downstream join, perfect
agreement that shares no key is a total break. **The measurement is therefore on
the id string, not on the partition.** That is not a weaker version of the
partition comparison; it is a different fact, and the partition comparison
cannot produce it.

Reading extents rather than tables is forced rather than chosen. Two arms'
tables never coexist on S3 -- the runner builds, scores and deletes one arm
before starting the next, because the bucket cannot hold two -- so the per-arm
extents CSV that ``track_pipeline_v2.export_track_extents`` writes before each
cleanup is the only place a cross-arm comparison can happen at all.

    python benchmarks/track_continuity.py --period 2025 \\
        --extents-dir ../opdi-portal/papers/track-construction-v2/data \\
        --results-dir .

No Spark and no S3: this reads CSVs the pipeline run already produced.
"""

import argparse
import csv
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

import provenance  # noqa: E402

__all__ = ["compare", "read_extents", "extents_name", "COMPARISONS", "FIELDS"]

#: Column order of the CSV, fixed here so the file the paper reads and the
#: interface this module promises cannot drift apart.
FIELDS = [
    "period", "before", "after", "n_before", "n_after", "identical_ids",
    "identical_pct", "mean_tracks_per_airframe_before",
    "mean_tracks_per_airframe_after",
]

#: The pairs worth reporting. The first two are the ablation's two steps; the
#: third is what a consumer of published data actually faces, and it is not
#: implied by the other two -- continuity does not compose, because a track can
#: survive one step and be renamed by the next.
COMPARISONS = [
    ("legacy", "airframe_only"),
    ("airframe_only", "standard"),
    ("legacy", "standard"),
]


def extents_name(method: str, period: str) -> str:
    """The per-arm extents file ``track_pipeline_v2`` writes."""
    return f"extents_{method}_{period}.csv"


def read_extents(path) -> list:
    """The ``(track_id, icao24)`` pairs from one arm's extents CSV.

    Only the two identity columns are read. ``t_start``, ``t_end`` and
    ``n_points`` are in the file for the paper's use and play no part here: a
    track whose id survives but whose extent moved is still a joinable key, and
    a track whose extent is identical but whose id changed is still a broken
    join. This function measures joinability.
    """
    with open(path, newline="") as fh:
        return [(r["track_id"], r["icao24"]) for r in csv.DictReader(fh)]


def compare(before, after) -> dict:
    """How much of *before*'s ``track_id`` vocabulary survives into *after*.

    Both arguments are sequences of ``(track_id, icao24)`` pairs, as
    :func:`read_extents` returns.

    ``identical_pct`` is expressed as a share of **before**, because that is
    the question a consumer holding already-published ids asks: of the ids I
    have, how many still resolve? A share of *after* would answer a different
    and less useful question, and averaging the two would answer neither.

    ``mean_tracks_per_airframe`` is the coarse shape of the change and is
    reported on both sides because ``identical_pct`` alone cannot distinguish
    the two ways an id can vanish. A pure rename and a merge of two tracks into
    one both score 0% identical; only the tracks-per-airframe figure says which
    happened, and they call for different sentences in a release note.
    """
    ids_before = {t for t, _ in before}
    ids_after = {t for t, _ in after}
    identical = len(ids_before & ids_after)

    def per_airframe(rows):
        by_frame = {}
        for track_id, icao24 in rows:
            by_frame.setdefault(icao24, set()).add(track_id)
        if not by_frame:
            return 0.0
        return sum(len(v) for v in by_frame.values()) / len(by_frame)

    n_before = len(ids_before)
    return {
        "n_before": n_before,
        "n_after": len(ids_after),
        "identical_ids": identical,
        "identical_pct": 0.0 if n_before == 0 else 100.0 * identical / n_before,
        "mean_tracks_per_airframe_before": per_airframe(before),
        "mean_tracks_per_airframe_after": per_airframe(after),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--period", required=True)
    ap.add_argument("--extents-dir", type=Path, required=True,
                    help="where the pipeline run staged its extents CSVs")
    ap.add_argument("--results-dir", type=Path, required=True)
    ap.add_argument("--out-name", default=None)
    args = ap.parse_args()

    out_name = args.out_name or f"continuity_{args.period}.csv"
    sys.stdout.reconfigure(line_buffering=True)

    cache, rows = {}, []
    for before, after in COMPARISONS:
        for method in (before, after):
            if method not in cache:
                path = args.extents_dir / extents_name(method, args.period)
                if not path.is_file():
                    raise SystemExit(
                        f"missing {path}; run the pipeline job for "
                        f"{args.period} first -- it writes the extents this "
                        f"comparison is the only consumer of")
                cache[method] = read_extents(path)
                print(f"  {path.name}: {len(cache[method])} tracks")

        row = {"period": args.period, "before": before, "after": after}
        row.update(compare(cache[before], cache[after]))
        rows.append(row)
        print(f"  {before} -> {after}: {row['identical_pct']:.2f}% of "
              f"{row['n_before']} ids survive")

    args.results_dir.mkdir(parents=True, exist_ok=True)
    out = args.results_dir / out_name
    with out.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    provenance.record(
        args.results_dir, out_name,
        script="benchmarks/track_continuity.py", argv=sys.argv[1:],
        code_paths=["benchmarks/track_continuity.py"],
        inputs={f"tracks_{m}": len(v) for m, v in sorted(cache.items())},
        notes=f"track_id survival between arms, period {args.period}, "
              f"measured on the id string.",
    )
    print(f"\n-> {out}")


if __name__ == "__main__":
    main()
