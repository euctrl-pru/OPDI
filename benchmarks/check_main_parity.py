"""Is the report describing the code that is actually on main?

Two failures this catches, and v6 shipped the first for three versions: a
report asserting `trend_radius_nm` ships at 20 NM while the code had 30,
because each version copied the claim rather than measuring it. The mirror is
a figure produced by code that never got merged.

**The recommendation is not written down anywhere.** It is the best cell of the
**pipeline grid** -- the job that calls `process_dai`, and so the only sweep
that runs main's own package. The harness sweep is a reimplementation that
never touches `opdi`, and although it covers more cells and both periods it is
not the authority. The two disagree, which is v6's central finding restated
rather than a defect.

    python benchmarks/check_main_parity.py <paper>/data

Exits non-zero if main's configuration is not the grid's best cell, if a path
the manifest names is missing from main, or if a figure was produced dirty or
from a commit that is not an ancestor of main.
"""

import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path

#: Grid column -> DetectionConfig attribute. These are the parameters the
#: pipeline grid varies, and so the ones it can speak to.
PARAMS = {
    "trend_ceiling": "trend_max_height_ft",
    "trend_radius_nm": "trend_radius_nm",
    "trend_vote_margin": "trend_vote_margin",
}


class ConfigurationNotOnGrid(Exception):
    """main's configuration is absent from the grid, so parity is not
    expressible.

    Distinct from a mismatch and a failure in its own right: a check against a
    grid that does not contain the thing under test is not a check. The harness
    sweep is in exactly this state -- its HEIGHT_CAPS has no 6,000 entry, so
    main's ceiling was never a cell it ran.
    """


def _rows(grid_csv):
    return [r for r in csv.DictReader(open(grid_csv))
            if r["run"].startswith("grid_")]


def best_cell(grid_csv) -> dict:
    """The highest-scoring arrival cell. Arrivals, because `trend` is the
    method the shipped configuration uses for that role."""
    return max(_rows(grid_csv), key=lambda r: float(r["ades_score"]))


def config_parity(grid_csv, cfg: dict):
    """(differing parameter names, rank/gap report).

    `cfg` maps DetectionConfig attribute names to values, so the *caller*
    decides which package to import from. That is deliberate: it lets the test
    pin that the values came from main's checkout rather than this module
    quietly importing whatever is on the path.
    """
    rows = sorted(_rows(grid_csv), key=lambda r: -float(r["ades_score"]))
    if not rows:
        raise ConfigurationNotOnGrid(f"{grid_csv} carries no grid_ rows")
    best = rows[0]

    def matches(row):
        return all(float(row[col]) == float(cfg[attr])
                   for col, attr in PARAMS.items())

    rank = next((i + 1 for i, r in enumerate(rows) if matches(r)), None)
    if rank is None:
        have = ", ".join(f"{a}={cfg[a]}" for a in PARAMS.values())
        raise ConfigurationNotOnGrid(
            f"main's configuration ({have}) is not among the {len(rows)} "
            f"cells of {Path(grid_csv).name}. A parity check against a grid "
            f"that does not contain the configuration under test is not a "
            f"check -- widen the grid.")

    diffs = [attr for col, attr in PARAMS.items()
             if float(best[col]) != float(cfg[attr])]
    report = {
        "rank": rank,
        "of": len(rows),
        "gap": float(best["ades_score"]) - float(rows[rank - 1]["ades_score"]),
        "best": {col: best[col] for col in PARAMS},
        "mine": {col: rows[rank - 1][col] for col in PARAMS},
    }
    return diffs, report


def _git(*args, repo):
    return subprocess.run(["git", "-C", str(repo), *args],
                          capture_output=True, text=True)


def _manifest_paths(manifest) -> set:
    m = json.loads(Path(manifest).read_text())
    paths = set()
    for entry in m.values():
        if not isinstance(entry, dict):
            continue
        if entry.get("script"):
            paths.add(entry["script"])
        paths.update(entry.get("code_paths") or [])
    return paths


def code_on_main(manifest, ref: str, repo) -> list:
    """Paths named in the manifest that do not exist at `ref`."""
    return [p for p in sorted(_manifest_paths(manifest))
            if _git("cat-file", "-e", f"{ref}:{p}", repo=repo).returncode != 0]


def shas_on_main(manifest, ref: str, repo) -> list:
    """Figures produced dirty, or by a commit that is not an ancestor of ref."""
    m = json.loads(Path(manifest).read_text())
    bad = []
    for name, entry in sorted(m.items()):
        if not isinstance(entry, dict) or "git_sha" not in entry:
            continue
        if entry.get("git_dirty"):
            bad.append(f"{name}: produced from a dirty tree")
            continue
        sha = entry["git_sha"]
        if _git("merge-base", "--is-ancestor", sha, ref,
                repo=repo).returncode != 0:
            bad.append(f"{name}: {sha} is not an ancestor of {ref}")
    return bad


def ref_exists(ref: str, repo) -> bool:
    return _git("rev-parse", "--verify", "--quiet", ref, repo=repo).returncode == 0


def detection_config_from(repo) -> dict:
    """The shipped parameters, loaded from a **named** checkout's `src`.

    Importing `opdi` ambiently is not good enough for this tool. Whichever
    `opdi` happens to be on `sys.path` wins, and that varies: pytest puts a
    worktree's own `src` ahead of the venv's editable install, so a check run
    from a worktree silently reads the branch's config while reporting on
    main's. The whole claim this module makes -- that the report describes the
    code on main -- would then be unfounded.

    So the caller names the checkout and this loads from it, evicting any
    already-imported `opdi` first.
    """
    import importlib

    repo = Path(repo).resolve()
    src = str(repo / "src")
    for name in [m for m in sys.modules if m == "opdi" or m.startswith("opdi.")]:
        del sys.modules[name]
    sys.path.insert(0, src)
    try:
        import opdi
        from opdi.config import DetectionConfig
        loaded = Path(opdi.__file__).resolve()
        if not str(loaded).startswith(src):
            raise RuntimeError(
                f"asked for opdi from {src} but got {loaded}. Refusing to "
                f"report on a package other than the one named.")
        c = DetectionConfig()
        return {
            "_from": str(loaded),
            "trend_max_height_ft": c.trend_max_height_ft,
            "trend_radius_nm": c.trend_radius_nm,
            "trend_vote_margin": c.trend_vote_margin,
        }
    finally:
        sys.path.remove(src)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("data", type=Path, help="the paper's data/ directory")
    ap.add_argument("--ref", default="origin/main")
    ap.add_argument("--repo", type=Path,
                    default=Path(__file__).resolve().parent.parent)
    args = ap.parse_args()

    cfg = detection_config_from(args.repo)
    print(f"opdi loaded from {cfg.pop('_from')}")

    failed = False
    try:
        diffs, report = config_parity(args.data / "trend_grid_v6.csv", cfg)
    except ConfigurationNotOnGrid as exc:
        # A clean exit, not a traceback: this is a expected-and-meaningful
        # outcome of the check, not a crash. It says the grid is too narrow to
        # answer the question, which is a different problem from a mismatch and
        # has a different fix.
        print(f"\nNOT CHECKABLE: {exc}")
        print("\nPARITY: FAIL (grid too narrow)")
        sys.exit(2)

    print(f"\nmain's cell {report['mine']} ranks {report['rank']} of "
          f"{report['of']}, {report['gap']:,.0f} behind the best cell "
          f"{report['best']}")
    for d in diffs:
        print(f"  DIFFERS  {d}: main has {cfg[d]}")
    failed |= bool(diffs)

    ref = args.ref
    if not ref_exists(ref, args.repo):
        # Loud, not silent. A check that cannot run without a network is a
        # check people disable; one that skips quietly is worse still.
        print(f"\nSKIPPING code and SHA checks: {ref} not available locally")
    else:
        for p in code_on_main(args.data / "_manifest.json", ref, args.repo):
            print(f"  MISSING on {ref}: {p}")
            failed = True
        for s in shas_on_main(args.data / "_manifest.json", ref, args.repo):
            print(f"  {s}")
            failed = True

    print("\nPARITY: " + ("FAIL" if failed else "OK"))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
