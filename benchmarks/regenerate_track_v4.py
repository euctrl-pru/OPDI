"""
The executable definition of the track-construction V4 analysis.

Every number in ``papers/track-construction-v4/`` that the final study
produced comes from exactly one job listed here, with exactly the arguments
listed here. The report renders by calling this module.

Each job declares the source files it depends on. An output is **stale** when
the fingerprint over those files differs from the one recorded when it was
written. Age is not staleness.

    python benchmarks/regenerate_track_v4.py --check     # what is stale
    python benchmarks/regenerate_track_v4.py             # run only what is stale
    python benchmarks/regenerate_track_v4.py --force     # run everything

``--check`` needs no credentials and no cluster.

Three jobs, each staging into its own subdirectory of ``data/`` with its own
``_manifest.json`` (the scripts record provenance beside their outputs, and the
staleness check reads the manifest beside the file):

  * ``final_grid`` -- the final cruise-link grid over 2026-06-14 and
    2025-06-14: implied speed {315, 400} kt, speed ratio {0.0, 0.85}, minimum
    stop {0, 20, 35} min, maximum gap {360, 480, 600} min, cruise floor 18000 ft.
  * ``final_direction`` -- the one point 400 kt / 0.0 / 0 / 600 min / 18000 ft
    with directional edges off and on.
  * ``stage2`` -- the chosen point run through the real pipeline and scored:
    flight list, events, level and distance across the hole, movements.

**``benchmarks/run_track_link_day.py`` is a prerequisite, not a job.** ``stage2``
needs both arms built first at ``research/a10`` and it is one day
(2026-06-14) on a 10% sample. It writes tables, not files under ``data/``, so a
staleness check cannot key on it, and re-running it would not be a no-op. If
``stage2`` reports table contents that disagree with the paper, rebuild the arms.

**``data/round1``, ``round2``, ``round3_fl*`` and ``round4_fl180`` hold the
earlier rounds of the study.** They explain how the method arrived where it is,
were produced by earlier versions of the code, and are deliberately not
regenerated: no job here names them and ``--force`` leaves them alone.
"""

import argparse
import ast
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

import provenance  # noqa: E402

#: The paper lives in the sibling ``opdi-portal`` checkout. That resolution is
#: wrong inside a git worktree, where ``REPO.parent`` is ``.claude/worktrees``
#: rather than the workspace root, so every output reads as "missing" and the
#: staleness report is a confident lie. ``OPDI_PAPER_DIR`` overrides it; the
#: default is unchanged for a normal checkout.
PAPER = Path(
    os.environ.get(
        "OPDI_PAPER_DIR",
        REPO.parent / "opdi-portal" / "papers" / "track-construction-v4",
    )
)
DATA = PAPER / "data"

#: What every job's answer rests on: the segmentation rules and their base,
#: the track processor, and the configuration and coverage that choose which rows
#: exist. ``config.py`` is edited often, so jobs read stale after changes that
#: cannot move their answer; the price of ``--check`` never reporting "current"
#: across a coverage flip.
CORE = [
    "src/opdi/pipeline/segmentation/methods.py",
    "src/opdi/pipeline/segmentation/base.py",
    "src/opdi/pipeline/tracks.py",
    "src/opdi/config.py",
    "src/opdi/coverage.py",
]

DAYS = ["2026-06-14", "2025-06-14"]
STAGE2_DAY = "2026-06-14"


def _grid_outputs(days):
    names = []
    for d in days:
        names += [f"link_grid_{d}.csv", f"link_controls_{d}.csv", f"link_funnel_{d}.csv"]
    names += ["link_grid_pooled.csv", "link_controls_pooled.csv", "link_recommended.csv"]
    return names


#: Written only when the grid picks a point (or is given one): the reasons
#: reports are read at that point. A grid that picks none produces none.
def _reasons_outputs(days):
    return [f"{stem}_{d}.csv"
            for stem in ("link_reasons", "link_positive_reasons",
                         "link_negative_reasons")
            for d in [*days, "pooled"]]


def script_code_paths(script):
    """The ``_CODE_PATHS`` a script declares for its own outputs.

    Read from the source with ``ast`` rather than by import: the scripts import
    pyspark at module level, and ``--check`` must run anywhere.
    """
    tree = ast.parse((REPO / script).read_text())
    for node in tree.body:
        if (isinstance(node, ast.Assign)
                and any(getattr(t, "id", "") == "_CODE_PATHS" for t in node.targets)):
            return list(ast.literal_eval(node.value))
    raise SystemExit(f"{script} declares no _CODE_PATHS")


STAGE2_OUTPUTS = [
    "link_flight_list.csv", "link_both_ends.csv", "link_events.csv",
    "link_level_across_hole.csv", "link_distance_across_hole.csv",
    "link_partition_check.csv", "link_movements.csv",
    "link_truth_all_flights.csv",
]


class Job:
    """One analysis step: a command, its outputs, and what it depends on."""

    def __init__(self, name, script, args, outputs, code_paths, notes="",
                 optional=()):
        self.name = name
        self.data = DATA / name
        self.script = script
        self.args = args
        self.outputs = outputs
        # Union with what the script itself says its outputs depend on, so the
        # runner can never know less than the script that records them.
        self.code_paths = list(dict.fromkeys(
            [script, *code_paths, *script_code_paths(script)]))
        self.notes = notes
        self.optional = set(optional)

    def stale(self):
        reasons = {}
        manifest = provenance.load_manifest(self.data)
        for staged in self.outputs.values():
            if (staged in self.optional and not (self.data / staged).is_file()
                    and staged not in manifest):
                continue  # never produced: the grid picked no point
            entry = manifest.get(staged)
            if entry is not None and entry.get("argv") != list(self.args):
                reasons[staged] = "arguments changed"
                continue
            bad, why = provenance.is_stale(self.data, staged, self.code_paths, [])
            if bad:
                reasons[staged] = why
        return reasons

    def run(self):
        """Run into a scratch directory, then stage and stamp the outputs.

        Writing to scratch first means a job that dies halfway leaves the
        committed data untouched rather than half-replacing it with something
        that renders but is wrong.

        The stamping re-records rather than copies, because the runner knows
        things the script does not -- the staged name, and the full dependency
        set this spec declares. It carries the script's own ``inputs`` and
        ``input_tables`` across: those are row counts and table identities only
        the run can know.
        """
        with tempfile.TemporaryDirectory(prefix=f"trackv4_{self.name}_") as tmp:
            cmd = [sys.executable, "-u", str(REPO / self.script),
                   *self.args, "--results-dir", tmp]
            print(f"\n=== {self.name} ===\n  {' '.join(cmd[2:])}", flush=True)
            r = subprocess.run(cmd, cwd=REPO)
            if r.returncode != 0:
                raise SystemExit(f"{self.name} failed with exit {r.returncode}")
            for produced, staged in self.outputs.items():
                src = Path(tmp) / produced
                if not src.is_file() and produced in self.optional:
                    # Drop a stale entry and file from an earlier run that did
                    # choose a point, so the paper cannot read old reasons.
                    (self.data / staged).unlink(missing_ok=True)
                    m = provenance.load_manifest(self.data)
                    if m.pop(staged, None) is not None:
                        provenance.save_manifest(self.data, m)
                    print(f"  no {produced} (nothing chosen)")
                    continue
                if not src.is_file():
                    raise SystemExit(f"{self.name} did not produce {produced}")
                self.data.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, self.data / staged)
                own = provenance.load_manifest(tmp).get(produced, {})
                provenance.record(self.data, staged, self.script, self.args,
                                  self.code_paths, inputs=own.get("inputs"),
                                  input_tables=list(own.get("input_tables") or {}),
                                  notes=self.notes)
                print(f"  staged {staged}")


def jobs() -> list:
    return [
        Job(
            name="final_grid",
            script="benchmarks/track_link_grid.py",
            args=["--days", *DAYS, "--grid-json", json.dumps({
                "link_min_implied_speed_kt": [315, 400],
                "link_min_speed_ratio": [0.0, 0.85],
                "link_min_stop_minutes": [0.0, 20.0, 35.0],
                "link_max_gap_minutes": [360, 480, 600],
                "link_cruise_floor_ft": [18000],
            })],
            outputs={n: n for n in _grid_outputs(DAYS) + _reasons_outputs(DAYS)},
            optional=_reasons_outputs(DAYS),
            code_paths=CORE,
            notes="Final cruise-link grid over two days, pooled, with the "
                  "recommendation and, when a point is chosen, the reasons "
                  "reports read at it.",
        ),
        Job(
            name="final_direction",
            script="benchmarks/track_link_grid.py",
            args=["--days", *DAYS, "--grid-json", json.dumps({
                "link_min_implied_speed_kt": [400],
                "link_min_speed_ratio": [0.0],
                "link_min_stop_minutes": [0.0],
                "link_max_gap_minutes": [600],
                "link_directional_edges": [False, True],
                "link_cruise_floor_ft": [18000],
            })],
            outputs={n: n for n in ("link_grid_pooled.csv",
                                    "link_controls_pooled.csv")},
            code_paths=CORE,
            notes="The chosen point with directional edges off and on.",
        ),
        Job(
            name="stage2",
            script="benchmarks/track_link_pipeline_score.py",
            args=["--day", STAGE2_DAY],
            outputs={n: n for n in STAGE2_OUTPUTS},
            code_paths=CORE + [
                "src/opdi/pipeline/flights.py",
                "src/opdi/pipeline/events.py",
                "benchmarks/run_track_link_day.py",
            ],
            notes="Both arms at research/a10 through the real pipeline, one "
                  "day on a 10% sample, scored against the flight list. "
                  "Needs run_track_link_day.py to have built the arms.",
        ),
    ]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true",
                    help="report staleness and exit; no cluster needed")
    ap.add_argument("--force", action="store_true", help="re-run every job")
    ap.add_argument("--only", nargs="+", help="run only these jobs, by name")
    ap.add_argument("--allow-stale", action="store_true",
                    help="report staleness but exit 0 -- for rendering a draft "
                         "without a cluster")
    args = ap.parse_args()

    todo = jobs()
    if args.only:
        todo = [j for j in todo if j.name in set(args.only)]

    stale = {j.name: j.stale() for j in todo}
    any_stale = {k: v for k, v in stale.items() if v}

    print(f"analysis at {provenance.git_sha()}"
          f"{' (dirty)' if provenance.git_dirty() else ''}\n")
    for j in todo:
        why = stale[j.name]
        if why:
            print(f"  STALE   {j.name}")
            for f, r in why.items():
                print(f"            {f}: {r}")
        else:
            print(f"  current {j.name}")

    if args.check:
        if any_stale and not args.allow_stale:
            raise SystemExit(
                f"\n{len(any_stale)} job(s) stale. Run without --check to "
                f"regenerate, or pass --allow-stale to render anyway.")
        print("\nall outputs current" if not any_stale else
              "\nrendering with stale outputs (--allow-stale)")
        return

    for j in todo:
        if args.force or stale[j.name]:
            j.run()
        else:
            print(f"  skipping {j.name} (current)")
    print("\ndone")


if __name__ == "__main__":
    main()
