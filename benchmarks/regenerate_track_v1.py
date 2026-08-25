"""
The executable definition of the track-construction V1 analysis.

Every number in ``papers/track-construction-v1/`` is produced by exactly one job
listed here, with exactly the arguments listed here. The report renders by
calling this module, so the numbers on the page are the numbers this code
produces -- not numbers someone once copied into a directory.

Each job declares the source files it depends on. An output is **stale** when the
fingerprint over those files differs from the one recorded when the output was
written, so editing ``segmentation/methods.py`` marks every arm job for re-run
while editing this docstring marks nothing. Age is not staleness: a file written
a month ago by unchanged code is current, and one written a minute ago by
since-changed code is not.

    python benchmarks/regenerate_track_v1.py --check     # what is stale
    python benchmarks/regenerate_track_v1.py             # run only what is stale
    python benchmarks/regenerate_track_v1.py --force     # run everything

``--check`` needs no credentials and no cluster. Running a stale job needs both,
because the numbers come from Spark over S3 against Network Manager reference
data and there is no way to recompute them without the data they come from.

**One job, one command, one file.** The sweep's grid extension is a separate job
writing a separate CSV rather than a ``--resume`` append onto the first, because
a job that cannot be reproduced by running its own declared command is not a
job -- and ``--resume`` matches on the parameter triple alone, so an appended
file silently mixes rows from two different code states with nothing in the CSV
to say so.

**The payoff jobs come in pairs.** ``payoff_*`` measures each arm as the pipeline
stands; ``payoff_fixcallsign_*`` measures the same arms with the flight list's
callsign labelling repaired. The pair is the study's central result -- the
difference between them is what ``flights.py``'s ``F.min("flight_id")`` costs a
segmentation that does not group on callsign -- so neither is optional and
neither can be derived from the other.
"""

import argparse
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
        REPO.parent / "opdi-portal" / "papers" / "track-construction-v1",
    )
)
DATA = PAPER / "data"

#: Source files whose contents define a result. Named explicitly: a dependency
#: worth re-running for is a dependency worth writing down.
SEG = [
    "src/opdi/pipeline/segmentation/base.py",
    "src/opdi/pipeline/segmentation/methods.py",
    "src/opdi/config.py",
]
SCORE = ["benchmarks/track_truth.py", "benchmarks/track_score.py",
         "benchmarks/osn_sample.py"]
#: The two diagnostics. The census depends on ground truth alone -- it never
#: reads a state vector -- so it must NOT carry SEG, or every segmentation edit
#: would mark a result that cannot have changed as stale. The histogram does
#: read tracks, and carries the same dependencies an arm does.
DIAG = ["benchmarks/track_diagnostics.py"]
CENSUS = DIAG + ["benchmarks/track_truth.py"]
#: The payoff runs the flight list, so it depends on the flight list too.
FLIGHTS = ["benchmarks/flight_list_v7.py", "src/opdi/pipeline/flights.py",
           "benchmarks/adep_ades.py"]

#: The full stage-1 grid, as one command. Kept here rather than left to
#: track_sweep.py's defaults so that changing those defaults marks this job
#: stale -- the point of the fingerprint.
GAP = ["10", "15", "20", "25", "30", "40", "50", "60", "90"]
LOW_GAP = ["3", "5", "10", "15", "20", "30"]
LOW_FT = ["1000", "2500", "5000", "7500", "10000"]


class Job:
    """One analysis step: a command, its outputs, and what it depends on."""

    def __init__(self, name, script, args, outputs, code_paths, notes=""):
        self.name = name
        self.script = script
        self.args = args
        self.outputs = outputs
        self.code_paths = [script] + list(code_paths)
        self.notes = notes

    def stale(self):
        reasons = {}
        for staged in self.outputs.values():
            bad, why = provenance.is_stale(DATA, staged, self.code_paths, [])
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
        the run can know, and re-recording without them is what left every
        staged output carrying ``inputs: {}``.
        """
        with tempfile.TemporaryDirectory(prefix=f"tcv1_{self.name}_") as tmp:
            cmd = [sys.executable, "-u", str(REPO / self.script),
                   *self.args, "--results-dir", tmp]
            print(f"\n=== {self.name} ===\n  {' '.join(cmd[2:])}", flush=True)
            r = subprocess.run(cmd, cwd=REPO)
            if r.returncode != 0:
                raise SystemExit(f"{self.name} failed with exit {r.returncode}")
            for produced, staged in self.outputs.items():
                src = Path(tmp) / produced
                if not src.is_file():
                    raise SystemExit(f"{self.name} did not produce {produced}")
                DATA.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, DATA / staged)
                own = provenance.load_manifest(tmp).get(produced, {})
                provenance.record(DATA, staged, self.script, self.args,
                                  self.code_paths, inputs=own.get("inputs"),
                                  input_tables=list(own.get("input_tables") or {}),
                                  notes=self.notes)
                print(f"  staged {staged}")


def jobs() -> list:
    out = []

    # --- the eight-arm ladder, both periods -----------------------------
    for period in ("2025", "2024"):
        out.append(Job(
            name=f"arms_{period}",
            script="benchmarks/track_methods.py",
            # --out-name is explicit: track_methods.py defaults to "arms.csv",
            # so declaring the staged name alone leaves the job producing a
            # file this spec never looks for.
            args=["--period", period, "--arms", "all",
                  "--out-name", f"arms_{period}.csv"],
            outputs={f"arms_{period}.csv": f"arms_{period}.csv"},
            code_paths=SEG + SCORE + ["benchmarks/track_methods.py"],
            notes="Eight segmentation arms scored against NM/APDF ground truth.",
        ))

    # --- A1 parameter sweep ---------------------------------------------
    # Stage 1 locates the optimum on ONE day: 235 cells at three days would be
    # ~8 h, and locating an optimum and quoting it are different claims.
    out.append(Job(
        name="sweep_stage1",
        script="benchmarks/track_sweep.py",
        args=["--period", "2025", "--days", "2025-06-05",
              "--grid-gap", *GAP, "--grid-low-alt-gap", *LOW_GAP,
              "--grid-low-alt-ft", *LOW_FT,
              "--out-name", "sweep_2025_stage1.csv"],
        outputs={"sweep_2025_stage1.csv": "sweep_2025_stage1.csv"},
        code_paths=SEG + SCORE + ["benchmarks/track_sweep.py"],
        notes="Stage 1: 235 cells, one day, to locate the optimum.",
    ))
    # Stage 1 put low_alt_ft's optimum on the grid edge with the curve still
    # rising. An optimum at an edge is where you stopped looking, so the grid
    # is extended past it -- and the extension is its own job and its own file,
    # not a --resume append, so each file is reproducible by one command.
    out.append(Job(
        name="sweep_stage1_ext",
        script="benchmarks/track_sweep.py",
        args=["--period", "2025", "--days", "2025-06-05",
              "--grid-gap", "40", "50", "60",
              "--grid-low-alt-gap", "5", "10", "15",
              "--grid-low-alt-ft", "15000", "20000", "30000",
              "--out-name", "sweep_2025_stage1_ext.csv"],
        outputs={"sweep_2025_stage1_ext.csv": "sweep_2025_stage1_ext.csv"},
        code_paths=SEG + SCORE + ["benchmarks/track_sweep.py"],
        notes="Stage 1 extension: low_alt_ft past the edge its optimum landed on.",
    ))
    # Stage 2 re-measures the interesting region on the FULL three days of both
    # periods, which is what the paper quotes.
    for period in ("2025", "2024"):
        out.append(Job(
            name=f"sweep_stage2_{period}",
            script="benchmarks/track_sweep.py",
            args=["--period", period,
                  "--grid-gap", "30", "50", "60",
                  "--grid-low-alt-gap", "10", "15",
                  "--grid-low-alt-ft", "5000", "20000",
                  "--out-name", f"sweep_{period}_stage2.csv"],
            outputs={f"sweep_{period}_stage2.csv": f"sweep_{period}_stage2.csv"},
            code_paths=SEG + SCORE + ["benchmarks/track_sweep.py"],
            notes="Stage 2: the production cell and the optimum, three days.",
        ))

    # --- ADEP/ADES payoff, as the pipeline stands ------------------------
    for period in ("2025", "2024"):
        out.append(Job(
            name=f"payoff_{period}",
            script="benchmarks/track_payoff.py",
            args=["--period", period, "--arms", "all"],
            outputs={f"payoff_{period}.csv": f"payoff_{period}.csv"},
            code_paths=SEG + FLIGHTS + ["benchmarks/track_payoff.py"],
            notes="V7 'shipped' held fixed; only track_id varies.",
        ))

    # --- ADEP/ADES payoff with the callsign labelling repaired -----------
    # legacy is carried in every one of these as the control: its tracks are
    # already callsign-homogeneous, so a correct relabelling must leave it
    # unmoved, and it has twice come back at exactly +0. Without it the other
    # arm's gain could be anything the run happened to change.
    for period in ("2025", "2024"):
        out.append(Job(
            name=f"payoff_fixcallsign_{period}",
            script="benchmarks/track_payoff.py",
            args=["--period", period, "--arms", "recommended", "legacy",
                  "--fix-callsign"],
            outputs={f"payoff_{period}.csv": f"payoff_fixcallsign_{period}.csv"},
            code_paths=SEG + FLIGHTS + ["benchmarks/track_payoff.py"],
            notes="Same arms with flights.py's F.min('flight_id') worked around; "
                  "legacy is the control and must not move.",
        ))

    # --- what the containment restriction costs -------------------------
    # V1 scores only ground-truth flights wholly inside the sampled window.
    # The defence of that is sound and is in the paper already; what was
    # missing is its price, which is a number and not an argument. Ground
    # truth only -- no state vectors, no arms, no S3 writes.
    for period in ("2025", "2024"):
        out.append(Job(
            name=f"containment_{period}",
            script="benchmarks/track_diagnostics.py",
            args=["--job", "containment", "--period", period,
                  "--out-name", f"containment_{period}.csv"],
            outputs={f"containment_{period}.csv": f"containment_{period}.csv"},
            code_paths=CENSUS,
            notes="How many ground-truth flights the wholly-inside-the-window "
                  "restriction excludes, and how much of each was visible.",
        ))

    # --- the boundary error as a distribution ---------------------------
    # p10/p50/p90 cannot tell a symmetric spread from a bimodal mixture, and
    # the two are different diagnoses. Three arms, not eight: each one is an
    # assignment table's worth of cluster time, and these are the arms whose
    # boundary numbers the paper discusses.
    for period in ("2025", "2024"):
        out.append(Job(
            name=f"boundary_hist_{period}",
            script="benchmarks/track_diagnostics.py",
            args=["--job", "boundary-hist", "--period", period,
                  "--arms", "recommended", "legacy", "ground_anchored",
                  "--out-name", f"boundary_hist_{period}.csv"],
            outputs={f"boundary_hist_{period}.csv": f"boundary_hist_{period}.csv"},
            code_paths=SEG + SCORE + DIAG,
            notes="Signed boundary offsets binned at 30 s over +/-1800 s. "
                  "Shares track_score.boundary_offsets with boundary_error, so "
                  "the bins and the published percentiles describe one sample.",
        ))

    return out


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
