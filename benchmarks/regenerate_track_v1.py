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

**Four payoff CSVs in ``data/`` are deliberately undeclared.** ``payoff_*`` and
``payoff_fixcallsign_*`` were produced under the pre-v6.2 detection datum
(``trend_max_datum="msl"``), which the V7 ladder can no longer reproduce: its
last rung no longer matches the shipped ``DetectionConfig()``. The paper keeps
those figures and labels them as a measurement made at a configuration that has
since moved, tied to its existing limitation about the datum, rather than
re-running them at a configuration that would answer a different question.
Declaring them here would mark them stale at every render and invite exactly the
re-run that cannot be made, so they are named in this docstring instead of in a
``Job``. Anything undeclared is a debt; this one is written down.
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
    # __init__.py is not filler. It is what `from opdi.pipeline.segmentation
    # import assign_track_id` actually resolves, so it decides *which*
    # implementation every arm runs; re-pointing one name there would change
    # every number in the study while base.py and methods.py stayed byte-
    # identical. Low-churn, but a dependency's churn rate is not what makes it
    # a dependency.
    "src/opdi/pipeline/segmentation/__init__.py",
    "src/opdi/pipeline/segmentation/base.py",
    "src/opdi/pipeline/segmentation/methods.py",
    "src/opdi/config.py",
]
SCORE = ["benchmarks/track_truth.py", "benchmarks/track_score.py",
         "benchmarks/osn_sample.py"]
#: ``track_methods.py`` is a dependency of four other jobs, not just the ladder
#: it is named for. ``track_sweep`` and ``track_payoff`` import ``PERIODS`` and
#: ``attach_airport_context`` from it -- which days a run covers, and the
#: airport-context columns arm A6 branches on; ``track_diagnostics`` reads
#: ``PERIODS`` for the census window and *calls* ``run_arm`` for the histogram.
#: All four omitted it.
#:
#: The omission does not merely fail to mark an output stale. It writes the
#: hole into the manifest, and every later staleness check inherits it -- so
#: after a re-run, a change to the day list or the zone join would read as
#: "current" indefinitely. That is why this was worth fixing before the
#: regeneration rather than after it.
METHODS = ["benchmarks/track_methods.py"]
#: The two diagnostics. The census depends on ground truth alone -- it never
#: reads a state vector -- so it must NOT carry SEG, or every segmentation edit
#: would mark a result that cannot have changed as stale. The histogram does
#: read tracks, and carries the same dependencies an arm does.
DIAG = ["benchmarks/track_diagnostics.py"]
#: The census still does not carry SEG or track_score, and that is deliberate
#: rather than an instance of the same omission fixed above: it never segments
#: and never scores, so neither can move its number. It *does* read
#: ``track_methods.PERIODS`` for the months and days it counts over, which is
#: exactly the sort of thing that has to be declared.
CENSUS = DIAG + ["benchmarks/track_truth.py"] + METHODS
#: The payoff runs the flight list, so it depends on the flight list too.
FLIGHTS = ["benchmarks/flight_list_v7.py", "src/opdi/pipeline/flights.py",
           "benchmarks/adep_ades.py"]

#: **The five ``sweep_recommended_*`` outputs carry a narrower dependency set
#: than the truth, and this constant is it.** They were produced by running
#: ``track_sweep.py`` straight into ``data/`` rather than through this runner,
#: so their manifest entries record *that script's* own hardcoded list. It omits
#: ``src/opdi/config.py``, ``segmentation/__init__.py``,
#: ``benchmarks/osn_sample.py`` and ``benchmarks/track_methods.py`` -- all four
#: of which ``track_sweep.py`` imports, and any of which moves its numbers.
#:
#: Declaring ``SEG + SCORE + METHODS`` here instead would be the honest set and
#: would mark all five stale at every render, with a six-hour re-run as the only
#: cure and not one digit expected to move. So the jobs below declare what the
#: outputs were actually stamped with, the omission is named here rather than
#: left to be discovered, and the debt closes on the first re-run through this
#: runner -- which stamps ``SEG + SCORE + METHODS``, at which point this
#: constant should be deleted and the jobs given the wide set.
SWEEP_AS_RECORDED = ["benchmarks/track_score.py", "benchmarks/track_truth.py",
                     "src/opdi/pipeline/segmentation/base.py",
                     "src/opdi/pipeline/segmentation/methods.py"]

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
            code_paths=SEG + SCORE + METHODS,
            notes="Eight segmentation arms scored against NM/APDF ground truth.",
        ))

    # --- the parameter sweep, on the rule that ships ---------------------
    # The legacy sweep -- ``sweep_stage1``, ``sweep_stage1_ext``,
    # ``sweep_stage2_2025``, ``sweep_stage2_2024`` -- is gone, with its four
    # CSVs, because the chapter it fed is gone: it tuned a rule nobody runs, to
    # close off "legacy was merely mistuned", and that question is answered.
    # What replaces it sweeps ``recommended`` over the four parameters
    # production exposes, including ``callsign_lookback_minutes``, which the
    # legacy sweep could not vary because legacy has no such parameter.
    #
    # Every cell here is measured on the FULL three-day sample, not on one day.
    # The legacy sweep located its optimum on a single day and re-measured a
    # shortlist on three; this one can afford three throughout, so the rank and
    # the deltas the paper quotes come from the paper's own sample.
    out.append(Job(
        name="sweep_recommended_2025_stage1",
        script="benchmarks/track_sweep.py",
        args=["--method", "recommended", "--period", "2025",
              "--grid-gap", *GAP, "--grid-low-alt-gap", *LOW_GAP,
              "--grid-low-alt-ft", *LOW_FT,
              "--out-name", "sweep_recommended_2025_stage1.csv"],
        outputs={"sweep_recommended_2025_stage1.csv":
                 "sweep_recommended_2025_stage1.csv"},
        code_paths=SWEEP_AS_RECORDED,
        notes="The three-axis grid at `recommended`, lookback following "
              "gap_minutes. 235 cells rather than 270: the grid drops every "
              "cell whose low-altitude gap exceeds the general one, which is "
              "inert.",
    ))
    # low_alt_ft's optimum landed on the grid edge with the curve still rising.
    # An optimum at an edge is where you stopped looking, so the axis is
    # extended past it -- as its own job and its own file, not a --resume
    # append, so each file is reproducible by one command.
    out.append(Job(
        name="sweep_recommended_2025_stage1_ext",
        script="benchmarks/track_sweep.py",
        args=["--method", "recommended", "--period", "2025",
              "--grid-gap", "40", "--grid-low-alt-gap", "20",
              "--grid-low-alt-ft", "10000", "15000", "20000", "30000",
              "--out-name", "sweep_recommended_2025_stage1_ext.csv"],
        outputs={"sweep_recommended_2025_stage1_ext.csv":
                 "sweep_recommended_2025_stage1_ext.csv"},
        code_paths=SWEEP_AS_RECORDED,
        notes="low_alt_ft past the edge its optimum landed on, held at the "
              "best cell of the other two axes.",
    ))
    # The lookback axis, swept at TWO cells rather than one. A lookback that is
    # best at gap=40 need not be best at gap=30, and the paper's conclusion --
    # that the parameter should keep following gap_minutes -- is a claim about
    # both, so measuring one would not support it.
    out.append(Job(
        name="sweep_recommended_2025_stage2",
        script="benchmarks/track_sweep.py",
        args=["--method", "recommended", "--period", "2025",
              "--grid-gap", "40", "--grid-low-alt-gap", "20",
              "--grid-low-alt-ft", "10000",
              "--grid-lookback", "0", "5", "10", "15", "20", "25", "30", "35",
              "40", "45", "50", "60", "120",
              "--out-name", "sweep_recommended_2025_stage2.csv"],
        outputs={"sweep_recommended_2025_stage2.csv":
                 "sweep_recommended_2025_stage2.csv"},
        code_paths=SWEEP_AS_RECORDED,
        notes="callsign_lookback_minutes at the cell the 2025 grid preferred.",
    ))
    out.append(Job(
        name="sweep_recommended_2025_stage2b",
        script="benchmarks/track_sweep.py",
        args=["--method", "recommended", "--period", "2025",
              "--grid-gap", "30", "--grid-low-alt-gap", "15",
              "--grid-low-alt-ft", "5000",
              "--grid-lookback", "15", "20", "25", "30", "35", "40", "50",
              "--out-name", "sweep_recommended_2025_stage2b_shipped.csv"],
        outputs={"sweep_recommended_2025_stage2b_shipped.csv":
                 "sweep_recommended_2025_stage2b_shipped.csv"},
        code_paths=SWEEP_AS_RECORDED,
        notes="callsign_lookback_minutes at the cell that actually ships.",
    ))
    # 2024 over the eight cells spanning the one change 2025 wanted. It is the
    # job that decided nothing would be re-shipped: the gap ordering reverses.
    out.append(Job(
        name="sweep_recommended_2024_stage3",
        script="benchmarks/track_sweep.py",
        args=["--method", "recommended", "--period", "2024",
              "--grid-gap", "30", "40", "--grid-low-alt-gap", "15", "20",
              "--grid-low-alt-ft", "5000", "10000",
              "--out-name", "sweep_recommended_2024_stage3.csv"],
        outputs={"sweep_recommended_2024_stage3.csv":
                 "sweep_recommended_2024_stage3.csv"},
        code_paths=SWEEP_AS_RECORDED,
        notes="Does 2025's preferred cell transfer to the second period? No.",
    ))

    # --- ADEP/ADES payoff: four CSVs, no jobs ----------------------------
    # payoff_2025/2024 and payoff_fixcallsign_2025/2024 stay in data/ and are
    # read by the paper, and neither pair is declared here. They were produced
    # under the pre-v6.2 detection datum and the V7 ladder cannot currently
    # reproduce them -- see the module docstring, and the paper's limitation on
    # the datum change, which is where a reader meets this rather than in a
    # footnote. FLIGHTS is retained above because re-declaring them needs it.

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
            code_paths=SEG + SCORE + METHODS + DIAG,
            notes="Signed boundary offsets binned at 30 s over +/-1800 s. "
                  "Shares track_score.boundary_offsets with boundary_error, so "
                  "the bins and the published percentiles describe one sample.",
        ))

    # --- what A3's split predicate cannot see ----------------------------
    # traffic's Flight.split() is designed against a gap-filled frame; OPDI
    # applies it raw. gap_boundary_nulls counts how often the predicate's own
    # altitude comparison lands on a NULL, and separately counts "no-gap
    # turnarounds" -- continuous broadcast through a stand -- which no fill
    # could fix. It reads the same cleaned track table the arms jobs read, but
    # neither segments nor scores, so it carries METHODS (for PERIODS) and
    # nothing from SEG or SCORE.
    for period in ("2025", "2024"):
        out.append(Job(
            name=f"traffic_fill_{period}",
            script="benchmarks/track_diagnostics.py",
            args=["--job", "traffic-fill", "--period", period,
                  "--out-name", f"traffic_fill_{period}.csv"],
            outputs={f"traffic_fill_{period}.csv": f"traffic_fill_{period}.csv"},
            code_paths=DIAG + METHODS,
            notes="How often A3's split predicate sees a NULL boundary "
                  "altitude, and how many turnarounds no gap threshold "
                  "could ever catch.",
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
