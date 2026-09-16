"""
The executable definition of the track-construction V3 analysis.

Every number in ``papers/track-construction-v3/`` is produced by exactly one
job listed here, with exactly the arguments listed here. The report renders
by calling this module, so the numbers on the page are the numbers this code
produces -- not numbers someone once copied into a directory.

Each job declares the source files it depends on. An output is **stale** when
the fingerprint over those files differs from the one recorded when the
output was written, so editing ``segmentation/methods.py`` marks every job
for re-run while editing this docstring marks nothing. Age is not staleness.

    python benchmarks/regenerate_track_v3.py --check     # what is stale
    python benchmarks/regenerate_track_v3.py             # run only what is stale
    python benchmarks/regenerate_track_v3.py --force     # run everything

``--check`` needs no credentials and no cluster.

**What V3 is, against V2.** V2 shipped ``recommended`` -- group on
``icao24``, break on a genuine non-blank callsign change -- and measured what
it recovered. It did not ask what happens when the callsign changes back. V3
answers that: a flicker (a callsign that goes X -> Y -> X and reverts within a
few samples) trips the callsign-change break twice, trisecting a flight the
break was supposed to leave whole. ``debounced`` (arm ``A9``) adds a hold: a
callsign change is only real once the new value has persisted for
``callsign_min_persistence_seconds``. This module's three jobs are the
validation for that fix -- not a new pipeline run like V2's, because the
question here is narrower and each of Tasks 1, 4 and 5 already answers one
piece of it directly against ``osn_tracks_clean`` or the published flight
list.

**Three jobs, not one pipeline sweep.** V2 ran steps 01 -> 02 -> 02a (-> 03
for two arms) because it was comparing group keys and needed the real
partition each key produces. V3's jobs re-segment already-ingested,
already-cleaned state vectors (Task 1, Task 4) or read the already-published
flight list (Task 5) -- cheaper, because the debounce is a pure refinement of
the callsign-change break already validated in V2, not a new grouping key.

  * ``flicker_2026`` -- the raw callsign sequence per airframe on
    2026-06-01: how often a change reverts, how long the transient value
    holds, and -- classifying every boundary ``recommended`` actually draws
    that day -- how much of the shipped rule's own fragmentation traces to a
    flicker rather than a genuine change.
  * ``fragmentation_2026`` -- tracks, tracks per flight, and the
    persisted-callsign guard, for ``recommended`` and for ``debounced`` at
    three persistence thresholds, all on 2026-06-01.
  * ``movements_2026`` -- APDF movement counts for the shipped arm
    (``opdi-prod``, already in production) and the debounced arm
    (``opdi/research/a9``, the self-contained warehouse
    ``run_debounced_day.py`` built for 2026-06-03), the guard this fix has to
    pass before it could ever become the default.

**Two measurements this module does not stage.** The genuine-merge
comparison (36 vs 37 two-flight fusions/day, ``debounced_vs_recommended_merge.py``)
and the persisted-callsign residual characterisation
(``debounced_2plus_persist.py``) are follow-ups Task 4 ran once, by hand, to
interpret the fragmentation job's guard column -- neither is a job here, and
the paper reads their numbers from a hand-staged CSV with that stated
plainly in its own manifest entry. Re-running them costs a cluster session
each and would not be a no-op even when current, because both scripts read
without writing anything a staleness check could key on. See
``data/_manifest.json``.
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
        REPO.parent / "opdi-portal" / "papers" / "track-construction-v3",
    )
)
DATA = PAPER / "data"

#: The segmentation modules the callsign-change break and the debounce both
#: live in. Every job here depends on both files, whatever else it also reads,
#: because every job re-segments with one of these two arms.
SEG = [
    "src/opdi/pipeline/segmentation/__init__.py",
    "src/opdi/pipeline/segmentation/base.py",
    "src/opdi/pipeline/segmentation/methods.py",
]


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
        the run can know.
        """
        with tempfile.TemporaryDirectory(prefix=f"tcv3_{self.name}_") as tmp:
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
    return [
        Job(
            name="flicker_2026",
            script="benchmarks/callsign_flicker.py",
            args=["--day", "2026-06-01", "--out-name", "flicker_2026.csv"],
            outputs={"flicker_2026.csv": "flicker_2026.csv"},
            code_paths=SEG,
            notes="Raw callsign sequences on 2026-06-01: how often a change "
                  "reverts, how long the transient value holds, and the "
                  "cause breakdown of every boundary `recommended` draws "
                  "that day.",
        ),
        Job(
            name="fragmentation_2026",
            script="benchmarks/track_fragmentation.py",
            args=["--day", "2026-06-01", "--holds", "15", "30", "60",
                  "--out-name", "fragmentation_2026.csv"],
            outputs={"fragmentation_2026.csv": "fragmentation_2026.csv"},
            code_paths=SEG,
            notes="Tracks, tracks per flight, and the persisted-callsign "
                  "guard, per arm and per persistence threshold, on "
                  "2026-06-01.",
        ),
        Job(
            name="movements_2026",
            script="benchmarks/movement_counts.py",
            args=["--days", "2026-06-01", "2026-06-02", "2026-06-03",
                  "--out-name", "movements_2026.csv"],
            outputs={"movements_2026.csv": "movements_2026.csv"},
            code_paths=SEG + ["benchmarks/run_debounced_day.py"],
            notes="APDF movement counts for the shipped arm (opdi-prod) and "
                  "the debounced arm (opdi/research/a9), the guard against "
                  "the shipped baseline.",
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
