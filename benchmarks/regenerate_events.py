"""
The executable definition of the flight-events analysis.

Every number in ``papers/flight-events-v1/`` is produced by exactly one job
listed here, with exactly the arguments listed here. The report renders by
calling this module, so the numbers on the page are the numbers this code
produces -- not numbers someone once copied into a directory.

Each job declares the source files it depends on. An output is **stale** when
the fingerprint over those files differs from the one recorded when the output
was written, so editing ``events.py`` marks every event job for re-run while
editing this docstring marks nothing. Age is not staleness: a file written a
month ago by unchanged code is current, and one written a minute ago by
since-changed code is not.

    python benchmarks/regenerate_events.py --check   # what is stale, run nothing
    python benchmarks/regenerate_events.py           # run only what is stale
    python benchmarks/regenerate_events.py --force   # run everything

``--check`` needs no credentials and no cluster. Running a stale job needs both.

**What differs from the V7 chain.** That one measured a categorical answer --
which aerodrome -- and could score a whole configuration with a single accuracy
figure. This one measures times, so every job carries a distribution rather
than a rate, and the ground-truth job runs first and alone: its bridge match
rate is the ceiling on every coverage number downstream, and running the ladder
before knowing it would mean interpreting detector misses that were really
reference misses.
"""

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

import provenance  # noqa: E402

PAPER = REPO.parent / "opdi-portal" / "papers" / "flight-events-v1"
DATA = PAPER / "data"

PERIODS = ("2025", "2024")

#: Source files whose contents define each job's result. Named explicitly: a
#: dependency worth re-running for is a dependency worth writing down.
GT = ["benchmarks/events_gt.py", "benchmarks/osn_sample.py"]
SCORE = GT + ["benchmarks/events_score.py"]
#: The detectors themselves. Every module that can change what step 04 emits,
#: because a ladder scored against a changed detector is scoring something
#: other than what it names.
DETECT = SCORE + [
    "benchmarks/event_bench.py",
    "src/opdi/config.py",
    "src/opdi/pipeline/events.py",
    "src/opdi/pipeline/crossings.py",
    "src/opdi/pipeline/level_segments.py",
    "src/opdi/pipeline/runways.py",
    "src/opdi/pipeline/ground.py",
]

T_TRACKS_CLEAN = "s3a://eurocontrol/opdi/osn_tracks_clean"
T_REF = "s3a://eurocontrol/opdi/research/reference"


class Job:
    """One analysis step producing CSVs into the paper's data directory."""

    def __init__(self, name, script, args, outputs, code_paths, notes="", inputs=()):
        self.name = name
        self.script = script
        self.args = args
        self.outputs = outputs
        self.code_paths = [script] + list(code_paths)
        self.notes = notes
        self.inputs = list(inputs)

    def stale(self):
        for produced, staged in self.outputs.items():
            is_stale, why = provenance.is_stale(
                DATA, staged, self.code_paths, self.inputs
            )
            if is_stale:
                return why
        return ""

    def run(self):
        """Run into a temporary directory, then stage what it produced.

        A job that dies halfway leaves the committed data untouched -- the
        alternative is a paper rendering from a directory that is half one run
        and half another, which is exactly the failure the provenance manifest
        exists to make impossible.
        """
        with tempfile.TemporaryDirectory() as tmp:
            cmd = [sys.executable, "-u", self.script, *self.args, "--results-dir", tmp]
            print(f"  $ {' '.join(cmd)}")
            subprocess.run(cmd, cwd=REPO, check=True)
            DATA.mkdir(parents=True, exist_ok=True)
            for produced, staged in self.outputs.items():
                src = Path(tmp) / produced
                if not src.is_file():
                    raise SystemExit(
                        f"{self.name}: expected {produced!r} but the job did not "
                        f"produce it"
                    )
                shutil.copy2(src, DATA / staged)
                provenance.record(
                    DATA, staged, self.script, self.args, self.code_paths,
                    notes=self.notes, input_tables=self.inputs,
                )


def jobs():
    out = []
    for period in PERIODS:
        out.append(
            Job(
                f"ground_truth_{period}",
                "benchmarks/events_gt.py",
                ["--period", period],
                {f"bridge_{period}.json": f"bridge_{period}.json"},
                GT,
                "the bridge match rate. Runs first and alone because it is the "
                "ceiling on every coverage figure downstream: a milestone that "
                "cannot be reached is indistinguishable from one that was "
                "missed unless this number is known.",
                inputs=[T_REF],
            )
        )
    for period in PERIODS:
        out.append(
            Job(
                f"ladder_{period}",
                "benchmarks/event_bench.py",
                ["--period", period, "--out-name", f"ladder_{period}.csv",
                 "--executors", "10"],
                {f"ladder_{period}.csv": f"ladder_{period}.csv",
                 f"inventory_{period}.csv": f"inventory_{period}.csv"},
                DETECT,
                "the cumulative ladder from events_v0.0.2 to the shipped "
                "configuration, one rung per change, scored on this period -- "
                "plus the extraction inventory, which is the only place the "
                "families APDF cannot score are visible at all.",
                inputs=[T_TRACKS_CLEAN, T_REF],
            )
        )
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true", help="report staleness, run nothing")
    ap.add_argument("--force", action="store_true", help="run everything")
    ap.add_argument("--only", nargs="*", default=None)
    args = ap.parse_args()

    todo = jobs()
    if args.only:
        names = {j.name for j in todo}
        unknown = [n for n in args.only if n not in names]
        if unknown:
            raise SystemExit(f"unknown job(s): {', '.join(unknown)}")
        todo = [j for j in todo if j.name in args.only]

    stale = {j.name: j.stale() for j in todo}
    if args.check:
        any_stale = False
        for j in todo:
            why = stale[j.name]
            print(f"  {'STALE' if why else 'ok   '}  {j.name}{'  -- ' + why if why else ''}")
            any_stale |= bool(why)
        return 1 if any_stale else 0

    for j in todo:
        if args.force or stale[j.name]:
            print(f"\n=== {j.name} ===  ({stale[j.name] or 'forced'})")
            j.run()
        else:
            print(f"  skipping {j.name} (current)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
