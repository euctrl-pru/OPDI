"""
The executable definition of the ADS-C fusion V1 analysis.

Every number in ``papers/adsc-fusion-v1/`` is produced by exactly one job
listed here, with exactly the arguments listed here. The report renders by
calling this module, so the numbers on the page are the numbers this code
produces.

Each job declares the source files it depends on. An output is **stale** when
the fingerprint over those files differs from the one recorded when the output
was written, so editing ``osn_adsc.py`` marks every pipeline job for re-run
while editing this docstring marks nothing. Age is not staleness.

    python benchmarks/regenerate_adsc_v1.py --check     # what is stale
    python benchmarks/regenerate_adsc_v1.py             # run only what is stale
    python benchmarks/regenerate_adsc_v1.py --force     # run everything

``--check`` needs no credentials and no cluster.

**What the study asks.** Whether merging OpenSky's ADS-C feed into the ADS-B
trajectories improves flight-event extraction. Four jobs, in the order the
question has to be answered:

  * ``census`` -- what the feed contains, over eleven days spread across the
    archive: rows, in-box rows, cadence, altitude bands, and the fill rate of
    every column. The paper's framing rests on this and on nothing else.
  * ``gain_2026`` -- what the merge did to the trajectories on
    2026-06-05/06/07: samples and track-hours added, reception holes closed,
    and the merge-neutrality guard (tracks holding two real callsigns, i.e.
    two flights fused).
  * ``time_sync_2026`` -- how far apart the two clocks are. For each ADS-C
    report bracketed by ADS-B within 60 s, the offset is decomposed into
    along-track and cross-track; cross-track is the position floor and
    along-track over ground speed is the implied time offset. Reported per
    airframe, because that is where the finding is.
  * ``events_2026`` -- what it did to the milestones, both arms scored against
    APDF 202606 over the twenty flight-events-v4 study aerodromes, per
    milestone and per aerodrome.

**The pipeline build is a prerequisite, not a job.** ``gain_2026`` and
``events_2026`` read the two arms at
``s3a://eurocontrol/opdi/research/adsc/{base,merged}``, built by
``benchmarks/run_adsc_day.py --day <d> --arm both`` for each of the three days
-- roughly six cluster-hours in total. It is not a job here for the same reason
the A9 study's day run was not: a staleness check cannot key on it (it writes
tables, not files under ``data/``), and re-running it would not be a no-op when
current. ``time_sync_2026`` needs only the shared inputs, so it can run after
step 01/01b alone. If a job below reports table contents that disagree with the
paper, rebuild the arms first.

**What this module does not measure.** Nothing is scored outside the European
box or outside June 2026, no sweep of the shadow half-windows was run, and the
per-airframe time offset is measured but never applied. All three are stated in
the paper's limitations chapter rather than implied by their absence.
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
        REPO.parent / "opdi-portal" / "papers" / "adsc-fusion-v1",
    )
)
DATA = PAPER / "data"

#: The code that decides what ADS-C contributes: the mapping and shadow filter,
#: the merge that puts it in front of segmentation, and the cleaning exemption
#: without which every ADS-C value is NULLed. A job that reads a pipeline table
#: depends on all three, because a change to any one changes its answer.
CORE = [
    "src/opdi/ingestion/osn_adsc.py",
    "src/opdi/pipeline/tracks.py",
    "src/opdi/cleaning/native.py",
]

#: The days both arms were built for. The flight-events-v4 period.
DAYS = ["2026-06-05", "2026-06-06", "2026-06-07"]

#: One day per half-year plus the three study days plus two days taken from the
#: archive's gap list, so "the extra columns are always empty" is a claim about
#: the archive and not about one Sunday in June.
CENSUS_DAYS = [
    "2023-09-01", "2024-03-01", "2024-09-02", "2025-03-01", "2025-09-01",
    "2026-03-22", "2026-06-01", "2026-06-05", "2026-06-06", "2026-06-07",
    "2026-10-15",
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
        with tempfile.TemporaryDirectory(prefix=f"adscv1_{self.name}_") as tmp:
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
            name="census",
            script="benchmarks/adsc_census.py",
            args=["--days", *CENSUS_DAYS, "--out-name", "adsc_census.csv",
                  "--local"],
            outputs={"adsc_census.csv": "adsc_census.csv"},
            code_paths=[],
            notes="What the raw ADS-C topic contains, eleven days across the "
                  "archive: rows, in-box rows, airframes, median cadence, "
                  "altitude and geography bands inside the coverage box, "
                  "exact duplicates, and the fill rate of every column. Reads "
                  "the topic directly -- no pipeline code can change this "
                  "answer, which is why it depends on none.",
        ),
        Job(
            name="gain_2026",
            script="benchmarks/adsc_coverage_gain.py",
            args=["--days", *DAYS,
                  "--gain-name", "adsc_gain_2026.csv",
                  "--holes-name", "adsc_holes_2026.csv"],
            outputs={"adsc_gain_2026.csv": "adsc_gain_2026.csv",
                     "adsc_holes_2026.csv": "adsc_holes_2026.csv"},
            code_paths=CORE,
            notes="Rows, tracks, track-hours and segments per arm; the "
                  "reception holes ADS-C closed; the two-callsign "
                  "merge-neutrality guard. Reads both arms at "
                  "research/adsc/{base,merged}, built by run_adsc_day.py.",
        ),
        Job(
            name="time_sync_2026",
            script="benchmarks/adsc_time_sync.py",
            args=["--days", *DAYS,
                  "--pairs-name", "adsc_time_sync_pairs_2026.csv",
                  "--out-name", "adsc_time_sync_2026.csv"],
            outputs={"adsc_time_sync_pairs_2026.csv":
                         "adsc_time_sync_pairs_2026.csv",
                     "adsc_time_sync_2026.csv": "adsc_time_sync_2026.csv"},
            code_paths=CORE,
            notes="Along/cross decomposition of each ADS-C report against "
                  "ADS-B interpolated to the same instant, per airframe. "
                  "Reads the shared inputs at research/adsc/input, so it does "
                  "not depend on either arm having been built.",
        ),
        Job(
            name="events_2026",
            script="benchmarks/adsc_events_compare.py",
            args=["--out-name", "adsc_events_2026.csv",
                  "--by-airport-name", "adsc_events_by_airport_2026.csv"],
            outputs={"adsc_events_2026.csv": "adsc_events_2026.csv",
                     "adsc_events_by_airport_2026.csv":
                         "adsc_events_by_airport_2026.csv"},
            code_paths=CORE + [
                "benchmarks/events_gt.py",
                "benchmarks/events_score.py",
                "benchmarks/event_bench.py",
                "src/opdi/pipeline/events.py",
            ],
            notes="Both arms against APDF 202606 over the twenty V4 study "
                  "aerodromes, through one truth frame: per milestone and per "
                  "aerodrome.",
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
