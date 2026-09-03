"""
The executable definition of the track-construction V2 analysis.

Every number in ``papers/track-construction-v2/`` is produced by exactly one job
listed here, with exactly the arguments listed here. The report renders by
calling this module, so the numbers on the page are the numbers this code
produces -- not numbers someone once copied into a directory.

Each job declares the source files it depends on. An output is **stale** when the
fingerprint over those files differs from the one recorded when the output was
written, so editing ``segmentation/methods.py`` marks every pipeline job for
re-run while editing this docstring marks nothing. Age is not staleness.

    python benchmarks/regenerate_track_v2.py --check     # what is stale
    python benchmarks/regenerate_track_v2.py             # run only what is stale
    python benchmarks/regenerate_track_v2.py --force     # run everything

``--check`` needs no credentials and no cluster.

**What V2 is, against V1.** V1 scored segmentations in a harness that read a
track table someone else had built. V2 runs steps 01, 02 and 02a for every
arm, plus step 03 for two of the three (see the next paragraph). That is why
:data:`STEPS` exists and V1 had no equivalent: in V1 the pipeline steps could
not change a result because no result went through them, and in V2 every
result does. The difference between the two studies' baselines is not noise to
be reconciled -- it is the harness-versus-pipeline gap this study was written
to measure.

Every arm is also scored twice -- airborne and gate-to-gate -- rather than
once; see ``track_pipeline_v2.score_segmentation`` for the ``gate_*`` columns
that adds to each row.

**Three arms for segmentation, two for the flight list.** ``standard`` is
``airframe_only`` plus the callsign-change break. Running only the ends of the
*segmentation* comparison gives a total that cannot be split into the two
changes that produced it, and the release note has to say which change bought
what -- so all three run steps 01, 02 and 02a and are scored on clustering.
Step 03 -- the expensive step -- and its ADEP/ADES score run only for
``legacy`` and ``standard`` (``track_pipeline_v2.FLIGHT_LIST_METHODS``):
``airframe_only`` is only ever the ablation's midpoint, and the downstream
question ADEP/ADES answers -- "does shipping this change ADEP/ADES" -- needs
the before and the after, not the midpoint.

**One day per period, not three.** V1's ladder ran three days because it
re-partitioned an existing table; here each arm materialises its own tracks and
its own cleaned copy through the real steps, at roughly 6.8 GB and forty
minutes for a single day. Three days across three arms and two periods is a
day of cluster time and more bucket than the study is allowed to hold at once.
The day is the same 5 June in both periods that every earlier study sampled.
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
        REPO.parent / "opdi-portal" / "papers" / "track-construction-v2",
    )
)
DATA = PAPER / "data"

#: The arms, in ablation order. `legacy` is what every published release was
#: built with; `airframe_only` drops callsign from the group key; `standard` is
#: `airframe_only` plus the callsign-change break and is the shipped default.
METHODS = ["legacy", "airframe_only", "standard"]

#: One day per period. See the module docstring for why not three.
DAYS = {"2025": "2025-06-05", "2024": "2024-06-05"}

#: Source files whose contents define a result. Named explicitly: a dependency
#: worth re-running for is a dependency worth writing down.
SEG = [
    # __init__.py decides *which* implementation `assign_track_id` resolves to,
    # so re-pointing one name there would change every number in the study
    # while base.py and methods.py stayed byte-identical.
    "src/opdi/pipeline/segmentation/__init__.py",
    "src/opdi/pipeline/segmentation/base.py",
    "src/opdi/pipeline/segmentation/methods.py",
    "src/opdi/config.py",
]
#: V2 runs the real steps, so the steps are dependencies. V1 did not need
#: these: it read a track table someone else had built. That difference is the
#: study.
#:
#: The cleaning modules are under ``src/opdi/cleaning/``, not
#: ``src/opdi/pipeline/cleaning/``. The distinction matters more than a typo
#: usually does, because ``provenance.fingerprint`` hashes the literal bytes
#: ``<missing>`` for a path that does not exist -- silently, with no warning --
#: so a misspelled dependency does not fail, it simply never marks anything
#: stale again.
STEPS = ["src/opdi/ingestion/osn_statevectors.py",
         "src/opdi/pipeline/tracks.py",
         "src/opdi/cleaning/cleaner.py",
         "src/opdi/cleaning/native.py",
         "src/opdi/pipeline/flights.py"]
#: The scorers, plus `track_diagnostics`, whose `null_rates` contributes
#: columns to the same row.
#:
#: `track_continuity.py` stays declared although no job reads it today: it
#: still supplies `extents_name`, which names the per-arm files
#: `export_track_extents` writes, and re-declaring the continuity job (see the
#: block at the end of `jobs()`) must not require remembering to add a
#: dependency back. Over-declaring costs a spurious re-run; under-declaring
#: costs a number nothing can account for.
SCORE = ["benchmarks/track_truth.py", "benchmarks/track_score.py",
         "benchmarks/osn_sample.py", "benchmarks/adep_ades.py",
         "benchmarks/flight_list_v7.py", "benchmarks/track_diagnostics.py",
         "benchmarks/track_continuity.py"]


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
        with tempfile.TemporaryDirectory(prefix=f"tcv2_{self.name}_") as tmp:
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

    # --- the three arms through the real pipeline, both periods ----------
    # One job per period rather than one per arm, because the arms share the
    # ingested state vectors: splitting them would re-ingest the same day three
    # times (~23 min each) and, worse, would let three arms segment three
    # separately-ingested copies of "the same" day, so a difference between
    # arms could be an artefact of the ingest rather than of the segmentation.
    for period in ("2025", "2024"):
        day = DAYS[period]
        # The per-arm extents are still written by `export_track_extents` into
        # the scratch directory, and its totals still feed the row -- but they
        # are **not declared outputs**, so they are not staged and the
        # continuity job below is not declared either. See the block after this
        # loop for why, and for what it would take to bring them back.
        outputs = {f"pipeline_{period}.csv": f"pipeline_{period}.csv"}
        out.append(Job(
            name=f"pipeline_{period}",
            script="benchmarks/track_pipeline_v2.py",
            args=["--period", period, "--days", day,
                  "--methods", *METHODS,
                  "--out-name", f"pipeline_{period}.csv"],
            outputs=outputs,
            code_paths=SEG + STEPS + SCORE,
            notes="Steps 01, 02 and 02a per arm (03 for legacy/standard only), "
                  "scored airborne and gate-to-gate against NM/APDF ground "
                  f"truth on {day}.",
        ))

    # --- does the id survive the change? ---------------------------------
    # **Not a declared job, and the paper answers it without one.**
    #
    # `track_continuity.py` compares the per-arm extents, and the extents were
    # written under a filename built from the PERIODS *dict* rather than the
    # period string -- `extents_standard_{'month': datetime.date(...), ...}.csv`
    # -- so staging failed on both periods and the scratch directories are
    # gone. The bug is fixed in `track_pipeline_v2.export_track_extents`'s
    # caller, but recovering the files needs a full pipeline re-run: about 3.8
    # hours per period, both periods, because each arm's cleaned table is
    # deleted at the end of its own iteration.
    #
    # That is not worth paying, because the continuity question is already
    # answered by outputs in hand:
    #
    # * **How many ids change?** All of them, by construction, and no run can
    #   say otherwise. `legacy` suffixes `_{year}_{month}` where `standard`
    #   suffixes an offset, and the two hash different group keys
    #   (`icao24||callsign` against `icao24`), so the identical-id count is
    #   zero for reasons visible in `segmentation/methods.py` rather than in
    #   any measurement.
    # * **How much does the partition change?** `n_tracks` in
    #   `pipeline_{period}.csv` -- 46,577 -> 31,778 on 2025 and 42,704 ->
    #   29,593 on 2024, about a 31% fall, reproduced by V1's harness at 32.6%
    #   and 30.9%. Tracks-per-airframe moves by the same ratio, since both arms
    #   see the same airframes.
    #
    # To bring the job back: re-declare the extents in the loop above as
    # `outputs[extents_name(m, period)]`, re-declare this job, and re-run both
    # pipeline jobs. Nothing else is needed -- `track_continuity.py` and its
    # tests are unchanged and still pass.

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
