"""
The executable definition of the flight-events V4 analysis.

Every number in ``papers/flight-events-v4/`` is produced by exactly one job
listed here, with exactly the arguments listed here. The report renders by
calling this module, so the numbers on the page are the numbers this code
produces -- not numbers someone once copied into a directory.

Each job declares the source files it depends on. An output is **stale** when
the fingerprint over those files differs from the one recorded when the output
was written, so editing ``runway_ops.py`` marks every event job for re-run
while editing this docstring marks nothing. Age is not staleness: a file
written a month ago by unchanged code is current, and one written a minute ago
by since-changed code is not.

    python benchmarks/regenerate_events_v4.py --check   # what is stale, run nothing
    python benchmarks/regenerate_events_v4.py           # run only what is stale
    python benchmarks/regenerate_events_v4.py --force   # run everything

``--check`` needs no credentials and no cluster. Running a stale job needs both.

**What differs from the V3 chain** (``regenerate_events.py``, which stays as it
is and still drives the V3 paper):

* **A different period, and a flight list that has to be built first.** V4 runs
  over 2026-06-05/06/07, because that is the only month whose APDF extract
  covers all twenty study aerodromes -- UGKO has 868 movements there and zero
  in ``apdf_202506``. The *published* flight list covers 2025 only, so every
  aerodrome-anchored family would come back empty over 2026 unless a 2026
  flight list exists. It is a `Stage` rather than a `Job` because its product
  is an S3 table, not a CSV, and because rebuilding it on every code change
  would cost hours to arrive at the same rows.
* **A different ladder.** ``--ladder v4``: the V3 rungs inherit
  ``events_v0.0.2`` and are not reproducible against a track table rebuilt by
  the A8 segmentation (``event_bench.LADDER`` says why in full), so V4 starts
  from the reconstructed v0.1.0 configuration instead.
* **The study set, everywhere.** ``--airports study`` is passed to the ground
  truth *and* to both scoring jobs. It is the denominator of every coverage
  figure, and a bridge rate computed over one population with coverage computed
  over another is two numbers that cannot be read together.
"""

import argparse
import shutil
import subprocess
import os
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

import provenance  # noqa: E402

def _find_portal() -> Path:
    """Locate the opdi-portal checkout that owns this paper.

    ``REPO.parent / "opdi-portal"`` holds only when ``opdi`` and
    ``opdi-portal`` are siblings in the workspace -- i.e. when this runs from
    the primary checkout. Run from a git *worktree*, ``REPO`` is
    ``.../opdi/.claude/worktrees/<name>`` and ``REPO.parent`` is the worktrees
    directory, where no ``opdi-portal`` exists; the outputs would then stage to
    a phantom path and the paper would render stale. So honour an explicit
    ``OPDI_PORTAL_DIR`` first, then walk up until an ``opdi-portal`` sibling
    appears.
    """
    env = os.environ.get("OPDI_PORTAL_DIR")
    if env:
        return Path(env)
    # Walk up for an ``opdi-portal`` sibling, but skip any candidate that lives
    # inside ``.claude/worktrees`` -- a prior buggy run may have *created* a
    # phantom ``.../worktrees/opdi-portal`` there, and finding it would just
    # reproduce the original misresolution. The real checkout is the sibling at
    # the workspace root, outside any worktree.
    for anc in REPO.parents:
        cand = anc / "opdi-portal"
        if cand.exists() and ".claude/worktrees" not in str(cand):
            return cand
    return REPO.parent / "opdi-portal"  # nothing found; original guess


PAPER = _find_portal() / "papers" / "flight-events-v4"
DATA = PAPER / "data"

PERIODS = ("2026",)

#: The rung whose event table the comparison jobs read: the shipped
#: configuration. Named once, because the ladder writes the table and the
#: comparison reads it, and a disagreement between the two would compare a
#: configuration against a truth nobody scored it on.
SHIPPED_RUNG = "V07_shipped"

#: Source files whose contents define each job's result. Named explicitly: a
#: dependency worth re-running for is a dependency worth writing down.
GT = ["benchmarks/events_gt.py", "benchmarks/osn_sample.py"]
SCORE = GT + ["benchmarks/events_score.py"]
#: The detectors themselves. Every module that can change what step 04 emits,
#: because a ladder scored against a changed detector is scoring something
#: other than what it names. The four V4 additions -- the A-CDM runway family,
#: the PRU vertical profile, the layout gate and the field-elevation join --
#: are exactly the code the V4 rungs switch on.
DETECT = SCORE + [
    "benchmarks/event_bench.py",
    "src/opdi/config.py",
    "src/opdi/pipeline/events.py",
    "src/opdi/pipeline/crossings.py",
    "src/opdi/pipeline/level_segments.py",
    "src/opdi/pipeline/runways.py",
    "src/opdi/pipeline/ground.py",
    "src/opdi/pipeline/runway_ops.py",
    "src/opdi/pipeline/vertical_pru.py",
    "src/opdi/pipeline/layout.py",
    "src/opdi/pipeline/elevation.py",
]

#: S3 prefixes the analysis reads and writes.
T_TRACKS_CLEAN = "s3a://eurocontrol/opdi/research/tracks_clean_2026"
T_FLIGHT_LIST = "s3a://eurocontrol/opdi/research/flight_list_2026"
T_REF = "s3a://eurocontrol/opdi/research/reference"


class Stage:
    """An upstream pipeline step, whose product is an S3 table, not a CSV.

    Taken from ``regenerate_v6.py``, with one adaptation: ``stale()`` returns a
    reason *string* rather than a dict, so a stage and a job can sit in one
    list and be reported by one loop -- this module's `Job.stale` predates the
    dict form and is the published shape of the V3 chain's output.

    Idempotent by its own existence check, so a stage whose table is already
    there is a fast no-op. That is what makes it affordable to put it in the
    chain rather than in a README -- and a README is exactly where the 2026
    flight list would otherwise live, which is how a study ends up reproducible
    only from whatever happened to be in the bucket.
    """

    def __init__(self, name, cmd, produces, code_paths, notes="", inputs=()):
        self.name = name
        self.cmd = cmd            # argv, run from the repo root
        self.produces = produces  # S3 prefix this stage fills
        self.code_paths = list(code_paths)
        self.notes = notes
        self.script = cmd[0] if cmd else ""
        self.args = cmd[1:]
        self.inputs = list(inputs)
        self.outputs = {}

    @property
    def key(self):
        return f"table:{self.produces}"

    def stale(self):
        """Stale when the table is absent or empty, or its input moved.

        A populated table is *not* rebuilt because the pipeline code changed --
        rebuilding the flight list for every edit to ``events.py`` would cost
        hours and change nothing about it. The recorded fingerprint still says
        which code filled it, so a mismatch stays visible in the provenance
        table even when it does not force a re-run.
        """
        ident = provenance.s3_identity(self.produces)
        if ident.get("error"):
            # No credentials, no boto3, no bucket. Reported as stale rather
            # than assumed current: a checkable claim that cannot be checked is
            # not a claim.
            return f"cannot check ({ident['error']})"
        if not ident.get("objects"):
            return "table absent or empty"
        entry = provenance.load_manifest(DATA).get(self.key)
        if entry is None:
            return "present, but no provenance recorded"
        why = provenance.inputs_changed(entry, self.inputs)
        return f"must rebuild: {why}" if why else ""

    def run(self, rebuild=False):
        """Build the table if it is missing; otherwise record what it is.

        Deliberately *not* rebuilt on ``--force``. This table is the input every
        V4 figure is computed from; regenerating it would change the flight
        population under the whole study to arrive at the same rows. When it is
        already there, the honest action is to record its identity and say it
        was not rebuilt.
        """
        print(f"\n=== {self.name} ===", flush=True)
        ident = provenance.s3_identity(self.produces)
        entry = provenance.load_manifest(DATA).get(self.key)
        stale_input = provenance.inputs_changed(entry, self.inputs) if entry else ""
        present = bool(ident.get("objects")) and not stale_input and not rebuild
        note = self.notes
        if stale_input:
            print(f"  {stale_input} -- rebuilding rather than recording")
        if present:
            print(f"  {self.produces}\n  present: {ident['objects']:,} objects, "
                  f"{ident['bytes'] / 1e9:.2f} GB -- recording, not rebuilding")
            note = (note + " | PRE-EXISTING: identity recorded without a "
                    "rebuild, so the command below is how it is built, not "
                    "necessarily how this copy was built.").strip(" |")
        else:
            cmd = [str(x) for x in self.cmd]
            print(f"  absent -- building\n  {' '.join(cmd)}", flush=True)
            r = subprocess.run(cmd, cwd=REPO)
            if r.returncode != 0:
                raise SystemExit(f"{self.name} failed with exit {r.returncode}")
        DATA.mkdir(parents=True, exist_ok=True)
        provenance.record(
            DATA, self.key, self.script, self.args, self.code_paths,
            notes=note, input_tables=[self.produces] + self.inputs,
        )
        print(f"  recorded {self.key}")


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
                return f"{staged}: {why}"
        return ""

    def run(self):
        """Run into a temporary directory, then stage what it produced.

        A job that dies halfway leaves the committed data untouched -- the
        alternative is a paper rendering from a directory that is half one run
        and half another, which is exactly the failure the provenance manifest
        exists to make impossible.
        """
        with tempfile.TemporaryDirectory(prefix=f"v4_{self.name}_") as tmp:
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


def stages() -> list:
    """The upstream table the whole chain stands on.

    Ground truth is deliberately absent, as it is in the V6 chain:
    ``research/reference`` is extracted by the ``eurocontrol`` R package against
    the PRISME Oracle warehouse, which runs only on a machine with that access,
    so no render on this cluster can rebuild it.

    The tracks are absent for a different reason: ``research/tracks_clean_2026``
    was built by the A8 ``recommended`` segmentation arm and is the *definition*
    of the V4 sample. It is declared as this stage's input, so a rebuild of it
    shows up as a reason to rebuild the flight list rather than passing
    unnoticed.
    """
    return [
        Stage(
            "flight_list_2026",
            [sys.executable, "-u", "benchmarks/flight_list_2026.py",
             "--executors", "12"],
            T_FLIGHT_LIST,
            ["benchmarks/flight_list_2026.py", "src/opdi/pipeline/flights.py",
             "src/opdi/config.py"],
            "the 2026 flight list, through step 03's own process_dai. Every "
            "aerodrome-anchored family -- airport events, rings, the runway "
            "milestones, the PRU tops -- resolves ADEP/ADES through it, and "
            "the published flight list holds 2025 only, so without this the "
            "ladder reports those families as empty rather than as absent.",
            inputs=[T_TRACKS_CLEAN],
        ),
    ]


def jobs(reuse_rungs=(), reuse_newer_than=None) -> list:
    out = []
    for period in PERIODS:
        out.append(
            Job(
                f"ground_truth_{period}",
                "benchmarks/events_gt.py",
                ["--period", period, "--airports", "study"],
                {f"bridge_{period}.json": f"bridge_{period}.json"},
                GT,
                "the bridge match rate, scoped to the twenty study "
                "aerodromes -- `events_gt.bridge_report` applies the same "
                "airport filter the milestones get, and carries the "
                "network-wide figure alongside it under `network` as context. "
                "Runs first and alone because it is the ceiling on every "
                "coverage figure downstream: a milestone that cannot be "
                "reached is indistinguishable from one that was missed unless "
                "this number is known -- and a ceiling computed over a "
                "different population than the coverage caps nothing.",
                inputs=[T_REF],
            )
        )
    for period in PERIODS:
        out.append(
            Job(
                f"ladder_{period}",
                "benchmarks/event_bench.py",
                ["--period", period, "--ladder", "v4", "--airports", "study",
                 "--out-name", f"ladder_{period}.csv", "--executors", "12"]
                + (["--reuse-table", *reuse_rungs,
                    "--reuse-newer-than", reuse_newer_than]
                   if reuse_rungs else []),
                {f"ladder_{period}.csv": f"ladder_{period}.csv",
                 f"inventory_{period}.csv": f"inventory_{period}.csv"},
                DETECT,
                "the cumulative ladder from the reconstructed v0.1.0 "
                "configuration to the shipped one, one behaviour per rung, "
                "scored on this period -- plus the extraction inventory, which "
                "is the only place the families APDF cannot score are visible "
                "at all. V01 is deliberately config-identical to V00: the "
                "cross-track tie-break is a bug fix, not a flag, and its effect "
                "is read off runway_2026.csv against V3's runway_2025.csv.",
                inputs=[T_TRACKS_CLEAN, T_FLIGHT_LIST, T_REF],
            )
        )
    for period in PERIODS:
        out.append(
            Job(
                f"compare_{period}",
                "benchmarks/events_compare.py",
                ["--period", period, "--ladder", "v4", "--rung", SHIPPED_RUNG,
                 "--airports", "study", "--executors", "12"],
                # Every CSV `events_compare.py` writes, not the subset the
                # paper happened to need first. An output this job produces but
                # does not declare is not copied into the paper's data/ -- so
                # the paper keeps whatever copy was there, and `--check`
                # reports the job "ok" because staleness is computed only over
                # declared outputs. That is how per_airport_by_detector,
                # pooling_rules and rings came to be read from a manual run
                # made at 39093a1, three commits before the on_ground fix,
                # while the tables beside them were current.
                {f"per_airport_{period}.csv": f"per_airport_{period}.csv",
                 f"per_airport_by_detector_{period}.csv":
                     f"per_airport_by_detector_{period}.csv",
                 f"pooling_rules_{period}.csv": f"pooling_rules_{period}.csv",
                 f"rings_{period}.csv": f"rings_{period}.csv",
                 f"floor_{period}.csv": f"floor_{period}.csv",
                 f"runway_{period}.csv": f"runway_{period}.csv",
                 f"resolution_{period}.csv": f"resolution_{period}.csv"},
                SCORE + ["benchmarks/events_compare.py", "benchmarks/event_bench.py"],
                "the shipped rung split three ways: per aerodrome (the network "
                "figure is an average over aerodromes differing in size by a "
                "factor of twenty), runway identity against AP_C_RWY, and by "
                "whether the aerodrome's APDF times are readable to the second "
                "-- 64% land on a whole minute, so an unstratified error "
                "distribution mostly measures the reference's quantisation. "
                "Reads the event table the ladder already wrote; re-runs no "
                "detector. The runway table carries both the stable "
                "milestone label (ATOT/ALDT either side of the vocabulary "
                "change) and the `det_type` that produced it, so it can be "
                "read against V3's runway_2025.csv column-to-column without "
                "pretending the two rows came from the same detector. It also "
                "computes the ring and inter-source-floor "
                "tables, which V4 does not stage: V3 measured those and "
                "nothing in V4 changes the ring detector.",
                inputs=[T_FLIGHT_LIST, T_REF],
            )
        )
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true", help="report staleness, run nothing")
    ap.add_argument("--force", action="store_true", help="run everything")
    ap.add_argument("--only", nargs="*", default=None)
    ap.add_argument("--skip-stages", action="store_true",
                    help="leave the upstream 2026 flight list out of the chain. "
                         "On by default nowhere: unlike V6's stages, this one "
                         "builds a table no production run ever writes, so a "
                         "chain without it is not reproducible.")
    ap.add_argument(
        "--reuse-rungs", nargs="*", default=[], metavar="RUNG",
        help=(
            "Score these ladder rungs from the event tables already on S3 "
            "rather than recomputing them, for resuming a ladder that died "
            "partway. The rung scores live only in the driver's memory until "
            "the final CSV write, so a crash on the last rung loses all eight "
            "sets of numbers while leaving seven completed tables intact. "
            "Requires --reuse-newer-than, and lands in the provenance manifest "
            "as part of argv, so the paper can show which rungs were reused."
        ),
    )
    ap.add_argument(
        "--reuse-newer-than", default=None, metavar="ISO8601",
        help="A reused rung table is accepted only if written after this instant.",
    )
    ap.add_argument("--rebuild-stage", nargs="*", default=[],
                    help="force these stages to rebuild their table rather "
                         "than record it")
    args = ap.parse_args()

    if args.reuse_rungs and not args.reuse_newer_than:
        raise SystemExit("--reuse-rungs requires --reuse-newer-than")
    todo = ([] if args.skip_stages else stages()) + jobs(
        reuse_rungs=args.reuse_rungs, reuse_newer_than=args.reuse_newer_than
    )
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

    rebuild = set(args.rebuild_stage)
    for j in todo:
        forced = j.name in rebuild
        if args.force or stale[j.name] or forced:
            print(f"\n=== {j.name} ===  ({stale[j.name] or 'forced'})")
            j.run(rebuild=forced) if isinstance(j, Stage) else j.run()
        else:
            print(f"  skipping {j.name} (current)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
