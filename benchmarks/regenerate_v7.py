"""
The executable definition of the V7 analysis.

Every number in ``papers/adep-ades-detection-v7/`` is produced by exactly one
job listed here, with exactly the arguments listed here. The report renders by
calling this module, so the numbers on the page are the numbers this code
produces -- not numbers someone once copied into a directory.

Each job declares the source files it depends on. An output is **stale** when
the fingerprint over those files differs from the one recorded when the output
was written, so editing ``flights.py`` marks every pipeline job for re-run while
editing this docstring marks nothing. Age is not staleness: a file written a
month ago by unchanged code is current, and one written a minute ago by
since-changed code is not.

    python benchmarks/regenerate_v7.py --check     # what is stale, run nothing
    python benchmarks/regenerate_v7.py             # run only what is stale
    python benchmarks/regenerate_v7.py --force     # run everything

``--check`` needs no credentials and no cluster. Running a stale job needs both,
because the numbers come from Spark over S3 against Network Manager reference
data, and there is no way to recompute them without the data they are computed
from.

**What differs from V7's predecessor.** The upstream stages now include step
02a, trajectory cleaning, because the flight list reads its output -- in V6 the
cleaning step existed and fed nothing. And every sweep runs on **both** periods,
because V7 chooses parameters on the two jointly rather than on one and checking
the other afterwards.
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

PAPER = REPO.parent / "opdi-portal" / "papers" / "adep-ades-detection-v7"
DATA = PAPER / "data"

DAYS_2025 = ["2025-06-05", "2025-06-06", "2025-06-07"]
DAYS_2024 = ["2024-06-05", "2024-06-06", "2024-06-07"]

#: Flight-level caps: 25 to 300 in steps of 25, plus the legacy 40 and the
#: shipped 60 so both stay on the grid rather than being interpolated between
#: neighbours. V6 stopped at FL120 with departures still rising, which read as
#: "higher is always better" when the curve had not been followed far enough.
FL_CAPS = sorted({40, 60} | set(range(25, 301, 25)))

#: Source files whose contents define each job's result. Named explicitly: a
#: dependency worth re-running for is a dependency worth writing down.
CORE = ["benchmarks/adep_ades.py", "benchmarks/osn_sample.py"]
PIPE = CORE + ["src/opdi/pipeline/flights.py", "src/opdi/config.py",
               "benchmarks/flight_list_v7.py"]

#: S3 prefixes the analysis reads.
T_TRACKS      = "s3a://eurocontrol/opdi/osn_tracks"
T_TRACKS_CLN  = "s3a://eurocontrol/opdi/osn_tracks_clean"
T_TRACKS24    = "s3a://eurocontrol/opdi/research/tracks"
T_TRACKS24_CL = "s3a://eurocontrol/opdi/research/tracks_clean"
T_ZONES       = "s3a://eurocontrol/opdi/h3_airport_detection_zones"
T_CAND        = "s3a://eurocontrol/opdi/opdi_endpoint_candidates"
T_CAND24      = "s3a://eurocontrol/opdi/research/cand_2024"
T_VOTES       = "s3a://eurocontrol/opdi/research/trend_votes"
T_VOTES24     = "s3a://eurocontrol/opdi/research/trend_votes_2024"
T_REF         = "s3a://eurocontrol/opdi/research/reference"

#: The installed console script, not ``python -m opdi.cli``: a module
#: invocation resolves `opdi` to ``opdi.py`` at the repo root, which shadows the
#: package in ``src/`` and fails with a bare ModuleNotFoundError.
OPDI = [str(REPO / ".venv310" / "bin" / "opdi"), "run", "--env", "opensky"]
PIPELINE_SRC = ["src/opdi/runner.py", "src/opdi/config.py"]


class Stage:
    """An upstream pipeline step, whose product is an S3 table, not a CSV.

    These are the steps the analysis used to assume had already been run:
    reference zones, ingestion, track building, cleaning, and the caches the
    sweeps read. Without them the chain is reproducible only from whatever
    happens to be in the bucket, which is not reproducibility.

    Each step is idempotent by its own progress log, so a stage whose data is
    already present is a fast no-op. That is what makes it affordable to put
    them in the chain rather than in a README.
    """

    def __init__(self, name, cmd, produces, code_paths, notes="", inputs=()):
        self.name = name
        self.cmd = cmd
        self.produces = produces
        self.code_paths = list(code_paths)
        self.notes = notes
        self.script = cmd[0] if cmd else ""
        self.args = cmd[1:]
        # S3 prefixes this stage derives from. Without these a stage whose
        # input has been rebuilt looks current, and the chain quietly keeps
        # serving a cache derived from data that no longer exists -- which is
        # exactly what happens when the sampler or the cleaner changes.
        self.inputs = list(inputs)
        self.outputs = {}

    @property
    def key(self):
        return f"table:{self.produces}"

    #: Below this a "table" is committer scaffolding, not data. A run killed
    #: mid-write leaves the magic committer's `.pendingset` markers behind --
    #: tens of objects and a few kilobytes -- and a table with objects in it
    #: otherwise reads as present, so the chain would *record* the wreckage
    #: rather than rebuild it. Nothing would fail; every figure downstream would
    #: just be computed from a table that is not there.
    MIN_BYTES = 1_000_000

    def stale(self):
        ident = provenance.s3_identity(self.produces)
        if ident.get("error"):
            return {self.produces: f"cannot check ({ident['error']})"}
        if not ident.get("objects"):
            return {self.produces: "table absent or empty"}
        if ident.get("bytes", 0) < self.MIN_BYTES:
            return {self.produces:
                    f"only {ident['bytes'] / 1e6:.2f} MB across "
                    f"{ident['objects']} objects -- committer scaffolding from "
                    f"an interrupted write, not a table"}
        entry = provenance.load_manifest(DATA).get(self.key)
        if entry is None:
            return {self.produces: "present, but no provenance recorded"}
        why = provenance.inputs_changed(entry, self.inputs)
        if why:
            return {self.produces: f"must rebuild: {why}"}
        return {}

    def run(self, extra=(), rebuild=False):
        """Build the table if it is missing; otherwise record what it is.

        Deliberately *not* rebuilt on ``--force``. These stages produce the
        inputs every published figure is computed from -- regenerating the
        airport zone table would change the candidate set under the whole study,
        and rebuilding tracks would cost hours to arrive at the same rows. When
        the table is already there the honest action is to record its identity
        and say it was not rebuilt, rather than churn the study's inputs for the
        sake of a green tick. ``--rebuild-stage`` names the exceptions.
        """
        print(f"\n=== {self.name} ===", flush=True)
        ident = provenance.s3_identity(self.produces)
        entry = provenance.load_manifest(DATA).get(self.key)
        stale_input = provenance.inputs_changed(entry, self.inputs) if entry else ""
        # Same threshold as `stale`: committer leftovers are not a table, and
        # recording them as one is the quietest way to lose a whole run.
        real = ident.get("bytes", 0) >= self.MIN_BYTES
        present = real and not stale_input and not rebuild
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
    """One analysis step: a command, its outputs, and what it depends on."""

    def __init__(self, name, script, args, outputs, code_paths, notes="",
                 inputs=()):
        self.name = name
        self.script = script
        self.args = args
        self.outputs = outputs
        self.code_paths = [script] + list(code_paths)
        self.notes = notes
        self.inputs = list(inputs)

    def stale(self):
        reasons = {}
        for staged in self.outputs.values():
            bad, why = provenance.is_stale(
                DATA, staged, self.code_paths, self.inputs)
            if bad:
                reasons[staged] = why
        return reasons

    def run(self, extra=()):
        """Run into a scratch directory, then stage and stamp the outputs.

        Writing to scratch first means a job that dies halfway leaves the
        committed data untouched, rather than half-replacing it with something
        that renders but is wrong.
        """
        with tempfile.TemporaryDirectory(prefix=f"v7_{self.name}_") as tmp:
            cmd = [sys.executable, "-u", str(REPO / self.script),
                   *self.args, "--results-dir", tmp, *extra]
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
                provenance.record(
                    DATA, staged, self.script, self.args,
                    self.code_paths, notes=self.notes, input_tables=self.inputs,
                )
                print(f"  staged {staged}")


def stages() -> list:
    """The upstream steps the analysis depends on.

    Ground truth is deliberately absent. ``research/reference`` is extracted by
    the ``eurocontrol`` R package against the PRISME Oracle warehouse, which
    runs only on a machine with that access -- so no render on this cluster can
    rebuild it, and claiming otherwise in a chain that silently reads a
    committed parquet would be worse than saying so.
    """
    return [
        Stage("00_reference_data",
              OPDI + ["--step", "00", "--start", DAYS_2025[0], "--end", DAYS_2025[-1]],
              T_ZONES, PIPELINE_SRC + ["src/opdi/reference/h3_airport_zones.py"],
              "H3 detection zones and the OurAirports reference the candidate "
              "builder reads field elevations from. Step ids are 00/01/02/... "
              "-- there is no 00a at this level, the substeps live inside 00"),

        Stage("01_02_sample",
              [sys.executable, "-u", "benchmarks/rebuild_sample.py",
               "--start", DAYS_2025[0], "--end", DAYS_2025[-1],
               "--executors", "10"],
              T_TRACKS,
              ["benchmarks/rebuild_sample.py",
               "src/opdi/ingestion/osn_statevectors.py",
               "src/opdi/pipeline/tracks.py", "src/opdi/config.py"],
              "ingest and track building with the bin-based sampler. Replaces "
              "rather than appends, because the rows being replaced were made "
              "by a different rule"),

        Stage("02a_clean_tracks",
              [sys.executable, "-u", "benchmarks/clean_tracks.py",
               "--period", "2025", "--executors", "10"],
              T_TRACKS_CLN,
              ["benchmarks/clean_tracks.py", "src/opdi/cleaning/cleaner.py",
               "src/opdi/cleaning/native.py", "src/opdi/config.py"],
              "trajectory cleaning -- the step the flight list now reads. In "
              "V6 this table existed and nothing read it",
              inputs=[T_TRACKS]),

        Stage("02a_clean_tracks_2024",
              [sys.executable, "-u", "benchmarks/clean_tracks.py",
               "--period", "2024", "--executors", "10"],
              T_TRACKS24_CL,
              ["benchmarks/clean_tracks.py", "src/opdi/cleaning/cleaner.py",
               "src/opdi/cleaning/native.py", "src/opdi/config.py"],
              "the same for the second period, so both are measured on the "
              "same input treatment",
              inputs=[T_TRACKS24]),

        Stage("03_endpoint_candidates",
              [sys.executable, "-u", "benchmarks/build_candidates.py",
               "--month", "202506", "--executors", "10"],
              T_CAND,
              ["benchmarks/build_candidates.py", "src/opdi/pipeline/flights.py",
               "src/opdi/config.py"],
              "first/last fix per track against every aerodrome within 110 NM; "
              "the cache the endpoint sweeps filter",
              inputs=[T_TRACKS_CLN, T_ZONES]),

        Stage("03_endpoint_candidates_2024",
              [sys.executable, "-u", "benchmarks/build_candidates_2024.py",
               "--days", *DAYS_2024, "--executors", "10"],
              T_CAND24,
              ["benchmarks/build_candidates_2024.py",
               "src/opdi/pipeline/flights.py"],
              "endpoint candidates for the second period, built by the "
              "pipeline's own builder over a pre-reduced endpoint table",
              inputs=[T_TRACKS24_CL]),

        Stage("03_trend_votes_2025",
              [sys.executable, "-u", "benchmarks/trend_sweep.py",
               "--months", "202506", "--days", *DAYS_2025, "--build",
               "--build-only", "--tracks", T_TRACKS_CLN,
               "--fl-caps", *[str(f) for f in FL_CAPS],
               "--executors", "10", "--results-dir", "/tmp/v7_votecache_2025"],
              T_VOTES, ["benchmarks/trend_sweep.py"],
              "vote counts per (track, aerodrome) at every flight-level cap, "
              "in one pass -- which is what makes a 14-cap grid affordable",
              inputs=[T_TRACKS_CLN, T_ZONES]),

        Stage("03_trend_votes_2024",
              [sys.executable, "-u", "benchmarks/trend_sweep.py",
               "--months", "202406", "--days", *DAYS_2024, "--build",
               "--tracks", T_TRACKS24_CL, "--add-h3", "--cache", T_VOTES24,
               "--out-name", "trend_sweep_2024.csv", "--build-only",
               "--fl-caps", *[str(f) for f in FL_CAPS],
               "--executors", "10", "--results-dir", "/tmp/v7_votecache_2024"],
              T_VOTES24, ["benchmarks/trend_sweep.py"],
              "second period; its tracks pre-date H3 so the index is computed",
              inputs=[T_TRACKS24_CL]),
    ]


def jobs() -> list:
    """Every job behind the report, in dependency order."""
    v7 = "benchmarks/flight_list_v7.py"
    return [
        # --- research sweeps, both periods, identical grids ---------------
        Job("trend_sweep_2025", "benchmarks/trend_sweep.py",
            ["--months", "202506", "--days", *DAYS_2025,
             "--tracks", T_TRACKS_CLN,
             "--fl-caps", *[str(f) for f in FL_CAPS], "--executors", "10"],
            {"trend_sweep.csv": "trend_sweep_2025.csv"}, CORE,
            "the full trend grid over the cached vote table",
            inputs=[T_VOTES, T_ZONES, T_REF]),

        Job("trend_sweep_2024", "benchmarks/trend_sweep.py",
            ["--months", "202406", "--days", *DAYS_2024,
             "--tracks", T_TRACKS24_CL, "--add-h3", "--cache", T_VOTES24,
             "--out-name", "trend_sweep_2024.csv",
             "--fl-caps", *[str(f) for f in FL_CAPS], "--executors", "10"],
            {"trend_sweep_2024.csv": "trend_sweep_2024.csv"}, CORE,
            "the identical grid on the second period, so the two can be ranked "
            "jointly rather than compared as two argmaxes",
            inputs=[T_VOTES24, T_ZONES, T_REF]),

        Job("endpoint_sweep_2025", "benchmarks/benchmark_modes.py",
            ["--months", "202506", "--days", *DAYS_2025, "--sweeps-only"],
            {"sweep_radius_height.csv": "sweep_radius_height_2025.csv",
             "sweep_penalty.csv": "sweep_penalty_2025.csv"}, CORE,
            "--sweeps-only: this script can also score pipeline output written "
            "by another run, which the report does not use",
            inputs=[T_CAND, T_REF]),

        Job("endpoint_sweep_2024", "benchmarks/benchmark_modes.py",
            ["--months", "202406", "--days", *DAYS_2024, "--sweeps-only",
             "--candidates", T_CAND24],
            {"sweep_radius_height.csv": "sweep_radius_height_2024.csv"}, CORE,
            "the endpoint grid on the second period",
            inputs=[T_CAND24, T_REF]),

        # --- the pipeline itself, both periods ----------------------------
        Job("ladder_2025", v7,
            ["--period", "2025", "--runs", "ladder",
             "--out-name", "ladder_2025.csv", "--executors", "10"],
            {"ladder_2025.csv": "ladder_2025.csv"}, PIPE,
            "thirteen cumulative steps from the legacy algorithm on raw "
            "tracks to the shipped pipeline on cleaned ones, each run through "
            "process_dai itself. verify_plan asserts the last rung IS the "
            "shipped configuration before any of it runs",
            inputs=[T_TRACKS_CLN, T_TRACKS, T_ZONES, T_CAND, T_REF]),

        Job("ladder_2024", v7,
            ["--period", "2024", "--runs", "ladder",
             "--out-name", "ladder_2024.csv", "--executors", "10"],
            {"ladder_2024.csv": "ladder_2024.csv"}, PIPE,
            "the same ladder on the second period: which steps hold and which "
            "were one sample's noise",
            inputs=[T_TRACKS24_CL, T_ZONES, T_CAND24, T_REF]),

        Job("modes_2025", v7,
            ["--period", "2025", "--runs", "modes",
             "--per-airport", "--out-name", "modes_2025.csv", "--executors", "10"],
            {"modes_2025.csv": "modes_2025.csv",
             "per_airport_v7.csv": "per_airport_2025.csv",
             "per_type_v7.csv": "per_type_2025.csv"}, PIPE,
            "the whole configurations the verdict chooses between. What "
            "cleaning is worth is the ladder's last rung, not a separate run: "
            "there it is measured given everything else already adopted",
            inputs=[T_TRACKS_CLN, T_ZONES, T_CAND, T_REF]),

        Job("modes_2024", v7,
            ["--period", "2024", "--runs", "modes",
             "--per-airport", "--out-name", "modes_2024.csv", "--executors", "10"],
            {"modes_2024.csv": "modes_2024.csv",
             "per_airport_v7.csv": "per_airport_2024.csv",
             "per_type_v7.csv": "per_type_2024.csv"}, PIPE,
            "the same configurations on the second period",
            inputs=[T_TRACKS24_CL, T_ZONES, T_CAND24, T_REF]),

        # Radius 20 NM only. Fourteen caps at two radii is fifty-six full
        # `process_dai` runs across the two periods, and the radius is already
        # answered twice over -- by the research sweep on both periods and by
        # the ladder, which walks it as its own step. The cap is the parameter
        # that needed the pipeline, because it is the one V6 measured through a
        # grid that stopped before the curve turned.
        Job("grid_2025", v7,
            ["--period", "2025", "--runs", "grid", "--grid-radius", "20",
             "--out-name", "trend_grid_2025.csv", "--executors", "10"],
            {"trend_grid_2025.csv": "trend_grid_2025.csv"}, PIPE,
            "the flight-level cap swept through the pipeline to FL300, so the "
            "departure side of the curve closes rather than running out",
            inputs=[T_TRACKS_CLN, T_ZONES, T_CAND, T_REF]),

        Job("grid_2024", v7,
            ["--period", "2024", "--runs", "grid", "--grid-radius", "20",
             "--out-name", "trend_grid_2024.csv", "--executors", "10"],
            {"trend_grid_2024.csv": "trend_grid_2024.csv"}, PIPE,
            "the same grid on the second period, so the cap is chosen on both",
            inputs=[T_TRACKS24_CL, T_ZONES, T_CAND24, T_REF]),

        # The sampler is deliberately NOT re-measured here.
        #
        # V6 compared the two samplers across every parameter cell of two
        # complete runs, on identical grids over identically-treated input, and
        # found the difference indistinguishable from zero. V7 cannot repeat
        # that: there is no fixed-phase arm left to compare against, and its
        # sweeps run on cleaned tracks over a different flight-level grid --
        # so a comparison against V6's arm would be confounded by the cleaning
        # and the grid rather than measuring the sampler.
        #
        # Two arms on different data is precisely the failure this study keeps
        # producing, and the honest handling is to cite the measurement that was
        # made properly rather than to manufacture one that was not.
    ]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true",
                    help="report staleness and exit; no cluster needed")
    ap.add_argument("--force", action="store_true", help="re-run every job")
    ap.add_argument("--only", nargs="+", help="run only these jobs, by name")
    ap.add_argument("--with-stages", action="store_true",
                    help="include the upstream pipeline steps -- reference "
                         "zones, ingestion, tracks, cleaning and the caches. "
                         "Off by default because they are idempotent and slow")
    ap.add_argument("--stages-only", action="store_true",
                    help="run the upstream steps and stop")
    ap.add_argument("--rebuild-stage", nargs="+", default=[],
                    help="force these stages to rebuild their table rather "
                         "than record it. Downstream stages then follow "
                         "automatically, because their inputs will have moved.")
    ap.add_argument("--allow-stale", action="store_true",
                    help="report staleness but exit 0 -- for rendering a draft "
                         "without a cluster")
    args = ap.parse_args()

    todo = []
    if args.with_stages or args.stages_only:
        todo += stages()
    if not args.stages_only:
        todo += jobs()
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

    rebuild = set(args.rebuild_stage)
    for j in todo:
        forced = j.name in rebuild
        if args.force or stale[j.name] or forced:
            j.run(rebuild=forced) if isinstance(j, Stage) else j.run()
        else:
            print(f"  skipping {j.name} (current)")
    print("\ndone")


if __name__ == "__main__":
    main()
