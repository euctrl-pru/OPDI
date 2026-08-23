"""
The executable definition of the v6.1 analysis: `trend` on the field-elevation
datum.

Every number in ``papers/adep-ades-detection-v6.1/`` is produced by exactly one
job listed here, with exactly the arguments listed here. The report renders by
calling this module; the numbers on the page are therefore the numbers this
code produces, not numbers someone once copied into a directory.

Forked from ``regenerate_v6.py``, which stays as V6's. Every arm runs on
**both periods** and the report pools them: the second period is confirmation,
not a second set of tables.

Each job declares the source files it depends on. An output is **stale** when
the fingerprint over those files differs from the one recorded when the output
was written -- so editing ``flights.py`` marks every pipeline job for re-run,
while editing this docstring marks nothing. Age is not staleness: a file
written a month ago by unchanged code is current, and one written a minute ago
by since-changed code is not.

    python benchmarks/regenerate_v61.py --check     # what is stale, run nothing
    python benchmarks/regenerate_v61.py             # run only what is stale
    python benchmarks/regenerate_v61.py --force     # run everything

``--check`` needs no credentials and no cluster. Running a stale job needs
both, because the numbers come from Spark over S3 against Network Manager
reference data. There is no way to recompute them without the data they are
computed from, and pretending otherwise would just move the staleness rather
than remove it.
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

def _find_portal() -> Path:
    """Locate the ``opdi-portal`` checkout.

    ``REPO.parent / "opdi-portal"`` is right when ``opdi/`` sits directly in the
    workspace root, and wrong inside a git worktree -- which lives at
    ``opdi/.claude/worktrees/<name>``, three levels deeper, so the naive path
    points at ``.../worktrees/opdi-portal`` and every output reads as missing
    rather than as stale. The two failures look identical in ``--check`` output
    and mean opposite things, so this walks up until a sibling ``opdi-portal``
    appears and the same command works from either place.

    ``OPDI_PORTAL`` overrides, for a checkout laid out some third way.
    """
    override = os.environ.get("OPDI_PORTAL")
    if override:
        return Path(override)
    for base in (REPO, *REPO.parents):
        candidate = base.parent / "opdi-portal"
        if (candidate / "papers").is_dir():
            return candidate
    raise SystemExit(
        "cannot find the opdi-portal checkout beside this repository. "
        "Set OPDI_PORTAL to its path."
    )


PAPER = _find_portal() / "papers" / "adep-ades-detection-v6.1"
DATA = PAPER / "data"

DAYS_2025 = ["2025-06-05", "2025-06-06", "2025-06-07"]
DAYS_2024 = ["2024-06-05", "2024-06-06", "2024-06-07"]

#: Source files whose contents define each job's result. Named explicitly:
#: a dependency worth re-running for is a dependency worth writing down.
CORE = ["benchmarks/adep_ades.py", "benchmarks/osn_sample.py"]
PIPE = CORE + ["src/opdi/pipeline/flights.py", "src/opdi/config.py",
               "benchmarks/flight_list_v61.py"]
#: Arm C reads the pipeline's per-airport output, so it inherits the pipeline's
#: dependencies as well as its own banding.
BANDS = PIPE + ["benchmarks/elevation_arms.py", "benchmarks/elevation_bands.py"]


class Stage:
    """An upstream pipeline step, whose product is an S3 table, not a CSV.

    These are the steps the analysis used to assume had already been run:
    reference zones, state-vector ingestion, track building, and the two caches
    the sweeps read. Without them the chain is reproducible only from whatever
    happened to be in the bucket, which is not reproducibility -- delete
    ``research/trend_votes`` and every sweep fails while the manifest still
    reports it current.

    Each step is idempotent by its own progress log, so a stage whose data is
    already present is a fast no-op. That is what makes it affordable to put
    them in the chain rather than in a README.
    """

    def __init__(self, name, cmd, produces, code_paths, notes="", inputs=()):
        self.name = name
        self.cmd = cmd            # argv, run from the repo root
        self.produces = produces  # S3 prefix this stage fills
        self.code_paths = list(code_paths)
        self.notes = notes
        self.script = cmd[0] if cmd else ""
        self.args = cmd[1:]
        # S3 prefixes this stage derives from. Without these a stage whose
        # input has been rebuilt looks current, and the pipeline quietly keeps
        # serving a cache derived from data that no longer exists -- which is
        # exactly what happens when the sampler changes.
        self.inputs = list(inputs)
        self.outputs = {}

    @property
    def key(self):
        return f"table:{self.produces}"

    def stale(self):
        """Stale when the table is absent or empty, or the code moved.

        A populated table is *not* re-derived just because the pipeline code
        changed -- rebuilding tracks for every edit to `flights.py` would cost
        hours and change nothing. The recorded fingerprint still says which
        code filled it, so a mismatch is visible in the provenance table even
        when it does not force a re-run.
        """
        ident = provenance.s3_identity(self.produces)
        if ident.get("error"):
            return {self.produces: f"cannot check ({ident['error']})"}
        if not ident.get("objects"):
            return {self.produces: "table absent or empty"}
        entry = provenance.load_manifest(DATA).get(self.key)
        if entry is None:
            return {self.produces: "present, but no provenance recorded"}
        why = provenance.inputs_changed(entry, self.inputs)
        if why:
            return {self.produces: f"must rebuild: {why}"}
        return {}

    def run(self, extra=(), rebuild=False):
        """Build the table if it is missing; otherwise just record what it is.

        ``rebuild`` forces the build. Stages deliberately do *not* rebuild on a
        code change -- editing ``flights.py`` would otherwise cost hours of
        track rebuilding for a table it does not affect -- so when a change
        genuinely does invalidate a table, as switching the sampler does, it
        has to be named.

        Deliberately *not* rebuilt on --force. These stages produce the inputs
        every published figure was computed from -- regenerating the airport
        zone table would change the candidate set under the whole study, and
        rebuilding tracks would cost hours to arrive at the same rows. When the
        table is already there, the honest action is to record its identity and
        say that it was not rebuilt, rather than to churn the study's inputs
        for the sake of a green tick.
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
    """One analysis step: a command, its outputs, and what it depends on."""

    def __init__(self, name, script, args, outputs, code_paths, notes="",
                 inputs=()):
        self.name = name
        self.script = script
        self.args = args
        self.outputs = outputs      # {produced filename: staged filename}
        self.code_paths = [script] + list(code_paths)
        self.notes = notes
        self.inputs = list(inputs)  # S3 prefixes this job reads

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
        with tempfile.TemporaryDirectory(prefix=f"v6_{self.name}_") as tmp:
            cmd = [sys.executable, "-u", str(REPO / self.script),
                   *self.args, "--results-dir", tmp, *extra]
            print(f"\n=== {self.name} ===\n  {' '.join(cmd[2:])}", flush=True)
            r = subprocess.run(cmd, cwd=REPO)
            if r.returncode != 0:
                raise SystemExit(f"{self.name} failed with exit {r.returncode}")
            for produced, staged in self.outputs.items():
                src = Path(tmp) / produced
                if not src.is_file():
                    raise SystemExit(
                        f"{self.name} did not produce {produced}")
                DATA.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, DATA / staged)
                provenance.record(
                    DATA, staged, self.script, self.args,
                    self.code_paths, notes=self.notes,
                    input_tables=self.inputs,
                )
                print(f"  staged {staged}")


#: S3 prefixes the analysis reads.
T_TRACKS   = "s3a://eurocontrol/opdi/osn_tracks"
T_TRACKS24 = "s3a://eurocontrol/opdi/research/tracks"
T_ZONES    = "s3a://eurocontrol/opdi/h3_airport_detection_zones"
T_CAND     = "s3a://eurocontrol/opdi/opdi_endpoint_candidates"
#: v6.1's own vote caches, deliberately *not* V6's `research/trend_votes`.
#: Writing into V6's prefix would leave its paper reproducible only against a
#: cache built for a different study.
T_VOTES    = "s3a://eurocontrol/opdi/research/trend_votes_agl"
T_VOTES24  = "s3a://eurocontrol/opdi/research/trend_votes_agl_2024"
T_SV       = "s3a://eurocontrol/opdi/osn_statevectors_v2"
T_REF      = "s3a://eurocontrol/opdi/research/reference"
T_CAND24   = "s3a://eurocontrol/opdi/research/cand_2024"

#: The same 2024 tracks, named the way *the pipeline* wants them.
#:
#: `trend_sweep_agl.py` reads parquet directly and needs the full URI;
#: `FlightListProcessor(tracks_table=...)` takes a bucket-relative name and
#: prefixes it itself. Passing the URI to the latter produced the splice
#: `s3a://eurocontrol/opdi/s3a:/eurocontrol/opdi/research/tracks` and a
#: PATH_NOT_FOUND two hours into a run. Two names because there are genuinely
#: two conventions, written down so the next caller picks the right one.
T_TRACKS24_NAME = "research/tracks"

#: The installed console script, not ``python -m opdi.cli``: a module
#: invocation resolves `opdi` to ``opdi.py`` at the repo root, which shadows the
#: package in ``src/`` and fails with a bare ModuleNotFoundError.
OPDI = [str(REPO / ".venv310" / "bin" / "opdi"), "run", "--env", "opensky"]
PIPELINE_SRC = ["src/opdi/runner.py", "src/opdi/config.py"]


def stages() -> list:
    """The upstream steps the analysis depends on.

    Ground truth is deliberately absent. ``research/reference`` is extracted by
    the ``eurocontrol`` R package against the PRISME Oracle warehouse, which
    runs only on a machine with that access -- so no render on this cluster can
    rebuild it, and claiming otherwise in a chain that silently reads a
    committed parquet would be worse than saying so.
    """
    return [
        Stage("01_02_rebuild_sample",
              [sys.executable, "-u", "benchmarks/rebuild_sample.py",
               "--start", DAYS_2025[0], "--end", DAYS_2025[-1],
               "--executors", "10"],
              T_TRACKS,
              ["benchmarks/rebuild_sample.py",
               "src/opdi/ingestion/osn_statevectors.py",
               "src/opdi/pipeline/tracks.py", "src/opdi/config.py"],
              "ingest and track building with the configured sampler. Replaces "
              "rather than appends, because the rows being replaced were made "
              "by a different rule"),

        Stage("00_reference_data",
              OPDI + ["--step", "00", "--start", DAYS_2025[0], "--end", DAYS_2025[-1]],
              T_ZONES, PIPELINE_SRC + ["src/opdi/reference/h3_airport_zones.py"],
              "H3 detection zones and the OurAirports reference the candidate "
              "builder reads field elevations from. Step ids are 00/01/02/... "
              "-- there is no 00a at this level, the substeps live inside 00"),

        Stage("03_endpoint_candidates",
              [sys.executable, "-u", "benchmarks/build_candidates.py",
               "--month", "202506", "--executors", "10"],
              T_CAND,
              ["benchmarks/build_candidates.py", "src/opdi/pipeline/flights.py",
               "src/opdi/config.py"],
              "first/last fix per track against every aerodrome within 110 NM; "
              "the cache the endpoint sweeps filter",
              inputs=[T_TRACKS, T_ZONES]),

        Stage("03_trend_votes_agl_2025",
              [sys.executable, "-u", "benchmarks/trend_sweep_agl.py",
               "--months", "202506", "--days", *DAYS_2025, "--build",
               "--build-only", "--cache", T_VOTES, "--executors", "10",
               "--results-dir", "/tmp/v61_votecache_2025"],
              T_VOTES, ["benchmarks/trend_sweep_agl.py",
                        "benchmarks/elevation_bands.py"],
              "vote counts per (track, aerodrome) at every cap on BOTH datums, "
              "in one pass. Writes v6.1's own prefix; V6's trend_votes is left "
              "alone so V6 stays reproducible",
              inputs=[T_TRACKS, T_ZONES]),

        Stage("03_trend_votes_agl_2024",
              [sys.executable, "-u", "benchmarks/trend_sweep_agl.py",
               "--months", "202406", "--days", *DAYS_2024, "--build",
               "--tracks", T_TRACKS24, "--add-h3", "--cache", T_VOTES24,
               "--build-only", "--executors", "10",
               "--results-dir", "/tmp/v61_votecache_2024"],
              T_VOTES24, ["benchmarks/trend_sweep_agl.py",
                          "benchmarks/elevation_bands.py"],
              "second period; its tracks pre-date H3 so the index is computed"),

        Stage("03_endpoint_candidates_2024",
              [sys.executable, "-u", "benchmarks/build_candidates_2024.py",
               "--days", *DAYS_2024, "--executors", "10"],
              T_CAND24,
              ["benchmarks/build_candidates_2024.py",
               "src/opdi/pipeline/flights.py"],
              "endpoint candidates for the second period, built by the "
              "pipeline's own builder over a pre-reduced endpoint table"),
    ]


def jobs() -> list:
    """The four arms of the v6.1 study, in dependency order.

    Every arm runs on both periods. The report pools them and treats 2024 as
    confirmation in prose rather than as a duplicate set of tables -- but the
    job list carries both explicitly, because an arm measured on one period and
    quietly reported as the study's result is the failure this study's own
    notes keep recording.
    """
    out = []
    for period, months, days, tracks, votes in (
        ("2025", "202506", DAYS_2025, None, T_VOTES),
        # The pipeline's spelling, not the sweep's -- see T_TRACKS24_NAME.
        ("2024", "202406", DAYS_2024, T_TRACKS24_NAME, T_VOTES24),
    ):
        tracks_arg = ["--tracks", tracks] if tracks else []
        cand = T_CAND if period == "2025" else T_CAND24
        trk = T_TRACKS if period == "2025" else T_TRACKS24

        # --- Arm A: the datum swap, one variable -------------------------
        out.append(Job(
            f"datum_swap_{period}", "benchmarks/flight_list_v61.py",
            ["--months", months, "--days", *days, *tracks_arg,
             "--runs", "datum_msl", "datum_field", "legacy",
             "--out-name", "datum_comparison.csv", "--executors", "10"],
            {"datum_comparison.csv": f"datum_swap_{period}.csv",
             "per_airport_v61.csv": f"per_airport_datum_{period}.csv"},
            PIPE,
            "Arm A: FL60 against 6,100 ft above field, through process_dai "
            "itself. The ceiling is identical -- flight_level is an int cast, "
            "so FL60 reaches 6,100 -- which leaves the datum as the only "
            "difference. `legacy` rides along as the published-constants control",
            inputs=[trk, T_ZONES, cand, T_REF]))

        # --- Arm C: the bands, read off Arm A's per-airport output --------
        #
        # Deliberately downstream of Arm A rather than a second scoring pass:
        # it reads the same `per_airport_counts` output the rest of the study
        # is scored with, so the banded numbers cannot disagree with the
        # headline ones.
        out.append(Job(
            f"elevation_bands_{period}", "benchmarks/elevation_arms.py",
            ["--per-airport", str(DATA / f"per_airport_datum_{period}.csv")],
            {"elevation_bands.csv": f"elevation_bands_{period}.csv",
             "elevation_per_airport.csv": f"elevation_per_airport_{period}.csv"},
            BANDS,
            "Arm C, the discriminating measurement: the datum's effect banded "
            "by field elevation, with both robustness controls. The census "
            "showed each treatment band rests on one aerodrome, so band means "
            "alone cannot separate an elevation effect from a Madrid effect",
            inputs=[T_REF]))

        # --- Arm B: the above-field ceiling swept over the cache ----------
        out.append(Job(
            f"height_sweep_{period}", "benchmarks/trend_sweep_agl.py",
            ["--months", months, "--days", *days, "--cache", votes,
             "--datum", "field", "--out-name", "trend_sweep.csv",
             "--executors", "10"],
            {"trend_sweep.csv": f"height_sweep_{period}.csv"},
            CORE + ["benchmarks/trend_sweep_agl.py",
                    "benchmarks/elevation_bands.py"],
            "Arm B: the above-field ceiling swept on its own terms, rather "
            "than inherited from the datum being abandoned",
            inputs=[votes, T_ZONES, T_REF]))

        # --- Arm B, control: the same sweep on the sea-level datum --------
        out.append(Job(
            f"fl_sweep_{period}", "benchmarks/trend_sweep_agl.py",
            ["--months", months, "--days", *days, "--cache", votes,
             "--datum", "msl", "--out-name", "trend_sweep.csv",
             "--executors", "10"],
            {"trend_sweep.csv": f"fl_sweep_{period}.csv"},
            CORE + ["benchmarks/trend_sweep_agl.py",
                    "benchmarks/elevation_bands.py"],
            "the same sweep on the sea-level datum, out of the same cache. "
            "Without it the height sweep has no like-for-like curve to be read "
            "against, and a shape would be reported where a comparison belongs",
            inputs=[votes, T_ZONES, T_REF]))

        # --- Arm D: pipeline fidelity at the swept ceiling ----------------
        out.append(Job(
            f"height_pipeline_{period}", "benchmarks/flight_list_v61.py",
            ["--months", months, "--days", *days, *tracks_arg,
             "--runs", "height_3000", "height_4000", "height_6100",
             "height_8000", "height_10000", "height_12000",
             "--out-name", "datum_comparison.csv", "--executors", "10"],
            {"datum_comparison.csv": f"height_pipeline_{period}.csv"},
            PIPE,
            "Arm D: the above-field ceiling walked through process_dai itself, "
            "so the shipped figure is a pipeline figure and not a harness one "
            "-- the check V6 makes a point of and the reason its tuned FL cap "
            "did not survive contact with production",
            inputs=[trk, T_ZONES, cand, T_REF]))

    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true",
                    help="report staleness and exit; no cluster needed")
    ap.add_argument("--force", action="store_true", help="re-run every job")
    ap.add_argument("--only", nargs="+", help="run only these jobs, by name")
    ap.add_argument("--with-stages", action="store_true",
                    help="include the upstream pipeline steps -- reference "
                         "zones, ingestion, tracks and the two caches. Off by "
                         "default because they are idempotent and slow; on, "
                         "the chain rebuilds the analysis from the archive "
                         "rather than from whatever is in the bucket.")
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
