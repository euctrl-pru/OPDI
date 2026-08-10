"""
The executable definition of the V6 analysis.

Every number in ``papers/adep-ades-detection-v6/`` is produced by exactly one
job listed here, with exactly the arguments listed here. The report renders by
calling this module; the numbers on the page are therefore the numbers this
code produces, not numbers someone once copied into a directory.

Each job declares the source files it depends on. An output is **stale** when
the fingerprint over those files differs from the one recorded when the output
was written -- so editing ``flights.py`` marks every pipeline job for re-run,
while editing this docstring marks nothing. Age is not staleness: a file
written a month ago by unchanged code is current, and one written a minute ago
by since-changed code is not.

    python benchmarks/regenerate_v6.py --check     # what is stale, run nothing
    python benchmarks/regenerate_v6.py             # run only what is stale
    python benchmarks/regenerate_v6.py --force     # run everything

``--check`` needs no credentials and no cluster. Running a stale job needs
both, because the numbers come from Spark over S3 against Network Manager
reference data. There is no way to recompute them without the data they are
computed from, and pretending otherwise would just move the staleness rather
than remove it.
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

PAPER = REPO.parent / "opdi-portal" / "papers" / "adep-ades-detection-v6"
DATA = PAPER / "data"

DAYS_2025 = ["2025-06-05", "2025-06-06", "2025-06-07"]
DAYS_2024 = ["2024-06-05", "2024-06-06", "2024-06-07"]

#: Source files whose contents define each job's result. Named explicitly:
#: a dependency worth re-running for is a dependency worth writing down.
CORE = ["benchmarks/adep_ades.py", "benchmarks/osn_sample.py"]
PIPE = CORE + ["src/opdi/pipeline/flights.py", "src/opdi/config.py",
               "benchmarks/flight_list_v6.py"]


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

    def __init__(self, name, cmd, produces, code_paths, notes=""):
        self.name = name
        self.cmd = cmd            # argv, run from the repo root
        self.produces = produces  # S3 prefix this stage fills
        self.code_paths = list(code_paths)
        self.notes = notes
        self.script = cmd[0] if cmd else ""
        self.args = cmd[1:]
        self.inputs = []
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
        return {}

    def run(self, extra=()):
        """Build the table if it is missing; otherwise just record what it is.

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
        present = bool(ident.get("objects"))
        note = self.notes
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
            notes=note, input_tables=[self.produces],
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
T_VOTES    = "s3a://eurocontrol/opdi/research/trend_votes"
T_VOTES24  = "s3a://eurocontrol/opdi/research/trend_votes_2024"
T_SV       = "s3a://eurocontrol/opdi/osn_statevectors_v2"
T_REF      = "s3a://eurocontrol/opdi/research/reference"
T_CAND24   = "s3a://eurocontrol/opdi/research/cand_2024"

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
        Stage("00_reference_data",
              OPDI + ["--step", "00", "--start", DAYS_2025[0], "--end", DAYS_2025[-1]],
              T_ZONES, PIPELINE_SRC + ["src/opdi/reference/h3_airport_zones.py"],
              "H3 detection zones and the OurAirports reference the candidate "
              "builder reads field elevations from. Step ids are 00/01/02/... "
              "-- there is no 00a at this level, the substeps live inside 00"),

        Stage("01_ingest_statevectors",
              OPDI + ["--step", "01", "--start", DAYS_2025[0], "--end", DAYS_2025[-1]],
              T_SV, PIPELINE_SRC + ["src/opdi/ingestion/osn_statevectors.py"],
              "bbox-filtered, 5 s decimated; never the raw global 1 s feed"),

        Stage("02_tracks",
              OPDI + ["--step", "02", "--start", DAYS_2025[0], "--end", DAYS_2025[-1]],
              T_TRACKS, PIPELINE_SRC + ["src/opdi/pipeline/tracks.py"],
              "track splitting is frozen: _add_track_id must not change"),

        Stage("03_endpoint_candidates",
              [sys.executable, "-c",
               "import sys; sys.path.insert(0,'src');"
               "from opdi.config import OPDIConfig;"
               "from opdi.utils.spark import get_spark_session;"
               "from opdi.pipeline.flights import FlightListProcessor;"
               "from datetime import date;"
               "cfg=OPDIConfig.for_environment('opensky');"
               "s=get_spark_session(app_name='opdi-candidates', config=cfg, distributed=True);"
               "FlightListProcessor(s,cfg).build_endpoint_candidates(date(2025,6,1))"],
              T_CAND, PIPELINE_SRC + ["src/opdi/pipeline/flights.py"],
              "first/last fix per track against every aerodrome within 110 NM; "
              "the cache the endpoint sweeps filter"),

        Stage("03_trend_votes_2025",
              [sys.executable, "-u", "benchmarks/trend_sweep.py",
               "--months", "202506", "--days", *DAYS_2025, "--build",
               "--executors", "10", "--results-dir", "/tmp/v6_votecache_2025"],
              T_VOTES, ["benchmarks/trend_sweep.py"],
              "vote counts per (track, aerodrome) at every FL cap, in one pass"),

        Stage("03_trend_votes_2024",
              [sys.executable, "-u", "benchmarks/trend_sweep.py",
               "--months", "202406", "--days", *DAYS_2024, "--build",
               "--tracks", T_TRACKS24, "--add-h3", "--cache", T_VOTES24,
               "--out-name", "trend_sweep_2024.csv", "--executors", "10",
               "--results-dir", "/tmp/v6_votecache_2024"],
              T_VOTES24, ["benchmarks/trend_sweep.py"],
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
    """Every job behind the report, in dependency order."""
    return [
        Job("trend_sweep_2025", "benchmarks/trend_sweep.py",
            ["--months", "202506", "--days", *DAYS_2025, "--executors", "10"],
            {"trend_sweep.csv": "trend_sweep_2025.csv"}, CORE,
            "371 cells over the cached vote table",
            inputs=[T_VOTES, T_ZONES, T_REF]),

        Job("trend_sweep_2024", "benchmarks/trend_sweep.py",
            ["--months", "202406", "--days", *DAYS_2024,
             "--tracks", "s3a://eurocontrol/opdi/research/tracks", "--add-h3",
             "--cache", "s3a://eurocontrol/opdi/research/trend_votes_2024",
             "--out-name", "trend_sweep_2024.csv", "--executors", "10"],
            {"trend_sweep_2024.csv": "trend_sweep_2024.csv"}, CORE,
            "second period",
            inputs=[T_VOTES24, T_ZONES, T_REF]),

        Job("endpoint_sweeps", "benchmarks/benchmark_modes.py",
            ["--months", "202506", "--days", *DAYS_2025, "--sweeps-only"],
            {"sweep_radius_height.csv": "sweep_radius_height_2025.csv",
             "sweep_penalty.csv": "sweep_penalty_2025.csv",
             "sweep_cone.csv": "sweep_cone_2025.csv"}, CORE,
            "--sweeps-only: this script can also score pipeline output written "
            "by another run, which the report does not use",
            inputs=[T_CAND, T_REF]),

        Job("bearing", "benchmarks/bearing_whole_sample.py",
            ["--months", "202506", "--days", *DAYS_2025, "--executors", "10"],
            {"whole_sample.csv": "bearing_whole_sample_v6.csv"},
            CORE + ["benchmarks/abstained_vertical.py"],
            "rescue / veto / replace / rerank against the endpoint baseline",
            inputs=[T_CAND, T_TRACKS, T_REF]),

        Job("modes", "benchmarks/flight_list_v6.py",
            ["--months", "202506", "--days", *DAYS_2025,
             "--trend-sweep", str(DATA / "trend_sweep_2025.csv"),
             "--endpoint-sweep", str(DATA / "sweep_radius_height_2025.csv"),
             "--runs", "legacy", "trend", "endpoint", "nearest", "combined",
             "recommended", "--trend-rank-by", "haversine", "--executors", "10"],
            {"mode_comparison_v6.csv": "mode_comparison_v6.csv",
             "per_airport_v6.csv": "per_airport_v6.csv",
             "per_type_v6.csv": "per_type_v6.csv"}, PIPE,
            "real process_dai runs; this is what the verdict is scored on",
            inputs=[T_TRACKS, T_ZONES, T_CAND, T_REF]),

        Job("trend_grid", "benchmarks/flight_list_v6.py",
            ["--months", "202506", "--days", *DAYS_2025,
             "--trend-sweep", str(DATA / "trend_sweep_2025.csv"),
             "--endpoint-sweep", str(DATA / "sweep_radius_height_2025.csv"),
             "--runs", *[f"grid_fl{fl}_r{r:g}_m2"
                         for fl in (40, 60, 80, 100, 120) for r in (20, 30)],
             "--grid-fl", "40", "60", "80", "100", "120",
             "--grid-radius", "20", "30", "--grid-margin", "2",
             "--trend-rank-by", "haversine", "--executors", "10"],
            {"mode_comparison_v6.csv": "trend_grid_v6.csv"}, PIPE,
            "trend FL cap x radius swept through process_dai itself, not the "
            "harness -- this is where production's own optimum is found",
            inputs=[T_TRACKS, T_ZONES, T_CAND, T_REF]),

        Job("endpoint_sweeps_2024", "benchmarks/benchmark_modes.py",
            ["--months", "202406", "--days", *DAYS_2024, "--sweeps-only",
             "--candidates", T_CAND24],
            {"sweep_radius_height.csv": "sweep_radius_height_2024.csv"}, CORE,
            "the endpoint grid on the second period -- the check the report "
            "previously listed as outstanding",
            inputs=[T_CAND24, T_REF]),

        Job("trend_bearing", "benchmarks/trend_bearing.py",
            ["--months", "202506", "--days", *DAYS_2025, "--executors", "10"],
            {"trend_bearing.csv": "trend_bearing_v6.csv"},
            CORE + ["benchmarks/abstained_vertical.py", "benchmarks/trend_sweep.py"],
            "bearing applied to trend rather than to the endpoint family: "
            "rerank, tie-break and veto against the shipped configuration",
            inputs=[T_VOTES, T_TRACKS, T_REF]),

        Job("vertical_measure", "benchmarks/vertical_measure.py",
            ["--executors", "10"],
            {"vertical_measure.csv": "vertical_measure_v6.csv"},
            CORE + ["benchmarks/abstained_vertical.py"],
            "which vertical measure can be trusted: broadcast rate, two-point "
            "slope, or OLS over the window",
            inputs=[T_CAND, T_TRACKS, T_REF]),

        Job("decimation", "benchmarks/decimation_end_to_end.py",
            ["--days", *DAYS_2025, "--month", "202506", "--skip-ingest",
             "--skip-build", "--executors", "10"],
            {"arm_comparison.csv": "decimation_v6.csv"},
            CORE + ["src/opdi/ingestion/osn_statevectors.py"],
            "bucket decimation against the modulo rule, end to end",
            inputs=[T_CAND, T_REF]),

        Job("pipeline_path_ring", "benchmarks/flight_list_v6.py",
            ["--months", "202506", "--days", *DAYS_2025,
             "--trend-sweep", str(DATA / "trend_sweep_2025.csv"),
             "--endpoint-sweep", str(DATA / "sweep_radius_height_2025.csv"),
             "--runs", "path0_legacy", "path1_penalty", "path2_flcap",
             "path3_margin", "path4_radius",
             "--trend-rank-by", "ring", "--executors", "10"],
            {"mode_comparison_v6.csv": "pipeline_path_ring_v6.csv"}, PIPE,
            "the same path under the OLD ring-count selection. Kept as a job "
            "rather than an archived file because the ring-vs-exact comparison "
            "is the report's central result, and half of it must not be a "
            "number nobody can regenerate",
            inputs=[T_TRACKS, T_ZONES, T_CAND, T_REF]),

        Job("pipeline_path", "benchmarks/flight_list_v6.py",
            ["--months", "202506", "--days", *DAYS_2025,
             "--trend-sweep", str(DATA / "trend_sweep_2025.csv"),
             "--endpoint-sweep", str(DATA / "sweep_radius_height_2025.csv"),
             "--runs", "path0_legacy", "path1_penalty", "path2_flcap",
             "path3_margin", "path4_radius",
             "--trend-rank-by", "haversine", "--executors", "10"],
            {"mode_comparison_v6.csv": "pipeline_path_v6.csv",
             "per_airport_v6.csv": "per_airport_path_v6.csv"}, PIPE,
            "the arrival tuning walked one parameter at a time",
            inputs=[T_TRACKS, T_ZONES, T_CAND, T_REF]),
    ]


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

    for j in todo:
        if args.force or stale[j.name]:
            j.run()
        else:
            print(f"  skipping {j.name} (current)")
    print("\ndone")


if __name__ == "__main__":
    main()
