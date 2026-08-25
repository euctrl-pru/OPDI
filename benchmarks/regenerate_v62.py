"""
The executable definition of the V6.2 analysis.

Every number in ``papers/adep-ades-detection-v6.2/`` is produced by exactly one
job listed here, with exactly the arguments listed here. The report renders by
calling this module; the numbers on the page are therefore the numbers this
code produces, not numbers someone once copied into a directory.

V6.2 is V6's study recomputed on the datum that ships: the trend altitude cut
is a height above the aerodrome's own field elevation, not a flight level. V6
remains frozen and reproducible -- ``regenerate_v6.py`` and ``flight_list_v6.py``
are untouched, and this module writes neither into V6's paper directory nor
into its vote caches.

Each job declares the source files it depends on. An output is **stale** when
the fingerprint over those files differs from the one recorded when the output
was written -- so editing ``flights.py`` marks every pipeline job for re-run,
while editing this docstring marks nothing. Age is not staleness: a file
written a month ago by unchanged code is current, and one written a minute ago
by since-changed code is not.

    python benchmarks/regenerate_v62.py --check     # what is stale, run nothing
    python benchmarks/regenerate_v62.py             # run only what is stale
    python benchmarks/regenerate_v62.py --force     # run everything

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

import flight_list_v62  # noqa: E402
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


PAPER = _find_portal() / "papers" / "adep-ades-detection-v6.2"
DATA = PAPER / "data"

#: V6's committed data, READ ONLY. The equivalence check joins against it, and
#: that is the only reason this study knows where V6's directory is. Nothing
#: writes here -- a test asserts it.
V6_DATA = _find_portal() / "papers" / "adep-ades-detection-v6" / "data"

DAYS_2025 = ["2025-06-05", "2025-06-06", "2025-06-07"]
DAYS_2024 = ["2024-06-05", "2024-06-06", "2024-06-07"]

#: Source files whose contents define each job's result. Named explicitly:
#: a dependency worth re-running for is a dependency worth writing down.
CORE = ["benchmarks/adep_ades.py", "benchmarks/osn_sample.py"]
PIPE = CORE + ["src/opdi/pipeline/flights.py", "src/opdi/config.py",
               "benchmarks/flight_list_v62.py"]
#: Arm C reads the banding along with the counts, so a band edit must mark it
#: stale. It is the study's discriminating measurement.
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
#: The paired vote caches, deliberately *not* V6's `research/trend_votes`.
#: They carry both `up_fl_*` and `up_agl_*` counts, which is what lets one
#: build serve both datums. Writing into V6's prefix would leave its paper
#: reproducible only against a cache built for a different study.
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
               "--results-dir", "/tmp/v62_votecache_2025"],
              T_VOTES, ["benchmarks/trend_sweep_agl.py",
                        "benchmarks/elevation_bands.py"],
              "vote counts per (track, aerodrome) at every cap on BOTH datums, "
              "in one pass. This is what lets the sea-level sweep and the "
              "above-field sweep be the same measurement read two ways. Writes "
              "its own prefix; V6's trend_votes is left alone so V6 stays "
              "reproducible",
              inputs=[T_TRACKS, T_ZONES]),

        Stage("03_trend_votes_agl_2024",
              [sys.executable, "-u", "benchmarks/trend_sweep_agl.py",
               "--months", "202406", "--days", *DAYS_2024, "--build",
               "--tracks", T_TRACKS24, "--add-h3", "--cache", T_VOTES24,
               "--build-only", "--executors", "10",
               "--results-dir", "/tmp/v62_votecache_2024"],
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
    """Every job behind the report, in dependency order.

    Three things distinguish this list from V6's.

    **V6's two `trend_sweep` jobs are gone.** `trend_sweep_agl.py` is a strict
    superset of `trend_sweep.py` -- same FL_CAPS, MARGINS, RADII_NM,
    PENALTIES_NM -- and its cache carries both datums' votes, so one run per
    period yields the sea-level curve and the above-field curve together. That
    is not an assumption: the sea-level arm was compared against V6's committed
    `trend_sweep_2025.csv` and `trend_sweep_2024.csv` on the join key
    (stage, stage2_role, fl_cap, radius_nm, penalty_nm, margin, k, legacy),
    giving 371 shared cells and zero differing cells on each period. Keeping
    both would be two jobs computing the same numbers on the same datum.

    **The pipeline arms read the above-field sweep**, because that is the datum
    they run on, and additionally the sea-level sweep for the path walk's lower
    rungs -- see `flight_list_v62.trend_ceiling_kwargs` for why mixing them
    silently mismeasures the rung that moves the datum.

    **The pipeline arms are 2025 only.** `process_dai` reads `h3_res_7` off the
    track table; the 2024 tracks pre-date H3 indexing, so the column is absent
    and the run dies on UNRESOLVED_COLUMN -- after the 2025 half has already
    been computed, which is the expensive place to find out. V6 met the same
    wall and ran the pipeline on 2025 alone. The second period confirms through
    the paired sweeps, which read a cache built with the index computed.
    """
    return [
        # --- the paired sweeps: both datums, both periods ------------------
        Job("height_sweep_2025", "benchmarks/trend_sweep_agl.py",
            ["--months", "202506", "--days", *DAYS_2025, "--cache", T_VOTES,
             "--datum", "field", "--out-name", "trend_sweep.csv",
             "--executors", "10"],
            {"trend_sweep.csv": "height_sweep_2025.csv"},
            CORE + ["benchmarks/trend_sweep_agl.py",
                    "benchmarks/elevation_bands.py"],
            "the above-field ceiling swept on its own terms, rather than "
            "inherited from the datum being abandoned",
            inputs=[T_VOTES, T_ZONES, T_REF]),

        Job("fl_sweep_2025", "benchmarks/trend_sweep_agl.py",
            ["--months", "202506", "--days", *DAYS_2025, "--cache", T_VOTES,
             "--datum", "msl", "--out-name", "trend_sweep.csv",
             "--executors", "10"],
            {"trend_sweep.csv": "fl_sweep_2025.csv"},
            CORE + ["benchmarks/trend_sweep_agl.py",
                    "benchmarks/elevation_bands.py"],
            "the same sweep on the sea-level datum, out of the same cache. "
            "Without it the height sweep has no like-for-like curve to be read "
            "against, and a shape would be reported where a comparison belongs. "
            "Also the replacement for V6's trend_sweep_2025, which it "
            "reproduces cell for cell",
            inputs=[T_VOTES, T_ZONES, T_REF]),

        Job("height_sweep_2024", "benchmarks/trend_sweep_agl.py",
            ["--months", "202406", "--days", *DAYS_2024, "--cache", T_VOTES24,
             "--datum", "field", "--out-name", "trend_sweep.csv",
             "--executors", "10"],
            {"trend_sweep.csv": "height_sweep_2024.csv"},
            CORE + ["benchmarks/trend_sweep_agl.py",
                    "benchmarks/elevation_bands.py"],
            "the above-field curve on the second period. With the pipeline "
            "arms unavailable here, the paired sweeps are this period's whole "
            "confirmation",
            inputs=[T_VOTES24, T_ZONES, T_REF]),

        Job("fl_sweep_2024", "benchmarks/trend_sweep_agl.py",
            ["--months", "202406", "--days", *DAYS_2024, "--cache", T_VOTES24,
             "--datum", "msl", "--out-name", "trend_sweep.csv",
             "--executors", "10"],
            {"trend_sweep.csv": "fl_sweep_2024.csv"},
            CORE + ["benchmarks/trend_sweep_agl.py",
                    "benchmarks/elevation_bands.py"],
            "the sea-level curve on the second period, and the replacement for "
            "V6's trend_sweep_2024",
            inputs=[T_VOTES24, T_ZONES, T_REF]),

        # --- the evidence for retiring V6's trend sweeps --------------------
        #
        # Needs no cluster: it reads two committed CSVs. Registered as a job
        # anyway, because "the sea-level arm reproduces V6's sweep" is a load-
        # bearing claim -- two jobs were deleted on the strength of it -- and a
        # load-bearing claim asserted in prose is exactly what this module
        # exists to stop.
        Job("sweep_equivalence", "benchmarks/sweep_equivalence.py",
            ["--pairs",
             f"2025={V6_DATA / 'trend_sweep_2025.csv'},"
             f"{DATA / 'fl_sweep_2025.csv'}",
             f"2024={V6_DATA / 'trend_sweep_2024.csv'},"
             f"{DATA / 'fl_sweep_2024.csv'}"],
            {"sweep_equivalence.csv": "sweep_equivalence.csv"},
            ["benchmarks/sweep_equivalence.py"],
            "cell-by-cell join of V6's committed trend sweep against the "
            "sea-level arm of this study's paired sweep, on both periods. Any "
            "non-zero count here means retiring V6's sweep jobs was wrong"),

        # --- what moved underneath this study since V6 ----------------------
        #
        # V6.2 claims to recompute V6 with one variable changed. That claim is
        # checkable, and it does not fully hold: `opdi_endpoint_candidates` was
        # rebuilt after V6 published. Measuring the drift is the only way the
        # report can say which of its numbers are comparable to V6's.
        Job("input_drift", "benchmarks/input_drift.py",
            ["--baseline", str(V6_DATA / "_manifest.json")],
            {"input_drift.csv": "input_drift.csv"},
            ["benchmarks/input_drift.py"],
            "each input table's identity as V6 recorded it, against the table "
            "as it stands now. Distinguishes the zero-byte directory marker "
            "that s3_identity stopped counting -- bookkeeping -- from a table "
            "genuinely rebuilt, which moves every figure derived from it"),

        # --- the endpoint family: datum-independent ------------------------
        #
        # These four do not read the trend altitude cut at all, so the datum
        # cannot move them.
        #
        # They were expected to reproduce V6's numbers exactly, and they do
        # not. The reason is not the datum: `opdi_endpoint_candidates` was
        # rebuilt on 2026-08-22, after V6 published -- 12% smaller, with the
        # fresh-broadcast share roughly tripled. `input_drift` above measures
        # that, and the report states it rather than presenting the difference
        # as a result.
        #
        # The consequence is worth being precise about. It costs this study the
        # *direct* verification that the datum leaves departures alone -- V6's
        # endpoint numbers and V6.2's are no longer a controlled comparison.
        # What survives is the within-study evidence: departures score zero in
        # every elevation band of `elevation_bands_2025`, where both arms read
        # the same candidate table. Weaker, and reported as such.
        Job("endpoint_sweeps", "benchmarks/benchmark_modes.py",
            ["--months", "202506", "--days", *DAYS_2025, "--sweeps-only"],
            {"sweep_radius_height.csv": "sweep_radius_height_2025.csv",
             "sweep_penalty.csv": "sweep_penalty_2025.csv",
             "sweep_cone.csv": "sweep_cone_2025.csv"}, CORE,
            "--sweeps-only: this script can also score pipeline output written "
            "by another run, which the report does not use",
            inputs=[T_CAND, T_REF]),

        Job("endpoint_sweeps_2024", "benchmarks/benchmark_modes.py",
            ["--months", "202406", "--days", *DAYS_2024, "--sweeps-only",
             "--candidates", T_CAND24],
            {"sweep_radius_height.csv": "sweep_radius_height_2024.csv"}, CORE,
            "the endpoint grid on the second period",
            inputs=[T_CAND24, T_REF]),

        Job("bearing", "benchmarks/bearing_whole_sample.py",
            ["--months", "202506", "--days", *DAYS_2025, "--executors", "10"],
            {"whole_sample.csv": "bearing_whole_sample_v6.csv"},
            CORE + ["benchmarks/abstained_vertical.py"],
            "rescue / veto / replace / rerank against the endpoint baseline",
            inputs=[T_CAND, T_TRACKS, T_REF]),

        Job("vertical_measure", "benchmarks/vertical_measure.py",
            ["--executors", "10"],
            {"vertical_measure.csv": "vertical_measure_v6.csv"},
            CORE + ["benchmarks/abstained_vertical.py"],
            "which vertical measure can be trusted: broadcast rate, two-point "
            "slope, or OLS over the window",
            inputs=[T_CAND, T_TRACKS, T_REF]),

        # --- unchanged from V6 ---------------------------------------------
        Job("sampler_comparison", "benchmarks/sampler_comparison.py", [],
            {"sampler_comparison.csv": "sampler_comparison_v6.csv"},
            CORE + ["benchmarks/sampler_comparison.py"],
            "bucket against modulo across every parameter cell of two full "
            "runs. Replaces the end-to-end decimation harness, whose two arms "
            "had drifted onto different periods and which cannot be run at all "
            "now that the bucket rule is the production default"),

        Job("merge_diagnosis", "benchmarks/merge_diagnosis.py",
            ["--executors", "8"],
            {"merge_shapes.csv": "merge_shapes_v6.csv",
             "merge_agreement.csv": "merge_agreement_v6.csv"},
            CORE,
            "whether taking the two roles from different methods loses "
            "arrivals, or only moves which track the benchmark pairs with each "
            "reference flight",
            inputs=[T_REF]),

        Job("trend_bearing", "benchmarks/trend_bearing.py",
            ["--months", "202506", "--days", *DAYS_2025, "--executors", "10"],
            {"trend_bearing.csv": "trend_bearing_v6.csv"},
            CORE + ["benchmarks/abstained_vertical.py",
                    "benchmarks/trend_sweep_agl.py"],
            "bearing applied to trend rather than to the endpoint family: "
            "rerank, tie-break and veto against the shipped configuration",
            inputs=[T_VOTES, T_TRACKS, T_REF]),

        # --- the pipeline arms, on the shipped datum ------------------------
        Job("modes_2025", "benchmarks/flight_list_v62.py",
            ["--months", "202506", "--days", *DAYS_2025,
             "--trend-sweep", str(DATA / "height_sweep_2025.csv"),
             "--trend-sweep-msl", str(DATA / "fl_sweep_2025.csv"),
             "--endpoint-sweep", str(DATA / "sweep_radius_height_2025.csv"),
             "--runs", "legacy", "trend", "endpoint", "nearest", "combined",
             "recommended", "--trend-rank-by", "haversine", "--executors", "10"],
            {"mode_comparison_v6.csv": "mode_comparison_v6.csv",
             "per_airport_v6.csv": "per_airport_v6.csv",
             "per_type_v6.csv": "per_type_v6.csv"}, PIPE,
            "real process_dai runs; this is what the verdict is scored on",
            inputs=[T_TRACKS, T_ZONES, T_CAND, T_REF]),

        Job("trend_grid_2025", "benchmarks/flight_list_v62.py",
            ["--months", "202506", "--days", *DAYS_2025,
             "--trend-sweep", str(DATA / "height_sweep_2025.csv"),
             "--trend-sweep-msl", str(DATA / "fl_sweep_2025.csv"),
             "--endpoint-sweep", str(DATA / "sweep_radius_height_2025.csv"),
             "--runs", *[f"grid_h{c}_r{r:g}_m{m}"
                         for c in flight_list_v62.GRID_HEIGHT_CAPS
                         for r in (20, 30)
                         for m in flight_list_v62.GRID_MARGINS],
             "--grid-height", *[str(c) for c in flight_list_v62.GRID_HEIGHT_CAPS],
             "--grid-radius", "20", "30",
             "--grid-margin", *[str(m) for m in flight_list_v62.GRID_MARGINS],
             "--trend-rank-by", "haversine", "--executors", "10"],
            {"mode_comparison_v6.csv": "trend_grid_v6.csv"}, PIPE,
            "the trend ceiling x radius swept through process_dai itself, in "
            "feet above field elevation. Brackets the shipped 6,000 ft with "
            "the 6,100 the sweep preferred: the sweep grid never contained "
            "6,000, so until this runs the claim that the ceiling is tuned "
            "rests on a grid missing the shipped value",
            inputs=[T_TRACKS, T_ZONES, T_CAND, T_REF]),

        Job("pipeline_path_ring_2025", "benchmarks/flight_list_v62.py",
            ["--months", "202506", "--days", *DAYS_2025,
             "--trend-sweep", str(DATA / "height_sweep_2025.csv"),
             "--trend-sweep-msl", str(DATA / "fl_sweep_2025.csv"),
             "--endpoint-sweep", str(DATA / "sweep_radius_height_2025.csv"),
             "--runs", "path0_legacy", "path1_penalty", "path2_ceiling",
             "path3_margin", "path4_radius", "path5_datum",
             "--trend-rank-by", "ring", "--executors", "10"],
            {"mode_comparison_v6.csv": "pipeline_path_ring_v6.csv"}, PIPE,
            "the same path under the OLD ring-count selection. Kept as a job "
            "rather than an archived file because the ring-vs-exact comparison "
            "is the report's central result, and half of it must not be a "
            "number nobody can regenerate",
            inputs=[T_TRACKS, T_ZONES, T_CAND, T_REF]),

        Job("pipeline_path_2025", "benchmarks/flight_list_v62.py",
            ["--months", "202506", "--days", *DAYS_2025,
             "--trend-sweep", str(DATA / "height_sweep_2025.csv"),
             "--trend-sweep-msl", str(DATA / "fl_sweep_2025.csv"),
             "--endpoint-sweep", str(DATA / "sweep_radius_height_2025.csv"),
             "--runs", "path0_legacy", "path1_penalty", "path2_ceiling",
             "path3_margin", "path4_radius", "path5_datum",
             "--trend-rank-by", "haversine", "--executors", "10"],
            {"mode_comparison_v6.csv": "pipeline_path_v6.csv",
             "per_airport_v6.csv": "per_airport_path_v6.csv"}, PIPE,
            "the arrival tuning walked one parameter at a time, with the datum "
            "as the fifth and final rung",
            inputs=[T_TRACKS, T_ZONES, T_CAND, T_REF]),

        # --- the datum arms, carried over from v6.1 -------------------------
        # The sweeps are passed even though the datum arms do not tune from
        # them: `flight_list_v62.py` requires both, and builds the path walk
        # and grid whether or not the run list asks for those runs.
        Job("datum_swap_2025", "benchmarks/flight_list_v62.py",
            ["--months", "202506", "--days", *DAYS_2025,
             "--trend-sweep", str(DATA / "height_sweep_2025.csv"),
             "--trend-sweep-msl", str(DATA / "fl_sweep_2025.csv"),
             "--endpoint-sweep", str(DATA / "sweep_radius_height_2025.csv"),
             "--runs", "datum_msl", "datum_field", "legacy",
             "--trend-rank-by", "haversine", "--executors", "10"],
            {"mode_comparison_v6.csv": "datum_swap_2025.csv",
             "per_airport_v6.csv": "per_airport_datum_2025.csv"},
            CORE + ["src/opdi/pipeline/flights.py", "src/opdi/config.py",
                    "benchmarks/flight_list_v62.py"],
            "FL60 against 6,100 ft above field, through process_dai itself. "
            "The ceiling is identical -- flight_level is an int cast, so FL60 "
            "reaches 6,100 -- which leaves the datum as the only difference. "
            "`legacy` rides along as the published-constants control",
            inputs=[T_TRACKS, T_ZONES, T_CAND, T_REF]),

        Job("elevation_bands_2025", "benchmarks/elevation_arms.py",
            ["--per-airport", str(DATA / "per_airport_datum_2025.csv")],
            {"elevation_bands.csv": "elevation_bands_2025.csv",
             "elevation_per_airport.csv": "elevation_per_airport_2025.csv"},
            BANDS,
            "the discriminating measurement: the datum's effect banded by "
            "field elevation, with both robustness controls. The census showed "
            "each treatment band rests on one aerodrome, so band means alone "
            "cannot separate an elevation effect from a Madrid effect",
            inputs=[T_REF]),
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
