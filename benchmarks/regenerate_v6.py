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


class Job:
    """One analysis step: a command, its outputs, and what it depends on."""

    def __init__(self, name, script, args, outputs, code_paths, notes=""):
        self.name = name
        self.script = script
        self.args = args
        self.outputs = outputs      # {produced filename: staged filename}
        self.code_paths = [script] + list(code_paths)
        self.notes = notes

    def stale(self):
        reasons = {}
        for staged in self.outputs.values():
            bad, why = provenance.is_stale(DATA, staged, self.code_paths)
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
                )
                print(f"  staged {staged}")


def jobs() -> list:
    """Every job behind the report, in dependency order."""
    return [
        Job("trend_sweep_2025", "benchmarks/trend_sweep.py",
            ["--months", "202506", "--days", *DAYS_2025, "--executors", "10"],
            {"trend_sweep.csv": "trend_sweep_2025.csv"}, CORE,
            "371 cells over the cached vote table; --build rebuilds that cache"),

        Job("trend_sweep_2024", "benchmarks/trend_sweep.py",
            ["--months", "202406", "--days", *DAYS_2024,
             "--tracks", "s3a://eurocontrol/opdi/research/tracks", "--add-h3",
             "--cache", "s3a://eurocontrol/opdi/research/trend_votes_2024",
             "--out-name", "trend_sweep_2024.csv", "--executors", "10"],
            {"trend_sweep_2024.csv": "trend_sweep_2024.csv"}, CORE,
            "second period; tracks pre-date H3 indexing so the index is computed"),

        Job("endpoint_sweeps", "benchmarks/benchmark_modes.py",
            ["--months", "202506", "--days", *DAYS_2025, "--sweeps-only"],
            {"sweep_radius_height.csv": "sweep_radius_height_2025.csv",
             "sweep_penalty.csv": "sweep_penalty_2025.csv",
             "sweep_cone.csv": "sweep_cone_2025.csv"}, CORE,
            "--sweeps-only: this script can also score pipeline output written "
            "by another run, which the report does not use"),

        Job("bearing", "benchmarks/bearing_whole_sample.py",
            ["--months", "202506", "--days", *DAYS_2025, "--executors", "10"],
            {"whole_sample.csv": "bearing_whole_sample_v6.csv"},
            CORE + ["benchmarks/abstained_vertical.py"],
            "rescue / veto / replace / rerank against the endpoint baseline"),

        Job("modes", "benchmarks/flight_list_v6.py",
            ["--months", "202506", "--days", *DAYS_2025,
             "--trend-sweep", str(DATA / "trend_sweep_2025.csv"),
             "--endpoint-sweep", str(DATA / "sweep_radius_height_2025.csv"),
             "--runs", "legacy", "trend", "endpoint", "nearest", "combined",
             "recommended", "--trend-rank-by", "haversine", "--executors", "10"],
            {"mode_comparison_v6.csv": "mode_comparison_v6.csv",
             "per_airport_v6.csv": "per_airport_v6.csv",
             "per_type_v6.csv": "per_type_v6.csv"}, PIPE,
            "real process_dai runs; this is what the verdict is scored on"),

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
            "harness -- this is where production's own optimum is found"),

        Job("pipeline_path", "benchmarks/flight_list_v6.py",
            ["--months", "202506", "--days", *DAYS_2025,
             "--trend-sweep", str(DATA / "trend_sweep_2025.csv"),
             "--endpoint-sweep", str(DATA / "sweep_radius_height_2025.csv"),
             "--runs", "path0_legacy", "path1_penalty", "path2_flcap",
             "path3_margin", "path4_radius",
             "--trend-rank-by", "haversine", "--executors", "10"],
            {"mode_comparison_v6.csv": "pipeline_path_v6.csv",
             "per_airport_v6.csv": "per_airport_path_v6.csv"}, PIPE,
            "the arrival tuning walked one parameter at a time"),
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
