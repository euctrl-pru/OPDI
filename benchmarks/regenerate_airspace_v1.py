"""Regenerate the figures behind the airspace-events-v1 paper (holding, point merge, FIR).

    python benchmarks/regenerate_airspace_v1.py --check     # what is stale
    python benchmarks/regenerate_airspace_v1.py             # run only what is stale
    python benchmarks/regenerate_airspace_v1.py --force     # run everything

Same arrangement as ``regenerate_track_v5.py``: each CSV under the paper's
``data/`` is produced by exactly one job declared here, stamped with the code
it depends on, and re-run only when that code changed.

Jobs and what they need:

``holding_labels``
    The holding rule against the Olive et al. 2019 labels for one day at
    EGLL and EIDW. Needs the 4TU trajectory files and ``hp_labels.parquet.gz``
    under ``OPDI_HOLDING_DATA_DIR`` (not committed: CC BY-NC-SA). Local Spark.
``fir``
    FIR crossings on the pipeline day and the H3-versus-polygon check. Needs
    the cluster and the study prefix written by ``run_airspace_events_day.py``.
``point_merge``
    Point merge usage per airport on the pipeline day. Same inputs.

The pipeline-day run itself (``run_airspace_events_day.py``: steps 00c, 03,
04 off and 04 on) is not a job here: it writes tables, not figures, and is
run by hand once. Its timings CSV and the extraction counts are copied in by
``benchmarks/airspace_counts.py``.
"""

import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

import provenance  # noqa: E402,F401
import regenerate_track_v4 as v4  # noqa: E402

PAPER = Path(os.environ.get(
    "OPDI_PAPER_DIR_AIRSPACE", REPO.parent / "opdi-portal" / "papers" / "airspace-events-v1"))
DATA = PAPER / "data"
PIPELINE_DAY = "2026-06-14"
LABEL_DAY = "2019-10-01"
HOLDING_DATA = os.environ.get("OPDI_HOLDING_DATA_DIR", str(Path.home() / "holding_data"))

CORE = [
    "src/opdi/config.py",
    "src/opdi/pipeline/holes.py",
]


def jobs() -> list:
    return [
        v4.Job(
            name="holding",
            script="benchmarks/holding_labels_score.py",
            args=["--day", LABEL_DAY, "--airports", "EGLL", "EIDW", "--data-dir", HOLDING_DATA],
            outputs={n: n for n in (f"holding_score_{LABEL_DAY}.csv", f"holding_sweep_{LABEL_DAY}.csv",
                                    f"holding_episodes_{LABEL_DAY}.csv")},
            code_paths=CORE + ["src/opdi/pipeline/holding.py"],
            data_root=DATA,
            notes="Shipped holding thresholds and a three-threshold sweep against the hand-corrected "
                  "2019 labels (one day, EGLL and EIDW), scored as the label paper scores.",
        ),
        v4.Job(
            name="fir",
            script="benchmarks/fir_consistency.py",
            args=["--day", PIPELINE_DAY],
            outputs={n: n for n in (f"fir_consistency_{PIPELINE_DAY}.csv", f"fir_crossings_{PIPELINE_DAY}.csv")},
            code_paths=CORE + ["src/opdi/pipeline/fir.py", "src/opdi/reference/h3_airspaces.py"],
            data_root=DATA,
            notes="H3 lookup against exact polygons on a sample of crossing tracks; the day's crossings.",
        ),
        v4.Job(
            name="point_merge",
            script="benchmarks/point_merge_report.py",
            args=["--day", PIPELINE_DAY],
            outputs={f"point_merge_usage_{PIPELINE_DAY}.csv": f"point_merge_usage_{PIPELINE_DAY}.csv"},
            code_paths=CORE + ["src/opdi/pipeline/point_merge.py", "src/opdi/reference/point_merge.py",
                               "src/opdi/reference/data/point_merge_systems.csv"],
            data_root=DATA,
            notes="Arrivals with a sequencing leg per covered airport on the pipeline day.",
        ),
    ]


def main() -> None:
    v4.main(jobs, __doc__)


if __name__ == "__main__":
    main()
