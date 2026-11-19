"""
The executable definition of the track-construction V5 analysis.

Every number in ``papers/track-construction-v5/`` comes from exactly one job
listed here, with exactly the arguments listed here, run on the code that makes
``cruise_linked`` the default track rule.

    python benchmarks/regenerate_track_v5.py --check     # what is stale
    python benchmarks/regenerate_track_v5.py             # run only what is stale
    python benchmarks/regenerate_track_v5.py --force     # run everything

``--check`` needs no credentials and no cluster. The job class, the staleness
rule and the command line are those of ``regenerate_track_v4.py``; only the job
list and the paper differ.

Two jobs, each staging into its own subdirectory of ``data/``:

  * ``two_days`` -- the shipped setting over 2026-06-14 and 2025-06-14, and the
    same with ``link_old_edge_climb`` and ``link_new_edge_descent`` each off and
    on, against Network Manager truth: how many flights end up in one track, and
    how many links join two flights. The reasons reports are read at the shipped
    setting.
  * ``pipeline_day`` -- both rules through the real pipeline for 2026-06-14 on
    one airframe in ten, scored: flight list, events, level segments and
    distance at a hole, distance on tracks without a hole, movements, and
    whether each arm's tables belong together.

**``benchmarks/run_track_link_day.py`` is a prerequisite, not a job.**
``pipeline_day`` needs both arms built first at ``research/a10``. It writes
tables, not files under ``data/``, so a staleness check cannot key on it. It
also writes ``data/pipeline_day/link_step_timings.csv`` (``--timings-csv``),
how long each pipeline step took per arm, which no job here regenerates.
"""

import json
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

import provenance  # noqa: E402,F401  (re-exported for the tests and the builder)
import regenerate_track_v4 as v4  # noqa: E402

#: See ``regenerate_track_v4.PAPER`` for why an override exists. Each paper has
#: its own variable: one shared name would point every builder at one paper.
PAPER = Path(
    os.environ.get(
        "OPDI_PAPER_DIR_V5",
        REPO.parent / "opdi-portal" / "papers" / "track-construction-v5",
    )
)
DATA = PAPER / "data"

DAYS = v4.DAYS
PIPELINE_DAY = "2026-06-14"

#: The setting ``SegmentationConfig`` ships. Named in full because the grid
#: script keeps its pre-study base for anything left out; a test holds this
#: equal to the config.
SHIPPED = {
    "link_min_implied_speed_kt": 400,
    "link_min_speed_ratio": 0.0,
    "link_min_stop_minutes": 0.0,
    "link_max_gap_minutes": 600,
    "link_cruise_floor_ft": 18000,
    "link_old_edge_climb": True,
    "link_new_edge_descent": True,
}

#: Every table ``track_link_pipeline_score.py`` writes. Kept here as a literal,
#: since importing the scorer imports pyspark and ``--check`` must run anywhere;
#: a test holds the two equal.
PIPELINE_DAY_OUTPUTS = [
    *v4.STAGE2_OUTPUTS,
    "link_consistency.csv",
    "link_distance_no_hole.csv",
]


def jobs() -> list:
    return [
        v4.Job(
            name="two_days",
            script="benchmarks/track_link_grid.py",
            args=[
                "--days",
                *DAYS,
                "--grid-json",
                json.dumps(
                    {
                        **{k: [v] for k, v in SHIPPED.items()},
                        "link_old_edge_climb": [False, True],
                        "link_new_edge_descent": [False, True],
                    }
                ),
                "--reasons-params-json",
                json.dumps(SHIPPED),
            ],
            outputs={n: n for n in v4._grid_outputs(DAYS) + v4._reasons_outputs(DAYS)},
            code_paths=v4.CORE,
            data_root=DATA,
            notes="The shipped setting over two days against Network Manager "
            "truth, with climb and descent edges each off and on; reasons "
            "read at the shipped setting.",
        ),
        v4.Job(
            name="pipeline_day",
            script="benchmarks/track_link_pipeline_score.py",
            args=["--day", PIPELINE_DAY],
            outputs={n: n for n in PIPELINE_DAY_OUTPUTS},
            code_paths=v4.CORE
            + [
                "src/opdi/pipeline/flights.py",
                "src/opdi/pipeline/events.py",
                "src/opdi/pipeline/holes.py",
                "benchmarks/run_track_link_day.py",
            ],
            data_root=DATA,
            notes="Both rules at research/a10 through the real pipeline, one "
            "day, one airframe in ten. Needs run_track_link_day.py to "
            "have built the arms.",
        ),
    ]


def main() -> None:
    v4.main(jobs, __doc__)


if __name__ == "__main__":
    main()
