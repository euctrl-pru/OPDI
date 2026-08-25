"""Guards on the main-parity checker.

Two failures it prevents. A report describing a configuration the code does not
have -- v6 claimed `trend_radius_nm` ships at 20 NM for three versions while
the code had 30, because each version copied the claim rather than measuring
it. And its mirror: a figure produced by code that never got merged.
"""

import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks"))

import pytest

import check_main_parity as parity


def _grid(tmp_path, rows):
    p = tmp_path / "trend_grid_v6.csv"
    with open(p, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    return p


#: main's configuration as a grid row.
BASE = {"run": "grid_h6000_r30_m0", "trend_ceiling": "6000",
        "trend_radius_nm": "30", "trend_vote_margin": "0",
        "trend_sched_penalty_nm": "10", "ades_score": "100"}

#: The swept optimum, scoring higher.
BETTER = dict(BASE, run="grid_h6100_r20_m2", trend_ceiling="6100",
              trend_radius_nm="20", trend_vote_margin="2", ades_score="200")

MAIN_CFG = {"trend_max_height_ft": 6000.0, "trend_radius_nm": 30.0,
            "trend_vote_margin": 0}


def test_best_cell_is_the_highest_scoring_row(tmp_path):
    p = _grid(tmp_path, [BASE, BETTER])
    assert parity.best_cell(p)["run"] == "grid_h6100_r20_m2"


def test_a_grid_without_mains_cell_is_a_distinct_failure(tmp_path):
    """A parity check against a grid that does not contain the configuration
    under test is not a check. It must say so rather than silently reporting a
    mismatch -- which is the state the harness sweep is in today, its
    HEIGHT_CAPS having no 6,000 entry."""
    p = _grid(tmp_path, [dict(BASE, trend_ceiling="9999")])
    with pytest.raises(parity.ConfigurationNotOnGrid):
        parity.config_parity(p, MAIN_CFG)


def test_matching_config_reports_no_differences(tmp_path):
    p = _grid(tmp_path, [BASE])
    diffs, report = parity.config_parity(p, MAIN_CFG)
    assert diffs == []
    assert report["rank"] == 1
    assert report["gap"] == 0


def test_differing_config_names_each_parameter(tmp_path):
    p = _grid(tmp_path, [BASE, BETTER])
    diffs, report = parity.config_parity(p, MAIN_CFG)
    assert set(diffs) == {"trend_max_height_ft", "trend_radius_nm",
                          "trend_vote_margin"}
    assert report["rank"] == 2      # main's cell is still located
    assert report["gap"] == 100     # 200 - 100


def test_a_dirty_figure_is_reported(tmp_path):
    m = tmp_path / "_manifest.json"
    m.write_text(json.dumps({
        "a.csv": {"git_sha": "deadbee", "git_dirty": True,
                  "script": "benchmarks/x.py", "code_paths": []}}))
    bad = parity.shas_on_main(m, ref="HEAD", repo=Path.cwd())
    assert any("dirty" in b for b in bad)


def test_a_missing_path_is_reported(tmp_path):
    m = tmp_path / "_manifest.json"
    m.write_text(json.dumps({
        "a.csv": {"git_sha": "HEAD", "git_dirty": False,
                  "script": "benchmarks/definitely_not_a_real_file.py",
                  "code_paths": []}}))
    repo = Path(__file__).resolve().parent.parent
    missing = parity.code_on_main(m, ref="HEAD", repo=repo)
    assert missing == ["benchmarks/definitely_not_a_real_file.py"]


MAIN_CHECKOUT = Path("/home/jupyter/work/opdi-workspace/opdi")


def test_the_config_is_read_from_the_checkout_it_was_asked_for():
    """Which `opdi` gets imported is not something to leave to sys.path.

    Measured, not assumed: under pytest a worktree's own `src` comes *ahead*
    of the venv's editable install, so an ambient `import opdi` inside a
    worktree reads the branch's config. A tool whose entire claim is "the
    report describes the code on main" cannot resolve its subject by accident.
    """
    cfg = parity.detection_config_from(MAIN_CHECKOUT)
    assert cfg["_from"].startswith(str(MAIN_CHECKOUT / "src"))
    assert "trend_max_height_ft" in cfg


def test_asking_for_a_worktree_gets_the_worktree():
    """The mirror: the loader honours whatever checkout it is given, so the
    caller -- not the ambient path -- decides what is being reported on."""
    wt = Path(__file__).resolve().parent.parent
    cfg = parity.detection_config_from(wt)
    assert cfg["_from"].startswith(str(wt / "src"))
