"""Guards on how v6.2 turns a sweep row into an altitude cut.

The failure this prevents is silent and expensive: a config carrying a
field-datum ceiling *and* a stale `trend_max_fl` is measured at whichever the
code reads first, while the paper reports the other. Nothing in the output
says which one ran.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks"))

import dataclasses

import pytest

import flight_list_v62
from opdi.config import DetectionConfig


def test_field_row_sets_the_height_and_the_datum():
    kw = flight_list_v62.trend_ceiling_kwargs({"datum": "field", "fl_cap": "6100"})
    assert kw["trend_max_datum"] == "field"
    assert kw["trend_max_height_ft"] == 6100.0


def test_field_row_does_not_also_set_a_flight_level():
    """Two ceilings on one config is the silent-drift failure."""
    kw = flight_list_v62.trend_ceiling_kwargs({"datum": "field", "fl_cap": "6100"})
    assert "trend_max_fl" not in kw


def test_msl_row_sets_the_flight_level_and_the_datum():
    kw = flight_list_v62.trend_ceiling_kwargs({"datum": "msl", "fl_cap": "60"})
    assert kw["trend_max_datum"] == "msl"
    assert kw["trend_max_fl"] == 60
    assert "trend_max_height_ft" not in kw


def test_a_row_with_no_datum_column_is_msl():
    """V6's own sweep CSVs pre-date the column. Reading one must not silently
    reinterpret a flight level as a height in feet -- FL60 as 60 ft above
    field would abstain on everything."""
    kw = flight_list_v62.trend_ceiling_kwargs({"fl_cap": "60"})
    assert kw["trend_max_datum"] == "msl"
    assert kw["trend_max_fl"] == 60


def test_an_unknown_datum_is_refused_not_defaulted():
    with pytest.raises(ValueError):
        flight_list_v62.trend_ceiling_kwargs({"datum": "agl", "fl_cap": "6100"})


@pytest.mark.parametrize("row", [
    {"datum": "field", "fl_cap": "6100"},
    {"datum": "msl", "fl_cap": "60"},
])
def test_the_kwargs_are_accepted_by_the_real_config(row):
    """A helper that returns keys DetectionConfig rejects is worse than no
    helper: it fails hours in, inside a Spark job."""
    cfg = dataclasses.replace(DetectionConfig(),
                              **flight_list_v62.trend_ceiling_kwargs(row))
    assert cfg.trend_max_datum in ("field", "msl")


def test_the_pipeline_grid_brackets_the_shipped_ceiling():
    """The shipped default is 6000 ft and the sweep grid never contained it,
    so "the ceiling is tuned" rested on a grid missing the tuned value. The
    pipeline grid must carry both 6000 and 6100 for the paper to settle it."""
    assert 6000 in flight_list_v62.GRID_HEIGHT_CAPS
    assert 6100 in flight_list_v62.GRID_HEIGHT_CAPS
    assert DetectionConfig().trend_max_height_ft in flight_list_v62.GRID_HEIGHT_CAPS


def test_a_worktree_resolves_its_pipeline_logs_to_the_main_checkout():
    """`OPDI_live/logs` is repo-level state and a worktree has no copy.

    Left worktree-relative, the endpoint-candidate progress log is not found,
    `process_dai` concludes the candidate table needs rebuilding, and it tries
    to write `opdi_endpoint_candidates` -- production. The write guard stops
    it, but only after the run has spent its time getting there.
    """
    wt = Path("/repo/.claude/worktrees/some-branch")
    assert flight_list_v62.main_checkout(wt) == Path("/repo")


def test_a_normal_checkout_is_left_alone():
    plain = Path("/repo")
    assert flight_list_v62.main_checkout(plain) == plain


def test_a_directory_merely_called_worktrees_is_not_treated_as_one():
    """The marker is `.claude/worktrees/<name>`, not the word anywhere in the
    path -- otherwise a project directory named `worktrees` would silently
    redirect its own pipeline logs three levels up."""
    p = Path("/repo/worktrees/thing")
    assert flight_list_v62.main_checkout(p) == p


def test_the_datum_arm_ceiling_matches_the_integer_flight_level_cut():
    """`flight_level` is an integer cast, so FL60 admits everything below
    6100 ft. Comparing the datums at 6000 would move the ceiling and the
    datum at once and measure neither."""
    assert flight_list_v62.DATUM_ARM_CEILING_FT == 6100.0


def test_the_grid_sweeps_the_margin_main_actually_ships():
    """The grid fixed the vote margin at 2 while DetectionConfig ships 0, so
    the shipped value was validated by nothing at all -- and main's
    configuration was not a cell the grid had ever run, which makes a parity
    check impossible rather than merely failing."""
    assert DetectionConfig().trend_vote_margin in flight_list_v62.GRID_MARGINS
    assert set(flight_list_v62.GRID_MARGINS) == {0, 2, 4}


def test_mains_whole_configuration_is_a_cell_of_the_grid():
    """Ceiling, radius and margin together. Each being present individually is
    not enough: the parity check looks for one cell matching all three."""
    c = DetectionConfig()
    assert c.trend_max_height_ft in flight_list_v62.GRID_HEIGHT_CAPS
    assert c.trend_radius_nm in (20.0, 30.0)
    assert c.trend_vote_margin in flight_list_v62.GRID_MARGINS


@pytest.mark.parametrize("fl,ft", [(40, 4100.0), (60, 6100.0), (80, 8100.0),
                                   (100, 10100.0), (200, 20100.0)])
def test_the_msl_equivalent_is_the_top_of_the_flight_level_not_its_face_value(fl, ft):
    """The cut is `flight_level <= fl` on an integer cast, so it reaches
    (fl + 1) * 100 ft. Using fl * 100 would shave 100 ft off the band and
    attribute the difference to the datum."""
    assert flight_list_v62.msl_equivalent_height_ft(fl) == ft


def test_the_datum_arm_ceiling_is_derived_not_typed():
    """Tying the constant to the rule is what stops the two drifting when a
    different sea-level optimum is chosen."""
    assert (flight_list_v62.DATUM_ARM_CEILING_FT
            == flight_list_v62.msl_equivalent_height_ft(60))


def test_a_field_sweep_cannot_supply_the_path_walks_lower_rungs():
    """Rungs 0-4 must sit on the sea-level datum, or the datum has already
    moved before rung 5 -- the rung whose whole purpose is to move it. Rung 5
    would then measure zero, which reads as evidence and is an artefact.

    `trend_ceiling_kwargs` is what the walk uses to decide, so the guard is
    that a field row is recognisable as such.
    """
    field = flight_list_v62.trend_ceiling_kwargs({"datum": "field",
                                                 "fl_cap": "6100"})
    assert field["trend_max_datum"] != "msl"
