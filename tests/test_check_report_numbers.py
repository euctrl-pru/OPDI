"""Guards on the number-provenance checker.

The failure it prevents: a result typed into prose, which goes stale when the
data moves and says nothing when it does. Three of that class were found in
v6.2 by tripping over them -- an accuracy delta of 0.16 pp against a real 0.15,
a "seventy per cent" against a real 70.0%, and a radius v6 claimed shipped and
which never did. This finds them by looking.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks"))

import pytest

import check_report_numbers as crn


def test_a_typed_result_is_caught():
    qmd = "The band gained **+109** correct arrivals.\n"
    found = crn.typed_numbers(qmd)
    assert [f.value for f in found] == ["109"]


def test_a_computed_number_is_not_caught():
    qmd = "The band gained **`r num(delta)`** correct arrivals.\n"
    assert crn.typed_numbers(qmd) == []


def test_one_line_can_carry_both():
    """A sentence with a computed number and a typed one must flag only the
    typed one -- skipping the whole line would hide it."""
    qmd = "Coverage `r pct(cov)` against a baseline of 68.29%.\n"
    assert [f.value for f in crn.typed_numbers(qmd)] == ["68.29"]


def test_numbers_inside_r_chunks_are_ignored():
    qmd = '```{r}\nx <- 12345\n```\nProse with no number.\n'
    assert crn.typed_numbers(qmd) == []


def test_small_integers_and_years_are_exempt():
    qmd = "Two of the three, measured in 2025, across 12 cells.\n"
    assert crn.typed_numbers(qmd) == []


@pytest.mark.parametrize("text", [
    "gained 0.12%", "fell 0.15 pp", "at 6,100 ft", "within 20 NM",
])
def test_a_unit_bearing_number_is_never_exempt(text):
    """0.12% is small but it is a result. The exemption is for bare counts."""
    assert crn.typed_numbers(text + "\n"), text


def test_identifiers_that_contain_digits_are_not_measurements():
    qmd = "At S6 the cut applies, see @sec-datum and step 04 under FL60.\n"
    assert crn.typed_numbers(qmd) == []


def test_an_allowlisted_value_passes(tmp_path):
    allow = tmp_path / "constants.yml"
    allow.write_text(
        '- value: "6,100"\n'
        '  why: "A definition: FL60 admits everything below 6,100 ft."\n')
    qmd = tmp_path / "p.qmd"
    qmd.write_text("The cut reaches 6,100 ft.\n")
    assert crn.check(qmd, allow) == []


def test_a_context_restricts_where_an_exemption_applies(tmp_path):
    """Keying an allowlist by value alone lets a constant silence a result
    that happens to share its number.

    Found in the real paper: 110 is the candidate search radius in NM *and*
    the ex-busiest robustness control in the elevation bands. Allowlisting the
    radius silenced the result, which is the precise failure this checker
    exists to prevent -- reintroduced by the checker's own escape hatch.
    """
    allow = tmp_path / "constants.yml"
    allow.write_text(
        '- value: "110"\n'
        '  context: "candidate"\n'
        '  why: "The candidate search radius in NM."\n')
    qmd = tmp_path / "p.qmd"
    qmd.write_text("Every candidate within 110 NM.\n"
                   "Dropping the busiest leaves +110 correct.\n")
    bad = crn.check(qmd, allow)
    assert [f.line for f in bad] == [2], \
        "the radius is exempt; the result on line 2 is not"


def test_an_entry_without_a_context_applies_everywhere(tmp_path):
    """Context is optional: a value that could not collide does not need one."""
    allow = tmp_path / "constants.yml"
    allow.write_text('- value: "6,100"\n  why: "A definition."\n')
    qmd = tmp_path / "p.qmd"
    qmd.write_text("The cut reaches 6,100 ft.\nAnd again at 6,100 ft.\n")
    assert crn.check(qmd, allow) == []


def test_an_allowlist_entry_without_a_why_is_rejected(tmp_path):
    """A bare list of permitted numbers is a place to silence the checker."""
    allow = tmp_path / "constants.yml"
    allow.write_text('- value: "6,100"\n')
    with pytest.raises(ValueError, match="why"):
        crn.load_allowlist(allow)


def test_headings_and_table_rows_are_skipped():
    qmd = "## Section 6.2\n| a | 109 |\n"
    assert crn.typed_numbers(qmd) == []


def _find_paper() -> Path:
    """Locate v6.2's directory, from a normal checkout or from a worktree.

    `parent.parent.parent` is right when opdi/ sits in the workspace root and
    wrong inside a git worktree, which lives at opdi/.claude/worktrees/<name>
    -- three levels deeper, so the naive path lands in `worktrees/`. The test
    then *skips*, which is the worst outcome available: the check silently
    does not run and the suite still reads green.
    """
    repo = Path(__file__).resolve().parent.parent
    for base in (repo, *repo.parents):
        candidate = base.parent / "opdi-portal" / "papers"
        if candidate.is_dir():
            return candidate / "adep-ades-detection-v6.2"
    raise RuntimeError("cannot find opdi-portal/papers beside this repository")


PAPER = _find_paper()


def test_the_paper_is_found_rather_than_skipped():
    """Guards the helper above. Without this the two tests below can skip
    forever and nobody notices they stopped checking anything."""
    assert PAPER.is_dir(), f"v6.2 not found at {PAPER}"
    assert (PAPER / "index.qmd").is_file()
    assert ".claude" not in str(PAPER)


@pytest.mark.xfail(strict=True, reason=(
    "The editorial task that converts these results to inline expressions has "
    "not run yet. strict=True so this fails loudly the moment it starts "
    "passing, forcing the marker off rather than leaving a permanent xfail."))
def test_the_real_paper_has_no_undeclared_numbers():
    """The check that matters. Every number in v6.2's prose is computed or
    declared, so none can go stale without this failing."""
    if not (PAPER / "index.qmd").is_file():
        pytest.skip(f"paper not found at {PAPER}")
    bad = crn.check(PAPER / "index.qmd", PAPER / "constants.yml")
    assert not bad, "undeclared numbers:\n" + "\n".join(
        f"  line {f.line}: {f.value} -- {f.text}" for f in bad)


def test_the_papers_allowlist_is_well_formed():
    """Every entry has a why. Runs today, unlike the test above."""
    if not (PAPER / "constants.yml").is_file():
        pytest.skip("no allowlist yet")
    allowed = crn.load_allowlist(PAPER / "constants.yml")
    assert allowed, "allowlist parsed as empty"


def test_yaml_front_matter_is_not_prose():
    """The subtitle carries the version number. Front matter is metadata the
    author sets deliberately, not a claim about the data."""
    qmd = ('---\ntitle: "A paper"\n'
           'subtitle: "Version 6.2 — the consolidated study"\n---\n'
           'Real prose with no number.\n')
    assert crn.typed_numbers(qmd) == []


def test_front_matter_only_counts_at_the_top():
    """A `---` mid-document is a horizontal rule, not a front-matter fence;
    treating it as one would silence everything after it."""
    qmd = "Prose.\n\n---\n\nThe band gained 109 correct.\n"
    assert [f.value for f in crn.typed_numbers(qmd)] == ["109"]
