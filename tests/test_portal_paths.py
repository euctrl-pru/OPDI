"""No benchmark this study runs may resolve the portal as
``REPO.parent / "opdi-portal"``.

That expression is right when `opdi/` sits directly in the workspace root and
wrong inside a git worktree, which lives at `opdi/.claude/worktrees/<name>` --
three levels deeper, so it points at `.../worktrees/opdi-portal`.

The failure never looks like a path bug, which is why it has recurred:

  * `regenerate_v61.py` reported every output as "output missing" --
    indistinguishable from "never generated", and the opposite of what a
    fingerprint result would mean. A whole staleness review was invalid.
  * `sampler_comparison.py` reported "no comparable outputs found", which reads
    as a statement about the data.
  * `flight_list_v62.py` resolved `OPDI_live/logs` the same way, concluded the
    endpoint candidate table needed rebuilding, and tried to write over
    production. The write guard caught it 26 minutes into a run.

Three times, three disguises. Hence a test rather than a fourth fix.

Detection is by AST, not by text: both fixed scripts *describe* the bad pattern
in their `_find_portal` docstrings, and a grep cannot tell an explanation from
an occurrence.
"""

import ast
from pathlib import Path

import pytest

BENCH = Path(__file__).resolve().parent.parent / "benchmarks"

#: The scripts this study runs. `regenerate_v6.py`, `regenerate_v7.py`,
#: `regenerate_events.py` and `export_results.py` carry the same latent bug but
#: belong to frozen studies, and quietly editing a frozen paper's harness is
#: not this study's business. They are listed here so the omission is a
#: decision on the record rather than an oversight.
IN_SCOPE = [
    "regenerate_v62.py",
    "flight_list_v62.py",
    "sampler_comparison.py",
    "check_main_parity.py",
    "check_report_numbers.py",
    "input_drift.py",
    "sweep_equivalence.py",
]

KNOWN_AFFECTED_ELSEWHERE = [
    "regenerate_v6.py", "regenerate_v7.py",
    "regenerate_events.py", "export_results.py",
]


def _naive_portal_expr(tree) -> bool:
    """True if the AST contains `REPO.parent / "opdi-portal"`.

    Specifically `REPO`, not any `.parent`. The correct fix walks up with a
    loop variable -- `for base in (REPO, *REPO.parents): base.parent /
    "opdi-portal"` -- so a detector keyed on `.parent` alone flags the cure
    along with the disease, and would have to be silenced to let the fix land.
    """
    for node in ast.walk(tree):
        if not isinstance(node, ast.BinOp) or not isinstance(node.op, ast.Div):
            continue
        right = node.right
        if not (isinstance(right, ast.Constant) and right.value == "opdi-portal"):
            continue
        left = node.left
        if (isinstance(left, ast.Attribute) and left.attr == "parent"
                and isinstance(left.value, ast.Name) and left.value.id == "REPO"):
            return True
    return False


def test_the_in_scope_list_names_real_files():
    """A list of names that do not exist passes every assertion below."""
    missing = [n for n in IN_SCOPE if not (BENCH / n).is_file()]
    assert not missing, f"IN_SCOPE names files that do not exist: {missing}"


@pytest.mark.parametrize("name", IN_SCOPE)
def test_no_script_resolves_the_portal_naively(name):
    tree = ast.parse((BENCH / name).read_text())
    assert not _naive_portal_expr(tree), (
        f"{name} resolves the portal as REPO.parent / 'opdi-portal', which is "
        f"wrong inside a worktree. Walk up until a sibling opdi-portal "
        f"appears -- see _find_portal() in regenerate_v62.py.")


def test_the_detector_actually_detects():
    """A guard that cannot fire is worse than no guard."""
    bad = ast.parse('REPO = 1\nDATA = REPO.parent / "opdi-portal" / "papers"\n')
    assert _naive_portal_expr(bad)


@pytest.mark.parametrize("src", [
    'DATA = _find_portal() / "papers"',
    # The fix itself: a search loop over a variable, which an over-broad
    # detector keyed on `.parent` would flag as the very bug it is fixing.
    'for base in (REPO, *REPO.parents):\n    c = base.parent / "opdi-portal"',
    # A docstring describing the bad pattern is not an occurrence of it.
    '"""Never write REPO.parent / \\"opdi-portal\\" here."""',
])
def test_the_detector_does_not_flag_the_cure(src):
    assert not _naive_portal_expr(ast.parse(src))


def test_the_finders_agree_on_where_the_portal_is():
    """Several scripts carry their own `_find_portal`. They must not drift into
    disagreeing about the answer."""
    import sys
    sys.path.insert(0, str(BENCH))
    found = {}
    for name in ("regenerate_v62", "sampler_comparison"):
        mod = __import__(name)
        if hasattr(mod, "_find_portal"):
            found[name] = mod._find_portal().resolve()
    assert found, "no _find_portal to compare"
    assert len(set(found.values())) == 1, f"finders disagree: {found}"
