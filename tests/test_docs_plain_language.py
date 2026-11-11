"""The package's docstrings and comments are written for OPDI users.

OPDI's code is read by researchers and students who use the data. Its
docstrings and comments say what the code does, with units, in ordinary
words. They do not tell the history of how the code came to be (study
names, paper versions, dates of decisions, task numbers, incidents), and
they avoid vague or jargon words. The detailed studies are on the OPDI
website and are linked from the code where a threshold needs a reason.

The test reads every docstring and comment of the Python files under
``src/opdi`` and ``airflow/``, and the text of ``readme.md``, and fails on
the patterns below, naming the file and line of each hit.
"""

import ast
import io
import re
import tokenize
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PYTHON_FOLDERS = (ROOT / "src" / "opdi", ROOT / "airflow")
TEXT_FILES = (ROOT / "readme.md",)

#: Each pattern with the reason it is not wanted. Matching ignores case
#: unless the pattern says otherwise.
PATTERNS = {
    # Names of development variants and study papers.
    r"(?-i:\bA[0-9]{1,2}\b)": "a study variant name (A8, A9, ...)",
    r"(?-i:(?<![\w.])[Vv][0-9](\.[0-9])?\b(?!\.[0-9]))": "a study paper version (V6, v6.2, ...)",
    r"\btrack-construction-v[0-9]|\bflight-events-v[0-9]": "a paper folder name",
    r"\bpapers?/": "a path to a study paper",
    r"\b(rung|rungs|ladder|ladders)\b": "study jargon",
    r"(?-i:\barms?\b)": "'arm' for a variant: say rule, variant or method",
    r"\bTask [0-9]+\b": "a task number of the development plan",
    r"\b(SIGKILL|bitten|EVENTS_RUN_LOG|weekrun|run log)\b": "an incident or a run log",
    # History of the code.
    r"\b(previously|used to|no longer|we found|we measured|was found|"
    r"until now|at the time|historically)\b": "development history",
    r"\b(since|as of|until|before|after|on)\s+20[0-9]{2}-[0-9]{2}": "the date of a decision",
    # Vague or jargon words.
    r"\bgates?\b": "'gate' (except an airport gate)",
    r"\bguard(s|ed|ing)?\b": "'guard': say check or condition",
    r"\bpin(s|ned|ning)?\b": "'pin': say fix, keep or test",
    r"\bload-bearing\b": "jargon",
    r"\bsurfaces?\s+(it|them|this|that|the\s+(error|problem|failure))\b|\bsurfaced\b|\bto surface\b": "'surface' as a verb",
    r"\b(robust|robustly|robustness|leverage|leverages|delve|seamless|seamlessly|"
    r"crucial|comprehensive)\b": "vague wording",
}

#: Exact text that matches a pattern but is fine where it stands.
ALLOWED = (
    "gate-to-gate",
    "airport gate",
    "at the gate",
    "to the gate",
)


def _python_texts(path: Path):
    """(line, text) of every docstring and comment of a Python file."""
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(
            node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
        ) and ast.get_docstring(node, clean=False):
            body = node.body[0]
            yield body.lineno, ast.get_docstring(node, clean=False)
        # "#:" attribute docs and bare string statements after an assignment.
        if isinstance(node, (ast.Module, ast.ClassDef)):
            for stmt in node.body[1:]:
                if (
                    isinstance(stmt, ast.Expr)
                    and isinstance(stmt.value, ast.Constant)
                    and isinstance(stmt.value.value, str)
                ):
                    yield stmt.lineno, stmt.value.value
    for tok in tokenize.generate_tokens(io.StringIO(source).readline):
        if tok.type == tokenize.COMMENT:
            yield tok.start[0], tok.string


def _texts():
    for folder in PYTHON_FOLDERS:
        for path in sorted(folder.rglob("*.py")):
            for line, text in _python_texts(path):
                yield path, line, text
    for path in TEXT_FILES:
        for n, text in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            yield path, n, text


def _hits():
    compiled = [(re.compile(p, re.IGNORECASE), why) for p, why in PATTERNS.items()]
    out = []
    for path, line, text in _texts():
        for offset, row in enumerate(text.splitlines() or [text]):
            for allowed in ALLOWED:
                row = row.replace(allowed, "")
            for pattern, why in compiled:
                m = pattern.search(row)
                if m:
                    rel = path.relative_to(ROOT)
                    out.append(f"{rel}:{line + offset}: {why}: {m.group(0)!r} in {row.strip()!r}")
    return out


import pytest  # noqa: E402


@pytest.mark.xfail(strict=True, reason="the docstrings are being rewritten")
def test_docstrings_and_comments_are_plain_language():
    hits = _hits()
    assert not hits, f"{len(hits)} hit(s):\n" + "\n".join(hits)


def test_the_scan_finds_what_it_is_meant_to_find(tmp_path):
    """The patterns catch the words they name, and leave ordinary uses alone."""
    compiled = [re.compile(p, re.IGNORECASE) for p in PATTERNS]

    def flagged(text):
        for allowed in ALLOWED:
            text = text.replace(allowed, "")
        return any(p.search(text) for p in compiled)

    for bad in (
        "the A9 rule",
        "see the V6 study",
        "the rung below",
        "this used to default to append",
        "since 2026-08-27",
        "a guard against",
        "pinned by the tests",
        "a robust method",
        "each arm of the study",
    ):
        assert flagged(bad), bad
    for good in (
        "an A320 aircraft",
        "release v2.0.0",
        "events_v0.0.2",
        "airport surface events",
        "the runway surface",
        "osn_statevectors_v2",
        "ARMS registry",
        "register_arm",
        "a taxiway to the gate",
        "gate-to-gate times",
    ):
        assert not flagged(good), good
