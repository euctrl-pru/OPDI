# V6.2 reader improvements and main-parity checks — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans
> to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make `adep-ades-detection-v6.2` stand alone for a reader who has never
seen the project, and make its numbers verifiably the product of `opdi` as it
stands on `main`.

**Architecture:** Two new checkers in `benchmarks/` become pytest tests — one
proving every number in the prose is computed or declared, one proving main's
`DetectionConfig()` equals the pipeline grid's best cell. The grid grows to
contain main's own configuration, the campaign is re-run from main, and the
paper gains an on-ramp, a usage example and an appendix.

**Tech Stack:** Python 3.10, pytest, PySpark 4.1.1 on Kubernetes, Quarto +
knitr (R), PyYAML.

**Spec:** `docs/superpowers/specs/2026-08-25-v62-reader-and-checks-design.md`

**Worktree:** `/home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/v62-reader`,
branch `v62-reader`, based on `main` at `d4bf5eb`.

## Global Constraints

- **Main is the authority.** Numbers come from `opdi` on `main`; checks run
  against that same package. Not a worktree, not a branch.
- **The pipeline grid decides the recommendation**, not the harness sweep. Only
  the grid calls `process_dai`.
- **This plan does NOT authorise changing `config.py`.** If parity fails, that
  is a reported result and a decision for the maintainers.
- **Both formats.** Anything HTML-only or PDF-only is a defect unless justified.
- **Commit messages carry no self-reference** — no `Co-Authored-By`, no
  "generated with".
- **`opdi-portal` is committed but NOT pushed.** Only `opdi/` is pushed.
- Tests: `.venv310/bin/python -m pytest tests/` from the repo root. 293 tests on
  `main` at `d4bf5eb`; all must stay green.
- The cluster is **shared**. Never launch while another `benchmarks/` driver
  runs, and gate on the namespace quota, not on `ps` — see `V62_RUN_NOTES.md`.
- A worktree needs `.env` symlinked from the main checkout before any benchmark
  runs; `.gitignore` covers it.

## Known state at plan time

Measured, not assumed — re-verify before relying on any of it.

| Fact | Value |
|---|---|
| main's `DetectionConfig()` | ceiling 6,000 ft, margin 0, radius 30 NM, penalty 10 NM, datum `field`, rank `haversine` |
| Pipeline grid best (2025, margin fixed 2) | 6,000 ft / 30 NM |
| Harness sweep best (joint, both periods) | 6,100 ft / margin 2 / 20 NM |
| main's cell on the harness grid | **absent** — `HEIGHT_CAPS` has no 6,000 |
| main's margin validated by | **nothing** — the grid fixes margin at 2 |
| Typed numerals in prose | 75 occurrences, 47 distinct |
| Prose lines referencing earlier versions | 27 |

---

### Task 1: Spike — can R render a diagram to both formats without a browser?

Throwaway. The output is an answer that decides Task 6's diagram approach.

**Files:**
- Create (throwaway): `/home/jupyter/.claude/jobs/238a706b/tmp/spike_diagram.R`

**Interfaces:**
- Consumes: nothing.
- Produces: an answer recorded in the plan's notes. If a path works, the exact
  R incantation Task 6 will reuse.

- [ ] **Step 1: Probe the DiagrammeR → SVG → PDF path**

```r
# /home/jupyter/.claude/jobs/238a706b/tmp/spike_diagram.R
# Can a DOT diagram reach PDF without a browser?
# DiagrammeRsvg is absent, so try to get SVG out of DiagrammeR directly.
dot <- 'digraph { rankdir=TB; A [label="S1 track"]; B [label="S6 altitude cut"]; A -> B }'
g <- DiagrammeR::grViz(dot)

ok <- FALSE
# DiagrammeR renders through viz.js in a V8 context; the htmlwidget carries the
# DOT source, and V8 can run the same library directly.
try({
  ctx <- V8::v8()
  viz <- system.file("htmlwidgets/lib/viz/viz.js", package = "DiagrammeR")
  cat("viz.js present:", file.exists(viz), "\n")
  if (file.exists(viz)) {
    ctx$source(viz)
    svg <- ctx$call("Viz", dot)
    writeLines(svg, "/tmp/spike.svg")
    rsvg::rsvg_pdf("/tmp/spike.svg", "/tmp/spike.pdf")
    ok <- file.exists("/tmp/spike.pdf") && file.size("/tmp/spike.pdf") > 1000
  }
}, silent = FALSE)
cat("PATH 1 (DiagrammeR/V8/rsvg):", ok, "\n")
```

Run: `R --vanilla -f /home/jupyter/.claude/jobs/238a706b/tmp/spike_diagram.R`

- [ ] **Step 2: If path 1 fails, probe igraph to a PDF device**

```r
# Renders to any graphics device, so both formats come free.
library(igraph)
g <- graph_from_literal(S1 -+ S2, S2 -+ S3, S3 -+ S6)
pdf("/tmp/spike_igraph.pdf", width = 6, height = 4)
plot(g, layout = layout_as_tree(g), vertex.shape = "rectangle",
     vertex.size = 40, vertex.size2 = 15)
dev.off()
cat("PATH 2 (igraph):", file.exists("/tmp/spike_igraph.pdf"), "\n")
```

- [ ] **Step 3: Confirm no Chromium was spawned**

Run: `ps -eo etime,stat,cmd | grep -i chrom | grep -v grep`
Expected: only the pre-existing defunct entries, aged 2 and 11 days. Any
process younger than this session means a probe reached the rasteriser and the
path must be rejected regardless of whether it produced output.

- [ ] **Step 4: Record the answer**

Append to `benchmarks/V62_RUN_NOTES.md` under a new `## Diagram rendering`
heading: which path worked, the exact call, or that none did and Task 6 falls
back to completing the PDF tables. Commit.

```bash
git add benchmarks/V62_RUN_NOTES.md
git commit -m "Record whether diagrams can reach PDF without a browser"
```

---

### Task 2: Number-provenance checker

**Files:**
- Create: `benchmarks/check_report_numbers.py`
- Create: `../opdi-portal/papers/adep-ades-detection-v6.2/constants.yml`
- Test: `tests/test_check_report_numbers.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `prose_lines(qmd_text: str) -> list[tuple[int, str]]` — line number and text
    for every line outside a fenced R chunk.
  - `typed_numbers(qmd_text: str) -> list[Finding]` where
    `Finding = namedtuple("Finding", "line value text")`.
  - `load_allowlist(path: Path) -> dict[str, str]` — value → why.
  - `check(qmd: Path, allowlist: Path) -> list[Finding]` — the undeclared ones.

- [ ] **Step 1: Write the failing test**

```python
"""Guards on the number-provenance checker.

The failure it prevents: a result typed into prose, which goes stale when the
data moves and says nothing when it does. Three of that class were found in
v6.2 by tripping over them; this finds them by looking.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks"))

import check_report_numbers as crn


def test_a_typed_result_is_caught():
    qmd = "The band gained **+109** correct arrivals.\n"
    found = crn.typed_numbers(qmd)
    assert [f.value for f in found] == ["109"]


def test_a_computed_number_is_not_caught():
    qmd = "The band gained **`r num(delta)`** correct arrivals.\n"
    assert crn.typed_numbers(qmd) == []


def test_numbers_inside_r_chunks_are_ignored():
    qmd = '```{r}\nx <- 12345\n```\nProse with no number.\n'
    assert crn.typed_numbers(qmd) == []


def test_small_integers_and_years_are_exempt():
    qmd = "Two of the three, measured in 2025, across 12 cells.\n"
    assert crn.typed_numbers(qmd) == []


def test_a_unit_bearing_number_is_never_exempt():
    """0.12% is small but it is a result. The exemption is for bare counts."""
    for text in ("gained 0.12%", "fell 0.15 pp", "at 6,100 ft", "within 20 NM"):
        assert crn.typed_numbers(text + "\n"), text


def test_an_allowlisted_value_passes(tmp_path):
    allow = tmp_path / "constants.yml"
    allow.write_text(
        '- value: "6,100"\n'
        '  why: "A definition: FL60 admits everything below 6,100 ft."\n')
    qmd = tmp_path / "p.qmd"
    qmd.write_text("The cut reaches 6,100 ft.\n")
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
```

Add `import pytest` at the top.

- [ ] **Step 2: Run to verify it fails**

Run: `.venv310/bin/python -m pytest tests/test_check_report_numbers.py -q`
Expected: FAIL, `ModuleNotFoundError: No module named 'check_report_numbers'`

- [ ] **Step 3: Implement the checker**

```python
"""Every number in the report's prose must be computed or declared.

A number a chunk prints came from a staged CSV and moves when the data moves.
A number typed into prose does not: it goes stale silently, and the reader
cannot tell the two apart. v6.2 shipped three such -- an accuracy delta of
0.16 pp against a real 0.15, a "seventy per cent" against a real 70.0%, and a
radius v6 claimed shipped and which never did.

This finds them by looking rather than by tripping over them.

    python benchmarks/check_report_numbers.py <paper>/index.qmd \\
        --allowlist <paper>/constants.yml
"""

import argparse
import re
import sys
from collections import namedtuple
from pathlib import Path

import yaml

Finding = namedtuple("Finding", "line value text")

#: A fenced R chunk. Whatever it prints is computed by construction.
CHUNK = re.compile(r"^```+\s*\{r[^}]*\}\s*$")
FENCE = re.compile(r"^```+\s*$")

#: Inline R. `r foo()` is the computed form, so a line's inline spans are cut
#: out before the scan rather than the whole line being skipped -- a sentence
#: can carry one computed number and one typed one.
INLINE_R = re.compile(r"`r [^`]*`")

#: A numeral, with any unit or sign glued to it.
NUM = re.compile(r"(?<![\w.])([+-]?\d[\d,]*(?:\.\d+)?)\s*(%|pp|ft|NM|km)?")

#: Identifiers that merely contain digits.
IDENT = re.compile(r"(?:@sec-[\w.-]+|FL\d+|S\d+|E\d+|path\d+|step \d+|"
                   r"res(?:olution)? \d+|v\d+(?:\.\d+)?|version \d+(?:\.\d+)?|"
                   r"H3|`[^`]*`)")


def prose_lines(text: str):
    """Line number and text for every line outside a fenced R chunk."""
    out, in_chunk = [], False
    for i, line in enumerate(text.splitlines(), 1):
        if not in_chunk and CHUNK.match(line):
            in_chunk = True
            continue
        if in_chunk and FENCE.match(line):
            in_chunk = False
            continue
        if not in_chunk:
            out.append((i, line))
    return out


def _exempt(value: str, unit) -> bool:
    """Bare counts and years need no declaration; anything with a unit does."""
    if unit:
        return False
    if "," in value or "." in value:
        return False          # a thousands separator or a decimal is a result
    try:
        n = int(value)
    except ValueError:
        return False
    return 0 <= n <= 12 or 2000 <= n <= 2099


def typed_numbers(text: str):
    """Numerals the author typed into prose."""
    found = []
    for i, line in enumerate(prose_lines(text), 0):
        pass
    for lineno, line in prose_lines(text):
        s = line.strip()
        if s.startswith("#") or s.startswith("|") or s.startswith(":::"):
            continue
        s = INLINE_R.sub(" ", s)     # computed spans are not typed
        s = IDENT.sub(" ", s)        # identifiers are not measurements
        for m in NUM.finditer(s):
            value, unit = m.group(1), m.group(2)
            if _exempt(value, unit):
                continue
            found.append(Finding(lineno, value.lstrip("+"), line.strip()[:100]))
    return found


def load_allowlist(path: Path) -> dict:
    """value -> why. A `why` is mandatory: see the module docstring."""
    if not path.is_file():
        return {}
    entries = yaml.safe_load(path.read_text()) or []
    out = {}
    for e in entries:
        if "value" not in e:
            raise ValueError(f"allowlist entry without a value: {e!r}")
        if not str(e.get("why", "")).strip():
            raise ValueError(
                f"allowlist entry {e['value']!r} has no `why`. A bare list of "
                f"permitted numbers becomes a place to silence this check; a "
                f"field that must say why the number is not a result makes "
                f"silencing it visible in review.")
        out[str(e["value"]).lstrip("+")] = e["why"]
    return out


def check(qmd: Path, allowlist: Path):
    allowed = load_allowlist(Path(allowlist))
    return [f for f in typed_numbers(Path(qmd).read_text())
            if f.value not in allowed]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("qmd", type=Path)
    ap.add_argument("--allowlist", type=Path, required=True)
    args = ap.parse_args()

    bad = check(args.qmd, args.allowlist)
    for f in bad:
        print(f"  line {f.line:5d}  {f.value:>10s}  {f.text}")
    print(f"\n{len(bad)} undeclared number(s) in prose")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
```

Delete the stray `for i, line in enumerate(prose_lines(text), 0): pass` loop —
it is a leftover and does nothing.

- [ ] **Step 4: Run to verify it passes**

Run: `.venv310/bin/python -m pytest tests/test_check_report_numbers.py -q`
Expected: PASS, 8 tests.

- [ ] **Step 5: Run it against the real paper and read the output**

```bash
.venv310/bin/python benchmarks/check_report_numbers.py \
  ../opdi-portal/papers/adep-ades-detection-v6.2/index.qmd \
  --allowlist ../opdi-portal/papers/adep-ades-detection-v6.2/constants.yml
```

Expect a long list on first run — the allowlist does not exist yet, so every
typed number is undeclared. **Read every line.** Each is either a constant that
belongs in the allowlist, or a result that Task 6 must make computed. Sort them
into those two piles before writing anything.

- [ ] **Step 6: Write the allowlist for the genuine constants**

Create `constants.yml`. One entry per constant, each with a `why` that says why
it is not a measurement. Start from these, which are known constants:

```yaml
# Numbers that appear in this paper's prose and are NOT results.
#
# Every entry must say why. A bare list of permitted numbers becomes a place to
# silence the checker; a `why` that must be written makes silencing it visible.

- value: "6,100"
  why: >
    A definition. `flight_level` is an integer cast, so `flight_level <= 60`
    admits everything below 6,100 ft. It follows from the cut, not the data.

- value: "6,000"
  why: >
    The shipped ceiling, `trend_max_height_ft`. A configured value, checked
    against the code by check_main_parity.py rather than measured here.

- value: "5.2"
  why: >
    The H3 resolution-7 edge length in km. A property of the indexing scheme.

- value: "15,000"
  why: >
    `endpoint_height_ft`, a configured value.

- value: "110"
  why: >
    The candidate-builder search radius in NM. A configured value.
```

Anything else the run in Step 5 flagged is a **result** and belongs in Task 6,
not here. If unsure which pile a number belongs in, the test is: would it change
if the campaign were re-run on different data? If yes, it is a result.

- [ ] **Step 7: Wire it into the test suite**

Append to `tests/test_check_report_numbers.py`:

```python
PAPER = (Path(__file__).resolve().parent.parent.parent
         / "opdi-portal" / "papers" / "adep-ades-detection-v6.2")


def test_the_real_paper_has_no_undeclared_numbers():
    """The check that matters. Every number in v6.2's prose is computed or
    declared, so none can go stale without this failing."""
    if not (PAPER / "index.qmd").is_file():
        pytest.skip(f"paper not found at {PAPER}")
    bad = crn.check(PAPER / "index.qmd", PAPER / "constants.yml")
    assert not bad, "undeclared numbers:\n" + "\n".join(
        f"  line {f.line}: {f.value} -- {f.text}" for f in bad)
```

This test **will fail** until Task 6 converts the results to computed values.
That is intended: it is the work list.

- [ ] **Step 8: Commit**

```bash
git add benchmarks/check_report_numbers.py tests/test_check_report_numbers.py
git commit -m "Add the number-provenance checker

Every numeral in the report's prose must be computed by an inline r
expression or declared in an allowlist that says why it is not a result.

The allowlist's why field is load-bearing. A bare list of permitted numbers
becomes a place to silence the check; a field that must justify each entry
makes silencing it visible in review."
```

Commit `constants.yml` separately in the portal repo:

```bash
cd ../opdi-portal
git add papers/adep-ades-detection-v6.2/constants.yml
git commit -m "Declare v6.2's prose constants, with why each is not a result"
```

---

### Task 3: Implementation-parity checker

**Files:**
- Create: `benchmarks/check_main_parity.py`
- Test: `tests/test_check_main_parity.py`

**Interfaces:**
- Consumes: `data/trend_grid_v6.csv` (columns `run`, `trend_ceiling`,
  `trend_radius_nm`, `trend_vote_margin`, `trend_sched_penalty_nm`,
  `ades_score`), `data/_manifest.json` (fields `git_sha`, `git_dirty`,
  `code_paths`, `script`).
- Produces:
  - `best_cell(grid_csv: Path) -> dict` — the highest `ades_score` row.
  - `config_parity(grid_csv: Path) -> tuple[list[str], dict]` — differing
    parameter names, and a rank/gap report.
  - `code_on_main(manifest: Path, ref: str) -> list[str]` — paths missing.
  - `shas_on_main(manifest: Path, ref: str) -> list[str]` — figures whose SHA is
    not an ancestor, or which were produced dirty.

- [ ] **Step 1: Write the failing test**

```python
"""Guards on the main-parity checker.

Two failures it prevents. A report describing a configuration the code does not
have -- v6 claimed trend_radius_nm ships at 20 NM for three versions while the
code had 30. And its mirror: a figure produced by code that never got merged.
"""
import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks"))

import pytest

import check_main_parity as cmp_


def _grid(tmp_path, rows):
    p = tmp_path / "grid.csv"
    with open(p, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    return p


BASE = {"run": "grid_h6000_r30_m0", "trend_ceiling": "6000",
        "trend_radius_nm": "30", "trend_vote_margin": "0",
        "trend_sched_penalty_nm": "10", "ades_score": "100"}


def test_best_cell_is_the_highest_scoring_row(tmp_path):
    hi = dict(BASE, run="grid_h6100_r20_m2", trend_ceiling="6100",
              trend_radius_nm="20", trend_vote_margin="2", ades_score="200")
    p = _grid(tmp_path, [BASE, hi])
    assert cmp_.best_cell(p)["run"] == "grid_h6100_r20_m2"


def test_a_grid_without_mains_cell_is_a_distinct_failure(tmp_path):
    """A parity check against a grid that does not contain the configuration
    under test is not a check. It must say so, not silently report a mismatch."""
    only = dict(BASE, trend_ceiling="9999")
    p = _grid(tmp_path, [only])
    with pytest.raises(cmp_.ConfigurationNotOnGrid):
        cmp_.config_parity(p, {"trend_max_height_ft": 6000.0,
                               "trend_radius_nm": 30.0,
                               "trend_vote_margin": 0})


def test_matching_config_reports_no_differences(tmp_path):
    p = _grid(tmp_path, [BASE])
    diffs, report = cmp_.config_parity(p, {"trend_max_height_ft": 6000.0,
                                           "trend_radius_nm": 30.0,
                                           "trend_vote_margin": 0})
    assert diffs == []
    assert report["rank"] == 1


def test_differing_config_names_each_parameter(tmp_path):
    hi = dict(BASE, run="grid_h6100_r20_m2", trend_ceiling="6100",
              trend_radius_nm="20", trend_vote_margin="2", ades_score="200")
    p = _grid(tmp_path, [BASE, hi])
    diffs, report = cmp_.config_parity(p, {"trend_max_height_ft": 6000.0,
                                           "trend_radius_nm": 30.0,
                                           "trend_vote_margin": 0})
    assert set(diffs) == {"trend_max_height_ft", "trend_radius_nm",
                          "trend_vote_margin"}
    assert report["rank"] == 2          # main's cell is still located


def test_a_dirty_figure_is_reported(tmp_path):
    m = tmp_path / "_manifest.json"
    m.write_text(json.dumps({
        "a.csv": {"git_sha": "deadbee", "git_dirty": True,
                  "script": "benchmarks/x.py", "code_paths": []}}))
    bad = cmp_.shas_on_main(m, ref="HEAD")
    assert any("dirty" in b for b in bad)
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv310/bin/python -m pytest tests/test_check_main_parity.py -q`
Expected: FAIL, `ModuleNotFoundError: No module named 'check_main_parity'`

- [ ] **Step 3: Implement**

```python
"""Is the report describing the code that is actually on main?

Two failures this catches, and v6 shipped the first for three versions: a
report asserting `trend_radius_nm` ships at 20 NM while the code had 30,
because each version copied the claim rather than measuring it. The mirror is
a figure produced by code that never got merged.

The recommendation is not written down anywhere. It is the best cell of the
**pipeline grid** -- the job that calls `process_dai`, and so the only sweep
that runs main's package. The harness sweep is a reimplementation and is not
the authority, even though it covers more cells and both periods.

    python benchmarks/check_main_parity.py <paper>/data
"""

import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path

#: Grid column -> DetectionConfig attribute.
PARAMS = {
    "trend_ceiling": "trend_max_height_ft",
    "trend_radius_nm": "trend_radius_nm",
    "trend_vote_margin": "trend_vote_margin",
}


class ConfigurationNotOnGrid(Exception):
    """main's configuration is absent from the grid, so parity is not
    expressible. Distinct from a mismatch, and a failure in its own right."""


def _rows(grid_csv: Path):
    return [r for r in csv.DictReader(open(grid_csv))
            if r["run"].startswith("grid_")]


def best_cell(grid_csv: Path) -> dict:
    return max(_rows(grid_csv), key=lambda r: float(r["ades_score"]))


def config_parity(grid_csv: Path, cfg: dict):
    """Differing parameters, plus where main's own cell ranks.

    `cfg` maps DetectionConfig attribute names to values, so the caller decides
    which package to import from -- which is how the test pins that it is
    main's.
    """
    rows = sorted(_rows(grid_csv), key=lambda r: -float(r["ades_score"]))
    best = rows[0]

    def matches(row):
        return all(float(row[col]) == float(cfg[attr])
                   for col, attr in PARAMS.items())

    rank = next((i + 1 for i, r in enumerate(rows) if matches(r)), None)
    if rank is None:
        raise ConfigurationNotOnGrid(
            f"main's configuration "
            + ", ".join(f"{a}={cfg[a]}" for a in PARAMS.values())
            + f" is not among the {len(rows)} cells of {grid_csv.name}. "
              f"A parity check against a grid that does not contain the "
              f"configuration under test is not a check -- widen the grid.")

    diffs = [attr for col, attr in PARAMS.items()
             if float(best[col]) != float(cfg[attr])]
    report = {
        "rank": rank,
        "of": len(rows),
        "gap": float(best["ades_score"]) - float(rows[rank - 1]["ades_score"]),
        "best": {col: best[col] for col in PARAMS},
    }
    return diffs, report


def _git(*args, ref_repo: Path):
    return subprocess.run(["git", "-C", str(ref_repo), *args],
                          capture_output=True, text=True)


def code_on_main(manifest: Path, ref: str, repo: Path) -> list:
    """Paths named in the manifest that do not exist at `ref`."""
    m = json.loads(Path(manifest).read_text())
    paths = set()
    for entry in m.values():
        if not isinstance(entry, dict):
            continue
        if entry.get("script"):
            paths.add(entry["script"])
        paths.update(entry.get("code_paths") or [])
    missing = []
    for p in sorted(paths):
        if _git("cat-file", "-e", f"{ref}:{p}", ref_repo=repo).returncode != 0:
            missing.append(p)
    return missing


def shas_on_main(manifest: Path, ref: str, repo: Path = Path(".")) -> list:
    """Figures produced dirty, or by a commit that is not an ancestor of ref."""
    m = json.loads(Path(manifest).read_text())
    bad = []
    for name, entry in sorted(m.items()):
        if not isinstance(entry, dict) or "git_sha" not in entry:
            continue
        if entry.get("git_dirty"):
            bad.append(f"{name}: produced from a dirty tree")
            continue
        sha = entry["git_sha"]
        r = _git("merge-base", "--is-ancestor", sha, ref, ref_repo=repo)
        if r.returncode != 0:
            bad.append(f"{name}: {sha} is not an ancestor of {ref}")
    return bad


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("data", type=Path, help="the paper's data/ directory")
    ap.add_argument("--ref", default="origin/main")
    ap.add_argument("--repo", type=Path,
                    default=Path(__file__).resolve().parent.parent)
    args = ap.parse_args()

    sys.path.insert(0, str(args.repo / "src"))
    from opdi.config import DetectionConfig
    import opdi
    print(f"opdi imported from {opdi.__file__}")

    c = DetectionConfig()
    cfg = {"trend_max_height_ft": c.trend_max_height_ft,
           "trend_radius_nm": c.trend_radius_nm,
           "trend_vote_margin": c.trend_vote_margin}

    diffs, report = config_parity(args.data / "trend_grid_v6.csv", cfg)
    print(f"main's cell ranks {report['rank']} of {report['of']}, "
          f"{report['gap']:,.0f} behind the best cell {report['best']}")
    for d in diffs:
        print(f"  DIFFERS  {d}: main={cfg[d]}")

    missing = code_on_main(args.data / "_manifest.json", args.ref, args.repo)
    for p in missing:
        print(f"  MISSING on {args.ref}: {p}")

    stale = shas_on_main(args.data / "_manifest.json", args.ref, args.repo)
    for s in stale:
        print(f"  {s}")

    sys.exit(1 if (diffs or missing or stale) else 0)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run to verify it passes**

Run: `.venv310/bin/python -m pytest tests/test_check_main_parity.py -q`
Expected: PASS, 5 tests.

- [ ] **Step 5: Add the test that pins which package is imported**

Append to `tests/test_check_main_parity.py`:

```python
MAIN = Path("/home/jupyter/work/opdi-workspace/opdi")


def test_tests_import_opdi_from_the_main_checkout():
    """The venv installs opdi as an editable install pointing at the main
    checkout's src, so a run from a worktree imports main's package -- which is
    the behaviour this study wants. Accidents do not stay true, so it is
    asserted: nobody can later 'fix' the import path and quietly start testing
    a branch."""
    import opdi
    resolved = Path(opdi.__file__).resolve()
    assert str(resolved).startswith(str(MAIN / "src")), (
        f"opdi imported from {resolved}, not from main's checkout")
```

- [ ] **Step 6: Run the whole suite**

Run: `.venv310/bin/python -m pytest tests/ -q`
Expected: green except `test_the_real_paper_has_no_undeclared_numbers` from
Task 2, which is the work list for Task 6.

- [ ] **Step 7: Commit**

```bash
git add benchmarks/check_main_parity.py tests/test_check_main_parity.py
git commit -m "Add the main-parity checker

Asserts main's DetectionConfig equals the pipeline grid's best cell, that
every path the manifest names exists on main, and that every figure's SHA is
an ancestor of main from a clean tree.

The recommendation is not written down: it is the best cell of the grid that
calls process_dai, the only sweep running main's package. A configuration
absent from the grid raises rather than reporting a mismatch, because a
parity check against a grid missing the thing under test is not a check."
```

---

### Task 4: Extend the pipeline grid to sweep the vote margin

Without this, Task 3's checker raises `ConfigurationNotOnGrid` against the real
data: the grid fixes margin at 2 and main ships 0.

**Files:**
- Modify: `benchmarks/flight_list_v62.py` — `GRID_MARGINS`, grid construction
- Modify: `benchmarks/regenerate_v62.py` — `trend_grid_2025` runs and args
- Test: `tests/test_flight_list_v62.py`, `tests/test_regenerate_v62.py`

**Interfaces:**
- Consumes: `flight_list_v62.GRID_HEIGHT_CAPS` (existing,
  `(3000, 4000, 6000, 6100, 8000, 10000)`).
- Produces: `flight_list_v62.GRID_MARGINS = (0, 2, 4)`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_flight_list_v62.py`:

```python
def test_the_grid_sweeps_the_margin_main_actually_ships():
    """The grid fixed the margin at 2 while DetectionConfig ships 0, so that
    parameter was validated by nothing. A parity check cannot locate a
    configuration the grid never ran."""
    from opdi.config import DetectionConfig
    assert DetectionConfig().trend_vote_margin in flight_list_v62.GRID_MARGINS
    assert set(flight_list_v62.GRID_MARGINS) == {0, 2, 4}
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv310/bin/python -m pytest tests/test_flight_list_v62.py -q`
Expected: FAIL, `AttributeError: ... has no attribute 'GRID_MARGINS'`

- [ ] **Step 3: Implement**

In `benchmarks/flight_list_v62.py`, beside `GRID_HEIGHT_CAPS`:

```python
#: Vote margins walked through `process_dai`.
#:
#: Includes 0 because that is what `DetectionConfig` ships. The grid previously
#: fixed the margin at 2, so the shipped value was validated by nothing at all
#: and a parity check could not even locate main's configuration on the grid.
GRID_MARGINS = (0, 2, 4)
```

Change the `--grid-margin` default:

```python
    ap.add_argument("--grid-margin", nargs="+", type=int,
                    default=list(GRID_MARGINS),
                    help="vote margins to walk through process_dai")
```

- [ ] **Step 4: Update the registry**

In `benchmarks/regenerate_v62.py`, `trend_grid_2025`:

```python
             "--runs", *[f"grid_h{c}_r{r:g}_m{m}"
                         for c in flight_list_v62.GRID_HEIGHT_CAPS
                         for r in (20, 30)
                         for m in flight_list_v62.GRID_MARGINS],
             "--grid-height", *[str(c) for c in flight_list_v62.GRID_HEIGHT_CAPS],
             "--grid-radius", "20", "30",
             "--grid-margin", *[str(m) for m in flight_list_v62.GRID_MARGINS],
```

- [ ] **Step 5: Update the grid-consistency test**

`tests/test_regenerate_v62.py::test_the_grid_runs_match_the_declared_ceilings`
currently checks ceilings only. Extend it:

```python
def test_the_grid_runs_match_the_declared_grid():
    """The run labels and the --grid-* lists are two spellings of the same
    grid. If they drift, the job asks for cells it never names."""
    jobs = {j.name: j for j in regenerate_v62.jobs()}
    args = _args(jobs["trend_grid_2025"])

    def values_after(flag):
        out = []
        for a in args[args.index(flag) + 1:]:
            if a.startswith("--"):
                break
            out.append(a)
        return out

    assert sorted(map(int, values_after("--grid-height"))) == \
        sorted(flight_list_v62.GRID_HEIGHT_CAPS)
    assert sorted(map(int, values_after("--grid-margin"))) == \
        sorted(flight_list_v62.GRID_MARGINS)
    expected = len(flight_list_v62.GRID_HEIGHT_CAPS) * 2 * \
        len(flight_list_v62.GRID_MARGINS)
    assert sum(1 for a in args if a.startswith("grid_h")) == expected
```

- [ ] **Step 6: Run the suite**

Run: `.venv310/bin/python -m pytest tests/ -q`
Expected: green except Task 2's paper test.

- [ ] **Step 7: Commit**

```bash
git add benchmarks/flight_list_v62.py benchmarks/regenerate_v62.py \
        tests/test_flight_list_v62.py tests/test_regenerate_v62.py
git commit -m "Sweep the vote margin the pipeline actually ships

The grid fixed the margin at 2 while DetectionConfig ships 0, so the shipped
value was validated by nothing and main's configuration was not a cell the
grid had ever run. 12 cells become 36."
```

---

### Task 5: Regenerate the campaign with main's package

**Files:** none created; this fills `papers/adep-ades-detection-v6.2/data/`.

**Preconditions:** Tasks 2–4 committed. `.env` symlinked into the worktree.

- [ ] **Step 1: Symlink credentials**

```bash
ln -sfn /home/jupyter/work/opdi-workspace/opdi/.env \
        /home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/v62-reader/.env
git -C /home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/v62-reader \
    check-ignore -v .env
```

Expected: `.gitignore:54 .env`. If it is not ignored, stop — do not commit a
credential.

- [ ] **Step 2: Confirm which package will be imported**

```bash
cd /home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/v62-reader
.venv310/bin/python -c "import opdi; print(opdi.__file__)" 2>/dev/null || \
/home/jupyter/work/opdi-workspace/opdi/.venv310/bin/python -c \
  "import opdi; print(opdi.__file__)"
```

Expected: a path under `/home/jupyter/work/opdi-workspace/opdi/src`. **Do not
set `PYTHONPATH` to the worktree's `src`** for this campaign — the numbers must
come from main's package, and the editable install already points there.

- [ ] **Step 3: Force a full re-run gated on the namespace quota**

Reuse `wait_quota_run_v62.sh` from the previous campaign, changing only the
working directory and adding `--force`:

```bash
#!/usr/bin/env bash
NEED_CPU=24
WT=/home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/v62-reader
PY=/home/jupyter/work/opdi-workspace/opdi/.venv310/bin/python
LOG=/home/jupyter/.claude/jobs/238a706b/tmp/v62_recampaign.log

free_cpu() {
  kubectl describe resourcequota -n eurocontrol 2>/dev/null \
    | awk '$1 == "limits.cpu" { print $3 - $2; found = 1 }
           END { if (!found) print 0 }'
}

: > "$LOG"; clear_count=0; launched=0
for _ in $(seq 1 1200); do
  f=$(free_cpu)
  if [ "${f:-0}" -ge "$NEED_CPU" ]; then
    clear_count=$((clear_count + 1))
    [ "$clear_count" -ge 4 ] && { launched=1; break; }
  else clear_count=0; fi
  sleep 30
done
[ "$launched" -eq 1 ] || { echo "never quiet" >> "$LOG"; exit 99; }

echo "launching ($(date -u +%H:%M:%S))" >> "$LOG"
cd "$WT" || exit 1
"$PY" -u benchmarks/regenerate_v62.py --force >> "$LOG" 2>&1
echo "campaign exit=$? ($(date -u +%H:%M:%S))" >> "$LOG"
```

`--force` is deliberate: the point is to reproduce every number from main, not
to skip what a fingerprint calls current.

**Budget five hours.** 19 jobs; the grid alone is 36 runs at roughly 4.2
minutes each.

- [ ] **Step 4: Compare every regenerated figure against its predecessor**

Before committing, diff each staged CSV against the version at `HEAD`:

```bash
cd /home/jupyter/work/opdi-workspace/opdi-portal
git diff --stat papers/adep-ades-detection-v6.2/data/
```

**Expected: only `trend_grid_v6.csv` changes**, because it gained 24 cells.
Anything else that moves is a finding — main's code differs in behaviour from
the branch that produced the original numbers. If that happens, stop and
investigate before writing any prose; do not paper over it.

- [ ] **Step 5: Run the parity checker against the fresh data**

```bash
cd /home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/v62-reader
.venv310/bin/python benchmarks/check_main_parity.py \
  ../../../opdi-portal/papers/adep-ades-detection-v6.2/data
```

Record the outcome verbatim in `V62_RUN_NOTES.md`, **whether it passes or
fails**. If main's margin 0 loses to margin 2, that is this campaign's headline
result and Task 6 reports it. **Do not change `config.py`.**

- [ ] **Step 6: Commit the data**

```bash
cd /home/jupyter/work/opdi-workspace/opdi-portal
git add papers/adep-ades-detection-v6.2/data
git commit -m "Regenerate v6.2's numbers with main's opdi package

Grid extended to 36 cells so main's own configuration is on it."
```

---

### Task 6: Editorial — the on-ramp, the appendix, the computed numbers

**Files:**
- Modify: `../opdi-portal/papers/adep-ades-detection-v6.2/index.qmd`

- [ ] **Step 1: Add the orientation section**

Immediately after `## Summary`, before `## Reading this page`, add
`## What this is about {#sec-orientation}` covering, in this order: what OPDI
is and what it publishes; what a flight list is; what ADEP and ADES mean; why
detecting them from ADS-B is hard; what the three methods are and why there are
three. Aim for 400–600 words. A reader must be able to start here and never
need another document.

- [ ] **Step 2: Add the glossary**

At the end of the orientation section, a definition list covering: ADEP, ADES,
track, H3 cell, coverage, accuracy, the exchange rate *k*, out-of-area,
abstention. One line each. Resist adding more — a glossary that explains
everything explains nothing.

- [ ] **Step 3: De-reference the prior-version mentions**

```bash
grep -nE "version [1-7]|V6|v6\.1|version 5" \
  ../opdi-portal/papers/adep-ades-detection-v6.2/index.qmd
```

For each of the 27, decide: does the sentence still make sense to someone who
has not read that version? If not, inline what it needs. Keep the citation as
attribution. Known offenders: *"Version 5 proposed asking whether the
trajectory points at the aerodrome"* and *"version 5 could not say which was
wrong"*.

- [ ] **Step 4: Convert the flagged results to computed values**

Re-run Task 2's checker and work through its list:

```bash
.venv310/bin/python benchmarks/check_report_numbers.py \
  ../opdi-portal/papers/adep-ades-detection-v6.2/index.qmd \
  --allowlist ../opdi-portal/papers/adep-ades-detection-v6.2/constants.yml
```

Each result becomes an inline `` `r ` `` reading the staged CSV. Example, for
the band figures at what is currently line 885:

```r
`r pct(arr$delta_correct[arr$band == "1500-3000"] /
       arr$n_correct_msl[arr$band == "1500-3000"])`
```

where `arr <- read_if("elevation_bands_2025.csv")` filtered to arrivals, added
to the setup chunk if not already there.

Repeat until the checker exits 0.

- [ ] **Step 5: Move the top-100 analysis to an appendix**

Cut `## The busiest 100 aerodromes {#sec-top100}` — the column definitions, the
`top100` chunk and the two 100-row tables — and paste it under a new top-level
`# Appendices` heading placed after `## Limitations`, as
`## Appendix A — The busiest 100 aerodromes {#sec-top100}`.

Use a plain heading, **not** Quarto's `.appendix` class: the class behaves
differently in HTML and PDF and this paper renders both.

In the body, where the section used to be, leave the finding as computed prose:

```r
`r sprintf("Across the hundred busiest aerodromes, the shipped configuration
recovers %s of arrivals against legacy's %s. Aerodrome-by-aerodrome detail is
in @sec-top100.", pct(top100_recall(SHIPPED, "arrivals")),
pct(top100_recall("legacy", "arrivals")))`
```

`top100_recall()` already exists in the setup chunk and is currently never
called. This gives it its purpose.

- [ ] **Step 6: Apply Task 1's diagram answer**

If the spike found a working path, convert the four `mermaid()` calls to it and
delete the `content-visible when-format="pdf"` fallbacks. If it did not,
complete the two prose fallbacks into full step tables so all four diagrams
have equivalent PDF content, and state the asymmetry in the orientation section.

- [ ] **Step 7: Report the parity result**

In `## What ships`, add the outcome of Task 5 Step 5 as computed prose: where
main's configuration ranks in the grid, and which parameters differ from the
best cell if any. If main's margin lost, say so plainly and say that this study
does not change it.

- [ ] **Step 8: Render both formats**

```bash
cd /home/jupyter/work/opdi-workspace/opdi-portal/papers
OPDI_RENDER=check timeout 2400 quarto render adep-ades-detection-v6.2
```

Expected: exit 0, no warnings, both `index.html` and
`adep-ades-detection-v6.2.pdf` produced.

- [ ] **Step 9: Verify the rendered output**

Run the checks from the previous campaign:

```bash
python3 /home/jupyter/.claude/jobs/238a706b/tmp/final_check.py
```

Expected: no "not yet measured", no "output missing", every CSV with a manifest
entry. Then read the PDF's table of contents and confirm the appendix is at the
end and the orientation section is at the front.

- [ ] **Step 10: Run the full suite**

Run: `.venv310/bin/python -m pytest tests/ -q`
Expected: **all green**, including
`test_the_real_paper_has_no_undeclared_numbers`.

- [ ] **Step 11: Commit**

```bash
cd /home/jupyter/work/opdi-workspace/opdi-portal
git add papers/adep-ades-detection-v6.2
git commit -m "Make v6.2 readable without having read v6

Adds an orientation section and glossary, inlines what the prior-version
references assumed, moves the hundred-aerodrome tables to an appendix
leaving the finding in the body as computed prose, and converts every typed
result to an inline expression so the checker passes."
```

---

### Task 7: Usage example

**Files:**
- Modify: `../opdi-portal/papers/adep-ades-detection-v6.2/index.qmd`

- [ ] **Step 1: Verify the API before documenting it**

```bash
cd /home/jupyter/work/opdi-workspace/opdi
grep -n "def run_pipeline" -A 16 src/opdi/runner.py
grep -n "opdi = " pyproject.toml
```

Confirm the signature still is
`run_pipeline(env, start_date, end_date, step, ..., adep_mode, ades_mode)` and
that the console script is `opdi = "opdi.cli:main"`. An example that does not
run is worse than none.

- [ ] **Step 2: Add the section**

After `## What ships`, add `## Running it {#sec-running}`:

````markdown
The configuration this report recommends is the **default**. `adep_mode` and
`ades_mode` default to `None`, which means `DetectionConfig()`'s own values —
so you get the recommendation by not overriding anything.

```python
from datetime import date
from opdi.runner import run_pipeline

# Step 03 is the flight list: one row per track, with ADEP and ADES.
run_pipeline(
    env="opensky",              # plain parquet over S3A; no Hive, no Iceberg
    step="03",
    start_date=date(2025, 6, 1),
    end_date=date(2025, 6, 30),
)
```

The same thing from the shell:

```bash
opdi run --env opensky --step 03 --start 2025-06-01 --end 2025-06-30
```

To depart from the recommendation, pass the modes explicitly — `"trend"`,
`"endpoint"` or `"nearest"`:

```python
run_pipeline(env="opensky", step="03",
             start_date=date(2025, 6, 1), end_date=date(2025, 6, 30),
             adep_mode="trend", ades_mode="trend")
```

Step 03 reads the tracks written by step 02 and the aerodrome zones from step
00, so those must have run for the same period first.
````

- [ ] **Step 3: Render and confirm the code block appears in both formats**

```bash
cd /home/jupyter/work/opdi-workspace/opdi-portal/papers
OPDI_RENDER=check timeout 2400 quarto render adep-ades-detection-v6.2
pdftotext adep-ades-detection-v6.2/adep-ades-detection-v6.2.pdf - | \
  grep -c "run_pipeline"
```

Expected: at least 2. `execute: echo: false` in `papers/_quarto.yml` suppresses
*executed* chunk source; a fenced ```` ```python ```` block is literal content
and is unaffected — but verify rather than assume.

- [ ] **Step 4: Commit**

```bash
cd /home/jupyter/work/opdi-workspace/opdi-portal
git add papers/adep-ades-detection-v6.2
git commit -m "Show how to run the recommended pipeline

The recommendation is the default: adep_mode and ades_mode default to None,
meaning DetectionConfig()'s values. You get it by not overriding anything."
```

---

### Task 8: Finish

- [ ] **Step 1: Full suite**

Run: `.venv310/bin/python -m pytest tests/ -q`
Expected: all green.

- [ ] **Step 2: Confirm v6 and v7 untouched**

```bash
cd /home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/v62-reader
git diff main --stat -- benchmarks/regenerate_v6.py benchmarks/flight_list_v6.py \
                        benchmarks/regenerate_v7.py
cd /home/jupyter/work/opdi-workspace/opdi-portal
git status --porcelain papers/adep-ades-detection-v6 papers/adep-ades-detection-v7
```

Expected: both empty.

- [ ] **Step 3: Push the branch**

```bash
cd /home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/v62-reader
git push -u origin v62-reader
```

**Do not merge to main and do not push `opdi-portal`.** Both are the user's
call; last time they asked explicitly.

- [ ] **Step 4: Report**

State: the parity result (pass or fail, with the rank and gap); whether any
regenerated figure moved; what the spike concluded about diagrams; and the
branch name. If parity failed, present it as a result with the numbers, and
name the decision it implies without taking it.

---

## Self-Review

**Spec coverage.** W0 → Task 1. W1 → Task 2. W2 → Task 3. W3 → Task 6. W4 →
Task 7. W5 → Tasks 4 and 5 (the grid extension is part of regenerating from
main, since the check cannot pass without it). Task 8 covers the spec's
requirement that `config.py` is not changed and that portal stays unpushed.

**Placeholders.** None. Every code step carries its code; every check states its
expected output. Task 6 Steps 1–3 are editorial and specify content and length
rather than exact prose, which is the correct granularity for writing.

**Type consistency.** `Finding(line, value, text)` defined in Task 2 and used in
Task 2 Steps 5 and 7 and Task 6 Step 4. `config_parity(grid_csv, cfg) ->
(diffs, report)` and `ConfigurationNotOnGrid` defined in Task 3 and used in
Task 5 Step 5. `GRID_MARGINS` defined in Task 4 and consumed by
`regenerate_v62.py` in the same task. `top100_recall(run, role)` already exists
in the paper's setup chunk and is called in Task 6 Step 5.

**Known risk, stated not designed away.** Task 5 may fail Task 3's parity check
on the vote margin, because main ships 0 and nothing has ever tested it through
the pipeline. The plan treats that as a result to report, not a defect to fix,
and explicitly forbids changing `config.py` to make the check pass.
