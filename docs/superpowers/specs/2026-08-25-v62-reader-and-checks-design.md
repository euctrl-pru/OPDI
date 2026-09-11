# V6.2 for a first-time reader, checked against main — design

**Status:** approved in chat 2026-08-25. Implementation plan to follow.

**Scope:** edits to `papers/adep-ades-detection-v6.2` **in place**, two new
verification tools in `opdi/benchmarks`, and a re-run of the campaign against
the `opdi` package as it stands on `main`.

The re-run is not expected to move any number — main is the merge of the branch
that produced them — but "merged, therefore equivalent" is an inference, and
this study exists because inferences about provenance were wrong twice. A
figure that does move is a finding.

## Problem

V6.2 was written as the successor to V6 and reads like one. A reader who has
not read V6 meets three obstacles, all measured rather than assumed:

* **27 prose lines reference earlier versions.** Many are fine — "version 6.2
  changed X" is self-explanatory. Several are not: *"Version 5 proposed asking
  whether the trajectory points at the aerodrome"* and *"version 5 could not say
  which was wrong"* assume a document the reader does not have.
* **75 numerals are typed into prose**, 47 distinct. Some are legitimate
  constants — H3 resolution 7 is ≈5.2 km, `FL60` admits everything below
  6,100 ft. Others are *results*: `+0.12%`, `+5.7%`, `+70 of the +109`, `+110`.
  A typed result is one that can go stale without anything saying so. Three of
  this class were already found and fixed during the v6.2 build (`0.16 pp`
  against a real `0.15`, "seventy per cent" against a real 70.0%, and V6's
  radius claim) — found by tripping over them, not by looking.
* **The four methodology diagrams exist only in HTML.** Rasterising mermaid
  needs a headless Chromium that does not start on this host, so the PDF gets
  fallback tables instead — and two of those four are prose reading "identical
  to the table above, with one difference".

Two further gaps the report does not currently close:

* Nothing checks that what the report says ships is what `opdi/` **main**
  actually contains. V6 asserted `trend_radius_nm` ships at 20 NM; the code has
  30, and every later version copied the claim rather than measuring it.
* The report never shows a reader **how to run** the thing it recommends.

## Constraints

* **Edit in place.** V6.2 keeps its identity. `opdi-portal` is not pushed, so
  nothing published moves underneath anyone.
* **Main is the authority.** Every number in the report is produced by the
  `opdi` package as it stands on `main`, and every check runs against that same
  package. Not a worktree, not a branch. See W5.
* **The checks need no cluster; regenerating the numbers does.** W1 and W2 run
  from committed files and `git`, so they belong in the test suite. W5 is a
  full campaign and needs Spark, S3 and credentials.
* **Both formats.** The paper renders to HTML and PDF. Anything format-specific
  is a defect unless deliberately justified.
* **No Chromium, no graphviz.** Installed and usable: `DiagrammeR`, `V8`,
  `rsvg`, `igraph`, `magick`, `gridExtra`. Not installed: `DiagrammeRsvg`,
  `graphviz`, `mermaid-cli`, any Chromium.

## Workstreams

Six, ordered so that the checkers produce the editorial work list rather than
the editorial work being guessed at, and so that the regenerated numbers are
checked the moment they land.

### W0 — Spike: browser-free diagram rendering

**A feasibility question, not code we keep.** Can a diagram be rendered from one
source into both HTML and PDF using only what is installed?

Probe, in order, stopping at the first that works:

1. `DiagrammeR::grViz()` → SVG via `V8` running the bundled viz.js → PDF via
   `rsvg::rsvg_pdf()`. `DiagrammeRsvg` normally bridges this; it is absent, so
   the probe is whether its job can be done directly.
2. `igraph` laid out and drawn to a graphics device. Renders to any device
   natively, so both formats come free; the cost is manual layout for a
   flowchart rather than a graph.
3. `grid`/`gridExtra` boxes and arrows. Total control, most work.

**Output:** an answer plus, if one path works, a single proof-of-concept
diagram rendered into both formats. Anything built stays labelled throwaway
until W3 adopts it.

**If all three fail:** W3 falls back to completing the PDF tables — all four
diagrams as full step tables rather than two as "identical to the above" — and
the format asymmetry is stated in the paper rather than hidden.

### W1 — Number-provenance checker

**New:** `benchmarks/check_report_numbers.py`, plus an allowlist beside the
paper and a test that runs it.

Every numeral in the paper's prose must be one of:

* **computed** — produced by an inline `` `r ` `` expression; or
* **declared** — present in an allowlist that records the value *and why it is
  a constant*.

Anything else fails the check.

The check applies to **prose the author typed**. Whatever a chunk prints is
computed by construction — it came from a staged CSV — so chunk bodies and
their output are out of scope.

**The allowlist lives beside the paper**, at
`papers/adep-ades-detection-v6.2/constants.yml`, not in the harness. It
describes that document, so it should travel with it. Each entry carries:

```yaml
- value: "6,100"
  why: >
    A definition, not a measurement. `flight_level` is an integer cast, so
    `flight_level <= 60` admits everything below 6,100 ft. The number follows
    from the cut, not from the data.
```

The `why` field is the point. A bare list of permitted numbers would become a
place to silence the checker; a field that must say *why this is not a result*
makes silencing it visible in review.

**Parsing:** strip fenced R chunks, then scan remaining lines. Skip inline `r`
expressions, table pipes, and headings.

**Exempt without declaration**, because requiring an allowlist entry for these
would produce a registry nobody reads:

* bare integers 0–12, which in prose are almost always counts or ordinals
  ("two of the three", "the fifth rung");
* four-digit years 2000–2099;
* section and step identifiers matched as such — `S6`, `E2`, `path5`, `step 04`,
  `FL60`, `@sec-*`.

Everything else — including any number carrying `%`, `pp`, `ft`, `NM`, a
thousands separator, or a decimal point — must be computed or declared. Those
markers are what a *result* looks like in this paper.

**Known work this generates:** at minimum `+0.12%`, `+5.7%`, `+70`, `+109`,
`+110`, `2,975` and `240` become computed, since all are results.

### W2 — Implementation-parity checker

**New:** `benchmarks/check_main_parity.py`, plus a test.

#### What "recommended" means

**The sweep decides, and the deciding sweep is the one that runs main's `opdi`
package.** The recommendation is not a value typed into this spec, into the
paper, or into `config.py`. It is the optimum of the **pipeline grid** — the
job that calls `process_dai` — produced by the code on `main`. So the loop
closes on main: main's code picks the configuration, and main's configuration
is then checked against it.

This is a deliberate change from V6's practice, which recorded a recommendation
in prose and let it drift from the code for three versions.

#### Why the pipeline grid and not the harness sweep

There are two instruments and **they disagree**:

| | Grid | Says |
|---|---|---|
| Harness sweep (`trend_sweep_agl.py`) | 280 cells, both periods | 6,100 ft / margin 2 / 20 NM |
| Pipeline grid (`flight_list_v62.py`) | 12 cells, 2025 only | 6,000 ft / 30 NM |

Only the pipeline grid calls `process_dai`; the harness sweep is a
reimplementation that never touches the `opdi` package. Since the criterion is
*the sweep using main's package*, the pipeline grid is the authority.

The disagreement is not a defect. It is V6's central finding restated — the
harness optimum need not survive contact with production — and the paper
already says so: *"'credible' is not 'verified', and the cheapest way to verify
is to run the thing itself."* The harness sweep stays in the report as
supporting evidence, and where the two differ the report says which is which.

#### The grid must contain main's configuration

Measured, not assumed: main's cell `(6,000 ft, margin 0, 30 NM, 10 NM)` is
**absent from the harness grid** — `HEIGHT_CAPS` has no 6,000 — so the check
could not even report where main ranks. The pipeline grid does carry 6,000 ft
and 30 NM, but **fixes the vote margin at 2** while main ships **0**, so that
parameter is currently validated by nothing at all.

So the pipeline grid is extended to sweep the margin: **6 ceilings × 2 radii ×
3 margins (0, 2, 4) = 36 cells**, up from 12. A parity check against a grid
that does not contain the thing being checked is not a check.

**The harness sweep is deliberately left at its current caps.** Adding 6,000 ft
there would mean rebuilding the vote cache — an expensive upstream stage — for
an instrument that is no longer the authority. The limitation is already stated
in the report and stays stated.

#### The assertions

1. **Config parity.** A freshly constructed `DetectionConfig()`, imported from
   **main's** `src/opdi`, equals the pipeline grid's best cell on every
   parameter that grid varies — ceiling, radius, vote margin. Mismatch fails,
   and the failure names each differing parameter with both values.
2. **Rank and gap are always reported**, pass or fail: where main's cell
   places in the pipeline grid, out of how many, and the score between it and
   the best cell. A check that only says "pass" teaches nobody anything; the
   rank is the interesting number even when it is 1.

   **If main's cell is absent from the grid, that is a failure in its own
   right**, distinct from a mismatch, and it says so. A parity check against a
   grid that does not contain the configuration being checked is not a check --
   which is exactly the state the harness sweep is in today.
3. **Code presence.** Every script and module named in `data/_manifest.json`
   exists at `origin/main` — `git cat-file -e origin/main:<path>`, no working
   tree needed.
4. **SHA ancestry.** Every manifest entry's `git_sha` is an ancestor of
   `origin/main` with `git_dirty` false. Catches the mirror of V6's failure: a
   figure produced by code that never got merged.

#### The known tension, and how it is handled

Letting the sweep decide means a grid whose argmax moves could demand a change
to a published default. That risk is real and is mitigated, not ignored:

* the **rank and gap are reported on every run**, so drift toward the edge of
  the plateau is visible long before it flips the argmax;
* the paper's existing plateau argument -- a value on a broad plateau is a safe
  recommendation, one on a sharp peak is not -- stays in the text next to it;
* the pipeline grid is **2025 only**, since `process_dai` cannot read the 2024
  tracks. So it is a single-sample instrument and the report must keep saying
  so. The harness sweep's two-period agreement stays in the report as the
  cross-period evidence the pipeline grid cannot supply.

If the check fails, the resolution is a decision, not an automatic edit:
either main's configuration changes, or the sweep grid is shown to be
mis-specified. **This spec does not authorise changing `config.py`.**

#### Offline behaviour

If `origin/main` is unavailable, assertions 3 and 4 **skip with an explicit
message** rather than failing. A check that cannot run without a network is a
check people disable. Assertions 1 and 2 need only the local `main` ref and the
staged sweeps, so they always run.

### W3 — Editorial for the first-time reader

**Modified:** `papers/adep-ades-detection-v6.2/index.qmd`.

* **An orientation section, before any result.** What OPDI is; what a flight
  list is; what ADEP and ADES mean; why three methods exist and what each is
  for. A reader should be able to start at the top and never need another
  document.
* **A short glossary.** ADEP/ADES, track, H3 cell, coverage against accuracy,
  the exchange rate *k*, out-of-area. Tight — a glossary that explains
  everything explains nothing.
* **De-reference the 27 prior-version mentions.** Each either explains itself
  in place or goes. "Version 5 proposed asking whether the trajectory points at
  the aerodrome" becomes a sentence that stands on its own; the citation stays
  as attribution, not as a prerequisite.
* **Diagrams** per W0's answer.
* **Top-100 analysis moves to an appendix.** See below.
* **Numbers** made computed wherever W1 flags them.

#### The top-100 appendix

The section is currently 78 lines carrying two 100-row, 9-column tables plus a
two-row summary, sited between the verdict and the stability check.

* The **column definitions and both 100-row tables** move to an appendix.
* The **body keeps the finding** — recall across the hundred busiest under the
  shipped configuration against legacy — as a computed sentence, with a pointer
  to the appendix for the detail.
* `top100_recall()` is currently **defined in the setup chunk and never
  called**. It computes exactly the summary the body now needs, so it stops
  being dead code.

**Appendices are ordinary top-level sections at the end**, under an
`# Appendices` heading — *not* Quarto's `.appendix` class, which behaves
differently between HTML and PDF. We render both, so the plain construction is
the one that cannot produce a format-specific surprise. Same reasoning that put
the mermaid fence behind `is_html_output()`.

`sec-per-type` (By aerodrome class) is adjacent and similar in character. It
stays in the body for now; moving it is a one-line change if wanted later.

### W4 — Usage example

**Modified:** `index.qmd`, new section.

Grounded in the real API: console script `opdi` → `opdi.cli:main`; and
`run_pipeline(env, start_date, end_date, step, adep_mode, ades_mode)` in
`src/opdi/runner.py`. Step `03` produces the flight list.

The headline for a newcomer is that **the recommendation is the default**:
`adep_mode` and `ades_mode` default to `None`, which means `DetectionConfig()`'s
own values — the configuration this report recommends. You get it by not
overriding anything.

The section shows the Python call, the CLI equivalent, where output lands, and
what to change if you want something other than the recommendation. It is
verified by W2's config-parity assertion, so the example cannot describe a
configuration the code does not have.

### W5 — Regenerate every number with main's package

**The report's numbers must come from `main`, not from the branch that
developed them.** They currently come from `v62-combined`, whose commits are
now merged — so W2's ancestry assertion would pass — but "merged, therefore
equivalent" is an inference, and this study exists because inferences about
provenance were wrong twice.

So the campaign is re-run against main:

* the harness invoked from a checkout of `main`, not a worktree of a branch;
* `PYTHONPATH` pointing at **main's** `src/opdi`, so `process_dai`,
  `DetectionConfig` and the altitude cut all come from the shipped package;
* both the pipeline arms and the sweeps, since the pipeline grid decides the
  recommendation W2 checks against;
* **the pipeline grid extended to 36 cells** -- 6 ceilings x 2 radii x 3 vote
  margins (0, 2, 4) -- so that main's own configuration is on the grid and its
  margin is tested rather than assumed;
* every resulting manifest entry recording a `git_sha` on `main` with
  `git_dirty` false — which W2 assertion 4 then verifies rather than assumes.

**This is the expensive part of the work**: 19 jobs, five of them through
`process_dai`, on a shared cluster. The previous campaign took roughly three
hours across two launches, and the grid growing from 12 cells to 36 adds
about 1.7 hours at the observed ~4.2 minutes per run -- so budget five hours,
and the namespace quota must be free. See `V62_RUN_NOTES.md` on why `ps` is the
wrong way to establish that.

**Expected outcome:** the existing numbers should be unchanged, because main's
code is the merge of the branch that produced them. Any figure that *does* move
is a finding, not a nuisance -- it would mean the merge changed behaviour that
nothing noticed. The 24 new grid cells are genuinely new measurements and carry
no such expectation.

**Known at the time of writing:** main ships margin 0 and the current grid
fixes margin at 2, so whether main's margin survives the pipeline is an open
question this campaign answers for the first time. It may fail W2. That is a
result, not a problem with the check.

**Ordering:** W5 runs after W1 and W2 exist, so that the regenerated numbers
are checked the moment they land rather than checked later by someone
remembering to.

## Testing

Both checkers become pytest tests, joining the existing 293. Neither needs a
cluster. W2's ancestry assertion skips loudly without a remote.

**Tests run against main's `opdi` package.** The venv installs `opdi` as an
editable install pointing at the **main checkout's** `src` — so a test run from
a worktree silently imports main's package unless `PYTHONPATH` overrides it.
That accident is the behaviour this spec wants, but it must become explicit:
the test that matters asserts `opdi.__file__` resolves under the main checkout,
so nobody can later "fix" the import path and quietly start testing a branch.

The checkers are themselves tested against **known-bad fixtures** — a snippet
with a typed result, a manifest entry with a dirty SHA — so that a checker
which silently passes everything is caught. A guard that cannot fail is worse
than no guard, and this study has already shipped one test that inspected an
empty set.

## Out of scope

* No rewrite of the paper's voice or restructure of its argument. V6.2's
  structure is what was asked for; W3 adds an on-ramp and removes unexplained
  back-references.
* **No change to `config.py`.** The two divergences V6.2 found — the radius and
  the vote margin — stay documented and unfixed. Changing a published default
  is the maintainers' decision, not a side effect of improving a report.
* No renaming or re-versioning: v6.2 keeps its identity.
