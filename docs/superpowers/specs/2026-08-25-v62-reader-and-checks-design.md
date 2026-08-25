# V6.2 for a first-time reader, and two checks — design

**Status:** approved in chat 2026-08-25. Implementation plan to follow.

**Scope:** edits to `papers/adep-ades-detection-v6.2` **in place**, plus two new
verification tools in `opdi/benchmarks`. No cluster campaign: nothing here
changes what was measured, so every staged figure stays valid.

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

* **Edit in place.** V6.2 keeps its identity and its numbers. `opdi-portal` is
  not pushed, so nothing published moves underneath anyone.
* **No cluster.** Every check must run without Spark, S3 or credentials.
* **Both formats.** The paper renders to HTML and PDF. Anything format-specific
  is a defect unless deliberately justified.
* **No Chromium, no graphviz.** Installed and usable: `DiagrammeR`, `V8`,
  `rsvg`, `igraph`, `magick`, `gridExtra`. Not installed: `DiagrammeRsvg`,
  `graphviz`, `mermaid-cli`, any Chromium.

## Workstreams

Five, ordered so that the checkers produce the editorial work list rather than
the editorial work being guessed at.

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

Three assertions:

1. **Config parity.** Every shipped value the report quotes equals a freshly
   constructed `DetectionConfig()`. The report already generates its "What
   ships" table from the `recommended` run's recorded parameters; this asserts
   that those recorded parameters still match the code *as it stands on main*,
   which the generated table cannot do by itself.
2. **Code presence.** Every script and module named in `data/_manifest.json`
   exists at `origin/main` — checked with `git cat-file -e origin/main:<path>`,
   which needs no working tree.
3. **SHA ancestry.** Every manifest entry's `git_sha` is an ancestor of
   `origin/main`, and its `git_dirty` is false. This catches the mirror of V6's
   failure: a figure produced by code that never got merged.

**Offline behaviour:** if `origin/main` is unavailable the test **skips with an
explicit message**, rather than failing. A check that cannot run without a
network is a check people disable.

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

## Testing

Both checkers become pytest tests, joining the existing 293. Neither needs a
cluster. W2's ancestry assertion skips loudly without a remote.

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
* No re-run of the campaign.
