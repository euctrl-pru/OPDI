# V6.2 Combined Paper Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to
> implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce `adep-ades-detection-v6.2` — V6's full parameterisation study
with every number recomputed on the shipped field-elevation datum, V6.1's datum
study and methodology diagrams folded in, and V6.1 deleted.

**Architecture:** Fork the two V6 harness scripts (`regenerate_v6.py`,
`flight_list_v6.py`) into `_v62` versions, following the fork convention already
used for V6.1. The fork absorbs V6.1's jobs, retires V6's two trend-sweep jobs in
favour of V6.1's paired sweeps (measured identical on the MSL arm, both periods),
and makes the trend ceiling datum-aware wherever a config is built from a sweep
row. V6 itself is untouched and stays reproducible as the historical record.

**Tech Stack:** Python 3.10, PySpark 4.1.1 on Kubernetes (client mode), pytest,
Quarto + knitr (R), parquet over S3A at `s3a://eurocontrol/opdi`.

**Spec:** No separate spec document. This plan was approved directly from the
in-chat design (brainstorming "bounded" path); the design is restated in full in
"Background and decisions" below, so the plan is self-contained.

**Worktree:** `/home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/v62-combined`,
branch `v62-combined`, based on `main` at `e7343b8`.

## Global Constraints

- **Never mutate a published `version` string.** V6 and V7 are frozen. No task
  may write into `papers/adep-ades-detection-v6/`, `.../v7/`, or the vote caches
  `research/trend_votes` and `research/trend_votes_2024`.
- **Units: storage is SI, human-facing is aviation.** New thresholds go in
  aviation units with the unit in the field name (`trend_max_height_ft`).
- **`flight_level` is an integer cast**, so `flight_level <= 60` admits
  everything below **6,100 ft**, not 6,000. The two cuts are not interchangeable
  at the same nominal ceiling. This is why the datum arms compare at 6,100.
- **Commit messages carry no self-reference** — no `Co-Authored-By`, no
  "generated with" trailer. Describe the change only.
- **`opdi-portal` is committed but NOT pushed.** Only `opdi/` is pushed.
- **Datasets live in `s3a://eurocontrol/opdi/`, never local disk.**
- Tests run with `.venv310/bin/python -m pytest tests/` from the repo root. The
  suite is 260 tests at `e7343b8` and must stay green.
- The cluster is **shared**. Never launch a benchmark while another
  `benchmarks/` driver is running — contention starves rather than queues.

---

## Background and decisions

Read this before Task 1. It is the reasoning the tasks assume.

### Why V6.1 was not what was asked for

V6.1 (628 lines) is a standalone study of one change. The request was an
*extended* V6 (1,634 lines, ~30 sections). V6.2 is V6's structure plus V6.1's
content, with the numbers re-measured on the datum that ships.

### What is actually stale in V6, and why

`regenerate_v6.py --check` reports 12 of 14 jobs stale, in three groups that
need different treatment:

| Group | Jobs | Cause | Treatment |
|---|---|---|---|
| Datum-dependent | `modes`, `trend_grid`, `pipeline_path`, `pipeline_path_ring`, `trend_bearing` | `flights.py` / `config.py` changed | **Re-run.** These are the multi-hour `flight_list` jobs. |
| Superseded | `trend_sweep_2025`, `trend_sweep_2024` | same | **Retire.** See below. |
| Bookkeeping only | `endpoint_sweeps`, `endpoint_sweeps_2024`, `bearing`, `vertical_measure` | input "24 objects → 23" | **Re-run.** Expect identical numbers. |
| Genuine new data | `merge_diagnosis` | `research/reference` 4 → 6 objects | **Re-run.** |
| Current | `sampler_comparison` | — | Carry over. |

The "24 → 23" drop is not a data change. It is the fix in
`benchmarks/provenance.py:s3_identity` that stops counting MinIO's zero-byte
`/`-suffixed directory marker. The underlying data is byte-identical and these
four jobs are datum-independent, so re-running them must reproduce the same
numbers. **Do not hand-patch the manifest to clear them** — that is precisely
the failure the module exists to prevent. Re-running converts V6.1's *inferred*
claim that departures are untouched (0 in every elevation band) into a direct
verification on the endpoint path, which is worth the cluster time.

### Why V6's trend sweeps are retired rather than re-run

`benchmarks/trend_sweep_agl.py` is a strict superset of
`benchmarks/trend_sweep.py`: identical `FL_CAPS`, `MARGINS`, `RADII_NM`,
`PENALTIES_NM`, `CACHE_RADIUS_NM`, `EARTH_R_NM`; it adds `HEIGHT_CAPS` and
`--datum {field,msl}`, and its vote cache carries both `up_fl_*` and `up_agl_*`
votes. One run per period yields both datums.

This was verified, not assumed. Comparing V6's `trend_sweep_2025.csv` against
V6.1's `fl_sweep_2025.csv` on the join key
`(stage, stage2_role, fl_cap, radius_nm, penalty_nm, margin, k, legacy)` gives
371 shared cells and **zero differing cells**; 2024 likewise, 371 cells, zero
differences. The equality is a reportable cross-check in the paper, not just an
internal justification.

The four sweep jobs V6.2 inherits from V6.1 (`height_sweep_2025`,
`fl_sweep_2025`, `height_sweep_2024`, `fl_sweep_2024`) are all **current** at
`e7343b8` and cost no cluster time.

### The cap column is shared between the two sweeps

`height_sweep_*.csv` carries the ceiling in the **`fl_cap` column** — values
2000…12000 — with `datum='field'`. `fl_sweep_*.csv` carries flight levels
20…200 with `datum='msl'`. So `argmax()` needs no change; only the *config built
from the winning row* has to dispatch on `datum`. That dispatch is Task 1.

### The path walk gains a fifth rung

`path_cfg()` starts from `DetectionConfig.legacy()`, which is
`trend_max_datum="msl", trend_max_fl=40`. So `path0`…`path4` remain on MSL
unchanged, and V6.2 appends `path5_datum` — path4 plus the field datum at the
equivalent ceiling. This isolates the datum as one rung of the ladder, which is
exactly what V6's "Which change is actually doing the work" section measures,
extended by one row. It is the cheapest possible way to answer "what did the
datum buy, inside the pipeline".

### The open question V6.2 must settle

The shipped default is `trend_max_height_ft = 6000.0` (`src/opdi/config.py:487`).
The sweep optimum was measured at **6,100 ft** — and **6,000 was never on the
sweep grid** (`HEIGHT_CAPS = (2000, 3000, 4000, 6100, 8000, 10000, 12000)`). So
the claim "the ceiling is tuned" currently rests on a grid that does not contain
the value that ships. V6.2's pipeline grid therefore includes **both 6000 and
6100**, and the paper reports which one the pipeline prefers. If 6,100 wins, that
is a recommendation to change the shipped default; if they tie or 6,000 wins, the
shipped value is vindicated. Either way the paper stops asserting a tuning it did
not measure.

`height_pipeline_2025` — the job V6.1 deliberately left unrun because it "would
confirm a value nothing is changing" — is subsumed by this grid, which now does
change something.

---

## File Structure

**Create:**
- `benchmarks/flight_list_v62.py` — pipeline runner. Fork of `flight_list_v6.py`.
  Adds datum-aware ceiling construction, `path5_datum`, and a height-cap grid.
- `benchmarks/regenerate_v62.py` — job registry. Fork of `regenerate_v6.py`,
  absorbing V6.1's jobs and dropping the two retired trend sweeps.
- `tests/test_flight_list_v62.py` — guards on ceiling construction.
- `tests/test_regenerate_v62.py` — guards on registry shape. Fork of
  `tests/test_regenerate_v61.py`.
- `../opdi-portal/papers/adep-ades-detection-v6.2/index.qmd` — the paper.
- `../opdi-portal/papers/adep-ades-detection-v6.2/data/` — staged CSVs.

**Delete (Task 7 only, after V6.2 renders):**
- `../opdi-portal/papers/adep-ades-detection-v6.1/`
- `benchmarks/regenerate_v61.py`, `benchmarks/flight_list_v61.py`,
  `tests/test_regenerate_v61.py`

**Never touch:** `benchmarks/regenerate_v6.py`, `benchmarks/flight_list_v6.py`,
`benchmarks/regenerate_v7.py`, `papers/adep-ades-detection-v6/`,
`papers/adep-ades-detection-v7/`.

---

### Task 1: Datum-aware trend ceiling in `flight_list_v62.py`

The whole correctness risk of V6.2 sits in one place: a config that carries a
field-datum ceiling *and* a stale `trend_max_fl`, or vice versa. Whichever the
code reads first wins, silently, and the arm is then measured at a ceiling the
paper does not report. One helper, tested, is the defence.

**Files:**
- Create: `benchmarks/flight_list_v62.py` (fork of `benchmarks/flight_list_v6.py`)
- Test: `tests/test_flight_list_v62.py`

**Interfaces:**
- Consumes: `opdi.config.DetectionConfig` (fields `trend_max_datum: str`,
  `trend_max_height_ft: float`, `trend_max_fl: int`; `__post_init__` raises
  `ValueError` unless `trend_max_datum in ("field", "msl")`).
- Produces:
  - `trend_ceiling_kwargs(row: dict) -> dict` — keyword arguments describing the
    altitude cut a sweep row selects. Used by Tasks 1 and 2.
  - `GRID_HEIGHT_CAPS: tuple[int, ...]` = `(3000, 4000, 6000, 6100, 8000, 10000)`
  - `DATUM_ARM_CEILING_FT: float` = `6100.0`

- [ ] **Step 1: Create the fork**

```bash
cd /home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/v62-combined
cp benchmarks/flight_list_v6.py benchmarks/flight_list_v62.py
```

Then update its module docstring: it builds flight lists on the **shipped
field-elevation datum**, and the sweep it reads may be either datum.

- [ ] **Step 2: Write the failing test**

Create `tests/test_flight_list_v62.py`:

```python
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


def test_the_datum_arm_ceiling_matches_the_integer_flight_level_cut():
    """`flight_level` is an integer cast, so FL60 admits everything below
    6100 ft. Comparing the datums at 6000 would move the ceiling and the
    datum at once and measure neither."""
    assert flight_list_v62.DATUM_ARM_CEILING_FT == 6100.0
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd /home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/v62-combined && .venv310/bin/python -m pytest tests/test_flight_list_v62.py -v`

Expected: FAIL — `AttributeError: module 'flight_list_v62' has no attribute 'trend_ceiling_kwargs'`.

- [ ] **Step 4: Implement**

In `benchmarks/flight_list_v62.py`, near the top after the imports:

```python
#: The ceiling the datum arms hold constant, in feet.
#:
#: `flight_level` is an integer cast, so `flight_level <= 60` admits everything
#: below 6,100 ft. Comparing a field-datum 6,000 against FL60 would move the
#: ceiling and the datum in the same step and measure neither.
DATUM_ARM_CEILING_FT = 6100.0

#: Ceilings walked through `process_dai` itself, in feet above field elevation.
#:
#: Carries both 6000 and 6100 deliberately. 6000 is what ships; 6100 is what the
#: sweep called optimal -- on a grid that never contained 6000. Until both are
#: run through the pipeline, "the ceiling is tuned" is a claim about a grid, not
#: about the shipped value.
GRID_HEIGHT_CAPS = (3000, 4000, 6000, 6100, 8000, 10000)


def trend_ceiling_kwargs(row: dict) -> dict:
    """Config keywords for the altitude cut a sweep row selects.

    Both sweeps carry the cap in `fl_cap`; `datum` says what the number means.
    On the field datum it is feet above field elevation, on MSL a flight level.

    Returns only the keys that apply, so a config can never carry two ceilings
    at once. That matters because `trend_altitude_cut` reads exactly one of
    them: a leftover `trend_max_fl` beside a field ceiling is inert on one code
    path and authoritative on the other, and nothing in the output says which
    ran.

    A row with no `datum` column is MSL -- that is V6's own CSV format, and
    reinterpreting FL60 as 60 ft above field would abstain on everything.
    """
    datum = row.get("datum") or "msl"
    if datum == "field":
        return {"trend_max_datum": "field",
                "trend_max_height_ft": float(row["fl_cap"])}
    if datum == "msl":
        return {"trend_max_datum": "msl", "trend_max_fl": int(row["fl_cap"])}
    raise ValueError(
        f"unknown datum {datum!r} in sweep row; expected 'field' or 'msl'. "
        f"Defaulting here would apply one altitude cut while reporting the other."
    )
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `.venv310/bin/python -m pytest tests/test_flight_list_v62.py -v`
Expected: PASS, 9 tests.

- [ ] **Step 6: Wire the helper into every place a ceiling is set**

Four edits in `benchmarks/flight_list_v62.py`. Replace each `trend_max_fl=`
assignment that derives from a sweep row.

(a) The `combined` / arrival config (was `trend_max_fl=int(t_ades["fl_cap"])`):

```python
            **trend_ceiling_kwargs(t_ades),
```

(b) The departure override (was `d.trend_max_fl = int(t_adep["fl_cap"])`):

```python
        for k_, v_ in trend_ceiling_kwargs(t_adep).items():
            setattr(d, k_, v_)
```

(c) The path walk. Replace the `t_fl, t_mg = ...` line and the `path` dict:

```python
    t_mg = int(t_ades["margin"])
    t_rd, t_pn = float(t_ades["radius_nm"]), float(t_ades["penalty_nm"])
    t_ceiling = trend_ceiling_kwargs(t_ades)
    path = {
        "path0_legacy":  path_cfg(),
        "path1_penalty": path_cfg(trend_sched_penalty_nm=t_pn),
        "path2_ceiling": path_cfg(trend_sched_penalty_nm=t_pn, **t_ceiling),
        "path3_margin":  path_cfg(trend_sched_penalty_nm=t_pn, **t_ceiling,
                                  trend_vote_margin=t_mg),
        "path4_radius":  path_cfg(trend_sched_penalty_nm=t_pn, **t_ceiling,
                                  trend_vote_margin=t_mg, trend_radius_nm=t_rd),
        # The fifth rung: path4 with the datum flipped, at the equivalent
        # ceiling. `path_cfg` starts from `legacy()`, which is MSL/FL40, so
        # rungs 0-4 are on the sea-level datum whatever sweep was read, and
        # this is the only step that moves the datum.
        "path5_datum":   path_cfg(trend_sched_penalty_nm=t_pn,
                                  trend_vote_margin=t_mg, trend_radius_nm=t_rd,
                                  trend_max_datum="field",
                                  trend_max_height_ft=DATUM_ARM_CEILING_FT),
    }
```

Note `path2_flcap` is renamed `path2_ceiling`: on the field datum it is not a
flight-level cap, and a run label that says otherwise ends up quoted in prose.

(d) The grid. Replace the `for fl in args.grid_fl:` block:

```python
    grid = {}
    for cap in args.grid_height:
        for rd in args.grid_radius:
            for mg in args.grid_margin:
                grid[f"grid_h{cap}_r{rd:g}_m{mg}"] = path_cfg(
                    trend_max_datum="field", trend_max_height_ft=float(cap),
                    trend_radius_nm=float(rd), trend_vote_margin=mg,
                    trend_sched_penalty_nm=args.grid_penalty,
                )
```

And replace the `--grid-fl` argument with:

```python
    ap.add_argument("--grid-height", nargs="+", type=int,
                    default=list(GRID_HEIGHT_CAPS),
                    help="trend ceilings to walk through process_dai, in feet "
                         "above field elevation")
```

- [ ] **Step 7: Verify the whole suite is still green**

Run: `.venv310/bin/python -m pytest tests/ -q`
Expected: 269 passed (260 existing + 9 new).

- [ ] **Step 8: Commit**

```bash
git add benchmarks/flight_list_v62.py tests/test_flight_list_v62.py
git commit -m "Add the v6.2 pipeline runner on the field-elevation datum

Forks flight_list_v6.py. The trend ceiling is built from the sweep row's
own datum rather than assumed to be a flight level, so a config can never
carry two ceilings at once. Adds path5_datum, isolating the datum as one
rung of the tuning ladder, and walks the pipeline grid over ceilings in
feet above field -- including both 6000, which ships, and 6100, which the
sweep called optimal on a grid that never contained 6000."
```

---

### Task 2: The `regenerate_v62.py` job registry

**Files:**
- Create: `benchmarks/regenerate_v62.py` (fork of `benchmarks/regenerate_v6.py`)
- Test: `tests/test_regenerate_v62.py` (fork of `tests/test_regenerate_v61.py`)

**Interfaces:**
- Consumes: `flight_list_v62.GRID_HEIGHT_CAPS` (Task 1); `provenance.record`,
  `provenance.is_stale`, `provenance.s3_identity`, `provenance.inputs_changed`.
- Produces: `jobs() -> list[Job]`, `stages() -> list[Stage]`, `PAPER: Path`,
  `DATA: Path`, `T_VOTES: str`, `T_VOTES24: str`.

- [ ] **Step 1: Create the fork**

```bash
cd /home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/v62-combined
cp benchmarks/regenerate_v6.py benchmarks/regenerate_v62.py
```

- [ ] **Step 2: Write the failing test**

Create `tests/test_regenerate_v62.py`. Start from `tests/test_regenerate_v61.py`
— every guard in it still applies — and add the V6.2-specific ones:

```python
"""Guards on the v6.2 job registry.

Needs no cluster: these check the registry's *shape*, which is where this study
has historically gone wrong. A job whose declared dependencies are incomplete
looks current while serving numbers from code that has since changed, and
nothing in the file or its timestamp says so.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks"))

import pytest

import regenerate_v62


def test_the_paper_is_v62_and_not_v6_v61_or_v7():
    """V6 and V7 are frozen. A stray output path is the one way this study
    could overwrite a published paper's figures."""
    assert regenerate_v62.PAPER.name == "adep-ades-detection-v6.2"
    assert regenerate_v62.DATA.parent.name == "adep-ades-detection-v6.2"


def test_no_job_writes_into_a_frozen_paper():
    for job in regenerate_v62.jobs():
        assert "adep-ades-detection-v6/" not in " ".join(map(str, job.args))
        assert "adep-ades-detection-v7/" not in " ".join(map(str, job.args))


def test_the_vote_caches_are_the_agl_ones():
    """Building into `research/trend_votes` would leave V6 reproducible only
    against a cache built for a different study, on a different datum."""
    assert regenerate_v62.T_VOTES.endswith("trend_votes_agl")
    assert regenerate_v62.T_VOTES24.endswith("trend_votes_agl_2024")
    for stage in regenerate_v62.stages():
        assert not stage.produces.endswith("research/trend_votes"), stage.name
        assert not stage.produces.endswith("research/trend_votes_2024"), stage.name


def test_no_job_runs_v6s_or_v61s_scripts():
    """The fork is only worth having if it is actually used."""
    for job in regenerate_v62.jobs():
        assert "flight_list_v6.py" not in job.script, job.name
        assert "flight_list_v61.py" not in job.script, job.name
        assert job.script != "benchmarks/trend_sweep.py", job.name
        for path in job.code_paths:
            assert path not in ("benchmarks/flight_list_v6.py",
                                "benchmarks/flight_list_v61.py",
                                "benchmarks/trend_sweep.py"), job.name


def test_the_retired_trend_sweeps_are_gone():
    """V6's trend_sweep jobs are superseded by the paired AGL sweeps, whose
    MSL arm reproduces them cell for cell on both periods. Keeping them would
    mean two jobs computing the same 371 cells on the same datum."""
    names = {j.name for j in regenerate_v62.jobs()}
    assert "trend_sweep_2025" not in names
    assert "trend_sweep_2024" not in names


def test_the_paired_sweeps_cover_both_periods_and_both_datums():
    names = {j.name for j in regenerate_v62.jobs()}
    for arm in ("height_sweep", "fl_sweep"):
        assert f"{arm}_2025" in names, arm
        assert f"{arm}_2024" in names, arm


def test_every_pipeline_job_fingerprints_the_pipeline():
    """A job that runs process_dai must fingerprint flights.py and config.py.
    Without them the datum change would not mark its own results stale, which
    is the whole mechanism the paper's provenance rests on."""
    for job in regenerate_v62.jobs():
        if "flight_list_v62" in job.script:
            assert "src/opdi/pipeline/flights.py" in job.code_paths, job.name
            assert "src/opdi/config.py" in job.code_paths, job.name
            assert "benchmarks/flight_list_v62.py" in job.code_paths, job.name


def test_arm_c_fingerprints_the_banding_it_reads_along():
    """Arm C's conclusion moves if the bands move, so a band edit must mark it
    stale. It is the study's discriminating measurement."""
    for job in regenerate_v62.jobs():
        if job.name.startswith("elevation_bands"):
            assert "benchmarks/elevation_bands.py" in job.code_paths, job.name
            assert "benchmarks/elevation_arms.py" in job.code_paths, job.name


def test_the_pipeline_arms_are_2025_only():
    """`process_dai` reads `h3_res_7` straight off the track table. The 2024
    tracks pre-date H3 indexing, so the column is absent and the run dies on
    UNRESOLVED_COLUMN -- after the 2025 half has already been computed."""
    names = {j.name for j in regenerate_v62.jobs()}
    for arm in ("datum_swap", "elevation_bands", "modes", "trend_grid",
                "pipeline_path"):
        assert f"{arm}_2025" in names or arm in names, arm
        assert f"{arm}_2024" not in names, (
            f"{arm}_2024 cannot run: the 2024 tracks carry no h3_res_7")


def test_every_staged_output_name_is_unique():
    """Two jobs staging the same filename silently overwrite each other's
    provenance -- which is exactly how V6 lost two entries."""
    seen = {}
    for job in regenerate_v62.jobs():
        for staged in job.outputs.values():
            assert staged not in seen, f"{staged}: {seen.get(staged)} vs {job.name}"
            seen[staged] = job.name


def test_the_pipeline_gets_a_relative_track_name_not_a_uri():
    """`FlightListProcessor(tracks_table=...)` prefixes the bucket itself, so
    handing it a URI splices `s3a://eurocontrol/opdi/s3a:/eurocontrol/...` and
    dies on PATH_NOT_FOUND -- hours into a run."""
    for job in regenerate_v62.jobs():
        if "flight_list_v62" not in job.script:
            continue
        if "--tracks" not in job.args:
            continue
        value = job.args[job.args.index("--tracks") + 1]
        assert not str(value).startswith(("s3a://", "s3://")), (
            f"{job.name} passes a URI to the pipeline: {value!r}")


def test_the_sweep_gets_a_uri_not_a_relative_name():
    """The mirror image: the sweep reads parquet itself and a bare name would
    resolve against the working directory."""
    for job in regenerate_v62.jobs():
        if "trend_sweep_agl" not in job.script:
            continue
        if "--cache" in job.args:
            assert str(job.args[job.args.index("--cache") + 1]).startswith("s3a://"), job.name


def test_the_portal_is_found_from_inside_a_worktree():
    """`REPO.parent / "opdi-portal"` is right when opdi/ sits in the workspace
    root and wrong inside a git worktree, which lives three levels deeper.
    The failure is silent: every output reads "output missing" -- which is
    indistinguishable from "never generated"."""
    assert regenerate_v62.PAPER.parent.name == "papers"
    assert regenerate_v62.PAPER.parent.parent.name == "opdi-portal"
    assert regenerate_v62.PAPER.parent.is_dir()
    assert ".claude" not in str(regenerate_v62.PAPER)


def test_pipeline_jobs_read_the_field_datum_sweep():
    """The pipeline arms must be tuned against the datum they run on. Passing
    the MSL sweep would select a ceiling in flight levels and then apply it as
    feet above field."""
    for job in regenerate_v62.jobs():
        if "flight_list_v62" not in job.script:
            continue
        if "--trend-sweep" not in job.args:
            continue
        value = str(job.args[job.args.index("--trend-sweep") + 1])
        assert "height_sweep" in value, f"{job.name} reads {value}"


def test_arm_c_consumes_what_arm_a_produces():
    """Arm C reads Arm A's per-airport CSV by name. If Arm A stops staging it,
    Arm C fails on a missing path hours into a run -- or reads a stale copy."""
    jobs = {j.name: j for j in regenerate_v62.jobs()}
    produced = set(jobs["datum_swap_2025"].outputs.values())
    assert "per_airport_datum_2025.csv" in produced
    consumed = " ".join(map(str, jobs["elevation_bands_2025"].args))
    assert "per_airport_datum_2025.csv" in consumed
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `.venv310/bin/python -m pytest tests/test_regenerate_v62.py -v`
Expected: FAIL — `PAPER.name == "adep-ades-detection-v6"`, retired sweeps still
present, pipeline jobs still on `flight_list_v6.py`.

- [ ] **Step 4: Edit `regenerate_v62.py` — paths, caches and code lists**

```python
PAPER = REPO.parent / "opdi-portal" / "papers" / "adep-ades-detection-v6.2"
DATA = PAPER / "data"
```

Replace the naive `PAPER` assignment with the worktree-safe search — copy
`_find_portal()` verbatim from `benchmarks/regenerate_v61.py`, which walks up
from `REPO` looking for a sibling `opdi-portal`, and use it:

```python
PAPER = _find_portal() / "papers" / "adep-ades-detection-v6.2"
```

Then the caches and the code dependency list:

```python
T_VOTES   = "s3a://eurocontrol/opdi/research/trend_votes_agl"
T_VOTES24 = "s3a://eurocontrol/opdi/research/trend_votes_agl_2024"

PIPE = CORE + ["src/opdi/pipeline/flights.py", "src/opdi/config.py",
               "benchmarks/flight_list_v62.py"]
```

Update the two vote-cache `Stage` entries to build with
`benchmarks/trend_sweep_agl.py` into the `_agl` prefixes — copy the stage
definitions from `regenerate_v61.py`, which already do exactly this.

- [ ] **Step 5: Edit `regenerate_v62.py` — the job list**

Delete `trend_sweep_2025` and `trend_sweep_2024`. Copy the four sweep jobs and
the two datum jobs from `regenerate_v61.py`'s `jobs()`: `height_sweep_2025`,
`fl_sweep_2025`, `height_sweep_2024`, `fl_sweep_2024`, `datum_swap_2025`,
`elevation_bands_2025`.

Then in each surviving `flight_list_v6.py` job, change the script to
`benchmarks/flight_list_v62.py` and repoint `--trend-sweep`:

```python
             "--trend-sweep", str(DATA / "height_sweep_2025.csv"),
```

Update `modes`, `trend_grid`, `pipeline_path`, `pipeline_path_ring`. Rename
`modes` → `modes_2025`, `trend_grid` → `trend_grid_2025`, `pipeline_path` →
`pipeline_path_2025`, `pipeline_path_ring` → `pipeline_path_ring_2025` so the
2025-only rule in the tests reads directly off the names.

`trend_grid_2025`'s runs and grid arguments become:

```python
             "--runs", *[f"grid_h{c}_r{r:g}_m2"
                         for c in flight_list_v62.GRID_HEIGHT_CAPS
                         for r in (20, 30)],
             "--grid-height", *[str(c) for c in flight_list_v62.GRID_HEIGHT_CAPS],
             "--grid-radius", "20", "30", "--grid-margin", "2",
```

with `import flight_list_v62` at the top of the module, so the grid is declared
once and the two lists cannot drift.

`pipeline_path_2025` and `pipeline_path_ring_2025` gain the fifth rung:

```python
             "--runs", "path0_legacy", "path1_penalty", "path2_ceiling",
             "path3_margin", "path4_radius", "path5_datum",
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `.venv310/bin/python -m pytest tests/test_regenerate_v62.py -v`
Expected: PASS.

- [ ] **Step 7: Run the whole suite**

Run: `.venv310/bin/python -m pytest tests/ -q`
Expected: all green. `tests/test_regenerate_v61.py` still passes — V6.1 is not
deleted until Task 7.

- [ ] **Step 8: Commit**

```bash
git add benchmarks/regenerate_v62.py tests/test_regenerate_v62.py
git commit -m "Add the v6.2 job registry

Forks regenerate_v6.py and absorbs v6.1's jobs. V6's two trend_sweep jobs
are retired rather than re-run: trend_sweep_agl.py is a strict superset,
and its MSL arm reproduces trend_sweep.py cell for cell on both periods --
371 shared grid points, zero differences. The pipeline arms read the
field-datum sweep, walk a fifth tuning rung that isolates the datum, and
grid the ceiling in feet above field."
```

---

### Task 3: Seed the data directory and verify staleness without a cluster

The point of this task is to reach a state where `--check` tells the truth, so
the campaign in Task 4 runs exactly the jobs that need running and no others.

**Files:**
- Create: `../opdi-portal/papers/adep-ades-detection-v6.2/data/`

- [ ] **Step 1: Copy the carried-over outputs and their provenance**

The seven current outputs cost no cluster time, but only if their manifest
entries come with them. Copy the CSVs *and* merge the matching `_manifest.json`
entries from both V6 and V6.1.

```bash
cd /home/jupyter/work/opdi-workspace/opdi-portal/papers
mkdir -p adep-ades-detection-v6.2/data
cp adep-ades-detection-v6.1/data/height_sweep_2025.csv \
   adep-ades-detection-v6.1/data/fl_sweep_2025.csv \
   adep-ades-detection-v6.1/data/height_sweep_2024.csv \
   adep-ades-detection-v6.1/data/fl_sweep_2024.csv \
   adep-ades-detection-v6.1/data/datum_swap_2025.csv \
   adep-ades-detection-v6.1/data/per_airport_datum_2025.csv \
   adep-ades-detection-v6.1/data/elevation_bands_2025.csv \
   adep-ades-detection-v6.1/data/elevation_per_airport_2025.csv \
   adep-ades-detection-v6/data/sampler_comparison_v6.csv \
   adep-ades-detection-v6.2/data/
```

- [ ] **Step 2: Merge the manifest entries**

Write `/home/jupyter/.claude/jobs/238a706b/tmp/seed_manifest.py`:

```python
"""Carry provenance across with the files it describes.

A copied CSV with no manifest entry is reported in the paper as unverified.
A copied CSV whose entry was *regenerated* here would be a lie -- it would
claim this run produced it. So the entries are moved verbatim, keyed by the
same filename, from whichever paper actually produced them.
"""
import json
from pathlib import Path

P = Path("/home/jupyter/work/opdi-workspace/opdi-portal/papers")
dst = P / "adep-ades-detection-v6.2" / "data" / "_manifest.json"
out = json.loads(dst.read_text()) if dst.is_file() else {}

FROM_V61 = ["height_sweep_2025.csv", "fl_sweep_2025.csv",
            "height_sweep_2024.csv", "fl_sweep_2024.csv",
            "datum_swap_2025.csv", "per_airport_datum_2025.csv",
            "elevation_bands_2025.csv", "elevation_per_airport_2025.csv"]
FROM_V6 = ["sampler_comparison_v6.csv"]

for paper, names in (("adep-ades-detection-v6.1", FROM_V61),
                     ("adep-ades-detection-v6", FROM_V6)):
    src = json.loads((P / paper / "data" / "_manifest.json").read_text())
    for n in names:
        if n not in src:
            raise SystemExit(f"{n} has no manifest entry in {paper}")
        out[n] = src[n]
    # Table identities travel too: without them every job that reads a table
    # reports "no provenance recorded" and rebuilds a stage it need not.
    for k, v in src.items():
        if k.startswith("table:"):
            out.setdefault(k, v)

dst.write_text(json.dumps(out, indent=2, sort_keys=True))
print(f"wrote {len(out)} entries to {dst}")
```

Run: `python3 /home/jupyter/.claude/jobs/238a706b/tmp/seed_manifest.py`

- [ ] **Step 3: Check staleness**

Run:
```bash
cd /home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/v62-combined
.venv310/bin/python benchmarks/regenerate_v62.py --check
```

Expected: the seven carried-over jobs report `current`; the ten to run report
`STALE`. If a carried-over job reports stale, **stop and diagnose** — a
fingerprint mismatch here means the code list in Task 2 differs from V6.1's, and
running the campaign would silently recompute things that did not need it.

- [ ] **Step 4: Commit the seed**

```bash
cd /home/jupyter/work/opdi-workspace/opdi-portal
git add papers/adep-ades-detection-v6.2/data
git commit -m "Seed v6.2 with the outputs v6.1 and v6 already verified

Provenance travels with the files rather than being regenerated, so the
entries still name the run that actually produced them."
```

---

### Task 4: Run the cluster campaign

**Files:** none created; this task fills `papers/adep-ades-detection-v6.2/data/`.

**Preconditions:** Tasks 1–3 committed, `--check` reporting exactly ten stale jobs.

- [ ] **Step 1: Wait for the cluster to go quiet**

Another job is running `benchmarks/track_sweep.py`. The executor pool is shared;
starting now starves both. Watch for *any* `benchmarks/` driver — that job cycles
through scripts with gaps between them, so a single-name watcher catches a gap
and launches into contention. Require a sustained quiet window.

Write `/home/jupyter/.claude/jobs/238a706b/tmp/wait_and_run_v62.sh`:

```bash
#!/usr/bin/env bash
# Launch the v6.2 campaign once the shared cluster is genuinely idle.
#
# `pgrep -f` would match this script's own command line and report "busy"
# forever, so the predicate uses `ps` and excludes our own driver by name.
REPO=/home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/v62-combined
PY=$REPO/.venv310/bin/python
LOG=/home/jupyter/.claude/jobs/238a706b/tmp/v62_campaign.log

others_running() {
  ps -eo cmd= \
    | grep -E "\.venv310/bin/python +-?u? *benchmarks/" \
    | grep -v "regenerate_v62.py" \
    | grep -v grep \
    | grep -q .
}

: > "$LOG"
clear_count=0
launched=0
for _ in $(seq 1 1200); do
  if others_running; then
    clear_count=0
  else
    clear_count=$((clear_count + 1))
    [ "$clear_count" -ge 10 ] && { launched=1; break; }   # five minutes quiet
  fi
  sleep 30
done

if [ "$launched" -ne 1 ]; then
  echo "cluster never went quiet; not launching" >> "$LOG"
  echo "exit=99" >> "$LOG"
  exit 99
fi

echo "launching v6.2 campaign ($(date -u +%H:%M:%S))" >> "$LOG"
cd "$REPO" || exit 1
"$PY" -u benchmarks/regenerate_v62.py >> "$LOG" 2>&1
echo "exit=$? ($(date -u +%H:%M:%S))" >> "$LOG"
```

Test the predicate before trusting it:
`bash -c 'source <the function>; others_running && echo BUSY || echo IDLE'`
must print `BUSY` now and `IDLE` when nothing else runs.

- [ ] **Step 2: Launch in the background**

```bash
chmod +x /home/jupyter/.claude/jobs/238a706b/tmp/wait_and_run_v62.sh
```

Run it with `run_in_background: true`. Do **not** poll with `sleep N; check` in
the foreground — a backgrounded `sleep` returns immediately and reports elapsed
time that has not passed.

- [ ] **Step 3: Verify each job as it lands**

Expected order and rough character:

| Job | Expectation |
|---|---|
| `endpoint_sweeps` | numbers **identical** to V6's `sweep_*_2025.csv` |
| `endpoint_sweeps_2024` | identical to V6's `sweep_radius_height_2024.csv` |
| `bearing` | identical to V6's `bearing_whole_sample_v6.csv` |
| `vertical_measure` | identical to V6's `vertical_measure_v6.csv` |
| `merge_diagnosis` | may differ — `research/reference` grew |
| `modes_2025` | differs — this is the datum change |
| `trend_grid_2025` | new grid, in feet above field |
| `pipeline_path_2025` | six rungs now |
| `pipeline_path_ring_2025` | six rungs now |
| `trend_bearing` | differs |

The four "identical" rows are a **verification, not a formality**: they are the
direct check that the datum change leaves the endpoint path untouched, which
V6.1 could only infer from departures scoring 0 in every band. If any of them
differs, stop — the change reaches further than the study claims, and the paper's
central argument needs revisiting before anything is written.

Compare with:

```bash
python3 - <<'PY'
import csv
from pathlib import Path
P = Path("/home/jupyter/work/opdi-workspace/opdi-portal/papers")
for name in ("sweep_radius_height_2025.csv", "sweep_penalty_2025.csv",
             "sweep_cone_2025.csv", "sweep_radius_height_2024.csv",
             "bearing_whole_sample_v6.csv", "vertical_measure_v6.csv"):
    a = list(csv.DictReader(open(P / "adep-ades-detection-v6" / "data" / name)))
    b = list(csv.DictReader(open(P / "adep-ades-detection-v6.2" / "data" / name)))
    print(name, "IDENTICAL" if a == b else f"DIFFERS ({len(a)} vs {len(b)} rows)")
PY
```

- [ ] **Step 4: Confirm everything is current**

Run: `.venv310/bin/python benchmarks/regenerate_v62.py --check`
Expected: `all outputs current`.

- [ ] **Step 5: Commit the data**

```bash
cd /home/jupyter/work/opdi-workspace/opdi-portal
git add papers/adep-ades-detection-v6.2/data
git commit -m "Record the v6.2 campaign on the field-elevation datum"
```

---

### Task 5: Write the merged paper

**Files:**
- Create: `../opdi-portal/papers/adep-ades-detection-v6.2/index.qmd`

**Interfaces:**
- Consumes: every CSV in `papers/adep-ades-detection-v6.2/data/`.
- Produces: nothing other code reads.

- [ ] **Step 1: Start from V6, not from scratch**

```bash
cd /home/jupyter/work/opdi-workspace/opdi-portal/papers
cp adep-ades-detection-v6/index.qmd adep-ades-detection-v6.2/index.qmd
```

Point the regeneration chunk at `regenerate_v62.py` and update the header
comment to describe V6.2.

- [ ] **Step 2: Carry over the mermaid helper**

Copy the `mermaid()` knitr helper verbatim from
`adep-ades-detection-v6.1/index.qmd` (its setup chunk), including the comment
explaining why it is **not** `::: {.content-visible when-format="html"}`: that
div hides the block only *after* Quarto has tried to rasterise it, rasterising
mermaid needs headless Chromium, Chromium does not start in this environment,
and the PDF render then hangs forever with defunct `chrome-headless` children
and no error. Emitting the fence from knitr means the block never exists in a
PDF render, so nothing tries to draw it.

- [ ] **Step 3: Place the four diagrams in `sec-methods`**

Move all four diagram blocks from V6.1 (`trend` arrivals, `trend` departures,
`endpoint` departures, `endpoint` arrivals) into V6's `## The three methods
{#sec-methods}` section, each after the prose describing that method. This is
where the diagrams were originally asked for: "each step explained early in the
report and shortly addressed with the diagram". Each diagram keeps its
accompanying table, which is what a PDF render shows instead.

- [ ] **Step 4: Insert the datum chapter after `sec-trend-sweep`**

Carry over from V6.1, in order: "A flight level is not a height", "The band
breakdown, read first" (`#sec-bands`), "What it costs", "Why it ships anyway"
(`#sec-adoption`), "The band that should have gained most, and did not", "The
ceiling survives the move" (`#sec-ceiling`), "And it replicates on the second
period" (`#sec-second-period`), "Why the aggregate was never going to show much"
(`#sec-dilution`).

Drop V6.1's "What this version changes" and "What this change cannot do" as
standalone top-level sections — in V6.2 the datum is not "what this version
changes", it is one of the parameters the study sets. Fold their content into
the chapter's opening paragraphs and into `#sec-adoption`.

- [ ] **Step 5: Add the sweep-equality cross-check**

New subsection in the datum chapter. State it as measured: comparing V6's
`trend_sweep_2025.csv` against the MSL arm of the paired sweep on the join key
`(stage, stage2_role, fl_cap, radius_nm, penalty_nm, margin, k, legacy)` gives
371 shared cells and zero differing cells, and the same on 2024. This is what
licenses retiring two jobs rather than re-running them, and it is also evidence
the AGL harness did not perturb the measurement it was built to extend.

- [ ] **Step 6: Add the endpoint-path verification**

In the datum chapter, report the four re-run jobs that returned identical
numbers. V6.1 could only *infer* that departures were untouched, from departures
scoring 0 in every elevation band. V6.2 verifies it directly: the endpoint
sweeps, the bearing test and the vertical measures were recomputed under the new
code and did not move.

- [ ] **Step 7: Extend "Which change is actually doing the work"**

V6's ladder gains `path5_datum`. Report what the fifth rung is worth on its own,
from `pipeline_path_v6.csv`, on both the `ring` and `haversine` ranking rules.

- [ ] **Step 8: Settle the 6,000 vs 6,100 question**

New subsection in `sec-pipeline`, reading `trend_grid_v6.csv`. The shipped
default is 6,000 ft; the sweep called 6,100 optimal on a grid that never
contained 6,000. Report which the pipeline prefers and state the consequence
plainly — either a recommendation to move the shipped default, or a statement
that the two are within noise and 6,000 stands. Do not assert the ceiling is
tuned without pointing at this table.

- [ ] **Step 9: Update `sec-defaults`, `sec-verdict` and the summary**

`## What ships` must list the field datum and the ceiling as shipped values.
The summary's headline numbers are computed in the setup chunk from the tables,
not written into the prose, so they follow automatically — but re-read the
opening paragraph against them, since V6's prose describes a flight-level cut.

- [ ] **Step 10: Update `sec-limitations`**

Remove the limitation that the trend cut ignores field elevation — it is fixed.
Add what remains open: the `>3000` elevation band, where 5 arrivals of 705 are
correct on *both* datums, which is a coverage problem upstream on the Anatolian
plateau and not a datum problem; and `height_pipeline_2025`, now subsumed by the
pipeline grid.

- [ ] **Step 11: Render**

```bash
cd /home/jupyter/work/opdi-workspace/opdi-portal/papers
OPDI_RENDER=check timeout 1800 quarto render adep-ades-detection-v6.2
```

`OPDI_RENDER=check` fails fast if anything is stale, which is what you want here
— a render that silently recomputes is a render whose numbers you did not watch.

- [ ] **Step 12: Read the rendered output**

Open the HTML and read it end to end. Specifically check: no section references
a figure that is not there; the provenance table lists every CSV with no
"unverified" rows; the four diagrams render; the arithmetic in the summary
agrees with the tables it claims to summarise.

- [ ] **Step 13: Commit**

```bash
cd /home/jupyter/work/opdi-workspace/opdi-portal
git add papers/adep-ades-detection-v6.2
git commit -m "Write the v6.2 report

V6's parameterisation study with every number recomputed on the shipped
field-elevation datum, the datum study folded in as a chapter of its own,
and the four methodology diagrams placed where the methods are introduced."
```

---

### Task 6: Verify V6 still renders

V6 is the historical record and must stay reproducible. Forking is only safe if
the fork changed nothing underneath it.

- [ ] **Step 1: Confirm V6's registry is untouched**

```bash
cd /home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/v62-combined
git diff main --stat -- benchmarks/regenerate_v6.py benchmarks/flight_list_v6.py
```

Expected: no output.

- [ ] **Step 2: Confirm V6's data is untouched**

```bash
cd /home/jupyter/work/opdi-workspace/opdi-portal
git status --porcelain papers/adep-ades-detection-v6
```

Expected: no output. If anything shows, a V6.2 job wrote into V6's directory —
revert it and fix the `DATA` path before going further.

- [ ] **Step 3: Render V6 from its committed cache**

```bash
cd /home/jupyter/work/opdi-workspace/opdi-portal/papers
OPDI_RENDER=allow-stale timeout 1800 quarto render adep-ades-detection-v6
```

`allow-stale` is correct here and not a workaround: V6 *is* stale, by design —
the code moved on. Its provenance table will say so, which is the honest record.

---

### Task 7: Retire V6.1

Only after V6.2 renders and reads correctly.

- [ ] **Step 1: Confirm nothing references V6.1**

```bash
cd /home/jupyter/work/opdi-workspace
grep -rn "adep-ades-detection-v6\.1\|regenerate_v61\|flight_list_v61" \
  --include="*.qmd" --include="*.py" --include="*.yml" --include="*.md" \
  opdi opdi-portal | grep -v "papers/adep-ades-detection-v6\.1/" \
  | grep -v "docs/superpowers/"
```

Expected: only `tests/test_regenerate_v61.py`, which goes in Step 2.

- [ ] **Step 2: Delete**

```bash
cd /home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/v62-combined
git rm benchmarks/regenerate_v61.py benchmarks/flight_list_v61.py \
       tests/test_regenerate_v61.py
cd /home/jupyter/work/opdi-workspace/opdi-portal
git rm -r papers/adep-ades-detection-v6.1
```

`benchmarks/trend_sweep_agl.py`, `benchmarks/elevation_bands.py`,
`benchmarks/elevation_arms.py` and `benchmarks/elevation_census.py` **stay** —
V6.2 runs them. So do `tests/test_trend_sweep_agl.py`,
`tests/test_elevation_bands.py` and `tests/test_elevation_arms.py`.

- [ ] **Step 3: Run the suite**

Run: `.venv310/bin/python -m pytest tests/ -q`
Expected: green, with `test_regenerate_v61.py`'s cases gone and
`test_regenerate_v62.py`'s in their place.

- [ ] **Step 4: Update `_quarto.yml` if it lists papers**

```bash
grep -n "adep-ades" /home/jupyter/work/opdi-workspace/opdi-portal/_quarto.yml
```

Replace any `v6.1` entry with `v6.2`. If no entry exists, nothing to do.

- [ ] **Step 5: Commit both repos**

```bash
cd /home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/v62-combined
git commit -m "Retire the v6.1 harness, superseded by v6.2

The AGL sweeps, the elevation banding and their tests stay -- v6.2 runs
them. Only the v6.1-specific registry and pipeline runner go."

cd /home/jupyter/work/opdi-workspace/opdi-portal
git commit -m "Retire the v6.1 paper, superseded by v6.2

Its content is carried into v6.2 in full. It was never pushed, so nothing
published is superseded and no link breaks."
```

---

### Task 8: Merge and record

- [ ] **Step 1: Push the branch and merge `opdi/` to main**

```bash
cd /home/jupyter/work/opdi-workspace/opdi/.claude/worktrees/v62-combined
.venv310/bin/python -m pytest tests/ -q          # green before merging
git push -u origin v62-combined
```

Then merge to `main` from the main checkout (never force-push, never push to
main directly without the merge):

```bash
cd /home/jupyter/work/opdi-workspace/opdi
git checkout main && git merge --no-ff v62-combined && git push origin main
```

- [ ] **Step 2: Commit `opdi-portal` locally — do NOT push**

`opdi-portal` is explicitly not pushed. Its commits stay local.

- [ ] **Step 3: Update the meta-repo submodule pointers**

```bash
cd /home/jupyter/work/opdi-workspace
git add opdi opdi-portal
git commit -m "Point at the v6.2 report and its harness"
```

- [ ] **Step 4: Write run notes**

Create `benchmarks/V62_RUN_NOTES.md` recording: which jobs ran and how long they
took; the four that returned identical numbers and what that verified; the
6,000-vs-6,100 outcome; anything that failed and why. Commit it.

- [ ] **Step 5: Clean up the worktree**

Only once `git rev-list --count main..v62-combined` is `0`:

```bash
cd /home/jupyter/work/opdi-workspace/opdi
git worktree remove .claude/worktrees/v62-combined
git branch -d v62-combined
```

---

## Self-Review

**Spec coverage.** The design had five parts: fork the harness (Tasks 1–2),
re-run on the shipped datum (Task 4), merge the prose with diagrams early
(Task 5), delete V6.1 (Task 7), keep V6 frozen (Task 6). All covered. Two items
surfaced during planning and were added rather than deferred: the fifth ladder
rung isolating the datum (Task 1 Step 6c, reported in Task 5 Step 7), and the
6,000-vs-6,100 question the sweep grid could not answer (Task 1 Step 4, settled
in Task 5 Step 8).

**Placeholders.** None. Every code step carries the code; every check states its
expected output; the two comparison scripts are written out in full.

**Type consistency.** `trend_ceiling_kwargs(row: dict) -> dict` is defined in
Task 1 and consumed in Task 1 Step 6 and Task 2 Step 5.
`GRID_HEIGHT_CAPS` is defined once in `flight_list_v62.py` and imported by
`regenerate_v62.py`, so the run labels and the grid cannot drift.
Run labels: `path0_legacy`, `path1_penalty`, `path2_ceiling`, `path3_margin`,
`path4_radius`, `path5_datum`, and `grid_h{cap}_r{radius}_m{margin}` — used
identically in `flight_list_v62.py` and in `regenerate_v62.py`'s `--runs`.
Job names carry the `_2025` suffix consistently, which the 2025-only test reads.

**Known risk not designed away.** Task 4 depends on a shared cluster going quiet.
If it does not, the campaign does not run and Tasks 5–8 cannot proceed on real
numbers. The watcher exits 99 and says so rather than launching into contention.
