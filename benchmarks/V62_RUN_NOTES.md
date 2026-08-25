# V6.2 run notes

What actually happened while producing `papers/adep-ades-detection-v6.2`, in
the order it happened, including the parts that went wrong. The paper reports
results; this reports the process, which is where the traps are.

## What v6.2 is

V6's parameterisation study with every number recomputed on the datum that
ships — `trend`'s altitude cut as a height above the aerodrome's own field
elevation — with v6.1's datum study and methodology diagrams folded in.

It exists because v6.1 was written as a standalone short report when what was
wanted was an extended v6. v6.1's content is carried into v6.2 in full and
v6.1 is retired.

## Decisions worth knowing

### V6's two `trend_sweep` jobs are retired, not re-run

`trend_sweep_agl.py` is a strict superset of `trend_sweep.py`: identical
`FL_CAPS`, `MARGINS`, `RADII_NM`, `PENALTIES_NM`, `CACHE_RADIUS_NM`, and a vote
cache carrying both `up_fl_*` and `up_agl_*` counts, so one build per period
yields both datums' curves.

That was checked rather than assumed, and the check is now a job
(`sweep_equivalence`). Joining V6's committed `trend_sweep_2025.csv` against
this study's `fl_sweep_2025.csv` on
`(stage, stage2_role, fl_cap, radius_nm, penalty_nm, margin, k, legacy)` gives
**371 shared cells and 0 differing values**, and the same on 2024. Two jobs
were deleted on the strength of that number, so it is measured on every render
rather than asserted here.

### The path walk's rungs 0–4 stay on the sea-level datum

This was a real bug in the plan, caught while writing the code.

The plan said rungs 0–4 remain on MSL automatically because `path_cfg()` starts
from `DetectionConfig.legacy()`. That is wrong: rung 2 applies `**t_ceiling`,
which sets `trend_max_datum` explicitly. Tuned from the above-field sweep, the
datum would move at rung 2 — four rungs before the rung whose entire purpose is
to move it — and `path5_datum` would report **zero**.

A zero there reads as "the datum is worth nothing", which is a finding. It
would have been an artefact of the harness measuring the same config twice.

Fixed by giving the walk `--trend-sweep-msl` for its lower rungs, which has the
side benefit that rungs 0–4 are then the same ladder V6 published and directly
comparable to it. `test_regenerate_v62.py` pins the coupling.

### 6,000 ft against 6,100 ft

`config.py` ships `trend_max_height_ft = 6000.0`. The research sweep's
`HEIGHT_CAPS` are `(2000, 3000, 4000, 6100, 8000, 10000, 12000)` — **6,000 is
not among them**. The grid was built around 6,100 because that is the height
equivalent of `FL60` (`flight_level` is an integer cast, so `<= 60` admits
everything below 6,100), and holding the ceiling fixed was what made the datum
comparison a one-variable change.

The consequence is that "the ceiling is tuned" rested on a grid that never
contained the shipped value. v6.1's prose went further and stated that 6,100 ft
is what ships, which is simply wrong.

v6.2's pipeline grid carries **both** 6,000 and 6,100 and reports which
`process_dai` prefers. This also subsumes v6.1's deliberately-unrun
`height_pipeline_2025`: that job was skipped because it "would confirm a value
nothing is changing", which was true only under the mistaken belief that the
sweep optimum and the shipped value were the same number.

### The datum pair moved out of `flight_list_v61.py`

So that nothing depends on the v6.1 runner and it can be retired. The cost is
that `datum_swap_2025` and `elevation_bands_2025` are stale on a fingerprint
that changed for a reason that is not a change in method — the script moved.
Re-running them is the honest way to clear that; hand-patching the manifest is
the failure `provenance.py` exists to prevent.

Note that the pair and the ladder's fifth rung measure different things and
both are wanted: the pair moves the datum at the **shipped** geometry, rung 5
at the **swept-tuned** geometry. They can disagree.

## What went wrong

### A worktree has no `.env`

Credentials are deliberately never committed, so every benchmark run from
`opdi/.claude/worktrees/<name>` fails at startup with
`No .env at ...; AWS credentials are required.`

Fixed with a symlink to the main checkout's `.env`. `.gitignore:54` covers
`.env`, so the symlink cannot be committed. Copying would have put a second
copy of the secret on disk for no benefit.

Cheap failure — one second, no partial output — but it costs a full launch
cycle each time, which on a contended cluster is not cheap at all.

### `ps` is the wrong way to tell whether the cluster is free

The first watcher polled for other `benchmarks/` drivers and launched after a
sustained quiet window. It reported IDLE and launched into a namespace that was
still full:

```
exceeded quota: eurocontrol-quota,
  requested: limits.cpu=2,limits.memory=13516Mi,
  used:      limits.cpu=30,limits.memory=192092Mi,
  limited:   limits.cpu=30,limits.memory=192Gi
```

**Executor pods outlive the driver process that requested them.** So `ps` goes
quiet while the quota is still pegged, and a watcher built on `ps` is measuring
a proxy that lags the thing it stands for. The run got zero executors and died
in `SparkContext` initialisation after ~25 s of `1 new failed executors`.

Replaced with `wait_quota_run_v62.sh`, which polls
`kubectl describe resourcequota -n eurocontrol` and requires 24 free
`limits.cpu` — 10 executors × 2 CPU plus the driver — sustained over two
minutes. It defaults to *zero free* on any parse failure, so a broken
`kubectl` waits rather than launching blind.

The jupyterlab pod holds 4 CPU permanently, so "completely empty" is not a
state that ever occurs; requiring headroom rather than emptiness is what makes
the check usable.

## Results

All 19 jobs current; 27 CSVs, all traced to a command, none unverified.

### The headline

`recommended` scores **134,132** against `legacy`'s **122,158** across both
roles — a gain of **+11,974** on 95,116 reference flights.

### The datum, at the shipped geometry

`datum_field` against `datum_msl`, arrivals, everything else held: **+194
correct, +105 wrong, score −16**; coverage +0.31 pp, accuracy −0.15 pp. And
**departures +0 correct**, which is the check that matters — `endpoint` serves
that role and the datum must not touch it.

These reproduce v6.1's numbers exactly, which is a real validation: the arms
moved from `flight_list_v61.py` to `flight_list_v62.py` and the port changed
nothing but the file they live in.

### The bands, which are the discriminating measurement

| Band | n | Δ correct | relative | leave-one-out | ex-busiest |
|---|---|---|---|---|---|
| `<500` | 66,612 | +57 | +0.12% | +33 | +57 |
| `500-1500` | 15,285 | +28 | +0.22% | +17 | +28 |
| `1500-3000` | 4,434 | **+109** | **+5.68%** | +70 | +110 |
| `>3000` | 705 | 0 | — | 0 | 0 |
| departures, every band | — | **0** | — | 0 | 0 |

The gradient is what elevation predicts, and the `1500-3000` result survives
both robustness controls: dropping the largest mover still leaves +70, and
dropping the busiest aerodrome leaves +110. Not a Madrid artefact.

### 6,000 ft against 6,100 ft — settled, in favour of what ships

Through `process_dai`, best arrival score at **6,000 ft is 61,698** and at
**6,100 ft is 61,654**. 6,000 wins at both radii (+40 at 20 NM, +44 at 30 NM).
Inside sampling noise, so the conclusion is that the shipped value stands, not
that 6,100 is beaten — but the sweep's preference for 6,100 does not survive
contact with the pipeline, which is the pattern V6 found for its FL cap.

This is the check v6.1 deliberately skipped, on the belief that 6,100 was
already shipping. It was not.

### The tuning ladder, with the datum as its fifth rung

| Rung | Exact distance | Ring selection |
|---|---|---|
| `path0_legacy` | 59,884 | 60,262 |
| `path1_penalty` | +825 | +279 |
| `path2_ceiling` | +791 | **−1,642** |
| `path3_margin` | +160 | +124 |
| `path4_radius` | −193 | −64 |
| `path5_datum` | **+11** | **−187** |

The datum rung is worth **+11** under exact distance — negligible, and exactly
what @sec-dilution predicts, since 70.0% of arrivals sit in the `<500` band
where the two datums are the same test. The ladder also reproduces V6's central
result independently: the ceiling step is +791 under exact distance and −1,642
under ring, same parameter and same data with opposite signs.

Rungs 0–4 sit on the sea-level datum and rung 5 is the only one that moves it.
That is the design working; see the plan-bug note above for what the
alternative would have reported.

## What this study found about earlier versions

Two claims in V6's published report do not hold, and both were found by
computing what V6 had typed.

**`trend_radius_nm` does not ship at 20 NM.** V6's "What ships" table says it
does; `DetectionConfig()` has **30**. The recommendation was made and never
applied, and every later version copied the claim rather than measuring it.
V6.2's table is generated from the `recommended` run — a bare
`DetectionConfig()` — so it cannot drift again. `trend_vote_margin` is
similarly **0**, not the 2 V6 records.

**Arrivals do not prefer the tighter radius at every cell.** V6 reports that
both roles do, and cites it as the stronger evidence for lowering the radius.
On the shipped datum arrivals prefer the *wider* 30 NM at every ceiling from
6,000 ft up — which includes the ceiling that ships. So the un-applied
recommendation turned out to be the right thing not to apply.

Neither is corrected in `config.py` here. Changing a published default is the
maintainers' decision; this study's job was to make the gap visible.

## Input drift, and a verification withdrawn

`opdi_endpoint_candidates` was rebuilt on 2026-08-22, after V6 published:
12% smaller, fresh-broadcast share roughly tripled (0.10 → 0.35). `cand_2024`,
`research/reference` and both vote caches also moved. Only
`h3_airport_detection_zones` and `osn_tracks` are marker-only — byte-identical
with the same mtime.

**Ground truth did not move for this study.** `research/reference` grew by
gaining other periods; the three-day slice still carries 95,116 and 92,799
reference flights, the same counts V6 reported, and `sweep_equivalence`
confirms it independently at 371 cells with zero differences on each period.

The cost is that the endpoint family's numbers are no longer comparable to
V6's, so the intended *direct* verification that the datum leaves departures
alone is unavailable and is not claimed. The within-study evidence — departures
+0 in every band, both arms on the same candidate table — stands in its place
and is reported as the weaker thing it is.

## Not run

Nothing deliberately skipped. `height_pipeline_2025` is not carried over from
v6.1 as a separate job because `trend_grid_2025` subsumes it — see the 6,000
against 6,100 note above.

## Diagram rendering without a browser

The four methodology diagrams are HTML-only: `mermaid()` emits its fence only
under `knitr::is_html_output()`, because rasterising mermaid needs a headless
Chromium that does not start here. A spike asked whether one source could serve
both formats using only what is installed.

| Path | Result |
|---|---|
| `DiagrammeR` viz.js via `V8`, then `rsvg` | **fails** — this DiagrammeR build ships no `viz.js` at all (0 candidates found) |
| `igraph` drawn to a PDF device | **works** — valid 1-page PDF |
| `grid` boxes and arrows | **works** — valid 1-page PDF |

No Chromium process was spawned: every `chrome-headless` entry on the host
remained 3, 11 or 12 days old.

**So it is feasible, and the recommended path is `grid`.** An R chunk that
draws with `grid` renders through whatever device Quarto has selected, so the
same source produces a figure in HTML *and* PDF with no browser and no
pre-rendered image to keep in step.

`igraph` also works but fights the content: these are flowcharts with
multi-line box labels, a decision diamond and yes/no edge labels, and igraph's
strengths are graph layout rather than annotated boxes.

**Practical shape:** all four diagrams are vertical chains with a single
branch, so one helper that lays out a chain of labelled boxes serves all four
rather than four hand-placed drawings. That is what makes the approach
affordable; without it, hand-laying four flowcharts in `grid` would not be
worth the gain over the existing step tables.

## The pipeline is not bit-reproducible at the margin

Re-running the campaign against main's package reproduced every figure exactly
**except** the `recommended` row of `mode_comparison_v6.csv`, where 5 arrivals
of 95,116 moved from correct to wrong -- score 62,147 to 62,132.

It is not a difference between main and the branch. The evidence:

* the recorded configuration is identical in both rows (6,000 ft, 30 NM,
  margin 0, `field`, `haversine`);
* every other run -- `legacy`, `trend`, `endpoint`, `nearest`, `combined` --
  reproduces cell for cell, and a behavioural change in shared code would not
  spare five of six runs;
* the movements pair up between **adjacent aerodromes**: EDDF loses one and
  EDFE gains one, and Frankfurt and Egelsbach are about 10 km apart. LICB,
  LIMC and LIMJ move the same way.

That is a **tie in the ranking broken differently between runs**. Two candidate
aerodromes at the same effective distance -- after the scheduled-service
penalty -- leave the winner to whatever order Spark happened to produce.

`recommended` is the only trend run at the full 30 NM radius, so it admits the
most candidates and meets the most near-ties. The narrower runs do not show it.

**What this bounds.** Every pipeline figure in the report carries roughly
±5 flights of run-to-run noise, or about 0.005% of the sample. That is far
below any effect the report argues from -- the datum's band result is +109 --
but it means these numbers should not be quoted to the last digit as though
they were exact, and a difference of a handful of flights between two
configurations is not a difference at all.

### The datum rung is inside the noise

Two independent runs of the tuning ladder put the datum rung at **+11** and
**+8**, against run-to-run noise of roughly ±15 score points (5 flights moving
between correct and wrong costs 15 at *k* = 2).

So the rung is **not measurably different from zero**, and the report says that
rather than quoting a figure to the unit.

This is not a weakness in the argument; it is what @sec-dilution predicts.
Seventy per cent of arrivals are at aerodromes below 500 ft, where the two
datums are the same test by construction, so an aggregate over the whole
sample cannot see a change that only acts on the rest. The case for the datum
rests on the band breakdown -- +109 correct in the 1,500-3,000 ft band,
surviving both the leave-one-out and drop-the-busiest controls -- and on the
argument that a cut should mean the same thing at every aerodrome.

The ladder's other rungs are far larger than the noise and unaffected: the
penalty is +825, the ceiling +791, and the ceiling step still flips sign
between ranking rules (+791 under exact distance, -1,642 under ring).
