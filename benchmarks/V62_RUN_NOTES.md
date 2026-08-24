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

<!-- Filled in when the campaign completes. Deliberately empty rather than
     populated with numbers carried over from v6.1: the whole point of the
     campaign is that the pipeline arms are recomputed on the shipped datum,
     and quoting the old ones here would defeat it. -->

## Not run

Nothing deliberately skipped. `height_pipeline_2025` is not carried over from
v6.1 as a separate job because `trend_grid_2025` subsumes it — see the 6,000
against 6,100 note above.
