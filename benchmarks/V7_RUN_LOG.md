# V7 run log

Written as the run proceeds, so its state is legible without reading a Spark
log, and so every decision made without review is visible afterwards.

Updated roughly every 25 minutes while the chain is running.

---

## What is being run

```
python benchmarks/regenerate_v7.py --with-stages
```

Everything from trajectory cleaning through both periods: caches, four research
sweeps, two thirteen-rung ladders, two mode comparisons and two flight-level
grids — all through `FlightListProcessor.process_dai`, the code path that writes
the published flight list.

Roughly 66 pipeline runs. Estimated 15–20 hours.

## What changed in the code first

Five changes, each behind a `DetectionConfig` field that `DetectionConfig.legacy()`
turns off, so every published flight list stays reproducible by name.

| Field | Ships as | What it does |
|---|---|---|
| `trend_rank_by` | `haversine` | Rank candidates on exact distance rather than H3 ring count. The enabling change: under ring selection a tuned flight-level cap *loses* ground and under exact distance the same value gains it. |
| `trend_radius_exact` | `True` | Cut the detection radius on distance rather than on hexagon band. Selecting bands by outer radius discarded hexagons straddling the boundary, and with them samples genuinely inside it. |
| `trend_smooth_before_cut` | `True` | Smooth barometric altitude before the flight-level cut, not after. The cut used to truncate the smoothing window exactly where altitude changes fastest. |
| `trend_bearing_tiebreak_nm` | `2.0` | Among candidates whose distances differ by less than 2 NM, prefer the one the track's course points at. |
| `trend_ooa` | `True` | Let `trend` emit the out-of-area marker. Arrivals ship from `trend`, and about one arrival in twelve genuinely originates outside the observed area — every one of them used to be a null indistinguishable from a detection failure. |

Plus two that are not thresholds:

* **`adep_mode` / `ades_mode` moved into the config**, defaulting to `endpoint`
  for departures and `trend` for arrivals. Before this, `process_dai` fell back
  to the literal `"trend"` for both roles, so the CLI and every other entry
  point that did not name the modes ran a configuration the study recommends for
  neither. The thresholds were tuned and the algorithm choice was not applied.
* **The flight list reads `osn_tracks_clean`.** Step 02a wrote a table nothing
  consumed; cleaning existed, was tested, and fed nothing.

New version string: **`v4.0.0`**, one for the whole flight list. Before it the
trend path stamped `v2.0.0` and the endpoint path `v3.0.0` and the merge
coalesced them, so a merged list carried whichever version the departure side
happened to have.

## Decisions taken without review

| # | Decision | Why |
|---|---|---|
| 1 | The out-of-area join is filtered back to rows naming at least one end | The join must be an outer one, because a flight that crossed the area without descending near an aerodrome has no row in the trend table and is exactly the flight the label exists for. Unfiltered it also admits every other unmatched track — thousands of rows naming nothing at either end, which would not fail but would change the denominator of every per-aerodrome count. |
| 2 | `clean_tracks.py` forces the first write of a rebuild to overwrite | `cleaner.py` writes with the default mode, which is append. Correct for the daily pipeline, where each run adds a month the table does not have; exactly wrong for a rebuild of the same month, which would double the table. A doubled table does not fail — it silently weights every aggregate that reads it. |
| 3 | The pipeline flight-level grid runs at one radius, not two | Fourteen caps at two radii is fifty-six full `process_dai` runs across the two periods. The radius is already answered twice over — by the research sweep on both periods, and by the ladder, which walks it as its own rung. The cap is the parameter that needed the pipeline, because it is the one version 6 measured through a grid that stopped before the curve turned. |
| 4 | **The sampler is not re-measured in V7** | There is no fixed-phase arm left to compare against, and these sweeps run on cleaned tracks over a different flight-level grid — so a comparison against version 6's arm would be confounded by the cleaning and the grid rather than measuring the sampler. Two arms on different data is precisely the failure this study keeps producing. V7 cites version 6's measurement and says it is cited. |
| 5 | Per-role algorithm lives in the configuration, not beside it | An earlier draft carried the roles alongside the detection config, and `verify_plan` caught the two disagreeing on the very first run. One source of truth rather than two. |

## Bugs caught before the run

* **The ladder did not end at the shipped configuration.** `verify_plan` asserts
  that the last rung matches `DetectionConfig()` in its detection settings, its
  per-role algorithms *and* its track table, and it failed — the roles were
  tracked in two places and had diverged. A ladder that ends anywhere else is
  not a decomposition of the change being made, and nothing in the output would
  have said so.
* **Batch S3 deletes fail on this endpoint.** `DeleteObjects` needs a
  `Content-Md5` header botocore does not send; single-object deletes are
  unaffected. Already documented in `DATASETS.md`, and worth re-reading before
  any cleanup: a failed delete does not look like a failure, and the next run
  quietly reuses the data you were trying to replace.

## Storage

Deleted 2026-08-12 **with approval**, freeing 16.51 GB:

* `research/sv_bucket`, `research/tracks_bucket`, `research/cand_bucket`
  (6.83 GB) — the decimation study's experimental arm, redundant since the
  bin-based sampler became the production default, which makes `osn_tracks`
  itself the bucket-sampled table.
* `osn_statevectors_v2` (9.68 GB) — step 01's output, consumed only by step 02,
  which is complete, and regenerable from the OpenSky archive.

Bucket after: **67.40 GB**, of which 42.81 GB is `osn_symposium_paper_2026` and
belongs to another project. OPDI's own footprint is about 24 GB. The two cleaned
track tables add roughly 22 GB, landing near 89 GB against a ~100 GB quota.

## Progress

### 2026-08-12 13:03 — 16:37 · step 02a, 2025

`osn_tracks_clean` written: **202 objects, 9.86 GB**. About 3.5 hours, most of
it the shuffle behind `repartition("track_id")`.

### 2026-08-12 16:39 — 16:57 · chain started, then stopped and restarted

The chain began, recorded the three upstream tables that were already present,
and started cleaning the 2024 period. **Stopped after 18 minutes and
relaunched**, for a defect that would not have surfaced for another eight hours.

**What was wrong.** The 2024 tracks are a research copy built before step 02
attached an H3 index, so they have no `h3_res_7`. The flight list's `trend` path
joins aerodrome detection zones on exactly that column, so `ladder_2024`,
`modes_2024` and `grid_2024` would every one of them have died on a missing
column — after the ladders and grids for 2025 had already run.

Caught by checking the 2024 schema against what the cleaner and the pipeline
each require, rather than by waiting for it. The fix attaches the index to the
cleaned 2024 output, which makes that table a drop-in for the real detection
code; the 2025 tracks already carry it because step 02 puts it there.

**Two further defects found while fixing it:**

1. **The redirect was applied to the wrong thing.** It mapped the table *name*,
   and `table_ref` registers a Spark temp view named after the table — so the
   cleaner's `SELECT * FROM research/tracks` would not have parsed, a slash not
   being a legal identifier. It now redirects `_s3_path` instead, which leaves
   the view named `osn_tracks` while reading the 2024 copy, and covers reads,
   writes and schema probes in one place rather than three that can disagree.
2. **An interrupted write looked like a finished table.** Killing the chain left
   the magic committer's `.pendingset` markers behind: 34 objects, 0.14 MB. A
   stage checks presence by object count, so on restart it would have *recorded*
   that wreckage as the 2024 clean tracks and moved on. Nothing would have
   failed; every 2024 figure would simply have been computed from a table that
   was not there. Stages now require a table to exceed 1 MB before they will
   believe in it, and the restart correctly reports the leftovers as
   "committer scaffolding from an interrupted write, not a table".

Nothing real was lost: no data had been committed, only markers.

### Checkpoints

Appended every 25 minutes. "Stage 6" is the shuffle-and-write behind
`repartition("track_id")`, which is where cleaning spends most of its time.

| UTC | Phase | Detail |
|---|---|---|
| 17:10 | 02a, 2024 | relaunched with the H3 fix |
| 17:35 | 02a, 2024 | stage 6 at 30/200 |
| 18:05 | 02a, 2024 | stage 6 at 70/200 |
| 18:30 | 02a, 2024 | stage 6 at 95/200 |
| 19:22 | 02a, 2024 | stage 6 at 169/200 |
| 19:57 | 02a, 2024 | 195/200, long tail begins |
| 20:32 | 02a, 2024 | 199/200, no failures, executors 3h21m with no restarts |
| 20:50 | 02a, 2024 | **done**, 202 objects, 6.17 GB |
| 20:50 | candidates | both periods built |
| 20:50 | vote cache 2025 | started |
| 21:18 | both vote caches | built, all 14 caps |
| 21:18 | trend_sweep_2025 | **staged** — 560 cells |
| 21:47 | trend_sweep_2024 | staged |
| 21:47 | endpoint_sweep_2025 | staged (radius x height, penalty) |
| 21:41 | endpoint_sweep_2024 | staged — all five sweeps done |
| 21:41 | ladder_2025 | **failed on rung 1**, fixed, chain resumed 22:15 |

### 2026-08-12 16:58 · chain relaunched

### 2026-08-12 17:35 · two more cross-period bugs, fixed while the cleaning runs

Both would have hit the 2024 half only, and both are the silent kind.

1. **The 2024 candidate builder read the 2025 tracks.** Its redirect mapped the
   name `osn_tracks`, and the processor stopped asking for that the moment it
   began resolving its track table from the configuration -- it now asks for
   `osn_tracks_clean`, so the mapping never fired and the read fell through to
   the 2025 table. The 2024 candidates would have been built from 2025 data
   with every table still saying 2024. The track source is now passed to the
   processor explicitly and the redirect covers the write alone.
2. **The 2024 builder read *raw* tracks.** `TRACKS_2024` pointed at
   `research/tracks`, so the two periods would have had different input
   treatment while the report claimed they matched. Now `research/tracks_clean`.
3. **`flight_list_v7` on 2024 had no candidate cache to use.** `process_dai`
   builds the cache when the month is missing from its log, and its default
   target is the production table -- which holds 2025, and which the write guard
   would have refused. The period now names its own cache and the redirect
   points reads and writes at it together.

Known and accepted: the 2024 vote-cache stage still passes `--add-h3`, which
recomputes an index the cleaned tracks now carry. It writes the same values, so
it costs some CPU and nothing else. Not worth restarting the chain for.

### Cleaning is expensive, and skewed

Both periods took about three and a half hours for three days of tracks, and
both spent most of it in one stage: the shuffle behind `repartition("track_id")`
in `cleaner.py`. Both also ended with a long tail -- the last five tasks of two
hundred taking as long as the preceding fifty -- which is hash-partition skew,
a few `track_id` buckets carrying far more data than the rest.

Worth knowing before anyone plans a backfill: cleaning a month rather than three
days is not a linear extrapolation of a fast job, it is a linear extrapolation
of a slow one. If it ever needs to be faster, the repartition is the thing to
look at, not the cleaning stages themselves.

Also worth knowing when watching one run: the S3A **magic committer stages every
task's output and commits at the end**, so the target table reads 0.00 GB right
up until the job finishes and then jumps to its full size. An empty target
part-way through a write is the expected state, not evidence of failure.

### First result: what cleaning actually masks

Both periods cleaned, and the NULL rates afterwards are close enough to call the
two identically treated -- which is the point of cleaning the second period at
all.

| Column | 2025 | 2024 |
|---|---|---|
| `baro_altitude` | 21.36% | 20.44% |
| `lat` / `lon` | 17.99% | 17.47% |
| `geo_altitude` | 12.11% | 11.39% |
| `heading` | 44.52% | 43.53% |
| `velocity` | 44.50% | 43.48% |
| `vert_rate` | 44.48% | 43.44% |

Two things follow.

**The prediction that cleaning costs coverage now has a size.** The trend path
drops fixes with no barometric altitude, and cleaning masks about **one fix in
five**. That is not a rounding effect, and the ladder's last rung is where it
gets priced. Whether the accuracy bought is worth it at `k = 2` is exactly the
question the rung answers -- but nobody should be surprised if arrival coverage
falls by a visible margin.

**The velocity columns are nearly half null, and that is correct.** ADS-B sends
position and velocity in separate message types, so identical consecutive
velocity values mean *repeated*, not *measured*. Masking them is the
stale-broadcast rule the PRC challenge work established, and a 44% rate is the
archive's carry-forward padding showing up honestly rather than a defect. It
also explains why every rate-based signal this study has tried -- vertical rate,
closing rate, the approach cone -- has failed where a position-based one worked.

### Candidate caches, both periods

* 2025: built from `osn_tracks_clean`.
* 2024: 306,366 endpoint rows reduced from `research/tracks_clean`, giving
  5,430,694 candidates. The log line `reducing .../research/tracks_clean to
  endpoints` is the fix from 17:35 working -- before it, this would silently
  have read the 2025 tracks.

### The flight-level curve closes

The review's sharpest question was whether the cap should go higher: *"It seems
to be getting better the higher we go."* Followed to FL300 on cleaned tracks, at
vote margin 2 and 20 NM, it does not.

| Cap | Dep coverage | Dep accuracy | Dep score | Arr coverage | Arr accuracy | Arr score |
|---|---|---|---|---|---|---|
| FL25 | 59.92% | 99.43% | 56,019 | 64.34% | 98.67% | 58,757 |
| FL40 | 68.51% | 99.34% | 63,875 | 66.77% | 98.37% | 60,402 |
| FL50 | 70.88% | 99.27% | 65,944 | 67.91% | 98.10% | **60,902** |
| FL60 | 71.91% | 99.21% | 66,766 | 68.56% | 97.77% | 60,844 |
| FL75 | 73.20% | 99.15% | 67,856 | 69.17% | 97.31% | 60,471 |
| FL100 | 75.19% | 99.01% | 69,391 | 70.13% | 95.61% | 57,931 |
| FL125 | 76.77% | 98.62% | **69,995** | 71.07% | 94.07% | 55,578 |
| FL150 | 77.61% | 97.91% | 69,188 | 71.63% | 92.94% | 53,710 |
| FL200 | 78.45% | 95.87% | 65,369 | 72.59% | 90.60% | 49,576 |
| FL250 | 78.96% | 93.66% | 60,819 | 73.27% | 87.71% | 43,995 |
| FL300 | 79.68% | 90.80% | 54,872 | 73.92% | 84.96% | 38,584 |

**Departures peak at FL125 and arrivals at FL50**, and both fall away steeply
after: by FL300 departures have lost 15,123 points from their peak and arrivals
22,318.

The intuition behind the question was right about the mechanism and wrong about
the conclusion. Coverage *does* rise monotonically with the cap, all the way to
FL300 — a higher cap admits more fixes and more fixes mean more answers. But
accuracy falls faster, because a fix taken at FL300 is a long way from whichever
aerodrome turns out to be the answer, and the vote it casts is correspondingly
less informative. The score turns where those two cross.

The shipped arrival cap of FL60 scores 60,844 against FL50's 60,902 — a
difference of 58 flights in 95,116, which is noise. The value is safe.

### Cleaning changed the optimal vote margin, and the radius

Ranking all 560 cells by both periods together -- each period's score divided by
its own reference count -- the shipped arrival cell places **20th of 560**. It is
beaten consistently, on both periods, by two changes.

**The vote margin should be 0, not 2.** At FL60 / 20 NM / 10 NM penalty:

| Margin | 2025 coverage | 2025 accuracy | 2025 score | 2024 score |
|---|---|---|---|---|
| **0** | 68.86% | 97.73% | **61,031** | **52,482** |
| 2 | 68.56% | 97.77% | 60,844 | 52,317 |
| 4 | 68.12% | 97.81% | 60,546 | 52,149 |
| 8 | 67.36% | 97.87% | 59,972 | 51,751 |
| 16 | 65.80% | 97.95% | 58,741 | 50,900 |

Monotonic on both periods: every increase in the margin costs coverage and buys
almost no accuracy -- 2.3 points of coverage from margin 0 to 16, for 0.22 of
accuracy.

**This is an interaction with cleaning, and it is the interesting part.** The
vote margin exists to stop noise flipping a climb/descent call. Cleaning already
removes that noise -- it masks a fifth of barometric altitudes and nearly half
the velocity columns -- so the margin is now defending against something that is
largely gone, and all it does is refuse flights it could have answered. The
value shipped, 2, was tuned on **raw** tracks in version 6, where it was worth
about 150 flights. On cleaned tracks its job has been done for it.

**The radius should be 30 NM, not 20.** An interior optimum on both periods:

| Radius | 2025 score | 2024 score |
|---|---|---|
| 15 NM | 59,417 | 51,190 |
| 20 NM | 60,844 | 52,317 |
| **30 NM** | **60,988** | **52,534** |
| 40 NM | 60,931 | 52,500 |
| 60 NM | 60,828 | 52,371 |

Version 6's joint ranking preferred 30 NM too, and shipped 20 NM on a
single-period argmax. Two periods and a different input treatment now agree.

**Nothing is being changed mid-run.** These are sweep figures, and this study's
rule is that the sweep proposes and the pipeline disposes -- the whole reason
version 6 went wrong was a value adopted on harness evidence that the pipeline
contradicted. The ladder walks both the margin and the radius as their own
rungs, so `ladder_2025` and `ladder_2024` will price exactly what the shipped
values cost. If the pipeline agrees, this is a defaults change for a follow-up
with two periods of evidence behind it, not something to ship tonight by
editing a config while the run that would have tested it is still going.

### 21:41 · the ladder failed on its first rung

```
TypeError: dict.update() got multiple values for keyword argument 'adep_mode'
```

Self-inflicted, and a direct consequence of moving the per-role algorithm into
`DetectionConfig`. The result row is built by naming the run's identity
explicitly and then expanding every detection field:

```python
m.update(run=run, period=..., adep_mode=adep_mode, ades_mode=ades_mode, ...,
         **{f.name: getattr(detection, f.name) for f in fields(detection)})
```

Once `adep_mode` became a *field*, it arrived twice.

Fixed by dropping the explicit pair -- the config is the single source of truth
for them now, which was the point of the move. Verified by reproducing the exact
`update` call without Spark rather than by relaunching and hoping.

**Cost: about six minutes.** It failed on the first rung, before any pipeline
run had finished, and everything ahead of it -- all five sweeps -- was already
staged. Worth saying plainly that a loud crash on rung one is the good version
of this failure: the same collision inside a rarely-taken branch would have run
for hours first.

The chain resumed at 22:15 with the six remaining pipeline jobs. Stages and
sweeps are recorded and current, so no `--with-stages` and no `--rebuild-stage`:
rebuilding a cache that is already right costs an hour and changes nothing.

### The ladder, first six rungs (2025, both roles from `trend`)

| Rung | Dep score | Δ dep | Arr score | Δ arr |
|---|---|---|---|---|
| L00 legacy | 63,064 | — | 60,600 | — |
| L01 + exact-distance ranking | 62,539 | **−525** | 60,255 | **−345** |
| L02 + exact radius cut | 62,512 | −27 | 60,255 | **0** |
| L03 + smooth before the cut | 62,714 | +202 | 60,211 | −44 |
| L04 + scheduled-service penalty | 63,563 | +849 | 61,072 | +861 |
| L05 + flight-level cap FL60 | 67,185 | +3,622 | 61,748 | +676 |

Three things worth stating carefully.

**Exact ranking is an enabler, not an improvement.** On its own, at legacy
thresholds, it *costs* 345 arrivals and 525 departures. Version 6 measured the
same sign at the same point. What it buys is what happens four rungs later: with
exact ranking the flight-level cap gains 676 arrivals, and under the old
ring-count selection version 6 measured the identical change *losing* about
1,700. So the honest sentence is not "exact ranking is worth X" -- it is "exact
ranking is what makes the cap tunable at all", and any report that quotes the
first rung alone gets the story backwards.

**The exact radius cut is inert here, and that is correct.** Arrivals are
byte-identical -- same coverage, same 63,243 correct, same 1,494 wrong -- and
departures move by 27. That pattern is the signature that flagged the inert
scheduled-service penalty in version 6, so it deserves a check rather than a
shrug: departures *did* move, so the parameter reaches the code. The reason it
does almost nothing is geometry. The zone table is banded in 5 NM steps, the
legacy radius is 30 NM, and 30 falls exactly on a band boundary -- so the band
filter and the exact cut select almost the same samples. It would matter at a
radius that falls *between* boundaries, and 30 does not.

**The scheduled-service penalty is the first unambiguous win**: +849 departures
and +861 arrivals, and it is the one tuned value version 6 also confirmed
through the pipeline.

### Two things to watch for, and what to do about them

**The vote cache is the job most likely to fail.** It pre-filters on
`flight_level <= max(FL_CAPS)`, which this run raises from FL200 to FL300 — and
most cruise traffic sits in exactly that band, so the join against aerodrome
zones out to 80 NM grows sharply. It also carries 42 aggregate columns instead
of 27.

*Contingency, if it dies or runs absurdly long:* rebuild the cache with caps
capped at FL200 and let the **pipeline** grid close the curve to FL300 on its
own. The two are independent — `flight_list_v7 --runs grid` runs `process_dai`
directly and never reads the vote cache — so nothing the review asked for is
lost. The research sweep already turns well below FL200 on both periods, so
FL225–300 in the *cache* buys precision on a part of the curve that is
monotonically falling.

**Out-of-area labelling adds rows, and rows can steal pairings.** `trend` with
`trend_ooa` on emits a row for a track it would previously have been silent
about. The benchmark pairs each reference flight with the track starting closest
to its off-block time, so a new row sharing an aircraft, callsign and date can
take a pairing from a track that carried a real answer. Version 6 documented
exactly this effect when departures and arrivals came from different methods.

*What to check in the results:* if `L09_ooa` shows **departure** coverage falling
while arrival coverage rises, that is the alignment moving and not the algorithm
getting worse. It should be reported as such rather than corrected away — a
benchmark that quietly adjusts its own alignment to flatter a result is worth
less than one that shows where it is fragile.

