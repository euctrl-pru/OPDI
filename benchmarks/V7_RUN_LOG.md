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
| 23:12 | ladder_2025 | **staged** — all 13 rungs |
| 23:12 | ladder_2024 | failed on rung 1, fixed, resumed 23:20 |
| 23:20 | ladder_2025 | re-running: the fix changed its fingerprint |
| 00:13 | ladder_2025 | re-run **staged**; 11 of 13 rungs byte-identical |
| 00:40 | ladder_2024 | 6 of 13 rungs, same pattern as 2025 |
| 01:05 | ladder_2024 | failed at rung 10; the write guard caught a cache rebuild |
| 00:43 | both ladders | re-running again — third time for 2025 |
| 01:48 | ladder_2025 | **staged**; ladder_2024 died on an UnboundLocalError |
| 02:03 | chain | relaunched, verified running by `chainstat.sh` |
| 03:21 | ladder_2025 | **staged** (4th run); ladder_2024 started |
| 04:04 | ladder_2024 | **rung 10 cleared** — the fix validated end to end |
| 04:30 | ladder_2024, modes_2025 | staged; a mislabelled column found in them |
| 04:32 | chain | stopped, column fixed, relaunched |
| 06:35 | both ladders + modes_2025 | **staged**, columns correct |
| 07:01 | modes_2024 | staged — the verdict holds on both samples |
| 08:04 | grid_2025 | staged — the curve closes in the pipeline too |
| 09:22 | **chain complete** | exit 0, all fifteen outputs staged |
| 09:43 | defaults changed | radius reverted to 30 NM, trend OOA off; chain re-running |
| 10:04 | ladder_2025 | died on a transient S3 fault at rung 4; retried |
| 10:55 | ladder_2025 | **staged** on the new defaults — better by 751 |
| 11:30 | ladder_2024 | staged — every rung now agrees on both samples |

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

### 23:12 · the 2024 ladder failed for the mirror-image of the earlier reason

```
AnalysisException: A column with name `h3_res_7` cannot be resolved
```

The same missing index as before, on the other table. This afternoon's fix
attached `h3_res_7` to the **cleaned** 2024 tracks; the ladder's first eleven
rungs read the **raw** ones deliberately, because that is where the published
data starts. Those are still unindexed.

Fixed by computing the column on read rather than materialising it: a second
12 GB copy of an 11.9 GB table, to add one derived column reproducible from two
others, would cost a sixth of the bucket's free space to store nothing new. The
wrapper is guarded on the column being absent, so it becomes a no-op the day
step 02's own output replaces the research copy.

**Cost: nothing computed was lost.** `ladder_2025` had already staged all
thirteen rungs, and the failure was on the first rung of the next job.

Twice now the pattern has been the same: a table built before some step existed,
read by code that assumes the step ran. Worth naming as a class rather than as
two incidents -- the research copies of the second period pre-date H3 indexing,
cleaning, and the current schema, and anything newly pointed at them should be
checked against what it expects rather than tried.

### Decision 6 · letting the 2025 ladder re-run rather than exempting it

Fixing `flight_list_v7.py` changed the fingerprint of every job that declares it
as a dependency, so `ladder_2025` -- staged and complete an hour earlier -- came
back stale and is running again. The fix cannot possibly affect it: the on-read
H3 indexing is guarded on the column being absent, and the 2025 tracks carry it.

**Letting it re-run anyway**, for two reasons.

The first is the discipline. The entire point of fingerprinting the source files
is that the *author* does not get to decide which changes could not have
mattered. This study's history is a list of changes that could not have mattered
and did -- an inert penalty, a redirect that stopped firing, a ranking rule that
inverted a conclusion. An exemption mechanism would be used exactly when
someone was confident, which is exactly when it would be wrong.

The second is that it costs an hour and buys a reproducibility check. The rerun
should reproduce the thirteen rungs **exactly**. If it does, that is evidence
the chain is deterministic; if it does not, that is a finding worth more than
the hour.

Revised estimate from here: about an hour for the 2025 ladder, an hour for 2024,
forty minutes for the two mode comparisons, an hour and a half for the two
grids.

### The re-run found something: tie resolution is not deterministic

The accidental re-run of `ladder_2025` was meant as a reproducibility check.
With all thirteen rungs now comparable, **eleven reproduce byte for byte** --
same coverage, same correct, same wrong, same score. Two do not:

| Rung | Run 1 arrivals | Run 2 arrivals | Difference |
|---|---|---|---|
| L00 legacy | 60,600 | 60,603 | 3 flights |
| L09 out-of-area | 62,277 | 62,265 | 12 flights |
| the other eleven | identical | identical | — |

Out of 95,116 reference flights, that is 0.003% and 0.013%. Departures are
identical on every rung.

**What the two have in common is ties.** Under `trend_rank_by="ring"`, `df_min`
keeps every sample whose `distance_from_center` equals the track's minimum --
and that column is an *integer* ring count, so several rows routinely survive
with identical `distance_km`. The winner among them is then settled by a
`row_number()` over an ordering that does not separate them, and by
`first(apt_ident)` in a `groupBy` that carries no ordering at all. Which row
wins depends on how the shuffle lands.

The out-of-area rung is the one that adds the most rows to the flight list, and
the benchmark pairs each reference flight with the nearest track by start time
-- so it too has more ties to resolve, in the *alignment* rather than in the
detection. That is a measurement effect, not an algorithm one, and it is the
same alignment fragility version 6 documented when the two roles came from
different methods.

**The shipped configuration reproduced exactly.** L12 -- the last rung, verified
by `verify_plan` to be `DetectionConfig()` on cleaned tracks -- is identical
across both runs, as are all ten other exact-distance rungs.

**Not repairing the legacy path.** Making its tie-break deterministic would
change what `DetectionConfig.legacy()` produces, and that preset exists for one
purpose: reproducing data already published. That data was made by this
algorithm, ties and all. A deterministic preset would reproduce something no
release ever contained.

So this is reported rather than repaired, and it is an argument for the study's
central change that nobody went looking for: **exact-distance ranking does not
arbitrate the ties, it removes them.** What ships is exactly reproducible; what
it replaces is not, by about one flight in thirty thousand.

Cost of discovering it: nothing. It fell out of a re-run taken for a different
reason, which is the case against exempting jobs whose fingerprint changed.

### The 2024 ladder reproduces the shape, not the size

First six rungs, both periods side by side (arrival score, and the change each
rung makes):

| Rung | 2025 arr | Δ | 2024 arr | Δ |
|---|---|---|---|---|
| L00 legacy | 60,600 | — | 48,411 | — |
| L01 exact ranking | 60,255 | −345 | 48,183 | −228 |
| L02 exact radius | 60,255 | 0 | 48,178 | −5 |
| L03 smooth first | 60,211 | −44 | 48,200 | +22 |
| L04 scheduled-service penalty | 61,072 | **+861** | 51,482 | **+3,282** |
| L05 flight-level cap FL60 | 61,748 | +676 | 53,178 | +1,696 |

Every sign agrees across the two periods, which is what a second sample is for.
Two differences in magnitude are worth noting.

**2024 is a harder sample.** Legacy arrival coverage is 60.83% against 2025's
68.06%, and accuracy 95.25% against 97.87%. Fewer flights are seen well enough
to name, and more of the names are wrong.

**The scheduled-service penalty is worth nearly four times as much there**:
+3,282 against +861. The penalty exists to stop a military or general-aviation
field winning on raw proximity, so its value scales with how often that happens
-- and on the harder sample, with more marginal calls, it happens far more. That
is the one tuned value version 6 also confirmed through the pipeline, and it is
now confirmed twice more.

The first three rungs are near-nil or slightly negative on both periods, exactly
as on 2025: the geometry fixes buy nothing on their own and everything through
what they let the thresholds do afterwards.

### 01:05 · the write guard earned its keep

`ladder_2024` reached rung 10, the first that takes departures from `endpoint`,
and stopped:

```
RuntimeError: refusing to write 'opdi_endpoint_candidates':
this benchmark writes only under 'research/'
```

**What would have happened without the guard is worse than the crash.**
`process_dai` builds the endpoint candidate cache whenever the month is absent
from its progress log, and the log copied into the run mentions 2025-06 only. So
it set out to rebuild the cache for 2024-06 -- from whatever track table *that
rung* reads, which at rung 10 is the **raw** 2024 tracks. The result would have
been `research/cand_2024` silently replaced by a cache derived from uncleaned
data, overwriting one derived from cleaned data, with the two periods then
measured on different input treatment and nothing anywhere saying so.

That is the exact failure this study keeps producing, and this time a guard
written for a different reason stopped it.

**Fixed by recording the month as already built.** The cache exists and is
correct -- the chain's own `03_endpoint_candidates_2024` stage made it from
cleaned tracks. Seeding the run's progress log with the period's month makes
`process_dai` skip the rebuild rather than attempt one the guard has to refuse.
A crash eight rungs in is a poor substitute for not trying.

Verified by round-tripping the log file the way `FlightListProcessor` reads it,
rather than by relaunching and watching.

### The cost of serial discovery, and what to do about it

Every fix to `flight_list_v7.py` changes the fingerprint of both ladders, so
each one costs about two hours of recomputation. Three fixes in, `ladder_2025`
has now run three times. The rule not to exempt jobs from re-running is still
right -- it is what produced the determinism finding -- but the *serial*
discovery of bugs is what makes it expensive, and that part is fixable.

So the remaining unexecuted paths were audited statically rather than waited
for. Four properties that a run would otherwise have to reach rung 10 to test:

* `index_on_read` wraps `read_table` and `redirect_candidates` wraps
  `_s3_path`, and neither captures the other's original -- so the two compose
  regardless of the order they are installed in, which is what the 2024
  `endpoint` rungs need.
* every 2024 run reads a table the period actually declares, so none can fall
  through to a 2025 default.
* rung 0 reads a table `index_on_read` covers.
* the *cleaned* 2024 tracks are deliberately **not** in `index_on_read`: they
  already carry `h3_res_7`, and recomputing it would be silent waste rather
  than a visible error.

All four hold. That is not proof the remaining rungs will pass, but it is the
part that could be checked in thirty seconds instead of two hours.

### The detection radius should go back to 30 NM

The four rungs after the flight-level cap, arrival score, both periods:

| Rung | 2025 Δ | 2024 Δ | Verdict |
|---|---|---|---|
| L06 vote margin 4 → 2 | +232 | +236 | consistent gain |
| **L07 radius 30 → 20 NM** | **+20** | **−247** | **disagrees** |
| L08 bearing tie-break | +1,092 | +534 | consistent, large |
| L09 out-of-area | −827 | −245 | consistent cost (alignment) |

**The radius is the one shipped value the evidence now contradicts.** In the
pipeline it gains 20 flights on 2025 -- noise on a sample of 95,116 -- and loses
247 on 2024. The research sweep, ranked over both periods jointly, independently
prefers 30 NM as an interior optimum. Two methods, two samples, one answer:
20 NM was a single-period argmax and it did not hold.

That is exactly the failure mode version 6 was built to catch, appearing in a
value version 6 shipped. It is not embarrassing that it appeared; it is the
point of running the second period through the pipeline rather than the harness.

**The bearing tie-break is confirmed twice more.** +1,092 and +534, on top of
the +1,140 the harness predicted. It is the largest consistent gain in the
ladder and the strongest-evidenced change in the study.

**The vote margin moves the right way but not far enough.** 4 → 2 gains on both
periods; the sweep says 0 gains again on both. The ladder never tests 2 → 0, so
that step has harness evidence only.

**Recommendation forming, graded by evidence:**

| Change | Evidence | Confidence |
|---|---|---|
| radius 20 → **30 NM** | pipeline, both periods; sweep, both periods | ship it |
| bearing tie-break **on** | pipeline, both periods; sweep | ship it (already default) |
| vote margin 2 → **0** | sweep only, both periods | measure through the pipeline first |

Nothing is being changed while the run is in flight. These go into version 7 as
a recommendation with the evidence beside each line, and the radius change wants
one more ladder run to confirm it end to end.

### 01:48 · two monitoring checks were lying, and one status report was wrong

`ladder_2024` failed immediately:

```
UnboundLocalError: local variable 'month' referenced before assignment
```

The candidate-log seeding added at 01:05 reads `month`, and it was placed beside
the log-directory setup -- which runs *before* `month` is derived. `--dry-run`
returns long before that line, so the dry run passed. Worth recording as a fact
about the tool rather than the bug: **`--dry-run` validates the plan and almost
nothing else.**

The bug is a two-line move. The more serious problem was that it went unnoticed,
because two ways of checking the run were both unreliable:

1. **`pgrep -c -f regenerate_v7.py` matched the checking command's own shell**,
   which contains that string. It could never return zero, so the chain always
   looked alive.
2. **Reading the log's last line looked like progress** when the file had not
   been written to for some time. A static tail and a live tail are
   indistinguishable without checking the file's age.

Together those produced a status report describing a chain that had already
exited. The timestamps in those reports were also extrapolated from timer
intervals rather than read from the clock, and had drifted ahead of real time.

`benchmarks/../chainstat.sh` now answers the question from evidence that cannot
be faked by a stale file: it excludes itself from the process match, prints the
log's age in minutes, and refuses to say "running" when the process count is
zero or the log has been silent for more than twenty minutes.

Nothing computed was lost: `ladder_2025.csv` was staged before the failure.

### 04:04 · rung 10 cleared, and the role switch is large on both periods

The rung that killed `ladder_2024` twice tonight -- once on the write guard,
once on my own `UnboundLocalError` -- completed:

| Period | Departures | Δ | Arrivals | Δ |
|---|---|---|---|---|
| 2025 | 71,806 | **+2,734** | 61,486 | −791 |
| 2024 | 62,980 | **+4,346** | 52,742 | −714 |

Taking departures from `endpoint` is the second-largest single change in the
study after the flight-level cap, and it is *larger* on the harder sample.
That fits what the two algorithms read: a departing aircraft's first fix is on
or near the runway, and `endpoint` uses exactly that, while `trend` needs enough
of a climb to vote on. Where reception is patchier, having to see a climb costs
more than having to see one fix.

The arrival cost on both periods, −791 and −714, is the merge alignment: the
combined list contains departure-only tracks, and the benchmark pairs each
reference flight with the track starting nearest its off-block time, so some
arrivals are scored against a track that carries none. Version 6 documented the
same effect at the same place.

Nine rungs before it reproduced the earlier 2024 pass exactly, including the
radius result -- so the one shipped value this study contradicts now has four
independent confirmations: the pipeline on 2024 twice, and the joint sweep on
both periods.

### 04:30 · the published CSV claimed a configuration it did not use

`modes_2025.csv` recorded `adep_mode=endpoint, ades_mode=trend` on **every**
row. Three of the five runs executed something else:

| Run | Recorded | Actually ran |
|---|---|---|
| `trend` | endpoint / trend | **trend / trend** |
| `endpoint` | endpoint / trend | **endpoint / endpoint** |
| `nearest` | endpoint / trend | **nearest / nearest** |
| `legacy`, `shipped` | correct | correct |

The cause is mine. When the earlier `dict.update` collision was fixed by
dropping the explicit mode arguments, the columns fell back to the *config's*
values -- and a single-mode run is built from `DetectionConfig()`, whose modes
are endpoint/trend, then run with both roles forced to one algorithm. So the row
described a configuration that was never executed.

**The scores were right and the report was right**: `build()` passes the modes
to `process_dai`, and the report labels rows by run name rather than by that
column. But the CSV is published data, and a reader of it has no way to know.
This is precisely the mislabelling version 6 shipped and version 7 exists to
prevent, so it is not something to document and leave.

Fixed with two updates in order -- the configuration first, then the run
identity and the *executed* modes, which overwrite it. `build()` now also
returns the resolved track table, so `tracks_table` names the data instead of
saying `config`. Verified on the case that was wrong, without Spark.

**Stopped the chain to do it.** Fixing now reaches a correct complete dataset
sooner than letting it finish and re-running afterwards: the partial data is
superseded either way, so the only question was which order costs less.

**A second self-match, in the stop.** `pkill -f run_v7b.sh` matched its own
shell and killed the command issuing it, leaving the chain running while
reporting failure. The same trap as `pgrep -c -f`, in the same session. Killing
now goes through an explicit PID list built with the matcher excluded.

### The result: twelve changes, graded on two independent samples

Both ladders complete on identical code. Change in **total** score (departures
plus arrivals) at each rung:

| Step | 2025 Δ | 2024 Δ | Verdict |
|---|---|---|---|
| + exact-distance ranking | −873 | −462 | consistently costs |
| + exact radius cut | −27 | −14 | consistently costs |
| + smooth before the cut | +158 | +293 | holds |
| + scheduled-service penalty | +1,710 | +3,450 | holds |
| + flight-level cap FL60 | +4,298 | +5,046 | holds |
| + vote margin 2 | +580 | +514 | holds |
| **+ radius 20 NM** | **+214** | **−79** | **disagrees** |
| + bearing tie-break | +1,275 | +1,041 | holds |
| + out-of-area | +335 | +321 | holds |
| + departures from `endpoint` | +1,955 | +3,632 | holds |
| + endpoint radius 30 NM | +207 | +66 | holds |
| + cleaned tracks (**shipped**) | −477 | −592 | consistently costs |

**Net: +9,355 on 2025 and +13,216 on 2024** -- 123,667 → 133,022 and
101,980 → 115,196. The harder sample gains more, which is the direction a real
improvement should go.

**Exactly one rung disagrees, and it is the radius.** Eleven of twelve changes
have the same sign on two independent samples. The one that does not is the
value this study already flagged from the sweeps, now confirmed from a third
direction. There is no ambiguity left about it.

**Out-of-area is net positive on both periods, and I had been describing it
badly.** Earlier entries here reported it as costing arrivals, which is true --
but departures gain more than arrivals lose, so the total is +335 and +321. The
arrival cost is the benchmark's pairing; the departure gain is real flights
correctly identified as starting outside the observed area. Reporting only the
arrival half made a positive change look like a concession.

**Cleaning costs, consistently and measurably**: −477 and −592. That is the
price of masking a fifth of barometric altitudes, and it is now a number rather
than an expectation. Whether it is worth paying is a judgement the report should
put to the reader with the accuracy gain beside it, not settle by assertion.

**Exact-distance ranking costs on its own, on both samples.** −873 and −462.
Its value is entirely in what it enables: the flight-level cap two rungs later
is worth +4,298 and +5,046, and under the ring selection it replaces, version 6
measured that same change *losing* about 1,700.

### The verdict, on both samples, in the same order

| Configuration | Departures by | Arrivals by | 2025 | 2024 |
|---|---|---|---|---|
| **shipped** | `endpoint` | `trend` | **133,022** | **115,193** |
| endpoint both roles | `endpoint` | `endpoint` | 131,032 | 114,702 |
| trend both roles | `trend` | `trend` | 130,072 | 111,070 |
| legacy | `trend` | `trend` | 122,158 | 100,591 |
| nearest | `nearest` | `nearest` | 86,837 | 73,490 |

**The ranking is identical on two independent samples**, and the recommendation
is first on both: **+10,864 (+8.9%)** on 2025 and **+14,602 (+14.5%)** on 2024
against the algorithm published before 2026.

Three things this table settles that version 6 could only assert.

*The split between roles is worth having.* Both single-algorithm configurations
score below the split on both periods, so taking departures from one method and
arrivals from another is not an artefact of the sample it was chosen on.

*`nearest` is not a serious option, and now that is measured twice.* It answers
far more often -- it never abstains -- and is wrong far more often. Scored, it
is last by a margin larger than the entire gain from legacy to shipped. This is
not a criticism of the `traffic` library, which is solving a different problem:
it names the nearest aerodrome and leaves the judgement to the caller.

*The gain is larger on the harder sample.* 14.5% against 8.9%, in the same
direction as the ladder's net. An improvement that shrinks where the data is
poorer would be suspicious; one that grows is doing what it claims.

### The flight-level curve, closed in the pipeline

The review's question was whether the cap should go higher. The research sweep
said no; this is the same question asked of `process_dai` itself, at 20 NM:

| Cap | Dep coverage | Dep accuracy | Dep score | Arr coverage | Arr accuracy | Arr score |
|---|---|---|---|---|---|---|
| FL25 | 61.97% | 98.86% | 56,929 | 65.02% | 98.41% | 58,891 |
| FL50 | 73.04% | 98.96% | 67,301 | 68.10% | 98.29% | 61,453 |
| FL60 | 74.10% | 98.96% | 68,279 | 68.72% | 98.18% | 61,793 |
| **FL75** | 75.43% | 98.83% | 69,230 | 69.19% | 98.00% | **61,858** |
| FL100 | 77.47% | 98.39% | 70,142 | 70.08% | 96.80% | 60,248 |
| **FL125** | 79.01% | 97.86% | **70,334** | 70.96% | 95.47% | 58,314 |
| FL200 | 80.51% | 96.09% | 67,603 | 72.37% | 92.42% | 53,185 |
| FL300 | 81.67% | 92.73% | 60,742 | 73.72% | 88.30% | 45,510 |

**Departures peak at FL125, arrivals at FL75**, and both fall away steeply: by
FL300 departures have lost 9,592 from their peak and arrivals 16,348.

Coverage rises monotonically all the way to FL300 -- 81.67% and 73.72% -- which
is the half of the intuition that was right. Accuracy falls faster, because a
fix taken at FL300 is a long way from whichever aerodrome turns out to be the
answer. The score turns where they cross.

The pipeline puts the arrival peak at FL75 where the research sweep put it at
FL50, with FL60 between them and within a few hundred of both. All three are on
a plateau: FL50, FL60 and FL75 span 405 flights out of 95,116. The shipped FL60
sits inside it, and nothing in either measurement argues for moving it.

That is the review's question answered twice, in the harness and in the code
that ships, on data neither was tuned against.

### Out-of-area on arrivals is a coin flip, and I had it wrong

Earlier entries here graded out-of-area labelling as "holds" on the strength of
its **total** delta, +335 and +321. That grading is right about the rung and
wrong about the shipped configuration, and the difference matters.

The ladder's out-of-area rung has *both* roles coming from `trend`, so its
departure gain is real -- but **the shipped configuration takes departures from
`endpoint`**, which has always had the label. So the trend-side out-of-area
change only ever reaches arrivals in what actually ships, and on arrivals it is
negative on both samples: −827 and −245.

The precision figures say why:

| Role | Truly out-of-area | Labelled | Recall | **Precision** |
|---|---|---|---|---|
| Departures (`endpoint`) | 8.30% | 0.74% | 7.96% | **89.20%** |
| Arrivals (`trend`) | 8.37% | 0.44% | 2.67% | **50.35%** |

An arrival label is right about half the time. Working the arithmetic: about 419
arrivals get the label, roughly 211 correctly and 208 wrongly, and each of those
was previously a *silence*. At `k = 2` that is **−205** -- converting nulls into
coin-flip answers is exactly the trade the scoring rule is designed to refuse.

**Why the asymmetry is real and not a tuning accident.** A departing track that
*starts* at the edge of the observed area almost certainly entered it from
outside. An arriving track that *ends* near the edge is ambiguous: it may be
leaving the area, or it may be a flight still bound for somewhere inside whose
reception was simply lost. The border test ported cleanly to departures because
the geometry supports it, and does not for arrivals.

**What this changes.** `trend_ooa` should not ship on for arrivals as it stands.
The options are a stricter arrival border test -- requiring the track to be
heading *out* of the area, which the bearing machinery already computes -- or
leaving arrival out-of-area to `endpoint` where its precision is 89%. That is a
V7 recommendation with a measurement behind it, not a defaults change to make
while the run that measured it is still going.

It is also a reminder about the ladder: a rung is graded on the configuration it
*is*, not the one that ships. Both readings belong in the report, and the
distinction has to be drawn where the table is.

### 09:22 · complete, and one thing left unresolved

The chain exited 0. Fifteen outputs, both periods, every one produced by
`regenerate_v7.py`. The report renders with no placeholders, no NA cells and no
broken cross-references, in HTML and PDF.

**`--check` is not clean, and it should not be forced.** While the run was in
flight, `src/opdi/config.py` gained a 254-line `EventConfig` class -- step 04
work from another workstream, along with untracked `pipeline/crossings.py` and
`tests/test_crossings.py`. `config.py` is a declared dependency of every
pipeline job, so all six are now marked *"code changed since this was
produced"*.

Verified: `EventConfig` is not referenced anywhere in `flights.py`,
`flight_list_v7.py` or `adep_ades.py`. It is added to `OPDIConfig` as a field
the flight-list path never reads. The numbers are unaffected in substance.

**Re-running would make the provenance worse, not better.** That change is
*uncommitted*. Re-running now would stamp every V7 figure with a fingerprint
over somebody else's half-finished working tree, and `git_dirty = True` -- a
state that will change again when they finish. The recorded provenance would
then point at a commit that never existed.

So this is left for the user rather than resolved unilaterally, and it is the
one thing in the run that a person has to decide:

* **re-run the six pipeline jobs once the step 04 work is committed** -- about
  four hours, and `--check` comes back clean; or
* **accept the current outputs**, whose numbers are right and whose provenance
  honestly records that the fingerprint moved after they were written.

The report renders with `OPDI_RENDER=allow-stale` in the meantime, which is
exactly what that flag is for: the figures are shown and the provenance section
says they are unverified against the current tree.

Everything else in this run is settled.

### 09:43 · the defaults changed, on review

The first version of this report *stated* two recommendations and left the
defaults alone, on the principle that a value should not be changed by the
document measuring it. Reviewed, that principle was misapplied: it guards
against a report quietly rewriting what it reports on, and it is not a reason to
keep shipping a value shown to be worse. Both are now changed.

| Field | Was | Now | Why |
|---|---|---|---|
| `trend_radius_nm` | 20.0 | **30.0** | The only ladder step whose sign differs between the two samples, +214 and −79, with the joint sweep preferring 30 as an interior optimum. Four measurements agree. |
| `trend_ooa` | True | **False** | Reaches only arrivals in the shipped configuration, where the label is right 50.35% of the time against `endpoint`'s 89.20%. Each one replaces a silence, so at `k = 2` it loses about 205 per sample. |

**The vote margin is deliberately unchanged.** The sweep prefers 0 on both
samples and the pipeline has never tested it. Shipping on harness evidence alone
is the mistake V6 made, and repeating it here while writing the report that
documents it would be difficult to defend.

**What the change forced, and why that is an improvement.** `verify_plan`
asserts the last ladder rung *is* `DetectionConfig()`, so the ladder failed
immediately -- correctly, since it still walked to a radius of 20 and switched
out-of-area on. Rather than weaken the assertion, the ladder now walks only the
changes actually adopted (eleven rungs), and the two rejected ones move to a new
`rejected` group that applies each to the **shipped configuration alone**.

That is a better measurement than the rung was. A rung answers "what would this
have done at that point in a sequence?"; the delta from shipped answers "should
this be on?", which is the question. The vote margin at 0 joins the group, so
the one parameter left hanging on harness evidence finally gets a pipeline
number.

Tests updated, 115 passing, including a new one pinning `trend_ooa` off so
"the feature is implemented, why is it disabled?" has to be answered against the
measurement rather than switched on by assumption.

### 10:04 · a transient S3 fault, and how it was told apart from a bug

`ladder_2025` died at rung 4:

```
NoSuchUploadException: The specified multipart upload does not exist.
The upload ID may be invalid, or the upload may have been aborted
or completed. (Status Code: 404)
```

Not a code fault. The magic committer had an in-flight multipart upload
cancelled underneath it while writing a flight list.

Checked before retrying, because "transient" is the easiest wrong diagnosis to
reach for:

* **the bucket is not full** -- 83.87 GB, with the quota around 100. A full
  bucket fails with `Bucket quota exceeded`, which this is not;
* **nothing in the failing path changed** -- the write is the same code that
  succeeded through six previous chain runs tonight;
* **stale uploads could not be inspected** -- the endpoint denies
  `ListMultipartUploads`, which is worth recording so the next person does not
  spend time on it.

Retried. The chain is idempotent and every pipeline job was stale anyway, so a
retry costs nothing beyond the four minutes already spent. If it recurs at the
same rung it is not transient and deserves a different answer.

### The defaults change was worth more than removing a bad value

The 2025 ladder on the new defaults, eleven rungs:

| Rung | Total | Δ |
|---|---|---|
| L00 legacy | 123,667 | — |
| L01 exact-distance ranking | 122,794 | −873 |
| L02 exact radius cut | 122,767 | −27 |
| L03 smooth before the cut | 122,925 | +158 |
| L04 scheduled-service penalty | 124,635 | +1,710 |
| L05 flight-level cap FL60 | 128,933 | +4,298 |
| L06 vote margin 2 | 129,513 | +580 |
| **L07 bearing tie-break** | 130,890 | **+1,377** |
| **L08 departures from `endpoint`** | 133,867 | **+2,977** |
| L09 endpoint radius 30 NM | 134,074 | +207 |
| L10 cleaned tracks (**shipped**) | 133,773 | −301 |

**Net +10,106, against +9,355 before — the shipped configuration is 751 better
than the one this report originally described.**

The interesting part is *where* the gain comes from. It is not simply the
removal of a value that failed on a second sample. Two other rungs improve:

* the **bearing tie-break** is worth +1,377 at 30 NM against +1,275 at 20;
* **departures from `endpoint`** is worth +2,977 against +1,955.

That has a mechanism. A wider radius admits more candidate aerodromes per track,
so there are more near-ties for alignment to resolve -- the tie-break has more
to do and does it well. The narrow radius was suppressing a change that had
already been adopted on its own merits. Cleaning also costs less: −301 against
−477.

So the value was not merely unsupported; it was holding back two other changes.
That is an argument for applying a finding rather than tabling it, and it would
have been invisible in a recommendations table.

### Every rung now agrees on both samples

With the radius reverted and out-of-area off, both ladders re-run:

| Rung | 2025 Δ | 2024 Δ | Verdict |
|---|---|---|---|
| exact-distance ranking | −873 | −462 | consistently costs (the enabler) |
| exact radius cut | −27 | −14 | no material effect |
| smooth before the cut | +158 | +287 | holds |
| scheduled-service penalty | +1,710 | +3,456 | holds |
| flight-level cap FL60 | +4,298 | +5,046 | holds |
| vote margin 2 | +580 | +514 | holds |
| bearing tie-break | +1,377 | +1,098 | holds |
| departures from `endpoint` | +2,977 | +4,099 | holds |
| endpoint radius 30 NM | +207 | +66 | holds |
| cleaned tracks (**shipped**) | −301 | −413 | consistently costs |

**Nothing disagrees any more.** The single step whose sign differed between the
samples was the one removed, and with it gone every remaining change points the
same way on two independent periods.

**Net: +10,106 on 2025 (was +9,355) and +13,677 on 2024 (was +13,216).** Better
on both, by 751 and 461.

And the two rungs that improved on 2025 improve on 2024 as well: the bearing
tie-break from +1,041 to +1,098, the endpoint switch from +3,632 to +4,099. The
interaction is real on both samples, not a feature of one.

This is the cleanest state the study has reached: eleven changes, two samples,
one direction each, and a shipped configuration that no measurement in the study
now contradicts.

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

