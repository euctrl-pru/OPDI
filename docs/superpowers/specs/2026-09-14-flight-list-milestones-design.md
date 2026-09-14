# Flight list milestones, runways, stands and ring crossings

**Status:** design, approved in conversation 2026-09-14.

Enrich `opdi_flight_list` so a flight's milestones can be read without joining
the event table, and so the layout mirrors PRISME's APDF closely enough to
compare against it directly.

## The problem

`opdi_flight_events` is long-form: one row per (flight, milestone). Answering
"when did this flight take off, from which runway, off which stand" means three
joins and a pivot. APDF, the ground truth OPDI benchmarks against, is
flight-shaped. Every comparison therefore begins by reshaping OPDI's output.

A second problem is that `events_v0.2.0` publishes **two events for one
purpose**: `ATOT` and `airborne` both name the take-off instant, `ALDT` and
`touchdown` both name the landing. A consumer must know which to prefer, and
the answer depends on the aerodrome.

## Decisions

### One variable per purpose

`airborne` and `touchdown` cease to be published types. In their place:

```
ATOT = coalesce(airborne, legacy ATOT)     info.method = "acdm" | "legacy"
ALDT = coalesce(touchdown, legacy ALDT)    info.method = "acdm" | "legacy"
AOBT = off-block                           (renamed; single source)
AIBT = on-block                            (renamed; single source)
```

A-CDM takes precedence because it is measurably the better instant: it is the
interpolated crossing of 15 ft above field elevation, where legacy `ATOT` is
the extreme sample of a detection window and carries a measured **+19 s median
bias**. Legacy fills the nulls, which is most of them — the A-CDM family
requires surface reception and reaches roughly 4-7% of the network against
legacy's ~90%.

`info.method` records which algorithm produced each value, **in the event
table only** -- the flight list carries the value alone. This is not
decoration. **The merged column is a mixture of two estimators with different
biases, and the mixing ratio varies by aerodrome** -- surface reception was
measured at 12.8% of in-stand samples at LSZH against 0.1% at EBBR. An
aggregate computed over the merged column therefore carries an
aerodrome-dependent bias that is invisible in the column itself. The method travels with the event, so the
mixture is recoverable by anyone who needs it; the flight list is the
convenience view and does not repeat it.

### Version

This changes the published type vocabulary, so it ships as **`events_v0.3.0`**.
`events_v0.2.0` stays reachable and reproducible; `CLAUDE.md` forbids mutating
a released version string.

### The tops keep one method

`top-of-climb` and `top-of-descent` are published by the PRC/PRU method only.
The fuzzy phase-classifier arm is dropped from the event table, and the PRU arm
takes the plain names -- so `top-of-climb-cco` and `top-of-descent-cdo` cease
to exist as separate types.

The PRU arm is also the better-covered one: 55,819 against 32,604 a day for the
climb, 55,810 against 32,541 for the descent.

### Ring radii

`ring_radii_nm` becomes `(40, 50, 60, 100, 110, 120)`.

Rings are computed from a haversine distance to the aerodrome's own
coordinates, not from the detection-zone grid, so the 110 NM extent of
`h3_airport_detection_zones` does not cap them. 120 NM is reachable.

Only the six radii the flight list needs are emitted. Every 10 NM was
considered and rejected: twelve radii would grow the event table by about 30%
(1.77M to ~2.3M rows a day) to carry six columns nobody reads.

## The new step

**Step 04b**, after events, before export. Per day, like everything else.

Numbered `04b` rather than `05` because 05 is the published parquet export and
renumbering it would break every operator's muscle memory for no gain.

### Columns added to `opdi_flight_list`

| Column | Source |
|---|---|
| `ATOT`, `ALDT`, `AOBT`, `AIBT` | merged events, joined on `flight_id` |
| `RWY_DEP` | `info.runway` of the `ATOT` event |
| `RWY_DEP_BEARING_DEG` | `info.runway_bearing_deg` of the `ATOT` event |
| `RWY_ARR` | `info.runway` of the `ALDT` event |
| `RWY_ARR_BEARING_DEG` | `info.runway_bearing_deg` of the `ALDT` event |
| `STND_DEP` | `info.osm_ref` of `exit-parking_position` |
| `STND_ARR` | `info.osm_ref` of `entry-parking_position` |
| `C{40,50,60,100,110,120}_ARR` | inbound crossing of that ring |
| `C{40,50,60,100,110,120}_DEP` | outbound crossing of that ring |

Additive only. Existing columns keep their names, types and meanings, so
current consumers are unaffected.

### The runway's direction, not just its name

`ATOT` and `ALDT` carry `info.runway_bearing_deg` beside `info.runway`, and
the flight list publishes it as `RWY_DEP_BEARING_DEG` / `RWY_ARR_BEARING_DEG`.

**Bearing, not heading.** The quantity is the runway centreline's *true*
bearing, computed from its two threshold coordinates -- a property of the
pavement, identical for every movement on it. A heading is what the aircraft
flies: magnetic, and wind-corrected, so two aircraft departing the same runway
in the same minute report different ones. The runway's own direction is the
thing that makes the column answer "which way was this runway used".

It is not redundant with `RWY_DEP`. The designator already names the direction
to the nearest ten degrees -- `07` and `25` are the two ends of one strip -- so
in the common case the bearing merely sharpens it. It earns its place in three
cases the designator cannot cover:

* the designator is null because the runway could not be named, while the
  traversal's geometry still fixes the direction used;
* magnetic drift has moved the pavement's true bearing away from the number
  painted on it, which is why aerodromes renumber runways;
* a consumer computing wind components, track angles or alignment statistics
  needs a number, and parsing `07L` into 70 degrees is both lossy and wrong.

The value comes from `runway_thresholds()`, which already derives a bearing per
*direction* rather than per strip, so no new geometry is introduced -- the
column publishes a quantity both detector arms already compute and then threw
away.

### Arrival and departure rings

Every flight crosses each ring twice. `_DEP` is the outbound crossing from
ADEP, `_ARR` the inbound crossing to ADES. The detector already emits both; the
distinction is the `role` already carried in the ring event.

## What this does not do

* Nothing further. The level-segment defect found during this design is fixed
  here; see below.

## Verification

A day already processed is reprocessed and compared:

* every flight-list row present before is present after, with its original
  columns unchanged -- checked by digest over the pre-existing column set;
* `ATOT` is non-null at least as often as legacy `ATOT` was, since it is a
  coalesce and cannot lose coverage;
* where `ATOT_METHOD = "legacy"`, `ATOT` equals the legacy event's time
  exactly;
* `RWY_DEP` matches `info.runway` of the same flight's `ATOT` event for every
  row where both exist.

For the level-segment fix specifically, on the same reprocessed day:

* **`level-start` and `level-end` counts are equal**, per flight and in total.
  They were 129,426 and 104,057; they should agree afterwards.
* the added ends are the one-sample segments the appendix predicts: every
  newly-emitted `level-end` shares its `event_time` and `flight_id` with an
  existing `level-start`.
* no `level-start` is lost -- the count may only rise, never fall, since the
  change adds a label and removes none.
* the difference is reported in the run log rather than left to be noticed:
  the count of coincident start/end samples is the size of the defect, and is
  worth recording once.


## Appendix: why `level-start` exceeds `level-end`

Measured on 2026-06-01: 129,426 starts against 104,057 ends. 17,356 of 44,461
flights carry more starts than ends, for 28,855 unmatched starts in total.

The cause is not the partition boundaries. `prev_phase` and `next_phase` are
built with `lag`/`lead` defaulting to the *string* `"None"` rather than NULL, so
a track that begins or ends in level flight is labelled correctly at both ends.

It is the `when` chain in `calculate_horizontal_segment_events`, which is
first-match-wins:

```
.when(t == first_cr_time, ["level-start", "top-of-climb"])
.when(t == last_cr_time,  ["level-end",   "top-of-descent"])
.when(start_of_segment,   ["level-start"])
.when(in CR/LVL & next_phase != phase, ["level-end"])
```

A sample that is **both the start and the end of a segment** -- a level segment
one sample long -- matches the third branch and can never reach the fourth. It
emits a start and no end. A single-sample *cruise* matches the first branch and
never the second, losing `level-end` and `top-of-descent` together.

The evidence fits: only 22% of unmatched starts fall within ten minutes of
`last_seen`, so the great majority are mid-flight, which is where single-sample
level segments occur.

### The fix

The chain exists in that shape because it had four things to say and only one
slot to say them in. Dropping the fuzzy tops removes two of the four, and what
remains is two independent questions rather than a precedence order:

```
is_level_start = start_of_segment OR t == first_cr_time
is_level_end   = (phase in CR,LVL AND next_phase != phase) OR t == last_cr_time

types = array_compact(array(
    when(is_level_start, "level-start"),
    when(is_level_end,   "level-end"),
))
```

A sample that is both now emits both. The defect is removed structurally rather
than special-cased: there is no longer a precedence for a one-sample segment to
fall foul of.

This is a behaviour change to a published family, and every level-segment
figure rests on it, so it is measured rather than asserted -- see Verification.

**Confidence:** high on the mechanism, from the code and that distribution.
Not directly measured -- confirming it needs the phase column from
`osn_tracks`, which was being rewritten when this was written.


## Acceptance: a fortnight, not a month

Once implemented, the acceptance run is **14 days** -- 2026-06-01 to 2026-06-14
-- not the month originally planned. A fortnight finishes overnight and can be
inspected the next day; a month cannot, and there is no point producing thirty
days under a vocabulary whose first fourteen have not been looked at.

At the measured per-day cost -- ingest ~20 min warm, tracks 11.7, cleaning 8.2,
flight list 5.7, events 10.6 -- a day is about 56 minutes, so a fortnight is
roughly **13 hours**.

The month run started before this design was settled was stopped at day one for
the same reason: it was producing `events_v0.2.0` output under a vocabulary
about to change. Its ingested state vectors remain valid and are reused; only
the ~41 minutes of that first ingest is carried forward rather than repeated.

What the fortnight is for, in order of what would send us back to the code:

1. `ATOT`/`ALDT` coverage against the legacy figures -- the merge must not
   lose rows, only gain precision on the subset A-CDM reaches.
2. `level-start` and `level-end` counts agreeing, per flight and in total.
3. The ring columns populated at all six radii, both directions.
4. `RWY_DEP`/`RWY_ARR` and `STND_DEP`/`STND_ARR` populated at the rates
   measured below, give or take the days differing.

## Expected fill rates

Measured on 2026-06-01, against the leg each column belongs to -- flights with
a known ADEP for departure columns, ADES for arrival:

| Column | Source | Fill |
|---|---|---|
| `ATOT` | legacy `ATOT` | 49.5% |
| `ALDT` | legacy `ALDT` | 71.6% |
| `AOBT` | `off-block` | 31.6% |
| `AIBT` | `on-block` | 35.1% |
| `STND_DEP` | `exit-parking_position` | 32.8% |
| `STND_ARR` | `entry-parking_position` | 35.1% |

Two figures are easy to reach for here and both mislead. The
industrialisation plan's 12.8% at LSZH against 0.1% at EBBR describes surface
reception **before** the `on_ground` projection fix -- the state that fix
removed, not the current one. The V4 campaign's 90.13% AOBT is real but is
pooled **against APDF at the twenty study aerodromes**, which were selected for
being the best-observed in Europe. Neither is the network-wide number.

Network-wide, across 1,036 aerodromes with layouts, blocks and stands reach
about a third of flights. `STND_DEP` tracks `AOBT` almost exactly (32.8%
against 31.6%) because both come from the same parking-position detection --
so a flight with a block time nearly always has a stand, and vice versa.

At a single well-observed aerodrome the picture is very different. EBBR, same
day:

| Column | EBBR | Network |
|---|---|---|
| `ALDT` | 99.1% | 71.6% |
| `AIBT` | 96.2% | 35.1% |
| `STND_ARR` | 96.2% | 35.1% |
| `STND_DEP` | 72.3% | 32.8% |
| `AOBT` | 67.4% | 31.6% |
| `ATOT` | 55.4% | 49.5% |

The arrival side is effectively complete: 96% of arrivals get both a block time
and a stand. Whatever the 0.1% in the industrialisation plan measured, it is
not what the code does now.

**Two caveats on that table, neither resolved.** The denominators are 534
departure legs and 318 arrival legs, but EBBR operates on the order of 230 of
each per day. The arrival count is plausible and the departure count is not, so
ADEP appears to be assigned to EBBR for legs that did not depart there. Every
departure percentage above is therefore a floor. And `ATOT` (55.4%) falls
*below* `AOBT` (67.4%) on that same denominator, which is backwards -- a
take-off happens fast, in the open, with good reception, while a stand is the
hardest place on the airfield to observe. Both are open questions, not
findings, and neither blocks this work: the enrichment step reports whatever
the events carry.
