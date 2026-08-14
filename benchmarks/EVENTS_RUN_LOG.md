# Flight events run log

Written as the work proceeds, so its state is legible without reading this
conversation, and so every decision taken without review is visible afterwards.
Same convention as `V7_RUN_LOG.md`.

Plan: `R0`–`R8` in the events plan. Sequence chosen deliberately — **finish the
detectors, then benchmark once** — because every code change alters the
provenance fingerprint and forces the ladder to re-run, and V7's ladder ran four
times for exactly that reason.

---

## Status

| Item | State |
|---|---|
| R0 make the config honest | **done** — `7eb5549` |
| R1 D1 ground membership above field elevation | **done** |
| R2 ring crossings (KPI05, KPI08) | **done** |
| R3 ICAO level segments (KPI17, KPI19) | **done** |
| R4 runway & touchdown | **done** — detector + ATOT/ALDT events |
| R5 ground milestones | **done** — AOBT/AIBT; T05 pushback deferred |
| R6 KPI18 cruise level | **not an event** — see decision 13 |
| R7 benchmark code | **done** — `events_gt`, `events_score`, `event_bench` |
| R7 ladder run | **not run** — needs many hours of serial cluster time |
| R8 paper | not started |

---

## Decisions taken without review

| # | Decision | Why |
|---|---|---|
| 1 | **D1 resolves ground membership against *both* ADEP and ADES elevation, taking the more permissive.** | The alternative — pick the nearer aerodrome per sample — needs aerodrome coordinates joined to every state vector and a distance per row. Taking the greater of the two memberships gives the same answer for a fraction of the cost: a track is only ever on the ground at one of its ends, and cruise sits far above both, so the "wrong" end can only ever contribute a zero. Two scalar columns per track, no geometry. |
| 2 | **A missing elevation coalesces to zero rather than dropping the flight.** | `NULL` would propagate through the membership and remove every phase for a flight whose aerodrome the flight list never named — turning a gap in *reference* data into a loss of *events*. Coalescing to zero degrades exactly to the published behaviour, which is the right failure direction. |
| 3 | **`attach_field_elevation` is called by the processor, not inside the detector.** | Keeps `calculate_horizontal_segment_events` a pure column-in/column-out function that tests can drive without a `StorageManager`. The detector uses the elevation columns when they are present and ignores them otherwise. |
| 4 | **The inert-flag test is updated in the same commit that implements the flag.** | R0 shipped `phase_ground_above_field=False` with a test pinning it inert. R1 flips it, and the full suite failed until that test moved — which is the mechanism working, not a nuisance. Each flag's default and its test move together with its implementation. |

| 5 | **Rings are built from the flight's own ADEP/ADES, not from `h3_airport_detection_zones`.** | Both indicators are defined against the flight's own aerodromes -- KPI08's ASMA is a cylinder around the destination, KPI05's reference area one around each end -- and APDF records one crossing per movement for the same reason. It is also far cheaper: the zone table multiplies every sample by every aerodrome within 110 NM and would need its 30 NM `max_radius_nm` ceiling raised in two places, where this multiplies each sample by at most two and needs no reference-table change. The cost is that a ring crossing is not detected for an aerodrome merely overflown, which is correct for these KPIs. |
| 6 | **Bearing is computed from the interpolated crossing position, not interpolated alongside it.** | A bearing is circular: interpolating between 359 and 1 gives 180, which would be wrong by half a turn on any crossing near due north -- and due north is not rare. |

| 7 | **ICAO level segments are a new family, not a re-tuning of `level-start`/`level-end`.** | The published pair comes from the fuzzy phase classifier and answers "does this look like level flight". ICAO asks a geometric question about a band anchored at the segment's own start. Changing the existing detector to ICAO's rule would silently redefine a published event type; adding a family leaves both available and forces the paper to say they are not interchangeable. |
| 8 | **Level-offs take TOC/TOD from the horizontal detector's own output rather than recomputing the phase pass.** | Two independent derivations of "where cruise began" would eventually disagree, and the exclusion box is defined against the TOC altitude -- so a disagreement would move the box and change which segments count. |

| 9 | **Only `TrackBasedRunwayDetection` is ported, not its polygon sibling.** | The polygon variant needs OpenAP for its phase call, shapely for a trapeze, and recurses into a second alignment pass. The track-based one is a filter, a median and a broadcast join, and its parallel-runway tie-break -- which traffic does with shapely -- is a cross-track distance in closed form. |
| 10 | **Runway bearings are computed from threshold positions, not from `le_heading_degT`.** | Those columns are frequently null in OurAirports and, where present, sometimes magnetic rather than true. traffic derives its own the same way, for the same reason. |
| 11 | **ATOT and ALDT are taken as the extremes of the runway-detection window, and reported as proxies.** | The earliest surviving sample of a departure is a lift-off proxy, the latest of an arrival a touchdown proxy. Measuring their bias against APDF is the benchmark's job; assuming it is zero would be the mistake. |

| 12 | **Off-block and on-block are anchored on the parking-position events, not on movement alone.** | Without the anchor, a track first received mid-taxi would report its first movement as an off-block. That is a different event, and it would read as an implausibly short taxi rather than as a miss -- a wrong answer dressed as a good one. |
| 13 | **KPI18 is not implemented as a detector.** | It needs the maximum cruise flight level per flight, the airport pair and its great-circle distance, then reference distributions across similar pairs grouped by aircraft performance class. Every input already exists once TOC/TOD do; none of it is a *milestone*. Building it as an event type would put a statistical aggregate in the event table. It belongs in the benchmark and paper layer, and is recorded here so the omission is deliberate rather than forgotten. |
| 14 | **T05 (pushback) is deferred.** | Its signal -- stopped, short movement, >=90 degree track reversal, stop, taxi -- is portable, but it needs a reliable stand geometry *and* dense ground reception at the same time. With AOBT coverage itself expected to be the limiting result, a milestone that needs strictly more than AOBT is not worth building before that number is measured. |

| 15 | **D7b resolves as "make the mode required", not "make re-processing idempotent".** | The plan's preferred fix -- delete-then-append per month -- turns out to be unavailable without a layout change, and the evidence is in `storage.py` itself. `insert_overwrite` overwrites *matching partitions* in Iceberg mode, and `opdi_flight_events` is partitioned by `(type, version)`, not by month, so it would destroy every other month. In S3 mode it is worse: it maps to `df.write.mode("overwrite").parquet(dir)`, which replaces the **entire table**. Replacing one month in place therefore needs either a month partition on a published table (a migration, not to be done unattended) or a read-filter-rewrite of a multi-gigabyte table every month. So: the mode is now required at every call site, the silent no-op is fixed, and re-processing safety rests on the per-month progress logs plus the deterministic ids, which at least make duplicates detectable. The month partition is recorded as deferred rather than done. |
| 16 | **An unrecognised mode now raises.** | Found while making the argument required: the Iceberg branch was an if/elif/elif with no else, so a typo'd mode string fell through every branch, wrote nothing, and returned success. Not the bug being hunted, but the same shape -- a failure that reports as a success. |

| 17 | **B3 (inline literals to config in `tracks.py`) is dropped: there was nothing to do.** | The altitude-cleaning block already reads `self.altitude_smoothing_window_minutes` and `self.max_vertical_rate_mps`; the only bare numbers in it are inside comments explaining what those values mean. The plan listed it from an earlier survey that had counted the comments. Recorded rather than silently skipped, and rather than manufacturing a change to justify the item. |

| 18 | **The write guard checks the resolved path, not the table name.** | `redirect_event_tables` sends `opdi_flight_events` to a research location by patching `_s3_path`, so a name-based guard would have rejected exactly the writes the redirect makes safe -- and, worse, would have *passed* a write whose name looked safe but whose path had been redirected elsewhere. Checking where the bytes land is the only test that means anything. |

---

## Measured

**Ground truth loader, run on the cluster against 2025-06 (period `2025`).**
First real measurement of this programme:

| | |
|---|---|
| APDF movements in the month | 1,224,742 |
| reaching `icao24` via the `ID` bridge | **1,154,338 (94.25%)** |
| milestones on the three benchmark days | 231,880 |
| ring crossings on those days | 113,444 |

Both derived counts are arithmetically consistent with the bridge, which is
the check worth having: 231,880 is almost exactly two milestones per reachable
movement (ATOT+AOBT for a departure, ALDT+AIBT for an arrival), and 113,444 is
two rings per arrival. A pivot that had dropped or duplicated a phase would
show up here as a count that is not a clean multiple.

---

## Things that bit, and what they cost

**A synthetic track shorter than the smoothing window is a different test.**
The first D1 fixture was seven samples over 35 s, against a 60 s majority
window — so the window covered the whole track, the majority label swallowed the
GND→CL transition, and *no* take-off could exist at any elevation. Both the
"with fix" and the "sea level, unaffected" cases failed, which is what showed it
was the fixture rather than the fix. Legs are now 75 s each.

**Editing a fixture's altitudes without its rates.** The second failure was
mine again: lengthening the ground leg from 3 to 15 samples left the
`i < 3` guard on `vert_rate` and `velocity`, so twelve samples sat at field
altitude while reporting a 2,300 ft/min climb. The guard now keys off the leg
length. Both failures were caught by the sea-level control case — worth keeping
a control arm in any test whose subject is a threshold.

**A flight has two ends.** The first ring tests asserted on all crossings and
failed with extra rows and a crossing time 34 s early. The detector was right:
the test flight's ADEP is EHAM, so rings were correctly built around *both*
aerodromes and the assertions were mixing the departure's rings into the
arrival's. Ring assertions now filter on `apt_icao`, and there is a test that
pins both ends being present rather than treating it as noise.


**A segment ends *at* the breaking point, not before it.** The first level
detector evaluated membership on each sample's step to the *next* one, so the
last level sample -- the one whose next step is the climb away -- was excluded
and every level-off came out one sample interval short, always in the same
direction. That is the same class of one-sided bias the crossing detector was
built to remove. Membership is now evaluated at the sample itself and the
forward step only *starts* a segment; the conformance test pins a 90 s
injected level-off at exactly 90 s.


**`&` binds tighter than `>=` in Python.** `F.col("n") >= F.lit(4) & F.col(x).isNotNull()`
parses as `n >= (4 & isNotNull)`. Here it raised a type error, which is the
lucky case -- with two compatible operands it would have been a silently
different filter. Parenthesised, with a comment saying why.

**A degree of longitude is not a degree of latitude.** The first runway fixture
placed thresholds by equal lat/lon offsets and called the result "070"; at
51 N, where a degree of longitude is 0.63 of a degree of latitude, the true
bearing was 077. The test was asserting against a number I had assumed rather
than computed. Fixture offsets are now derived from the intended bearing.
