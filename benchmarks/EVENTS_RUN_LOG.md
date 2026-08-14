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
| R2 ring crossings (KPI05, KPI08) | in progress |
| R3 ICAO level segments (KPI17, KPI19) | not started |
| R4 runway & touchdown | not started |
| R5 ground milestones | not started |
| R6 KPI18 cruise level | not started |
| R7 benchmark code + ladder run | not started |
| R8 paper | not started |

---

## Decisions taken without review

| # | Decision | Why |
|---|---|---|
| 1 | **D1 resolves ground membership against *both* ADEP and ADES elevation, taking the more permissive.** | The alternative — pick the nearer aerodrome per sample — needs aerodrome coordinates joined to every state vector and a distance per row. Taking the greater of the two memberships gives the same answer for a fraction of the cost: a track is only ever on the ground at one of its ends, and cruise sits far above both, so the "wrong" end can only ever contribute a zero. Two scalar columns per track, no geometry. |
| 2 | **A missing elevation coalesces to zero rather than dropping the flight.** | `NULL` would propagate through the membership and remove every phase for a flight whose aerodrome the flight list never named — turning a gap in *reference* data into a loss of *events*. Coalescing to zero degrades exactly to the published behaviour, which is the right failure direction. |
| 3 | **`attach_field_elevation` is called by the processor, not inside the detector.** | Keeps `calculate_horizontal_segment_events` a pure column-in/column-out function that tests can drive without a `StorageManager`. The detector uses the elevation columns when they are present and ignores them otherwise. |
| 4 | **The inert-flag test is updated in the same commit that implements the flag.** | R0 shipped `phase_ground_above_field=False` with a test pinning it inert. R1 flips it, and the full suite failed until that test moved — which is the mechanism working, not a nuisance. Each flag's default and its test move together with its implementation. |

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
