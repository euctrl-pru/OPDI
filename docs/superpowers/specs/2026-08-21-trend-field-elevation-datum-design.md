# `trend` on the field-elevation datum

**Date:** 2026-08-21
**Status:** approved design, not yet planned
**Repos touched:** `opdi/` (pipeline, config, benchmarks), `opdi-portal/` (paper v6.1)

## The problem

`trend` cuts altitude on **flight level** — pressure altitude above the standard
datum. `endpoint` cuts on **height above field elevation**. Step 04's phase
detection cuts on height above field elevation. `trend` is the only one of the
three still measuring against sea level, and nothing in the study has ever
questioned it.

The consequence is not subtle. `trend_max_fl = 60` admits samples below 6,000 ft
AMSL. At Amsterdam that is 6,000 ft of usable climb and descent. At Erzurum,
field elevation 5,763 ft, it is roughly 240 ft — so almost the whole approach and
departure sits above the cap and never votes. The method does not become
*inaccurate* at high aerodromes; it goes **silent** at them, and its silence is
read downstream as "could not tell".

This is the same failure `phase_ground_above_field` was introduced to fix in
step 04, where `config.py` states it plainly: the loss "is not random: it is
biased against high-elevation aerodromes."

### Where the code says so

| Location | What it shows |
|---|---|
| `src/opdi/pipeline/flights.py:663` | `flight_level = baro_altitude * 3.28084 / 100` — an MSL-datum quantity |
| `src/opdi/pipeline/flights.py:665` | `filter(col("flight_level") <= trend_max_fl)` — the cut, with no elevation term |
| `src/opdi/pipeline/flights.py:668` | the zone join happens **after** the cut, so at cut time no aerodrome is known |
| `src/opdi/pipeline/flights.py:565-576` | `endpoint` builds `agl_ft = baro_altitude * 3.28084 - apt_elevation_ft`: *"Height above the aerodrome, not above the ellipsoid: a fixed cut-off means nothing at a field sitting at 5,000 ft."* |
| `src/opdi/config.py:750` | `phase_ground_above_field` — step 04 already moved to this datum |
| `reference/h3_airport_zones.py:568-583` | the zone table carries no `elevation_ft`, so the join cannot supply it today |
| `papers/adep-ades-detection-v6/index.qmd:1201` | V6 states the asymmetry out loud and does not question it |

### What is *not* wrong

Two properties bound the blast radius, and both should be stated in the paper so
the result is not over-claimed:

* **The vote is datum-invariant.** `trend` votes on the *sign* of a smoothed
  altitude change. Adding a constant offset to every altitude in a track cannot
  flip a sign. Field elevation therefore changes only **which samples are
  admitted to vote**, never how a vote is cast. This is a coverage change first;
  any accuracy movement is a second-order consequence of coverage.
* **The smoothing is untouched.** `trend_smooth_before_cut` computes the rolling
  mean over the whole track, partitioned by `track_id` alone, *before* any
  altitude filter. Moving the cut cannot change a single smoothed value.

`nearest` is deliberately excluded from this work: it applies no altitude
condition at all, on any datum, because it is the naive `traffic`-faithful
baseline the other methods are scored against. Giving it one would defeat its
purpose.

## Design

### 1. Production change — `_fetch_and_label_sv`

Move the cut from before the join to after it.

Today: compute `flight_level` → smooth → **cut** → join zones → exact radius test.
New: compute → smooth → **wide pre-filter** → join zones → attach elevation →
**exact per-aerodrome cut** → exact radius test.

The exact cut is a single expression:

```python
height_ft = col("baro_altitude") * 3.28084 - coalesce(col("apt_elevation_ft"), lit(0.0))
```

The `coalesce` carries two cases deliberately, rather than as an accident:

* **Aerodrome with unknown elevation** falls back to the MSL cut — today's
  published behaviour — rather than being dropped. This matches the convention
  `phase_ground_above_field` set: a missing elevation degrades to the old
  behaviour, it does not remove the flight.
* **Sample matching no aerodrome** (the left join's nulls) also falls back to the
  MSL cut, leaving that population bit-for-bit unchanged. Those rows exist only
  to keep otherwise-unnamed tracks present; `flights.py:701-706` is explicit that
  dropping them "would remove tracks from the flight list entirely rather than
  leaving them unnamed". A datum change must not disturb them as a side effect.

The **wide pre-filter** is a performance guard only, and must be provably
lossless:

```
flight_level <= (trend_max_height_ft + max_field_elevation_ft) / 100
```

It keeps the join affordable — that pre-cut is why the join is tractable at all —
while admitting every sample that could survive the exact cut at any aerodrome.
`max_field_elevation_ft` is a **runtime `max()` over the aerodromes actually
present in the reference table**, not a hardcoded constant, so it cannot go stale
if the reference set widens. A hardcoded value would fail exactly the way this
whole class of bug fails: silently, by never firing.

**Shared helper.** Elevation comes from the same `oa_airports` broadcast join
`endpoint` already performs. Factor that block out of
`_build_endpoint_candidates` into `_attach_field_elevation()` and use it from
both paths, so the two provably read the same source and cannot drift onto
different elevations for the same aerodrome.

### 2. Config surface

```python
trend_max_height_ft: float = 6000.0   # height above field elevation — shipped
trend_max_datum: str = "field"        # "field" | "msl"
trend_max_fl: int = 60                # retained; used when trend_max_datum == "msl"
```

Units are aviation and carried in the field name, per the repo convention.
"FL60 above field elevation" is a contradiction in terms — a flight level is by
definition an MSL pressure quantity — so the new threshold gets an honest name
rather than reusing the old one under a boolean.

`DetectionConfig.legacy()` pins `trend_max_datum="msl"` alongside its existing
`trend_max_fl=40`, so both released-data reproduction and V6 reproduction
continue to work unchanged.

**Consumers.** `trend_max_fl` has exactly one production reader —
`flights.py:665` — which is what makes this change tightly localised. Two
benchmark scripts also set it, `flight_list_v6.py` and `flight_list_v7.py`; both
need a way to pass the datum through so an arm can select it per run. V7's
harness is touched only enough to keep it running unchanged on the MSL datum,
since V7 is out of scope for this study.

`6000.0` is a placeholder standing for the FL60 equivalent. **The sweep sets the
shipped number.** It must not be presented as tuned before it is measured.

### 3. Version string — decision required before republication

`_version_for` already stamps `FLIGHT_LIST_VERSION` (`"v4.0.0"`) for any
non-legacy configuration, so this change is technically covered without touching
anything. But `benchmarks/V61_RUN_NOTES.md` records that a new version string for
the sampler and ranking changes is **"still outstanding and must be done before
any republication"** — meaning `v4.0.0` already spans several unreleased
algorithm changes.

Proposal: bump **once**, to `v5.0.0`, covering sampler + ranking + datum
together, rather than adding a third unversioned change to the pile. This is a
published-contract decision and is called out here rather than made silently.

### 4. Measurement

The vote cache is already the right shape. `trend_sweep.py:build_cache` keys on
`(track_id, apt_ident)` and applies each cap as a conditional sum **after** the
aerodrome join, so an above-field family costs an elevation join and a second set
of aggregate columns — **not a second pass over the tracks**.

The cache writes to a **new prefix**, `research/trend_votes_agl`, leaving V6's
`research/trend_votes` untouched so V6 remains reproducible.

Every arm runs on **both periods** — 2025-06-05…07 and 2024-06-05…07 — as
standard. The report pools them and treats the second period as confirmation in
prose; it does not present two parallel sets of tables.

| Arm | What it measures |
|---|---|
| **A — datum swap** | Shipped configuration, run twice: `trend_max_datum="msl"` at `trend_max_fl=60` against `trend_max_datum="field"` at `trend_max_height_ft=6000`. Numerically the same ceiling, different datum — so the arm isolates one variable and nothing else. |
| **B — cap sweep** | The above-field optimum found on its own terms, rather than inherited from the datum we are abandoning. |
| **C — per-elevation bands** | Coverage and accuracy change banded by field elevation (<500 / 500–1500 / 1500–3000 / >3000 ft), plus a per-aerodrome view and a leave-one-out check on each treatment band. |

> **Amended 2026-08-21, after the feasibility census.** The census passed the
> gate — ~700 movements per role in `>3000` across 26 aerodromes, both periods
> agreeing — but showed each treatment band is dominated by a single aerodrome:
> LEMD is 40% of `1500-3000`, LTAC is 54% of `>3000`. Band means alone cannot
> then separate an elevation effect from a Madrid effect, so Arm C also reports
> per-aerodrome deltas and re-computes each band with its largest contributor
> removed. Without that, criterion 3 below is a test that cannot fail.
>
> The census also tempered the expected effect size. The Erzurum example that
> motivates this spec (5,763 ft, ~240 ft of vote room under FL60) is real but
> rare — it carries under ~110 movements. The elevated traffic that exists sits
> at 1,500–3,500 ft, where FL60 still leaves 3,000–4,100 ft to vote in. The
> paper should argue from that population, not from Erzurum.
| **D — pipeline fidelity** | The winning cell re-run through `process_dai` itself, so the shipped figure is a pipeline figure and not a harness figure. |

### 5. Paper and diagrams

New `opdi-portal/papers/adep-ades-detection-v6.1/`, with `regenerate_v61.py`
forked from V6's harness and cut to the four arms. V6 and V7 are untouched.

**Diagrams** follow V7's established idiom, which exists because
` ```{mermaid} ` does not render reliably to PDF on this toolchain: the mermaid
block is wrapped in `::: {.content-visible when-format="html"}` and an
equivalent **table** is supplied in a `when-format="pdf"` block
(`papers/adep-ades-detection-v7/index.qmd:222-265`). Each diagram therefore
ships in two forms.

Four flows — `endpoint` and `trend`, each for departures and arrivals — showing
inputs, the altitude test **with its datum named explicitly on each**, the
ranking rule, and the three-way outcome (`aerodrome` / `out_of_area` /
`undetermined`). Placed early, in a methodology section, with each step written
out in prose beside the diagram.

V6 already explains both methods at `index.qmd:336` and `index.qmd:456`. Audit
what is there, carry across what still holds, and write only the genuinely
missing steps — do not duplicate existing text.

## Success criteria

The change ships to production only if **all three** hold:

1. Pooled arrival coverage gain **≥ 0.5 pp** — the bar V6 set for adopting a
   change on measured grounds.
2. Accuracy does not fall.
3. **Arm C shows the gain concentrated at elevated fields.**

Criterion 3 is the discriminating one and is not negotiable. If the gain is
uniform across elevation bands, then whatever produced it is not the datum, and
the change must not be shipped on this reasoning even if criteria 1 and 2 pass.
A result that cannot fail is not a result.

The research half of the paper is published either way. A measured null at high
aerodromes is a publishable finding and closes the question.

## Risks

**The sample may not contain enough high-elevation traffic.** The whole study
rests on Arm C, which needs ground-truthed movements at elevated fields. The
bounding box is not the constraint — it reaches 49.7°E and 26.7°N, so the
Anatolian plateau (Erzurum ~5,760 ft, Van ~5,480 ft), the Spanish plateau and the
Alpine fields are all in scope. Whether six specific days carry enough movements
there to move a number is unknown.

**Mitigation, and the first step of the plan:** a cheap pre-flight count of
ground-truthed movements per field-elevation band across both samples, run
*before* any cache rebuild. If it comes back thin, that is discovered for the
price of one small query rather than after a full regeneration, and the study is
re-scoped then rather than abandoned late.

**Secondary:** the wide pre-filter increases the row count entering the zone
join. The increase is bounded by `max_field_elevation_ft / 100` flight levels and
applies only below that ceiling, but the join cost should be measured on the
first run rather than assumed.

## Out of scope

* `nearest` — no altitude condition by design; see above.
* Re-tuning `trend`'s radius, margin or penalty. V6 tuned those on the MSL datum;
  re-sweeping them here would confound the datum result with four other moving
  parts. If Arm B suggests they have moved, that is a finding for a later study.
* Adding `elevation_ft` to the H3 zone reference table. It would avoid a second
  join, but forces a step-00a regeneration and changes a reference schema for a
  marginal gain. The `oa_airports` broadcast join is what `endpoint` already
  does, and symmetry with it is worth more here than one fewer join.
* Any change to `tracks.py:_add_track_id`, which is frozen.
