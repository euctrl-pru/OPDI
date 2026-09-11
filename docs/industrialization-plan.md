# Industrialising AOBT / AIBT / ATOT / ALDT at network scale

> **Status as of 2026-09-11 — most of this plan is done, and much of it is
> obsolete.** The document below is kept as written on 2026-09-07 because it
> records why each decision was made. Read this header first; several sections
> describe a build that no longer exists.
>
> **What changed.** The layout build moved off the public Overpass API to
> **pyosmium reading a local OSM extract** (PR #8). That did not just swap a
> data source -- it removed the constraint the plan was organised around.
> Overpass throttling, Nominatim geocoding, retry-with-backoff, per-airport
> persistence and "do not parallelise against the public endpoint" are all
> moot: there is no network call per airport. The 1,036-aerodrome build takes
> ~6 minutes.
>
> | Section | Status |
> |---|---|
> | §3.2 Overpass constraint, retries, per-airport persistence | **Obsolete** — no per-airport network call |
> | §3.2 "Consider a Geofabrik extract … the recommended end state" | **Done** — this is what shipped |
> | §3.3a Duplicate columns silently dropped | **Done** |
> | §3.3b Per-airport Spark append is O(N) | **Done** — one bulk write per invocation |
> | §3.4 Replace the Nominatim geocode with a bbox | **Done, and superseded** — primary assignment is containment in OSM's own `aeroway=aerodrome` polygon; the bbox is the fallback |
> | §3.4 Over-capture guard | **Done**, and still needed |
> | §3.4 A/B not yet verified | **Done** — `benchmarks/compare_layout_sources.py`; 74 of 75 cell-sets match the Overpass baseline exactly |
> | §5.1 Merge the `on_ground` layout fix | **Done** |
> | §5.3 Backfill large+medium in bbox | **Done** — 1,036 aerodromes, 2,900,230 rows |
> | §5.4 Publish, keeping the previous build | **Done** — `publish_layouts.py`, backup retained |
> | §4 Never broadcast the network-wide grid | **Done** |
> | §4 Pre-filter the grid to the batch's aerodromes | **Done 2026-09-11** — `pipeline/layout.py`, semi-join before the `h3_res_12` join |
> | §5.5 Re-run the V4 benchmark for AOBT/AIBT | **Partial** — the 9.10 → 90.13% gain is credited to the `on_ground` fix; the scale-out's own contribution is not separately measured, so the two remain confounded |
> | §5.6 Block-time pooling rule | **Open** — still no second block detector |
> | §6 Record the layout vintage on an event | **Open** — see below |
> | §3.5 Quarterly rebuild cadence | **Open** — nothing schedules it |
>
> **§1 is wrong now.** It says AOBT/AIBT are pinned to twenty aerodromes with
> stand polygons. The published table covers **1,036**, with 291,892
> `parking_position` cells. The "20 airports" was never a code constant -- it
> was simply what had been run.
>
> **On §6 (layout vintage).** `run_week.py` addresses this structurally rather
> than by adding a column: a run builds its reference tables into its own
> warehouse prefix alongside its data, so the prefix *is* the vintage and every
> event is traceable to the geometry built beside it. That does not help data
> already published from a shared prefix, so a `hexaero_build_utc` column is
> still worth adding -- deliberately not done here, because the published table
> lacks it and code that required it would fail to read what is already out
> there.
>
> **Two defects found while checking this plan, both now fixed:**
> `REFERENCE_SUBSTEPS` iterated in id order, so 00a and 00b ran before 00d
> created `oa_airports` -- invisible on an established warehouse, fatal on a
> from-scratch build; and `AirportLayoutGenerator.fetch_airport_list` still
> downloaded the public OurAirports CSV, reinstating the network dependency the
> extract removed and risking a loop list from a different snapshot than the
> geometry.

**Status:** written 2026-09-07, from the flight-events-v4 campaign. Every number
below is measured on that campaign's sample (2026-06-05..07, twenty aerodromes)
or on the reference tables themselves; none is an estimate unless it says so.

The four milestones do **not** share a scaling problem. Three of them are
already network-wide; one is pinned to twenty airports. Knowing which is which
is most of the work, so it comes first.

---

## 1. Where each milestone's geometry comes from

| Milestone | Detector | Geometry it needs | Source table | Airports covered today |
|---|---|---|---|---|
| **ATOT** | `detect_runway_movements` | runway thresholds + bearings | `oa_runways` | **40,899** (all OurAirports) |
| **ALDT** | `detect_runway_movements` | same | `oa_runways` | **40,899** |
| `airborne` | `runway_ops` traversals | res-12 runway + approach cells | `h3_runway_zones` | **1,197** |
| `touchdown` | `runway_ops` traversals | same | `h3_runway_zones` | **1,197** |
| **AOBT** (`off-block`) | `calculate_block_events` | **stand polygons** | `hexaero_airport_layouts` | **20** |
| **AIBT** (`on-block`) | `calculate_block_events` | **stand polygons** | `hexaero_airport_layouts` | **20** |

**ATOT and ALDT need no industrialisation.** They read `oa_runways`, which is a
committed reference table covering every airport OurAirports knows. Their
coverage limit is reception and flight-list resolution, not geometry. Measured
network-wide on the V4 sample: ATOT 89.8%, ALDT 95.9%.

**`airborne`/`touchdown` are already built out.** `h3_runway_zones` was
generated for the whole EUROCONTROL bounding box in this campaign: 7,069,645
res-12 cells over 1,197 airports, built in 36 s of geometry plus 155 s of
write. Step **00f** (`h3_runway_grid.py`) reproduces it.

**AOBT and AIBT are the whole problem.** Stand polygons come from OpenStreetMap
via step 00b, and that step has only ever been run for twenty curated airports.
Fifteen of the twenty *study* aerodromes have no stand geometry at all, so
nothing can fire there regardless of reception.

---

## 2. What is already fixed, and what that changes

Two defects were fixed during the campaign and must be in place before any
scale-out is measured, or the scale-out will be judged on the wrong baseline.

* **`layout.py` discarded every surface sample.** An ADS-B surface position
  message carries no altitude -- neither barometric nor geometric, both measured
  non-null on **0.1%** of EBBR's in-stand state vectors. `dropna(subset=[...,
  "baro_altitude_c"])` deleted those rows outright, and the low-altitude gate
  would have dropped whatever survived (`NULL <= 2000` is NULL). At EBBR,
  **2,013 aircraft sit in stand polygons and 57 reached the detector.** Both now
  consult `on_ground`, true on 99.9% of those samples. Behind
  `airport_admit_on_ground`, off under `legacy()`.
* **The runway family had the same class of bug** (`min_by(height, event_time)`
  returning the null-baro ground sample), fixed earlier in the campaign.

**Consequence for planning:** AOBT/AIBT coverage of ~1% at the five airports
that *do* have stands was never evidence that stands are useless. It was the
detector throwing the evidence away. The scale-out and the fix have to be
measured together.

---

## 3. The OSM layout build at scale

### 3.1 Volume

Measured over 14 study airports: **137,798 cells, 9,843 cells/airport,
25,291 stand cells**. Extrapolated:

| Scope | Airports | Cells | Parquet |
|---|---:|---:|---:|
| Study set | 20 | ~0.2 M | ~4 MB |
| Large+medium in bbox | 1,353 | **~13.3 M** | **~0.26 GB** |
| Plus `h3_runway_zones` | — | ~7.1 M | ~0.15 GB |

~20 M reference cells in total. That is small on disk and **large for a
broadcast join** -- see §4.

### 3.2 Runtime and the Overpass constraint

The build is one Overpass query per airport, driver-side (`osmnx`), not a Spark
job. Measured cold: **mean 8.5 s, median 3.5 s, max 26 s** per airport, giving
**~3-4 h for 1,353 airports** serially. Warm (osmnx cache) it is ~4 s.

**Overpass is the binding constraint and it pushes back.** In the twenty-airport
run, throughput collapsed after ~11 airports (one airport took 646 s) and
**five returned empty** -- including LPPT, which demonstrably *has* stands,
proving the empties were throttling and not absent data. Plan for it:

* **Retry with backoff**, 3 attempts per airport. Recovered the empties in the
  study run.
* **Persist per airport, not at the end.** The first attempt lost 20 airports
  of work to a crash in the final `pd.concat`. Write one parquet per airport as
  it completes and merge afterwards; the loop then resumes for free.
* **Respect the resumable success log** already in `process_all`, so a killed
  run restarts where it stopped rather than re-querying.
* **Consider a self-hosted Overpass or a Geofabrik extract** if the build is to
  run regularly. A single Europe extract removes the rate limit entirely and
  turns a 4-hour throttled crawl into a local query. This is the recommended
  end state; the public endpoint is acceptable for a one-off backfill.
* **Do not parallelise against the public endpoint.** It is a shared resource
  and the throttling above is what its operators intend.

### 3.3 Two defects in the generator to fix first

* **Duplicate column labels.** `process_airport` returns frames whose columns
  are not unique. `pd.concat` raises `InvalidIndexError`, and
  `spark.createDataFrame` warns *"columns are not unique, some columns will be
  omitted"* -- meaning the production path is **silently dropping columns**.
  De-duplicate at the source.
* **Per-airport Spark append is O(N).** The same pattern in the runway-grid
  generator stalled to ~4.5 min/airport by airport 422 of 1,353, because each
  append re-commits a growing table. It was fixed there (one bulk write per
  invocation, commit `11b069c`); `h3_airport_layouts.process_all` still has it
  and must get the same treatment before a 1,353-airport run.

### 3.4 Making the query smaller

The Overpass payload is already minimal in one sense -- `retrieve_osm_data`
passes `tags={"aeroway": AEROWAY_TAGS}`, so the seven aeroway types are filtered
**server-side** and only matching features come back. The waste is not in what
is returned; it is in how the area is specified.

`features_from_place(f"{icao} Airport", ...)` resolves the name through
**Nominatim** first, then sends Overpass a `poly:` filter of the resulting place
polygon. Each airport therefore costs two rate-limited services and an expensive
spatial predicate, and it inherits a failure mode that is silent: a name that
does not resolve yields *no data* rather than an error. Five of twenty airports
returned empty in this campaign -- EDDP, LPPT, EGNT, EGCC, UGKO -- and LPPT
demonstrably has stands, so at least some of those empties are this.

**Proposal: query a bounding box built from `oa_runways`.** Both thresholds of
every runway are already in that table, and runways bound an airport's long
axis, so their extent plus a margin (~1.5 km covers aprons, stands and
taxiways) is a sound envelope. That removes the geocode entirely: one service
instead of two, a trivial predicate instead of a polygon, and a deterministic
area that does not depend on a name matching.

Two things it needs to be correct:

* **An over-capture guard.** The risk is the opposite of loss: a box can catch a
  neighbouring airfield, and `hexagonify_airport` stamps `apt_icao` on whatever
  comes back, which would mislabel those features. Drop any feature whose
  nearest `oa_airports` aerodrome is not the one being built.
* **Response caching** (`ox.settings.use_cache`), because this build will be
  re-run -- one crash in this campaign lost twenty airports of Overpass work.

**Not yet verified, and it must be before anyone relies on it.** The A/B --
same airport, place-based versus bbox, comparing feature sets and wall time --
was attempted on EICK and could not be completed: `overpass-api.de` had already
stopped answering us, and both mirrors then timed out too, one of them inside
osmnx's own rate-limit status call. Run the comparison on three or four
aerodromes of different sizes (a hub, a mid-size, a small field) and confirm the
returned feature sets match before switching the generator over.

**The endpoint exhaustion is itself the argument for §3.2's recommendation.** A
local Geofabrik extract removes the rate limit, the geocode and the mirrors from
the problem at once, and makes the A/B above cheap to run.

### 3.5 Refresh cadence

OSM is a live map: stands are added, renamed and re-drawn. Runways move rarely;
aprons and stands change often. Suggest **quarterly** rebuilds, with the
published table versioned by build date so an event can be traced to the
geometry that produced it. A rebuild that silently changes the H3 cell set
changes which movements are detected, and nothing in the current event schema
records which layout vintage was in force -- **that is a gap worth closing at
the same time** (see §6).

---

## 4. Joining ~20 M reference cells at scale

This is where the runway campaign lost the most time, so the lesson is recorded
rather than relearned.

* **Never `F.broadcast` a network-wide grid.** Broadcasting the 7 M-cell
  `h3_runway_zones` OOM'd the driver JVM outright. Let Spark choose the strategy
  (commit `65f2152`).
* **Pre-filter the grid to the aerodromes in the batch before the join.** The
  join is already gated on `array_contains(sv.apt, apt_icao)`, so only the
  flight's own aerodromes can match; restricting the grid first turns a
  network-wide broadcast into a small one. For a twenty-aerodrome study that is
  tens of thousands of cells rather than millions.
* **Expect the layout join to grow with coverage, not with data.** Today it
  matches 20 airports; at 1,353 it matches ~66× more reference rows against the
  same state vectors. Watch the shuffle on `h3_res_12` and re-check
  `spark.sql.shuffle.partitions` (96 in the campaign) once the layout table is
  full-size.

---

## 5. Sequencing

1. **Merge the layout fix** (`fix/layout-admit-on-ground`) -- without it, any
   scale-out is measured against a detector that discards its own inputs.
2. **Fix the generator** (§3.3: duplicate columns, bulk write).
3. **Backfill OSM layouts** for all large+medium in bbox, resumable, per-airport
   persistence, retries. ~3-4 h on the public endpoint; minutes on a local
   extract.
4. **Publish** as a versioned `hexaero_airport_layouts` build; keep the previous
   build until the new one is validated.
5. **Re-run the V4 benchmark** for AOBT/AIBT and the taxiway/apron family. This
   is the acceptance test, and it must be run **after** steps 1-3 together.
6. **Decide the block-time pooling rule** the way the runway family's was
   decided -- there is no second block detector today, so this is a placeholder
   until one exists.

## 6. Open questions, stated rather than assumed

* **Reception is the floor and no amount of geometry lifts it.** Even with
  stands present and the fix in place, a stand only yields a block time if the
  aircraft is heard there. LSZH reported barometric altitude on the ground for
  12.8% of in-stand samples against EBBR's 0.1% -- an aircraft- and
  receiver-mix effect nobody controls. **The scale-out should be expected to
  raise AOBT/AIBT coverage from ~1% to something well short of ATOT/ALDT's
  ~90%, and the target should be set from a measurement, not from parity.**
* **No layout vintage is recorded on an event.** Two runs months apart against
  different OSM snapshots produce different events with nothing to distinguish
  them. Adding the layout build date to `info` would close this.
* **`opdi_h3_airspace_ref` is still generated and read by nothing** -- unrelated
  to these four milestones, but it is the same class of reference-table drift
  and worth resolving while this area is open.
* **Whether to extend beyond large+medium.** `oa_runways` covers 40,899
  airports and gives ATOT/ALDT everywhere; the OSM layout build is scoped to
  1,353. Small-airport block times are probably not worth the crawl, but that
  is a product decision rather than a technical one.
