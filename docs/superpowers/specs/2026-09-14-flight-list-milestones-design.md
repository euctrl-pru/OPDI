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

`info.method` records which algorithm produced each value. This is not
decoration. **The merged column is a mixture of two estimators with different
biases, and the mixing ratio varies by aerodrome** -- surface reception was
measured at 12.8% of in-stand samples at LSZH against 0.1% at EBBR. An
aggregate computed over the merged column therefore carries an
aerodrome-dependent bias that is invisible in the column itself. The method
must travel with the value.

### Version

This changes the published type vocabulary, so it ships as **`events_v0.3.0`**.
`events_v0.2.0` stays reachable and reproducible; `CLAUDE.md` forbids mutating
a released version string.

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
| `ATOT_METHOD`, `ALDT_METHOD` | `info.method` of the event chosen |
| `RWY_DEP` | `info.runway` of the `ATOT` event |
| `RWY_ARR` | `info.runway` of the `ALDT` event |
| `STND_DEP` | `info.osm_ref` of `exit-parking_position` |
| `STND_ARR` | `info.osm_ref` of `entry-parking_position` |
| `C{40,50,60,100,110,120}_ARR` | inbound crossing of that ring |
| `C{40,50,60,100,110,120}_DEP` | outbound crossing of that ring |

Additive only. Existing columns keep their names, types and meanings, so
current consumers are unaffected.

### Arrival and departure rings

Every flight crosses each ring twice. `_DEP` is the outbound crossing from
ADEP, `_ARR` the inbound crossing to ADES. The detector already emits both; the
distinction is the `role` already carried in the ring event.

## What this does not do

* **The tops are left alone.** `top-of-climb` and `top-of-climb-cco` are the
  same duplication as ATOT/`airborne`, and so are the descent pair. They are
  *not* merged here, because unlike take-off there is no reference source that
  records a top of climb -- so "prefer the more accurate one" has no
  measurement behind it. Merging them would be a preference presented as a
  fact.
* **`level-start` exceeds `level-end` by 25,369 events a day**, which should
  not happen if every segment has two ends. Noted, not investigated, and not
  addressed here.

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
