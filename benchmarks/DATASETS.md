# Datasets in `s3://eurocontrol/opdi/`

Every dataset written to the OpenSky S3 bucket is recorded here. Endpoint
`https://s3.opensky-network.org`; credentials `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` from `opdi/.env`.

**Nothing is persisted to local disk.** Dev boxes are scratch; S3 is reachable from the OSN
server and the work laptop. If a dataset isn't listed here, treat it as unidentified — do not
build on it and do not delete it without checking.

Ground-truth extracts are the one standing exception: they live in `opdi/reference/` under
git-lfs, because OSN pulls them with a shallow clone of this repo.

---

## Pipeline prefixes

> ⚠️ **Re-measure before planning against these figures.** The line below read
> 67.40 GB for ten days while the bucket grew to 96.89 GB. A study was planned
> against the stale number and had to stop at the point of writing, because the
> remaining headroom was smaller than what it needed. This bucket is written by
> several concurrent projects, so a figure here is a snapshot, not a standing
> fact. The one-liner under "The bucket has a quota" takes about a minute.

Measured **2026-08-23: 96.89 GB** across the whole bucket — **3.11 GB of
headroom**, and it grew by 1.29 GB during the twenty minutes the audit took.
Of that, **42.81 GB is `opdi/osn_symposium_paper_2026/` and belongs to another
project** — leave it alone. OPDI's own footprint is now about 54 GB.

(Previously recorded here: 67.40 GB on 2026-08-12. Most of the 29.5 GB of growth
since is the flight-events V3 ladder — 58 `research/events_20{24,25}_*` prefixes
totalling 11.41 GB, written 2026-08-14/15 — plus
`research/trend_votes_agl{,_2024}` at 1.29 GB written 2026-08-22.)

### Inventory at 2026-08-23

Everything above 0.5 GB, with what consumes it.

| GB | Prefix | Last written | Consumed by |
|---|---|---|---|
| 42.81 | `osn_symposium_paper_2026/` | 2026-08-09 | **Another project.** Note it has *not* grown in two weeks, contrary to the "still growing" note this file used to carry. |
| 11.93 | `research/tracks/` | 2026-08-05 | V6/V7 second period. **Do not prune** — not rebuildable, since the 2024 state vectors were never ingested here. |
| 11.41 | `research/events_20{24,25}_*` (58 prefixes) | 2026-08-14/15 | Flight-events V3 ladder. Written by `event_bench.py`, read by rung name by `events_compare.py`. |
| 10.16 | `osn_tracks/` | 2026-08-10 | Pipeline step 02. V7's early ladder rungs read the raw table deliberately. |
| 9.86 | `osn_tracks_clean/` | 2026-08-12 | Pipeline step 02a; what the flight list reads from v4.0.0 on. |
| 6.17 | `research/tracks_clean/` | 2026-08-12 | V7's 2024 period. **Regenerable** — `clean_tracks.py --period 2024` from `research/tracks`. |
| 1.29 | `research/trend_votes_agl{,_2024}/` | 2026-08-22 | Not referenced by any committed code as of this writing — live work in progress. |
| 1.20 | `research/trend_votes{,_2024}/` | 2026-08-12 | Read by both `regenerate_v6.py` and `regenerate_v7.py`. |
| 0.54 | `research/reference/` | 2026-08-13 | Ground-truth mirror, so remote executors can read it. |

The ~40 `research/flight_list_v{6,7}_*` prefixes are 0.01 GB each, roughly 0.3 GB
in total — not worth pruning.

The ADEP/ADES version 4 study ran the pipeline itself rather than a private
harness, so several of these were rewritten by that run and no longer hold what
earlier notes here described. The column says which.

| Prefix | Size | Objects | Contents |
|---|---|---|---|
| `opdi/osn_statevectors_v2/` | — | — | **Deleted 2026-08-12 with approval.** Step 01's output for 2025-06-05/06/07: bbox- and 5 s-reduced at ingest, so the raw global 1 s feed was never persisted. It is consumed only by step 02, which was complete, and it is regenerable by re-ingesting from the OpenSky archive (about 40 minutes). Deleted to make room for the cleaned track tables. |
| `opdi/osn_tracks_clean/` | ~10 GB | — | **New 2026-08.** Step 02a output: `osn_tracks` with implausible values masked to NULL and rows preserved, plus `segment_id` at coverage gaps. This is what the flight list reads from v4.0.0 onward — before that, step 02a wrote a table nothing consumed. |
| `opdi/osn_tracks/` | 10.16 GB | 5 | **Rewritten 2026-08**, same three days. Full track schema incl. `track_id`, `h3_res_7`, `h3_res_12`, `*_altitude_c`. This is *not* the 2025-08-01 snapshot earlier revisions of this file described; that one was deleted. |
| `opdi/h3_airport_detection_zones/` | 0.22 GB | 152 | **Regenerated 2026-08.** 33,751,619 (aerodrome, cell) rows for the 1,353 large and medium aerodromes in the bbox. H3 res-7 cells in concentric bands — 5 NM steps to 40 NM, then 10 NM steps to 110 NM — with `min_c_radius_nm` / `max_c_radius_nm` on every row, so a consumer picks its own radius at read time. Reaches 110 NM so the same table can serve ASMA 40/100 NM ring crossings. |
| `opdi/opdi_endpoint_candidates/` | 0.16 GB | 20 | **New 2026-08.** 7,491,655 rows. For each track's first and last sample, every aerodrome whose zone contains it, with the exact great-circle distance, field elevation and height above it. Written by `pipeline/flights.py:build_endpoint_candidates` with `mode="overwrite"` — it appended in an early revision and silently doubled itself. This cache is what makes a threshold sweep a filter rather than a pipeline run. |
| `opdi/opdi_flight_list/` | 0.02 GB | 7 | Flight list output. |
| `opdi/opdi_flight_events/` | 0.30 GB | 7 | Published flight events. |
| `opdi/opdi_measurements/` | 0.34 GB | 4 | Published measurements. |
| `opdi/hexaero_airport_layouts/` | 0.01 GB | 22 | HexAero airport ground layouts (step 00b). |
| `opdi/osn_aircraft_db/` | 0.01 GB | 6 | OpenSky aircraft database incl. `icao_aircraft_class`. |
| `opdi/oa_airports/`, `oa_runways/`, `oa_navaids/`, `oa_regions/`, `oa_countries/`, `oa_airport_frequencies/` | 0.01 GB | 60 | OurAirports reference (step 00d). `oa_airports` carries the elevation the endpoint rule needs; the zone table does not. |
| `opdi/example_output/` | — | 3 | Example output. |

Gone, deliberately:

* `opdi/opdi_h3_airspace_ref/` — step 00c output, read by nothing in the
  codebase. Deleted; step 00c regenerates it if an airspace event ever needs it.
* `opdi/research/sv_bucket/`, `research/tracks_bucket/`, `research/cand_bucket/`
  — 6.83 GB, the decimation study's **experimental arm**. Deleted 2026-08-12
  with approval, and redundant rather than merely old: the bin-based sampler is
  now the production default, so `osn_tracks` *is* the bucket-sampled table and
  a second copy under a research name preserved nothing. The published
  decimation paper renders from committed CSVs and never read them. Only
  `benchmarks/decimation_end_to_end.py` and `arrivals_bucket_vs_trend.py`
  reference them, and neither is in the V6 or V7 chain.
* `opdi/research/statevectors/` — superseded. Step 01 has an `ingest_from_s3`
  path for the `opensky` environment that does what `osn_sample.py` was written
  to do, so research reads the pipeline's own state vectors.

Not this work:

* `opdi/osn_symposium_paper_2026/` — **42.81 GB, 309 objects** as of
  2026-08-12, written by a concurrent job and still growing. Recorded here so it
  is not mistaken for an unidentified prefix. Do not build on it or delete it.
  It is now the single largest thing in the bucket, well over half of the total,
  so any headroom calculation has to start by setting it aside.

---

## Compute

Spark runs **distributed on the OSN Kubernetes cluster**, not in the JupyterLab pod:
`k8s://https://192.168.60.102:6443`, namespace `eurocontrol`, image
`docker.io/quintengs/opdi-spark:v4.1.1-5`, client deploy mode with the driver in this pod.

### Sizing a research job

The namespace `ResourceQuota` is **30 CPU / 192 GiB**. The JupyterLab driver pod
holds 4 CPU / 16 GiB and each executor costs 2 CPU / 14 GiB (12 GiB heap + 2 GiB
overhead), so the ceiling is about **12 executors**. Request more and the extra
pods pend indefinitely rather than failing, which looks like a hung job.

The `opensky` environment defaults to 4 executors, which is right for the
production pipeline sharing the namespace. Batch backfills should ask for more
explicitly rather than changing that default:

```bash
python benchmarks/osn_sample.py 2024-06-01 2024-07-01 --executors 10
```

Check headroom before scaling up — the quota is shared:

```bash
kubectl get resourcequota -n eurocontrol
```

### The bucket has a quota

Writes fail with `AWSBadRequestException: ... Bucket quota exceeded` once the
bucket is full, and they fail **at the end of a job**, after the work is done
and the results have been printed to stdout but before they are persisted. A
whole chain can therefore appear to run and leave nothing behind.

The quota is around 100 GB. As of 2026-08-12 the bucket holds 67.40 GB, of
which 42.81 GB is another project's symposium data -- so OPDI has about 24 GB
in use and rather less headroom than the raw total suggests.

Track builds are the expensive artefact (~10 GB per 3-day sample per variant).
Since v4.0.0 there are two per period, raw and cleaned, and the cleaned one is
what the flight list reads.

Check before a long run:

```python
tot = sum(o["Size"] for p in s3.get_paginator("list_objects_v2")
          .paginate(Bucket="eurocontrol") for o in p.get("Contents", []))
```

### Deleting objects

The OpenSky endpoint rejects S3 batch `DeleteObjects` with
`MissingContentMD5` — it requires a `Content-Md5` header that botocore does not
send. Single-object `delete_object` calls are unaffected:

```python
for k in keys:
    s3.delete_object(Bucket="eurocontrol", Key=k)
```

This matters because the benchmark harness is idempotent: `_materialise_tracks`
skips any day whose `_SUCCESS` marker exists. A failed delete therefore does not
look like a failure — the next run quietly reuses the old data and produces
results identical to the ones you were trying to replace. If a re-run returns
byte-identical numbers after a code change, check the object timestamps before
believing them.

### One job at a time

**Two distributed Spark jobs cannot run concurrently from this pod**, however
much quota is free. The driver runs in client mode inside the JupyterLab pod
and executors reach it through the `jupyterlab` Service, which routes exactly
two ports:

```console
$ kubectl get svc jupyterlab -n eurocontrol
spark              7078 -> 7078
spark-blockmanager 7079 -> 7079
```

`spark.driver.port` is therefore fixed by the cluster, not chosen by the job.
A second job cannot bind 7078; moving it to a free port fails differently,
because the Service does not route the new port and every executor dies unable
to reach the driver:

```
ERROR ExecutorPodsLifecycleManager: Max number of executor failures (8) reached
java.lang.IllegalStateException: Spark context stopped while waiting for backend
```

That reads like a quota or cluster fault and is neither. Both failure modes
cost about the same amount of time to misdiagnose, so: **run jobs
sequentially**, and give a backfill more `--executors` rather than starting a
second job. Genuine concurrency would need extra ports added to the Service,
which is a change to the JupyterLab deployment.

`--ui-port` remains useful for a different reason: Spark silently falls back
from 4040 to 4041 when the port is taken, and the proxy path is hard-coded to
one port, so a job started while a stale UI is bound would otherwise serve its
UI where nothing links to it.

This is not optional. The JupyterLab pod is capped at **16 GB** (`/sys/fs/cgroup/memory.max`)
while `free(1)` reports the host's 251 GB. In `local[*]` mode every executor task runs inside
that cap and the JVM is OOM-killed with no crash dump — py4j reports only "Answer from Java
side is empty", which looks like a network fault.

**The driver's pyspark must match the cluster image exactly.** pyspark 4.2.0 against the 4.1.1
image fails with `InvalidClassException: local class incompatible` once executors start doing
real work. Pinned to `pyspark==4.1.1`.

## Research (written by this work)

All under `opdi/research/`, deliberately separate from the pipeline prefixes above.

| Prefix | Size | Produced by | Contents | From |
|---|---|---|---|---|
| `opdi/research/flight_list_trend/` | 0.01 GB | `flights.py:process_dai(mode="trend")` | 113,999 rows. The production altitude-trend algorithm, now reading H3 candidates with exact distances. | v4, 2025-06-05/07 |
| `opdi/research/flight_list_nearest/` | 0.02 GB | `process_dai(mode="nearest")` | 233,144 rows. Nearest aerodrome by effective distance, never abstains. | v4, 2025-06-05/07 |
| `opdi/research/flight_list_endpoint/` | 0.01 GB | `process_dai(mode="endpoint")` | 233,144 rows. Nearest, but only within `radius_nm` and below `height_ft` above field elevation; otherwise out-of-area or nothing. | v4, 2025-06-05/07 |
| `opdi/research/flight_list_v6_{legacy,trend,endpoint,nearest,combined,recommended}/` | 0.10 GB total | `benchmarks/flight_list_v6.py` | Six flight lists built by `process_dai` itself at the parameters V6 recommends, so the tuned numbers come from the pipeline rather than from a sweep harness. `legacy` is the pre-V6 constants and reproduces the published production figures (68.72% / 98.82% ADEP); `recommended` is what now ships. | v6, 2025-06-05/07 |
| `opdi/research/flight_list_v6_path{0..4}_*/` | 0.06 GB total | `benchmarks/flight_list_v6.py --runs path0_legacy ...` | The arrival tuning walked one parameter at a time through the pipeline. This is what showed the harness's flight-level cap losing ground in production, and what caught the scheduled-service penalty being inert. | v6, 2025-06-05/07 |
| `opdi/research/tracks_clean/` | 6.17 GB | `benchmarks/clean_tracks.py --period 2024` | **New 2026-08.** The second period after step 02a cleaning, with `h3_res_7` attached on the way out -- these tracks pre-date step 02's indexing and the flight list's `trend` path joins aerodrome zones on it. Same treatment as `osn_tracks_clean` gives 2025, which is what lets the two periods be compared at all. | v7, 2024-06-05/07 |
| `opdi/research/cand_2024/` | 0.13 GB | `benchmarks/build_candidates_2024.py` | **Rebuilt 2026-08 from cleaned tracks.** 5,430,694 endpoint candidates for the second period, built by the pipeline's own `build_endpoint_candidates` rather than a reimplementation. Read by the 2024 `endpoint` runs, which redirect the pipeline's table name to it. | v7, 2024-06-05/07 |
| `opdi/research/tracks_2024_ends/` | 0.03 GB | `benchmarks/build_candidates_2024.py` | 306,366 rows: first and last sample per track for the second period, H3-indexed. Exists so the candidate builder can run over two rows per track instead of indexing 12 GB to reach them. | v7 |
| `opdi/research/flight_list_v7_*/` | ~0.15 GB total | `benchmarks/flight_list_v7.py` | One flight list per run of the V7 study -- eleven ladder rungs, five whole configurations, three rejected changes, and a shared `_grid` prefix the fourteen grid cells overwrite in turn. Every one written by `process_dai` itself. | v7, both periods |
| `opdi/research/reference/` | 0.28 GB | uploaded from `reference/` | Ground-truth mirror. The git-lfs copy is on the driver's local disk, which remote executors cannot read. | done |
| `opdi/research/tracks/aircraft=known/day=.../` | 11.93 GB | `benchmarks/adep_ades.py` | Tracks via the frozen `_add_track_id`, aeroplanes only (`icao_aircraft_class` starting L or A). Rebuilt only when the day is absent. | v1–v3 |
| `opdi/research/adep_ades/results/<tag>/` | 0.00 GB | `benchmarks/adep_ades.py` | Per-method coverage/accuracy, one row per method. `<tag>` is `<airport_set>_r<radius>_fl<max_fl>`. | v1–v3, 7 methods |
| `opdi/research/adep_ades/cascade_diag/<tag>_<ladder>_vs_<control>/` | 0.00 GB | `--diagnose-cascade` | Per-rung attribution for M6: how many flights each rung answered, its accuracy on them, and the control's accuracy on the same flights. | v1–v3 |
| `opdi/research/adep_ades/abstain_sweep/<airport_set>/` | 0.00 GB | `--sweep-abstain` | M7 coverage/accuracy over the endpoint distance x height grid (8 x 6 = 48 rows). | v1–v3 |

::: warning
**Do not prune `research/tracks/`.** An earlier revision of this file
recommended it as the first thing to delete, on the grounds that version 4 did
not read it. Versions 6 and 7 do: it is the **second period**, 2024-06-05/07,
and it is the only sample any parameter is validated against. It is also no
longer rebuildable from state vectors on this bucket, because the 2024 state
vectors were never ingested here.
:::

### Reproducing version 4

```bash
RANGE="--env opensky --start 2025-06-05 --end 2025-06-08"
opdi run $RANGE --step 00a   # zones: 5 NM bands to 110 NM
opdi run $RANGE --step 01    # state vectors, bbox and 5 s applied at ingest
opdi run $RANGE --step 02    # tracks, with H3 res 7 and 12
```

```python
from datetime import date
from opdi.pipeline.flights import FlightListProcessor

fp = FlightListProcessor(spark, config)
fp.build_endpoint_candidates(date(2025, 6, 1))    # the cache; write it once
for mode in ("trend", "nearest", "endpoint"):
    fp.process_dai(date(2025, 6, 1), mode=mode, skip_if_processed=False,
                   abstention_radius_nm=40, abstention_height_ft=15000,
                   sched_penalty_nm=10,
                   table_name=f"research/flight_list_{mode}",
                   write_mode="overwrite")
```

```bash
python benchmarks/benchmark_modes.py --months 202506 \
    --days 2025-06-05 2025-06-06 2025-06-07 --local --results-dir <dir>
```

The benchmark takes `--local` because its inputs are small — 7.5M cached
candidates, 233k flight records, 95k ground-truth flights. Running it on the
cluster costs a slot the namespace does not have to spare. Add `--skip-sweeps`
to get the headline table and both cross-checks without the 90-cell grid; that
is a couple of minutes rather than a quarter of an hour.

⚠️ **`PYSPARK_PYTHON` is per-mode, not per-repo.** On the cluster it must be
`python3` — the image's interpreter, since this venv's path does not exist
there. In local mode the workers run on this machine, so the same value picks
up the system 3.13 against a 3.10 driver and every task dies with
`PYTHON_VERSION_MISMATCH`. `_build_spark_local` now overrides it to
`sys.executable`, so only the distributed path needs the env var set.

### Why these months

`2024-06` and `2025-06` match the committed ground truth in `opdi/reference/`
(`apdf_202406`, `flights_202406`, `apdf_202506`, `flights_202506`). Two separate months allow
methodology weights to be fitted on one and validated on the other, which is the only honest
way to report a tuned combination.

### Driver environment

The driver must match the cluster image on **both** axes, or jobs fail after
scheduling successfully:

| | Cluster image `quintengs/opdi-spark:v4.1.1-5` | Driver |
|---|---|---|
| Spark/pyspark | 4.1.1 | `pyspark==4.1.1` (4.2.0 -> `InvalidClassException`) |
| Python | 3.10 | `.venv310` (3.13 -> `PYTHON_VERSION_MISMATCH`) |

`.venv310` is built with `uv venv --python 3.10`. Run jobs as:

```bash
PYSPARK_DRIVER_PYTHON=$PWD/.venv310/bin/python PYSPARK_PYTHON=python3 \
  .venv310/bin/python benchmarks/adep_ades.py --days 2025-06-05 --months 202506
```

`PYSPARK_PYTHON=python3` matters: it names the interpreter *on the executors*.
Pointing it at the driver's venv path makes executors fail with
`Cannot run program "./.venv310/bin/python"` — that path exists only here.

Python workers are unavoidable even with no UDFs: `spark.createDataFrame` on a
Python list (the airport cell offsets) is enough to spawn them.

## Ground truth semantics

Settled empirically for Task 4 (`track_truth.py`), against `reference/flights_202506.parquet`
(957,396 rows) and `reference/apdf_202506.parquet` (612,395 DEP rows, similar ARR count), by
joining NM to APDF on `(callsign, day, aerodrome)` — **not** on callsign alone across the
whole month. Callsign alone is close to a cross join (~600k x 957k rows) and can exhaust a
16 GB pod's memory before it prints anything; adding a day key (and, for the tighter check,
the aerodrome) brings it down to an ordinary merge.

**`TAXI_TIME_3` is taxi-out time only** (off-block `AOBT_3` to actual take-off), not total
(out + in) taxi time:

- `AOBT_3 + TAXI_TIME_3` predicts the real ATOT (APDF DEP `MVT_TIME_UTC`) with **median error
  0 s and IQR 17 s** (462,676 distinct callsign/day keys, 499,497 merged rows on a
  callsign+day join; IQR tightens to 14 s when the join also keys on `ADEP`). 93.5% of matched
  rows land within +-300 s, 93.8% within +-1 h — the remaining tail is callsign-reuse /
  multi-leg join noise, not inference error.
- `ARVT_3` is *already* a landing time (ALDT-like), not a gate/in-block arrival time: against
  real APDF ALDT (ARR `MVT_TIME_UTC`, joined on `(callsign, day, ADES)`) it matches with
  median error 0 s and IQR 25 s (467,868 merged rows). This is also why
  `(ARVT_3 - AOBT_3) - FLT_DUR_3` reproduces `TAXI_TIME_3`'s own distribution almost exactly
  (means 12.396 vs 12.399 minutes) — there is no separate taxi-in term hiding inside `ARVT_3`.

**Conclusion:** the ~120 s IQR threshold this question was checked against is not crossed —
17 s and 25 s are both roughly an order of magnitude under it. NM-inferred boundary times
(`AOBT_3 + TAXI_TIME_3` for take-off, `ARVT_3` for landing) are therefore precise enough to
use **both for matching and for boundary error**, not restricted to APDF-covered aerodromes
only. `track_truth.py` still stamps `t_source` (`"apdf"` vs `"nm_inferred"`) on every row of
`load_flight_intervals`, so a later study can revisit this per airport if some subgroup turns
out not to hold — but the blanket "boundary error only at APDF airports" fallback is not
needed for this dataset.

### Reproducing these numbers

Both sides run against the committed reference parquet, no cluster and no credentials. Per the
project's provenance rule, a number without a recipe is unverified rather than fact — this is
that recipe, for both the departure-side number and the arrival-side one.

**Departure side** (`AOBT_3 + TAXI_TIME_3` vs real ATOT). The naive `dep.merge(nm,
left_on="AP_C_FLTID", right_on="AIRCRAFT_ID")` merges on callsign alone across the whole
month — close to a cross join (612k x 957k rows) that can exhaust a 16 GB pod before it prints
anything. Add a day key to both sides first:

```bash
.venv310/bin/python - <<'PY'
import pandas as pd
nm = pd.read_parquet("reference/flights_202506.parquet",
                      columns=["AIRCRAFT_ADDRESS", "AIRCRAFT_ID", "ADEP", "ADES",
                               "AOBT_3", "ARVT_3", "TAXI_TIME_3"])
ap = pd.read_parquet("reference/apdf_202506.parquet",
                      columns=["AP_C_FLTID", "ADEP_ICAO", "ADES_ICAO",
                               "SRC_PHASE", "MVT_TIME_UTC"])
dep = ap[ap.SRC_PHASE == "DEP"].copy()
dep["_day"] = dep["MVT_TIME_UTC"].dt.date
nm = nm.copy()
nm["_day"] = nm["AOBT_3"].dt.date
d = dep.merge(nm, left_on=["AP_C_FLTID", "_day"], right_on=["AIRCRAFT_ID", "_day"],
              how="inner")
d["atot_hat"] = d.AOBT_3 + pd.to_timedelta(d.TAXI_TIME_3, unit="m")
err = (d.atot_hat - d.MVT_TIME_UTC).dt.total_seconds()
print("ATOT inference error (s):", err.describe())
print("IQR (s):", err.quantile(0.75) - err.quantile(0.25))
PY
```

**Arrival side** (`ARVT_3` vs real ALDT) — the number that unlocks boundary error across the
whole ECAC sample instead of only APDF airports. Same day-key hazard applies; join on
`(callsign, day, ADES)` for the tighter, less noisy check:

```bash
.venv310/bin/python - <<'PY'
import pandas as pd
nm = pd.read_parquet("reference/flights_202506.parquet",
                      columns=["AIRCRAFT_ADDRESS", "AIRCRAFT_ID", "ADEP", "ADES",
                               "AOBT_3", "ARVT_3", "TAXI_TIME_3"])
ap = pd.read_parquet("reference/apdf_202506.parquet",
                      columns=["AP_C_FLTID", "ADEP_ICAO", "ADES_ICAO",
                               "SRC_PHASE", "MVT_TIME_UTC"])
arr = ap[ap.SRC_PHASE == "ARR"].copy()
arr["_day"] = arr["MVT_TIME_UTC"].dt.date
nm = nm.copy()
nm["_arr_day"] = nm["ARVT_3"].dt.date
d = arr.merge(nm, left_on=["AP_C_FLTID", "_day", "ADES_ICAO"],
              right_on=["AIRCRAFT_ID", "_arr_day", "ADES"], how="inner")
err = (d.ARVT_3 - d.MVT_TIME_UTC).dt.total_seconds()
print("ARVT_3 vs real ALDT error (s):", err.describe())
print("IQR (s):", err.quantile(0.75) - err.quantile(0.25))
PY
```

Both are throwaway pandas diagnostics reading committed files on the driver -- the "everything
is native Spark" rule governs `track_truth.py` itself, not this kind of one-off probe.
