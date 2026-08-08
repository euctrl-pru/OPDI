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

Measured 2026-08-08: **42.69 GB** across the whole bucket.

The ADEP/ADES version 4 study ran the pipeline itself rather than a private
harness, so several of these were rewritten by that run and no longer hold what
earlier notes here described. The column says which.

| Prefix | Size | Objects | Contents |
|---|---|---|---|
| `opdi/osn_statevectors_v2/` | 6.13 GB | 469 | **Rewritten 2026-08.** 142,630,930 rows for 2025-06-05/06/07, already reduced to the Europe bbox and 5 s at ingest — step 01 applies both filters before anything is written, so the raw global 1 s feed is never persisted. |
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
* `opdi/research/statevectors/` — superseded. Step 01 has an `ingest_from_s3`
  path for the `opensky` environment that does what `osn_sample.py` was written
  to do, so research reads the pipeline's own state vectors.

Not this work:

* `opdi/osn_symposium_paper_2026/` — 13.09 GB, 86 objects, written by a
  concurrent job. Recorded here so it is not mistaken for an unidentified
  prefix. Do not build on it or delete it.

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

Production is ~48 GB and the quota is around 100 GB, so research output has to
be pruned rather than accumulated. Track builds are the expensive artefact
(~4 GB per 3-day sample per filter variant) and are all rebuildable from the
state vectors, which are the thing worth keeping.

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
| `opdi/research/reference/` | 0.28 GB | uploaded from `reference/` | Ground-truth mirror. The git-lfs copy is on the driver's local disk, which remote executors cannot read. | done |
| `opdi/research/tracks/aircraft=known/day=.../` | 11.93 GB | `benchmarks/adep_ades.py` | Tracks via the frozen `_add_track_id`, aeroplanes only (`icao_aircraft_class` starting L or A). Rebuilt only when the day is absent. | v1–v3 |
| `opdi/research/adep_ades/results/<tag>/` | 0.00 GB | `benchmarks/adep_ades.py` | Per-method coverage/accuracy, one row per method. `<tag>` is `<airport_set>_r<radius>_fl<max_fl>`. | v1–v3, 7 methods |
| `opdi/research/adep_ades/cascade_diag/<tag>_<ladder>_vs_<control>/` | 0.00 GB | `--diagnose-cascade` | Per-rung attribution for M6: how many flights each rung answered, its accuracy on them, and the control's accuracy on the same flights. | v1–v3 |
| `opdi/research/adep_ades/abstain_sweep/<airport_set>/` | 0.00 GB | `--sweep-abstain` | M7 coverage/accuracy over the endpoint distance x height grid (8 x 6 = 48 rows). | v1–v3 |

**Prune `research/tracks/` first.** At 11.93 GB it is by far the largest research
artefact, it belongs to versions 1–3, and it is fully rebuildable from the state
vectors. Version 4 does not read it — the pipeline builds its own tracks.

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
cluster costs a slot the namespace does not have to spare.

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
