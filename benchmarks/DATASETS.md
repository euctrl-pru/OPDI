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

## Production (pre-existing — **do not overwrite**)

Written 2026-06-02, ~48 GB total across 15,010 objects.

| Prefix | Size | Contents |
|---|---|---|
| `opdi/osn_tracks/` | 40.04 GB | **838,853,387 rows, a single day: 2025-08-01.** Full track schema incl. `track_id`, `h3_res_7`, `h3_res_12`, `*_altitude_c`. One unpartitioned parquet file, 287 row groups. |
| `opdi/osn_statevectors_v2/` | 2.17 GB | Raw state vectors, 167 objects. |
| `opdi/opdi_flight_list/` | 0.01 GB | Flight list output. |
| `opdi/opdi_flight_events/` | — | Published flight events. |
| `opdi/opdi_measurements/` | — | Published measurements. |
| `opdi/h3_airport_detection_zones/` | 0.09 GB | Airport H3 res-7 detection zones (step 00a output). |
| `opdi/hexaero_airport_layouts/` | — | HexAero airport ground layouts (step 00b). |
| `opdi/opdi_h3_airspace_ref/` | — | Airspace H3 reference (step 00c). Generated, read by nothing. |
| `opdi/osn_aircraft_db/` | 0.01 GB | OpenSky aircraft database incl. `icao_aircraft_class`. |
| `opdi/oa_airports/`, `oa_runways/`, `oa_navaids/`, `oa_regions/`, `oa_countries/`, `oa_airport_frequencies/` | 0.01 GB | OurAirports reference (step 00d). |
| `opdi/example_output/` | — | Example output. |

> `osn_tracks` covering only **2025-08-01** is the operative constraint on reuse: it does not
> overlap the 2024-06 / 2025-06 ground-truth extracts, so ADEP/ADES research cannot be run
> against it directly. It is still useful as a smoke-test dataset with real `track_id`s.

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

All under `opdi/research/`, deliberately separate from the production prefixes above.

| Prefix | Produced by | Contents | Status |
|---|---|---|---|
| `opdi/research/statevectors/day=YYYY-MM-DD/` | `benchmarks/osn_sample.py` | OSN state vectors from `s3a://opensky-hdfs-backup/tables_v4/state_vectors`, filtered to the OPDI Europe bbox `(-25.86653, 26.74617, 49.65699, 70.25976)` and decimated to 5 s — identical to `ingestion/osn_statevectors.py:_apply_filters`. ~1.5 GB/day. Schema renamed to OPDI snake_case. | in progress |

### Reproducing

```bash
python benchmarks/osn_sample.py 2025-06-05 2025-06-08     # [start, end)
```

Idempotent: a day whose `_SUCCESS` marker already exists is skipped, so an interrupted run
resumes rather than restarting.

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

| `opdi/research/reference/` | uploaded from `reference/` | Ground-truth mirror. The git-lfs copy is on the driver's local disk, which remote executors cannot read. | done |
| `opdi/research/tracks/aircraft=known/day=.../` | `benchmarks/adep_ades.py` | Tracks via the frozen `_add_track_id`, aeroplanes only (`icao_aircraft_class` starting L or A). Rebuilt only when the day is absent. | 2025-06-05 |
| `opdi/research/adep_ades/results/<tag>/` | `benchmarks/adep_ades.py` | Per-method coverage/accuracy, one row per method. `<tag>` is `<airport_set>_r<radius>_fl<max_fl>`. | 2025-06-05, 7 methods |
| `opdi/research/adep_ades/cascade_diag/<tag>_<ladder>_vs_<control>/` | `--diagnose-cascade` | Per-rung attribution for M6: how many flights each rung answered, its accuracy on them, and the control's accuracy on the same flights. | 2025-06-05 |
| `opdi/research/adep_ades/abstain_sweep/<airport_set>/` | `--sweep-abstain` | M7 coverage/accuracy over the endpoint distance x height grid (8 x 6 = 48 rows). The figure the paper's operating-point discussion is built on. | 2025-06-05 |
