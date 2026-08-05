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
