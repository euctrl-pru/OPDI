# Running OPDI with Airflow

This folder holds the Airflow set-up of OPDI, the Open Performance Data
Initiative. It is written for the person who operates the OpenSky Airflow
instance (Airflow 2.7.2).

- `dags/opdi_pipeline.py`: the DAG file. A DAG (directed acyclic graph) is a
  set of tasks that Airflow runs in a fixed order.

## What it does

OPDI turns the position reports that aircraft broadcast (ADS-B, collected by
the OpenSky Network) into two published files per day:

- `flight_list_YYYYMMDD.parquet`: one row per flight that started that day.
- `flight_events_YYYYMMDD.parquet`: one row per event of those flights (for
  example take-off, landing, the start of a level segment), with the event's
  measurements.

A day is built by seven pipeline steps, run in this order:

| Task          | Step | What it does                                                     |
|---------------|------|------------------------------------------------------------------|
| `ingest`      | 01   | Fetches the position reports of the day before, the day and the day after. |
| `tracks`      | 02   | Splits the reports into one track (sequence of positions) per flight. |
| `clean`       | 02a  | Removes values that cannot be trusted.                           |
| `flight_list` | 03   | Builds the flight list: departure and destination airports.      |
| `events`      | 04   | Detects the events and their measurements.                       |
| `milestones`  | 04b  | Adds the main times (take-off, landing, off-block, in-block) to the flight list. |
| `publish`     | 09   | Writes the day's two published files, the monthly index and the catalogue. |

Before them, `reference` builds the reference tables once (airports,
runways, aircraft); later runs see they exist and do nothing. `check_space`
stops the day when the bucket is nearly full. After them, `prune` removes
the internal tables that are no longer needed.

The file defines two DAGs:

- **`opdi_daily`** runs every day at 06:00 UTC and processes the day two days
  before the run date (Variable `opdi_lag_days`). The delay is needed because
  a flight that starts late in the evening ends the next day, so a day can
  only be built once the day after it is complete. Each step is its own task,
  so the Airflow view shows where a day stands.
- **`opdi_backfill`** is started by hand and processes a range of days, one
  day at a time, newest first by default. Each day is one task that runs all
  its steps.

Every task runs a shell command, `python -m opdi.orchestration ...`. The
DAG file contains no OPDI code and imports only Airflow.

## Where the data goes

All data is written to one warehouse (a storage prefix), by default
`s3a://eurocontrol/opdi`. Each release has its own folder in it, the release
root, `s3a://eurocontrol/opdi/v2.0.0/`:

| Folder        | Holds                                                                 |
|---------------|-----------------------------------------------------------------------|
| `published/`  | The published files: `flight_list/`, `flight_events/`, `index/`, `catalogue.parquet`. |
| `_state/`     | Which units are finished (`done/`) and the steps' progress logs (`logs/`). |
| other folders | Internal tables: reference tables and the working tables of each step. |

Nothing outside the release root is read, changed or deleted.

Because the state is kept in the warehouse, not on a worker's disk, any
worker can continue a day that another worker started, and running a
command again skips the work that is already finished.

## Prerequisites

The commands need a machine (or container) that has:

1. **The OPDI package** installed with its dependencies, in Python 3.10 or
   later: `pip install /path/to/opdi` or `pip install -e /path/to/opdi`
   from a checkout. Check with `python -m opdi.orchestration plan --start
   2025-01-01 --end 2025-01-02`, which needs no Spark and prints two dates.
2. **Java 17** and network access to the OpenSky S3 endpoint
   (`https://s3.opensky-network.org`) and to the Kubernetes cluster that runs
   the Spark executors (`https://192.168.60.102:6443`, namespace
   `eurocontrol`).
3. **A host name the executors can reach.** The Spark driver runs where the
   command runs, and the executors connect back to it under the host name
   `jupyterlab` (`SparkConfig.k8s_driver_host`). In practice the commands
   therefore run inside the JupyterLab pod that hosts the driver, reached
   through the command prefix (see below).
4. **S3 credentials** in the environment, `AWS_ACCESS_KEY_ID` and
   `AWS_SECRET_ACCESS_KEY`, or in a `.env` file (see `.env.template` in the
   OPDI repository). The `.env` file is looked for first in the directory the
   command runs from, then, when OPDI runs from a checkout of the repository,
   at the root of that checkout; the first one found is read. A variable that
   is already set in the environment is never replaced by the file.
5. **At least 16 GB of memory** for the driver, and only one OPDI command at
   a time on that machine. The driver's memory is sized from what the machine
   has free when the command starts.
6. **For the first reference build only:** an OpenStreetMap extract of the
   world's aerodromes, named by the environment variable `OPDI_OSM_PBF`. The
   reference task refuses to start without it.

Airflow itself needs nothing beyond its core: the DAG file uses only
`BashOperator`, `PythonOperator` and `EmptyOperator`.

## Installing the DAG

Copy `dags/opdi_pipeline.py` into the Airflow DAG folder. Both DAGs appear
paused; set the Variables first.

## Variables

Set them under Admin > Variables. All are optional.

| Variable              | Default   | Meaning |
|-----------------------|-----------|---------|
| `opdi_command_prefix` | empty     | Text placed before `python -m opdi.orchestration` in every command. It says where OPDI runs. Examples: `kubectl exec -n eurocontrol <driver-pod> --` (run inside the pod that hosts the Spark driver), `source /opt/opdi/.venv/bin/activate &&` (a virtual environment on the worker). |
| `opdi_warehouse`      | empty     | The warehouse. Empty means the `OPDI_WAREHOUSE` environment variable where the command runs, else `s3a://eurocontrol/opdi`. |
| `opdi_min_free_gb`    | `5`       | Free space, in GB, a day needs before it starts. |
| `opdi_lag_days`       | `2`       | How many days the daily run stays behind the run date. |
| `opdi_keep_internal`  | `false`   | `true` keeps the internal tables of each day after publishing (needs much more space). |
| `opdi_keep_working`   | `true`    | `false` also removes the working events and measurements of days that are already published, except the 7 days on each side of the day just run. See "How disk space is protected". |

Every command prints the warehouse, the release root and the coverage when
it starts, and warns when `OPDI_WAREHOUSE` points elsewhere. Check the first
lines of a task log after any change.

## Starting the daily run

1. Set the Variables.
2. Unpause `opdi_daily`.

The first run builds the reference tables (one to several hours), then the
day. Later runs take the time of one day. Days that were missed while the DAG
was paused are not caught up by themselves (`catchup` is off): run them with
`opdi_backfill`.

## Running the first backfill

The first backfill covers 2025-01-01 up to the latest complete day, newest
first.

1. **Pause `opdi_daily`** while a backfill runs. Two runs at the same time
   would share the internal tables of the position reports, and step 01 of
   one would remove the days the other needs.
2. Open `opdi_backfill`, choose "Trigger DAG w/ config" and set:

   ```json
   {"start_date": "2025-01-01", "end_date": "", "order": "newest_first"}
   ```

   An empty `end_date` means the day the daily run would process today
   (today minus `opdi_lag_days`).
3. The DAG builds the reference tables (`reference`), lists the days
   (`plan_days`) and then runs one `process_day` task per day, one at a time.
   Each `process_day` checks the free space, runs the seven steps and removes
   the internal tables that are no longer needed.
4. When it is done, unpause `opdi_daily`.

The same can be done without Airflow, one day at a time:

```bash
python -m opdi.orchestration reference
python -m opdi.orchestration day --day 2026-11-09
```

## Extending back to 2019

Trigger `opdi_backfill` again with the older range, for example:

```json
{"start_date": "2019-01-01", "end_date": "2024-12-31", "order": "newest_first"}
```

Each `process_day` first reads which steps of its day are finished, with a
small local Spark session, and skips a finished day in well under a minute.
A range that overlaps the first backfill therefore costs little.

## Changing the warehouse

Set the Variable `opdi_warehouse` (for every run) or the backfill parameter
`warehouse` (for one run), for example `s3a://eurocontrol/opdi-test`. The
release root is then `s3a://eurocontrol/opdi-test/v2.0.0/`.

The coverage (worldwide or the European box) is recorded in the release
root when the reference tables are built. A command with the other coverage
is refused, with a message naming both, rather than mixing the two in one
place.

## When a day fails

1. Open the log of the failed task. The last lines name the step, the day
   and the error.
2. Fix the cause (for example credentials, free space, a cluster that was
   down). Airflow already retries each task twice, ten minutes apart.
3. Clear the failed task in Airflow. It runs again and skips every step that
   is already finished, so the day continues where it stopped.
4. To build a finished step again (for example after a correction), run it
   by hand with `--force`:

   ```bash
   python -m opdi.orchestration step --step 04 --day 2026-11-09 --force
   python -m opdi.orchestration step --step 09 --day 2026-11-09 --force
   ```

   Publishing a day again replaces its two files, its index rows and its
   catalogue row; readers never see a half-written file.

A failed day in a backfill does not stop the other days. Run the failed
days again afterwards with a backfill over just those days, or with
`python -m opdi.orchestration day --day ...`.

## How disk space is protected

The bucket has a quota (200 GB by default; set `OPDI_BUCKET_QUOTA_GB` where
the commands run if it differs). S3 reports no free space of its own, so the
free space is the quota minus the bytes stored in the whole bucket.

- **Before each day** `check_space` (or the start of `process_day`) stops
  when less than `opdi_min_free_gb` GB are free. Nothing is written in that
  case.
- **After each day** `prune` removes the day folders of the three largest
  internal tables (position reports `osn_statevectors_v2`, tracks
  `osn_tracks` and cleaned tracks `osn_tracks_clean`) that lie outside a
  window from one day before to two days after the day just published. That
  window holds what the next day needs, whichever order the days run in.
- **Kept by default, removed on request:** the working events and
  measurements tables (`opdi_flight_events`, `opdi_measurements`). Keeping
  them lets a day be published again without running steps 04 and 04b
  again, but they take about three times the space of the published files.
  Set the Variable `opdi_keep_working` to `false` (or pass
  `--drop-published-working` to the `prune` and `day` commands) to remove
  their day folders once a day is published. A day folder is removed only
  when both published files of that day and its catalogue row exist, and
  only when the day is more than 7 days (`--working-keep-days`) before or
  after the day just run. A day that is not yet published is never touched.
  To publish a removed day again, run steps 04, 04b and 09 for it again.
- **Never removed:** the published files, the reference tables, the
  progress records and the working flight list (`opdi_flight_list`), which
  the flight list of later days is built against.
- With `opdi_keep_internal` set to `true` nothing is removed, whatever
  `opdi_keep_working` says; plan for about 20 GB per worldwide day in that
  case, mostly tracks.

To remove the working events and measurements of every published day older
than a week in one go, without running a day:

```bash
python -m opdi.orchestration prune --day 2026-11-09 --drop-published-working
```

To see what is used:

```bash
python -m opdi.orchestration check-space --min-free-gb 0
```

## What a day costs

Measured by publishing one worldwide day (14 June 2026) from existing working
tables on the OpenSky cluster with six executors:

| Item | Value |
|---|---|
| Flights in the flight list | 175,615 |
| Events in the merged file | 6,773,617 |
| Flight-list file | 7.3 MB |
| Merged flight-events file | 411 MB (68 row groups of 100,000 rows) |
| Monthly flight index, one day in it | 4.4 MB |
| Time for the publishing step alone | 228 s |
| Reading all 27 events of one flight by `flight_id` | 1 of 68 row groups read, about 1 s |

What this means for the bucket:

- **Published files:** about 0.42 GB per day, so about 155 GB per year of
  worldwide data.
- **Working tables** (flight list, events, measurements): about 1.3 GB per
  day on top of that when they are kept, which is the default. Almost all of
  it is events and measurements; with `opdi_keep_working` set to `false`
  only the working flight list stays for days older than a week, a small
  part of the 1.3 GB.
- So a worldwide day costs about 1.7 GB with the default settings and a
  little over 0.42 GB with `opdi_keep_working` set to `false`.
- The first backfill (January 2025 to November 2026, about 680 days) therefore
  needs roughly 290 GB for the published files alone, and about 1.2 TB with
  the working tables kept. Both are more than a 200 GB quota. Raise the
  quota, or publish to a larger warehouse (see "Changing the warehouse"),
  and set `opdi_keep_working` to `false`, before starting it.

In that check 3,442 events (0.05%) belonged to tracks with no row in the
flight list. They are published with empty flight columns.
