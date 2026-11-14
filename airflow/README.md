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
runways, aircraft); later runs check that each table is there and not empty,
and build again only a table that is missing. `check_space`
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
   world's aerodromes, named by the environment variable `OPDI_OSM_PBF`, and
   the `osmium` package that reads it (`pip install "/path/to/opdi[reference]"`).
   The reference task refuses to start without the extract.

Airflow itself needs nothing beyond its core: the DAG file uses only
`BashOperator`, `PythonOperator` and `EmptyOperator`.

## Installing the DAG

Copy `dags/opdi_pipeline.py` into the Airflow DAG folder. Both DAGs appear
paused; set the Variables first.

Create the pool `opdi` with one slot, once:

```bash
airflow pools set opdi 1 "OPDI: one pipeline command at a time"
```

Every task of both DAGs runs in this pool, so only one OPDI command runs at a
time, whichever DAG it belongs to. The DAG file loads without the pool, but
Airflow does not start a task whose pool does not exist: the tasks stay in the
"scheduled" state until the pool is created.

Publishing also takes a lock file, `_state/publish.lock` in the release root,
so two publishes into one release never run at once even outside Airflow. A
publish that is stopped from outside (a killed process) leaves the lock. The
next publish then fails with a message; when the lock is older than six hours
the message says to remove it, after checking that no OPDI command is still
running, with `python -m opdi.orchestration clear-lock`.

## Variables

Set them under Admin > Variables. All are optional.

| Variable              | Default   | Meaning |
|-----------------------|-----------|---------|
| `opdi_command_prefix` | empty     | Text placed before `python -m opdi.orchestration` in every command. It says where OPDI runs. Examples: `kubectl exec -n eurocontrol <driver-pod> --` (run inside the pod that hosts the Spark driver), `source /opt/opdi/.venv/bin/activate &&` (a virtual environment on the worker). |
| `opdi_warehouse`      | empty     | The warehouse. Empty means the `OPDI_WAREHOUSE` environment variable where the command runs, else `s3a://eurocontrol/opdi`. |
| `opdi_min_free_gb`    | `25`      | Free space, in GB, a day needs before it starts. A worldwide day writes about 20 GB of internal tables (mostly tracks) before `prune` removes them. |
| `opdi_lag_days`       | `2`       | How many days the daily run stays behind the run date. At least 2: day D is built from the position reports of D-1 to D+1, and at 06:00 UTC on day R the reports of R-1 are not complete yet. A smaller value stops the run at its first task with a message. |
| `opdi_keep_internal`  | `false`   | `true` keeps the internal tables of each day after publishing (needs much more space). |
| `opdi_keep_working`   | `true`    | `false` also removes the working events and measurements of days that are already published, except the 7 days on each side of the day just run. See "How disk space is protected". |

Every command prints the warehouse, the release root and the coverage when
it starts, and warns when `OPDI_WAREHOUSE` points elsewhere. Check the first
lines of a task log after any change.

The warehouse and the other values are quoted for the shell in every
command, so each stays one argument. The backfill parameter `warehouse`
accepts only a storage address (`s3a://...`, `s3://...`, `file://...`) or
an absolute path, made of letters, digits and `. _ / -`. The command prefix
is not quoted: it is shell text on purpose.

**Stopping a command that runs through a prefix.** With a prefix such as
`kubectl exec ... --`, Airflow runs `kubectl` on the worker, and `kubectl`
runs the OPDI command in the pod. When a task reaches its time limit
(`execution_timeout`, 6 hours, 24 hours for a backfill day) or is marked
failed by hand, Airflow stops only the local `kubectl` process; the OPDI
command and its Spark job go on in the pod. Stop it there:

```bash
kubectl exec -n eurocontrol <driver-pod> -- pkill -f opdi.orchestration
```

If it was publishing, remove the lock it leaves with
`python -m opdi.orchestration clear-lock` (see "Installing the DAG").

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

Trigger `opdi_backfill` again for the older days, one year per run, newest
year first. One run holds at most 1,000 days (Airflow maps a task over at
most 1,024 values), and a longer range is refused by `plan_days` with a
message. For example, six runs, one after the other:

```json
{"start_date": "2024-01-01", "end_date": "2024-12-31", "order": "newest_first"}
{"start_date": "2023-01-01", "end_date": "2023-12-31", "order": "newest_first"}
{"start_date": "2022-01-01", "end_date": "2022-12-31", "order": "newest_first"}
{"start_date": "2021-01-01", "end_date": "2021-12-31", "order": "newest_first"}
{"start_date": "2020-01-01", "end_date": "2020-12-31", "order": "newest_first"}
{"start_date": "2019-01-01", "end_date": "2019-12-31", "order": "newest_first"}
```

The first backfill, 2025-01-01 to the latest complete day, stays under 1,000
days until September 2027; split it by year as well after that.

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

Every step records in the warehouse that it finished (`_state/done/`). Before
a step runs, it checks that the day folders it reads are there: step 02 reads
the position reports of the day before, the day and the day after; steps
02a, 03 and 04 read the day's tracks; steps 04b and 09 read the day's flight
list and events. A day that has started but is not yet published keeps these
folders: neither `prune` nor the step 01 of another day removes them.

**In the daily DAG** a failed task is retried twice, ten minutes apart. While
it waits for a retry the run is still running, and the next morning's run
waits (`max_active_runs=1`). When the task has failed for good, the next run
starts but stops at its first task, `previous_run_finished`, so that no later
day runs while a day is broken. To release it:

1. Open the log of the failed task. The last lines name the step, the day and
   the error. Fix the cause (credentials, free space, a cluster that was
   down).
2. Clear the failed task. It runs again, skips the steps that are finished
   and continues where the day stopped.
3. When that run has succeeded, clear `previous_run_finished` of each run
   after it that stopped there, oldest first. (Clearing the whole later run
   works as well.)

To give up on a day instead, mark its failed run as successful in Airflow and
process the day later with `opdi_backfill` or `day --day ...`.

**In the backfill DAG** a day is not retried (`retries=0`): a task that waits
for a retry does not count as running, so the next day would start in the
meantime. A failed day stays failed and the other days go on. Run the failed
days again afterwards, deliberately, with a backfill over just those days or
with `python -m opdi.orchestration day --day ...`.

What to run, by the step that failed (replace the day):

| Failed at | Run |
|---|---|
| any step, nothing removed since | Clear the task, or `python -m opdi.orchestration day --day 2026-11-09`. The day continues where it stopped. |
| any step, and the day's folders were removed since (for example by hand) | `python -m opdi.orchestration day --day 2026-11-09`. The day sees which folder is gone, clears its marks from the step that writes it and starts again from step 01, which fetches only the missing report days. |
| `publish` (09), with the message about a lock | A publish stopped without removing its lock. Check that no OPDI command is running, then `python -m opdi.orchestration clear-lock` and run step 09 again. |
| `publish` (09), "refusing to replace ... with an empty file" | The working tables hold nothing for that day, but a published file has rows. Find out why first. Only when the day really is empty, publish it with `python -m opdi.orchestration step --step 09 --day 2026-11-09 --allow-empty`. |
| `publish` (09), "the working events of that day are missing" | The day's working events were removed (`--drop-published-working`). Build the day again from step 01: `python -m opdi.orchestration day --day 2026-11-09 --force`. |

A single step whose input folders are gone does not run: `step --step 04 --day
...` then fails, names the missing folder and prints the command that builds
the day again.

To build a finished day again (for example after a correction), use
`--force`. A published day whose internal data were removed is rebuilt from
step 01 with `day --force`:

```bash
python -m opdi.orchestration day --day 2026-11-09 --force
```

Publishing a day again replaces its two files, its index rows and its
catalogue row. Both new files are written completely first, under
`_state/tmp/`, and then moved into place one after the other. A file is
therefore missing for a moment while it is replaced, and for that moment a
reader can see the new flight list beside the old events file; a reader never
sees a partly written file. If publishing stops part way, publishing the day
again ends in the correct state.

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
  their day folders once a day is published. Publishing a day reads the
  working events of the day before and after it as well, so the folder of a
  day is removed only when that day and both days beside it are published
  (both files and a catalogue row exist), and only when the day is more than
  7 days (`--working-keep-days`) before or after the day just run. A day that
  is not yet published, and the days beside it, are never touched.
  The events of a day are only published again after the day is built again
  from step 01 (`day --day ... --force`): its tracks are removed as well.
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

### Time

Measured on one worldwide day, 14 June 2026, on the OpenSky cluster. An
executor is one Spark worker process with 2 CPU cores and 14 GiB of memory;
the pipeline uses 12 of them (`executor_instances` in
`src/opdi/config/environments.py`), the most the namespace quota holds: 30
CPUs and 192 GiB in all, of which the pod that runs the driver takes 4 CPUs
and 16 GiB.

| Step | What it does | Time | How the figure was obtained |
|---|---|---|---|
| 01 | fetch and thin the reports | 396 s | scaled from a 4-hour slice of the day |
| 02 | build the tracks | 2,133 s | scaled from one aircraft in 12 |
| 02a | clean the tracks | 1,225 s | scaled from one aircraft in 12 |
| 03 | flight list | 387 s | measured on the full day |
| 04 | events and measurements | 1,741 s | measured on the full day |
| 04b | milestones in the flight list | 55 s | measured on the full day |
| 09 | published files | 246 s | measured on the full day |
| **all** | | **6,183 s (1.7 h)** | 2,429 s measured, 3,754 s scaled |

Steps 01, 02 and 02a write about 20 GB for a full day, more than the bucket
had free, so they were run on a part of the day: step 01 on the first 2 and
4 hours, steps 02 and 02a on all 59 hours that step 02 reads, but for one
aircraft in 12 and one in 24. A scaled figure is the work of that part (its
executor-seconds: the sum of the time of all its tasks) times the share of
the day it covers, divided by the cores at work (85% of them on average, as
measured on the full-day steps), plus the stages that run as a single task,
times the share. A part of a day runs in one or two rounds of tasks, so its
own run time says little about a full day. Steps 01 to 02a were measured
with 8 executors; their figures assume that 12 executors do the same work
1.5 times as fast.

The number of executors does not change the results. Neither does the order
in which Spark happens to process the rows, with one exception: when one H3
cell lies on several parts of an airport layout (two taxiways, a taxiway and
a stand), the layout event names one of them in its `info` text, and which
one can change from one run to the next.

**Where the time goes.** A day costs about 120,000 executor-seconds: about
43,500 in step 02, 35,000 in step 04, 25,000 in step 02a and 8,000 in step
01. With 24 cores busy 85% of the time that is about 5,800 s, so this
cluster does not process a worldwide day in one hour. Step 02 reads 59 hours
of reports for each day (the hours before the day, the day, and the 24 hours
after it), so each report is split into tracks on two to three days; building
the tracks once per hour of reports, and carrying the tracks still open from
one day to the next, would remove most of that work. Otherwise about twice
the cores would be needed.

`benchmarks/day_timing.py` measures a day step by step and stage by stage;
`benchmarks/day_compare.py` compares the outputs of two runs of a day.

### Space

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
flight list. They are published with empty flight columns, as are the events
whose flight is in the flight list of a day two or more days away from the
day stored on the event.
