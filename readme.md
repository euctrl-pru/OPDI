# OPDI: the Open Performance Data Initiative

OPDI is an open data set for research on air traffic performance, produced by
the Performance Review Unit of EUROCONTROL. It is built from the position
reports that aircraft broadcast (ADS-B), as collected by the
[OpenSky Network](https://opensky-network.org/), and published as two files
per day:

- **the flight list**: one row per flight, with its aircraft, its departure
  and destination airports, the first and last time it was seen, and its main
  times (off-block, take-off, landing, in-block), runways, stands and the
  times it crossed distance circles around its airports;
- **the flight events**: one row per event of those flights (for example
  take-off, top of climb, the start and end of a level segment, the crossing
  of a flight level, entry onto a runway, a holding pattern), with the
  event's time, position and altitude, its measurements (distance flown and
  time passed since the start of the flight, holding duration, and others)
  and the flight's columns.

This repository holds the pipeline that produces the files (the `opdi`
Python package, which runs on Apache Spark), a reader for the published
files, and the scripts of the studies behind each method.

The data, each column, each method and its validation are described on the
OPDI website, <https://www.opdi.aero>:

- the data: <https://www.opdi.aero/content/data/index.html> (download,
  data dictionary, releases, known limitations);
- the methods: <https://www.opdi.aero/content/methods/> (one page per method,
  for example `track-identity.html`, `airports.html`, `runway-milestones.html`).

The current release is **v2.0.0**. The previous release, v0.0.2, can still be
reproduced (see [Reproducing release v0.0.2](#reproducing-release-v002)).

## Contents

- [Reading the published files](#reading-the-published-files)
- [Installing the pipeline](#installing-the-pipeline)
- [How the pipeline works](#how-the-pipeline-works)
- [Running one day](#running-one-day)
- [Running a period](#running-a-period)
- [Where the output goes](#where-the-output-goes)
- [Coverage: worldwide or Europe](#coverage-worldwide-or-europe)
- [Configuration](#configuration)
- [Reproducing release v0.0.2](#reproducing-release-v002)
- [Studies and their variants](#studies-and-their-variants)
- [Tests](#tests)
- [Environment variables](#environment-variables)
- [Package layout](#package-layout)

## Reading the published files

The files are Parquet files, one per table and day:

```
<release>/flight_list/flight_list_YYYYMMDD.parquet
<release>/flight_events/flight_events_YYYYMMDD.parquet
<release>/index/flight_index_YYYYMM.parquet   # which day holds a flight
<release>/catalogue.parquet                   # one row per published day
```

A day's files hold the flights that were first seen on that day (UTC). Any
Parquet reader works (pandas, R `arrow`, DuckDB, Spark). The package includes
a small reader that needs no Spark:

```bash
pip install "opdi[reader]"      # pyarrow, pandas and DuckDB
```

```python
from datetime import date
from opdi import published

flights = published.read_flight_list(date(2026, 6, 14))
events = published.read_flight_events(date(2026, 6, 14), types=["ATOT", "ALDT"])

# The shapes of release v0.0.2, where events and measurements were separate tables
events_v0 = published.as_events(events)
measurements_v0 = published.as_measurements(events)
```

The files are read from `published.PUBLISHED_BASE_URL` by default; set the
environment variable `OPDI_PUBLISHED_URL`, or pass `base=`, to read from a
local folder (`/data/opdi/v2.0.0/`) or an S3 address (`s3://bucket/folder/`).

For SQL, `published.duckdb_views()` defines DuckDB views with the same names as
the Trino tables: `flight_list`, `flight_events`, `flight_index`, and the
release v0.0.2 shapes `events`, `measurements` and `flight_list_v0`:

```python
import duckdb
from opdi import published

con = duckdb.connect()
con.execute(published.duckdb_views(start=date(2026, 6, 14), end=date(2026, 6, 20)))
con.sql("select type, count(*) from events group by type").show()
```

The Trino definitions of the same tables and views are in
`src/opdi/sql/trino/` (regenerate them with `python -m opdi.sql.render`). Every
column, with its type, unit and meaning, is declared once in
`src/opdi/output/schema.py`, and the website's data dictionary is built from
it.

## Installing the pipeline

The pipeline needs Python 3.10 or later, Java 17 and Apache Spark 3.5 or
later (PySpark is installed with the package).

```bash
git clone https://github.com/euctrl-pru/opdi.git
cd opdi
pip install -e .              # the pipeline
pip install -e ".[dev]"       # plus the tools for tests and linting
pip install -e ".[airflow]"   # on the machine that runs the Airflow scheduler
```

On a Spark cluster, the executors need the same Python dependencies; the
image `docker/Dockerfile` (`quintengs/opdi-spark`) provides them. A
dependency added to the package must also be added there.

## How the pipeline works

The pipeline runs as numbered steps. Each step reads the tables of the steps
before it and writes its own:

| Step | What it does | Writes | Module |
|------|--------------|--------|--------|
| 00 | Reference tables, built once: airport detection zones (00a), airport layouts from OpenStreetMap (00b), airspaces (00c), OurAirports airports and runways (00d), the OpenSky aircraft database (00e), runway and approach grid (00f) | `h3_airport_detection_zones`, `hexaero_airport_layouts`, `opdi_h3_airspace_ref`, `oa_airports`, `oa_runways`, `osn_aircraft_db`, `h3_runway_zones` | `opdi.reference`, `opdi.ingestion` |
| 01 | Fetch the position reports (state vectors) of the day before, the day and the day after, thinned to one report per aircraft every 5 seconds | `osn_statevectors_v2` | `opdi.ingestion.osn_statevectors` |
| 02 | Split the reports into tracks, one per flight (`track_id`) | `osn_tracks` | `opdi.pipeline.tracks` |
| 02a | Clean the tracks: set values that cannot be trusted to NULL | `osn_tracks_clean` | `opdi.cleaning` |
| 03 | Flight list: departure and destination airports, aircraft data | `opdi_flight_list` | `opdi.pipeline.flights` |
| 04 | Flight events and their measurements | `opdi_flight_events`, `opdi_measurements` | `opdi.pipeline.events` |
| 04b | Copy the main milestones onto the flight list | `opdi_flight_list` | `opdi.pipeline.flight_list_step` |
| 09 | Publish the day's flight list and flight events files, the monthly index and the catalogue | `published/` | `opdi.output.published` |

Step 01b, off by default, adds ADS-C reports (satellite position reports,
mostly over oceans) to step 02; see `AdscConfig` in `src/opdi/config.py`.

A day's steps process the flights that started on that day, whole, even when
they end on the next day. A day can therefore be processed only once the
following day is complete.

## Running one day

On the OpenSky cluster, one command runs every step of a day, checks the free
space first and skips the steps already finished:

```bash
python -m opdi.orchestration reference                 # step 00, once
python -m opdi.orchestration day --day 2026-06-14      # steps 01 to 09
python -m opdi.orchestration step --step 04 --day 2026-06-14 --force   # one step again
```

Every finished step is recorded in the warehouse, so running a command again
continues where it stopped, on any machine. `python -m opdi.orchestration
--help` lists every command, including `check-space` and `prune` (removing the
working tables that are no longer needed).

Step 00b needs a local OpenStreetMap extract of the airports (set
`OPDI_OSM_PBF`); see `opdi.reference.pbf_filter` to reduce a regional or
planet extract to the aeroway features it needs.

## Running a period

Two ways, both day by day:

- **Airflow.** `airflow/dags/opdi_pipeline.py` defines a daily run and a
  backfill of any range of days. `airflow/README.md` explains the set-up, the
  settings, how disk space is protected and what a day costs.
- **`run_period.py`**, from a terminal on the machine that hosts the Spark
  driver:

  ```bash
  python run_period.py --start 2026-06-01 --dry-run   # show the plan
  python run_period.py --start 2026-06-01             # seven days
  python run_period.py --start 2026-06-01 --days 1 --steps 04
  ```

  Running the same command again after a failure continues where it stopped.

`opdi run` (and `python -m opdi`, with the same arguments) runs the steps over a date range in one local
Spark session, without the cluster; it is meant for small tests.

## Where the output goes

All tables are kept in one **warehouse**, a storage prefix. In the `opensky`
environment it is the `--warehouse` argument if given, else the
`OPDI_WAREHOUSE` environment variable, else `s3a://eurocontrol/opdi`. Each
release has its own folder in it, the release root, for example
`s3a://eurocontrol/opdi/v2.0.0/`:

| Folder | Holds |
|--------|-------|
| `published/` | The published files: `flight_list/`, `flight_events/`, `index/`, `catalogue.parquet` |
| `_state/` | Which steps of which days are finished, and the steps' progress logs |
| other folders | The reference tables and the working tables of the steps |

Every command prints the warehouse, the release root and the coverage when it
starts. In the `local` environment the tables are Iceberg tables in a local
catalog (or plain Parquet folders when `spark.enable_iceberg` is off).

## Coverage: worldwide or Europe

The pipeline processes the whole world by default. `--europe` on the command
line, or `OPDIConfig.for_environment(env, worldwide=False)` in code, keeps
only positions inside the European box (`opdi.coverage.EUROPE_BBOX`), which
is the coverage of release v0.0.2. A warehouse records the coverage that
built it, and a run with the other coverage is refused, so the two are never
mixed.

The accuracy figures on the website were measured against European reference
data. Outside Europe the results have not been validated.

## Configuration

Every setting is a field of a dataclass in `src/opdi/config.py`, with its
meaning, unit and default described there:

```python
from opdi.config import OPDIConfig

config = OPDIConfig.for_environment("opensky")   # or "local"
config.detection.endpoint_radius_nm               # 30.0
config.events.ring_radii_nm                       # (40.0, 50.0, 60.0, 100.0, 110.0, 120.0)
config.segmentation.method                        # "standard"
```

Thresholds are in aviation units with the unit in the field name (`_ft`,
`_nm`, `_kt`, `_seconds`). The stored position reports are in SI units
(metres, metres per second), as OpenSky delivers them; where a threshold meets
such a column, the column is scaled to the threshold's unit for the
comparison only.

## Reproducing release v0.0.2

Release v0.0.2 used other methods for track identity, airports and events.
They remain available as settings:

```python
from dataclasses import replace
from opdi.config import DetectionConfig, EventConfig, OPDIConfig

config = OPDIConfig.for_environment("opensky", worldwide=False)
config.segmentation.method = "legacy"          # track rule of v0.0.2
config.ingestion.decimation = "modulo"         # thinning of v0.0.2
config.detection = DetectionConfig.legacy()    # airport detection of v0.0.2
config.events = EventConfig.legacy()           # events of v0.0.2 (events_v0.0.2)
config.cleaning = replace(config.cleaning, feeds_flight_list=False)  # uncleaned tracks
```

The test `tests/test_golden_outputs.py` runs both this configuration and the
default one over a synthetic day and compares every output with reference
files, so a change that alters either is noticed.

The `track_id` values of release v0.0.2 and of release v2.0.0 differ for the
same flight, because the track rules differ, and nothing in the tables says
which rule produced an id; ids of the two releases do not join.

## Studies and their variants

Every method was chosen by a study that compared it with alternatives against
reference data (EUROCONTROL's airport operations data). The studies are
summarised on the website, with links to the full reports. Their scripts are
in `benchmarks/`, which is separate from the package: the package never
imports from it. The alternative methods the studies compared, which are not
part of the package, are in `benchmarks/study_variants/`; a study script
registers them before use (see `benchmarks/README.md`).

## Tests

```bash
.venv310/bin/python -m pytest tests/                       # all tests
.venv310/bin/python -m pytest tests/test_golden_outputs.py # outputs on a synthetic day
```

The tests run on a local Spark session and need no cluster or credentials.
The tests of the study scripts are in `tests/benchmarks/`. When a change is
meant to alter the outputs, write new reference files with
`python tools/make_golden.py` and commit them with the change. Formatting and
linting: `black .` and `ruff check src airflow tools`.

## Environment variables

| Variable | Meaning |
|----------|---------|
| `OPDI_WAREHOUSE` | The warehouse of the `opensky` environment (default `s3a://eurocontrol/opdi`) |
| `OPDI_OSM_PBF` | Local OpenStreetMap extract for step 00b |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | Credentials for the OpenSky S3 storage (also read from a `.env` file) |
| `OSN_USERNAME`, `OSN_KEY` | OpenSky credentials for the MinIO download path of step 01 |
| `OPDI_BUCKET_QUOTA_GB` | Storage quota of the bucket, for the free-space check (default 200) |
| `OPDI_PUBLISHED_URL` | Where `opdi.published` reads the published files from |

## Package layout

```
src/opdi/
  config.py            settings of every step
  coverage.py          worldwide or a bounding box
  orchestration.py     one step of one day at a time (used by Airflow)
  periodrun.py         a period of days (run_period.py)
  runner.py, cli.py    a range of dates in one session (opdi run)
  ingestion/           step 01 (state vectors, ADS-C) and the external reference data
  reference/           step 00: H3 reference tables
  pipeline/            steps 02, 03, 04 and 04b; one module per event family
  cleaning/            step 02a
  output/              step 09 and the declared columns of the published files
  published.py         reading the published files, without Spark
  sql/                 Trino and DuckDB definitions of the published tables
  utils/               storage, Spark session, dates, geometry, H3
airflow/               the Airflow DAGs and their README
benchmarks/            the study scripts (not part of the package)
tests/                 the tests
```

## Licence and contact

MIT licence. EUROCONTROL Performance Review Unit, pru-support@eurocontrol.int,
<https://github.com/euctrl-pru/opdi>.

OPDI is built on data from the [OpenSky Network](https://opensky-network.org/),
[OurAirports](https://ourairports.com/) and
[OpenStreetMap](https://www.openstreetmap.org/), and uses the
[H3](https://h3geo.org/) hexagonal grid.
