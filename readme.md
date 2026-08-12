# OPDI - Open Performance Data Initiative

A pip-installable Python package for processing OpenSky Network aviation data through modular ETL pipelines. OPDI transforms raw ADS-B state vectors into enriched flight datasets with geospatial H3 indexing, event detection, and data quality monitoring.

## Installation

### From source (editable / development mode)

```bash
cd OPDI-dev
pip install -e .
```

### With development tools (pytest, black, mypy, ruff)

```bash
pip install -e ".[dev]"
```

### With documentation tools (Sphinx)

```bash
pip install -e ".[docs]"
```

### All extras

```bash
pip install -e ".[dev,docs]"
```

### Requirements

- Python >= 3.8
- Apache Spark >= 3.3.0 (PySpark)
- All other dependencies are installed automatically from `pyproject.toml`

## Quick start

```python
from datetime import date
from opdi.config import OPDIConfig
from opdi.utils.spark_helpers import get_spark

config = OPDIConfig.for_environment("dev")   # "dev", "live", "local", or "opensky"
spark  = get_spark(env="dev", app_name="My OPDI Analysis")

# Run one pipeline step
from opdi.pipeline import TrackProcessor
processor = TrackProcessor(spark, config)
processor.process_month(date(2024, 1, 1))

spark.stop()
```

## Running the full pipeline

The package provides three equivalent ways to run the complete 00-08 pipeline:

### 1. CLI (after `pip install`)

```bash
opdi run --env live --start 2024-01-01 --end 2024-06-01

# Single step only
opdi run --step 02 --env dev --start 2024-01-01 --end 2024-02-01
```

### 2. Standalone script

```bash
python opdi.py --env live --start 2024-01-01 --end 2024-06-01
```

### 3. Programmatic

```python
from opdi.runner import run_pipeline
run_pipeline(env="live", start_date=date(2024, 1, 1), end_date=date(2024, 6, 1))
```

## Pipeline stages

| Step | Script origin | Module | Description |
|------|--------------|--------|-------------|
| 00a | `00_create_h3_airport_detection_areas.py` | `reference.h3_airport_zones` | Airport H3 detection zones (0-40 NM rings) |
| 00b | `00_create_h3_airport_layouts.py` | `reference.h3_airport_layouts` | Airport ground layouts from OSM (res 12) |
| 00c | `00_create_h3_airspaces.py` | `reference.h3_airspaces` | ANSP / FIR / country boundaries (res 7) |
| 00d | `00_etl_ourairports.py` | `ingestion.ourairports` | OurAirports reference data (6 datasets) |
| 00e | `00_osn_aircraft_db.py` | `ingestion.osn_aircraft_db` | OpenSky aircraft database |
| 01 | `01_osn_statevectors_etl.py` | `ingestion.osn_statevectors` | State vector ingestion from OpenSky S3 |
| 02 | `02_osn_tracks_etl.py` | `pipeline.tracks` | Track creation (SHA256 IDs, H3, distance, alt cleaning) |
| 03 | `03_opdi_flight_list_v2.py` | `pipeline.flights` | Flight list (DAI + overflights, aircraft enrichment) |
| 04 | `04_opdi_flight_events_etl.py` | `pipeline.events` | Events & measurements (fuzzy phases, FL crossings, airport events) |
| 05 | `05_extract_OPDI.py` | `output.parquet_exporter` | Export to parquet (monthly + 10-day intervals) |
| 06 | `06_cleanup.py` | `output.csv_exporter` | Deduplication + CSV.gz export |
| 07 | `07_get_stats.py` | `monitoring.basic_stats` | Table row counts |
| 08 | `08_get_advanced_stats.py` | `monitoring.advanced_stats` | Data quality report + Plotly visualization |

## OpenSky Network environment

The `opensky` environment connects directly to the OpenSky Network S3 bucket and stores pipeline output as parquet on S3 (`s3a://eurocontrol/opdi/`). It does not require Hive or Iceberg.

### Non-distributed (local driver only)

```python
from opdi.utils.spark_helpers import get_spark

spark = get_spark("opensky", app_name="OPDI Analysis")
```

### Distributed (Kubernetes executors)

```python
spark = get_spark("opensky", distributed=True)

# With a custom Docker image containing Python dependencies
spark = get_spark(
    "opensky",
    distributed=True,
    container_image="my-registry/opdi-spark:v4.1.1",
)
```

### Running pipeline steps on OpenSky

All pipeline steps use a `StorageManager` abstraction (`opdi.utils.storage`) that transparently switches between Iceberg tables and S3 parquet depending on the environment. No code changes are needed in individual steps.

```python
from opdi.config import OPDIConfig
from opdi.utils.spark_helpers import get_spark
from opdi.ingestion.osn_aircraft_db import AircraftDatabaseIngestion

config = OPDIConfig.for_environment("opensky")
spark = get_spark("opensky", app_name="Aircraft DB Ingestion")

ingestion = AircraftDatabaseIngestion(spark, config)
ingestion.create_table_if_not_exists()  # no-op on S3
ingestion.ingest(mode="overwrite")      # writes to s3a://eurocontrol/opdi/osn_aircraft_db/
```

### Reading OpenSky state vectors directly

```python
# State vectors are available as hourly partitions
df = spark.read.parquet("s3a://opensky-hdfs-backup/tables_v4/state_vectors/hour=1771952400")
df.show(10)
```

See `notebooks/opensky_quickstart.ipynb` for a ready-to-run notebook.

## Building a flight list

Step 03 turns tracks into a flight list: one row per flight with a departure
aerodrome (`ADEP`), a destination (`ADES`), and how each was determined.

Departures and arrivals are **not** produced by the same algorithm, because
they are not equally hard. Departures use `endpoint` — the aerodrome nearest
the track's first fix, accepted only if that fix is close and low, and
abstained on otherwise. Arrivals use `trend` — a vote over smoothed barometric
altitude near each candidate aerodrome, which decides climb from descent.
Both rank candidates on exact great-circle distance, with a penalty against
aerodromes without scheduled service.

Both the algorithms and their thresholds are `DetectionConfig` defaults, and
those defaults are the values measured in
[the ADEP/ADES detection study](https://www.eurocontrol.int/opdi) — so the
recommended configuration is what you get by passing nothing:

```python
from datetime import date

from opdi.config import OPDIConfig
from opdi.pipeline.flights import FlightListProcessor
from opdi.utils.spark_helpers import get_spark

config = OPDIConfig.for_environment("opensky")
spark = get_spark("opensky", distributed=True, app_name="OPDI flight list")

FlightListProcessor(spark, config).process_dai(
    month=date(2025, 6, 1),   # any date inside the month to process
)
```

`process_dai` reads `osn_tracks` for the month, builds the endpoint candidate
cache if it is not already there, and appends to `opdi_flight_list`. Pass
`write_mode="overwrite"` and a `table_name` of your own to build a comparison
table instead of extending the published one.

To use one algorithm for both roles — for a like-for-like comparison, say —
name it explicitly:

```python
processor.process_dai(month=date(2025, 6, 1), mode="trend")           # both roles
processor.process_dai(month=date(2025, 6, 1), ades_mode="endpoint")   # arrivals only
```

The shipped settings, all overridable on `DetectionConfig`:

| Parameter | Ships as | Applies to |
|---|---|---|
| `adep_mode` | `"endpoint"` | which algorithm names the departure |
| `ades_mode` | `"trend"` | which algorithm names the destination |
| `trend_max_fl` | FL60 | `trend` — samples above this are ignored |
| `trend_radius_nm` | 20 NM | `trend` — zone radius for the sample-to-aerodrome join |
| `trend_vote_margin` | 2 | `trend` — climb votes must beat descent votes by this many |
| `trend_sched_penalty_nm` | 10 NM | `trend` — added to an aerodrome without scheduled service |
| `trend_rank_by` | `"haversine"` | `trend` — exact distance, not H3 ring count |
| `endpoint_radius_nm` | 30 NM | `endpoint` — how far the first/last fix may be |
| `endpoint_height_ft` | 15,000 ft | `endpoint` — above *field elevation*, not sea level |
| `endpoint_sched_penalty_nm` | 10 NM | `endpoint` |

To override one, build the config with a modified `DetectionConfig`:

```python
import dataclasses

from opdi.config import DetectionConfig, OPDIConfig

config = OPDIConfig.for_environment("opensky")
config.detection = dataclasses.replace(config.detection, trend_max_fl=80)
```

### Reproducing a flight list published before 2026

The defaults changed in 2026. Re-running an older month with them will **not**
reproduce what was released — the thresholds moved and so did the rule for
choosing among candidate aerodromes. `DetectionConfig.legacy()` returns the
values every earlier release was built with:

```python
from opdi.config import DetectionConfig, OPDIConfig

config = OPDIConfig.for_environment("opensky")
# FL40, 30 NM, margin 4, no penalty, ring ranking, and `trend` for both roles
config.detection = DetectionConfig.legacy()

FlightListProcessor(spark, config).process_dai(month=date(2024, 6, 1))
```

One caveat `legacy()` cannot cover: the state-vector sampler changed at the
same time, from a fixed-phase `event_time % 5` filter to keeping the newest row
in each 5 s bin. The rescued rows sit at track boundaries, so `track_id` values
differ. Re-ingesting an old month reproduces the *aerodromes* but not the
identifiers.

### From the command line

```bash
# Flight list only, for one month — uses the DetectionConfig defaults
opdi run --step 03 --env opensky --start 2025-06-01 --end 2025-07-01

# Override the algorithm for one role
opdi run --step 03 --env opensky --start 2025-06-01 --end 2025-07-01 \
    --adep-mode endpoint --ades-mode trend

# Everything the flight list depends on, in order
opdi run --env opensky --start 2025-06-01 --end 2025-07-01
```

## Package structure

```
src/opdi/
├── config.py               # Centralised configuration (dev/live/local/opensky)
├── runner.py               # Full pipeline orchestrator (steps 00-08)
├── cli.py                  # CLI entry point (`opdi run ...`)
├── ingestion/              # Data source connectors
│   ├── osn_statevectors.py # OpenSky S3 state vectors
│   ├── osn_aircraft_db.py  # Aircraft metadata
│   └── ourairports.py      # OurAirports reference data
├── reference/              # H3 reference data generators
│   ├── h3_airport_zones.py # Concentric detection rings
│   ├── h3_airport_layouts.py # Ground infrastructure from OSM
│   └── h3_airspaces.py     # ANSP/FIR/country boundaries
├── pipeline/               # Core transformations
│   ├── tracks.py           # State vectors -> flight tracks
│   ├── flights.py          # Tracks -> flight list (ADEP/ADES)
│   └── events.py           # Flight events & measurements
├── output/                 # Data export
│   ├── parquet_exporter.py # Parquet export with intervals
│   └── csv_exporter.py     # Deduplication + CSV.gz
├── monitoring/             # Data quality
│   ├── basic_stats.py      # Row counts
│   └── advanced_stats.py   # Anomaly detection + Plotly
└── utils/                  # Shared utilities
    ├── storage.py           # Storage abstraction (Iceberg / S3 parquet)
    ├── datetime_helpers.py  # Date ranges, month boundaries
    ├── spark_helpers.py     # Spark session factory
    ├── geospatial.py        # Haversine, bearing, distance
    └── h3_helpers.py        # H3 index operations
```

## Configuration

```python
from opdi.config import OPDIConfig

config = OPDIConfig.for_environment("dev")      # Development (Azure/Iceberg)
config = OPDIConfig.for_environment("live")     # Production (Azure/Iceberg)
config = OPDIConfig.for_environment("local")    # Local testing
config = OPDIConfig.for_environment("opensky")  # OpenSky Network S3

# All settings are accessible as typed dataclass attributes
config.project.project_name             # "project_opdi"
config.h3.airport_detection_resolution  # 7
config.ingestion.batch_size             # 250
config.spark.executor_memory            # "12G"
```

## Documentation

Full API documentation is built with Sphinx from the Google-style docstrings embedded in every module.

### Building the docs locally

```bash
# Install doc dependencies
pip install -e ".[docs]"

# Build HTML
cd docs
make html            # Linux / macOS
.\make.bat html      # Windows (PowerShell)

# Open in browser
# Linux/macOS:   open _build/html/index.html
# Windows:       start _build\html\index.html
```

The generated site includes:

- **Getting Started** - installation, prerequisites, quick start
- **Pipeline Overview** - all 9 stages with data flow diagram
- **API Reference** - auto-generated from docstrings for every class and function

### Online docs

Documentation is published automatically to GitHub Pages on every push to `main`. See the repository's **Environments** tab for the live URL.

## Development

```bash
# Install everything
pip install -e ".[dev,docs]"

# Run tests
pytest

# Format code
black src/opdi

# Lint
ruff check src/opdi

# Type check
mypy src/opdi

# Build docs
cd docs && make html
```

## Environment variables

For OpenSky Network data ingestion, set:

```bash
export OSN_USERNAME="your_username"
export OSN_KEY="your_api_key"
```

Obtain credentials from [opensky-network.org](https://opensky-network.org/).

## License

MIT

## Contact

EUROCONTROL Performance Review Unit
- Email: pru-support@eurocontrol.int
- Repository: [github.com/euctrl-pru/opdi](https://github.com/euctrl-pru/opdi)

## Acknowledgements

- [OpenSky Network](https://opensky-network.org/) for ADS-B data
- [OurAirports](https://ourairports.com/) for airport reference data
- [Uber H3](https://h3geo.org/) for hexagonal geospatial indexing
