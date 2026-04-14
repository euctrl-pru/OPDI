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

config = OPDIConfig.for_environment("dev")   # "dev", "live", or "local"
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

## Package structure

```
src/opdi/
├── config.py               # Centralised configuration (dev/live/local)
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
    ├── datetime_helpers.py  # Date ranges, month boundaries
    ├── spark_helpers.py     # Spark session factory
    ├── geospatial.py        # Haversine, bearing, distance
    └── h3_helpers.py        # H3 index operations
```

## Configuration

```python
from opdi.config import OPDIConfig

config = OPDIConfig.for_environment("dev")    # Development
config = OPDIConfig.for_environment("live")   # Production (Cloudera)
config = OPDIConfig.for_environment("local")  # Local testing

# All settings are accessible as typed dataclass attributes
config.project.project_name           # "project_opdi_dev"
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
