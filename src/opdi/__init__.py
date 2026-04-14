"""
OPDI — Open Performance Data Initiative pipeline module.

Architecture
------------
- **ingestion**:  Data source connectors (statevectors, aircraft DB, airports, airspaces).
- **transforms**: Core data manipulation pipeline (tracks, flight list, flight events, H3 grids).
- **output**:     Data sinks (Iceberg tables, parquet extraction, cleanup).
- **utils**:      Shared helpers (month generation, timestamps, Spark session builder).
- **validation**: Data quality checks and statistics.
"""

__version__ = "2.0.0"
