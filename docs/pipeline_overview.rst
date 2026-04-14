Pipeline Overview
=================

The OPDI pipeline transforms raw ADS-B surveillance data into structured
aviation performance datasets through a sequence of numbered stages. Each stage
corresponds to one or more modules in the ``opdi`` package.

Stage 00 -- Reference Data & Ingestion Setup
---------------------------------------------

Before processing flight data, several reference datasets must be prepared.

**Airport detection zones** (:mod:`opdi.reference.h3_airport_zones`)
   Generate H3 hexagonal zones around airport reference points at resolution 7
   (~5.2 km per hexagon). These zones are later used to detect departures and
   arrivals.

**Airport ground layouts** (:mod:`opdi.reference.h3_airport_layouts`)
   Build H3 representations of runways, taxiways, and aprons at resolution 12
   (~307 m) using OpenStreetMap data retrieved via ``osmnx``.

**Airspace boundaries** (:mod:`opdi.reference.h3_airspaces`)
   Encode FIR / UIR / TMA airspace polygons into H3 hexagons for efficient
   spatial joins.

**OurAirports** (:mod:`opdi.ingestion.ourairports`)
   Download and ingest airport metadata (ICAO codes, coordinates, runway info)
   from the OurAirports open dataset.

**Aircraft database** (:mod:`opdi.ingestion.osn_aircraft_db`)
   Download the OpenSky Network aircraft database containing registration,
   model, and operator information keyed by ICAO 24-bit transponder address.

Stage 01 -- State Vector Ingestion
------------------------------------

:mod:`opdi.ingestion.osn_statevectors`

Raw ADS-B state vectors are downloaded from the OpenSky Network MinIO server in
batched Parquet files. The ingestion module:

- Authenticates via MinIO using ``OSN_USERNAME`` / ``OSN_KEY``.
- Downloads files in configurable batches (default 250).
- Normalises column names from camelCase to snake_case.
- Writes to an Iceberg table partitioned by date.
- Tracks progress so that re-runs skip already-processed files.

Stage 02 -- Track Creation
--------------------------

:mod:`opdi.pipeline.tracks`

State vectors are grouped into flight tracks by:

1. Sorting by ``(icao24, event_time)``.
2. Splitting where the time gap exceeds a configurable threshold (default
   30 minutes).
3. Assigning a deterministic ``track_id`` via SHA-256 hashing.
4. Encoding each position into H3 hexagons at resolutions 7 and 12.
5. Computing segment and cumulative distances using the Haversine formula.
6. Cleaning altitude outliers using a configurable maximum vertical rate.

Stage 03 -- Flight List Generation
------------------------------------

:mod:`opdi.pipeline.flights`

Tracks are classified into flights:

- **Departures** -- the first track point falls inside an airport H3 zone.
- **Arrivals** -- the last track point falls inside an airport H3 zone.
- **Overflights** -- no match with any airport zone.

Each flight record is enriched with aircraft metadata from the aircraft
database (registration, model, operator) and written to the OPDI flight list
table.

Stage 04 -- Flight Event Detection
-------------------------------------

:mod:`opdi.pipeline.events`

Detailed events are extracted from each track:

**Horizontal segment events**
   Flight phase classification (ground, climb, descent, cruise, level) using
   vertical-rate thresholds. Key milestones such as top-of-climb and
   top-of-descent are identified.

**Vertical crossing events**
   Detection of flight-level crossings at FL50, FL70, FL100, and FL245 --
   important thresholds for performance analysis.

**Airport surface events**
   Entry and exit of runway, taxiway, and apron areas detected by matching H3
   positions against the airport layout reference.

**Measurement records**
   Distance flown and time elapsed are computed between consecutive events.

Stage 05 -- Data Extraction
----------------------------

:mod:`opdi.output.parquet_exporter`

The flight list, events, and measurements tables are exported to Parquet files
with configurable time intervals (typically 10-day windows). This stage
produces the files that are published as the OPDI open dataset.

Stage 06 -- Cleanup
--------------------

:mod:`opdi.output.csv_exporter`

Exported Parquet files are deduplicated and converted to compressed CSV for
distribution. The :func:`~opdi.output.csv_exporter.clean_and_save_data`
function removes duplicate rows and saves the result as gzip-compressed CSV.

Stage 07 -- Basic Statistics
-----------------------------

:mod:`opdi.monitoring.basic_stats`

Row counts and basic summary statistics are collected across all OPDI tables
to monitor pipeline health and data completeness.

Stage 08 -- Advanced Statistics
---------------------------------

:mod:`opdi.monitoring.advanced_stats`

In-depth data quality analysis:

- Daily row-count trend analysis
- Anomaly detection (suspiciously low counts)
- Known outage tracking
- MinIO bucket availability monitoring
- Interactive Plotly visualisations for exploration

Data Flow Diagram
-----------------

.. code-block:: text

   OpenSky Network              OurAirports         OpenStreetMap
        |                           |                     |
        v                           v                     v
   [01] State Vectors          [00] Airports         [00] Layouts
        |                           |                     |
        v                           v                     v
   [02] Tracks  <--------  [00] H3 Airport Zones / Airspaces
        |
        v
   [03] Flight List  <----  Aircraft Database
        |
        v
   [04] Flight Events
        |
        v
   [05] Parquet Export  -->  [06] CSV Export
        |
        v
   [07] Basic Stats  -->  [08] Advanced Stats

See Also
--------

- :doc:`getting_started` for installation instructions.
- :doc:`api/index` for detailed module-level documentation.
