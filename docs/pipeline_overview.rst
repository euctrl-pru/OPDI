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
   Build H3 representations of runways, taxiways, aprons, stands, hangars,
   thresholds and de-icing pads at resolution 12 (~307 m) from a **local
   OpenStreetMap extract**, configured through ``h3.airport_layout_pbf_path``
   or ``OPDI_OSM_PBF``. The extract is reduced to aeroway geometry once by
   :func:`opdi.reference.pbf_filter.filter_aeroway_pbf` and the result cached
   beside it. Features are assigned to the aerodrome whose OSM boundary
   (``aeroway=aerodrome``, matched on its ``icao`` tag) contains them, falling
   back to a runway-extent bounding box where OSM maps the aerodrome as a bare
   node. With no extract configured the step falls back to the ``osmnx`` /
   Overpass path, which works for a single airport but cannot build the
   network. See ``readme.md``, step 00b.

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

Stage 09 -- Published Daily Files
----------------------------------

:mod:`opdi.output.published`

Each day's flight list and flight events (with their measurements as columns)
are written as one Parquet file each, under ``<release root>/published``,
together with a monthly flight index and a catalogue of the published days.

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
   [04] Flight Events  -->  [04b] Milestones on the flight list
        |
        v
   [09] Published daily files

See Also
--------

- :doc:`getting_started` for installation instructions.
- :doc:`api/index` for detailed module-level documentation.
