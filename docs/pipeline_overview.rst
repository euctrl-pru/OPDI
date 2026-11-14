Pipeline Overview
=================

The OPDI pipeline turns the position reports that aircraft broadcast (ADS-B,
collected by the OpenSky Network) into a daily flight list and daily flight
events. It runs as numbered steps; each step reads the tables of the steps
before it and writes its own. Each method, its thresholds and its validation
are described on the OPDI website, https://www.opdi.aero/content/methods/;
this page gives the order of the steps and the module of each.

Step 00 -- Reference tables
---------------------------

Built once per warehouse; a later run reuses the tables that are complete.

**Airport detection zones** (:mod:`opdi.reference.h3_airport_zones`, 00a)
   Rings of H3 cells at resolution 7 (about 5 km across) around every airport,
   every 5 NM out to 110 NM. Step 03 matches reports to them.

**Airport layouts** (:mod:`opdi.reference.h3_airport_layouts`, 00b)
   Runways, taxiways, aprons, stands and other parts of each airport, from a
   local OpenStreetMap extract (``OPDI_OSM_PBF``), as H3 cells at resolution 12
   (about 20 m across). Step 04 matches reports to them.

**Airspaces** (:mod:`opdi.reference.h3_airspaces`, 00c)
   Flight information regions (FIRs) and other airspaces as compact H3 cells,
   for the FIR crossings of step 04.

**OurAirports** (:mod:`opdi.ingestion.ourairports`, 00d)
   Airport positions and elevations, and runway thresholds.

**Aircraft database** (:mod:`opdi.ingestion.osn_aircraft_db`, 00e)
   Registration, model, type and operator by ICAO 24-bit address.

**Runway grid** (:mod:`opdi.reference.h3_runway_grid`, 00f)
   Runways and approach corridors as H3 cells, for the runway milestones.

Step 01 -- Position reports
---------------------------

:mod:`opdi.ingestion.osn_statevectors`

The state vectors of the day before, the day and the day after are read from
the OpenSky archive, limited to the coverage (worldwide by default), thinned
to one report per aircraft every 5 seconds and written per day. Altitudes are
in metres and speeds in metres per second, as OpenSky delivers them.

Step 02 -- Tracks
-----------------

:mod:`opdi.pipeline.tracks`, :mod:`opdi.pipeline.segmentation`

The reports are split into tracks, one per flight. The default rule groups the
reports by aircraft address and starts a new track when the call sign changes
for longer than 30 seconds, at a gap of more than 15 minutes at low altitude,
or at a gap of more than 30 minutes unless both sides of the gap fit one
continuous flight at cruise level. The ``track_id`` is the hash of the group
key and the second of the track's first report. Each report also gets its H3
cells, the distance flown since the start of the track and checked altitudes.

Step 02a -- Cleaning
--------------------

:mod:`opdi.cleaning`

Values that cannot be trusted (repeated values, spikes, isolated values) are
set to NULL, keeping the rows, and every gap of 5 minutes or more starts a new
segment.

Step 03 -- Flight list
----------------------

:mod:`opdi.pipeline.flights`

For every track, the departure airport is named from its first report near an
airport (the ``endpoint`` method) and the destination airport from the
altitude trend of its reports near an airport (the ``trend`` method). The
tracks that cannot be placed near any airport and last at least 5 minutes are
added as overflights. Each row gets the aircraft's data.

Step 04 -- Flight events and measurements
-----------------------------------------

:mod:`opdi.pipeline.events` and one module per event family

**Flight phases** by fuzzy logic (the membership functions of OpenAP) on
altitude, vertical rate and speed, giving level-start and level-end; the top of
climb and top of descent of the vertical flight efficiency method; level-offs
in climb and descent.

**Crossings** of flight levels (FL050, FL070, FL100, FL245) and of distance
circles around the departure and destination airports (40 NM to 120 NM).

**Runway milestones** (line-up, take-off roll, airborne, threshold crossing,
touchdown, runway vacated, go-around, runway crossings), take-off and landing
times, and off-block and in-block times.

**Airport layout events**: entry and exit of runways, taxiways, aprons and
stands.

**Holding patterns, point merge legs and FIR crossings.**

**Measurements**: every event carries the distance flown and the time passed
since the start of its track (cumulative, not since the previous event), and
some families add their own (holding duration and circuits, point merge leg
time and distance).

Step 04b -- Milestones on the flight list
-----------------------------------------

:mod:`opdi.pipeline.flight_list_step`

The main times, runways, stands and circle crossings of each flight are copied
from its events onto the flight list, and the departure and destination
airports are corrected where a runway event names them.

Step 09 -- Published daily files
--------------------------------

:mod:`opdi.output.published`

Each day's flight list and flight events (with their measurements as columns)
are written as one Parquet file each, under ``<release root>/published``,
together with a monthly flight index and a catalogue of the published days.
:mod:`opdi.published` reads them without Spark.

Data flow
---------

.. code-block:: text

   OpenSky Network              OurAirports         OpenStreetMap
        |                           |                     |
        v                           v                     v
   [01] State vectors          [00] Airports         [00] Layouts
        |                           |                     |
        v                           v                     v
   [02] Tracks  <--------  [00] Airport zones, runway grid, airspaces
        |
        v
   [02a] Cleaned tracks
        |
        v
   [03] Flight list  <----  Aircraft database
        |
        v
   [04] Flight events  -->  [04b] Milestones on the flight list
        |
        v
   [09] Published daily files

See also
--------

- :doc:`getting_started` for installation.
- :doc:`api/index` for the modules.
