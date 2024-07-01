# OPDI

## Python scripts

### OPDI v2.0.0

**OurAirports**
*  `python/v2.0.0/00_etl_ourairports.py` - Downloads all datasets from OurAirports (airports, runways, ...) and extracts into Hive.

**OSN raw data ETL** 
*  `python/v2.0.0/01_osn_statevectors_etl.py` - Downloads OSN data dump and stores on disk & uploads OSN data to hive.

**OSN tracks ETL**
*  `python/v2.0.0/02_tracks_etl.py` - Allocates IDs to each track in the data based on heuristics.

**Flight table ETL**
* `python/v2.0.0/03_osn_flight_table.py` - Allocates flights to ADEP / ADES and creates flight table.

**Milestones / Flight Events ETL**
*  `python/v2.0.0/04_osn_flight_events_etl.py` - Extracts flight events using OpenAP & PySpark algorithms.  

**Extracting OPDI**
* `python/v2.0.0/04_osn_flight_events_etl.py` - Extracts OPDI datasets for upload to opdi.aero.

## SQL scripts

**OurAirports**
*  `SQL/OurAirports/create_oa_*.sql` - Creates tables for datasets from OurAirports (airports, runways, ...) on Hive.

**OSN ETL** 
*  `SQL/create_osn_*.sql` - Creates table(s) to store OSN data on Hive.

**Milestones ETL**
*  `SQL/create_airport_*.sql` - Creates airport grids tables on Hive. 

## Random
**Scratchbooks**
* `Airport_coverage_data.ipynb` - File from Abd which served as inspiration.
* `Untitled1.ipynb` - Scratchbook for ad hoc calculations
* `airport_grid_creation.ipynb` - Scratchbook for .py file creations.
* `data-download.ipynb` - Scratchbook for ETL of OurAirports and OSN datadump downloads.
* ...



