# Borealis OSN 3DI

## Python scripts

**Scratchbooks**
* `Airport_coverage_data.ipynb` - File from Abd which served as inspiration.
* `Untitled1.ipynb` - Scratchbook for ad hoc calculations
* `airport_grid_creation.ipynb` - Scratchbook for .py file creations.
* `data-download.ipynb` - Scratchbook for ETL of OurAirports and OSN datadump downloads.

**OurAirports**
*  `etl_ourairports.py` - Downloads all datasets from OurAirports (airports, runways, ...) and extracts into Hive.

**OSN ETL** 
*  `download_odn_dump.py` - Downloads OSN data dump and stores on disk.
*  `upload_to_hive.py` - Uploads OSN data to hive.

**Traffic ETL**
*  `fetch-data-daily.py` - Fetches daily OSN data using Traffic and stores on disk.
*  `fetch-data-full.py` - Fetches OSN data for period using Traffic and stores on disk.

**Milestones ETL**
*  `create_airport_grids.py` - Creates airport grids and dumps data into Hive. 
*  `create_airtport_proximity_flight_data.py` - Takes airports grid and OSN data and extracts data in proximity of airports. Extracts into Hive.

## SQL scripts

**OurAirports**
*  `create_oa_*.sql` - Creates tables for datasets from OurAirports (airports, runways, ...) on Hive.

**OSN ETL** 
*  `create_osn_*.sql` - Creates table(s) to store OSN data on Hive.

**Milestones ETL**
*  `create_airport_*.py` - Creates airport grids tables on Hive. 



