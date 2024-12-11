

# Open Performance Data Initiative

## Overview
<img style='border: 1px solid black' align="right" width="200" src="https://github.com/euctrl-pru/OPDI/blob/main/figures/Logo%20Open%20Performance%20Data%20Initiative.png?raw=true" alt="OPDI logo" />

The **Open Performance Data Initiative (OPDI)** is a project launched in 2022 by the **Performance Review Commission (PRC)** to promote transparency, reproducibility, and accessibility of performance data in the **European Air Traffic Management (ATM)** system. The goal is to create a harmonized, open data environment that fosters data-driven decisions and ensures accountability across the aviation industry. For more information, see the [OPDI Portal](https://opdi.aero). 

This repository contains the core **ETL pipelines**, **SQL scripts**, and **scratch notebooks** used to extract, transform, and load (ETL) data for the OPDI platform.

## Project Structure

### Python Scripts

#### 1. OurAirports ETL
- `python/v2.0.0/00_etl_ourairports.py`: Downloads and processes datasets from **OurAirports** (e.g., airports, runways) and loads the data into **Hive**.

#### 2. OSN Raw Data ETL
- `python/v2.0.0/01_osn_statevectors_etl.py`: Downloads **OpenSky Network (OSN)** data and uploads it to **Hive**.

#### 3. OSN Tracks ETL
- `python/v2.0.0/02_tracks_etl.py`: Identifies and allocates IDs to each track in the OSN dataset using custom heuristics.

#### 4. Flight Table ETL
- `python/v2.0.0/03_osn_flight_table.py`: Maps flights to ADEP (departure) and ADES (arrival) airports and generates the flight table.

#### 5. Milestones / Flight Events ETL
- `python/v2.0.0/04_osn_flight_events_etl.py`: Extracts flight events using **OpenAP** & **PySpark** algorithms.

#### 6. OPDI Data Extraction
- `python/v2.0.0/05_extract_opdi.py`: Extracts datasets for upload to the [OPDI platform](https://opdi.aero).

### SQL Scripts

#### OurAirports
- `SQL/OurAirports/create_oa_*.sql`: Creates Hive tables for the OurAirports datasets (airports, runways, etc.).

#### OSN ETL
- `SQL/create_osn_*.sql`: SQL scripts for creating tables to store OSN data in Hive.

#### Milestones ETL
- `SQL/create_airport_*.sql`: Creates airport grid tables in Hive for milestone tracking.

### Notebooks & Scratch Files

#### Notebooks
- `Airport_coverage_data.ipynb`: Exploratory notebook for airport coverage analysis.
- `Untitled1.ipynb`: Scratchbook for ad hoc calculations and explorations.
- `airport_grid_creation.ipynb`: Notebook used for creating Python scripts related to airport grid generation.
- `data-download.ipynb`: Notebook handling the download of data from **OurAirports** and **OSN**.

## Getting Started

### Prerequisites

- **Python 3.x**
- **Hive**
- **PySpark 3.x.x**
- **OpenSky Network API key** - Request via [OpenSky Network website](https://opensky-network.org/). 

### Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/your-org/opdi-etl.git
    ```

2. Install required dependencies:

    ```bash
    pip install -r requirements.txt
    ```

3. Set up your Hive environment by ensuring the correct permissions and configurations for storing the downloaded data.

4. Run the ETL scripts in sequence starting with `00_etl_ourairports.py`.

    ```bash
    python python/v2.0.0/00_etl_ourairports.py
    ```

### Running the Pipelines

Each script can be run individually. Below is an example of how to run the **OSN raw data ETL**:

```bash
python python/v2.0.0/01_osn_statevectors_etl.py
```

Ensure the configuration files are set up properly for each step, especially paths for input/output directories and API keys where applicable.

## Contributing
We welcome contributions to the project. Please follow these steps:

1. Fork the repository.
2. Create a feature branch (git checkout -b feature/my-new-feature).
3. Commit your changes (git commit -am 'Add some feature').
4. Push to the branch (git push origin feature/my-new-feature).
5. Create a new pull request.

## License
This project is licensed under the MIT License - see the LICENSE file for details.

## Acronyms
* ATM: Air Traffic Management
* OPDI: Open Performance Data Initiative
* PRC: Performance Review Commission
* OSN: OpenSky Network
* ADEP/ADES: Departure/Arrival Airports
