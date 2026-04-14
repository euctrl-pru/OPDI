Getting Started
===============

This guide covers installation, prerequisites, and a quick-start example for
the OPDI package.

Prerequisites
-------------

- **Python** >= 3.8
- **Apache Spark** >= 3.3.0
- Access to a Cloudera environment (production) or a local Spark cluster
- For OpenSky data ingestion: ``OSN_USERNAME`` and ``OSN_KEY`` environment
  variables

Installation
------------

Install from source (editable mode)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   cd OPDI-dev
   pip install -e .

Install with development tools
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   pip install -e ".[dev]"

This pulls in ``pytest``, ``black``, ``mypy``, and ``ruff`` for development.

Dependencies
^^^^^^^^^^^^

All required dependencies are declared in ``pyproject.toml`` and installed
automatically:

- PySpark >= 3.3.0
- H3 >= 3.7.0 and h3-pyspark >= 1.0.0
- Shapely >= 2.0.0
- Pandas >= 1.5.0
- NumPy >= 1.23.0
- Plotly >= 5.0.0
- And more -- see ``pyproject.toml`` for the full list.

Building the documentation
^^^^^^^^^^^^^^^^^^^^^^^^^^

Install Sphinx and the Furo theme, then build:

.. code-block:: bash

   pip install sphinx furo
   cd docs
   make html          # Linux / macOS
   make.bat html      # Windows

The generated HTML will be in ``docs/_build/html/``.

Quick Start
-----------

1. Create a configuration and Spark session
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from opdi.config import OPDIConfig
   from opdi.utils.spark_helpers import get_spark

   config = OPDIConfig.for_environment("dev")  # "dev", "live", or "local"
   spark = get_spark(env="dev", app_name="My OPDI App")

2. Ingest the aircraft database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from opdi.ingestion import AircraftDatabaseIngestion

   aircraft_ingest = AircraftDatabaseIngestion(spark, config)
   aircraft_ingest.create_table_if_not_exists()
   count = aircraft_ingest.ingest(mode="overwrite")
   print(f"Ingested {count} aircraft records")

3. Process monthly data
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from datetime import date
   from opdi.utils.datetime_helpers import generate_months

   months = generate_months(date(2024, 1, 1), date(2024, 3, 1))
   for month in months:
       print(f"Processing {month.strftime('%B %Y')}")

4. Clean up
^^^^^^^^^^^

Always stop the Spark session when you are finished:

.. code-block:: python

   spark.stop()

Environment Configuration
-------------------------

OPDI supports three environments:

+----------+--------------------------------------------------+
| Name     | Description                                      |
+==========+==================================================+
| ``dev``  | Development -- moderate memory, suitable for      |
|          | prototyping                                      |
+----------+--------------------------------------------------+
| ``live`` | Production -- full resource allocation on the     |
|          | Cloudera cluster                                 |
+----------+--------------------------------------------------+
| ``local``| Local testing -- lightweight Spark session         |
+----------+--------------------------------------------------+

Override Spark settings when needed:

.. code-block:: python

   config = OPDIConfig.for_environment("dev")
   config.spark.driver_memory = "16G"
   config.spark.executor_memory = "20G"

OpenSky Network Credentials
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Set the following environment variables before running ingestion:

.. code-block:: bash

   export OSN_USERNAME="your_username"
   export OSN_KEY="your_api_key"

Obtain credentials from the `OpenSky Network <https://opensky-network.org/>`_.

Next Steps
----------

- Read the :doc:`pipeline_overview` to understand the end-to-end data flow.
- Browse the :doc:`api/index` for detailed module documentation.
