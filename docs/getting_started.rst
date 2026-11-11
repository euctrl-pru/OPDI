Getting Started
===============

This guide covers installation, prerequisites, and a quick-start example for
the OPDI package.

Prerequisites
-------------

- **Python** 3.10 or later and **Java** 17
- **Apache Spark** 3.5 or later (PySpark is installed with the package)
- A local Spark session, or the OpenSky Network cluster (``opensky``
  environment) with S3 credentials (``AWS_ACCESS_KEY_ID``,
  ``AWS_SECRET_ACCESS_KEY``)

Installation
------------

Install from source (editable mode)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   git clone https://github.com/euctrl-pru/opdi.git
   cd opdi
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

- PySpark >= 3.5.1
- h3 version 4 and h3-pyspark >= 1.0.0
- Shapely >= 2.0.0
- Pandas >= 1.5.0
- NumPy >= 1.23.0
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

   config = OPDIConfig.for_environment("opensky")  # "opensky" or "local"
   spark = get_spark(env="opensky", app_name="My OPDI App")

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

OPDI supports two environments:

+-------------+------------------------------------------------+
| Name        | Description                                    |
+=============+================================================+
| ``opensky`` | Production on the OpenSky cluster: parquet     |
|             | tables on S3, Spark executors on Kubernetes    |
+-------------+------------------------------------------------+
| ``local``   | Local testing -- lightweight Spark session      |
+-------------+------------------------------------------------+

Override Spark settings when needed:

.. code-block:: python

   config = OPDIConfig.for_environment("opensky")
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
