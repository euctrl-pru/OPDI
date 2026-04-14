OPDI -- Open Performance Data Initiative
========================================

**Version:** |release|

OPDI is EUROCONTROL's Open Performance Data Initiative, a Python package for
processing OpenSky Network aviation data through modular ETL pipelines. It
transforms raw ADS-B state vectors into structured flight tracks, flight lists,
and flight events using PySpark and H3 hexagonal indexing.

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   getting_started
   pipeline_overview

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api/index

Key Features
------------

- **Modular ingestion** from OpenSky Network (state vectors, aircraft database)
  and OurAirports
- **H3 hexagonal encoding** of airport detection zones, ground layouts, and
  airspace boundaries
- **Track processing** with gap-based splitting, distance calculation, and
  altitude cleaning
- **Flight list generation** with departure/arrival detection using H3 zones
- **Flight event detection** including phase classification, level crossings,
  and airport surface events
- **Flexible export** to Parquet and CSV formats
- **Data quality monitoring** with anomaly detection and interactive
  visualizations

Quick Links
-----------

- **Source code:** `github.com/euctrl-pru/opdi <https://github.com/euctrl-pru/opdi>`_
- **Issue tracker:** `GitHub Issues <https://github.com/euctrl-pru/opdi/issues>`_
- **License:** MIT


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
