OPDI -- Open Performance Data Initiative
========================================

**Version:** |release|

OPDI is EUROCONTROL's Open Performance Data Initiative. This package turns the
ADS-B position reports collected by the OpenSky Network into a daily flight
list and daily flight events, with PySpark and the H3 hexagonal grid. The data,
the methods and their validation are described on the OPDI website,
https://www.opdi.aero.

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
- **Track building** and trajectory cleaning
- **Flight list generation** with departure/arrival detection using H3 zones
- **Flight event detection** including phase classification, level crossings,
  and airport surface events
- **Daily published files**: the flight list and the flight events, as Parquet

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
