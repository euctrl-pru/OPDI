#!/usr/bin/env python
"""Run the OPDI pipeline over a period of days, one day at a time.

The same as ``python -m opdi.periodrun``, kept for scripts that call it by
this name. Uses the default configuration of release v2.0.0. Each day goes through all
steps before the next day starts; a finished step is recorded, so running the
same command again after a failure continues where it stopped.

Usage::

    # Show the plan without starting Spark
    python run_period.py --start 2026-06-01 --dry-run

    # Process seven days (the default length)
    python run_period.py --start 2026-06-01

    # One day, one step
    python run_period.py --start 2026-06-01 --days 1 --steps 04

    # Into another warehouse
    python run_period.py --start 2026-06-01 --warehouse s3a://eurocontrol/scratch

Runs are worldwide unless ``--europe`` is given. The warehouse is
``--warehouse`` if given, else the ``OPDI_WAREHOUSE`` environment variable,
else ``s3a://eurocontrol/opdi``. Everything, including the reference tables
built by step 00, is written in the release folder inside it
(``<warehouse>/v2.0.0``), so a run reads only tables it produced. A
warehouse built with one coverage refuses a run with the other.

Step 00b needs a local OpenStreetMap extract: set ``OPDI_OSM_PBF`` or
``h3.airport_layout_pbf_path``. The run checks this before it starts.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from opdi.periodrun import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
