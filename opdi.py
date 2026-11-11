#!/usr/bin/env python
"""Run the OPDI pipeline over a range of dates, in one Spark session.

The same as ``opdi run`` after ``pip install``. It uses a local Spark
session; to process days on the OpenSky cluster, use ``run_period.py`` or
``python -m opdi.orchestration`` instead.

Usage::

    # Every step, default settings
    python opdi.py --start 2024-01-01 --end 2024-06-01

    # The local environment
    python opdi.py --env local --start 2024-01-01 --end 2024-02-01

    # One step
    python opdi.py --step 02 --start 2024-01-01 --end 2024-02-01

Pipeline steps
--------------
00  Reference tables (airport zones, layouts, airspaces, OurAirports, aircraft database)
01  Ingestion of the state vectors from the OpenSky Network archive
02  Tracks (the reports of each flight)
02a Trajectory cleaning
03  Flight list (departure and destination airports, overflights)
04  Flight events and measurements
04b Main milestones copied onto the flight list
09  Published daily files (flight list and flight events)
"""

import argparse
import sys
from datetime import date


def main(argv=None):
    """Run the command line; returns the exit code."""
    parser = argparse.ArgumentParser(
        description="OPDI v2.0.0: run the pipeline over a range of dates.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--env",
        choices=["local", "opensky"],
        default="opensky",
        help="Environment (default: opensky). The Spark session is local in "
        "both; a run on the cluster goes through run_period.py.",
    )
    parser.add_argument(
        "--start",
        default="2022-01-01",
        help="Start date YYYY-MM-DD (default: 2022-01-01).",
    )
    parser.add_argument(
        "--end",
        default=None,
        help="End date YYYY-MM-DD (default: today).",
    )
    parser.add_argument(
        "--step",
        default=None,
        help="Run only this step (00 to 04b, or 09). Omit to run all.",
    )
    parser.add_argument(
        "--airports-hex-path",
        default="data/airport_hex/zones_res7_processed.parquet",
        help="Path to airport hex zones (default: data/airport_hex/zones_res7_processed.parquet).",
    )
    coverage = parser.add_mutually_exclusive_group()
    coverage.add_argument(
        "--worldwide",
        dest="worldwide",
        action="store_true",
        default=True,
        help="Process the whole world (the default). A warehouse built with "
        "the other coverage is refused at step 00.",
    )
    coverage.add_argument(
        "--europe",
        dest="worldwide",
        action="store_false",
        help="Keep only positions inside the European box. In the local "
        "environment this also selects the European warehouse.",
    )

    args = parser.parse_args(argv)

    start_date = date.fromisoformat(args.start)
    end_date = date.fromisoformat(args.end) if args.end else date.today()

    from opdi.runner import run_pipeline

    return run_pipeline(
        env=args.env,
        start_date=start_date,
        end_date=end_date,
        step=args.step,
        airports_hex_path=args.airports_hex_path,
        worldwide=args.worldwide,
    )


if __name__ == "__main__":
    sys.exit(main())
