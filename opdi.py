#!/usr/bin/env python
"""
OPDI full pipeline script.

Runs the OPDI data processing pipeline from start to finish.

Usage::

    # Run the full pipeline with default settings (opensky environment)
    python opdi.py

    # Run for a specific environment and date range
    python opdi.py --env local --start 2024-01-01 --end 2024-06-01

    # Run only a specific step
    python opdi.py --step 02 --start 2024-01-01 --end 2024-02-01

    # Worldwide (the default)
    python opdi.py --env opensky --worldwide --start 2024-01-01 --end 2024-02-01

    # After pip install, also available as:
    opdi run --start 2024-01-01 --end 2024-06-01

Pipeline steps
--------------
00  Reference data (airport zones, layouts, airspaces, OurAirports, aircraft DB)
01  State vector ingestion from OpenSky Network S3
02  Track processing (state vectors -> continuous flight tracks)
03  Flight list generation (departures, arrivals, overflights)
04  Flight events & measurements (phases, FL crossings, airport events)
04b Flight list milestones (the main events copied onto the flight list)
09  Published daily files (flight list and flight events)
"""

import argparse
import sys
from datetime import date


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="OPDI v2.0.0 - Full pipeline runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--env",
        choices=["local", "opensky"],
        default="opensky",
        help="Target environment (default: opensky). NOTE: this script and "
        "`opdi run` build a local-only Spark session -- they call "
        "create_session without distributed=True -- so --env opensky "
        "here does not attach to the Kubernetes cluster. A real "
        "cluster run goes through run_period.py.",
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
        help="Run worldwide (the default; kept so existing commands still "
        "parse). No coverage filter is applied to ingest, detection or "
        "reference geometry. In the opensky environment the coverage does "
        "not change the warehouse; a run whose coverage differs from the "
        "one that built the warehouse is refused at step 00.",
    )
    coverage.add_argument(
        "--europe",
        dest="worldwide",
        action="store_false",
        help="Apply the published European bounding box. In the local "
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
