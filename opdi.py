#!/usr/bin/env python
"""
OPDI full pipeline script.

Runs the complete OPDI data processing pipeline from start to finish,
equivalent to executing all ``00_*`` through ``08_*`` scripts in
``OPDI-live/python/v2.0.0/`` sequentially.

Usage::

    # Run the full pipeline with default settings (dev environment)
    python opdi.py

    # Run for a specific environment and date range
    python opdi.py --env live --start 2024-01-01 --end 2024-06-01

    # Run only a specific step
    python opdi.py --step 02 --start 2024-01-01 --end 2024-02-01

    # After pip install, also available as:
    opdi run --env live --start 2024-01-01 --end 2024-06-01

Pipeline steps
--------------
00  Reference data (airport zones, layouts, airspaces, OurAirports, aircraft DB)
01  State vector ingestion from OpenSky Network S3
02  Track processing (state vectors -> continuous flight tracks)
03  Flight list generation (departures, arrivals, overflights)
04  Flight events & measurements (phases, FL crossings, airport events)
05  Export to parquet files
06  Cleanup: deduplication + CSV.gz export
07  Basic statistics (row counts)
08  Advanced statistics & data quality report
"""

import argparse
import sys
from datetime import date


def main():
    parser = argparse.ArgumentParser(
        description="OPDI v2.0.0 - Full pipeline runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--env",
        choices=["dev", "live", "local"],
        default="dev",
        help="Target environment (default: dev).",
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
        help="Run only this step (00-08). Omit to run all.",
    )
    parser.add_argument(
        "--airports-hex-path",
        default="data/airport_hex/zones_res7_processed.parquet",
        help="Path to airport hex zones (default: data/airport_hex/zones_res7_processed.parquet).",
    )
    parser.add_argument(
        "--export-dir",
        default="data/OPDI/v002",
        help="Export output directory (default: data/OPDI/v002).",
    )
    parser.add_argument(
        "--last-n-months",
        type=int,
        default=4,
        help="Only export last N months (default: 4).",
    )

    args = parser.parse_args()

    start_date = date.fromisoformat(args.start)
    end_date = date.fromisoformat(args.end) if args.end else date.today()

    from opdi.runner import run_pipeline

    return run_pipeline(
        env=args.env,
        start_date=start_date,
        end_date=end_date,
        step=args.step,
        airports_hex_path=args.airports_hex_path,
        export_dir=args.export_dir,
        last_n_months=args.last_n_months,
    )


if __name__ == "__main__":
    sys.exit(main())
