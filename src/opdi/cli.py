"""
Command-line interface for the OPDI pipeline.

Provides an ``opdi`` CLI command (registered via pyproject.toml
``[project.scripts]``) that dispatches to individual pipeline stages
or runs the full pipeline end-to-end.

Usage::

    opdi --help
    opdi run --env live --start 2024-01-01 --end 2024-06-01
    opdi run --step 02 --env dev --start 2024-01-01 --end 2024-02-01
"""

import argparse
import sys


def main(argv=None):
    """Entry point for the ``opdi`` command."""
    parser = argparse.ArgumentParser(
        prog="opdi",
        description="OPDI - Open Performance Data Initiative pipeline runner",
    )
    parser.add_argument(
        "--version", action="store_true", help="Show package version and exit."
    )

    subparsers = parser.add_subparsers(dest="command")

    # --- run sub-command ---
    run_parser = subparsers.add_parser("run", help="Run pipeline stages.")
    run_parser.add_argument(
        "--env",
        choices=["dev", "live", "local"],
        default="dev",
        help="Environment (default: dev).",
    )
    run_parser.add_argument(
        "--start",
        required=True,
        help="Start date in YYYY-MM-DD format.",
    )
    run_parser.add_argument(
        "--end",
        required=True,
        help="End date in YYYY-MM-DD format.",
    )
    run_parser.add_argument(
        "--step",
        default=None,
        help="Run only this step (00-08). Omit to run all steps.",
    )
    run_parser.add_argument(
        "--no-airport-zones",
        action="store_true",
        default=False,
        help="Skip 00a: Airport H3 detection zone generation.",
    )
    run_parser.add_argument(
        "--no-airport-layouts",
        action="store_true",
        default=False,
        help="Skip 00b: Airport ground layout generation (OSM -> H3).",
    )
    run_parser.add_argument(
        "--no-airspaces",
        action="store_true",
        default=False,
        help="Skip 00c: Airspace boundary generation (ANSP/FIR -> H3).",
    )
    run_parser.add_argument(
        "--no-ourairports",
        action="store_true",
        default=False,
        help="Skip 00d: OurAirports reference data ingestion.",
    )
    run_parser.add_argument(
        "--no-aircraft-db",
        action="store_true",
        default=False,
        help="Skip 00e: OpenSky aircraft database ingestion.",
    )
    run_parser.add_argument(
        "--airports-hex-path",
        default="data/airport_hex/zones_res7_processed.parquet",
        help="Path to pre-generated airport hex zones parquet.",
    )

    args = parser.parse_args(argv)

    if args.version:
        from opdi import __version__
        print(f"opdi {__version__}")
        return 0

    if args.command == "run":
        from opdi.runner import run_pipeline
        from datetime import date

        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)

        return run_pipeline(
            env=args.env,
            start_date=start,
            end_date=end,
            step=args.step,
            run_airport_zones=not args.no_airport_zones,
            run_airport_layouts=not args.no_airport_layouts,
            run_airspaces=not args.no_airspaces,
            run_ourairports=not args.no_ourairports,
            run_aircraft_db=not args.no_aircraft_db,
            airports_hex_path=args.airports_hex_path,
        )

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
