"""The ``opdi`` command.

Installed with the package. ``opdi run`` runs the pipeline steps over a range
of dates in one local Spark session (see :func:`opdi.runner.run_pipeline`).
To process days on the OpenSky cluster, use ``run_period.py`` or
``python -m opdi.orchestration``.

Usage::

    opdi --help
    opdi run --start 2024-01-01 --end 2024-06-01
    opdi run --step 02 --env local --start 2024-01-01 --end 2024-02-01
"""

import argparse
import sys


def main(argv=None):
    """Entry point for the ``opdi`` command."""
    parser = argparse.ArgumentParser(
        prog="opdi",
        description="OPDI, the Open Performance Data Initiative: run the pipeline.",
    )
    parser.add_argument("--version", action="store_true", help="Show package version and exit.")

    subparsers = parser.add_subparsers(dest="command")

    # --- run sub-command ---
    run_parser = subparsers.add_parser("run", help="Run the pipeline steps over a range of dates.")
    run_parser.add_argument(
        "--env",
        choices=["local", "opensky"],
        default="opensky",
        help="Environment (default: opensky).",
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
        help="Run only this step: 00, 01, 01b (ADS-C ingestion), 02, "
        "02a (trajectory cleaning), 03, 04, 04b (flight list milestones) "
        "or 09 (published daily files). Omit to run all steps.",
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
        "--adep-mode",
        choices=["trend", "endpoint"],
        default=None,
        help="Method that names the departure airport in step 03 "
        "(default: DetectionConfig.adep_mode, endpoint).",
    )
    run_parser.add_argument(
        "--ades-mode",
        choices=["trend", "endpoint"],
        default=None,
        help="Method that names the destination airport in step 03 "
        "(default: DetectionConfig.ades_mode, trend).",
    )
    run_parser.add_argument(
        "--airports-hex-path",
        default="data/airport_hex/zones_res7_processed.parquet",
        help="Local Parquet file of the airport zones, used when the table "
        "h3_airport_detection_zones does not exist.",
    )
    coverage = run_parser.add_mutually_exclusive_group()
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

    if args.version:
        from opdi import __version__

        print(f"opdi {__version__}")
        return 0

    if args.command == "run":
        from datetime import date

        from opdi.runner import run_pipeline

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
            adep_mode=args.adep_mode,
            ades_mode=args.ades_mode,
            worldwide=args.worldwide,
        )

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
