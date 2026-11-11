#!/usr/bin/env python
"""Run the OPDI pipeline over a period of days, one day at a time.

Uses the default configuration of release v2.0.0. Each day goes through all
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

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from opdi.periodrun import (  # noqa: E402
    _UNSET,
    DEFAULT_WAREHOUSE,
    OPTIONAL_STEPS,
    PERIOD_STEPS,
    run_period,
)


def _day(value: str) -> date:
    """A ``YYYY-MM-DD`` command-line value as a date."""
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected a date as YYYY-MM-DD, got {value!r}") from None


def main(argv=None) -> int:
    """Run the command line; returns the exit code (0 when every step finished)."""
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--start", type=_day, required=True, help="First day of the window (YYYY-MM-DD), inclusive."
    )
    p.add_argument("--days", type=int, default=7, help="Window length in days (default: 7).")
    p.add_argument(
        "--env",
        default="opensky",
        choices=["local", "opensky"],
        help="Environment (default: opensky).",
    )
    p.add_argument(
        "--steps",
        nargs="+",
        default=list(PERIOD_STEPS),
        help=f"Steps to run, in order (default: {' '.join(PERIOD_STEPS)}). "
        f"Also available: {' '.join(OPTIONAL_STEPS)}.",
    )
    p.add_argument(
        "--state-path",
        type=Path,
        default=None,
        help="Where completion is recorded " "(default: logs/periodrun_{start}_{days}d.json).",
    )
    p.add_argument(
        "--force", action="store_true", help="Run steps again that are recorded as finished."
    )
    p.add_argument(
        "--driver-memory",
        default=None,
        help="Driver memory to ask for, for example '6g'; reduced to what the "
        "container can hold.",
    )
    p.add_argument("--executors", type=int, default=None, help="Number of Spark executors.")
    p.add_argument(
        "--warehouse",
        default=_UNSET,
        help="Warehouse to read and write, in its release folder "
        "<warehouse>/v2.0.0, reference tables included "
        f"(default: OPDI_WAREHOUSE if set, else {DEFAULT_WAREHOUSE}).",
    )
    p.add_argument(
        "--prune-tracks",
        action="store_true",
        default=False,
        help="Remove each day's osn_tracks and osn_tracks_clean once steps 03 "
        "and 04 have finished for it. The tracks take about 19 GB a day "
        "worldwide; without them a day's events cannot be computed again "
        "without fetching its reports again.",
    )
    p.add_argument(
        "--keep-tracks-on",
        nargs="*",
        default=[],
        type=date.fromisoformat,
        metavar="YYYY-MM-DD",
        help="Days whose tracks are kept with --prune-tracks.",
    )
    coverage = p.add_mutually_exclusive_group()
    coverage.add_argument(
        "--worldwide",
        dest="worldwide",
        action="store_true",
        default=True,
        help="Process the whole world (the default).",
    )
    coverage.add_argument(
        "--europe",
        dest="worldwide",
        action="store_false",
        help="Keep only positions inside the European box.",
    )
    p.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Skip the checks before the run (OpenStreetMap extract present, "
        "reference tables readable).",
    )
    p.add_argument(
        "--dry-run", action="store_true", help="Print the plan and stop, without starting Spark."
    )
    args = p.parse_args(argv)

    return run_period(
        env=args.env,
        start=args.start,
        days=args.days,
        steps=args.steps,
        state_path=args.state_path,
        force=args.force,
        driver_memory=args.driver_memory,
        executors=args.executors,
        skip_preflight=args.skip_preflight,
        dry_run=args.dry_run,
        warehouse=args.warehouse,
        worldwide=args.worldwide,
        prune_tracks=args.prune_tracks,
        keep_tracks_on=args.keep_tracks_on,
    )


if __name__ == "__main__":
    raise SystemExit(main())
