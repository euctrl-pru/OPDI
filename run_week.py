#!/usr/bin/env python
"""Run the OPDI pipeline over a week, with the shipped configuration.

Every algorithm choice comes from the dataclass defaults, which are the
configurations the portal's studies recommend -- A8 ``standard`` segmentation
(track-construction-v2), ``endpoint``/``trend`` detection
(adep-ades-detection-v6.2) and ``events_v0.2.0`` (flight-events-v4).
``OPDIConfig.for_environment`` sets only ``project`` and ``spark``, so there is
nothing to pass to get them and no flag here that would change them.

Usage::

    # See the plan without touching the cluster
    .venv310/bin/python run_week.py --start 2026-06-01 --dry-run

    # Run it
    .venv310/bin/python run_week.py --start 2026-06-01

    # Resume after a failure -- same command, finished steps are skipped
    .venv310/bin/python run_week.py --start 2026-06-01

    # Just one step
    .venv310/bin/python run_week.py --start 2026-06-01 --steps 04

Everything is written under ``--warehouse`` (default
``s3a://eurocontrol/opdi-prod``), **including the reference tables**: step 00
runs first and rebuilds airport zones, ground layouts, the runway grid,
OurAirports and the aircraft database into that prefix. A run therefore reads
only tables it produced, and cannot touch the published ``s3a://eurocontrol/opdi``.

The reference substeps execute in dependency order rather than id order -- 00d
creates ``oa_airports``, which 00a, 00b and 00f all read. See
``runner.REFERENCE_SUBSTEPS``.

Step 00b needs a local OSM extract; set ``OPDI_OSM_PBF`` or
``h3.airport_layout_pbf_path``. Without one it falls back to the public
Overpass API, which fails silently at this scale. The run checks this before
creating a session.
"""
import argparse
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from opdi.weekrun import (  # noqa: E402
    DEFAULT_WAREHOUSE,
    OPTIONAL_STEPS,
    WEEK_STEPS,
    run_week,
)


def _day(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"expected a date as YYYY-MM-DD, got {value!r}"
        )


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--start", type=_day, required=True,
                   help="First day of the window (YYYY-MM-DD), inclusive.")
    p.add_argument("--days", type=int, default=7,
                   help="Window length in days (default: 7).")
    p.add_argument("--env", default="opensky",
                   choices=["dev", "live", "local", "opensky"],
                   help="Environment (default: opensky).")
    p.add_argument("--steps", nargs="+", default=list(WEEK_STEPS),
                   help=f"Steps to run, in order (default: {' '.join(WEEK_STEPS)}). "
                        f"Also available: {' '.join(OPTIONAL_STEPS)}.")
    p.add_argument("--state-path", type=Path, default=None,
                   help="Where completion is recorded "
                        "(default: logs/weekrun_{start}_{days}d.json).")
    p.add_argument("--force", action="store_true",
                   help="Re-run steps already recorded as complete.")
    p.add_argument("--driver-memory", default=None,
                   help="Driver heap, e.g. '6g'. Still capped to what the "
                        "container can hold.")
    p.add_argument("--executors", type=int, default=None,
                   help="Override spark.executor.instances.")
    p.add_argument("--warehouse", default=DEFAULT_WAREHOUSE,
                   help="Prefix every table is written to and read from "
                        f"(default: {DEFAULT_WAREHOUSE}). Reference tables "
                        "included -- a run reuses nothing from elsewhere.")
    p.add_argument("--allow-existing", action="store_true",
                   help="Build even though the warehouse already holds "
                        "reference tables. Off by default so a rebuild cannot "
                        "silently merge with a previous attempt.")
    p.add_argument("--skip-preflight", action="store_true",
                   help="Skip the pre-run checks (OSM extract present, "
                        "warehouse clear, reference tables readable).")
    p.add_argument("--dry-run", action="store_true",
                   help="Print the plan and exit without creating a session.")
    args = p.parse_args(argv)

    return run_week(
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
        allow_existing=args.allow_existing,
    )


if __name__ == "__main__":
    raise SystemExit(main())
