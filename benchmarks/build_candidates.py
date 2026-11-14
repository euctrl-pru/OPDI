"""
Build the endpoint candidate cache for a month.

A thin wrapper around ``FlightListProcessor.build_endpoint_candidates`` so the
regeneration chain can call it like any other job. It previously lived as an
inline ``python -c`` one-liner in the chain, which put an import path beyond
the reach of every check that would have caught it being wrong -- and it was
wrong, so the chain died an hour into a run on a typo no test could see.

    python benchmarks/build_candidates.py --month 202506
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))
sys.path.insert(0, str(REPO / "src"))

import osn_sample
from osn_sample import build_spark, load_dotenv

#: The warehouse and coverage this study reads and writes, named here rather
#: than taken from the OPDI_WAREHOUSE environment variable (which points at
#: the production warehouse on the cluster). Its tables are the European ones
#: under s3a://eurocontrol/opdi, outside any release folder.
WAREHOUSE = "s3a://eurocontrol/opdi"
WORLDWIDE = False


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--month", default="202506")
    ap.add_argument("--max-radius-nm", type=float, default=110.0)
    ap.add_argument(
        "--results-dir", default=None, help="unused; accepted so the chain can call this like a job"
    )
    ap.add_argument("--executors", type=int, default=10)
    ap.add_argument("--ui-port", type=int, default=4055)
    ap.add_argument("--rebuild", action="store_true", default=True)
    args = ap.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
    load_dotenv()
    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(6, "8g", distributed=True)
    spark.sparkContext.setLogLevel("ERROR")

    from opdi.config import OPDIConfig
    from opdi.pipeline.flights import FlightListProcessor

    month = datetime.strptime(args.month, "%Y%m").date().replace(day=1)
    cfg = OPDIConfig.for_environment("opensky", worldwide=WORLDWIDE, warehouse=WAREHOUSE)
    # The study tables sit directly in the warehouse, not in a release folder.
    cfg.project.release = ""
    proc = FlightListProcessor(spark, cfg)
    proc.build_endpoint_candidates(month, max_radius_nm=args.max_radius_nm, rebuild=args.rebuild)
    print(f"endpoint candidates built for {month:%Y-%m}")
    spark.stop()


if __name__ == "__main__":
    main()
