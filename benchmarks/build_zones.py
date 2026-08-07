"""
Generate the H3 aerodrome detection zones to one or more destinations.

A thin command-line front end over
``opdi.reference.h3_airport_zones.AirportDetectionZoneGenerator.save_prepared``.
All of the behaviour -- ring reach, multi-destination writing, aerodrome
batching for local output -- lives in the package, because it is pipeline
infrastructure rather than benchmarking. This script only parses arguments and
picks the Spark session.

The same generation happens as part of the pipeline, and that is the normal
route:

    opdi run 00a          # writes h3_airport_detection_zones via StorageManager

Use this when a destination, aerodrome set or ring reach needs overriding
without editing pipeline kwargs:

    python benchmarks/build_zones.py \\
        --out s3a://eurocontrol/opdi/h3_airport_detection_zones /data/zones_110nm
"""

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

import osn_sample
from osn_sample import build_spark, load_dotenv

AIRPORTS_TABLE = "s3a://eurocontrol/opdi/oa_airports"

#: Detection needs large and medium aerodromes; small fields and heliports were
#: measured to cost accuracy without adding coverage (ADEP/ADES study v2).
AIRPORT_TYPES = ["large_airport", "medium_airport"]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", nargs="+", required=True,
                    help="one or more destinations; s3a://, s3:// and hdfs:// "
                         "are written from the executors, anything else is a "
                         "local directory collected to the driver")
    ap.add_argument("--batch", type=int, default=150,
                    help="aerodromes per batch when a local destination is given")
    ap.add_argument("--max-radius-nm", type=float, default=None,
                    help="ring reach; defaults to the generator's outermost ring")
    ap.add_argument("--types", nargs="+", default=AIRPORT_TYPES)
    ap.add_argument("--executors", type=int, default=8)
    ap.add_argument("--ui-port", type=int, default=4041)
    args = ap.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
    load_dotenv()
    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(6, "9g")
    spark.sparkContext.setLogLevel("ERROR")

    from opdi.config import OPDIConfig
    from opdi.reference.h3_airport_zones import AirportDetectionZoneGenerator

    gen = AirportDetectionZoneGenerator(spark, OPDIConfig.for_environment("opensky"))
    print(f"resolution {gen.resolution}, rings {gen.radii_nm}")

    # The ingested OurAirports table, not the public CSV: reachable from the
    # cluster, and the same snapshot the rest of the pipeline reads.
    airports = spark.read.parquet(AIRPORTS_TABLE)

    total = gen.save_prepared(
        destinations=args.out,
        max_radius_nm=args.max_radius_nm,
        airport_types=args.types,
        airports_df=airports,
        batch=args.batch,
    )
    print(f"\n{total:,} rows -> {', '.join(args.out)}")
    spark.stop()


if __name__ == "__main__":
    main()
