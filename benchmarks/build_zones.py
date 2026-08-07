"""
Regenerate the H3 airport detection zones at 5 NM steps out to 110 NM.

Uses ``opdi.reference.h3_airport_zones.AirportDetectionZoneGenerator`` -- the
production generator -- rather than a parallel implementation. The only things
this script decides are *which* airports go in, *how far* the rings reach and
*where* the result is written.

Why regenerate at all. The shipped table was built with rings
``[0, 5, 10, 20, 30, 40]`` and then flattened by ``prepare_for_flight_list``,
which dropped ``min_c_radius_nm``/``max_c_radius_nm``. That baked the detection
radius into the table: a consumer could only take the radius the generator had
chosen. Keeping the band columns turns the radius into a query-time filter, so
the detection sweep runs in the pipeline's own H3 terms instead of through a
parallel great-circle calculation.

Reaching 110 NM covers the ASMA C40 and C100 ring crossings with margin, at
about 32 M cells for large and medium aerodromes -- small enough to keep a
single resolution, so state vectors join on the ``h3_res_7`` they already
carry and no parent lookup is needed.

    python benchmarks/build_zones.py --out s3a://eurocontrol/opdi/h3_airport_zones_110nm
    python benchmarks/build_zones.py --out /local/path --local     # when the bucket is full
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
#: measured to cost accuracy without adding coverage (see the ADEP/ADES study).
#: Keeping them out is also what holds the table under a gigabyte.
AIRPORT_TYPES = ["large_airport", "medium_airport"]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", required=True, help="output path (s3a:// or local)")
    ap.add_argument("--max-radius-nm", type=float, default=110.0)
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

    cfg = OPDIConfig.for_environment("opensky")
    gen = AirportDetectionZoneGenerator(spark, cfg)
    print(f"resolution {gen.resolution}, rings {gen.radii_nm}")

    # Feed the ingested OurAirports table rather than the public CSV: it is
    # reachable from the cluster, and it guarantees the zones are built from
    # the same snapshot the rest of the pipeline reads.
    airports = spark.read.parquet(AIRPORTS_TABLE)
    print(f"airports in table: {airports.count():,}")

    gen.generate(airports_df=airports)
    sdf = gen.prepare_for_flight_list_spark(
        max_radius_nm=args.max_radius_nm, airport_types=args.types
    )

    sdf.write.mode("overwrite").parquet(args.out)
    n = spark.read.parquet(args.out).count()
    print(f"\n{n:,} (aerodrome, hex) rows -> {args.out}")
    spark.stop()


if __name__ == "__main__":
    main()
