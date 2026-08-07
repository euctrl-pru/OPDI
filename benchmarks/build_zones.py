"""
Generate the H3 aerodrome detection zones, to S3 or to local disk.

Uses ``opdi.reference.h3_airport_zones.AirportDetectionZoneGenerator`` -- the
production step-00a generator -- rather than a parallel implementation. The only
things decided here are which aerodromes go in, how far the rings reach, and
where the result lands.

Two modes:

``--out s3a://...``
    One Spark write, exactly what pipeline step 00a does. Preferred.

``--out <local dir> --local``
    Generates in aerodrome batches and collects each to the driver. Needed
    because a cluster job cannot write to the driver's local disk -- executors
    would each write to their own filesystem -- and because the whole table at
    full reach is tens of millions of rows, too large to collect in one piece.

Once the bucket has room, the S3 copy is produced by the pipeline itself:

    opdi run 00a                      # writes h3_airport_detection_zones
    python benchmarks/build_zones.py --out s3a://eurocontrol/opdi/h3_airport_detection_zones

Both call the same generator with the same defaults; the second only exists to
override the aerodrome set or the reach without editing pipeline kwargs.
"""

import argparse
import shutil
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

import osn_sample
from osn_sample import build_spark, load_dotenv

AIRPORTS_TABLE = "s3a://eurocontrol/opdi/oa_airports"

#: Detection needs large and medium aerodromes; small fields and heliports were
#: measured to cost accuracy without adding coverage (ADEP/ADES study v2), and
#: excluding them is also what holds the table under a gigabyte.
AIRPORT_TYPES = ["large_airport", "medium_airport"]


def _generator(spark, args):
    from opdi.config import OPDIConfig
    from opdi.reference.h3_airport_zones import AirportDetectionZoneGenerator

    cfg = OPDIConfig.for_environment("opensky")
    gen = AirportDetectionZoneGenerator(spark, cfg)
    print(f"resolution {gen.resolution}, rings {gen.radii_nm}")
    return gen


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", required=True, help="s3a:// path, or a local directory with --local")
    ap.add_argument("--local", action="store_true",
                    help="collect in batches to the driver instead of writing from executors")
    ap.add_argument("--batch", type=int, default=150,
                    help="aerodromes per batch in --local mode")
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

    from pyspark.sql import functions as F

    gen = _generator(spark, args)
    reach = args.max_radius_nm or max(gen.radii_nm)

    # The ingested OurAirports table, not the public CSV: reachable from the
    # cluster, and guaranteed to be the snapshot the rest of the pipeline reads.
    airports = spark.read.parquet(AIRPORTS_TABLE).filter(F.col("type").isin(args.types))
    idents = [r[0] for r in airports.select("ident").distinct().collect()]
    print(f"aerodromes: {len(idents):,} ({', '.join(args.types)})")

    if not args.local:
        gen.generate(airports_df=airports)
        sdf = gen.prepare_for_flight_list_spark(max_radius_nm=reach, airport_types=args.types)
        sdf.write.mode("overwrite").parquet(args.out)
        print(f"\n{spark.read.parquet(args.out).count():,} rows -> {args.out}")
        spark.stop()
        return

    out = Path(args.out)
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    total = 0
    for i in range(0, len(idents), args.batch):
        chunk = idents[i:i + args.batch]
        gen.generate(airports_df=airports.filter(F.col("ident").isin(chunk)))
        sdf = gen.prepare_for_flight_list_spark(max_radius_nm=reach, airport_types=args.types)
        pdf = sdf.toPandas()
        part = out / f"part-{i // args.batch:04d}.parquet"
        pdf.to_parquet(part, index=False)
        total += len(pdf)
        print(f"  batch {i // args.batch + 1:>3}: {len(chunk):>4} aerodromes, "
              f"{len(pdf):>9,} rows  (total {total:,})")

    print(f"\n{total:,} rows -> {out}")
    spark.stop()


if __name__ == "__main__":
    main()
