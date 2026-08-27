"""How much ground-truthed traffic the sample carries per elevation band.

The feasibility gate for the v6.1 study. Arm C asks whether moving `trend`'s
altitude cut onto the field-elevation datum helps *at elevated aerodromes
specifically*; if the sample holds only a handful of movements above 3,000 ft,
that question has no answer and the study needs re-scoping before anything
expensive is rebuilt.

Reads ground truth and the aerodrome reference only. No vote cache, no track
scan -- it is meant to be run first and to cost almost nothing.

Out-of-area aerodromes are excluded. `label_ground_truth` rewrites them to the
OOA sentinel, and a flight whose origin lies outside the observed area is one
no method could have named from the trajectory -- counting it would pad the
denominator with flights the datum change cannot possibly affect.

    python benchmarks/elevation_census.py --results-dir <dir>
"""

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import functions as F

import osn_sample
from adep_ades import OOA, airport_locations, label_ground_truth, load_ground_truth
from elevation_bands import BANDS, airport_elevations, elevation_band
from osn_sample import build_spark, load_dotenv

DAYS_2025 = ["2025-06-05", "2025-06-06", "2025-06-07"]
DAYS_2024 = ["2024-06-05", "2024-06-06", "2024-06-07"]


def census(spark, months, days, period):
    """Ground-truthed movements per (band, role) for one period."""
    gt = label_ground_truth(
        load_ground_truth(spark, months, days), airport_locations(spark)
    )
    elev = airport_elevations(spark)

    # One row per (flight, role). A departure from a high field and an arrival
    # at one are both movements the datum change could affect, and counting
    # flights instead would hide whichever role is the scarcer of the two.
    moves = (
        gt.select(F.col("gt_adep").alias("apt"), F.lit("departure").alias("role"))
        .unionByName(
            gt.select(F.col("gt_ades").alias("apt"), F.lit("arrival").alias("role"))
        )
        .filter(F.col("apt").isNotNull() & (F.col("apt") != F.lit(OOA)))
    )

    return (
        moves.join(elev, moves.apt == elev._apt, "left")
        .withColumn("band", elevation_band(F.col("_elev_ft")))
        .groupBy("band", "role")
        .agg(
            F.count("*").alias("movements"),
            F.countDistinct("apt").alias("aerodromes"),
        )
        .withColumn("period", F.lit(period))
    )


def top_aerodromes(spark, months, days, period, min_elev_ft=1500.0, n=15):
    """The elevated aerodromes carrying the most traffic, for context.

    A band total can be healthy while resting on one busy field, which would
    make Arm C a measurement of that field rather than of elevation. This is
    how that shows up before the expensive part rather than after it.
    """
    gt = label_ground_truth(
        load_ground_truth(spark, months, days), airport_locations(spark)
    )
    elev = airport_elevations(spark)
    moves = (
        gt.select(F.col("gt_adep").alias("apt"))
        .unionByName(gt.select(F.col("gt_ades").alias("apt")))
        .filter(F.col("apt").isNotNull() & (F.col("apt") != F.lit(OOA)))
    )
    return (
        moves.join(elev, moves.apt == elev._apt, "inner")
        .filter(F.col("_elev_ft") >= F.lit(min_elev_ft))
        .groupBy("apt", "_elev_ft")
        .agg(F.count("*").alias("movements"))
        .withColumn("period", F.lit(period))
        .orderBy(F.col("movements").desc())
        .limit(n)
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--executors", type=int, default=4)
    ap.add_argument("--ui-port", type=int, default=4046)
    args = ap.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
    load_dotenv()
    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(4, "4g", distributed=True)
    spark.sparkContext.setLogLevel("ERROR")

    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)

    both = census(spark, ["202506"], DAYS_2025, "2025-06").unionByName(
        census(spark, ["202406"], DAYS_2024, "2024-06")
    )
    pdf = both.toPandas()
    pdf.to_csv(out / "elevation_census.csv", index=False)

    order = [label for label, _, _ in BANDS] + ["unknown"]
    print("\n=== ground-truthed in-area movements per field-elevation band ===")
    print(
        pdf.pivot_table(
            index="band", columns=["period", "role"],
            values="movements", aggfunc="sum", fill_value=0,
        ).reindex(order).to_string()
    )
    print("\n=== distinct aerodromes per band ===")
    print(
        pdf.pivot_table(
            index="band", columns=["period", "role"],
            values="aerodromes", aggfunc="sum", fill_value=0,
        ).reindex(order).to_string()
    )

    tops = top_aerodromes(spark, ["202506"], DAYS_2025, "2025-06").toPandas()
    tops.to_csv(out / "elevation_top_aerodromes.csv", index=False)
    print("\n=== busiest aerodromes at or above 1,500 ft (2025 sample) ===")
    print(tops.to_string(index=False))

    spark.stop()


if __name__ == "__main__":
    main()
