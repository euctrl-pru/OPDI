"""
Why does merging two roles lose arrivals?

The `recommended` flight list takes ADEP from `endpoint` and ADES from `trend`.
Its arrival half should be identical to the trend-only run built at the same
arrival parameters -- the merge is supposed to be a join, not a recomputation.
It is not identical: arrival coverage falls by nearly two points.

This compares the two tables directly: row counts, duplicate ids, and how many
tracks carry an ADES in one and not the other. Read-only.

    python benchmarks/merge_diagnosis.py --results-dir <dir>
"""

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import functions as F

import osn_sample
from osn_sample import build_spark, load_dotenv

BASE = "s3a://eurocontrol/opdi/research/flight_list_v6_{run}"


def summarise(spark, run):
    df = spark.read.parquet(BASE.format(run=run))
    n = df.count()
    ids = df.select("ID").distinct().count()
    ades = df.filter(F.col("ADES").isNotNull()).count()
    adep = df.filter(F.col("ADEP").isNotNull()).count()
    print(f"  {run:<14} rows={n:>7,}  distinct ids={ids:>7,}  "
          f"ADEP set={adep:>7,}  ADES set={ades:>7,}")
    return df


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results-dir", type=Path, required=True)
    ap.add_argument("--executors", type=int, default=6)
    ap.add_argument("--ui-port", type=int, default=4047)
    args = ap.parse_args()
    args.results_dir.mkdir(parents=True, exist_ok=True)

    load_dotenv()
    osn_sample.RESEARCH_EXECUTORS = args.executors
    osn_sample.UI_PORT = args.ui_port
    spark = build_spark(6, "8g", distributed=True)

    print("=== table shapes ===")
    rec = summarise(spark, "recommended")
    p1 = summarise(spark, "path1_penalty")

    # path1 is trend-only at the arrival parameters the recommendation uses, so
    # its ADES column is the one `recommended` should have reproduced exactly.
    a = rec.select(F.col("ID").alias("id"), F.col("ADES").alias("ades_merged"))
    b = p1.select(F.col("ID").alias("id"), F.col("ADES").alias("ades_alone"))
    j = a.join(b, "id", "outer")

    print("\n=== arrivals, merged against trend-only ===")
    agg = j.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.when(F.col("ades_merged").isNull() & F.col("ades_alone").isNotNull(), 1)
              .otherwise(0)).alias("lost_by_merge"),
        F.sum(F.when(F.col("ades_merged").isNotNull() & F.col("ades_alone").isNull(), 1)
              .otherwise(0)).alias("gained_by_merge"),
        F.sum(F.when(F.col("ades_merged") != F.col("ades_alone"), 1)
              .otherwise(0)).alias("disagree"),
        F.sum(F.when(F.col("ades_merged") == F.col("ades_alone"), 1)
              .otherwise(0)).alias("agree"),
    ).first()
    for k in ("rows", "agree", "disagree", "lost_by_merge", "gained_by_merge"):
        print(f"  {k:<16} {agg[k]:>8,}")

    # If the merge duplicates a track, the id appears more than once and any
    # downstream join to ground truth can pick either copy.
    print("\n=== duplicate ids in the merged table ===")
    dup = (rec.groupBy("ID").count().filter(F.col("count") > 1))
    nd = dup.count()
    print(f"  ids appearing more than once: {nd:,}")
    if nd:
        dup.orderBy(F.col("count").desc()).show(5, truncate=False)

    import pandas as pd

    rows_rec, rows_p1 = rec.count(), p1.count()
    pd.DataFrame([
        {"table": "recommended (ADEP endpoint + ADES trend)", "rows": rows_rec,
         "adep_set": rec.filter(F.col("ADEP").isNotNull()).count(),
         "ades_set": rec.filter(F.col("ADES").isNotNull()).count()},
        {"table": "path1_penalty (trend only, same arrival parameters)",
         "rows": rows_p1,
         "adep_set": p1.filter(F.col("ADEP").isNotNull()).count(),
         "ades_set": p1.filter(F.col("ADES").isNotNull()).count()},
    ]).to_csv(args.results_dir / "merge_shapes.csv", index=False)

    pd.DataFrame([{
        "rows_joined": agg["rows"], "agree": agg["agree"],
        "disagree": agg["disagree"], "lost_by_merge": agg["lost_by_merge"],
        "gained_by_merge": agg["gained_by_merge"], "duplicate_ids": nd,
    }]).to_csv(args.results_dir / "merge_agreement.csv", index=False)
    print(f"\nwritten to {args.results_dir}")

    spark.stop()


if __name__ == "__main__":
    main()
