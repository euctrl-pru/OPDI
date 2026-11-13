#!/usr/bin/env python
"""Compare the outputs of two runs of one day: same flights, same events, same measurements.

    export PYSPARK_DRIVER_PYTHON=$PWD/.venv310/bin/python
    unset PYSPARK_PYTHON
    .venv310/bin/python -u benchmarks/day_compare.py --day 2026-06-14 \\
        --before base --after final --out <paper>/data/result_comparison.csv

Both runs are folders written by ``benchmarks/day_timing.py`` under
``s3a://eurocontrol/opdi-world/research/speed/<label>/``; nothing is written
there. The comparison reads the published files of the day (step 09), which
hold every event with its measurements and the flight list, and the working
tables of steps 03 and 04.

**Flight list.** Flights are matched by aircraft address and first time
seen (``icao24``, ``first_seen``): flights only in one run, and matched
flights whose departure or destination airport differs.

**Events.** Matched by flight, type and time to the second. A key that
occurs more than once is matched as often as it occurs in both runs. For
each type: events before, after, matched, only before, only after, and the
matched share (matched / the larger of the two counts).

**Measurements.** For matched events, each measurement column of the
published file (distance flown, time passed, the holding and point merge
measures) is compared: the number of matched events where the two values
differ by more than 1e-6, or where one is missing and the other is not.

**Other columns.** For matched events, the number whose position, altitude
or ``info`` text differ, so that a moved event is not counted as unchanged.

**Files.** Rows and the sum of each numeric column of the published files.

The table has the columns ``part``, ``item``, ``before``, ``after``,
``matched``, ``only_before``, ``only_after``, ``matched_share`` and ``note``.
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

PREFIX = "s3a://eurocontrol/opdi-world/research/speed"
COLUMNS = (
    "part",
    "item",
    "before",
    "after",
    "matched",
    "only_before",
    "only_after",
    "matched_share",
    "note",
)
#: Measurement columns of the published events file.
MEASURES = (
    "distance_flown_nm",
    "time_passed_s",
    "holding_duration_s",
    "holding_distance_nm",
    "holding_circuits",
    "pms_leg_time_s",
    "pms_leg_distance_nm",
    "pms_direct_to_distance_nm",
)
#: Columns of a matched event compared exactly (numbers within 1e-6).
EVENT_FIELDS = ("longitude", "latitude", "altitude_ft", "info")
TOLERANCE = 1e-6


def share(matched: int, before: int, after: int) -> float:
    """Matched events as a share of the larger count; 1.0 when both are empty."""
    largest = max(before, after)
    return 1.0 if largest == 0 else matched / largest


def published(label: str, kind: str, day: date) -> str:
    return f"{PREFIX}/{label}/published/{kind}/{kind}_{day:%Y%m%d}.parquet"


def _keyed_events(df):
    """Events with the match key and an occurrence number inside equal keys."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    key = ["flight_id", "type", "_sec"]
    df = df.withColumn("_sec", F.date_trunc("second", F.col("event_time")))
    w = Window.partitionBy(*key).orderBy("longitude", "latitude", "altitude_ft", "info", "event_id")
    return df.withColumn("_n", F.row_number().over(w)), key + ["_n"]


def _differs(a, b, numeric: bool):
    from pyspark.sql import functions as F

    if numeric:
        return (a.isNull() != b.isNull()) | (F.abs(a - b) > TOLERANCE)
    return ~a.eqNullSafe(b)


def compare_events(before, after) -> list[dict]:
    """Rows per event type, and per measurement and field over all matched events."""
    from pyspark.sql import functions as F

    b, key = _keyed_events(before)
    a, _ = _keyed_events(after)
    fields = [c for c in MEASURES + EVENT_FIELDS if c in before.columns and c in after.columns]
    numeric = {c: c != "info" for c in fields}
    bb = b.select(*key, *[F.col(c).alias(f"b_{c}") for c in fields]).withColumn("_b", F.lit(1))
    aa = a.select(*key, *[F.col(c).alias(f"a_{c}") for c in fields]).withColumn("_a", F.lit(1))
    joined = bb.join(aa, on=key, how="full_outer")
    aggs = [
        F.count("_b").alias("before"),
        F.count("_a").alias("after"),
        F.sum(F.when(F.col("_a").isNotNull() & F.col("_b").isNotNull(), 1).otherwise(0)).alias(
            "matched"
        ),
    ]
    both = F.col("_a").isNotNull() & F.col("_b").isNotNull()
    for c in fields:
        diff = both & _differs(F.col(f"b_{c}"), F.col(f"a_{c}"), numeric[c])
        aggs.append(F.sum(F.when(diff, 1).otherwise(0)).alias(f"diff_{c}"))
    per_type = joined.groupBy("type").agg(*aggs).orderBy("type").collect()

    rows = []
    totals = {f"diff_{c}": 0 for c in fields}
    for r in per_type:
        rows.append(
            {
                "part": "events",
                "item": r["type"],
                "before": r["before"],
                "after": r["after"],
                "matched": r["matched"],
                "only_before": r["before"] - r["matched"],
                "only_after": r["after"] - r["matched"],
                "matched_share": round(share(r["matched"], r["before"], r["after"]), 6),
                "note": "; ".join(
                    f"{c} differs on {r[f'diff_{c}']}" for c in fields if r[f"diff_{c}"]
                ),
            }
        )
        for c in fields:
            totals[f"diff_{c}"] += r[f"diff_{c}"]
    matched_all = sum(r["matched"] for r in per_type)
    for c in fields:
        rows.append(
            {
                "part": "measurement" if c in MEASURES else "event field",
                "item": c,
                "matched": matched_all,
                "note": f"{totals[f'diff_{c}']} matched events differ"
                + (f" by more than {TOLERANCE:g}" if numeric[c] else ""),
                "only_before": "",
                "only_after": "",
                "before": "",
                "after": "",
                "matched_share": (
                    round(1 - totals[f"diff_{c}"] / matched_all, 6) if matched_all else 1.0
                ),
            }
        )
    return rows


def compare_flights(before, after) -> list[dict]:
    """Flights matched by (icao24, first_seen); airports of the matched flights."""
    from pyspark.sql import functions as F

    key = ["icao24", "first_seen"]
    bb = before.select(*key, F.col("adep").alias("b_adep"), F.col("ades").alias("b_ades"))
    aa = after.select(*key, F.col("adep").alias("a_adep"), F.col("ades").alias("a_ades"))
    bb = bb.withColumn("_b", F.lit(1))
    aa = aa.withColumn("_a", F.lit(1))
    j = bb.join(aa, on=key, how="full_outer")
    both = F.col("_a").isNotNull() & F.col("_b").isNotNull()
    r = j.agg(
        F.count("_b").alias("before"),
        F.count("_a").alias("after"),
        F.sum(F.when(both, 1).otherwise(0)).alias("matched"),
        F.sum(F.when(both & ~F.col("b_adep").eqNullSafe(F.col("a_adep")), 1).otherwise(0)).alias(
            "adep"
        ),
        F.sum(F.when(both & ~F.col("b_ades").eqNullSafe(F.col("a_ades")), 1).otherwise(0)).alias(
            "ades"
        ),
    ).first()
    return [
        {
            "part": "flight list",
            "item": "flights",
            "before": r["before"],
            "after": r["after"],
            "matched": r["matched"],
            "only_before": r["before"] - r["matched"],
            "only_after": r["after"] - r["matched"],
            "matched_share": round(share(r["matched"], r["before"], r["after"]), 6),
            "note": f"departure airport differs on {r['adep']}; "
            f"destination airport differs on {r['ades']}",
        }
    ]


def compare_sums(before, after, name: str) -> list[dict]:
    """Rows and the sum of every numeric column of one published file."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import NumericType

    cols = [f.name for f in before.schema.fields if isinstance(f.dataType, NumericType)]
    cols = [c for c in cols if c in after.columns]

    def sums(df):
        return df.agg(F.count(F.lit(1)).alias("_rows"), *[F.sum(c).alias(c) for c in cols]).first()

    sb, sa = sums(before), sums(after)
    rows = [
        {
            "part": f"file {name}",
            "item": "rows",
            "before": sb["_rows"],
            "after": sa["_rows"],
            "note": "",
        }
    ]
    for c in cols:
        rows.append(
            {
                "part": f"file {name}",
                "item": f"sum of {c}",
                "before": sb[c],
                "after": sa[c],
                "note": "",
            }
        )
    return rows


def write_rows(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(COLUMNS), extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in COLUMNS})


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--day", required=True)
    ap.add_argument("--before", required=True, help="label of the run before")
    ap.add_argument("--after", required=True, help="label of the run after")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--executors", type=int, default=6)
    args = ap.parse_args(argv)
    day = date.fromisoformat(args.day)

    from day_timing import build_config
    from run_airspace_events_day import S3_PATIENCE

    from opdi.utils.spark_helpers import SparkSessionManager

    spark = SparkSessionManager.create_session(
        app_name=f"day compare {args.before} {args.after}",
        config=build_config(args.executors),
        distributed=True,
        extra_configs=S3_PATIENCE,
    )
    try:
        ev_b = spark.read.parquet(published(args.before, "flight_events", day))
        ev_a = spark.read.parquet(published(args.after, "flight_events", day))
        fl_b = spark.read.parquet(published(args.before, "flight_list", day))
        fl_a = spark.read.parquet(published(args.after, "flight_list", day))
        rows = compare_flights(fl_b, fl_a)
        rows += compare_events(ev_b, ev_a)
        rows += compare_sums(fl_b, fl_a, "flight_list")
        rows += compare_sums(ev_b, ev_a, "flight_events")
    finally:
        spark.stop()
    write_rows(args.out, rows)
    for r in rows:
        print(
            f"{r['part']:<14} {str(r['item']):<34} {str(r.get('before', '')):>14} "
            f"{str(r.get('after', '')):>14} {str(r.get('matched', '')):>10} "
            f"{str(r.get('matched_share', '')):>9}  {r.get('note', '')}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
