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

**Content** (``events`` also writes it for the published events and flight
list; ``tracks`` and ``statevectors`` write only it). Rows are matched on a
key, with an occurrence number inside equal keys: rows in each run, matched,
only before, only after; for every other column, the number of matched rows
whose values differ (exactly for text, integers, times and flags, by more
than 1e-9 for decimal numbers, and as JSON text for lists); and, for tables
of tracks, the track ids in each run and in both.

    day_compare.py tracks --day 2026-06-14 --before m12before --after m12after --out ...
    day_compare.py statevectors --before s2before --after s2after --out ...

``flightlist`` compares the working flight list of the day (step 03, and
04b when it ran) on the track id. ``tracks`` compares ``osn_tracks`` and
``osn_tracks_clean`` of the day on
``(track_id, event_time)``; ``statevectors`` compares the whole
``osn_statevectors_v2`` of both runs on ``(icao24, event_time)``. The output
and the input tables are recorded in the ``_manifest.json`` beside the CSV
(``benchmarks/provenance.py``).

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
#: Tolerance of decimal numbers in the content comparison.
CONTENT_TOLERANCE = 1e-9


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


def content_rows(part: str, counts: dict, diffs: dict, ids: dict | None = None) -> list[dict]:
    """The CSV rows of a content comparison.

    Args:
        part: Name of the table compared.
        counts: ``before``, ``after`` and ``matched`` rows, and optionally
            ``differing``: matched rows with at least one column differing.
        diffs: For each compared column, the matched rows where it differs.
        ids: ``before``, ``after`` and ``both``: distinct track ids, or None.
    """
    b, a, m = counts["before"], counts["after"], counts["matched"]
    differing = {c: n for c, n in diffs.items() if n}
    rows = [
        {
            "part": part,
            "item": "rows",
            "before": b,
            "after": a,
            "matched": m,
            "only_before": b - m,
            "only_after": a - m,
            "matched_share": round(share(m, b, a), 6),
            "note": (
                "columns differing: "
                + "; ".join(f"{c} on {n}" for c, n in sorted(differing.items()))
                if differing
                else f"every other column equal on all matched rows ({len(diffs)} columns)"
            ),
        }
    ]
    if "differing" in counts:
        rows.append(
            {
                "part": part,
                "item": "matched rows with a column differing",
                "matched": m,
                "matched_share": round(1 - counts["differing"] / m, 6) if m else 1.0,
                "note": f"{counts['differing']} matched rows differ in at least one column",
            }
        )
    if ids is not None:
        rows.append(
            {
                "part": part,
                "item": "track ids",
                "before": ids["before"],
                "after": ids["after"],
                "matched": ids["both"],
                "only_before": ids["before"] - ids["both"],
                "only_after": ids["after"] - ids["both"],
                "matched_share": round(share(ids["both"], ids["before"], ids["after"]), 6),
                "note": "",
            }
        )
    for c in sorted(diffs):
        rows.append(
            {
                "part": part,
                "item": f"column {c}",
                "matched": m,
                "matched_share": round(1 - diffs[c] / m, 6) if m else 1.0,
                "note": f"{diffs[c]} matched rows differ",
            }
        )
    return rows


def compare_content(
    before, after, key: list, part: str, id_col: str | None = None, skip=()
) -> list[dict]:
    """Rows matched on ``key`` and every other column compared (see the module text)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, FloatType

    common = [c for c in before.columns if c in after.columns and c not in skip]
    rest = [c for c in common if c not in key]
    kinds = {f.name: f.dataType for f in before.schema.fields}

    def plain(df):
        cols = []
        for c in common:
            t = kinds[c].typeName()
            cols.append(F.to_json(F.col(c)).alias(c) if t in ("array", "map", "struct") else c)
        df = df.select(*cols)
        w = Window.partitionBy(*key).orderBy(*[F.col(c).asc_nulls_first() for c in rest])
        return df.withColumn("_n", F.row_number().over(w))

    k = key + ["_n"]
    bb = plain(before).select(*k, *[F.col(c).alias(f"b_{c}") for c in rest], F.lit(1).alias("_b"))
    aa = plain(after).select(*k, *[F.col(c).alias(f"a_{c}") for c in rest], F.lit(1).alias("_a"))
    j = bb.join(aa, on=k, how="full_outer")
    both = F.col("_a").isNotNull() & F.col("_b").isNotNull()
    aggs = [
        F.count("_b").alias("before"),
        F.count("_a").alias("after"),
        F.sum(F.when(both, 1).otherwise(0)).alias("matched"),
    ]
    any_diff = F.lit(False)
    for i, c in enumerate(rest):
        x, y = F.col(f"b_{c}"), F.col(f"a_{c}")
        if isinstance(kinds[c], (DoubleType, FloatType)):
            diff = (x.isNull() != y.isNull()) | F.coalesce(
                F.abs(x - y) > CONTENT_TOLERANCE, F.lit(False)
            )
        else:
            diff = ~x.eqNullSafe(y)
        any_diff = any_diff | diff
        aggs.append(F.sum(F.when(both & diff, 1).otherwise(0)).alias(f"_d{i}"))
    aggs.append(F.sum(F.when(both & any_diff, 1).otherwise(0)).alias("differing"))
    r = j.agg(*aggs).first()
    counts = {n: int(r[n] or 0) for n in ("before", "after", "matched", "differing")}
    diffs = {c: int(r[f"_d{i}"] or 0) for i, c in enumerate(rest)}
    ids = None
    if id_col:
        ib = before.select(F.col(id_col).alias("_id")).distinct().withColumn("_b", F.lit(1))
        ia = after.select(F.col(id_col).alias("_id")).distinct().withColumn("_a", F.lit(1))
        r = (
            ib.join(ia, "_id", "full_outer")
            .agg(
                F.count("_b").alias("before"),
                F.count("_a").alias("after"),
                F.sum(
                    F.when(F.col("_a").isNotNull() & F.col("_b").isNotNull(), 1).otherwise(0)
                ).alias("both"),
            )
            .first()
        )
        ids = {n: int(r[n] or 0) for n in ("before", "after", "both")}
    return content_rows(part, counts, diffs, ids)


def write_rows(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(COLUMNS), extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in COLUMNS})


SUBCOMMANDS = ("events", "tracks", "statevectors", "flightlist")


def table(label: str, name: str) -> str:
    """A working table of the run ``label`` (as ``day_timing.py`` writes it)."""
    return f"{PREFIX}/{label}/{name}"


def inputs_of(command: str, before: str, after: str, day: date | None) -> list[str]:
    """The folders a comparison reads, for the manifest."""
    if command == "tracks":
        names = ["osn_tracks", "osn_tracks_clean"]
        return [table(label, n) for label in (before, after) for n in names]
    if command == "statevectors":
        return [table(label, "osn_statevectors_v2") for label in (before, after)]
    if command == "flightlist":
        return [table(label, "opdi_flight_list") for label in (before, after)]
    return [
        published(label, kind, day)
        for label in (before, after)
        for kind in ("flight_events", "flight_list")
    ]


def stamp(out: Path, argv: list, inputs: list[str], notes: str = "") -> None:
    """Record ``out`` in the manifest beside it, with the command line and the inputs."""
    import provenance

    provenance.record(
        out.parent,
        out.name,
        script="benchmarks/day_compare.py",
        argv=list(argv),
        code_paths=["benchmarks/day_compare.py", "src/opdi"],
        notes=notes,
        input_tables=inputs,
    )


def parse(argv: list):
    """Parse the command line; without a subcommand, ``events`` is meant."""
    if not argv or argv[0] not in SUBCOMMANDS + ("-h", "--help"):
        argv = ["events"] + list(argv)
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="command", required=True)
    for name in SUBCOMMANDS:
        p = sub.add_parser(name)
        p.add_argument("--day", required=(name != "statevectors"))
        p.add_argument("--before", required=True, help="label of the run before")
        p.add_argument("--after", required=True, help="label of the run after")
        p.add_argument("--out", type=Path, required=True)
        p.add_argument("--executors", type=int, default=6)
        p.add_argument("--notes", default="", help="what the two runs are, for the manifest")
    return ap.parse_args(argv)


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    args = parse(argv)
    day = date.fromisoformat(args.day) if args.day else None

    from day_timing import build_config
    from run_airspace_events_day import S3_PATIENCE

    from opdi.utils.spark_helpers import SparkSessionManager

    spark = SparkSessionManager.create_session(
        app_name=f"day compare {args.command} {args.before} {args.after}",
        config=build_config(args.executors),
        distributed=True,
        extra_configs=S3_PATIENCE,
    )
    try:
        from pyspark.sql import functions as F

        if args.command == "tracks":
            rows = []
            for name in ("osn_tracks", "osn_tracks_clean"):
                tb, ta = (
                    spark.read.parquet(table(label, name)).filter(F.col("dof") == F.lit(day))
                    for label in (args.before, args.after)
                )
                rows += compare_content(tb, ta, ["track_id", "event_time"], name, id_col="track_id")
        elif args.command == "flightlist":
            fb, fa = (
                spark.read.parquet(table(label, "opdi_flight_list")).filter(
                    F.col("DOF") == F.lit(day)
                )
                for label in (args.before, args.after)
            )
            rows = compare_content(fb, fa, ["id"], "opdi_flight_list")
        elif args.command == "statevectors":
            sb, sa = (
                spark.read.parquet(table(label, "osn_statevectors_v2"))
                for label in (args.before, args.after)
            )
            rows = compare_content(sb, sa, ["icao24", "event_time"], "osn_statevectors_v2")
        else:
            ev_b = spark.read.parquet(published(args.before, "flight_events", day))
            ev_a = spark.read.parquet(published(args.after, "flight_events", day))
            fl_b = spark.read.parquet(published(args.before, "flight_list", day))
            fl_a = spark.read.parquet(published(args.after, "flight_list", day))
            rows = compare_flights(fl_b, fl_a)
            rows += compare_events(ev_b, ev_a)
            rows += compare_sums(fl_b, fl_a, "flight_list")
            rows += compare_sums(ev_b, ev_a, "flight_events")
            rows += compare_content(
                ev_b, ev_a, ["flight_id", "type", "event_time"], "flight_events content"
            )
            rows += compare_content(fl_b, fl_a, ["flight_id"], "flight_list content")
    finally:
        spark.stop()
    write_rows(args.out, rows)
    stamp(args.out, argv, inputs_of(args.command, args.before, args.after, day), args.notes)
    for r in rows:
        print(
            f"{r['part']:<22} {str(r['item']):<34} {str(r.get('before', '')):>14} "
            f"{str(r.get('after', '')):>14} {str(r.get('matched', '')):>10} "
            f"{str(r.get('matched_share', '')):>9}  {r.get('note', '')}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
