#!/usr/bin/env python
"""Score both ADS-C fusion arms against APDF, per milestone and per aerodrome.

The question is narrow: with the same state vectors, the same segmentation,
the same cleaning thresholds and the same detectors, does adding ADS-C change
what the pipeline detects? So both arms are scored through one truth frame,
built once -- ``events_gt.build(spark, "2026", airports="study")``, the
flight-events-v4 period and study set.

**Nothing about the truth is re-derived here.** The APDF-to-aircraft-address
bridge, the Europe/Paris timestamps behind the ``_UTC`` suffixes, the
IATA-versus-ICAO callsign vocabularies and the eight-character padding on
ADS-B callsigns each cost this project a run; ``events_gt`` closes all four and
is imported rather than imitated.

**Read the result against the coverage-gain and time-sync jobs, not alone.**
ADS-C is a cruise feed: ~5k rows a day inside the box, 94-99% of them above
FL300, never on the ground, 1-67 rows a day below 10,000 ft. It cannot reach a
runway or A-CDM milestone, and a flat table for that family is the expected
outcome rather than a failure of the measurement. The families where a change
is physically possible are the ones anchored at the edges of coverage:
``first_seen``, ``last_seen``, the FL crossings and -- through the flight list
-- ADEP/ADES on North Atlantic arrivals.

    unset OPDI_WAREHOUSE    # table paths here are absolute, but events_gt's
                            # reference reads are not: production exports this
                            # variable and it would move the truth frame.
    python benchmarks/adsc_events_compare.py --results-dir /tmp/adsc_score
"""

import argparse
import csv
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import functions as F  # noqa: E402

import events_gt  # noqa: E402
import events_score  # noqa: E402
from event_bench import detected_events, milestone_map  # noqa: E402

#: The warehouse the ``opensky`` environment resolves relative table names
#: against. Named because :func:`arm_tables` has to express one of its two
#: paths relative to it and the other absolute -- see that docstring.
WAREHOUSE = "s3a://eurocontrol/opdi"

ARM_PREFIX = f"{WAREHOUSE}/research/adsc"
ARMS = ("base", "merged")

#: The flight-events-v4 period: APDF 202606, days 2026-06-05/06/07. Named here
#: rather than passed, because a comparison between arms over different days
#: is not a comparison.
PERIOD = "2026"


def scoring_config():
    """The European configuration, stated rather than defaulted.

    APDF is European and the arms live under ``opdi/``. Since the worldwide
    default, saying nothing resolves the flight list under ``opdi-world/``.
    """
    from opdi.config import OPDIConfig

    return OPDIConfig.for_environment("opensky", worldwide=False)


def arm_tables(prefix: str, arm: str) -> dict:
    """One arm's event table and flight list, each in the form its reader wants.

    **Two paths, two conventions, and the asymmetry is not cosmetic:**

    * ``events`` is **absolute**. ``detected_events`` branches on the
      ``s3a://`` prefix and reads it with ``spark.read.parquet``, so a
      relative name there would be resolved against ``OPDI_WAREHOUSE`` --
      which production exports.
    * ``flight_list`` is **warehouse-relative**. It is handed to
      ``storage.read_table``, which resolves every name as
      ``{warehouse}/{name}``. An absolute path there yields
      ``s3a://eurocontrol/opdi/s3a:/eurocontrol/opdi/research/...`` -- a path
      containing its own prefix twice -- and a PATH_NOT_FOUND that names it.
      That is exactly how this job failed on its first run. ``event_bench``'s
      own ``PERIOD_TRACKS`` entries use relative names (``research/
      flight_list_2026``) for the same reason.

    Both are derived from one *prefix* argument so the two cannot drift apart.
    """
    if arm not in ARMS:
        raise ValueError(f"unknown arm {arm!r}; expected one of {ARMS}")
    if not prefix.startswith(WAREHOUSE + "/"):
        raise ValueError(
            f"prefix {prefix!r} must sit under {WAREHOUSE!r}: the flight list "
            f"is read through StorageManager, which can only resolve a name "
            f"relative to that warehouse."
        )
    relative = prefix[len(WAREHOUSE) + 1:]
    return {
        "events": f"{prefix}/{arm}/opdi_flight_events",
        "flight_list": f"{relative}/{arm}/opdi_flight_list",
    }


def score_arm(spark, storage, truth, prefix: str, arm: str, mapping: dict):
    """``(per_milestone, per_airport)`` for one arm, each carrying ``arm``."""
    tables = arm_tables(prefix, arm)
    det = detected_events(
        spark, tables["events"], storage=storage, identity="flight_list",
        flight_list=tables["flight_list"], mapping=mapping,
    )
    aligned = events_score.align(truth, det).cache()
    per_ms = events_score.score(aligned).withColumn("arm", F.lit(arm))
    per_ap = events_score.score_by_airport(aligned).withColumn("arm", F.lit(arm))
    events_score.guard_not_all_zero(per_ms)
    return per_ms, per_ap


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--prefix", default=ARM_PREFIX)
    ap.add_argument("--arms", nargs="+", default=list(ARMS), choices=list(ARMS))
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--out-name", default="adsc_events_2026.csv")
    ap.add_argument("--by-airport-name",
                    default="adsc_events_by_airport_2026.csv")
    ap.add_argument("--cores", type=int, default=4)
    ap.add_argument("--driver-memory", default="8g")
    ap.add_argument("--executors", type=int, default=6)
    ap.add_argument("--ui-port", type=int, default=4066)
    args = ap.parse_args()

    import osn_sample
    from osn_sample import build_spark, load_dotenv

    load_dotenv()
    osn_sample.UI_PORT = args.ui_port
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(args.cores, args.driver_memory, distributed=True)
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    from opdi.utils.storage import StorageManager

    config = scoring_config()
    storage = StorageManager(spark, config)

    # The shipped vocabulary, which is what both arms' step 04 ran. `landing`
    # means two different physical things either side of
    # `emit_runway_milestones`, so the map has to come from the configuration
    # that produced the table rather than from a version string.
    mapping = milestone_map(config.events)

    ms_rows, ap_rows = [], []
    try:
        truth, _rings, _report = events_gt.build(
            spark, PERIOD, airports="study")
        truth.cache()
        for arm in args.arms:
            print(f"\n=== scoring arm {arm} ===", flush=True)
            per_ms, per_ap = score_arm(
                spark, storage, truth, args.prefix, arm, mapping)
            ms_rows += [r.asDict() for r in per_ms.collect()]
            ap_rows += [r.asDict() for r in per_ap.collect()]
    finally:
        spark.stop()

    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)

    def write(name, rows):
        if not rows:
            raise SystemExit(
                f"{name}: no rows. An empty score table is not a null result "
                f"-- it means the identity bridge found nothing, and a paper "
                f"must not report it as agreement.")
        fields = ["arm"] + sorted(set(rows[0]) - {"arm"})
        with open(out / name, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)
        print(f"  wrote {out / name} ({len(rows)} rows)")

    write(args.out_name, ms_rows)
    write(args.by_airport_name, ap_rows)

    import provenance
    for name in (args.out_name, args.by_airport_name):
        provenance.record(
            out, name, "benchmarks/adsc_events_compare.py", sys.argv[1:],
            ["benchmarks/adsc_events_compare.py", "benchmarks/events_gt.py",
             "benchmarks/events_score.py", "benchmarks/event_bench.py",
             "src/opdi/pipeline/events.py"],
            inputs={"period": PERIOD, "arms": args.arms, "airports": "study"},
            input_tables=[arm_tables(args.prefix, a)["events"]
                          for a in args.arms])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
