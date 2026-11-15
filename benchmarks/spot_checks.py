#!/usr/bin/env python
"""Spot checks: one processed day and a few dozen examples of every published item.

    export PYSPARK_DRIVER_PYTHON=$PWD/.venv310/bin/python
    unset PYSPARK_PYTHON
    # 1. Steps 01 to 04b and 09 of one day, from the raw archive (about 2 h).
    .venv310/bin/python -u benchmarks/spot_checks.py run-day --day 2026-06-14 --label d0614
    # 2. Draw the examples and bring them, with the reports around them, to the pod.
    .venv310/bin/python -u benchmarks/spot_checks.py sample --day 2026-06-14 --label d0614
    # 3. Write one reading sheet per example (pandas only, no Spark).
    .venv310/bin/python -u benchmarks/spot_checks.py sheet
    # 4. Delete the processed day.
    .venv310/bin/python -u benchmarks/spot_checks.py clean --label d0614

**What it is for.** A reader who wants to find odd cases in a flight event
needs to see single examples: the event, the flight it belongs to, the other
events of the same track and the position reports around it, and the airport
or Network Manager record of the same flight where one exists. This script
prepares those examples so that they can be read without Spark.

**run-day** runs the pipeline's own steps through ``benchmarks/day_timing.py``
(the same code as the daily run), with every table written under
``s3a://eurocontrol/opdi-world/research/spot/<label>/``. The position reports
are deleted after step 02 and the uncleaned tracks after step 02a; the
cleaned tracks, the flight list, the endpoint candidates, the events, the
measurements, the removed events and the published files are kept. Step 09
runs in a Spark session of its own (it ran short of memory straight after
step 04 in the same session).

**sample** reads the published events and flight list of the day and, per
item (see :func:`items`), computes a few measures of every event, draws 30
examples at random (fixed seed) and up to 20 from the tails of those
measures, and brings to the driver only those examples, the events of their
tracks and the reports from 3 minutes before to 3 minutes after each (for
the items that need the whole flight: the whole track, thinned). It also
writes, per item, the percentiles of the measures over all events of the day.

**sheet** turns each item's parquet into one markdown file of sheets, grouped
in six folders by reading family, with ``INDEX.md`` and ``inventory.csv``.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

#: Where this script writes on the bucket, and nowhere else.
PREFIX = "s3a://eurocontrol/opdi-world/research/spot"
ALLOWED_WRITE_ROOT = re.compile(r"^s3a://eurocontrol/opdi-world/research/spot/[A-Za-z0-9_.-]+/")
JOB = Path("/home/jupyter/.claude/jobs/616d5918/tmp/spot")
DEFAULT_SAMPLES = JOB / "samples"

#: Tables a run deletes once no later step reads them. The cleaned tracks are
#: kept: the events step is run again on them later.
DROP_AFTER = {
    "02": ("osn_statevectors_v2",),
    "02a": ("osn_tracks",),
}


# --------------------------------------------------------------------------
# run-day and clean
# --------------------------------------------------------------------------


def point_day_timing_here():
    """Make ``day_timing`` write under :data:`PREFIX` and keep the cleaned tracks."""
    import day_timing

    day_timing.PREFIX = PREFIX
    day_timing.ALLOWED_WRITE_ROOT = ALLOWED_WRITE_ROOT
    day_timing.COMPACT_ZONES = f"{PREFIX}/ref/h3_airport_detection_zones"
    day_timing.EVENTLOG_ROOT = JOB / "eventlog"
    day_timing.DEFAULT_OUT = JOB / "timing"
    day_timing.DROP_AFTER = dict(DROP_AFTER)
    return day_timing


#: The two parts of ``run-day``, each in its own process and Spark session.
RUN_PARTS = (("01", "02", "02a", "03", "04", "04b"), ("09",))


#: Spark settings of every part. A task that fails on an executor is retried on
#: another one: twice a dying executor refused all eight attempts of one task
#: within a second, which ended the step.
RUN_CONF = (
    "spark.eventLog.logStageExecutorMetrics=true",
    "spark.excludeOnFailure.enabled=true",
    "spark.excludeOnFailure.task.maxTaskAttemptsPerExecutor=1",
)


def day_timing_args(day: str, label: str, steps, executors: int) -> list:
    """The ``day_timing run`` arguments of one part of ``run-day``."""
    args = ["run", "--day", day, "--label", label, "--steps", *steps]
    args += ["--executors", str(executors), "--s3", "production", "--out", str(JOB / "timing")]
    for item in RUN_CONF:
        args += ["--conf", item]
    if any(s in ("01", "02", "02a") for s in steps):
        args += ["--full", "--drop-intermediates"]
    elif "02a" not in steps:
        # A part without the track steps reads this run's cleaned tracks and
        # endpoint candidates, not the warehouse's.
        args += [
            "--read-tables",
            f"osn_tracks_clean@{label}",
            f"opdi_endpoint_candidates@{label}",
        ]
    return args


def run_part(args) -> int:
    """One part of ``run-day`` (called in a process of its own)."""
    dt = point_day_timing_here()
    return dt.main(day_timing_args(args.day, args.label, args.steps, args.executors))


def run_day(args) -> int:
    """Steps 01 to 04b, then step 09 in a second process."""
    for steps in RUN_PARTS:
        if args.only and not set(steps) & set(args.only):
            continue
        cmd = [sys.executable, "-u", __file__, "run-part", "--day", args.day]
        cmd += ["--label", args.label, "--executors", str(args.executors), "--steps", *steps]
        print(f"\n=== {' '.join(steps)} ===", flush=True)
        rc = subprocess.call(cmd, env=os.environ.copy())
        if rc != 0:
            print(f"part {' '.join(steps)} failed with {rc}", flush=True)
            return rc
    return 0


def clean(args) -> int:
    """Delete ``research/spot/<label>/`` and nothing else."""
    dt = point_day_timing_here()
    return dt.clean(args)


# --------------------------------------------------------------------------
# Items: what is sampled, in which reading family, and from which detector
# --------------------------------------------------------------------------

#: The six reading families of Task 2, as folder name -> what they hold.
FAMILIES = {
    "a_runway": "take-off, landing, runway milestones and go-around",
    "b_block_surface": "block times and airport-surface events",
    "c_vertical": "tops of climb and descent, level segments, level-offs, flight-level crossings",
    "d_rings_fir_seen_measurements": "distance rings, FIR crossings, first and last seen, "
    "distance and time measurements",
    "e_holding_pointmerge": "holding and point merge, with their measurements",
    "f_flight_list": "the flight list",
}

#: Event type pattern -> (family, detector source file, methods page, profile).
#: The profile says which reports a sheet shows: ``window`` the reports from
#: 3 minutes before to 3 minutes after the event; ``whole`` also the altitude
#: profile of the whole flight; ``episode`` the reports from the start to the
#: end of the episode (holding, point merge leg).
EVENT_RULES = (
    (
        r"^(ATOT|ALDT)$",
        "a_runway",
        "src/opdi/pipeline/runway_ops.py (fallback: events.py)",
        "methods/takeoff-landing.qmd",
        "window",
    ),
    (
        r"^(take-off-roll|xing-runway-threshold)$",
        "a_runway",
        "src/opdi/pipeline/runway_ops.py",
        "methods/takeoff-landing.qmd",
        "window",
    ),
    (
        r"^(line-up|runway-vacated|runway-crossing-entry|runway-crossing-vacated|go-around)$",
        "a_runway",
        "src/opdi/pipeline/runway_ops.py",
        "methods/runway-milestones.qmd",
        "window",
    ),
    (
        r"^(AOBT|AIBT)$",
        "b_block_surface",
        "src/opdi/pipeline/ground.py (block_events)",
        "methods/block-times.qmd",
        "window",
    ),
    (
        r"^(entry|exit)-",
        "b_block_surface",
        "src/opdi/pipeline/layout.py",
        "methods/surface-events.qmd",
        "window",
    ),
    (
        r"^top-of-",
        "c_vertical",
        "src/opdi/pipeline/vertical_pru.py",
        "methods/vertical-events.qmd",
        "whole",
    ),
    (
        r"^level-off-",
        "c_vertical",
        "src/opdi/pipeline/level_segments.py",
        "methods/vertical-events.qmd",
        "whole",
    ),
    (
        r"^level-(start|end)$",
        "c_vertical",
        "src/opdi/pipeline/events.py (level segments)",
        "methods/vertical-events.qmd",
        "whole",
    ),
    (
        r"^xing-fl\d+$",
        "c_vertical",
        "src/opdi/pipeline/events.py (flight-level crossings)",
        "methods/vertical-events.qmd",
        "window",
    ),
    (
        r"^xing-\d+nm$",
        "d_rings_fir_seen_measurements",
        "src/opdi/pipeline/crossings.py",
        "methods/distance-rings.qmd",
        "window",
    ),
    (
        r"^xing-fir$",
        "d_rings_fir_seen_measurements",
        "src/opdi/pipeline/fir.py",
        "methods/fir-crossings.qmd",
        "window",
    ),
    (
        r"^(first_seen|last_seen)$",
        "d_rings_fir_seen_measurements",
        "src/opdi/pipeline/events.py (first and last seen)",
        "methods/first-last-seen.qmd",
        "whole",
    ),
    (
        r"^holding-",
        "e_holding_pointmerge",
        "src/opdi/pipeline/holding.py",
        "methods/holding.qmd",
        "episode",
    ),
    (
        r"^pms-leg-",
        "e_holding_pointmerge",
        "src/opdi/pipeline/point_merge.py",
        "methods/point-merge.qmd",
        "episode",
    ),
    (
        r"^pms-merge-point$",
        "e_holding_pointmerge",
        "src/opdi/pipeline/point_merge.py",
        "methods/point-merge.qmd",
        "window",
    ),
)

#: Event type -> (airport record column of the same flight, what it is). The
#: record columns are built by :func:`flight_records`.
EVENT_RECORD = {
    "AOBT": ("dep_block", "off-block time (airport record)"),
    "exit-parking_position": ("dep_block", "off-block time (airport record)"),
    "ATOT": ("dep_mvt", "take-off time (airport record)"),
    "line-up": ("dep_mvt", "take-off time (airport record)"),
    "take-off-roll": ("dep_mvt", "take-off time (airport record)"),
    "ALDT": ("arr_mvt", "landing time (airport record)"),
    "xing-runway-threshold": ("arr_mvt", "landing time (airport record)"),
    "runway-vacated": ("arr_mvt", "landing time (airport record)"),
    "go-around": ("arr_mvt", "landing time (airport record)"),
    "AIBT": ("arr_block", "in-block time (airport record)"),
    "entry-parking_position": ("arr_block", "in-block time (airport record)"),
}
#: Ring crossings with a record: radius -> record columns (outbound, inbound).
RING_RECORD = {40: ("dep_c40", "arr_c40"), 100: ("dep_c100", "arr_c100")}

#: Event type -> the type of its partner, and whether the partner comes after
#: it (True) or before it (False). ``pair_s`` is the time between the two, in
#: seconds, always positive when the partner exists.
PAIRS = {
    "take-off-roll": ("ATOT", True),
    "line-up": ("ATOT", True),
    "AOBT": ("ATOT", True),
    "xing-runway-threshold": ("ALDT", True),
    "ALDT": ("runway-vacated", True),
    "runway-vacated": ("ALDT", False),
    "AIBT": ("ALDT", False),
    "runway-crossing-entry": ("runway-crossing-vacated", True),
    "runway-crossing-vacated": ("runway-crossing-entry", False),
    "level-start": ("level-end", True),
    "level-end": ("level-start", False),
    "level-off-climb-start": ("level-off-climb-end", True),
    "level-off-climb-end": ("level-off-climb-start", False),
    "level-off-descent-start": ("level-off-descent-end", True),
    "level-off-descent-end": ("level-off-descent-start", False),
    "holding-start": ("holding-end", True),
    "holding-end": ("holding-start", False),
    "pms-leg-entry": ("pms-leg-exit", True),
    "pms-leg-exit": ("pms-leg-entry", False),
    "top-of-climb": ("top-of-descent", True),
    "top-of-descent": ("top-of-climb", False),
    "first_seen": ("last_seen", True),
    "last_seen": ("first_seen", False),
}


def pair_of(event_type: str):
    """The partner of an event type (see :data:`PAIRS`); entries and exits pair by element."""
    if event_type in PAIRS:
        return PAIRS[event_type]
    if event_type.startswith("entry-"):
        return "exit-" + event_type[len("entry-") :], True
    if event_type.startswith("exit-"):
        return "entry-" + event_type[len("exit-") :], False
    return None


#: The measures of an event, with what each is (unit in the name), and the
#: tails drawn from it: ``both`` lowest and highest, ``high`` highest only.
EVENT_MEASURES = (
    (
        "height_ft",
        "height of the aircraft above the airport concerned at the nearest report, ft",
        "both",
    ),
    ("speed_kt", "ground speed at the nearest report, kt", "both"),
    ("vrate_fpm", "vertical rate at the nearest report, ft/min", "both"),
    ("prev_gap_s", "time since the previous event of the track, s", "both"),
    ("next_gap_s", "time to the next event of the track, s", "both"),
    ("pair_s", "time to the partner event (see the item), s", "both"),
    ("airport_dist_nm", "distance of the event's position from the airport concerned, NM", "both"),
    ("record_diff_s", "event time minus the record's time, s", "both"),
    (
        "small_airport",
        "1 when the airport concerned is a small airport, heliport or seaplane base",
        "high",
    ),
)

#: Measurement items: name -> (event types carrying it, measures).
MEASUREMENT_ITEMS = {
    "m_distance_flown_nm": (
        None,
        (
            (
                "distance_flown_nm",
                "the published distance flown since the first report, NM",
                "both",
            ),
            (
                "excess_nm",
                "distance flown minus the great-circle distance from the first report "
                "to the event, NM (negative is impossible)",
                "both",
            ),
        ),
    ),
    "m_time_passed_s": (
        None,
        (
            ("time_passed_s", "the published time since the first report, s", "both"),
            (
                "elapsed_diff_s",
                "time passed minus (event time minus the track's first report), s",
                "both",
            ),
        ),
    ),
    "m_holding": (
        ("holding-end",),
        (
            ("holding_duration_s", "published holding duration, s", "both"),
            ("holding_distance_nm", "published holding distance, NM", "both"),
            ("holding_circuits", "published number of loops", "both"),
            ("pair_diff_s", "holding duration minus (holding-end minus holding-start), s", "both"),
        ),
    ),
    "m_pms": (
        ("pms-leg-exit",),
        (
            ("pms_leg_time_s", "published time on the point merge leg, s", "both"),
            ("pms_leg_distance_nm", "published distance along the leg, NM", "both"),
            (
                "pms_direct_to_distance_nm",
                "published distance to the merge point at exit, NM",
                "both",
            ),
            ("pair_diff_s", "leg time minus (pms-leg-exit minus pms-leg-entry), s", "both"),
        ),
    ),
}

#: Flight-list items: name -> (columns shown, measures, methods page, source).
FLIGHT_LIST_ITEMS = {
    "fl_identity_times": (
        ("flight_id", "icao24", "flt_id", "dof", "first_seen", "last_seen"),
        (
            ("seen_span_min", "last_seen minus first_seen, min", "both"),
            ("first_gap_s", "first_seen minus the track's first report, s", "both"),
            ("last_gap_s", "last_seen minus the track's last report, s", "both"),
        ),
    ),
    "fl_departure_airport": (
        ("adep", "adep_source", "adep_p"),
        (
            ("first_dist_adep_nm", "distance of the first report from adep, NM", "both"),
            ("first_hgt_adep_ft", "height of the first report above adep, ft", "both"),
            ("nm_adep_differs", "1 when Network Manager names another departure airport", "high"),
            ("adep_missing", "1 when adep is empty", "high"),
        ),
    ),
    "fl_destination_airport": (
        ("ades", "ades_source", "ades_p"),
        (
            ("last_dist_ades_nm", "distance of the last report from ades, NM", "both"),
            ("last_hgt_ades_ft", "height of the last report above ades, ft", "both"),
            ("nm_ades_differs", "1 when Network Manager names another destination airport", "high"),
            ("ades_missing", "1 when ades is empty", "high"),
        ),
    ),
    "fl_alternative_airports": (
        ("adep", "adep_p", "ades", "ades_p"),
        (
            ("alt_dep_dist_nm", "distance between adep and adep_p, NM", "both"),
            ("alt_arr_dist_nm", "distance between ades and ades_p, NM", "both"),
        ),
    ),
    "fl_aircraft": (
        ("registration", "model", "typecode", "icao_aircraft_class", "icao_operator"),
        (
            ("nm_type_differs", "1 when Network Manager names another aircraft type", "high"),
            ("typecode_missing", "1 when typecode is empty", "high"),
        ),
    ),
    "fl_takeoff_landing": (
        ("atot", "aldt", "rwy_dep", "rwy_dep_bearing_deg", "rwy_arr", "rwy_arr_bearing_deg"),
        (
            ("atot_diff_s", "atot minus the airport record's take-off time, s", "both"),
            ("aldt_diff_s", "aldt minus the airport record's landing time, s", "both"),
            ("airborne_min", "aldt minus atot, min", "both"),
            ("rwy_dep_differs", "1 when the airport record names another departure runway", "high"),
            ("rwy_arr_differs", "1 when the airport record names another arrival runway", "high"),
        ),
    ),
    "fl_block_stands": (
        ("aobt", "aibt", "stnd_dep", "stnd_arr"),
        (
            ("aobt_diff_s", "aobt minus the airport record's off-block time, s", "both"),
            ("aibt_diff_s", "aibt minus the airport record's in-block time, s", "both"),
            ("taxi_out_min", "atot minus aobt, min", "both"),
            ("taxi_in_min", "aibt minus aldt, min", "both"),
        ),
    ),
    "fl_rings": (
        (
            "c40_dep",
            "c50_dep",
            "c60_dep",
            "c100_dep",
            "c110_dep",
            "c120_dep",
            "c40_arr",
            "c50_arr",
            "c60_arr",
            "c100_arr",
            "c110_arr",
            "c120_arr",
        ),
        (
            ("c40_arr_diff_s", "c40_arr minus the airport record's 40 NM crossing, s", "both"),
            ("c100_arr_diff_s", "c100_arr minus the airport record's 100 NM crossing, s", "both"),
            ("c40_dep_after_atot_s", "c40_dep minus atot, s", "both"),
            ("c40_arr_before_aldt_s", "aldt minus c40_arr, s", "both"),
        ),
    ),
    "fl_flags": (
        ("superseded_dep", "superseded_arr", "movement_dep", "movement_arr"),
        (
            ("superseded_any", "1 when superseded_dep or superseded_arr is true", "high"),
            ("not_movement_any", "1 when movement_dep or movement_arr is false", "high"),
        ),
    ),
    "fl_track_duration": (
        ("track_duration_min",),
        (
            ("track_duration_min", "published track duration, min", "both"),
            (
                "duration_diff_min",
                "track_duration_min minus (last report minus first report), min",
                "both",
            ),
        ),
    ),
}
FLIGHT_LIST_SOURCE = "src/opdi/pipeline/flights.py, src/opdi/pipeline/flight_list_milestones.py"
FLIGHT_LIST_PAGE = "methods/airports.qmd"


def event_rule(event_type: str):
    """``(family, source, page, profile)`` of an event type, or None when unknown."""
    for pattern, family, source, page, profile in EVENT_RULES:
        if re.search(pattern, event_type):
            return family, source, page, profile
    return None


def item_file_name(name: str) -> str:
    """A file-name-safe form of an item name."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", name)


# --------------------------------------------------------------------------
# Drawing the examples (pandas)
# --------------------------------------------------------------------------

#: Examples drawn at random, at most drawn from the tails, and the size below
#: which an item is read in full.
N_RANDOM = 30
N_TAIL = 20
READ_WHOLE_BELOW = 50
SEED = 20260614


def random_key(keys, seed: int = SEED):
    """A reproducible random number in [0, 1) per key, from a hash of the seed and the key."""
    import hashlib

    import numpy as np

    out = np.empty(len(keys), dtype="float64")
    for i, k in enumerate(keys):
        h = hashlib.blake2b(f"{seed}:{k}".encode(), digest_size=8).digest()
        out[i] = int.from_bytes(h, "big") / 2**64
    return out


def draw_examples(
    df,
    measures,
    key: str = "event_id",
    n_random: int = N_RANDOM,
    n_tail: int = N_TAIL,
    whole_below: int = READ_WHOLE_BELOW,
    total: int | None = None,
    seed: int = SEED,
):
    """The examples of one item: ``n_random`` at random and up to ``n_tail`` from the tails.

    Args:
        df: the rows of the item (all of them, or a pool that holds at least
            the ``n_random`` rows with the smallest ``_rand`` and, for each
            measure, the ``n_tail`` lowest and highest rows).
        measures: ``(column, description, sides)`` with ``sides`` ``both``
            or ``high``.
        key: the column that names a row.
        total: rows of the item on the day when ``df`` is a pool; defaults
            to ``len(df)``.
        seed: used when ``df`` has no ``_rand`` column.

    Returns:
        The chosen rows, random ones first, with ``drawn`` saying why each
        was chosen (``random``, ``all`` for a small item, or ``lowest <m>`` /
        ``highest <m>``, several separated by ``; ``).

    The tails are taken in turns: the lowest of the first measure, its
    highest, the lowest of the second, and so on, then the second lowest of
    each, until ``n_tail`` rows that were not already chosen are taken. A row
    without a value of a measure is not in its tails.
    """
    import pandas as pd

    df = df.copy()
    if "_rand" not in df.columns:
        df["_rand"] = random_key(df[key].astype(str).tolist(), seed)
    total = len(df) if total is None else total
    if total < whole_below:
        out = df.sort_values("_rand").copy()
        out["drawn"] = "all"
        return out.reset_index(drop=True)

    reasons: dict = {}
    order = []
    for k in df.sort_values(["_rand", key])[key].head(n_random):
        reasons[k] = ["random"]
        order.append(k)

    tails = []
    for col, _desc, sides in measures:
        if col not in df.columns:
            continue
        vals = pd.to_numeric(df[col], errors="coerce")
        known = df.loc[vals.notna(), [key]].assign(_v=vals[vals.notna()])
        if known.empty:
            continue
        if sides == "both":
            tails.append((f"lowest {col}", known.sort_values(["_v", key])[key].tolist()))
        tails.append(
            (
                f"highest {col}",
                known.sort_values(["_v", key], ascending=[False, True])[key].tolist(),
            )
        )
    taken = 0
    depth = 0
    while taken < n_tail and any(depth < len(lst) for _, lst in tails):
        for label, lst in tails:
            if taken >= n_tail:
                break
            if depth >= len(lst):
                continue
            k = lst[depth]
            if k in reasons:
                if reasons[k] != ["random"] and label not in reasons[k]:
                    reasons[k].append(label)
                continue
            reasons[k] = [label]
            order.append(k)
            taken += 1
        depth += 1

    out = df.set_index(key).loc[order].reset_index()
    out["drawn"] = ["; ".join(reasons[k]) for k in order]
    return out


# --------------------------------------------------------------------------
# Summaries (the bulk of each item)
# --------------------------------------------------------------------------

PERCENTILES = (0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99)


def summary_rows(df, measures):
    """Percentiles of each measure over all rows of an item (pandas).

    Returns one dict per measure: ``measure``, ``description``, ``count``
    and ``p1`` ... ``p99``.
    """
    import pandas as pd

    rows = []
    for col, desc, _ in measures:
        vals = (
            pd.to_numeric(df[col], errors="coerce").dropna()
            if col in df.columns
            else pd.Series([], dtype=float)
        )
        row = {"measure": col, "description": desc, "count": int(len(vals))}
        for p in PERCENTILES:
            row[f"p{round(p * 100)}"] = float(vals.quantile(p)) if len(vals) else None
        rows.append(row)
    return rows


def fmt_num(x, digits: int = 1) -> str:
    """A number for a table: thousands separated, ``digits`` decimals, empty for a missing value."""
    import math

    if x is None:
        return ""
    try:
        f = float(x)
    except (TypeError, ValueError):
        return str(x)
    if math.isnan(f):
        return ""
    if abs(f) >= 1000 or float(f).is_integer() and digits <= 1:
        return f"{f:,.0f}"
    return f"{f:,.{digits}f}"


def summary_markdown(
    item: str, total: int, rows, record: dict | None = None, note: str = ""
) -> str:
    """The text of ``summary_<item>.md``.

    ``record``: ``with_record`` and ``within_60s`` counts, and ``what``
    (the record compared with), or None when the item has no record.
    """
    out = [f"# {item}: the whole day", ""]
    out.append(f"Rows of the item on the day: {total:,}.")
    if record:
        wr, w60 = record["with_record"], record["within_60s"]
        share = 100.0 * wr / total if total else 0.0
        s60 = 100.0 * w60 / wr if wr else 0.0
        out.append(
            f"With a record ({record['what']}): {wr:,} ({share:.1f}% of the rows); "
            f"{record.get('within_label', 'within 60 s of it')}: {w60:,} "
            f"({s60:.1f}% of those with a record)."
        )
    else:
        out.append("No airport or Network Manager record to compare this item with.")
    if note:
        out.append(note)
    out += ["", "| measure | count | p1 | p5 | p25 | p50 | p75 | p95 | p99 | what it is |"]
    out.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---|")
    for r in rows:
        cells = [fmt_num(r.get(f"p{round(p * 100)}")) for p in PERCENTILES]
        out.append(
            f"| {r['measure']} | {r['count']:,} | " + " | ".join(cells) + f" | {r['description']} |"
        )
    return "\n".join(out) + "\n"


# --------------------------------------------------------------------------
# The reading sheets (pandas)
# --------------------------------------------------------------------------

FT_PER_M = 3.28084
KT_PER_MPS = 1.94384
FTMIN_PER_MPS = 196.850394
EARTH_RADIUS_NM = 3440.065
#: Lines of a table of reports, and of the whole-flight profile.
MAX_REPORT_LINES = 40
#: Events of the track listed in full up to this many; beyond, the nearest in time.
MAX_EVENT_LINES = 60


def thin(dts, n: int = MAX_REPORT_LINES, nearest: int = 16, keep=()):
    """Positions of at most ``n`` of the times ``dts`` (seconds from the event), in time order.

    Kept always: the ``nearest`` closest to the event, the first and the
    last, and the positions in ``keep``; the rest of the ``n`` lines are
    spread evenly over the remaining reports.
    """
    import numpy as np

    dts = np.asarray(dts, dtype="float64")
    m = len(dts)
    if m <= n:
        return list(range(m))
    chosen = set(int(i) for i in keep if 0 <= i < m)
    chosen |= {0, m - 1}
    for i in np.argsort(np.abs(dts), kind="stable")[:nearest]:
        chosen.add(int(i))
    rest = [i for i in range(m) if i not in chosen]
    room = n - len(chosen)
    if room > 0 and rest:
        picks = np.linspace(0, len(rest) - 1, num=min(room, len(rest)))
        chosen |= {rest[int(round(x))] for x in picks}
    return sorted(chosen)[: max(n, len(chosen))]


def _nan(x) -> bool:
    try:
        import math

        return x is None or (isinstance(x, float) and math.isnan(x))
    except TypeError:
        return False


def _is_missing(x) -> bool:
    import pandas as pd

    if x is None:
        return True
    try:
        return bool(pd.isna(x))
    except (TypeError, ValueError):
        return False


def fmt_time(ts, day=None) -> str:
    """``HH:MM:SS`` of a UTC time, with ``(+1 d)`` or ``(-1 d)`` when it is not on ``day``."""
    import pandas as pd

    if _is_missing(ts):
        return ""
    t = pd.Timestamp(ts)
    if t.tzinfo is not None:
        t = t.tz_convert("UTC").tz_localize(None)
    text = t.strftime("%H:%M:%S")
    if day is not None:
        delta = (t.normalize() - pd.Timestamp(day).normalize()).days
        if delta:
            text += f" ({delta:+d} d)"
    return text


def seconds_between(a, b):
    """``a - b`` in seconds, or None when either is missing."""
    import pandas as pd

    if _is_missing(a) or _is_missing(b):
        return None
    ta, tb = pd.Timestamp(a), pd.Timestamp(b)
    if ta.tzinfo is not None:
        ta = ta.tz_convert("UTC").tz_localize(None)
    if tb.tzinfo is not None:
        tb = tb.tz_convert("UTC").tz_localize(None)
    return (ta - tb).total_seconds()


def fmt_signed(x, unit: str = "s", digits: int = 0) -> str:
    if _is_missing(x):
        return ""
    return f"{float(x):+,.{digits}f} {unit}".strip()


def haversine_nm(lat1, lon1, lat2, lon2):
    """Great-circle distance in NM (numpy, element-wise)."""
    import numpy as np

    p1, p2 = np.radians(lat1), np.radians(lat2)
    dp, dl = p2 - p1, np.radians(np.asarray(lon2) - np.asarray(lon1))
    a = np.sin(dp / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_NM * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


def bearing_deg(lat1, lon1, lat2, lon2):
    """Initial bearing from point 1 to point 2, degrees from true north (numpy)."""
    import numpy as np

    p1, p2 = np.radians(lat1), np.radians(lat2)
    dl = np.radians(np.asarray(lon2) - np.asarray(lon1))
    x = np.sin(dl) * np.cos(p2)
    y = np.cos(p1) * np.sin(p2) - np.sin(p1) * np.cos(p2) * np.cos(dl)
    return (np.degrees(np.arctan2(x, y)) + 360.0) % 360.0


def runway_position(lat, lon, thr_lat, thr_lon, rwy_bearing):
    """``(along_nm, cross_nm)`` of a point from a runway threshold.

    Along: positive towards the far end, negative short of the threshold
    (the formula of :func:`opdi.pipeline.runway_ops.along_track_nm`). Across:
    distance from the extended centre line (that of
    :func:`opdi.pipeline.runways.cross_track_nm`).
    """
    import numpy as np

    d = haversine_nm(thr_lat, thr_lon, lat, lon)
    brg = bearing_deg(thr_lat, thr_lon, lat, lon)
    along = d * np.cos(np.radians(brg - rwy_bearing))
    cross = np.abs(
        np.arcsin(np.sin(d / EARTH_RADIUS_NM) * np.sin(np.radians(brg - rwy_bearing)))
        * EARTH_RADIUS_NM
    )
    return float(along), float(cross)


def parse_info(text) -> dict:
    import json

    if _is_missing(text) or not str(text).strip():
        return {}
    try:
        value = json.loads(text)
    except ValueError:
        return {"_raw": str(text)}
    return value if isinstance(value, dict) else {"_raw": str(text)}


#: The ``info`` fields shown in the list of a track's events, in this order.
SHORT_INFO = (
    "apt_icao",
    "osm_airport",
    "runway",
    "osm_aeroway",
    "osm_ref",
    "direction",
    "crossing_seq",
    "fir_from",
    "fir_to",
    "method",
    "traversal_class",
    "duration_s",
    "opdi_time_in_use_s",
    "level_ft",
    "pattern",
    "circuits",
    "merge_point",
    "leg_seq",
    "relocated",
    "stand",
)


def short_info(text, width: int = 70) -> str:
    """The main ``info`` fields of an event as ``key=value``, cut to ``width`` characters."""
    info = parse_info(text)
    parts = [f"{k}={info[k]}" for k in SHORT_INFO if k in info and info[k] not in ("", None)]
    out = " ".join(parts)
    return out if len(out) <= width else out[: width - 1] + "…"


def _cell(x, digits: int = 0) -> str:
    if _is_missing(x):
        return ""
    import numpy as np

    if isinstance(x, (bool, np.bool_)):
        return "yes" if x else "no"
    if isinstance(x, (int, float)) or hasattr(x, "dtype"):
        try:
            return f"{float(x):,.{digits}f}"
        except (TypeError, ValueError):
            return str(x)
    return str(x)


def fmt_value(x, day=None, digits: int = 1) -> str:
    """A value of any kind for a sheet: a time as ``HH:MM:SS``, a number, a flag, a text."""
    import datetime as _dt

    import numpy as np
    import pandas as pd

    if _is_missing(x):
        return ""
    if isinstance(x, (pd.Timestamp, _dt.datetime, np.datetime64)):
        return fmt_time(x, day)
    if isinstance(x, (bool, np.bool_)):
        return "yes" if x else "no"
    if isinstance(x, _dt.date):
        return x.isoformat()
    return _cell(x, digits)


def _apt_text(ident, airports) -> str:
    """``EDDF (Frankfurt am Main, large_airport, elevation 364 ft)``."""
    if _is_missing(ident) or not ident:
        return "unknown"
    if airports is None or ident not in airports.index:
        return f"{ident} (not in OurAirports)"
    a = airports.loc[ident]
    elev = (
        "elevation unknown"
        if _is_missing(a.get("elevation_ft"))
        else f"elevation {a['elevation_ft']:,.0f} ft"
    )
    return f"{ident} ({a.get('name', '')}, {a.get('type', '')}, {elev})"


def _elev(ident, airports):
    if airports is None or _is_missing(ident) or ident not in airports.index:
        return None
    e = airports.loc[ident].get("elevation_ft")
    return None if _is_missing(e) else float(e)


def _apt_pos(ident, airports):
    if airports is None or _is_missing(ident) or ident not in airports.index:
        return None
    a = airports.loc[ident]
    return float(a["latitude_deg"]), float(a["longitude_deg"])


def report_table(reports, event_time, day, apt, airports, title, n=MAX_REPORT_LINES, mark=True):
    """A markdown table of position reports around ``event_time``.

    ``reports``: ``event_time``, ``lat``, ``lon``, ``alt_m`` (cleaned
    barometric altitude), ``raw_alt_m``, ``gs_ms``, ``vr_ms``, ``on_ground``,
    ``heading`` and optionally ``cum_nm``. Heights are above ``apt``.
    Marks: ``*`` the report nearest the event, ``<`` and ``>`` the last
    report before and the first after it.
    """
    import numpy as np

    if reports is None or len(reports) == 0:
        return f"**{title}**: no reports.\n"
    r = reports.sort_values("event_time").reset_index(drop=True)
    dts = np.array([seconds_between(t, event_time) for t in r["event_time"]], dtype="float64")
    before = np.where(dts < 0)[0]
    after = np.where(dts > 0)[0]
    nearest = int(np.argmin(np.abs(dts)))
    keep = [nearest]
    if len(before):
        keep.append(int(before[-1]))
    if len(after):
        keep.append(int(after[0]))
    idx = thin(dts, n=n, keep=keep)
    elev = _elev(apt, airports)
    pos = _apt_pos(apt, airports)
    has_cum = "cum_nm" in r.columns
    head = (
        "| | time UTC | Δ s | lat ° | lon ° | alt ft | hgt ft | gs kt | vr ft/min | gnd | hdg ° |"
    )
    sep = "|---|---|---:|---:|---:|---:|---:|---:|---:|---|---:|"
    if pos:
        head += f" to {apt} NM |"
        sep += "---:|"
    if has_cum:
        head += " flown NM |"
        sep += "---:|"
    apt_note = (
        f"height above {apt}, elevation {elev:,.0f} ft"
        if elev is not None
        else f"height unknown ({apt or 'no airport'} has no elevation)"
    )
    lines = [
        f"**{title}** ({len(idx)} of {len(r)} reports shown; {apt_note}; "
        "`*` nearest the event, `<` `>` the reports either side; alt `~` = removed by cleaning, raw value)",  # noqa: E501
        "",
        head,
        sep,
    ]
    for i in idx:
        row = r.iloc[i]
        m = ""
        if mark:
            if i == nearest:
                m += "*"
            if len(before) and i == before[-1]:
                m += "<"
            if len(after) and i == after[0]:
                m += ">"
        alt = row.get("alt_m")
        if _is_missing(alt):
            raw = row.get("raw_alt_m")
            alt_txt = "" if _is_missing(raw) else f"~{float(raw) * FT_PER_M:,.0f}"
            hgt = ""
        else:
            alt_ft = float(alt) * FT_PER_M
            alt_txt = f"{alt_ft:,.0f}"
            hgt = "" if elev is None else f"{int(round(alt_ft - elev)):,d}"
        gnd = row.get("on_ground")
        cells = [
            m,
            fmt_time(row["event_time"], day),
            f"{dts[i]:+.0f}",
            _cell(row.get("lat"), 5),
            _cell(row.get("lon"), 5),
            alt_txt,
            hgt,
            "" if _is_missing(row.get("gs_ms")) else f"{float(row['gs_ms']) * KT_PER_MPS:.0f}",
            "" if _is_missing(row.get("vr_ms")) else f"{float(row['vr_ms']) * FTMIN_PER_MPS:,.0f}",
            "" if _is_missing(gnd) else ("G" if bool(gnd) else "-"),
            _cell(row.get("heading"), 0),
        ]
        if pos:
            d = (
                haversine_nm(pos[0], pos[1], row.get("lat"), row.get("lon"))
                if not _is_missing(row.get("lat"))
                else None
            )
            cells.append("" if d is None else f"{float(d):.2f}")
        if has_cum:
            cells.append(_cell(row.get("cum_nm"), 1))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


def events_table(events, event_id, event_time, day) -> str:
    """The events of the track in time order, the example marked ``◀``."""
    import numpy as np

    if events is None or len(events) == 0:
        return "**Events of the track**: none.\n"
    e = events.sort_values(["event_time", "type"]).reset_index(drop=True)
    dts = np.array([seconds_between(t, event_time) or 0.0 for t in e["event_time"]])
    idx = list(range(len(e)))
    note = ""
    if len(e) > MAX_EVENT_LINES:
        idx = sorted(np.argsort(np.abs(dts), kind="stable")[:MAX_EVENT_LINES].tolist())
        note = f"; the {MAX_EVENT_LINES} nearest in time shown"
    lines = [
        f"**Events of the track** ({len(e)}{note}; `◀` = this example)",
        "",
        "| | time UTC | Δ s | type | alt ft | flown NM | detail |",
        "|---|---|---:|---|---:|---:|---|",
    ]
    for i in idx:
        row = e.iloc[i]
        mark = "◀" if row.get("event_id") == event_id else ""
        lines.append(
            f"| {mark} | {fmt_time(row['event_time'], day)} | {dts[i]:+,.0f} | {row['type']} | "
            f"{_cell(row.get('altitude_ft'))} | {_cell(row.get('distance_flown_nm'), 1)} | "
            f"{short_info(row.get('info'))} |"
        )
    return "\n".join(lines) + "\n"


def _record_lines(ex, day, item_record=None) -> list:
    """The airport record and Network Manager lines of a sheet."""
    out = []
    for side, label in (("dep", "departure"), ("arr", "arrival")):
        apt = ex.get(f"{side}_apt")
        if _is_missing(apt):
            continue
        mvt = "take-off" if side == "dep" else "landing"
        blk = "off-block" if side == "dep" else "in-block"
        parts = [
            f"{mvt} {fmt_time(ex.get(f'{side}_mvt'), day)}",
            f"{blk} {fmt_time(ex.get(f'{side}_block'), day)}",
        ]
        for c in ("c40", "c100"):
            if not _is_missing(ex.get(f"{side}_{c}")):
                parts.append(f"{c[1:]} NM crossing {fmt_time(ex.get(f'{side}_{c}'), day)}")
        parts.append(f"runway {ex.get(f'{side}_rwy') or '–'}")
        parts.append(f"stand {ex.get(f'{side}_stand') or '–'}")
        out.append(f"- **Airport record, {label} at {apt}**: " + " · ".join(parts))
    if item_record:
        col, what = item_record
        diff = seconds_between(ex.get("event_time"), ex.get(col))
        if diff is not None:
            out.append(f"- **Difference from the record**: event minus {what} = {diff:+,.0f} s")
        else:
            out.append(f"- **Difference from the record**: no {what} for this flight")
    if not _is_missing(ex.get("nm_adep")) or not _is_missing(ex.get("nm_ades")):
        parts = [
            f"{ex.get('nm_adep')} → {ex.get('nm_ades')}",
            f"off-block {fmt_time(ex.get('nm_aobt'), day)}",
            f"arrival {fmt_time(ex.get('nm_arvt'), day)}",
            f"route length {_cell(ex.get('nm_rte_len_nm'))} NM",
            f"type {ex.get('nm_type') or '–'}",
            f"registration {ex.get('nm_registration') or '–'}",
        ]
        agree = []
        for a, b, name in (("adep", "nm_adep", "departure"), ("ades", "nm_ades", "destination")):
            if not _is_missing(ex.get(a)) and not _is_missing(ex.get(b)):
                agree.append(f"{name} {'agrees' if ex.get(a) == ex.get(b) else 'DIFFERS'}")
        out.append(
            "- **Network Manager**: "
            + " · ".join(parts)
            + (f" ({', '.join(agree)})" if agree else "")
        )
    else:
        out.append("- **Network Manager**: no flight matched (icao24 + call sign within 6 h)")
    return out


def _flight_line(ex, day) -> str:
    """The flight's row of the flight list, in one line."""

    def yn(x):
        return "" if _is_missing(x) else ("yes" if bool(x) else "no")

    return (
        "- **Flight list**: "
        f"adep {ex.get('adep') or '–'} ({ex.get('adep_source') or '–'}; alternative {ex.get('adep_p') or '–'}) · "  # noqa: E501
        f"ades {ex.get('ades') or '–'} ({ex.get('ades_source') or '–'}; alternative {ex.get('ades_p') or '–'}) · "  # noqa: E501
        f"first seen {fmt_time(ex.get('first_seen'), day)} · last seen {fmt_time(ex.get('last_seen'), day)} · "  # noqa: E501
        f"aobt {fmt_time(ex.get('aobt'), day)} · atot {fmt_time(ex.get('atot'), day)} · "
        f"aldt {fmt_time(ex.get('aldt'), day)} · aibt {fmt_time(ex.get('aibt'), day)} · "
        f"runways {ex.get('rwy_dep') or '–'}/{ex.get('rwy_arr') or '–'} · "
        f"stands {ex.get('stnd_dep') or '–'}/{ex.get('stnd_arr') or '–'} · "
        f"superseded {yn(ex.get('superseded_dep'))}/{yn(ex.get('superseded_arr'))} · "
        f"movement {yn(ex.get('movement_dep'))}/{yn(ex.get('movement_arr'))} · "
        f"duration {_cell(ex.get('track_duration_min'), 1)} min"
    )


def _header(item, ex, day) -> list:
    return [
        f"### {item} · {ex['ex_id']} · drawn: {ex.get('drawn', '')}",
        "",
        f"**{item}** · track `{ex.get('flight_id')}` · callsign {ex.get('flt_id') or '–'} · "
        f"icao24 {ex.get('icao24') or '–'} · type {ex.get('typecode') or '–'} · "
        f"{ex.get('adep') or '?'} → {ex.get('ades') or '?'} · day {day}",
        "",
    ]


def _turn_deg(headings) -> float | None:
    """Total signed turn of a sequence of headings, degrees (right positive)."""
    import numpy as np

    h = np.asarray([x for x in headings if not _is_missing(x)], dtype="float64")
    if len(h) < 2:
        return None
    d = (np.diff(h) + 180.0) % 360.0 - 180.0
    return float(d.sum())


def event_sheet(meta, ex, events, reports, airports, runways, day) -> str:
    """The sheet of one example of an event or measurement item.

    Args:
        meta: the item's ``name``, ``profile``, ``record`` (column and
            what it is, or None) and ``pair`` (partner type, after).
        ex: the example's row (published event row, measures, records).
        events: the events of its track.
        reports: its reports, with ``part`` ``window``, ``profile`` or ``episode``.
        airports: OurAirports indexed by ``ident``.
        runways: OurAirports runways.
    """
    item = meta["name"]
    info = parse_info(ex.get("info"))
    apt = ex.get("ctx_airport")
    et = ex.get("event_time")
    out = _header(item, ex, day)
    ev = [
        f"{fmt_time(et, day)} UTC",
        f"{_cell(ex.get('latitude'), 5)}, {_cell(ex.get('longitude'), 5)}",
        f"altitude {_cell(ex.get('altitude_ft'))} ft",
        f"airport concerned {_apt_text(apt, airports)}",
        f"height above it {_cell(ex.get('height_ft'))} ft",
        f"{_cell(ex.get('airport_dist_nm'), 2)} NM from it",
        f"ground speed {_cell(ex.get('speed_kt'))} kt",
        f"vertical rate {_cell(ex.get('vrate_fpm'))} ft/min",
        f"nearest report {_cell(ex.get('ctx_gap_s'), 1)} s away",
        f"share of reports within 60 s flagged on the ground {_cell(ex.get('ctx_on_ground'), 2)}",
    ]
    out.append("- **Event**: " + " · ".join(ev))
    out.append(f"- **info**: `{ex.get('info') or ''}`")
    ms = []
    for c, unit in (
        ("distance_flown_nm", "NM"),
        ("time_passed_s", "s"),
        ("holding_duration_s", "s"),
        ("holding_distance_nm", "NM"),
        ("holding_circuits", "loops"),
        ("pms_leg_time_s", "s"),
        ("pms_leg_distance_nm", "NM"),
        ("pms_direct_to_distance_nm", "NM"),
    ):
        if not _is_missing(ex.get(c)):
            ms.append(f"{c} {_cell(ex.get(c), 1)} {unit}")
    out.append("- **Measurements**: " + (" · ".join(ms) if ms else "none"))
    if meta.get("kind") == "measurement" or not _is_missing(ex.get("distance_flown_nm")):
        checks = []
        pa, pb = _apt_pos(ex.get("adep"), airports), _apt_pos(ex.get("ades"), airports)
        if pa and pb:
            gc = float(haversine_nm(pa[0], pa[1], pb[0], pb[1]))
            checks.append(f"great circle {ex.get('adep')} to {ex.get('ades')} {gc:,.1f} NM")
        if not _is_missing(ex.get("nm_rte_len_nm")):
            checks.append(f"Network Manager route length {float(ex['nm_rte_len_nm']):,.0f} NM")
        if not _is_missing(ex.get("excess_nm")):
            gcf = float(ex["distance_flown_nm"]) - float(ex["excess_nm"])
            checks.append(
                f"great circle from the first report to the event {gcf:,.1f} NM "
                f"(distance flown minus it {float(ex['excess_nm']):+,.1f} NM)"
            )
        el = seconds_between(et, ex.get("trk_first_time"))
        if el is not None:
            checks.append(f"time since the track's first report {el:,.0f} s")
        if not _is_missing(ex.get("elapsed_diff_s")):
            checks.append(f"time passed minus it {float(ex['elapsed_diff_s']):+,.0f} s")
        if not _is_missing(ex.get("pair_diff_s")):
            checks.append(
                f"published duration minus the time between the two events "
                f"{float(ex['pair_diff_s']):+,.0f} s"
            )
        if checks:
            out.append("- **Independent figures**: " + " · ".join(checks))
    out.append(
        f"- **Gaps**: {_cell(ex.get('prev_gap_s'))} s since the previous event · "
        f"{_cell(ex.get('next_gap_s'))} s to the next"
    )
    if meta.get("pair"):
        ptype, after = meta["pair"]
        if _is_missing(ex.get("pair_time")):
            out.append(
                f"- **Partner**: no {ptype} {'after' if after else 'before'} it in the track"
            )
        else:
            out.append(
                f"- **Partner**: {ptype} at {fmt_time(ex.get('pair_time'), day)} "
                f"({_cell(ex.get('pair_s'))} s {'later' if after else 'earlier'})"
            )
    rwy = info.get("runway") or info.get("rwy_ident")
    if rwy:
        line = f"- **Runway** {rwy} of {info.get('apt_icao') or apt}"
        brg = info.get("runway_bearing_deg")
        rw = None
        if runways is not None and len(runways):
            a = info.get("apt_icao") or apt
            sub = runways[runways["airport_ident"] == a]
            for _, rr in sub.iterrows():
                for end in ("le", "he"):
                    if str(rr.get(f"{end}_ident")) == str(rwy):
                        rw = (
                            rr.get(f"{end}_latitude_deg"),
                            rr.get(f"{end}_longitude_deg"),
                            rr.get(f"{end}_heading_degT"),
                            rr.get("length_ft"),
                        )
        if brg is not None:
            line += f", bearing {float(brg):.1f}°"
        if rw and not _is_missing(rw[0]) and not _is_missing(ex.get("latitude")):
            b = float(brg) if brg is not None else rw[2]
            if not _is_missing(b):
                along, cross = runway_position(
                    ex["latitude"], ex["longitude"], float(rw[0]), float(rw[1]), float(b)
                )
                length = (
                    ""
                    if _is_missing(rw[3])
                    else f" (runway length {float(rw[3]) / 6076.12:.2f} NM)"
                )
                line += f"; event {along:+.2f} NM along from the threshold of {rwy}{length}, {cross:.2f} NM off the centre line (OurAirports threshold)"  # noqa: E501
        elif rw is None:
            line += "; threshold not in OurAirports"
        for k in ("align_deg", "max_gs_kt", "traversal_class", "milestone", "method"):
            if k in info:
                line += f" · {k} {info[k]}"
        out.append(line)
    if "osm_aeroway" in info or "osm_id" in info:
        out.append(
            f"- **Layout element**: {info.get('osm_aeroway', '?')} `{info.get('osm_ref', '–')}` "
            f"(OpenStreetMap id {info.get('osm_id', '?')}) at {info.get('osm_airport', '?')}; "
            f"time in it {info.get('opdi_time_in_use_s', '?')} s"
        )
    if "fir_from" in info or "fir_to" in info:
        out.append(
            f"- **FIR crossing**: from {info.get('fir_from', '?')} to {info.get('fir_to', '?')} "
            f"at FL {info.get('fl', '?')}; bracket {info.get('bracket_s', '?')} s"
        )
    elif "bracket_s" in info:
        out.append(
            f"- **Crossing**: direction {info.get('direction', '?')}, crossing number "
            f"{info.get('crossing_seq', '?')}, bracket {info.get('bracket_s')} s "
            "(the reports either side are marked `<` `>` below)"
        )
    epi = reports[reports["part"] == "episode"] if reports is not None and len(reports) else None
    if meta.get("profile") == "episode" and epi is not None and len(epi):
        turn = _turn_deg(epi.sort_values("event_time")["heading"].tolist())
        start, end = epi["event_time"].min(), epi["event_time"].max()
        out.append(
            f"- **Episode**: reports {fmt_time(start, day)} to {fmt_time(end, day)} "
            f"({seconds_between(end, start):,.0f} s, margins of 120 s included); "
            f"turn summed from the headings {'' if turn is None else f'{turn:+,.0f}° ({abs(turn) / 360:.1f} loops)'}"  # noqa: E501
        )
    out += _record_lines(ex, day, meta.get("record"))
    out.append(_flight_line(ex, day))
    out.append("")
    out.append(events_table(events, ex.get("event_id"), et, day))
    if reports is not None and len(reports):
        win = reports[reports["part"] == "window"]
        prof = reports[reports["part"] == "profile"]
        if meta.get("profile") == "episode" and epi is not None and len(epi):
            out.append(report_table(epi, et, day, apt, airports, "Reports of the episode"))
        n_win = 20 if meta.get("profile") == "whole" else MAX_REPORT_LINES
        out.append(
            report_table(
                win, et, day, apt, airports, "Reports from 3 min before to 3 min after", n=n_win
            )
        )
        if len(prof):
            out.append(
                report_table(prof, et, day, apt, airports, "Altitude profile of the whole flight")
            )
    else:
        out.append("**Reports**: none within 3 min.\n")
    return "\n".join(out) + "\n"


def flight_sheet(meta, ex, events, reports, candidates, airports, day) -> str:
    """The sheet of one example of a flight-list item: the whole track summarised."""
    item = meta["name"]
    out = _header(item, ex, day)
    shown = meta.get("columns", ())
    out.append(
        "- **This item's columns**: "
        + " · ".join(f"{c} {fmt_value(ex.get(c), day) or '–'}" for c in shown)
    )

    meas = [
        f"{m} {_cell(ex.get(m), 1)}"
        for m, _, _ in meta.get("measures", ())
        if not _is_missing(ex.get(m))
    ]
    if meas:
        out.append("- **Measures**: " + " · ".join(meas))
    out.append(_flight_line(ex, day))
    for end, apt_col in (("first", "adep"), ("last", "ades")):
        apt = ex.get(apt_col)
        elev = _elev(apt, airports)
        pos = _apt_pos(apt, airports)
        alt = ex.get(f"trk_{end}_alt_ft")
        dist = (
            float(haversine_nm(pos[0], pos[1], ex.get(f"trk_{end}_lat"), ex.get(f"trk_{end}_lon")))
            if pos and not _is_missing(ex.get(f"trk_{end}_lat"))
            else None
        )
        out.append(
            f"- **{end.capitalize()} report**: {fmt_time(ex.get(f'trk_{end}_time'), day)} · "
            f"{_cell(ex.get(f'trk_{end}_lat'), 5)}, {_cell(ex.get(f'trk_{end}_lon'), 5)} · "
            f"altitude {_cell(alt)} ft · ground speed {_cell(ex.get(f'trk_{end}_gs_kt'))} kt · "
            f"on ground {_cell(ex.get(f'trk_{end}_on_ground'))} · "
            f"{'' if dist is None else f'{dist:,.1f} NM from {apt}'}"
            f"{'' if elev is None or _is_missing(alt) else f', {float(alt) - elev:,.0f} ft above it'}"  # noqa: E501
        )
    out.append(
        f"- **Track**: {_cell(ex.get('trk_reports'))} reports · highest altitude "
        f"{_cell(ex.get('trk_max_alt_ft'))} ft · share on ground {_cell(ex.get('trk_on_ground'), 2)} · "  # noqa: E501
        f"adep {_apt_text(ex.get('adep'), airports)} · ades {_apt_text(ex.get('ades'), airports)}"
    )
    out += _record_lines(ex, day)
    if candidates is not None and len(candidates):
        out.append("")
        out.append(
            "**Airport candidates** (endpoint-candidates table; dist = distance of the end report from the airport, eff = effective distance used to rank, NM; hgt = height of the end report above the airport, ft)"  # noqa: E501
        )
        out.append("")
        out.append(
            "| end | airport | type | dist NM | eff NM | hgt ft | elev known | gnd | at border | chosen |"  # noqa: E501
        )
        out.append("|---|---|---|---:|---:|---:|---|---|---|---|")
        for _, c in candidates.sort_values(["role", "eff_nm"]).iterrows():
            chosen = c["apt_ident"] == (ex.get("adep") if c["role"] == "adep" else ex.get("ades"))
            alt = c["apt_ident"] == (ex.get("adep_p") if c["role"] == "adep" else ex.get("ades_p"))
            typ = (
                airports.loc[c["apt_ident"]].get("type", "")
                if airports is not None and c["apt_ident"] in airports.index
                else ""
            )
            out.append(
                f"| {'departure' if c['role'] == 'adep' else 'arrival'} | {c['apt_ident']} | {typ} | {_cell(c.get('dist_nm'), 2)} | "  # noqa: E501
                f"{_cell(c.get('eff_nm'), 2)} | {_cell(c.get('agl_ft'))} | {_cell(c.get('elev_known'))} | "  # noqa: E501
                f"{_cell(c.get('on_ground'))} | {_cell(c.get('at_border'))} | "
                f"{'chosen' if chosen else ('alternative' if alt else '')} |"
            )
    else:
        out.append("- **Airport candidates**: none in the endpoint-candidates table")
    out.append("")
    first_t = ex.get("trk_first_time")
    if events is not None and len(events):
        counts = events["type"].value_counts()
        out.append(
            "**Events of the track by type**: "
            + ", ".join(f"{t} {n}" for t, n in counts.sort_index().items())
        )
        out.append("")
        key = events[
            events["type"].isin(
                [
                    "first_seen",
                    "AOBT",
                    "line-up",
                    "take-off-roll",
                    "ATOT",
                    "xing-runway-threshold",
                    "ALDT",
                    "runway-vacated",
                    "AIBT",
                    "go-around",
                    "last_seen",
                ]
            )
        ]
        out.append(
            events_table(key, None, first_t, day).replace(
                "**Events of the track**", "**Runway, block and track-limit events**"
            )
        )
    if reports is not None and len(reports):
        prof = reports[reports["part"] == "profile"]
        out.append(
            report_table(
                prof,
                first_t,
                day,
                ex.get("adep"),
                airports,
                "Whole track, Δ from the first report, height above adep",
                mark=False,
            )
        )
    return "\n".join(out) + "\n"


# --------------------------------------------------------------------------
# Flight-list measures (pandas, on all flights of the day)
# --------------------------------------------------------------------------

#: Flight-list items read on a part of the flights only: name -> which rows.
FLIGHT_LIST_POPULATION = {
    "fl_alternative_airports": lambda d: d["adep_p"].notna() | d["ades_p"].notna(),
    "fl_takeoff_landing": lambda d: d["atot"].notna() | d["aldt"].notna(),
    "fl_block_stands": lambda d: d["aobt"].notna() | d["aibt"].notna(),
    "fl_rings": lambda d: d[[c for c in d.columns if re.match(r"^c\d+_(dep|arr)$", c)]]
    .notna()
    .any(axis=1),
}

#: Flight-list item -> (what is compared, the count of agreeing rows, the count with a record).
FLIGHT_LIST_RECORD = {
    "fl_departure_airport": ("Network Manager departure airport", "nm_adep", "adep"),
    "fl_destination_airport": ("Network Manager destination airport", "nm_ades", "ades"),
    "fl_aircraft": ("Network Manager aircraft type", "nm_type", "typecode"),
    "fl_takeoff_landing": ("airport record take-off time", "atot_diff_s", None),
    "fl_block_stands": ("airport record off-block time", "aobt_diff_s", None),
    "fl_rings": ("airport record 40 NM arrival crossing", "c40_arr_diff_s", None),
}


def _minutes(a, b):
    return (a - b).dt.total_seconds() / 60.0


def flight_list_measures(fl, airports):
    """The measures of :data:`FLIGHT_LIST_ITEMS` for every flight (pandas, vectorised).

    ``fl``: the published flight list joined with the track summary
    (``trk_*``) and the records of :func:`flight_records`; ``airports``:
    OurAirports indexed by ``ident``.
    """
    import numpy as np
    import pandas as pd

    d = _to_naive(fl.copy())

    def apt(col, field):
        return d[col].map(airports[field]) if col in d.columns else np.nan

    def dist(lat, lon, apt_col):
        alat, alon = apt(apt_col, "latitude_deg"), apt(apt_col, "longitude_deg")
        return pd.Series(
            haversine_nm(
                d[lat].astype(float), d[lon].astype(float), alat.astype(float), alon.astype(float)
            ),
            index=d.index,
        )

    def differs(a, b):
        known = d[a].notna() & d[b].notna()
        out = pd.Series(np.nan, index=d.index)
        out[known] = (
            d.loc[known, a].astype(str).str.strip().str.upper()
            != d.loc[known, b].astype(str).str.strip().str.upper()
        ).astype(float)
        return out

    def rwy_norm(x):
        return x.astype("string").str.strip().str.upper().str.lstrip("0")

    d["seen_span_min"] = _minutes(d["last_seen"], d["first_seen"])
    d["first_gap_s"] = (d["first_seen"] - d["trk_first_time"]).dt.total_seconds()
    d["last_gap_s"] = (d["last_seen"] - d["trk_last_time"]).dt.total_seconds()
    d["first_dist_adep_nm"] = dist("trk_first_lat", "trk_first_lon", "adep")
    d["first_hgt_adep_ft"] = d["trk_first_alt_ft"] - apt("adep", "elevation_ft")
    d["last_dist_ades_nm"] = dist("trk_last_lat", "trk_last_lon", "ades")
    d["last_hgt_ades_ft"] = d["trk_last_alt_ft"] - apt("ades", "elevation_ft")
    d["nm_adep_differs"] = differs("adep", "nm_adep")
    d["nm_ades_differs"] = differs("ades", "nm_ades")
    d["adep_missing"] = d["adep"].isna().astype(float)
    d["ades_missing"] = d["ades"].isna().astype(float)
    d["alt_dep_dist_nm"] = pd.Series(
        haversine_nm(
            apt("adep", "latitude_deg").astype(float),
            apt("adep", "longitude_deg").astype(float),
            apt("adep_p", "latitude_deg").astype(float),
            apt("adep_p", "longitude_deg").astype(float),
        ),
        index=d.index,
    )
    d["alt_arr_dist_nm"] = pd.Series(
        haversine_nm(
            apt("ades", "latitude_deg").astype(float),
            apt("ades", "longitude_deg").astype(float),
            apt("ades_p", "latitude_deg").astype(float),
            apt("ades_p", "longitude_deg").astype(float),
        ),
        index=d.index,
    )
    d["nm_type_differs"] = differs("typecode", "nm_type")
    d["typecode_missing"] = d["typecode"].isna().astype(float)
    d["atot_diff_s"] = (d["atot"] - d["dep_mvt"]).dt.total_seconds()
    d["aldt_diff_s"] = (d["aldt"] - d["arr_mvt"]).dt.total_seconds()
    d["airborne_min"] = _minutes(d["aldt"], d["atot"])
    for mine, rec, name in (
        ("rwy_dep", "dep_rwy", "rwy_dep_differs"),
        ("rwy_arr", "arr_rwy", "rwy_arr_differs"),
    ):
        known = d[mine].notna() & d[rec].notna()
        out = pd.Series(np.nan, index=d.index)
        out[known] = (rwy_norm(d.loc[known, mine]) != rwy_norm(d.loc[known, rec])).astype(float)
        d[name] = out
    d["aobt_diff_s"] = (d["aobt"] - d["dep_block"]).dt.total_seconds()
    d["aibt_diff_s"] = (d["aibt"] - d["arr_block"]).dt.total_seconds()
    d["taxi_out_min"] = _minutes(d["atot"], d["aobt"])
    d["taxi_in_min"] = _minutes(d["aibt"], d["aldt"])
    d["c40_arr_diff_s"] = (d["c40_arr"] - d["arr_c40"]).dt.total_seconds()
    d["c100_arr_diff_s"] = (d["c100_arr"] - d["arr_c100"]).dt.total_seconds()
    d["c40_dep_after_atot_s"] = (d["c40_dep"] - d["atot"]).dt.total_seconds()
    d["c40_arr_before_aldt_s"] = (d["aldt"] - d["c40_arr"]).dt.total_seconds()
    d["superseded_any"] = (
        d["superseded_dep"].fillna(False).astype(bool)
        | d["superseded_arr"].fillna(False).astype(bool)
    ).astype(float)
    d["not_movement_any"] = (
        ~d["movement_dep"].fillna(True).astype(bool) | ~d["movement_arr"].fillna(True).astype(bool)
    ).astype(float)
    d["duration_diff_min"] = d["track_duration_min"] - _minutes(
        d["trk_last_time"], d["trk_first_time"]
    )
    return d


def flight_list_record(name, d):
    """``record`` of :func:`summary_markdown` for a flight-list item, or None."""
    if name not in FLIGHT_LIST_RECORD:
        return None
    what, rec, mine = FLIGHT_LIST_RECORD[name]
    if mine is None:
        with_record = int(d[rec].notna().sum())
        within = int((d[rec].abs() <= 60).sum())
        return {"what": what, "with_record": with_record, "within_60s": within}
    known = d[rec].notna() & d[mine].notna()
    agree = known & (
        d[rec].astype(str).str.strip().str.upper() == d[mine].astype(str).str.strip().str.upper()
    )
    return {
        "what": what,
        "with_record": int(known.sum()),
        "within_60s": int(agree.sum()),
        "within_label": "the same",
    }


# --------------------------------------------------------------------------
# sample: the Spark part
# --------------------------------------------------------------------------

WORLD = "s3a://eurocontrol/opdi-world"
#: Seconds of reports either side of an event, and the margin around an episode.
WINDOW_S = 180
EPISODE_MARGIN_S = 120
#: Reports a whole-track profile or an episode keeps on the cluster (thinned again on the pod).
PROFILE_ROWS = 80
EPISODE_ROWS = 120
#: A record of a flight is looked for within this many seconds.
RECORD_SEARCH_S = 6 * 3600


def run_paths(label: str, day: str) -> dict:
    """The tables of the run ``label`` that ``sample`` reads."""
    root = f"{PREFIX}/{label}"
    ymd = day.replace("-", "")
    return {
        "root": root,
        "events": f"{root}/published/flight_events/flight_events_{ymd}.parquet",
        "flight_list": f"{root}/published/flight_list/flight_list_{ymd}.parquet",
        "tracks": f"{root}/osn_tracks_clean",
        "candidates": f"{root}/opdi_endpoint_candidates",
        "rejected": f"{root}/opdi_flight_events_rejected",
        "measures": f"{root}/_spot_event_measures",
        "airports": f"{WORLD}/oa_airports",
    }


def spark_session(executors: int, name: str):
    from opdi.utils.spark_helpers import SparkSessionManager

    dt = point_day_timing_here()
    cfg = dt.build_config(executors)
    return SparkSessionManager.create_session(
        app_name=name,
        config=cfg,
        distributed=True,
        extra_configs={"spark.sql.session.timeZone": "UTC", "spark.sql.shuffle.partitions": "400"},
    )


def _nearest(left, right, key_left, key_right, anchor, time_col, cols, id_col="flight_id"):
    """For each row of ``left``, the row of ``right`` with the same key nearest in time."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    cond = None
    for a, b in zip(key_left, key_right):
        c = F.col(a) == F.col(b)
        cond = c if cond is None else cond & c
    j = left.select(id_col, anchor, *key_left).join(right, cond, "inner")
    j = j.withColumn("_d", F.abs(F.col(anchor).cast("double") - F.col(time_col).cast("double")))
    j = j.filter(F.col("_d") <= RECORD_SEARCH_S)
    w = Window.partitionBy(id_col).orderBy(F.col("_d").asc())
    return j.withColumn("_n", F.row_number().over(w)).filter("_n = 1").select(id_col, *cols)


def flight_records(spark, fl):
    """Per flight of ``fl``: its airport records (departure, arrival) and Network Manager flight.

    Matched on icao24 and call sign, the nearest in time within 6 h: the
    departure record to the take-off (else off-block, else first seen), the
    arrival record to the landing (else in-block, else last seen), the
    Network Manager flight's off-block time to the off-block (else take-off,
    else first seen).
    """
    import events_gt
    from pyspark.sql import functions as F

    events_gt.assert_utc_session(spark)
    apdf = events_gt.load_apdf(spark, "202606", full=False)
    nm = events_gt.load_flights(spark, "202606")
    b = events_gt.bridge(apdf, nm).filter(F.col("icao24").isNotNull())
    flk = fl.select(
        "flight_id",
        F.lower(F.col("icao24")).alias("_f_icao24"),
        F.trim(F.col("flt_id")).alias("_f_cs"),
        F.coalesce("atot", "aobt", "first_seen").alias("_dep_anchor"),
        F.coalesce("aldt", "aibt", "last_seen").alias("_arr_anchor"),
        F.coalesce("aobt", "atot", "first_seen").alias("_nm_anchor"),
    )
    keys = ("_f_icao24", "_f_cs")
    out = fl.select("flight_id")
    for phase, p, apt in (("DEP", "dep", "ADEP_ICAO"), ("ARR", "arr", "ADES_ICAO")):
        side = b.filter(F.col("SRC_PHASE") == phase).select(
            F.col("icao24").alias("_r_icao24"),
            F.col("callsign").alias("_r_cs"),
            F.col("MVT_TIME_UTC").alias(f"{p}_mvt"),
            F.col("BLOCK_TIME_UTC").alias(f"{p}_block"),
            F.col(apt).alias(f"{p}_apt"),
            F.col("AP_C_RWY").alias(f"{p}_rwy"),
            F.col("AP_C_STND").alias(f"{p}_stand"),
            F.col("C40_CROSS_TIME").alias(f"{p}_c40"),
            F.col("C100_CROSS_TIME").alias(f"{p}_c100"),
        )
        cols = [f"{p}_{c}" for c in ("mvt", "block", "apt", "rwy", "stand", "c40", "c100")]
        rec = _nearest(flk, side, keys, ("_r_icao24", "_r_cs"), f"_{p}_anchor", f"{p}_mvt", cols)
        out = out.join(rec, "flight_id", "left")
    nmf = nm.select(
        F.lower(F.col("AIRCRAFT_ADDRESS")).alias("_n_icao24"),
        F.trim(F.col("AIRCRAFT_ID")).alias("_n_cs"),
        F.col("ADEP").alias("nm_adep"),
        F.col("ADES").alias("nm_ades"),
        F.col("AOBT_3").alias("nm_aobt"),
        F.col("ARVT_3").alias("nm_arvt"),
        F.col("RTE_LEN_3").cast("double").alias("nm_rte_len_nm"),
        F.col("AIRCRAFT_TYPE_ICAO_ID").alias("nm_type"),
        F.col("REGISTRATION").alias("nm_registration"),
    ).filter(F.col("_n_icao24").isNotNull())
    cols = [
        "nm_adep",
        "nm_ades",
        "nm_aobt",
        "nm_arvt",
        "nm_rte_len_nm",
        "nm_type",
        "nm_registration",
    ]
    rec = _nearest(flk, nmf, keys, ("_n_icao24", "_n_cs"), "_nm_anchor", "nm_aobt", cols)
    return out.join(rec, "flight_id", "left")


def track_summary(tracks):
    """Per track: reports, first and last report (time, position, altitude, speed, ground flag)."""
    from pyspark.sql import functions as F

    t = tracks.select(
        "track_id",
        "event_time",
        "lat",
        "lon",
        (F.col("baro_altitude_c") * F.lit(FT_PER_M)).alias("alt_ft"),
        (F.col("velocity") * F.lit(KT_PER_MPS)).alias("gs_kt"),
        "on_ground",
    )
    pos = F.when(F.col("lat").isNotNull() & F.col("lon").isNotNull(), F.col("event_time"))
    st = F.struct("event_time", "lat", "lon", "alt_ft", "gs_kt", "on_ground")
    agg = t.groupBy(F.col("track_id").alias("flight_id")).agg(
        F.count(F.lit(1)).alias("trk_reports"),
        F.min_by(st, pos).alias("_f"),
        F.max_by(st, pos).alias("_l"),
        F.max("alt_ft").alias("trk_max_alt_ft"),
        F.avg(F.col("on_ground").cast("double")).alias("trk_on_ground"),
    )
    for end, s_ in (("first", "_f"), ("last", "_l")):
        for f, name in (
            ("event_time", "time"),
            ("lat", "lat"),
            ("lon", "lon"),
            ("alt_ft", "alt_ft"),
            ("gs_kt", "gs_kt"),
            ("on_ground", "on_ground"),
        ):
            agg = agg.withColumn(f"trk_{end}_{name}", F.col(f"{s_}.{f}"))
    return agg.drop("_f", "_l")


def _info(field: str):
    from pyspark.sql import functions as F

    return F.get_json_object(F.col("info"), f"$.{field}")


def event_measures(spark, ev, tracks, airports, records, summary, types):
    """Every event of the day with the measures of :data:`EVENT_MEASURES` and the checks."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    from opdi.pipeline.plausibility import event_context
    from opdi.utils.geometry import haversine_nm as hav

    keep = [
        "event_id",
        "flight_id",
        "type",
        "event_time",
        "latitude",
        "longitude",
        "altitude_ft",
        "info",
        "distance_flown_nm",
        "time_passed_s",
        "holding_duration_s",
        "holding_distance_nm",
        "holding_circuits",
        "pms_leg_time_s",
        "pms_leg_distance_nm",
        "pms_direct_to_distance_nm",
        "adep",
        "ades",
    ]
    e = ev.select(*keep).withColumn("track_id", F.col("flight_id"))
    e = e.withColumn("apt_icao", F.coalesce(_info("apt_icao"), _info("osm_airport")))
    rep = tracks.select(
        "track_id",
        "event_time",
        "lat",
        "lon",
        "baro_altitude_c",
        "velocity",
        "vert_rate",
        "on_ground",
    )
    e = event_context(e, rep, airports, 60.0)
    apt = F.broadcast(
        airports.dropDuplicates(["ident"]).select(
            F.col("ident").alias("_a_ident"),
            F.col("latitude_deg").cast("double").alias("_a_lat"),
            F.col("longitude_deg").cast("double").alias("_a_lon"),
            F.col("type").alias("_a_type"),
        )
    )
    e = e.join(apt, F.col("ctx_airport") == F.col("_a_ident"), "left")
    e = (
        e.withColumn("height_ft", F.col("ctx_height_ft"))
        .withColumn("speed_kt", F.col("ctx_speed_kt"))
        .withColumn("vrate_fpm", F.col("ctx_vertical_rate_ft_min"))
        .withColumn(
            "airport_dist_nm",
            hav(F.col("latitude"), F.col("longitude"), F.col("_a_lat"), F.col("_a_lon")),
        )
        .withColumn(
            "small_airport",
            F.when(
                F.col("_a_type").isin("small_airport", "heliport", "seaplane_base", "closed"), 1.0
            ).when(F.col("_a_type").isNotNull(), 0.0),
        )
        .drop("_a_ident", "_a_lat", "_a_lon", "_a_type")
    )
    w = Window.partitionBy("flight_id").orderBy("event_time", "type", "event_id")
    e = e.withColumn(
        "prev_gap_s",
        F.col("event_time").cast("double") - F.lag("event_time").over(w).cast("double"),
    )
    e = e.withColumn(
        "next_gap_s",
        F.lead("event_time").over(w).cast("double") - F.col("event_time").cast("double"),
    )

    # The partner event: same track, the partner's type, the same element
    # (layout), holding or leg number, nearest in the stated direction.
    pk = (
        F.when(F.col("type").rlike("^(entry|exit)-"), _info("osm_id"))
        .when(F.col("type").startswith("holding-"), _info("holding_seq"))
        .when(F.col("type").startswith("pms-leg-"), _info("leg_seq"))
        .otherwise(F.lit(""))
    )
    e = e.withColumn("_pkey", F.coalesce(pk, F.lit("")))
    mapping = [(t, *pair_of(t)) for t in types if pair_of(t)]
    if mapping:
        m = spark.createDataFrame(mapping, "type string, _ptype string, _pafter boolean")
        left = e.select("event_id", "flight_id", "type", "event_time", "_pkey").join(
            F.broadcast(m), "type"
        )
        right = e.select(
            F.col("flight_id").alias("_pf"),
            F.col("type").alias("_pt"),
            F.col("event_time").alias("pair_time"),
            F.col("_pkey").alias("_ppk"),
            F.col("event_id").alias("_pid"),
        )
        j = left.join(
            right,
            (F.col("flight_id") == F.col("_pf"))
            & (F.col("_ptype") == F.col("_pt"))
            & (F.col("_pkey") == F.col("_ppk")),
        )
        j = j.filter(F.col("_pid") != F.col("event_id"))
        d = F.col("pair_time").cast("double") - F.col("event_time").cast("double")
        j = j.withColumn("pair_s", F.when(F.col("_pafter"), d).otherwise(-d)).filter(
            F.col("pair_s") >= 0
        )
        wp = Window.partitionBy("event_id").orderBy("pair_s")
        j = (
            j.withColumn("_n", F.row_number().over(wp))
            .filter("_n = 1")
            .select("event_id", "pair_time", "pair_s")
        )
        e = e.join(j, "event_id", "left")
    else:
        e = e.withColumn("pair_time", F.lit(None).cast("timestamp")).withColumn(
            "pair_s", F.lit(None).cast("double")
        )

    e = e.join(records, "flight_id", "left")
    rec = None
    for t, (c, _) in EVENT_RECORD.items():
        rec = (
            F.when(F.col("type") == t, F.col(c))
            if rec is None
            else rec.when(F.col("type") == t, F.col(c))
        )
    direction = _info("direction")
    for radius, (dep_c, arr_c) in RING_RECORD.items():
        t = f"xing-{radius}nm"
        rec = rec.when(
            (F.col("type") == t)
            & (direction == "inbound")
            & (F.col("apt_icao") == F.col("arr_apt")),
            F.col(arr_c),
        )
        rec = rec.when(
            (F.col("type") == t)
            & (direction == "outbound")
            & (F.col("apt_icao") == F.col("dep_apt")),
            F.col(dep_c),
        )
    e = e.withColumn("rec_time", rec)
    e = e.withColumn(
        "record_diff_s", F.col("event_time").cast("double") - F.col("rec_time").cast("double")
    )

    sm = summary.select("flight_id", "trk_first_time", "trk_first_lat", "trk_first_lon")
    e = e.join(sm, "flight_id", "left")
    e = e.withColumn(
        "excess_nm",
        F.col("distance_flown_nm")
        - hav(
            F.col("trk_first_lat"), F.col("trk_first_lon"), F.col("latitude"), F.col("longitude")
        ),
    )
    e = e.withColumn(
        "elapsed_diff_s",
        F.col("time_passed_s")
        - (F.col("event_time").cast("double") - F.col("trk_first_time").cast("double")),
    )
    e = e.withColumn(
        "pair_diff_s",
        F.when(F.col("type") == "holding-end", F.col("holding_duration_s") - F.col("pair_s")).when(
            F.col("type") == "pms-leg-exit", F.col("pms_leg_time_s") - F.col("pair_s")
        ),
    )
    e = e.withColumn(
        "_rand", (F.abs(F.xxhash64(F.lit(SEED), F.col("event_id"))) / F.lit(9.223372036854776e18))
    )
    return e.drop("_pkey", "track_id", "trk_first_lat", "trk_first_lon")


def event_items(types):
    """The event items of the day: one per published type, with its family, source and profile."""
    out = []
    for t in sorted(types):
        rule = event_rule(t)
        if rule is None:
            print(f"  WARNING: no reading family for event type {t!r}; put under c_vertical")
            rule = ("c_vertical", "unknown", "", "window")
        family, source, page, profile = rule
        rec = EVENT_RECORD.get(t)
        m = re.match(r"^xing-(\d+)nm$", t)
        if m and int(m.group(1)) in RING_RECORD:
            rec = ("rec_time", f"{m.group(1)} NM crossing (airport record)")
        out.append(
            {
                "name": t,
                "kind": "event",
                "family": family,
                "source": source,
                "page": page,
                "profile": profile,
                "record": rec,
                "pair": pair_of(t),
                "types": [t],
                "measures": EVENT_MEASURES,
            }
        )
    for name, (types_, measures) in MEASUREMENT_ITEMS.items():
        family = (
            "e_holding_pointmerge"
            if name in ("m_holding", "m_pms")
            else "d_rings_fir_seen_measurements"
        )
        page = {"m_holding": "methods/holding.qmd", "m_pms": "methods/point-merge.qmd"}.get(
            name, "methods/measurements.qmd"
        )
        source = {
            "m_holding": "src/opdi/pipeline/holding.py",
            "m_pms": "src/opdi/pipeline/point_merge.py",
        }.get(name, "src/opdi/pipeline/events.py (measurements)")
        out.append(
            {
                "name": name,
                "kind": "measurement",
                "family": family,
                "source": source,
                "page": page,
                "profile": "episode" if name in ("m_holding", "m_pms") else "whole",
                "record": None,
                "pair": pair_of(types_[0]) if types_ else None,
                "types": list(types_) if types_ else None,
                "measures": measures,
                "value": measures[0][0],
            }
        )
    return out


def _population(em, item):
    from pyspark.sql import functions as F

    df = em
    if item["types"]:
        df = df.filter(F.col("type").isin(*item["types"]))
    if item["kind"] == "measurement":
        df = df.filter(F.col(item["value"]).isNotNull())
    return df


def pool_and_summary(spark, em, items):
    """Per item: the pool of candidate examples (to the driver) and the summary of all rows."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    pools, summaries = {}, {}
    ev_items = [it for it in items if it["kind"] == "event"]
    # Event items: one pass over all events, the windows partitioned by type.
    cols = [m for m, _, _ in EVENT_MEASURES]
    base = em.select("event_id", "type", "_rand", "rec_time", *cols)
    n = Window.partitionBy("type")
    flags = [
        F.row_number().over(n.orderBy("_rand")) <= N_RANDOM,
        F.count(F.lit(1)).over(n) < READ_WHOLE_BELOW,
    ]
    for m, _, sides in EVENT_MEASURES:
        flags.append(F.row_number().over(n.orderBy(F.col(m).desc_nulls_last())) <= N_TAIL)
        if sides == "both":
            flags.append(F.row_number().over(n.orderBy(F.col(m).asc_nulls_last())) <= N_TAIL)
    keep = flags[0]
    for f in flags[1:]:
        keep = keep | f
    ids = base.withColumn("_keep", keep).filter("_keep").select("event_id", "type").toPandas()
    print(f"  pool of the event items: {len(ids):,} rows", flush=True)

    aggs = [
        F.count(F.lit(1)).alias("rows"),
        F.count("rec_time").alias("with_record"),
        F.sum(F.when(F.abs(F.col("record_diff_s")) <= 60, 1).otherwise(0)).alias("within_60s"),
    ]
    for m in cols:
        aggs.append(F.count(m).alias(f"{m}__count"))
        aggs.append(F.percentile_approx(m, list(PERCENTILES), 10000).alias(f"{m}__p"))
    summ = base.groupBy("type").agg(*aggs).toPandas()
    for it in ev_items:
        row = summ[summ["type"] == it["name"]]
        summaries[it["name"]] = row.iloc[0].to_dict() if len(row) else None

    for it in items:
        if it["kind"] != "measurement":
            continue
        pop = _population(em, it)
        mcols = [m for m, _, _ in it["measures"]]
        b = pop.select("event_id", "type", "_rand", *mcols).withColumn("_item", F.lit(it["name"]))
        n = Window.partitionBy("_item")
        flags = [
            F.row_number().over(n.orderBy("_rand")) <= N_RANDOM,
            F.count(F.lit(1)).over(n) < READ_WHOLE_BELOW,
        ]
        for m, _, sides in it["measures"]:
            flags.append(F.row_number().over(n.orderBy(F.col(m).desc_nulls_last())) <= N_TAIL)
            if sides == "both":
                flags.append(F.row_number().over(n.orderBy(F.col(m).asc_nulls_last())) <= N_TAIL)
        keep = flags[0]
        for f in flags[1:]:
            keep = keep | f
        pools[it["name"]] = (
            b.withColumn("_keep", keep).filter("_keep").select("event_id").toPandas()
        )
        aggs = [F.count(F.lit(1)).alias("rows")]
        for m in mcols:
            aggs.append(F.count(m).alias(f"{m}__count"))
            aggs.append(F.percentile_approx(m, list(PERCENTILES), 10000).alias(f"{m}__p"))
        summaries[it["name"]] = b.agg(*aggs).toPandas().iloc[0].to_dict()
    for it in ev_items:
        pools[it["name"]] = ids[ids["type"] == it["name"]][["event_id"]]
    return pools, summaries


def _summary_from_spark(it, row):
    """``summary_rows`` and ``record`` of an item from its Spark aggregate row."""
    rows = []
    for m, desc, _ in it["measures"]:
        p = row.get(f"{m}__p") if row else None
        r = {
            "measure": m,
            "description": desc,
            "count": int(row.get(f"{m}__count", 0)) if row else 0,
        }
        for i, q in enumerate(PERCENTILES):
            r[f"p{round(q * 100)}"] = None if p is None or len(p) <= i else p[i]
        rows.append(r)
    record = None
    if it["kind"] == "event" and it["record"] and row:
        record = {
            "what": it["record"][1],
            "with_record": int(row["with_record"]),
            "within_60s": int(row["within_60s"]),
        }
    return rows, (int(row["rows"]) if row else 0), record


def _airports_pandas():
    import milestone_plausibility as mp
    import pyarrow.dataset as ds

    fs = mp._s3fs()
    a = (
        ds.dataset("eurocontrol/opdi-world/oa_airports", filesystem=fs, format="parquet")
        .to_table(
            columns=[
                "ident",
                "type",
                "name",
                "latitude_deg",
                "longitude_deg",
                "elevation_ft",
                "iso_country",
            ]
        )
        .to_pandas()
    )
    a = a.dropna(subset=["ident"]).drop_duplicates("ident").set_index("ident")
    for c in ("latitude_deg", "longitude_deg", "elevation_ft"):
        a[c] = a[c].astype(float)
    r = (
        ds.dataset("eurocontrol/opdi-world/oa_runways", filesystem=fs, format="parquet")
        .to_table()
        .to_pandas()
    )
    return a, r


def context_frames(spark, tracks, ev, examples):
    """Reports and events of the examples' tracks, thinned on the cluster.

    ``examples``: pandas rows with ``ex_id``, ``flight_id``, ``event_time``,
    ``profile`` and, for episodes, ``epi_start`` and ``epi_end``.
    """
    import pandas as pd
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    sel = examples[["ex_id", "flight_id", "event_time", "profile", "epi_start", "epi_end"]].copy()
    for c in ("event_time", "epi_start", "epi_end"):
        sel[c] = pd.to_datetime(sel[c])
    sdf = F.broadcast(
        spark.createDataFrame(
            sel,
            "ex_id string, flight_id string, event_time timestamp, profile string, epi_start timestamp, epi_end timestamp",  # noqa: E501
        ).withColumnRenamed("event_time", "_et")
    )
    t = tracks.select(
        F.col("track_id").alias("flight_id"),
        "event_time",
        "lat",
        "lon",
        F.col("baro_altitude_c").alias("alt_m"),
        F.col("baro_altitude").alias("raw_alt_m"),
        F.col("velocity").alias("gs_ms"),
        F.col("vert_rate").alias("vr_ms"),
        "on_ground",
        "heading",
        F.col("cumulative_distance_nm").alias("cum_nm"),
    )
    j = t.join(sdf, "flight_id")
    dt = F.col("event_time").cast("double") - F.col("_et").cast("double")
    j = j.withColumn("_dt", dt)
    win = j.filter(F.abs(F.col("_dt")) <= WINDOW_S)
    wb = Window.partitionBy("ex_id", F.floor(F.col("_dt") / 10)).orderBy(F.abs(F.col("_dt")))
    win = win.withColumn("_n", F.row_number().over(wb)).filter(
        (F.abs(F.col("_dt")) <= 30) | (F.col("_n") == 1)
    )
    win = win.withColumn("part", F.lit("window"))

    def thinned(df, rows, part):
        w = Window.partitionBy("ex_id").orderBy("event_time")
        wa = Window.partitionBy("ex_id")
        df = df.withColumn("_r", F.row_number().over(w)).withColumn(
            "_c", F.count(F.lit(1)).over(wa)
        )
        step = F.greatest(F.lit(1), F.ceil(F.col("_c") / F.lit(rows)))
        df = df.filter(
            (F.col("_r") % step == 0)
            | (F.col("_r") == 1)
            | (F.col("_r") == F.col("_c"))
            | (F.abs(F.col("_dt")) <= 30)
        )
        return df.withColumn("part", F.lit(part))

    prof = thinned(j.filter(F.col("profile").isin("whole", "flight")), PROFILE_ROWS, "profile")
    epi = j.filter(
        (F.col("profile") == "episode")
        & (
            F.col("event_time")
            >= F.col("epi_start") - F.expr(f"INTERVAL {EPISODE_MARGIN_S} SECONDS")
        )
        & (F.col("event_time") <= F.col("epi_end") + F.expr(f"INTERVAL {EPISODE_MARGIN_S} SECONDS"))
    )
    epi = thinned(epi, EPISODE_ROWS, "episode")
    cols = [
        "ex_id",
        "part",
        "flight_id",
        "event_time",
        "lat",
        "lon",
        "alt_m",
        "raw_alt_m",
        "gs_ms",
        "vr_ms",
        "on_ground",
        "heading",
        "cum_nm",
    ]
    reports = (
        win.select(*cols).unionByName(prof.select(*cols)).unionByName(epi.select(*cols)).toPandas()
    )
    fids = spark.createDataFrame(sel[["flight_id"]].drop_duplicates(), "flight_id string")
    events = (
        ev.join(F.broadcast(fids), "flight_id")
        .select(
            "event_id",
            "flight_id",
            "type",
            "event_time",
            "latitude",
            "longitude",
            "altitude_ft",
            "distance_flown_nm",
            "time_passed_s",
            "info",
        )
        .toPandas()
    )
    return reports, events


def _to_naive(df):
    import pandas as pd

    for c in df.columns:
        if isinstance(df[c].dtype, pd.DatetimeTZDtype):
            df[c] = df[c].dt.tz_convert("UTC").dt.tz_localize(None)
    return df


def sample(args) -> int:
    """Draw the examples of every item and bring them, with their context, to the pod."""
    import json

    import pandas as pd
    from pyspark.sql import functions as F

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    paths = run_paths(args.label, args.day)
    spark = spark_session(args.executors, f"spot checks sample {args.label}")
    try:
        ev = spark.read.parquet(paths["events"])
        fl = spark.read.parquet(paths["flight_list"])
        tracks = spark.read.parquet(paths["tracks"]).filter(
            F.col("dof") == F.lit(args.day).cast("date")
        )
        airports = spark.read.parquet(paths["airports"])
        print(f"events {paths['events']}", flush=True)
        records = flight_records(spark, fl).localCheckpoint()
        summary = track_summary(tracks).localCheckpoint()
        types = [r["type"] for r in ev.select("type").distinct().collect()]
        print(f"  {len(types)} event types: {sorted(types)}", flush=True)
        if args.rebuild_measures or not _exists(spark, paths["measures"]):
            em = event_measures(spark, ev, tracks, airports, records, summary, types)
            em.write.mode("overwrite").parquet(paths["measures"])
            print(f"  wrote {paths['measures']}", flush=True)
        em = spark.read.parquet(paths["measures"])
        items = event_items(types)
        pools, summaries = pool_and_summary(spark, em, items)

        # Flight list: every flight to the driver (one row each).
        flx = fl.join(summary, "flight_id", "left").join(records, "flight_id", "left").toPandas()
        flx = _to_naive(flx)
        apts, rwys = _airports_pandas()
        flx = flight_list_measures(flx, apts)
        rejected = {}
        try:
            rj = spark.read.parquet(paths["rejected"]).filter(
                F.col("dof") == F.lit(args.day).cast("date")
            )
            rejected = {r["type"]: r["count"] for r in rj.groupBy("type").count().collect()}
        except Exception as exc:  # noqa: BLE001 -- the count is informative only
            print(f"  no rejected events read: {exc}")

        # Draw the examples of the event and measurement items.
        all_ids = pd.concat([p.assign(_item=k) for k, p in pools.items()], ignore_index=True)
        sid = spark.createDataFrame(all_ids[["event_id"]].drop_duplicates(), "event_id string")
        pool_rows = em.join(F.broadcast(sid), "event_id").toPandas()
        pool_rows = _to_naive(pool_rows)
        print(f"  pool rows to the driver: {len(pool_rows):,}", flush=True)
        meta = []
        chosen = []
        for it in items:
            ids = set(pools[it["name"]]["event_id"])
            p = pool_rows[pool_rows["event_id"].isin(ids)]
            rows, total, record = _summary_from_spark(it, summaries.get(it["name"]))
            ex = draw_examples(p, it["measures"], total=total)
            ex.insert(0, "item", it["name"])
            ex.insert(1, "ex_id", [f"{it['name']}#{i + 1:02d}" for i in range(len(ex))])
            chosen.append((it, ex, rows, total, record))
        for name, (columns, measures) in FLIGHT_LIST_ITEMS.items():
            pop = flx[FLIGHT_LIST_POPULATION[name](flx)] if name in FLIGHT_LIST_POPULATION else flx
            it = {
                "name": name,
                "kind": "flight-list",
                "family": "f_flight_list",
                "source": FLIGHT_LIST_SOURCE,
                "page": FLIGHT_LIST_PAGE,
                "profile": "flight",
                "record": None,
                "pair": None,
                "columns": columns,
                "measures": measures,
            }
            pop = pop.assign(_rand=random_key(pop["flight_id"].astype(str).tolist(), SEED))
            ex = draw_examples(pop, measures, key="flight_id")
            ex.insert(0, "item", name)
            ex.insert(1, "ex_id", [f"{name}#{i + 1:02d}" for i in range(len(ex))])
            ex["event_time"] = ex["trk_first_time"]
            rows = summary_rows(pop, measures)
            chosen.append((it, ex, rows, len(pop), flight_list_record(name, pop)))

        # Context of all examples in one pass.
        sel = []
        for it, ex, *_ in chosen:
            part = ex[["ex_id", "flight_id", "event_time"]].copy()
            part["profile"] = it["profile"]
            part["epi_start"] = pd.NaT
            part["epi_end"] = pd.NaT
            if it["profile"] == "episode" and "pair_time" in ex.columns:
                pt = pd.to_datetime(ex["pair_time"])
                part["epi_start"] = pd.concat([ex["event_time"], pt], axis=1).min(axis=1)
                part["epi_end"] = pd.concat([ex["event_time"], pt], axis=1).max(axis=1)
            sel.append(part)
        sel = pd.concat(sel, ignore_index=True)
        reports, events = context_frames(spark, tracks, ev, sel)
        reports, events = _to_naive(reports), _to_naive(events)
        print(f"  reports to the driver: {len(reports):,}; events: {len(events):,}", flush=True)
        fl_ids = pd.concat(
            [ex["flight_id"] for it, ex, *_ in chosen if it["kind"] == "flight-list"]
        ).drop_duplicates()
        cand = (
            spark.read.parquet(paths["candidates"])
            .join(
                F.broadcast(spark.createDataFrame(fl_ids.to_frame("track_id"), "track_id string")),
                "track_id",
            )
            .toPandas()
        )
        cand = _to_naive(cand)
        # Flight-list columns of the event examples' flights.
        ev_fids = pd.concat(
            [ex["flight_id"] for it, ex, *_ in chosen if it["kind"] != "flight-list"]
        ).drop_duplicates()
        fl_cols = [c for c in flx.columns if c not in ("adep", "ades")]
        flrows = flx[flx["flight_id"].isin(set(ev_fids))][fl_cols]
    finally:
        spark.stop()

    for it, ex, rows, total, record in chosen:
        fam = out / it["family"]
        fam.mkdir(parents=True, exist_ok=True)
        base = item_file_name(it["name"])
        if it["kind"] != "flight-list":
            extra = [c for c in flrows.columns if c not in ex.columns or c == "flight_id"]
            ex = ex.merge(flrows[extra], on="flight_id", how="left")
        ex.to_parquet(fam / f"{base}.parquet", index=False)
        ids = set(ex["ex_id"])
        reports[reports["ex_id"].isin(ids)].to_parquet(fam / f"{base}_reports.parquet", index=False)
        events[events["flight_id"].isin(set(ex["flight_id"]))].to_parquet(
            fam / f"{base}_events.parquet", index=False
        )
        if it["kind"] == "flight-list":
            cand[cand["track_id"].isin(set(ex["flight_id"]))].to_parquet(
                fam / f"{base}_candidates.parquet", index=False
            )
        note = ""
        if it["kind"] == "event" and it["name"] in rejected:
            note = f"Removed by the plausibility checks on the day (not published, not counted above): {rejected[it['name']]:,}."  # noqa: E501
        (fam / f"summary_{base}.md").write_text(
            summary_markdown(it["name"], total, rows, record, note)
        )
        meta.append(
            {
                "kind": it["kind"],
                "name": it["name"],
                "family": it["family"],
                "count": total,
                "with_record": record["with_record"] if record else "",
                "record": record["what"] if record else "",
                "rejected": rejected.get(it["name"], "") if it["kind"] == "event" else "",
                "drawn": len(ex),
                "methods_page": it["page"],
                "source": it["source"],
                "profile": it["profile"],
                "record_col": list(it["record"]) if it.get("record") else None,
                "pair": list(it["pair"]) if it.get("pair") else None,
                "columns": list(it.get("columns", ())),
                "measures": [list(m) for m in it["measures"]],
            }
        )
        print(f"  {it['family']}/{base}: {total:,} rows, {len(ex)} examples", flush=True)
    apts.reset_index().to_parquet(out / "_airports.parquet", index=False)
    rwys.to_parquet(out / "_runways.parquet", index=False)
    (out / "_items.json").write_text(
        json.dumps({"day": args.day, "label": args.label, "items": meta}, indent=1, default=str)
    )
    print(f"wrote {len(meta)} items under {out}")
    return 0


def _exists(spark, path) -> bool:
    try:
        spark.read.parquet(path).limit(1).count()
        return True
    except Exception:  # noqa: BLE001
        return False


# --------------------------------------------------------------------------
# sheet: the reading sheets, the index and the inventory (pandas only)
# --------------------------------------------------------------------------

INVENTORY_COLUMNS = (
    "family",
    "kind",
    "name",
    "count",
    "with_record",
    "record",
    "rejected",
    "drawn",
    "methods_page",
    "source",
)

LEGEND = """How to read a sheet. Times are UTC, as HH:MM:SS (a "(+1 d)" marks the next day);
"Δ s" is the time from the example's event (for the flight list: from the track's first
report). Altitudes are barometric, in feet (ft); "hgt ft" is the height above the airport
named in the table's title, from its OurAirports elevation. Ground speed "gs" in knots
(kt), vertical rate "vr" in feet per minute, heading "hdg" in degrees from true north,
"gnd" G = report flagged on the ground. In the reports, `*` is the report nearest the
event, `<` and `>` the last report before and the first after it; an altitude shown as
`~1,234` was removed by the cleaning step (the raw value is shown). The event's own row
in the list of the track's events is marked `◀`. "Airport record" is the airport
operator's record of the same flight (EUROCONTROL Airport Operator Data Flow), "Network
Manager" its flight plan record (matched on icao24 and call sign, nearest within 6 h).
"""


def write_sheets(samples: Path) -> int:
    """Write ``<item>.md`` beside each item's parquet, ``INDEX.md`` and ``inventory.csv``."""
    import csv
    import json

    import pandas as pd

    meta = json.loads((samples / "_items.json").read_text())
    day = meta["day"]
    airports = pd.read_parquet(samples / "_airports.parquet").set_index("ident")
    runways = pd.read_parquet(samples / "_runways.parquet")
    index = [
        f"# Spot checks of {day}: index",
        "",
        f"Processed from the raw archive with the code of the `industrialisation` branch (run "
        f"`{meta['label']}`). One folder per reading family; per item: `<item>.md` the sheets, "
        "`summary_<item>.md` the measures over all rows of the day, `<item>.parquet` the "
        "examples with their measures and records, `<item>_events.parquet` and "
        "`<item>_reports.parquet` their context (`<item>_candidates.parquet` for the flight list).",
        "",
        LEGEND,
    ]
    inv = []
    for fam, what in FAMILIES.items():
        items = [m for m in meta["items"] if m["family"] == fam]
        index += [
            f"## {fam}: {what}",
            "",
            "| item | kind | rows on the day | with a record | removed by checks | examples | files | detector |",  # noqa: E501
            "|---|---|---:|---:|---:|---:|---|---|",
        ]
        for m in items:
            base = item_file_name(m["name"])
            ex = pd.read_parquet(samples / fam / f"{base}.parquet")
            rep = pd.read_parquet(samples / fam / f"{base}_reports.parquet")
            evs = pd.read_parquet(samples / fam / f"{base}_events.parquet")
            cand_path = samples / fam / f"{base}_candidates.parquet"
            cand = pd.read_parquet(cand_path) if cand_path.exists() else None
            item_meta = {
                "name": m["name"],
                "kind": m["kind"],
                "profile": m["profile"],
                "record": tuple(m["record_col"]) if m.get("record_col") else None,
                "pair": tuple(m["pair"]) if m.get("pair") else None,
                "columns": tuple(m.get("columns") or ()),
                "measures": [tuple(x) for x in m["measures"]],
            }
            text = [
                f"# {m['name']}: {len(ex)} examples of {m['count']:,} on {day}",
                "",
                f"Kind: {m['kind']}. Detector: `{m['source']}`. Methods page: `{m['methods_page']}`. "  # noqa: E501
                f"Whole-day measures: `summary_{base}.md`.",
                "",
                "Measures used to draw the tail examples: "
                + "; ".join(f"`{c}` {d}" for c, d, _ in item_meta["measures"])
                + ".",
                "",
                LEGEND,
            ]
            rep_by = {k: g for k, g in rep.groupby("ex_id")} if len(rep) else {}
            ev_by = {k: g for k, g in evs.groupby("flight_id")} if len(evs) else {}
            cand_by = (
                {k: g for k, g in cand.groupby("track_id")}
                if cand is not None and len(cand)
                else {}
            )
            for _, row in ex.iterrows():
                r = row.to_dict()
                reports = rep_by.get(r["ex_id"])
                events = ev_by.get(r["flight_id"])
                text.append("---")
                text.append("")
                if m["kind"] == "flight-list":
                    text.append(
                        flight_sheet(
                            item_meta,
                            r,
                            events,
                            reports,
                            cand_by.get(r["flight_id"]),
                            airports,
                            day,
                        )
                    )
                else:
                    text.append(event_sheet(item_meta, r, events, reports, airports, runways, day))
            (samples / fam / f"{base}.md").write_text("\n".join(text))
            files = f"`{fam}/{base}.md`, `summary_{base}.md`, `{base}.parquet`"
            index.append(
                f"| {m['name']} | {m['kind']} | {m['count']:,} | {m['with_record']} | {m['rejected']} | "  # noqa: E501
                f"{len(ex)} | {files} | `{m['source']}` |"
            )
            inv.append({**{k: m.get(k, "") for k in INVENTORY_COLUMNS}, "drawn": len(ex)})
        index.append("")
    (samples / "INDEX.md").write_text("\n".join(index) + "\n")
    with (samples / "inventory.csv").open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(INVENTORY_COLUMNS))
        w.writeheader()
        for r in inv:
            w.writerow(r)
    print(f"wrote sheets of {len(inv)} items, INDEX.md and inventory.csv under {samples}")
    return 0


# --------------------------------------------------------------------------
# Command line
# --------------------------------------------------------------------------


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="command", required=True)
    r = sub.add_parser("run-day")
    r.add_argument("--day", required=True)
    r.add_argument("--label", required=True)
    r.add_argument("--executors", type=int, default=13)
    r.add_argument("--only", nargs="+", default=None, help="run only the parts with these steps")
    p = sub.add_parser("run-part")
    p.add_argument("--day", required=True)
    p.add_argument("--label", required=True)
    p.add_argument("--executors", type=int, default=13)
    p.add_argument("--steps", nargs="+", required=True)
    sm = sub.add_parser("sample")
    sm.add_argument("--day", required=True)
    sm.add_argument("--label", required=True)
    sm.add_argument("--executors", type=int, default=13)
    sm.add_argument("--out", default=str(DEFAULT_SAMPLES))
    sm.add_argument("--rebuild-measures", action="store_true", help="recompute the measures table")
    sh = sub.add_parser("sheet")
    sh.add_argument("--out", default=str(DEFAULT_SAMPLES))
    c = sub.add_parser("clean")
    c.add_argument("--label", required=True)
    args = ap.parse_args(argv)
    if args.command == "sample":
        return sample(args)
    if args.command == "sheet":
        return write_sheets(Path(args.out))
    if args.command == "run-day":
        return run_day(args)
    if args.command == "run-part":
        return run_part(args)
    if args.command == "clean":
        return clean(args)
    raise SystemExit(f"unknown command {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
