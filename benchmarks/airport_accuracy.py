#!/usr/bin/env python
"""Availability and accuracy of every published item, overall and per airport.

    export PYSPARK_DRIVER_PYTHON=$PWD/.venv310/bin/python
    unset PYSPARK_PYTHON
    # 1. One day at a time (Spark, takes the machine's Spark lock): the counts
    #    and the comparisons with the reference records, as small local tables.
    .venv310/bin/python -u benchmarks/airport_accuracy.py extract --day 2026-06-10
    .venv310/bin/python -u benchmarks/airport_accuracy.py extract --day 2026-06-14
    .venv310/bin/python -u benchmarks/airport_accuracy.py extract --day 2026-06-17
    # 2. The tables (pandas only), written with their provenance.
    .venv310/bin/python -u benchmarks/airport_accuracy.py tables --out <folder>

**Two measures, never merged.** *Availability* says how often an item
exists. It is given in two ways: (a) among OPDI's own flights, the share of
the flights that could carry the item (departures for a departure item,
arrivals for an arrival item, every flight for the others) that do carry it;
this needs no reference and is given everywhere; (b) among the movements of a
reference, the share for which OPDI has the item; this also counts the flights
OPDI did not see at all. *Accuracy* says how close the item is to the
reference when both exist: for a time, the share within 30, 60 and 300 s, the
median and 90th percentile of the absolute difference and the mean signed
difference (OPDI minus reference, over the differences of at most 300 s); for
an airport, a runway, a stand or an aircraft type, the share equal.

**References.** Airport records are the airport operators' movement records
(APDF, ``reference/apdf_<month>.parquet``): off-block, take-off, landing and
in-block times, runway, stand, and the times the flight crossed 40 and 100 NM
from the airport. They exist for some European airports only. Network Manager
flights (``reference/flights_<month>.parquet``) give the departure and
destination airport, the aircraft type and the route length of each flight in
or through European airspace, and two times: take-off (off-block time plus
taxi time) and landing. A record is matched on the aircraft address, the call
sign and the airport, the nearest within 6 h, as in
``benchmarks/corrections_scorecard.py``, whose functions this script uses for
the event times so that the two give the same figures for the same day.

**Record runways.** A record's runway is compared only when it names a
runway of its airport in OurAirports (``oa_runways``, open or closed); a
number without a side letter (``31``) stands for the one open runway end of
that number with a side letter (``31L``) when there is exactly one. Any
other value counts as no runway named: Bergen, Stavanger and Trondheim send ``01`` in
every record, whatever runway was used.

**What is never published.** The tables hold counts and shares per item and
airport: no flight, track id or call sign.

**Days.** :data:`DAYS` names the processed days and where their tables are.
Each day was processed from the raw archive with one version of the code
(``benchmarks/spot_checks.py run-day``, steps 01 to 04b).
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
BENCH = REPO / "benchmarks"
if str(BENCH) not in sys.path:
    sys.path.insert(0, str(BENCH))
if str(REPO / "src") not in sys.path:
    sys.path.insert(0, str(REPO / "src"))

WAREHOUSE = "s3a://eurocontrol/opdi-world"
SPOT = f"{WAREHOUSE}/research/spot"
#: Day -> the folder holding its flight list, events and measurements.
DAYS = {
    "2026-06-10": f"{SPOT}/d0610n",
    "2026-06-14": f"{SPOT}/runs/next_day",
    "2026-06-17": f"{SPOT}/d0617n",
}
MONTH = "202606"
JOB = Path("/home/jupyter/.claude/jobs/616d5918/tmp/acc")
EXTRACT = JOB / "extract"

#: Records are looked for within this many seconds of the flight.
SEARCH_S = 6 * 3600
#: Labels (percent).
HIGH_PCT, PARTIAL_PCT = 80.0, 50.0
GOOD_PCT, FAIR_PCT = 80.0, 50.0
#: Fewer reference movements than this: the accuracy label is "no reference".
MIN_REFERENCE = 30
#: Fewer compared values than this: the accuracy label is "too few compared".
MIN_COMPARED = 30
#: Airports per table.
TOP_N = 100
#: A share is called stable across the days when it moves less than this many points.
STABLE_POINTS = 5.0
#: A time in seconds is called stable when it moves less than this many seconds,
#: or less than :data:`STABLE_SHARE` of its middle value.
STABLE_SECONDS, STABLE_SHARE = 10.0, 0.2

# --------------------------------------------------------------------------
# Items
# --------------------------------------------------------------------------

#: Event types on the departure side and on the arrival side of a flight.
DEPARTURE_TYPES = {"ATOT", "AOBT", "line-up", "take-off-roll"}
ARRIVAL_TYPES = {
    "ALDT",
    "AIBT",
    "runway-vacated",
    "xing-runway-threshold",
    "go-around",
    "holding-start",
    "holding-end",
    "pms-leg-entry",
    "pms-leg-exit",
    "pms-merge-point",
}
#: The ring radii the flight list publishes.
RING_RADII_NM = (40, 50, 60, 100, 110, 120)
#: Flight-list columns scored or counted.
FLIGHT_LIST_COLUMNS = (
    ["ADEP", "ADES", "TYPECODE", "FIRST_SEEN", "LAST_SEEN"]
    + ["AOBT", "ATOT", "ALDT", "AIBT", "RWY_DEP", "RWY_ARR", "STND_DEP", "STND_ARR"]
    + [f"C{r}_DEP" for r in RING_RADII_NM]
    + [f"C{r}_ARR" for r in RING_RADII_NM]
)
FLIGHT_LIST_DEPARTURE = {"AOBT", "ATOT", "RWY_DEP", "STND_DEP"} | {
    f"C{r}_DEP" for r in RING_RADII_NM
}
FLIGHT_LIST_ARRIVAL = {"ALDT", "AIBT", "RWY_ARR", "STND_ARR"} | {f"C{r}_ARR" for r in RING_RADII_NM}
MEASUREMENTS = ("Distance flown (NM)", "Time Passed (s)")
FL_PREFIX = "flight list: "
MEAS_PREFIX = "measurement: "


def item_side(item: str) -> str:
    """``departure``, ``arrival`` or ``any``: which flights at an airport could carry ``item``.

    Event items are the event type (with the direction for the ring
    crossings, ``xing-40nm inbound``, and the method for the take-off and
    landing times, ``ATOT (acdm)``); flight-list items start with
    ``flight list: ``, measurements with ``measurement: ``.
    """
    if item.startswith(FL_PREFIX):
        col = item[len(FL_PREFIX) :]
        if col in FLIGHT_LIST_DEPARTURE:
            return "departure"
        if col in FLIGHT_LIST_ARRIVAL:
            return "arrival"
        return "any"
    if item.startswith(MEAS_PREFIX):
        return "any"
    base = item.split(" (")[0]
    if re.match(r"^xing-[0-9]+nm ", base):
        return "departure" if base.endswith(" outbound") else "arrival"
    if base in DEPARTURE_TYPES:
        return "departure"
    if base in ARRIVAL_TYPES:
        return "arrival"
    return "any"


#: Airports counted as European: ICAO codes starting with E or L, and the
#: European codes under B, G and U (Iceland, Kosovo, the Canary Islands,
#: Ukraine, Armenia, Georgia, Azerbaijan).
EUROPEAN_PREFIXES = ("E", "L", "BI", "BK", "GC", "UK", "UD", "UG", "UB")


def is_european(icao) -> bool:
    """Whether an airport counts as European for the ranking (see :data:`EUROPEAN_PREFIXES`)."""
    if not isinstance(icao, str) or len(icao) != 4:
        return False
    return icao.upper().startswith(EUROPEAN_PREFIXES)


# --------------------------------------------------------------------------
# Labels and arithmetic (pandas)
# --------------------------------------------------------------------------


def _missing(x) -> bool:
    return x is None or (isinstance(x, float) and math.isnan(x))


def availability_label(pct):
    """``high`` (at least 80%), ``partial`` (50 to 80%), ``low`` (under 50%); None when unknown."""
    if _missing(pct):
        return None
    if pct >= HIGH_PCT:
        return "high"
    if pct >= PARTIAL_PCT:
        return "partial"
    return "low"


def accuracy_label(pct, reference_movements, compared) -> str:
    """``good`` / ``fair`` / ``limited`` from the share within 60 s (or the share equal).

    ``no reference`` when fewer than :data:`MIN_REFERENCE` reference
    movements exist; ``too few compared`` when the reference exists but fewer
    than :data:`MIN_COMPARED` values could be compared with it.
    """
    if _missing(reference_movements) or reference_movements < MIN_REFERENCE:
        return "no reference"
    if _missing(compared) or compared < MIN_COMPARED or _missing(pct):
        return "too few compared"
    if pct >= GOOD_PCT:
        return "good"
    if pct >= FAIR_PCT:
        return "fair"
    return "limited"


def _pct(n, base):
    return 100.0 * n / base if base else float("nan")


def time_stats(diffs) -> dict:
    """The accuracy measures of time differences in seconds (OPDI minus reference)."""
    d = np.asarray(diffs, dtype=float)
    d = d[~np.isnan(d)]
    a = np.abs(d)
    n = len(d)
    near = d[a <= 300]
    nan = float("nan")
    return {
        "compared": n,
        "within_30s_pct": _pct(int((a <= 30).sum()), n),
        "within_60s_pct": _pct(int((a <= 60).sum()), n),
        "within_300s_pct": _pct(int((a <= 300).sum()), n),
        "median_abs_diff_s": float(np.median(a)) if n else nan,
        "p90_abs_diff_s": float(np.percentile(a, 90)) if n else nan,
        "mean_signed_diff_within_300s_s": float(near.mean()) if len(near) else nan,
    }


def _with_refs(stats: pd.DataFrame, refs: pd.DataFrame, keys: list) -> pd.DataFrame:
    extra = [c for c in ("reference_tracked",) if c in refs.columns]
    out = refs[keys + ["reference_movements"] + extra].merge(stats, on=keys, how="outer")
    out["reference_movements"] = out["reference_movements"].fillna(0).astype(int)
    for c in ("found", "compared"):
        out[c] = out[c].fillna(0).astype(int)
    out["availability_ref_pct"] = [
        _pct(f, r) for f, r in zip(out["found"], out["reference_movements"])
    ]
    return out


def score_times(matched: pd.DataFrame, refs: pd.DataFrame, keys: list) -> pd.DataFrame:
    """Per group of ``keys``: records found, availability against the records, time accuracy.

    ``matched`` holds one row per OPDI value compared with a record:
    ``keys``, ``rec_key`` (one value per record) and ``diff_s``. ``refs``
    holds ``keys`` and ``reference_movements``. A record matched by several
    values is found once; each value is compared.
    """
    rows = []
    for k, g in matched.groupby(keys, sort=True, dropna=False):
        k = k if isinstance(k, tuple) else (k,)
        r = dict(zip(keys, k))
        r["found"] = int(g["rec_key"].nunique())
        r.update(time_stats(g["diff_s"].to_numpy()))
        rows.append(r)
    stats = pd.DataFrame(rows, columns=keys + ["found", *time_stats([]).keys()])
    return _with_refs(stats, refs, keys)


NO_NAME = {"NONE", "NAN", ""}


def _runway_key(x):
    if _missing(x):
        return None
    m = re.match(r"^0*([0-9]{1,2})([LRCG]?)$", str(x).strip().upper())
    if not m or not 1 <= int(m.group(1)) <= 36:
        return None
    return f"{int(m.group(1)):02d}{m.group(2)}"


def _stand_key(x):
    if _missing(x):
        return None
    s = re.sub(r"\s+", "", str(x)).upper()
    if s in NO_NAME:
        return None
    return s.lstrip("0") or "0"


def _plain_key(x):
    if _missing(x):
        return None
    s = str(x).strip().upper()
    return None if s in NO_NAME else s


_KEYS = {"runway": _runway_key, "stand": _stand_key, "airport": _plain_key, "type": _plain_key}


def record_runways(values: pd.DataFrame, runways: pd.DataFrame) -> pd.DataFrame:
    """The runway each record names, checked against the runways of its airport.

    ``values`` holds ``rec_apt`` (the airport) and ``rec_rwy`` (the runway
    designator the record gives); ``runways`` the runway table of OurAirports
    (``oa_runways``: ``airport_ident``, ``le_ident``, ``he_ident``,
    ``closed``). Returned: ``values`` with ``rwy_key``, the designator to
    compare with OPDI's, or None when the record names no runway of the
    airport. Three cases:

    - the record names one end of one of the airport's runways (open or
      closed): that designator, written as two digits and a side letter;
    - the record gives a number without a side letter (``31``) and exactly
      one open runway of the airport has an end with that number and a side
      letter (``31L``): that end;
    - otherwise the record value is not a runway of the airport (for example
      ``01`` in every record of an airport whose only runway is 17/35): None,
      and the record counts as naming no runway.

    An airport with no runway in the table keeps its record values as they
    are, since there is nothing to check them against.
    """
    rw = runways.copy()
    ends = pd.concat(
        [
            rw[["airport_ident", "le_ident", "closed"]].rename(columns={"le_ident": "end"}),
            rw[["airport_ident", "he_ident", "closed"]].rename(columns={"he_ident": "end"}),
        ],
        ignore_index=True,
    )
    ends["key"] = ends["end"].map(_runway_key)
    ends = ends[ends["key"].notna()]
    known = {a: set(g["key"]) for a, g in ends.groupby("airport_ident")}
    open_ends = ends[~ends["closed"].fillna(False).astype(bool)]
    by_number = {}
    for a, g in open_ends.groupby("airport_ident"):
        for k in sorted(set(g["key"])):
            if len(k) == 3:
                by_number.setdefault((a, k[:2]), []).append(k)

    def one(apt, value):
        key = _runway_key(value)
        if key is None or apt not in known:
            return key
        if key in known[apt]:
            return key
        sided = by_number.get((apt, key), []) if len(key) == 2 else []
        return sided[0] if len(sided) == 1 else None

    out = values.copy()
    out["rwy_key"] = [one(a, v) for a, v in zip(out["rec_apt"], out["rec_rwy"])]
    return out


def agreement(named, recorded, kind: str) -> str:
    """``equal``, ``differs``, ``none named`` (OPDI gives none) or ``no record value``."""
    key = _KEYS[kind]
    r = key(recorded)
    if r is None:
        return "no record value"
    n = key(named)
    if n is None:
        return "none named"
    return "equal" if n == r else "differs"


def score_equal(frame: pd.DataFrame, refs: pd.DataFrame, keys: list, kind: str) -> pd.DataFrame:
    """Per group: records whose value OPDI names (found), and the share of them equal.

    ``frame`` holds one row per record matched to an OPDI flight, with
    ``named`` (OPDI's value) and ``recorded``. Records without a value are
    left out; ``refs`` counts the reference movements of each group.
    """
    f = frame.copy()
    f["verdict"] = [agreement(a, b, kind) for a, b in zip(f["named"], f["recorded"])]
    f = f[f["verdict"] != "no record value"]
    rows = []
    for k, g in f.groupby(keys, sort=True, dropna=False):
        k = k if isinstance(k, tuple) else (k,)
        r = dict(zip(keys, k))
        named = g[g["verdict"] != "none named"]
        r["found"] = len(named)
        r["compared"] = len(named)
        r["share_equal_pct"] = _pct(int((named["verdict"] == "equal").sum()), len(named))
        rows.append(r)
    stats = pd.DataFrame(rows, columns=keys + ["found", "compared", "share_equal_pct"])
    return _with_refs(stats, refs, keys)


SIDE_BASE = ("departure", "arrival", "any")


def own_availability(bases: pd.DataFrame, carriers: pd.DataFrame, items) -> pd.DataFrame:
    """Per airport and item: flights that could carry the item, flights that do, the share.

    ``bases``: ``airport``, ``side``, ``flights`` (departures, arrivals and
    all flights of the airport). ``carriers``: ``airport``, ``item``,
    ``carriers``. Every airport gets every item, with 0 where none carries it.
    """
    grid = pd.DataFrame({"item": list(items)})
    grid["side"] = grid["item"].map(item_side)
    grid = bases.merge(grid, on="side")
    out = grid.merge(carriers[["airport", "item", "carriers"]], on=["airport", "item"], how="left")
    out["flights_with_item"] = out["carriers"].fillna(0).astype(int)
    out = out.rename(columns={"flights": "flights_eligible"}).drop(columns="carriers")
    out["availability_own_pct"] = [
        _pct(c, b) for c, b in zip(out["flights_with_item"], out["flights_eligible"])
    ]
    return out


def day_spread(per_day: pd.DataFrame, keys: list, measures: list) -> pd.DataFrame:
    """Per key and measure: the value of each day, the spread (largest minus smallest), stability.

    A share (a measure ending in ``_pct``) is stable when it moves less than
    :data:`STABLE_POINTS` points; a time when it moves less than
    :data:`STABLE_SECONDS` s or less than :data:`STABLE_SHARE` of its middle value.
    """
    rows = []
    days = sorted(per_day["day"].unique())
    for k, g in per_day.groupby(keys, sort=True, dropna=False):
        k = k if isinstance(k, tuple) else (k,)
        for m in measures:
            vals = g.set_index("day")[m]
            r = dict(zip(keys, k))
            r["measure"] = m
            for d in days:
                r[d] = float(vals.get(d, float("nan")))
            v = np.array([r[d] for d in days], dtype=float)
            v = v[~np.isnan(v)]
            if len(v) < 2:
                continue
            spread = float(v.max() - v.min())
            r["spread"] = spread
            if m.endswith("_pct"):
                r["stable"] = spread < STABLE_POINTS
            else:
                r["stable"] = spread < max(STABLE_SECONDS, STABLE_SHARE * float(np.median(v)))
            rows.append(r)
    return pd.DataFrame(rows)


def top_airports(counts: pd.Series, n: int, europe_only: bool) -> pd.DataFrame:
    """The ``n`` airports with the most movements; ties by ICAO code."""
    c = counts[[is_european(a) for a in counts.index]] if europe_only else counts
    f = pd.DataFrame({"airport": c.index, "movements": c.to_numpy()})
    f = f.sort_values(["movements", "airport"], ascending=[False, True]).head(n)
    f["rank"] = np.arange(1, len(f) + 1)
    return f.reset_index(drop=True)


# --------------------------------------------------------------------------
# extract (Spark): one day into small local tables
# --------------------------------------------------------------------------

#: Scorecard record items -> (item here, reference it is compared with).
RECORD_ITEMS = {
    "AOBT": ("AOBT", "off-block time"),
    "AIBT": ("AIBT", "in-block time"),
    "ATOT": ("ATOT", "take-off time"),
    "ATOT (acdm)": ("ATOT (acdm)", "take-off time"),
    "ATOT (legacy)": ("ATOT (legacy)", "take-off time"),
    "ALDT": ("ALDT", "landing time"),
    "ALDT (acdm)": ("ALDT (acdm)", "landing time"),
    "ALDT (legacy)": ("ALDT (legacy)", "landing time"),
    "take-off-roll (vs take-off record)": ("take-off-roll", "take-off time"),
    "line-up (vs take-off record)": ("line-up", "take-off time"),
    "runway-vacated (vs landing record)": ("runway-vacated", "landing time"),
    "xing-runway-threshold (vs landing record)": ("xing-runway-threshold", "landing time"),
    "xing-40nm inbound": ("xing-40nm inbound", "40 NM crossing time"),
    "xing-100nm inbound": ("xing-100nm inbound", "100 NM crossing time"),
    "xing-40nm outbound": ("xing-40nm outbound", "40 NM crossing time"),
    "xing-100nm outbound": ("xing-100nm outbound", "100 NM crossing time"),
    "exit-parking_position (vs off-block record, nearest per record)": (
        "exit-parking_position",
        "off-block time (nearest event per record)",
    ),
    "entry-parking_position (vs in-block record, nearest per record)": (
        "entry-parking_position",
        "in-block time (nearest event per record)",
    ),
}
#: Record kind of each item compared with airport records.
RECORD_KIND = {
    "AOBT": "AOBT",
    "AIBT": "AIBT",
    "ATOT": "ATOT",
    "ATOT (acdm)": "ATOT",
    "ATOT (legacy)": "ATOT",
    "ALDT": "ALDT",
    "ALDT (acdm)": "ALDT",
    "ALDT (legacy)": "ALDT",
    "take-off-roll": "ATOT",
    "line-up": "ATOT",
    "runway-vacated": "ALDT",
    "xing-runway-threshold": "ALDT",
    "xing-40nm inbound": "C40_ARR",
    "xing-100nm inbound": "C100_ARR",
    "xing-40nm outbound": "C40_DEP",
    "xing-100nm outbound": "C100_DEP",
    "exit-parking_position": "AOBT",
    "entry-parking_position": "AIBT",
    FL_PREFIX + "AOBT": "AOBT",
    FL_PREFIX + "ATOT": "ATOT",
    FL_PREFIX + "ALDT": "ALDT",
    FL_PREFIX + "AIBT": "AIBT",
    FL_PREFIX + "C40_DEP": "C40_DEP",
    FL_PREFIX + "C100_DEP": "C100_DEP",
    FL_PREFIX + "C40_ARR": "C40_ARR",
    FL_PREFIX + "C100_ARR": "C100_ARR",
    FL_PREFIX + "RWY_DEP": "ATOT",
    FL_PREFIX + "RWY_ARR": "ALDT",
    FL_PREFIX + "STND_DEP": "AOBT",
    FL_PREFIX + "STND_ARR": "AIBT",
    "ATOT runway (info.runway)": "ATOT",
    "ALDT runway (info.runway)": "ALDT",
    "AOBT stand (info.stand)": "AOBT",
    "AIBT stand (info.stand)": "AIBT",
}
#: Flight-list columns compared with airport records: column -> (record kind,
#: column OPDI matches the record on when the column itself is empty).
FL_RECORD_COLUMNS = {
    "AOBT": ("AOBT", ("fl_aobt", "fl_atot", "fs")),
    "ATOT": ("ATOT", ("fl_atot", "fl_aobt", "fs")),
    "ALDT": ("ALDT", ("fl_aldt", "fl_aibt", "ls")),
    "AIBT": ("AIBT", ("fl_aibt", "fl_aldt", "ls")),
    "C40_DEP": ("C40_DEP", ("c40_dep", "fl_atot", "fs")),
    "C100_DEP": ("C100_DEP", ("c100_dep", "fl_atot", "fs")),
    "C40_ARR": ("C40_ARR", ("c40_arr", "fl_aldt", "ls")),
    "C100_ARR": ("C100_ARR", ("c100_arr", "fl_aldt", "ls")),
}


def _day_bounds(day: str) -> tuple[float, float]:
    t0 = pd.Timestamp(day, tz="UTC").timestamp()
    return t0, t0 + 86400.0


def _events(spark, root: str, day: str):
    """The day's events, in the shape ``corrections_scorecard.match_records`` reads."""
    import corrections_scorecard as cs
    from pyspark.sql import functions as F

    ev = spark.read.parquet(f"{root}/opdi_flight_events").filter(
        F.col("dof") == F.lit(day).cast("date")
    )
    i = cs._info
    return ev.select(
        "id",
        "flight_id",
        "type",
        F.col("event_time").cast("double").alias("ts"),
        F.col("latitude").alias("lat"),
        F.col("longitude").alias("lon"),
        F.col("altitude").alias("alt"),
        "info",
        F.coalesce(i("apt_icao"), i("osm_airport")).alias("apt"),
        i("runway").alias("runway"),
        i("stand").alias("stand"),
        i("method").alias("method"),
        i("direction").alias("direction"),
    ).withColumn("_eid", F.monotonically_increasing_id())


def _flight_list(spark, root: str, day: str):
    """The day's flight list with the columns this script reads."""
    import corrections_scorecard as cs
    from pyspark.sql import functions as F

    s = cs._secs
    from opdi.pipeline.flight_legs import with_flight_rows

    on_day = F.col("DOF") == F.lit(day).cast("date")
    fl = spark.read.parquet(f"{root}/opdi_flight_list").filter(on_day)
    # One row per flight: the rows of the flights of tracks that hold several
    # (opdi_flight_legs, step 04b) in place of their track's row.
    n_legs, _ = cs.s3_size(f"{root}/opdi_flight_legs")
    legs = spark.read.parquet(f"{root}/opdi_flight_legs").filter(on_day) if n_legs else None
    fl = with_flight_rows(fl, legs)
    cols = [
        F.col("ID").alias("flight_id"),
        F.col("TRACK_ID").alias("track_id"),
        s("LEG_START").alias("_lo"),
        s("LEG_END").alias("_hi"),
        F.lower("ICAO24").alias("icao24"),
        F.trim("FLT_ID").alias("cs"),
        F.col("ADEP").alias("adep"),
        F.col("ADES").alias("ades"),
        F.col("TYPECODE").alias("typecode"),
        s("FIRST_SEEN").alias("fs"),
        s("LAST_SEEN").alias("ls"),
        s("ATOT").alias("fl_atot"),
        s("ALDT").alias("fl_aldt"),
        s("AOBT").alias("fl_aobt"),
        s("AIBT").alias("fl_aibt"),
        F.col("RWY_DEP").alias("rwy_dep"),
        F.col("RWY_ARR").alias("rwy_arr"),
        F.col("STND_DEP").alias("stnd_dep"),
        F.col("STND_ARR").alias("stnd_arr"),
        s("C40_DEP").alias("c40_dep"),
        s("C100_DEP").alias("c100_dep"),
        s("C40_ARR").alias("c40_arr"),
        s("C100_ARR").alias("c100_arr"),
        F.coalesce(F.col("MOVEMENT_DEP"), F.col("ADEP").isNotNull()).alias("mov_dep"),
        F.coalesce(F.col("MOVEMENT_ARR"), F.col("ADES").isNotNull()).alias("mov_arr"),
    ]
    present = []
    for c in FLIGHT_LIST_COLUMNS:
        v = F.col(c)
        ok = v.isNotNull()
        if c.startswith(("STND", "RWY", "ADE", "TYPE")):
            ok = ok & ~F.upper(F.trim(v.cast("string"))).isin(*NO_NAME)
        present.append(F.struct(F.lit(FL_PREFIX + c).alias("item"), ok.alias("ok")))
    return fl.select(*cols, F.array(*present).alias("_present"))


def _records(spark, day: str):
    """The airport records of the day (long form), the scorecard's own function."""
    import corrections_scorecard as cs

    cs.DAY = day
    cs.MONTH = MONTH
    return cs.airport_records(spark)


def _oa_runways(spark) -> pd.DataFrame:
    """The runway table of OurAirports (``oa_runways``) of the warehouse, the columns
    :func:`record_runways` reads."""
    return (
        spark.read.parquet(f"{WAREHOUSE}/oa_runways")
        .select("airport_ident", "le_ident", "he_ident", "closed")
        .toPandas()
    )


def _checked_records(spark, recs, runways: pd.DataFrame):
    """``recs`` with ``rec_rwy`` replaced by the checked designator of :func:`record_runways`.

    Also returned: the record values that name no runway of their airport,
    with their number of records, so that the extract says what it set aside.
    """
    from pyspark.sql import functions as F

    values = recs.groupBy("rec_apt", "rec_rwy").count().toPandas()
    checked = record_runways(values, runways)
    rows = checked[["rec_apt", "rec_rwy", "rwy_key"]].astype(object)
    lookup = spark.createDataFrame(
        rows.where(rows.notna(), None), "rec_apt string, rec_rwy string, rwy_key string"
    )
    out = (
        recs.join(F.broadcast(lookup), ["rec_apt", "rec_rwy"], "left")
        .withColumn("rec_rwy", F.col("rwy_key"))
        .drop("rwy_key")
    )
    dropped = checked[checked["rwy_key"].isna() & checked["rec_rwy"].map(_runway_key).notna()]
    return out, dropped[["rec_apt", "rec_rwy", "count"]].sort_values(["rec_apt", "rec_rwy"])


def _record_counts(spark, recs, FLX, day: str) -> pd.DataFrame:
    """Records per kind and airport: all with an aircraft address, and those of OPDI's flights."""
    import events_gt
    from pyspark.sql import functions as F

    c = F.col

    def named(col):
        return c(col).isNotNull() & ~F.upper(F.trim(c(col))).isin(*NO_NAME)

    allr = recs.groupBy("kind", "rec_apt").agg(
        F.count(F.lit(1)).alias("reference_movements"),
        F.count(F.when(named("rec_rwy"), 1)).alias("with_runway"),
        F.count(F.when(named("rec_stand"), 1)).alias("with_stand"),
    )
    pop = (
        recs.join(
            FLX.select(c("icao24").alias("_a"), c("cs").alias("_c"), "fs", "ls"), ["_a", "_c"]
        )
        .filter(c("rec_s").between(c("fs") - SEARCH_S, c("ls") + SEARCH_S))
        .select("kind", "_a", "_c", "rec_apt", "rec_s")
        .dropDuplicates()
        .groupBy("kind", "rec_apt")
        .agg(F.count(F.lit(1)).alias("reference_tracked"))
    )
    # Every movement of the day, with or without an aircraft address.
    b = events_gt.bridge(
        events_gt.load_apdf(spark, MONTH, full=False), events_gt.load_flights(spark, MONTH)
    ).filter(F.to_date("MVT_TIME_UTC") == F.lit(day).cast("date"))
    mv = b.groupBy(
        F.when(c("SRC_PHASE") == "DEP", "ATOT").otherwise("ALDT").alias("kind"),
        F.when(c("SRC_PHASE") == "DEP", c("ADEP_ICAO")).otherwise(c("ADES_ICAO")).alias("rec_apt"),
    ).agg(F.count(F.lit(1)).alias("movements_all"))
    out = allr.join(pop, ["kind", "rec_apt"], "left").join(mv, ["kind", "rec_apt"], "left")
    return out.toPandas()


def _flight_list_vs_records(recs, FLX) -> pd.DataFrame:
    """Flight-list times, runways and stands against the records of the row's own airports.

    ``FLX`` holds one row per flight (:func:`_flight_list`). Each row is
    compared with the nearest record of its aircraft, call sign and airport,
    and each record with one row only.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    c = F.col
    parts = []
    for col, (kind, anchor) in FL_RECORD_COLUMNS.items():
        side = "adep" if kind in ("AOBT", "ATOT", "C40_DEP", "C100_DEP") else "ades"
        value = {"AOBT": "fl_aobt", "ATOT": "fl_atot", "ALDT": "fl_aldt", "AIBT": "fl_aibt"}.get(
            col, col.lower()
        )
        named_rwy = {"ATOT": "rwy_dep", "ALDT": "rwy_arr"}.get(col)
        named_stand = {"AOBT": "stnd_dep", "AIBT": "stnd_arr"}.get(col)
        f = FLX.select(
            "flight_id",
            c("icao24").alias("_a"),
            c("cs").alias("_c"),
            c(side).alias("_apt"),
            F.coalesce(*[c(a) for a in anchor]).alias("_t"),
            c(value).alias("_v"),
            (c(named_rwy) if named_rwy else F.lit(None).cast("string")).alias("named_rwy"),
            (c(named_stand) if named_stand else F.lit(None).cast("string")).alias("named_stand"),
        )
        j = f.join(recs.filter(c("kind") == kind), ["_a", "_c"]).filter(
            (c("rec_apt") == c("_apt")) & (F.abs(c("_t") - c("rec_s")) <= SEARCH_S)
        )
        w = Window.partitionBy("flight_id").orderBy(F.abs(c("_t") - c("rec_s")))
        j = j.withColumn("_r", F.row_number().over(w)).filter(c("_r") == 1)
        # One flight row per record: a track that flies the same pair of
        # airports several times has a row for each flight, and a row whose
        # own record is missing must not be compared with the record of
        # another flight. The record keeps the row nearest to it in time.
        wr = Window.partitionBy("_a", "_c", "rec_apt", "rec_s").orderBy(
            F.abs(c("_t") - c("rec_s")), "flight_id"
        )
        j = j.withColumn("_rr", F.row_number().over(wr)).filter(c("_rr") == 1)
        parts.append(
            j.select(
                F.lit(col).alias("column"),
                F.lit(kind).alias("kind"),
                "rec_apt",
                F.sha2(F.concat_ws("|", "_a", "_c", "rec_apt", c("rec_s").cast("string")), 256)
                .substr(1, 16)
                .alias("rec_key"),
                (c("_v") - c("rec_s")).alias("diff_s"),
                "subminute",
                "named_rwy",
                "rec_rwy",
                "named_stand",
                "rec_stand",
            )
        )
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out.toPandas()


def _nm(spark, E, FLX, M, day: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Network Manager flights of the day, each with the nearest OPDI flight-list row.

    A Network Manager flight belongs to the day of its off-block time. Its
    take-off time is the off-block time plus the taxi time, its landing time
    ``ARVT_3``. Returns (one row per flight with an aircraft address, the
    flights per airport with and without an address).
    """
    import events_gt
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    c = F.col
    t0, t1 = _day_bounds(day)
    raw = events_gt.load_flights(spark, MONTH).filter(
        c("AOBT_3").cast("double").between(t0, t1 - 1e-6)
    )
    counts = (
        raw.select(c("ADEP").alias("airport"), F.lit("departure").alias("side"))
        .unionByName(raw.select(c("ADES").alias("airport"), F.lit("arrival").alias("side")))
        .groupBy("airport", "side")
        .agg(F.count(F.lit(1)).alias("nm_flights"))
        .toPandas()
    )
    nm = raw.select(
        F.lower("AIRCRAFT_ADDRESS").alias("_a"),
        F.trim("AIRCRAFT_ID").alias("_c"),
        c("AOBT_3").cast("double").alias("nm_aobt"),
        (c("AOBT_3").cast("double") + c("TAXI_TIME_3").cast("double") * 60.0).alias("nm_atot"),
        c("ARVT_3").cast("double").alias("nm_aldt"),
        c("ADEP").alias("nm_adep"),
        c("ADES").alias("nm_ades"),
        c("AIRCRAFT_TYPE_ICAO_ID").alias("nm_type"),
        c("RTE_LEN_3").cast("double").alias("route_nm"),
    ).filter(c("_a").isNotNull() & c("_c").isNotNull())
    nm = nm.withColumn("_nid", F.monotonically_increasing_id())
    f = FLX.select(
        "flight_id",
        c("icao24").alias("_a"),
        c("cs").alias("_c"),
        "adep",
        "ades",
        "typecode",
        "fl_atot",
        "fl_aldt",
        F.coalesce("fl_aobt", "fl_atot", "fs").alias("_t"),
    )
    j = nm.join(f, ["_a", "_c"]).filter(F.abs(c("_t") - c("nm_aobt")) <= SEARCH_S)
    w = Window.partitionBy("_nid").orderBy(F.abs(c("_t") - c("nm_aobt")), "flight_id")
    j = j.withColumn("_r", F.row_number().over(w)).filter(c("_r") == 1).drop("_r", "_t")
    # The nearest take-off and landing event at Network Manager's airports.
    ev = j.select("_nid", "flight_id", "nm_adep", "nm_ades", "nm_atot", "nm_aldt").join(
        E.filter(c("type").isin("ATOT", "ALDT")).select("flight_id", "type", "ts", "apt"),
        "flight_id",
    )
    ev = ev.join(FLX.select("flight_id", "adep", "ades"), "flight_id", "left")
    side_ok = F.when(
        c("type") == "ATOT", F.coalesce(c("apt"), c("adep")) == c("nm_adep")
    ).otherwise(F.coalesce(c("apt"), c("ades")) == c("nm_ades"))
    ev = ev.filter(side_ok).withColumn(
        "_d", c("ts") - F.when(c("type") == "ATOT", c("nm_atot")).otherwise(c("nm_aldt"))
    )
    ev = ev.filter(F.abs("_d") <= SEARCH_S)
    eva = ev.groupBy("_nid").agg(
        F.min_by(
            F.when(c("type") == "ATOT", c("_d")), F.when(c("type") == "ATOT", F.abs("_d"))
        ).alias("ev_atot_diff_s"),
        F.min_by(
            F.when(c("type") == "ALDT", c("_d")), F.when(c("type") == "ALDT", F.abs("_d"))
        ).alias("ev_aldt_diff_s"),
    )
    dist = (
        M.filter(c("type") == "Distance flown (NM)")
        .select(c("milestone_id").alias("id"), "value")
        .join(E.select("id", "flight_id", "ts"), "id")
        .groupBy("flight_id")
        .agg(F.max_by("value", "ts").alias("distance_nm"))
    )
    out = (
        nm.join(
            j.select("_nid", "flight_id", "adep", "ades", "typecode", "fl_atot", "fl_aldt"),
            "_nid",
            "left",
        )
        .join(eva, "_nid", "left")
        .join(dist, "flight_id", "left")
        .select(
            "nm_adep",
            "nm_ades",
            "nm_type",
            "route_nm",
            c("flight_id").isNotNull().alias("matched"),
            "adep",
            "ades",
            "typecode",
            F.when(c("adep") == c("nm_adep"), c("fl_atot") - c("nm_atot")).alias("fl_atot_diff_s"),
            F.when(c("ades") == c("nm_ades"), c("fl_aldt") - c("nm_aldt")).alias("fl_aldt_diff_s"),
            "ev_atot_diff_s",
            "ev_aldt_diff_s",
            "distance_nm",
        )
    )
    return out.toPandas(), counts


def _own(spark, E, FLX, M) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Which items OPDI's own flights carry, per airport and over all flights.

    Returns (bases: flights per airport and side; carriers per airport and
    item; pooled: per item the flights that could carry it, those that do,
    and the number of events or values).
    """
    from pyspark.sql import functions as F

    c = F.col
    ring = c("type").rlike(r"^xing-[0-9]+nm$")
    base_item = F.when(
        ring, F.concat_ws(" ", "type", F.coalesce("direction", F.lit("?")))
    ).otherwise(c("type"))
    ev = E.select("flight_id", base_item.alias("item"), "apt").unionByName(
        E.filter(c("type").isin("ATOT", "ALDT") & c("method").isNotNull()).select(
            "flight_id",
            F.concat(c("type"), F.lit(" ("), c("method"), F.lit(")")).alias("item"),
            "apt",
        )
    )
    ms = (
        M.select(c("milestone_id").alias("id"), "type")
        .join(E.select("id", "flight_id"), "id")
        .select(
            "flight_id",
            F.concat(F.lit(MEAS_PREFIX), c("type")).alias("item"),
            F.lit(None).cast("string").alias("apt"),
        )
    )
    fl = (
        FLX.select("flight_id", F.explode("_present").alias("p"))
        .filter(c("p.ok"))
        .select("flight_id", c("p.item").alias("item"), F.lit(None).cast("string").alias("apt"))
    )
    allv = ev.unionByName(ms).unionByName(fl)
    counts = allv.groupBy("item").agg(F.count(F.lit(1)).alias("values")).toPandas()
    items = sorted(counts["item"])
    sides = spark.createDataFrame(
        pd.DataFrame({"item": items, "side": [item_side(i) for i in items]})
    )
    pres = allv.join(F.broadcast(sides), "item")
    mv = FLX.select("flight_id", "adep", "ades", "mov_dep", "mov_arr")
    # A flight's movements at airports: departure, arrival, and "any" (either end).
    moves = (
        mv.filter(c("adep").isNotNull() & c("mov_dep"))
        .select("flight_id", c("adep").alias("airport"), F.lit("departure").alias("side"))
        .unionByName(
            mv.filter(c("ades").isNotNull() & c("mov_arr")).select(
                "flight_id", c("ades").alias("airport"), F.lit("arrival").alias("side")
            )
        )
    )
    anyv = moves.select("flight_id", "airport").distinct().withColumn("side", F.lit("any"))
    moves = moves.unionByName(anyv).persist()
    bases = moves.groupBy("airport", "side").agg(F.count(F.lit(1)).alias("flights")).toPandas()
    carriers = (
        pres.join(moves, ["flight_id", "side"])
        .filter(c("apt").isNull() | (c("apt") == c("airport")))
        .groupBy("airport", "item")
        .agg(F.countDistinct("flight_id").alias("carriers"))
        .toPandas()
    )
    # Over all flights: a departure item among the flights with a departure, and so on.
    n_all = FLX.count()
    n_dep = FLX.filter(c("adep").isNotNull() & c("mov_dep")).count()
    n_arr = FLX.filter(c("ades").isNotNull() & c("mov_arr")).count()
    elig = (
        pres.join(mv, "flight_id")
        .filter(
            (c("side") == "any")
            | (
                (c("side") == "departure")
                & c("adep").isNotNull()
                & c("mov_dep")
                & (c("apt").isNull() | (c("apt") == c("adep")))
            )
            | (
                (c("side") == "arrival")
                & c("ades").isNotNull()
                & c("mov_arr")
                & (c("apt").isNull() | (c("apt") == c("ades")))
            )
        )
        .groupBy("item")
        .agg(F.countDistinct("flight_id").alias("flights_with_item"))
        .toPandas()
    )
    pooled = counts.merge(elig, on="item", how="left")
    pooled["side"] = pooled["item"].map(item_side)
    pooled["flights_eligible"] = pooled["side"].map(
        {"any": n_all, "departure": n_dep, "arrival": n_arr}
    )
    pooled["flights_with_item"] = pooled["flights_with_item"].fillna(0).astype(int)
    moves.unpersist()
    return bases, carriers, pooled


def _apdf_matched(spark, E, FLX, day: str) -> pd.DataFrame:
    """Events against airport records, with ``corrections_scorecard.match_records``."""
    import corrections_scorecard as cs

    cs.DAY = day
    cs.MONTH = MONTH
    matched, n_ev, records, found, _flr = cs.match_records(spark, {"E": E, "FLX": FLX})
    m = matched.copy()
    m["rec_key"] = (
        m["_a"].astype(str)
        + "|"
        + m["_c"].astype(str)
        + "|"
        + m["rec_apt"].astype(str)
        + "|"
        + m["rec_s"].astype(str)
    ).map(lambda s: __import__("hashlib").sha256(s.encode()).hexdigest()[:16])
    keep = [
        "item",
        "kind",
        "rec_apt",
        "rec_key",
        "diff_s",
        "subminute",
        "runway",
        "rec_rwy",
        "stand",
        "rec_stand",
    ]
    check = {"events": n_ev, "records": records, "found": found}
    return m[keep], check


def extract(args) -> int:
    """Write the day's small tables to ``EXTRACT/<day>/``."""
    import corrections_scorecard as cs

    day = args.day
    root = args.root or DAYS[day]
    folder = Path(args.extract) if args.extract else EXTRACT
    out = folder / day
    out.mkdir(parents=True, exist_ok=True)
    started = time.time()
    with cs.spark_lock(f"airport_accuracy extract {day}"):
        spark = cs.spark_session(f"airport-accuracy-{day}", executors=args.executors)
        from pyspark import StorageLevel
        from pyspark.sql import functions as F

        keep = StorageLevel.MEMORY_AND_DISK
        E = _events(spark, root, day).persist(keep)
        FLX = _flight_list(spark, root, day).persist(keep)
        M = (
            spark.read.parquet(f"{root}/opdi_measurements")
            .filter(F.col("dof") == F.lit(day).cast("date"))
            .select("milestone_id", "type", "value")
            .persist(keep)
        )
        meta = {"day": day, "root": root, "events": E.count(), "flights": FLX.count()}
        print(f"  {day}: {meta['events']:,} events, {meta['flights']:,} flights", flush=True)
        matched, check = _apdf_matched(spark, E, FLX, day)
        runways = _oa_runways(spark)
        matched["rec_rwy"] = record_runways(matched[["rec_apt", "rec_rwy"]], runways)["rwy_key"]
        matched.to_parquet(out / "apdf_events.parquet", index=False)
        meta["scorecard_check"] = check
        recs, dropped = _checked_records(spark, _records(spark, day), runways)
        recs = recs.persist(keep)
        meta["record_runways_not_at_airport"] = dropped.to_dict("records")
        _record_counts(spark, recs, FLX, day).to_parquet(out / "apdf_records.parquet", index=False)
        _flight_list_vs_records(recs, FLX).to_parquet(out / "apdf_flight_list.parquet", index=False)
        recs.unpersist()
        nm, nm_counts = _nm(spark, E, FLX, M, day)
        nm.to_parquet(out / "nm.parquet", index=False)
        nm_counts.to_parquet(out / "nm_counts.parquet", index=False)
        apt_file = folder / "airports.parquet"
        if not apt_file.exists():
            spark.read.parquet(f"{WAREHOUSE}/oa_airports").select(
                F.col("ident").alias("airport"), "name", "iso_country"
            ).dropDuplicates(["airport"]).toPandas().to_parquet(apt_file, index=False)
        bases, carriers, pooled = _own(spark, E, FLX, M)
        bases.to_parquet(out / "own_bases.parquet", index=False)
        carriers.to_parquet(out / "own_carriers.parquet", index=False)
        pooled.to_parquet(out / "own_pooled.parquet", index=False)
        spark.stop()
    meta["seconds"] = round(time.time() - started)
    (out / "meta.json").write_text(json.dumps(meta, indent=1, default=str))
    print(f"  {day}: extract written to {out} in {meta['seconds']} s", flush=True)
    return 0


# --------------------------------------------------------------------------
# tables (pandas): the staged CSVs
# --------------------------------------------------------------------------

APDF = "airport records"
NM = "Network Manager"
NONE = "no reference"
#: Items compared with Network Manager at every airport.
NM_ITEMS = {
    FL_PREFIX + "ADEP": ("departure", "departure airport", "airport"),
    FL_PREFIX + "ADES": ("arrival", "destination airport", "airport"),
    FL_PREFIX + "TYPECODE": ("departure", "aircraft type", "type"),
    MEAS_PREFIX + "Distance flown (NM)": ("departure", "route length", "ratio"),
}
#: Times compared with Network Manager where an airport has no airport records.
NM_TIME_ITEMS = {"ATOT", "ALDT", FL_PREFIX + "ATOT", FL_PREFIX + "ALDT"}
#: Items compared for equality with airport records: item -> (record kind, extract source,
#: OPDI column, record column, count of records with a value, comparison).
APDF_EQUAL = {
    "ATOT runway (info.runway)": ("ATOT", "events", "runway", "rec_rwy", "with_runway", "runway"),
    "ALDT runway (info.runway)": ("ALDT", "events", "runway", "rec_rwy", "with_runway", "runway"),
    "AOBT stand (info.stand)": ("AOBT", "events", "stand", "rec_stand", "with_stand", "stand"),
    "AIBT stand (info.stand)": ("AIBT", "events", "stand", "rec_stand", "with_stand", "stand"),
    FL_PREFIX + "RWY_DEP": ("ATOT", "ATOT", "named_rwy", "rec_rwy", "with_runway", "runway"),
    FL_PREFIX + "RWY_ARR": ("ALDT", "ALDT", "named_rwy", "rec_rwy", "with_runway", "runway"),
    FL_PREFIX + "STND_DEP": ("AOBT", "AOBT", "named_stand", "rec_stand", "with_stand", "stand"),
    FL_PREFIX + "STND_ARR": ("AIBT", "AIBT", "named_stand", "rec_stand", "with_stand", "stand"),
}
FL_TIME_COLUMNS = ("AOBT", "ATOT", "ALDT", "AIBT", "C40_DEP", "C100_DEP", "C40_ARR", "C100_ARR")
RECORD_TEXT = {
    "AOBT": "off-block time",
    "ATOT": "take-off time",
    "ALDT": "landing time",
    "AIBT": "in-block time",
    "C40_DEP": "40 NM crossing time",
    "C100_DEP": "100 NM crossing time",
    "C40_ARR": "40 NM crossing time",
    "C100_ARR": "100 NM crossing time",
}
EXTRACT_TABLES = (
    "apdf_events",
    "apdf_records",
    "apdf_flight_list",
    "nm",
    "nm_counts",
    "own_bases",
    "own_carriers",
    "own_pooled",
)


def item_kind(item: str) -> str:
    """``flight list``, ``measurement`` or ``event``."""
    if item.startswith(FL_PREFIX):
        return "flight list"
    if item.startswith(MEAS_PREFIX):
        return "measurement"
    return "event"


def reference_for(item: str, has_records: bool, european: bool):
    """The reference an item is scored against at one airport, or None.

    Departure and destination airport, aircraft type and distance flown:
    Network Manager everywhere. Times, runways and stands: the airport
    records where the airport has them; where it has none, a European
    airport's take-off and landing times are compared with Network Manager.
    """
    if item in NM_ITEMS:
        return NM
    if item in RECORD_KIND:
        if has_records:
            return APDF
        if european and item in NM_TIME_ITEMS:
            return NM
    return None


def load_extracts(days, folder: Path = EXTRACT) -> dict:
    """The extract tables of ``days``, each with a ``day`` column."""
    out = {}
    for name in EXTRACT_TABLES:
        out[name] = pd.concat(
            [pd.read_parquet(folder / d / f"{name}.parquet").assign(day=d) for d in days],
            ignore_index=True,
        )
    out["airports"] = pd.read_parquet(folder / "airports.parquet")
    out["meta"] = {d: json.loads((folder / d / "meta.json").read_text()) for d in days}
    return out


def comparisons(x: dict) -> dict:
    """Long frames of every comparison: times, equal values, distance ratios, reference counts.

    Every frame has ``day``, ``item``, ``reference`` and ``airport``.
    """
    ev = x["apdf_events"].copy()
    ev["item"] = ev["item"].map(lambda i: RECORD_ITEMS[i][0])
    ev["airport"] = ev["rec_apt"]
    ev["rec_key"] = ev["day"] + "|" + ev["rec_key"]
    times = [
        ev.assign(reference=APDF)[["day", "item", "reference", "airport", "rec_key", "diff_s"]]
    ]
    fl = x["apdf_flight_list"].copy()
    fl["airport"] = fl["rec_apt"]
    fl["rec_key"] = fl["day"] + "|" + fl["rec_key"]
    t = fl[fl["column"].isin(FL_TIME_COLUMNS) & fl["diff_s"].notna()].copy()
    t["item"] = FL_PREFIX + t["column"]
    times.append(
        t.assign(reference=APDF)[["day", "item", "reference", "airport", "rec_key", "diff_s"]]
    )
    nm = x["nm"].reset_index(drop=True).copy()
    nm["rec_key"] = nm["day"] + "|nm|" + nm.index.astype(str)
    for item, col, apt in (
        ("ATOT", "ev_atot_diff_s", "nm_adep"),
        (FL_PREFIX + "ATOT", "fl_atot_diff_s", "nm_adep"),
        ("ALDT", "ev_aldt_diff_s", "nm_ades"),
        (FL_PREFIX + "ALDT", "fl_aldt_diff_s", "nm_ades"),
    ):
        g = nm[nm[col].notna()]
        times.append(
            pd.DataFrame(
                {
                    "day": g["day"],
                    "item": item,
                    "reference": NM,
                    "airport": g[apt],
                    "rec_key": g["rec_key"],
                    "diff_s": g[col],
                }
            )
        )
    times = pd.concat(times, ignore_index=True)

    equals = []
    for item, (_kind, src, named, rec, _w, how) in APDF_EQUAL.items():
        if src == "events":
            g = ev[ev["item"] == item.split(" ")[0]]
        else:
            g = fl[fl["column"] == src]
        equals.append(
            pd.DataFrame(
                {
                    "day": g["day"],
                    "item": item,
                    "reference": APDF,
                    "airport": g["airport"],
                    "named": g[named],
                    "recorded": g[rec],
                    "how": how,
                }
            )
        )
    for item, (side, _text, how) in NM_ITEMS.items():
        if how == "ratio":
            continue
        apt = "nm_adep" if side == "departure" else "nm_ades"
        named = {"airport": "adep" if side == "departure" else "ades", "type": "typecode"}[how]
        rec = {"airport": apt, "type": "nm_type"}[how]
        equals.append(
            pd.DataFrame(
                {
                    "day": nm["day"],
                    "item": item,
                    "reference": NM,
                    "airport": nm[apt],
                    "named": nm[named].where(nm["matched"]),
                    "recorded": nm[rec],
                    "how": how,
                }
            )
        )
    equals = pd.concat(equals, ignore_index=True)

    r = nm[nm["route_nm"].notna() & (nm["route_nm"] > 0)]
    ratios = pd.DataFrame(
        {
            "day": r["day"],
            "item": MEAS_PREFIX + "Distance flown (NM)",
            "reference": NM,
            "airport": r["nm_adep"],
            "ratio": r["distance_nm"] / r["route_nm"],
        }
    )

    rc = x["apdf_records"]
    refs = []
    for item, kind in RECORD_KIND.items():
        g = rc[rc["kind"] == kind]
        col = APDF_EQUAL[item][4] if item in APDF_EQUAL else "reference_movements"
        refs.append(
            pd.DataFrame(
                {
                    "day": g["day"],
                    "item": item,
                    "reference": APDF,
                    "airport": g["rec_apt"],
                    "reference_movements": g[col].fillna(0).astype(int),
                    "reference_tracked": g["reference_tracked"].fillna(0).astype(int),
                }
            )
        )
    for item, (side, _text, how) in NM_ITEMS.items():
        apt = "nm_adep" if side == "departure" else "nm_ades"
        src = r if how == "ratio" else nm
        g = src.groupby(["day", apt]).size().rename("reference_movements").reset_index()
        refs.append(g.rename(columns={apt: "airport"}).assign(item=item, reference=NM))
    for item in NM_TIME_ITEMS:
        apt = "nm_adep" if "ATOT" in item else "nm_ades"
        g = nm.groupby(["day", apt]).size().rename("reference_movements").reset_index()
        refs.append(g.rename(columns={apt: "airport"}).assign(item=item, reference=NM))
    refs = pd.concat(refs, ignore_index=True)
    return {"times": times, "equals": equals, "ratios": ratios, "refs": refs}


def score_ratios(frame: pd.DataFrame, refs: pd.DataFrame, keys: list) -> pd.DataFrame:
    """Distance flown against the route length: median ratio and share within 10%."""
    rows = []
    for k, g in frame.groupby(keys, sort=True, dropna=False):
        k = k if isinstance(k, tuple) else (k,)
        v = g["ratio"].dropna().to_numpy(dtype=float)
        r = dict(zip(keys, k))
        r["found"] = r["compared"] = len(v)
        r["median_ratio"] = float(np.median(v)) if len(v) else float("nan")
        r["within_10pct_pct"] = _pct(int((np.abs(v - 1) <= 0.1).sum()), len(v))
        rows.append(r)
    stats = pd.DataFrame(
        rows, columns=keys + ["found", "compared", "median_ratio", "within_10pct_pct"]
    )
    return _with_refs(stats, refs, keys)


def score_all(cmp: dict, keys: list, keep=None) -> pd.DataFrame:
    """Every comparison scored per group of ``keys`` (which include ``item`` and ``reference``).

    ``keep``, when given, is a function of a frame returning the rows to keep
    (applied to the comparisons and to the reference counts alike).
    """

    def sel(f):
        return f[keep(f)] if keep is not None else f

    refs = (
        sel(cmp["refs"])
        .groupby(keys, as_index=False, dropna=False)[["reference_movements", "reference_tracked"]]
        .sum(min_count=1)
    )
    parts = []
    t = sel(cmp["times"])
    time_items = set(t["item"]) | {i for i in RECORD_KIND if i not in APDF_EQUAL} | NM_TIME_ITEMS
    rt = refs[refs["item"].isin(time_items)]
    parts.append(score_times(t, rt, keys).assign(accuracy_measure="share within 60 s"))
    e = sel(cmp["equals"])
    for how in ("runway", "stand", "airport", "type"):
        g = e[e["how"] == how]
        items = (
            set(g["item"])
            | {i for i, v in APDF_EQUAL.items() if v[5] == how}
            | {i for i, v in NM_ITEMS.items() if v[2] == how}
        )
        re_ = refs[refs["item"].isin(items)]
        parts.append(score_equal(g, re_, keys, how).assign(accuracy_measure="share equal"))
    rr = refs[refs["item"] == MEAS_PREFIX + "Distance flown (NM)"]
    parts.append(
        score_ratios(sel(cmp["ratios"]), rr, keys).assign(
            accuracy_measure="share within 10% of the route length"
        )
    )
    out = pd.concat(parts, ignore_index=True)
    out["accuracy_value_pct"] = out["within_60s_pct"].where(
        out["accuracy_measure"] == "share within 60 s",
        out["share_equal_pct"].where(
            out["accuracy_measure"] == "share equal", out["within_10pct_pct"]
        ),
    )
    out["accuracy_label"] = [
        accuracy_label(v, r, c)
        for v, r, c in zip(out["accuracy_value_pct"], out["reference_movements"], out["compared"])
    ]
    out["availability_ref_label"] = out["availability_ref_pct"].map(availability_label)
    return out


def compared_with(item: str, reference: str) -> str:
    """What the reference value is, in words."""
    if reference == NM:
        if item in NM_ITEMS:
            return NM_ITEMS[item][1]
        return "take-off time (off-block + taxi time)" if "ATOT" in item else "landing time"
    if item in RECORD_ITEMS.values() or item in {v[0] for v in RECORD_ITEMS.values()}:
        for v in RECORD_ITEMS.values():
            if v[0] == item:
                return v[1]
    if item in APDF_EQUAL:
        return "runway" if APDF_EQUAL[item][5] == "runway" else "stand"
    kind = RECORD_KIND.get(item)
    return RECORD_TEXT.get(kind, "")


#: Measure columns of the staged tables, in order.
SCORE_COLUMNS = [
    "reference_movements",
    "reference_tracked",
    "found",
    "availability_ref_pct",
    "availability_ref_label",
    "compared",
    "within_30s_pct",
    "within_60s_pct",
    "within_300s_pct",
    "median_abs_diff_s",
    "p90_abs_diff_s",
    "mean_signed_diff_within_300s_s",
    "share_equal_pct",
    "median_ratio",
    "within_10pct_pct",
    "accuracy_measure",
    "accuracy_value_pct",
    "accuracy_label",
]
OWN_COLUMNS = [
    "flights_eligible",
    "flights_with_item",
    "availability_own_pct",
    "availability_own_label",
]


def _own_pooled(x: dict, days) -> pd.DataFrame:
    p = x["own_pooled"]
    p = p[p["day"].isin(days)]
    g = p.groupby("item", as_index=False)[["values", "flights_eligible", "flights_with_item"]].sum()
    g["values_per_day"] = g["values"] / len(days)
    g["availability_own_pct"] = [
        _pct(c, b) for c, b in zip(g["flights_with_item"], g["flights_eligible"])
    ]
    g["availability_own_label"] = g["availability_own_pct"].map(availability_label)
    return g


def item_table(x: dict, cmp: dict, days, label: str, record_airports: set) -> pd.DataFrame:
    """``item_accuracy`` rows of one set of days: one row per item and reference."""
    own = _own_pooled(x, days)

    def keep(f):
        ok = f["day"].isin(days)
        nm_time = (f["reference"] == NM) & f["item"].isin(NM_TIME_ITEMS)
        eu_no_rec = f["airport"].map(lambda a: is_european(a) and a not in record_airports)
        return ok & (~nm_time | eu_no_rec.astype(bool))

    sc = score_all(cmp, ["item", "reference"], keep)
    sc = sc[(sc["reference_movements"] > 0) | (sc["found"] > 0)]
    items = sorted(set(own["item"]) | set(sc["item"]))
    rows = pd.DataFrame({"item": items})
    rows = rows.merge(sc, on="item", how="left")
    rows["reference"] = rows["reference"].fillna(NONE)
    rows = rows.merge(own, on="item", how="left")
    rows.insert(0, "days", label)
    rows["kind"] = rows["item"].map(item_kind)
    rows["side"] = rows["item"].map(item_side)
    rows["compared_with"] = [
        compared_with(i, r) if r != NONE else "" for i, r in zip(rows["item"], rows["reference"])
    ]
    rows.loc[rows["reference"] == NONE, "accuracy_label"] = NONE
    rows["reference_scope"] = np.select(
        [
            rows["reference"] == APDF,
            (rows["reference"] == NM) & rows["item"].isin(NM_TIME_ITEMS),
            rows["reference"] == NM,
        ],
        [
            "movements at the airports with airport records",
            "flights at European airports without airport records",
            "flights in Network Manager's records (in or through European airspace)",
        ],
        default="",
    )
    cols = (
        ["days", "item", "kind", "side", "values", "values_per_day"]
        + OWN_COLUMNS
        + ["reference", "compared_with", "reference_scope"]
        + SCORE_COLUMNS
    )
    return rows[cols].sort_values(["kind", "item", "reference"]).reset_index(drop=True)


def airport_table(
    x: dict,
    cmp: dict,
    airports: pd.DataFrame,
    record_airports: set,
) -> pd.DataFrame:
    """Per airport of ``airports`` (``airport``, ``rank``) and item: availability and accuracy."""
    days = sorted(x["own_bases"]["day"].unique())
    bases = x["own_bases"].groupby(["airport", "side"], as_index=False)["flights"].sum()
    carriers = x["own_carriers"].groupby(["airport", "item"], as_index=False)["carriers"].sum()
    items = sorted(i for i in x["own_pooled"]["item"].unique() if " (" not in i)
    items += [i for i in APDF_EQUAL if i not in items]
    own = own_availability(bases[bases["airport"].isin(airports["airport"])], carriers, items)
    own["availability_own_label"] = own["availability_own_pct"].map(availability_label)
    eq = pd.DataFrame({"item": [i for i in APDF_EQUAL]})
    eq["side"] = eq["item"].map(item_side)
    sc = score_all(
        cmp,
        ["item", "reference", "airport"],
        lambda f: f["airport"].isin(airports["airport"]) & f["day"].isin(days),
    )
    out = airports[["rank", "airport"]].merge(pd.DataFrame({"item": items}), how="cross")
    out = out.merge(own.drop(columns="side"), on=["airport", "item"], how="left")
    # A runway or stand named in an event is a property of that event: its
    # availability among OPDI's flights is the event's own row.
    named_in_event = [i for i in APDF_EQUAL if not i.startswith(FL_PREFIX)]
    out.loc[out["item"].isin(named_in_event), OWN_COLUMNS] = np.nan
    out["has_airport_records"] = out["airport"].isin(record_airports)
    out["in_europe"] = out["airport"].map(is_european)
    out["reference"] = [
        reference_for(i, h, e) or NONE
        for i, h, e in zip(out["item"], out["has_airport_records"], out["in_europe"])
    ]
    out = out.merge(sc, on=["item", "reference", "airport"], how="left")
    out.loc[out["reference"] == NONE, "accuracy_label"] = NONE
    fill = out["reference"] != NONE
    out.loc[fill, "reference_movements"] = out.loc[fill, "reference_movements"].fillna(0)
    out.loc[fill & out["accuracy_label"].isna(), "accuracy_label"] = NONE
    out["kind"] = out["item"].map(item_kind)
    out["side"] = out["item"].map(item_side)
    out["compared_with"] = [
        compared_with(i, r) if r != NONE else "" for i, r in zip(out["item"], out["reference"])
    ]
    return out


def airport_names(x: dict, codes) -> pd.DataFrame:
    a = x["airports"].set_index("airport")
    return pd.DataFrame(
        {
            "airport": list(codes),
            "name": [a["name"].get(c) if c in a.index else None for c in codes],
            "country": [a["iso_country"].get(c) if c in a.index else None for c in codes],
        }
    )


EUROPE_COLUMNS = [
    "rank",
    "airport",
    "name",
    "country",
    "nm_movements",
    "has_airport_records",
    "item",
    "kind",
    "side",
    "flights_eligible",
    "flights_with_item",
    "availability_own_pct",
    "availability_own_label",
    "reference",
    "compared_with",
    "reference_movements",
    "found",
    "availability_ref_pct",
    "availability_ref_label",
    "compared",
    "within_60s_pct",
    "median_abs_diff_s",
    "p90_abs_diff_s",
    "share_equal_pct",
    "median_ratio",
    "within_10pct_pct",
    "accuracy_measure",
    "accuracy_value_pct",
    "accuracy_label",
]
WORLD_COLUMNS = [
    "rank",
    "airport",
    "name",
    "country",
    "opdi_movements",
    "in_europe",
    "has_airport_records",
    "reference_note",
    "item",
    "kind",
    "side",
    "flights_eligible",
    "flights_with_item",
    "availability_own_pct",
    "availability_own_label",
    "reference",
    "compared_with",
    "reference_movements",
    "found",
    "availability_ref_pct",
    "availability_ref_label",
    "compared",
    "within_60s_pct",
    "median_abs_diff_s",
    "share_equal_pct",
    "accuracy_measure",
    "accuracy_value_pct",
    "accuracy_label",
]


def reference_note(in_europe: bool, has_records: bool) -> str:
    if has_records:
        return "airport records and Network Manager"
    if in_europe:
        return "no airport records; Network Manager only"
    return (
        "outside Europe: no reference for the airport's own movements; Network Manager "
        "covers only its flights to and from Europe (departure and destination airport, "
        "aircraft type)"
    )


def build_tables(x: dict) -> dict:
    """Every staged table from the loaded extracts."""
    days = sorted(x["meta"])
    cmp = comparisons(x)
    rc = x["apdf_records"]
    record_airports = set(rc.loc[rc["reference_movements"] > 0, "rec_apt"])
    together = "all three days" if len(days) == 3 else f"all {len(days)} days"
    items = [item_table(x, cmp, days, together, record_airports)]
    items += [item_table(x, cmp, [d], d, record_airports) for d in days]
    item_acc = pd.concat(items, ignore_index=True)

    nmc = x["nm_counts"].groupby("airport")["nm_flights"].sum()
    eu = top_airports(nmc, TOP_N, europe_only=True)
    eu_tab = airport_table(x, cmp, eu, record_airports).merge(
        eu[["airport", "movements"]].rename(columns={"movements": "nm_movements"}), on="airport"
    )
    eu_tab = eu_tab.merge(airport_names(x, eu["airport"]), on="airport")
    eu_tab = eu_tab[EUROPE_COLUMNS].sort_values(["rank", "kind", "item"]).reset_index(drop=True)

    b = x["own_bases"]
    opdi = b[b["side"] != "any"].groupby("airport")["flights"].sum()
    world = top_airports(opdi, TOP_N, europe_only=False)
    w_tab = airport_table(x, cmp, world, record_airports).merge(
        world[["airport", "movements"]].rename(columns={"movements": "opdi_movements"}),
        on="airport",
    )
    w_tab = w_tab.merge(airport_names(x, world["airport"]), on="airport")
    w_tab["reference_note"] = [
        reference_note(e, h) for e, h in zip(w_tab["in_europe"], w_tab["has_airport_records"])
    ]
    w_tab = w_tab[WORLD_COLUMNS].sort_values(["rank", "kind", "item"]).reset_index(drop=True)

    per_day = item_acc[item_acc["days"] != together].rename(columns={"days": "day"})
    spread = day_spread(
        per_day,
        ["item", "reference"],
        [
            "availability_own_pct",
            "availability_ref_pct",
            "within_60s_pct",
            "median_abs_diff_s",
            "share_equal_pct",
        ],
    )
    spread = spread.dropna(subset=["spread"]) if len(spread) else spread

    codes = list(dict.fromkeys(list(eu["airport"]) + list(world["airport"])))
    ap = airport_names(x, codes)
    ap["rank_europe"] = ap["airport"].map(dict(zip(eu["airport"], eu["rank"])))
    ap["rank_world"] = ap["airport"].map(dict(zip(world["airport"], world["rank"])))
    ap["nm_movements"] = ap["airport"].map(nmc).fillna(0).astype(int)
    ap["opdi_movements"] = ap["airport"].map(opdi).fillna(0).astype(int)
    ap["airport_record_movements"] = (
        ap["airport"]
        .map(rc[rc["kind"].isin(["ATOT", "ALDT"])].groupby("rec_apt")["movements_all"].sum())
        .fillna(0)
        .astype(int)
    )
    ap["has_airport_records"] = ap["airport"].isin(record_airports)
    ap["in_europe"] = ap["airport"].map(is_european)
    own_all = pd.concat([eu_tab, w_tab])[["airport", "item", "availability_own_pct"]]
    own_all = own_all.drop_duplicates(["airport", "item"]).set_index(["airport", "item"])[
        "availability_own_pct"
    ]
    for col, item in (
        ("departures_with_off_block_pct", "AOBT"),
        ("departures_with_line_up_pct", "line-up"),
        ("departures_with_stand_pct", FL_PREFIX + "STND_DEP"),
        ("departures_with_take_off_pct", "ATOT"),
        ("arrivals_with_landing_pct", "ALDT"),
        ("arrivals_with_in_block_pct", "AIBT"),
    ):
        ap[col] = [own_all.get((a, item), float("nan")) for a in ap["airport"]]
    ap = ap.sort_values(["rank_europe", "rank_world"]).reset_index(drop=True)
    return {
        "item_accuracy.csv": item_acc,
        "airport_accuracy_europe.csv": eu_tab,
        "airport_accuracy_world.csv": w_tab,
        "day_spread.csv": spread,
        "airports.csv": ap,
    }


def scorecard_check(x: dict, day: str, scorecard: Path) -> pd.DataFrame:
    """The day's ATOT, ALDT, AOBT and AIBT against airport records, beside the scorecard's lines."""
    cmp = comparisons(x)
    sc = score_all(
        cmp, ["item", "reference"], lambda f: (f["day"] == day) & (f["reference"] == APDF)
    )
    card = pd.read_csv(scorecard)
    card = card[card["section"] == "i"]
    rows = []
    for item in ("ATOT", "ALDT", "AOBT", "AIBT"):
        s = sc[sc["item"] == item].iloc[0]

        def line(measure, _item=item):
            v = card[(card["item"] == _item) & (card["measure"] == measure)]["value"]
            return float(v.iloc[0]) if len(v) else float("nan")

        for name, mine, theirs in (
            ("records of OPDI's flights", s["reference_tracked"], line("records")),
            ("events with a record", s["compared"], line("events with a record")),
            (
                "share of records found",
                s["found"] / s["reference_tracked"],
                line("share of records found"),
            ),
            ("share within 60 s", s["within_60s_pct"] / 100, line("share within 60 s")),
            (
                "median absolute difference s",
                s["median_abs_diff_s"],
                line("median absolute difference s"),
            ),
        ):
            rows.append(
                {
                    "item": item,
                    "measure": name,
                    "here": float(mine),
                    "scorecard": theirs,
                    "equal": abs(float(mine) - theirs) < 1e-6,
                }
            )
    return pd.DataFrame(rows)


def tables(args) -> int:
    """Write the staged CSVs and their manifest into ``args.out``."""
    import provenance

    days = args.days or sorted(DAYS)
    x = load_extracts(days, Path(args.extract) if args.extract else EXTRACT)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    built = build_tables(x)
    meta = x["meta"]
    inputs = {f"{d} flights": meta[d]["flights"] for d in days}
    inputs.update({f"{d} events": meta[d]["events"] for d in days})
    roots = [meta[d]["root"] for d in days]
    for name, frame in built.items():
        frame.to_csv(out / name, index=False, float_format="%.4g")
        provenance.record(
            out,
            name,
            script="benchmarks/airport_accuracy.py",
            argv=sys.argv[1:],
            code_paths=[
                BENCH / "airport_accuracy.py",
                BENCH / "corrections_scorecard.py",
                BENCH / "events_gt.py",
            ],
            inputs=dict(inputs, rows=len(frame)),
            notes=(
                "days "
                + ", ".join(days)
                + "; tables "
                + ", ".join(roots)
                + "; references reference/apdf_202606.parquet and reference/flights_202606.parquet"
            ),
        )
        print(f"  {name}: {len(frame):,} rows", flush=True)
    if args.scorecard:
        chk = scorecard_check(x, args.check_day, Path(args.scorecard))
        chk.to_csv(JOB / "scorecard_check.csv", index=False)
        print(chk.to_string(index=False), flush=True)
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="command", required=True)
    e = sub.add_parser("extract")
    e.add_argument("--day", required=True, choices=sorted(DAYS))
    e.add_argument("--root", default=None, help="folder of the day's tables (default: DAYS)")
    e.add_argument("--executors", type=int, default=13)
    e.add_argument("--extract", default=None, help=f"folder of the extracts (default: {EXTRACT})")
    t = sub.add_parser("tables")
    t.add_argument("--out", required=True)
    t.add_argument("--days", nargs="+", default=None)
    t.add_argument("--scorecard", default=None, help="a scorecard CSV to check one day against")
    t.add_argument("--check-day", default="2026-06-14")
    t.add_argument("--extract", default=None, help=f"folder of the extracts (default: {EXTRACT})")
    args = ap.parse_args(argv)
    if args.command == "extract":
        return extract(args)
    return tables(args)


if __name__ == "__main__":
    raise SystemExit(main())
