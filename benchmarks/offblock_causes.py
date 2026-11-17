#!/usr/bin/env python
"""Why an off-block (or in-block) time is missing or wrong: one cause per airport record.

    export PYSPARK_DRIVER_PYTHON=$PWD/.venv310/bin/python
    unset PYSPARK_PYTHON
    # 1. One day (Spark, takes the machine's Spark lock): one row per airport
    #    record of an off-block or in-block time, with what OPDI has for it.
    .venv310/bin/python -u benchmarks/offblock_causes.py extract --day 2026-06-14 \\
        --root s3a://eurocontrol/opdi-world/research/spot/runs/cur \\
        --tracks s3a://eurocontrol/opdi-world/research/spot/d0614c
    .venv310/bin/python -u benchmarks/offblock_causes.py extract --day 2026-06-10 \\
        --root s3a://eurocontrol/opdi-world/research/spot/d0610
    # 2. The tables (pandas only).
    .venv310/bin/python -u benchmarks/offblock_causes.py tables --out <folder>

**The question.** Airport records give an off-block time for many more
departures than OPDI does, while the in-block time is found more often. Is
the aircraft simply not received at the stand (the transponder is switched
on late, or the stand is out of reach of the receivers), or does OPDI miss
or mistime a push-back that its reports show?

**One cause per record.** Each record of an off-block time (``AOBT``) of
the day is given the first cause that applies, in this order:

* ``f``: OPDI has an off-block time within 60 s of the record.
* ``a1``: no OPDI flight of that aircraft and call sign (no track at all).
* ``a2``: the flight's track starts after the recorded take-off time.
* ``b``: the track's first report comes after the recorded off-block time.
  Split by how late (0 to 1, 1 to 3, 3 to 10, more than 10 minutes) and by
  the state of the first report with a position: on a runway, on a taxiway,
  moving elsewhere (ground speed above 2 kt), or standing.
* ``c``: reports exist before the recorded time, but in the 5 minutes
  around it (150 s either side) none has a position (``c_none``) or fewer
  than 10 do (``c_sparse``, sparser than one per 30 s).
* ``d``: positions exist around the recorded time and OPDI gives no time.
* ``e``: positions exist and the OPDI time is more than 60 s away
  (``e_early``: before the record, ``e_late``: after it).

Causes a, b and c describe data that is not there; d and e are what a rule
could improve. The in-block time (``AIBT``) is the mirror image: ``a2`` is a
track that ends before the recorded landing, ``b`` a last report before the
recorded in-block time (how early, and the state of the last report).

A day processed without its cleaned tracks (``--tracks`` not given) only
has the flight list: ``b`` is then read from the flight's first (last) seen
time, its state is unknown, and c and d are given together (``cd``).

**Matching.** As in ``benchmarks/corrections_scorecard.py``: an OPDI event is
matched to the nearest record of the same aircraft address, call sign and
airport within 6 h, and a record keeps its nearest event. The flight of a
record is the flight-list row of the same aircraft and call sign within 6 h
whose airport is the record's (else any), nearest in time.

**Other tracks of the aircraft.** For each record the extract also counts
the reports of the same aircraft at the airport (within 3 NM, below 1,500 ft
above it or flagged on the ground) that lie in another track in the 20
minutes before the off-block time (after the in-block time). These show
where a track was split at the stand rather than not received.

**What is never published.** ``tables`` writes counts and shares only: no
flight, track id or call sign. The extract (local, not published) holds the
records and their flights, so that cases can be read.
"""

from __future__ import annotations

import argparse
import json
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

MONTH = "202606"
WORLD = "s3a://eurocontrol/opdi-world"
JOB = Path("/home/jupyter/.claude/jobs/616d5918/tmp/next/offblock")
EXTRACT = JOB / "extract"
#: The 100 busiest European airports, by Network Manager movements.
AIRPORTS_CSV = Path(
    "/home/jupyter/work/opdi-workspace/opdi-portal/.claude/worktrees/publication/"
    "assets/accuracy/airport_accuracy_europe.csv"
)

SEARCH_S = 6 * 3600
#: Seconds either side of the recorded time in which positions are counted.
AROUND_S = 150.0
#: Fewer positions than this in those 300 s is sparser than one per 30 s.
MIN_POSITIONS = 10
#: Seconds before the off-block time (after the in-block time) in which reports
#: of the same aircraft in another track are counted.
OTHER_TRACK_S = 20 * 60
OTHER_TRACK_NM = 3.0
OTHER_TRACK_MAX_FT = 1500.0
MOVING_KT = 2.0
FT_PER_M = 3.28084
KT_PER_MPS = 1.94384

#: Record kind -> (runway record of the same movement, side, airport column).
BLOCK_KINDS = {"AOBT": ("ATOT", "dep", "adep"), "AIBT": ("ALDT", "arr", "ades")}
#: Lateness bins (minutes) of cause b.
LATE_BINS = [0, 1, 3, 10, np.inf]
LATE_LABELS = ["0-1 min", "1-3 min", "3-10 min", ">10 min"]
#: Aeroway priority when one H3 cell holds several parts of the airport.
AEROWAY_ORDER = ["runway", "taxiway", "parking_position", "apron"]


# --------------------------------------------------------------------------
# extract (Spark)
# --------------------------------------------------------------------------


def _records(spark, day: str):
    """Block-time records of the day, each with the runway time of the same movement."""
    import corrections_scorecard as cs
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    cs.DAY, cs.MONTH = day, MONTH
    recs = cs.airport_records(spark).filter(F.col("kind").isin("AOBT", "AIBT", "ATOT", "ALDT"))
    blk = recs.filter(F.col("kind").isin(*BLOCK_KINDS))
    rwy = recs.filter(F.col("kind").isin("ATOT", "ALDT")).select(
        F.when(F.col("kind") == "ATOT", "AOBT").otherwise("AIBT").alias("kind"),
        "_a",
        "_c",
        "rec_apt",
        F.col("rec_s").alias("rwy_s"),
    )
    keys = ["kind", "_a", "_c", "rec_apt", "rec_s"]
    blk = blk.dropDuplicates(keys)
    j = blk.join(rwy, ["kind", "_a", "_c", "rec_apt"], "left").filter(
        F.col("rwy_s").isNull() | (F.abs(F.col("rwy_s") - F.col("rec_s")) <= 3 * 3600)
    )
    w = Window.partitionBy(*keys).orderBy(F.abs(F.col("rwy_s") - F.col("rec_s")).asc_nulls_last())
    j = j.withColumn("_r", F.row_number().over(w)).filter(F.col("_r") == 1).drop("_r")
    # Records whose only runway records lie more than 3 h away were dropped by
    # the filter: add them back without a runway time.
    lost = blk.join(j.select(*keys), keys, "left_anti").withColumn(
        "rwy_s", F.lit(None).cast("double")
    )
    return j.unionByName(lost).withColumn(
        "rec_key", F.sha2(F.concat_ws("|", *[F.col(k).cast("string") for k in keys]), 256)
    )


def _matched_events(E, FLX, blk):
    """Per record: the nearest OPDI block event of the same aircraft, call sign and airport."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    c = F.col
    ev = (
        E.filter(c("type").isin(*BLOCK_KINDS))
        .join(
            FLX.select("flight_id", c("icao24").alias("_a"), c("cs").alias("_c"), "adep", "ades"),
            "flight_id",
        )
        .withColumn(
            "_apt",
            F.coalesce(c("apt"), F.when(c("type") == "AOBT", c("adep")).otherwise(c("ades"))),
        )
    )
    j = ev.join(blk.withColumnRenamed("kind", "type"), ["type", "_a", "_c"]).filter(
        (c("rec_apt") == c("_apt")) & (F.abs(c("ts") - c("rec_s")) <= SEARCH_S)
    )
    j = j.withColumn("diff_s", c("ts") - c("rec_s"))
    w = Window.partitionBy("_eid").orderBy(F.abs("diff_s"), "rec_s")
    j = j.withColumn("_r", F.row_number().over(w)).filter(c("_r") == 1).drop("_r")
    wr = Window.partitionBy("rec_key").orderBy(F.abs("diff_s"), "ts", "id")
    j = j.withColumn("_r", F.row_number().over(wr)).filter(c("_r") == 1)
    return j.select(
        "rec_key",
        c("ts").alias("opdi_s"),
        c("flight_id").alias("ev_flight"),
        c("info").alias("opdi_info"),
    )


def _flights(FLX, blk, ev):
    """Per record: its OPDI flight (or none) and the number of candidate flights."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    c = F.col
    f = blk.join(ev, "rec_key", "left").join(
        FLX.select(
            c("icao24").alias("_a"), c("cs").alias("_c"), "flight_id", "adep", "ades", "fs", "ls"
        ),
        ["_a", "_c"],
        "left",
    )
    f = f.filter(
        c("flight_id").isNull() | c("rec_s").between(c("fs") - SEARCH_S, c("ls") + SEARCH_S)
    )
    side_apt = F.when(c("kind") == "AOBT", c("adep")).otherwise(c("ades"))
    dist = F.greatest(c("fs") - c("rec_s"), c("rec_s") - c("ls"), F.lit(0.0))
    w = Window.partitionBy("rec_key").orderBy(
        (c("flight_id") == c("ev_flight")).desc_nulls_last(),
        (side_apt == c("rec_apt")).desc_nulls_last(),
        dist.asc_nulls_last(),
        "flight_id",
    )
    n = Window.partitionBy("rec_key")
    f = f.withColumn("n_flights", F.count("flight_id").over(n))
    f = f.withColumn("_r", F.row_number().over(w)).filter(c("_r") == 1).drop("_r")
    kept = blk.join(f.select("rec_key"), "rec_key", "left_anti")
    f = f.unionByName(kept.join(ev, "rec_key", "left"), allowMissingColumns=True)
    return f.withColumn("airport_matches", side_apt == c("rec_apt"))


def _track_facts(spark, tracks_root: str, day: str, recs):
    """Per record: the reports of its own track and of the aircraft's other tracks."""
    from pyspark.sql import functions as F

    c = F.col
    tr = spark.read.parquet(f"{tracks_root}/osn_tracks_clean").filter(
        c("dof") == F.lit(day).cast("date")
    )
    tr = tr.select(
        "track_id",
        F.lower("icao24").alias("_a"),
        c("event_time").cast("double").alias("t"),
        "lat",
        "lon",
        (c("velocity") * KT_PER_MPS).alias("kt"),
        "on_ground",
        (c("baro_altitude_c") * FT_PER_M).alias("alt_ft"),
        "h3_res_12",
    )
    own = recs.filter(c("flight_id").isNotNull()).select(
        "rec_key", "kind", "rec_s", "rec_apt", c("flight_id").alias("track_id")
    )
    ids = F.broadcast(own.select("track_id").distinct())
    rep = tr.join(ids, "track_id", "left_semi").join(own, "track_id")
    t, T = c("t"), c("rec_s")
    pos = c("lat").isNotNull() & c("lon").isNotNull()
    near = F.abs(t - T) <= AROUND_S
    state = F.struct("kt", "on_ground", "alt_ft", "h3_res_12", "lat", "lon")
    facts = rep.groupBy("rec_key").agg(
        F.min("t").alias("t_first"),
        F.max("t").alias("t_last"),
        F.min(F.when(pos, t)).alias("t_first_pos"),
        F.max(F.when(pos, t)).alias("t_last_pos"),
        F.min_by(F.when(pos, state), F.when(pos, t)).alias("first_pos"),
        F.max_by(F.when(pos, state), F.when(pos, t)).alias("last_pos"),
        F.count(F.when(near, 1)).alias("n_rep_around"),
        F.count(F.when(near & pos, 1)).alias("n_pos_around"),
        F.count(F.when(t < T, 1)).alias("n_rep_before"),
        F.count(F.when((t < T) & pos, 1)).alias("n_pos_before"),
        F.count(F.when(t > T, 1)).alias("n_rep_after"),
        F.count(F.when((t > T) & pos, 1)).alias("n_pos_after"),
    )
    # The part of the airport at the first and last position.
    lay = spark.read.parquet(f"{WORLD}/hexaero_airport_layouts").select(
        c("hexaero_apt_icao").alias("rec_apt"),
        c("hexaero_h3_id").alias("h3"),
        c("hexaero_aeroway").alias("aeroway"),
    )
    prio = F.coalesce(
        F.array_position(F.array(*[F.lit(a) for a in AEROWAY_ORDER]), c("aeroway")),
        F.lit(99),
    )
    lay = (
        lay.groupBy("rec_apt", "h3")
        .agg(F.min(F.struct(prio.alias("p"), c("aeroway").alias("a"))).alias("_m"))
        .select("rec_apt", "h3", c("_m.a").alias("aeroway"))
    )
    keys = recs.select("rec_key", "rec_apt")
    for which in ("first_pos", "last_pos"):
        cells = facts.select("rec_key", c(f"{which}.h3_res_12").alias("h3")).join(keys, "rec_key")
        named = cells.join(lay, ["rec_apt", "h3"]).select(
            "rec_key", c("aeroway").alias(f"{which}_aeroway")
        )
        facts = facts.join(named, "rec_key", "left")

    # Reports of the same aircraft at the airport, in another track.
    apts = (
        spark.read.parquet(f"{WORLD}/oa_airports")
        .select(
            c("ident").alias("rec_apt"),
            c("latitude_deg").cast("double").alias("apt_lat"),
            c("longitude_deg").cast("double").alias("apt_lon"),
            c("elevation_ft").cast("double").alias("apt_ft"),
        )
        .dropDuplicates(["rec_apt"])
    )
    rk = recs.select("rec_key", "kind", "rec_s", "rec_apt", "_a", c("flight_id").alias("own")).join(
        apts, "rec_apt", "left"
    )
    who = F.broadcast(rk.select("_a").distinct())
    other = tr.join(who, "_a", "left_semi").join(rk, "_a")
    from opdi.utils.geometry import haversine_nm

    lo = F.when(c("kind") == "AOBT", T - OTHER_TRACK_S).otherwise(T)
    hi = F.when(c("kind") == "AOBT", T).otherwise(T + OTHER_TRACK_S)
    other = other.filter(
        (c("track_id") != F.coalesce(c("own"), F.lit("")))
        & t.between(lo, hi)
        & pos
        & (haversine_nm(c("lat"), c("lon"), c("apt_lat"), c("apt_lon")) <= OTHER_TRACK_NM)
        & (
            F.coalesce(c("on_ground"), F.lit(False))
            | c("alt_ft").isNull()
            | (c("alt_ft") - F.coalesce(c("apt_ft"), F.lit(0.0)) <= OTHER_TRACK_MAX_FT)
        )
    )
    other = other.groupBy("rec_key").agg(
        F.count("*").alias("n_other"),
        F.countDistinct("track_id").alias("n_other_tracks"),
        F.max("t").alias("t_other_last"),
        F.min("t").alias("t_other_first"),
    )
    return facts.join(other, "rec_key", "left")


def extract(args) -> int:
    """Write the day's records with their OPDI facts to ``EXTRACT/<day>/records.parquet``."""
    import airport_accuracy as aa
    import corrections_scorecard as cs

    day = args.day
    out = EXTRACT / day
    out.mkdir(parents=True, exist_ok=True)
    started = time.time()
    with cs.spark_lock(f"offblock_causes extract {day}"):
        spark = cs.spark_session(f"offblock-causes-{day}", executors=args.executors)
        from pyspark import StorageLevel
        from pyspark.sql import functions as F

        keep = StorageLevel.MEMORY_AND_DISK
        try:
            E = aa._events(spark, args.root, day).filter(F.col("type").isin(*BLOCK_KINDS))
            FLX = aa._flight_list(spark, args.root, day).drop("_present").persist(keep)
            blk = _records(spark, day).persist(keep)
            ev = _matched_events(E, FLX, blk)
            recs = _flights(FLX, blk, ev).persist(keep)
            meta = {
                "day": day,
                "root": args.root,
                "tracks": args.tracks,
                "records": recs.count(),
            }
            if args.tracks:
                facts = _track_facts(spark, args.tracks, day, recs)
                recs = recs.join(facts, "rec_key", "left")
            pdf = recs.toPandas()
        finally:
            spark.stop()
    for col in ("first_pos", "last_pos"):
        if col in pdf.columns:
            for f in ("kt", "on_ground", "alt_ft", "lat", "lon"):
                pdf[f"{col}_{f}"] = pdf[col].map(lambda r, f=f: None if r is None else r[f])
            pdf = pdf.drop(columns=[col])
    pdf.to_parquet(out / "records.parquet", index=False)
    meta["seconds"] = round(time.time() - started)
    (out / "meta.json").write_text(json.dumps(meta, indent=1, default=str))
    print(json.dumps(meta), flush=True)
    return 0


# --------------------------------------------------------------------------
# tables (pandas)
# --------------------------------------------------------------------------

CAUSES = ["a1", "a2", "b", "c_none", "c_sparse", "cd", "d", "e_early", "e_late", "f"]
CAUSE_TEXT = {
    "a1": "no OPDI track of the flight",
    "a2": "track starts after the take-off (ends before the landing)",
    "b": "first report after the off-block time (last report before the in-block time)",
    "c_none": "reports before (after) the time, none with a position within 150 s",
    "c_sparse": "fewer than 10 positions within 150 s of the time",
    "cd": "reports before (after) the time, no OPDI time (positions not read)",
    "d": "positions around the time, no OPDI time",
    "e_early": "OPDI time more than 60 s before the record",
    "e_late": "OPDI time more than 60 s after the record",
    "f": "OPDI time within 60 s of the record",
}
#: Groups of causes: data that does not exist, and what a rule could improve.
GROUPS = {
    "no data (a+b+c)": ["a1", "a2", "b", "c_none", "c_sparse"],
    "rule (d+e)": ["d", "e_early", "e_late"],
}


def classify(df: pd.DataFrame, with_tracks: bool) -> pd.DataFrame:
    """Add ``cause``, ``late_min`` (cause b), ``state`` (cause b) and ``lead_s``.

    ``lead_s`` is the time from the first report to the recorded off-block
    time (from the recorded in-block time to the last report): positive when
    the aircraft was received at the stand before it left (after it arrived).
    """
    d = df.copy()
    dep = d["kind"] == "AOBT"
    diff = d["opdi_s"] - d["rec_s"]
    first = d["t_first"] if with_tracks else d["fs"]
    last = d["t_last"] if with_tracks else d["ls"]
    d["lead_s"] = np.where(dep, d["rec_s"] - first, last - d["rec_s"])
    no_flight = d["flight_id"].isna()
    rwy = d["rwy_s"]
    beyond_runway = np.where(dep, first > rwy, last < rwy) & rwy.notna()
    late = d["lead_s"] < 0
    cause = pd.Series("", index=d.index, dtype=object)

    def put(mask, name):
        m = mask & (cause == "")
        cause[m] = name

    put(diff.abs() <= 60, "f")
    put(no_flight, "a1")
    put(pd.Series(beyond_runway, index=d.index), "a2")
    put(late, "b")
    if with_tracks:
        put(d["n_pos_around"].fillna(0) == 0, "c_none")
        put(d["n_pos_around"].fillna(0) < MIN_POSITIONS, "c_sparse")
        put(d["opdi_s"].isna(), "d")
    else:
        put(d["opdi_s"].isna(), "cd")
    put(diff < 0, "e_early")
    put(diff > 0, "e_late")
    d["cause"] = cause
    d["late_min"] = pd.cut(-d["lead_s"] / 60.0, LATE_BINS, labels=LATE_LABELS, right=False)
    d.loc[d["cause"] != "b", "late_min"] = np.nan
    if with_tracks:
        side = np.where(dep, "first_pos", "last_pos")
        aero = np.where(dep, d.get("first_pos_aeroway"), d.get("last_pos_aeroway"))
        kt = np.where(dep, d.get("first_pos_kt"), d.get("last_pos_kt")).astype(float)
        state = np.select(
            [aero == "runway", aero == "taxiway", kt > MOVING_KT, pd.isna(kt)],
            ["on runway", "on taxiway", "moving elsewhere", "speed unknown"],
            "standing",
        )
        state = np.where(pd.isna(d["t_first_pos"]), "no position", state)
        d["state"] = np.where(d["cause"] == "b", state, None)
        del side
    else:
        d["state"] = None
    d["diff_s"] = diff
    return d


def _shares(g: pd.DataFrame) -> dict:
    n = len(g)
    out = {"records": n}
    counts = g["cause"].value_counts()
    for k in CAUSES:
        out[f"n_{k}"] = int(counts.get(k, 0))
        out[f"pct_{k}"] = round(100.0 * counts.get(k, 0) / n, 1) if n else np.nan
    for name, ks in GROUPS.items():
        out[f"pct_{name}"] = (
            round(100.0 * sum(counts.get(k, 0) for k in ks) / n, 1) if n else np.nan
        )
    lead = g.loc[g["flight_id"].notna(), "lead_s"].dropna()
    for q, lab in ((0.25, "q1"), (0.5, "median"), (0.75, "q3")):
        out[f"lead_{lab}_s"] = round(float(lead.quantile(q)), 0) if len(lead) else np.nan
    out["pct_received_before"] = round(100.0 * (lead > 0).mean(), 1) if len(lead) else np.nan
    return out


def load(days) -> tuple[dict, dict]:
    """Per day: the records classified from the reports (with tracks) and from the flight list."""
    full, coarse = {}, {}
    for day in days:
        p = EXTRACT / day / "records.parquet"
        if p.exists():
            df = pd.read_parquet(p)
            coarse[day] = classify(df, False)
            if "t_first" in df.columns:
                full[day] = classify(df, True)
    return full, coarse


def _rows(scope: str, day: str, detail: str, d: pd.DataFrame, airports) -> list[dict]:
    rows = []
    for kind, g in d.groupby("kind"):
        rows.append(
            {
                "scope": scope,
                "day": day,
                "airport": "all",
                "kind": kind,
                "detail": detail,
                **_shares(g),
            }
        )
        for apt, ga in g[g["rec_apt"].isin(airports)].groupby("rec_apt"):
            rows.append(
                {
                    "scope": scope,
                    "day": day,
                    "airport": apt,
                    "kind": kind,
                    "detail": detail,
                    **_shares(ga),
                }
            )
    return rows


def tables(args) -> int:
    full, coarse = load(args.days)
    busiest = pd.read_csv(AIRPORTS_CSV, usecols=["rank", "airport", "name"]).drop_duplicates(
        "airport"
    )
    busiest = busiest[busiest["rank"] <= 100]
    apts = set(busiest["airport"])
    rows = []
    for day, d in full.items():
        rows += _rows("day", day, "reports", d, apts)
    for day, d in coarse.items():
        rows += _rows("day", day, "flight list", d, apts)
    if coarse:
        pooled = pd.concat(coarse.values(), ignore_index=True)
        rows += _rows("days pooled", "+".join(coarse), "flight list", pooled, apts)
    res = pd.DataFrame(rows).merge(
        busiest.rename(columns={"rank": "airport_rank"}), on="airport", how="left"
    )
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    res.to_csv(out / "offblock_causes.csv", index=False)
    # Cause b on the days with reports: how late, and the state of the first (last) position.
    det = []
    for day, d in full.items():
        for kind, g in d[d["cause"] == "b"].groupby("kind"):
            tot = int((d["kind"] == kind).sum())
            for (lm, st), gg in g.groupby(["late_min", "state"], observed=True):
                det.append(
                    {
                        "day": day,
                        "kind": kind,
                        "late": lm,
                        "state": st,
                        "n": len(gg),
                        "pct_of_records": round(100 * len(gg) / tot, 2),
                    }
                )
    pd.DataFrame(det).to_csv(out / "offblock_causes_b_detail.csv", index=False)
    show = res[res["airport"] == "all"].set_index(["scope", "day", "detail", "kind"])
    print(show.T.to_string())
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = p.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("extract")
    e.add_argument("--day", required=True)
    e.add_argument(
        "--root", required=True, help="folder with opdi_flight_events and opdi_flight_list"
    )
    e.add_argument("--tracks", default=None, help="folder with osn_tracks_clean")
    e.add_argument("--executors", type=int, default=13)
    t = sub.add_parser("tables")
    t.add_argument("--out", required=True)
    t.add_argument("--days", nargs="+", default=["2026-06-10", "2026-06-14", "2026-06-17"])
    args = p.parse_args(argv)
    return {"extract": extract, "tables": tables}[args.cmd](args)


if __name__ == "__main__":
    raise SystemExit(main())
