"""Which flight events the aircraft's own track contradicts.

    # 1. On the cluster: the context of every event of one day, written to a
    #    small working table.
    .venv310/bin/python -u benchmarks/milestone_plausibility.py context --day 2026-06-14
    # 2. On the driver alone (pandas, no Spark): the four tables of the study.
    .venv310/bin/python -u benchmarks/milestone_plausibility.py analyse --day 2026-06-14 \\
        --results-dir <portal>/papers/milestone-plausibility-v1/data
    # 3. Delete the working table once the tables are staged.
    .venv310/bin/python -u benchmarks/milestone_plausibility.py clean --day 2026-06-14

An event says that something happened at a time: an off-block time (``AOBT``)
says the aircraft started to move from its stand, a take-off time (``ATOT``)
that it left the runway. The track says what the aircraft was doing at that
time: its height above the airport, its ground speed, whether its transponder
reported it on the ground. Where the two disagree clearly (an off-block time
while the aircraft flies at 3,000 ft and 250 kt), the event cannot be true.

**Context.** :func:`event_context` adds to each event the state of the aircraft
at the event's time, read from the nearest position report of the same track
within a time window. Heights are measured above the airport the event belongs
to. Nothing is guessed: an event with no report near its time, or whose airport
is unknown, gets an unknown (NULL) context.

**Airport records.** At about ninety European airports the airport operator
reports, for each flight, the actual off-block, take-off, landing and in-block
times (EUROCONTROL's Airport Operator Data Flow, APDF). An event that lies
within 60 s of the record of the same flight and milestone is called
*confirmed*. A check may remove at most 0.1% of the confirmed events of a type.

**Outputs** (``analyse``), each stamped in ``_manifest.json``:

* ``context_by_type.csv`` -- per event type, the share with known context and
  the percentiles of height, speed, vertical rate and distance to the airport;
* ``sequence_by_flight.csv`` -- the order of paired milestones of one track
  and the time between them;
* ``candidate_rules.csv`` -- each candidate check at each threshold: events
  removed, confirmed events removed, removed events far from the record;
* ``examples.csv`` -- up to 50 removed events per check, with their context.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List, Optional

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

import provenance  # noqa: E402

# The context of an event, and the airport sides it uses, live in the package:
# step 04 uses the same function for its plausibility checks.
from opdi.pipeline.plausibility import (  # noqa: E402
    ARRIVAL_SIDE,  # noqa: F401  (part of this script's interface)
    DEPARTURE_SIDE,  # noqa: F401
    FT_PER_M,
    FTMIN_PER_MPS,  # noqa: F401
    KT_PER_MPS,  # noqa: F401
    event_context,
)
from opdi.pipeline.runway_ops import RUNWAY_THRESHOLD_CROSSING  # noqa: E402

#: The working tables of this study, and the only place it writes.
SCRATCH = "s3a://eurocontrol/opdi-world/research/plausibility"
WORLD = "s3a://eurocontrol/opdi-world"
EVENTS = f"{WORLD}/research/airspace_events/on/opdi_flight_events"
FLIGHT_LIST = f"{WORLD}/research/airspace_events/shared/opdi_flight_list"
TRACKS = f"{WORLD}/osn_tracks_clean"
AIRPORTS = f"{WORLD}/oa_airports"

#: The four milestones the airport records give.
RECORDED = ("AOBT", "AIBT", "ATOT", "ALDT")
#: An event within this many seconds of the record of its flight is confirmed.
CONFIRMED_S = 60.0
#: A removed event further than this from the record is clearly wrong.
FAR_S = 300.0
#: The record of a flight is looked for within this many seconds of the event.
RECORD_SEARCH_S = 6 * 3600.0

_CODE_PATHS = [
    "benchmarks/milestone_plausibility.py",
    "benchmarks/events_gt.py",
    "src/opdi/pipeline/plausibility.py",
    "src/opdi/utils/geometry.py",
]


def scratch_path(day: str) -> str:
    """The working table of one day."""
    return f"{SCRATCH}/context_{day}"


def check_write_path(path: str) -> None:
    if not path.startswith(SCRATCH + "/"):
        raise AssertionError(f"refusing to write {path!r}: this study writes under {SCRATCH}")


# ---------------------------------------------------------------------------
# Step 1: the working table (Spark, on the cluster)
# ---------------------------------------------------------------------------


def _info(field: str):
    from pyspark.sql import functions as F

    return F.get_json_object(F.col("info"), f"$.{field}")


def build_context(spark, day: str, window_s: float):
    """Every event of ``day`` with its context, its flight and its record."""
    import events_gt
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    from opdi.utils.geometry import haversine_nm

    ev = spark.read.parquet(EVENTS).filter(F.col("dof") == F.lit(day).cast("date"))
    fl = (
        spark.read.parquet(FLIGHT_LIST)
        .filter(F.col("DOF") == F.lit(day).cast("date"))
        .select(
            F.col("ID").alias("_fl_id"),
            F.col("ADEP").alias("adep"),
            F.col("ADES").alias("ades"),
            F.lower(F.col("ICAO24")).alias("icao24"),
            F.trim(F.col("FLT_ID")).alias("callsign"),
            F.col("TYPECODE").alias("typecode"),
            F.col("ICAO_AIRCRAFT_CLASS").alias("aircraft_class"),
        )
    )
    ev = ev.select(
        F.col("id").alias("event_id"),
        F.col("flight_id").alias("track_id"),
        "type",
        "event_time",
        F.col("latitude").alias("ev_lat"),
        F.col("longitude").alias("ev_lon"),
        F.col("altitude").alias("ev_altitude_ft"),
        F.coalesce(_info("apt_icao"), _info("osm_airport")).alias("apt_icao"),
        _info("method").alias("method"),
        _info("direction").alias("direction"),
        _info("stand_exit").alias("_stand_exit"),
        _info("stand_entry").alias("_stand_entry"),
    )

    # A block time names no airport. It is measured against the flight's
    # departure (off-block) or destination (in-block) airport, not against the
    # stand in its ``info``: the detector takes the latest stand entry of the
    # whole track, which can lie at the other airport.
    stand_time = F.to_timestamp(F.coalesce(F.col("_stand_exit"), F.col("_stand_entry")))
    ev = ev.withColumn("stand_time", stand_time).drop("_stand_exit", "_stand_entry")
    ev = ev.join(fl, F.col("track_id") == F.col("_fl_id"), "left")
    ev = ev.drop("_fl_id")

    tracks = spark.read.parquet(TRACKS).filter(F.col("dof") == F.lit(day).cast("date"))
    tracks = tracks.select(
        "track_id",
        "event_time",
        "lat",
        "lon",
        "baro_altitude_c",
        "velocity",
        "vert_rate",
        "on_ground",
    )
    airports = spark.read.parquet(AIRPORTS)

    ctx = event_context(ev, tracks, airports, window_s)

    # The event's own published position, against its airport (used for the
    # distance circles).
    apt = F.broadcast(
        airports.select(
            F.col("ident").alias("_x_ident"),
            F.col("latitude_deg").cast("double").alias("_x_lat"),
            F.col("longitude_deg").cast("double").alias("_x_lon"),
        )
    )
    ctx = ctx.join(apt, F.col("ctx_airport") == F.col("_x_ident"), "left")
    ctx = ctx.withColumn(
        "ev_airport_distance_nm",
        haversine_nm(F.col("ev_lat"), F.col("ev_lon"), F.col("_x_lat"), F.col("_x_lon")),
    ).drop("_x_ident", "_x_lat", "_x_lon")

    # The track as a whole: when it starts and ends, and how high.
    t = tracks.withColumn("_alt_ft", F.col("baro_altitude_c") * F.lit(FT_PER_M))
    known_alt = F.when(F.col("_alt_ft").isNotNull(), F.col("event_time"))
    summary = t.groupBy(F.col("track_id").alias("_t_track")).agg(
        F.count(F.lit(1)).alias("trk_reports"),
        F.min("event_time").alias("trk_first_time"),
        F.max("event_time").alias("trk_last_time"),
        F.min_by(F.col("_alt_ft"), known_alt).alias("trk_first_altitude_ft"),
        F.max_by(F.col("_alt_ft"), known_alt).alias("trk_last_altitude_ft"),
        F.max("_alt_ft").alias("trk_max_altitude_ft"),
        F.avg(F.col("on_ground").cast("double")).alias("trk_on_ground"),
    )
    ctx = ctx.join(summary, F.col("track_id") == F.col("_t_track"), "left").drop("_t_track")

    # The airport records of the four milestones, for the flights of the day
    # and the days either side (a flight seen late lands the next day).
    events_gt.assert_utc_session(spark)
    spec = events_gt.PERIODS["2026"]
    apdf = events_gt.load_apdf(spark, spec["month"], full=False)
    flights = events_gt.load_flights(spark, spec["month"])
    d = F.lit(day).cast("date")
    days = [str(x) for x in spark.range(1).select(F.date_sub(d, 1), d, F.date_add(d, 1)).first()]
    truth = events_gt.milestones(events_gt.bridge(apdf, flights), days, "all").select(
        F.col("icao24").alias("_g_icao24"),
        F.col("callsign").alias("_g_callsign"),
        F.col("milestone").alias("_g_type"),
        F.col("gt_time"),
        F.col("gt_airport"),
        F.col("gt_subminute"),
    )
    rec = ctx.filter(F.col("type").isin(*RECORDED)).select(
        "event_id", "icao24", "callsign", "type", "event_time"
    )
    rec = rec.join(
        truth,
        (F.col("icao24") == F.col("_g_icao24"))
        & (F.col("callsign") == F.col("_g_callsign"))
        & (F.col("type") == F.col("_g_type")),
        "inner",
    ).withColumn("error_s", F.col("event_time").cast("double") - F.col("gt_time").cast("double"))
    rec = rec.filter(F.abs(F.col("error_s")) <= F.lit(RECORD_SEARCH_S))
    best = Window.partitionBy("event_id").orderBy(F.abs(F.col("error_s")).asc())
    rec = (
        rec.withColumn("_r", F.row_number().over(best))
        .filter(F.col("_r") == 1)
        .select(
            F.col("event_id").alias("_r_eid"), "gt_time", "gt_airport", "gt_subminute", "error_s"
        )
    )
    ctx = ctx.join(rec, F.col("event_id") == F.col("_r_eid"), "left").drop("_r_eid")

    # Airports that report records this day: a flight there without a record
    # is a flight the records do not cover, not a wrong event.
    apdf_airports = truth.select(F.col("gt_airport").alias("_ap")).distinct()
    ctx = ctx.join(
        F.broadcast(apdf_airports.withColumn("ctx_airport_has_records", F.lit(True))),
        F.col("ctx_airport") == F.col("_ap"),
        "left",
    ).drop("_ap")
    return ctx, truth


def run_context(args) -> int:
    from pyspark.sql import functions as F
    from run_airspace_events_day import study_session

    spark = study_session("milestone plausibility context", executors=args.executors)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", "400")
    out = scratch_path(args.day)
    check_write_path(out)
    ctx, truth = build_context(spark, args.day, args.window_s)
    ctx.repartition(24).write.mode("overwrite").parquet(out)
    n_truth = (
        truth.filter(F.to_date("gt_time") == F.lit(args.day).cast("date"))
        .groupBy("_g_type")
        .count()
    )
    stats = {r["_g_type"]: r["count"] for r in n_truth.collect()}
    meta = {"day": args.day, "window_s": args.window_s, "records_on_day": stats}
    check_write_path(out + "_meta")
    spark.createDataFrame([(json.dumps(meta),)], ["meta"]).coalesce(1).write.mode("overwrite").text(
        out + "_meta"
    )
    print("records on the day:", stats)
    print(f"wrote {out}")
    spark.stop()
    return 0


# ---------------------------------------------------------------------------
# Step 2: the tables of the study (pandas, no Spark)
# ---------------------------------------------------------------------------


def _s3fs():
    import pyarrow.fs as pafs

    import opdi.config.dotenv  # noqa: F401  (reads .env into the environment)

    return pafs.S3FileSystem(
        access_key=os.environ["AWS_ACCESS_KEY_ID"],
        secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        endpoint_override=os.environ.get("OPDI_S3_ENDPOINT", "https://s3.opensky-network.org"),
    )


def load_context(day: str, cache_dir: Optional[Path] = None):
    """The working table of ``day`` as a pandas frame (cached locally)."""
    import pandas as pd
    import pyarrow.parquet as pq

    if cache_dir is not None:
        local = Path(cache_dir) / f"context_{day}.parquet"
        if local.is_file():
            return pd.read_parquet(local)
    path = scratch_path(day).replace("s3a://", "")
    table = pq.read_table(path, filesystem=_s3fs())
    df = table.to_pandas()
    if cache_dir is not None:
        Path(cache_dir).mkdir(parents=True, exist_ok=True)
        df.to_parquet(local)
    return df


def load_meta(day: str, cache_dir: Optional[Path] = None) -> dict:
    """What the context step recorded (window, records on the day); cached."""
    local = Path(cache_dir) / f"context_{day}_meta.json" if cache_dir is not None else None
    if local is not None and local.is_file():
        return json.loads(local.read_text())
    meta = _load_meta_s3(day)
    if local is not None and meta:
        local.write_text(json.dumps(meta))
    return meta


def _load_meta_s3(day: str) -> dict:
    fs = _s3fs()
    path = (scratch_path(day) + "_meta").replace("s3a://", "")
    for info in fs.get_file_info(__import__("pyarrow.fs").fs.FileSelector(path)):
        if info.path.endswith(".txt") or "part-" in info.path:
            with fs.open_input_stream(info.path) as f:
                text = f.read().decode().strip()
            if text:
                return json.loads(text)
    return {}


# ---------------------------------------------------------------------------
# Event families and the candidate checks
# ---------------------------------------------------------------------------

#: Parts of an airport layout (``entry-<part>`` / ``exit-<part>``).
SURFACE_PARTS = (
    "apron",
    "deicing_pad",
    "hangar",
    "parking_position",
    "taxiway",
    "threshold",
    "runway",
)
SURFACE_TYPES = tuple(f"{side}-{p}" for p in SURFACE_PARTS for side in ("entry", "exit"))
#: Events that can only happen with the aircraft on the ground.
GROUND_TYPES = (
    "AOBT",
    "AIBT",
    "off-block",
    "on-block",
    "line-up",
    "take-off-roll",
    "runway-vacated",
    "runway-crossing-entry",
    "runway-crossing-vacated",
) + SURFACE_TYPES
#: Runway times: the aircraft is on the runway or a few feet above it.
RUNWAY_TIMES = ("ATOT", "ALDT", "airborne", "touchdown", RUNWAY_THRESHOLD_CROSSING)
BLOCK_TYPES = ("AOBT", "AIBT")
TOP_TYPES = ("top-of-climb", "top-of-descent")
LEVEL_TYPES = (
    "level-start",
    "level-end",
    "level-off-climb-start",
    "level-off-climb-end",
    "level-off-descent-start",
    "level-off-descent-end",
)
NAVIGATION_TYPES = (
    "holding-start",
    "holding-end",
    "pms-leg-entry",
    "pms-leg-exit",
    "pms-merge-point",
    "xing-fir",
    "go-around",
)

#: Ground events whose detector does not itself test the height: the block
#: times and the layout events. The runway family (line-up, take-off roll,
#: runway vacated, runway crossings) is classified on its own heights, so a
#: contradicting report next to it points at an altitude error, not a wrong
#: event.
CHECKED_GROUND_TYPES = BLOCK_TYPES + SURFACE_TYPES

#: Percentiles reported for every measure.
PERCENTILES = (1, 5, 25, 50, 75, 95, 99)

#: Above the highest airports in the world (Daocheng Yading, 14,472 ft): an
#: aircraft this high is in the air whatever airport it belongs to.
ABOVE_EVERY_AIRPORT_FT = 15000.0


def _ring_radius(types):
    import pandas as pd

    r = types.str.extract(r"^xing-(\d+)nm$")[0]
    return pd.to_numeric(r, errors="coerce")


def prepare(df):
    """Add the record columns and the per-track milestone times."""
    df = df.copy()
    # The event tables this study reads were written with the default settings
    # before the threshold crossing was named ``xing-runway-threshold``; in
    # them it is called ``landing``. The default settings emit no other event
    # of that name, so both names are read as the threshold crossing.
    df["type"] = df["type"].replace({"landing": RUNWAY_THRESHOLD_CROSSING})
    df["has_record"] = df["error_s"].notna()
    df["abs_error_s"] = df["error_s"].abs()
    df["confirmed"] = df["abs_error_s"] <= CONFIRMED_S
    df["far"] = df["abs_error_s"] > FAR_S
    df["t_s"] = _seconds(df["event_time"])
    df["ring_radius_nm"] = _ring_radius(df["type"])
    # First and last time of each type per track: the sequence checks.
    for typ, how in (
        ("ATOT", "min"),
        ("ALDT", "max"),
        ("AOBT", "min"),
        ("AIBT", "max"),
        ("take-off-roll", "min"),
        ("line-up", "min"),
        (RUNWAY_THRESHOLD_CROSSING, "max"),
        ("runway-vacated", "max"),
        ("top-of-climb", "min"),
        ("top-of-descent", "max"),
    ):
        per = df.loc[df["type"] == typ].groupby("track_id")["t_s"].agg(how)
        df[f"trk_{typ}_s"] = df["track_id"].map(per).astype(float)
    df["trk_first_s"] = _seconds(df["trk_first_time"])
    df["trk_last_s"] = _seconds(df["trk_last_time"])
    return df


def _seconds(times):
    """Seconds since 1970 as floats; a missing time gives NaN, not a huge negative number."""
    import numpy as np

    out = times.astype("int64").astype(float) / 1e9
    out[times.isna().to_numpy()] = np.nan
    return out


def _gt(a, b):
    """``a > b`` where both are known; False where either is unknown."""
    known = a.notna() & (b.notna() if hasattr(b, "notna") else True)
    return (a > b).fillna(False) & known


def rule_masks(df, rule: str, p: dict):
    """The events ``rule`` removes with parameters ``p`` (a boolean Series).

    Unknown facts never remove an event: every comparison with a NULL is False.
    """
    import pandas as pd

    typ = df["type"]
    h, s = df["ctx_height_ft"], df["ctx_speed_kt"]
    if rule == "missing_time":
        return df["event_time"].isna()
    if rule == "airborne_ground_event":
        m = typ.isin(p.get("types", GROUND_TYPES))
        high = _gt(h, p["height_ft"])
        if p.get("altitude_ft") is not None:
            # Airport unknown: an altitude above every airport is enough.
            high |= h.isna() & _gt(df["ctx_altitude_ft"], p["altitude_ft"])
        m &= high
        if p.get("speed_kt") is not None:
            m &= _gt(s, p["speed_kt"])
        if p.get("on_ground_max") is not None:
            m &= (df["ctx_on_ground"] <= p["on_ground_max"]).fillna(False)
        return m
    if rule == "ground_event_speed_only":
        return typ.isin(GROUND_TYPES) & _gt(s, p["speed_kt"])
    if rule == "high_above_every_airport":
        m = typ.isin(GROUND_TYPES + RUNWAY_TIMES) & _gt(df["ctx_altitude_ft"], p["altitude_ft"])
        return m & _gt(s, p["speed_kt"])
    if rule == "runway_time_too_high":
        m = typ.isin(p.get("types", RUNWAY_TIMES)) & _gt(h, p["height_ft"])
        if p.get("speed_kt") is not None:
            m &= _gt(s, p["speed_kt"])
        return m
    if rule == "block_after_takeoff":
        return (
            (typ == "AOBT")
            & ~(df["t_s"] < df["trk_ATOT_s"] - p.get("tolerance_s", 0))
            & df["trk_ATOT_s"].notna()
        )
    if rule == "block_before_landing":
        return (
            (typ == "AIBT")
            & ~(df["t_s"] > df["trk_ALDT_s"] + p.get("tolerance_s", 0))
            & df["trk_ALDT_s"].notna()
        )
    if rule == "taxi_too_long":
        lim = p["minutes"] * 60.0
        out_ = (typ == "AOBT") & _gt(df["trk_ATOT_s"] - df["t_s"], lim)
        in_ = (typ == "AIBT") & _gt(df["t_s"] - df["trk_ALDT_s"], lim)
        return out_ | in_
    if rule == "takeoff_after_landing":
        bad = _gt(df["trk_ATOT_s"], df["trk_ALDT_s"])
        return (typ == p["remove"]) & bad
    if rule == "top_order":
        return typ.isin(TOP_TYPES) & _gt(df["trk_top-of-climb_s"], df["trk_top-of-descent_s"])
    if rule == "top_on_ground":
        return (
            typ.isin(TOP_TYPES)
            & (h <= p["height_ft"]).fillna(False)
            & (s < p["speed_kt"]).fillna(False)
        )
    if rule == "far_from_airport":
        m = typ.isin(p.get("types", RUNWAY_TIMES + BLOCK_TYPES)) & _gt(
            df["ctx_airport_distance_nm"], p["distance_nm"]
        )
        if p.get("height_ft") is not None:
            m &= _gt(h, p["height_ft"])
        return m
    if rule == "level_on_ground":
        return (
            typ.isin(LEVEL_TYPES)
            & (h <= p["height_ft"]).fillna(False)
            & (s < p["speed_kt"]).fillna(False)
        )
    if rule == "navigation_on_ground":
        return (
            typ.isin(NAVIGATION_TYPES)
            & (h <= p["height_ft"]).fillna(False)
            & (s < p["speed_kt"]).fillna(False)
        )
    if rule == "ring_off_radius":
        off = (df["ev_airport_distance_nm"] - df["ring_radius_nm"]).abs()
        return df["ring_radius_nm"].notna() & _gt(off, pd.Series(p["off_nm"], index=df.index))
    raise ValueError(f"unknown rule {rule!r}")


#: The candidate checks and the thresholds tried for each.
SWEEP = (
    [
        ("airborne_ground_event", {"height_ft": h, "speed_kt": s})
        for h in (300, 500, 1000, 1500, 2000)
        for s in (None, 30, 50, 80, 100)
    ]
    + [
        ("airborne_ground_event", {"height_ft": h, "speed_kt": s, "on_ground_max": 0.0})
        for h in (500, 1000, 1500, 2000)
        for s in (50, 80, 100)
    ]
    + [
        (
            "airborne_ground_event",
            {
                "types": CHECKED_GROUND_TYPES,
                "height_ft": h,
                "altitude_ft": ABOVE_EVERY_AIRPORT_FT,
                "speed_kt": s,
                "on_ground_max": 0.0,
            },
        )
        for h in (1000, 1500, 2000)
        for s in (80, 100)
    ]
    + [("missing_time", {})]
    + [("ground_event_speed_only", {"speed_kt": s}) for s in (50, 80, 100, 150)]
    + [
        ("high_above_every_airport", {"altitude_ft": ABOVE_EVERY_AIRPORT_FT, "speed_kt": s})
        for s in (50, 100)
    ]
    + [("runway_time_too_high", {"height_ft": h}) for h in (300, 500, 1000, 1500, 2000, 3000)]
    + [("runway_time_too_high", {"height_ft": h, "speed_kt": 100}) for h in (1000, 1500, 2000)]
    + [("block_after_takeoff", {"tolerance_s": 0}), ("block_before_landing", {"tolerance_s": 0})]
    + [("taxi_too_long", {"minutes": m}) for m in (30, 45, 60, 90, 120, 180)]
    + [("takeoff_after_landing", {"remove": r}) for r in ("ATOT", "ALDT")]
    + [("top_order", {})]
    + [
        ("top_on_ground", {"height_ft": h, "speed_kt": s})
        for h in (100, 300, 1000)
        for s in (50, 80)
    ]
    + [("far_from_airport", {"distance_nm": d}) for d in (3, 5, 8, 10, 20)]
    + [("far_from_airport", {"distance_nm": d, "height_ft": 1000}) for d in (3, 5, 10)]
    + [("level_on_ground", {"height_ft": h, "speed_kt": s}) for h in (100, 300) for s in (50, 80)]
    + [("navigation_on_ground", {"height_ft": h, "speed_kt": 50}) for h in (100, 300)]
    + [("ring_off_radius", {"off_nm": o}) for o in (0.5, 1.0, 2.0)]
)


#: The proposed checks with their thresholds; ``examples.csv`` shows what each removes.
CHOSEN = {
    "missing_time": ("missing_time", {}),
    "airborne_ground_event": (
        "airborne_ground_event",
        {
            "types": CHECKED_GROUND_TYPES,
            "height_ft": 1500,
            "altitude_ft": ABOVE_EVERY_AIRPORT_FT,
            "speed_kt": 80,
            "on_ground_max": 0.0,
        },
    ),
    "block_after_takeoff": ("block_after_takeoff", {"tolerance_s": 0}),
    "block_before_landing": ("block_before_landing", {"tolerance_s": 0}),
}


def _pcts(x):
    import numpy as np

    x = np.asarray(x, dtype=float)
    x = x[~np.isnan(x)]
    if len(x) == 0:
        return {f"p{q:02d}": np.nan for q in PERCENTILES}
    v = np.percentile(x, PERCENTILES)
    return {f"p{q:02d}": round(float(y), 1) for q, y in zip(PERCENTILES, v)}


def context_by_type(df):
    """Per type (and, for the recorded four, per subset): context percentiles."""
    import pandas as pd

    measures = {
        "height_ft": "ctx_height_ft",
        "altitude_ft": "ctx_altitude_ft",
        "speed_kt": "ctx_speed_kt",
        "vertical_rate_ft_min": "ctx_vertical_rate_ft_min",
        "airport_distance_nm": "ctx_airport_distance_nm",
        "gap_s": "ctx_gap_s",
        "on_ground_share": "ctx_on_ground",
        "event_airport_distance_nm": "ev_airport_distance_nm",
        "error_s": "error_s",
    }
    rows = []
    subsets = [("all", None), ("confirmed", "confirmed"), ("record_far", "far")]
    for typ, g in df.groupby("type"):
        for name, col in subsets:
            if col is not None and typ not in RECORDED:
                continue
            sub = g if col is None else g[g[col]]
            n = len(sub)
            if n == 0:
                continue
            for mname, mcol in measures.items():
                if mname == "error_s" and typ not in RECORDED:
                    continue
                if mname == "event_airport_distance_nm" and not str(typ).startswith("xing-"):
                    continue
                known = sub[mcol].notna().sum()
                rows.append(
                    {
                        "type": typ,
                        "subset": name,
                        "n_events": n,
                        "measure": mname,
                        "n_known": int(known),
                        "share_known_pct": round(100.0 * known / n, 2),
                        **_pcts(sub[mcol]),
                    }
                )
    return pd.DataFrame(rows)


#: Pairs of milestones of one track: (name, earlier type, later type).
SEQUENCES = (
    ("taxi_out", "AOBT", "ATOT"),
    ("off_block_to_roll", "AOBT", "take-off-roll"),
    ("line_up_to_roll", "line-up", "take-off-roll"),
    ("roll_to_take_off", "take-off-roll", "ATOT"),
    ("flight", "ATOT", "ALDT"),
    ("threshold_to_landing", RUNWAY_THRESHOLD_CROSSING, "ALDT"),
    ("landing_to_vacated", "ALDT", "runway-vacated"),
    ("vacated_to_in_block", "runway-vacated", "AIBT"),
    ("taxi_in", "ALDT", "AIBT"),
    ("block_to_block", "AOBT", "AIBT"),
    ("top_of_climb_to_top_of_descent", "top-of-climb", "top-of-descent"),
)


def sequence_by_flight(df):
    """Order of paired milestones per track, and the time between them (s)."""
    import pandas as pd

    per_track = df.drop_duplicates("track_id").set_index("track_id")
    conf = {t: set(df.loc[(df["type"] == t) & df["confirmed"], "track_id"]) for t in RECORDED}
    rows = []
    for name, a, b in SEQUENCES:
        ta, tb = per_track[f"trk_{a}_s"], per_track[f"trk_{b}_s"]
        both = ta.notna() & tb.notna()
        diff = (tb - ta)[both]
        viol = diff < 0
        row = {
            "check": name,
            "earlier": a,
            "later": b,
            "n_tracks_with_both": int(both.sum()),
            "n_order_violations": int(viol.sum()),
            "share_violations_pct": round(100.0 * viol.mean(), 3) if len(diff) else None,
        }
        for t in (a, b):
            if t in conf:
                ids = diff.index.isin(list(conf[t]))
                row[f"n_with_{t}_confirmed"] = int(ids.sum())
                row[f"n_violations_with_{t}_confirmed"] = int((viol & ids).sum())
        row.update({f"diff_s_{k}": v for k, v in _pcts(diff).items()})
        rows.append(row)

    # How the block times sit in their track: the cases the detector gets wrong.
    blk = df[df["type"].isin(BLOCK_TYPES)].copy()
    has_atot = blk["trk_ATOT_s"].notna()
    has_aldt = blk["trk_ALDT_s"].notna()
    at_start = (blk["t_s"] - blk["trk_first_s"]).abs() <= 30
    at_end = (blk["t_s"] - blk["trk_last_s"]).abs() <= 30
    airborne = _gt(blk["ctx_height_ft"], 1000) & _gt(blk["ctx_speed_kt"], 80)
    for typ in BLOCK_TYPES:
        g = blk["type"] == typ
        for label, m in (
            ("track_has_ATOT_and_ALDT", has_atot & has_aldt),
            ("track_has_ATOT_only", has_atot & ~has_aldt),
            ("track_has_ALDT_only", ~has_atot & has_aldt),
            ("track_has_neither", ~has_atot & ~has_aldt),
            ("within_30s_of_track_start", at_start),
            ("within_30s_of_track_end", at_end),
            ("above_1000ft_and_80kt", airborne),
        ):
            sel = blk[g & m]
            rows.append(
                {
                    "check": f"{typ}_pattern:{label}",
                    "earlier": typ,
                    "later": "",
                    "n_tracks_with_both": int(len(sel)),
                    "n_order_violations": int(_gt(sel["ctx_height_ft"], 1000).sum()),
                    "share_violations_pct": (
                        round(100.0 * _gt(sel["ctx_height_ft"], 1000).mean(), 2)
                        if len(sel)
                        else None
                    ),
                    f"n_with_{typ}_confirmed": int(sel["confirmed"].sum()),
                    f"n_violations_with_{typ}_confirmed": int(
                        (sel["confirmed"] & _gt(sel["ctx_height_ft"], 1000)).sum()
                    ),
                }
            )
    return pd.DataFrame(rows)


def _params_label(p: dict) -> str:
    def show(v):
        if isinstance(v, (tuple, list)):
            return "blocks_and_layout" if tuple(v) == CHECKED_GROUND_TYPES else "+".join(v)
        return v

    return ";".join(f"{k}={show(v)}" for k, v in p.items())


def proposed_mask(df, chosen=None):
    """Events removed by at least one proposed check."""
    chosen = CHOSEN if chosen is None else chosen
    m = None
    for rule, p in chosen.values():
        r = rule_masks(df, rule, p)
        m = r if m is None else (m | r)
    return m


def candidate_rules(df, sweep=SWEEP):
    """Every candidate check at every threshold, per type and over all types.

    The last rows (``rule == "all_proposed"``) count the events removed by at
    least one of the proposed checks (:data:`CHOSEN`).
    """
    import pandas as pd

    rows = []
    for rule, p in list(sweep) + [("all_proposed", {})]:
        m = proposed_mask(df) if rule == "all_proposed" else rule_masks(df, rule, p)
        removed = df[m]
        if rule == "all_proposed":
            scope = tuple(df.loc[m, "type"].unique())
        elif rule == "airborne_ground_event":
            scope = p.get("types", GROUND_TYPES)
        elif rule == "ground_event_speed_only":
            scope = GROUND_TYPES
        elif rule == "missing_time":
            scope = tuple(df.loc[df["event_time"].isna(), "type"].unique())
        elif rule == "high_above_every_airport":
            scope = GROUND_TYPES + RUNWAY_TIMES
        elif rule == "runway_time_too_high":
            scope = p.get("types", RUNWAY_TIMES)
        elif rule in ("block_after_takeoff",):
            scope = ("AOBT",)
        elif rule in ("block_before_landing",):
            scope = ("AIBT",)
        elif rule == "taxi_too_long":
            scope = BLOCK_TYPES
        elif rule == "takeoff_after_landing":
            scope = (p["remove"],)
        elif rule in ("top_order", "top_on_ground"):
            scope = TOP_TYPES
        elif rule == "far_from_airport":
            scope = p.get("types", RUNWAY_TIMES + BLOCK_TYPES)
        elif rule == "level_on_ground":
            scope = LEVEL_TYPES
        elif rule == "navigation_on_ground":
            scope = NAVIGATION_TYPES
        else:
            scope = tuple(
                t
                for t in df["type"].unique()
                if str(t).startswith("xing-") and str(t).endswith("nm")
            )
        types = [t for t in scope if (df["type"] == t).any()]
        for typ in types + ["ALL"]:
            base = df[df["type"].isin(types)] if typ == "ALL" else df[df["type"] == typ]
            rem = (
                removed[removed["type"].isin(types)]
                if typ == "ALL"
                else removed[removed["type"] == typ]
            )
            n, k = len(base), len(rem)
            nc, kc = int(base["confirmed"].sum()), int(rem["confirmed"].sum())
            kr, kf = int(rem["has_record"].sum()), int(rem["far"].sum())
            rows.append(
                {
                    "rule": rule,
                    "params": _params_label(p),
                    "type": typ,
                    "n_events": n,
                    "n_removed": k,
                    "share_removed_pct": round(100.0 * k / n, 3) if n else None,
                    "n_with_record": int(base["has_record"].sum()),
                    "n_confirmed": nc,
                    "n_confirmed_removed": kc,
                    "share_confirmed_removed_pct": round(100.0 * kc / nc, 3) if nc else None,
                    "n_removed_with_record": kr,
                    "n_removed_far_from_record": kf,
                    "share_removed_far_pct": round(100.0 * kf / kr, 1) if kr else None,
                }
            )
    return pd.DataFrame(rows)


EXAMPLE_COLUMNS = [
    "rule",
    "params",
    "track_id",
    "type",
    "event_time",
    "method",
    "adep",
    "ades",
    "ctx_airport",
    "ctx_airport_elevation_ft",
    "ctx_height_ft",
    "ctx_altitude_ft",
    "ctx_speed_kt",
    "ctx_vertical_rate_ft_min",
    "ctx_on_ground",
    "ctx_gap_s",
    "ctx_airport_distance_nm",
    "trk_first_time",
    "trk_last_time",
    "trk_first_altitude_ft",
    "trk_last_altitude_ft",
    "typecode",
    "aircraft_class",
    "gt_time",
    "error_s",
]


def examples(df, chosen=None, n=50, seed=1):
    """Up to ``n`` removed events per proposed check: confirmed ones first,
    then those with a record, then the rest at random."""
    import pandas as pd

    chosen = CHOSEN if chosen is None else chosen
    out = []
    for name, (rule, p) in chosen.items():
        rem = df[rule_masks(df, rule, p)].copy()
        if rem.empty:
            continue
        rem["_o"] = (~rem["confirmed"]).astype(int) * 2 + (~rem["has_record"]).astype(int)
        rem = rem.sample(frac=1.0, random_state=seed).sort_values("_o", kind="stable").head(n)
        rem["rule"] = name
        rem["params"] = _params_label(p)
        out.append(rem[[c for c in EXAMPLE_COLUMNS if c in rem.columns]])
    if not out:
        return pd.DataFrame(columns=EXAMPLE_COLUMNS)
    return pd.concat(out, ignore_index=True)


def run_analyse(args, argv) -> int:
    df = prepare(load_context(args.day, args.cache_dir))
    meta = load_meta(args.day, args.cache_dir)
    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)
    tables = {
        "context_by_type.csv": context_by_type(df),
        "sequence_by_flight.csv": sequence_by_flight(df),
        "candidate_rules.csv": candidate_rules(df),
        "examples.csv": examples(df),
    }
    rec = df[df["type"].isin(RECORDED)]
    inputs = {
        "day": args.day,
        "events": int(len(df)),
        "tracks": int(df["track_id"].nunique()),
        "recorded_type_events": int(len(rec)),
        "with_record": int(rec["has_record"].sum()),
        "confirmed": int(rec["confirmed"].sum()),
        "records_on_day": meta.get("records_on_day", {}),
        "window_s": meta.get("window_s"),
    }
    for name, table in tables.items():
        table.to_csv(out / name, index=False)
        provenance.record(
            out,
            name,
            "benchmarks/milestone_plausibility.py",
            argv,
            [REPO / p for p in _CODE_PATHS],
            inputs={**inputs, "rows": int(len(table))},
            input_tables=[EVENTS, FLIGHT_LIST, f"{TRACKS}/dof={args.day}", AIRPORTS],
            notes="Context from the working table " + scratch_path(args.day),
        )
        print(f"wrote {out / name} ({len(table)} rows)")
    print(json.dumps(inputs, indent=1, default=str))
    return 0


# ---------------------------------------------------------------------------
# Before and after the checks (Spark, on the cluster)
# ---------------------------------------------------------------------------

#: The events of step 04 run with the checks and the two detector corrections
#: (``benchmarks/run_plausibility_day.py``).
AFTER_ROOT = f"{SCRATCH}/after"
AFTER_EVENTS = f"{AFTER_ROOT}/opdi_flight_events"
AFTER_REJECTED = f"{AFTER_ROOT}/opdi_flight_events_rejected"
#: The two off-block times confirmed by the records that the checks of Task 1
#: would remove (ENGM, LDZA).
KNOWN_CASES = ("72093ba15069144d_1781424689", "b639fcfdfe5eef4d_1781472634")
#: Types whose rows carry a ``direction`` in ``info``: the crossings.
CROSSING_PREFIX = "xing-"


def _records(spark, day: str):
    """The airport records of the four milestones, for the day and the days either side."""
    import events_gt
    from pyspark.sql import functions as F

    events_gt.assert_utc_session(spark)
    spec = events_gt.PERIODS["2026"]
    apdf = events_gt.load_apdf(spark, spec["month"], full=False)
    flights = events_gt.load_flights(spark, spec["month"])
    d = F.lit(day).cast("date")
    days = [str(x) for x in spark.range(1).select(F.date_sub(d, 1), d, F.date_add(d, 1)).first()]
    return events_gt.milestones(events_gt.bridge(apdf, flights), days, "all").select(
        F.col("icao24").alias("_g_icao24"),
        F.col("callsign").alias("_g_callsign"),
        F.col("milestone").alias("_g_type"),
        F.col("gt_time"),
        F.col("gt_airport"),
    )


def _scored(events, flights, truth):
    """Events of the four recorded types with the nearest record of their flight.

    Adds ``error_s`` (event minus record, NULL without a record) and the
    record's key (``_g_*``, ``gt_time``).
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    ev = events.filter(F.col("type").isin(*RECORDED)).withColumn(
        "_eid", F.monotonically_increasing_id()
    )
    ev = ev.join(flights, F.col("flight_id") == F.col("_fl_id"), "left").drop("_fl_id")
    ev = ev.cache()
    rec = ev.join(
        truth,
        (F.col("icao24") == F.col("_g_icao24"))
        & (F.col("callsign") == F.col("_g_callsign"))
        & (F.col("type") == F.col("_g_type")),
        "inner",
    ).withColumn("error_s", F.col("event_time").cast("double") - F.col("gt_time").cast("double"))
    rec = rec.filter(F.abs(F.col("error_s")) <= F.lit(RECORD_SEARCH_S))
    best = Window.partitionBy("_eid").orderBy(F.abs(F.col("error_s")).asc())
    rec = (
        rec.withColumn("_r", F.row_number().over(best))
        .filter(F.col("_r") == 1)
        .select("_eid", "_g_icao24", "_g_callsign", "gt_time", "gt_airport", "error_s")
    )
    return ev.join(rec, "_eid", "left")


def _record_summary(scored, truth_day):
    """Per type: events, with a record, confirmed, shares, 90th percentile, coverage."""
    from pyspark.sql import functions as F

    a = F.abs(F.col("error_s"))
    per_event = scored.groupBy("type").agg(
        F.count(F.lit(1)).alias("events"),
        F.count("error_s").alias("events_with_record"),
        F.sum(F.when(a <= CONFIRMED_S, 1).otherwise(0)).alias("confirmed_60s"),
        F.sum(F.when(a <= FAR_S, 1).otherwise(0)).alias("within_300s"),
        F.percentile_approx(a, 0.9, 100000).alias("p90_abs_error_s"),
        F.percentile_approx(F.when(a <= CONFIRMED_S, F.col("error_s")), 0.5, 100000).alias(
            "median_error_confirmed_s"
        ),
    )
    found = (
        scored.filter(F.col("error_s").isNotNull())
        .select("type", "_g_icao24", "_g_callsign", "gt_time", (a <= CONFIRMED_S).alias("_c"))
        .groupBy("type", "_g_icao24", "_g_callsign", "gt_time")
        .agg(F.max("_c").alias("_c"))
        .join(
            truth_day.select(
                F.col("_g_type").alias("type"), "_g_icao24", "_g_callsign", "gt_time"
            ).distinct(),
            ["type", "_g_icao24", "_g_callsign", "gt_time"],
            "inner",
        )
        .groupBy("type")
        .agg(
            F.count(F.lit(1)).alias("records_found"),
            F.sum(F.col("_c").cast("int")).alias("records_confirmed"),
        )
    )
    on_day = truth_day.groupBy(F.col("_g_type").alias("type")).agg(
        F.countDistinct("_g_icao24", "_g_callsign", "gt_time").alias("records_on_day")
    )
    return {
        r["type"]: r.asDict()
        for r in per_event.join(found, "type", "left").join(on_day, "type", "left").collect()
    }


def _type_counts(events):
    """Per type: events, events without a time, and crossings sharing their time."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    ev = events.withColumn("_dir", F.get_json_object(F.col("info"), "$.direction"))
    same = Window.partitionBy("flight_id", "type", "event_time")
    ev = ev.withColumn(
        "_n_same",
        F.when(F.col("event_time").isNotNull(), F.count(F.lit(1)).over(same)),
    ).withColumn(
        "_dirs_same",
        F.when(F.col("event_time").isNotNull(), F.size(F.collect_set("_dir").over(same))),
    )
    xing = F.col("type").startswith(CROSSING_PREFIX)
    rows = ev.groupBy("type").agg(
        F.count(F.lit(1)).alias("events"),
        F.sum(F.col("event_time").isNull().cast("int")).alias("without_time"),
        F.sum(F.when(xing & (F.col("_n_same") > 1), 1).otherwise(0)).alias(
            "crossings_sharing_time"
        ),
        F.sum(F.when(xing & (F.col("_dirs_same") > 1), 1).otherwise(0)).alias(
            "crossings_sharing_time_other_direction"
        ),
    )
    return {r["type"]: r.asDict() for r in rows.collect()}


def _null_fate(before, after):
    """Of the events without a time before: how many have a time after, how many are gone.

    Counted per (track, type, direction, airport): the crossings that happen are
    the same before and after, so a group that has fewer events after than
    before (with and without time) lost the ones that could not be placed.
    """
    from pyspark.sql import functions as F

    def keyed(df):
        return df.select(
            "flight_id",
            "type",
            F.coalesce(F.get_json_object("info", "$.direction"), F.lit("")).alias("_dir"),
            F.coalesce(F.get_json_object("info", "$.apt_icao"), F.lit("")).alias("_apt"),
            F.col("event_time").isNull().alias("_null"),
        )

    key = ["flight_id", "type", "_dir", "_apt"]
    b = (
        keyed(before)
        .groupBy(*key)
        .agg(F.sum(F.col("_null").cast("int")).alias("_b_null"), F.count(F.lit(1)).alias("_b_all"))
    )
    b = b.filter(F.col("_b_null") > 0)
    a = keyed(after).filter(~F.col("_null")).groupBy(*key).agg(F.count(F.lit(1)).alias("_a"))
    j = b.join(a, key, "left").withColumn("_a", F.coalesce("_a", F.lit(0)))
    j = j.withColumn(
        "_gone", F.least(F.col("_b_null"), F.greatest(F.col("_b_all") - F.col("_a"), F.lit(0)))
    )
    rows = j.groupBy("type").agg(
        F.sum("_b_null").alias("null_before"),
        F.sum(F.col("_b_null") - F.col("_gone")).alias("null_now_timed"),
        F.sum("_gone").alias("null_not_emitted"),
    )
    return {r["type"]: r.asDict() for r in rows.collect()}


def _kept_in_place(before_scored, after):
    """Per type: confirmed events before, and how many have the same time after."""
    from pyspark.sql import functions as F

    conf = before_scored.filter(F.abs(F.col("error_s")) <= CONFIRMED_S).select(
        "flight_id", "type", "event_time"
    )
    there = after.select("flight_id", "type", "event_time").distinct().withColumn("_x", F.lit(1))
    rows = (
        conf.join(there, ["flight_id", "type", "event_time"], "left")
        .groupBy("type")
        .agg(
            F.count(F.lit(1)).alias("confirmed_before"),
            F.sum(F.coalesce("_x", F.lit(0))).alias("confirmed_before_same_time_after"),
        )
    )
    return {r["type"]: r.asDict() for r in rows.collect()}


#: A block time counts as placed in flight when the aircraft is more than
#: this high above the airport and faster than :data:`IN_FLIGHT_SPEED_KT` at
#: the event (the measure of the context study, ``context_by_type.csv``).
IN_FLIGHT_HEIGHT_FT = 500.0
IN_FLIGHT_SPEED_KT = 80.0


def _in_flight(spark, day, sides):
    """Per side and block type: events, events with known context, events in flight.

    ``sides`` maps a name to a frame of events (``flight_id``, ``type``,
    ``event_time``). The context is that of :func:`event_context` within
    60 s, measured above the departure airport (``AOBT``) or the destination
    (``AIBT``) of the flight list, as in the context study: the stand airport
    a block time names is not used, so that both sides are measured alike.
    """
    from pyspark.sql import functions as F

    d = F.lit(day).cast("date")
    frames = None
    for name, df in sides.items():
        part = df.filter(F.col("type").isin("AOBT", "AIBT")).select(
            F.lit(name).alias("side"),
            F.col("flight_id").alias("track_id"),
            "type",
            "event_time",
        )
        frames = part if frames is None else frames.unionByName(part)
    fl = (
        spark.read.parquet(FLIGHT_LIST)
        .filter(F.col("DOF") == d)
        .select(
            F.col("ID").alias("_fl_id"), F.col("ADEP").alias("adep"), F.col("ADES").alias("ades")
        )
        .dropDuplicates(["_fl_id"])
    )
    ev = frames.join(fl, F.col("track_id") == F.col("_fl_id"), "left").drop("_fl_id")
    tracks = (
        spark.read.parquet(TRACKS)
        .filter(F.col("dof") == d)
        .select(
            "track_id",
            "event_time",
            "lat",
            "lon",
            "baro_altitude_c",
            "velocity",
            "vert_rate",
            "on_ground",
        )
    )
    ctx = event_context(ev, tracks, spark.read.parquet(AIRPORTS), 60.0)
    known = F.col("ctx_height_ft").isNotNull() & F.col("ctx_speed_kt").isNotNull()
    flying = (
        known
        & (F.col("ctx_height_ft") > F.lit(IN_FLIGHT_HEIGHT_FT))
        & (F.col("ctx_speed_kt") > F.lit(IN_FLIGHT_SPEED_KT))
    )
    rows = ctx.groupBy("side", "type").agg(
        F.count(F.lit(1)).alias("events"),
        F.sum(known.cast("int")).alias("known_context"),
        F.sum(flying.cast("int")).alias("in_flight"),
    )
    return {(r["side"], r["type"]): r.asDict() for r in rows.collect()}


def run_before_after(args, argv) -> int:
    """Score the events before and after the checks against the airport records."""
    import pandas as pd
    from pyspark.sql import functions as F
    from run_airspace_events_day import study_session

    spark = study_session("milestone plausibility before/after", executors=args.executors)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", "400")
    d = F.lit(args.day).cast("date")
    before = spark.read.parquet(EVENTS).filter(F.col("dof") == d)
    after = spark.read.parquet(AFTER_EVENTS).filter(F.col("dof") == d)
    rejected = spark.read.parquet(AFTER_REJECTED).filter(F.col("dof") == d)
    flights = (
        spark.read.parquet(FLIGHT_LIST)
        .filter(F.col("DOF") == d)
        .select(
            F.col("ID").alias("_fl_id"),
            F.lower(F.col("ICAO24")).alias("icao24"),
            F.trim(F.col("FLT_ID")).alias("callsign"),
        )
    )
    truth = _records(spark, args.day).cache()
    truth_day = truth.filter(F.to_date("gt_time") == d).filter(F.col("gt_airport").isNotNull())

    b_sc, a_sc = _scored(before, flights, truth), _scored(after, flights, truth)
    stats = {
        "before": _record_summary(b_sc, truth_day),
        "after": _record_summary(a_sc, truth_day),
    }
    counts = {"before": _type_counts(before), "after": _type_counts(after)}
    fate = _null_fate(before, after)
    in_place = _kept_in_place(b_sc, after)

    types = sorted(set(counts["before"]) | set(counts["after"]))
    rows = []
    for t in types:
        row = {"type": t}
        for side in ("before", "after"):
            c = counts[side].get(t, {})
            row[f"events_{side}"] = c.get("events", 0)
            row[f"without_time_{side}"] = c.get("without_time", 0)
            row[f"crossings_sharing_time_{side}"] = c.get("crossings_sharing_time", 0)
            row[f"crossings_sharing_time_other_direction_{side}"] = c.get(
                "crossings_sharing_time_other_direction", 0
            )
        f = fate.get(t, {})
        row["null_before"] = f.get("null_before", 0)
        row["null_now_timed"] = f.get("null_now_timed", 0)
        row["null_not_emitted"] = f.get("null_not_emitted", 0)
        if t in RECORDED:
            for side in ("before", "after"):
                s = stats[side].get(t, {})
                n_rec = s.get("events_with_record") or 0
                row[f"with_record_{side}"] = n_rec
                row[f"confirmed_60s_{side}"] = s.get("confirmed_60s") or 0
                row[f"share_60s_{side}_pct"] = (
                    round(100.0 * (s.get("confirmed_60s") or 0) / n_rec, 2) if n_rec else None
                )
                row[f"share_300s_{side}_pct"] = (
                    round(100.0 * (s.get("within_300s") or 0) / n_rec, 2) if n_rec else None
                )
                row[f"p90_abs_error_s_{side}"] = s.get("p90_abs_error_s")
                row[f"median_error_confirmed_s_{side}"] = s.get("median_error_confirmed_s")
                row[f"records_on_day_{side}"] = s.get("records_on_day")
                row[f"records_found_{side}"] = s.get("records_found")
                row[f"records_confirmed_{side}"] = s.get("records_confirmed")
                on_day = s.get("records_on_day") or 0
                row[f"coverage_{side}_pct"] = (
                    round(100.0 * (s.get("records_found") or 0) / on_day, 2) if on_day else None
                )
            k = in_place.get(t, {})
            row["confirmed_before_same_time_after"] = k.get("confirmed_before_same_time_after")
        rows.append(row)
    table = pd.DataFrame(rows)

    # Block times placed in flight: before (the code before the checks and
    # the detector corrections), as the detector gives them now (kept and
    # removed together), and as published (kept).
    detector = after.select("flight_id", "type", "event_time").unionByName(
        rejected.select("flight_id", "type", "event_time")
    )
    flying = _in_flight(spark, args.day, {"before": before, "detector": detector, "after": after})
    in_flight_rows = []
    for t in ("AOBT", "AIBT"):
        row = {"type": t}
        for side in ("before", "detector", "after"):
            f = flying.get((side, t), {})
            n, k, a = f.get("events", 0), f.get("known_context", 0), f.get("in_flight", 0)
            row[f"events_{side}"] = n
            row[f"known_context_{side}"] = k
            row[f"in_flight_{side}"] = a
            row[f"in_flight_{side}_pct"] = round(100.0 * a / k, 2) if k else None
        in_flight_rows.append(row)
    in_flight_table = pd.DataFrame(in_flight_rows)

    # Every event that a record confirms before and that is not at the same
    # time after, to be read one by one.
    lost = (
        b_sc.filter(F.abs(F.col("error_s")) <= CONFIRMED_S)
        .select("flight_id", "type", "event_time", "error_s", "gt_airport")
        .join(
            after.select("flight_id", "type", "event_time").distinct().withColumn("_x", F.lit(1)),
            ["flight_id", "type", "event_time"],
            "left",
        )
        .filter(F.col("_x").isNull())
        .drop("_x")
    )
    lost_confirmed = [
        f"lost confirmed: {r['flight_id']} {r['type']} {r['event_time']} record at "
        f"{r['gt_airport']} error {r['error_s']:.0f} s"
        for r in lost.orderBy("type", "flight_id").limit(1000).collect()
    ]

    r_sc = _scored(rejected, flights, truth)
    a = F.abs(F.col("error_s"))
    rec_part = r_sc.groupBy("reject_rule", "type").agg(
        F.count("error_s").alias("removed_with_record"),
        F.sum(F.when(a <= CONFIRMED_S, 1).otherwise(0)).alias("removed_confirmed_60s"),
        F.sum(F.when(a > FAR_S, 1).otherwise(0)).alias("removed_far_300s"),
    )
    removed = (
        rejected.groupBy("reject_rule", "type")
        .agg(F.count(F.lit(1)).alias("removed"))
        .join(rec_part, ["reject_rule", "type"], "left")
        .toPandas()
        .sort_values(["reject_rule", "type"])
    )
    for c in ("removed_with_record", "removed_confirmed_60s", "removed_far_300s"):
        removed[c] = removed[c].fillna(0).astype(int)
    total = {
        "reject_rule": "ALL",
        "type": "ALL",
        **{c: int(removed[c].sum()) for c in removed.columns if c.startswith("removed")},
    }
    removed = pd.concat([removed, pd.DataFrame([total])], ignore_index=True)

    # Every removed event that a record confirms, to be read one by one.
    confirmed_removed = [
        f"removed confirmed: {r['reject_rule']} {r['flight_id']} {r['type']} "
        f"{r['event_time']} record {r['gt_time']} at {r['gt_airport']} "
        f"error {r['error_s']:.0f} s detail {r['reject_detail']}"
        for r in r_sc.filter(a <= CONFIRMED_S)
        .select(
            "reject_rule",
            "flight_id",
            "type",
            "event_time",
            "gt_time",
            "gt_airport",
            "error_s",
            "reject_detail",
        )
        .orderBy("reject_rule", "type", "flight_id")
        .limit(500)
        .collect()
    ]
    known = []
    for name, df in (("before", before), ("after", after), ("rejected", rejected)):
        for r in (
            df.filter(F.col("flight_id").isin(*KNOWN_CASES))
            .filter(F.col("type").isin("AOBT", "AIBT", "ATOT", "ALDT"))
            .select("flight_id", "type", "event_time", "info")
            .orderBy("flight_id", "event_time")
            .collect()
        ):
            known.append(f"{name}: {r['flight_id']} {r['type']} {r['event_time']} {r['info']}")
    spark.stop()

    out = Path(args.results_dir)
    out.mkdir(parents=True, exist_ok=True)
    inputs = {
        "day": args.day,
        "known_cases": known,
        "confirmed_removed": confirmed_removed,
        "lost_confirmed": lost_confirmed,
        "in_flight_height_ft": IN_FLIGHT_HEIGHT_FT,
        "in_flight_speed_kt": IN_FLIGHT_SPEED_KT,
    }
    for name, frame in (
        ("before_after.csv", table),
        ("removed_by_rule.csv", removed),
        ("in_flight_before_after.csv", in_flight_table),
    ):
        frame.to_csv(out / name, index=False)
        provenance.record(
            out,
            name,
            "benchmarks/milestone_plausibility.py",
            argv,
            [REPO / p for p in _CODE_PATHS + _AFTER_CODE_PATHS],
            inputs={**inputs, "rows": int(len(frame))},
            input_tables=[EVENTS, AFTER_EVENTS, AFTER_REJECTED, FLIGHT_LIST, TRACKS],
            notes=(
                "Before: the events of research/airspace_events/on. After: step 04 run by "
                "benchmarks/run_plausibility_day.py under " + AFTER_ROOT + "."
            ),
        )
        print(f"wrote {out / name} ({len(frame)} rows)")
    print(table.to_string())
    print(removed.to_string())
    print(in_flight_table.to_string())
    print("\n".join(known))
    print("\n".join(confirmed_removed))
    print("\n".join(lost_confirmed))
    return 0


#: The code the "after" events depend on, besides :data:`_CODE_PATHS`.
_AFTER_CODE_PATHS = [
    "benchmarks/run_plausibility_day.py",
    "src/opdi/pipeline/events.py",
    "src/opdi/pipeline/ground.py",
    "src/opdi/pipeline/crossings.py",
    "src/opdi/pipeline/runway_ops.py",
    "src/opdi/config/events.py",
]


def run_clean_after(args) -> int:
    """Delete the events of the "after" run, object by object."""
    import boto3

    import opdi.config.dotenv  # noqa: F401  (reads .env into the environment)

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("OPDI_S3_ENDPOINT", "https://s3.opensky-network.org"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    check_write_path(AFTER_ROOT + "/x")
    bucket, _, key = AFTER_ROOT.replace("s3a://", "").partition("/")
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=key + "/"):
        for o in page.get("Contents", []):
            s3.delete_object(Bucket=bucket, Key=o["Key"])
            n += 1
    print(f"deleted {AFTER_ROOT} ({n} objects)")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("context")
    c.add_argument("--day", required=True)
    c.add_argument("--window-s", type=float, default=60.0)
    c.add_argument("--executors", type=int, default=8)
    a = sub.add_parser("analyse")
    a.add_argument("--day", required=True)
    a.add_argument("--results-dir", required=True)
    a.add_argument("--cache-dir", default=None)
    k = sub.add_parser("clean")
    k.add_argument("--day", required=True)
    b = sub.add_parser("before-after")
    b.add_argument("--day", required=True)
    b.add_argument("--results-dir", required=True)
    b.add_argument("--executors", type=int, default=8)
    sub.add_parser("clean-after")
    args = ap.parse_args(argv)
    if args.cmd == "context":
        return run_context(args)
    if args.cmd == "analyse":
        return run_analyse(args, list(sys.argv[1:] if argv is None else argv))
    if args.cmd == "clean":
        return run_clean(args)
    if args.cmd == "before-after":
        return run_before_after(args, list(sys.argv[1:] if argv is None else argv))
    if args.cmd == "clean-after":
        return run_clean_after(args)
    return 1


def run_clean(args) -> int:
    """Delete the working tables of ``day``, object by object."""
    import boto3

    import opdi.config.dotenv  # noqa: F401  (reads .env into the environment)

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("OPDI_S3_ENDPOINT", "https://s3.opensky-network.org"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    for p in (scratch_path(args.day), scratch_path(args.day) + "_meta"):
        check_write_path(p)
        bucket, _, key = p.replace("s3a://", "").partition("/")
        n = 0
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=key + "/"):
            for o in page.get("Contents", []):
                s3.delete_object(Bucket=bucket, Key=o["Key"])
                n += 1
        print(f"deleted {p} ({n} objects)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
