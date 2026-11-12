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

#: Storage is SI; these are the conversions ``events.py`` uses.
FT_PER_M = 3.28084
KT_PER_MPS = 1.94384
FTMIN_PER_MPS = 196.850394

#: Events that happen at the departure airport. Their height is measured above
#: the flight's departure airport unless the event names an airport itself.
DEPARTURE_SIDE = ("AOBT", "off-block", "line-up", "take-off-roll", "ATOT", "airborne", "take-off")
#: Events that happen at the destination airport.
ARRIVAL_SIDE = (
    "AIBT",
    "on-block",
    "ALDT",
    "touchdown",
    "landing",
    "runway-vacated",
    "go-around",
)

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
    "src/opdi/utils/geometry.py",
]


def scratch_path(day: str) -> str:
    """The working table of one day."""
    return f"{SCRATCH}/context_{day}"


def _guard(path: str) -> None:
    if not path.startswith(SCRATCH + "/"):
        raise AssertionError(f"refusing to write {path!r}: this study writes under {SCRATCH}")


# ---------------------------------------------------------------------------
# Context of an event
# ---------------------------------------------------------------------------


def event_context(events, tracks, airports, window_s: float = 60.0):
    """Add to each event the state of the aircraft at the event's time.

    Args:
        events: one row per event with ``event_id`` (unique), ``track_id``,
            ``type`` and ``event_time``, and optionally ``apt_icao`` (the
            airport the event names), ``adep`` and ``ades`` (the flight's
            departure and destination airports).
        tracks: position reports with ``track_id``, ``event_time``, ``lat``,
            ``lon``, ``baro_altitude_c`` (m), ``velocity`` (m/s), ``vert_rate``
            (m/s) and optionally ``on_ground``.
        airports: ``ident``, ``latitude_deg``, ``longitude_deg``,
            ``elevation_ft`` (the OurAirports table).
        window_s: only reports within this many seconds of the event are read.

    Returns:
        ``events`` with these columns added, each NULL when unknown:

        * ``ctx_gap_s`` -- seconds to the nearest report of the track;
        * ``ctx_altitude_ft`` -- barometric altitude of the nearest report that
          has one;
        * ``ctx_height_ft`` -- that altitude minus the elevation of the event's
          airport (``ctx_airport``);
        * ``ctx_speed_kt``, ``ctx_vertical_rate_ft_min`` -- ground speed and
          vertical rate of the nearest report that has them;
        * ``ctx_on_ground`` -- share of the reports in the window flagged on the
          ground (0 to 1);
        * ``ctx_lat``, ``ctx_lon`` -- position of the nearest report with one;
        * ``ctx_airport`` -- the airport the event belongs to: the one it names,
          else the departure airport for a departure-side type
          (:data:`DEPARTURE_SIDE`), the destination for an arrival-side type
          (:data:`ARRIVAL_SIDE`), and for every other type the nearer of the
          two to the aircraft;
        * ``ctx_airport_elevation_ft``, ``ctx_airport_distance_nm``.

    When two reports are equally near, the earlier one is used.
    """
    from pyspark.sql import functions as F

    from opdi.utils.geometry import haversine_nm

    w = float(window_s)
    ev = events
    for c in ("apt_icao", "adep", "ades"):
        if c not in ev.columns:
            ev = ev.withColumn(c, F.lit(None).cast("string"))

    # Reports are joined through time buckets one window wide: an event meets
    # the reports of its own bucket and of the two next to it, which holds
    # every report within the window and few others.
    probe = ev.select(
        F.col("event_id").alias("_eid"),
        F.col("track_id").alias("_etrack"),
        F.col("event_time").cast("double").alias("_et"),
    ).withColumn("_eb0", F.floor(F.col("_et") / F.lit(w)))
    probe = probe.withColumn(
        "_eb", F.explode(F.array(F.col("_eb0") - 1, F.col("_eb0"), F.col("_eb0") + 1))
    ).drop("_eb0")

    on_ground = (
        F.col("on_ground").cast("double")
        if "on_ground" in tracks.columns
        else F.lit(None).cast("double")
    )
    rep = tracks.select(
        F.col("track_id").alias("_rtrack"),
        F.col("event_time").cast("double").alias("_rt"),
        F.col("lat").alias("_lat"),
        F.col("lon").alias("_lon"),
        F.col("baro_altitude_c").alias("_alt_m"),
        F.col("velocity").alias("_gs_ms"),
        F.col("vert_rate").alias("_vr_ms"),
        on_ground.alias("_gnd"),
    ).withColumn("_rb", F.floor(F.col("_rt") / F.lit(w)))

    pairs = probe.join(
        rep, (probe["_etrack"] == rep["_rtrack"]) & (probe["_eb"] == rep["_rb"]), "inner"
    )
    pairs = pairs.withColumn("_dt", F.col("_rt") - F.col("_et")).filter(
        F.abs(F.col("_dt")) <= F.lit(w)
    )
    # Nearest first; a report after the event loses a tie to one before it.
    pairs = pairs.withColumn(
        "_key", F.abs(F.col("_dt")) + F.when(F.col("_dt") > 0, F.lit(1e-3)).otherwise(F.lit(0.0))
    )

    def nearest(value, *needed):
        known = F.lit(True)
        for c in needed:
            known = known & F.col(c).isNotNull()
        return F.min_by(value, F.when(known, F.col("_key")))

    agg = pairs.groupBy("_eid").agg(
        F.min(F.abs(F.col("_dt"))).alias("ctx_gap_s"),
        nearest(F.col("_alt_m"), "_alt_m").alias("_alt_m"),
        nearest(F.col("_gs_ms"), "_gs_ms").alias("_gs_ms"),
        nearest(F.col("_vr_ms"), "_vr_ms").alias("_vr_ms"),
        nearest(F.struct(F.col("_lat"), F.col("_lon")), "_lat", "_lon").alias("_pos"),
        F.avg(F.col("_gnd")).alias("ctx_on_ground"),
    )

    out = ev.join(agg, ev["event_id"] == agg["_eid"], "left").drop("_eid")
    out = (
        out.withColumn("ctx_altitude_ft", F.col("_alt_m") * F.lit(FT_PER_M))
        .withColumn("ctx_speed_kt", F.col("_gs_ms") * F.lit(KT_PER_MPS))
        .withColumn("ctx_vertical_rate_ft_min", F.col("_vr_ms") * F.lit(FTMIN_PER_MPS))
        .withColumn("ctx_lat", F.col("_pos._lat"))
        .withColumn("ctx_lon", F.col("_pos._lon"))
        .drop("_alt_m", "_gs_ms", "_vr_ms", "_pos")
    )

    apt = airports.select(
        F.col("ident"),
        F.col("latitude_deg").cast("double").alias("_alat"),
        F.col("longitude_deg").cast("double").alias("_alon"),
        F.col("elevation_ft").cast("double").alias("_aelev"),
    )

    def airport(prefix):
        return F.broadcast(
            apt.select(
                F.col("ident").alias(f"{prefix}_ident"),
                F.col("_alat").alias(f"{prefix}_lat"),
                F.col("_alon").alias(f"{prefix}_lon"),
            )
        )

    out = out.join(airport("_dep"), F.col("adep") == F.col("_dep_ident"), "left")
    out = out.join(airport("_arr"), F.col("ades") == F.col("_arr_ident"), "left")
    d_dep = haversine_nm(F.col("ctx_lat"), F.col("ctx_lon"), F.col("_dep_lat"), F.col("_dep_lon"))
    d_arr = haversine_nm(F.col("ctx_lat"), F.col("ctx_lon"), F.col("_arr_lat"), F.col("_arr_lon"))
    nearer = (
        F.when(d_dep.isNull(), F.when(d_arr.isNotNull(), F.col("ades")))
        .when(d_arr.isNull(), F.col("adep"))
        .when(d_dep <= d_arr, F.col("adep"))
        .otherwise(F.col("ades"))
    )
    out = out.withColumn(
        "ctx_airport",
        F.coalesce(
            F.col("apt_icao"),
            F.when(F.col("type").isin(*DEPARTURE_SIDE), F.col("adep"))
            .when(F.col("type").isin(*ARRIVAL_SIDE), F.col("ades"))
            .otherwise(nearer),
        ),
    ).drop("_dep_ident", "_dep_lat", "_dep_lon", "_arr_ident", "_arr_lat", "_arr_lon")

    own = F.broadcast(
        apt.select(
            F.col("ident").alias("_own_ident"),
            F.col("_alat").alias("_own_lat"),
            F.col("_alon").alias("_own_lon"),
            F.col("_aelev").alias("ctx_airport_elevation_ft"),
        )
    )
    out = out.join(own, F.col("ctx_airport") == F.col("_own_ident"), "left")
    out = out.withColumn(
        "ctx_height_ft", F.col("ctx_altitude_ft") - F.col("ctx_airport_elevation_ft")
    ).withColumn(
        "ctx_airport_distance_nm",
        haversine_nm(F.col("ctx_lat"), F.col("ctx_lon"), F.col("_own_lat"), F.col("_own_lon")),
    )
    return out.drop("_own_ident", "_own_lat", "_own_lon")


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
    _guard(out)
    ctx, truth = build_context(spark, args.day, args.window_s)
    ctx.repartition(24).write.mode("overwrite").parquet(out)
    n_truth = (
        truth.filter(F.to_date("gt_time") == F.lit(args.day).cast("date"))
        .groupBy("_g_type")
        .count()
    )
    stats = {r["_g_type"]: r["count"] for r in n_truth.collect()}
    meta = {"day": args.day, "window_s": args.window_s, "records_on_day": stats}
    _guard(out + "_meta")
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
RUNWAY_TIMES = ("ATOT", "ALDT", "airborne", "touchdown", "landing")
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
        ("landing", "max"),
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
    ("threshold_to_landing", "landing", "ALDT"),
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
    args = ap.parse_args(argv)
    if args.cmd == "context":
        return run_context(args)
    if args.cmd == "analyse":
        return run_analyse(args, list(sys.argv[1:] if argv is None else argv))
    if args.cmd == "clean":
        return run_clean(args)
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
        _guard(p)
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
