#!/usr/bin/env python
"""Did keeping one track across a hole help the flight list, and what did it damage?

Stage 2 of the ``cruise_linked`` study (spec 2026-11-03). One sampled day went
through the real pipeline for both arms (``run_track_link_day.py``); this scores
them against each other. Tables are read and nothing is written to S3.

**Who is scored.** Flights that take off and land on the day, from Network Manager,
on the sampled airframes. Only one day was built, so the arms differ at both ends of
it: A10 moves an overnight piece to the day its track started, and picks up pieces
after midnight that A9 gives to the next day. Flights inside the day are complete in
both. A hole counts as scored when both edges lie inside such a flight; the holes
that do not are counted in the level table, not hidden.

``link_flight_list.csv``, ``link_both_ends.csv``
    The owner's goal. ``adsc_flight_list_truth``'s scorer, per flight and paired, so
    the result is a count of flights that changed answer. ``both_ends`` also has a
    row for the flights crossing the European box, ``crosses_the_box``, which is the
    population the rule is for.

``link_events.csv``
    Events per type in each arm, on airframes with at least one linked track, inside
    a scored flight give or take the flight list's slack. A linked track is one whose
    ``track_id`` under A10 spans a gap over ``gap_minutes``. Detectors have never seen
    such a track, and this is the first look at what they make of one.

``link_level_across_hole.csv``
    A level segment across a hole: the last level event at or before the hole is a
    start and the first at or after it is an end. A start somewhere before and an end
    somewhere after is not enough, as that holds for nearly any track with two
    segments. ``level-off-*`` is the same question for the other level family and has
    its own row, and ``any`` counts a track damaged in either once. The share of tracks
    on the ``any`` row is what the paper's withdrawal condition reads.

``link_distance_across_hole.csv``
    Whether ``cumulative_distance_nm`` carries across a hole as the great circle
    between its edges. ``opdi_measurements`` holds that value only at events, and no
    event sits on a hole's edge, so it cannot answer hole by hole. The jump is taken
    instead from ``events.add_distance_measure`` rerun on the arm's cleaned table,
    which is what step 04 ran on it, and the rerun is checked against the stored value
    at each track's ``last_seen``. Edges are the nearest samples with a position. A
    masked edge keeps its row and that function reads it as zero distance, so a hole
    with one shows as a large difference and is flagged.

``link_partition_check.csv``
    Nothing else moved. The shared state vectors, cut to the window the A10 arm read,
    are segmented under both rules. Every A9 boundary missing from A10 must be a hole the
    rule links, and A10 must add none. Those two rules are applied here with the
    parameters given, so they alone could pass for a rule the arm never ran. So the arm's
    own tracks of the day must also be the re-segmented ones, by first and last sample,
    in both directions, and ``passed`` needs that too.

``link_movements.csv``
    APDF movements (ATOT, ALDT) of the scored flights with a detection in each arm within
    ``MOVEMENT_MATCH_MINUTES``, from ``events_gt`` truth and
    ``event_bench.detected_events``. The ``all`` row is the gate: no more than 0.1% lower.
    Scored per truth row with off-block in the key, and not through
    ``events_score.align``, which keeps one row per airframe, callsign, day and milestone.
    That makes two legs of one callsign one row, which either leg's detection answers at
    any time of day, and about 3% of the flights inside 2026-06-14 are such extra legs,
    thirty times the limit.

Verdicts on the withdrawal conditions are the reader's: this writes the numbers. A
failed check is a result, so it does not stop the run.

    export PYSPARK_DRIVER_PYTHON=$PWD/.venv310/bin/python
    unset PYSPARK_PYTHON    # executors use the opdi-spark image's python
    .venv310/bin/python -u benchmarks/track_link_pipeline_score.py \\
        --day 2026-06-14 --results-dir $OPDI_PAPER_DIR/data

Pass the ``--link-params-json`` the arm was run with, if any. The partition check applies
the rule again, and fails if the arm's tracks show it was run with something else.

``--truth`` is read on the driver, so it decides the flight-list truth and which flights
are scored. It does not reach the APDF bridge: executors cannot read the driver's files,
so ``events_gt`` bridges from its S3 mirror, and a flight in ``--truth`` with no row in
that mirror has no movement to count. Provenance names both.
"""

import argparse
import dataclasses
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Callable, Dict, List, Optional

import pandas as pd
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

import adsc_flight_list_truth as flight_list  # noqa: E402
import events_gt  # noqa: E402
import events_score  # noqa: E402
import provenance  # noqa: E402
from run_track_link_day import (  # noqa: E402
    ARMS,
    REFERENCE_WAREHOUSE,
    SHARED_INPUTS,
    arm_path,
    build_config,
)
from track_link_controls import real_hole_candidates  # noqa: E402
from track_link_grid import holes_inside_flights  # noqa: E402

from opdi.pipeline.events import add_distance_measure  # noqa: E402
from opdi.pipeline.flights import haversine_nm  # noqa: E402
from opdi.pipeline.segmentation import SegmentationParams, assign_track_id  # noqa: E402
from opdi.pipeline.segmentation.base import BreakRule  # noqa: E402
from opdi.pipeline.segmentation.methods import (  # noqa: E402
    callsign_memory_minutes,
    cruise_linked,
    debounced,
    debounced_callsign_change,
    legacy,
    link_feature_columns,
    linkable,
)
from opdi.pipeline.tracks import TrackProcessor, required_lookback_hours  # noqa: E402

OUTPUTS = (
    "link_flight_list.csv",
    "link_both_ends.csv",
    "link_events.csv",
    "link_level_across_hole.csv",
    "link_distance_across_hole.csv",
    "link_partition_check.csv",
    "link_movements.csv",
    "link_truth_all_flights.csv",
    "link_consistency.csv",
    "link_distance_no_hole.csv",
)

#: How far the movement count of A10 may fall below A9's. The spec's limit.
MOVEMENT_FALL_LIMIT = 0.001

#: How far from the truth movement a detection may be and still be that movement, in
#: minutes, either side. A detection hours away belongs to another leg.
MOVEMENT_MATCH_MINUTES = 30.0

#: One truth movement. Off-block keeps two legs of one callsign apart.
_MOVEMENT_KEY = ["icao24", "callsign", "gt_aobt", "milestone"]

#: The milestones that are movements. Block times are not: APDF counts a movement once.
MOVEMENTS = ("ATOT", "ALDT")

#: A difference this size means distance was lost or invented, not rounded.
DISTANCE_TOLERANCE_NM = 1.0

#: A track long enough for a share of its distance to mean something.
LONG_TRACK_NM = 200.0

#: The measurement type holding ``cumulative_distance_nm``. See ``events.py``.
DISTANCE_MEASUREMENT = "Distance flown (NM)"

_CROSSING_FLOWS = ("europe_to_outside", "outside_to_europe")

#: The paired scorer's generic arm names, and this study's.
_ARM_WORDS = {"base": "debounced", "merged": "cruise_linked"}

_HOLE_KEY = ["track_id", "t_old_edge", "t_new_edge"]

_CODE_PATHS = [
    "benchmarks/track_link_pipeline_score.py",
    "benchmarks/adsc_flight_list_truth.py",
    "benchmarks/run_track_link_day.py",
    "benchmarks/track_link_controls.py",
    "benchmarks/track_link_grid.py",
    "benchmarks/events_gt.py",
    "benchmarks/events_score.py",
    "benchmarks/event_bench.py",
    "src/opdi/pipeline/events.py",
    "src/opdi/pipeline/holes.py",
    "src/opdi/pipeline/flights.py",
    "src/opdi/pipeline/segmentation/__init__.py",
    "src/opdi/pipeline/segmentation/base.py",
    "src/opdi/pipeline/segmentation/methods.py",
]

_NOTES = (
    "One day through the real pipeline, one airframe in ten. Flights that take off and "
    "land on the day; Network Manager and APDF truth, European. In "
    "link_truth_all_flights.csv the landing_on_older, take_off_on_newer and both counts are "
    "lower bounds, as pieces starting after midnight are not in this one-day run; its labels "
    "are unaffected."
)


def _input_tables(month: str) -> List[str]:
    """Every table the job reads. A prefix over several would hide which one moved.

    Both APDF names are listed because ``load_apdf`` falls back from one to the other, so
    the one with no objects recorded is the one that was not read.
    """
    per_arm = ("osn_tracks_clean", "opdi_flight_list", "opdi_flight_events", "opdi_measurements")
    base = events_gt.REFERENCE_BASE
    return [
        arm_path(ARMS[0], "osn_statevectors_v2"),
        *[arm_path(arm, table) for arm in ARMS for table in per_arm],
        f"{REFERENCE_WAREHOUSE}/oa_airports",
        f"{base}/flights_{month}.parquet",
        f"{base}/apdf_full_{month}.parquet",
        f"{base}/apdf_{month}.parquet",
    ]


# --- who is scored -----------------------------------------------------------


def inside_the_day(truth: pd.DataFrame, day: str) -> pd.DataFrame:
    """The flights with both ``t_off`` and ``t_on`` on *day*."""
    on_day = (truth["t_off"].dt.strftime("%Y-%m-%d") == day) & (
        truth["t_on"].dt.strftime("%Y-%m-%d") == day
    )
    return truth[on_day].reset_index(drop=True)


def scored_flights_frame(spark, truth: pd.DataFrame) -> DataFrame:
    """Truth flights as a frame of ``flight_key``, ``icao24``, ``t_off``, ``t_land``.

    Sent as text and parsed in the session's zone, because a naive ``datetime`` handed
    to ``createDataFrame`` is read in the driver's zone instead.
    """
    return _with_parsed_times(
        spark.createDataFrame(
            [
                (int(r.flight_key), r.icao24, _stamp(r.t_off), _stamp(r.t_on))
                for r in truth.itertuples()
            ],
            "flight_key long, icao24 string, t_off string, t_land string",
        ),
        "t_off",
        "t_land",
    )


def _stamp(value) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S.%f")


def _with_parsed_times(frame: DataFrame, *columns) -> DataFrame:
    for name in columns:
        frame = frame.withColumn(name, F.to_timestamp(name))
    return frame


def linked_holes(tracks: DataFrame, p: SegmentationParams) -> DataFrame:
    """One row per gap over ``p.gap_minutes`` inside one ``track_id``."""
    window = Window.partitionBy("track_id").orderBy("event_time")
    return (
        tracks.select("track_id", F.lower("icao24").alias("icao24"), "event_time")
        .withColumn("t_old_edge", F.lag("event_time").over(window))
        .withColumn(
            "gap_min",
            (F.unix_timestamp("event_time") - F.unix_timestamp("t_old_edge")) / 60.0,
        )
        .filter(F.col("gap_min") > p.gap_minutes)
        .select(
            "track_id", "icao24", "t_old_edge", F.col("event_time").alias("t_new_edge"), "gap_min"
        )
    )


def scored_holes(holes: DataFrame, flights: DataFrame) -> DataFrame:
    """The holes with both edges inside a scored flight, once each."""
    return holes_inside_flights(holes, flights).drop("flight_key").dropDuplicates(_HOLE_KEY)


# --- the flight list ---------------------------------------------------------


def _plain_arm_names(table: pd.DataFrame) -> pd.DataFrame:
    """The scorer's generic arm names, replaced by this study's."""
    renamed = {}
    for column in table.columns:
        prefix, _, rest = column.partition("_")
        if prefix in _ARM_WORDS and rest:
            renamed[column] = f"{_ARM_WORDS[prefix]}_{rest}"
    return table.rename(columns=renamed)


def score_flight_list(
    truth: pd.DataFrame, split: pd.DataFrame, linked: pd.DataFrame
) -> tuple:
    """``(per view and end, both ends on one row)``, from the paired scorer.

    ``match_one_record`` is an upper bound, fair only for both ends on one row, so it
    feeds that table alone. The sides use the two views a consumer could apply.
    """
    pairs = pd.concat(
        [
            flight_list.pair_table(truth, match(truth, split), match(truth, linked), view)
            for view, match in (
                ("callsign", flight_list.match_callsign),
                ("overlap", flight_list.match_overlap),
            )
        ],
        ignore_index=True,
    )

    def both_ends(flights: pd.DataFrame) -> pd.DataFrame:
        return flight_list.both_ends_table(
            flights,
            flight_list.match_one_record(flights, split),
            flight_list.match_one_record(flights, linked),
            flight_list.rows_per_flight(flights, split),
            flight_list.rows_per_flight(flights, linked),
        )

    crossing = truth[truth["flow"].isin(_CROSSING_FLOWS)].reset_index(drop=True)
    tables = [both_ends(truth)]
    if len(crossing):
        tables.append(both_ends(crossing.assign(flow="crosses_the_box")))
    both = pd.concat(tables, ignore_index=True)
    return _plain_arm_names(pairs), _plain_arm_names(both)


def _flight_list_rows(table: DataFrame) -> pd.DataFrame:
    return table.select(
        F.lower("icao24").alias("icao24"),
        F.trim("flt_id").alias("callsign"),
        "adep",
        "ades",
        "first_seen",
        "last_seen",
    ).toPandas()


# --- detector damage ---------------------------------------------------------


def _seconds(column: str) -> Column:
    return F.unix_timestamp(F.col(column))


def compare_events(
    split_events: DataFrame,
    split_tracks: DataFrame,
    linked_events: DataFrame,
    linked_tracks: DataFrame,
    holes: DataFrame,
    flights: DataFrame,
) -> List[Dict]:
    """Events per type in each arm, on the airframes of *holes*, inside *flights*.

    With the flight list's slack, since a track starts and ends with reception. A semi
    join, so an event in the slack of two flights counts once.
    """
    airframes = holes.select("icao24").distinct()
    slack_s = flight_list.OVERLAP_SLACK.total_seconds()
    windows = F.broadcast(
        flights.select(F.col("icao24").alias("_flight_icao24"), "t_off", "t_land")
    )

    def counted(events: DataFrame, tracks: DataFrame) -> Dict[str, int]:
        scored = (
            events.withColumnRenamed("flight_id", "track_id")
            .join(tracks.select("track_id", "icao24").distinct(), on="track_id")
            .join(airframes, on="icao24", how="left_semi")
            .join(
                windows,
                (F.col("icao24") == F.col("_flight_icao24"))
                & (_seconds("event_time") >= _seconds("t_off") - slack_s)
                & (_seconds("event_time") <= _seconds("t_land") + slack_s),
                "left_semi",
            )
        )
        return {r["type"]: r["count"] for r in scored.groupBy("type").count().collect()}

    split, linked = counted(split_events, split_tracks), counted(linked_events, linked_tracks)
    return [
        {
            "type": kind,
            "debounced_events": split.get(kind, 0),
            "cruise_linked_events": linked.get(kind, 0),
            "change": linked.get(kind, 0) - split.get(kind, 0),
        }
        for kind in sorted(set(split) | set(linked))
    ]


def level_across_holes(events: DataFrame, holes: DataFrame) -> List[Dict]:
    """A row per level family and an ``any`` row: segments spanning a hole, and on what tracks.

    Two events can share an instant, as a one-sample segment does, and then the segment
    closes on the old edge and opens on the new one rather than spanning. Ordering on
    ``(time, is_end)`` says so: the maximum before takes the end, the minimum after takes
    the start.
    """
    family = (
        F.when(F.col("type").isin("level-start", "level-end"), "level")
        .when(F.col("type").rlike("^level-off-.*-(start|end)$"), "level_off")
    )
    marks = events.select(
        F.col("flight_id").alias("track_id"),
        family.alias("family"),
        "event_time",
        F.col("type").endswith("-end").alias("is_end"),
    ).filter(F.col("family").isNotNull())

    mark = F.struct(F.col("event_time"), F.col("is_end"))
    families = holes.sparkSession.createDataFrame([("level",), ("level_off",)], ["family"])
    per_hole = (
        holes.select(*_HOLE_KEY)
        .crossJoin(F.broadcast(families))
        .join(marks, on=["track_id", "family"], how="left")
        .groupBy(*_HOLE_KEY, "family")
        .agg(
            F.max(F.when(F.col("event_time") <= F.col("t_old_edge"), mark)).alias("before"),
            F.min(F.when(F.col("event_time") >= F.col("t_new_edge"), mark)).alias("after"),
        )
        .collect()
    )

    def hole(r) -> tuple:
        return r["track_id"], r["t_old_edge"], r["t_new_edge"]

    rows = []
    for name, members in (
        ("level", ("level",)),
        ("level_off", ("level_off",)),
        ("any", ("level", "level_off")),
    ):
        scored = [r for r in per_hole if r["family"] in members]
        spanned = [
            r
            for r in scored
            if r["before"] is not None
            and r["after"] is not None
            and not r["before"]["is_end"]
            and r["after"]["is_end"]
        ]
        tracks = {r["track_id"] for r in scored}
        damaged = {r["track_id"] for r in spanned}
        rows.append(
            {
                "family": name,
                "scored_holes": len({hole(r) for r in scored}),
                "scored_tracks": len(tracks),
                "segments_across_a_hole": len(
                    {
                        (r["family"], r["track_id"], r["before"]["event_time"], r["after"]["event_time"])
                        for r in spanned
                    }
                ),
                "holes_inside_a_segment": len({hole(r) for r in spanned}),
                "tracks_with_a_segment_across_a_hole": len(damaged),
                "share_of_tracks": len(damaged) / len(tracks) if tracks else None,
            }
        )
    return rows


def distance_across_holes(
    tracks: DataFrame, holes: DataFrame, last_known_position: bool = True
) -> DataFrame:
    """Per hole, the jump in cumulative distance and the great circle between its edges.

    ``last_known_position`` is the pipeline's switch between the distance it computes now
    and the released one, which lost the hole wherever an edge position was blank.

    The edges are the nearest samples with a position either side. ``edge_masked`` says
    one of them is not the sample next to the hole. A hole with no position left on one
    side has no row, since there is no great circle to compare with.
    """
    window = Window.partitionBy("track_id").orderBy("event_time")
    positioned = (
        add_distance_measure(
            tracks.select("track_id", "event_time", "lat", "lon"),
            last_known_position=last_known_position,
        )
        .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
        .withColumn("_prev_time", F.lag("event_time").over(window))
        .withColumn("_prev_lat", F.lag("lat").over(window))
        .withColumn("_prev_lon", F.lag("lon").over(window))
        .withColumn("_prev_distance", F.lag("cumulative_distance_nm").over(window))
    )
    return (
        positioned.join(holes.select(*_HOLE_KEY, "gap_min"), on="track_id")
        # No sample lies inside a hole, so exactly the first positioned sample at or after
        # the new edge has its previous one at or before the old edge.
        .filter(
            (F.col("event_time") >= F.col("t_new_edge"))
            & (F.col("_prev_time") <= F.col("t_old_edge"))
        )
        .withColumn("jump_nm", F.col("cumulative_distance_nm") - F.col("_prev_distance"))
        .withColumn(
            "great_circle_nm",
            haversine_nm(F.col("_prev_lat"), F.col("_prev_lon"), F.col("lat"), F.col("lon")),
        )
        .select(
            *_HOLE_KEY,
            "gap_min",
            "jump_nm",
            "great_circle_nm",
            (F.col("jump_nm") - F.col("great_circle_nm")).alias("difference_nm"),
            (
                (F.col("_prev_time") != F.col("t_old_edge"))
                | (F.col("event_time") != F.col("t_new_edge"))
            ).alias("edge_masked"),
        )
    )


def summarise_distance(per_hole: pd.DataFrame, holes_scored: int) -> Dict:
    """Median and 95th percentile of the difference, and how many holes are off."""
    difference = per_hole["difference_nm"].astype(float)

    def quantile(series: pd.Series, q: float) -> Optional[float]:
        return float(series.quantile(q)) if len(series) else None

    return {
        "holes_scored": holes_scored,
        "holes_compared": len(per_hole),
        "holes_with_no_position_on_one_side": holes_scored - len(per_hole),
        "median_difference_nm": quantile(difference, 0.5),
        "p95_difference_nm": quantile(difference, 0.95),
        "p95_absolute_difference_nm": quantile(difference.abs(), 0.95),
        "holes_off_by_over_1_nm": int((difference.abs() > DISTANCE_TOLERANCE_NM).sum()),
        "holes_with_a_masked_edge": int(per_hole["edge_masked"].sum()),
    }


def stored_distance_check(tracks: DataFrame, events: DataFrame, measurements: DataFrame) -> Dict:
    """Does the rerun reach the value step 04 stored at each track's ``last_seen``?

    This is what ties :func:`distance_across_holes` to the published measurement, which
    has no value at the hole itself. No track checked is not a clean result, so the off
    count is then empty, and ``stored_distance_verdict`` says which case this is.
    """
    rerun = (
        add_distance_measure(tracks.select("track_id", "event_time", "lat", "lon"))
        .groupBy("track_id")
        .agg(F.max("cumulative_distance_nm").alias("_rerun_nm"))
    )
    stored = (
        events.filter(F.col("type") == "last_seen")
        .select(F.col("id").alias("milestone_id"), F.col("flight_id").alias("track_id"))
        .join(
            measurements.filter(F.col("type") == DISTANCE_MEASUREMENT).select(
                "milestone_id", F.col("value").alias("_stored_nm")
            ),
            on="milestone_id",
        )
    )
    joined = stored.join(rerun, on="track_id")
    off = F.abs(F.col("_stored_nm") - F.col("_rerun_nm")) > DISTANCE_TOLERANCE_NM
    row = joined.agg(
        F.count(F.lit(1)).alias("n"),
        F.coalesce(F.sum(F.when(off, 1).otherwise(0)), F.lit(0)).alias("off"),
    ).first()
    scored_tracks = tracks.select("track_id").distinct().count()
    checked = row["n"]
    if checked == 0:
        verdict = "not_checked"
    elif row["off"] > 0:
        verdict = "differs"
    elif checked < scored_tracks:
        verdict = "some_tracks_not_checked"
    else:
        verdict = "agrees"
    return {
        "scored_tracks": scored_tracks,
        "tracks_checked_against_stored_distance": checked,
        "tracks_off_stored_distance_by_over_1_nm": row["off"] if checked else None,
        "stored_distance_verdict": verdict,
    }


def distance_without_holes(tracks: DataFrame) -> Dict:
    """The released distance against the one from the last known position, per track.

    For tracks that hold no hole, so the difference is the positions cleaning blanked
    and nothing else. *tracks* is the ``debounced`` arm, where no track has a hole.
    """
    samples = tracks.select("track_id", "event_time", "lat", "lon")

    def total(last_known_position: bool, name: str) -> DataFrame:
        return (
            add_distance_measure(samples, last_known_position=last_known_position)
            .groupBy("track_id")
            .agg(F.max("cumulative_distance_nm").alias(name))
        )

    per_track = total(False, "released_nm").join(total(True, "new_nm"), on="track_id").toPandas()
    short = per_track["new_nm"] - per_track["released_nm"]
    long_tracks = per_track["new_nm"] > LONG_TRACK_NM
    short_share = (short / per_track["new_nm"])[long_tracks]
    off = int((short > DISTANCE_TOLERANCE_NM).sum())

    def quantile(series: pd.Series, q: float) -> Optional[float]:
        return float(series.quantile(q)) if len(series) else None

    return {
        "tracks": len(per_track),
        "released_total_nm": float(per_track["released_nm"].sum()),
        "new_total_nm": float(per_track["new_nm"].sum()),
        "share_of_distance_missing": float(short.sum() / per_track["new_nm"].sum())
        if len(per_track) and per_track["new_nm"].sum()
        else None,
        "tracks_short_by_over_1_nm": off,
        "share_of_tracks_short_by_over_1_nm": off / len(per_track) if len(per_track) else None,
        "median_short_nm": quantile(short, 0.5),
        "long_tracks": int(long_tracks.sum()),
        "long_tracks_median_share_missing": quantile(short_share, 0.5),
        "long_tracks_p90_share_missing": quantile(short_share, 0.9),
    }


# --- the two arms describe the same tracks -----------------------------------


def rows_without_a_track(flight_list: DataFrame, tracks: DataFrame) -> int:
    """Flight-list rows whose ``id`` is no track of the arm.

    Not zero means the flight list was built from other tracks than the arm now holds --
    a candidate table left over from an earlier run did exactly that -- and nothing
    scored from it describes the arm.
    """
    ids = tracks.select(F.col("track_id").alias("id")).distinct()
    return flight_list.join(ids, on="id", how="left_anti").count()


def named_among_alternatives(flight_list: DataFrame) -> int:
    """Rows naming an aerodrome that is also in that row's own alternatives."""

    def listed(named: str, alternatives: str) -> Column:
        return F.coalesce(
            F.array_contains(F.split(F.col(alternatives), ", "), F.col(named)), F.lit(False)
        )

    return flight_list.filter(listed("adep", "adep_p") | listed("ades", "ades_p")).count()


def unchanged_tracks(split_tracks: DataFrame, linked_tracks: DataFrame) -> DataFrame:
    """``split_id`` and ``linked_id`` of the tracks that are the same under both rules.

    The same means the same airframe, first sample and last sample. Ids are not compared.
    """
    key = ["icao24", "t_first", "t_last"]
    return (
        _track_extents(split_tracks)
        .withColumnRenamed("track_id", "split_id")
        .join(_track_extents(linked_tracks).withColumnRenamed("track_id", "linked_id"), on=key)
        .select("split_id", "linked_id")
    )


def unchanged_track_row(
    pairs: DataFrame,
    split_flight_list: DataFrame,
    linked_flight_list: DataFrame,
    split_events: DataFrame,
    linked_events: DataFrame,
) -> Dict:
    """On tracks the rule did not touch: how many got other airports or other events.

    All should be zero. The rule changes which samples share a track and nothing else,
    so a difference here is a step that does not give the same answer twice. Events are
    compared twice: by type and time, and again with ``info``, because ``info`` holds
    order-dependent details -- which of several overlapping layout elements is named
    at one instant, the sequence number of two crossings in one second -- that differ
    between two runs of the same code on the same track.
    """

    def airports(flight_list: DataFrame, key: str, prefix: str) -> DataFrame:
        return flight_list.select(
            F.col("id").alias(key),
            F.col("adep").alias(f"{prefix}_adep"),
            F.col("ades").alias(f"{prefix}_ades"),
        )

    with_info = "info" in split_events.columns and "info" in linked_events.columns

    def event_set(events: DataFrame, key: str, name: str, info: bool) -> DataFrame:
        fields = [F.col("type"), F.col("event_time")] + ([F.col("info")] if info else [])
        return events.groupBy(F.col("flight_id").alias(key)).agg(
            F.sort_array(F.collect_list(F.struct(*fields))).alias(name)
        )

    joined = (
        pairs.join(airports(split_flight_list, "split_id", "split"), on="split_id", how="left")
        .join(airports(linked_flight_list, "linked_id", "linked"), on="linked_id", how="left")
        .join(event_set(split_events, "split_id", "split_events", False), on="split_id", how="left")
        .join(event_set(linked_events, "linked_id", "linked_events", False), on="linked_id", how="left")
    )
    if with_info:
        joined = joined.join(
            event_set(split_events, "split_id", "split_full", True), on="split_id", how="left"
        ).join(event_set(linked_events, "linked_id", "linked_full", True), on="linked_id", how="left")
    other_airports = ~(
        F.col("split_adep").eqNullSafe(F.col("linked_adep"))
        & F.col("split_ades").eqNullSafe(F.col("linked_ades"))
    )
    other_events = ~F.col("split_events").eqNullSafe(F.col("linked_events"))
    other_info = (
        ~F.col("split_full").eqNullSafe(F.col("linked_full")) if with_info else F.lit(False)
    )
    row = joined.agg(
        F.count(F.lit(1)).alias("n"),
        F.coalesce(F.sum(other_airports.cast("int")), F.lit(0)).alias("airports"),
        F.coalesce(F.sum(other_events.cast("int")), F.lit(0)).alias("events"),
        F.coalesce(F.sum(other_info.cast("int")), F.lit(0)).alias("info"),
    ).first()
    return {
        "unchanged_tracks": row["n"],
        "unchanged_tracks_with_other_airports": row["airports"],
        "unchanged_tracks_with_other_events": row["events"],
        "unchanged_tracks_with_other_event_info": row["info"],
    }


# --- nothing else moved ------------------------------------------------------


def _boundaries(tracks: DataFrame) -> DataFrame:
    """``icao24`` and ``t_boundary``, the start of each track but an airframe's first."""
    starts = tracks.groupBy(F.lower("icao24").alias("icao24"), "track_id").agg(
        F.min(F.to_timestamp("event_time")).alias("t_boundary")
    )
    return (
        starts.withColumn("_first", F.min("t_boundary").over(Window.partitionBy("icao24")))
        .filter(F.col("t_boundary") > F.col("_first"))
        .select("icao24", "t_boundary")
    )


def debounced_with_a10_memory() -> BreakRule:
    """A9 with A10's callsign memory and nothing else: the diagnostic in between.

    A10 differs from A9 in two ways: it links holes, and it forgets a callsign older
    than the longest linkable hole. A boundary that this rule and A9 disagree on is the
    memory's; one that A10 removes at a hole :func:`linkable` accepts is the link's.
    """

    def expr(p):
        return debounced_callsign_change(p, callsign_memory_minutes(p)) | legacy().break_expr(p)

    return BreakRule(
        name="debounced_with_memory", group_cols=["icao24"], break_expr=expr,
        month_suffix=False, id_from_start_time=True,
    )


def partition_check(
    samples: DataFrame, split: DataFrame, linked: DataFrame, p: SegmentationParams
) -> Dict:
    """Compare where the two rules cut one input.

    *split* and *linked* are *samples* segmented by A9 and A10. A boundary A10 removed
    must be a hole the rule's own :func:`linkable` accepts at *p* or one the callsign
    memory removes, and A10 may add only what the memory adds
    (:func:`debounced_with_a10_memory`).
    """
    features = {name: F.col(name) for name in link_feature_columns(p)}
    linkable_holes = (
        real_hole_candidates(samples, p)
        .filter(linkable(features, p))
        .select(F.lower("icao24").alias("icao24"), F.col("t_new_edge").alias("t_boundary"))
        .distinct()
    )
    key = ["icao24", "t_boundary"]
    before = _boundaries(split).cache()
    after = _boundaries(linked).cache()
    memory = _boundaries(assign_track_id(samples, debounced_with_a10_memory(), p)).cache()
    removed = before.join(after, on=key, how="left_anti").cache()
    removed_n = removed.count()
    not_at_hole = removed.join(linkable_holes, on=key, how="left_anti").cache()
    removed_by_memory_n = not_at_hole.join(memory, on=key, how="left_anti").count()
    removed_elsewhere_n = not_at_hole.count() - removed_by_memory_n
    added = after.join(before, on=key, how="left_anti").cache()
    added_n = added.count()
    added_by_memory_n = added.join(memory, on=key, how="left_semi").count()
    added_elsewhere_n = added_n - added_by_memory_n
    return {
        "debounced_boundaries": before.count(),
        "cruise_linked_boundaries": after.count(),
        "removed_boundaries": removed_n,
        "removed_at_a_linked_hole": removed_n - not_at_hole.count(),
        "removed_by_callsign_memory": removed_by_memory_n,
        "removed_elsewhere": removed_elsewhere_n,
        "added_boundaries": added_n,
        "added_by_callsign_memory": added_by_memory_n,
        "added_elsewhere": added_elsewhere_n,
        "passed": removed_elsewhere_n == 0 and added_elsewhere_n == 0,
    }


def arm_read_window(samples: DataFrame, day: str, p: SegmentationParams) -> DataFrame:
    """*samples* cut to what step 02 reads for *day* under ``cruise_linked``.

    See ``TrackProcessor.process_day``. A sample outside it could end a track later than
    the arm could have seen, and read as a disagreement.
    """
    start = dt.datetime.fromisoformat(day)
    read_start = start - dt.timedelta(hours=required_lookback_hours("cruise_linked", p))
    read_end = start + dt.timedelta(days=1, hours=TrackProcessor.DEFAULT_LOOKAHEAD_HOURS)
    return samples.filter(
        (F.col("event_time") >= F.to_timestamp(F.lit(read_start.isoformat(" "))))
        & (F.col("event_time") < F.to_timestamp(F.lit(read_end.isoformat(" "))))
    )


def _track_extents(tracks: DataFrame) -> DataFrame:
    """``icao24`` and the first and last sample of each track."""
    return tracks.groupBy(F.lower("icao24").alias("icao24"), "track_id").agg(
        F.min(F.to_timestamp("event_time")).alias("t_first"),
        F.max(F.to_timestamp("event_time")).alias("t_last"),
    )


def arm_agreement(arm_tracks: DataFrame, resegmented: DataFrame, day: str) -> Dict[str, int]:
    """Whether an arm's own tracks of *day* are what the rule gives now, in both directions.

    Ids are not compared, as a track is known by its airframe and extent. The last sample
    is there because a hole after midnight changes no start on the day.
    """
    key = ["icao24", "t_first", "t_last"]
    on_the_day = F.to_date("t_first") == F.lit(day).cast("date")
    arm = _track_extents(arm_tracks).filter(on_the_day).select(*key).cache()
    rule = _track_extents(resegmented).filter(on_the_day).select(*key).cache()
    return {
        "arm_tracks": arm.count(),
        "resegmented_tracks": rule.count(),
        "arm_tracks_not_resegmented": arm.join(rule, on=key, how="left_anti").count(),
        "resegmented_tracks_not_in_arm": rule.join(arm, on=key, how="left_anti").count(),
    }


def partition_row(
    samples: DataFrame, arm_tracks: DataFrame, p: SegmentationParams, day: str
) -> Dict:
    """The table's row: the two rules on *samples*, and the A10 arm against its rule."""
    linked = assign_track_id(samples, cruise_linked(), p).cache()
    rules = partition_check(samples, assign_track_id(samples, debounced(), p), linked, p)
    arm = arm_agreement(arm_tracks, linked, day)
    agrees = arm["arm_tracks_not_resegmented"] == 0 and arm["resegmented_tracks_not_in_arm"] == 0
    return {**rules, **arm, "passed": rules["passed"] and agrees}


def movement_row(
    movement: str, apdf_movements: int, split_matched: int, linked_matched: int
) -> Dict:
    """One line of the movement table. A10 may fall by ``MOVEMENT_FALL_LIMIT`` at most."""
    fall = (split_matched - linked_matched) / split_matched if split_matched else None
    return {
        "movement": movement,
        "apdf_movements": apdf_movements,
        "debounced_matched": split_matched,
        "cruise_linked_matched": linked_matched,
        "fall_share": fall,
        "passed": linked_matched >= split_matched * (1.0 - MOVEMENT_FALL_LIMIT),
    }


def scored_movements(spark, milestones: DataFrame, scored: pd.DataFrame) -> DataFrame:
    """The ATOT and ALDT truth rows of the scored flights.

    Matched as ``events_gt.bridge`` carries a flight: airframe, ICAO callsign and
    off-block, the last of which keeps two legs of one callsign apart. A flight with no
    callsign has nothing to match a detection on, and is left out here.
    """
    named = scored.dropna(subset=["callsign"])
    flights = _with_parsed_times(
        spark.createDataFrame(
            [(r.icao24, r.callsign, _stamp(r.t_off)) for r in named.itertuples()],
            "icao24 string, callsign string, gt_aobt string",
        ),
        "gt_aobt",
    )
    return milestones.filter(F.col("milestone").isin(list(MOVEMENTS))).join(
        F.broadcast(flights), on=["icao24", "callsign", "gt_aobt"], how="left_semi"
    )


def matched_movements(truth: DataFrame, detected: DataFrame) -> DataFrame:
    """``milestone``, ``n_truth`` and ``n_detected``, counted per truth row.

    A truth row is matched by any detection of its airframe, callsign and milestone within
    ``MOVEMENT_MATCH_MINUTES``. The time decides, so the day of the track is not needed.
    """
    rows = truth.select(*_MOVEMENT_KEY, "gt_time").dropDuplicates(_MOVEMENT_KEY)
    apart_s = F.abs(F.col("event_time").cast("double") - F.col("gt_time").cast("double"))
    hits = (
        rows.join(
            detected.select("icao24", "callsign", "milestone", "event_time"),
            on=["icao24", "callsign", "milestone"],
        )
        .filter(apart_s <= MOVEMENT_MATCH_MINUTES * 60.0)
        .select(*_MOVEMENT_KEY)
        .distinct()
        .withColumn("_matched", F.lit(1))
    )
    return (
        rows.join(hits, on=_MOVEMENT_KEY, how="left")
        .groupBy("milestone")
        .agg(
            F.count(F.lit(1)).alias("n_truth"),
            F.count("_matched").alias("n_detected"),
        )
    )


def movement_rows(truth: DataFrame, split: DataFrame, linked: DataFrame) -> List[Dict]:
    """A row per milestone and an ``all`` row, which is the gate."""

    def matched(detected: DataFrame) -> Dict[str, tuple]:
        scored = matched_movements(truth, detected)
        events_score.guard_not_all_zero(scored)
        return {r["milestone"]: (r["n_truth"], r["n_detected"]) for r in scored.collect()}

    before, after = matched(split), matched(linked)
    if not before:
        raise SystemExit(
            "No APDF movement reached a scored flight: an identity-join failure, not a "
            "result. Check the airframe, callsign and off-block of the two sides."
        )
    rows = [
        movement_row(m, before[m][0], before[m][1], after[m][1]) for m in MOVEMENTS if m in before
    ]
    rows.append(
        movement_row(
            "all",
            sum(r["apdf_movements"] for r in rows),
            sum(r["debounced_matched"] for r in rows),
            sum(r["cruise_linked_matched"] for r in rows),
        )
    )
    return rows


# --- holes against every Network Manager flight -------------------------------

HOLE_LABELS = ("false_join", "true_link", "unknown", "airframe_not_in_truth")

LANDING_FAMILY = ("ALDT", "AIBT")
TAKE_OFF_FAMILY = ("ATOT", "AOBT")


def load_all_flights(path, airframes) -> pd.DataFrame:
    """Every NM flight of *airframes* in the truth file, as ``icao24``, ``t_off``, ``t_on`` (UTC).

    Not :func:`adsc_flight_list_truth.load_truth`, which keeps one day's off-block and
    needs ADEP and ADES: a flight on the next day, or with no aerodrome, still bounds a
    hole.

    "Any day" means any flight in the file, which is one month plus a few hours, so a
    hole near a month edge can miss a flight in the neighbouring month.
    """
    t = pd.read_parquet(path, columns=["AIRCRAFT_ADDRESS", "AOBT_3", "ARVT_3"]).dropna(
        subset=["AIRCRAFT_ADDRESS", "AOBT_3", "ARVT_3"]
    )
    t = pd.DataFrame(
        {
            "icao24": t["AIRCRAFT_ADDRESS"].str.lower().str.strip(),
            "t_off": pd.to_datetime(t["AOBT_3"], utc=True).dt.tz_localize(None),
            "t_on": pd.to_datetime(t["ARVT_3"], utc=True).dt.tz_localize(None),
        }
    )
    return t[t["icao24"].isin(set(airframes))].reset_index(drop=True)


def label_holes_all_flights(
    holes: pd.DataFrame, nm: pd.DataFrame, pieces: pd.DataFrame, events: pd.DataFrame
) -> pd.DataFrame:
    """Each hole against every flight of its airframe in *nm*, whatever the day.

    ``holes``: ``icao24, t_old_edge, t_new_edge``. ``nm``: ``icao24, t_off, t_on``.
    ``pieces``: the debounced tracks, ``icao24, t_start, t_end``. ``events``: the
    debounced arm's detected events, ``icao24, milestone, event_time``.

    ``false_join`` when an off-block or on-block of the airframe falls strictly inside the
    hole: the aircraft was parked there. ``true_link`` when the hole lies within one
    flight's block window. ``unknown`` when the airframe is in *nm* and neither holds.
    Detected events never decide the label; they are only counted beside it, as
    ``landing_on_older`` (ALDT or AIBT on the older piece) and ``take_off_on_newer``
    (ATOT or AOBT on the newer one). An event counts anywhere on the piece, not only
    near the hole edge. Pieces are the one day's debounced tracks, so a piece starting
    after midnight is absent and the three event counts are lower bounds; the labels
    are unaffected.
    """
    by_nm = {k: g for k, g in nm.groupby("icao24")}
    by_piece = {k: g for k, g in pieces.groupby("icao24")}
    by_event = {k: g for k, g in events.groupby("icao24")}

    def has(icao24, milestones, start, end) -> bool:
        e = by_event.get(icao24)
        if e is None:
            return False
        return bool(
            (
                e["milestone"].isin(milestones)
                & (e["event_time"] >= start)
                & (e["event_time"] <= end)
            ).any()
        )

    def on_piece(icao24, edge, milestones) -> bool:
        p = by_piece.get(icao24)
        if p is None:
            return False
        containing = p[(p["t_start"] <= edge) & (p["t_end"] >= edge)]
        return any(has(icao24, milestones, r.t_start, r.t_end) for r in containing.itertuples())

    rows = []
    for h in holes.itertuples():
        flights = by_nm.get(h.icao24)
        if flights is None:
            label = "airframe_not_in_truth"
        else:
            parked = (
                ((flights["t_off"] > h.t_old_edge) & (flights["t_off"] < h.t_new_edge))
                | ((flights["t_on"] > h.t_old_edge) & (flights["t_on"] < h.t_new_edge))
            ).any()
            within = (
                (flights["t_off"] <= h.t_old_edge) & (flights["t_on"] >= h.t_new_edge)
            ).any()
            label = "false_join" if parked else "true_link" if within else "unknown"
        rows.append(
            {
                "label": label,
                "landing_on_older": on_piece(h.icao24, h.t_old_edge, LANDING_FAMILY),
                "take_off_on_newer": on_piece(h.icao24, h.t_new_edge, TAKE_OFF_FAMILY),
            }
        )
    return pd.DataFrame(rows, columns=["label", "landing_on_older", "take_off_on_newer"])


def summarise_hole_labels(labelled: pd.DataFrame) -> List[Dict]:
    """One row per label, always all four, with the counts of events on the pieces."""
    out = []
    for label in HOLE_LABELS:
        g = labelled[labelled["label"] == label]
        out.append(
            {
                "label": label,
                "holes": len(g),
                "landing_on_older": int(g["landing_on_older"].sum()),
                "take_off_on_newer": int(g["take_off_on_newer"].sum()),
                "both": int((g["landing_on_older"] & g["take_off_on_newer"]).sum()),
            }
        )
    return out


def truth_all_flights_rows(
    holes: DataFrame, tracks: DataFrame, events: pd.DataFrame, truth_path
) -> List[Dict]:
    """The rows of ``link_truth_all_flights.csv``.

    *holes* are the linked arm's, *tracks* the debounced arm's ``osn_tracks_clean`` and
    *events* its detected events as a pandas frame of ``icao24, milestone, event_time``.
    """
    hole_rows = holes.select("icao24", "t_old_edge", "t_new_edge").toPandas()
    pieces = (
        tracks.groupBy("track_id", "icao24")
        .agg(F.min("event_time").alias("t_start"), F.max("event_time").alias("t_end"))
        .toPandas()
    )
    nm = load_all_flights(truth_path, set(hole_rows["icao24"]))
    return summarise_hole_labels(label_holes_all_flights(hole_rows, nm, pieces, events))


# --- the job -----------------------------------------------------------------


def one_day(table: DataFrame, name: str, day: str) -> DataFrame:
    """*table* with lower-case columns, cut to *day*'s ``dof``. Shared inputs have none.

    The flight list writes upper-case columns and the event tables lower-case.
    """
    table = table.toDF(*[c.lower() for c in table.columns])
    if name in SHARED_INPUTS:
        return table
    return table.filter(F.col("dof").cast("date") == F.lit(day).cast("date"))


def score_day(
    spark,
    day: str,
    read: Callable[[str, str], DataFrame],
    truth: pd.DataFrame,
    p: SegmentationParams,
    apdf: DataFrame,
    detected: Dict[str, DataFrame],
    write: Callable[[str, List[Dict]], None],
    truth_path: str,
) -> None:
    """Every table in :data:`OUTPUTS`, written as each is ready.

    ``read(arm, table)`` gives an arm's table for the day, ``truth`` the labelled flights
    inside the day, ``apdf`` the long-form milestones of ``events_gt``, and ``detected``
    each arm's ``event_bench.detected_events``.
    """
    if truth.empty:
        raise SystemExit(f"no truth flights inside {day}: nothing can be scored")

    split_name, linked_name = ARMS
    tracks = {
        arm: read(arm, "osn_tracks_clean")
        .select("track_id", F.lower("icao24").alias("icao24"), "event_time", "lat", "lon")
        .cache()
        for arm in ARMS
    }
    events = {
        arm: read(arm, "opdi_flight_events").select("id", "flight_id", "type", "event_time", "info")
        for arm in ARMS
    }

    # First, as it reads the state vectors, the table a short bucket gives up soonest.
    samples = arm_read_window(
        read(split_name, "osn_statevectors_v2").select(
            "event_time", "icao24", "lat", "lon", "baro_altitude", "velocity", "vert_rate", "callsign"
        ),
        day,
        p,
    ).cache()
    write(
        "link_partition_check.csv", [partition_row(samples, tracks[linked_name], p, day)]
    )
    samples.unpersist()

    # Next, as everything below is meaningless when an arm's tables do not belong together.
    flight_lists = {
        arm: read(arm, "opdi_flight_list").select("id", "adep", "ades", "adep_p", "ades_p")
        for arm in ARMS
    }
    consistency = {}
    for arm in ARMS:
        consistency[f"{arm}_flight_list_rows"] = flight_lists[arm].count()
        consistency[f"{arm}_rows_without_a_track"] = rows_without_a_track(
            flight_lists[arm], tracks[arm]
        )
        consistency[f"{arm}_named_among_alternatives"] = named_among_alternatives(
            flight_lists[arm]
        )
    consistency.update(
        unchanged_track_row(
            unchanged_tracks(tracks[split_name], tracks[linked_name]),
            flight_lists[split_name],
            flight_lists[linked_name],
            events[split_name],
            events[linked_name],
        )
    )
    write("link_consistency.csv", [consistency])
    orphans = {arm: consistency[f"{arm}_rows_without_a_track"] for arm in ARMS}
    if any(orphans.values()):
        raise SystemExit(
            f"flight-list rows without a track: {orphans}. The flight list was not built "
            "from the arm's current tracks; re-run step 03 for that arm."
        )

    flights = scored_flights_frame(spark, truth).cache()
    holes = linked_holes(tracks[linked_name], p).cache()
    holes_in_flights = scored_holes(holes, flights).cache()

    # The owner's goal.
    pairs, both = score_flight_list(
        truth,
        _flight_list_rows(read(split_name, "opdi_flight_list")),
        _flight_list_rows(read(linked_name, "opdi_flight_list")),
    )
    write("link_flight_list.csv", pairs.to_dict("records"))
    write("link_both_ends.csv", both.to_dict("records"))

    # Detector damage.
    write(
        "link_events.csv",
        compare_events(
            events[split_name], tracks[split_name], events[linked_name], tracks[linked_name],
            holes, flights,
        ),
    )
    linked_everywhere = {
        "linked_holes_anywhere": holes.count(),
        "linked_tracks_anywhere": holes.select("track_id").distinct().count(),
    }
    write(
        "link_level_across_hole.csv",
        [
            {**row, **linked_everywhere}
            for row in level_across_holes(events[linked_name], holes_in_flights)
        ],
    )
    scored_tracks = tracks[linked_name].join(
        holes_in_flights.select("track_id").distinct(), on="track_id", how="left_semi"
    )
    per_hole = distance_across_holes(scored_tracks, holes_in_flights).toPandas()
    write(
        "link_distance_across_hole.csv",
        [
            {
                **summarise_distance(per_hole, holes_in_flights.count()),
                **stored_distance_check(
                    scored_tracks, events[linked_name], read(linked_name, "opdi_measurements")
                ),
            }
        ],
    )
    write("link_distance_no_hole.csv", [distance_without_holes(tracks[split_name])])

    write(
        "link_truth_all_flights.csv",
        truth_all_flights_rows(
            holes,
            tracks[split_name],
            detected[split_name].select("icao24", "milestone", "event_time").toPandas(),
            truth_path,
        ),
    )

    write(
        "link_movements.csv",
        movement_rows(
            scored_movements(spark, apdf, truth), detected[split_name], detected[linked_name]
        ),
    )


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--day", required=True, help="YYYY-MM-DD")
    ap.add_argument(
        "--truth",
        default=None,
        help="flight-list truth and which flights are scored, read on the driver. The APDF "
        "bridge stays on events_gt's S3 mirror. Default: reference/flights_<YYYYMM>.parquet",
    )
    ap.add_argument("--results-dir", type=Path, required=True)
    ap.add_argument("--executors", type=int, default=8)
    ap.add_argument(
        "--link-params-json", default=None, help="the overrides the arm was run with, if any"
    )
    args = ap.parse_args()

    month = args.day[:7].replace("-", "")
    truth_path = args.truth or str(REPO / f"reference/flights_{month}.parquet")
    link_params = json.loads(args.link_params_json) if args.link_params_json else None
    configs = {arm: build_config(arm, link_params) for arm in ARMS}
    p = SegmentationParams.from_config(configs["cruise_linked"])

    import osn_sample
    from event_bench import detected_events, milestone_map
    from osn_sample import build_spark, load_dotenv

    from opdi.coverage import EUROPE_BBOX
    from opdi.utils.storage import StorageManager

    load_dotenv()
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(4, "8g", distributed=True)
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    events_gt.assert_utc_session(spark)

    def read(arm: str, table: str) -> DataFrame:
        return one_day(spark.read.parquet(arm_path(arm, table)), table, args.day)

    def write(name: str, rows: List[Dict]) -> None:
        table = pd.DataFrame(rows)
        table.to_csv(args.results_dir / name, index=False)
        provenance.record(
            args.results_dir, name, "benchmarks/track_link_pipeline_score.py", sys.argv[1:],
            _CODE_PATHS,
            inputs={
                "day": args.day,
                "truth_flights": len(truth),
                "truth": truth_path,
                "truth_sha256_16": provenance.file_hash(Path(truth_path)),
                "link_params": link_params,
                "link_settings": {
                    k: v for k, v in dataclasses.asdict(p).items() if k.startswith("link_")
                },
            },
            notes=_NOTES,
            input_tables=_input_tables(month),
        )
        print(f"\nwrote {args.results_dir / name}\n{table.to_string(index=False)}", flush=True)

    args.results_dir.mkdir(parents=True, exist_ok=True)
    try:
        airframes = [
            r[0]
            for r in read("debounced", "osn_tracks_clean")
            .select(F.lower("icao24"))
            .distinct()
            .collect()
        ]
        min_lon, min_lat, max_lon, max_lat = EUROPE_BBOX
        in_box = {
            r[0]
            for r in spark.read.parquet(f"{REFERENCE_WAREHOUSE}/oa_airports")
            .where(
                F.col("longitude_deg").between(min_lon, max_lon)
                & F.col("latitude_deg").between(min_lat, max_lat)
            )
            .select("ident")
            .collect()
        }
        truth = inside_the_day(
            flight_list.label_far_end(
                flight_list.load_truth(truth_path, args.day, airframes), in_box
            ),
            args.day,
        )
        print(f"truth flights inside {args.day}: {len(truth):,}")
        print(truth["flow"].value_counts().to_string(), flush=True)

        bridged = events_gt.bridge(
            events_gt.load_apdf(spark, month), events_gt.load_flights(spark, month)
        ).cache()
        relative = len(REFERENCE_WAREHOUSE) + 1
        detected = {
            arm: detected_events(
                spark,
                arm_path(arm, "opdi_flight_events"),
                storage=StorageManager(spark, configs[arm]),
                identity="flight_list",
                flight_list=arm_path(arm, "opdi_flight_list")[relative:],
                mapping=milestone_map(configs[arm].events),
            )
            for arm in ARMS
        }
        score_day(
            spark,
            args.day,
            read,
            truth,
            p,
            events_gt.milestones(bridged, [args.day]),
            detected,
            write,
            truth_path,
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
