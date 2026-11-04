#!/usr/bin/env python
"""Which A10 parameters link the most flights while staying within the safety limits?

Stage 1 of the ``cruise_linked`` study (spec 2026-11-03). Each grid point's link
decision is applied to real reception holes and to the holes of
``track_link_controls``. Among the points within three safety limits, on numbers pooled
over the days, the pick is the most flights crossing the European box in one track, which
is the owner's goal, then the most flights in one track overall, then the more
conservative settings: the higher ``link_min_implied_speed_kt``, the shorter
``link_max_gap_minutes``, the higher ``link_min_speed_ratio``, the higher
``link_min_stop_minutes``, and direction off before on. The limits are:

  negative controls   the upper bound on the false-join rate is at most 1%, over the
                      controls of every family whose gap the point allows. For
                      ``cruise_cruise`` the realised depth must also be at least 30 min
                      on one side. A base cut is too easy, as speed rejects nearly all
                      of it, and a gap over the point's ``link_max_gap_minutes`` tests
                      nothing, so counting either as a pass would dilute the bound and
                      give short-gap points an easier limit. Both have columns of their
                      own. The low-edge families always count at a point with
                      ``link_directional_edges`` on, whatever their depth. With it off
                      they fail the floor by construction and test nothing, for the
                      same reason, so they are kept apart there too. The bound is per
                      control, but depths of one pair are not independent, so the count
                      of pairs with a false join is reported beside it.
  same or blank       the same bound is at most 5% among the controls the callsign
  callsign            test cannot reject, in the same population as the 1% limit, and
                      for the same reason. Those too shallow have columns of their
                      own. Under 60 reads ``insufficient``: it does not fail the point,
                      and it marks the choice provisional. This is a count and not a
                      reading of the bound, which sits just under 5% at 59.
  real holes          false joins are at most 0.1% of the holes the point links that
                      have a known label. It was 1%, tightened after round 3 showed 1%
                      could not separate settings. Only a linked hole counts: there each
                      edge is at cruise, or climbing, or descending, which is what makes
                      a landing or take-off inside it the proof of a stop. Unknown
                      labels are reported, not scored.

A point with nothing to measure it on is not within the limits. If no point passes,
none is recommended, and ``link_recommended.csv`` names the point with the lowest
bound, labelled not recommended, for Stage 2 to run at.

**Four reports beside the choice.** None of them touches a limit.

  why holes stay split  ``link_reasons_{day}.csv`` and ``link_reasons_pooled.csv``, for
                        the real holes truth calls ``true_link``, whatever the rule
                        decides. One row per population, length band and
                        ``first_failed_test``, ``linked`` when none fails.
                        ``only_failed_test_holes`` is how many of the row's ``holes``
                        fail that test and no other, so it is what relaxing that one
                        test would gain. ``link_positive_reasons_*.csv`` counts missed
                        positive controls the same way. Both are read at the chosen
                        point, or at ``--reasons-params-json``, once the days are
                        pooled, and ``settings_from`` says which.
  the target population ``crossing_*`` and ``other_*`` in each grid row split its flight
                        counts, as ``base_`` does for A9. Crossing means exactly one
                        end inside ``EUROPE_BBOX``, as ``adsc_flight_list_truth`` labels
                        it: an ident ``oa_airports`` does not place counts as outside.
  the callsign test     ``negative_limit_false_joins_no_callsign`` counts what the 1%
                        limit's controls would do without that test, with its bound.
  how the edge was read ``linked_edge_read_through_blank`` and ``linked_by_direction``
                        count linked holes whose position came from past a blank sample,
                        or whose edge is below the floor, and ``false_joined_*`` those
                        of them labelled false joins. ``link_controls_*.csv`` has each
                        control family on rows of its own.

**Why 54 points do not need 54 segmentations.** A10 breaks where A9 breaks, except at
a gap over ``gap_minutes`` that links. Nothing else about the rule depends on a link:
a callsign break is evaluated over the whole airframe and is not "recent" across such
a gap, and a low-altitude break cannot fire where a hole links: the new edge is at cruise,
or ``link_checks`` refuses one that sample is below ``low_alt_ft``. So
a flight's tracks under a point are its A9 tracks minus the holes linked inside it,
and ``test_counting_from_decisions_agrees_with_really_re_segmenting`` holds this
against A10 really being run. A hole counts for a flight only with both edges inside
the flight's interval: otherwise the flight's samples all lie on one side of it.

**Not from raw.** This re-segments ``opdi-world/osn_tracks``, as
``track_fragmentation.py`` does, and that table holds only tracks starting on the day.
So the scored population is flights that take off and land on the day, which leans
westbound. Truth is European, so most of the world's holes stay ``unknown``.

**Cost.** One day is about 150M rows, and the day's frames are each built once. The
heavy steps are the whole-day feature windows of the real holes; the controls and
funnel, which together make some dozens of passes over the samples inside truth flights,
as each family makes its own cuts, so those samples are cached first; and the two-callsign counts, which segment the whole
day under both arms, once per day, for the chosen point only. Each point then runs
about seven small jobs over small cached frames, and the reasons two per day over
frames kept until the days are pooled. Elapsed time per stage is printed.
Nothing is written to S3. Results go to a local directory, and each day's are on disk
before the next day starts.

    python benchmarks/track_link_grid.py --days 2026-06-14 2025-06-14 \
        --results-dir benchmarks/out/track_link_grid

``--grid-json`` replaces the grid for a later round, naming ``link_*`` settings:

    --grid-json '{"link_min_implied_speed_kt": [300, 350], "link_max_gap_minutes": [360]}'

Settings it does not name keep their defaults. Those that shape the cuts and features
(:data:`ONE_VALUE_PER_RUN`) may take only one value, so vary them across runs.
``link_directional_edges`` takes ``true`` and ``false``:

    --grid-json '{"link_directional_edges": [false, true], "link_max_gap_minutes": [360]}'

``--reasons-params-json`` names the one point the reasons are read at, in place of the
chosen one, and need not be in the grid:

    --reasons-params-json '{"link_min_implied_speed_kt": 315, "link_max_gap_minutes": 360}'
"""

import argparse
import csv
import datetime as dt
import itertools
import json
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, fields, replace
from functools import reduce
from operator import and_
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

import provenance
from track_fragmentation import measure
from track_link_controls import (
    LOW_EDGE_FAMILIES,
    control_funnel,
    flight_samples,
    label_real_holes,
    negative_controls,
    positive_controls,
    real_hole_candidates,
    rule_of_three_upper,
)
from track_truth import REFERENCE_BASE, load_flight_intervals

from opdi.coverage import EUROPE_BBOX
from opdi.pipeline.segmentation import SegmentationParams, assign_track_id
from opdi.pipeline.segmentation.methods import (
    debounced,
    link_checks,
    link_feature_columns,
    linkable,
)

SOURCE = "s3a://eurocontrol/opdi-world/osn_tracks"
AIRPORTS = "s3a://eurocontrol/opdi-world/oa_airports"
DEFAULT_DAYS = ("2026-06-14", "2025-06-14")

#: The study's grid, 3 x 2 x 3 x 3 = 54 points.
GRID = {
    "link_min_implied_speed_kt": (250.0, 315.0, 400.0),
    "link_min_speed_ratio": (0.0, 0.85),
    "link_min_stop_minutes": (0.0, 20.0, 35.0),
    "link_max_gap_minutes": (120.0, 240.0, 360.0),
}

#: Settings the cuts and features are made with, once per run, so they cannot vary
#: inside one. ``link_cruise_floor_ft`` is both a threshold and where controls are cut,
#: and ``link_edge_vert_rate_fpm`` both a threshold and what a low edge is cut at.
ONE_VALUE_PER_RUN = (
    "link_cruise_floor_ft",
    "link_edge_vert_rate_fpm",
    "link_callsign_lookahead_minutes",
    "link_callsign_lookback_minutes",
    "link_edge_position_minutes",
)

#: Link settings that are a switch, which ``--grid-json`` gives as ``true`` or ``false``.
SWITCHES = ("link_directional_edges",)

NEGATIVE_UPPER_LIMIT = 0.01
SAME_CALLSIGN_UPPER_LIMIT = 0.05
#: Of the holes a point links that have a known label.
REAL_FALSE_JOIN_LIMIT = 0.001
SAME_CALLSIGN_MIN_CONTROLS = 60
#: The realised depth, on at least one side, that puts a ``cruise_cruise`` control
#: under the 1% limit.
NEGATIVE_LIMIT_MIN_DEPTH_MIN = 30.0

#: What each grid row sums, so that pooling recomputes every rate and bound from
#: counts rather than averaging the days' rates.
COUNT_FIELDS = (
    "flights",
    "tracks_on_flights",
    "flights_in_one_track",
    "flights_with_no_track",
    "base_tracks_on_flights",
    "base_flights_in_one_track",
    "real_holes",
    "real_holes_linked",
    "linked_true_link",
    "linked_false_join",
    "linked_unknown",
    "linked_edge_read_through_blank",
    "false_joined_edge_read_through_blank",
    "linked_by_direction",
    "false_joined_by_direction",
    "negative_controls",
    "negative_pairs",
    "negative_false_joins",
    "negative_pairs_with_false_join",
    "negative_limit_controls",
    "negative_limit_false_joins",
    "negative_limit_pairs_with_false_join",
    "negative_limit_false_joins_no_callsign",
    "negative_shallow_controls",
    "negative_shallow_false_joins",
    "negative_over_gap_controls",
    "negative_low_edge_controls",
    "negative_low_edge_false_joins",
    "negative_low_edge_untested_controls",
    "same_callsign_controls",
    "same_callsign_false_joins",
    "same_callsign_blank",
    "same_callsign_shallow_controls",
    "same_callsign_shallow_false_joins",
    "positive_controls",
    "positive_within_gap",
    "positive_linked",
    "positive_low_edge_controls",
    "positive_low_edge_within_gap",
    "positive_low_edge_linked",
    "crossing_flights",
    "crossing_flights_in_one_track",
    "crossing_tracks",
    "base_crossing_flights_in_one_track",
    "base_crossing_tracks",
    "other_flights",
    "other_flights_in_one_track",
    "other_tracks",
    "base_other_flights_in_one_track",
    "base_other_tracks",
)

_STRATUM_KEYS = (
    "point_id", "kind", "control_family", "gap_stratum", "depth_min", "requested_gap_min",
    "same_callsign", "within_link_max_gap",
)

#: Enough to keep the many small per-point jobs from running hundreds of tasks.
_FEATURE_PARTITIONS = 16

_TRACK_COLUMNS = (
    "icao24", "event_time", "lat", "lon", "baro_altitude", "velocity", "vert_rate", "callsign",
)

_CODE_PATHS = [
    "benchmarks/track_link_grid.py",
    "benchmarks/track_link_controls.py",
    "benchmarks/track_truth.py",
    "benchmarks/track_fragmentation.py",
    "src/opdi/coverage.py",
    "src/opdi/pipeline/flights.py",
    "src/opdi/pipeline/segmentation/__init__.py",
    "src/opdi/pipeline/segmentation/base.py",
    "src/opdi/pipeline/segmentation/methods.py",
]

_NOTES = (
    "Re-segmentation of opdi-world/osn_tracks, not from raw. Flights inside one day; "
    "Network Manager truth, European."
)


# --- the grid ----------------------------------------------------------------


def _product(settings: Dict[str, Sequence[float]]) -> List[SegmentationParams]:
    names = list(settings)
    return [
        replace(SegmentationParams(), **dict(zip(names, values)))
        for values in itertools.product(*settings.values())
    ]


def grid_points() -> List[SegmentationParams]:
    """The study's 54 points. Every setting outside the grid stays at its default."""
    return _product(GRID)


def _link_fields() -> List[str]:
    return [f.name for f in fields(SegmentationParams) if f.name.startswith("link_")]


def grid_from_json(text: str) -> List[SegmentationParams]:
    """A grid from ``{setting: [values]}``, in place of :func:`grid_points`.

    Raises ``ValueError`` rather than running something other than what was asked:
    a misspelt setting would otherwise run the defaults and look like a result.
    """
    settings = json.loads(text)
    if not isinstance(settings, dict) or not settings:
        raise ValueError("--grid-json needs an object of {setting: [values]}")
    for name, values in settings.items():
        if name not in _link_fields():
            raise ValueError(f"{name} is not a link setting; choose from {_link_fields()}")
        if name in SWITCHES:
            if not isinstance(values, list) or not values or not all(isinstance(v, bool) for v in values):
                raise ValueError(f"{name} needs a non-empty list of true or false, got {values!r}")
        elif (
            not isinstance(values, list)
            or not values
            or not all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in values)
        ):
            raise ValueError(f"{name} needs a non-empty list of numbers, got {values!r}")
        if name in ONE_VALUE_PER_RUN and len(values) > 1:
            raise ValueError(
                f"{name} shapes the cuts and features, which are made once per run; "
                f"give it one value and run again for another, got {values!r}"
            )
    return _product(
        {
            name: values if name in SWITCHES else [float(v) for v in values]
            for name, values in settings.items()
        }
    )


def reasons_params_from_json(text: str, grid: List[SegmentationParams]) -> SegmentationParams:
    """The one point of ``--reasons-params-json``, ``{setting: value}``.

    Validated as :func:`grid_from_json` is, and for the same reason. The features are
    made once, with the grid's :data:`ONE_VALUE_PER_RUN`, so the point takes those from
    the grid, and asking for another value raises rather than reading the wrong features.
    """
    settings = json.loads(text)
    if not isinstance(settings, dict) or not settings:
        raise ValueError("--reasons-params-json needs an object of {setting: value}")
    [point] = grid_from_json(json.dumps({name: [value] for name, value in settings.items()}))
    for name in ONE_VALUE_PER_RUN:
        if name in settings and getattr(point, name) != getattr(grid[0], name):
            raise ValueError(
                f"{name} shapes the cuts and features, which this run makes at "
                f"{getattr(grid[0], name)}, so it cannot read reasons at {settings[name]}"
            )
    return replace(point, **{name: getattr(grid[0], name) for name in ONE_VALUE_PER_RUN})


def truth_months(day: str) -> List[str]:
    """The truth files for a day. A flight in the window can key to an adjacent day.

    See ``track_truth.load_flight_intervals``: ``months`` decides which files are read
    at all, so a day on a month's edge needs the adjacent month as well.
    """
    d = dt.date.fromisoformat(day)
    return sorted({(d + dt.timedelta(days=n)).strftime("%Y%m") for n in (-1, 0, 1)})


# --- counting one point ------------------------------------------------------


def _linked(p: SegmentationParams) -> Column:
    """The rule's verdict at *p*, read from a frame of stored features."""
    return linkable({name: F.col(name) for name in link_feature_columns(p)}, p)


def _checks(p: SegmentationParams) -> Dict[str, Column]:
    """The rule's tests at *p*, read from a frame of stored features."""
    return link_checks({name: F.col(name) for name in link_feature_columns(p)}, p)


def _linked_without_callsign(p: SegmentationParams) -> Column:
    """The rule's verdict at *p* with the callsign test removed and nothing else."""
    checks = _checks(p)
    del checks["callsign_agrees"]
    return reduce(and_, checks.values())


def _first_failed_test(checks: Dict[str, Column]) -> Column:
    """The name of the first test that fails, in the rule's order, or ``linked``."""
    tests = iter(checks.items())
    name, check = next(tests)
    first = F.when(~check, name)
    for name, check in tests:
        first = first.when(~check, name)
    return first.otherwise("linked")


def _sum_when(condition: Column) -> Column:
    return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0))


def real_hole_counts(holes: DataFrame, p: SegmentationParams) -> Dict[str, int]:
    """All labelled holes, and those *p* links by label. See the module docstring.

    The last four split out two ways a link rests on less than a plain cruise edge. A
    position read through a blank sample shows as ``position_gap_min`` differing from
    ``gap_min``. An edge below the floor can only have passed by direction, given a link.
    """
    linked, label = _linked(p), F.col("label")
    false_join = label == "false_join"
    read_through_blank = F.col("position_gap_min") != F.col("gap_min")
    by_direction = (F.col("alt_before_ft") < p.link_cruise_floor_ft) | (
        F.col("alt_after_ft") < p.link_cruise_floor_ft
    )
    row = holes.agg(
        F.count(F.lit(1)).alias("real_holes"),
        _sum_when(linked).alias("real_holes_linked"),
        _sum_when(linked & (label == "true_link")).alias("linked_true_link"),
        _sum_when(linked & (label == "false_join")).alias("linked_false_join"),
        _sum_when(linked & (label == "unknown")).alias("linked_unknown"),
        _sum_when(linked & read_through_blank).alias("linked_edge_read_through_blank"),
        _sum_when(linked & false_join & read_through_blank).alias("false_joined_edge_read_through_blank"),
        _sum_when(linked & by_direction).alias("linked_by_direction"),
        _sum_when(linked & false_join & by_direction).alias("false_joined_by_direction"),
    ).first()
    return row.asDict()


def negative_counts(controls: DataFrame, p: SegmentationParams) -> Dict[str, int]:
    """Negative controls: all, those under the 1% limit, and those kept out of it.

    Every control is exactly one of ``negative_limit_controls``,
    ``negative_shallow_controls`` (``cruise_cruise``, the gap is allowed, the depth too
    small), ``negative_over_gap_controls`` or ``negative_low_edge_untested_controls``
    (a low-edge family, the gap allowed, at a point with direction off). The same or
    blank callsign group is drawn from the controls under the limit, with its shallow
    controls counted apart.

    With direction off a low edge cannot link, since the cuts are below the floor. So
    those controls are no evidence about the point, and counted they would let a point
    over the limit on ``cruise_cruise`` pass on rejections that test nothing.
    ``negative_low_edge_controls`` are those under the limit, with their false joins.
    """
    linked = _linked(p)
    within_gap = F.col("gap_min") <= p.link_max_gap_minutes
    cruise_cruise = F.col("control_family") == "cruise_cruise"
    low_edge = ~cruise_cruise
    # `greatest` skips a NULL, so one side's depth is enough, as it is meant to be.
    deep = F.coalesce(
        F.greatest("old_depth_min", "new_depth_min") >= NEGATIVE_LIMIT_MIN_DEPTH_MIN, F.lit(False)
    )
    low_edge_counts = low_edge & F.lit(p.link_directional_edges)
    under_limit = within_gap & ((cruise_cruise & deep) | low_edge_counts)
    shallow = within_gap & cruise_cruise & ~deep
    untested = within_gap & low_edge & ~F.lit(p.link_directional_edges)
    same = F.col("same_callsign") & under_limit
    same_shallow = F.col("same_callsign") & shallow
    blank = F.col("cs_before").isNull() | F.col("cs_after").isNull()
    row = controls.agg(
        F.count(F.lit(1)).alias("negative_controls"),
        F.countDistinct("pair_id").alias("negative_pairs"),
        _sum_when(linked).alias("negative_false_joins"),
        F.countDistinct(F.when(linked, F.col("pair_id"))).alias("negative_pairs_with_false_join"),
        _sum_when(under_limit).alias("negative_limit_controls"),
        _sum_when(under_limit & linked).alias("negative_limit_false_joins"),
        F.countDistinct(F.when(under_limit & linked, F.col("pair_id"))).alias(
            "negative_limit_pairs_with_false_join"
        ),
        _sum_when(under_limit & _linked_without_callsign(p)).alias(
            "negative_limit_false_joins_no_callsign"
        ),
        _sum_when(shallow).alias("negative_shallow_controls"),
        _sum_when(shallow & linked).alias("negative_shallow_false_joins"),
        _sum_when(~within_gap).alias("negative_over_gap_controls"),
        _sum_when(under_limit & low_edge).alias("negative_low_edge_controls"),
        _sum_when(under_limit & low_edge & linked).alias("negative_low_edge_false_joins"),
        _sum_when(untested).alias("negative_low_edge_untested_controls"),
        _sum_when(same).alias("same_callsign_controls"),
        _sum_when(same & linked).alias("same_callsign_false_joins"),
        _sum_when(same & blank).alias("same_callsign_blank"),
        _sum_when(same_shallow).alias("same_callsign_shallow_controls"),
        _sum_when(same_shallow & linked).alias("same_callsign_shallow_false_joins"),
    ).first()
    return row.asDict()


def positive_counts(controls: DataFrame, p: SegmentationParams) -> Dict[str, int]:
    """Positive controls, those the point allows, and those it links, per kind of edge.

    The ``low_edge`` family apart, because with direction off it is missed entirely,
    and that says nothing about how the rule does on a cruise.
    """
    linked = _linked(p)
    within_gap = F.col("gap_min") <= p.link_max_gap_minutes
    low_edge = F.col("control_family") == "low_edge"
    row = controls.agg(
        _sum_when(~low_edge).alias("positive_controls"),
        _sum_when(~low_edge & within_gap).alias("positive_within_gap"),
        _sum_when(~low_edge & linked).alias("positive_linked"),
        _sum_when(low_edge).alias("positive_low_edge_controls"),
        _sum_when(low_edge & within_gap).alias("positive_low_edge_within_gap"),
        _sum_when(low_edge & linked).alias("positive_low_edge_linked"),
    ).first()
    return row.asDict()


def _finish_stratum(row: Dict) -> Dict:
    """Add the share of wrong decisions, and for negative controls the upper bound."""
    n, wrong = row["n"], row["wrong"]
    upper = rule_of_three_upper(wrong, n) if row["kind"] == "negative" else None
    return {**row, "wrong_share": wrong / n if n else None, "wrong_upper_95": upper}


def strata_counts(controls: DataFrame, kind: str, p: SegmentationParams, point_id: int) -> List[Dict]:
    """Controls per family and stratum at one point. ``wrong`` is the decision not wanted.

    For a ``negative`` control that is a link, for a ``positive`` one no link. Controls
    over the point's ``link_max_gap_minutes`` have ``within_link_max_gap`` false, which
    keeps a correct rejection, or a positive control the point never allowed, apart.
    """
    within_gap = F.col("gap_min") <= p.link_max_gap_minutes
    wanted = ~_linked(p) if kind == "negative" else _linked(p)
    size = "depth_min" if kind == "negative" else "requested_gap_min"
    rows = (
        controls.withColumn("within_link_max_gap", within_gap)
        .groupBy("control_family", size, "gap_stratum", "same_callsign", "within_link_max_gap")
        .agg(F.count(F.lit(1)).alias("n"), _sum_when(~wanted).alias("wrong"))
        .orderBy("control_family", size, "gap_stratum", "same_callsign", "within_link_max_gap")
        .collect()
    )
    return [
        _finish_stratum(
            {
                "point_id": point_id,
                "kind": kind,
                "control_family": r["control_family"],
                "gap_stratum": r["gap_stratum"],
                "depth_min": r[size] if kind == "negative" else None,
                "requested_gap_min": r[size] if kind == "positive" else None,
                "same_callsign": r["same_callsign"],
                "within_link_max_gap": r["within_link_max_gap"],
                "n": r["n"],
                "wrong": r["wrong"],
            }
        )
        for r in rows
    ]


def flight_base_tracks(samples: DataFrame, truth: DataFrame, p: SegmentationParams) -> DataFrame:
    """``flight_key`` and ``base_tracks``, the A9 tracks with a sample in the flight.

    Every truth flight stays, at zero when it has no sample: a flight OPDI never saw
    is a flight it failed to put in one track.
    """
    tracked = assign_track_id(samples, debounced(), p).select(
        F.col("icao24").alias("_sample_icao24"),
        F.to_timestamp("event_time").alias("_ts"),
        "track_id",
    )
    flights = truth.select("flight_key", "icao24", "t_off", "t_land").distinct()
    return (
        flights.join(
            tracked,
            (F.col("icao24") == F.col("_sample_icao24"))
            & F.col("_ts").between(F.col("t_off"), F.col("t_land")),
            "left",
        )
        .groupBy("flight_key")
        .agg(F.countDistinct("track_id").alias("base_tracks"))
    )


def holes_inside_flights(holes: DataFrame, truth: DataFrame) -> DataFrame:
    """Each hole with both edges inside a truth flight, under that flight's key."""
    flights = truth.select(
        "flight_key", F.col("icao24").alias("_truth_icao24"), "t_off", "t_land"
    ).distinct()
    return (
        holes.join(
            flights,
            (F.col("icao24") == F.col("_truth_icao24"))
            & (F.col("t_off") <= F.col("t_old_edge"))
            & (F.col("t_new_edge") <= F.col("t_land")),
        )
        .drop("_truth_icao24", "t_off", "t_land")
        .dropDuplicates(["flight_key", "t_old_edge", "t_new_edge"])
    )


def _flights_with_tracks(flights: DataFrame, holes_in_flights: DataFrame, p: SegmentationParams) -> DataFrame:
    """*flights* with ``_tracks``, each flight's A9 tracks less its holes *p* links."""
    linked = (
        holes_in_flights.filter(_linked(p))
        .groupBy("flight_key")
        .agg(F.count(F.lit(1)).alias("_linked_holes"))
    )
    tracks = F.col("base_tracks") - F.coalesce(F.col("_linked_holes"), F.lit(0))
    return flights.join(linked, on="flight_key", how="left").withColumn("_tracks", tracks)


def flight_track_counts(flights: DataFrame, holes_in_flights: DataFrame, p: SegmentationParams) -> Dict[str, int]:
    """Tracks on truth flights at *p*: each flight's A9 tracks less its linked holes.

    The ``base_`` counts are A9 itself, what the current rule scores, so the gain of
    every point is in its row.
    """
    row = (
        _flights_with_tracks(flights, holes_in_flights, p)
        .agg(
            F.count(F.lit(1)).alias("flights"),
            F.coalesce(F.sum("_tracks"), F.lit(0)).alias("tracks_on_flights"),
            _sum_when(F.col("_tracks") == 1).alias("flights_in_one_track"),
            _sum_when(F.col("_tracks") == 0).alias("flights_with_no_track"),
            F.coalesce(F.sum("base_tracks"), F.lit(0)).alias("base_tracks_on_flights"),
            _sum_when(F.col("base_tracks") == 1).alias("base_flights_in_one_track"),
        )
        .first()
    )
    return row.asDict()


# --- the two populations -----------------------------------------------------


def with_population(truth: DataFrame, airports: DataFrame) -> DataFrame:
    """Add ``crosses_box``: exactly one of ADEP and ADES is inside ``EUROPE_BBOX``.

    The Spark form of ``adsc_flight_list_truth.label_far_end``, whose two crossing flows
    this is. An ident ``airports`` does not place counts as outside, which is where NM's
    ZZZZ and AFIL go. Trimmed, as that function's reader trims and
    ``load_flight_intervals`` does not. ``airports`` has the ``oa_airports`` columns.
    """
    min_lon, min_lat, max_lon, max_lat = EUROPE_BBOX
    in_box = airports.where(
        F.col("longitude_deg").between(min_lon, max_lon)
        & F.col("latitude_deg").between(min_lat, max_lat)
    ).select("ident").distinct()

    def inside(frame: DataFrame, column: str, flag: str) -> DataFrame:
        idents = F.broadcast(
            in_box.select(F.col("ident").alias(f"{flag}_ident"), F.lit(True).alias(flag))
        )
        return frame.join(idents, F.trim(F.col(column)) == F.col(f"{flag}_ident"), "left").drop(f"{flag}_ident")

    return (
        inside(inside(truth, "gt_adep", "_adep_in_box"), "gt_ades", "_ades_in_box")
        .withColumn(
            "crosses_box",
            F.coalesce(F.col("_adep_in_box"), F.lit(False)) != F.coalesce(F.col("_ades_in_box"), F.lit(False)),
        )
        .drop("_adep_in_box", "_ades_in_box")
    )


def population_track_counts(
    flights: DataFrame, holes_in_flights: DataFrame, p: SegmentationParams
) -> Dict[str, int]:
    """:func:`flight_track_counts` for each population. *flights* has ``crosses_box``."""
    tracks, base = F.col("_tracks"), F.col("base_tracks")
    aggregates = []
    for name, member in (("crossing", F.col("crosses_box")), ("other", ~F.col("crosses_box"))):
        aggregates += [
            _sum_when(member).alias(f"{name}_flights"),
            _sum_when(member & (tracks == 1)).alias(f"{name}_flights_in_one_track"),
            F.coalesce(F.sum(F.when(member, tracks)), F.lit(0)).alias(f"{name}_tracks"),
            _sum_when(member & (base == 1)).alias(f"base_{name}_flights_in_one_track"),
            F.coalesce(F.sum(F.when(member, base)), F.lit(0)).alias(f"base_{name}_tracks"),
        ]
    return _flights_with_tracks(flights, holes_in_flights, p).agg(*aggregates).first().asDict()


# --- why a hole stays split ---------------------------------------------------


def true_link_holes(holes: DataFrame, truth: DataFrame) -> DataFrame:
    """The labelled holes inside one truth flight, one row each, with ``population``.

    NM can hold two rows over one flight, so a hole can lie inside two: it counts once,
    as ``crosses_box`` if either row does, since that is the population at stake.
    """
    key = ["icao24", "t_old_edge", "t_new_edge"]
    inside = holes.filter(F.col("label") == "true_link")
    flights = truth.select(
        F.col("icao24").alias("_truth_icao24"), "t_off", "t_land", "crosses_box"
    ).distinct()
    populations = (
        inside.select(*key)
        .join(
            flights,
            (F.col("icao24") == F.col("_truth_icao24"))
            & (F.col("t_off") <= F.col("t_old_edge"))
            & (F.col("t_new_edge") <= F.col("t_land")),
        )
        .groupBy(*key)
        .agg(F.max("crosses_box").alias("_crosses_box"))
    )
    return (
        inside.join(populations, on=key)
        .withColumn("population", F.when(F.col("_crosses_box"), "crosses_box").otherwise("other"))
        .drop("_crosses_box")
    )


def _gap_band(gap_min: Column) -> Column:
    """The length bands of the reasons. Upper edges belong to the lower one."""
    return (
        F.when(gap_min <= 60.0, "30-60")
        .when(gap_min <= 120.0, "60-120")
        .when(gap_min <= 240.0, "120-240")
        .when(gap_min <= 360.0, "240-360")
        .otherwise("over-360")
    )


def reason_counts(holes: DataFrame, p: SegmentationParams) -> List[Dict]:
    """Holes per population, length band and first failed test at *p*.

    A hole failing two tests counts under the first, and not in
    ``only_failed_test_holes``: relaxing either alone would still not link it.
    """
    checks = _checks(p)
    failed = sum(F.when(check, 0).otherwise(1) for check in checks.values())
    rows = (
        holes.withColumn("gap_band", _gap_band(F.col("gap_min")))
        .withColumn("first_failed_test", _first_failed_test(checks))
        .groupBy("population", "gap_band", "first_failed_test")
        .agg(F.count(F.lit(1)).alias("holes"), _sum_when(failed == 1).alias("only_failed_test_holes"))
        .orderBy("population", "gap_band", "first_failed_test")
        .collect()
    )
    return [r.asDict() for r in rows]


def positive_reason_counts(controls: DataFrame, p: SegmentationParams) -> List[Dict]:
    """Positive controls *p* misses, per family, requested length and first failed test."""
    rows = (
        controls.withColumn("first_failed_test", _first_failed_test(_checks(p)))
        .filter(F.col("first_failed_test") != "linked")
        .groupBy("control_family", "requested_gap_min", "first_failed_test")
        .agg(F.count(F.lit(1)).alias("positive_missed_by_test"))
        .orderBy("control_family", "requested_gap_min", "first_failed_test")
        .collect()
    )
    return [r.asDict() for r in rows]


# --- the safety limits and the pick ------------------------------------------


def _share(part: int, whole: int) -> Optional[float]:
    return part / whole if whole else None


def with_verdicts(row: Dict) -> Dict:
    """The row's counts, with every rate, bound and safety limit verdict derived."""
    known = row["linked_true_link"] + row["linked_false_join"]
    negative_upper = rule_of_three_upper(
        row["negative_limit_false_joins"], row["negative_limit_controls"]
    )
    same_n = row["same_callsign_controls"]
    same_upper = rule_of_three_upper(row["same_callsign_false_joins"], same_n)
    real_rate = _share(row["linked_false_join"], known)

    if same_n < SAME_CALLSIGN_MIN_CONTROLS:
        same_status = "insufficient"
    elif same_upper <= SAME_CALLSIGN_UPPER_LIMIT:
        same_status = "within_limit"
    else:
        same_status = "over_limit"
    negative_ok = negative_upper is not None and negative_upper <= NEGATIVE_UPPER_LIMIT
    real_ok = real_rate is not None and real_rate <= REAL_FALSE_JOIN_LIMIT
    missed = row["positive_within_gap"] - row["positive_linked"]

    population_shares = {}
    for name in ("crossing", "other"):
        n = row[f"{name}_flights"]
        population_shares.update(
            {
                f"{name}_share_one_track": _share(row[f"{name}_flights_in_one_track"], n),
                f"{name}_tracks_per_flight": _share(row[f"{name}_tracks"], n),
                f"base_{name}_share_one_track": _share(row[f"base_{name}_flights_in_one_track"], n),
                f"base_{name}_tracks_per_flight": _share(row[f"base_{name}_tracks"], n),
            }
        )

    return {
        **row,
        "tracks_per_flight": _share(row["tracks_on_flights"], row["flights"]),
        "share_one_track": _share(row["flights_in_one_track"], row["flights"]),
        "base_tracks_per_flight": _share(row["base_tracks_on_flights"], row["flights"]),
        "base_share_one_track": _share(row["base_flights_in_one_track"], row["flights"]),
        "linked_unknown_share": _share(row["linked_unknown"], row["real_holes_linked"]),
        "real_false_join_rate": real_rate,
        "negative_false_join_rate": _share(
            row["negative_limit_false_joins"], row["negative_limit_controls"]
        ),
        "negative_false_join_upper_95": negative_upper,
        "negative_no_callsign_false_join_rate": _share(
            row["negative_limit_false_joins_no_callsign"], row["negative_limit_controls"]
        ),
        "negative_no_callsign_upper_95": rule_of_three_upper(
            row["negative_limit_false_joins_no_callsign"], row["negative_limit_controls"]
        ),
        "same_callsign_false_join_rate": _share(row["same_callsign_false_joins"], same_n),
        "same_callsign_upper_95": same_upper,
        "same_callsign_blank_share": _share(row["same_callsign_blank"], same_n),
        "positive_missed_share": _share(missed, row["positive_within_gap"]),
        "positive_low_edge_missed_share": _share(
            row["positive_low_edge_within_gap"] - row["positive_low_edge_linked"],
            row["positive_low_edge_within_gap"],
        ),
        **population_shares,
        "negative_within_limit": negative_ok,
        "same_callsign_status": same_status,
        "real_holes_within_limit": real_ok,
        "within_limits": negative_ok and real_ok and same_status != "over_limit",
        "provisional": same_status == "insufficient",
    }


def pick_recommended(rows: List[Dict]) -> Optional[Dict]:
    """The row within the limits that best serves the owner's goal, or ``None``.

    The most flights crossing the European box in one track, as those are the flights
    the rule exists for, then the most flights in one track overall. A tie then goes
    to the more conservative settings, as a wrongly glued flight is worse than a split
    one: the higher ``link_min_implied_speed_kt``, the shorter ``link_max_gap_minutes``,
    the higher ``link_min_speed_ratio``, the higher ``link_min_stop_minutes``, then
    ``link_directional_edges`` off. The ratio and stop are there because the grid lists
    each off setting first, so without them a tie would fall to the point with those
    checks off. A tie that far means direction linked nothing more. The false-join bound is not
    compared here, because a point inside the limits has already met it. The first point
    takes any tie left, so the pick is deterministic.
    """
    passing = [r for r in rows if r["within_limits"]]
    if not passing:
        return None
    return min(
        passing,
        key=lambda r: (
            -r["crossing_flights_in_one_track"],
            -r["flights_in_one_track"],
            -r["link_min_implied_speed_kt"],
            r["link_max_gap_minutes"],
            -r["link_min_speed_ratio"],
            -r["link_min_stop_minutes"],
            r["link_directional_edges"],
            r["point_id"],
        ),
    )


def lowest_bound_point(rows: List[Dict]) -> Optional[Dict]:
    """The row with the lowest false-join bound, or ``None`` with no controls at all.

    Where Stage 2 runs if nothing is recommended. A tie goes to the most flights in
    one track, which leaves Stage 2 the most linked tracks to examine.
    """
    bounded = [r for r in rows if r["negative_false_join_upper_95"] is not None]
    if not bounded:
        return None
    return min(
        bounded,
        key=lambda r: (r["negative_false_join_upper_95"], -r["flights_in_one_track"], r["point_id"]),
    )


def recommendation(rows: List[Dict]) -> Dict:
    """What the study concludes: the chosen row, under a ``status`` that says how."""
    picked = pick_recommended(rows)
    if picked is not None:
        status = "recommended_provisional" if picked["provisional"] else "recommended"
        return {"status": status, **picked}
    lowest = lowest_bound_point(rows)
    if lowest is not None:
        return {"status": "not_recommended_lowest_bound", **lowest}
    return {"status": "no_evidence"}


# --- pooling -----------------------------------------------------------------


def pool_rows(days: List[List[Dict]]) -> List[Dict]:
    """One row per point, with counts summed over the days and verdicts recomputed."""
    pooled = {}
    for row in itertools.chain.from_iterable(days):
        into = pooled.get(row["point_id"])
        if into is None:
            pooled[row["point_id"]] = {**row, "day": "pooled"}
        else:
            for name in COUNT_FIELDS:
                into[name] += row[name]
    return [with_verdicts(pooled[point_id]) for point_id in sorted(pooled)]


def pool_strata(days: List[List[Dict]]) -> List[Dict]:
    """One row per point and stratum, with counts summed over the days."""
    pooled = {}
    for row in itertools.chain.from_iterable(days):
        key = tuple(row[k] for k in _STRATUM_KEYS)
        into = pooled.get(key)
        if into is None:
            pooled[key] = {**row, "day": "pooled"}
        else:
            into["n"] += row["n"]
            into["wrong"] += row["wrong"]
    return [_finish_stratum(row) for row in pooled.values()]


def pool_counts(days: Iterable[List[Dict]], count_names: Sequence[str]) -> List[Dict]:
    """One row per key, the key being every field but ``day`` and *count_names*."""
    pooled = {}
    for row in itertools.chain.from_iterable(days):
        key = tuple(v for name, v in row.items() if name != "day" and name not in count_names)
        into = pooled.get(key)
        if into is None:
            pooled[key] = {**row, "day": "pooled"}
        else:
            for name in count_names:
                into[name] += row[name]
    return list(pooled.values())


# --- one day -----------------------------------------------------------------


@contextmanager
def _timed(stage: str):
    """Print how long a stage took, so a real day shows where the time goes."""
    started = time.perf_counter()
    yield
    print(f"{stage}: {time.perf_counter() - started:.0f} s", flush=True)


def _keep(frame: DataFrame) -> DataFrame:
    """Build a small frame now and hold it, for the many jobs to come."""
    kept = frame.repartition(_FEATURE_PARTITIONS).persist()
    kept.count()
    return kept


@dataclass
class ReasonFrames:
    """What the reasons are counted on, kept until the days are pooled and a point chosen."""

    true_link_holes: DataFrame
    positive: DataFrame

    def release(self) -> None:
        self.true_link_holes.unpersist()
        self.positive.unpersist()


@dataclass
class DayFeatures:
    """Everything a grid point is scored on, for one day, built once."""

    holes: DataFrame
    negative: DataFrame
    positive: DataFrame
    flights: DataFrame
    holes_in_flights: DataFrame
    funnel: Dict
    reasons: ReasonFrames

    def release(self) -> None:
        """Free all but the :class:`ReasonFrames`, which outlive the day."""
        for frame in (self.holes, self.negative, self.flights, self.holes_in_flights):
            frame.unpersist()


def _controls_per_family(controls: DataFrame) -> Dict[str, int]:
    return {r["control_family"]: r["count"] for r in controls.groupBy("control_family").count().collect()}


def build_features(tracks: DataFrame, truth: DataFrame, p: SegmentationParams) -> DayFeatures:
    """Label the day's holes, make its controls and count each flight's A9 tracks.

    *p* gives what the features depend on, which a grid holds constant. Real holes need
    the whole day and are read once. Everything scored against truth needs only the
    airframes truth names, and the controls need only the samples inside its flights.
    Those are cached first, because the controls and the funnel pass over them some
    dozens of times.
    """
    tracks = tracks.select(*_TRACK_COLUMNS)
    floor_ft = p.link_cruise_floor_ft
    with _timed("truth"):
        truth = _keep(truth.select("flight_key", "icao24", "t_off", "t_land", "crosses_box"))

    with _timed("real holes"):
        # Kept before labelling, which reads its input twice.
        candidates = _keep(real_hole_candidates(tracks, p))
        holes = _keep(label_real_holes(candidates, truth))
        candidates.unpersist()

    with _timed("samples of truth's airframes"):
        airframes = tracks.join(
            truth.select("icao24").distinct(), on="icao24", how="left_semi"
        ).persist(StorageLevel.MEMORY_AND_DISK)
        airframes.count()
    with _timed("A9 tracks per flight"):
        flights = _keep(
            flight_base_tracks(airframes, truth, p).join(
                truth.select("flight_key", "crosses_box").distinct(), on="flight_key"
            )
        )
    with _timed("samples inside flights"):
        samples = flight_samples(airframes, truth).persist(StorageLevel.MEMORY_AND_DISK)
        samples.count()
        airframes.unpersist()

    with _timed("negative controls"):
        negative = _keep(negative_controls(airframes, truth, floor_ft, p=p, samples=samples))
    with _timed("positive controls"):
        positive = _keep(positive_controls(airframes, truth, floor_ft, p=p, samples=samples))
    with _timed("control funnel"):
        negative_scored = _controls_per_family(negative)
        positive_scored = _controls_per_family(positive)
        funnel = {
            **control_funnel(airframes, truth, floor_ft, p=p, samples=samples),
            "negative_controls_scored": negative_scored.get("cruise_cruise", 0),
            **{
                f"{family}_controls_scored": negative_scored.get(family, 0)
                for family in LOW_EDGE_FAMILIES
            },
            "positive_controls_scored": positive_scored.get("cruise_cruise", 0),
            "positive_low_edge_controls_scored": positive_scored.get("low_edge", 0),
        }
    holes_in_flights = _keep(holes_inside_flights(holes, truth))
    reasons = ReasonFrames(_keep(true_link_holes(holes, truth)), positive)

    samples.unpersist()
    truth.unpersist()
    return DayFeatures(holes, negative, positive, flights, holes_in_flights, funnel, reasons)


def score_day(day: str, tracks: DataFrame, truth: DataFrame, grid: List[SegmentationParams]) -> tuple:
    """Grid rows, strata rows, the funnel row and the frames the reasons need, for one day."""
    print(f"--- {day}", flush=True)
    with _timed(f"{day}: features"):
        features = build_features(tracks, truth, grid[0])
    rows, strata = [], []
    with _timed(f"{day}: grid of {len(grid)} points"):
        for point_id, p in enumerate(grid):
            rows.append(
                with_verdicts(
                    {
                        "day": day,
                        "point_id": point_id,
                        **{name: getattr(p, name) for name in _link_fields()},
                        **flight_track_counts(features.flights, features.holes_in_flights, p),
                        **population_track_counts(features.flights, features.holes_in_flights, p),
                        **real_hole_counts(features.holes, p),
                        **negative_counts(features.negative, p),
                        **positive_counts(features.positive, p),
                    }
                )
            )
            for kind, controls in (("negative", features.negative), ("positive", features.positive)):
                strata += [{"day": day, **s} for s in strata_counts(controls, kind, p, point_id)]
    funnel = {"day": day, **features.funnel}
    features.release()
    return rows, strata, funnel, features.reasons


def write_reasons(
    frames_per_day: Dict[str, ReasonFrames],
    p: SegmentationParams,
    settings_from: str,
    stage: Callable[..., None],
) -> None:
    """Stage both reasons reports for each day and pooled, each row naming its settings."""
    settings = {"settings_from": settings_from, **{name: getattr(p, name) for name in _link_fields()}}
    reports = (
        (
            "link_reasons",
            lambda frames: reason_counts(frames.true_link_holes, p),
            ("population", "gap_band", "first_failed_test"),
            ("holes", "only_failed_test_holes"),
        ),
        (
            "link_positive_reasons",
            lambda frames: positive_reason_counts(frames.positive, p),
            ("control_family", "requested_gap_min", "first_failed_test"),
            ("positive_missed_by_test",),
        ),
    )
    for stem, count, key_names, count_names in reports:
        # Named here because a day can have no rows, and a CSV keeps its header then.
        columns = ["day", *settings, *key_names, *count_names]
        per_day = []
        for day, frames in frames_per_day.items():
            per_day.append([{"day": day, **settings, **row} for row in count(frames)])
            stage(f"{stem}_{day}.csv", per_day[-1], columns)
        stage(f"{stem}_pooled.csv", pool_counts(per_day, count_names), columns)


def two_callsign_counts(tracks: DataFrame, p: SegmentationParams) -> Dict[str, int]:
    """The V3 two-callsign counts for one day, at *p* and for A9 beside it.

    Reported and not limited, since longer tracks raise them with no merge: see the
    spec. A9 is what gives the two numbers a meaning.
    """
    tracks = tracks.select(*_TRACK_COLUMNS)
    counts = {}
    for arm, prefix in (("cruise_linked", ""), ("debounced", "debounced_")):
        m = measure(tracks, arm, p.callsign_min_persistence_seconds, params=p)
        for name in ("tracks_2plus_raw_callsigns", "tracks_2plus_persisted_callsigns"):
            counts[prefix + name] = m[name]
    return counts


# --- the study ---------------------------------------------------------------


def run_study(
    days: Sequence[str],
    grid: List[SegmentationParams],
    load_tracks: Callable[[str], DataFrame],
    load_truth: Callable[[str], DataFrame],
    results_dir: Path,
    argv: Sequence[str] = (),
    input_tables: Sequence[str] = (),
    reference_base: str = REFERENCE_BASE,
    reasons_params: Optional[SegmentationParams] = None,
) -> Dict:
    """Score each day, pool them, choose, and stage every CSV with its provenance.

    ``load_truth`` gives flights with ``crosses_box``: see :func:`with_population`.
    ``reasons_params`` reads the reasons there rather than at the chosen point.
    """
    results_dir = Path(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    def stage(name: str, rows: List[Dict], columns: Optional[List[str]] = None) -> None:
        with (results_dir / name).open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=columns or list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        provenance.record(
            results_dir, name, "benchmarks/track_link_grid.py", list(argv), _CODE_PATHS,
            inputs={
                "days": list(days),
                "grid_points": len(grid),
                "truth_months": sorted({m for day in days for m in truth_months(day)}),
                "reference_base": reference_base,
            },
            notes=_NOTES,
            input_tables=list(input_tables),
        )
        print(f"wrote {results_dir / name}", flush=True)

    all_rows, all_strata, reason_frames = [], [], {}
    for day in days:
        rows, strata, funnel, reason_frames[day] = score_day(
            day, load_tracks(day), load_truth(day), grid
        )
        all_rows.append(rows)
        all_strata.append(strata)
        stage(f"link_grid_{day}.csv", rows)
        stage(f"link_controls_{day}.csv", strata)
        stage(f"link_funnel_{day}.csv", [funnel])

    pooled = pool_rows(all_rows)
    stage("link_grid_pooled.csv", pooled)
    stage("link_controls_pooled.csv", pool_strata(all_strata))

    # Staged before the heavy counts below, so losing those loses nothing else.
    choice = recommendation(pooled)
    stage("link_recommended.csv", [choice])
    print(f"choice: {choice['status']}, point {choice.get('point_id')}", flush=True)

    if reasons_params is not None:
        write_reasons(reason_frames, reasons_params, "given", stage)
    elif "point_id" in choice:
        write_reasons(reason_frames, grid[choice["point_id"]], choice["status"], stage)
    else:
        print("no reasons: nothing chosen and no settings given", flush=True)
    for frames in reason_frames.values():
        frames.release()

    if "point_id" in choice:
        totals: Dict[str, int] = {}
        for day in days:
            with _timed(f"{day}: two-callsign counts"):
                counts = two_callsign_counts(load_tracks(day), grid[choice["point_id"]])
            print(f"{day}: {counts}", flush=True)
            for name, value in counts.items():
                totals[name] = totals.get(name, 0) + value
        choice = {**choice, **totals}
        stage("link_recommended.csv", [choice])
    return choice


def read_tracks(spark, source: str, day: str) -> DataFrame:
    """One ``dof`` partition of a tracks table: the tracks that start on *day*."""
    return (
        spark.read.parquet(source)
        .filter(F.col("dof").cast("date") == F.lit(day).cast("date"))
        .withColumn("icao24", F.lower("icao24"))
    )


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--days", nargs="+", default=list(DEFAULT_DAYS))
    ap.add_argument("--source", default=SOURCE)
    ap.add_argument("--reference-base", default=REFERENCE_BASE)
    ap.add_argument("--airports", default=AIRPORTS, help="oa_airports, to place ADEP and ADES")
    ap.add_argument("--results-dir", type=Path, required=True)
    ap.add_argument("--grid-json", default=None, help="replace the grid: see the docstring")
    ap.add_argument(
        "--reasons-params-json", default=None, help="read the reasons here: see the docstring"
    )
    ap.add_argument("--executors", type=int, default=8)
    args = ap.parse_args()

    # Before the session exists, so a bad grid costs nothing.
    grid = grid_from_json(args.grid_json) if args.grid_json else grid_points()
    reasons_params = (
        reasons_params_from_json(args.reasons_params_json, grid) if args.reasons_params_json else None
    )

    import osn_sample
    from osn_sample import build_spark, load_dotenv

    load_dotenv()
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(4, "8g", distributed=True)
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    def load_truth(day: str) -> DataFrame:
        return with_population(
            load_flight_intervals(
                spark, truth_months(day), [day], reference_base=args.reference_base
            ),
            spark.read.parquet(args.airports),
        )

    try:
        run_study(
            args.days, grid, lambda day: read_tracks(spark, args.source, day), load_truth,
            args.results_dir,
            argv=sys.argv[1:],
            input_tables=[f"{args.source}/dof={day}" for day in args.days] + [args.airports],
            reference_base=args.reference_base,
            reasons_params=reasons_params,
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
