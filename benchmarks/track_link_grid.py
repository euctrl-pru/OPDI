#!/usr/bin/env python
"""Which A10 parameters link the most flights while staying within the safety limits?

Stage 1 of the ``cruise_linked`` study (spec 2026-11-03). Each grid point's link
decision is applied to real reception holes and to the holes of
``track_link_controls``, and the point with the most truth flights in one track is
picked among those within three safety limits, on numbers pooled over the days:

  negative controls   the upper bound on the false-join rate is at most 1%, over all
                      controls. One longer than the point's ``link_max_gap_minutes``
                      is a correct rejection and counts in that total. The bound is
                      per control, but depths of one pair are not independent, so
                      the count of pairs with a false join is reported beside it.
  same or blank       the same bound is at most 5% among the controls the callsign
  callsign            test cannot reject, counting only gaps the point allows. Under
                      60 of them reads ``insufficient``: it does not fail the point,
                      and it marks the choice provisional. This is a count and not a
                      reading of the bound, which sits just under 5% at 59.
  real holes          false joins are at most 1% of the holes the point links that
                      have a known label. Only a linked hole counts: there both edges
                      are at cruise, which is what makes a landing or take-off inside
                      it the proof of a stop. Unknown labels are reported, not scored.

A point with nothing to measure it on is not within the limits. If no point passes,
none is recommended, and ``link_recommended.csv`` names the point with the lowest
bound, labelled not recommended, for Stage 2 to run at.

**Why 54 points do not need 54 segmentations.** A10 breaks where A9 breaks, except at
a gap over ``gap_minutes`` that links. Nothing else about the rule depends on a link:
a callsign break is evaluated over the whole airframe and is not "recent" across such
a gap, and a low-altitude break cannot fire where the new edge must be at cruise. So
a flight's tracks under a point are its A9 tracks minus the holes linked inside it,
and ``test_counting_from_decisions_agrees_with_really_re_segmenting`` holds this
against A10 really being run. A hole counts for a flight only with both edges inside
the flight's interval: otherwise the flight's samples all lie on one side of it.

**Not from raw.** This re-segments ``opdi-world/osn_tracks``, as
``track_fragmentation.py`` does, and that table holds only tracks starting on the day.
So the scored population is flights that take off and land on the day, which leans
westbound. Truth is European, so most of the world's holes stay ``unknown``.

**Cost.** One day is about 150M rows. The feature frames are built once per day and
are small, and each point then runs a few jobs over them, so the grid costs minutes
per day. The heavy steps are the feature windows over the whole day (once per day)
and the two-callsign counts, which segment the whole day under both arms, once per
day, for the chosen point only. Nothing is written to S3. Results go to a local
directory, and each day's are on disk before the next day starts.

    python benchmarks/track_link_grid.py --days 2026-06-14 2025-06-14 \
        --results-dir benchmarks/out/track_link_grid

``--grid-json`` replaces the grid for a later round, naming ``link_*`` settings:

    --grid-json '{"link_min_implied_speed_kt": [300, 350], "link_max_gap_minutes": [360]}'

Settings it does not name keep their defaults. Those that shape the cuts and features
(:data:`ONE_VALUE_PER_RUN`) may take only one value, so vary them across runs.
"""

import argparse
import csv
import datetime as dt
import itertools
import json
import sys
from dataclasses import dataclass, fields, replace
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

import provenance
from track_fragmentation import measure
from track_link_controls import (
    control_funnel,
    label_real_holes,
    negative_controls,
    positive_controls,
    real_hole_candidates,
    rule_of_three_upper,
)
from track_truth import REFERENCE_BASE, load_flight_intervals

from opdi.pipeline.segmentation import SegmentationParams, assign_track_id
from opdi.pipeline.segmentation.methods import debounced, link_feature_columns, linkable

SOURCE = "s3a://eurocontrol/opdi-world/osn_tracks"
DEFAULT_DAYS = ("2026-06-14", "2025-06-14")

#: The study's grid, 3 x 2 x 3 x 3 = 54 points.
GRID = {
    "link_min_implied_speed_kt": (250.0, 315.0, 400.0),
    "link_min_speed_ratio": (0.0, 0.85),
    "link_min_stop_minutes": (0.0, 20.0, 35.0),
    "link_max_gap_minutes": (120.0, 240.0, 360.0),
}

#: Settings the cuts and features are made with, once per run, so they cannot vary
#: inside one. ``link_cruise_floor_ft`` is both a threshold and where controls are cut.
ONE_VALUE_PER_RUN = (
    "link_cruise_floor_ft",
    "link_callsign_lookahead_minutes",
    "link_callsign_lookback_minutes",
)

NEGATIVE_UPPER_LIMIT = 0.01
SAME_CALLSIGN_UPPER_LIMIT = 0.05
REAL_HOLE_FALSE_JOIN_LIMIT = 0.01
SAME_CALLSIGN_MIN_CONTROLS = 60

#: What each grid row sums, so that pooling recomputes every rate and bound from
#: counts rather than averaging the days' rates.
COUNT_FIELDS = (
    "flights",
    "tracks_on_flights",
    "flights_in_one_track",
    "flights_with_no_track",
    "real_holes",
    "real_holes_linked",
    "linked_true_link",
    "linked_false_join",
    "linked_unknown",
    "negative_controls",
    "negative_pairs",
    "negative_within_gap",
    "negative_false_joins",
    "negative_pairs_with_false_join",
    "same_callsign_controls",
    "same_callsign_false_joins",
    "same_callsign_blank",
    "positive_controls",
    "positive_within_gap",
    "positive_linked",
)

_STRATUM_KEYS = (
    "point_id", "kind", "gap_stratum", "depth_min", "requested_gap_min",
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
        if (
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
    return _product({name: [float(v) for v in values] for name, values in settings.items()})


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


def _sum_when(condition: Column) -> Column:
    return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0))


def real_hole_counts(holes: DataFrame, p: SegmentationParams) -> Dict[str, int]:
    """All labelled holes, and those *p* links by label. See the module docstring."""
    linked, label = _linked(p), F.col("label")
    row = holes.agg(
        F.count(F.lit(1)).alias("real_holes"),
        _sum_when(linked).alias("real_holes_linked"),
        _sum_when(linked & (label == "true_link")).alias("linked_true_link"),
        _sum_when(linked & (label == "false_join")).alias("linked_false_join"),
        _sum_when(linked & (label == "unknown")).alias("linked_unknown"),
    ).first()
    return row.asDict()


def negative_counts(controls: DataFrame, p: SegmentationParams) -> Dict[str, int]:
    """Every negative control, and the same or blank callsign group among allowed gaps."""
    linked = _linked(p)
    within_gap = F.col("gap_min") <= p.link_max_gap_minutes
    same = F.col("same_callsign") & within_gap
    blank = F.col("cs_before").isNull() | F.col("cs_after").isNull()
    row = controls.agg(
        F.count(F.lit(1)).alias("negative_controls"),
        F.countDistinct("pair_id").alias("negative_pairs"),
        _sum_when(within_gap).alias("negative_within_gap"),
        _sum_when(linked).alias("negative_false_joins"),
        F.countDistinct(F.when(linked, F.col("pair_id"))).alias("negative_pairs_with_false_join"),
        _sum_when(same).alias("same_callsign_controls"),
        _sum_when(same & linked).alias("same_callsign_false_joins"),
        _sum_when(same & blank).alias("same_callsign_blank"),
    ).first()
    return row.asDict()


def positive_counts(controls: DataFrame, p: SegmentationParams) -> Dict[str, int]:
    """Positive controls, those the point allows, and those it links."""
    row = controls.agg(
        F.count(F.lit(1)).alias("positive_controls"),
        _sum_when(F.col("gap_min") <= p.link_max_gap_minutes).alias("positive_within_gap"),
        _sum_when(_linked(p)).alias("positive_linked"),
    ).first()
    return row.asDict()


def _finish_stratum(row: Dict) -> Dict:
    """Add the share of wrong decisions, and for negative controls the upper bound."""
    n, wrong = row["n"], row["wrong"]
    upper = rule_of_three_upper(wrong, n) if row["kind"] == "negative" else None
    return {**row, "wrong_share": wrong / n if n else None, "wrong_upper_95": upper}


def strata_counts(controls: DataFrame, kind: str, p: SegmentationParams, point_id: int) -> List[Dict]:
    """Controls per stratum at one point. ``wrong`` is the decision that was not wanted.

    For a ``negative`` control that is a link, for a ``positive`` one no link. Controls
    over the point's ``link_max_gap_minutes`` have ``within_link_max_gap`` false, which
    keeps a correct rejection, or a positive control the point never allowed, apart.
    """
    within_gap = F.col("gap_min") <= p.link_max_gap_minutes
    wanted = ~_linked(p) if kind == "negative" else _linked(p)
    size = "depth_min" if kind == "negative" else "requested_gap_min"
    rows = (
        controls.withColumn("within_link_max_gap", within_gap)
        .groupBy(size, "gap_stratum", "same_callsign", "within_link_max_gap")
        .agg(F.count(F.lit(1)).alias("n"), _sum_when(~wanted).alias("wrong"))
        .orderBy(size, "gap_stratum", "same_callsign", "within_link_max_gap")
        .collect()
    )
    return [
        _finish_stratum(
            {
                "point_id": point_id,
                "kind": kind,
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


def flight_track_counts(flights: DataFrame, holes_in_flights: DataFrame, p: SegmentationParams) -> Dict[str, int]:
    """Tracks on truth flights at *p*: each flight's A9 tracks less its linked holes."""
    linked = (
        holes_in_flights.filter(_linked(p))
        .groupBy("flight_key")
        .agg(F.count(F.lit(1)).alias("_linked_holes"))
    )
    tracks = F.col("base_tracks") - F.coalesce(F.col("_linked_holes"), F.lit(0))
    row = (
        flights.join(linked, on="flight_key", how="left")
        .withColumn("_tracks", tracks)
        .agg(
            F.count(F.lit(1)).alias("flights"),
            F.coalesce(F.sum("_tracks"), F.lit(0)).alias("tracks_on_flights"),
            _sum_when(F.col("_tracks") == 1).alias("flights_in_one_track"),
            _sum_when(F.col("_tracks") == 0).alias("flights_with_no_track"),
        )
        .first()
    )
    return row.asDict()


# --- the safety limits and the pick ------------------------------------------


def _share(part: int, whole: int) -> Optional[float]:
    return part / whole if whole else None


def with_verdicts(row: Dict) -> Dict:
    """The row's counts, with every rate, bound and safety limit verdict derived."""
    known = row["linked_true_link"] + row["linked_false_join"]
    negative_upper = rule_of_three_upper(row["negative_false_joins"], row["negative_controls"])
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
    real_ok = real_rate is not None and real_rate <= REAL_HOLE_FALSE_JOIN_LIMIT
    missed = row["positive_within_gap"] - row["positive_linked"]

    return {
        **row,
        "tracks_per_flight": _share(row["tracks_on_flights"], row["flights"]),
        "share_one_track": _share(row["flights_in_one_track"], row["flights"]),
        "linked_unknown_share": _share(row["linked_unknown"], row["real_holes_linked"]),
        "real_false_join_rate": real_rate,
        "negative_false_join_rate": _share(row["negative_false_joins"], row["negative_controls"]),
        "negative_false_join_upper_95": negative_upper,
        "negative_within_gap_upper_95": rule_of_three_upper(
            row["negative_false_joins"], row["negative_within_gap"]
        ),
        "same_callsign_false_join_rate": _share(row["same_callsign_false_joins"], same_n),
        "same_callsign_upper_95": same_upper,
        "same_callsign_blank_share": _share(row["same_callsign_blank"], same_n),
        "positive_missed_share": _share(missed, row["positive_within_gap"]),
        "negative_within_limit": negative_ok,
        "same_callsign_status": same_status,
        "real_holes_within_limit": real_ok,
        "within_limits": negative_ok and real_ok and same_status != "over_limit",
        "provisional": same_status == "insufficient",
    }


def pick_recommended(rows: List[Dict]) -> Optional[Dict]:
    """The row within the limits with the most flights in one track, or ``None``.

    A tie goes to the lower false-join bound, as a wrongly glued flight is worse than
    a split one, and then to the first point, so the pick is deterministic.
    """
    passing = [r for r in rows if r["within_limits"]]
    if not passing:
        return None
    return min(
        passing,
        key=lambda r: (-r["flights_in_one_track"], r["negative_false_join_upper_95"], r["point_id"]),
    )


def lowest_bound_point(rows: List[Dict]) -> Optional[Dict]:
    """The row with the lowest false-join bound, or ``None`` with no controls at all.

    Where Stage 2 runs if nothing is recommended. Bounds share one ``n``, so ties are
    common; those go to the most flights in one track, which leaves Stage 2 the most
    linked tracks to examine.
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


# --- one day -----------------------------------------------------------------


def _keep(frame: DataFrame) -> DataFrame:
    """Build a small frame now and hold it, for the many jobs to come."""
    kept = frame.repartition(_FEATURE_PARTITIONS).persist()
    kept.count()
    return kept


@dataclass
class DayFeatures:
    """Everything a grid point is scored on, for one day, built once."""

    holes: DataFrame
    negative: DataFrame
    positive: DataFrame
    flights: DataFrame
    holes_in_flights: DataFrame
    funnel: Dict

    def release(self) -> None:
        for frame in (self.holes, self.negative, self.positive, self.flights, self.holes_in_flights):
            frame.unpersist()


def build_features(tracks: DataFrame, truth: DataFrame, p: SegmentationParams) -> DayFeatures:
    """Label the day's holes, make its controls and count each flight's A9 tracks.

    *p* gives what the features depend on, which a grid holds constant. Holes need the
    whole day. Everything scored against truth needs only the airframes truth names,
    and reads them several times, so that part is cached and the rest is read once.
    """
    tracks = tracks.select(*_TRACK_COLUMNS)
    truth = _keep(truth.select("flight_key", "icao24", "t_off", "t_land"))
    samples = tracks.join(truth.select("icao24").distinct(), on="icao24", how="left_semi").persist()
    floor_ft = p.link_cruise_floor_ft

    holes = _keep(label_real_holes(real_hole_candidates(tracks, p), truth))
    negative = _keep(negative_controls(samples, truth, floor_ft, p=p))
    positive = _keep(positive_controls(samples, truth, floor_ft, p=p))
    funnel = {
        **control_funnel(samples, truth, floor_ft, p=p),
        "negative_controls_scored": negative.count(),
        "positive_controls_scored": positive.count(),
    }
    flights = _keep(flight_base_tracks(samples, truth, p))
    holes_in_flights = _keep(holes_inside_flights(holes, truth))

    samples.unpersist()
    truth.unpersist()
    return DayFeatures(holes, negative, positive, flights, holes_in_flights, funnel)


def score_day(day: str, tracks: DataFrame, truth: DataFrame, grid: List[SegmentationParams]) -> tuple:
    """Grid rows, strata rows and the funnel row for one day."""
    features = build_features(tracks, truth, grid[0])
    rows, strata = [], []
    for point_id, p in enumerate(grid):
        rows.append(
            with_verdicts(
                {
                    "day": day,
                    "point_id": point_id,
                    **{name: getattr(p, name) for name in _link_fields()},
                    **flight_track_counts(features.flights, features.holes_in_flights, p),
                    **real_hole_counts(features.holes, p),
                    **negative_counts(features.negative, p),
                    **positive_counts(features.positive, p),
                }
            )
        )
        for kind, controls in (("negative", features.negative), ("positive", features.positive)):
            strata += [{"day": day, **s} for s in strata_counts(controls, kind, p, point_id)]
        print(f"{day}: point {point_id + 1} of {len(grid)}", flush=True)
    funnel = {"day": day, **features.funnel}
    features.release()
    return rows, strata, funnel


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
) -> Dict:
    """Score each day, pool them, choose, and stage every CSV with its provenance."""
    results_dir = Path(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    def stage(name: str, rows: List[Dict]) -> None:
        with (results_dir / name).open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
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

    all_rows, all_strata = [], []
    for day in days:
        rows, strata, funnel = score_day(day, load_tracks(day), load_truth(day), grid)
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

    if "point_id" in choice:
        totals: Dict[str, int] = {}
        for day in days:
            counts = two_callsign_counts(load_tracks(day), grid[choice["point_id"]])
            print(f"{day}: {counts}", flush=True)
            for name, value in counts.items():
                totals[name] = totals.get(name, 0) + value
        choice = {**choice, **totals}
        stage("link_recommended.csv", [choice])
    return choice


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--days", nargs="+", default=list(DEFAULT_DAYS))
    ap.add_argument("--source", default=SOURCE)
    ap.add_argument("--reference-base", default=REFERENCE_BASE)
    ap.add_argument("--results-dir", type=Path, required=True)
    ap.add_argument("--grid-json", default=None, help="replace the grid: see the docstring")
    ap.add_argument("--executors", type=int, default=8)
    args = ap.parse_args()

    # Before the session exists, so a bad grid costs nothing.
    grid = grid_from_json(args.grid_json) if args.grid_json else grid_points()

    import osn_sample
    from osn_sample import build_spark, load_dotenv

    load_dotenv()
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(4, "8g", distributed=True)
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    def load_tracks(day: str) -> DataFrame:
        return (
            spark.read.parquet(args.source)
            .filter(F.col("dof").cast("date") == F.lit(day).cast("date"))
            .withColumn("icao24", F.lower("icao24"))
        )

    def load_truth(day: str) -> DataFrame:
        return load_flight_intervals(
            spark, truth_months(day), [day], reference_base=args.reference_base
        )

    try:
        run_study(
            args.days, grid, load_tracks, load_truth, args.results_dir,
            argv=sys.argv[1:],
            input_tables=[f"{args.source}/dof={day}" for day in args.days],
            reference_base=args.reference_base,
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
