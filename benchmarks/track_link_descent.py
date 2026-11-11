#!/usr/bin/env python
"""Holes whose new edge is descending: what the rule sees there, with the truth's label.

    .venv310/bin/python -u benchmarks/track_link_descent.py \\
        --days 2026-06-14 2025-06-14 --results-dir <paper>/data/descent

``cruise_linked`` has a switch, ``link_new_edge_descent``, that lets a hole link when
the aircraft reappears below the cruise floor but descending. It is off: version 4 of
the track-construction study measured 3 false joins in 119 checkable holes linked only
that way, over the 0.1% limit, and never looked at which three. This exports the holes
the question is about, so a fixed list of extra conditions can be tried on them offline
(``--score``, see :data:`CANDIDATES`) before the rule is touched.

Per day it writes:

* ``descent_holes_{day}.csv`` -- one row per real hole (a gap over 30 min of one
  airframe) whose new edge is below the floor and descending, whatever the other
  checks say: the label against Network Manager (``true_link``, ``false_join``,
  ``unknown``), the population of the flight it lies in (``crosses_box``, ``other``,
  or ``none`` when no truth flight holds both edges), the edge features, the callsign
  state, and ``other_checks_pass`` -- every check of the rule except the new edge's.
* ``descent_controls_{day}.csv.gz`` -- one row per ``cruise_descent`` hidden-landing
  control (the first flight's cruise cut to the second flight's descent), with the
  same features and ``joined``: whether the rule with descent on links it.

Read with the same loaders as ``track_link_grid.py``: the tracks of the day and the
Network Manager truth with ``crosses_box``.
"""

import argparse
import csv
import dataclasses
import sys
from functools import reduce
from operator import and_
from pathlib import Path
from typing import Dict, List, Optional

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "benchmarks"))

import provenance  # noqa: E402
from pyspark.sql import Column, DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from track_link_controls import (  # noqa: E402
    label_real_holes,
    negative_controls,
    real_hole_candidates,
)

from opdi.pipeline.segmentation import SegmentationParams  # noqa: E402
from opdi.pipeline.segmentation.methods import (  # noqa: E402
    LINK_DIRECTION_MIN_RATES,
    link_checks,
    link_feature_columns,
)

_CODE_PATHS = [
    "benchmarks/track_link_descent.py",
    "benchmarks/track_link_controls.py",
    "benchmarks/track_link_grid.py",
    "src/opdi/pipeline/segmentation/methods.py",
    "src/opdi/pipeline/segmentation/base.py",
    "src/opdi/config",
]

HOLE_COLUMNS = [
    "icao24",
    "track_id",
    "t_old_edge",
    "t_new_edge",
    "gap_min",
    "population",
    "label",
    "event_near_edge",
    "event_outside_box_inside",
    "descent_edge",
    "alt_after_ft",
    "vr_after_fpm",
    "vr_after_samples",
    "alt_before_ft",
    "vr_before_fpm",
    "implied_kt",
    "cs_before",
    "cs_after",
    "callsign_state",
    "other_checks_pass",
]

CONTROL_COLUMNS = [
    "control_id",
    "pair_id",
    "requested_gap_min",
    "depth_min",
    "gap_min",
    "same_callsign",
    "descent_edge",
    "alt_after_ft",
    "vr_after_fpm",
    "vr_after_samples",
    "alt_before_ft",
    "vr_before_fpm",
    "implied_kt",
    "cs_before",
    "cs_after",
    "callsign_state",
    "other_checks_pass",
    "joined",
]


def _features(p: SegmentationParams) -> Dict[str, Column]:
    return {name: F.col(name) for name in link_feature_columns(p)}


def callsign_state() -> Column:
    """``both_equal``, ``differ``, ``one_blank`` or ``both_blank`` from the two callsigns."""
    before, after = F.col("cs_before"), F.col("cs_after")
    return (
        F.when(before.isNull() & after.isNull(), "both_blank")
        .when(before.isNull() | after.isNull(), "one_blank")
        .when(before == after, "both_equal")
        .otherwise("differ")
    )


def below_floor_descending(p: SegmentationParams) -> Column:
    """The new edge is below the cruise floor and descending, on enough rates.

    The population this study is about. It does not require the edge to be above
    ``low_alt_ft``; :func:`descent_edge` does, as the rule does.
    """
    return (
        (F.col("alt_after_ft") < p.link_cruise_floor_ft)
        & (F.col("vr_after_fpm") < -p.link_edge_vert_rate_fpm)
        & (F.col("vr_after_samples") >= LINK_DIRECTION_MIN_RATES)
    )


def descent_edge(p: SegmentationParams) -> Column:
    """What ``link_new_edge_descent`` accepts as a new edge, as ``link_checks`` spells it."""
    return below_floor_descending(p) & (F.col("alt_after_ft") >= p.low_alt_ft)


def other_checks_pass(p: SegmentationParams) -> Column:
    """Every check of the rule but the new edge's, at *p*. Never NULL."""
    checks = link_checks(_features(p), p)
    del checks["cruise_at_new_edge"]
    return F.coalesce(reduce(and_, checks.values()), F.lit(False))


def implied_kt() -> Column:
    return F.when(
        F.col("position_gap_min") > 0.0, F.col("gc_nm") / (F.col("position_gap_min") / 60.0)
    )


def _populations(holes: DataFrame, truth: DataFrame) -> DataFrame:
    """``population`` per hole: the flight holding both edges, ``crosses_box`` if any does."""
    key = ["icao24", "t_old_edge", "t_new_edge"]
    crosses = F.col("crosses_box") if "crosses_box" in truth.columns else F.lit(False)
    flights = truth.select(
        F.col("icao24").alias("_truth_icao24"), "t_off", "t_land", crosses.alias("_crosses")
    ).distinct()
    inside = (
        holes.select(*key)
        .join(
            flights,
            (F.col("icao24") == F.col("_truth_icao24"))
            & (F.col("t_off") <= F.col("t_old_edge"))
            & (F.col("t_new_edge") <= F.col("t_land")),
        )
        .groupBy(*key)
        .agg(F.max("_crosses").alias("_crosses"))
    )
    return (
        holes.join(inside, on=key, how="left")
        .withColumn(
            "population",
            F.when(F.col("_crosses").isNull(), "none")
            .when(F.col("_crosses"), "crosses_box")
            .otherwise("other"),
        )
        .drop("_crosses")
    )


def _describe(frame: DataFrame, p: SegmentationParams) -> DataFrame:
    return (
        frame.withColumn("descent_edge", F.coalesce(descent_edge(p), F.lit(False)))
        .withColumn("implied_kt", implied_kt())
        .withColumn("callsign_state", callsign_state())
        .withColumn("other_checks_pass", other_checks_pass(p))
    )


def descent_holes(tracks: DataFrame, truth: DataFrame, p: SegmentationParams) -> DataFrame:
    """One row per real hole whose new edge is below the floor and descending."""
    holes = real_hole_candidates(tracks, p).filter(
        F.coalesce(below_floor_descending(p), F.lit(False))
    )
    if "track_id" not in holes.columns:
        holes = holes.withColumn("track_id", F.lit(None).cast("string"))
    labelled = label_real_holes(holes, truth)
    return _describe(_populations(labelled, truth), p).select(*HOLE_COLUMNS)


def descent_controls(
    tracks: DataFrame, truth: DataFrame, p: SegmentationParams, samples: Optional[DataFrame] = None
) -> DataFrame:
    """One row per ``cruise_descent`` control, with ``joined`` under descent on."""
    controls = negative_controls(
        tracks, truth, p.link_cruise_floor_ft, p=p, samples=samples
    ).filter(F.col("control_family") == "cruise_descent")
    with_descent = dataclasses.replace(p, link_new_edge_descent=True)
    joined = F.coalesce(
        reduce(and_, link_checks(_features(with_descent), with_descent).values()), F.lit(False)
    )
    return _describe(controls, p).withColumn("joined", joined).select(*CONTROL_COLUMNS)


# --- the candidates, scored offline ---------------------------------------------------

#: The extra conditions tried on a hole linked only by descent, fixed in advance so the
#: data cannot be searched until something fits. Each is a predicate over one exported
#: row (``pandas`` namespace), and the combinations are conjunctions.
CANDIDATES = {
    "C1": lambda r: r["alt_after_ft"] >= DESCENT_FLOOR_FT,
    "C2": lambda r: r["callsign_state"] == "both_equal",
    "C3": lambda r: r["gap_min"] <= DESCENT_MAX_GAP_MIN,
}
DESCENT_FLOOR_FT = 10000.0
DESCENT_MAX_GAP_MIN = 360.0
COMBINATIONS = ["none", "C1", "C2", "C3", "C1+C2", "C1+C3", "C2+C3", "C1+C2+C3"]

#: The limits a candidate has to meet, pooled over the days: no false join among
#: checkable real holes (0.1% below 1,000 cases means none); hidden landings joined at
#: most 1% as a 95% upper bound, and at most 5% among the same-callsign ones. Below
#: ``MIN_CHECKABLE`` checkable holes the result is reported but not a measurement, and a
#: condition that links fewer than ``MIN_CROSSING_LINKED`` crossing-flight holes is not
#: worth a setting.
HIDDEN_LIMIT = 0.01
SAME_CALLSIGN_LIMIT = 0.05
MIN_CHECKABLE = 50
MIN_CROSSING_LINKED = 20


def _passes(frame, candidate: str):
    """Boolean Series: the rows of *frame* a candidate keeps."""
    import pandas as pd

    keep = pd.Series(True, index=frame.index)
    if candidate != "none":
        for name in candidate.split("+"):
            keep &= frame.apply(CANDIDATES[name], axis=1) if len(frame) else keep
    return keep


def _truthy(series):
    return series.astype(str).str.lower().isin(["true", "1", "1.0"])


def score_candidates(holes, controls):
    """One row per candidate: the limits' counts over the exported holes and controls.

    *holes* and *controls* are the exported CSVs as DataFrames (one day or several
    concatenated). A hole counts only where the rule with descent on would link it:
    ``descent_edge`` and ``other_checks_pass``. A control counts wherever its edge is a
    descent edge, whatever the other checks say: a hidden landing the speed check
    rejects is a hidden landing the rule did not join, which is the limit's question.
    """
    import pandas as pd
    from track_link_controls import rule_of_three_upper

    linked = holes[_truthy(holes["descent_edge"]) & _truthy(holes["other_checks_pass"])]
    exposed = controls[_truthy(controls["descent_edge"])]
    rows = []
    for candidate in COMBINATIONS:
        h = linked[_passes(linked, candidate)]
        c = exposed[_passes(exposed, candidate)]
        checkable = h[h["label"].isin(["true_link", "false_join"])]
        false_joins = int((h["label"] == "false_join").sum())
        joined = int(_truthy(c["joined"]).sum()) if len(c) else 0
        same = c[_truthy(c["same_callsign"])] if len(c) else c
        same_joined = int(_truthy(same["joined"]).sum()) if len(same) else 0
        hidden_upper = rule_of_three_upper(joined, len(c))
        same_upper = rule_of_three_upper(same_joined, len(same))
        within = (
            false_joins == 0
            and hidden_upper is not None
            and hidden_upper <= HIDDEN_LIMIT
            and (same_upper is None or same_upper <= SAME_CALLSIGN_LIMIT)
        )
        crossing = int((h["population"] == "crosses_box").sum())
        rows.append(
            {
                "candidate": candidate,
                "conditions": 0 if candidate == "none" else candidate.count("+") + 1,
                "real_linked": len(h),
                "real_crossing_linked": crossing,
                "real_checkable": len(checkable),
                "real_false_joins": false_joins,
                "real_unknown": int((h["label"] == "unknown").sum()),
                "false_join_upper_95": rule_of_three_upper(false_joins, len(checkable)),
                "controls": len(c),
                "controls_joined": joined,
                "controls_joined_upper_95": hidden_upper,
                "same_callsign_controls": len(same),
                "same_callsign_joined": same_joined,
                "same_callsign_upper_95": same_upper,
                "within_limits": bool(within),
                "adoptable": bool(
                    within and len(checkable) >= MIN_CHECKABLE and crossing >= MIN_CROSSING_LINKED
                ),
            }
        )
    return pd.DataFrame(rows)


def choose(scored) -> Optional[str]:
    """The adoptable candidate with the fewest conditions; ties go to C2, then C1, then C3."""
    order = {
        name: i
        for i, name in enumerate(["none", "C2", "C1", "C3", "C1+C2", "C2+C3", "C1+C3", "C1+C2+C3"])
    }
    good = scored[scored["adoptable"].astype(bool)]
    if good.empty:
        return None
    best = sorted(good.to_dict("records"), key=lambda r: (r["conditions"], order[r["candidate"]]))
    return best[0]["candidate"]


def write_rows(
    results_dir,
    name: str,
    rows: List[Dict],
    argv: List[str],
    input_tables: List[str],
    notes: str = "",
) -> None:
    """Write *rows* as a CSV beside its provenance entry."""
    import gzip

    results_dir = Path(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    columns = list(rows[0].keys()) if rows else []
    opener = gzip.open if name.endswith(".gz") else open
    with opener(results_dir / name, "wt", newline="") as fh:
        out = csv.DictWriter(fh, fieldnames=columns)
        out.writeheader()
        out.writerows(rows)
    provenance.record(
        results_dir,
        name,
        "benchmarks/track_link_descent.py",
        argv,
        _CODE_PATHS,
        inputs={"rows": len(rows)},
        notes=notes,
        input_tables=input_tables,
    )


def _rows(frame: DataFrame) -> List[Dict]:
    return [
        {k: (v.isoformat(" ") if hasattr(v, "isoformat") else v) for k, v in r.asDict().items()}
        for r in frame.collect()
    ]


def score_main(args) -> int:
    """``--score``: the candidates over every exported day, and the false joins in full."""
    import pandas as pd

    def staged(stem):
        files = sorted(Path(args.results_dir).glob(f"{stem}_*.csv*"))
        if not files:
            raise SystemExit(f"no {stem}_*.csv under {args.results_dir}: run the export first")
        return pd.concat([pd.read_csv(f) for f in files], ignore_index=True), [
            f.name for f in files
        ]

    holes, hole_files = staged("descent_holes")
    controls, control_files = staged("descent_controls")
    scored = score_candidates(holes, controls)
    inputs = hole_files + control_files
    write_rows(
        args.results_dir,
        "descent_candidates.csv",
        scored.to_dict("records"),
        sys.argv[1:],
        inputs,
        notes="Candidates scored over every exported day; see CANDIDATES.",
    )
    linked = holes[_truthy(holes["descent_edge"]) & _truthy(holes["other_checks_pass"])]
    false_joins = linked[linked["label"] == "false_join"]
    write_rows(
        args.results_dir,
        "descent_false_joins.csv",
        false_joins.to_dict("records"),
        sys.argv[1:],
        inputs,
        notes="The false joins among holes linked only by descent, in full.",
    )
    print(scored.to_string(index=False))
    print(f"chosen: {choose(scored)}")
    return 0


def main() -> int:
    from track_link_grid import (
        AIRPORTS,
        DEFAULT_DAYS,
        REFERENCE_BASE,
        SOURCE,
        read_tracks,
        truth_months,
        with_population,
    )

    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--days", nargs="+", default=list(DEFAULT_DAYS))
    ap.add_argument("--source", default=SOURCE)
    ap.add_argument("--reference-base", default=REFERENCE_BASE)
    ap.add_argument("--airports", default=AIRPORTS)
    ap.add_argument("--results-dir", type=Path, required=True)
    ap.add_argument("--executors", type=int, default=8)
    ap.add_argument(
        "--score", action="store_true", help="score the candidates on the exported CSVs; no cluster"
    )
    args = ap.parse_args()
    if args.score:
        return score_main(args)
    p = SegmentationParams()

    import osn_sample
    from osn_sample import build_spark, load_dotenv
    from track_truth import load_flight_intervals

    load_dotenv()
    osn_sample.RESEARCH_EXECUTORS = args.executors
    spark = build_spark(4, "8g", distributed=True)
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        airports = spark.read.parquet(args.airports)
        for day in args.days:
            tracks = read_tracks(spark, args.source, day).cache()
            truth = with_population(
                load_flight_intervals(
                    spark, truth_months(day), [day], reference_base=args.reference_base
                ),
                airports,
            ).cache()
            tables = [f"{args.source}/dof={day}", args.airports]
            holes = _rows(descent_holes(tracks, truth, p))
            write_rows(
                args.results_dir,
                f"descent_holes_{day}.csv",
                holes,
                sys.argv[1:],
                tables,
                notes="Real holes whose new edge is below the floor and descending.",
            )
            print(f"{day}: {len(holes)} holes with a descending new edge", flush=True)
            controls = _rows(descent_controls(tracks, truth, p))
            # Gzipped: eighty thousand rows a day, which pandas reads as it is.
            write_rows(
                args.results_dir,
                f"descent_controls_{day}.csv.gz",
                controls,
                sys.argv[1:],
                tables,
                notes="cruise_descent hidden-landing controls, with the rule's verdict under descent on.",  # noqa: E501
            )
            print(f"{day}: {len(controls)} cruise_descent controls", flush=True)
            tracks.unpersist()
            truth.unpersist()
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
