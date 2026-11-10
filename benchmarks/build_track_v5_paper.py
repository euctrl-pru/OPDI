#!/usr/bin/env python
"""Fill the track-construction-v5 report from its staged CSVs.

The same arrangement as ``build_track_v4_paper.py``, whose generic parts this reuses:
``report_template.md`` holds the prose with ``{{ name }}`` placeholders, every value is
computed here from a CSV under ``data/``, and a missing file, row, column or placeholder
stops the build. It writes ``index.qmd``, ``index.html`` and, with ``--pdf``,
``track-construction-v5.pdf``.

    python benchmarks/build_track_v5_paper.py [--paper-dir DIR] [--check] [--pdf]
"""

import csv
import gzip
import json
import math
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

from build_track_v4_paper import (  # noqa: E402
    CODE_CONSTANTS,
    MissingValue,
    Paper,
    code_constant,
    labelled,
    n,
    num,
    one,
    pct,
    read_csv,
    table,
    upper_95,
    where,
)
import build_track_v4_paper as v4  # noqa: E402

#: ``OPDI_PAPER_DIR_V5``, not the v4 builder's ``OPDI_PAPER_DIR``: one shared
#: name would point both builders at one paper.
PAPER = Path(
    os.environ.get(
        "OPDI_PAPER_DIR_V5",
        REPO.parent / "opdi-portal" / "papers" / "track-construction-v5",
    )
)

TITLE = "One flight, one track across a reception gap"
SUBTITLE = (
    "Version 5: the rule becomes the default -- what was fixed, what was audited, "
    "and what it measures now"
)

#: Numbers the prose may state without a placeholder: definitions and limits of the
#: study, and figures that come from the code or from the trace of version 4's run,
#: never measurements of this version.
ALLOWED_LITERALS = {
    "30",    # the gap that starts a new track, minutes
    "3",     # hours a day's run used to read back; the three vertical rates
    "5",     # the 5% limit
    "1",     # the 1% limit; the 1 NM tolerance
    "0.1",   # the 0.1% limit
    "95",    # confidence of the upper bounds
    "300",   # checkable climb holes version 4 wanted
    "60",    # the callsign lookback, minutes
    "128",   # the group at which Spark's aggregation falls back to sorting
    "279",   # flight-list rows without a track in version 4's pipeline day
    "119",   # checkable descent holes version 4 counted its 3 false joins in
    "2026",  # the date of the A8 boundary
    "08", "27",
    "4",     # version 4
    "2", "6", "7", "8", "9", "10", "6.1", "6.2", "6.3",   # section numbers
    "02", "04",   # pipeline steps 02 to 04b
    "5,000", # the low-altitude limit a descent edge must stay above, feet
    "50", "20",  # the descent candidates' floors: checkable holes, crossing holes linked
}

#: What the independent audit ran, from its report
#: (``$CLAUDE_JOB_DIR/tmp/audit/rule_audit.md``, 2026-11-09): decisions of the Spark rule
#: compared with a plain-Python copy over randomly generated aircraft.
AUDIT_DECISIONS = 691_524


def class_constant(cls, name, relative):
    """A class attribute's literal default, read from the source and not imported."""
    import ast

    path = REPO / relative
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, ast.ClassDef) and node.name == cls:
            for item in node.body:
                if (isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name)
                        and item.target.id == name and isinstance(item.value, ast.Constant)):
                    return item.value.value
    raise MissingValue(f"no {cls}.{name} in {path}")


def descent_shipped():
    """Whether the code ships with the descending-edge allowance on."""
    return bool(class_constant("SegmentationConfig", "link_new_edge_descent", "src/opdi/config.py"))


def _shipped(grid):
    """The shipped setting's row: climb on, descent as the code ships it."""
    return one(grid, link_old_edge_climb="True", link_new_edge_descent=str(descent_shipped()))


def _minutes(seconds):
    return f"{seconds / 60:.0f} min"


def plural(count, noun):
    """``1 false join``, ``2 false joins``."""
    return f"{n(count)} {noun}" if int(round(float(count))) == 1 else f"{n(count)} {noun}s"


def build_context(data):
    """Every placeholder of the report, as formatted strings."""
    data = Path(data)
    c = {key: n(code_constant(*where_from)) for key, where_from in CODE_CONSTANTS.items()
         if key in ("edge_samples", "min_rates")}

    # --- two days against Network Manager ---
    grid = read_csv(data, "two_days/link_grid_pooled.csv")
    rec = _shipped(grid)
    no_climb = one(grid, link_old_edge_climb="False", link_new_edge_descent=str(descent_shipped()))
    gap = num(rec, "link_max_gap_minutes")
    c.update(
        day_new="2026-06-14",
        day_old="2025-06-14",
        floor_ft=n(num(rec, "link_cruise_floor_ft")),
        min_speed_kt=n(num(rec, "link_min_implied_speed_kt")),
        max_speed_kt=n(num(rec, "link_max_implied_speed_kt")),
        max_gap_min=n(gap),
        vert_rate_fpm=n(num(rec, "link_edge_vert_rate_fpm")),
        lookahead_min=n(num(rec, "link_callsign_lookahead_minutes")),
        lookback_min=n(num(rec, "link_callsign_lookback_minutes")),
        edge_position_min=n(num(rec, "link_edge_position_minutes")),
        read_back_hours=n(
            math.ceil(gap / 60)
            + math.ceil(max(num(rec, "link_callsign_lookback_minutes"),
                            num(rec, "link_edge_position_minutes"), 0.5) / 60)
        ),
        gap_60=n(60),
        gap_360=n(360),
        audit_decisions=n(AUDIT_DECISIONS),
    )

    fl, cf, of = num(rec, "flights"), num(rec, "crossing_flights"), num(rec, "other_flights")
    c.update(
        crossing_flights=n(cf),
        crossing_base=pct(num(rec, "base_crossing_flights_in_one_track"), cf),
        crossing_new=pct(num(rec, "crossing_flights_in_one_track"), cf),
        crossing_no_climb=pct(num(no_climb, "crossing_flights_in_one_track"), cf),
        all_base=pct(num(rec, "base_flights_in_one_track"), fl),
        all_new=pct(num(rec, "flights_in_one_track"), fl),
        crossing_base_tracks=n(num(rec, "base_crossing_tracks")),
        crossing_rows_base=f"{num(rec, 'base_crossing_tracks') / cf:.2f}",
        flights_no_track=n(num(rec, "flights_with_no_track")),
    )

    def share_row(name, flights, base, new, base_tracks, new_tracks):
        return [
            name, n(flights), f"{n(base)} ({pct(base, flights)})", f"{n(new)} ({pct(new, flights)})",
            f"{base_tracks / flights:.2f}", f"{new_tracks / flights:.2f}",
        ]

    c["table_one_track"] = table(
        ["Flights", "Number", "In one track, previous rule", "In one track, new rule",
         "Tracks per flight, previous", "Tracks per flight, new"],
        [
            share_row("Crossing the edge of the European box", cf,
                      num(rec, "base_crossing_flights_in_one_track"),
                      num(rec, "crossing_flights_in_one_track"),
                      num(rec, "base_crossing_tracks"), num(rec, "crossing_tracks")),
            share_row("All others", of,
                      num(rec, "base_other_flights_in_one_track"),
                      num(rec, "other_flights_in_one_track"),
                      num(rec, "base_other_tracks"), num(rec, "other_tracks")),
            share_row("All", fl, num(rec, "base_flights_in_one_track"),
                      num(rec, "flights_in_one_track"),
                      num(rec, "base_tracks_on_flights"), num(rec, "tracks_on_flights")),
        ],
    )

    lab = labelled(rec)
    linked = num(rec, "real_holes_linked")
    climb_lab = num(rec, "linked_by_climb_labelled")
    descent_lab = num(rec, "linked_by_descent_labelled")
    same_n = num(rec, "same_callsign_controls")
    positive = num(rec, "positive_within_gap")
    c.update(
        linked_holes=n(linked),
        false_joins=n(num(rec, "linked_false_join")),
        false_joins_phrase=plural(num(rec, "linked_false_join"), "false join"),
        labelled_holes=n(lab),
        false_join_rate=pct(num(rec, "linked_false_join"), lab, 2),
        unknown_holes=n(num(rec, "linked_unknown")),
        unknown_share=pct(num(rec, "linked_unknown"), linked),
        climb_false=n(num(rec, "false_joined_by_climb")),
        climb_labelled=n(climb_lab),
        climb_upper=pct(upper_95(num(rec, "false_joined_by_climb"), climb_lab), 1),
        descent_false=n(num(rec, "false_joined_by_descent")),
        descent_labelled=n(descent_lab),
        limit_controls=n(num(rec, "negative_limit_controls")),
        limit_joined=n(num(rec, "negative_limit_false_joins")),
        limit_upper=pct(num(rec, "negative_false_join_upper_95"), 1, 3),
        same_controls=n(same_n),
        same_joined=n(num(rec, "same_callsign_false_joins")),
        same_upper=pct(num(rec, "same_callsign_upper_95"), 1, 2),
        positive_tests=n(positive),
        positive_rejoined=n(num(rec, "positive_linked")),
        positive_rate=pct(num(rec, "positive_linked"), positive),
    )
    if rec.get("within_limits") != "True":
        raise MissingValue("the shipped setting is no longer within the limits: the prose needs rewriting")

    # --- reappearing on descent ---
    candidates = read_csv(data, "descent/descent_candidates.csv")
    chosen = [r for r in candidates if r["adoptable"] == "True"]
    chosen_name = min(chosen, key=lambda r: int(r["conditions"]))["candidate"] if chosen else None
    adopted = chosen_name is not None
    if adopted != descent_shipped():
        raise MissingValue(
            f"the code ships descent {'on' if descent_shipped() else 'off'} but the staged "
            f"candidates say {'adopt ' + chosen_name if adopted else 'adopt nothing'}"
        )
    exported, relabelled = {}, []
    for stem in ("descent_holes", "descent_controls"):
        rows = 0
        for day in (c["day_new"], c["day_old"]):
            path = data / "descent" / f"{stem}_{day}.csv"
            if not path.is_file() and path.with_suffix(".csv.gz").is_file():
                path = path.with_suffix(".csv.gz")   # the controls are gzipped
            if not path.is_file():
                raise MissingValue(f"missing {path}")
            opener = gzip.open if path.suffix == ".gz" else open
            with opener(path, "rt", newline="") as fh:
                day_rows = list(csv.DictReader(fh))
            rows += len(day_rows)
            if stem == "descent_holes":
                relabelled += [
                    r for r in day_rows
                    if r["event_outside_box_inside"] == "True" and r["descent_edge"] == "True"
                    and r["other_checks_pass"] == "True"
                ]
        exported[stem] = rows
    if not relabelled:
        raise MissingValue("no hole linked by descent had an event outside the box: the prose needs rewriting")
    if any(r["callsign_state"] != "both_equal" for r in relabelled):
        raise MissingValue("a relabelled descent hole has differing or blank callsigns: the prose needs rewriting")

    def span(column, digits=0):
        values = sorted(float(r[column]) for r in relabelled)
        return f"{values[0]:.{digits}f} to {values[-1]:.{digits}f}"

    def bound(row, column):
        value = row.get(column)
        return pct(float(value), 1, 2) if value not in (None, "") else "no bound"

    c.update(
        descent_holes_exported=n(exported["descent_holes"]),
        descent_controls_exported=n(exported["descent_controls"]),
        descent_floor_ft=n(code_constant("DESCENT_FLOOR_FT", "benchmarks/track_link_descent.py")),
        descent_max_gap_min=n(code_constant("DESCENT_MAX_GAP_MIN", "benchmarks/track_link_descent.py")),
        descent_adopted=adopted,
        descent_chosen=(
            "the descending edge as defined, with no extra condition" if chosen_name == "none"
            else chosen_name or "none"
        ),
        descent_relabelled=n(len(relabelled)),
        relabelled_all=n(num(rec, "linked_event_outside_box")),
        relabelled_other=n(num(rec, "linked_event_outside_box") - len(relabelled)),
        relabelled_gap_range=span("gap_min"),
        relabelled_speed_range=span("implied_kt"),
        table_descent_candidates=table(
            ["Condition", "Holes kept", "On crossing flights", "Checkable", "False joins",
             "Hidden landings joined", "Upper bound, 95%", "Qualifies"],
            [
                [
                    "no extra condition" if r["candidate"] == "none" else r["candidate"].replace("+", " and "),
                    n(num(r, "real_linked")), n(num(r, "real_crossing_linked")),
                    n(num(r, "real_checkable")), n(num(r, "real_false_joins")),
                    f"{n(num(r, 'controls_joined'))} of {n(num(r, 'controls'))}",
                    bound(r, "controls_joined_upper_95"),
                    "yes" if r["adoptable"] == "True" else ("limits only" if r["within_limits"] == "True" else "no"),
                ]
                for r in candidates
            ],
        ),
    )
    none_row = one(candidates, candidate="none")
    kept = one(candidates, candidate=chosen_name) if adopted else none_row
    climb_only = one(grid, link_old_edge_climb="True", link_new_edge_descent="False")
    c.update(
        descent_kept=n(num(kept, "real_linked")),
        descent_crossing_kept=n(num(kept, "real_crossing_linked")),
        descent_kept_false=n(num(kept, "real_false_joins")),
        descent_kept_checkable=n(num(kept, "real_checkable")),
        descent_kept_hidden=n(num(kept, "controls_joined")),
        descent_kept_controls=n(num(kept, "controls")),
        descent_kept_hidden_upper=bound(kept, "controls_joined_upper_95"),
        descent_kept_upper=bound(kept, "false_join_upper_95"),
        descent_unknown_share=pct(num(kept, "real_unknown"), num(kept, "real_linked")),
        crossing_no_descent=pct(num(climb_only, "crossing_flights_in_one_track"), cf),
    )

    # --- one day through the whole pipeline ---
    both = read_csv(data, "pipeline_day/link_both_ends.csv")
    cross = one(both, flow="crosses_the_box")
    all_rows = [r for r in both if r["flow"] != "crosses_the_box"]
    all_n = sum(num(r, "n") for r in all_rows)
    c.update(
        s2_crossing=n(num(cross, "n")),
        s2_both_base=n(num(cross, "debounced_both_right")),
        s2_both_new=n(num(cross, "cruise_linked_both_right")),
        s2_rows_base=f"{num(cross, 'debounced_rows_per_flight'):.2f}",
        s2_rows_new=f"{num(cross, 'cruise_linked_rows_per_flight'):.2f}",
        s2_all=n(all_n),
        s2_all_both_base=n(sum(num(r, "debounced_both_right") for r in all_rows)),
        s2_all_both_new=n(sum(num(r, "cruise_linked_both_right") for r in all_rows)),
    )

    part = one(read_csv(data, "pipeline_day/link_partition_check.csv"))
    moves = one(read_csv(data, "pipeline_day/link_movements.csv"), movement="all")
    if part["passed"] != "True" or moves["passed"] != "True":
        raise MissingValue("a nothing-else-moved check failed: the prose needs rewriting")
    if num(part, "removed_elsewhere") != 0 or num(part, "added_elsewhere") != 0:
        raise MissingValue("boundaries moved away from holes: the prose needs rewriting")
    base_m, new_m = num(moves, "debounced_matched"), num(moves, "cruise_linked_matched")
    c.update(
        s2_removed_boundaries=n(num(part, "removed_boundaries")),
        s2_removed_at_holes=n(num(part, "removed_at_a_linked_hole")),
        s2_memory_removed=n(num(part, "removed_by_callsign_memory")),
        s2_memory_added=n(num(part, "added_by_callsign_memory")),
        s2_boundaries_base=n(num(part, "debounced_boundaries")),
        movements_truth=n(num(moves, "apdf_movements")),
        movements_base=n(base_m),
        movements_new=n(new_m),
        movement_change=(f"{int(new_m - base_m):+d}" if new_m != base_m else "no change"),
    )

    level = one(read_csv(data, "pipeline_day/link_level_across_hole.csv"), family="any")
    dist = one(read_csv(data, "pipeline_day/link_distance_across_hole.csv"))
    compared = num(dist, "holes_compared")
    c.update(
        s2_linked_holes=n(num(level, "linked_holes_anywhere")),
        s2_scored_holes=n(num(level, "scored_holes")),
        s2_linked_tracks=n(num(level, "linked_tracks_anywhere")),
        level_tracks=n(num(level, "tracks_with_a_segment_across_a_hole")),
        level_scored=n(num(level, "scored_tracks")),
        dist_median=f"{num(dist, 'median_difference_nm'):.1f}",
        dist_compared=n(compared),
        dist_off=n(num(dist, "holes_off_by_over_1_nm")),
        dist_within_share=pct(compared - num(dist, "holes_off_by_over_1_nm"), compared),
    )
    if dist["stored_distance_verdict"] != "agrees":
        raise MissingValue(
            f"the recomputed distance {dist['stored_distance_verdict']} with the stored one"
        )

    cons = one(read_csv(data, "pipeline_day/link_consistency.csv"))
    c.update(
        orphans_base=n(num(cons, "debounced_rows_without_a_track")),
        orphans_new=n(num(cons, "cruise_linked_rows_without_a_track")),
        pick_dup_base=n(num(cons, "debounced_named_among_alternatives")),
        pick_dup_new=n(num(cons, "cruise_linked_named_among_alternatives")),
        unchanged_tracks=n(num(cons, "unchanged_tracks")),
        unchanged_other_airports=n(num(cons, "unchanged_tracks_with_other_airports")),
        unchanged_other_events=n(num(cons, "unchanged_tracks_with_other_events")),
        unchanged_other_info=n(num(cons, "unchanged_tracks_with_other_event_info")),
    )

    no_hole = one(read_csv(data, "pipeline_day/link_distance_no_hole.csv"))
    c.update(
        nohole_missing_share=pct(num(no_hole, "share_of_distance_missing"), 1),
        nohole_tracks=n(num(no_hole, "tracks")),
        nohole_short_tracks=n(num(no_hole, "tracks_short_by_over_1_nm")),
        nohole_median_short=f"{num(no_hole, 'median_short_nm'):.1f}",
    )

    timings = read_csv(data, "pipeline_day/link_step_timings.csv")
    steps = ("02", "02a", "03", "04", "04b")

    def total(arm):
        rows = where(timings, arm=arm)
        missing = [s for s in steps if not where(rows, step=s)]
        if missing:
            raise MissingValue(f"no timing for {arm} steps {missing}")
        return sum(num(one(rows, step=s), "seconds") for s in steps)

    c.update(time_base=_minutes(total("debounced")), time_new=_minutes(total("cruise_linked")))

    truth = read_csv(data, "pipeline_day/link_truth_all_flights.csv")
    t = {key: one(truth, label=key) for key in ("true_link", "false_join", "unknown", "airframe_not_in_truth")}
    c.update(
        truth_holes=n(sum(num(r, "holes") for r in t.values())),
        truth_true=n(num(t["true_link"], "holes")),
        truth_false=n(num(t["false_join"], "holes")),
        truth_unknown=n(num(t["unknown"], "holes")),
        truth_out=n(num(t["airframe_not_in_truth"], "holes")),
        truth_true_both=n(num(t["true_link"], "both")),
    )

    events = read_csv(data, "pipeline_day/link_events.csv")
    changed = [r for r in events if num(r, "change") != 0]

    def event_change(kind):
        return n(-num(one(events, type=kind), "change"))

    c.update(ev_aobt=event_change("AOBT"), ev_aibt=event_change("AIBT"))
    for kind in ("first_seen", "last_seen", "top-of-climb", "top-of-descent"):
        if -num(one(events, type=kind), "change") != num(level, "scored_holes"):
            raise MissingValue(f"{kind} did not fall by the number of scored holes: the prose needs rewriting")
    c["table_events"] = table(
        ["Event type (on aircraft with a linked hole)", "Previous rule", "New rule", "Change"],
        [[f"`{r['type']}`", n(num(r, "debounced_events")), n(num(r, "cruise_linked_events")),
          f"{int(num(r, 'change')):+d}"] for r in sorted(changed, key=lambda r: num(r, "change"))],
    ) if changed else "No event type changed in number on the aircraft with a linked hole."

    # --- provenance ---
    shas = set()
    for job in ("two_days", "pipeline_day", "descent"):
        path = data / job / "_manifest.json"
        if not path.is_file():
            raise MissingValue(f"missing {path}")
        for entry in json.loads(path.read_text()).values():
            shas.add((entry["git_sha"], bool(entry["git_dirty"])))
    c["manifest_shas"] = ", ".join(f"`{s}`" + (" (dirty)" if d else "") for s, d in sorted(shas))
    return c


V5 = Paper("v5", TITLE, SUBTITLE, ALLOWED_LITERALS, build_context)


def main(argv=None):
    return v4.main(argv, paper=V5, paper_dir=PAPER, description=__doc__)


if __name__ == "__main__":
    sys.exit(main())
