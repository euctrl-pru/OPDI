#!/usr/bin/env python
"""Fill the track-construction-v4 report from its staged CSVs.

``report_template.md`` in the paper directory holds the prose, with ``{{ name }}``
placeholders. Each value is computed here from a CSV under ``data/``, percentages
included, and any missing file, row, column or placeholder stops the build, so a gap
can never read as a result. It writes:

    index.qmd    front matter, the regeneration chunk, then the prose with literal numbers
    index.html   the same prose as one standalone page

Quarto and R do not run on the study's machine, so ``index.html`` comes from the small
converter below rather than from a render.

    python benchmarks/build_track_v4_paper.py [--paper-dir DIR] [--check]

``--check`` writes nothing and fails if ``index.qmd`` differs from what the CSVs give.
"""

import argparse
import csv
import html
import json
import math
import os
import re
import sys
from pathlib import Path

import jinja2

REPO = Path(__file__).resolve().parent.parent
PAPER = Path(
    os.environ.get(
        "OPDI_PAPER_DIR",
        REPO.parent / "opdi-portal" / "papers" / "track-construction-v4",
    )
)

TITLE = "One flight, one track across a reception gap"
SUBTITLE = (
    "Version 4: a rule that keeps one track_id across a long ADS-B gap, what it gains, "
    "and why it should not be the default yet"
)
DESCRIPTION = TITLE

#: The one figure the study recorded but never staged as a CSV: a one-off check of
#: 2026-06-14 written down in the study's ledger. The prose says so where it uses it.
UNSTAGED_BLANK_EDGE_HOLES = 1073
UNSTAGED_CRUISE_HOLES = 7355

#: Numbers the prose may state without a placeholder: definitions and limits of the
#: study and figures quoted from the literature review, never measurements.
ALLOWED_LITERALS = {
    "3",     # hours a day's run reads back under the other rules
    "5",     # samples at an edge; the 5% limit
    "10",    # public practice, minutes
    "30",    # the current rule's gap, minutes; public practice
    "0.5",   # OpenSky's rejoin tolerance, degrees
    "2021",  # year of that paper
    "1",     # the 1% limit
    "0.1",   # the 0.1% limit
    "300",   # labelled holes the direction limit wants
    "95",    # the confidence of the upper bounds; share of holes in a percentile
}

TEST_NAMES = {
    "cruise_at_new_edge": "Below the floor after the hole",
    "cruise_at_old_edge": "Below the floor before the hole",
    "implied_speed_not_too_slow": "Implied speed too slow, or no position to compute it",
    "gap_not_too_long": "Hole too long",
    "callsign_agrees": "Callsign differs",
    "implied_speed_not_impossible": "Implied speed impossible",
    "not_descend_then_climb": "Descent then climb",
}


class MissingValue(SystemExit):
    """A value the report needs is not there."""


def read_csv(data, relative):
    path = Path(data) / relative
    if not path.is_file():
        raise MissingValue(f"missing {path}: the report cannot be built without it")
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        raise MissingValue(f"{path} has no rows")
    return rows


def same(a, b):
    """Filter equality, numeric where both sides are numbers ('400.0' matches 400)."""
    if str(a) == str(b):
        return True
    try:
        return float(a) == float(b)
    except (TypeError, ValueError):
        return False


def where(rows, **filters):
    return [r for r in rows if all(same(r.get(k), v) for k, v in filters.items())]


def one(rows, **filters):
    found = where(rows, **filters)
    if len(found) != 1:
        raise MissingValue(f"expected one row for {filters}, found {len(found)}")
    return found[0]


def num(row, column):
    """A column as a float; a blank or absent cell stops the build."""
    value = row.get(column)
    if value in (None, ""):
        raise MissingValue(f"no value for {column}")
    return float(value)


def n(x):
    return f"{int(round(float(x))):,}"


def pct(part, whole, digits=1):
    if float(whole) == 0:
        raise MissingValue(f"percentage of zero: {part} of {whole}")
    return f"{100.0 * float(part) / float(whole):.{digits}f}%"


def table(header, rows):
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    lines += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return "\n".join(lines)


def int_col(row, column):
    return int(num(row, column))


def labelled(row):
    return int_col(row, "linked_true_link") + int_col(row, "linked_false_join")


def setting(rows, speed, gap, ratio=0.0, stop=0.0):
    return one(
        rows,
        link_min_implied_speed_kt=speed,
        link_max_gap_minutes=gap,
        link_min_speed_ratio=ratio,
        link_min_stop_minutes=stop,
    )


def settings_row(label, r):
    lab = labelled(r)
    missed = int_col(r, "positive_within_gap") - int_col(r, "positive_linked")
    return [
        label,
        pct(num(r, "crossing_flights_in_one_track"), num(r, "crossing_flights")),
        pct(num(r, "flights_in_one_track"), num(r, "flights")),
        f"{n(r['linked_false_join'])} of {n(lab)} ({pct(num(r, 'linked_false_join'), lab, 2)})",
        f"{n(r['negative_limit_false_joins'])} of {n(r['negative_limit_controls'])}",
        pct(missed, num(r, "positive_within_gap")),
        "yes" if r["within_limits"] == "True" else "no",
    ]


def build_context(data):
    """Every placeholder of the report, as formatted strings."""
    data = Path(data)
    c = {}

    # --- the chosen settings, over both days ---
    rec = one(read_csv(data, "final_grid/link_recommended.csv"), status="recommended")
    grid = read_csv(data, "final_grid/link_grid_pooled.csv")
    if not same(rec["point_id"], setting(grid, 400, 600)["point_id"]):
        raise MissingValue("the recommended row is not 400 kt, 600 min: the prose needs rewriting")
    speed, gap = num(rec, "link_min_implied_speed_kt"), num(rec, "link_max_gap_minutes")
    floor = num(rec, "link_cruise_floor_ft")
    c.update(
        floor_ft=n(floor),
        min_speed_kt=n(speed),
        max_speed_kt=n(num(rec, "link_max_implied_speed_kt")),
        max_gap_min=n(gap),
        vert_rate_fpm=n(num(rec, "link_edge_vert_rate_fpm")),
        lookahead_min=n(num(rec, "link_callsign_lookahead_minutes")),
        lookback_min=n(num(rec, "link_callsign_lookback_minutes")),
        edge_position_min=n(num(rec, "link_edge_position_minutes")),
        read_back_hours=n(
            math.ceil(gap / 60)
            + math.ceil(max(num(rec, "link_callsign_lookback_minutes"),
                            num(rec, "link_edge_position_minutes")) / 60)
        ),
    )

    fl, cf, of = num(rec, "flights"), num(rec, "crossing_flights"), num(rec, "other_flights")
    c.update(
        crossing_flights=n(cf),
        crossing_base=pct(num(rec, "base_crossing_flights_in_one_track"), cf),
        crossing_new=pct(num(rec, "crossing_flights_in_one_track"), cf),
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
        ["Flights", "Number", "In one track, current rule", "In one track, new rule",
         "Tracks per flight, current", "Tracks per flight, new"],
        [
            share_row("Crossing the European box", cf,
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

    # --- false joins ---
    lab = labelled(rec)
    linked = num(rec, "real_holes_linked")
    limit_n = num(rec, "negative_limit_controls")
    same_n = num(rec, "same_callsign_controls")
    days = {d: one(read_csv(data, f"final_grid/link_grid_{d}.csv"), point_id=rec["point_id"])
            for d in ("2026-06-14", "2025-06-14")}
    c.update(
        day_new="2026-06-14",
        day_old="2025-06-14",
        real_holes=n(num(rec, "real_holes")),
        linked_holes=n(linked),
        false_joins=n(num(rec, "linked_false_join")),
        labelled_holes=n(lab),
        false_join_rate=pct(num(rec, "linked_false_join"), lab, 2),
        unknown_holes=n(num(rec, "linked_unknown")),
        unknown_share=pct(num(rec, "linked_unknown"), linked),
        false_joins_near_edge=n(num(rec, "false_joined_near_edge")),
        false_joins_blank_edge=n(num(rec, "false_joined_edge_read_through_blank")),
        false_joins_day_new=n(num(days["2026-06-14"], "linked_false_join")),
        false_joins_day_old=n(num(days["2025-06-14"], "linked_false_join")),
        limit_controls=n(limit_n),
        limit_joined=n(num(rec, "negative_limit_false_joins")),
        limit_upper=pct(num(rec, "negative_false_join_upper_95"), 1, 3),
        pairs=n(num(rec, "negative_pairs")),
        shallow_controls=n(num(rec, "negative_shallow_controls")),
        shallow_joined=n(num(rec, "negative_shallow_false_joins")),
        no_callsign_joined=n(num(rec, "negative_limit_false_joins_no_callsign")),
        no_callsign_upper=pct(num(rec, "negative_no_callsign_upper_95"), 1, 3),
        same_controls=n(same_n),
        same_joined=n(num(rec, "same_callsign_false_joins")),
        same_upper=pct(num(rec, "same_callsign_upper_95"), 1, 2),
        same_blank=n(num(rec, "same_callsign_blank")),
        same_blank_share=pct(num(rec, "same_callsign_blank"), same_n),
        positive_tests=n(num(rec, "positive_within_gap")),
        positive_missed=n(num(rec, "positive_within_gap") - num(rec, "positive_linked")),
        positive_missed_share=pct(num(rec, "positive_within_gap") - num(rec, "positive_linked"),
                                  num(rec, "positive_within_gap")),
    )

    # Controls in the limit, by gap length: how thin the hard cases get.
    controls = where(read_csv(data, "final_grid/link_controls_pooled.csv"),
                     point_id=rec["point_id"], kind="negative", counts_toward_limit="True")
    same_rows = where(controls, same_callsign="True")
    smallest = min(same_rows, key=lambda r: num(r, "n"))
    c.update(
        smallest_same_n=n(num(smallest, "n")),
        smallest_same_upper=pct(num(smallest, "wrong_upper_95"), 1, 0),
        smallest_same_band=smallest["gap_stratum"],
    )
    if sum(num(r, "n") for r in controls) != limit_n:
        raise MissingValue("the controls table does not add up to the limit's count")

    # All other hidden-landing tests have an edge below the floor by construction.
    neg_reasons = read_csv(data, "final_grid/link_negative_reasons_pooled.csv")
    low = [r for r in neg_reasons
           if r["control_family"] in ("climb_cruise", "climb_descent", "cruise_descent")]
    c["low_edge_tests"] = n(sum(num(r, "controls") for r in low))

    # --- the settings table ---
    c["table_settings"] = table(
        ["Speed floor", "Longest hole", "Crossing flights in one track", "All flights in one track",
         "Real false joins, of checkable", "Hidden landings joined",
         "Cut flights not rejoined", "Within limits"],
        [
            [f"{n(sp)} kt", f"{n(g)} min", *settings_row("", setting(grid, sp, g))[1:]]
            for sp, g in ((400, 600), (400, 480), (400, 360), (315, 600), (315, 480), (315, 360))
        ],
    )
    s315_600, s315_480 = setting(grid, 315, 600), setting(grid, 315, 480)
    s_ratio = setting(grid, 400, 600, ratio=0.85)
    s_stop35, s_stop20 = setting(grid, 400, 600, stop=35), setting(grid, 400, 600, stop=20)
    c.update(
        s315_600_crossing=pct(num(s315_600, "crossing_flights_in_one_track"), cf),
        s315_600_false=n(num(s315_600, "linked_false_join")),
        s315_600_rate=pct(num(s315_600, "linked_false_join"), labelled(s315_600), 2),
        slow_speed_kt=n(315),
        s315_480_gap=n(480),
        s315_480_crossing_n=n(num(s315_480, "crossing_flights_in_one_track")),
        chosen_crossing_n=n(num(rec, "crossing_flights_in_one_track")),
        crossing_tie_gap=n(num(rec, "crossing_flights_in_one_track")
                           - num(s315_480, "crossing_flights_in_one_track")),
        s315_480_false=n(num(s315_480, "linked_false_join")),
        s315_480_hidden=n(num(s315_480, "negative_limit_false_joins")),
        s315_480_missed=pct(num(s315_480, "positive_within_gap") - num(s315_480, "positive_linked"),
                            num(s315_480, "positive_within_gap")),
        ratio_value="0.85",
        ratio_crossing=pct(num(s_ratio, "crossing_flights_in_one_track"), cf),
        ratio_false=n(num(s_ratio, "linked_false_join")),
        stop35_min=n(35),
        stop20_min=n(20),
        stop35_crossing=pct(num(s_stop35, "crossing_flights_in_one_track"), cf),
        stop20_crossing=pct(num(s_stop20, "crossing_flights_in_one_track"), cf),
        stop35_false=n(num(s_stop35, "linked_false_join")),
    )

    # --- what keeps holes split ---
    reasons = read_csv(data, "final_grid/link_reasons_pooled.csv")

    def reason_total(population, test, column):
        return sum(num(r, column) for r in where(reasons, population=population,
                                                 first_failed_test=test))

    def split_total(population):
        return sum(num(r, "holes") for r in where(reasons, population=population)
                   if r["first_failed_test"] != "linked")

    tests = sorted(
        {r["first_failed_test"] for r in reasons if r["first_failed_test"] != "linked"},
        key=lambda t: -(reason_total("crosses_box", t, "holes") + reason_total("other", t, "holes")),
    )
    unknown_tests = set(tests) - set(TEST_NAMES)
    if unknown_tests:
        raise MissingValue(f"no plain name for tests {sorted(unknown_tests)}")
    rows = [
        [TEST_NAMES[t], n(reason_total("crosses_box", t, "holes")),
         n(reason_total("crosses_box", t, "only_failed_test_holes")),
         n(reason_total("other", t, "holes")), n(reason_total("other", t, "only_failed_test_holes"))]
        for t in tests
    ]
    rows.append(["**All holes left split**", f"**{n(split_total('crosses_box'))}**", "",
                 f"**{n(split_total('other'))}**", ""])
    c["table_reasons"] = table(
        ["First test failed", "Crossing flights: holes", "of which failed nothing else",
         "Other flights: holes", "of which failed nothing else"], rows)
    c.update(
        crossing_holes_linked=n(reason_total("crosses_box", "linked", "holes")),
        crossing_holes_split=n(split_total("crosses_box")),
        new_edge_holes=n(reason_total("crosses_box", "cruise_at_new_edge", "holes")),
        new_edge_only=n(reason_total("crosses_box", "cruise_at_new_edge", "only_failed_test_holes")),
        new_edge_share=pct(reason_total("crosses_box", "cruise_at_new_edge", "holes"),
                           split_total("crosses_box")),
        too_long_holes=n(reason_total("crosses_box", "gap_not_too_long", "holes")),
    )

    # --- artificial gaps missed ---
    positive = read_csv(data, "final_grid/link_positive_reasons_pooled.csv")
    pos_rows = where(positive, control_family="cruise_cruise")

    def by_gap(req):
        return [r for r in pos_rows if same(r["requested_gap_min"], req)]

    pos_controls = where(read_csv(data, "final_grid/link_controls_pooled.csv"),
                          point_id=rec["point_id"], kind="positive", control_family="cruise_cruise")
    miss = {}
    for req in (60, 120, 180, 360):
        tested = sum(num(r, "n") for r in pos_controls if same(r["requested_gap_min"], req))
        missed = sum(num(r, "positive_missed_by_test") for r in by_gap(req))
        miss[req] = pct(missed, tested)
    c.update(miss_60=miss[60], miss_120=miss[120], miss_180=miss[180], miss_360=miss[360],
             gap_60=n(60), gap_120=n(120), gap_180=n(180), gap_360=n(360))
    slow_missed = sum(num(r, "positive_missed_by_test") for r in pos_rows
                      if r["first_failed_test"] == "implied_speed_not_too_slow")
    c.update(
        positive_too_slow=n(slow_missed),
        positive_too_slow_share=pct(slow_missed, num(rec, "positive_within_gap")
                                    - num(rec, "positive_linked")),
        s315_missed=pct(num(s315_600, "positive_within_gap") - num(s315_600, "positive_linked"),
                        num(s315_600, "positive_within_gap")),
        real_linked_drop=pct(num(s315_600, "real_holes_linked") - linked,
                             num(s315_600, "real_holes_linked")),
        tests_linked_drop=pct(num(s315_600, "positive_linked") - num(rec, "positive_linked"),
                              num(s315_600, "positive_linked")),
        other_flight_share=pct(of, fl),
    )

    # --- how the method got here ---
    round2 = read_csv(data, "round2/link_grid_pooled.csv")
    fl250 = read_csv(data, "round3_fl250/link_grid_pooled.csv")
    fl180 = read_csv(data, "round3_fl180/link_grid_pooled.csv")
    fl100 = read_csv(data, "round3_fl100/link_grid_pooled.csv")
    r2 = setting(round2, 400, 360)
    b360, b600 = setting(fl250, 400, 360), setting(fl250, 400, 600)
    r180 = setting(fl180, 400, 600)
    r100, r100_ratio = setting(fl100, 400, 600), setting(fl100, 400, 600, ratio=0.85)
    if num(r180, "crossing_flights_in_one_track") != num(rec, "crossing_flights_in_one_track"):
        raise MissingValue("the earlier 18,000 ft run no longer matches the final one")
    c.update(
        blank_edge_holes=n(UNSTAGED_BLANK_EDGE_HOLES),
        cruise_holes=n(UNSTAGED_CRUISE_HOLES),
        blank_edge_share=pct(UNSTAGED_BLANK_EDGE_HOLES, UNSTAGED_CRUISE_HOLES, 0),
        floor_high_ft=n(num(b360, "link_cruise_floor_ft")),
        floor_low_ft=n(num(r100, "link_cruise_floor_ft")),
        gap_short_min=n(360),
        before_blank_crossing=pct(num(r2, "crossing_flights_in_one_track"), cf),
        after_blank_crossing=pct(num(b360, "crossing_flights_in_one_track"), cf),
        before_blank_linked=n(num(r2, "real_holes_linked")),
        after_blank_linked=n(num(b360, "real_holes_linked")),
        before_blank_all=pct(num(r2, "flights_in_one_track"), fl),
        final_blank_linked=n(num(rec, "linked_edge_read_through_blank")),
        final_blank_share=pct(num(rec, "linked_edge_read_through_blank"), linked),
        high_floor_long_crossing=pct(num(b600, "crossing_flights_in_one_track"), cf),
        low_floor_crossing=pct(num(r100, "crossing_flights_in_one_track"), cf),
        low_floor_false=n(num(r100, "linked_false_join")),
        low_floor_labelled=n(labelled(r100)),
        low_floor_rate=pct(num(r100, "linked_false_join"), labelled(r100), 2),
        low_floor_ratio_crossing=pct(num(r100_ratio, "crossing_flights_in_one_track"), cf),
        low_floor_ratio_false=n(num(r100_ratio, "linked_false_join")),
        low_floor_ratio_labelled=n(labelled(r100_ratio)),
        low_floor_ratio_rate=pct(num(r100_ratio, "linked_false_join"), labelled(r100_ratio), 2),
    )

    direction = read_csv(data, "final_direction/link_grid_pooled.csv")
    d_on = one(direction, link_directional_edges="True")
    c.update(
        dir_crossing=pct(num(d_on, "crossing_flights_in_one_track"), cf),
        dir_false=n(num(d_on, "false_joined_by_direction")),
        dir_labelled=n(num(d_on, "linked_by_direction_labelled")),
        dir_rate=pct(num(d_on, "false_joined_by_direction"), num(d_on, "linked_by_direction_labelled")),
        dir_all_false=n(num(d_on, "linked_false_join")),
        dir_all_labelled=n(labelled(d_on)),
        dir_all_rate=pct(num(d_on, "linked_false_join"), labelled(d_on), 3),
        dir_tests=n(num(d_on, "negative_low_edge_controls")),
        dir_tests_joined=n(num(d_on, "negative_low_edge_false_joins")),
        dir_same_tests=n(num(d_on, "same_callsign_low_edge_controls")),
        dir_same_joined=n(num(d_on, "same_callsign_low_edge_false_joins")),
    )
    if d_on["within_limits"] != "False":
        raise MissingValue("direction-aware edges now pass their limits: the prose needs rewriting")

    # --- the real pipeline day ---
    both = read_csv(data, "stage2/link_both_ends.csv")
    names = (("crosses_the_box", "Crossing the European box"),
             ("europe_to_outside", "of which Europe to outside"),
             ("outside_to_europe", "of which outside to Europe"),
             ("intra_europe", "Inside Europe"))

    def both_row(flow, label):
        r = one(both, flow=flow)
        return [
            label, n(num(r, "n")),
            f"{num(r, 'debounced_rows_per_flight'):.2f}", f"{num(r, 'cruise_linked_rows_per_flight'):.2f}",
            n(num(r, "debounced_one_row")), n(num(r, "cruise_linked_one_row")),
            n(num(r, "debounced_both_right")), n(num(r, "cruise_linked_both_right")),
        ]

    c["table_flight_list"] = table(
        ["Flights", "Number", "Rows per flight, current", "Rows per flight, new",
         "On one row, current", "On one row, new", "Both airports right on one row, current",
         "Both airports right on one row, new"],
        [both_row(flow, label) for flow, label in names],
    )
    cross = one(both, flow="crosses_the_box")
    intra = one(both, flow="intra_europe")
    pairs = read_csv(data, "stage2/link_flight_list.csv")
    callsign = where(pairs, view="callsign")
    far_none = sum(num(r, "debounced_none") for r in where(callsign, end="far")
                   if r["flow"] in ("europe_to_outside", "outside_to_europe"))
    c.update(
        s2_crossing=n(num(cross, "n")),
        s2_both_base=n(num(cross, "debounced_both_right")),
        s2_both_new=n(num(cross, "cruise_linked_both_right")),
        s2_both_share=pct(num(cross, "cruise_linked_both_right"), num(cross, "n")),
        s2_one_row_base_share=pct(num(cross, "debounced_one_row"), num(cross, "n")),
        s2_one_row_new_share=pct(num(cross, "cruise_linked_one_row"), num(cross, "n")),
        s2_more_rows=n(num(cross, "more_rows")),
        s2_far_none=n(far_none),
        s2_far_none_share=pct(far_none, num(cross, "n")),
        s2_none_to_right=n(sum(num(r, "none_to_right") for r in pairs)),
        intra_gained=n(num(intra, "gained")),
        intra_lost=n(num(intra, "lost")),
        intra_n=n(num(intra, "n")),
        intra_ades_right_to_wrong=n(num(one(callsign, flow="intra_europe", side="ades"), "right_to_wrong")),
        intra_ades_wrong_to_right=n(num(one(callsign, flow="intra_europe", side="ades"), "wrong_to_right")),
    )

    part = one(read_csv(data, "stage2/link_partition_check.csv"))
    moves = read_csv(data, "stage2/link_movements.csv")
    move_all = one(moves, movement="all")
    if part["passed"] != "True" or move_all["passed"] != "True":
        raise MissingValue("a nothing-else-moved check failed: the prose needs rewriting")
    c.update(
        boundaries_base=n(num(part, "debounced_boundaries")),
        boundaries_new=n(num(part, "cruise_linked_boundaries")),
        boundaries_removed=n(num(part, "removed_boundaries")),
        boundaries_at_holes=n(num(part, "removed_at_a_linked_hole")),
        boundaries_elsewhere=n(num(part, "removed_elsewhere")),
        boundaries_added=n(num(part, "added_boundaries")),
        arm_tracks=n(num(part, "arm_tracks")),
        moves_total=n(num(move_all, "apdf_movements")),
        moves_base=n(num(move_all, "debounced_matched")),
        moves_new=n(num(move_all, "cruise_linked_matched")),
    )

    events = read_csv(data, "stage2/link_events.csv")
    changed = [r for r in events if num(r, "change") != 0]
    c["table_events"] = table(
        ["Event type", "Current rule", "New rule", "Change"],
        [[f"`{r['type']}`", n(num(r, "debounced_events")), n(num(r, "cruise_linked_events")),
          f"{int(num(r, 'change')):+d}"] for r in sorted(changed, key=lambda r: num(r, "change"))],
    )

    def event_change(kind):
        return f"{int(num(one(events, type=kind), 'change')):+d}"

    c.update(
        ev_aobt=event_change("AOBT"),
        ev_aibt=event_change("AIBT"),
        ev_fl245=event_change("xing-fl245"),
        ev_fl100=event_change("xing-fl100"),
    )

    truth = read_csv(data, "stage2/link_truth_all_flights.csv")
    label_names = (("true_link", "True link"), ("false_join", "False join"), ("unknown", "Unknown"),
                   ("airframe_not_in_truth", "Airframe not in Network Manager"))
    c["table_truth"] = table(
        ["Label", "Holes", "With a landing or on-block event on the older piece",
         "With a take-off or off-block event on the newer piece", "With both"],
        [[label, *(n(num(one(truth, label=key), col))
                   for col in ("holes", "landing_on_older", "take_off_on_newer", "both"))]
         for key, label in label_names],
    )
    t_true = one(truth, label="true_link")
    t_false = one(truth, label="false_join")
    t_unknown = one(truth, label="unknown")
    t_out = one(truth, label="airframe_not_in_truth")
    t_all = sum(num(one(truth, label=key), "holes") for key, _ in label_names)
    c.update(
        truth_holes=n(t_all),
        truth_true=n(num(t_true, "holes")),
        truth_false=n(num(t_false, "holes")),
        truth_unknown=n(num(t_unknown, "holes")),
        truth_out=n(num(t_out, "holes")),
        truth_true_both=n(num(t_true, "both")),
    )

    level = one(read_csv(data, "stage2/link_level_across_hole.csv"), family="any")
    dist = one(read_csv(data, "stage2/link_distance_across_hole.csv"))
    if dist["stored_distance_verdict"] != "agrees":
        raise MissingValue("the stored distance no longer agrees with the rerun")
    c.update(
        level_tracks=n(num(level, "tracks_with_a_segment_across_a_hole")),
        level_scored=n(num(level, "scored_tracks")),
        level_share=pct(num(level, "tracks_with_a_segment_across_a_hole"), num(level, "scored_tracks")),
        level_segments=n(num(level, "segments_across_a_hole")),
        level_holes=n(num(level, "holes_inside_a_segment")),
        scored_holes=n(num(level, "scored_holes")),
        linked_anywhere=n(num(level, "linked_holes_anywhere")),
        dist_median=n(-num(dist, "median_difference_nm")),
        dist_p95=n(-num(dist, "p95_difference_nm")),
        dist_compared=n(num(dist, "holes_compared")),
        dist_off=n(num(dist, "holes_off_by_over_1_nm")),
        dist_masked=n(num(dist, "holes_with_a_masked_edge")),
        dist_tracks=n(num(dist, "tracks_checked_against_stored_distance")),
        dist_tracks_off=n(num(dist, "tracks_off_stored_distance_by_over_1_nm")),
    )

    # --- provenance ---
    shas = set()
    for job in ("final_grid", "final_direction", "stage2"):
        path = data / job / "_manifest.json"
        if not path.is_file():
            raise MissingValue(f"missing {path}")
        for entry in json.loads(path.read_text()).values():
            shas.add((entry["git_sha"], bool(entry["git_dirty"])))
    c["manifest_shas"] = ", ".join(f"`{s}`" + (" (dirty)" if d else "") for s, d in sorted(shas))
    return c


def render(template_text, context):
    """The template with every placeholder filled; an unknown name stops the build."""
    env = jinja2.Environment(undefined=jinja2.StrictUndefined, keep_trailing_newline=True)
    try:
        return env.from_string(template_text).render(**context)
    except jinja2.UndefinedError as e:
        raise MissingValue(f"report_template.md: {e}") from e


def literal_numbers(template_text):
    """Numbers typed into the prose rather than coming from a placeholder or the allow-list."""
    text = re.sub(r"\{\{.*?\}\}", " ", template_text)
    text = re.sub(r"```.*?```", " ", text, flags=re.S)
    text = re.sub(r"`[^`]*`", " ", text)
    text = re.sub(r"\]\([^)]*\)", "]", text)
    found = re.findall(r"(?<![\w.])\d+(?:,\d{3})*(?:\.\d+)?", text)
    return sorted({t for t in found if t not in ALLOWED_LITERALS})


# --- a small Markdown converter: headings, paragraphs, lists, tables, code, bold, links ---

def inline(text):
    held = []

    def hold(markup):
        held.append(markup)
        return f"\0{len(held) - 1}\0"

    text = re.sub(r"`([^`]+)`", lambda m: hold(f"<code>{html.escape(m.group(1))}</code>"), text)
    text = html.escape(text)
    text = re.sub(r"\[([^\]]+)\]\(([^)\s]+)\)",
                  lambda m: f'<a href="{m.group(2)}">{m.group(1)}</a>', text)
    text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
    return re.sub(r"\0(\d+)\0", lambda m: held[int(m.group(1))], text)


def _cells(line):
    return [c.strip() for c in line.strip().strip("|").split("|")]


def markdown_to_html(md):
    lines, out, i = md.split("\n"), [], 0
    while i < len(lines):
        line = lines[i]
        if not line.strip():
            i += 1
        elif line.startswith("```"):
            i += 1
            code = []
            while i < len(lines) and not lines[i].startswith("```"):
                code.append(lines[i])
                i += 1
            i += 1
            out.append(f"<pre><code>{html.escape(chr(10).join(code))}</code></pre>")
        elif re.match(r"#{1,6} ", line):
            level = len(line) - len(line.lstrip("#"))
            out.append(f"<h{level}>{inline(line[level:].strip())}</h{level}>")
            i += 1
        elif line.startswith("|"):
            block = []
            while i < len(lines) and lines[i].startswith("|"):
                block.append(lines[i])
                i += 1
            head = "".join(f"<th>{inline(c)}</th>" for c in _cells(block[0]))
            body = "".join(
                "<tr>" + "".join(f"<td>{inline(c)}</td>" for c in _cells(r)) + "</tr>"
                for r in block[2:]
            )
            out.append(f'<div class="table-wrap"><table><thead><tr>{head}</tr></thead>'
                       f"<tbody>{body}</tbody></table></div>")
        elif re.match(r"(- |\d+\. )", line):
            tag = "ul" if line.startswith("- ") else "ol"
            items = []
            while i < len(lines) and (re.match(r"(- |\d+\. )", lines[i])
                                      or (items and lines[i].startswith("  "))):
                if lines[i].startswith("  "):
                    items[-1] += " " + lines[i].strip()
                else:
                    items.append(re.sub(r"^(- |\d+\. )", "", lines[i]))
                i += 1
            out.append(f"<{tag}>" + "".join(f"<li>{inline(x)}</li>" for x in items) + f"</{tag}>")
        else:
            para = []
            while i < len(lines) and lines[i].strip() and not re.match(
                r"(#{1,6} |\||```|- |\d+\. )", lines[i]
            ):
                para.append(lines[i].strip())
                i += 1
            if not para:  # a line no branch above took: never loop on it
                para.append(lines[i].strip())
                i += 1
            out.append(f"<p>{inline(' '.join(para))}</p>")
    return "\n".join(out)


CSS = """
:root { --bg: #ffffff; --fg: #1f2328; --muted: #57606a; --line: #d0d7de; --band: #f6f8fa; --link: #0a58b0; }
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) { --bg: #14181d; --fg: #e6e9ec; --muted: #a0a9b3; --line: #39424c; --band: #1d232a; --link: #7db4f0; }
}
:root[data-theme="dark"] { --bg: #14181d; --fg: #e6e9ec; --muted: #a0a9b3; --line: #39424c; --band: #1d232a; --link: #7db4f0; }
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--fg);
  font: 16px/1.55 system-ui, -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }
main { max-width: 860px; margin: 0 auto; padding: 24px 16px 64px; }
h1 { font-size: 1.8rem; line-height: 1.2; margin: 0 0 .4rem; }
.subtitle { color: var(--muted); margin: 0 0 1rem; }
.note { color: var(--muted); font-size: .9rem; border-top: 1px solid var(--line); padding-top: .6rem; }
h2 { font-size: 1.25rem; margin: 2.2rem 0 .6rem; }
a { color: var(--link); }
code { font: .9em ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; background: var(--band); padding: 0 .2em; border-radius: 3px; }
pre { background: var(--band); padding: .8rem; overflow-x: auto; border-radius: 4px; }
pre code { padding: 0; }
.table-wrap { overflow-x: auto; margin: 1rem 0; }
table { border-collapse: collapse; font-size: .9rem; min-width: 100%; }
th, td { border-bottom: 1px solid var(--line); padding: .4rem .6rem; text-align: left; vertical-align: top; }
th { background: var(--band); font-weight: 600; }
"""


def to_html(md):
    return (
        '<!doctype html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f"<title>{html.escape(DESCRIPTION)}</title>\n<style>{CSS}</style>\n</head>\n<body>\n<main>\n"
        f"<h1>{html.escape(TITLE)}</h1>\n<p class=\"subtitle\">{html.escape(SUBTITLE)}</p>\n"
        '<p class="note">EUROCONTROL Performance Review Unit. Built from the staged CSVs by '
        "<code>benchmarks/build_track_v4_paper.py</code>; not a Quarto render.</p>\n"
        f"{markdown_to_html(md)}\n</main>\n</body>\n</html>\n"
    )


REGENERATE_CHUNK = r"""```{r regenerate}
#| include: false
#| cache: false

# Rendering this page runs the analysis behind it.
#
# `regenerate_track_v4.py` is the executable definition of the study: each CSV
# behind this page is produced by exactly one job it declares, with exactly the
# arguments it declares. It re-runs a job when the *code that job depends on*
# has changed since the output was written, not when the file is old.
#
# The prose below holds literal numbers, filled in by
# `benchmarks/build_track_v4_paper.py`, so regenerating alone would leave them
# behind. The second call therefore fails the render if this file is no longer
# what the CSVs give: run the builder, then render again.
#
# OPDI_RENDER selects what happens instead:
#
#   run          (default) regenerate anything stale, then render
#   check        fail fast naming every stale job, regenerate nothing
#   allow-stale  render a draft from whatever is staged, after naming staleness
#   skip         do not invoke the entrypoint at all
REPO   <- Sys.getenv("OPDI_REPO_DIR",
                     normalizePath(file.path(getwd(), "..", "..", "..", "opdi"),
                                   mustWork = FALSE))
PYTHON <- Sys.getenv("OPDI_PYTHON", file.path(REPO, ".venv310", "bin", "python"))
MODE   <- Sys.getenv("OPDI_RENDER", "run")

# A missing entrypoint must not read as "nothing to regenerate". Skipping it
# silently is how a page ends up publishing numbers no command can account for.
regen <- file.path(REPO, "benchmarks", "regenerate_track_v4.py")
build <- file.path(REPO, "benchmarks", "build_track_v4_paper.py")
if (!file.exists(regen) && MODE != "skip") {
  stop("regenerate_track_v4.py not found under REPO=", REPO,
       " -- set OPDI_REPO_DIR, or OPDI_RENDER=skip to render without it.")
}

# `skip` short-circuits before system2, so no Python process starts and no Spark
# session is created. Every other mode reaches the entrypoint.
if (MODE != "skip") {
  flags <- switch(MODE,
                  "run" = character(0),
                  "check" = "--check",
                  "allow-stale" = c("--check", "--allow-stale"),
                  character(0))
  status <- system2(PYTHON, c(shQuote(regen), flags), stdout = "", stderr = "")
  if (!identical(status, 0L)) {
    stop("regenerate_track_v4.py exited ", status,
         " -- the figures below would not match the code that is checked out. ",
         "Set OPDI_RENDER=allow-stale to render anyway.")
  }
  if (MODE != "allow-stale" &&
      !identical(system2(PYTHON, c(shQuote(build), "--paper-dir", shQuote(getwd()), "--check"),
                         stdout = "", stderr = ""), 0L)) {
    stop("index.qmd no longer matches the CSVs: run build_track_v4_paper.py and render again.")
  }
}
```
"""


def to_qmd(md):
    return (
        f'---\ntitle: "{TITLE}"\nsubtitle: "{SUBTITLE}"\n'
        'author: "EUROCONTROL Performance Review Unit"\ndate: last-modified\n---\n\n'
        f"{REGENERATE_CHUNK}\n{md}"
    )


def build(paper_dir):
    """``(qmd, html)`` for the paper directory."""
    paper_dir = Path(paper_dir)
    template = paper_dir / "report_template.md"
    if not template.is_file():
        raise MissingValue(f"missing {template}")
    text = template.read_text()
    stray = literal_numbers(text)
    if stray:
        raise MissingValue(f"report_template.md states numbers with no source: {stray}")
    md = render(text, build_context(paper_dir / "data"))
    return to_qmd(md), to_html(md)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--paper-dir", default=str(PAPER))
    ap.add_argument("--check", action="store_true", help="fail if index.qmd is out of date; write nothing")
    args = ap.parse_args(argv)
    paper_dir = Path(args.paper_dir)
    qmd, page = build(paper_dir)
    if args.check:
        current = paper_dir / "index.qmd"
        if not current.is_file() or current.read_text() != qmd:
            raise SystemExit(f"{current} is not what the CSVs give: run build_track_v4_paper.py")
        print("index.qmd matches the CSVs")
        return 0
    (paper_dir / "index.qmd").write_text(qmd)
    (paper_dir / "index.html").write_text(page)
    print(f"wrote {paper_dir / 'index.qmd'} and {paper_dir / 'index.html'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
