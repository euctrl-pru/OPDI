#!/usr/bin/env python
"""Fill the track-construction-v4 report from its staged CSVs.

``report_template.md`` in the paper directory holds the prose, with ``{{ name }}``
placeholders. Each value is computed here from a CSV under ``data/``, percentages
included, and any missing file, row, column or placeholder stops the build, so a gap
can never read as a result. It writes:

    index.qmd    front matter, the regeneration chunk, then the prose with literal numbers
    index.html   the same prose as one standalone page
    track-construction-v4.pdf   the same again as an A4 PDF, with --pdf (needs reportlab)

Quarto and R do not run on the study's machine, so ``index.html`` comes from the small
converter below rather than from a render.

    python benchmarks/build_track_v4_paper.py [--paper-dir DIR] [--check]

``--check`` writes nothing and fails if ``index.qmd`` differs from what the CSVs give.
"""

import argparse
import ast
import csv
import html
import json
import math
import os
import re
import sys
from datetime import date
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

#: Numbers the prose may state without a placeholder: definitions and limits of the
#: study and figures quoted from the literature review, never measurements.
ALLOWED_LITERALS = {
    "3",  # hours a day's run reads back under the other rules
    "5",  # the 5% limit
    "10",  # public practice, minutes
    "30",  # the current rule's gap, minutes; public practice
    "0.5",  # OpenSky's rejoin tolerance, degrees
    "2021",  # year of that paper
    "1",  # the 1% limit; the 1 NM tolerance on distance
    "0.1",  # the 0.1% limit
    "300",  # labelled holes a low-edge limit wants
    "95",  # the confidence of the upper bounds; share of holes in a percentile
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


#: Constants of the rule and the study the prose states, and the file that defines each.
CODE_CONSTANTS = {
    "edge_samples": ("LINK_EDGE_SAMPLES", "src/opdi/pipeline/segmentation/methods.py"),
    "min_rates": ("LINK_DIRECTION_MIN_RATES", "src/opdi/pipeline/segmentation/methods.py"),
    "near_edge_min": ("EVENT_NEAR_EDGE_MIN", "benchmarks/track_link_controls.py"),
}

FAMILY_NAMES = {"cruise_cruise": "cruise edges", "climb_cruise": "a climbing edge"}


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


def code_constant(name, relative):
    """A module-level number, read from the source and not imported.

    Those modules import pyspark, which the scratch environment that writes the PDF does
    not have. A test holds each value to the real import.
    """
    path = REPO / relative
    if not path.is_file():
        raise MissingValue(f"missing {path}")
    for node in ast.parse(path.read_text()).body:
        if (
            isinstance(node, ast.Assign)
            and isinstance(node.value, ast.Constant)
            and any(isinstance(t, ast.Name) and t.id == name for t in node.targets)
        ):
            return node.value.value
    raise MissingValue(f"no constant {name} in {path}")


def chance_all_in_one_drawn(events, side, total):
    """Chance all *events* land on a side of *side* of *total*, each case taken once."""
    if events > total or total <= 0:
        raise MissingValue(f"no chance for {events} events among {total}")
    return math.comb(int(side), int(events)) / math.comb(int(total), int(events))


def chance_all_in_one_repeated(events, side, total):
    """The same for rare events among so many tests that drawing them again changes nothing."""
    if total <= 0:
        raise MissingValue(f"no chance for {events} events among {total}")
    return (float(side) / float(total)) ** int(events)


def upper_95(events, trials):
    """One-sided 95% upper bound on a rate, as the study computes it (Clopper-Pearson).

    The study's own lives beside Spark imports the builder does not need. A test holds
    this one to the values in the staged CSVs, so the two cannot drift.
    """
    events, trials = int(events), int(trials)
    if trials <= 0:
        raise MissingValue(f"no upper bound on {events} of {trials}")

    def at_most_events(rate):
        term = (1.0 - rate) ** trials
        total = term
        for i in range(events):
            term *= (trials - i) / (i + 1) * rate / (1.0 - rate)
            total += term
        return total

    low, high = 0.0, 1.0
    for _ in range(100):
        mid = (low + high) / 2.0
        if at_most_events(mid) > 0.05:
            low = mid
        else:
            high = mid
    return high


def edge_rows(rows):
    """The four settings of the two edge flags, each named, each required to exist."""
    return {
        name: one(rows, link_old_edge_climb=climb, link_new_edge_descent=descent)
        for name, climb, descent in (
            ("off", "False", "False"),
            ("climb", "True", "False"),
            ("descent", "False", "True"),
            ("both", "True", "True"),
        )
    }


def check_edge_decision(rec, edges):
    """Stop if the staged data no longer says what the prose says about the two flags."""
    if rec.get("status") != "recommended_provisional" or rec.get("provisional") != "True":
        raise MissingValue(
            f"the pick is {rec.get('status')}, not provisional: the prose needs rewriting"
        )
    if rec.get("link_old_edge_climb") != "True" or rec.get("link_new_edge_descent") != "False":
        raise MissingValue("the pick is not climb on, descent off: the prose needs rewriting")
    verdicts = {name: r.get("within_limits") for name, r in edges.items()}
    if verdicts != {"off": "True", "climb": "True", "descent": "False", "both": "False"}:
        raise MissingValue(f"the edge settings now read {verdicts}: the prose needs rewriting")
    if edges["descent"].get("real_by_descent_status") != "over_limit":
        raise MissingValue(
            "descent edges no longer fail their own limit: the prose needs rewriting"
        )
    if edges["climb"].get("real_by_climb_status") != "insufficient":
        raise MissingValue(
            "climb edges are no longer short of labelled holes: the prose needs rewriting"
        )
    for column in (
        "crossing_flights_in_one_track",
        "linked_false_join",
        "negative_limit_false_joins",
    ):
        if num(rec, column) != num(edges["climb"], column):
            raise MissingValue(f"final_grid and final_edges differ on {column}")


def blank_edge_values(rec):
    """How often an edge's position is read past a blank, over all holes and over linked.

    The two are different populations, which the prose must say.
    """
    blank, holes = num(rec, "real_holes_edge_read_through_blank"), num(rec, "real_holes")
    return {
        "blank_edge_holes": n(blank),
        "blank_edge_share": pct(blank, holes),
    }


def build_context(data):
    """Every placeholder of the report, as formatted strings."""
    data = Path(data)
    c = {key: n(code_constant(*where_from)) for key, where_from in CODE_CONSTANTS.items()}

    # --- the chosen settings, over both days ---
    rec = one(read_csv(data, "final_grid/link_recommended.csv"))
    edge_grid = read_csv(data, "final_edges/link_grid_pooled.csv")
    edges = edge_rows(edge_grid)
    check_edge_decision(rec, edges)
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
            + math.ceil(
                max(
                    num(rec, "link_callsign_lookback_minutes"),
                    num(rec, "link_edge_position_minutes"),
                )
                / 60
            )
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
            name,
            n(flights),
            f"{n(base)} ({pct(base, flights)})",
            f"{n(new)} ({pct(new, flights)})",
            f"{base_tracks / flights:.2f}",
            f"{new_tracks / flights:.2f}",
        ]

    c["table_one_track"] = table(
        [
            "Flights",
            "Number",
            "In one track, current rule",
            "In one track, new rule",
            "Tracks per flight, current",
            "Tracks per flight, new",
        ],
        [
            share_row(
                "Crossing the European box",
                cf,
                num(rec, "base_crossing_flights_in_one_track"),
                num(rec, "crossing_flights_in_one_track"),
                num(rec, "base_crossing_tracks"),
                num(rec, "crossing_tracks"),
            ),
            share_row(
                "All others",
                of,
                num(rec, "base_other_flights_in_one_track"),
                num(rec, "other_flights_in_one_track"),
                num(rec, "base_other_tracks"),
                num(rec, "other_tracks"),
            ),
            share_row(
                "All",
                fl,
                num(rec, "base_flights_in_one_track"),
                num(rec, "flights_in_one_track"),
                num(rec, "base_tracks_on_flights"),
                num(rec, "tracks_on_flights"),
            ),
        ],
    )

    # --- false joins ---
    lab = labelled(rec)
    linked = num(rec, "real_holes_linked")
    limit_n = num(rec, "negative_limit_controls")
    same_n = num(rec, "same_callsign_controls")
    days = {
        d: one(read_csv(data, f"final_grid/link_grid_{d}.csv"), point_id=rec["point_id"])
        for d in ("2026-06-14", "2025-06-14")
    }
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
        positive_missed_share=pct(
            num(rec, "positive_within_gap") - num(rec, "positive_linked"),
            num(rec, "positive_within_gap"),
        ),
    )

    # Controls in the limit, by gap length: how thin the hard cases get.
    controls = where(
        read_csv(data, "final_grid/link_controls_pooled.csv"),
        point_id=rec["point_id"],
        kind="negative",
        counts_toward_limit="True",
    )
    same_rows = where(controls, same_callsign="True")
    smallest = min(same_rows, key=lambda r: num(r, "n"))
    c.update(
        smallest_same_n=n(num(smallest, "n")),
        smallest_same_upper=pct(num(smallest, "wrong_upper_95"), 1, 0),
        smallest_same_band=smallest["gap_stratum"],
        smallest_same_depth=n(num(smallest, "depth_min")),
        smallest_same_family=FAMILY_NAMES[smallest["control_family"]],
    )
    if sum(num(r, "n") for r in controls) != limit_n:
        raise MissingValue("the controls table does not add up to the limit's count")

    # Of those within the longest hole allowed, as the direction-aware count later is.
    c["low_edge_tests"] = n(num(rec, "negative_low_edge_untested_controls"))

    # --- the settings table ---
    c["table_settings"] = table(
        [
            "Speed floor",
            "Longest hole",
            "Crossing flights in one track",
            "All flights in one track",
            "Real false joins, of checkable",
            "Hidden landings joined",
            "Cut flights not rejoined",
            "Within limits",
        ],
        [
            [f"{n(sp)} kt", f"{n(g)} min", *settings_row("", setting(grid, sp, g))[1:]]
            for sp, g in ((400, 600), (400, 480), (400, 360), (315, 600), (315, 480), (315, 360))
        ],
    )
    s315_600, s315_480 = setting(grid, 315, 600), setting(grid, 315, 480)
    s_ratio = setting(grid, 400, 600, ratio=0.85)
    s315_ratio = setting(grid, 315, 600, ratio=0.85)
    if s315_480["within_limits"] != "False" or s315_ratio["within_limits"] != "True":
        raise MissingValue(
            "the nearest alternatives changed sides of the limits: the prose needs rewriting"
        )
    s_stop35, s_stop20 = setting(grid, 400, 600, stop=35), setting(grid, 400, 600, stop=20)
    c.update(
        s315_600_crossing=pct(num(s315_600, "crossing_flights_in_one_track"), cf),
        s315_600_false=n(num(s315_600, "linked_false_join")),
        s315_600_rate=pct(num(s315_600, "linked_false_join"), labelled(s315_600), 2),
        slow_speed_kt=n(315),
        s315_480_gap=n(480),
        s315_480_crossing_n=n(num(s315_480, "crossing_flights_in_one_track")),
        chosen_crossing_n=n(num(rec, "crossing_flights_in_one_track")),
        crossing_tie_gap=n(
            num(rec, "crossing_flights_in_one_track")
            - num(s315_480, "crossing_flights_in_one_track")
        ),
        s315_480_false=n(num(s315_480, "linked_false_join")),
        s315_480_labelled=n(labelled(s315_480)),
        s315_480_rate=pct(num(s315_480, "linked_false_join"), labelled(s315_480), 3),
        s315_600_labelled=n(labelled(s315_600)),
        ratio315_crossing_n=n(num(s315_ratio, "crossing_flights_in_one_track")),
        ratio315_false=n(num(s315_ratio, "linked_false_join")),
        ratio315_hidden=n(num(s315_ratio, "negative_limit_false_joins")),
        ratio315_missed=pct(
            num(s315_ratio, "positive_within_gap") - num(s315_ratio, "positive_linked"),
            num(s315_ratio, "positive_within_gap"),
        ),
        s315_480_hidden=n(num(s315_480, "negative_limit_false_joins")),
        ratio_value="0.85",
        ratio_crossing=pct(num(s_ratio, "crossing_flights_in_one_track"), cf),
        ratio_false=n(num(s_ratio, "linked_false_join")),
        stop35_min=n(35),
        stop20_min=n(20),
        stop35_crossing=pct(num(s_stop35, "crossing_flights_in_one_track"), cf),
        stop20_crossing=pct(num(s_stop20, "crossing_flights_in_one_track"), cf),
        stop35_false=n(num(s_stop35, "linked_false_join")),
        stop35_hidden=n(num(s_stop35, "negative_limit_false_joins")),
        hidden_at_480=n(num(setting(grid, 400, 480), "negative_limit_false_joins")),
        hidden_at_360=n(num(setting(grid, 400, 360), "negative_limit_false_joins")),
    )

    # --- what keeps holes split ---
    reasons = read_csv(data, "final_grid/link_reasons_pooled.csv")

    def reason_total(population, test, column):
        return sum(
            num(r, column) for r in where(reasons, population=population, first_failed_test=test)
        )

    def split_total(population):
        return sum(
            num(r, "holes")
            for r in where(reasons, population=population)
            if r["first_failed_test"] != "linked"
        )

    tests = sorted(
        {r["first_failed_test"] for r in reasons if r["first_failed_test"] != "linked"},
        key=lambda t: -(
            reason_total("crosses_box", t, "holes") + reason_total("other", t, "holes")
        ),
    )
    unknown_tests = set(tests) - set(TEST_NAMES)
    if unknown_tests:
        raise MissingValue(f"no plain name for tests {sorted(unknown_tests)}")
    rows = [
        [
            TEST_NAMES[t],
            n(reason_total("crosses_box", t, "holes")),
            n(reason_total("crosses_box", t, "only_failed_test_holes")),
            n(reason_total("other", t, "holes")),
            n(reason_total("other", t, "only_failed_test_holes")),
        ]
        for t in tests
    ]
    rows.append(
        [
            "**All holes left split**",
            f"**{n(split_total('crosses_box'))}**",
            "",
            f"**{n(split_total('other'))}**",
            "",
        ]
    )
    c["table_reasons"] = table(
        [
            "First test failed",
            "Crossing flights: holes",
            "of which failed nothing else",
            "Other flights: holes",
            "of which failed nothing else",
        ],
        rows,
    )
    c.update(
        crossing_holes_linked=n(reason_total("crosses_box", "linked", "holes")),
        crossing_holes_split=n(split_total("crosses_box")),
        old_edge_holes=n(reason_total("crosses_box", "cruise_at_old_edge", "holes")),
        old_edge_share=pct(
            reason_total("crosses_box", "cruise_at_old_edge", "holes"), split_total("crosses_box")
        ),
        speed_holes=n(reason_total("crosses_box", "implied_speed_not_too_slow", "holes")),
        speed_share=pct(
            reason_total("crosses_box", "implied_speed_not_too_slow", "holes"),
            split_total("crosses_box"),
        ),
        speed_only=n(
            reason_total("crosses_box", "implied_speed_not_too_slow", "only_failed_test_holes")
        ),
        new_edge_holes=n(reason_total("crosses_box", "cruise_at_new_edge", "holes")),
        new_edge_only=n(
            reason_total("crosses_box", "cruise_at_new_edge", "only_failed_test_holes")
        ),
        new_edge_share=pct(
            reason_total("crosses_box", "cruise_at_new_edge", "holes"), split_total("crosses_box")
        ),
        too_long_holes=n(reason_total("crosses_box", "gap_not_too_long", "holes")),
    )

    # --- artificial gaps missed ---
    positive = read_csv(data, "final_grid/link_positive_reasons_pooled.csv")
    pos_rows = where(positive, control_family="cruise_cruise")

    def by_gap(req):
        return [r for r in pos_rows if same(r["requested_gap_min"], req)]

    pos_controls = where(
        read_csv(data, "final_grid/link_controls_pooled.csv"),
        point_id=rec["point_id"],
        kind="positive",
        control_family="cruise_cruise",
    )
    miss = {}
    for req in (60, 120, 180, 360):
        tested = sum(num(r, "n") for r in pos_controls if same(r["requested_gap_min"], req))
        missed = sum(num(r, "positive_missed_by_test") for r in by_gap(req))
        miss[req] = pct(missed, tested)
    c.update(
        miss_60=miss[60],
        miss_120=miss[120],
        miss_180=miss[180],
        miss_360=miss[360],
        gap_60=n(60),
        gap_120=n(120),
        gap_180=n(180),
        gap_360=n(360),
    )
    slow_missed = sum(
        num(r, "positive_missed_by_test")
        for r in pos_rows
        if r["first_failed_test"] == "implied_speed_not_too_slow"
    )
    c.update(
        positive_too_slow=n(slow_missed),
        positive_too_slow_share=pct(
            slow_missed, num(rec, "positive_within_gap") - num(rec, "positive_linked")
        ),
        s315_missed=pct(
            num(s315_600, "positive_within_gap") - num(s315_600, "positive_linked"),
            num(s315_600, "positive_within_gap"),
        ),
        real_linked_drop=pct(
            num(s315_600, "real_holes_linked") - linked, num(s315_600, "real_holes_linked")
        ),
        tests_linked_drop=pct(
            num(s315_600, "positive_linked") - num(rec, "positive_linked"),
            num(s315_600, "positive_linked"),
        ),
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
    # Those three predate reading altitude past a blank. Holding the earlier 18,000 ft run
    # to the first edge row's flight count keeps them comparable on the population at least.
    if num(r180, "crossing_flights") != num(edges["off"], "crossing_flights"):
        raise MissingValue("the earlier 18,000 ft run covers different flights from the final one")
    c.update(
        **blank_edge_values(rec),
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
        early_crossing=pct(num(r180, "crossing_flights_in_one_track"), cf),
        early_linked=n(num(r180, "real_holes_linked")),
        early_false=n(num(r180, "linked_false_join")),
        early_labelled=n(labelled(r180)),
        low_floor_crossing=pct(num(r100, "crossing_flights_in_one_track"), cf),
        low_floor_false=n(num(r100, "linked_false_join")),
        low_floor_labelled=n(labelled(r100)),
        low_floor_rate=pct(num(r100, "linked_false_join"), labelled(r100), 2),
        low_floor_ratio_crossing=pct(num(r100_ratio, "crossing_flights_in_one_track"), cf),
        low_floor_ratio_false=n(num(r100_ratio, "linked_false_join")),
        low_floor_ratio_labelled=n(labelled(r100_ratio)),
        low_floor_ratio_rate=pct(num(r100_ratio, "linked_false_join"), labelled(r100_ratio), 2),
    )

    # --- the two edge settings ---
    off, climb, descent, both_on = edges["off"], edges["climb"], edges["descent"], edges["both"]
    if num(climb, "negative_low_edge_untested_controls") != num(
        rec, "negative_low_edge_untested_controls"
    ):
        raise MissingValue("final_edges and final_grid differ on the low-edge tests")

    def points(row):
        return f"{100.0 * (num(row, 'crossing_flights_in_one_track') - num(off, 'crossing_flights_in_one_track')) / cf:.1f}"

    climb_lab, descent_lab = num(climb, "linked_by_climb_labelled"), num(
        descent, "linked_by_descent_labelled"
    )
    climb_tests = num(climb, "negative_climb_cruise_controls")
    descent_tests = num(descent, "negative_cruise_descent_controls")
    if (
        num(climb, "false_joined_by_climb") != 0
        or num(descent, "negative_cruise_descent_false_joins") != 0
    ):
        raise MissingValue(
            "the false joins no longer all fall on one side: the prose needs rewriting"
        )
    c.update(
        low_labelled=n(climb_lab + descent_lab),
        low_hidden_tests=n(climb_tests + descent_tests),
        chance_real=f"{chance_all_in_one_drawn(num(descent, 'false_joined_by_descent'), descent_lab, climb_lab + descent_lab):.3f}",
        chance_hidden=f"{chance_all_in_one_repeated(num(climb, 'negative_climb_cruise_false_joins'), climb_tests, climb_tests + descent_tests):.3f}",
        off_crossing=pct(num(off, "crossing_flights_in_one_track"), cf),
        off_linked=n(num(off, "real_holes_linked")),
        off_false=n(num(off, "linked_false_join")),
        off_labelled=n(labelled(off)),
        climb_points=points(climb),
        climb_linked=n(num(climb, "linked_by_climb")),
        climb_false=n(num(climb, "false_joined_by_climb")),
        climb_labelled=n(climb_lab),
        climb_unknown_share=pct(
            num(climb, "linked_by_climb") - climb_lab, num(climb, "linked_by_climb")
        ),
        climb_upper=pct(upper_95(num(climb, "false_joined_by_climb"), climb_lab), 1),
        climb_hidden=n(num(climb, "negative_climb_cruise_false_joins")),
        climb_hidden_tests=n(num(climb, "negative_climb_cruise_controls")),
        climb_same_joined=n(num(climb, "same_callsign_low_edge_false_joins")),
        climb_same_tests=n(num(climb, "same_callsign_low_edge_controls")),
        hidden_pairs=n(num(climb, "negative_limit_pairs_with_false_join")),
        cruise_hidden=n(num(off, "negative_limit_false_joins")),
        cruise_hidden_tests=n(num(off, "negative_limit_controls")),
        descent_crossing=pct(num(descent, "crossing_flights_in_one_track"), cf),
        descent_points=points(descent),
        descent_false=n(num(descent, "false_joined_by_descent")),
        descent_labelled=n(descent_lab),
        descent_rate=pct(num(descent, "false_joined_by_descent"), descent_lab),
        descent_one_rate=pct(1, descent_lab, 2),
        descent_near_edge=n(
            num(descent, "false_joined_near_edge") - num(off, "false_joined_near_edge")
        ),
        descent_hidden=n(num(descent, "negative_cruise_descent_false_joins")),
        descent_hidden_tests=n(num(descent, "negative_cruise_descent_controls")),
        both_crossing=pct(num(both_on, "crossing_flights_in_one_track"), cf),
        both_extra_flights=n(
            num(both_on, "crossing_flights_in_one_track")
            - num(climb, "crossing_flights_in_one_track")
        ),
        both_false=n(num(both_on, "linked_false_join")),
        both_labelled=n(labelled(both_on)),
        both_rate=pct(num(both_on, "linked_false_join"), labelled(both_on), 3),
        low_cut_tests=n(num(climb, "positive_low_edge_within_gap")),
        low_cut_chosen=n(num(climb, "positive_low_edge_linked")),
        low_cut_both=n(num(both_on, "positive_low_edge_linked")),
        alt_old=n(num(rec, "linked_altitude_read_past_blank_old")),
        alt_old_share=pct(num(rec, "linked_altitude_read_past_blank_old"), linked),
        alt_old_false=n(num(rec, "false_joined_altitude_read_past_blank_old")),
        alt_new=n(num(rec, "linked_altitude_read_past_blank_new")),
        alt_new_share=pct(num(rec, "linked_altitude_read_past_blank_new"), linked),
        alt_new_false=n(num(rec, "false_joined_altitude_read_past_blank_new")),
    )

    def edge_setting_row(label, row, only_false, only_labelled):
        only = (
            f"{n(num(row, only_false))} of {n(num(row, only_labelled))}" if only_false else "none"
        )
        return [
            label,
            pct(num(row, "crossing_flights_in_one_track"), cf),
            pct(num(row, "flights_in_one_track"), fl),
            only,
            f"{n(num(row, 'linked_false_join'))} of {n(labelled(row))} "
            f"({pct(num(row, 'linked_false_join'), labelled(row), 3)})",
            f"{n(num(row, 'negative_limit_false_joins'))} of {n(num(row, 'negative_limit_controls'))}",
            (
                "yes, provisional"
                if row["within_limits"] == "True" and row["provisional"] == "True"
                else "yes" if row["within_limits"] == "True" else "no"
            ),
        ]

    c["table_edges"] = table(
        [
            "Edges below the floor allowed",
            "Crossing flights in one track",
            "All flights in one track",
            "Real false joins among holes linked only that way, of checkable",
            "Real false joins overall, of checkable",
            "Hidden landings joined",
            "Within limits",
        ],
        [
            edge_setting_row("Neither", off, None, None),
            edge_setting_row(
                "**Old edge climbing (chosen)**",
                climb,
                "false_joined_by_climb",
                "linked_by_climb_labelled",
            ),
            edge_setting_row(
                "New edge descending",
                descent,
                "false_joined_by_descent",
                "linked_by_descent_labelled",
            ),
            edge_setting_row(
                "Both", both_on, "false_joined_by_direction", "linked_by_direction_labelled"
            ),
        ],
    )

    # Hidden landings by family: each is counted at the setting that first allows it.
    edge_controls = read_csv(data, "final_edges/link_controls_pooled.csv")

    def family_row(label, row, family, total_column, joined_column, status):
        cuts = where(
            edge_controls,
            point_id=row["point_id"],
            kind="negative",
            control_family=family,
            counts_toward_limit="True",
        )
        tests = sum(num(r, "n") for r in cuts)
        joined = sum(num(r, "wrong") for r in cuts)
        hard = [r for r in cuts if r["same_callsign"] == "True"]
        hard_tests = sum(num(r, "n") for r in hard)
        hard_joined = sum(num(r, "wrong") for r in hard)
        if tests != num(row, total_column) or joined != num(row, joined_column):
            raise MissingValue(f"the {family} tests do not add up to the grid's count")
        return [
            label,
            n(tests),
            n(joined),
            pct(upper_95(joined, tests), 1, 3),
            n(hard_tests),
            n(hard_joined),
            pct(upper_95(hard_joined, hard_tests), 1, 2),
            status,
        ]

    c["table_hidden_families"] = table(
        [
            "Edges either side of the cut",
            "Tests",
            "Joined",
            "At most, 95%",
            "Same or blank callsign: tests",
            "Joined",
            "At most, 95%",
            "In the chosen rule",
        ],
        [
            family_row(
                "Cruise, then cruise",
                off,
                "cruise_cruise",
                "negative_limit_controls",
                "negative_limit_false_joins",
                "yes",
            ),
            family_row(
                "Climb, then cruise",
                climb,
                "climb_cruise",
                "negative_climb_cruise_controls",
                "negative_climb_cruise_false_joins",
                "yes",
            ),
            family_row(
                "Cruise, then descent",
                descent,
                "cruise_descent",
                "negative_cruise_descent_controls",
                "negative_cruise_descent_false_joins",
                "no",
            ),
            family_row(
                "Climb, then descent",
                both_on,
                "climb_descent",
                "negative_climb_descent_controls",
                "negative_climb_descent_false_joins",
                "no",
            ),
        ],
    )

    # --- the real pipeline day ---
    both = read_csv(data, "stage2/link_both_ends.csv")
    names = (
        ("crosses_the_box", "Crossing the European box"),
        ("europe_to_outside", "of which Europe to outside"),
        ("outside_to_europe", "of which outside to Europe"),
        ("intra_europe", "Inside Europe"),
    )

    def both_row(flow, label):
        r = one(both, flow=flow)
        return [
            label,
            n(num(r, "n")),
            f"{num(r, 'debounced_rows_per_flight'):.2f}",
            f"{num(r, 'cruise_linked_rows_per_flight'):.2f}",
            n(num(r, "debounced_one_row")),
            n(num(r, "cruise_linked_one_row")),
            n(num(r, "debounced_both_right")),
            n(num(r, "cruise_linked_both_right")),
        ]

    c["table_flight_list"] = table(
        [
            "Flights",
            "Number",
            "Rows per flight, current",
            "Rows per flight, new",
            "On one row, current",
            "On one row, new",
            "Both airports right on one row, current",
            "Both airports right on one row, new",
        ],
        [both_row(flow, label) for flow, label in names],
    )
    cross = one(both, flow="crosses_the_box")
    intra = one(both, flow="intra_europe")
    pairs = read_csv(data, "stage2/link_flight_list.csv")
    callsign = where(pairs, view="callsign")
    far_none = sum(
        num(r, "debounced_none")
        for r in where(callsign, end="far")
        if r["flow"] in ("europe_to_outside", "outside_to_europe")
    )
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
        intra_ades_right_to_wrong=n(
            num(one(callsign, flow="intra_europe", side="ades"), "right_to_wrong")
        ),
        intra_ades_wrong_to_right=n(
            num(one(callsign, flow="intra_europe", side="ades"), "wrong_to_right")
        ),
        cross_ades_right_to_wrong=n(
            sum(
                num(r, "right_to_wrong")
                for r in where(callsign, side="ades")
                if r["flow"] in ("europe_to_outside", "outside_to_europe")
            )
        ),
        cross_ades_wrong_to_right=n(
            sum(
                num(r, "wrong_to_right")
                for r in where(callsign, side="ades")
                if r["flow"] in ("europe_to_outside", "outside_to_europe")
            )
        ),
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
        [
            [
                f"`{r['type']}`",
                n(num(r, "debounced_events")),
                n(num(r, "cruise_linked_events")),
                f"{int(num(r, 'change')):+d}",
            ]
            for r in sorted(changed, key=lambda r: num(r, "change"))
        ],
    )

    def event_change(kind):
        return f"{int(num(one(events, type=kind), 'change')):+d}"

    c.update(
        ev_aobt=event_change("AOBT"),
        ev_aibt=event_change("AIBT"),
        ev_fl245=event_change("xing-fl245"),
        ev_fl100=event_change("xing-fl100"),
        ev_fl50=event_change("xing-fl50"),
        ev_entry_taxiway=event_change("entry-taxiway"),
        ev_exit_taxiway=event_change("exit-taxiway"),
        ev_entry_apron=event_change("entry-apron"),
        ev_exit_apron=event_change("exit-apron"),
        ev_entry_parking=event_change("entry-parking_position"),
        ev_exit_parking=event_change("exit-parking_position"),
        ev_vacated=event_change("runway-vacated"),
        ev_level_off_start=event_change("level-off-descent-start"),
        ev_level_off_end=event_change("level-off-descent-end"),
    )

    truth = read_csv(data, "stage2/link_truth_all_flights.csv")
    label_names = (
        ("true_link", "True link"),
        ("false_join", "False join"),
        ("unknown", "Unknown"),
        ("airframe_not_in_truth", "Airframe not in Network Manager"),
    )
    c["table_truth"] = table(
        [
            "Label",
            "Holes",
            "With a landing or on-block event on the older piece",
            "With a take-off or off-block event on the newer piece",
            "With both",
        ],
        [
            [
                label,
                *(
                    n(num(one(truth, label=key), col))
                    for col in ("holes", "landing_on_older", "take_off_on_newer", "both")
                ),
            ]
            for key, label in label_names
        ],
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
        level_share=pct(
            num(level, "tracks_with_a_segment_across_a_hole"), num(level, "scored_tracks")
        ),
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
    for job in ("final_grid", "final_edges", "stage2"):
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


def literal_numbers(template_text, allowed=None):
    """Numbers typed into the prose rather than coming from a placeholder or the allow-list."""
    allowed = ALLOWED_LITERALS if allowed is None else allowed
    text = re.sub(r"\{\{.*?\}\}", " ", template_text)
    text = re.sub(r"```.*?```", " ", text, flags=re.S)
    text = re.sub(r"`[^`]*`", " ", text)
    text = re.sub(r"\]\([^)]*\)", "]", text)
    found = re.findall(r"(?<![\w.])\d+(?:,\d{3})*(?:\.\d+)?", text)
    return sorted({t for t in found if t not in allowed})


# --- a small Markdown converter: headings, paragraphs, lists, tables, code, bold, links ---


def inline(text):
    held = []

    def hold(markup):
        held.append(markup)
        return f"\0{len(held) - 1}\0"

    text = re.sub(r"`([^`]+)`", lambda m: hold(f"<code>{html.escape(m.group(1))}</code>"), text)
    text = html.escape(text)
    text = re.sub(
        r"\[([^\]]+)\]\(([^)\s]+)\)", lambda m: f'<a href="{m.group(2)}">{m.group(1)}</a>', text
    )
    text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
    return re.sub(r"\0(\d+)\0", lambda m: held[int(m.group(1))], text)


def _cells(line):
    return [c.strip() for c in line.strip().strip("|").split("|")]


def blocks(md):
    """Markdown as ``(kind, data)``: heading (level, text), table (rows, header first),
    list (ordered?, items), code (text) or para (text)."""
    lines, i = md.split("\n"), 0
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
            yield "code", "\n".join(code)
        elif re.match(r"#{1,6} ", line):
            level = len(line) - len(line.lstrip("#"))
            yield "heading", (level, line[level:].strip())
            i += 1
        elif line.startswith("|"):
            block = []
            while i < len(lines) and lines[i].startswith("|"):
                block.append(lines[i])
                i += 1
            yield "table", [_cells(r) for r in block[:1] + block[2:]]
        elif re.match(r"(- |\d+\. )", line):
            ordered = not line.startswith("- ")
            items = []
            while i < len(lines) and (
                re.match(r"(- |\d+\. )", lines[i]) or (items and lines[i].startswith("  "))
            ):
                if lines[i].startswith("  "):
                    items[-1] += " " + lines[i].strip()
                else:
                    items.append(re.sub(r"^(- |\d+\. )", "", lines[i]))
                i += 1
            yield "list", (ordered, items)
        else:
            para = []
            while (
                i < len(lines)
                and lines[i].strip()
                and not re.match(r"(#{1,6} |\||```|- |\d+\. )", lines[i])
            ):
                para.append(lines[i].strip())
                i += 1
            if not para:  # a line no branch above took: never loop on it
                para.append(lines[i].strip())
                i += 1
            yield "para", " ".join(para)


def markdown_to_html(md):
    out = []
    for kind, d in blocks(md):
        if kind == "code":
            out.append(f"<pre><code>{html.escape(d)}</code></pre>")
        elif kind == "heading":
            out.append(f"<h{d[0]}>{inline(d[1])}</h{d[0]}>")
        elif kind == "table":
            head = "".join(f"<th>{inline(c)}</th>" for c in d[0])
            body = "".join(
                "<tr>" + "".join(f"<td>{inline(c)}</td>" for c in r) + "</tr>" for r in d[1:]
            )
            out.append(
                f'<div class="table-wrap"><table><thead><tr>{head}</tr></thead>'
                f"<tbody>{body}</tbody></table></div>"
            )
        elif kind == "list":
            tag = "ol" if d[0] else "ul"
            out.append(f"<{tag}>" + "".join(f"<li>{inline(x)}</li>" for x in d[1]) + f"</{tag}>")
        else:
            out.append(f"<p>{inline(d)}</p>")
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


class Paper:
    """What differs between the reports this module can build.

    ``build_track_v5_paper.py`` builds its report with the functions below and its own
    ``Paper``; everything else -- reading CSVs, filling placeholders, HTML, PDF, the
    Quarto front matter -- is shared.
    """

    def __init__(
        self,
        version,
        title,
        subtitle,
        allowed_literals,
        context,
        render_default="run",
        builder=None,
        pdf_name=None,
        regenerate=None,
    ):
        self.version = version
        self.title = title
        self.subtitle = subtitle
        self.allowed_literals = allowed_literals
        self.context = context
        #: What ``OPDI_RENDER`` means when unset. A superseded paper is frozen: ``skip``.
        self.render_default = render_default
        #: The three file names a paper outside the track-construction series
        #: passes explicitly (``build_airspace_v1_paper.py`` does).
        self.builder = builder or f"build_track_{version}_paper.py"
        self.pdf_name = pdf_name or f"track-construction-{version}.pdf"
        self.regenerate = regenerate or f"regenerate_track_{version}.py"


def to_html(md, paper=None):
    paper = paper or V4
    return (
        '<!doctype html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f"<title>{html.escape(paper.title)}</title>\n<style>{CSS}</style>\n</head>\n<body>\n<main>\n"
        f'<h1>{html.escape(paper.title)}</h1>\n<p class="subtitle">{html.escape(paper.subtitle)}</p>\n'
        '<p class="note">EUROCONTROL Performance Review Unit. Built from the staged CSVs by '
        f"<code>benchmarks/{paper.builder}</code>; not a Quarto render.</p>\n"
        f"{markdown_to_html(md)}\n</main>\n</body>\n</html>\n"
    )


# --- PDF: the same blocks, laid out with reportlab (imported only for --pdf) ---

FONT_DIRS = ("/usr/share/fonts/truetype/dejavu", "/usr/share/fonts/dejavu", "/usr/share/fonts/TTF")


def col_widths(rows, total, measure, minimum=28.0):
    """Column widths summing to ``total`` points. ``measure(text)`` is a string's width.

    A column wants its longest word (so nothing breaks mid-word) at least, and its
    longest cell at most; the spare width goes to columns in proportion to what they
    still want, and when even the longest words do not fit, widths shrink pro rata.
    """
    ncol = max(len(r) for r in rows)
    pad = [[(r[c] if c < len(r) else "") for r in rows] for c in range(ncol)]
    floor = [max([minimum] + [measure(w) for t in col for w in t.split()]) for col in pad]
    want = [max([f] + [measure(t) for t in col]) for f, col in zip(floor, pad)]
    if sum(floor) >= total:
        return [f * total / sum(floor) for f in floor]
    if sum(want) <= total:
        return [w + (total - sum(want)) / ncol for w in want]
    spare = (total - sum(floor)) / sum(w - f for w, f in zip(want, floor))
    return [f + (w - f) * spare for f, w in zip(floor, want)]


def to_pdf(md, out_path, paper=None):
    """Write the report as an A4 PDF; reportlab is imported here, not at module load."""
    paper = paper or V4
    try:
        from reportlab.lib import colors
        from reportlab.lib.enums import TA_LEFT
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle
        from reportlab.lib.units import mm
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        from reportlab.platypus import (
            KeepTogether,
            ListFlowable,
            ListItem,
            Paragraph,
            Preformatted,
            SimpleDocTemplate,
            Spacer,
            Table,
            TableStyle,
        )
    except ImportError as e:
        raise SystemExit(
            "--pdf needs reportlab, which the project does not depend on: install it into "
            f"a scratch environment (pip install reportlab) and run again ({e})"
        )
    fonts = {
        "Body": "DejaVuSans.ttf",
        "Body-Bold": "DejaVuSans-Bold.ttf",
        "Body-Italic": "DejaVuSans-Oblique.ttf",
        "Body-BoldItalic": "DejaVuSans-BoldOblique.ttf",
        "Mono": "DejaVuSansMono.ttf",
        "Mono-Bold": "DejaVuSansMono-Bold.ttf",
    }
    for name, fname in fonts.items():
        path = next((Path(d) / fname for d in FONT_DIRS if (Path(d) / fname).is_file()), None)
        if path is None:
            raise SystemExit(f"--pdf needs the DejaVu font {fname} (looked in {FONT_DIRS})")
        pdfmetrics.registerFont(TTFont(name, str(path)))
    pdfmetrics.registerFontFamily(
        "Body", normal="Body", bold="Body-Bold", italic="Body-Italic", boldItalic="Body-BoldItalic"
    )
    pdfmetrics.registerFontFamily(
        "Mono", normal="Mono", bold="Mono-Bold", italic="Mono", boldItalic="Mono-Bold"
    )

    ink, muted, accent, band = (
        colors.HexColor(h) for h in ("#1f2328", "#57606a", "#0a3d7a", "#f0f3f6")
    )
    S = lambda name, **kw: ParagraphStyle(
        name, fontName=kw.pop("fontName", "Body"), textColor=kw.pop("textColor", ink), **kw
    )
    body = S("body", fontSize=9.5, leading=13.5, spaceAfter=6, alignment=TA_LEFT)
    cell = S("cell", fontSize=8, leading=10.5)
    head_cell = S("hcell", fontSize=8, leading=10.5, fontName="Body-Bold", textColor=colors.white)
    heads = {
        1: S(
            "h1",
            fontSize=15,
            leading=19,
            fontName="Body-Bold",
            textColor=accent,
            spaceBefore=16,
            spaceAfter=6,
        ),
        2: S(
            "h2",
            fontSize=12,
            leading=16,
            fontName="Body-Bold",
            textColor=accent,
            spaceBefore=12,
            spaceAfter=4,
        ),
        3: S("h3", fontSize=10.5, leading=14, fontName="Body-Bold", spaceBefore=9, spaceAfter=3),
    }
    code_style = S(
        "code",
        fontName="Mono",
        fontSize=7.5,
        leading=10,
        backColor=band,
        borderPadding=5,
        spaceBefore=4,
        spaceAfter=10,
    )

    def markup(text):  # the HTML inline() writes, in reportlab's dialect
        text = inline(text)
        text = re.sub(r"<a href=[^>]*>(.*?)</a>", r"\1", text)
        text = text.replace("<strong>", "<b>").replace("</strong>", "</b>")
        return text.replace("<code>", '<font name="Mono" size="8.2">').replace("</code>", "</font>")

    width = A4[0] - 2 * 20 * mm

    def footer(canvas, doc):
        canvas.saveState()
        canvas.setFont("Body", 7.5)
        canvas.setFillColor(muted)
        canvas.drawString(20 * mm, 11 * mm, f"EUROCONTROL PRU  \u00b7  {paper.title}")
        canvas.drawRightString(A4[0] - 20 * mm, 11 * mm, f"Page {doc.page}")
        canvas.setStrokeColor(colors.HexColor("#d0d7de"))
        canvas.line(20 * mm, 15 * mm, A4[0] - 20 * mm, 15 * mm)
        canvas.restoreState()

    story = [
        Paragraph(
            html.escape(paper.title),
            S(
                "title",
                fontSize=22,
                leading=27,
                fontName="Body-Bold",
                textColor=accent,
                spaceAfter=6,
            ),
        ),
        Paragraph(
            html.escape(paper.subtitle),
            S("sub", fontSize=11, leading=15, textColor=muted, spaceAfter=8),
        ),
        Paragraph(
            f"EUROCONTROL Performance Review Unit  \u00b7  {date.today():%d %B %Y}",
            S("meta", fontSize=9, leading=12, textColor=muted, spaceAfter=4),
        ),
        Table(
            [[""]],
            colWidths=[width],
            rowHeights=[2],
            style=[("LINEABOVE", (0, 0), (-1, 0), 1.2, accent)],
        ),
        Spacer(1, 6),
    ]
    for kind, d in blocks(md):
        if kind == "heading":
            story.append(Paragraph(markup(d[1]), heads[min(d[0], 3)]))
        elif kind == "para":
            story.append(Paragraph(markup(d), body))
        elif kind == "code":
            story.append(Preformatted(d, code_style))
        elif kind == "list":
            items = [ListItem(Paragraph(markup(x), body), leftIndent=14) for x in d[1]]
            story.append(
                ListFlowable(
                    items,
                    bulletType="1" if d[0] else "bullet",
                    start=1 if d[0] else "\u2022",
                    bulletFontName="Body",
                    bulletFontSize=9,
                    leftIndent=14,
                )
            )
        else:
            plain = [[re.sub(r"[`*]", "", c) for c in r] for r in d]
            widths = col_widths(
                plain, width - 8, lambda t: pdfmetrics.stringWidth(t, "Body-Bold", 8) + 9
            )
            data = [
                [Paragraph(markup(c), head_cell if r == 0 else cell) for c in row]
                for r, row in enumerate(d)
            ]
            story.append(
                Table(
                    data,
                    colWidths=widths,
                    repeatRows=1,
                    splitByRow=1,
                    hAlign="LEFT",
                    style=TableStyle(
                        [
                            ("BACKGROUND", (0, 0), (-1, 0), accent),
                            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, band]),
                            ("LINEBELOW", (0, 0), (-1, -1), 0.3, colors.HexColor("#d0d7de")),
                            ("VALIGN", (0, 0), (-1, -1), "TOP"),
                            ("LEFTPADDING", (0, 0), (-1, -1), 4),
                            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                            ("TOPPADDING", (0, 0), (-1, -1), 3),
                            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                        ]
                    ),
                )
            )
            story.append(Spacer(1, 8))
    SimpleDocTemplate(
        str(out_path),
        pagesize=A4,
        leftMargin=20 * mm,
        rightMargin=20 * mm,
        topMargin=18 * mm,
        bottomMargin=22 * mm,
        title=paper.title,
        author="EUROCONTROL Performance Review Unit",
    ).build(story, onFirstPage=footer, onLaterPages=footer)


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
#   run          regenerate anything stale, then render
#   check        fail fast naming every stale job, regenerate nothing
#   allow-stale  render a draft from whatever is staged, after naming staleness
#   skip         do not invoke the entrypoint at all
#
# Unset, it is "@DEFAULT@".
REPO   <- Sys.getenv("OPDI_REPO_DIR",
                     normalizePath(file.path(getwd(), "..", "..", "..", "opdi"),
                                   mustWork = FALSE))
PYTHON <- Sys.getenv("OPDI_PYTHON", file.path(REPO, ".venv310", "bin", "python"))
MODE   <- Sys.getenv("OPDI_RENDER", "@DEFAULT@")

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


def to_qmd(md, paper=None):
    paper = paper or V4
    chunk = (
        REGENERATE_CHUNK.replace("regenerate_track_v4.py", paper.regenerate)
        .replace("build_track_v4_paper.py", paper.builder)
        .replace("@DEFAULT@", paper.render_default)
    )
    return (
        f'---\ntitle: "{paper.title}"\nsubtitle: "{paper.subtitle}"\n'
        'author: "EUROCONTROL Performance Review Unit"\ndate: last-modified\n---\n\n'
        f"{chunk}\n{md}"
    )


def build(paper_dir, paper=None):
    """``(qmd, html, md)`` for the paper directory."""
    paper = paper or V4
    paper_dir = Path(paper_dir)
    template = paper_dir / "report_template.md"
    if not template.is_file():
        raise MissingValue(f"missing {template}")
    text = template.read_text()
    stray = literal_numbers(text, paper.allowed_literals)
    if stray:
        raise MissingValue(f"report_template.md states numbers with no source: {stray}")
    md = render(text, paper.context(paper_dir / "data"))
    return to_qmd(md, paper), to_html(md, paper), md


#: This module's own report. Superseded by version 5 and frozen: a render no longer
#: runs the analysis unless ``OPDI_RENDER`` asks for it, so the page keeps showing the
#: numbers version 4 published rather than what the current code would give.
V4 = Paper("v4", TITLE, SUBTITLE, ALLOWED_LITERALS, build_context, render_default="skip")


def main(argv=None, paper=None, paper_dir=PAPER, description=__doc__):
    paper = paper or V4
    ap = argparse.ArgumentParser(description=description)
    ap.add_argument("--paper-dir", default=str(paper_dir))
    ap.add_argument(
        "--check", action="store_true", help="fail if index.qmd is out of date; write nothing"
    )
    ap.add_argument(
        "--pdf", action="store_true", help=f"also write {paper.pdf_name} (needs reportlab)"
    )
    args = ap.parse_args(argv)
    paper_dir = Path(args.paper_dir)
    qmd, page, md = build(paper_dir, paper)
    if args.check:
        current = paper_dir / "index.qmd"
        if not current.is_file() or current.read_text() != qmd:
            raise SystemExit(f"{current} is not what the CSVs give: run {paper.builder}")
        print("index.qmd matches the CSVs")
        return 0
    (paper_dir / "index.qmd").write_text(qmd)
    (paper_dir / "index.html").write_text(page)
    if args.pdf:
        to_pdf(md, paper_dir / paper.pdf_name, paper)
        print(f"wrote {paper_dir / paper.pdf_name}")
    print(f"wrote {paper_dir / 'index.qmd'} and {paper_dir / 'index.html'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
