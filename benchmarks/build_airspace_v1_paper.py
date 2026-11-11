"""Fill the airspace-events-v1 report (holding, point merge, FIR) from its staged CSVs.

Same arrangement as ``build_track_v5_paper.py``: ``report_template.md`` holds the
prose with ``{{ name }}`` placeholders, every number comes from a CSV under
``data/`` or from a constant read out of the source, and a missing file, row,
column or placeholder stops the build. Writes ``index.qmd``, ``index.html`` and,
with ``--pdf``, ``airspace-events-v1.pdf``.

    python benchmarks/build_airspace_v1_paper.py [--paper-dir DIR] [--check] [--pdf]
"""

import csv
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

import build_track_v4_paper as v4  # noqa: E402
from build_track_v4_paper import MissingValue, Paper, n, num, one, read_csv, table  # noqa: E402
from build_track_v5_paper import class_constant  # noqa: E402

PAPER = Path(os.environ.get(
    "OPDI_PAPER_DIR_AIRSPACE", REPO.parent / "opdi-portal" / "papers" / "airspace-events-v1"))

TITLE = "Holding, point merge and FIR crossings as flight events"
SUBTITLE = "Three new event families: how each is detected, what it measures, and how well it does"

ALLOWED_LITERALS = {
    "2019", "01", "10",          # the label day
    "2026", "06", "14",          # the pipeline day
    "524",                       # the AIRAC cycle
    "11", "28", "12", "26", "2024",  # its validity
    "0.84", "0.70", "0.76",      # the neural detector's bar
    "61", "23",                  # the catalogue
    "6",                         # the label window, minutes
    "2",                         # the window step
    "1", "3", "4", "5", "7", "8", "9",  # section numbers and small counts stated in prose
    "60",                        # dwell seconds; candidate radius
    "360", "300", "330", "80", "15", "2,500", "140", "240", "120",  # holding thresholds
    "45", "90", "1.5", "600",    # point merge thresholds
    "0.4",                       # turning floor
    "195", "245",                # the FIR/UIR split levels
    "42", "13",                  # merge point to airport distances cited
    "100",                       # per cent
    "50",                        # half a window
    "00", "04",                  # pipeline steps 00c and 04
    "2023",                      # Melo (2023)
}

PIPELINE_DAY = "2026-06-14"
LABEL_DAY = "2019-10-01"


def _metric(rows, name):
    row = one(rows, metric=name)
    return row.get("value")


def _fmt(x, digits=2):
    if x in (None, ""):
        raise MissingValue("blank value")
    return f"{float(x):.{digits}f}"


def holding_context(data):
    score = read_csv(data, f"holding/holding_score_{LABEL_DAY}.csv")
    sweep = read_csv(data, f"holding/holding_sweep_{LABEL_DAY}.csv")
    episodes = read_csv(data, f"holding/holding_episodes_{LABEL_DAY}.csv")
    rows = {r["airport"]: r for r in score}
    ctx = {}
    for apt in ("EGLL", "EIDW", "ALL"):
        r = rows[apt]
        ctx[f"h_{apt.lower()}_p"] = _fmt(num(r, "precision"))
        ctx[f"h_{apt.lower()}_r"] = _fmt(num(r, "recall"))
        ctx[f"h_{apt.lower()}_f1"] = _fmt(num(r, "f1"))
        ctx[f"h_{apt.lower()}_iou"] = _fmt(num(r, "iou"))
        ctx[f"h_{apt.lower()}_flights"] = n(num(r, "n_flights"))
        ctx[f"h_{apt.lower()}_positive"] = n(num(r, "n_positive"))
        ctx[f"h_{apt.lower()}_windows"] = n(num(r, "n_windows"))
        ctx[f"h_{apt.lower()}_episodes"] = n(num(r, "n_episodes"))
        ctx[f"h_{apt.lower()}_p_any"] = _fmt(num(r, "precision_any_overlap"))
        ctx[f"h_{apt.lower()}_f1_any"] = _fmt(num(r, "f1_any_overlap"))
    best = {}
    for r in sweep:
        apt = r["airport"]
        if apt not in best or float(r["f1"]) > float(best[apt]["f1"]):
            best[apt] = r
    ctx["h_sweep_table"] = table(
        ["Airport", "Net turn (deg)", "Displacement (NM)", "Min duration (s)", "Precision", "Recall", "F1"],
        [[a, n(num(b, "holding_net_turn_min_deg")), n(num(b, "holding_max_displacement_nm")),
          n(num(b, "holding_min_duration_seconds")), _fmt(num(b, "precision")), _fmt(num(b, "recall")),
          _fmt(num(b, "f1"))] for a, b in sorted(best.items())])
    ctx["h_sweep_points"] = n(len({(r["holding_net_turn_min_deg"], r["holding_max_displacement_nm"],
                                    r["holding_min_duration_seconds"]) for r in sweep}))
    f1s = {a: float(b["f1"]) for a, b in best.items()}
    shipped = {a: float(rows[a]["f1"]) for a in ("EGLL", "EIDW")}
    ctx["h_sweep_gain_max"] = _fmt(max(f1s[a] - shipped[a] for a in shipped))
    durations = sorted(float(r["duration_s"]) for r in episodes)
    ctx["h_episode_median_min"] = _fmt(durations[len(durations) // 2] / 60.0, 1) if durations else "0"
    ctx["h_orbits"] = n(sum(1 for r in episodes if r["pattern"] == "orbit"))
    ctx["h_racetracks"] = n(sum(1 for r in episodes if r["pattern"] == "racetrack"))
    return ctx


def point_merge_context(data):
    usage = read_csv(data, f"point_merge/point_merge_usage_{PIPELINE_DAY}.csv")
    rows = [r for r in usage if float(r["arrivals"] or 0) > 0]
    rows.sort(key=lambda r: -float(r["arrivals"]))
    ctx = {
        "pm_airports_covered": n(len(usage)),
        "pm_airports_with_arrivals": n(len(rows)),
        "pm_arrivals": n(sum(float(r["arrivals"]) for r in rows)),
        "pm_arrivals_with_leg": n(sum(float(r["arrivals_with_leg"]) for r in rows)),
        "pm_legs": n(sum(float(r["legs"]) for r in rows)),
    }
    top = rows[:15]
    ctx["pm_usage_table"] = table(
        ["Airport", "Arrivals", "With a leg", "Share", "Median leg distance (NM)", "Median leg level (ft)",
         "Median leg time (s)", "Merge point, no leg"],
        [[r["apt_icao"], n(num(r, "arrivals")), n(num(r, "arrivals_with_leg")),
          (f"{float(r['leg_share_pct']):.1f}%" if r.get("leg_share_pct") not in (None, "") else "-"),
          (r["median_leg_distance_nm"] or "-"), (r["median_leg_level_ft"] or "-"),
          (r["median_leg_duration_s"] or "-"), n(num(r, "merge_point_without_leg"))] for r in top])
    eidw = one(usage, apt_icao="EIDW")
    ctx["pm_eidw_share"] = f"{float(eidw['leg_share_pct']):.1f}%" if eidw.get("leg_share_pct") else "0%"
    ctx["pm_eidw_arrivals"] = n(num(eidw, "arrivals"))
    ctx["pm_eidw_with_leg"] = n(num(eidw, "arrivals_with_leg"))
    for apt in ("ENGM", "EGLC", "LPPT", "LTFM"):
        r = one(usage, apt_icao=apt)
        ctx[f"pm_{apt.lower()}_share"] = f"{float(r['leg_share_pct']):.1f}%" if r.get("leg_share_pct") else "0%"
    check_path = Path(data) / "point_merge" / "eidw_visual_check.csv"
    if check_path.is_file():
        with check_path.open() as fh:
            check = list(csv.DictReader(fh))
        det = [r for r in check if r["detected"].lower() in ("true", "1", "yes")]
        und = [r for r in check if r["detected"].lower() not in ("true", "1", "yes")]
        ctx["pm_check_detected"] = n(len(det))
        ctx["pm_check_detected_real"] = n(sum(1 for r in det if r["judged_real_leg"].lower() == "yes"))
        ctx["pm_check_undetected"] = n(len(und))
        ctx["pm_check_undetected_missed"] = n(sum(1 for r in und if r["judged_real_leg"].lower() == "yes"))
    else:
        raise MissingValue(f"missing {check_path}: the visual check has not been recorded")
    return ctx


def fir_context(data):
    cons = read_csv(data, f"fir/fir_consistency_{PIPELINE_DAY}.csv")
    cross = read_csv(data, f"fir/fir_crossings_{PIPELINE_DAY}.csv")
    ctx = {
        "fir_samples": n(_metric(cons, "samples")),
        "fir_sample_tracks": n(_metric(cons, "tracks")),
        "fir_agree_pct": f"{float(_metric(cons, 'agree_pct')):.2f}%",
        "fir_agree_both_pct": f"{float(_metric(cons, 'agree_where_both_pct')):.2f}%",
        "fir_h3_null_pct": f"{float(_metric(cons, 'h3_null_pct')):.2f}%",
        "fir_exact_null_pct": f"{float(_metric(cons, 'exact_null_pct')):.2f}%",
        "fir_h3_crossings_sample": n(_metric(cons, "h3_crossings")),
        "fir_exact_crossings_sample": n(_metric(cons, "exact_crossings")),
        "fir_matched_sample": n(_metric(cons, "matched_crossings")),
        "fir_timing_p50": _fmt(_metric(cons, "timing_error_p50_s"), 0),
        "fir_timing_p90": _fmt(_metric(cons, "timing_error_p90_s"), 0),
        "fir_timing_max": _fmt(_metric(cons, "timing_error_max_s"), 0),
        "fir_double_match_dropped": n(_metric(cons, "double_match_rows_dropped")),
        "fir_tracks_day": n(_metric(cross, "tracks_on_day")),
        "fir_tracks_crossing": n(_metric(cross, "tracks_with_a_crossing")),
        "fir_crossings_day": n(_metric(cross, "crossings")),
        "fir_per_track_p50": _fmt(_metric(cross, "crossings_per_crossing_track_p50"), 0),
        "fir_per_track_p90": _fmt(_metric(cross, "crossings_per_crossing_track_p90"), 0),
        "fir_per_track_max": n(_metric(cross, "crossings_per_crossing_track_max")),
        "fir_vertical": n(_metric(cross, "vertical_crossings_same_prefix")),
        "fir_bracket_p50": _fmt(_metric(cross, "bracket_s_p50"), 0),
        "fir_airac": str(_metric(cross, "airac_cfmu")),
    }
    pairs = [r for r in cross if r["metric"].startswith("pair ")]
    ctx["fir_pairs_table"] = table(["Boundary", "Crossings"],
                                   [[r["metric"][5:].replace("->", " to "), n(num(r, "value"))] for r in pairs[:10]])
    return ctx


def pipeline_context(data):
    timings = read_csv(data, "pipeline_day/timings.csv")
    counts = read_csv(data, "pipeline_day/counts.csv")
    off = sum(float(r["seconds"]) for r in timings if r["arm"] == "off" and r["step"] == "04")
    on = sum(float(r["seconds"]) for r in timings if r["arm"] == "on" and r["step"] == "04")
    if not off or not on:
        raise MissingValue("timings.csv lacks a step 04 row for one arm")
    ctx = {
        "pd_step04_off_min": _fmt(off / 60.0, 0),
        "pd_step04_on_min": _fmt(on / 60.0, 0),
        "pd_step04_extra_pct": f"{100.0 * (on - off) / off:.0f}%",
    }
    by = {(r["kind"], r["name"]): float(r["count"]) for r in counts}
    ctx["pd_counts_table"] = table(
        ["Event or measurement", "Rows on the day"],
        [[name, n(c)] for (kind, name), c in sorted(by.items()) if kind in ("event", "measurement")])
    ctx["pd_holding_pairs"] = n(by.get(("event", "holding-start"), 0))
    ctx["pd_leg_pairs"] = n(by.get(("event", "pms-leg-entry"), 0))
    ctx["pd_merge_points"] = n(by.get(("event", "pms-merge-point"), 0))
    ctx["pd_fir"] = n(by.get(("event", "xing-fir"), 0))
    ctx["pd_tracks"] = n(by.get(("input", "tracks"), 0))
    return ctx


def shipped_context():
    cfg = "src/opdi/config.py"
    flags = {f: bool(class_constant("EventConfig", f, cfg))
             for f in ("emit_holding", "emit_point_merge", "emit_fir_crossings")}
    on = [k for k, v in flags.items() if v]
    off = [k for k, v in flags.items() if not v]
    names = {"emit_holding": "holding", "emit_point_merge": "point merge", "emit_fir_crossings": "FIR crossings"}
    return {
        "shipped_on": ", ".join(names[k] for k in on) or "none",
        "shipped_off": ", ".join(names[k] for k in off) or "none",
        "holding_default": "on" if flags["emit_holding"] else "off",
        "pms_default": "on" if flags["emit_point_merge"] else "off",
        "fir_default": "on" if flags["emit_fir_crossings"] else "off",
        "holding_window_s": n(class_constant("EventConfig", "holding_window_seconds", cfg)),
        "holding_net_turn": n(class_constant("EventConfig", "holding_net_turn_min_deg", cfg)),
        "holding_disp_nm": n(class_constant("EventConfig", "holding_max_displacement_nm", cfg)),
        "holding_min_s": n(class_constant("EventConfig", "holding_min_duration_seconds", cfg)),
        "pms_band_nm": str(class_constant("EventConfig", "pms_leg_distance_band_nm", cfg)),
        "pms_min_leg_s": n(class_constant("EventConfig", "pms_leg_min_duration_seconds", cfg)),
        "pms_align_deg": n(class_constant("EventConfig", "pms_direct_align_deg", cfg)),
        "fir_dwell_s": n(class_constant("EventConfig", "fir_min_dwell_seconds", cfg)),
        "pms_sites": n(_count_pms_sites()),
        "pms_sites_placed": n(_count_pms_sites(placed=True)),
    }


def _count_pms_sites(placed=False):
    path = REPO / "src" / "opdi" / "reference" / "data" / "point_merge_systems.csv"
    with path.open() as fh:
        rows = list(csv.DictReader(fh))
    if placed:
        rows = [r for r in rows if r["confidence"] in ("high", "medium")]
    return len({r["apt_icao"] for r in rows})


def build_context(data):
    ctx = {}
    ctx.update(shipped_context())
    ctx.update(holding_context(data))
    ctx.update(point_merge_context(data))
    ctx.update(fir_context(data))
    ctx.update(pipeline_context(data))
    return ctx


V1 = Paper("airspace-v1", TITLE, SUBTITLE, ALLOWED_LITERALS, build_context, render_default="run",
           builder="build_airspace_v1_paper.py", pdf_name="airspace-events-v1.pdf",
           regenerate="regenerate_airspace_v1.py")


def main(argv=None):
    return v4.main(argv, paper=V1, paper_dir=PAPER, description=__doc__)


if __name__ == "__main__":
    sys.exit(main())
