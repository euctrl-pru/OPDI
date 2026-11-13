"""The ADS-C study script: where it writes, and the arithmetic of its comparison.

No Spark: every function here works on small pandas frames or on strings. The
Spark reductions are tested in ``test_adsc_effect_spark.py``.
"""

import re
from datetime import date
from pathlib import Path

import adsc_effect as ae
import numpy as np
import pandas as pd
import pytest

T0 = pd.Timestamp("2026-06-14 08:00:00")


def minutes(m):
    return pd.Timedelta(minutes=m)


# --- where the script reads and writes --------------------------------------


def test_every_table_the_pipeline_names_is_classified():
    # A table in none of the sets would resolve nowhere and stop the run part
    # way; a table in the wrong set would be read from or written to the wrong
    # place without any error.
    src = Path(ae.REPO) / "src" / "opdi"
    pattern = re.compile(
        r"""(?:read_table|table_ref|table_exists)\(\s*["']([a-z_0-9]+)["']"""
        r"""|write_table\([^,]+,\s*["']([a-z_0-9]+)["']"""
    )
    found = set()
    for p in src.rglob("*.py"):
        for m in pattern.finditer(p.read_text()):
            found.add(m.group(1) or m.group(2))
    for name in ("REJECTED_TABLE", "AIRSPACE_TABLE", "STAGING_TABLE"):
        for p in src.rglob("*.py"):
            m = re.search(rf'^{name} = "([a-z_0-9]+)"', p.read_text(), re.M)
            if m:
                found.add(m.group(1))
    # Tables only steps outside 01 to 04b use (reference building, publishing).
    outside = {"oa_countries", "oa_regions", "oa_navaids", "oa_airport_frequencies"}
    assert found - outside <= set(ae.ALL_TABLES), sorted(found - outside - set(ae.ALL_TABLES))


def test_the_table_sets_do_not_overlap():
    sets = [set(ae.PASSTHROUGH), {ae.FIR_TABLE}, set(ae.SHARED_INPUTS), set(ae.DERIVED)]
    for i, a in enumerate(sets):
        for b in sets[i + 1 :]:
            assert not a & b


def test_shared_inputs_are_the_same_for_both_runs():
    for t in ae.SHARED_INPUTS:
        assert ae.table_path("base", t) == ae.table_path("merged", t)
        assert ae.table_path("base", t) == f"{ae.PREFIX}/input/{t}"


def test_derived_tables_are_separate_per_run():
    assert ae.table_path("base", "osn_tracks_clean") == f"{ae.PREFIX}/base/osn_tracks_clean"
    assert ae.table_path("merged", "osn_tracks_clean") == f"{ae.PREFIX}/merged/osn_tracks_clean"


def test_the_fir_reference_is_read_from_the_airspace_study():
    assert ae.table_path("merged", ae.FIR_TABLE) == ae.FIR_REFERENCE
    with pytest.raises(PermissionError):
        ae.check_write_path(ae.FIR_REFERENCE)


def test_reference_tables_are_not_study_tables():
    with pytest.raises(RuntimeError):
        ae.table_path("base", "oa_airports")


def test_unknown_run_is_refused():
    with pytest.raises(ValueError):
        ae.table_path("other", "osn_tracks")


@pytest.mark.parametrize(
    "path",
    [
        "s3a://eurocontrol/opdi-world/osn_tracks",
        "s3a://eurocontrol/opdi-world/research/adsc_airframes/base/osn_tracks",
        "s3a://eurocontrol/opdi-world/research/adsc_v2",
        "s3a://eurocontrol/opdi/research/adsc_v2/base",
        "s3a://eurocontrol/",
    ],
)
def test_writes_outside_the_study_folder_are_refused(path):
    with pytest.raises(PermissionError):
        ae.check_write_path(path)


def test_writes_inside_the_study_folder_are_allowed():
    ae.check_write_path(f"{ae.PREFIX}/merged/osn_tracks")


def test_the_two_configurations_differ_only_in_adsc():
    b, m = ae.build_config("base"), ae.build_config("merged")
    assert b.adsc.enabled is False and m.adsc.enabled is True
    m.adsc.enabled = False
    assert repr(b) == repr(m)


def test_both_runs_use_todays_default_track_rule_worldwide():
    from opdi.config import OPDIConfig

    default = OPDIConfig.for_environment("opensky", worldwide=True)
    cfg = ae.build_config("merged")
    assert cfg.segmentation == default.segmentation
    assert cfg.cleaning == default.cleaning
    assert cfg.events == default.events
    assert cfg.coverage.bbox is None


def test_clean_keeps_to_the_study_folder():
    root = ae.PREFIX.replace("s3a://eurocontrol/", "")
    keys = [f"{root}/base/osn_tracks_clean/a.parquet", f"{root}/input/osn_adsc/b.parquet"]
    assert ae.clean_keys(keys, "all") == keys
    assert ae.clean_keys(keys, "base") == keys[:1]
    with pytest.raises(PermissionError):
        ae.clean_keys(keys + ["opdi-world/osn_tracks/c.parquet"], "all")


def test_fetch_needs_the_adsc_day_and_an_empty_adsb_table():
    day = date(2026, 6, 14)
    ae.check_before_fetch([], ["2026-06-13", "2026-06-14"], day)
    with pytest.raises(RuntimeError, match="earlier list"):
        ae.check_before_fetch(["2026-06-13"], ["2026-06-14"], day)
    with pytest.raises(RuntimeError, match="01b"):
        ae.check_before_fetch([], ["2026-06-13"], day)


def test_a_run_needs_three_days_of_both_inputs():
    day = date(2026, 6, 14)
    three = ae.window_days(day)
    assert three == ["2026-06-13", "2026-06-14", "2026-06-15"]
    ae.check_before_run(three, three, day)
    with pytest.raises(RuntimeError, match="2026-06-15"):
        ae.check_before_run(three[:2], three, day)


def test_timings_keep_one_row_per_run_and_step(tmp_path):
    p = tmp_path / "t.csv"
    ae.record_timing(p, "base", "02", 100.0, 1.0)
    ae.record_timing(p, "merged", "02", 120.0, 1.5)
    ae.record_timing(p, "base", "02", 90.0, 1.2)
    t = pd.read_csv(p, dtype={"step": str})
    assert len(t) == 2
    assert t.set_index("run").loc["base", "seconds"] == 90.0


# --- gaps ----------------------------------------------------------------------


def test_gap_classes_have_closed_lower_edges():
    c = ae.gap_class(pd.Series([4.9, 5.0, 9.99, 10.0, 30.0, 240.0, 1000.0]))
    assert pd.isna(c[0])
    assert list(c[1:]) == [
        "5-10 min",
        "5-10 min",
        "10-20 min",
        "30-60 min",
        "240+ min",
        "240+ min",
    ]


def test_gap_table_counts_and_sums_per_run():
    gaps = pd.DataFrame(
        {"run": ["base", "base", "base", "merged"], "gap_s": [3600.0, 4000.0, 100.0, 700.0]}
    )
    t = ae.gap_table(gaps)
    row = t[(t["run"] == "base") & (t["gap_class"] == "60-120 min")].iloc[0]
    assert row["gaps"] == 2
    assert row["gap_hours"] == pytest.approx(7600.0 / 3600.0)
    assert set(t["run"]) == {"base", "merged"}
    assert len(t) == 2  # the 100 s gap is below the floor


# --- ADS-C reports -------------------------------------------------------------


def test_adsc_positions_relative_to_adsb():
    before = pd.Series([10.0, 400.0, 2000.0, np.nan, 500.0, np.nan, 2000.0])
    after = pd.Series([4000.0, 400.0, 1900.0, 50.0, np.nan, np.nan, 100.0])
    assert list(ae.adsc_position(before, after)) == [
        "near_adsb",
        "in_gap",
        "in_hole",
        "before_first_adsb",
        "after_last_adsb",
        "no_adsb",
        "near_adsb",
    ]


def test_a_report_exactly_at_the_hole_limit_is_not_in_a_hole():
    s = pd.Series([ae.HOLE_SECONDS])
    assert ae.adsc_position(s, s).iloc[0] == "in_gap"


def test_adsc_report_table_shares_add_up():
    pos = pd.DataFrame(
        {"icao24": ["a", "a", "b", "c"], "position": ["in_hole", "near_adsb", "in_hole", "no_adsb"]}
    )
    t = ae.adsc_report_table(pos, {"input_reports_on_day": 10})
    v = t.set_index("metric")
    assert v.loc["input_reports_on_day", "value"] == 10
    assert v.loc["kept_in_merged_tracks", "value"] == 4
    assert v.loc["aircraft_kept_in_merged_tracks", "value"] == 3
    assert v.loc["position_in_hole", "share"] == pytest.approx(0.5)
    shares = v.loc[v.index.str.startswith("position_"), "share"]
    assert shares.sum() == pytest.approx(1.0)


# --- reference flights ----------------------------------------------------------


def _truth():
    return pd.DataFrame(
        {
            "flight_key": [0, 1, 2],
            "icao24": ["a", "a", "b"],
            "t_off": [T0, T0 + minutes(300), T0],
            "t_on": [T0 + minutes(240), T0 + minutes(420), T0 + minutes(500)],
            "flow": ["outside_to_europe", "intra_europe", "europe_to_outside"],
        }
    )


def _tracks(rows):
    return pd.DataFrame(
        [
            {
                "track_id": t,
                "icao24": i,
                "first_seen": T0 + minutes(s),
                "last_seen": T0 + minutes(e),
                "adsc_reports": (rest or [0])[0],
            }
            for t, i, s, e, *rest in rows
        ]
    )


def test_overlap_counts_tracks_per_flight():
    truth = _truth()
    tracks = _tracks([("t1", "a", 10, 100), ("t2", "a", 150, 230), ("t3", "b", 5, 490)])
    n = ae.overlap_counts(truth, tracks)
    assert list(n) == [2, 0, 1]


def test_tracks_per_flight_and_its_changes():
    truth = _truth()
    base = _tracks([("t1", "a", 10, 100), ("t2", "a", 150, 230), ("t3", "b", 5, 490)])
    merged = _tracks([("m1", "a", 10, 230, 5), ("m3", "b", 5, 490, 0)])
    table, per_flight = ae.tracks_per_flight_table(truth, {"base": base, "merged": merged})
    row = table.set_index(["run", "flow"]).loc[("merged", "outside_to_europe")]
    assert row["one_track"] == 1 and row["flights"] == 1
    assert list(per_flight["tracks_base"]) == [2, 0, 1]
    assert list(per_flight["tracks_merged"]) == [1, 0, 1]
    ch = ae.changed_flights_table(per_flight).set_index("flow")
    assert ch.loc["outside_to_europe", "became_one_track"] == 1
    assert ch.loc["outside_to_europe", "fewer_tracks"] == 1
    assert ch["more_tracks"].sum() == 0


def test_a_track_over_two_flights_is_a_false_join():
    truth = _truth()
    tracks = _tracks([("x", "a", 10, 400, 3), ("y", "b", 0, 500, 0)])
    fj = ae.false_joins(truth, tracks)
    assert list(fj["track_id"]) == ["x"]
    assert fj["flights"].iloc[0] == 2
    t = ae.false_join_table(truth, {"merged": tracks}).iloc[0]
    assert t["false_joins"] == 1 and t["false_joins_with_adsc"] == 1


def test_a_track_touching_only_the_ground_part_of_a_flight_is_not_a_false_join():
    truth = _truth()
    # Ends 5 min after the second flight's off-block: inside the margin.
    tracks = _tracks([("x", "a", 10, 305, 0)])
    assert ae.false_joins(truth, tracks).empty


def test_great_circle_of_a_known_pair():
    # One degree of latitude is 60 NM on this sphere, to within 0.1%.
    assert ae.great_circle_nm(0.0, 0.0, 1.0, 0.0) == pytest.approx(60.04, rel=1e-3)


def test_distance_table_uses_only_flights_with_a_base_hole_in_both_runs():
    pf = pd.DataFrame(
        {
            "run": ["base", "merged", "base", "merged", "base"],
            "flight_key": [0, 0, 1, 1, 2],
            "distance_nm": [900.0, 1000.0, 500.0, 500.0, 700.0],
            "gc_nm": [1000.0, 1000.0, 500.0, 500.0, 600.0],
            "ref_nm": [1050.0, 1050.0, np.nan, np.nan, 800.0],
            "base_hole": [True, True, False, False, True],
        }
    )
    t = ae.distance_table(pf).set_index("run")
    # Flight 1 has no hole; flight 2 lacks the merged run.
    assert t.loc["base", "flights"] == 1 and t.loc["merged", "flights"] == 1
    assert t.loc["base", "median_ratio_to_great_circle"] == pytest.approx(0.9)
    assert t.loc["base", "share_below_great_circle"] == 1.0
    assert t.loc["merged", "median_ratio_to_reference"] == pytest.approx(1000 / 1050)
    assert t.loc["merged", "share_within_5pct_of_reference"] == 1.0
    assert t.loc["base", "share_within_5pct_of_reference"] == 0.0


def test_reference_flights_keep_two_legs_apart(tmp_path):
    p = tmp_path / "f.parquet"
    pd.DataFrame(
        {
            "AIRCRAFT_ADDRESS": ["ABC123", "abc123", "def456", None],
            "AIRCRAFT_ID": ["DLH1 ", "DLH1", "AFR2", "X"],
            "ADEP": ["EDDF", "KJFK", "LFPG", "EGLL"],
            "ADES": ["KJFK", "EDDF", "FAOR", "EDDF"],
            "AOBT_3": pd.to_datetime(
                ["2026-06-14 10:00", "2026-06-14 22:00", "2026-06-16 01:00", "2026-06-14 05:00"]
            ).tz_localize("Europe/Paris"),
            "ARVT_3": pd.to_datetime(
                ["2026-06-14 19:00", "2026-06-15 07:00", "2026-06-16 12:00", "2026-06-14 06:00"]
            ).tz_localize("Europe/Paris"),
            "RTE_LEN_3": [3300, 3350, 4800, 400],
        }
    ).to_parquet(p)
    t = ae.load_reference_flights(p, ["2026-06-14"], {"abc123", "def456"})
    assert list(t["flight_key"]) == [0, 1]
    assert list(t["callsign"]) == ["DLH1", "DLH1"]
    # Paris summer time is UTC+2.
    assert t["t_off"].iloc[0] == pd.Timestamp("2026-06-14 08:00")
    assert list(ae.inside_day(t, "2026-06-14")["gt_ades"]) == ["KJFK"]


# --- events --------------------------------------------------------------------


def test_shift_classes():
    c = ae.shift_class(pd.Series([0.0, -5.0, 10.0, 11.0, 299.0, 4000.0, np.nan]))
    assert list(c[:6]) == ["0 s", "<=10 s", "<=10 s", "<=60 s", "<=300 s", ">3600 s"]
    assert pd.isna(c.iloc[6])


def test_events_table_counts_each_side():
    status = pd.DataFrame(
        {
            "type": ["xing-fir"] * 4 + ["level-start"],
            "in_base_hole": [False, False, True, True, False],
            "status": ["same_time", "moved", "only_merged", "moved", "same_time"],
            "n": [10, 2, 5, 1, 7],
        }
    )
    moved = pd.DataFrame(
        {
            "type": ["xing-fir", "xing-fir"],
            "in_base_hole": [False, True],
            "median_shift_s": [30.0, 600.0],
            "p90_shift_s": [50.0, 600.0],
        }
    )
    t = ae.events_table(status, moved).set_index(["type", "in_base_hole"])
    row = t.loc[("xing-fir", False)]
    assert row["base_events"] == 12 and row["merged_events"] == 12
    assert row["unchanged_share"] == pytest.approx(10 / 12)
    hole = t.loc[("xing-fir", True)]
    assert hole["base_events"] == 1 and hole["merged_events"] == 6
    assert hole["median_shift_s"] == 600.0
    assert t.loc[("level-start", False), "only_base"] == 0
    assert pd.isna(t.loc[("level-start", False), "median_shift_s"])


def test_fir_table_puts_both_runs_side_by_side():
    fir = pd.DataFrame(
        {
            "run": ["base", "merged", "merged"],
            "fir_to": ["EGGXFIR", "EGGXFIR", "LFFFFIR"],
            "crossings": [3, 10, 4],
            "bracket_over_limit": [3, 8, 0],
            "median_bracket_s": [5000.0, 700.0, 5.0],
        }
    )
    t = ae.fir_table(fir).set_index("fir_to")
    assert t.loc["EGGXFIR", "oceanic"]
    assert not t.loc["LFFFFIR", "oceanic"]
    assert t.loc["EGGXFIR", "added"] == 7
    assert t.loc["LFFFFIR", "crossings_base"] == 0
    assert t.index[0] == "EGGXFIR"


def test_rejected_share_is_of_all_detected_events():
    rejected = pd.DataFrame(
        {"run": ["base"], "reject_rule": ["airborne_ground_event"], "type": ["AOBT"], "n": [5]}
    )
    kept = pd.DataFrame({"run": ["base", "base"], "type": ["AOBT", "AIBT"], "n": [95, 100]})
    t = ae.rejected_table(rejected, kept).iloc[0]
    assert t["removed"] == 5 and t["kept"] == 95
    assert t["removed_share"] == pytest.approx(0.05)


def test_timing_table_shows_the_time_adsc_adds():
    timings = pd.DataFrame(
        {
            "run": ["inputs", "inputs", "base", "merged", "base", "merged"],
            "step": ["01b", "01", "02", "02", "04", "04"],
            "seconds": [30.0, 3000.0, 300.0, 330.0, 600.0, 590.0],
        }
    )
    t = ae.timing_table(timings).set_index("step")
    assert t.loc["02", "added_s"] == 30.0
    assert t.loc["04", "added_s"] == -10.0
    assert t.loc["01", "shared_inputs"] == 3000.0
    assert pd.isna(t.loc["01", "base"])


def test_flight_gap_table_counts_only_flights_seen():
    pf = pd.DataFrame(
        {
            "run": ["base", "base", "base", "merged", "merged", "merged"],
            "flow": ["outside_to_europe"] * 6,
            "reports": [100, 50, np.nan, 110, 60, 3],
            "adsc_reports": [0, 0, np.nan, 10, 10, 3],
            "max_gap_s": [7200.0, 600.0, np.nan, 1200.0, 600.0, 900.0],
            "holes": [1, 0, np.nan, 0, 0, 0],
        }
    )
    t = ae.flight_gap_table(pf).set_index("run")
    assert t.loc["base", "flights"] == 3 and t.loc["base", "flights_seen"] == 2
    assert t.loc["base", "flights_with_hole"] == 1 and t.loc["merged", "flights_with_hole"] == 0
    assert t.loc["base", "median_longest_gap_min"] == pytest.approx(65.0)
    assert t.loc["merged", "flights_with_adsc"] == 3
