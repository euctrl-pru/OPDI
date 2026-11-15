"""The spot checks: how examples are drawn, and what a reading sheet says (pandas only)."""

import numpy as np
import pandas as pd
import pytest
import spot_checks as s

DAY = "2026-06-14"
MEASURES = (
    ("height_ft", "height", "both"),
    ("speed_kt", "speed", "both"),
    ("small_airport", "small", "high"),
)


def _items(n, seed=0):
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {
            "event_id": [f"e{i:04d}" for i in range(n)],
            "height_ft": rng.normal(0, 100, n),
            "speed_kt": rng.normal(150, 20, n),
            "small_airport": (rng.random(n) < 0.05).astype(float),
        }
    )


# --- drawing ----------------------------------------------------------------


def test_random_part_is_reproducible_from_the_seed():
    df = _items(500)
    a = s.draw_examples(df, MEASURES, seed=1)
    b = s.draw_examples(df, MEASURES, seed=1)
    c = s.draw_examples(df, MEASURES, seed=2)
    ra = a.loc[a["drawn"] == "random", "event_id"].tolist()
    assert ra == b.loc[b["drawn"] == "random", "event_id"].tolist()
    assert len(ra) == s.N_RANDOM
    assert set(ra) != set(c.loc[c["drawn"] == "random", "event_id"])


def test_random_key_does_not_depend_on_row_order():
    keys = ["a", "b", "c"]
    k1 = s.random_key(keys, 7)
    k2 = s.random_key(list(reversed(keys)), 7)
    assert list(k1) == list(reversed(list(k2)))
    assert all(0 <= x < 1 for x in k1)


def test_tails_are_drawn_from_the_named_measures():
    df = _items(500)
    out = s.draw_examples(df, MEASURES)
    tails = out[out["drawn"] != "random"]
    assert 0 < len(tails) <= s.N_TAIL
    assert len(out) == out["event_id"].nunique()
    lo = df.loc[df["height_ft"].idxmin(), "event_id"]
    hi = df.loc[df["speed_kt"].idxmax(), "event_id"]
    assert lo in set(out["event_id"]) and hi in set(out["event_id"])
    drawn = dict(zip(out["event_id"], out["drawn"]))
    assert "lowest height_ft" in drawn[lo] or drawn[lo] == "random"
    # "high" only: no lowest of the flag
    assert not any("lowest small_airport" in d for d in out["drawn"])
    assert any("highest small_airport" in d for d in out["drawn"])
    # every reason names a measure of the list
    names = {m for m, _, _ in MEASURES}
    for d in tails["drawn"]:
        for part in d.split("; "):
            assert part.split(" ", 1)[1] in names


def test_rows_without_a_value_are_not_in_a_tail():
    df = _items(200)
    df.loc[df.index[:150], "height_ft"] = np.nan
    out = s.draw_examples(df, MEASURES)
    for _, r in out.iterrows():
        if "height_ft" in r["drawn"]:
            assert not np.isnan(r["height_ft"])


def test_a_small_item_is_returned_whole():
    df = _items(49)
    out = s.draw_examples(df, MEASURES)
    assert len(out) == 49
    assert set(out["drawn"]) == {"all"}
    assert set(out["event_id"]) == set(df["event_id"])


def test_a_pool_gives_the_same_examples_as_all_rows():
    df = _items(2000)
    df["_rand"] = s.random_key(df["event_id"].tolist(), s.SEED)
    full = s.draw_examples(df, MEASURES)
    keep = set(df.nsmallest(s.N_RANDOM, "_rand")["event_id"])
    for m, _, _ in MEASURES:
        keep |= set(df.nsmallest(s.N_TAIL, m)["event_id"]) | set(
            df.nlargest(s.N_TAIL, m)["event_id"]
        )
    pool = df[df["event_id"].isin(keep)]
    part = s.draw_examples(pool, MEASURES, total=len(df))
    assert part["event_id"].tolist() == full["event_id"].tolist()
    assert part["drawn"].tolist() == full["drawn"].tolist()


def test_thin_keeps_the_nearest_the_ends_and_the_marked():
    dts = np.arange(-180, 181, 1.0)
    idx = s.thin(dts, n=40, keep=[5])
    assert len(idx) <= 40
    assert idx == sorted(idx)
    assert 0 in idx and len(dts) - 1 in idx and 5 in idx
    assert int(np.argmin(np.abs(dts))) in idx
    assert s.thin([1.0, 2.0], n=40) == [0, 1]


def test_items_map_to_families_and_partners():
    assert s.event_rule("ATOT")[0] == "a_runway"
    assert s.event_rule("entry-taxiway")[0] == "b_block_surface"
    assert s.event_rule("xing-fl100")[0] == "c_vertical"
    assert s.event_rule("xing-40nm")[0] == "d_rings_fir_seen_measurements"
    assert s.event_rule("holding-end")[3] == "episode"
    assert s.event_rule("no-such-type") is None
    assert s.pair_of("entry-apron") == ("exit-apron", True)
    assert s.pair_of("exit-apron") == ("entry-apron", False)
    assert s.pair_of("xing-fl50") is None


# --- summaries ----------------------------------------------------------------


def test_summary_gives_percentiles_counts_and_record_shares():
    df = pd.DataFrame({"height_ft": np.arange(101.0), "speed_kt": [np.nan] * 101})
    rows = s.summary_rows(df, MEASURES[:2])
    assert rows[0]["count"] == 101 and rows[0]["p50"] == 50.0 and rows[0]["p99"] == 99.0
    assert rows[1]["count"] == 0 and rows[1]["p50"] is None
    text = s.summary_markdown(
        "ATOT", 200, rows, {"with_record": 50, "within_60s": 40, "what": "take-off time"}
    )
    assert "Rows of the item on the day: 200." in text
    assert "50 (25.0% of the rows)" in text and "40 (80.0% of those with a record)" in text
    assert "| height_ft | 101 |" in text


# --- sheets -----------------------------------------------------------------


def _airports():
    return pd.DataFrame(
        {
            "ident": ["EDDF", "LEBL"],
            "name": ["Frankfurt", "Barcelona"],
            "type": ["large_airport", "large_airport"],
            "latitude_deg": [50.0333, 41.2971],
            "longitude_deg": [8.5706, 2.0785],
            "elevation_ft": [364.0, 12.0],
        }
    ).set_index("ident")


def _runways():
    return pd.DataFrame(
        {
            "airport_ident": ["EDDF"],
            "le_ident": ["07C"],
            "le_latitude_deg": [50.0326],
            "le_longitude_deg": [8.5344],
            "le_heading_degT": [69.6],
            "he_ident": ["25C"],
            "he_latitude_deg": [50.0451],
            "he_longitude_deg": [8.5870],
            "he_heading_degT": [249.6],
            "length_ft": [13123.0],
        }
    )


def _event_example():
    t0 = pd.Timestamp("2026-06-14 08:12:03")
    ex = {
        "ex_id": "ATOT#03",
        "drawn": "highest speed_kt",
        "event_id": "ev1",
        "flight_id": "abc_1781412334",
        "flt_id": "DLH123",
        "icao24": "3c6444",
        "typecode": "A320",
        "adep": "EDDF",
        "ades": "LEBL",
        "type": "ATOT",
        "event_time": t0,
        "latitude": 50.040,
        "longitude": 8.570,
        "altitude_ft": 380.0,
        "info": '{"apt_icao": "EDDF", "runway": "25C", "runway_bearing_deg": 249.6, "method": "acdm"}',  # noqa: E501
        "distance_flown_nm": 2.3,
        "time_passed_s": 812.0,
        "ctx_airport": "EDDF",
        "height_ft": 16.0,
        "speed_kt": 152.0,
        "vrate_fpm": 1200.0,
        "airport_dist_nm": 0.4,
        "ctx_gap_s": 0.5,
        "ctx_on_ground": 0.5,
        "prev_gap_s": 37.0,
        "next_gap_s": 60.0,
        "pair_time": None,
        "pair_s": None,
        "dep_apt": "EDDF",
        "dep_mvt": pd.Timestamp("2026-06-14 08:12:00"),
        "dep_block": pd.Timestamp("2026-06-14 07:58:12"),
        "dep_rwy": "25C",
        "dep_stand": "V155",
        "nm_adep": "EDDF",
        "nm_ades": "LEBL",
        "nm_aobt": pd.Timestamp("2026-06-14 07:58:00"),
        "nm_rte_len_nm": 612.0,
        "nm_type": "A320",
    }
    events = pd.DataFrame(
        {
            "event_id": ["ev0", "ev1", "ev2"],
            "type": ["take-off-roll", "ATOT", "xing-fl50"],
            "event_time": [t0 - pd.Timedelta(seconds=37), t0, t0 + pd.Timedelta(seconds=60)],
            "altitude_ft": [370.0, 380.0, 5000.0],
            "distance_flown_nm": [1.0, 2.3, 9.0],
            "info": ['{"runway": "25C"}', ex["info"], '{"direction": "up"}'],
        }
    )
    times = [t0 + pd.Timedelta(seconds=x) for x in (-10, -2, 3, 9)]
    reports = pd.DataFrame(
        {
            "part": "window",
            "event_time": times,
            "lat": [50.041, 50.040, 50.039, 50.038],
            "lon": [8.58, 8.57, 8.56, 8.55],
            "alt_m": [110.9, 115.0, np.nan, 160.0],
            "raw_alt_m": [110.9, 115.0, 9999.0, 160.0],
            "gs_ms": [70.0, 78.0, 80.0, 82.0],
            "vr_ms": [0.0, 3.0, 5.0, 6.0],
            "on_ground": [True, True, False, False],
            "heading": [249.0, 249.0, 250.0, 250.0],
        }
    )
    return ex, events, reports


def test_event_sheet_holds_header_event_record_and_marked_rows():
    ex, events, reports = _event_example()
    meta = {
        "name": "ATOT",
        "profile": "window",
        "record": ("dep_mvt", "take-off time"),
        "pair": None,
    }
    text = s.event_sheet(meta, ex, events, reports, _airports(), _runways(), DAY)
    head = text.splitlines()[2]
    for part in ("**ATOT**", "abc_1781412334", "DLH123", "A320", "EDDF → LEBL"):
        assert part in head
    assert "08:12:03 UTC" in text
    assert "EDDF (Frankfurt, large_airport, elevation 364 ft)" in text
    assert "event minus take-off time = +3 s" in text
    assert "Runway** 25C of EDDF, bearing 249.6°" in text and "NM along from the threshold" in text
    assert "Network Manager**: EDDF → LEBL" in text and "route length 612 NM" in text
    # the example's own event row is marked, the others not
    marked = [ln for ln in text.splitlines() if ln.startswith("| ◀ |")]
    assert len(marked) == 1 and "ATOT" in marked[0]
    # report marks and height above the airport: 115 m = 377 ft, 13 ft above EDDF
    rows = [ln for ln in text.splitlines() if ln.startswith("| *<") or ln.startswith("| <")]
    assert rows and "| 377 | 13 |" in rows[0]
    assert any(ln.startswith("| > |") or ln.startswith("| *> |") for ln in text.splitlines())
    assert "~32,805" in text  # removed by cleaning: raw value shown with ~


def test_sheet_says_when_there_is_no_record_or_partner():
    ex, events, reports = _event_example()
    for k in ("dep_mvt", "nm_adep", "nm_ades"):
        ex[k] = None
    meta = {
        "name": "ATOT",
        "profile": "window",
        "record": ("dep_mvt", "take-off time"),
        "pair": ("runway-vacated", True),
    }
    text = s.event_sheet(meta, ex, events, reports, _airports(), _runways(), DAY)
    assert "no take-off time for this flight" in text
    assert "no flight matched" in text
    assert "no runway-vacated after it in the track" in text


def test_flight_sheet_lists_ends_candidates_and_network_manager():
    ex, events, reports = _event_example()
    ex.update(
        {
            "ex_id": "fl_departure_airport#01",
            "adep_source": "zone",
            "adep_p": "EDFE",
            "trk_first_time": pd.Timestamp("2026-06-14 07:50:00"),
            "trk_first_lat": 50.03,
            "trk_first_lon": 8.57,
            "trk_first_alt_ft": 370.0,
            "trk_first_gs_kt": 0.0,
            "trk_first_on_ground": True,
            "first_dist_adep_nm": 0.1,
        }
    )
    cand = pd.DataFrame(
        {
            "role": ["adep", "adep", "ades"],
            "apt_ident": ["EDDF", "EDFE", "LEBL"],
            "dist_nm": [0.1, 4.0, 0.5],
            "eff_nm": [0.1, 4.0, 0.5],
            "agl_ft": [6.0, 300.0, 3.0],
            "elev_known": [True, True, True],
            "on_ground": [True, True, True],
            "at_border": [False, False, False],
        }
    )
    reports = reports.assign(part="profile")
    meta = {
        "name": "fl_departure_airport",
        "columns": ("adep", "adep_source", "adep_p"),
        "measures": s.FLIGHT_LIST_ITEMS["fl_departure_airport"][1],
    }
    text = s.flight_sheet(meta, ex, events, reports, cand, _airports(), DAY)
    assert "This item's columns**: adep EDDF · adep_source zone · adep_p EDFE" in text
    assert "First report**: 07:50:00" in text and "NM from EDDF" in text and "6 ft above it" in text
    lines = text.splitlines()
    assert any("| departure | EDDF |" in ln and "chosen" in ln for ln in lines)
    assert any("| departure | EDFE |" in ln and "alternative" in ln for ln in lines)
    assert "Network Manager**: EDDF → LEBL" in text


def test_times_carry_the_day_offset():
    assert s.fmt_time(pd.Timestamp("2026-06-15 00:10:00"), DAY) == "00:10:00 (+1 d)"
    assert s.fmt_time(pd.Timestamp("2026-06-14 23:59:59"), DAY) == "23:59:59"
    assert s.fmt_time(None) == ""


def test_runway_position_along_and_across():
    rw = _runways().iloc[0]
    along, cross = s.runway_position(
        rw["le_latitude_deg"],
        rw["le_longitude_deg"],
        rw["he_latitude_deg"],
        rw["he_longitude_deg"],
        249.6,
    )
    assert along == pytest.approx(13123 / 6076.12, rel=0.05) and cross < 0.05


def _flight_list_frame():
    t = pd.Timestamp("2026-06-14 08:00:00")
    return pd.DataFrame(
        {
            "flight_id": ["f1", "f2"],
            "adep": ["EDDF", None],
            "ades": ["LEBL", "LEBL"],
            "adep_p": [None, None],
            "ades_p": ["EDDF", None],
            "typecode": ["A320", None],
            "nm_type": ["A321", None],
            "nm_adep": ["EDDF", "LEBL"],
            "nm_ades": ["LEBL", "LEBL"],
            "first_seen": [t, t],
            "last_seen": [t + pd.Timedelta(hours=2)] * 2,
            "trk_first_time": [t, t],
            "trk_last_time": [t + pd.Timedelta(hours=2)] * 2,
            "trk_first_lat": [50.0333, 41.3],
            "trk_first_lon": [8.5706, 2.08],
            "trk_first_alt_ft": [374.0, 30.0],
            "trk_last_lat": [41.2971, 41.2971],
            "trk_last_lon": [2.0785, 2.0785],
            "trk_last_alt_ft": [20.0, 20.0],
            "atot": [t + pd.Timedelta(minutes=15), pd.NaT],
            "dep_mvt": [t + pd.Timedelta(minutes=14), pd.NaT],
            "aldt": [t + pd.Timedelta(minutes=110), pd.NaT],
            "arr_mvt": [pd.NaT, pd.NaT],
            "aobt": [t + pd.Timedelta(minutes=2), pd.NaT],
            "dep_block": [t + pd.Timedelta(minutes=2), pd.NaT],
            "aibt": [pd.NaT, pd.NaT],
            "arr_block": [pd.NaT, pd.NaT],
            "rwy_dep": ["07C", None],
            "dep_rwy": ["7C", None],
            "rwy_arr": [None, None],
            "arr_rwy": [None, None],
            **{
                f"c{r}_{s_}": [pd.NaT, pd.NaT]
                for r in (40, 50, 60, 100, 110, 120)
                for s_ in ("dep", "arr")
            },
            "arr_c40": [pd.NaT, pd.NaT],
            "arr_c100": [pd.NaT, pd.NaT],
            "superseded_dep": [False, True],
            "superseded_arr": [False, False],
            "movement_dep": [True, True],
            "movement_arr": [True, False],
            "track_duration_min": [120.0, 121.0],
        }
    )


def test_flight_list_measures():
    d = s.flight_list_measures(_flight_list_frame(), _airports())
    assert d.loc[0, "first_dist_adep_nm"] < 0.01 and d.loc[0, "first_hgt_adep_ft"] == 10.0
    assert np.isnan(d.loc[1, "first_dist_adep_nm"]) and d.loc[1, "adep_missing"] == 1.0
    assert d.loc[0, "nm_adep_differs"] == 0.0 and np.isnan(d.loc[1, "nm_adep_differs"])
    assert d.loc[0, "nm_type_differs"] == 1.0
    assert d.loc[0, "atot_diff_s"] == 60.0 and d.loc[0, "taxi_out_min"] == 13.0
    assert d.loc[0, "rwy_dep_differs"] == 0.0  # 07C and 7C are the same runway
    assert d.loc[0, "alt_arr_dist_nm"] > 500
    assert list(d["superseded_any"]) == [0.0, 1.0] and list(d["not_movement_any"]) == [0.0, 1.0]
    assert d.loc[1, "duration_diff_min"] == 1.0
    rec = s.flight_list_record("fl_departure_airport", d)
    assert rec["with_record"] == 1 and rec["within_60s"] == 1


def test_write_sheets_builds_index_inventory_and_one_file_per_item(tmp_path):
    import json

    ex, events, reports = _event_example()
    fam = tmp_path / "a_runway"
    fam.mkdir()
    exdf = pd.DataFrame([ex])
    exdf.to_parquet(fam / "ATOT.parquet")
    reports.assign(ex_id="ATOT#03").to_parquet(fam / "ATOT_reports.parquet")
    events.assign(flight_id=ex["flight_id"]).to_parquet(fam / "ATOT_events.parquet")
    _airports().reset_index().to_parquet(tmp_path / "_airports.parquet")
    _runways().to_parquet(tmp_path / "_runways.parquet")
    item = {
        "kind": "event",
        "name": "ATOT",
        "family": "a_runway",
        "count": 1234,
        "with_record": 600,
        "record": "take-off time",
        "rejected": 3,
        "duplicate_ids": 2,
        "drawn": 1,
        "methods_page": "methods/takeoff-landing.qmd",
        "source": "src/opdi/pipeline/runway_ops.py",
        "profile": "window",
        "record_col": ["dep_mvt", "take-off time"],
        "pair": None,
        "columns": [],
        "measures": [list(m) for m in s.EVENT_MEASURES],
    }
    (tmp_path / "_items.json").write_text(json.dumps({"day": DAY, "label": "t", "items": [item]}))
    assert s.write_sheets(tmp_path) == 0
    sheet = (fam / "ATOT.md").read_text()
    assert sheet.startswith("# ATOT: 1 examples of 1,234 on 2026-06-14")
    assert "### ATOT · ATOT#03" in sheet
    index = (tmp_path / "INDEX.md").read_text()
    assert "## a_runway" in index and "| ATOT | event | 1,234 | 600 | 3 | 2 | 1 |" in index
    inv = pd.read_csv(tmp_path / "inventory.csv")
    assert list(inv.columns) == list(s.INVENTORY_COLUMNS)
    assert (
        inv.loc[0, "count"] == 1234 and inv.loc[0, "methods_page"] == "methods/takeoff-landing.qmd"
    )
