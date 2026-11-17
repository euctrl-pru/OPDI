"""The arithmetic and the labels of ``benchmarks/airport_accuracy.py`` (pandas only, no Spark)."""

import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "benchmarks"))

import airport_accuracy as aa  # noqa: E402

# ---------------------------------------------------------------- labels


@pytest.mark.parametrize(
    "pct, label",
    [
        (100.0, "high"),
        (80.0, "high"),
        (79.9, "partial"),
        (50.0, "partial"),
        (49.9, "low"),
        (0.0, "low"),
        (None, None),
        (float("nan"), None),
    ],
)
def test_availability_label(pct, label):
    assert aa.availability_label(pct) == label


@pytest.mark.parametrize(
    "pct, refs, compared, label",
    [
        (80.0, 30, 30, "good"),
        (79.9, 500, 400, "fair"),
        (50.0, 500, 400, "fair"),
        (49.9, 500, 400, "limited"),
        (95.0, 29, 29, "no reference"),
        (95.0, 0, 0, "no reference"),
        (95.0, 300, 12, "too few compared"),
        (None, 300, 0, "too few compared"),
    ],
)
def test_accuracy_label(pct, refs, compared, label):
    assert aa.accuracy_label(pct, refs, compared) == label


# ---------------------------------------------------------------- items


@pytest.mark.parametrize(
    "item, side",
    [
        ("ATOT", "departure"),
        ("ATOT (acdm)", "departure"),
        ("AOBT", "departure"),
        ("line-up", "departure"),
        ("take-off-roll", "departure"),
        ("xing-40nm outbound", "departure"),
        ("ALDT", "arrival"),
        ("ALDT (legacy)", "arrival"),
        ("AIBT", "arrival"),
        ("runway-vacated", "arrival"),
        ("xing-runway-threshold", "arrival"),
        ("go-around", "arrival"),
        ("holding-start", "arrival"),
        ("pms-merge-point", "arrival"),
        ("xing-100nm inbound", "arrival"),
        ("top-of-climb", "any"),
        ("entry-parking_position", "any"),
        ("xing-fl100", "any"),
        ("flight list: ATOT", "departure"),
        ("flight list: STND_ARR", "arrival"),
        ("flight list: C40_DEP", "departure"),
        ("flight list: C100_ARR", "arrival"),
        ("flight list: RWY_DEP", "departure"),
        ("flight list: ADEP", "any"),
        ("flight list: FIRST_SEEN", "any"),
        ("measurement: Distance flown (NM)", "any"),
    ],
)
def test_item_side(item, side):
    assert aa.item_side(item) == side


@pytest.mark.parametrize(
    "icao, europe",
    [
        ("EGLL", True),
        ("LFPG", True),
        ("LTFM", True),
        ("BIKF", True),
        ("GCLP", True),
        ("UKBB", True),
        ("UDYZ", True),
        ("KJFK", False),
        ("OMDB", False),
        ("GMMN", False),
        ("UUEE", False),
        (None, False),
        ("", False),
    ],
)
def test_is_european(icao, europe):
    assert aa.is_european(icao) is europe


# ---------------------------------------------------------------- times


def test_time_stats_shares_median_p90_and_signed_mean():
    d = np.array([-10.0, 20.0, 50.0, -90.0, 400.0])
    s = aa.time_stats(d)
    assert s["compared"] == 5
    assert s["within_30s_pct"] == pytest.approx(40.0)
    assert s["within_60s_pct"] == pytest.approx(60.0)
    assert s["within_300s_pct"] == pytest.approx(80.0)
    assert s["median_abs_diff_s"] == pytest.approx(50.0)
    assert s["p90_abs_diff_s"] == pytest.approx(np.percentile(np.abs(d), 90))
    # The signed mean leaves out the differences over 300 s, as the scorecard does.
    assert s["mean_signed_diff_within_300s_s"] == pytest.approx((-10 + 20 + 50 - 90) / 4)


def test_time_stats_of_nothing_is_empty_not_zero():
    s = aa.time_stats(np.array([]))
    assert s["compared"] == 0
    assert all(s[k] is None or math.isnan(s[k]) for k in s if k != "compared")


def _matched():
    # Two records at EDDF (one found twice), one at LFPG; a record key per record.
    return pd.DataFrame(
        {
            "item": ["ATOT", "ATOT", "ATOT", "ATOT"],
            "airport": ["EDDF", "EDDF", "EDDF", "LFPG"],
            "rec_key": ["a", "a", "b", "c"],
            "diff_s": [10.0, 70.0, -20.0, 200.0],
        }
    )


def test_score_times_per_airport_counts_records_found_once():
    refs = pd.DataFrame(
        {"item": ["ATOT", "ATOT"], "airport": ["EDDF", "LFPG"], "reference_movements": [4, 2]}
    )
    out = aa.score_times(_matched(), refs, ["item", "airport"]).set_index("airport")
    assert out.loc["EDDF", "found"] == 2
    assert out.loc["EDDF", "availability_ref_pct"] == pytest.approx(50.0)
    assert out.loc["EDDF", "compared"] == 3
    assert out.loc["EDDF", "within_60s_pct"] == pytest.approx(200 / 3)
    assert out.loc["LFPG", "found"] == 1
    assert out.loc["LFPG", "within_60s_pct"] == pytest.approx(0.0)


def test_score_times_keeps_airports_with_records_and_no_event():
    refs = pd.DataFrame(
        {"item": ["ATOT", "ATOT"], "airport": ["EDDF", "LIRF"], "reference_movements": [4, 7]}
    )
    out = aa.score_times(_matched(), refs, ["item", "airport"]).set_index("airport")
    assert out.loc["LIRF", "found"] == 0
    assert out.loc["LIRF", "availability_ref_pct"] == pytest.approx(0.0)
    assert out.loc["LIRF", "compared"] == 0
    # A found record at an airport the reference table does not list is not dropped.
    assert "LFPG" in out.index


def test_score_times_pooled_equals_sum_of_airports():
    refs = pd.DataFrame(
        {"item": ["ATOT", "ATOT"], "airport": ["EDDF", "LFPG"], "reference_movements": [4, 2]}
    )
    m = _matched()
    pooled = aa.score_times(
        m, refs.groupby("item", as_index=False)["reference_movements"].sum(), ["item"]
    ).iloc[0]
    assert pooled["found"] == 3
    assert pooled["reference_movements"] == 6
    assert pooled["compared"] == 4
    assert pooled["within_60s_pct"] == pytest.approx(50.0)


# ---------------------------------------------------------------- equal values


@pytest.mark.parametrize(
    "named, rec, kind, verdict",
    [
        ("09L", "9L", "runway", "equal"),
        ("27R", "09L", "runway", "differs"),
        (None, "09L", "runway", "none named"),
        ("09L", None, "runway", "no record value"),
        ("A 12", "a12", "stand", "equal"),
        ("012", "12", "stand", "equal"),
        ("B1", "B2", "stand", "differs"),
        ("None", "B2", "stand", "none named"),
        ("EGLL", "EGLL", "airport", "equal"),
        ("EGKK", "EGLL", "airport", "differs"),
        (None, "EGLL", "airport", "none named"),
    ],
)
def test_agreement(named, rec, kind, verdict):
    assert aa.agreement(named, rec, kind) == verdict


def test_score_equal_separates_availability_from_accuracy():
    f = pd.DataFrame(
        {
            "item": ["RWY"] * 5,
            "airport": ["EDDF"] * 5,
            "named": ["25C", "25C", None, "07C", "25C"],
            "recorded": ["25C", "25C", "25C", "25C", None],
        }
    )
    refs = pd.DataFrame({"item": ["RWY"], "airport": ["EDDF"], "reference_movements": [10]})
    out = aa.score_equal(f, refs, ["item", "airport"], "runway").iloc[0]
    # Five records matched, four of them name a runway; OPDI names one for three of those.
    assert out["found"] == 3
    assert out["availability_ref_pct"] == pytest.approx(30.0)
    assert out["compared"] == 3
    assert out["share_equal_pct"] == pytest.approx(200 / 3)


# ---------------------------------------------------------------- own availability


def test_own_availability_fills_items_an_airport_never_has():
    bases = pd.DataFrame(
        {
            "airport": ["EDDF", "EDDF", "EDDF", "KJFK", "KJFK", "KJFK"],
            "side": ["departure", "arrival", "any"] * 2,
            "flights": [10, 8, 17, 5, 5, 9],
        }
    )
    carriers = pd.DataFrame(
        {"airport": ["EDDF", "KJFK"], "item": ["ATOT", "top-of-climb"], "carriers": [7, 9]}
    )
    out = aa.own_availability(bases, carriers, ["ATOT", "top-of-climb"])
    out = out.set_index(["airport", "item"])
    assert out.loc[("EDDF", "ATOT"), "flights_eligible"] == 10
    assert out.loc[("EDDF", "ATOT"), "availability_own_pct"] == pytest.approx(70.0)
    assert out.loc[("KJFK", "ATOT"), "flights_with_item"] == 0
    assert out.loc[("KJFK", "top-of-climb"), "flights_eligible"] == 9
    assert out.loc[("KJFK", "top-of-climb"), "availability_own_pct"] == pytest.approx(100.0)


# ---------------------------------------------------------------- day spread


def test_day_spread_is_max_minus_min_per_measure():
    per_day = pd.DataFrame(
        {
            "day": ["2026-06-10", "2026-06-14", "2026-06-17"],
            "item": ["ATOT"] * 3,
            "reference": ["airport records"] * 3,
            "within_60s_pct": [92.0, 93.0, 95.5],
            "median_abs_diff_s": [20.0, 21.0, 19.0],
        }
    )
    out = aa.day_spread(per_day, ["item", "reference"], ["within_60s_pct", "median_abs_diff_s"])
    w = out[out["measure"] == "within_60s_pct"].iloc[0]
    assert w["spread"] == pytest.approx(3.5)
    assert w["2026-06-14"] == pytest.approx(93.0)
    assert bool(w["stable"]) is True
    m = out[out["measure"] == "median_abs_diff_s"].iloc[0]
    assert m["spread"] == pytest.approx(2.0)


def test_day_spread_flags_a_share_that_moves_more_than_five_points():
    per_day = pd.DataFrame(
        {"day": ["a", "b", "c"], "item": ["AOBT"] * 3, "availability_own_pct": [30.0, 37.0, 31.0]}
    )
    out = aa.day_spread(per_day, ["item"], ["availability_own_pct"]).iloc[0]
    assert bool(out["stable"]) is False


# ---------------------------------------------------------------- airports


def test_top_airports_ranks_by_movements_and_filters_europe():
    counts = pd.Series({"EGLL": 50, "KJFK": 90, "LFPG": 70, "EDDF": 70, "EHAM": 10})
    eu = aa.top_airports(counts, 3, europe_only=True)
    assert list(eu["airport"]) == ["EDDF", "LFPG", "EGLL"]
    assert list(eu["rank"]) == [1, 2, 3]
    world = aa.top_airports(counts, 2, europe_only=False)
    assert list(world["airport"]) == ["KJFK", "EDDF"]


# ---------------------------------------------------------------- which reference


@pytest.mark.parametrize(
    "item, records, europe, ref",
    [
        ("ATOT", True, True, "airport records"),
        ("ATOT", False, True, "Network Manager"),
        ("ATOT", False, False, None),
        ("AOBT", False, True, None),
        ("flight list: ALDT", False, True, "Network Manager"),
        ("flight list: STND_DEP", True, True, "airport records"),
        ("flight list: ADEP", False, False, "Network Manager"),
        ("flight list: ADEP", True, True, "Network Manager"),
        ("top-of-climb", True, True, None),
    ],
)
def test_reference_for(item, records, europe, ref):
    assert aa.reference_for(item, records, europe) == ref


# ---------------------------------------------------------------- the tables end to end


def _extracts():
    """Two days, two airports with airport records (EDDF) or without (LOWW), one outside Europe."""
    days = ["2026-06-10", "2026-06-14"]
    x = {}
    x["apdf_events"] = pd.DataFrame(
        {
            "item": ["ATOT", "ATOT", "AOBT"],
            "kind": ["ATOT", "ATOT", "AOBT"],
            "rec_apt": ["EDDF"] * 3,
            "rec_key": ["k1", "k2", "k3"],
            "diff_s": [10.0, 90.0, 30.0],
            "subminute": [True] * 3,
            "runway": ["25C", "07C", None],
            "rec_rwy": ["25C"] * 3,
            "stand": [None, None, "A1"],
            "rec_stand": [None, None, "A1"],
            "day": days[0],
        }
    )
    x["apdf_records"] = pd.DataFrame(
        {
            "kind": ["ATOT", "AOBT", "ALDT"],
            "rec_apt": ["EDDF"] * 3,
            "reference_movements": [4, 4, 3],
            "with_runway": [4, 0, 3],
            "with_stand": [0, 4, 0],
            "reference_tracked": [3, 3, 2],
            "movements_all": [5, 5, 3],
            "day": days[0],
        }
    )
    x["apdf_flight_list"] = pd.DataFrame(
        {
            "column": ["ATOT", "ATOT"],
            "kind": ["ATOT", "ATOT"],
            "rec_apt": ["EDDF", "EDDF"],
            "rec_key": ["k1", "k2"],
            "diff_s": [5.0, None],
            "subminute": [True, True],
            "named_rwy": ["25C", "25C"],
            "rec_rwy": ["25C", "25C"],
            "named_stand": [None, None],
            "rec_stand": [None, None],
            "day": days[0],
        }
    )
    x["nm"] = pd.DataFrame(
        {
            "nm_adep": ["LOWW", "LOWW", "EDDF"],
            "nm_ades": ["EDDF", "KJFK", "LOWW"],
            "nm_type": ["A320", "B77W", "A320"],
            "route_nm": [300.0, 3600.0, 300.0],
            "matched": [True, True, False],
            "adep": ["LOWW", "LOWW", None],
            "ades": ["EDDF", None, None],
            "typecode": ["A320", "B77W", None],
            "fl_atot_diff_s": [20.0, None, None],
            "fl_aldt_diff_s": [None, None, None],
            "ev_atot_diff_s": [20.0, 400.0, None],
            "ev_aldt_diff_s": [None, None, None],
            "distance_nm": [290.0, 2000.0, None],
            "day": days[1],
        }
    )
    x["nm_counts"] = pd.DataFrame(
        {
            "airport": ["LOWW", "EDDF", "EDDF", "LOWW", "KJFK"],
            "side": ["departure", "departure", "arrival", "arrival", "arrival"],
            "nm_flights": [2, 1, 1, 1, 1],
            "day": days[1],
        }
    )
    bases = []
    for d in days:
        for a, n in (("EDDF", 10), ("LOWW", 6), ("KJFK", 20)):
            for s in aa.SIDE_BASE:
                bases.append({"airport": a, "side": s, "flights": n, "day": d})
    x["own_bases"] = pd.DataFrame(bases)
    x["own_carriers"] = pd.DataFrame(
        {
            "airport": ["EDDF", "KJFK", "EDDF"],
            "item": ["ATOT", "ATOT", "AOBT"],
            "carriers": [8, 15, 2],
            "day": [days[0], days[0], days[1]],
        }
    )
    x["own_pooled"] = pd.DataFrame(
        {
            "item": ["ATOT", "AOBT", "top-of-climb", "ATOT", "AOBT", "top-of-climb"],
            "values": [30, 4, 50, 31, 5, 52],
            "flights_with_item": [28, 4, 40, 29, 5, 41],
            "flights_eligible": [36, 36, 40, 36, 36, 41],
            "side": ["departure", "departure", "any"] * 2,
            "day": [days[0]] * 3 + [days[1]] * 3,
        }
    )
    x["airports"] = pd.DataFrame(
        {
            "airport": ["EDDF", "LOWW", "KJFK"],
            "name": ["Frankfurt", "Vienna", "JFK"],
            "iso_country": ["DE", "AT", "US"],
        }
    )
    x["meta"] = {d: {"flights": 1, "events": 1, "root": "r"} for d in days}
    return x


def test_build_tables_item_rows():
    t = aa.build_tables(_extracts())["item_accuracy.csv"]
    allx = t[t["days"] == "all 2 days"]
    atot = allx[(allx["item"] == "ATOT") & (allx["reference"] == "airport records")].iloc[0]
    assert atot["reference_movements"] == 4
    assert atot["found"] == 2
    assert atot["within_60s_pct"] == pytest.approx(50.0)
    assert atot["flights_with_item"] == 57
    # The Network Manager take-off times count only at European airports without records.
    nm = allx[(allx["item"] == "ATOT") & (allx["reference"] == "Network Manager")].iloc[0]
    assert nm["reference_movements"] == 2
    assert nm["compared"] == 2
    tc = allx[allx["item"] == "top-of-climb"].iloc[0]
    assert tc["reference"] == "no reference"
    assert tc["accuracy_label"] == "no reference"
    assert tc["values_per_day"] == pytest.approx(51.0)
    adep = allx[allx["item"] == "flight list: ADEP"].iloc[0]
    assert adep["reference_movements"] == 3
    assert adep["found"] == 2
    assert adep["share_equal_pct"] == pytest.approx(100.0)
    rwy = allx[allx["item"] == "flight list: RWY_DEP"].iloc[0]
    assert rwy["reference_movements"] == 4
    assert rwy["share_equal_pct"] == pytest.approx(100.0)


def test_build_tables_airports():
    b = aa.build_tables(_extracts())
    eu = b["airport_accuracy_europe.csv"]
    assert set(eu["airport"]) == {"EDDF", "LOWW"}
    assert eu.groupby("airport")["item"].nunique().nunique() == 1  # every airport, every item
    loww = eu[(eu["airport"] == "LOWW") & (eu["item"] == "ATOT")].iloc[0]
    assert loww["reference"] == "Network Manager"
    assert loww["accuracy_label"] == "no reference"  # 2 movements only
    eddf_aobt = eu[(eu["airport"] == "EDDF") & (eu["item"] == "AOBT")].iloc[0]
    assert eddf_aobt["reference"] == "airport records"
    assert eddf_aobt["flights_eligible"] == 20
    assert eddf_aobt["availability_own_pct"] == pytest.approx(10.0)
    named = eu[(eu["airport"] == "EDDF") & (eu["item"] == "ATOT runway (info.runway)")].iloc[0]
    assert pd.isna(named["flights_with_item"])
    w = b["airport_accuracy_world.csv"]
    assert w["airport"].iloc[0] == "KJFK"
    jfk = w[(w["airport"] == "KJFK") & (w["item"] == "AOBT")].iloc[0]
    assert jfk["reference"] == "no reference"
    assert "outside Europe" in jfk["reference_note"]
    ap = b["airports.csv"]
    assert ap.set_index("airport").loc["EDDF", "departures_with_take_off_pct"] == pytest.approx(
        40.0
    )
