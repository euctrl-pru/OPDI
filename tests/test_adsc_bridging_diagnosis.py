"""The hole verdicts: does ADS-C leave a gap the segmentation would still split on?"""

import adsc_bridging_diagnosis as b
import pandas as pd


def _t(s):
    return pd.Timestamp(f"2026-06-05 {s}")


def test_remaining_gap_is_the_longest_silence_left():
    got = b.largest_remaining_gap_min(_t("10:00"), _t("12:00"), [_t("10:20"), _t("10:40")])
    assert got == 80.0


def test_remaining_gap_ignores_samples_outside_the_hole():
    assert b.largest_remaining_gap_min(_t("10:00"), _t("11:00"), [_t("09:00")]) == 60.0


def test_verdicts():
    assert b.classify_hole(3, 10.0, 30000.0, True) == "joined"
    assert b.classify_hole(0, 120.0, 30000.0, False) == "no_adsc_in_hole"
    assert b.classify_hole(3, 45.0, 30000.0, False) == "adsc_too_sparse"
    assert b.classify_hole(3, 20.0, 3000.0, False) == "low_altitude_rule"
    assert b.classify_hole(3, 20.0, 30000.0, False) == "dense_enough_but_split"
    assert b.classify_hole(3, 20.0, None, False) == "dense_enough_but_split"


def test_a_hole_must_sit_inside_the_flight():
    truth = pd.DataFrame({"icao24": ["a"], "flight_key": [0], "flow": ["x"],
                          "t_off": [_t("08:00")], "t_on": [_t("16:00")]})
    holes = pd.DataFrame({
        "icao24": ["a", "a"],
        "hole_start": [_t("10:00"), _t("17:00")],
        "hole_end": [_t("12:00"), _t("19:00")]})
    got = b.holes_in_flights(holes, truth)
    assert got["hole_start"].tolist() == [_t("10:00")]


def test_diagnose_reads_clean_adsc_for_the_verdict_and_keeps_raw_beside_it():
    holes = pd.DataFrame({
        "flight_key": [0], "icao24": ["a"], "flow": ["x"],
        "hole_start": [_t("10:00")], "hole_end": [_t("11:00")],
        "start_alt_ft": [35000.0], "end_alt_ft": [35000.0], "joined": [False]})
    raw = pd.DataFrame({"icao24": ["a"], "event_time": [_t("10:30")]})
    clean = pd.DataFrame({"icao24": pd.Series([], dtype=str),
                          "event_time": pd.Series([], dtype="datetime64[ns]")})
    d = b.diagnose(holes, raw, clean).iloc[0]
    assert d["adsc_raw"] == 1 and d["adsc_clean"] == 0
    assert d["residual_raw_min"] == 30.0 and d["residual_clean_min"] == 60.0
    assert d["why"] == "no_adsc_in_hole"
    s = b.summarise(b.diagnose(holes, raw, clean)).set_index("metric")["value"]
    assert s["holes_raw_would_close_clean_does_not"] == 1
