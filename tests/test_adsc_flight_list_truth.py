"""The paired far-end scorer: pure pandas, no cluster."""

import adsc_flight_list_truth as t
import pandas as pd


def _truth(rows):
    return pd.DataFrame(
        rows,
        columns=["flight_key", "icao24", "callsign", "gt_adep", "gt_ades", "t_off", "t_on", "flow"],
    ).astype({"t_off": "datetime64[ns]", "t_on": "datetime64[ns]"})


def _flights(rows):
    return pd.DataFrame(
        rows, columns=["icao24", "callsign", "adep", "ades", "first_seen", "last_seen"]
    ).astype({"first_seen": "datetime64[ns]", "last_seen": "datetime64[ns]"})


TRUTH = _truth(
    [
        (
            0,
            "aaa",
            "DLH1",
            "KJFK",
            "EDDF",
            "2026-06-05 01:00",
            "2026-06-05 09:00",
            "outside_to_europe",
        ),
    ]
)


def test_far_end_label_counts_an_unknown_ident_as_outside():
    truth = pd.DataFrame(
        {"gt_adep": ["EDDF", "KJFK", "EDDF", "ZZZZ"], "gt_ades": ["EGLL", "EDDF", "KJFK", "KJFK"]}
    )
    got = t.label_far_end(truth, {"EDDF", "EGLL"})["flow"].tolist()
    assert got == ["intra_europe", "outside_to_europe", "europe_to_outside", "neither_in_box"]


def test_the_far_end_of_each_flow_is_the_side_outside():
    truth = pd.DataFrame({"flow": ["outside_to_europe", "europe_to_outside", "intra_europe"]})
    assert t._far_mask(truth, "adep").tolist() == [True, False, False]
    assert t._far_mask(truth, "ades").tolist() == [False, True, False]


def test_callsign_join_ignores_a_track_with_no_callsign():
    fl = _flights([("aaa", None, "KJFK", None, "2026-06-05 02:00", "2026-06-05 03:00")])
    got = t.match_callsign(TRUTH, fl)
    assert got.loc[0, "adep"] is None or pd.isna(got.loc[0, "adep"])


def test_callsign_join_reads_each_end_from_its_own_row():
    # The two-track long-haul flight: the regression this view exists to avoid.
    # Picking the row nearest off-block returned the first piece every time, so
    # ADES scored 0 of 1,120 on real data.
    fl = _flights(
        [
            ("aaa", "DLH1", "KJFK", None, "2026-06-05 01:10", "2026-06-05 03:00"),
            ("aaa", "DLH1", None, "EDDF", "2026-06-05 06:00", "2026-06-05 08:50"),
        ]
    )
    got = t.match_callsign(TRUTH, fl)
    assert got.loc[0, "adep"] == "KJFK"
    assert got.loc[0, "ades"] == "EDDF"


def test_one_record_does_not_combine_two_rows():
    fl = _flights(
        [
            ("aaa", "DLH1", "KJFK", None, "2026-06-05 01:10", "2026-06-05 03:00"),
            ("aaa", "DLH1", None, "EDDF", "2026-06-05 06:00", "2026-06-05 08:50"),
        ]
    )
    got = t.match_one_record(TRUTH, fl)
    assert got.loc[0, "adep"] == "KJFK"
    assert pd.isna(got.loc[0, "ades"])


def test_load_truth_converts_paris_block_times_to_the_utc_day(tmp_path):
    # NM stores Europe/Paris. 00:30 local on the 6th is still the 5th in UTC.
    p = tmp_path / "t.parquet"
    pd.DataFrame(
        {
            "AIRCRAFT_ADDRESS": ["ABC123", "ABC123", "FFF000", None],
            "AIRCRAFT_ID": ["DLH1 ", "DLH2", "DLH3", "DLH4"],
            "ADEP": ["KJFK", "KJFK", "KJFK", "KJFK"],
            "ADES": ["EDDF", "EDDF", "EDDF", "EDDF"],
            "AOBT_3": pd.to_datetime(
                ["2026-06-06 00:30", "2026-06-06 03:00", "2026-06-05 12:00", "2026-06-05 12:00"]
            ).tz_localize("Europe/Paris"),
            "ARVT_3": pd.to_datetime(["2026-06-06 08:00"] * 4).tz_localize("Europe/Paris"),
        }
    ).to_parquet(p)
    got = t.load_truth(p, "2026-06-05", ["abc123"])
    assert got["callsign"].tolist() == ["DLH1"]
    assert got["icao24"].tolist() == ["abc123"]
    assert got["t_off"].tolist() == [pd.Timestamp("2026-06-05 22:30")]


def test_a_split_flight_reads_the_same_way_in_either_direction():
    # one_record's tie-break makes ADES vanish, which is why it is not a side
    # view. The views that are must name both ends whichever way the flight flew.
    out = _truth(
        [
            (
                0,
                "aaa",
                "DLH1",
                "EDDF",
                "KJFK",
                "2026-06-05 01:00",
                "2026-06-05 09:00",
                "europe_to_outside",
            )
        ]
    )
    fl = _flights(
        [
            ("aaa", "DLH1", "EDDF", None, "2026-06-05 01:10", "2026-06-05 03:00"),
            ("aaa", "DLH1", None, "KJFK", "2026-06-05 06:00", "2026-06-05 08:50"),
        ]
    )
    for fn in (t.match_callsign, t.match_overlap):
        assert fn(out, fl).loc[0, "adep"] == "EDDF"
        assert fn(out, fl).loc[0, "ades"] == "KJFK"


def test_a_tie_on_time_does_not_depend_on_row_order():
    rows = [
        ("aaa", "DLH1", "ZZZZ", None, "2026-06-05 01:10", "2026-06-05 03:00"),
        ("aaa", "DLH1", "GGGG", None, "2026-06-05 01:10", "2026-06-05 03:00"),
    ]
    a = t.match_overlap(TRUTH, _flights(rows)).loc[0, "adep"]
    b = t.match_overlap(TRUTH, _flights(rows[::-1])).loc[0, "adep"]
    assert a == b == "GGGG"


def test_pair_table_aligns_a_subset_population_by_flight_key():
    truth = _truth(
        [
            (5, "b", "Y", "KBOS", "EDDF", "2026-06-05", "2026-06-05", "outside_to_europe"),
            (2, "a", "X", "KJFK", "EDDF", "2026-06-05", "2026-06-05", "outside_to_europe"),
        ]
    )
    full = pd.DataFrame({"adep": ["KJFK", "WRNG", "KBOS"], "ades": ["EDDF"] * 3}, index=[2, 3, 5])
    far = t.pair_table(truth, full, full, "callsign")
    far = far[(far["side"] == "adep") & (far["end"] == "far")].iloc[0]
    assert far["n"] == 2 and far["base_right"] == 2


def test_both_ends_counts_a_join_of_two_rows_into_one():
    split = _flights(
        [
            ("aaa", "DLH1", "KJFK", None, "2026-06-05 01:10", "2026-06-05 03:00"),
            ("aaa", "DLH1", None, "EDDF", "2026-06-05 06:00", "2026-06-05 08:50"),
        ]
    )
    joined = _flights(
        [
            ("aaa", "DLH1", "KJFK", "EDDF", "2026-06-05 01:10", "2026-06-05 08:50"),
        ]
    )
    tb = t.both_ends_table(
        TRUTH,
        t.match_one_record(TRUTH, split),
        t.match_one_record(TRUTH, joined),
        t.rows_per_flight(TRUTH, split),
        t.rows_per_flight(TRUTH, joined),
    ).iloc[0]
    assert tb["base_both_right"] == 0 and tb["merged_both_right"] == 1
    assert tb["gained"] == 1 and tb["lost"] == 0
    assert tb["base_one_row"] == 0 and tb["merged_one_row"] == 1
    assert tb["fewer_rows"] == 1


def test_overlap_lets_a_fragment_with_no_callsign_answer():
    fl = _flights(
        [
            ("aaa", None, "KJFK", None, "2026-06-05 01:30", "2026-06-05 01:40"),
            ("aaa", "DLH1", None, "EDDF", "2026-06-05 05:00", "2026-06-05 09:00"),
        ]
    )
    got = t.match_overlap(TRUTH, fl)
    assert got.loc[0, "adep"] == "KJFK"
    assert got.loc[0, "ades"] == "EDDF"


def test_overlap_rejects_a_track_outside_the_window():
    fl = _flights([("aaa", "X", "CYYZ", "CYUL", "2026-06-05 20:00", "2026-06-05 21:00")])
    assert pd.isna(t.match_overlap(TRUTH, fl).loc[0, "adep"])


def test_outcome_separates_right_wrong_and_none():
    got = t.outcome(pd.Series(["A", "B", None]), pd.Series(["A", "A", "A"]))
    assert got.tolist() == ["right", "wrong", "none"]


def test_pair_table_counts_moves_on_the_same_flight():
    truth = _truth(
        [
            (0, "a", "X", "KJFK", "EDDF", "2026-06-05", "2026-06-05", "outside_to_europe"),
            (1, "b", "Y", "KBOS", "EDDF", "2026-06-05", "2026-06-05", "outside_to_europe"),
            (2, "c", "Z", "KORD", "EDDF", "2026-06-05", "2026-06-05", "outside_to_europe"),
            (3, "d", "W", "KIAD", "EDDF", "2026-06-05", "2026-06-05", "outside_to_europe"),
        ]
    )
    base = pd.DataFrame(
        {"adep": [None, "KBOS", "KXXX", "KAAA"], "ades": ["EDDF"] * 4}, index=[0, 1, 2, 3]
    )
    merged = pd.DataFrame(
        {"adep": ["KJFK", None, "KORD", "KBBB"], "ades": ["EDDF"] * 4}, index=[0, 1, 2, 3]
    )
    tb = t.pair_table(truth, base, merged, "callsign")
    far = tb[(tb["side"] == "adep") & (tb["end"] == "far")].iloc[0]
    assert far["n"] == 4
    assert far["none_to_right"] == 1
    assert far["right_to_none"] == 1
    assert far["wrong_to_right"] == 1
    assert far["wrong_to_other_wrong"] == 1
    assert far["base_right"] == 1 and far["merged_right"] == 2
    near = tb[(tb["side"] == "ades") & (tb["end"] == "near")].iloc[0]
    assert near["name_changed"] == 0


def test_adsc_samples_are_counted_inside_the_block_window_only():
    adsc = pd.DataFrame(
        {
            "icao24": ["aaa", "aaa", "bbb"],
            "event_time": pd.to_datetime(
                ["2026-06-05 02:00", "2026-06-05 12:00", "2026-06-05 02:00"]
            ),
        }
    )
    assert t.flights_with_adsc(TRUTH, adsc).tolist() == [1]
