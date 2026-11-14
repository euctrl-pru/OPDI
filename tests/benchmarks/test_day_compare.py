"""The day comparison matches events by flight, type and second, and counts what differs."""

from datetime import datetime

import day_compare as c

EV = (
    "event_id string, flight_id string, type string, event_time timestamp, "
    "longitude double, latitude double, altitude_ft double, info string, "
    "distance_flown_nm double, time_passed_s double"
)


def _ev(spark, rows):
    return spark.createDataFrame(rows, EV)


def test_share_is_matched_over_the_larger_count():
    assert c.share(999, 1000, 990) == 0.999
    assert c.share(0, 0, 0) == 1.0


def test_events_match_to_the_second_and_moved_values_are_counted(spark):
    t0 = datetime(2026, 6, 14, 10, 0, 0)
    before = _ev(
        spark,
        [
            ("a", "f1", "airborne", t0, 1.0, 2.0, 100.0, "{}", 10.0, 60.0),
            ("b", "f1", "touchdown", t0.replace(minute=50), 1.0, 2.0, 0.0, "{}", 20.0, 3000.0),
            ("c", "f2", "airborne", t0, 1.0, 2.0, 100.0, "{}", 5.0, 1.0),
        ],
    )
    after = _ev(
        spark,
        [
            # Same second, a few milliseconds later: matched; the distance moved.
            (
                "a2",
                "f1",
                "airborne",
                t0.replace(microsecond=400_000),
                1.0,
                2.0,
                100.0,
                "{}",
                10.5,
                60.0,
            ),
            ("b", "f1", "touchdown", t0.replace(minute=50), 1.0, 2.0, 0.0, "{}", 20.0, 3000.0),
            ("d", "f3", "airborne", t0, 1.0, 2.0, 100.0, "{}", 5.0, 1.0),
        ],
    )
    rows = {(r["part"], r["item"]): r for r in c.compare_events(before, after)}
    air = rows[("events", "airborne")]
    assert (air["before"], air["after"], air["matched"]) == (2, 2, 1)
    assert (air["only_before"], air["only_after"], air["matched_share"]) == (1, 1, 0.5)
    assert "distance_flown_nm differs on 1" in air["note"]
    assert rows[("events", "touchdown")]["matched_share"] == 1.0
    assert rows[("measurement", "distance_flown_nm")]["note"].startswith("1 matched")
    assert rows[("measurement", "time_passed_s")]["note"].startswith("0 matched")


def test_a_repeated_key_matches_as_often_as_it_occurs_in_both(spark):
    t0 = datetime(2026, 6, 14, 10, 0, 0)
    row = ("x", "f1", "xing-fir", t0, 1.0, 2.0, 3.0, "{}", 1.0, 1.0)
    before = _ev(spark, [row, row])
    after = _ev(spark, [row])
    (ev,) = [r for r in c.compare_events(before, after) if r["part"] == "events"]
    assert (ev["before"], ev["after"], ev["matched"], ev["only_before"]) == (2, 1, 1, 1)


def test_flights_match_by_address_and_first_seen_and_airports_are_compared(spark):
    ddl = "icao24 string, first_seen timestamp, adep string, ades string"
    t0 = datetime(2026, 6, 14, 10, 0, 0)
    before = spark.createDataFrame(
        [("a", t0, "EBBR", "LFPG"), ("b", t0, "EDDF", None), ("c", t0, "EGLL", "KJFK")], ddl
    )
    after = spark.createDataFrame(
        [("a", t0, "EBBR", "LFPO"), ("b", t0, "EDDF", None), ("d", t0, "EGLL", "KJFK")], ddl
    )
    (r,) = c.compare_flights(before, after)
    assert (r["before"], r["after"], r["matched"], r["only_before"], r["only_after"]) == (
        3,
        3,
        2,
        1,
        1,
    )
    assert r["note"] == "departure airport differs on 0; destination airport differs on 1"


# -- content comparison ----------------------------------------------------------


TR = "track_id string, event_time timestamp, lat double, callsign string, n bigint"


def _pandas_content(before, after, key):
    """The same comparison in pandas, as the reference for the Spark one."""
    import pandas as pd

    rest = [c for c in before.columns if c not in key]

    def numbered(df):
        df = df.sort_values(key + rest, na_position="first").copy()
        df["_n"] = df.groupby(key).cumcount()
        return df

    m = numbered(before).merge(numbered(after), on=key + ["_n"], how="inner", suffixes=("_b", "_a"))
    diffs = {}
    for col in rest:
        x, y = m[f"{col}_b"], m[f"{col}_a"]
        if pd.api.types.is_float_dtype(x):
            d = (x.isna() != y.isna()) | ((x - y).abs() > 1e-9)
        else:
            d = ~((x == y) | (x.isna() & y.isna()))
        diffs[col] = int(d.sum())
    return {"before": len(before), "after": len(after), "matched": len(m)}, diffs


def test_content_counts_rows_matched_and_the_columns_that_differ(spark):
    import pandas as pd

    t = datetime(2026, 6, 14, 10, 0, 0)
    b_rows = [
        ("a", t, 50.0, "X1", 1),
        ("a", t.replace(second=5), 50.1, "X1", 2),
        ("a", t.replace(second=10), 50.2, "X1", 3),  # only before
        ("b", t, 40.0, None, 4),
        ("b", t, 40.0, None, 4),  # a repeated row: matched as often as in both
    ]
    a_rows = [
        ("a", t, 50.0 + 1e-12, "X1", 1),  # within the tolerance
        ("a", t.replace(second=5), 50.1 + 1e-6, "X2", 2),  # both columns moved
        ("b", t, 40.0, None, 4),
        ("c", t, 30.0, "Y", 9),  # only after
    ]
    before = spark.createDataFrame(b_rows, TR)
    after = spark.createDataFrame(a_rows, TR)
    rows = c.compare_content(before, after, ["track_id", "event_time"], "tracks", "track_id")
    by = {r["item"]: r for r in rows}
    assert (by["rows"]["before"], by["rows"]["after"], by["rows"]["matched"]) == (5, 4, 3)
    assert (by["rows"]["only_before"], by["rows"]["only_after"]) == (2, 1)
    assert by["column lat"]["note"] == "1 matched rows differ"
    assert by["column callsign"]["note"] == "1 matched rows differ"
    assert by["column n"]["note"] == "0 matched rows differ"
    assert by["matched rows with a column differing"]["note"].startswith("1 matched rows")
    ids = by["track ids"]
    assert (ids["before"], ids["after"], ids["matched"], ids["only_after"]) == (2, 3, 2, 1)

    cols = ["track_id", "event_time", "lat", "callsign", "n"]
    counts, diffs = _pandas_content(
        pd.DataFrame(b_rows, columns=cols), pd.DataFrame(a_rows, columns=cols), cols[:2]
    )
    assert counts == {k: by["rows"][k] for k in ("before", "after", "matched")}
    assert diffs == {k: int(by[f"column {k}"]["note"].split()[0]) for k in diffs}


def test_content_rows_arithmetic():
    rows = c.content_rows(
        "x",
        {"before": 10, "after": 8, "matched": 8},
        {"a": 0, "b": 2},
        {"before": 3, "after": 2, "both": 2},
    )
    first, ids, a, b = rows
    assert (first["only_before"], first["only_after"], first["matched_share"]) == (2, 0, 0.8)
    assert first["note"] == "columns differing: b on 2"
    assert (ids["only_before"], ids["matched_share"]) == (1, round(2 / 3, 6))
    assert (a["item"], a["matched_share"], b["matched_share"]) == ("column a", 1.0, 0.75)
    equal = c.content_rows("x", {"before": 1, "after": 1, "matched": 1}, {"a": 0})
    assert equal[0]["note"] == "every other column equal on all matched rows (1 columns)"


def test_without_a_subcommand_the_events_are_compared():
    args = c.parse(["--day", "2026-06-14", "--before", "a", "--after", "b", "--out", "x.csv"])
    assert args.command == "events"
    args = c.parse(["statevectors", "--before", "a", "--after", "b", "--out", "x.csv"])
    assert args.command == "statevectors" and args.day is None


def test_the_manifest_records_the_command_line_and_the_inputs(tmp_path, monkeypatch):
    import json

    import provenance

    monkeypatch.setattr(provenance, "s3_identity", lambda p: {"objects": 1, "bytes": 2})
    out = tmp_path / "content_comparison.csv"
    out.write_text("part\n")
    argv = ["tracks", "--day", "2026-06-14", "--before", "a", "--after", "b", "--out", str(out)]
    inputs = c.inputs_of("tracks", "a", "b", None)
    c.stamp(out, argv, inputs, "a = before, b = after")
    entry = json.loads((tmp_path / "_manifest.json").read_text())["content_comparison.csv"]
    assert entry["argv"] == argv
    assert sorted(entry["input_tables"]) == sorted(inputs)
    assert f"{c.PREFIX}/a/osn_tracks_clean" in entry["input_tables"]
    assert entry["notes"] == "a = before, b = after"


def test_the_rows_with_any_column_differing_are_counted_once():
    rows = c.content_rows(
        "x", {"before": 4, "after": 4, "matched": 4, "differing": 1}, {"a": 1, "b": 1}
    )
    (row,) = [r for r in rows if r["item"] == "matched rows with a column differing"]
    assert row["note"] == "1 matched rows differ in at least one column"
    assert row["matched_share"] == 0.75
