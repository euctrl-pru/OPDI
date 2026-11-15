"""The scoring arithmetic of ``benchmarks/corrections_scorecard.py`` (pandas only, no Spark)."""

import math
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "benchmarks"))

import corrections_scorecard as cs  # noqa: E402

# ---------------------------------------------------------------- runways and stands


@pytest.mark.parametrize(
    "text, parts",
    [
        ("9L", ("09", "L")),
        ("09L", ("09", "L")),
        (" 27r ", ("27", "R")),
        ("36", ("36", "")),
        ("05C", ("05", "C")),
        ("H1", None),
        ("37", None),
        ("", None),
        (None, None),
        (float("nan"), None),
    ],
)
def test_runway_parts(text, parts):
    assert cs.runway_parts(text) == parts


@pytest.mark.parametrize(
    "rwy, other",
    [
        ("09L", "27R"),
        ("27R", "09L"),
        ("18", "36"),
        ("36", "18"),
        ("01", "19"),
        ("05C", "23C"),
        ("3", "21"),
    ],
)
def test_reciprocal_runway(rwy, other):
    assert cs.reciprocal_runway(rwy) == other


@pytest.mark.parametrize(
    "named, record, verdict",
    [
        ("09L", "09L", "exact"),
        ("9L", "09L", "exact"),
        ("27R", "09L", "reciprocal"),
        ("27L", "09L", "other"),
        ("09R", "09L", "other"),
        (None, "09L", "none named"),
        ("09L", None, "no record runway"),
        ("09L", "", "no record runway"),
    ],
)
def test_runway_agreement(named, record, verdict):
    assert cs.runway_agreement(named, record) == verdict


@pytest.mark.parametrize(
    "named, record, verdict",
    [
        ("05", "5", "equal"),
        ("b17", "B17", "equal"),
        ("B 17", "B17", "equal"),
        ("B18", "B17", "differs"),
        ("None", "B17", "none named"),
        ("nan", "B17", "none named"),
        (None, "B17", "none named"),
        ("B17", None, "no record stand"),
        ("0", "00", "equal"),
    ],
)
def test_stand_agreement(named, record, verdict):
    assert cs.stand_agreement(named, record) == verdict


def test_angle_diff():
    assert cs.angle_diff(10, 350) == pytest.approx(20)
    assert cs.angle_diff(90, 270) == pytest.approx(180)
    assert cs.angle_diff(271, 90) == pytest.approx(179)
    assert cs.angle_diff(45, 45) == 0


# ---------------------------------------------------------------- section i


def _rows(rows, item):
    return {r["measure"]: (r["value"], r["count"]) for r in rows if r["item"] == item}


def test_record_lines_shares_and_differences():
    matched = pd.DataFrame(
        {
            "item": ["ATOT"] * 5,
            "diff_s": [-10.0, 20.0, 45.0, 200.0, 4000.0],
            "subminute": [True, False, True, True, False],
        }
    )
    rows = cs.record_lines(matched, {"ATOT": 8, "AOBT": 2}, {"ATOT": 10, "AOBT": 4}, {"ATOT": 5})
    r = _rows(rows, "ATOT")
    assert r["events"] == (8, 8)
    assert r["records"] == (10, 10)
    assert r["events with a record"] == (5, 8)
    assert r["share of records found"] == (0.5, 10)
    assert r["share within 30 s"] == (0.4, 5)
    assert r["share within 60 s"] == (0.6, 5)
    assert r["share within 300 s"] == (0.8, 5)
    # with seconds: -10, 45, 200 -> two within 60 s of three
    assert r["share within 60 s (records with seconds)"][0] == pytest.approx(2 / 3)
    assert r["median |difference| s"] == (45.0, 5)
    assert r["mean difference s (within 300 s; + = event later)"] == (pytest.approx(63.75), 4)
    a = _rows(rows, "AOBT")
    assert a["events with a record"] == (0, 2)
    assert a["share within 60 s"] == (None, 0)
    assert a["median |difference| s"] == (None, 0)


def test_category_lines_share_per_item():
    f = pd.DataFrame(
        {"item": ["a", "a", "a", "b"], "cat": ["exact", "reciprocal", "exact", "other"]}
    )
    rows = cs.category_lines("i", f, "item", "cat", ("exact", "reciprocal", "other"), "runway ")
    r = _rows(rows, "a")
    assert r["runway exact"] == (pytest.approx(2 / 3), 3)
    assert r["runway reciprocal"] == (pytest.approx(1 / 3), 3)
    assert r["runway other"] == (0.0, 3)
    assert _rows(rows, "b")["runway other"] == (1.0, 1)


# ---------------------------------------------------------------- section ii


def test_nm_lines():
    fl = pd.DataFrame(
        {
            "adep": ["EGLL", "LFPG", None, "EDDF"],
            "ades": ["LFPG", "LFPG", "EDDM", None],
            "adep_source": ["aerodrome", "runway_event", None, "aerodrome"],
            "ades_source": ["aerodrome", "runway_event", "aerodrome", None],
            "nm_adep": ["EGLL", "LFPB", "EDDM", "EDDF"],
            "nm_ades": ["LFPG", "LFPG", "EDDM", "EDDK"],
            "typecode": ["A320", "B738", None, "A320"],
            "nm_type": ["A320", "B38M", "A321", None],
            "distance_nm": [200.0, 400.0, None, 100.0],
            "route_nm": [200.0, 200.0, 300.0, 400.0],
        }
    )
    rows = cs.nm_lines(fl)
    r = _rows(rows, "ADEP source all")
    assert r["share right"] == (0.5, 4)
    assert r["share wrong"] == (0.25, 4)
    assert r["share none"] == (0.25, 4)
    assert _rows(rows, "ADEP source runway_event")["share wrong"] == (1.0, 1)
    t = _rows(rows, "aircraft type")
    assert t["share agrees"] == (pytest.approx(1 / 3), 3)
    assert t["share differs"] == (pytest.approx(1 / 3), 3)
    assert t["share missing"] == (pytest.approx(1 / 3), 3)
    d = _rows(rows, "distance flown / route length")
    assert d["p50"] == (1.0, 3)
    assert d["share above 1.5"] == (pytest.approx(1 / 3), 3)
    assert d["share below 0.5"] == (pytest.approx(1 / 3), 3)


# ---------------------------------------------------------------- section iii and the files


def test_counter_rows_value_is_n_count_is_base():
    rows = cs.counter_rows(
        [("no_pos", "ATOT (acdm)", 3, 30), ("no_pos", "ALDT (acdm)", 0, 0)],
        {"no_pos": "a2.C6"},
        {"no_pos": "event without a position"},
    )
    assert rows == [cs.row("iii", "ATOT (acdm)", "event without a position", 3, 30, "a2.C6")]
    assert cs.value_share(rows[0]) == pytest.approx(0.1)
    assert cs.value_share(cs.row("i", "x", "share within 60 s", 0.4, 10)) == 0.4


def test_scorecard_round_trip_and_markdown(tmp_path):
    rows = [
        cs.row("i", "ATOT", "share within 60 s", 0.4321, 100, "a1"),
        cs.row("iii", "AOBT", "block time in the air", 5, 50, "b1.2"),
        cs.row("v", "step 04", "seconds", 612.0, ""),
    ]
    p = tmp_path / "scorecard_x.csv"
    cs.write_scorecard(p, rows)
    back = cs.read_scorecard(p)
    assert [r["measure"] for r in back] == [r["measure"] for r in rows]
    assert back[0]["value"] == "0.4321" and back[1]["count"] == "50"
    md = cs.scorecard_markdown(
        "x", back, {"scope": "sample", "steps": ["04", "04b"], "commit": "abc"}
    )
    assert "## i. against airport records" in md
    assert "| AOBT | block time in the air | 5 | 50 | 10.0% | b1.2 |" in md
    assert "43.2%" in md
    assert "## v. time" in md


def test_compare_rows_changes_and_missing_lines():
    before = [
        {
            "section": "i",
            "item": "ATOT",
            "measure": "share within 60 s",
            "value": "0.5",
            "count": "10",
            "finding": "",
        },
        {
            "section": "iii",
            "item": "AOBT",
            "measure": "odd",
            "value": "4",
            "count": "40",
            "finding": "b1",
        },
        {
            "section": "iii",
            "item": "AIBT",
            "measure": "odd",
            "value": "2",
            "count": "20",
            "finding": "b1",
        },
    ]
    after = [
        {
            "section": "i",
            "item": "ATOT",
            "measure": "share within 60 s",
            "value": "0.6",
            "count": "10",
            "finding": "",
        },
        {
            "section": "iii",
            "item": "AOBT",
            "measure": "odd",
            "value": "1",
            "count": "50",
            "finding": "b1",
        },
        {
            "section": "iii",
            "item": "go-around",
            "measure": "odd",
            "value": "3",
            "count": "30",
            "finding": "a3",
        },
    ]
    rows = {(r["item"], r["measure"]): r for r in cs.compare_rows(before, after)}
    assert rows[("ATOT", "share within 60 s")]["change"] == pytest.approx(0.1)
    aobt = rows[("AOBT", "odd")]
    assert aobt["change"] == -3 and aobt["before_share"] == pytest.approx(0.1)
    assert aobt["after_share"] == pytest.approx(0.02)
    # a counter missing on one side counts as 0 there
    assert rows[("AIBT", "odd")]["after"] == 0.0 and rows[("AIBT", "odd")]["change"] == -2
    assert rows[("go-around", "odd")]["before"] == 0.0 and rows[("go-around", "odd")]["change"] == 3


# ---------------------------------------------------------------- compare: lost events and verdict


def _confirmed():
    return pd.DataFrame(
        {
            "flight_id": ["a", "b", "c", "d"],
            "type": ["ATOT", "ATOT", "AOBT", "ALDT"],
            "event_id": ["1", "2", "3", "4"],
            "event_time": [1000.0, 2000.0, 3000.0, 4000.0],
            "record_s": [1010.0, 2000.0, 3030.0, 4000.0],
            "diff_s": [-10.0, 0.0, -30.0, 0.0],
            "info": ["", "", "", ""],
        }
    )


def test_lost_confirmed_kept_moved_lost():
    after = pd.DataFrame(
        {
            "flight_id": ["a", "b", "c", "c"],
            "type": ["ATOT", "ATOT", "AOBT", "AOBT"],
            "event_time": [1000.4, 2050.0, 3500.0, 9000.0],
            "event_id": ["x", "y", "z", "w"],
        }
    )
    out = cs.lost_confirmed(_confirmed(), after)
    fate = dict(zip(out["flight_id"], out["fate"]))
    assert "a" not in fate  # same time within 1 s
    assert fate["b"] == "moved (still within 60 s)"  # 50 s from its record
    assert fate["c"] == "lost"  # nearest is 470 s from the record
    assert fate["d"] == "lost"  # type gone for the flight
    assert len(out) == 3


def test_lost_confirmed_empty():
    out = cs.lost_confirmed(
        _confirmed().iloc[0:0], pd.DataFrame(columns=["flight_id", "type", "event_time"])
    )
    assert len(out) == 0 and "fate" in out.columns


def test_verdict():
    assert cs.verdict(0, 0, 600, 620) == (0, [])
    code, why = cs.verdict(2, 1, 600, 600)
    assert code == 1 and "2 confirmed event(s) lost (allowed 1)" in why[0]
    assert cs.verdict(2, 2, 600, 600)[0] == 0
    code, why = cs.verdict(0, 0, 600, 631)
    assert code == 1 and "+5.2%" in why[0]
    assert cs.verdict(0, 0, None, 900)[0] == 0


def test_agreement_rows():
    s = [
        {"section": "iii", "item": "AOBT", "measure": "odd", "value": "10", "count": "100"},
        {"section": "iii", "item": "AIBT", "measure": "odd", "value": "30", "count": "100"},
        {"section": "iii", "item": "rare", "measure": "odd", "value": "1", "count": "10"},
        {
            "section": "i",
            "item": "ATOT",
            "measure": "share within 60 s",
            "value": "0.5",
            "count": "100",
        },
        {
            "section": "i",
            "item": "ATOT",
            "measure": "median |difference| s",
            "value": "20",
            "count": "100",
        },
        {"section": "iv", "item": "ATOT", "measure": "events", "value": "100", "count": "100"},
    ]
    d = [
        {"section": "iii", "item": "AOBT", "measure": "odd", "value": "110", "count": "1000"},
        {"section": "iii", "item": "AIBT", "measure": "odd", "value": "100", "count": "1000"},
        {"section": "iii", "item": "rare", "measure": "odd", "value": "50", "count": "1000"},
        {
            "section": "i",
            "item": "ATOT",
            "measure": "share within 60 s",
            "value": "0.51",
            "count": "1000",
        },
        {
            "section": "i",
            "item": "ATOT",
            "measure": "median |difference| s",
            "value": "20",
            "count": "1000",
        },
        {"section": "iv", "item": "ATOT", "measure": "events", "value": "1000", "count": "1000"},
    ]
    v = {(r["item"], r["measure"]): r["verdict"] for r in cs.agreement_rows(s, d)}
    assert v[("AOBT", "odd")] == "agrees"
    assert v[("AIBT", "odd")] == "differs"
    assert v[("rare", "odd")] == "too few on the sample"
    assert v[("ATOT", "share within 60 s")] == "agrees"
    assert ("ATOT", "median |difference| s") not in v  # not a share
    assert ("ATOT", "events") not in v  # section iv is not compared


# ---------------------------------------------------------------- runs, paths, timings


def test_write_paths():
    cs.check_write_path(f"{cs.PREFIX}/runs/base/opdi_flight_events")
    cs.check_write_path(f"{cs.PREFIX}/dsub/osn_tracks_clean")
    cs.check_write_path(f"{cs.PREFIX}/d0614_03/opdi_flight_list")
    for bad in (
        f"{cs.PREFIX}/d0614/opdi_flight_events",
        f"{cs.PREFIX}/runs/",
        "s3a://eurocontrol/opdi-world/x/",
    ):
        with pytest.raises(AssertionError):
            cs.check_write_path(bad)
    with pytest.raises(SystemExit):
        cs.run_root("../d0614")


def test_scope_sources():
    s = cs.scope_sources("sample", ["04", "04b"])
    assert s == {
        "osn_tracks_clean": "dsub",
        "opdi_endpoint_candidates": "dsub",
        "opdi_flight_list": "dsub",
    }
    d = cs.scope_sources("day", ["04", "04b"])
    assert d["osn_tracks_clean"] == "d0614" and d["opdi_flight_list"] == "d0614_03"
    assert cs.scope_sources("day", ["03", "04", "04b"]) == {"osn_tracks_clean": "d0614"}


def test_step_rows(tmp_path):
    (tmp_path / "step_timings.csv").write_text(
        "label,step,seconds,rows_out\nruns/x,04,600,10\nruns/x,04b,40,5\nruns/y,04,1,1\n"
    )
    (tmp_path / "stage_timings.csv").write_text(
        "label,step,executor_run_s\nruns/x,04,100.5\nruns/x,04,50\nruns/x,04b,7\nruns/y,04,9\n"
    )
    s = cs.step_rows(tmp_path, "runs/x")
    assert s["04"] == {"seconds": 600.0, "rows_out": "10", "executor_s": 150.5, "stages": 2}
    assert s["04b"]["executor_s"] == 7.0
    assert cs.events_seconds({"steps_timed": s}) == 600.0
    rows = cs.section_v({"steps_timed": s, "seconds": 700})
    assert cs.row("v", "step 04", "executor-seconds", 150.5, "") in rows


def test_example_track_ids(tmp_path):
    (tmp_path / "a").mkdir()
    pd.DataFrame({"flight_id": ["t1", "t2", None]}).to_parquet(tmp_path / "a" / "x.parquet")
    pd.DataFrame({"flight_id": ["t2", "t3"], "v": [1, 2]}).to_parquet(tmp_path / "a" / "y.parquet")
    pd.DataFrame({"other": [1]}).to_parquet(tmp_path / "a" / "z.parquet")
    assert cs.example_track_ids(tmp_path) == ["t1", "t2", "t3"]


# ---------------------------------------------------------------- the lock


def test_lock_taken_once_per_process_tree(tmp_path):
    lock = tmp_path / "spark.lock"
    with cs.spark_lock("outer", lock):
        assert os.getpid() in cs._lock_holders(lock)
        with cs.spark_lock("inner", lock):  # same process: does not wait for itself
            pass
    assert cs._lock_holders(lock) == []


def test_lock_waits_for_another_process(tmp_path, capsys):
    lock = tmp_path / "spark.lock"
    holder = subprocess.Popen(["flock", str(lock), "sleep", "6"])
    try:
        for _ in range(50):
            if cs._lock_holders(lock):
                break
            import time

            time.sleep(0.1)
        with cs.spark_lock("test", lock, wait_s=60):
            pass
        assert "waiting for the Spark lock" in capsys.readouterr().out
    finally:
        holder.wait()


def test_share():
    assert cs.share(1, 4) == 0.25
    assert cs.share(1, 0) is None
    assert math.isclose(cs.share(2, 3), 2 / 3)
