"""The bookkeeping of the day timing script: stages from the event log, scaling, paths."""

import json
import subprocess
import sys
import textwrap
import traceback
from pathlib import Path

import day_timing as t
import pytest

REPO = Path(__file__).resolve().parents[2]


def _stage(sid, attempt=0, submitted=1_000, completed=11_000, acc=None, scopes=(), failure=None):
    info = {
        "Stage ID": sid,
        "Stage Attempt ID": attempt,
        "Stage Name": "parquet at NativeMethodAccessorImpl.java:0",
        "Number of Tasks": 3,
        "Submission Time": submitted,
        "Completion Time": completed,
        "Accumulables": [{"Name": k, "Value": v} for k, v in (acc or {}).items()],
        "RDD Info": [{"Scope": json.dumps({"id": "1", "name": n})} for n in scopes],
    }
    if failure:
        info["Failure Reason"] = failure
    return info


def _lines(*events):
    return [json.dumps(e) for e in events]


def _submitted(sid, group, site=""):
    return {
        "Event": "SparkListenerStageSubmitted",
        "Stage Info": {"Stage ID": sid},
        "Properties": {"spark.jobGroup.id": group, "callSite.short": site},
    }


def _task(sid, launch, finish, attempt=0, failed=False):
    return {
        "Event": "SparkListenerTaskEnd",
        "Stage ID": sid,
        "Stage Attempt ID": attempt,
        "Task Info": {"Launch Time": launch, "Finish Time": finish, "Failed": failed},
    }


def test_a_stage_row_carries_step_call_site_bytes_and_task_spread():
    acc = {
        "internal.metrics.executorRunTime": 25_000,
        "internal.metrics.input.bytesRead": 7,
        "internal.metrics.shuffle.read.remoteBytesRead": 5,
        "internal.metrics.shuffle.read.localBytesRead": 6,
        "internal.metrics.shuffle.write.bytesWritten": 9,
        "internal.metrics.output.bytesWritten": 4,
        "internal.metrics.memoryBytesSpilled": 2,
        "internal.metrics.diskBytesSpilled": 1,
    }
    lines = _lines(
        _submitted(3, "step-04", "events.py:1935 _etl_flight_events_and_measures"),
        _task(3, 0, 1_000),
        _task(3, 0, 2_000),
        _task(3, 0, 9_000),
        _task(3, 0, 50_000, failed=True),
        {
            "Event": "SparkListenerStageCompleted",
            "Stage Info": _stage(3, acc=acc, scopes=("Window", "Exchange", "Scan parquet x")),
        },
    )
    (row,) = t.stage_rows(lines, "base")
    assert row["step"] == "04"
    assert row["call_site"] == "events.py:1935 _etl_flight_events_and_measures"
    assert row["operators"] == "Scan parquet Window Exchange"
    assert row["seconds"] == 10.0
    assert row["executor_run_s"] == 25
    # The failed task is left out of the spread.
    assert (row["task_max_s"], row["task_median_s"]) == (9.0, 2.0)
    assert row["input_bytes"] == 7
    assert row["shuffle_read_bytes"] == 11
    assert (row["shuffle_write_bytes"], row["output_bytes"]) == (9, 4)
    assert (row["spill_memory_bytes"], row["spill_disk_bytes"]) == (2, 1)
    assert row["failure"] == ""


def test_stages_of_the_row_counts_and_outside_a_step_are_left_out():
    lines = _lines(
        _submitted(1, t.COUNT_GROUP),
        _submitted(2, ""),
        _submitted(4, "step-09"),
        *[{"Event": "SparkListenerStageCompleted", "Stage Info": _stage(s)} for s in (1, 2, 4)],
    )
    assert [r["stage_id"] for r in t.stage_rows(lines, "x")] == [4]


def test_each_attempt_of_a_stage_is_its_own_row_and_a_failure_is_named():
    lines = _lines(
        _submitted(5, "step-02"),
        _task(5, 0, 4_000, attempt=0),
        _task(5, 0, 1_000, attempt=1),
        {
            "Event": "SparkListenerStageCompleted",
            "Stage Info": _stage(5, 0, failure="FetchFailed\nstack"),
        },
        {"Event": "SparkListenerStageCompleted", "Stage Info": _stage(5, 1)},
    )
    rows = t.stage_rows(lines, "x")
    assert [(r["attempt"], r["task_max_s"], r["failure"]) for r in rows] == [
        (0, 4.0, "FetchFailed"),
        (1, 1.0, ""),
    ]


def test_without_a_call_site_the_stage_name_is_kept():
    lines = _lines(
        _submitted(6, "step-03"),
        {"Event": "SparkListenerStageCompleted", "Stage Info": _stage(6)},
    )
    (row,) = t.stage_rows(lines, "x")
    assert row["call_site"] == "parquet at NativeMethodAccessorImpl.java:0"


def test_a_line_through_two_slices_gives_the_fixed_cost_and_the_cost_per_row():
    seconds, fixed, per_row = t.extrapolate([(100, 60), (200, 100)], 1_000)
    assert fixed == pytest.approx(20)
    assert per_row == pytest.approx(0.4)
    assert seconds == pytest.approx(420)


def test_one_slice_scales_in_proportion():
    seconds, fixed, per_row = t.extrapolate([(100, 50)], 1_000)
    assert (seconds, fixed) == (pytest.approx(500), 0.0)


def test_a_negative_fixed_cost_is_refitted_through_zero():
    # Two slices whose line would cross the time axis below zero.
    seconds, fixed, _ = t.extrapolate([(100, 10), (200, 100)], 1_000)
    assert fixed == 0.0
    assert seconds == pytest.approx(1_000 * (100 * 10 + 200 * 100) / (100**2 + 200**2))


def test_scaled_rows_name_their_slices_and_the_factor():
    slices = [
        {
            "label": "s2",
            "step": "02",
            "seconds": "60",
            "rows_in": "100",
            "scope": "first 2 h",
            "executors": "8",
            "cores": "2",
            "settings": "",
        },
        {
            "label": "s4",
            "step": "02",
            "seconds": "100",
            "rows_in": "200",
            "scope": "first 4 h",
            "executors": "8",
            "cores": "2",
            "settings": "",
        },
    ]
    (row,) = t.scaled_rows(slices, {"01": 1, "02": 1_000, "02a": 1}, "scaled")
    assert row["full_day_seconds"] == 420
    assert row["scale_factor"] == 5.0
    assert "s2" in row["scope"] and "s4" in row["scope"]


def test_a_rerun_replaces_its_own_rows_only(tmp_path):
    path = tmp_path / "steps.csv"
    t.replace_rows(path, ("label", "step"), [{"label": "a", "step": "01"}], "a")
    t.replace_rows(path, ("label", "step"), [{"label": "b", "step": "01"}], "b")
    t.replace_rows(path, ("label", "step"), [{"label": "a", "step": "02"}], "a")
    assert path.read_text().splitlines() == ["label,step", "b,01", "a,02"]


def test_tables_resolve_to_the_run_or_to_the_warehouse():
    assert t.table_path("x", "opdi_flight_events", False) == f"{t.PREFIX}/x/opdi_flight_events"
    assert t.table_path("x", "osn_tracks_clean", False) == f"{t.INPUT_WAREHOUSE}/osn_tracks_clean"
    assert t.table_path("x", "osn_tracks_clean", True) == f"{t.PREFIX}/x/osn_tracks_clean"
    assert t.table_path("x", "opdi_h3_airspace_ref", False) == t.FIR_REFERENCE


def test_writes_outside_the_speed_folder_are_refused():
    t.check_write_path(f"{t.PREFIX}/base/opdi_flight_events")
    for bad in (
        f"{t.INPUT_WAREHOUSE}/osn_tracks_clean",
        f"{t.PREFIX}/",
        "s3a://eurocontrol/opdi-world/research/airspace_events/on/x",
    ):
        with pytest.raises(AssertionError):
            t.check_write_path(bad)
    assert t.scratch_key("base") == "opdi-world/research/speed/base/"
    with pytest.raises(AssertionError):
        t.scratch_key("")


def test_the_call_site_is_the_innermost_pipeline_frame():
    stack = traceback.StackSummary.from_list(
        [
            ("/x/benchmarks/day_timing.py", 10, "run", ""),
            ("/x/src/opdi/pipeline/events.py", 1935, "_etl", ""),
            ("/v/lib/pyspark/sql/readwriter.py", 5, "parquet", ""),
            ("/x/benchmarks/day_timing.py", 20, "inner", ""),
        ]
    )
    assert t.python_call_site(stack) == "events.py:1935 _etl"


def test_a_real_event_log_gives_the_step_and_the_python_line(tmp_path):
    """A local Spark run with the event log on: its stages carry the step and this file's line."""
    script = tmp_path / "job.py"
    script.write_text(textwrap.dedent(f"""
            import sys
            sys.path.insert(0, {str(REPO / 'benchmarks')!r})
            sys.path.insert(0, {str(REPO / 'src')!r})
            import day_timing as t
            from pyspark.sql import SparkSession, Window, functions as F
            spark = (SparkSession.builder.master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.eventLog.enabled", "true")
                .config("spark.eventLog.dir", "file://{tmp_path}/log")
                .config("spark.eventLog.compress", "false").getOrCreate())
            t.install_call_sites(spark)
            spark.sparkContext.setJobGroup("step-04", "step 04")
            df = spark.range(1000).withColumn("g", F.col("id") % 3)
            w = Window.partitionBy("g").orderBy("id")
            df.withColumn("x", F.lag("id").over(w)).write.parquet("{tmp_path}/out")
            spark.sparkContext.setJobGroup(t.COUNT_GROUP, "count")
            spark.read.parquet("{tmp_path}/out").count()
            spark.stop()
            """))
    (tmp_path / "log").mkdir()
    subprocess.run([sys.executable, str(script)], check=True, capture_output=True)
    rows = t.stage_rows(t.event_log_lines(tmp_path / "log"), "local")
    assert rows and {r["step"] for r in rows} == {"04"}
    assert all(r["call_site"].startswith("job.py:") for r in rows)
    assert any("Window" in r["operators"] for r in rows)
    assert sum(r["output_bytes"] for r in rows) > 0


def test_call_site_paths_are_cut_to_the_file_name():
    assert t._short_site("first at /home/x/src/opdi/pipeline/flights.py:513") == (
        "first at flights.py:513"
    )
