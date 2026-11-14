"""The DAG file imported with a stand-in for Airflow, its commands rendered.

``tests/airflow_stub.py`` replaces Airflow by small classes that record what
the DAG file builds, and renders each templated command with Jinja the way
Airflow does. The tests check the commands a task would run.
"""

import shlex
import types
from pathlib import Path

import airflow_stub
import pytest

DAG_FILE = Path(__file__).resolve().parent.parent / "airflow" / "dags" / "opdi_pipeline.py"


@pytest.fixture(scope="module")
def loaded():
    return airflow_stub.load_dag_file(DAG_FILE)


@pytest.fixture
def dags(loaded):
    return loaded[1]


@pytest.fixture
def module(loaded):
    return loaded[0]


def _run(previous_state):
    previous = None if previous_state is None else types.SimpleNamespace(state=previous_state)
    run = types.SimpleNamespace(
        get_previous_dagrun=lambda: previous, run_id="scheduled__2026-11-11"
    )
    return {"dag_run": run}


def test_the_daily_run_starts_by_checking_the_previous_run(dags, module):
    daily = dags["opdi_daily"]
    assert daily.order()[0] == "previous_run_finished"
    assert daily.order() == module.DAILY_TASK_ORDER
    gate = daily.tasks["previous_run_finished"]
    assert gate.args["retries"] == 0
    check = gate.kwargs["python_callable"]
    check(**_run(None))
    check(**_run("success"))
    with pytest.raises(RuntimeError, match="previous run"):
        check(**_run("failed"))


def test_the_daily_run_never_overlaps_another(dags):
    assert dags["opdi_daily"].kwargs["max_active_runs"] == 1
    assert dags["opdi_daily"].kwargs["catchup"] is False


def test_a_failed_backfill_day_is_not_retried(dags):
    backfill = dags["opdi_backfill"]
    assert backfill.kwargs["max_active_runs"] == 1
    day = backfill.tasks["process_day"]
    assert day.args["retries"] == 0
    assert day.kwargs["max_active_tis_per_dag"] == 1


def test_every_task_runs_in_the_one_slot_opdi_pool(dags):
    for dag in dags.values():
        for task in dag.tasks.values():
            assert task.args.get("pool") == "opdi", (dag.dag_id, task.task_id)


def test_a_backfill_of_more_than_1000_days_is_refused_with_advice(module):
    assert len(module.plan_days("2024-01-01", "2026-09-26")) == 1000
    with pytest.raises(ValueError, match="Split the range by year"):
        module.plan_days("2019-01-01", "2024-12-31")


# --- the commands, rendered ----------------------------------------------------------

VARIABLES = {
    "opdi_command_prefix": "kubectl exec -n eurocontrol driver --",
    "opdi_warehouse": "s3a://eurocontrol/opdi-test",
    "opdi_min_free_gb": "25",
    "opdi_lag_days": "2",
    "opdi_keep_internal": "false",
    "opdi_keep_working": "true",
}


def _render(dag, task_id, variables=VARIABLES, params=None):
    task = dag.tasks[task_id]
    return airflow_stub.render(
        task.kwargs["bash_command"],
        variables=variables,
        params=params,
        filters=dag.kwargs.get("user_defined_filters"),
    )


def test_every_daily_command_rendered(dags):
    daily = dags["opdi_daily"]
    prefix = "kubectl exec -n eurocontrol driver -- python -m opdi.orchestration"
    wh = "--warehouse s3a://eurocontrol/opdi-test"
    expected = {
        "reference": f"{prefix} reference {wh}",
        "check_space": f"{prefix} check-space --min-free-gb 25 {wh}",
        "ingest": f"{prefix} step --step 01 --day 2026-11-09 {wh}",
        "tracks": f"{prefix} step --step 02 --day 2026-11-09 {wh}",
        "clean": f"{prefix} step --step 02a --day 2026-11-09 {wh}",
        "flight_list": f"{prefix} step --step 03 --day 2026-11-09 {wh}",
        "events": f"{prefix} step --step 04 --day 2026-11-09 {wh}",
        "milestones": f"{prefix} step --step 04b --day 2026-11-09 {wh}",
        "publish": f"{prefix} step --step 09 --day 2026-11-09 {wh}",
        "prune": f"{prefix} prune --day 2026-11-09 {wh}",
    }
    for task_id, command in expected.items():
        assert " ".join(_render(daily, task_id).split()) == command, task_id


def test_the_daily_commands_without_variables(dags):
    daily = dags["opdi_daily"]
    assert _render(daily, "publish", variables={}).split() == [
        "python",
        "-m",
        "opdi.orchestration",
        "step",
        "--step",
        "09",
        "--day",
        "2026-11-09",
    ]
    assert "--min-free-gb 25" in _render(daily, "check_space", variables={})


def test_prune_follows_the_two_variables(dags):
    daily = dags["opdi_daily"]
    keep = dict(VARIABLES, opdi_keep_internal="TRUE")
    assert _render(daily, "prune", keep).startswith("echo ")
    drop = dict(VARIABLES, opdi_keep_working="false")
    assert _render(daily, "prune", drop).rstrip().endswith("--drop-published-working")


def test_an_interpolated_value_never_becomes_a_second_command(dags):
    daily, backfill = dags["opdi_daily"], dags["opdi_backfill"]
    bad = dict(VARIABLES, opdi_warehouse="s3a://b/x; echo INJECTED")
    for task_id in ("reference", "check_space", "publish", "prune"):
        words = shlex.split(_render(daily, task_id, bad))
        assert "s3a://b/x; echo INJECTED" in words, task_id
        assert "echo" not in words, task_id
    words = shlex.split(_render(backfill, "reference", bad, params={"warehouse": ""}))
    assert "s3a://b/x; echo INJECTED" in words


def test_the_warehouse_parameter_accepts_only_a_storage_address(dags):
    import re

    param = dags["opdi_backfill"].kwargs["params"]["warehouse"]
    pattern = re.compile(param.kwargs["pattern"])
    for good in ("", "s3a://eurocontrol/opdi-test", "/data/opdi", "file:///data/opdi"):
        assert pattern.fullmatch(good), good
    for bad in ("s3a://b/x; echo INJECTED", "s3a://b/$(id)", "a b", "s3a://b/x`id`"):
        assert not pattern.fullmatch(bad), bad


def test_the_backfill_commands(dags, module, monkeypatch):
    monkeypatch.setattr(airflow_stub.Variable, "values", dict(VARIABLES))
    context = {
        "params": {
            "start_date": "2026-06-13",
            "end_date": "2026-06-14",
            "order": "newest_first",
            "warehouse": "s3a://b/x; echo INJECTED",
            "keep_internal": True,
        }
    }
    commands = module._backfill_commands(**context)
    assert [shlex.split(c) for c in commands] == [
        [
            "kubectl",
            "exec",
            "-n",
            "eurocontrol",
            "driver",
            "--",
            "python",
            "-m",
            "opdi.orchestration",
            "day",
            "--day",
            day,
            "--min-free-gb",
            "25",
            "--warehouse",
            "s3a://b/x; echo INJECTED",
            "--keep-internal",
        ]
        for day in ("2026-06-14", "2026-06-13")
    ]
    rendered = _render(dags["opdi_backfill"], "reference", params={"warehouse": ""})
    assert shlex.split(rendered)[-2:] == ["--warehouse", "s3a://eurocontrol/opdi-test"]


def test_a_lag_under_two_days_is_refused(module, monkeypatch):
    monkeypatch.setattr(airflow_stub.Variable, "values", dict(VARIABLES, opdi_lag_days="1"))
    with pytest.raises(ValueError, match="at least 2"):
        module._previous_run_finished(**_run(None))
    with pytest.raises(ValueError, match="at least 2"):
        module._backfill_commands(params={"start_date": "2026-06-13", "end_date": ""})
