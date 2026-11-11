"""The DAG file imported with a stand-in for Airflow, its commands rendered.

``tests/airflow_stub.py`` replaces Airflow by small classes that record what
the DAG file builds, and renders each templated command with Jinja the way
Airflow does. The tests check the commands a task would run.
"""

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
