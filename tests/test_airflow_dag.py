"""Tests for the Airflow DAG file.

Airflow is not a dependency of OPDI. When it is installed the DAG file is
loaded with Airflow's own loader; otherwise the file is checked by reading
its source.
"""

import ast
from datetime import date
from pathlib import Path

import pytest

from opdi import orchestration

DAG_FILE = Path(__file__).resolve().parent.parent / "airflow" / "dags" / "opdi_pipeline.py"

ALLOWED_MODULES = {
    "airflow",
    "airflow.operators.bash",
    "airflow.operators.python",
    "airflow.operators.empty",
    "airflow.models",
    "airflow.models.param",
    "__future__",
    "datetime",
    "json",
}
DAILY_TASKS = [
    "reference",
    "check_space",
    "ingest",
    "tracks",
    "clean",
    "flight_list",
    "events",
    "milestones",
    "publish",
    "prune",
]


@pytest.fixture(scope="module")
def tree():
    return ast.parse(DAG_FILE.read_text())


def _constant(tree, name):
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not found")


def _keyword_values(tree, keyword):
    return [
        k.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        for k in node.keywords
        if k.arg == keyword and isinstance(k.value, ast.Constant)
    ]


def test_only_airflow_core_and_the_standard_library_are_imported(tree):
    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert alias.name in ALLOWED_MODULES, alias.name
        elif isinstance(node, ast.ImportFrom):
            assert node.module in ALLOWED_MODULES, node.module
            if node.module.startswith("airflow.operators"):
                for alias in node.names:
                    assert alias.name in {"BashOperator", "PythonOperator", "EmptyOperator"}


def test_opdi_is_never_imported_by_the_dag_file(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            assert not any(a.name.split(".")[0] == "opdi" for a in node.names)
        if isinstance(node, ast.ImportFrom):
            assert (node.module or "").split(".")[0] != "opdi"


def test_the_two_dags(tree):
    assert set(_keyword_values(tree, "dag_id")) == {"opdi_daily", "opdi_backfill"}
    source = DAG_FILE.read_text()
    assert '"0 6 * * *"' in source
    assert "catchup=False" in source and "max_active_runs=1" in source


def test_the_daily_tasks_in_order(tree):
    steps = _constant(tree, "DAY_STEPS")
    assert [step for _, step in steps] == list(orchestration.DAY_STEPS)
    assert _constant(tree, "DAILY_TASK_ORDER") == DAILY_TASKS
    assert [name for name, _ in steps] == DAILY_TASKS[2:-1]


def test_the_backfill_is_mapped_over_days_one_at_a_time(tree):
    source = DAG_FILE.read_text()
    assert ".expand(" in source and ".partial(" in source
    assert "max_active_tis_per_dag=1" in source
    params = _constant(tree, "BACKFILL_PARAMS")
    assert set(params) == {"start_date", "end_date", "order", "warehouse", "keep_internal"}
    assert params["order"] == "newest_first"


def test_variables_have_defaults(tree):
    assert _constant(tree, "VARIABLE_DEFAULTS") == {
        "opdi_command_prefix": "",
        "opdi_warehouse": "",
        "opdi_min_free_gb": "5",
        "opdi_lag_days": "2",
        "opdi_keep_internal": "false",
    }


def test_the_day_planning_matches_the_package(tree):
    """The DAG plans the backfill days itself, without importing opdi."""
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "plan_days")
    namespace = {}
    exec(
        compile(ast.Module(body=[fn], type_ignores=[]), str(DAG_FILE), "exec"),
        {"date": date, "timedelta": __import__("datetime").timedelta},
        namespace,
    )
    plan = namespace["plan_days"]
    for order in ("newest_first", "oldest_first"):
        got = plan("2025-01-01", "2025-01-10", order)
        assert got == [
            d.isoformat() for d in orchestration.days(date(2025, 1, 1), date(2025, 1, 10), order)
        ]


def test_the_dag_loads_in_airflow():
    pytest.importorskip("airflow.models")
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(DAG_FILE.parent), include_examples=False)
    assert not bag.import_errors
    assert set(bag.dag_ids) >= {"opdi_daily", "opdi_backfill"}
    daily = bag.get_dag("opdi_daily")
    order = [t.task_id for t in daily.topological_sort()]
    assert order == DAILY_TASKS
