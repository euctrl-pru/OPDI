"""A small stand-in for Airflow 2.7.2, enough to import the DAG file.

Airflow is not a dependency of OPDI and is not installed for the tests. This
module puts fake ``airflow`` modules into ``sys.modules`` that record what
the DAG file builds: the DAGs, their tasks, the order of the tasks and every
argument. The task commands can then be rendered with Jinja the way Airflow
renders them (see :func:`render`).
"""

import importlib.util
import sys
import types
from datetime import date, datetime, timedelta

import jinja2


class Variable:
    values: dict = {}

    @classmethod
    def get(cls, name, default_var=None):
        return cls.values.get(name, default_var)


class Param:
    def __init__(self, default=None, **kwargs):
        self.default = default
        self.kwargs = kwargs


class _Output:
    def __init__(self, task):
        self.task = task


class BaseOperator:
    def __init__(self, task_id, **kwargs):
        self.task_id = task_id
        self.kwargs = kwargs
        self.downstream = []
        self.upstream = []
        self.dag = DAG.current
        self.dag.tasks[task_id] = self
        args = dict(self.dag.kwargs.get("default_args") or {})
        args.update(kwargs)
        self.args = args

    def __rshift__(self, other):
        self.downstream.append(other)
        other.upstream.append(self)
        return other

    @property
    def output(self):
        return _Output(self)


class BashOperator(BaseOperator):
    @classmethod
    def partial(cls, **kwargs):
        return _Partial(cls, kwargs)


class PythonOperator(BaseOperator):
    pass


class EmptyOperator(BaseOperator):
    pass


class _Partial:
    def __init__(self, cls, kwargs):
        self.cls, self.kwargs = cls, kwargs

    def expand(self, **mapped):
        task = self.cls(**self.kwargs)
        task.mapped = mapped
        return task


class DAG:
    current = None
    all: dict = {}

    def __init__(self, dag_id, **kwargs):
        self.dag_id = dag_id
        self.kwargs = kwargs
        self.tasks: dict = {}
        DAG.all[dag_id] = self

    def __enter__(self):
        DAG.current = self
        return self

    def __exit__(self, *exc):
        DAG.current = None

    def order(self):
        """Task ids in the order they run (the tasks form one chain)."""
        first = [t for t in self.tasks.values() if not t.upstream]
        assert len(first) == 1, [t.task_id for t in first]
        out, task = [], first[0]
        while task is not None:
            out.append(task.task_id)
            assert len(task.downstream) <= 1
            task = task.downstream[0] if task.downstream else None
        return out


def load_dag_file(path):
    """Import the DAG file with the fake Airflow; returns (module, DAG.all)."""
    DAG.all = {}
    modules = {
        "airflow": types.ModuleType("airflow"),
        "airflow.models": types.ModuleType("airflow.models"),
        "airflow.models.param": types.ModuleType("airflow.models.param"),
        "airflow.operators": types.ModuleType("airflow.operators"),
        "airflow.operators.bash": types.ModuleType("airflow.operators.bash"),
        "airflow.operators.python": types.ModuleType("airflow.operators.python"),
        "airflow.operators.empty": types.ModuleType("airflow.operators.empty"),
    }
    modules["airflow"].DAG = DAG
    modules["airflow.models"].Variable = Variable
    modules["airflow.models.param"].Param = Param
    modules["airflow.operators.bash"].BashOperator = BashOperator
    modules["airflow.operators.python"].PythonOperator = PythonOperator
    modules["airflow.operators.empty"].EmptyOperator = EmptyOperator
    saved = {name: sys.modules.get(name) for name in modules}
    sys.modules.update(modules)
    try:
        spec = importlib.util.spec_from_file_location("opdi_dag_under_test", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        for name, old in saved.items():
            if old is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = old
    return module, dict(DAG.all)


class _VarValue:
    def __init__(self, values):
        self.values = values

    def get(self, name, default=None):
        return self.values.get(name, default)


def _ds_add(ds, days):
    return (date.fromisoformat(ds) + timedelta(days=days)).isoformat()


def render(template, variables=None, params=None, data_interval_end=None):
    """``template`` rendered as Airflow renders a templated field.

    Airflow renders with a sandboxed Jinja environment whose context holds
    ``var`` (the Variables), ``params``, ``macros`` and the dates of the run.
    """
    env = jinja2.sandbox.SandboxedEnvironment(undefined=jinja2.StrictUndefined)
    context = {
        "var": types.SimpleNamespace(value=_VarValue(dict(variables or {}))),
        "params": dict(params or {}),
        "macros": types.SimpleNamespace(ds_add=_ds_add),
        "data_interval_end": data_interval_end or datetime(2026, 11, 11, 6, 0),
    }
    return env.from_string(template).render(**context)
