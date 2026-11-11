"""Airflow DAGs that run the OPDI pipeline day by day.

Two DAGs (directed acyclic graphs: sets of tasks with a fixed order):

* ``opdi_daily`` runs every morning and processes one day, two days back by
  default, one task per pipeline step.
* ``opdi_backfill`` is started by hand and processes a range of days, one day
  at a time, newest first by default.

Every task is a shell command, ``python -m opdi.orchestration ...``, placed
after a configurable prefix (the Airflow Variable ``opdi_command_prefix``).
The prefix says where the OPDI package runs: for example a virtual
environment on the worker, or a ``kubectl exec`` into the machine that runs
the Spark driver. This file therefore imports only Airflow, never OPDI.
See ``airflow/README.md``.

Written for Airflow 2.7.2 and its core operators only.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

#: The Airflow Variables this file reads, with the value used when a Variable
#: is not set.
VARIABLE_DEFAULTS = {
    # Text placed before "python -m opdi.orchestration" in every command.
    "opdi_command_prefix": "",
    # Warehouse to use; empty means the workers' OPDI_WAREHOUSE, else
    # s3a://eurocontrol/opdi.
    "opdi_warehouse": "",
    # Free space a day needs before it starts, in GB.
    "opdi_min_free_gb": "5",
    # How many days the daily run stays behind the run date.
    "opdi_lag_days": "2",
    # "true" keeps the internal tables of each day after publishing.
    "opdi_keep_internal": "false",
    # "false" also removes the working events and measurements of days that
    # are already published, outside a window of 7 days around the day run.
    "opdi_keep_working": "true",
}

#: The pipeline steps of one day: task id and step number, in order.
DAY_STEPS = (
    ("ingest", "01"),
    ("tracks", "02"),
    ("clean", "02a"),
    ("flight_list", "03"),
    ("events", "04"),
    ("milestones", "04b"),
    ("publish", "09"),
)

#: The tasks of the daily DAG, in the order they run.
DAILY_TASK_ORDER = [
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

#: The parameters of the backfill DAG and their defaults. An empty end date
#: means the day the daily run would process today.
BACKFILL_PARAMS = {
    "start_date": "2025-01-01",
    "end_date": "",
    "order": "newest_first",
    "warehouse": "",
    "keep_internal": False,
}

DEFAULT_ARGS = {
    "owner": "opdi",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=6),
}

#: A whole day in the backfill runs every step in one task, so it gets longer.
DAY_TIMEOUT = timedelta(hours=24)

START = datetime(2025, 1, 1, tzinfo=timezone.utc)


def _var(name: str) -> str:
    """Jinja text that reads an Airflow Variable when the task runs."""
    return f"{{{{ var.value.get('{name}', '{VARIABLE_DEFAULTS[name]}') }}}}"


COMMAND = _var("opdi_command_prefix") + " python -m opdi.orchestration"
WAREHOUSE = (
    "{% set wh = var.value.get('opdi_warehouse', '') %}"
    "{% if wh %} --warehouse {{ wh }}{% endif %}"
)
DAILY_DAY = (
    "{{ macros.ds_add(data_interval_end.strftime('%Y-%m-%d'), "
    "-(var.value.get('opdi_lag_days', '2') | int)) }}"
)
KEEP_INTERNAL = "var.value.get('opdi_keep_internal', 'false') | lower == 'true'"
DROP_WORKING = (
    "{% if var.value.get('opdi_keep_working', 'true') | lower == 'false' %}"
    " --drop-published-working{% endif %}"
)


def plan_days(start: str, end: str, order: str = "newest_first") -> list:
    """Every day from ``start`` to ``end`` (YYYY-MM-DD, both included), in order.

    The same rule as ``opdi.orchestration.days``, repeated here so that this
    file needs no OPDI import.
    """
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if last < first:
        raise ValueError(f"end_date {end} is before start_date {start}")
    if order not in ("newest_first", "oldest_first"):
        raise ValueError(f"order must be newest_first or oldest_first, not {order!r}")
    result = [(first + timedelta(days=n)).isoformat() for n in range((last - first).days + 1)]
    return result[::-1] if order == "newest_first" else result


def _variable(name: str) -> str:
    return Variable.get(name, default_var=VARIABLE_DEFAULTS[name])


def _backfill_commands(**context) -> list:
    """One shell command per day of the backfill, in the order asked for."""
    params = context["params"]
    end = (
        params.get("end_date")
        or (
            datetime.now(timezone.utc).date() - timedelta(days=int(_variable("opdi_lag_days")))
        ).isoformat()
    )
    prefix = _variable("opdi_command_prefix")
    warehouse = params.get("warehouse") or _variable("opdi_warehouse")
    keep = bool(params.get("keep_internal")) or _variable("opdi_keep_internal").lower() == "true"
    options = f" --min-free-gb {_variable('opdi_min_free_gb')}"
    if warehouse:
        options += f" --warehouse {warehouse}"
    if keep:
        options += " --keep-internal"
    if _variable("opdi_keep_working").lower() == "false":
        options += " --drop-published-working"
    days = plan_days(params["start_date"], end, params.get("order") or "newest_first")
    print(f"backfill of {len(days)} day(s), {days[0]} first, {days[-1]} last")
    return [f"{prefix} python -m opdi.orchestration day --day {d}{options}" for d in days]


DAILY_DOC = """
### OPDI daily run

Processes one day every morning at 06:00 UTC: the day `opdi_lag_days` days
(2 by default) before the run date. A day can only be processed once the day
after it is complete, because flights that start late in the evening end
after midnight.

Tasks, in order: `reference` builds any missing reference table (it skips
what exists), `check_space` stops the run when the bucket is nearly full,
then one task per pipeline step, then `prune` removes the internal tables
that are no longer needed (unless `opdi_keep_internal` is `true`). With
`opdi_keep_working` set to `false`, `prune` also removes the working events
and measurements of days already published.

Each task records its result in the warehouse. Clearing a failed task and
running it again continues where it stopped. Missed days are not caught up
automatically: start `opdi_backfill` for them.
"""

BACKFILL_DOC = """
### OPDI backfill

Processes a range of days, one day at a time. Start it with "Trigger DAG
w/ config" and set:

* `start_date`, `end_date`: the first and last day (YYYY-MM-DD, both
  included). An empty `end_date` means the day the daily run would process
  today.
* `order`: `newest_first` (default) or `oldest_first`.
* `warehouse`: another warehouse for this run only; empty means the
  `opdi_warehouse` Variable.
* `keep_internal`: keep the internal tables of every day.

To reach back to 2019, trigger it again with `start_date` 2019-01-01 and
`end_date` the day before the earliest day already done. Days already
finished are skipped, so overlapping ranges cost only a quick check.
"""

with DAG(
    dag_id="opdi_daily",
    description="OPDI: process and publish one day, every morning",
    schedule="0 6 * * *",
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    doc_md=DAILY_DOC,
    tags=["opdi"],
) as daily:
    tasks = [
        BashOperator(task_id="reference", bash_command=f"{COMMAND} reference{WAREHOUSE}"),
        BashOperator(
            task_id="check_space",
            bash_command=(
                f"{COMMAND} check-space --min-free-gb {_var('opdi_min_free_gb')}{WAREHOUSE}"
            ),
        ),
    ]
    for name, step in DAY_STEPS:
        tasks.append(
            BashOperator(
                task_id=name,
                bash_command=f"{COMMAND} step --step {step} --day {DAILY_DAY}{WAREHOUSE}",
            )
        )
    tasks.append(
        BashOperator(
            task_id="prune",
            bash_command=(
                "{% if " + KEEP_INTERNAL + " %}echo 'opdi_keep_internal is true: nothing removed'"
                "{% else %}"
                + f"{COMMAND} prune --day {DAILY_DAY}{WAREHOUSE}{DROP_WORKING}"
                + "{% endif %}"
            ),
        )
    )
    for before, after in zip(tasks, tasks[1:]):
        before >> after

with DAG(
    dag_id="opdi_backfill",
    description="OPDI: process and publish a range of days, one day at a time",
    schedule=None,
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    doc_md=BACKFILL_DOC,
    params={
        "start_date": Param(
            BACKFILL_PARAMS["start_date"],
            type="string",
            format="date",
            description="First day, YYYY-MM-DD.",
        ),
        "end_date": Param(
            BACKFILL_PARAMS["end_date"],
            type="string",
            description="Last day, YYYY-MM-DD; empty for the latest day.",
        ),
        "order": Param(BACKFILL_PARAMS["order"], enum=["newest_first", "oldest_first"]),
        "warehouse": Param(
            BACKFILL_PARAMS["warehouse"],
            type="string",
            description="Warehouse for this run; empty for the Variable.",
        ),
        "keep_internal": Param(BACKFILL_PARAMS["keep_internal"], type="boolean"),
    },
    tags=["opdi"],
) as backfill:
    reference = BashOperator(
        task_id="reference",
        bash_command=(
            f"{COMMAND} reference"
            "{% set wh = params.warehouse or var.value.get('opdi_warehouse', '') %}"
            "{% if wh %} --warehouse {{ wh }}{% endif %}"
        ),
    )
    plan = PythonOperator(task_id="plan_days", python_callable=_backfill_commands)
    process_day = BashOperator.partial(
        task_id="process_day",
        max_active_tis_per_dag=1,
        execution_timeout=DAY_TIMEOUT,
    ).expand(bash_command=plan.output)
    done = EmptyOperator(task_id="done")
    reference >> plan >> process_day >> done
