"""Reload a date range of history, then rebuild the models and re-check quality (manual trigger).

Trigger with a date range (the UI shows a form; or from the CLI)::

    airflow dags trigger historical_backfill --conf '{"start_date": "2026-11-01", "end_date": "2026-11-03"}'

Parameters
----------
- ``start_date`` / ``end_date``: inclusive ``YYYY-MM-DD`` (validated three times: JSON schema in the
  trigger form, the ``validate_range`` task, and the loader's own argument parser).
- ``only_new``: skip batches whose latest load already succeeded (default: re-verify everything,
  which is also safe).

A batch is loaded whole when it overlaps the range, so events just outside the range are loaded too;
that is harmless because loading is idempotent.

Tasks
-----
``validate_range`` -> ``load_range`` (``ecomm-load``) -> ``dbt_build`` (``ecomm-dbt``) ->
``quality_checks`` (``ecomm-quality``).

Late and out-of-order events (spec 2.5) are handled here: events that arrive after their window
was loaded sit in newer committed batches; running a backfill over the window picks them up, and
order state is resolved by ``order_version`` in dbt, never by arrival order.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from ecomm_common import (
    DBT_PROJECT_DIR,
    DEFAULT_ARGS,
    RUN_ENV,
    WAREHOUSE_POOL,
    cli,
    notify_failure,
)

MAX_RANGE_DAYS = 1100  # about three years: a typo guard, not a technical limit
DATE_PATTERN = r"^\d{4}-\d{2}-\d{2}$"

PARAMS = {
    "start_date": Param(
        "2026-11-01",
        type="string",
        format="date",
        pattern=DATE_PATTERN,
        description="First date to reload (inclusive, YYYY-MM-DD).",
    ),
    "end_date": Param(
        "2026-11-03",
        type="string",
        format="date",
        pattern=DATE_PATTERN,
        description="Last date to reload (inclusive, YYYY-MM-DD).",
    ),
    "only_new": Param(
        False,
        type="boolean",
        description="Skip batches whose latest load already succeeded.",
    ),
}

# Values reach the shell through environment variables and are always double-quoted there,
# never interpolated into the command text.
RANGE_ENV = {
    **RUN_ENV,
    "BACKFILL_START": "{{ params.start_date }}",
    "BACKFILL_END": "{{ params.end_date }}",
}

LOAD_ARGS = (
    '--start "$BACKFILL_START"',
    '--end "$BACKFILL_END"',
    "{% if params.only_new %}--only-new{% endif %}",
    '--run-id "$ECOMM_RUN_ID"',
)


def check_range(start_text: str, end_text: str) -> tuple[date, date]:
    """Parse and validate the range. Raises ``ValueError`` with a message for the operator."""
    try:
        start, end = date.fromisoformat(start_text), date.fromisoformat(end_text)
    except ValueError as exc:
        raise ValueError(f"dates must be YYYY-MM-DD ({exc})") from exc
    if end < start:
        raise ValueError(f"end_date {end} is before start_date {start}")
    if (end - start).days > MAX_RANGE_DAYS:
        raise ValueError(
            f"range of {(end - start).days} days exceeds the {MAX_RANGE_DAYS}-day guard"
        )
    return start, end


with DAG(
    dag_id="historical_backfill",
    description="Reload a date range, rebuild the models and re-check data quality.",
    doc_md=__doc__,
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    params=PARAMS,
    max_active_runs=1,
    max_active_tasks=2,
    dagrun_timeout=timedelta(hours=4),
    default_args=DEFAULT_ARGS,
    on_failure_callback=notify_failure,
    tags=["ecomm", "backfill", "manual"],
) as dag:

    @task(task_id="validate_range", retries=0, execution_timeout=timedelta(minutes=2))
    def validate_range(**context: Any) -> dict[str, str]:
        params = context["params"]
        try:
            start, end = check_range(str(params["start_date"]), str(params["end_date"]))
        except ValueError as exc:
            raise AirflowFailException(str(exc)) from exc  # a bad range is never retried
        return {"start": start.isoformat(), "end": end.isoformat()}

    load_range = BashOperator(
        task_id="load_range",
        bash_command=cli("load", *LOAD_ARGS),
        env=RANGE_ENV,
        append_env=True,
        pool=WAREHOUSE_POOL,
        execution_timeout=timedelta(hours=1),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=cli("dbt", f"--project-dir '{DBT_PROJECT_DIR}'", '--run-id "$ECOMM_RUN_ID"'),
        env=RUN_ENV,
        append_env=True,
        pool=WAREHOUSE_POOL,
        execution_timeout=timedelta(minutes=45),
    )

    quality_checks = BashOperator(
        task_id="quality_checks",
        bash_command=cli("quality"),
        pool=WAREHOUSE_POOL,
        execution_timeout=timedelta(minutes=15),
    )

    validate_range() >> load_range >> dbt_build >> quality_checks
