"""Checks on the DAG code that need no Airflow installation.

Airflow is not a dependency of this project (it lives in its own image), so ``ecomm_common`` is
imported here with a tiny stand-in for ``airflow.datasets``. Full DAG parsing is done by the real
Airflow in ``tests/integration/test_airflow.py``.
"""

from __future__ import annotations

import ast
import importlib.util
import json
import logging
import re
import sys
import types
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

DAGS = Path(__file__).resolve().parents[3] / "airflow" / "dags"
DAG_FILES = {
    "raw_to_warehouse_daily": DAGS / "raw_to_warehouse_daily.py",
    "dbt_transform_and_test": DAGS / "dbt_transform_and_test.py",
    "cross_cloud_reconciliation": DAGS / "cross_cloud_reconciliation.py",
    "historical_backfill": DAGS / "historical_backfill.py",
}


@pytest.fixture(scope="module")
def common() -> Any:
    fake_datasets = types.ModuleType("airflow.datasets")
    fake_datasets.Dataset = lambda uri: SimpleNamespace(uri=uri)  # type: ignore[attr-defined]
    saved = {k: sys.modules.get(k) for k in ("airflow", "airflow.datasets")}
    sys.modules["airflow"] = types.ModuleType("airflow")
    sys.modules["airflow.datasets"] = fake_datasets
    spec = importlib.util.spec_from_file_location(
        "ecomm_common_under_test", DAGS / "ecomm_common.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    yield module
    for key, value in saved.items():
        if value is None:
            sys.modules.pop(key, None)
        else:
            sys.modules[key] = value


# --- the command builder ---------------------------------------------------------------------------


def test_cli_runs_the_module_in_the_isolated_venv_with_strict_bash(common: Any) -> None:
    command = common.cli("load", "--only-new", '--run-id "$ECOMM_RUN_ID"')
    assert command.startswith("set -euo pipefail\n")
    assert "ecomm-venv/bin/python -m ecomm.warehouse.cli" in command
    assert "PYTHONPATH=/opt/ecomm/src" in command
    assert "--demo-dir /data/pipeline" in command
    assert '--only-new --run-id "$ECOMM_RUN_ID"' in command  # caller-quoted args pass through


def test_every_tool_maps_to_an_importable_module_path(common: Any) -> None:
    root = Path(__file__).resolve().parents[3] / "src"
    for tool, module in common.MODULES.items():
        assert (root / Path(*module.split("."))).with_suffix(".py").is_file(), (tool, module)
        assert "main" in (root / Path(*module.split("."))).with_suffix(".py").read_text()


def test_paths_with_spaces_are_quoted(common: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(common, "DATA_DIR", "/data/my pipeline; rm -rf /")
    command = common.cli("report")
    assert "'/data/my pipeline; rm -rf /'" in command


# --- failure notification --------------------------------------------------------------------------------


def failure_context(task_id: str = "dbt_build", exception: object = None) -> dict[str, Any]:
    ti = SimpleNamespace(
        task_id=task_id,
        dag_id="dbt_transform_and_test",
        run_id="manual__2026-11-10T00:00:00+00:00",
        try_number=3,
        max_tries=2,
        log_url="http://localhost:8081/log?dag_id=x",
    )
    run = SimpleNamespace(dag_id="dbt_transform_and_test", run_id=ti.run_id)
    return {"task_instance": ti, "dag_run": run, "exception": exception}


def test_failure_record_carries_run_id_log_url_and_guidance(common: Any) -> None:
    record = common.build_failure_record(failure_context(exception=RuntimeError("boom")))
    assert record["dag_id"] == "dbt_transform_and_test" and record["task_id"] == "dbt_build"
    assert record["run_id"].startswith("manual__") and record["log_url"].startswith("http")
    assert record["error"] == "boom" and "dbt-runs" in record["hint"]


def test_unknown_tasks_get_the_default_hint_and_long_errors_are_truncated(common: Any) -> None:
    record = common.build_failure_record(failure_context("something_new", ValueError("x" * 5000)))
    assert record["hint"] == common.DEFAULT_HINT and len(record["error"]) == 500


def test_every_dag_task_has_a_specific_failure_hint(common: Any) -> None:
    task_ids = set()
    for path in DAG_FILES.values():
        task_ids |= set(re.findall(r'task_id="(\w+)"', path.read_text(encoding="utf-8")))
    assert task_ids <= set(common.FAILURE_HINTS), task_ids - set(common.FAILURE_HINTS)


def test_notify_failure_logs_one_json_line(common: Any, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.ERROR, logger="airflow.task"):
        common.notify_failure(failure_context("load_new_batches", OSError("disk full")))
    [message] = [r.getMessage() for r in caplog.records]
    assert json.loads(message)["task_id"] == "load_new_batches"


def test_a_callback_for_a_context_without_task_info_does_not_raise(common: Any) -> None:
    assert common.build_failure_record({})["task_id"] is None


# --- DAG-wide policy ---------------------------------------------------------------------------------------


def test_default_retries_are_bounded_and_timeouts_are_set(common: Any) -> None:
    args = common.DEFAULT_ARGS
    assert 1 <= args["retries"] <= 3
    assert args["retry_exponential_backoff"] is True
    assert (
        timedelta(seconds=30)
        <= args["retry_delay"]
        <= args["max_retry_delay"]
        <= timedelta(minutes=30)
    )
    assert timedelta(minutes=1) <= args["execution_timeout"] <= timedelta(hours=2)
    assert args["on_failure_callback"] is common.notify_failure and args["depends_on_past"] is False


@pytest.mark.parametrize("dag_id", sorted(DAG_FILES))
def test_each_dag_file_declares_the_required_safety_settings(dag_id: str) -> None:
    text = DAG_FILES[dag_id].read_text(encoding="utf-8")
    assert f'dag_id="{dag_id}"' in text
    for required in (
        "catchup=False",
        "max_active_runs=1",
        "max_active_tasks=2",
        "dagrun_timeout=",
        "default_args=DEFAULT_ARGS",
        "on_failure_callback=notify_failure",
        "doc_md=__doc__",
        "execution_timeout=",
    ):
        assert required in text, f"{dag_id} lacks {required}"


def test_the_four_required_dags_exist_and_no_others_are_missing() -> None:
    files = {p.stem for p in DAGS.glob("*.py")} - {"ecomm_common"}
    assert files == set(DAG_FILES)


def test_every_task_that_opens_the_warehouse_takes_the_single_writer_pool() -> None:
    needs_pool = {"load_new_batches", "dbt_build", "quality_checks", "build_dashboard",
                  "export_metrics", "load_range"}  # fmt: skip
    for dag_id, path in DAG_FILES.items():
        text = path.read_text(encoding="utf-8")
        for task_id in re.findall(r'task_id="(\w+)"', text):
            block = text[text.index(f'task_id="{task_id}"') :]
            block = block[: block.find(")\n\n") if ")\n\n" in block else len(block)]
            if task_id in needs_pool:
                assert "pool=WAREHOUSE_POOL" in block, f"{dag_id}.{task_id} must use the pool"


def test_the_daily_load_publishes_the_dataset_that_triggers_dbt() -> None:
    load = DAG_FILES["raw_to_warehouse_daily"].read_text(encoding="utf-8")
    dbt = DAG_FILES["dbt_transform_and_test"].read_text(encoding="utf-8")
    assert "outlets=[RAW_EVENTS_DATASET]" in load and "schedule=[RAW_EVENTS_DATASET]" in dbt


def test_no_secrets_or_credentials_in_dag_source() -> None:
    forbidden = re.compile(
        r"(password|passwd|secret|token|api[_-]?key|private[_-]?key|AKIA[0-9A-Z]{16})", re.I
    )
    for path in (*DAG_FILES.values(), DAGS / "ecomm_common.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):  # documentation may talk about secrets; code may not hold any
            if isinstance(node, ast.Module | ast.FunctionDef | ast.ClassDef) and ast.get_docstring(
                node
            ):
                node.body = node.body[1:]
        code = ast.unparse(tree)
        match = forbidden.search(code)
        assert not match, f"{path.name}: code mentions {match.group(0)!r}"


def test_backfill_values_reach_the_shell_only_through_quoted_environment_variables() -> None:
    text = DAG_FILES["historical_backfill"].read_text(encoding="utf-8")
    assert '"$BACKFILL_START"' in text and '"$BACKFILL_END"' in text
    assert "params.start_date }}" in text and "params.end_date }}" in text
    # the templated values appear only in the env mapping, never inside a command string
    command_lines = [ln for ln in text.splitlines() if "cli(" in ln or ln.strip().startswith("'--")]
    assert not any(
        "params." in ln for ln in command_lines if "env" not in ln and "only_new" not in ln
    )


# --- the backfill's range check (extracted from the DAG file without importing Airflow) -----------------------


def load_check_range() -> Any:
    tree = ast.parse(DAG_FILES["historical_backfill"].read_text(encoding="utf-8"))
    wanted = [
        node
        for node in tree.body
        if (isinstance(node, ast.FunctionDef) and node.name == "check_range")
        or (isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == "MAX_RANGE_DAYS")
    ]
    namespace: dict[str, Any] = {"date": date}
    exec(compile(ast.Module(body=wanted, type_ignores=[]), "check_range", "exec"), namespace)  # noqa: S102
    return namespace["check_range"], namespace["MAX_RANGE_DAYS"]


def test_range_check_accepts_valid_ranges_inclusive() -> None:
    check_range, _ = load_check_range()
    assert check_range("2026-11-01", "2026-11-03") == (date(2026, 11, 1), date(2026, 11, 3))
    assert check_range("2026-11-01", "2026-11-01")[0] == check_range("2026-11-01", "2026-11-01")[1]


@pytest.mark.parametrize(
    ("start", "end", "message"),
    [
        ("2026-11-05", "2026-11-01", "before start_date"),
        ("2026-13-01", "2026-11-01", "YYYY-MM-DD"),
        ("yesterday", "today", "YYYY-MM-DD"),
        ("2026-11-01; rm -rf /", "2026-11-02", "YYYY-MM-DD"),
        ("2020-01-01", "2026-01-01", "guard"),
    ],
)
def test_range_check_rejects_bad_input_with_a_clear_message(
    start: str, end: str, message: str
) -> None:
    check_range, _ = load_check_range()
    with pytest.raises(ValueError, match=message):
        check_range(start, end)
