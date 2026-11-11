"""Job configuration and failure reporting (no Spark needed)."""

from __future__ import annotations

from datetime import timedelta

import pytest

from ecomm.spark.job import build_parser, config_from_args, summarize_failure

STACK = """org.apache.spark.sql.streaming.StreamingQueryException: [STREAM_FAILED] Query terminated
\tat org.apache.spark.sql.execution.streaming.StreamExecution.org$apache$spark$sql$execution(...)
Caused by: py4j.Py4JException: An exception was thrown from the Python foreachBatch function
Traceback (most recent call last):
  File "/app/src/ecomm/spark/job.py", line 1, in handle
ecomm.spark.sink.OffsetGapError: batch itest-b00002: 3 partition(s) lost messages between Kafka and the sink
\tat org.apache.spark.api.python.PythonRunner.run(...)
"""


def test_failure_summary_prefers_our_own_error_line() -> None:
    summary = summarize_failure(STACK)
    assert summary.startswith("ecomm.spark.sink.OffsetGapError: batch itest-b00002")
    assert "lost messages between Kafka and the sink" in summary
    assert "\n" not in summary and "at org.apache" not in summary


@pytest.mark.parametrize("name", ["BatchConflictError", "BatchCommitError"])
def test_other_pipeline_errors_are_also_recognised(name: str) -> None:
    assert summarize_failure(f"noise\nmodule.{name}: reason here\nmore").endswith("reason here")


def test_unknown_failures_are_collapsed_and_truncated() -> None:
    text = "line one\n   line two\n" + "x" * 2000
    summary = summarize_failure(text, limit=50)
    assert summary == (" ".join(text.split()))[:50] and len(summary) == 50


def test_defaults_describe_a_safe_local_run() -> None:
    cfg = config_from_args(build_parser().parse_args([]))
    assert cfg.trigger == "available-now" and cfg.fail_on_data_loss is True
    assert cfg.late_threshold == timedelta(minutes=60)
    assert cfg.topics == "commerce.orders,commerce.payments,commerce.inventory,commerce.customers"
    assert cfg.max_offsets_per_trigger == 5000 and cfg.run_id == "spark-main"


def test_topic_prefix_and_options_are_applied() -> None:
    args = build_parser().parse_args(
        [
            "--topic-prefix",
            "shop",
            "--trigger",
            "processing-time:10s",
            "--run-id",
            "r2",
            "--no-fail-on-data-loss",
            "--late-threshold-minutes",
            "5",
        ]  # fmt: skip
    )
    cfg = config_from_args(args)
    assert cfg.topics.startswith("shop.orders,")
    assert cfg.fail_on_data_loss is False and cfg.late_threshold == timedelta(minutes=5)
    assert (cfg.trigger, cfg.run_id) == ("processing-time:10s", "r2")


@pytest.mark.parametrize("bad", [["--trigger", "continuous"], ["--max-offsets-per-trigger", "0"]])
def test_invalid_options_exit_with_a_message(bad: list[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        config_from_args(build_parser().parse_args(bad))
    assert "error:" in str(exc.value)
