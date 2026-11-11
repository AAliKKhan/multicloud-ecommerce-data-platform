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


def test_failure_summary_prefers_our_own_error_message() -> None:
    summary = summarize_failure(STACK)
    assert summary.startswith("ecomm.spark.sink.OffsetGapError: batch itest-b00002")
    assert "lost messages between Kafka and the sink" in summary
    assert "\n" not in summary


def test_a_traceback_source_line_naming_the_error_is_not_mistaken_for_the_message() -> None:
    """Seen in a real run: the traceback shows `raise OffsetGapError(` before the message."""
    text = (
        'File "/app/src/ecomm/spark/sink.py", line 313, in process_batch\n'
        "    raise OffsetGapError(\n"
        "ecomm.spark.sink.OffsetGapError: batch itest-b00003: 2 partition(s) lost messages "
        "between Kafka and the sink\n"
    )
    summary = summarize_failure(text)
    assert summary.startswith("ecomm.spark.sink.OffsetGapError: batch itest-b00003")


@pytest.mark.parametrize("name", ["BatchConflictError", "BatchCommitError"])
def test_other_pipeline_errors_are_also_recognised(name: str) -> None:
    summary = summarize_failure(f"noise\nmodule.{name}: reason here\nmore")
    assert summary.startswith(f"module.{name}: reason here") and "noise" not in summary


def test_unknown_failures_keep_the_tail_where_a_traceback_names_the_exception() -> None:
    text = "Traceback (most recent call last):\n" + "frame " * 500 + "\nKeyError: 'the real cause'"
    summary = summarize_failure(text, limit=60)
    assert len(summary) == 60 and summary.startswith("...")
    assert summary.endswith("KeyError: 'the real cause'")
    assert summarize_failure("short\ntext") == "short text"


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
