"""Job configuration and failure reporting (no Spark needed)."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from ecomm.spark import job
from ecomm.spark.job import (
    build_parser,
    check_destinations,
    config_from_args,
    make_sink_config,
    summarize_failure,
)
from ecomm.spark.sink import SinkConfig
from ecomm.storage.interface import StorageError
from ecomm.storage.local import LocalObjectStore
from ecomm.storage.s3 import S3ObjectStore
from ecomm.storage.testing import FlakyObjectStore, InMemoryObjectStore

# A throw-away value that is never sent anywhere (nothing listens on port 1).
FAKE_SECRET = "fake-secret-for-tests-only"  # noqa: S105
MINIO_ENV = {
    "ECOMM_S3_BACKEND": "minio",
    "ECOMM_S3_BUCKET": "ecomm-raw-minio",
    "ECOMM_S3_ACCESS_KEY": "pipeline-user",
    "ECOMM_S3_SECRET_KEY": FAKE_SECRET,
    "ECOMM_S3_ENDPOINT_URL": "http://127.0.0.1:1",
}

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


# --- where the raw zone goes: a local directory, or a MinIO bucket ---------------------------------


def job_cfg(tmp_path: Path, *extra: str) -> Any:
    return config_from_args(
        build_parser().parse_args(["--lake-dir", str(tmp_path / "lake"), *extra])
    )


def test_by_default_both_destinations_and_the_control_store_are_local_directories(
    tmp_path: Path,
) -> None:
    sink = make_sink_config(job_cfg(tmp_path), env={})
    assert set(sink.stores) == {"s3-local", "gcs-local"}
    assert all(isinstance(s, LocalObjectStore) for s in sink.stores.values())
    assert isinstance(sink.control_store, LocalObjectStore)
    assert (tmp_path / "lake" / "s3-local").is_dir()  # the layout the loader and the tools expect


def test_the_minio_backend_replaces_only_the_s3_destination(tmp_path: Path) -> None:
    sink = make_sink_config(job_cfg(tmp_path), env=MINIO_ENV)
    assert set(sink.stores) == {"s3-minio", "gcs-local"}
    assert isinstance(sink.stores["s3-minio"], S3ObjectStore)
    assert isinstance(sink.stores["gcs-local"], LocalObjectStore)
    assert isinstance(sink.control_store, LocalObjectStore)  # ledger and progress stay on disk
    assert (tmp_path / "lake" / "gcs-local").is_dir() and not (
        tmp_path / "lake" / "s3-local"
    ).exists()


def test_the_sink_does_not_care_what_the_destinations_are_called() -> None:
    """process_batch is exercised against stores named s3/gcs in test_spark_logic; the real job
    names them s3-minio and gcs-local, so keep that from ever mattering."""
    s3, gcs = InMemoryObjectStore("s3-minio"), InMemoryObjectStore("gcs-local")
    sink = SinkConfig("commerce", "r", {"s3-minio": s3, "gcs-local": gcs}, InMemoryObjectStore("c"))
    assert sorted(sink.stores) == ["gcs-local", "s3-minio"]


def test_a_reachable_pair_of_destinations_passes_the_probe() -> None:
    sink = SinkConfig(
        "commerce",
        "r",
        {"a": InMemoryObjectStore("a"), "b": InMemoryObjectStore("b")},
        InMemoryObjectStore("c"),
    )
    check_destinations(sink)  # does not raise


def test_the_probe_names_the_destination_that_cannot_be_listed() -> None:
    broken = FlakyObjectStore(InMemoryObjectStore("s3-minio"), fail_reads=1)
    sink = SinkConfig(
        "commerce",
        "r",
        {"gcs-local": InMemoryObjectStore("gcs-local"), "s3-minio": broken},  # type: ignore[dict-item]
        InMemoryObjectStore("c"),
    )
    with pytest.raises(StorageError, match="destination s3-minio is not reachable"):
        check_destinations(sink)


def no_spark(*_: Any, **__: Any) -> int:
    raise AssertionError("Spark must not be started")


def test_main_exits_2_without_starting_spark_when_minio_is_selected_but_not_configured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)  # there is no docker/minio/.env here
    for key in MINIO_ENV:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("ECOMM_S3_BACKEND", "minio")
    monkeypatch.setattr(job, "run", no_spark)
    assert job.main(["--lake-dir", str(tmp_path / "lake")]) == 2
    err = capsys.readouterr().err
    assert "storage not configured" in err and "generate_env.py" in err


def test_main_exits_1_without_starting_spark_when_a_destination_is_unreachable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    for key, value in MINIO_ENV.items():
        monkeypatch.setenv(key, value)  # the endpoint is a closed port
    monkeypatch.setattr(job, "run", no_spark)
    assert job.main(["--lake-dir", str(tmp_path / "lake")]) == 1
    err = capsys.readouterr().err
    assert "destination unreachable; not starting Spark" in err and "s3-minio" in err
    assert FAKE_SECRET not in err  # credentials never reach the logs


def test_main_hands_the_prepared_sink_to_the_query_when_everything_is_reachable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ECOMM_S3_BACKEND", raising=False)
    seen: dict[str, Any] = {}

    def fake_run(cfg: Any, sink_cfg: Any = None) -> int:
        seen["destinations"] = sorted(sink_cfg.stores)
        return 0

    monkeypatch.setattr(job, "run", fake_run)
    assert job.main(["--lake-dir", str(tmp_path / "lake")]) == 0
    assert seen["destinations"] == ["gcs-local", "s3-local"]
