"""``ecomm.lake.ready_cli``: the Airflow DAG's "is there anything committed to load?" check.

It replaced a shell ``find`` on a local folder, which could not see a MinIO (or S3) bucket. The exit
codes are a contract with the DAG: 0 = batches present, 99 = nothing committed yet (the DAG skips),
anything else = fail. The one thing it must never do is turn an unreachable lake into 99: a skipped
run looks healthy, so an outage would go unnoticed.
"""

from __future__ import annotations

import re
import time
from datetime import timedelta
from pathlib import Path

import pytest

from ecomm.generator import GeneratorConfig, generate
from ecomm.lake.commit import commit_batch
from ecomm.lake.ingest import classify
from ecomm.lake.raw import PreparedBatch, prepare_batch
from ecomm.lake.ready_cli import EXIT_FAILED, EXIT_NOTHING, EXIT_READY, check, main
from ecomm.lake.sources import messages_from_emissions
from ecomm.storage.interface import StorageError
from ecomm.storage.local import LocalObjectStore
from ecomm.storage.testing import FlakyObjectStore, InMemoryObjectStore

REPO = Path(__file__).resolve().parents[3]
# A throw-away value that is never sent anywhere (nothing listens on port 1).
FAKE_SECRET = "fake-secret-for-tests-only"  # noqa: S105
MINIO_ENV = {
    "ECOMM_S3_BACKEND": "minio",
    "ECOMM_S3_BUCKET": "ecomm-raw-minio",
    "ECOMM_S3_ACCESS_KEY": "pipeline-user",
    "ECOMM_S3_SECRET_KEY": FAKE_SECRET,
    "ECOMM_S3_ENDPOINT_URL": "http://127.0.0.1:1",
}


def batch() -> PreparedBatch:
    config = GeneratorConfig(seed=9, orders=15, customers=5, products=4)
    classified = classify(
        messages_from_emissions(generate(config).emissions),
        set(),
        processed_at=config.end,
        late_threshold=timedelta(minutes=60),
    )
    return prepare_batch(
        classified.valid, source="commerce", run_id="r", batch_id="r-b0", ingested_at=config.end
    )


def test_the_exit_codes_are_the_dags_contract() -> None:
    assert (EXIT_READY, EXIT_NOTHING, EXIT_FAILED) == (0, 99, 1)
    common = (REPO / "airflow" / "dags" / "ecomm_common.py").read_text(encoding="utf-8")
    match = re.search(r"^SKIP_EXIT_CODE = (\d+)", common, re.MULTILINE)
    assert match and int(match.group(1)) == EXIT_NOTHING  # the DAG skips on exactly this code


def test_an_empty_store_means_nothing_to_load_yet() -> None:
    code, message = check(InMemoryObjectStore("s3"), "commerce")
    assert code == EXIT_NOTHING and "nothing to load" in message


def test_a_committed_batch_means_ready() -> None:
    store = InMemoryObjectStore("s3")
    assert commit_batch(batch(), {"s3": store}).status.value == "COMPLETE"
    code, message = check(store, "commerce")
    assert code == EXIT_READY and "manifest.json" in message


def test_data_without_a_manifest_is_not_a_committed_batch() -> None:
    """The manifest is the commit marker: raw objects alone (a half-written batch) do not count."""
    store = InMemoryObjectStore("s3")
    store.write_object("raw/commerce/order_created/event_date=2026-11-01/part-x.jsonl", b"{}\n")
    assert check(store, "commerce")[0] == EXIT_NOTHING


def test_batches_of_another_source_do_not_count() -> None:
    store = InMemoryObjectStore("s3")
    commit_batch(batch(), {"s3": store})
    assert check(store, "another-source")[0] == EXIT_NOTHING


def test_an_unreadable_store_is_a_failure_never_a_skip() -> None:
    store = FlakyObjectStore(InMemoryObjectStore("s3"), fail_reads=1)
    code, message = check(store, "commerce")  # type: ignore[arg-type]
    assert code == EXIT_FAILED and "cannot list" in message
    assert code != EXIT_NOTHING


def test_main_reports_a_missing_lake_directory_and_creates_nothing(tmp_path: Path) -> None:
    nowhere = tmp_path / "none"
    assert main(["--demo-dir", str(nowhere)]) == EXIT_FAILED
    assert not nowhere.exists()  # a typo must not leave an empty lake behind


def test_main_on_an_existing_empty_lake_skips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ECOMM_S3_BACKEND", raising=False)
    LocalObjectStore(tmp_path / "lake" / "s3-local")
    assert main(["--demo-dir", str(tmp_path)]) == EXIT_NOTHING


def test_main_on_a_local_lake_with_a_committed_batch_is_ready(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ECOMM_S3_BACKEND", raising=False)
    store = LocalObjectStore(tmp_path / "lake" / "s3-local")
    commit_batch(batch(), {"s3": store})
    assert main(["--demo-dir", str(tmp_path)]) == EXIT_READY


def test_main_with_minio_selected_but_not_configured_fails_with_the_fix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)  # there is no docker/minio/.env here
    for key in MINIO_ENV:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("ECOMM_S3_BACKEND", "minio")
    assert main(["--demo-dir", str(tmp_path)]) == EXIT_FAILED
    assert "generate_env.py" in capsys.readouterr().err


def test_main_with_an_unreachable_bucket_fails_fast_without_leaking_the_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    for key, value in MINIO_ENV.items():
        monkeypatch.setenv(key, value)  # the endpoint is a closed port
    started = time.monotonic()
    code = main(["--demo-dir", str(tmp_path)])
    elapsed = time.monotonic() - started
    captured = capsys.readouterr()
    assert (
        code == EXIT_FAILED and code != EXIT_NOTHING
    )  # an outage must not look like "nothing yet"
    assert elapsed < 30
    assert FAKE_SECRET not in captured.out + captured.err
    assert "not reachable" in captured.err or "cannot list" in captured.err


def test_the_dag_calls_this_tool_and_no_longer_looks_at_a_local_folder() -> None:
    text = (REPO / "airflow" / "dags" / "raw_to_warehouse_daily.py").read_text(encoding="utf-8")
    assert 'cli("lake_ready")' in text
    assert "find " not in text and "s3-local" not in text and "[ ! -d" not in text
    common = (REPO / "airflow" / "dags" / "ecomm_common.py").read_text(encoding="utf-8")
    assert '"lake_ready": "ecomm.lake.ready_cli"' in common


def test_a_storage_error_is_what_an_unreachable_store_raises() -> None:
    """Guards the assumption behind 'unreadable is a failure': listing raises StorageError."""
    flaky = FlakyObjectStore(InMemoryObjectStore("s3"), fail_reads=1)
    with pytest.raises(StorageError):
        flaky.list_objects("manifests/")
