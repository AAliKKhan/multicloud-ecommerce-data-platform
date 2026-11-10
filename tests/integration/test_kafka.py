"""Kafka integration tests: need the compose broker (``docker compose up -d kafka``).

They use a throw-away topic prefix per run, so they never touch ``commerce.*``
data, and delete their topics afterwards. Skipped automatically when no broker
is reachable, so a laptop without Docker (or CI without Kafka) stays green.
"""

from __future__ import annotations

import json
import os
import uuid
from collections import Counter, defaultdict
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path

import pytest

from ecomm.generator import GeneratorConfig, generate
from ecomm.producer.cli import main as kafka_main
from ecomm.producer.cli import retopic
from ecomm.producer.kafka_client import (
    NO_EXPIRY,
    check_health,
    consume_messages,
    create_producer,
    delete_topics,
    ensure_topics,
    expected_topics,
)
from ecomm.producer.publisher import EventPublisher, PublishItem, extract_event_id

pytestmark = pytest.mark.integration

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
START = GeneratorConfig().start
CONFIG = GeneratorConfig(
    seed=31,
    orders=60,
    customers=12,
    products=8,
    end=START + timedelta(hours=12),
    malformed_rate=0.05,
    late_rate=0.05,
    out_of_order_rate=0.10,
    duplicate_rate=0.08,
)


@pytest.fixture(scope="module")
def prefix() -> Iterator[str]:
    if not check_health(BOOTSTRAP, timeout_s=3.0).ok:
        pytest.skip(f"no Kafka broker at {BOOTSTRAP} (docker compose up -d kafka)")
    name = f"itest{uuid.uuid4().hex[:8]}"
    topics = expected_topics(name)
    ensure_topics(BOOTSTRAP, topics, partitions=3, retention_ms=NO_EXPIRY)
    yield name
    delete_topics(BOOTSTRAP, topics)


@pytest.fixture(scope="module")
def sent(prefix: str) -> dict[str, object]:
    emissions = generate(CONFIG).emissions
    items = [
        PublishItem(
            retopic(e.topic, prefix),
            e.key,
            e.value,
            extract_event_id(e.value),
            int(e.arrival_time.timestamp() * 1000),
        )
        for e in emissions
    ]
    publisher = EventPublisher(create_producer(BOOTSTRAP), flush_timeout_s=60)
    report = publisher.publish(items)
    topics = expected_topics(prefix, include_dlq=False)
    consumed = consume_messages(BOOTSTRAP, topics, max_messages=len(items), timeout_s=60)
    return {"emissions": emissions, "items": items, "report": report, "consumed": consumed}


def test_every_message_is_delivered_and_can_be_consumed(sent: dict[str, object]) -> None:
    report = sent["report"]
    items = sent["items"]
    consumed = sent["consumed"]
    assert report.ok, report.to_json_dict()  # type: ignore[attr-defined]
    assert report.delivered == len(items)  # type: ignore[attr-defined]
    assert len(consumed) == len(items)  # type: ignore[arg-type]


def test_counts_per_topic_match(sent: dict[str, object], prefix: str) -> None:
    produced = Counter(i.topic for i in sent["items"])  # type: ignore[attr-defined]
    consumed = Counter(m.topic for m in sent["consumed"])  # type: ignore[attr-defined]
    assert consumed == produced
    assert set(produced) == set(expected_topics(prefix, include_dlq=False))


def test_same_key_always_lands_in_the_same_partition_in_order(sent: dict[str, object]) -> None:
    partitions: dict[tuple[str, str], set[int]] = defaultdict(set)
    by_key: dict[tuple[str, str], list[str]] = defaultdict(list)
    for m in sorted(sent["consumed"], key=lambda m: (m.partition, m.offset)):  # type: ignore[attr-defined]
        partitions[(m.topic, m.key or "")].add(m.partition)
        by_key[(m.topic, m.key or "")].append(m.value)
    assert all(len(p) == 1 for p in partitions.values())

    expected: dict[tuple[str, str], list[str]] = defaultdict(list)
    for item in sent["items"]:  # type: ignore[attr-defined]
        expected[(item.topic, item.key)].append(item.value)
    assert by_key == expected  # per-entity order is preserved


def test_headers_carry_event_id_only_when_the_value_has_one(sent: dict[str, object]) -> None:
    for m in sent["consumed"]:  # type: ignore[attr-defined]
        known = extract_event_id(m.value)
        assert m.headers.get("event_id") == known
    assert any(extract_event_id(m.value) is None for m in sent["consumed"])  # malformed exist


def test_kafka_timestamps_carry_the_simulated_arrival_time(sent: dict[str, object]) -> None:
    expected = Counter((i.topic, i.key, i.value, i.timestamp_ms) for i in sent["items"])  # type: ignore[attr-defined]
    actual = Counter((m.topic, m.key, m.value, m.timestamp_ms) for m in sent["consumed"])  # type: ignore[attr-defined]
    assert actual == expected


def test_duplicates_are_not_removed_by_the_producer(sent: dict[str, object]) -> None:
    ids = [extract_event_id(m.value) for m in sent["consumed"] if extract_event_id(m.value)]  # type: ignore[attr-defined]
    assert len(set(ids)) < len(ids)  # dedup is the consumer's job (ADR 0001)


def test_dead_letter_topic_exists_and_is_empty(prefix: str) -> None:
    health = check_health(BOOTSTRAP, expected_topics(prefix))
    assert health.ok and f"{prefix}.dlq" in health.topics_found
    assert consume_messages(BOOTSTRAP, [f"{prefix}.dlq"], max_messages=1, timeout_s=3) == []


def test_publishing_to_a_missing_topic_is_reported_as_a_failure(prefix: str) -> None:
    producer = create_producer(BOOTSTRAP, **{"message.timeout.ms": 3000})
    publisher = EventPublisher(
        producer, max_attempts=2, flush_timeout_s=20, retry_base_delay_s=0.05, sleep=lambda _: None
    )
    report = publisher.publish([PublishItem(f"{prefix}.no_such_topic", "k", "v", "e1")])
    assert not report.ok and report.delivered == 0
    assert report.failed[0].event_id == "e1" and report.failed[0].error


def test_cli_produce_end_to_end_with_a_custom_prefix(
    prefix: str, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    report_file = tmp_path / "report.json"
    code = kafka_main(
        [
            "produce",
            "--orders",
            "10",
            "--customers",
            "4",
            "--products",
            "4",
            "--seed",
            "5",
            "--bootstrap",
            BOOTSTRAP,
            "--topic-prefix",
            prefix,
            "--report",
            str(report_file),
        ]
    )
    assert code == 0
    report = json.loads(report_file.read_text())
    assert report["ok"] and report["delivered"] > 10 and report["failed"] == 0
    assert all(topic.startswith(f"{prefix}.") for topic in report["delivered_by_topic"])
