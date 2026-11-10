"""Real Kafka plumbing: producer config, health check, topic admin, test consumer.

Kept apart from :mod:`ecomm.producer.publisher` so the publishing logic stays
testable without a broker. ``confluent-kafka`` is an optional extra
(``uv sync --extra kafka``); a missing install gives an actionable error.
"""

from __future__ import annotations

import contextlib
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from ecomm.contracts.events import DLQ_SUFFIX, TOPIC_SUFFIXES
from ecomm.logging_utils import get_logger

# The local broker port is published on the IPv4 loopback only, while "localhost"
# may resolve to ::1 first. Forcing IPv4 avoids multi-second connection stalls.
IPV4 = {"broker.address.family": "v4"}


class KafkaUnavailableError(RuntimeError):
    """confluent-kafka is not installed."""


def _import_kafka() -> Any:
    try:
        import confluent_kafka
    except ImportError as exc:
        raise KafkaUnavailableError(
            "confluent-kafka is not installed; run: uv sync --extra kafka"
        ) from exc
    return confluent_kafka


def expected_topics(prefix: str = "commerce", include_dlq: bool = True) -> list[str]:
    names = [f"{prefix}.{suffix}" for suffix in TOPIC_SUFFIXES]
    if include_dlq:
        names.append(f"{prefix}.{DLQ_SUFFIX}")
    return names


def build_producer_config(
    bootstrap_servers: str,
    *,
    client_id: str = "ecomm-producer",
    message_timeout_ms: int = 30_000,
) -> dict[str, Any]:
    """Idempotent, acks=all producer with bounded delivery time.

    ``message.timeout.ms`` bounds librdkafka's internal retries, so a broker
    outage turns into a visible delivery failure instead of an endless wait.
    """
    return {
        **IPV4,
        "bootstrap.servers": bootstrap_servers,
        "client.id": client_id,
        "enable.idempotence": True,  # implies acks=all, retries, ordering
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 20,
        "message.timeout.ms": message_timeout_ms,
        "request.timeout.ms": 10_000,
        "socket.timeout.ms": 10_000,
        "queue.buffering.max.messages": 100_000,
    }


def create_producer(bootstrap_servers: str, **overrides: Any) -> Any:
    kafka = _import_kafka()
    config = build_producer_config(bootstrap_servers)
    config.update(overrides)
    return kafka.Producer(config, logger=get_logger("ecomm.kafka"))  # structured client logs


@dataclass
class HealthReport:
    ok: bool
    bootstrap_servers: str
    brokers: int = 0
    topics_found: list[str] = field(default_factory=list)
    topics_missing: list[str] = field(default_factory=list)
    error: str | None = None

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "bootstrap_servers": self.bootstrap_servers,
            "brokers": self.brokers,
            "topics_found": sorted(self.topics_found),
            "topics_missing": sorted(self.topics_missing),
            "error": self.error,
        }


def check_health(
    bootstrap_servers: str, topics: list[str] | None = None, timeout_s: float = 5.0
) -> HealthReport:
    """Broker reachable and (optionally) all expected topics present."""
    kafka = _import_kafka()
    producer = kafka.Producer(
        {
            **IPV4,
            "bootstrap.servers": bootstrap_servers,
            "socket.timeout.ms": int(timeout_s * 1000),
            "client.id": "ecomm-healthcheck",
        },
        logger=get_logger("ecomm.kafka", "CRITICAL"),  # failure is reported in the result
    )
    try:
        metadata = producer.list_topics(timeout=timeout_s)
    except kafka.KafkaException as exc:
        return HealthReport(
            False, bootstrap_servers, error=str(exc.args[0]) if exc.args else "error"
        )
    brokers = len(metadata.brokers)
    present = {name for name, topic in metadata.topics.items() if topic.error is None}
    wanted = topics or []
    missing = [t for t in wanted if t not in present]
    return HealthReport(
        ok=brokers > 0 and not missing,
        bootstrap_servers=bootstrap_servers,
        brokers=brokers,
        topics_found=[t for t in wanted if t in present],
        topics_missing=missing,
        error=None if brokers > 0 else "no brokers in metadata",
    )


def ensure_topics(
    bootstrap_servers: str, topics: list[str], partitions: int = 3, timeout_s: float = 30.0
) -> list[str]:
    """Create any missing topics (replication factor 1: single-broker dev stack).

    Returns the names that were created. Existing topics are left untouched.
    """
    kafka = _import_kafka()
    from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined,unused-ignore]

    admin = AdminClient({**IPV4, "bootstrap.servers": bootstrap_servers})
    existing = set(admin.list_topics(timeout=timeout_s).topics)
    todo = [t for t in topics if t not in existing]
    created: list[str] = []
    futures = admin.create_topics(
        [NewTopic(t, num_partitions=partitions, replication_factor=1) for t in todo],
        operation_timeout=timeout_s,
    )
    for name, future in futures.items():
        try:
            future.result(timeout_s)
            created.append(name)
        except kafka.KafkaException as exc:
            if exc.args and exc.args[0].code() != kafka.KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    return sorted(created)


def delete_topics(bootstrap_servers: str, topics: list[str], timeout_s: float = 30.0) -> None:
    from confluent_kafka.admin import AdminClient  # type: ignore[attr-defined,unused-ignore]

    admin = AdminClient({**IPV4, "bootstrap.servers": bootstrap_servers})
    for future in admin.delete_topics(topics, operation_timeout=timeout_s).values():
        with contextlib.suppress(Exception):  # best-effort cleanup
            future.result(timeout_s)


@dataclass(frozen=True)
class ConsumedMessage:
    topic: str
    partition: int
    offset: int
    key: str | None
    value: str
    headers: dict[str, str]
    timestamp_ms: int = 0


def consume_messages(
    bootstrap_servers: str,
    topics: list[str],
    *,
    max_messages: int,
    timeout_s: float = 15.0,
    group_id: str | None = None,
    from_beginning: bool = True,
) -> list[ConsumedMessage]:
    """Read up to ``max_messages`` (used to verify what landed on the topics).

    Uses a unique throw-away consumer group by default so it never disturbs a
    real consumer's committed offsets, and never commits.
    """
    kafka = _import_kafka()
    consumer = kafka.Consumer(
        {
            **IPV4,
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id or f"ecomm-inspect-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest" if from_beginning else "latest",
            "enable.auto.commit": False,
            "session.timeout.ms": 10_000,
        }
    )
    messages: list[ConsumedMessage] = []
    deadline = time.monotonic() + timeout_s
    try:
        consumer.subscribe(topics)
        while len(messages) < max_messages and time.monotonic() < deadline:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                continue  # partition EOF and transient errors: keep polling until deadline
            raw_headers = msg.headers() or []
            messages.append(
                ConsumedMessage(
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    key=msg.key().decode() if msg.key() is not None else None,
                    value=msg.value().decode() if msg.value() is not None else "",
                    headers={k: (v.decode() if v else "") for k, v in raw_headers},
                    timestamp_ms=int(msg.timestamp()[1]),
                )
            )
    finally:
        consumer.close()
    return messages
