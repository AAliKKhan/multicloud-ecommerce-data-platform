from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import pytest

from ecomm.producer.publisher import EventPublisher, PublishItem, extract_event_id


class FakeKafkaError:
    def __init__(self, name: str, retriable: bool = True) -> None:
        self._name = name
        self._retriable = retriable

    def name(self) -> str:
        return self._name

    def retriable(self) -> bool:
        return self._retriable


class FakeMessage:
    def __init__(self, partition: int, offset: int) -> None:
        self._p, self._o = partition, offset

    def partition(self) -> int:
        return self._p

    def offset(self) -> int:
        return self._o


class FakeProducer:
    """Acknowledges on poll()/flush(), like librdkafka's delivery reports.

    ``plan[value]`` is a list of per-attempt outcomes: None = delivered,
    a FakeKafkaError = failed. ``silent`` values are never acknowledged.
    """

    def __init__(
        self,
        plan: dict[str, list[FakeKafkaError | None]] | None = None,
        silent: set[str] | None = None,
        buffer_errors: int = 0,
        sync_error: Callable[[str], Exception | None] | None = None,
    ) -> None:
        self.plan = plan or {}
        self.silent = silent or set()
        self.buffer_errors = buffer_errors
        self.sync_error = sync_error
        self.queue: list[tuple[str, Any]] = []
        self.produced: list[dict[str, Any]] = []
        self.attempts: dict[str, int] = {}
        self.polls: list[float] = []
        self.offset = 0

    def produce(self, topic: str, **kwargs: Any) -> None:
        if self.buffer_errors > 0:
            self.buffer_errors -= 1
            raise BufferError("queue full")
        value = kwargs["value"].decode()
        if self.sync_error and (exc := self.sync_error(value)):
            raise exc
        self.produced.append({"topic": topic, **kwargs})
        self.queue.append((value, kwargs["on_delivery"]))

    def _drain(self) -> None:
        pending, self.queue = self.queue, []
        for value, callback in pending:
            if value in self.silent:
                continue
            attempt = self.attempts.get(value, 0)
            self.attempts[value] = attempt + 1
            outcomes = self.plan.get(value, [])
            outcome = outcomes[attempt] if attempt < len(outcomes) else None
            if outcome is None:
                self.offset += 1
                callback(None, FakeMessage(0, self.offset))
            else:
                callback(outcome, None)

    def poll(self, timeout: float = 0) -> int:
        self.polls.append(timeout)
        self._drain()
        return 0

    def flush(self, timeout: float = 0) -> int:
        self._drain()
        return len(self.queue)


def items(n: int, topic: str = "commerce.orders") -> list[PublishItem]:
    return [PublishItem(topic, f"key-{i}", f"value-{i}", f"evt-{i}") for i in range(n)]


def publisher(producer: FakeProducer, **kwargs: Any) -> tuple[EventPublisher, list[float]]:
    sleeps: list[float] = []
    kwargs.setdefault("sleep", sleeps.append)
    return EventPublisher(producer, **kwargs), sleeps


# --- happy path ----------------------------------------------------------------------------------


def test_all_messages_are_delivered_with_key_and_header() -> None:
    producer = FakeProducer()
    pub, _ = publisher(producer)
    report = pub.publish(items(5))

    assert report.ok and report.delivered == 5 and report.attempted == 5 and report.retried == 0
    first = producer.produced[0]
    assert first["topic"] == "commerce.orders"
    assert first["key"] == b"key-0" and first["value"] == b"value-0"
    assert first["headers"] == [("event_id", b"evt-0")]
    assert report.delivered_by_topic == {"commerce.orders": 5}


def test_message_without_event_id_has_no_header() -> None:
    producer = FakeProducer()
    publisher(producer)[0].publish([PublishItem("t", "k", "v")])
    assert producer.produced[0]["headers"] is None


def test_empty_input_is_a_successful_noop() -> None:
    report = publisher(FakeProducer())[0].publish([])
    assert report.ok and report.delivered == 0


# --- retry behaviour ------------------------------------------------------------------------------


def test_transient_failure_is_retried_and_then_delivered() -> None:
    producer = FakeProducer(plan={"value-1": [FakeKafkaError("REQUEST_TIMED_OUT"), None]})
    pub, sleeps = publisher(producer, retry_base_delay_s=0.5)
    report = pub.publish(items(3))
    assert report.ok and report.delivered == 3 and report.retried == 1
    assert sleeps == [0.5]
    assert producer.attempts["value-1"] == 2 and producer.attempts["value-0"] == 1


def test_backoff_grows_and_is_capped() -> None:
    pub, _ = publisher(FakeProducer(), retry_base_delay_s=1, retry_max_delay_s=5)
    assert [pub.backoff(n) for n in range(1, 6)] == [1, 2, 4, 5, 5]


def test_permanent_failure_is_reported_after_bounded_attempts() -> None:
    always = [FakeKafkaError("BROKER_NOT_AVAILABLE")] * 10
    producer = FakeProducer(plan={"value-0": always})
    pub, sleeps = publisher(producer, max_attempts=3)
    report = pub.publish(items(2))

    assert not report.ok and report.delivered == 1
    [failure] = report.failed
    assert failure.event_id == "evt-0" and failure.error == "BROKER_NOT_AVAILABLE"
    assert failure.attempts == 3 and failure.retriable
    assert producer.attempts["value-0"] == 3  # bounded: never more than max_attempts
    assert len(sleeps) == 2


def test_non_retriable_failure_is_not_retried() -> None:
    producer = FakeProducer(
        plan={"value-0": [FakeKafkaError("MSG_SIZE_TOO_LARGE", retriable=False)]}
    )
    pub, sleeps = publisher(producer, max_attempts=5)
    report = pub.publish(items(1))
    assert not report.ok and report.retried == 0 and sleeps == []
    assert report.failed[0].attempts == 1 and not report.failed[0].retriable


def test_unacknowledged_messages_count_as_failed_flush_timeouts() -> None:
    producer = FakeProducer(silent={"value-1"})
    pub, _ = publisher(producer, max_attempts=2)
    report = pub.publish(items(3))
    assert not report.ok and report.delivered == 2
    assert report.failed[0].error == "FLUSH_TIMEOUT"


def test_synchronous_produce_errors_are_failures_not_crashes() -> None:
    producer = FakeProducer(sync_error=lambda v: ValueError("too big") if v == "value-1" else None)
    report = publisher(producer)[0].publish(items(3))
    assert not report.ok and report.delivered == 2
    assert report.failed[0].error == "ValueError" and not report.failed[0].retriable


def test_full_local_queue_is_waited_out_not_dropped() -> None:
    producer = FakeProducer(buffer_errors=3)
    report = publisher(producer, queue_full_wait_s=0.1)[0].publish(items(2))
    assert report.ok and report.delivered == 2
    assert producer.polls.count(0.1) == 3  # polled to free space, then retried


def test_persistently_full_queue_becomes_a_visible_failure() -> None:
    producer = FakeProducer(buffer_errors=10_000)
    report = publisher(producer, queue_full_retries=2, max_attempts=1)[0].publish(items(1))
    assert not report.ok and report.failed[0].error == "LOCAL_QUEUE_FULL"


def test_every_message_is_accounted_for_exactly_once() -> None:
    plan = {
        "value-1": [FakeKafkaError("REQUEST_TIMED_OUT"), None],
        "value-2": [FakeKafkaError("BROKER_NOT_AVAILABLE")] * 9,
        "value-3": [FakeKafkaError("MSG_SIZE_TOO_LARGE", retriable=False)],
    }
    report = publisher(FakeProducer(plan=plan), max_attempts=3)[0].publish(items(6))
    assert report.delivered + len(report.failed) == 6
    assert {f.event_id for f in report.failed} == {"evt-2", "evt-3"}


# --- validation, interruption, report ---------------------------------------------------------------


def good_value() -> str:
    return json.dumps(
        {
            "event_id": "11111111-1111-4111-8111-111111111111",
            "event_type": "order_cancelled",
            "schema_version": 1,
            "event_time": "2026-11-10T10:15:30.000Z",
            "producer": "p",
            "correlation_id": "22222222-2222-4222-8222-222222222222",
            "payload": {
                "order_id": "o1",
                "order_version": 2,
                "cancelled_at": "2026-11-10T10:15:30.000Z",
                "reason_code": "customer_request",
            },
        }
    )


def test_validation_mode_refuses_invalid_messages_with_a_code() -> None:
    producer = FakeProducer()
    pub, _ = publisher(producer, validate=True)
    batch = [
        PublishItem("t", "k1", good_value()),
        PublishItem("t", "k2", "{not json"),
        PublishItem("t", "k3", json.dumps({"event_id": "x"})),
    ]
    report = pub.publish(batch)
    assert report.delivered == 1 and len(producer.produced) == 1
    assert [r.code for r in report.rejected] == ["INVALID_JSON", "MISSING_FIELD"]
    assert not report.ok  # rejected messages make the run non-OK


def test_default_mode_sends_malformed_messages_through() -> None:
    producer = FakeProducer()
    report = publisher(producer)[0].publish([PublishItem("t", "k", "{not json")])
    assert report.ok and report.delivered == 1


def test_stop_request_interrupts_and_is_reported() -> None:
    producer = FakeProducer()
    calls = {"n": 0}

    def should_stop() -> bool:
        calls["n"] += 1
        return calls["n"] > 3  # allow three messages, then stop

    pub, _ = publisher(producer, should_stop=should_stop)
    report = pub.publish(items(10))
    assert report.interrupted and not report.ok
    assert report.delivered == 3 and report.attempted == 3  # the rest was never consumed


def test_interrupt_during_a_retry_round_loses_no_message() -> None:
    producer = FakeProducer(
        plan={f"value-{i}": [FakeKafkaError("REQUEST_TIMED_OUT")] for i in range(4)}
    )
    state = {"calls": 0}

    def should_stop() -> bool:
        state["calls"] += 1
        return state["calls"] > 5  # 4 sends in round 1, then 1 in the retry round

    report = publisher(producer, should_stop=should_stop, max_attempts=3)[0].publish(items(4))
    assert report.interrupted
    assert report.delivered + len(report.failed) == 4
    assert any(f.error == "INTERRUPTED" for f in report.failed)


def test_report_json_is_serialisable_and_truncates_samples() -> None:
    always = [FakeKafkaError("X", retriable=False)]
    plan = {f"value-{i}": always for i in range(80)}
    report = publisher(FakeProducer(plan=plan))[0].publish(items(80))
    data = report.to_json_dict()
    assert data["failed"] == 80 and len(data["failure_samples"]) == 50
    json.dumps(data)


def test_max_attempts_must_be_positive() -> None:
    with pytest.raises(ValueError, match="max_attempts"):
        EventPublisher(FakeProducer(), max_attempts=0)


def test_extract_event_id() -> None:
    assert extract_event_id(good_value()) == "11111111-1111-4111-8111-111111111111"
    assert extract_event_id("{broken") is None
    assert extract_event_id("[1]") is None
    assert extract_event_id('{"event_id": 5}') is None
