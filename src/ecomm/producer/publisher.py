"""Kafka event publisher: message keys, delivery callbacks, bounded retry.

Delivery guarantee: **at-least-once**. The underlying producer is idempotent
(no duplicates from its own internal retries), but when a message is reported
failed and re-sent by a later round, the first copy may in fact have been
written. Consumers therefore de-duplicate on ``event_id`` (ADR 0001).

Nothing is dropped silently. Every message handed to :meth:`EventPublisher.publish`
ends up in exactly one of ``delivered``, ``failed`` or ``rejected`` (invalid;
only when validation is on); if the run is interrupted, messages that were never
enqueued are not consumed from the input and ``interrupted`` is set.
:attr:`PublishReport.ok` is true only when everything was delivered.

The publisher depends on a small producer protocol, so retry and failure handling
is unit-tested with a fake and exercised against real Kafka in integration tests.
"""

from __future__ import annotations

import json
import logging
import time
from collections import Counter
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from typing import Any, Protocol

from ecomm.contracts import ContractViolation, parse_event
from ecomm.logging_utils import get_logger, log

MAX_FAILURE_SAMPLES = 50


class ProducerLike(Protocol):
    def produce(self, topic: str, **kwargs: Any) -> None: ...

    def poll(self, timeout: float = ...) -> int: ...

    def flush(self, timeout: float = ...) -> int: ...


@dataclass(frozen=True)
class PublishItem:
    topic: str
    key: str
    value: str  # raw message value; may be deliberately invalid
    event_id: str | None = None  # sent as a header when known


def extract_event_id(value: str) -> str | None:
    """Best-effort ``event_id`` from a message value (None for malformed input)."""
    try:
        data = json.loads(value)
    except ValueError:
        return None
    if isinstance(data, dict) and isinstance(data.get("event_id"), str):
        return str(data["event_id"])
    return None


@dataclass(frozen=True)
class FailedMessage:
    topic: str
    key: str
    event_id: str | None
    error: str
    attempts: int
    retriable: bool


@dataclass(frozen=True)
class RejectedMessage:
    topic: str
    key: str
    code: str


@dataclass
class PublishReport:
    attempted: int = 0  # messages enqueued in the first round
    delivered: int = 0
    retried: int = 0  # extra send attempts after a failure
    interrupted: bool = False
    delivered_by_topic: Counter[str] = field(default_factory=Counter)
    failed: list[FailedMessage] = field(default_factory=list)
    rejected: list[RejectedMessage] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failed and not self.rejected and not self.interrupted

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "attempted": self.attempted,
            "delivered": self.delivered,
            "retried": self.retried,
            "failed": len(self.failed),
            "rejected": len(self.rejected),
            "interrupted": self.interrupted,
            "delivered_by_topic": dict(sorted(self.delivered_by_topic.items())),
            "failure_samples": [f.__dict__ for f in self.failed[:MAX_FAILURE_SAMPLES]],
            "rejected_samples": [r.__dict__ for r in self.rejected[:MAX_FAILURE_SAMPLES]],
        }


Numbered = tuple[int, PublishItem]


@dataclass
class _Round:
    sent: dict[int, PublishItem] = field(default_factory=dict)
    delivered: dict[int, tuple[int, int]] = field(default_factory=dict)  # n -> (partition, offset)
    errors: dict[int, tuple[str, bool]] = field(default_factory=dict)  # n -> (error, retriable)
    interrupted: bool = False


# librdkafka reports a message that timed out locally (broker down, network cut) as
# not "retriable" because *it* gave up. For us that is the main case for another
# bounded round, so these local transient errors are treated as retriable.
_TRANSIENT_LOCAL = frozenset(
    {"_MSG_TIMED_OUT", "_TRANSPORT", "_ALL_BROKERS_DOWN", "_TIMED_OUT", "_RESOLVE", "_QUEUE_FULL"}
)
# Retrying cannot fix these: they need a code or configuration change.
_PERMANENT = frozenset(
    {
        "MSG_SIZE_TOO_LARGE",
        "_MSG_SIZE_TOO_LARGE",
        "TOPIC_AUTHORIZATION_FAILED",
        "CLUSTER_AUTHORIZATION_FAILED",
        "SASL_AUTHENTICATION_FAILED",
        "INVALID_TOPIC_EXCEPTION",
        "_INVALID_ARG",
        "_FATAL",
    }
)


def _describe(error: Any) -> tuple[str, bool]:
    try:
        name = str(error.name())
    except Exception:
        name = type(error).__name__
    if name in _PERMANENT:
        return name, False
    if name in _TRANSIENT_LOCAL:
        return name, True
    try:
        return name, bool(error.retriable())
    except Exception:
        return name, True


class EventPublisher:
    def __init__(
        self,
        producer: ProducerLike,
        *,
        max_attempts: int = 3,
        retry_base_delay_s: float = 0.5,
        retry_max_delay_s: float = 8.0,
        flush_timeout_s: float = 30.0,
        queue_full_retries: int = 20,
        queue_full_wait_s: float = 0.25,
        validate: bool = False,
        should_stop: Callable[[], bool] = lambda: False,
        sleep: Callable[[float], None] = time.sleep,
        logger: logging.Logger | None = None,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        self.producer = producer
        self.max_attempts = max_attempts
        self.retry_base_delay_s = retry_base_delay_s
        self.retry_max_delay_s = retry_max_delay_s
        self.flush_timeout_s = flush_timeout_s
        self.queue_full_retries = queue_full_retries
        self.queue_full_wait_s = queue_full_wait_s
        self.validate = validate
        self.should_stop = should_stop
        self.sleep = sleep
        self.log = logger or get_logger("ecomm.producer")

    def backoff(self, attempt: int) -> float:
        """Delay before retry round ``attempt`` (1-based): exponential and capped."""
        return float(min(self.retry_max_delay_s, self.retry_base_delay_s * 2.0 ** (attempt - 1)))

    # --- one send round -------------------------------------------------------------------

    def _produce_one(self, n: int, item: PublishItem, state: _Round) -> None:
        def on_delivery(err: Any, msg: Any) -> None:
            if err is not None:
                state.errors[n] = _describe(err)
            else:
                state.delivered[n] = (int(msg.partition()), int(msg.offset()))

        state.sent[n] = item
        headers = [("event_id", item.event_id.encode())] if item.event_id else None
        for _ in range(self.queue_full_retries + 1):
            try:
                self.producer.produce(
                    item.topic,
                    key=item.key.encode(),
                    value=item.value.encode(),
                    headers=headers,
                    on_delivery=on_delivery,
                )
            except BufferError:
                self.producer.poll(self.queue_full_wait_s)  # let deliveries free queue space
                continue
            except Exception as exc:  # synchronous rejection, e.g. message too large
                state.errors[n] = (type(exc).__name__, False)
            return
        state.errors[n] = ("LOCAL_QUEUE_FULL", True)

    def _send_round(self, numbered: Iterable[Numbered]) -> _Round:
        state = _Round()
        for n, item in numbered:
            if self.should_stop():
                state.interrupted = True
                break
            self._produce_one(n, item, state)
            self.producer.poll(0)
        self.producer.flush(self.flush_timeout_s)
        return state

    # --- public API -------------------------------------------------------------------------

    def publish(self, items: Iterable[PublishItem]) -> PublishReport:
        report = PublishReport()
        numbered: Iterable[Numbered] = enumerate(self._prefilter(items, report))
        attempts: Counter[int] = Counter()

        for attempt in range(1, self.max_attempts + 1):
            state = self._send_round(numbered)
            if attempt == 1:
                report.attempted = len(state.sent)
            retry: list[Numbered] = []
            for n in sorted(state.sent):
                item = state.sent[n]
                attempts[n] += 1
                if n in state.delivered:
                    report.delivered += 1
                    report.delivered_by_topic[item.topic] += 1
                    continue
                error, retriable = state.errors.get(n, ("FLUSH_TIMEOUT", True))
                log(
                    self.log,
                    logging.WARNING,
                    "delivery failed",
                    topic=item.topic,
                    event_id=item.event_id,
                    error=error,
                    attempt=attempts[n],
                    retriable=retriable,
                )
                if retriable and attempts[n] < self.max_attempts and not state.interrupted:
                    retry.append((n, item))
                else:
                    report.failed.append(
                        FailedMessage(
                            item.topic, item.key, item.event_id, error, attempts[n], retriable
                        )
                    )
            if state.interrupted:
                report.interrupted = True
                if attempt > 1:  # retry candidates that were never re-sent must not vanish
                    for n, item in numbered:
                        if n not in state.sent:
                            report.failed.append(
                                FailedMessage(
                                    item.topic,
                                    item.key,
                                    item.event_id,
                                    "INTERRUPTED",
                                    attempts[n],
                                    True,
                                )
                            )
                break
            if not retry:
                break
            report.retried += len(retry)
            delay = self.backoff(attempt)
            log(self.log, logging.INFO, "retrying failed messages", count=len(retry), delay_s=delay)
            self.sleep(delay)
            numbered = retry

        log(
            self.log,
            logging.INFO,
            "publish finished",
            delivered=report.delivered,
            failed=len(report.failed),
            rejected=len(report.rejected),
            retried=report.retried,
            interrupted=report.interrupted,
        )
        return report

    # --- helpers ----------------------------------------------------------------------------

    def _prefilter(
        self, items: Iterable[PublishItem], report: PublishReport
    ) -> Iterator[PublishItem]:
        for item in items:
            if self.validate:
                try:
                    parse_event(item.value)
                except ContractViolation as violation:
                    report.rejected.append(
                        RejectedMessage(item.topic, item.key, violation.code.value)
                    )
                    continue
            yield item
