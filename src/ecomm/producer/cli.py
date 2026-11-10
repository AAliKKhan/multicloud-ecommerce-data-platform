"""``ecomm-kafka``: publish synthetic events to Kafka and inspect the topics.

    docker compose up -d kafka kafka-init kafka-ui
    uv run ecomm-kafka health
    uv run ecomm-kafka produce --orders 100 --rate 20
    uv run ecomm-kafka consume --topic orders --max 5
    uv run ecomm-kafka topics --create         # only needed without the kafka-init job

Exit codes: 0 success, 1 some message not delivered (or unhealthy), 2 bad
arguments, 3 Kafka unreachable or client library missing.
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from types import FrameType

from pydantic import ValidationError

from ecomm.config import ConfigError, load_settings
from ecomm.generator import generate
from ecomm.generator.cli import add_generator_arguments, config_from_args
from ecomm.generator.output import write_products_csv
from ecomm.logging_utils import get_logger
from ecomm.producer.kafka_client import (
    KafkaUnavailableError,
    check_health,
    consume_messages,
    create_producer,
    ensure_topics,
    expected_topics,
)
from ecomm.producer.publisher import EventPublisher, PublishItem, extract_event_id

DEFAULT_PREFIX = "commerce"


def retopic(topic: str, prefix: str) -> str:
    """The generator names topics ``commerce.<suffix>``; apply a custom prefix."""
    return topic if prefix == DEFAULT_PREFIX else f"{prefix}.{topic.split('.', 1)[1]}"


def paced(
    items: Iterable[PublishItem],
    rate: float,
    should_stop: Callable[[], bool],
    sleep: Callable[[float], None] = time.sleep,
) -> Iterator[PublishItem]:
    """Yield items at ``rate`` per second (0 = unthrottled), stopping early on request."""
    delay = 1.0 / rate if rate > 0 else 0.0
    for item in items:
        if should_stop():
            return
        yield item
        if delay:
            sleep(delay)


def install_stop_handlers(stop: threading.Event) -> None:
    def handler(signum: int, frame: FrameType | None) -> None:
        stop.set()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


def _common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--bootstrap", help="bootstrap servers (default: KAFKA_BOOTSTRAP_SERVERS)")
    parser.add_argument("--topic-prefix", help="topic prefix (default: KAFKA_TOPIC_PREFIX)")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ecomm-kafka", description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    produce = sub.add_parser("produce", help="generate events and publish them")
    add_generator_arguments(produce)
    _common(produce)
    produce.add_argument("--rate", type=float, default=0.0, help="events per second (0 = max)")
    produce.add_argument(
        "--validate",
        action="store_true",
        help="validate at the producer boundary and refuse invalid messages "
        "(default: send everything so the consumer-side quarantine is exercised)",
    )
    produce.add_argument("--max-attempts", type=int, default=3)
    produce.add_argument("--flush-timeout", type=float, default=30.0)
    produce.add_argument("--report", type=Path, help="also write the JSON report to this file")
    produce.add_argument(
        "--no-simulated-timestamps",
        action="store_true",
        help="use the real send time as the Kafka timestamp instead of the generator's "
        "simulated arrival time (with the default, replaying a past window still shows "
        "realistic late events to consumers)",
    )
    produce.add_argument(
        "--products-csv", type=Path, help="also write the product catalogue seed (dbt seed)"
    )

    health = sub.add_parser("health", help="check broker reachability and topics")
    _common(health)
    health.add_argument("--timeout", type=float, default=5.0)

    topics = sub.add_parser("topics", help="list the pipeline topics, optionally create them")
    _common(topics)
    topics.add_argument("--create", action="store_true", help="create missing topics")
    topics.add_argument("--partitions", type=int, default=3)

    consume = sub.add_parser("consume", help="read messages (does not commit offsets)")
    _common(consume)
    consume.add_argument("--topic", required=True, help="suffix (orders) or full topic name")
    consume.add_argument("--max", type=int, default=5)
    consume.add_argument("--timeout", type=float, default=15.0)
    consume.add_argument("--latest", action="store_true", help="only new messages")
    return parser


def _resolve(args: argparse.Namespace) -> tuple[str, str]:
    overrides = {}
    if args.bootstrap:
        overrides["kafka_bootstrap_servers"] = args.bootstrap
    if args.topic_prefix:
        overrides["kafka_topic_prefix"] = args.topic_prefix
    settings = load_settings(**overrides)
    return settings.kafka_bootstrap_servers, settings.kafka_topic_prefix


def _print(obj: object) -> None:
    print(json.dumps(obj, indent=2))


def cmd_health(args: argparse.Namespace, bootstrap: str, prefix: str) -> int:
    report = check_health(bootstrap, expected_topics(prefix), args.timeout)
    _print(report.to_json_dict())
    return 0 if report.ok else 1


def cmd_topics(args: argparse.Namespace, bootstrap: str, prefix: str) -> int:
    wanted = expected_topics(prefix)
    created = ensure_topics(bootstrap, wanted, args.partitions) if args.create else []
    report = check_health(bootstrap, wanted)
    _print({**report.to_json_dict(), "created": created})
    return 0 if report.ok else 1


def cmd_consume(args: argparse.Namespace, bootstrap: str, prefix: str) -> int:
    topic = args.topic if "." in args.topic else f"{prefix}.{args.topic}"
    messages = consume_messages(
        bootstrap,
        [topic],
        max_messages=args.max,
        timeout_s=args.timeout,
        from_beginning=not args.latest,
    )
    for m in messages:
        shown = m.value if len(m.value) <= 160 else m.value[:160] + "..."
        print(f"{m.topic}[{m.partition}]@{m.offset} key={m.key} headers={m.headers} value={shown}")
    print(f"{len(messages)} message(s)", file=sys.stderr)
    return 0 if messages else 1


def cmd_produce(args: argparse.Namespace, bootstrap: str, prefix: str) -> int:
    try:
        config = config_from_args(args)
    except (ValidationError, ValueError) as exc:
        print(f"error: invalid arguments: {exc}", file=sys.stderr)
        return 2

    health = check_health(bootstrap, expected_topics(prefix))
    if not health.ok:
        print(
            f"error: Kafka at {bootstrap} is not ready ({health.error or 'topics missing: '}"
            f"{','.join(health.topics_missing)}).\n"
            "Start it with: docker compose up -d kafka kafka-init",
            file=sys.stderr,
        )
        return 3

    stop = threading.Event()
    install_stop_handlers(stop)
    generated = generate(config)
    if args.products_csv:
        write_products_csv(generated, args.products_csv)
    items = (
        PublishItem(
            topic=retopic(e.topic, prefix),
            key=e.key,
            value=e.value,
            event_id=extract_event_id(e.value),
            timestamp_ms=None
            if args.no_simulated_timestamps
            else int(e.arrival_time.timestamp() * 1000),
        )
        for e in generated.emissions
    )
    producer = create_producer(bootstrap)
    publisher = EventPublisher(
        producer,
        max_attempts=args.max_attempts,
        flush_timeout_s=args.flush_timeout,
        validate=args.validate,
        should_stop=stop.is_set,
        logger=get_logger("ecomm.producer"),
    )
    report = publisher.publish(paced(items, args.rate, stop.is_set))
    payload = report.to_json_dict()
    _print(payload)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return 0 if report.ok else 1


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        bootstrap, prefix = _resolve(args)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    handlers = {
        "produce": cmd_produce,
        "health": cmd_health,
        "topics": cmd_topics,
        "consume": cmd_consume,
    }
    try:
        return handlers[args.command](args, bootstrap, prefix)
    except KafkaUnavailableError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
