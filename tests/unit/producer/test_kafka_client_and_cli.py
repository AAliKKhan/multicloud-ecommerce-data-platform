from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest

from ecomm.producer.cli import build_parser, main, paced, retopic
from ecomm.producer.kafka_client import (
    build_producer_config,
    check_health,
    create_producer,
    expected_topics,
)
from ecomm.producer.publisher import EventPublisher, PublishItem

DEAD_BROKER = "127.0.0.1:1"  # nothing listens here


def test_producer_config_is_idempotent_acks_all_and_time_bounded() -> None:
    config = build_producer_config("broker:9092")
    assert config["enable.idempotence"] is True
    assert config["acks"] == "all"
    assert config["message.timeout.ms"] <= 60_000  # bounded: outages become failures
    assert config["broker.address.family"] == "v4"
    assert config["bootstrap.servers"] == "broker:9092"


def test_expected_topics_follow_the_contract_and_prefix() -> None:
    assert expected_topics() == [
        "commerce.orders",
        "commerce.payments",
        "commerce.inventory",
        "commerce.customers",
        "commerce.dlq",
    ]
    assert expected_topics("shop", include_dlq=False)[0] == "shop.orders"
    assert len(expected_topics("shop", include_dlq=False)) == 4


def test_retopic_applies_a_custom_prefix() -> None:
    assert retopic("commerce.orders", "commerce") == "commerce.orders"
    assert retopic("commerce.orders", "shop") == "shop.orders"


def test_paced_yields_everything_and_sleeps_between_items() -> None:
    sleeps: list[float] = []
    out = list(
        paced([PublishItem("t", "k", str(i)) for i in range(3)], 10, lambda: False, sleeps.append)
    )
    assert len(out) == 3 and sleeps == [0.1, 0.1, 0.1]


def test_paced_stops_when_asked() -> None:
    stop = threading.Event()
    seen = []
    for item in paced([PublishItem("t", "k", str(i)) for i in range(10)], 0, stop.is_set):
        seen.append(item)
        if len(seen) == 4:
            stop.set()
    assert len(seen) == 4


def test_parser_accepts_generator_and_producer_options() -> None:
    args = build_parser().parse_args(
        [
            "produce",
            "--orders",
            "5",
            "--seed",
            "9",
            "--rate",
            "50",
            "--validate",
            "--max-attempts",
            "2",
        ]
    )
    assert (args.orders, args.seed, args.rate, args.validate, args.max_attempts) == (
        5,
        9,
        50,
        True,
        2,
    )


# --- a real librdkafka client with no broker: failures must be loud ------------------------------


def test_health_check_reports_an_unreachable_broker() -> None:
    started = time.monotonic()
    report = check_health(DEAD_BROKER, expected_topics(), timeout_s=2.0)
    assert not report.ok and report.brokers == 0 and report.error
    assert time.monotonic() - started < 10


def test_publishing_to_a_dead_broker_is_a_visible_failure_not_silence() -> None:
    producer = create_producer(DEAD_BROKER, **{"message.timeout.ms": 1500})
    publisher = EventPublisher(
        producer, max_attempts=2, flush_timeout_s=10, retry_base_delay_s=0.01, sleep=lambda _: None
    )
    report = publisher.publish([PublishItem("commerce.orders", "k1", "v1", "e1")])

    assert not report.ok
    assert report.delivered == 0 and len(report.failed) == 1
    failure = report.failed[0]
    assert failure.event_id == "e1" and failure.attempts == 2 and failure.error


def test_cli_exits_3_with_a_helpful_message_when_kafka_is_down(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code = main(
        [
            "produce",
            "--orders",
            "2",
            "--customers",
            "2",
            "--products",
            "2",
            "--bootstrap",
            DEAD_BROKER,
        ]
    )
    err = capsys.readouterr().err
    assert code == 3
    assert "not ready" in err and "docker compose up" in err


def test_cli_rejects_invalid_generator_arguments(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["produce", "--malformed-rate", "5", "--bootstrap", DEAD_BROKER]) == 2
    assert "invalid arguments" in capsys.readouterr().err


def test_cli_health_exit_code_when_down(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["health", "--bootstrap", DEAD_BROKER, "--timeout", "1.5"]) == 1
    assert json.loads(capsys.readouterr().out)["ok"] is False


def test_report_file_is_not_written_when_nothing_was_attempted(tmp_path: Path) -> None:
    target = tmp_path / "r.json"
    main(
        [
            "produce",
            "--orders",
            "2",
            "--customers",
            "2",
            "--products",
            "2",
            "--bootstrap",
            DEAD_BROKER,
            "--report",
            str(target),
        ]
    )
    assert not target.exists()  # pre-flight failed before publishing
