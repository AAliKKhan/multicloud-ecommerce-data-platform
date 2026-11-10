"""Static safety checks for docker-compose.yml and the topic script (no Docker needed)."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from ecomm.contracts.events import DLQ_SUFFIX, TOPIC_SUFFIXES

REPO = Path(__file__).resolve().parents[3]
COMPOSE = yaml.safe_load((REPO / "docker-compose.yml").read_text(encoding="utf-8"))
SCRIPT = (REPO / "docker" / "kafka" / "create-topics.sh").read_text(encoding="utf-8")
SERVICES = COMPOSE["services"]


def test_images_are_pinned_never_latest() -> None:
    for name, service in SERVICES.items():
        image = service["image"]
        assert ":" in image and not image.endswith(":latest"), f"{name}: {image}"
        assert re.search(r":v?\d", image), f"{name} is not pinned to a version: {image}"


def test_published_ports_are_loopback_only() -> None:
    for name, service in SERVICES.items():
        for port in service.get("ports", []):
            assert str(port).startswith("127.0.0.1:"), f"{name} exposes {port} beyond loopback"


def test_long_running_services_have_health_or_dependency_gates_and_limits() -> None:
    assert "healthcheck" in SERVICES["kafka"]
    for name in ("kafka-init", "kafka-ui"):
        assert SERVICES[name]["depends_on"]["kafka"]["condition"] == "service_healthy"
    for name, service in SERVICES.items():
        assert "mem_limit" in service, f"{name} has no memory limit"
    assert SERVICES["kafka"]["restart"] == "unless-stopped"
    assert SERVICES["kafka-init"]["restart"] == "no"


def test_kafka_data_lives_on_the_named_volume() -> None:
    env = SERVICES["kafka"]["environment"]
    assert env["KAFKA_LOG_DIRS"] == "/var/lib/kafka/data"
    assert "kafka-data:/var/lib/kafka/data" in SERVICES["kafka"]["volumes"]
    assert "kafka-data" in COMPOSE["volumes"]


def test_single_node_settings_are_consistent() -> None:
    env = SERVICES["kafka"]["environment"]
    assert env["KAFKA_PROCESS_ROLES"] == "broker,controller"  # KRaft, no ZooKeeper
    assert env["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"] == 1
    assert env["KAFKA_AUTO_CREATE_TOPICS_ENABLE"] == "false"
    assert "127.0.0.1" in env["KAFKA_ADVERTISED_LISTENERS"]
    assert "kafka:19092" in env["KAFKA_ADVERTISED_LISTENERS"]  # for other containers


def test_ui_has_no_dynamic_config_and_points_at_the_internal_listener() -> None:
    env = SERVICES["kafka-ui"]["environment"]
    assert env["KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS"] == "kafka:19092"
    assert env["DYNAMIC_CONFIG_ENABLED"] == "false"


def test_spark_service_is_opt_in_bounded_and_read_only_for_the_source() -> None:
    spark = SERVICES["spark"]
    assert spark["profiles"] == ["spark"]  # never started by a plain `docker compose up`
    assert spark["depends_on"]["kafka"]["condition"] == "service_healthy"
    assert spark["restart"] == "no"  # a batch job: do not loop on failure
    assert "./src:/app/src:ro" in spark["volumes"]  # the code is mounted read-only
    assert spark["entrypoint"][-2:] == ["-m", "ecomm.spark.job"]  # args are passed to the job
    assert spark["environment"]["KAFKA_BOOTSTRAP_SERVERS"] == "kafka:19092"  # internal listener
    assert spark["mem_limit"] == "2g"


def test_spark_dockerfile_pins_versions_and_bakes_in_the_connector() -> None:
    text = (REPO / "docker" / "spark" / "Dockerfile").read_text(encoding="utf-8")
    assert "FROM python:3.11-slim" in text  # the project needs Python >= 3.11
    assert "SPARK_VERSION=3.5.6" in text and "spark-sql-kafka-0-10_2.12" in text
    assert "numpy==1.26.4" in text  # PySpark 3.5 needs numpy < 2
    assert (REPO / "docker" / "spark" / "dist" / ".gitkeep").is_file()


def test_topic_script_matches_the_event_contract() -> None:
    created = set(re.findall(r"^create (\w+) (\d+)$", SCRIPT, re.MULTILINE))
    names = {name for name, _ in created}
    assert names == set(TOPIC_SUFFIXES) | {DLQ_SUFFIX}
    assert "--if-not-exists" in SCRIPT  # idempotent
    assert "--replication-factor 1" in SCRIPT  # single broker
    assert "KAFKA_TOPIC_PREFIX:-commerce" in SCRIPT


def test_topic_script_uses_unix_line_endings() -> None:
    raw = (REPO / "docker" / "kafka" / "create-topics.sh").read_bytes()
    assert b"\r\n" not in raw and raw.startswith(b"#!/usr/bin/env bash")


def test_no_secrets_in_compose() -> None:
    text = (REPO / "docker-compose.yml").read_text(encoding="utf-8").lower()
    for word in ("password", "secret", "token", "private_key"):
        assert word not in text
