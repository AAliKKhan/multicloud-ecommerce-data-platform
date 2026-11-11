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


AIRFLOW_SERVICES = [name for name in SERVICES if name.startswith("airflow-")]


def test_no_secret_values_in_compose() -> None:
    """Secrets come from the generated, git-ignored env file; none appear as literal values."""
    sensitive = ("password", "secret", "token", "private_key", "fernet")
    for name, service in SERVICES.items():
        env = service.get("environment") or {}
        for key, value in env.items():
            if any(word in key.lower() for word in sensitive):
                assert str(value).startswith("${"), f"{name}.{key} holds a literal secret"
    text = (REPO / "docker-compose.yml").read_text(encoding="utf-8")
    assert "-----BEGIN" not in text


def test_airflow_services_are_opt_in_bounded_and_loopback_only() -> None:
    assert set(AIRFLOW_SERVICES) == {
        "airflow-postgres", "airflow-init", "airflow-scheduler", "airflow-webserver"
    }  # fmt: skip
    for name in AIRFLOW_SERVICES:
        service = SERVICES[name]
        assert service["profiles"] == ["airflow"], name  # never started by a plain `up`
        assert "mem_limit" in service, name
    assert "ports" not in SERVICES["airflow-postgres"]  # the database is not published
    assert SERVICES["airflow-webserver"]["ports"] == ["127.0.0.1:${AIRFLOW_UI_PORT:-8081}:8080"]


def test_airflow_start_order_secrets_and_mounts() -> None:
    assert (
        SERVICES["airflow-init"]["depends_on"]["airflow-postgres"]["condition"] == "service_healthy"
    )
    for name in ("airflow-scheduler", "airflow-webserver"):
        assert (
            SERVICES[name]["depends_on"]["airflow-init"]["condition"]
            == "service_completed_successfully"
        )
    assert SERVICES["airflow-init"]["restart"] == "no"
    common = SERVICES["airflow-scheduler"]
    assert common["entrypoint"][0] == "/opt/airflow/require-secrets.sh"  # fails with instructions
    assert common["env_file"][0]["path"] == "docker/airflow/.env"
    assert common["environment"]["AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION"] == "true"
    assert common["environment"]["AIRFLOW__API__AUTH_BACKENDS"].endswith("backend.session")
    volumes = common["volumes"]
    for mount in (
        "./airflow/dags:/opt/airflow/dags:ro",
        "./src:/opt/ecomm/src:ro",
        "./dbt:/opt/ecomm/dbt:ro",
    ):
        assert mount in volumes  # code is read-only inside the containers
    assert any(v.endswith(":/data/pipeline") for v in volumes)
    assert {"airflow-pgdata", "airflow-logs"} <= set(COMPOSE["volumes"])


def test_airflow_image_is_pinned_and_the_venv_matches_the_lockfile() -> None:
    text = (REPO / "docker" / "airflow" / "Dockerfile").read_text(encoding="utf-8")
    assert "FROM apache/airflow:2.10.5-python3.11" in text
    lock = (REPO / "uv.lock").read_text(encoding="utf-8")
    for arg, package in (
        ("DUCKDB_VERSION", "duckdb"),
        ("DBT_CORE_VERSION", "dbt-core"),
        ("DBT_DUCKDB_VERSION", "dbt-duckdb"),
        ("PROMETHEUS_CLIENT_VERSION", "prometheus-client"),
        ("PYDANTIC_VERSION", "pydantic"),
        ("PYDANTIC_SETTINGS_VERSION", "pydantic-settings"),
    ):
        pinned = re.search(rf"ARG {arg}=(\S+)", text)
        locked = re.search(rf'name = "{package}"\nversion = "([^"]+)"', lock)
        assert pinned and locked and pinned.group(1) == locked.group(1), (package, arg)


def test_airflow_shell_scripts_use_unix_line_endings() -> None:
    for name in ("init.sh", "require-secrets.sh"):
        raw = (REPO / "docker" / "airflow" / name).read_bytes()
        assert b"\r\n" not in raw and raw.startswith(b"#!/usr/bin/env bash"), name
