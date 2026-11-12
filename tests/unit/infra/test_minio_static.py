"""Static checks of the MinIO stand-in (compose, image recipe, policy, secrets). No Docker needed."""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

from ecomm.storage.s3 import S3ObjectStore  # noqa: F401 - the adapter MinIO exercises

REPO = Path(__file__).resolve().parents[3]
MINIO_DIR = REPO / "docker" / "minio"
COMPOSE = yaml.safe_load((REPO / "docker-compose.yml").read_text(encoding="utf-8"))
SERVICES = COMPOSE["services"]
TERRAFORM = (REPO / "infra" / "terraform" / "aws" / "main.tf").read_text(encoding="utf-8")
# The last MinIO release; the fix for GHSA-jjjj-jwhf-8rgr (service-account privilege escalation).
PATCHED_TAG = "RELEASE.2025-10-15T17-29-55Z"


def _policy() -> dict[str, Any]:
    text = (MINIO_DIR / "pipeline-policy.json").read_text(encoding="utf-8")
    return json.loads(text.replace("__BUCKET__", "ecomm-raw-minio"))  # type: ignore[no-any-return]


def _terraform_pipeline_prefixes() -> list[str]:
    match = re.search(r"pipeline_prefixes\s*=\s*\[([^\]]*)\]", TERRAFORM)
    assert match, "pipeline_prefixes not found in the Terraform"
    return re.findall(r'"([^"]+)"', match.group(1))


def _terraform_pipeline_actions() -> set[str]:
    block = TERRAFORM.split('data "aws_iam_policy_document" "pipeline"')[1].split(
        'data "aws_iam_policy_document" "analyst"'
    )[0]
    actions: set[str] = set()
    for found in re.findall(r"actions\s*=\s*\[([^\]]*)\]", block):
        actions.update(re.findall(r'"([^"]+)"', found))
    return {a for a in actions if a.startswith("s3:")}  # the optional KMS statement is AWS-only


def test_minio_services_are_opt_in_pinned_and_bounded() -> None:
    for name in ("minio", "minio-init"):
        service = SERVICES[name]
        assert service["profiles"] == ["minio"], name  # never started by a plain `up`
        assert "mem_limit" in service, name
        assert re.search(r":\d", service["image"]) and not service["image"].endswith(":latest")
    assert "build" in SERVICES["minio"]
    assert "minio-data" in COMPOSE["volumes"]
    assert SERVICES["minio-init"]["restart"] == "no"
    assert SERVICES["minio-init"]["depends_on"]["minio"]["condition"] == "service_healthy"


def test_both_ports_are_loopback_only() -> None:
    ports = SERVICES["minio"]["ports"]
    assert ports == [
        "127.0.0.1:${MINIO_PORT:-9000}:9000",
        "127.0.0.1:${MINIO_CONSOLE_PORT:-9001}:9001",
    ]
    assert "ports" not in SERVICES["minio-init"]


def test_the_server_refuses_to_start_without_root_credentials() -> None:
    entrypoint = SERVICES["minio"]["entrypoint"]
    assert entrypoint[:3] == [
        "/bin/sh",
        "/require-secrets.sh",
        "MINIO_ROOT_USER,MINIO_ROOT_PASSWORD",
    ]
    assert "environment" not in SERVICES["minio"]  # no literal credentials anywhere in compose


def test_the_pipeline_users_key_never_reaches_the_server_and_root_never_reaches_the_pipeline() -> (
    None
):
    server_files = [e["path"] for e in SERVICES["minio"]["env_file"]]
    init_files = [e["path"] for e in SERVICES["minio-init"]["env_file"]]
    assert server_files == ["docker/minio/.env.root"]
    assert set(init_files) == {"docker/minio/.env.root", "docker/minio/.env"}
    # No other service reads the root file.
    for name, service in SERVICES.items():
        if name in ("minio", "minio-init"):
            continue
        for entry in service.get("env_file", []):
            path = entry["path"] if isinstance(entry, dict) else entry
            assert "minio/.env.root" not in str(path), name


def test_policy_allows_exactly_what_the_terraform_pipeline_role_allows() -> None:
    actions = {a for s in _policy()["Statement"] for a in s["Action"]}
    assert actions == _terraform_pipeline_actions() == {
        "s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload", "s3:GetBucketLocation",
    }  # fmt: skip


def test_policy_prefixes_equal_the_terraform_prefixes() -> None:
    prefixes = _terraform_pipeline_prefixes()
    statements = {s["Sid"]: s for s in _policy()["Statement"]}
    listed = set(statements["ListPipelinePrefixes"]["Condition"]["StringLike"]["s3:prefix"])
    assert listed == set(prefixes) | {f"{p}*" for p in prefixes}
    objects = set(statements["ReadWritePipelineObjects"]["Resource"])
    assert objects == {f"arn:aws:s3:::ecomm-raw-minio/{p}*" for p in prefixes}


def test_policy_cannot_delete_or_reach_any_other_bucket() -> None:
    policy = _policy()
    for statement in policy["Statement"]:
        assert statement["Effect"] == "Allow"
        assert not any(a in ("s3:*", "*") or "Delete" in a for a in statement["Action"])
        assert all(r.startswith("arn:aws:s3:::ecomm-raw-minio") for r in statement["Resource"])
        assert statement["Resource"] != "*"


def test_init_uses_a_plain_user_and_policy_not_service_accounts() -> None:
    full = (MINIO_DIR / "init.sh").read_text(encoding="utf-8")
    # Judge the commands, not the explanatory comments (which name what is deliberately avoided).
    script = "\n".join(line for line in full.splitlines() if not line.lstrip().startswith("#"))
    assert "admin user add" in script and "admin policy attach" in script
    assert (
        "svcacct" not in script and "service-account" not in script and "sts" not in script.lower()
    )
    assert "anonymous set none" in script  # the bucket is private
    assert "version enable" in script  # like the Terraform bucket
    assert "\r" not in full


def test_the_image_is_built_from_the_patched_pinned_source() -> None:
    text = (MINIO_DIR / "Dockerfile").read_text(encoding="utf-8")
    assert f"ARG MINIO_TAG={PATCHED_TAG}" in text
    assert re.search(r"^ARG MC_TAG=RELEASE\.\d{4}-\d\d-\d\dT[\d-]+Z$", text, re.MULTILINE)
    for line in re.findall(r"^FROM (\S+)", text, re.MULTILINE):
        assert re.search(r":\d", line) and ":latest" not in line, line  # pinned base images
    assert re.search(r"^USER minio$", text, re.MULTILINE)  # never root
    assert "CGO_ENABLED=0" in text
    assert "MINIO_UPDATE=off" in text


def test_spark_gets_the_pipeline_users_key_and_the_in_network_endpoint_never_root() -> None:
    spark = SERVICES["spark"]
    env_files = [e["path"] if isinstance(e, dict) else e for e in spark["env_file"]]
    assert env_files == ["docker/minio/.env"]  # the pipeline user's file, not .env.root
    assert spark["env_file"][0]["required"] is False  # a plain local run needs no MinIO secrets
    env = spark["environment"]
    assert env["ECOMM_S3_ENDPOINT_URL"] == "http://minio:9000"  # a host name inside the network
    assert env["ECOMM_S3_BACKEND"] == "${ECOMM_S3_BACKEND:-local}"  # local unless asked
    assert not {"ECOMM_S3_ACCESS_KEY", "ECOMM_S3_SECRET_KEY"} & set(env)  # never literal in compose
    assert not any(k.startswith("MINIO_ROOT") for k in env)
    # Starting Spark must not drag MinIO in: the two are separate opt-in profiles.
    assert "minio" not in spark["depends_on"]


def test_the_spark_image_has_the_s3_client_at_the_locked_versions() -> None:
    text = (REPO / "docker" / "spark" / "Dockerfile").read_text(encoding="utf-8")
    lock = (REPO / "uv.lock").read_text(encoding="utf-8")
    for arg, package in (("BOTO3_VERSION", "boto3"), ("BOTOCORE_VERSION", "botocore")):
        pinned = re.search(rf"ARG {arg}=(\S+)", text)
        locked = re.search(rf'name = "{package}"\nversion = "([^"]+)"', lock)
        assert pinned and locked and pinned.group(1) == locked.group(1), (package, arg)
    # Last layer: the large PySpark and connector layers above it keep their build cache.
    assert text.index("BOTO3_VERSION") > text.index("spark-sql-kafka-0-10")
    assert text.index("BOTO3_VERSION") < text.index("ENV PYTHONPATH=/app/src")


def _env_file_paths(service: str) -> list[str]:
    return [e["path"] if isinstance(e, dict) else e for e in SERVICES[service]["env_file"]]


@pytest.mark.parametrize(
    "service", ["airflow-scheduler", "airflow-webserver", "airflow-init", "ecomm-exporter"]
)
def test_airflow_and_the_exporter_get_the_pipeline_key_and_the_in_network_endpoint_never_root(
    service: str,
) -> None:
    paths = _env_file_paths(service)
    assert "docker/minio/.env" in paths and not any("minio/.env.root" in p for p in paths)
    minio_entry = next(e for e in SERVICES[service]["env_file"] if e["path"] == "docker/minio/.env")
    assert minio_entry["required"] is False  # a plain local run needs no MinIO secrets
    env = SERVICES[service]["environment"]
    assert env["ECOMM_S3_ENDPOINT_URL"] == "http://minio:9000"  # a host name inside the network
    assert env["ECOMM_S3_BACKEND"] == "${ECOMM_S3_BACKEND:-local}"  # local unless asked
    assert not {"ECOMM_S3_ACCESS_KEY", "ECOMM_S3_SECRET_KEY"} & set(env)  # never literal in compose
    assert not any(k.startswith("MINIO_ROOT") for k in env)
    # Independent profiles: starting these must not drag MinIO in (or fail without it).
    assert "minio" not in (SERVICES[service].get("depends_on") or {})


def test_the_airflow_env_file_order_keeps_its_own_secrets_first() -> None:
    assert _env_file_paths("airflow-scheduler")[0] == "docker/airflow/.env"


def test_the_airflow_and_exporter_images_have_the_s3_client_at_the_locked_versions() -> None:
    lock = (REPO / "uv.lock").read_text(encoding="utf-8")
    for image in ("airflow", "exporter"):
        text = (REPO / "docker" / image / "Dockerfile").read_text(encoding="utf-8")
        for arg, package in (("BOTO3_VERSION", "boto3"), ("BOTOCORE_VERSION", "botocore")):
            pinned = re.search(rf"ARG {arg}=(\S+)", text)
            locked = re.search(rf'name = "{package}"\nversion = "([^"]+)"', lock)
            assert pinned and locked and pinned.group(1) == locked.group(1), (image, package)
    # In the Airflow image the client goes into the isolated project venv, not Airflow's own
    # environment (whose dependencies are pinned by the official constraints).
    airflow = (REPO / "docker" / "airflow" / "Dockerfile").read_text(encoding="utf-8")
    assert "ecomm-venv/bin/pip install" in airflow.split("BOTO3_VERSION}")[0].splitlines()[-2]


def test_the_compose_image_tag_matches_the_pinned_release() -> None:
    assert SERVICES["minio"]["image"] == SERVICES["minio-init"]["image"] == "ecomm-minio:2025-10-15"
    assert "2025-10-15" in PATCHED_TAG


def test_scripts_use_unix_line_endings() -> None:
    for name in ("init.sh", "require-secrets.sh", "Dockerfile", "pipeline-policy.json"):
        assert "\r" not in (MINIO_DIR / name).read_text(encoding="utf-8"), name


def test_secret_files_are_git_ignored() -> None:
    ignore = (REPO / ".gitignore").read_text(encoding="utf-8").splitlines()
    assert ".env" in ignore and ".env.*" in ignore  # covers docker/minio/.env and .env.root


def _generator() -> Any:
    spec = importlib.util.spec_from_file_location(
        "minio_generate_env", MINIO_DIR / "generate_env.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["minio_generate_env"] = module
    spec.loader.exec_module(module)
    return module


def test_generated_secrets_are_random_split_and_never_overwritten(tmp_path: Path) -> None:
    generator = _generator()
    assert generator.main(["--dir", str(tmp_path)]) == 0
    app = dict(
        line.split("=", 1) for line in (tmp_path / ".env").read_text().splitlines() if "=" in line
    )
    root = dict(
        line.split("=", 1)
        for line in (tmp_path / ".env.root").read_text().splitlines()
        if "=" in line
    )
    assert set(root) == {"MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD"}
    assert set(app) == {
        "ECOMM_S3_BUCKET", "ECOMM_S3_ENDPOINT_URL", "ECOMM_S3_ACCESS_KEY", "ECOMM_S3_SECRET_KEY",
    }  # fmt: skip
    assert root["MINIO_ROOT_USER"] != app["ECOMM_S3_ACCESS_KEY"]  # the pipeline is not root
    assert len(app["ECOMM_S3_SECRET_KEY"]) >= 24 and len(root["MINIO_ROOT_PASSWORD"]) >= 24
    assert app["ECOMM_S3_ENDPOINT_URL"].startswith("http://127.0.0.1:")  # loopback
    before = (tmp_path / ".env").read_text()
    generator.main(["--dir", str(tmp_path)])
    assert (tmp_path / ".env").read_text() == before
    generator.main(["--dir", str(tmp_path), "--force"])
    assert (tmp_path / ".env").read_text() != before
