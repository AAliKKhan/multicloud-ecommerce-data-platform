"""Static checks of the BI stack (compose, secrets, init script). No Docker needed."""

from __future__ import annotations

from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[3]
COMPOSE = yaml.safe_load((REPO / "docker-compose.yml").read_text(encoding="utf-8"))
SERVICES = COMPOSE["services"]
BI = ("metabase-postgres", "metabase")


def test_bi_services_are_opt_in_pinned_and_bounded() -> None:
    for name in BI:
        service = SERVICES[name]
        assert service["profiles"] == ["bi"], name  # never started by a plain `up`
        assert "mem_limit" in service, name
        assert not service["image"].endswith(":latest"), name
    assert "metabase-pgdata" in COMPOSE["volumes"]


def test_only_loopback_ports_are_published() -> None:
    assert SERVICES["metabase"]["ports"] == ["127.0.0.1:${METABASE_PORT:-3001}:3000"]
    assert SERVICES["metabase-postgres"]["ports"] == ["127.0.0.1:${BI_POSTGRES_PORT:-5433}:5432"]


def test_metabase_waits_for_postgres_and_refuses_to_start_without_its_secret() -> None:
    metabase = SERVICES["metabase"]
    assert metabase["depends_on"]["metabase-postgres"]["condition"] == "service_healthy"
    assert metabase["entrypoint"][:3] == ["/bin/sh", "/require-secrets.sh", "MB_DB_PASS"]
    assert "MB_DB_PASS" not in metabase["environment"]  # comes from the generated file
    assert metabase["environment"]["MB_ANON_TRACKING_ENABLED"] == "false"
    assert metabase["environment"]["MB_CHECK_FOR_UPDATES"] == "false"


def test_the_postgres_superuser_password_is_only_given_to_the_postgres_container() -> None:
    postgres_files = [entry["path"] for entry in SERVICES["metabase-postgres"]["env_file"]]
    metabase_files = [entry["path"] for entry in SERVICES["metabase"]["env_file"]]
    assert "docker/bi/.env.postgres" in postgres_files
    assert "docker/bi/.env.postgres" not in metabase_files
    superuser_file = (REPO / "docker" / "bi" / "generate_env.py").read_text(encoding="utf-8")
    assert '"POSTGRES_PASSWORD"' in superuser_file


def test_init_script_creates_least_privilege_roles() -> None:
    script = (REPO / "docker" / "bi" / "init-databases.sh").read_text(encoding="utf-8")
    assert "\r" not in script  # runs in a Linux container
    assert "GRANT USAGE ON SCHEMA analytics TO bi_reader" in script
    assert "GRANT SELECT ON TABLES TO bi_reader" in script
    assert "REVOKE ALL ON DATABASE bi FROM PUBLIC" in script
    assert "SUPERUSER" not in script.upper()  # no role is made a superuser
    assert "bi_reader" in script and "bi_publisher" in script
    require = (REPO / "docker" / "bi" / "require-secrets.sh").read_text(encoding="utf-8")
    assert "\r" not in require


def test_generated_secret_files_are_git_ignored() -> None:
    ignore = (REPO / ".gitignore").read_text(encoding="utf-8").splitlines()
    assert ".env" in ignore and ".env.*" in ignore  # covers docker/bi/.env and .env.postgres
