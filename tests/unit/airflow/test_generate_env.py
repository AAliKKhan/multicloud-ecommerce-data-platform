from __future__ import annotations

import base64
import importlib.util
import os
import stat
import sys
from pathlib import Path
from types import ModuleType

import pytest

SCRIPT = Path(__file__).resolve().parents[3] / "docker" / "airflow" / "generate_env.py"


@pytest.fixture(scope="module")
def gen() -> ModuleType:
    spec = importlib.util.spec_from_file_location("generate_env_under_test", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def parse(text: str) -> dict[str, str]:
    return dict(
        line.split("=", 1) for line in text.splitlines() if line and not line.startswith("#")
    )


def test_values_are_random_and_consistent(gen: ModuleType) -> None:
    a, b = gen.new_values(), gen.new_values()
    for key in ("POSTGRES_PASSWORD", "AIRFLOW__CORE__FERNET_KEY", "AIRFLOW__WEBSERVER__SECRET_KEY",
                "AIRFLOW_ADMIN_PASSWORD"):  # fmt: skip
        assert a[key] != b[key] and len(a[key]) >= 16
    assert a["POSTGRES_PASSWORD"] in a["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
    assert a["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"].startswith("postgresql+psycopg2://airflow:")
    assert a["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"].endswith("@airflow-postgres:5432/airflow")


def test_the_fernet_key_is_valid_and_the_db_password_is_url_safe(gen: ModuleType) -> None:
    values = gen.new_values()
    assert len(base64.urlsafe_b64decode(values["AIRFLOW__CORE__FERNET_KEY"])) == 32
    assert all(c.isalnum() or c in "-_" for c in values["POSTGRES_PASSWORD"])  # safe inside a URL


def test_writes_the_file_once_and_refuses_to_overwrite(gen: ModuleType, tmp_path: Path,
                                                       capsys: pytest.CaptureFixture[str]) -> None:  # fmt: skip
    target = tmp_path / "airflow" / ".env"
    assert gen.main(["--path", str(target)]) == 0
    first = target.read_text()
    assert "password:" not in capsys.readouterr().out.lower().replace("password: run again", "")
    assert gen.main(["--path", str(target)]) == 0
    assert target.read_text() == first  # untouched: a running database keeps its password
    assert "not overwriting" in capsys.readouterr().out
    assert gen.main(["--path", str(target), "--force"]) == 0
    assert target.read_text() != first


def test_generated_file_has_exactly_what_the_services_require(
    gen: ModuleType, tmp_path: Path
) -> None:
    target = tmp_path / ".env"
    gen.main(["--path", str(target)])
    values = parse(target.read_text())
    required = {"AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "AIRFLOW__CORE__FERNET_KEY",
                "AIRFLOW__WEBSERVER__SECRET_KEY", "POSTGRES_PASSWORD", "AIRFLOW_ADMIN_PASSWORD"}  # fmt: skip
    assert required <= set(values) and all(values[k] for k in required)
    # the entrypoint guard and the generator agree on the names
    guard = (SCRIPT.parent / "require-secrets.sh").read_text()
    for name in ("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "AIRFLOW__CORE__FERNET_KEY",
                 "AIRFLOW__WEBSERVER__SECRET_KEY", "POSTGRES_PASSWORD"):  # fmt: skip
        assert name in guard and name in values


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX permission bits")
def test_the_secrets_file_is_private_to_the_owner(gen: ModuleType, tmp_path: Path) -> None:
    target = tmp_path / ".env"
    gen.main(["--path", str(target)])
    assert stat.S_IMODE(os.stat(target).st_mode) == 0o600


def test_print_password_only_when_asked(gen: ModuleType, tmp_path: Path,
                                        capsys: pytest.CaptureFixture[str]) -> None:  # fmt: skip
    target = tmp_path / ".env"
    gen.main(["--path", str(target)])
    quiet = capsys.readouterr().out
    password = parse(target.read_text())["AIRFLOW_ADMIN_PASSWORD"]
    assert password not in quiet
    gen.main(["--path", str(target), "--print-password"])
    assert password in capsys.readouterr().out


def test_the_real_secrets_file_is_ignored_by_git_and_the_example_has_no_values() -> None:
    repo = SCRIPT.parents[2]
    example = parse((SCRIPT.parent / ".env.example").read_text())
    assert example and all(value == "" for value in example.values())
    assert ".env" in (repo / ".gitignore").read_text().splitlines()
