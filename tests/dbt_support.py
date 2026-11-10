"""Helpers to run the dbt project from tests (local DuckDB build, offline BigQuery compile)."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

REPO_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = REPO_ROOT / "dbt"


def copy_project(destination: Path) -> Path:
    """Copy the dbt project (without build output) so tests never touch the repo's target/."""
    project = destination / "dbt"
    shutil.copytree(
        DBT_DIR, project, ignore=shutil.ignore_patterns("target", "logs", "dbt_packages")
    )
    return project


def throwaway_keyfile_json() -> str:
    """A syntactically valid but fake service-account key (never authorised anywhere)."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    return json.dumps(
        {
            "type": "service_account",
            "project_id": "offline-compile-only",
            "private_key_id": "0" * 40,
            "private_key": pem,
            "client_email": "offline@offline-compile-only.iam.gserviceaccount.com",
            "client_id": "0",
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    )


def run_dbt(
    project: Path, *args: str, env: dict[str, str] | None = None, timeout: int = 600
) -> subprocess.CompletedProcess[str]:
    command = [
        sys.executable,
        "-m",
        "dbt.cli.main",
        *args,
        "--project-dir",
        str(project),
        "--profiles-dir",
        str(project),
        "--no-use-colors",
    ]
    full_env = {**os.environ, **(env or {})}
    return subprocess.run(  # noqa: S603 - fixed argv, no shell
        command, capture_output=True, text=True, env=full_env, timeout=timeout, check=False
    )
