"""Connection settings for the BI stack, read from the git-ignored ``docker/bi/.env`` file.

Passwords never travel on the command line: they come from the generated env file (or the process
environment, which wins, so CI can inject its own).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_ENV_FILE = Path("docker/bi/.env")


class SettingsError(RuntimeError):
    """The BI settings are missing or incomplete (the message says how to fix it)."""


def read_env_file(path: Path) -> dict[str, str]:
    """Parse ``KEY=value`` lines (comments and blanks ignored). A missing file is an empty dict."""
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


@dataclass(frozen=True)
class BiSettings:
    """Everything the host-side tools need to reach Postgres and Metabase."""

    postgres_host: str
    postgres_port: int
    publisher_password: str
    reader_password: str
    metabase_url: str
    admin_email: str
    admin_password: str
    # Where Metabase (inside the compose network) reaches Postgres. Differs from the host view.
    postgres_host_for_metabase: str = "metabase-postgres"
    postgres_port_for_metabase: int = 5432

    @classmethod
    def load(cls, env_file: Path = DEFAULT_ENV_FILE) -> BiSettings:
        values = {**read_env_file(env_file), **os.environ}
        required = (
            "BI_PUBLISHER_PASSWORD",
            "BI_READER_PASSWORD",
            "METABASE_ADMIN_EMAIL",
            "METABASE_ADMIN_PASSWORD",
        )
        missing = [key for key in required if not values.get(key)]
        if missing:
            raise SettingsError(
                f"missing {', '.join(missing)}. Create the random, git-ignored secrets with: "
                "python docker/bi/generate_env.py"
            )
        return cls(
            postgres_host=values.get("BI_POSTGRES_HOST", "127.0.0.1"),
            postgres_port=int(values.get("BI_POSTGRES_PORT", "5433")),
            publisher_password=values["BI_PUBLISHER_PASSWORD"],
            reader_password=values["BI_READER_PASSWORD"],
            metabase_url=values.get("METABASE_URL", "http://127.0.0.1:3001").rstrip("/"),
            admin_email=values["METABASE_ADMIN_EMAIL"],
            admin_password=values["METABASE_ADMIN_PASSWORD"],
        )

    def publisher_connection(self) -> dict[str, Any]:
        return {
            "host": self.postgres_host,
            "port": self.postgres_port,
            "dbname": "bi",
            "user": "bi_publisher",
            "password": self.publisher_password,
            "connect_timeout": 10,
        }
