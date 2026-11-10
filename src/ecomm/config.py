"""Typed, validated runtime configuration.

Local mode needs no cloud credentials. Cloud profiles (``dev-cloud`` and
``prod-like``) require explicit bucket, project and dataset settings and fail
at startup with an actionable message listing every missing variable.

No secrets are stored here: cloud authentication uses the standard AWS/GCP
credential chains (environment, workload identity), never this config.
"""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Any

from pydantic import ValidationError, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_VALID_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})
# Conservative intersection of S3 and GCS bucket naming rules.
_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")

_CLOUD_REQUIRED = (
    "aws_region",
    "aws_s3_bucket",
    "gcp_project_id",
    "gcp_region",
    "gcs_bucket",
    "bigquery_dataset",
)


class ConfigError(ValueError):
    """Raised when configuration is missing or invalid."""


class AppEnv(StrEnum):
    LOCAL = "local"
    DEV_CLOUD = "dev-cloud"
    PROD_LIKE = "prod-like"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    app_env: AppEnv = AppEnv.LOCAL
    log_level: str = "INFO"

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_prefix: str = "commerce"

    # Event-time lateness policy used by streaming windows (see ADR 0002).
    late_event_threshold_minutes: int = 60

    aws_region: str | None = None
    aws_s3_bucket: str | None = None
    gcp_project_id: str | None = None
    gcp_region: str | None = None
    gcs_bucket: str | None = None
    bigquery_dataset: str | None = None

    @property
    def is_cloud(self) -> bool:
        return self.app_env is not AppEnv.LOCAL

    def topic(self, name: str) -> str:
        """Return the fully-qualified Kafka topic name, e.g. ``commerce.orders``."""
        return f"{self.kafka_topic_prefix}.{name}"

    @field_validator("log_level")
    @classmethod
    def _check_log_level(cls, value: str) -> str:
        upper = value.upper()
        if upper not in _VALID_LOG_LEVELS:
            raise ValueError(f"LOG_LEVEL must be one of {sorted(_VALID_LOG_LEVELS)}, got {value!r}")
        return upper

    @field_validator("late_event_threshold_minutes")
    @classmethod
    def _check_threshold(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("LATE_EVENT_THRESHOLD_MINUTES must be a positive integer")
        return value

    @field_validator("aws_s3_bucket", "gcs_bucket")
    @classmethod
    def _check_bucket(cls, value: str | None) -> str | None:
        if value is not None and not _BUCKET_RE.fullmatch(value):
            raise ValueError(
                f"bucket name {value!r} is invalid: use 3-63 lowercase letters, "
                "digits, dots or hyphens"
            )
        return value

    @model_validator(mode="after")
    def _require_cloud_settings(self) -> Settings:
        if not self.is_cloud:
            return self
        missing = [name.upper() for name in _CLOUD_REQUIRED if not getattr(self, name)]
        if missing:
            raise ValueError(
                f"APP_ENV={self.app_env.value} requires these variables: {', '.join(missing)}. "
                "Set them in your environment or .env (see .env.example), "
                "or use APP_ENV=local."
            )
        return self


def load_settings(**overrides: Any) -> Settings:
    """Load settings, converting validation errors into one actionable ``ConfigError``."""
    try:
        return Settings(**overrides)
    except ValidationError as exc:
        problems = []
        for err in exc.errors():
            location = ".".join(str(part) for part in err["loc"]).upper()
            detail = err["msg"].removeprefix("Value error, ")
            problems.append(f"{location}: {detail}" if location else detail)
        raise ConfigError("Invalid configuration:\n- " + "\n- ".join(problems)) from exc
