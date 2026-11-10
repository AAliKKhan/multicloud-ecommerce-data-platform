"""Generator configuration (all behaviour is derived from ``seed``)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ecomm.contracts.events import PRODUCER_NAME
from ecomm.contracts.types import UtcDateTime

DEFAULT_START = datetime(2026, 11, 1, tzinfo=UTC)


class GeneratorConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    seed: int = 42
    start: UtcDateTime = DEFAULT_START
    end: UtcDateTime = DEFAULT_START + timedelta(days=1)

    orders: int = Field(default=200, ge=1, le=1_000_000)
    customers: int = Field(default=50, ge=1, le=100_000)
    products: int = Field(default=30, ge=1, le=5_000)
    locations: int = Field(default=3, ge=1, le=20)

    # Probabilities per base event. malformed/late/out_of_order are mutually
    # exclusive per event; duplicates can accompany any well-formed event.
    malformed_rate: float = Field(default=0.0, ge=0, le=1)
    late_rate: float = Field(default=0.0, ge=0, le=1)
    out_of_order_rate: float = Field(default=0.0, ge=0, le=1)
    duplicate_rate: float = Field(default=0.0, ge=0, le=1)

    # Late events arrive 1.5x-4x this many minutes after their event time.
    late_threshold_minutes: int = Field(default=60, ge=1)
    producer: str = PRODUCER_NAME

    @model_validator(mode="after")
    def _check(self) -> GeneratorConfig:
        if self.end - self.start < timedelta(minutes=10):
            raise ValueError("end must be at least 10 minutes after start")
        if self.malformed_rate + self.late_rate + self.out_of_order_rate > 1:
            raise ValueError("malformed + late + out_of_order rates must not exceed 1")
        if self.products * self.locations > 50_000:
            raise ValueError("products * locations must not exceed 50000")
        return self
