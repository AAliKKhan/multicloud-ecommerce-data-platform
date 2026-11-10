from __future__ import annotations

from datetime import UTC, datetime, timedelta

from ecomm.generator import GeneratorConfig

LATE_THRESHOLD = timedelta(minutes=60)
PROCESSED_AT = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)

CONFIG = GeneratorConfig(
    seed=11,
    orders=250,
    customers=40,
    malformed_rate=0.05,
    late_rate=0.05,
    out_of_order_rate=0.10,
    duplicate_rate=0.06,
)
