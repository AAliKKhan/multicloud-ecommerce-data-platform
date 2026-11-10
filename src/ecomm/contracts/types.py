"""Shared field types for event contracts.

- Timestamps must be ISO-8601 *strings* (or ``datetime`` objects when built in
  code) with an explicit timezone, normalised to UTC and written as
  ``YYYY-MM-DDTHH:MM:SS.mmmZ``. Bare numbers are rejected on purpose.
- Money is ``Decimal`` with two decimal places and is serialised as a JSON
  string (``"19.99"``) so no precision is lost between systems.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any

from pydantic import AfterValidator, BeforeValidator, Field, PlainSerializer, StringConstraints


def _require_timestamp_input(value: Any) -> Any:
    if isinstance(value, datetime | str):
        return value
    raise ValueError("timestamp must be an ISO-8601 string")


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return value.astimezone(UTC)


def format_utc(value: datetime) -> str:
    """Format as ``2026-11-10T10:15:30.000Z``."""
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


UtcDateTime = Annotated[
    datetime,
    BeforeValidator(_require_timestamp_input),
    AfterValidator(_to_utc),
    PlainSerializer(format_utc, return_type=str, when_used="json"),
]

Money = Annotated[Decimal, Field(ge=0, max_digits=12, decimal_places=2)]
PositiveMoney = Annotated[Decimal, Field(gt=0, max_digits=12, decimal_places=2)]

Identifier = Annotated[
    str, StringConstraints(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.:-]+$")
]
CountryCode = Annotated[str, StringConstraints(pattern=r"^[A-Z]{2}$")]
ShortText = Annotated[str, StringConstraints(min_length=1, max_length=80)]


class Currency(StrEnum):
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"


class OrderStatus(StrEnum):
    CREATED = "created"
    CONFIRMED = "confirmed"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"


class CancellationReason(StrEnum):
    CUSTOMER_REQUEST = "customer_request"
    PAYMENT_FAILED = "payment_failed"
    OUT_OF_STOCK = "out_of_stock"
    FRAUD_SUSPECTED = "fraud_suspected"


class PaymentMethod(StrEnum):
    CARD = "card"
    WALLET = "wallet"
    BANK_TRANSFER = "bank_transfer"
    GIFT_CARD = "gift_card"


class FailureCategory(StrEnum):
    INSUFFICIENT_FUNDS = "insufficient_funds"
    DECLINED = "declined"
    TIMEOUT = "timeout"
    RISK_REJECTED = "risk_rejected"


class CustomerSegment(StrEnum):
    NEW = "new"
    REGULAR = "regular"
    VIP = "vip"


class InventoryReason(StrEnum):
    RESTOCK = "restock"
    ORDER_RESERVATION = "order_reservation"
    CYCLE_COUNT = "cycle_count"
    DAMAGE = "damage"
