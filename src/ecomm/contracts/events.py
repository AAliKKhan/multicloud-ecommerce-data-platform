"""Event registry: event types, their payload models, Kafka topics and keys."""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from typing import Any

from ecomm.contracts import payloads as p

SCHEMA_VERSION = 1
SUPPORTED_SCHEMA_VERSIONS = frozenset({SCHEMA_VERSION})
PRODUCER_NAME = "synthetic-commerce-generator"


class EventType(StrEnum):
    ORDER_CREATED = "order_created"
    ORDER_UPDATED = "order_updated"
    ORDER_CANCELLED = "order_cancelled"
    PAYMENT_AUTHORIZED = "payment_authorized"
    PAYMENT_CAPTURED = "payment_captured"
    PAYMENT_FAILED = "payment_failed"
    PAYMENT_REFUNDED = "payment_refunded"
    STOCK_RECEIVED = "stock_received"
    STOCK_RESERVED = "stock_reserved"
    STOCK_ADJUSTED = "stock_adjusted"
    CUSTOMER_CREATED = "customer_created"
    CUSTOMER_UPDATED = "customer_updated"


PAYLOAD_MODELS: dict[EventType, type[p.Payload]] = {
    EventType.ORDER_CREATED: p.OrderCreated,
    EventType.ORDER_UPDATED: p.OrderUpdated,
    EventType.ORDER_CANCELLED: p.OrderCancelled,
    EventType.PAYMENT_AUTHORIZED: p.PaymentBase,
    EventType.PAYMENT_CAPTURED: p.PaymentBase,
    EventType.PAYMENT_FAILED: p.PaymentFailed,
    EventType.PAYMENT_REFUNDED: p.PaymentBase,
    EventType.STOCK_RECEIVED: p.StockReceived,
    EventType.STOCK_RESERVED: p.StockReserved,
    EventType.STOCK_ADJUSTED: p.StockAdjusted,
    EventType.CUSTOMER_CREATED: p.CustomerCreated,
    EventType.CUSTOMER_UPDATED: p.CustomerUpdated,
}

# Topic suffix per event type; the full name is ``<prefix>.<suffix>``.
TOPIC_SUFFIX: dict[EventType, str] = {
    EventType.ORDER_CREATED: "orders",
    EventType.ORDER_UPDATED: "orders",
    EventType.ORDER_CANCELLED: "orders",
    EventType.PAYMENT_AUTHORIZED: "payments",
    EventType.PAYMENT_CAPTURED: "payments",
    EventType.PAYMENT_FAILED: "payments",
    EventType.PAYMENT_REFUNDED: "payments",
    EventType.STOCK_RECEIVED: "inventory",
    EventType.STOCK_RESERVED: "inventory",
    EventType.STOCK_ADJUSTED: "inventory",
    EventType.CUSTOMER_CREATED: "customers",
    EventType.CUSTOMER_UPDATED: "customers",
}

TOPIC_SUFFIXES = ("orders", "payments", "inventory", "customers")
DLQ_SUFFIX = "dlq"

_KEY_FIELD = {
    "orders": "order_id",
    "payments": "payment_id",
    "customers": "customer_id",
}


def topic_name(event_type: EventType, prefix: str = "commerce") -> str:
    return f"{prefix}.{TOPIC_SUFFIX[event_type]}"


def message_key(event_type: EventType, payload: Mapping[str, Any]) -> str:
    """Kafka message key: the entity ID that must stay ordered within a partition."""
    suffix = TOPIC_SUFFIX[event_type]
    if suffix == "inventory":
        return f"{payload['product_id']}:{payload['location_id']}"
    return str(payload[_KEY_FIELD[suffix]])
