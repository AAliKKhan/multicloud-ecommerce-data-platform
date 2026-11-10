import copy
import json
from typing import Any

import pytest

from ecomm.contracts import (
    ContractViolation,
    EventType,
    ViolationCode,
    message_key,
    parse_event,
    topic_name,
)

TS = "2026-11-10T10:15:30.000Z"
UUID_A = "11111111-1111-4111-8111-111111111111"
UUID_B = "22222222-2222-4222-8222-222222222222"

ITEM = {
    "order_item_id": "oi_1",
    "product_id": "prod_1",
    "quantity": 2,
    "unit_price": "10.00",
    "line_discount": "1.00",
    "line_total": "19.00",
}
PAYMENT = {
    "payment_id": "pay_1",
    "order_id": "ord_1",
    "payment_method": "card",
    "amount": "24.00",
    "currency": "USD",
    "provider_reference": "SYN-AB12CD34",
    "payment_time": TS,
}
INVENTORY = {
    "inventory_event_id": "inv_1",
    "product_id": "prod_1",
    "location_id": "loc_1",
    "quantity_delta": 5,
    "reason": "restock",
    "event_time": TS,
}
CUSTOMER = {
    "customer_id": "cus_1",
    "created_at": TS,
    "country_code": "US",
    "city": "Springfield",
    "customer_segment": "new",
    "is_active": True,
}

PAYLOADS: dict[str, dict[str, Any]] = {
    "order_created": {
        "order_id": "ord_1",
        "customer_id": "cus_1",
        "order_status": "created",
        "order_version": 1,
        "order_time": TS,
        "currency": "USD",
        "subtotal": "20.00",
        "discount_amount": "1.00",
        "tax_amount": "1.52",
        "shipping_amount": "4.48",
        "total_amount": "25.00",
        "items": [ITEM],
    },
    "order_updated": {
        "order_id": "ord_1",
        "order_version": 2,
        "updated_at": TS,
        "order_status": "shipped",
    },
    "order_cancelled": {
        "order_id": "ord_1",
        "order_version": 3,
        "cancelled_at": TS,
        "reason_code": "customer_request",
    },
    "payment_authorized": PAYMENT,
    "payment_captured": PAYMENT,
    "payment_failed": {**PAYMENT, "failure_category": "declined"},
    "payment_refunded": PAYMENT,
    "stock_received": INVENTORY,
    "stock_reserved": {**INVENTORY, "quantity_delta": -2, "reason": "order_reservation"},
    "stock_adjusted": {**INVENTORY, "quantity_delta": -1, "reason": "damage"},
    "customer_created": CUSTOMER,
    "customer_updated": {**CUSTOMER, "updated_at": TS, "customer_segment": "vip"},
}


def make_event(event_type: str) -> dict[str, Any]:
    return {
        "event_id": UUID_A,
        "event_type": event_type,
        "schema_version": 1,
        "event_time": TS,
        "producer": "synthetic-commerce-generator",
        "correlation_id": UUID_B,
        "payload": copy.deepcopy(PAYLOADS[event_type]),
    }


def violation(raw: str | bytes) -> ContractViolation:
    with pytest.raises(ContractViolation) as exc:
        parse_event(raw)
    return exc.value


def test_samples_cover_every_event_type() -> None:
    assert set(PAYLOADS) == {t.value for t in EventType}


@pytest.mark.parametrize("event_type", sorted(PAYLOADS))
def test_every_event_type_parses(event_type: str) -> None:
    parsed = parse_event(json.dumps(make_event(event_type)))
    assert parsed.envelope.event_type.value == event_type
    assert str(parsed.envelope.event_id) == UUID_A


@pytest.mark.parametrize("event_type", sorted(PAYLOADS))
def test_topic_and_key_for_every_event_type(event_type: str) -> None:
    et = EventType(event_type)
    assert topic_name(et).startswith("commerce.")
    assert message_key(et, PAYLOADS[event_type])


def test_inventory_key_combines_product_and_location() -> None:
    assert message_key(EventType.STOCK_RECEIVED, INVENTORY) == "prod_1:loc_1"
    assert topic_name(EventType.STOCK_RECEIVED, "shop") == "shop.inventory"


def test_event_time_is_normalised_to_utc_and_serialised_with_z() -> None:
    event = make_event("order_cancelled")
    event["event_time"] = "2026-11-10T12:15:30+02:00"
    parsed = parse_event(json.dumps(event))
    assert parsed.envelope.event_time.isoformat() == "2026-11-10T10:15:30+00:00"
    assert '"event_time":"2026-11-10T10:15:30.000Z"' in parsed.envelope.to_json()


def test_unknown_fields_are_ignored() -> None:
    event = make_event("payment_captured")
    event["new_envelope_field"] = 1
    event["payload"]["new_payload_field"] = "x"
    assert parse_event(json.dumps(event)).payload is not None


def test_invalid_json() -> None:
    assert violation('{"event_id": ').code is ViolationCode.INVALID_JSON
    assert violation(b"\xff\xfe").code is ViolationCode.INVALID_JSON


def test_not_an_object() -> None:
    assert violation("[1, 2]").code is ViolationCode.NOT_AN_OBJECT


def test_missing_envelope_field_is_named() -> None:
    event = make_event("order_cancelled")
    del event["event_id"]
    err = violation(json.dumps(event))
    assert err.code is ViolationCode.MISSING_FIELD
    assert "event_id" in err.detail


def test_unknown_event_type() -> None:
    event = make_event("order_cancelled")
    event["event_type"] = "order_exploded"
    assert violation(json.dumps(event)).code is ViolationCode.UNKNOWN_EVENT_TYPE


def test_unsupported_schema_version() -> None:
    event = make_event("order_cancelled")
    event["schema_version"] = 2
    assert violation(json.dumps(event)).code is ViolationCode.UNSUPPORTED_SCHEMA_VERSION


@pytest.mark.parametrize("bad", ["one", True, 1.5])
def test_schema_version_must_be_integer(bad: Any) -> None:
    event = make_event("order_cancelled")
    event["schema_version"] = bad
    assert violation(json.dumps(event)).code is ViolationCode.INVALID_FIELD


@pytest.mark.parametrize("bad", ["not-a-time", 1731233730, "2026-11-10T10:15:30"])
def test_bad_event_time(bad: Any) -> None:
    event = make_event("order_cancelled")
    event["event_time"] = bad
    assert violation(json.dumps(event)).code is ViolationCode.INVALID_FIELD


def test_bad_event_id() -> None:
    event = make_event("order_cancelled")
    event["event_id"] = "nope"
    assert violation(json.dumps(event)).code is ViolationCode.INVALID_FIELD


def test_payload_missing_required_field() -> None:
    event = make_event("payment_captured")
    del event["payload"]["payment_id"]
    err = violation(json.dumps(event))
    assert err.code is ViolationCode.INVALID_PAYLOAD
    assert "payment_id" in err.detail


def test_payload_must_be_an_object() -> None:
    event = make_event("payment_captured")
    event["payload"] = None
    assert violation(json.dumps(event)).code is ViolationCode.INVALID_FIELD


def test_payment_failed_requires_failure_category() -> None:
    event = make_event("payment_failed")
    del event["payload"]["failure_category"]
    assert violation(json.dumps(event)).code is ViolationCode.INVALID_PAYLOAD


@pytest.mark.parametrize("amount", ["-5.00", "0", "abc", "1.234"])
def test_payment_amount_bounds(amount: str) -> None:
    event = make_event("payment_captured")
    event["payload"]["amount"] = amount
    assert violation(json.dumps(event)).code is ViolationCode.INVALID_PAYLOAD


def test_unknown_currency_is_rejected() -> None:
    event = make_event("payment_captured")
    event["payload"]["currency"] = "XXX"
    assert violation(json.dumps(event)).code is ViolationCode.INVALID_PAYLOAD


def test_order_total_must_reconcile() -> None:
    event = make_event("order_created")
    event["payload"]["total_amount"] = "26.00"
    err = violation(json.dumps(event))
    assert err.code is ViolationCode.INVALID_PAYLOAD
    assert "total_amount" in err.detail


def test_order_line_total_must_reconcile() -> None:
    event = make_event("order_created")
    event["payload"]["items"][0]["line_total"] = "20.00"
    assert violation(json.dumps(event)).code is ViolationCode.INVALID_PAYLOAD


def test_order_requires_items() -> None:
    event = make_event("order_created")
    event["payload"]["items"] = []
    assert violation(json.dumps(event)).code is ViolationCode.INVALID_PAYLOAD


def test_quantity_bounds() -> None:
    event = make_event("order_created")
    event["payload"]["items"][0]["quantity"] = 0
    assert violation(json.dumps(event)).code is ViolationCode.INVALID_PAYLOAD


def test_order_updated_cannot_cancel() -> None:
    event = make_event("order_updated")
    event["payload"]["order_status"] = "cancelled"
    assert violation(json.dumps(event)).code is ViolationCode.INVALID_PAYLOAD


@pytest.mark.parametrize(
    ("event_type", "delta"),
    [("stock_received", -1), ("stock_reserved", 1), ("stock_adjusted", 0), ("stock_received", 0)],
)
def test_inventory_delta_direction(event_type: str, delta: int) -> None:
    event = make_event(event_type)
    event["payload"]["quantity_delta"] = delta
    assert violation(json.dumps(event)).code is ViolationCode.INVALID_PAYLOAD


def test_country_code_must_be_two_uppercase_letters() -> None:
    event = make_event("customer_created")
    event["payload"]["country_code"] = "usa"
    assert violation(json.dumps(event)).code is ViolationCode.INVALID_PAYLOAD


def test_violation_detail_never_echoes_input_values() -> None:
    event = make_event("payment_captured")
    event["payload"]["amount"] = "SECRET-VALUE-123"
    assert "SECRET-VALUE-123" not in violation(json.dumps(event)).detail


def test_no_payload_schema_contains_pii_or_card_fields() -> None:
    forbidden = {"email", "phone", "name", "card_number", "cvv", "password", "address"}
    keys: set[str] = set()
    for payload in PAYLOADS.values():
        keys |= set(payload)
    assert not keys & forbidden
