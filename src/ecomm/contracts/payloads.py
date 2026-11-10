"""Payload schemas, one per event type (schema_version 1).

Compatibility policy: unknown fields are *ignored* (forward compatible), while
missing or invalid required fields are violations. Breaking changes must bump
``schema_version``. Payloads never carry names, e-mail addresses, phone
numbers, card numbers or any other personal data.
"""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ecomm.contracts.types import (
    CancellationReason,
    CountryCode,
    Currency,
    CustomerSegment,
    FailureCategory,
    Identifier,
    InventoryReason,
    Money,
    OrderStatus,
    PaymentMethod,
    PositiveMoney,
    ShortText,
    UtcDateTime,
)


class Payload(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)


# --- customers -------------------------------------------------------------


class CustomerCreated(Payload):
    customer_id: Identifier
    created_at: UtcDateTime
    country_code: CountryCode
    city: ShortText
    customer_segment: CustomerSegment
    is_active: bool


class CustomerUpdated(CustomerCreated):
    updated_at: UtcDateTime


# --- orders ----------------------------------------------------------------


class OrderItem(Payload):
    order_item_id: Identifier
    product_id: Identifier
    quantity: int = Field(ge=1, le=100)
    unit_price: PositiveMoney
    line_discount: Money
    line_total: Money

    @model_validator(mode="after")
    def _check_line(self) -> OrderItem:
        gross = self.unit_price * self.quantity
        if self.line_discount > gross:
            raise ValueError("line_discount exceeds line gross amount")
        if self.line_total != gross - self.line_discount:
            raise ValueError("line_total must equal quantity * unit_price - line_discount")
        return self


class OrderCreated(Payload):
    order_id: Identifier
    customer_id: Identifier
    order_status: OrderStatus
    order_version: int = Field(ge=1)
    order_time: UtcDateTime
    currency: Currency
    subtotal: Money
    discount_amount: Money
    tax_amount: Money
    shipping_amount: Money
    total_amount: PositiveMoney
    items: list[OrderItem] = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def _check_totals(self) -> OrderCreated:
        if self.order_status is not OrderStatus.CREATED:
            raise ValueError("order_created must have order_status 'created'")
        subtotal = sum((i.unit_price * i.quantity for i in self.items), Decimal(0))
        discount = sum((i.line_discount for i in self.items), Decimal(0))
        if self.subtotal != subtotal:
            raise ValueError("subtotal must equal the sum of item gross amounts")
        if self.discount_amount != discount:
            raise ValueError("discount_amount must equal the sum of line discounts")
        expected = subtotal - discount + self.tax_amount + self.shipping_amount
        if self.total_amount != expected:
            raise ValueError("total_amount must equal subtotal - discount + tax + shipping")
        return self


class OrderUpdated(Payload):
    order_id: Identifier
    order_version: int = Field(ge=2)
    updated_at: UtcDateTime
    order_status: OrderStatus

    @model_validator(mode="after")
    def _not_cancel(self) -> OrderUpdated:
        if self.order_status is OrderStatus.CANCELLED:
            raise ValueError("use order_cancelled to cancel an order")
        return self


class OrderCancelled(Payload):
    order_id: Identifier
    order_version: int = Field(ge=2)
    cancelled_at: UtcDateTime
    reason_code: CancellationReason


# --- payments --------------------------------------------------------------


class PaymentBase(Payload):
    """Authorized / captured / refunded. ``amount`` is the amount of *this* event."""

    payment_id: Identifier
    order_id: Identifier
    payment_method: PaymentMethod
    amount: PositiveMoney
    currency: Currency
    provider_reference: Identifier  # synthetic, e.g. SYN-AB12CD34
    payment_time: UtcDateTime


class PaymentFailed(PaymentBase):
    failure_category: FailureCategory


# --- inventory -------------------------------------------------------------


class InventoryBase(Payload):
    inventory_event_id: Identifier
    product_id: Identifier
    location_id: Identifier
    quantity_delta: int = Field(ge=-100_000, le=100_000)
    reason: InventoryReason
    event_time: UtcDateTime

    @model_validator(mode="after")
    def _nonzero(self) -> InventoryBase:
        if self.quantity_delta == 0:
            raise ValueError("quantity_delta must not be zero")
        return self


class StockReceived(InventoryBase):
    @model_validator(mode="after")
    def _positive(self) -> StockReceived:
        if self.quantity_delta < 0:
            raise ValueError("stock_received requires a positive quantity_delta")
        return self


class StockReserved(InventoryBase):
    @model_validator(mode="after")
    def _negative(self) -> StockReserved:
        if self.quantity_delta > 0:
            raise ValueError("stock_reserved requires a negative quantity_delta")
        return self


class StockAdjusted(InventoryBase):
    pass
