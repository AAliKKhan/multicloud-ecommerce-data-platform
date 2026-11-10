"""Deterministic shop simulation: produces the *clean* event history.

Everything is derived from ``config.seed`` through a single ``random.Random``
so the same configuration always yields byte-identical events. Faults
(duplicates, malformed, late, out-of-order) are layered on afterwards by
:mod:`ecomm.generator.faults`.
"""

from __future__ import annotations

import random
import uuid
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from ecomm.contracts import payloads as p
from ecomm.contracts.envelope import Envelope
from ecomm.contracts.events import SCHEMA_VERSION, EventType
from ecomm.contracts.types import (
    CancellationReason,
    Currency,
    CustomerSegment,
    FailureCategory,
    InventoryReason,
    OrderStatus,
    PaymentMethod,
)
from ecomm.generator.catalog import Product, build_products, money
from ecomm.generator.config import GeneratorConfig

_PLACES = (
    ("US", "Springfield"),
    ("US", "Portland"),
    ("GB", "Leeds"),
    ("DE", "Bremen"),
    ("FR", "Lyon"),
    ("CA", "Calgary"),
    ("PK", "Lahore"),
    ("AU", "Perth"),
)
_TAX_RATE = Decimal("0.08")
_FREE_SHIPPING_FROM = Decimal("100.00")
_FIRST_ORDER_OFFSET_MS = 60_000  # leaves room for opening stock events


@dataclass(frozen=True)
class SimulationResult:
    events: list[Envelope]  # sorted by event_time (stable)
    products: list[Product]


@dataclass(frozen=True)
class _StockPlan:
    time: datetime
    kind: str  # "receive" | "reserve" | "adjust"
    product_id: str
    location_id: str
    delta: int
    correlation_id: uuid.UUID | None


class _Simulation:
    def __init__(self, config: GeneratorConfig) -> None:
        self.cfg = config
        self.rng = random.Random(config.seed)
        self.events: list[Envelope] = []
        self.stock_plans: list[_StockPlan] = []
        self.used_ids: set[str] = set()

    # --- primitives --------------------------------------------------------

    def new_uuid(self) -> uuid.UUID:
        return uuid.UUID(int=self.rng.getrandbits(128), version=4)

    def new_id(self, prefix: str) -> str:
        while True:
            value = f"{prefix}_{self.rng.getrandbits(48):012x}"
            if value not in self.used_ids:
                self.used_ids.add(value)
                return value

    def after(self, moment: datetime, low_s: float, high_s: float) -> datetime:
        millis = int(self.rng.uniform(low_s, high_s) * 1000)
        return moment + timedelta(milliseconds=millis)

    def add(
        self,
        event_type: EventType,
        payload: p.Payload,
        when: datetime,
        correlation_id: uuid.UUID | None = None,
    ) -> None:
        if when > self.cfg.end:  # history is clipped at the end of the window
            return
        self.events.append(
            Envelope(
                event_id=self.new_uuid(),
                event_type=event_type,
                schema_version=SCHEMA_VERSION,
                event_time=when,
                producer=self.cfg.producer,
                correlation_id=correlation_id or self.new_uuid(),
                payload=payload.model_dump(mode="json"),
            )
        )

    # --- customers ---------------------------------------------------------

    def customers(self) -> list[tuple[str, datetime]]:
        cfg, rng = self.cfg, self.rng
        span_ms = int((cfg.end - cfg.start).total_seconds() * 1000)
        offsets = [0, *sorted(rng.randrange(0, span_ms // 20) for _ in range(cfg.customers - 1))]
        created: list[tuple[str, datetime]] = []
        for index, offset in enumerate(offsets, start=1):
            customer_id = f"cus_{index:05d}"
            when = cfg.start + timedelta(milliseconds=offset)
            country, city = rng.choice(_PLACES)
            segment = rng.choices(list(CustomerSegment), [60, 30, 10])[0]
            self.add(
                EventType.CUSTOMER_CREATED,
                p.CustomerCreated(
                    customer_id=customer_id,
                    created_at=when,
                    country_code=country,
                    city=city,
                    customer_segment=segment,
                    is_active=True,
                ),
                when,
            )
            if rng.random() < 0.15:
                changed = self.after(when, 600, 6 * 3600)
                self.add(
                    EventType.CUSTOMER_UPDATED,
                    p.CustomerUpdated(
                        customer_id=customer_id,
                        created_at=when,
                        country_code=country,
                        city=city,
                        customer_segment=rng.choice(list(CustomerSegment)),
                        is_active=rng.random() < 0.95,
                        updated_at=changed,
                    ),
                    changed,
                )
            created.append((customer_id, when))
        return created

    # --- orders ------------------------------------------------------------

    def orders(self, customers: list[tuple[str, datetime]], products: list[Product]) -> None:
        cfg, rng = self.cfg, self.rng
        span_ms = int((cfg.end - cfg.start).total_seconds() * 1000)
        created_times = [created for _, created in customers]
        order_offsets = sorted(
            rng.randrange(_FIRST_ORDER_OFFSET_MS, span_ms + 1) for _ in range(cfg.orders)
        )
        locations = [f"loc_{n:02d}" for n in range(1, cfg.locations + 1)]
        for offset in order_offsets:
            when = cfg.start + timedelta(milliseconds=offset)
            known = bisect_right(created_times, when)  # customers that already exist
            customer_id = customers[rng.randrange(known)][0]
            order_id = self.new_id("ord")
            correlation = self.new_uuid()

            items: list[p.OrderItem] = []
            chosen = rng.sample(
                products, min(len(products), rng.choices([1, 2, 3, 4], [50, 30, 15, 5])[0])
            )
            for index, product in enumerate(chosen, start=1):
                quantity = rng.choices([1, 2, 3, 4, 5], [50, 25, 13, 8, 4])[0]
                gross = product.unit_price * quantity
                discount = money(0)
                if rng.random() < 0.2:
                    discount = money(gross * Decimal(rng.choice(["0.05", "0.10", "0.15", "0.20"])))
                items.append(
                    p.OrderItem(
                        order_item_id=f"{order_id}_i{index}",
                        product_id=product.product_id,
                        quantity=quantity,
                        unit_price=product.unit_price,
                        line_discount=discount,
                        line_total=gross - discount,
                    )
                )
                self.stock_plans.append(
                    _StockPlan(
                        time=when + timedelta(seconds=1),
                        kind="reserve",
                        product_id=product.product_id,
                        location_id=rng.choice(locations),
                        delta=-quantity,
                        correlation_id=correlation,
                    )
                )

            subtotal = sum((i.unit_price * i.quantity for i in items), Decimal(0))
            discount_total = sum((i.line_discount for i in items), Decimal(0))
            net = subtotal - discount_total
            tax = money(net * _TAX_RATE)
            shipping = money(0) if net >= _FREE_SHIPPING_FROM else money(rng.choice([4.99, 9.99]))
            total = net + tax + shipping
            self.add(
                EventType.ORDER_CREATED,
                p.OrderCreated(
                    order_id=order_id,
                    customer_id=customer_id,
                    order_status=OrderStatus.CREATED,
                    order_version=1,
                    order_time=when,
                    currency=Currency.USD,
                    subtotal=subtotal,
                    discount_amount=discount_total,
                    tax_amount=tax,
                    shipping_amount=shipping,
                    total_amount=total,
                    items=items,
                ),
                when,
                correlation,
            )
            if rng.random() < 0.03:  # occasional cycle count / damage
                self.stock_plans.append(
                    _StockPlan(
                        time=when + timedelta(seconds=2),
                        kind="adjust",
                        product_id=rng.choice(products).product_id,
                        location_id=rng.choice(locations),
                        delta=rng.choice([-5, -3, -2, -1, 1, 2, 3]),
                        correlation_id=None,
                    )
                )
            self.lifecycle(order_id, total, when, correlation)

    def lifecycle(self, order_id: str, total: Decimal, placed: datetime, corr: uuid.UUID) -> None:
        rng = self.rng
        payment_id = self.new_id("pay")
        reference = f"SYN-{rng.getrandbits(32):08X}"
        method = rng.choices(list(PaymentMethod), [60, 20, 15, 5])[0]

        def payment(amount: Decimal, when: datetime) -> p.PaymentBase:
            return p.PaymentBase(
                payment_id=payment_id,
                order_id=order_id,
                payment_method=method,
                amount=amount,
                currency=Currency.USD,
                provider_reference=reference,
                payment_time=when,
            )

        paid_at = self.after(placed, 5, 60)
        if rng.random() < 0.07:  # payment attempt fails, order is cancelled
            self.add(
                EventType.PAYMENT_FAILED,
                p.PaymentFailed(
                    **payment(total, paid_at).model_dump(),
                    failure_category=rng.choice(list(FailureCategory)),
                ),
                paid_at,
                corr,
            )
            cancelled_at = self.after(paid_at, 60, 300)
            self.add(
                EventType.ORDER_CANCELLED,
                p.OrderCancelled(
                    order_id=order_id,
                    order_version=2,
                    cancelled_at=cancelled_at,
                    reason_code=CancellationReason.PAYMENT_FAILED,
                ),
                cancelled_at,
                corr,
            )
            return

        self.add(
            EventType.PAYMENT_AUTHORIZED,
            payment(total, paid_at),
            paid_at,
            corr,
        )
        captured_at = self.after(paid_at, 1, 30)
        self.add(
            EventType.PAYMENT_CAPTURED,
            payment(total, captured_at),
            captured_at,
            corr,
        )

        def status(version: int, new_status: OrderStatus, when: datetime) -> None:
            self.add(
                EventType.ORDER_UPDATED,
                p.OrderUpdated(
                    order_id=order_id,
                    order_version=version,
                    updated_at=when,
                    order_status=new_status,
                ),
                when,
                corr,
            )

        confirmed_at = self.after(captured_at, 60, 300)
        status(2, OrderStatus.CONFIRMED, confirmed_at)

        if rng.random() < 0.04:  # customer cancels after payment -> full refund
            cancelled_at = self.after(confirmed_at, 600, 7200)
            self.add(
                EventType.ORDER_CANCELLED,
                p.OrderCancelled(
                    order_id=order_id,
                    order_version=3,
                    cancelled_at=cancelled_at,
                    reason_code=CancellationReason.CUSTOMER_REQUEST,
                ),
                cancelled_at,
                corr,
            )
            refunded_at = self.after(cancelled_at, 60, 600)
            self.add(
                EventType.PAYMENT_REFUNDED,
                payment(total, refunded_at),
                refunded_at,
                corr,
            )
            return

        shipped_at = self.after(confirmed_at, 3600, 86_400)
        status(3, OrderStatus.SHIPPED, shipped_at)
        delivered_at = self.after(shipped_at, 86_400, 5 * 86_400)
        status(4, OrderStatus.DELIVERED, delivered_at)

        if rng.random() < 0.03:  # partial refund after delivery
            fraction = Decimal(rng.choice(["0.10", "0.25", "0.50"]))
            refunded_at = self.after(delivered_at, 3600, 3 * 86_400)
            self.add(
                EventType.PAYMENT_REFUNDED,
                payment(money(total * fraction), refunded_at),
                refunded_at,
                corr,
            )

    # --- inventory ---------------------------------------------------------

    def opening_stock(self, products: list[Product]) -> None:
        rng = self.rng
        tick = 0
        for product in products:
            for n in range(1, self.cfg.locations + 1):
                self.stock_plans.append(
                    _StockPlan(
                        time=self.cfg.start + timedelta(milliseconds=tick),
                        kind="receive",
                        product_id=product.product_id,
                        location_id=f"loc_{n:02d}",
                        delta=rng.randrange(100, 501),
                        correlation_id=None,
                    )
                )
                tick += 1

    def inventory(self) -> None:
        """Replay stock plans in event-time order so on-hand never goes negative.

        When a reservation would overdraw a location, a restock event is
        inserted 1 ms earlier. Adjustments that would overdraw are skipped.
        """
        rng = self.rng
        on_hand: dict[tuple[str, str], int] = {}

        def record(
            event_type: EventType,
            plan: _StockPlan,
            when: datetime,
            delta: int,
            reason: InventoryReason,
        ) -> None:
            model = {
                EventType.STOCK_RECEIVED: p.StockReceived,
                EventType.STOCK_RESERVED: p.StockReserved,
                EventType.STOCK_ADJUSTED: p.StockAdjusted,
            }[event_type]
            self.add(
                event_type,
                model(
                    inventory_event_id=self.new_id("inv"),
                    product_id=plan.product_id,
                    location_id=plan.location_id,
                    quantity_delta=delta,
                    reason=reason,
                    event_time=when,
                ),
                when,
                plan.correlation_id,
            )

        for plan in sorted(self.stock_plans, key=lambda x: x.time):  # stable
            key = (plan.product_id, plan.location_id)
            have = on_hand.get(key, 0)
            if plan.kind == "receive":
                record(
                    EventType.STOCK_RECEIVED, plan, plan.time, plan.delta, InventoryReason.RESTOCK
                )
                on_hand[key] = have + plan.delta
            elif plan.kind == "reserve":
                if have + plan.delta < 0:
                    restock = -plan.delta - have + rng.randrange(50, 301)
                    when = plan.time - timedelta(milliseconds=1)
                    record(EventType.STOCK_RECEIVED, plan, when, restock, InventoryReason.RESTOCK)
                    have += restock
                record(
                    EventType.STOCK_RESERVED,
                    plan,
                    plan.time,
                    plan.delta,
                    InventoryReason.ORDER_RESERVATION,
                )
                on_hand[key] = have + plan.delta
            else:  # adjust
                if have + plan.delta < 0:
                    continue
                reason = InventoryReason.DAMAGE if plan.delta < 0 else InventoryReason.CYCLE_COUNT
                record(EventType.STOCK_ADJUSTED, plan, plan.time, plan.delta, reason)
                on_hand[key] = have + plan.delta


def simulate(config: GeneratorConfig) -> SimulationResult:
    sim = _Simulation(config)
    products = build_products(sim.rng, config.products)
    customers = sim.customers()
    sim.opening_stock(products)
    sim.orders(customers, products)
    sim.inventory()
    events = sorted(sim.events, key=lambda e: e.event_time)  # stable
    return SimulationResult(events=events, products=products)
