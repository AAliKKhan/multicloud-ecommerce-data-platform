from collections import Counter, defaultdict
from datetime import timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from ecomm.contracts import ContractViolation, EventType, parse_event
from ecomm.contracts import payloads as p
from ecomm.generator import Emission, FaultKind, GeneratorConfig, generate

CLEAN = GeneratorConfig(seed=7, orders=400, customers=60)
MESSY = GeneratorConfig(
    seed=7,
    orders=400,
    customers=60,
    malformed_rate=0.05,
    late_rate=0.05,
    out_of_order_rate=0.10,
    duplicate_rate=0.05,
)


@pytest.fixture(scope="module")
def clean() -> list[Emission]:
    return generate(CLEAN).emissions


@pytest.fixture(scope="module")
def messy() -> list[Emission]:
    return generate(MESSY).emissions


def parsed(emissions: list[Emission]) -> list[tuple[Emission, p.Payload, EventType]]:
    out = []
    for e in emissions:
        event = parse_event(e.value)
        out.append((e, event.payload, event.envelope.event_type))
    return out


# --- determinism ------------------------------------------------------------


def test_same_seed_gives_identical_stream() -> None:
    first = generate(MESSY)
    second = generate(MESSY)
    assert first.emissions == second.emissions
    assert first.products == second.products


def test_different_seed_gives_different_stream() -> None:
    other = generate(MESSY.model_copy(update={"seed": 8}))
    assert [e.value for e in other.emissions] != [e.value for e in generate(MESSY).emissions]


# --- clean stream -------------------------------------------------------------


def test_clean_stream_is_fully_valid_and_has_no_faults(clean: list[Emission]) -> None:
    assert all(e.fault is None for e in clean)
    assert len(parsed(clean)) == len(clean)  # raises on any violation


def test_every_event_type_is_generated(clean: list[Emission]) -> None:
    seen = {e.event_type for e in clean}
    assert seen == {t.value for t in EventType}


def test_event_ids_are_unique_in_a_clean_run(clean: list[Emission]) -> None:
    assert len({e.event_id for e in clean}) == len(clean)


def test_events_stay_inside_the_window(clean: list[Emission]) -> None:
    assert all(CLEAN.start <= e.event_time <= CLEAN.end for e in clean)


def test_clean_stream_is_in_event_time_order(clean: list[Emission]) -> None:
    times = [e.event_time for e in clean]
    assert times == sorted(times)
    assert [e.seq for e in clean] == list(range(len(clean)))


def test_orders_match_requested_count(clean: list[Emission]) -> None:
    assert Counter(e.event_type for e in clean)["order_created"] == CLEAN.orders


def test_topics_and_keys(clean: list[Emission]) -> None:
    topics = {e.topic for e in clean}
    assert topics == {
        "commerce.orders",
        "commerce.payments",
        "commerce.inventory",
        "commerce.customers",
    }
    for emission, payload, event_type in parsed(clean):
        if event_type.value.startswith("stock_"):
            assert isinstance(payload, p.InventoryBase)
            assert emission.key == f"{payload.product_id}:{payload.location_id}"
        elif event_type.value.startswith("order_"):
            assert emission.key == getattr(payload, "order_id")  # noqa: B009
        elif event_type.value.startswith("payment_"):
            assert emission.key == getattr(payload, "payment_id")  # noqa: B009
        else:
            assert emission.key == getattr(payload, "customer_id")  # noqa: B009


# --- business constraints -------------------------------------------------------


def test_customers_exist_before_they_order(clean: list[Emission]) -> None:
    created = {}
    for _, payload, event_type in parsed(clean):
        if event_type is EventType.CUSTOMER_CREATED:
            assert isinstance(payload, p.CustomerCreated)
            created[payload.customer_id] = payload.created_at
    for _, payload, event_type in parsed(clean):
        if event_type is EventType.ORDER_CREATED:
            assert isinstance(payload, p.OrderCreated)
            assert created[payload.customer_id] <= payload.order_time


def test_payments_reconcile_to_orders(clean: list[Emission]) -> None:
    totals: dict[str, Decimal] = {}
    captured: dict[str, Decimal] = {}
    refunded: dict[str, Decimal] = defaultdict(Decimal)
    for _, payload, event_type in parsed(clean):
        if event_type is EventType.ORDER_CREATED:
            assert isinstance(payload, p.OrderCreated)
            totals[payload.order_id] = payload.total_amount
        elif event_type is EventType.PAYMENT_CAPTURED:
            assert isinstance(payload, p.PaymentBase)
            captured[payload.order_id] = payload.amount
        elif event_type is EventType.PAYMENT_REFUNDED:
            assert isinstance(payload, p.PaymentBase)
            refunded[payload.order_id] += payload.amount
    assert captured
    for order_id, amount in captured.items():
        assert amount == totals[order_id]
        assert refunded[order_id] <= amount


def test_order_versions_increase_and_follow_creation(clean: list[Emission]) -> None:
    versions: dict[str, list[int]] = defaultdict(list)
    for _, payload, event_type in parsed(clean):
        if event_type is EventType.ORDER_CREATED:
            assert isinstance(payload, p.OrderCreated)
            versions[payload.order_id].append(payload.order_version)
        elif isinstance(payload, p.OrderUpdated | p.OrderCancelled):
            assert payload.order_id in versions, "update seen before creation"
            versions[payload.order_id].append(payload.order_version)
    assert all(v == sorted(v) and v[0] == 1 for v in versions.values())


def test_cancelled_and_failed_paths_exist(clean: list[Emission]) -> None:
    kinds = Counter(e.event_type for e in clean)
    assert kinds["payment_failed"] > 0
    assert kinds["order_cancelled"] > 0
    assert kinds["payment_refunded"] > 0


def test_inventory_never_goes_negative(clean: list[Emission]) -> None:
    on_hand: dict[tuple[str, str], int] = defaultdict(int)
    for _, payload, _ in parsed(clean):
        if isinstance(payload, p.InventoryBase):
            key = (payload.product_id, payload.location_id)
            on_hand[key] += payload.quantity_delta
            assert on_hand[key] >= 0, key


# --- fault injection ---------------------------------------------------------------


def test_malformed_events_are_rejected_and_the_rest_are_valid(messy: list[Emission]) -> None:
    malformed = [e for e in messy if e.fault is FaultKind.MALFORMED]
    assert len(malformed) > 5
    for e in messy:
        if e.fault is FaultKind.MALFORMED:
            with pytest.raises(ContractViolation):
                parse_event(e.value)
        else:
            parse_event(e.value)


def test_malformed_variety(messy: list[Emission]) -> None:
    kinds = {e.fault_detail for e in messy if e.fault is FaultKind.MALFORMED}
    assert len(kinds) >= 5


def test_duplicates_repeat_an_earlier_event(messy: list[Emission]) -> None:
    first_seen: dict[str, Emission] = {}
    duplicates = 0
    for e in messy:
        if e.fault is FaultKind.DUPLICATE:
            duplicates += 1
            original = first_seen[e.event_id]  # the original must already have arrived
            assert original.value == e.value
            assert original.arrival_time <= e.arrival_time
        else:
            first_seen.setdefault(e.event_id, e)
    assert duplicates > 5


def test_late_events_arrive_after_the_threshold(messy: list[Emission]) -> None:
    late = [e for e in messy if e.fault is FaultKind.LATE]
    assert len(late) > 5
    floor = timedelta(minutes=MESSY.late_threshold_minutes * 1.5)
    assert all(e.arrival_time - e.event_time >= floor for e in late)


def test_out_of_order_events_break_event_time_ordering(messy: list[Emission]) -> None:
    inversions = sum(
        1 for a, b in zip(messy, messy[1:], strict=False) if b.event_time < a.event_time
    )
    assert inversions > 5
    assert [e.arrival_time for e in messy] == sorted(e.arrival_time for e in messy)


def test_deduplicating_valid_events_recovers_the_clean_history(messy: list[Emission]) -> None:
    unique = {e.event_id for e in messy if e.fault is not FaultKind.MALFORMED}
    clean_ids = {e.event_id for e in generate(CLEAN).emissions}
    # Faults use their own random stream, so the underlying history is identical.
    assert unique <= clean_ids
    assert len(unique) > 0.8 * len(clean_ids)


# --- configuration -----------------------------------------------------------------------


def test_catalog_is_valid() -> None:
    products = generate(CLEAN).products
    assert len(products) == CLEAN.products
    assert len({x.product_id for x in products}) == len(products)
    assert len({x.sku for x in products}) == len(products)
    assert all(x.unit_price > 0 for x in products)


@pytest.mark.parametrize(
    "overrides",
    [
        {"malformed_rate": 1.5},
        {"malformed_rate": 0.6, "late_rate": 0.6},
        {"orders": 0},
        {"end": CLEAN.start + timedelta(minutes=1)},
        {"products": 5000, "locations": 20},
    ],
)
def test_invalid_config_is_rejected(overrides: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        GeneratorConfig(**overrides)  # type: ignore[arg-type]
